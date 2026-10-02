"""Numeric DICOM tag release with suppression, generalization, and Laplace noise.

Records are synthetic tag matrices: group, element, and a number. This module
never accepts a name or a medical record number. The in-process queue stands
in for the Kinesis stream ``imaging.tags.anonymized``. Epsilon spent is the
configured budget split across measurement fields. The release is an
accounting report, not a formal privacy proof.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import statistics
import struct
import sys
from collections import deque
from typing import Final

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)

LOGGER = logging.getLogger("dicom.privacy")

STREAM: Final[str] = "imaging.tags.anonymized"
FRAME: Final[struct.Struct] = struct.Struct("<HHdII")
RING_CAPACITY: Final[int] = 512
AGE_BIN: Final[float] = 5.0
AGE_TOP: Final[float] = 90.0

PATIENT_ID: Final[tuple[int, int]] = (0x0010, 0x0020)
OTHER_PATIENT_ID: Final[tuple[int, int]] = (0x0010, 0x1000)
ACCESSION: Final[tuple[int, int]] = (0x0008, 0x0050)
AGE: Final[tuple[int, int]] = (0x0010, 0x1010)
POSTAL: Final[tuple[int, int]] = (0x0010, 0x1040)
PIXEL: Final[tuple[int, int]] = (0x7FE0, 0x0010)
SLICE_LOCATION: Final[tuple[int, int]] = (0x0020, 0x1041)
EXPOSURE_INDEX: Final[tuple[int, int]] = (0x0018, 0x6020)

DIRECT_IDENTIFIERS: Final[frozenset[tuple[int, int]]] = frozenset(
    {PATIENT_ID, OTHER_PATIENT_ID, ACCESSION}
)
QUASI_IDENTIFIERS: Final[frozenset[tuple[int, int]]] = frozenset({AGE, POSTAL})

# Three-digit prefixes treated as populations under the Safe Harbor 20,000 rule.
RESTRICTED_ZIP3: Final[frozenset[int]] = frozenset(
    {36, 59, 102, 203, 369, 556, 692, 821, 823, 878, 879, 884, 893}
)

MEASUREMENT_BOUNDS: Final[dict[tuple[int, int], tuple[float, float]]] = {
    PIXEL: (0.0, 4095.0),
    SLICE_LOCATION: (-500.0, 500.0),
    EXPOSURE_INDEX: (0.0, 1.0),
}
DEFAULT_BOUNDS: Final[tuple[float, float]] = (-1.0e6, 1.0e6)


class EngineKernelException(Exception):
    """Raised when a tag matrix cannot be released."""


def _mean_stdev(samples: list[float]) -> tuple[float, float]:
    if not samples:
        return 0.0, 0.0
    center = statistics.fmean(samples)
    if len(samples) == 1:
        return center, 0.0
    return center, statistics.pstdev(samples)


def generalize_age(value: float) -> float:
    """Bin age by five years and collapse ages of 90 and above."""
    if math.isnan(value) or not math.isfinite(value):
        raise EngineKernelException("non-finite age")
    if value < 0.0:
        raise EngineKernelException("negative age")
    if value >= AGE_TOP:
        return AGE_TOP
    return math.floor(value / AGE_BIN) * AGE_BIN


def generalize_postal(value: float) -> float:
    """Truncate a numeric postal field to three digits, or to 0 if restricted."""
    if math.isnan(value) or not math.isfinite(value) or value < 0.0:
        raise EngineKernelException("invalid postal value")
    postal = int(math.floor(value))
    zip3 = postal // 100 if postal >= 1000 else postal
    if zip3 in RESTRICTED_ZIP3:
        return 0.0
    return float(zip3)


class DicomDifferentialPrivacyMatrix:
    """Suppress identifiers, generalize quasi-identifiers, noise measurements."""

    def __init__(
        self,
        epsilon: float = 0.5,
        seed: int = 17,
        ring_capacity: int = RING_CAPACITY,
    ) -> None:
        if not math.isfinite(epsilon) or epsilon <= 0.0:
            raise EngineKernelException("epsilon must be finite and positive")
        if ring_capacity < 1:
            raise EngineKernelException("ring capacity must be positive")
        self._epsilon = float(epsilon)
        self._rng = random.Random(seed)
        self._ring_capacity = ring_capacity
        self._lock = asyncio.Lock()
        self._inbound: asyncio.Queue[bytes] = asyncio.Queue()
        self._ring: deque[dict[str, object]] = deque(maxlen=ring_capacity)
        self._epsilon_spent = 0.0

    def pack_tag(
        self,
        group: int,
        element: int,
        value: float,
        slice_index: int,
        series: int,
    ) -> bytes:
        """Pack one numeric tag. Group and element are unsigned 16-bit DICOM ids."""
        if group < 0 or group > 0xFFFF or element < 0 or element > 0xFFFF:
            raise EngineKernelException(
                f"tag id out of range group={group} element={element}"
            )
        if slice_index < 0 or series < 0:
            raise EngineKernelException("slice index and series must be non-negative")
        try:
            return FRAME.pack(group, element, float(value), slice_index, series)
        except (struct.error, OverflowError) as exc:
            raise EngineKernelException("tag pack failed") from exc

    def unpack_tag(self, payload: bytes) -> dict[str, object]:
        """Unpack one tag record."""
        if len(payload) != FRAME.size:
            raise EngineKernelException(f"tag length {len(payload)} != {FRAME.size}")
        try:
            group, element, value, slice_index, series = FRAME.unpack(payload)
        except struct.error as exc:
            raise EngineKernelException("tag unpack failed") from exc
        return {
            "group": int(group),
            "element": int(element),
            "value": float(value),
            "slice_index": int(slice_index),
            "series": int(series),
        }

    def _laplace(self, scale: float) -> float:
        """Inverse-CDF sample from Laplace(0, scale)."""
        if scale == 0.0:
            return 0.0
        draw = self._rng.uniform(-0.5, 0.5)
        if draw == 0.0:
            return 0.0
        magnitude = math.log(1.0 - 2.0 * abs(draw))
        return -scale * math.copysign(1.0, draw) * magnitude

    def _bounds(self, key: tuple[int, int]) -> tuple[float, float]:
        return MEASUREMENT_BOUNDS.get(key, DEFAULT_BOUNDS)

    def _bound_non_finite(
        self,
        value: float,
        low: float,
        high: float,
    ) -> tuple[float, bool]:
        if math.isnan(value):
            LOGGER.warning("NaN measurement bounded to %.6f", low)
            return low, True
        if math.isinf(value):
            held = high if value > 0.0 else low
            LOGGER.warning("infinite measurement bounded to %.6f", held)
            return held, True
        return value, False

    def _release_locked(
        self,
        payloads: list[bytes],
        study_slices: int,
    ) -> dict[str, object]:
        records = [self.unpack_tag(payload) for payload in payloads]
        for record in records:
            if int(record["slice_index"]) < 0:
                raise EngineKernelException("negative slice index")
        present = {
            int(record["slice_index"])
            for record in records
            if int(record["slice_index"]) < study_slices
        }
        dropout = study_slices - len(present)
        if dropout * 2 > study_slices:
            raise EngineKernelException(
                f"slice dropout {dropout} of {study_slices} exceeds half the study"
            )
        if dropout:
            LOGGER.warning(
                "slice dropout missing=%d study_slices=%d",
                dropout,
                study_slices,
            )
        measurements = [
            record
            for record in records
            if (int(record["group"]), int(record["element"])) not in DIRECT_IDENTIFIERS
            and (int(record["group"]), int(record["element"])) not in QUASI_IDENTIFIERS
        ]
        share = self._epsilon / float(len(measurements)) if measurements else 0.0
        released: list[dict[str, object]] = []
        suppressed = 0
        generalized = 0
        noised = 0
        bounded = 0
        saturated = 0
        for record in records:
            key = (int(record["group"]), int(record["element"]))
            if key in DIRECT_IDENTIFIERS:
                suppressed += 1
                continue
            if key == AGE:
                value = generalize_age(float(record["value"]))
                action = "generalize-age"
                generalized += 1
                scale = 0.0
            elif key == POSTAL:
                value = generalize_postal(float(record["value"]))
                action = "generalize-postal"
                generalized += 1
                scale = 0.0
            else:
                low, high = self._bounds(key)
                raw, was_bounded = self._bound_non_finite(
                    float(record["value"]), low, high
                )
                if was_bounded:
                    bounded += 1
                if raw < low:
                    raw = low
                elif raw > high:
                    raw = high
                sensitivity = high - low
                scale = sensitivity / share if share > 0.0 else 0.0
                noised_value = raw + self._laplace(scale)
                if noised_value < low or noised_value > high:
                    saturated += 1
                    noised_value = min(high, max(low, noised_value))
                if not math.isfinite(noised_value):
                    raise EngineKernelException("noise produced a non-finite value")
                value = noised_value
                action = "laplace"
                noised += 1
            released.append(
                {
                    "group": key[0],
                    "element": key[1],
                    "value": value,
                    "slice_index": int(record["slice_index"]),
                    "series": int(record["series"]),
                    "action": action,
                    "laplace_scale": scale,
                }
            )
        epsilon_spent = self._epsilon if noised else 0.0
        self._epsilon_spent += epsilon_spent
        for row in released:
            self._ring.append(row)
            encoded = FRAME.pack(
                int(row["group"]),
                int(row["element"]),
                float(row["value"]),
                int(row["slice_index"]),
                int(row["series"]),
            )
            self._outbound_hold(encoded)
        values = [float(row["value"]) for row in released]
        mean_value, stdev_value = _mean_stdev(values)
        return {
            "stream": STREAM,
            "epsilon": self._epsilon,
            "epsilon_per_field": share,
            "epsilon_spent": epsilon_spent,
            "epsilon_spent_total": self._epsilon_spent,
            "suppressed_count": suppressed,
            "generalized_count": generalized,
            "noised_count": noised,
            "dropout_count": dropout,
            "non_finite_bounds": bounded,
            "saturated_count": saturated,
            "mean_value": mean_value,
            "stdev_value": stdev_value,
            "ring_depth": len(self._ring),
            "records": released,
        }

    def _outbound_hold(self, payload: bytes) -> None:
        """Keep a bounded copy of bytes that would be put on the stream."""
        if not hasattr(self, "_held"):
            self._held: deque[bytes] = deque(maxlen=self._ring_capacity)
        self._held.append(payload)

    async def run_worker(
        self,
        payloads: list[bytes],
        study_slices: int,
    ) -> dict[str, object]:
        """Release one study onto ``imaging.tags.anonymized``."""
        if study_slices < 1:
            raise EngineKernelException("study_slices must be positive")
        if not payloads:
            raise EngineKernelException("empty tag batch")
        for payload in payloads:
            await self._inbound.put(payload)
        buffered: list[bytes] = []
        for _ in range(len(payloads)):
            buffered.append(await self._inbound.get())
        async with self._lock:
            report = self._release_locked(buffered, study_slices)
            depth = self._inbound.qsize()
        decoded: dict[str, object] = json.loads(json.dumps(report))
        LOGGER.info(
            "release stream=%s epsilon_spent=%.6f suppressed=%d "
            "generalized=%d noised=%d dropout=%d inbound_residual=%d",
            STREAM,
            decoded["epsilon_spent"],
            decoded["suppressed_count"],
            decoded["generalized_count"],
            decoded["noised_count"],
            decoded["dropout_count"],
            depth,
        )
        return decoded


async def _scenario() -> None:
    engine = DicomDifferentialPrivacyMatrix(epsilon=0.8, seed=17)
    frames = [
        engine.pack_tag(0x0010, 0x0020, 4_821_993.0, 0, 3),
        engine.pack_tag(0x0010, 0x1010, 95.0, 0, 3),
        engine.pack_tag(0x0010, 0x1040, 94107.0, 0, 3),
        engine.pack_tag(0x7FE0, 0x0010, 1800.0, 0, 3),
        engine.pack_tag(0x7FE0, 0x0010, math.inf, 1, 3),
        engine.pack_tag(0x0018, 0x6020, 0.42, 1, 3),
        engine.pack_tag(0x0020, 0x1041, 12.5, 3, 3),
    ]
    report = await engine.run_worker(frames, study_slices=4)
    sys.stdout.write(json.dumps(report) + "\n")


if __name__ == "__main__":
    asyncio.run(_scenario())
