"""Suppression, generalization, and Laplace noise on numeric DICOM tags."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import statistics
import struct
from collections import deque
from typing import Mapping, Sequence

from .exceptions import EngineKernelException
from .wire import FORMAT, pack_tag, unpack_tag

LOGGER = logging.getLogger(__name__)

STREAM: str = "imaging.tags.anonymized"
RING_CAPACITY: int = 256
AGE_BIN_YEARS: float = 5.0
AGE_CAP_YEARS: float = 90.0

# Numeric tag ids only. This module never stores a patient name or an MRN string.
PATIENT_NAME_TAG: int = 0x00100010
PATIENT_ID_TAG: int = 0x00100020
PATIENT_BIRTH_TAG: int = 0x00100030
OTHER_PATIENT_ID_TAG: int = 0x00101000
ACCESSION_TAG: int = 0x00080050
INSTITUTION_TAG: int = 0x00080080
INSTITUTION_ADDRESS_TAG: int = 0x00080081
AGE_TAG: int = 0x00101010
POSTAL_TAG: int = 0x00101040
PIXEL_TAG: int = 0x7FE00010
SLICE_LOCATION_TAG: int = 0x00201041
EXPOSURE_TAG: int = 0x00186020

DIRECT_IDENTIFIER_TAGS: frozenset[int] = frozenset(
    {
        PATIENT_NAME_TAG,
        PATIENT_ID_TAG,
        PATIENT_BIRTH_TAG,
        OTHER_PATIENT_ID_TAG,
        ACCESSION_TAG,
        INSTITUTION_TAG,
        INSTITUTION_ADDRESS_TAG,
    }
)

MEASUREMENT_BOUNDS: dict[int, tuple[float, float]] = {
    PIXEL_TAG: (0.0, 4095.0),
    SLICE_LOCATION_TAG: (-500.0, 500.0),
    EXPOSURE_TAG: (0.0, 1.0),
}
DEFAULT_BOUNDS: tuple[float, float] = (-1.0e6, 1.0e6)


def _as_int(record: Mapping[str, object], key: str) -> int:
    raw = record[key]
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise EngineKernelException(f"{key} must be an integer")
    return raw


def _as_float(record: Mapping[str, object], key: str) -> float:
    raw = record[key]
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise EngineKernelException(f"{key} must be numeric")
    return float(raw)


def _as_bool(record: Mapping[str, object], key: str) -> bool:
    raw = record.get(key, False)
    if not isinstance(raw, bool):
        raise EngineKernelException(f"{key} must be a bool")
    return raw


def _check_size(payload: bytes) -> None:
    expected = struct.calcsize(FORMAT)
    if len(payload) != expected:
        raise EngineKernelException(f"tag length {len(payload)} != {expected}")


def _json_object(report: dict[str, object]) -> dict[str, object]:
    decoded = json.loads(json.dumps(report, allow_nan=False))
    if not isinstance(decoded, dict):
        raise EngineKernelException("report was not a JSON object")
    return decoded


def laplace_noise(rng: random.Random, scale: float) -> float:
    """Inverse-CDF Laplace(0, scale) draw.

    ``u`` is uniform on (-0.5, 0.5) excluding 0.
    noise = -scale * copysign(1, u) * log(1 - 2 * abs(u)).
    """
    if scale == 0.0:
        return 0.0
    while True:
        draw = rng.uniform(-0.5, 0.5)
        if draw != 0.0 and abs(draw) < 0.5:
            break
    return -scale * math.copysign(1.0, draw) * math.log(1.0 - 2.0 * abs(draw))


def generalize_age(value: float) -> float:
    """Bin age by five years and cap the top bin at 90."""
    if value < 0.0:
        raise EngineKernelException("negative age")
    if value >= AGE_CAP_YEARS:
        return AGE_CAP_YEARS
    return math.floor(value / AGE_BIN_YEARS) * AGE_BIN_YEARS


def generalize_postal(value: float) -> float:
    """Keep the first three digits of a postal integer."""
    if value < 0.0:
        raise EngineKernelException("negative postal")
    postal = int(math.floor(value))
    digits = str(postal)
    if len(digits) <= 3:
        return float(postal)
    return float(int(digits[:3]))


class DicomDifferentialPrivacyMatrix:
    """Release synthetic numeric tags on ``imaging.tags.anonymized``."""

    def __init__(
        self,
        epsilon: float = 1.0,
        seed: int = 17,
        bounds: Mapping[int, tuple[float, float]] | None = None,
        ring_capacity: int = RING_CAPACITY,
    ) -> None:
        if not math.isfinite(epsilon) or epsilon <= 0.0:
            raise EngineKernelException("epsilon must be finite and positive")
        if ring_capacity < 1:
            raise EngineKernelException("ring capacity must be positive")
        self._epsilon = float(epsilon)
        self._rng = random.Random(seed)
        self._lock = asyncio.Lock()
        self._inbound: asyncio.Queue[bytes] = asyncio.Queue()
        self._outbound: asyncio.Queue[bytes] = asyncio.Queue(maxsize=ring_capacity)
        self._ring: deque[dict[str, object]] = deque(maxlen=ring_capacity)
        self._epsilon_spent = 0.0
        self._bounds: dict[int, tuple[float, float]] = dict(MEASUREMENT_BOUNDS)
        if bounds is not None:
            for tag, pair in bounds.items():
                lo, hi = pair
                if not math.isfinite(lo) or not math.isfinite(hi) or hi < lo:
                    raise EngineKernelException(f"invalid bounds for tag {tag}")
                self._bounds[int(tag)] = (float(lo), float(hi))
        self._frame_size = struct.calcsize(FORMAT)

    async def run(self, records: Sequence[Mapping[str, object]]) -> dict[str, object]:
        """Suppress, generalize, and noise one study. Returns a JSON object."""
        batch = list(records)
        if not batch:
            raise EngineKernelException("empty tag batch")
        async with self._lock:
            frames = [self._decode(record) for record in batch]
            self._reject_non_finite(frames)
            report = self._release(frames)
        LOGGER.info(
            "stream=%s suppressed=%d generalized=%d noised=%d "
            "epsilon_spent=%.6f dropout=%d",
            STREAM,
            report["suppressed"],
            report["generalized"],
            report["noised"],
            report["epsilon_spent"],
            report["dropout"],
        )
        return _json_object(report)

    def ingest(self, frame: Mapping[str, object]) -> dict[str, object]:
        """Validate one unpacked tag. ``run`` holds the lock and calls this."""
        tag = int(frame["tag"])
        value = float(frame["value"])
        slice_index = int(frame["slice_index"])
        series = int(frame["series"])
        missing = bool(frame["missing"])
        if slice_index < 0 or series < 0:
            raise EngineKernelException("slice index and series must be non-negative")
        return {
            "tag": tag,
            "value": value,
            "slice_index": slice_index,
            "series": series,
            "missing": missing,
        }

    def _decode(self, record: Mapping[str, object]) -> dict[str, object]:
        payload = pack_tag(
            _as_int(record, "tag"),
            _as_float(record, "value"),
            _as_int(record, "slice_index"),
            _as_int(record, "series"),
            _as_bool(record, "missing"),
        )
        _check_size(payload)
        if len(payload) != self._frame_size:
            raise EngineKernelException("frame size mismatch")
        self._inbound.put_nowait(payload)
        queued = self._inbound.get_nowait()
        return self.ingest(unpack_tag(queued))

    def _reject_non_finite(self, frames: Sequence[Mapping[str, object]]) -> None:
        for frame in frames:
            if bool(frame["missing"]):
                continue
            value = float(frame["value"])
            if math.isnan(value) or math.isinf(value) or not math.isfinite(value):
                raise EngineKernelException(
                    f"non-finite measurement tag={frame['tag']} value={value}"
                )
            tag = int(frame["tag"])
            if tag == AGE_TAG and value < 0.0:
                raise EngineKernelException("negative age")
            if tag == POSTAL_TAG and value < 0.0:
                raise EngineKernelException("negative postal")

    def _bounds_for(self, tag: int) -> tuple[float, float]:
        return self._bounds.get(tag, DEFAULT_BOUNDS)

    def _release(self, frames: Sequence[Mapping[str, object]]) -> dict[str, object]:
        suppressed = 0
        generalized = 0
        noised = 0
        dropout = 0
        released: list[dict[str, object]] = []
        for frame in frames:
            tag = int(frame["tag"])
            if bool(frame["missing"]):
                dropout += 1
                LOGGER.warning(
                    "slice dropout tag=%d slice_index=%d",
                    tag,
                    int(frame["slice_index"]),
                )
                continue
            if tag in DIRECT_IDENTIFIER_TAGS:
                suppressed += 1
                continue
            if tag == AGE_TAG:
                value = generalize_age(float(frame["value"]))
                action = "generalize-age"
                generalized += 1
                scale = 0.0
            elif tag == POSTAL_TAG:
                value = generalize_postal(float(frame["value"]))
                action = "generalize-postal"
                generalized += 1
                scale = 0.0
            else:
                lo, hi = self._bounds_for(tag)
                raw = float(frame["value"])
                if raw < lo:
                    raw = lo
                elif raw > hi:
                    raw = hi
                sensitivity = hi - lo
                scale = sensitivity / self._epsilon
                value = raw + laplace_noise(self._rng, scale)
                if not math.isfinite(value):
                    raise EngineKernelException("noise produced a non-finite value")
                action = "laplace"
                noised += 1
                self._epsilon_spent += self._epsilon
            row = {
                "tag": tag,
                "value": value,
                "slice_index": int(frame["slice_index"]),
                "series": int(frame["series"]),
                "action": action,
                "laplace_scale": scale,
            }
            released.append(row)
            self._ring.append(row)
            self._publish(row)
        values = [float(row["value"]) for row in released] or [0.0]
        mean_released = statistics.fmean(values)
        if len(values) > 1:
            spread = statistics.pstdev(values)
        else:
            spread = 0.0
        return {
            "stream": STREAM,
            "suppressed": suppressed,
            "generalized": generalized,
            "noised": noised,
            "epsilon_spent": self._epsilon_spent,
            "dropout": dropout,
            "released": released,
            "mean_released": mean_released,
            "stdev_released": spread,
            "ring_depth": len(self._ring),
        }

    def _publish(self, row: Mapping[str, object]) -> None:
        blob = pack_tag(
            int(row["tag"]),
            float(row["value"]),
            int(row["slice_index"]),
            int(row["series"]),
            False,
        )
        if len(blob) != struct.calcsize(FORMAT):
            raise EngineKernelException("released frame size mismatch")
        if self._outbound.full():
            self._outbound.get_nowait()
        self._outbound.put_nowait(blob)
