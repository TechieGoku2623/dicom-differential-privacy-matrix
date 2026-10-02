"""Deterministic checks and a 5000-iteration latency benchmark."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import sys
import time
import tracemalloc

from .engine import (
    AGE_TAG,
    EXPOSURE_TAG,
    PATIENT_ID_TAG,
    PIXEL_TAG,
    POSTAL_TAG,
    DicomDifferentialPrivacyMatrix,
)
from .exceptions import EngineKernelException
from .wire import pack_tag, unpack_tag

ITERATIONS: int = 5_000
SEED: int = 514


def _percentile(samples: list[float], fraction: float) -> float:
    ordered = sorted(samples)
    if not ordered:
        return 0.0
    index = math.ceil(fraction * len(ordered)) - 1
    return ordered[max(0, min(index, len(ordered) - 1))]


def _tag(
    tag: int,
    value: float,
    slice_index: int,
    *,
    missing: bool = False,
    series: int = 1,
) -> dict[str, object]:
    return {
        "tag": tag,
        "value": value,
        "slice_index": slice_index,
        "series": series,
        "missing": missing,
    }


async def _checks(failures: list[str]) -> None:
    payload = pack_tag(EXPOSURE_TAG, 0.5, 4, 9, False)
    frame = unpack_tag(payload)
    if (
        frame["tag"] != EXPOSURE_TAG
        or frame["value"] != 0.5
        or frame["slice_index"] != 4
        or frame["series"] != 9
        or frame["missing"] is not False
    ):
        failures.append("finite tag did not round-trip")
    if payload != pack_tag(EXPOSURE_TAG, 0.5, 4, 9, False):
        failures.append("tag pack was not stable")
    flagged = unpack_tag(pack_tag(PIXEL_TAG, 0.0, 2, 1, True))
    if flagged["missing"] is not True:
        failures.append("missing flag did not round-trip")
    try:
        unpack_tag(b"\x00\x01")
        failures.append("short tag did not raise")
    except EngineKernelException as exc:
        if not str(exc):
            failures.append("short tag exception had no message")

    engine = DicomDifferentialPrivacyMatrix(epsilon=0.5, seed=11)
    report = await engine.run(
        [
            _tag(PATIENT_ID_TAG, 8_241_113.0, 0),
            _tag(AGE_TAG, 95.0, 0),
            _tag(AGE_TAG, 47.0, 1),
            _tag(POSTAL_TAG, 94_107.0, 0),
            _tag(EXPOSURE_TAG, 0.4, 1),
        ]
    )
    if report["suppressed"] != 1 or report["generalized"] != 3 or report["noised"] != 1:
        failures.append(
            f"counts suppressed={report['suppressed']} "
            f"generalized={report['generalized']} noised={report['noised']}"
        )
    if "8241113" in json.dumps(report):
        failures.append("identifier value leaked into the release")
    if not math.isclose(float(report["epsilon_spent"]), 0.5, abs_tol=1e-12):
        failures.append("epsilon spent did not match one measurement")
    released = report["released"]
    if not isinstance(released, list):
        failures.append("released rows missing")
    else:
        ages = [row["value"] for row in released if row["tag"] == AGE_TAG]
        postals = [row["value"] for row in released if row["tag"] == POSTAL_TAG]
        if ages != [90.0, 45.0]:
            failures.append(f"age bins were {ages}")
        if postals != [941.0]:
            failures.append(f"postal truncation was {postals}")
        if any(row["tag"] == PATIENT_ID_TAG for row in released):
            failures.append("direct identifier was emitted")

    twin_a = DicomDifferentialPrivacyMatrix(epsilon=0.5, seed=11)
    twin_b = DicomDifferentialPrivacyMatrix(epsilon=0.5, seed=11)
    twin_c = DicomDifferentialPrivacyMatrix(epsilon=0.5, seed=99)
    same = [_tag(EXPOSURE_TAG, 0.4, 0), _tag(PIXEL_TAG, 100.0, 1)]
    left = await twin_a.run(same)
    right = await twin_b.run(same)
    other = await twin_c.run(same)
    if left["released"] != right["released"]:
        failures.append("same seed did not reproduce the release")
    if left["released"] == other["released"]:
        failures.append("different seeds produced the same noise")

    clipped = await DicomDifferentialPrivacyMatrix(epsilon=1_000_000.0, seed=3).run(
        [_tag(PIXEL_TAG, 99_999.0, 0)]
    )
    pixel = float(clipped["released"][0]["value"])
    if abs(pixel - 4095.0) >= 1.0:
        failures.append("pixel clip did not hold under a large epsilon")
    if not math.isclose(
        float(clipped["released"][0]["laplace_scale"]), 4095.0 / 1_000_000.0
    ):
        failures.append("laplace scale was not sensitivity/epsilon")

    try:
        await DicomDifferentialPrivacyMatrix(epsilon=1.0, seed=1).run(
            [_tag(PIXEL_TAG, float("nan"), 0)]
        )
        failures.append("NaN measurement did not raise")
    except EngineKernelException as exc:
        if not str(exc):
            failures.append("NaN exception had no message")
    try:
        await DicomDifferentialPrivacyMatrix(epsilon=1.0, seed=1).run(
            [_tag(EXPOSURE_TAG, math.inf, 0)]
        )
        failures.append("infinite measurement did not raise")
    except EngineKernelException as exc:
        if not str(exc):
            failures.append("infinity exception had no message")

    missing = await DicomDifferentialPrivacyMatrix(epsilon=1.0, seed=5).run(
        [
            _tag(EXPOSURE_TAG, 0.4, 0, missing=True),
            _tag(EXPOSURE_TAG, 0.4, 1, missing=False),
        ]
    )
    if missing["dropout"] != 1 or missing["noised"] != 1:
        failures.append("missing slice did not increment dropout without a measurement")
    slices = [row["slice_index"] for row in missing["released"]]
    if slices != [1]:
        failures.append(f"missing slice was emitted: {slices}")

    try:
        DicomDifferentialPrivacyMatrix(epsilon=0.0, seed=1)
        failures.append("non-positive epsilon did not raise")
    except EngineKernelException as exc:
        if not str(exc):
            failures.append("epsilon exception had no message")
    try:
        DicomDifferentialPrivacyMatrix(epsilon=math.inf, seed=1)
        failures.append("infinite epsilon did not raise")
    except EngineKernelException as exc:
        if not str(exc):
            failures.append("infinite epsilon exception had no message")


def _batches(rng: random.Random) -> list[list[dict[str, object]]]:
    batches: list[list[dict[str, object]]] = []
    for index in range(ITERATIONS):
        value = rng.random()
        batches.append([_tag(EXPOSURE_TAG, value, index % 8, series=1)])
    return batches


async def _benchmark(batches: list[list[dict[str, object]]]) -> list[float]:
    engine = DicomDifferentialPrivacyMatrix(epsilon=1.0, seed=SEED)
    samples: list[float] = []
    for batch in batches:
        started = time.perf_counter_ns()
        await engine.run(batch)
        samples.append((time.perf_counter_ns() - started) / 1_000.0)
    return samples


def main() -> int:
    """Print one status dict and exit 0 only when every check passes."""
    logging.getLogger("dicom_differential_privacy_matrix").setLevel(logging.ERROR)
    failures: list[str] = []
    asyncio.run(_checks(failures))
    probe = DicomDifferentialPrivacyMatrix(epsilon=1.0, seed=1)
    started = time.perf_counter_ns()
    asyncio.run(probe.run([_tag(EXPOSURE_TAG, 0.25, 0)]))
    latency_us = (time.perf_counter_ns() - started) / 1_000.0
    batches = _batches(random.Random(SEED))
    tracemalloc.start()
    samples = asyncio.run(_benchmark(batches))
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    status = {
        "status": "PASS" if not failures else "FAIL",
        "failures": failures,
        "latency_us": round(latency_us, 3),
        "memory_peak_bytes": peak,
        "benchmark_iterations": len(samples),
        "benchmark_avg_us": round(sum(samples) / len(samples), 3),
        "benchmark_p99_us": round(_percentile(samples, 0.99), 3),
    }
    sys.stdout.write(json.dumps(status) + "\n")
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
