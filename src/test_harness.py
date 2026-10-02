"""Deterministic checks and a short latency benchmark for the DICOM release."""

from __future__ import annotations

import asyncio
import json
import math
import random
import sys
import time
import tracemalloc
from pathlib import Path


def _load():
    root = Path(__file__).resolve().parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    import main

    return main


def _percentile(samples: list[float], fraction: float) -> float:
    ordered = sorted(samples)
    if not ordered:
        return 0.0
    index = math.ceil(fraction * len(ordered)) - 1
    index = max(0, min(index, len(ordered) - 1))
    return ordered[index]


def _tag(mod, engine, group: int, element: int, value: float, slice_index: int):
    return engine.pack_tag(group, element, value, slice_index, 1)


async def _checks(mod, failures: list[str]) -> None:
    rng = random.Random(514)
    engine = mod.DicomDifferentialPrivacyMatrix(epsilon=0.5, seed=11)
    frames = [
        _tag(mod, engine, 0x0010, 0x0020, 4_821_993.0, 0),
        _tag(mod, engine, 0x0010, 0x1010, 95.0, 0),
        _tag(mod, engine, 0x0010, 0x1010, 47.0, 1),
        _tag(mod, engine, 0x0010, 0x1040, 94107.0, 0),
        _tag(mod, engine, 0x0010, 0x1040, 3645.0, 1),
        _tag(mod, engine, 0x7FE0, 0x0010, 1500.0, 0),
        _tag(mod, engine, 0x7FE0, 0x0010, math.inf, 1),
        _tag(mod, engine, 0x7FE0, 0x0010, float("nan"), 3),
    ]
    # rng perturbs only an unused exposure so the seed stays on the engine.
    _ = rng.random()
    report = await engine.run_worker(frames, study_slices=4)
    if report["suppressed_count"] != 1:
        failures.append("patient identifier was not suppressed")
    if "4821993" in json.dumps(report):
        failures.append("identifier value leaked into the release")
    if report["dropout_count"] != 1:
        failures.append("missing slice was not counted")
    if report["non_finite_bounds"] < 2:
        failures.append("inf and NaN pixels were not bounded")
    if report["generalized_count"] != 4:
        failures.append("quasi-identifier generalization count mismatch")
    ages = [
        row["value"]
        for row in report["records"]
        if row["group"] == 0x0010 and row["element"] == 0x1010
    ]
    if ages != [90.0, 45.0]:
        failures.append(f"age bins were {ages}")
    postals = [
        row["value"]
        for row in report["records"]
        if row["group"] == 0x0010 and row["element"] == 0x1040
    ]
    if postals != [941.0, 0.0]:
        failures.append(f"postal generalization was {postals}")
    pixels = [
        row["value"]
        for row in report["records"]
        if row["group"] == 0x7FE0 and row["element"] == 0x0010
    ]
    if len(pixels) != 3 or any(not math.isfinite(value) for value in pixels):
        failures.append("pixel release was not finite")
    if any(value < 0.0 or value > 4095.0 for value in pixels):
        failures.append("pixel release left the 12-bit bound")
    if report["epsilon_spent"] != 0.5:
        failures.append("epsilon spent did not match the budget")
    if report["stream"] != "imaging.tags.anonymized":
        failures.append("stream name mismatch")
    if any(
        row["group"] == 0x0010 and row["element"] == 0x0020 for row in report["records"]
    ):
        failures.append("suppressed tag was emitted")

    twin_a = mod.DicomDifferentialPrivacyMatrix(epsilon=0.5, seed=11)
    twin_b = mod.DicomDifferentialPrivacyMatrix(epsilon=0.5, seed=11)
    twin_c = mod.DicomDifferentialPrivacyMatrix(epsilon=0.5, seed=99)
    same_frames = [
        _tag(mod, twin_a, 0x0018, 0x6020, 0.4, 0),
        _tag(mod, twin_a, 0x7FE0, 0x0010, 100.0, 1),
    ]
    # Rebuild identical bytes from each engine so pack does not depend on rng.
    frames_b = [
        _tag(mod, twin_b, 0x0018, 0x6020, 0.4, 0),
        _tag(mod, twin_b, 0x7FE0, 0x0010, 100.0, 1),
    ]
    frames_c = [
        _tag(mod, twin_c, 0x0018, 0x6020, 0.4, 0),
        _tag(mod, twin_c, 0x7FE0, 0x0010, 100.0, 1),
    ]
    left = await twin_a.run_worker(same_frames, study_slices=2)
    right = await twin_b.run_worker(frames_b, study_slices=2)
    other = await twin_c.run_worker(frames_c, study_slices=2)
    if left["records"] != right["records"]:
        failures.append("same seed did not reproduce the release")
    if left["records"] == other["records"]:
        failures.append("different seeds produced the same noise")

    tight = mod.DicomDifferentialPrivacyMatrix(epsilon=1_000.0, seed=3)
    tight_report = await tight.run_worker(
        [_tag(mod, tight, 0x0018, 0x6020, 0.4, 0)],
        study_slices=1,
    )
    released = tight_report["records"][0]["value"]
    if abs(released - 0.4) >= 0.05:
        failures.append("high-epsilon exposure moved more than the bound allows")
    if not math.isclose(tight_report["epsilon_per_field"], 1_000.0):
        failures.append("single-field budget was not the full epsilon")
    scale = tight_report["records"][0]["laplace_scale"]
    if not math.isclose(scale, 1.0 / 1_000.0):
        failures.append("laplace scale was not sensitivity/epsilon")

    heavy = mod.DicomDifferentialPrivacyMatrix(epsilon=0.4, seed=1)
    try:
        await heavy.run_worker(
            [_tag(mod, heavy, 0x7FE0, 0x0010, 10.0, 0)],
            study_slices=8,
        )
        failures.append("majority slice dropout did not raise")
    except mod.EngineKernelException as exc:
        if not str(exc):
            failures.append("kernel exception had no message")

    broken = mod.DicomDifferentialPrivacyMatrix(epsilon=0.4, seed=1)
    try:
        await broken.run_worker([b"\x00\x01"], study_slices=1)
        failures.append("truncated tag did not raise")
    except mod.EngineKernelException as exc:
        if not str(exc):
            failures.append("kernel exception had no message")

    try:
        mod.DicomDifferentialPrivacyMatrix(epsilon=0.0, seed=1)
        failures.append("non-positive epsilon did not raise")
    except mod.EngineKernelException as exc:
        if not str(exc):
            failures.append("kernel exception had no message")

    aged = mod.DicomDifferentialPrivacyMatrix(epsilon=0.2, seed=1)
    try:
        await aged.run_worker(
            [_tag(mod, aged, 0x0010, 0x1010, -4.0, 0)],
            study_slices=1,
        )
        failures.append("negative age did not raise")
    except mod.EngineKernelException as exc:
        if not str(exc):
            failures.append("kernel exception had no message")

    identifier_only = mod.DicomDifferentialPrivacyMatrix(epsilon=0.2, seed=1)
    quiet = await identifier_only.run_worker(
        [_tag(mod, identifier_only, 0x0008, 0x0050, 55.0, 0)],
        study_slices=1,
    )
    if (
        quiet["suppressed_count"] != 1
        or quiet["records"]
        or quiet["epsilon_spent"] != 0.0
    ):
        failures.append("identifier-only slice was not a zero-spend suppression")


async def _benchmark(mod) -> list[float]:
    rng = random.Random(514)
    engine = mod.DicomDifferentialPrivacyMatrix(epsilon=1.0, seed=514)
    samples: list[float] = []
    for _ in range(8_000):
        value = rng.random()
        payload = engine.pack_tag(0x0018, 0x6020, value, 0, 1)
        started = time.perf_counter_ns()
        await engine.run_worker([payload], study_slices=1)
        samples.append((time.perf_counter_ns() - started) / 1_000.0)
    return samples


def main() -> int:
    mod = _load()
    failures: list[str] = []
    asyncio.run(_checks(mod, failures))
    probe = mod.DicomDifferentialPrivacyMatrix(epsilon=1.0, seed=1)
    payload = probe.pack_tag(0x0018, 0x6020, 0.25, 0, 1)
    started = time.perf_counter_ns()
    asyncio.run(probe.run_worker([payload], study_slices=1))
    latency_us = (time.perf_counter_ns() - started) / 1_000.0
    tracemalloc.start()
    samples = asyncio.run(_benchmark(mod))
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
    sys.stdout.write(repr(status) + "\n")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
