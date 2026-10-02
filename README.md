# DICOM Differential Privacy Matrix

A high-throughput, low-latency asynchronous engine engineered to resolve identifier leakage in synthetic DICOM numeric tag matrices by suppressing direct identifiers, generalizing quasi-identifiers, and adding Laplace noise at an explicit epsilon.

## 🏗️ Systems Architecture & Event Topology

`DicomDifferentialPrivacyMatrix` accepts packed numeric tags only: group, element, a float, a slice index, and a series number. There is no patient-name field and no medical-record-number field. `pack_tag` / `unpack_tag` are the struct boundary. `run_worker` is the coroutine that drains a study.

The in-process queue stands in for the stream `imaging.tags.anonymized`. Epsilon is a constructor argument (default 0.5) and is split across measurement fields. The release report records `epsilon_spent`. That report is an accounting ledger, not a formal privacy proof. Shared release state is guarded by an `asyncio.Lock`. `logging.basicConfig` writes timestamped lines. A malformed tag or a non-positive epsilon raises `EngineKernelException`.

## 📊 Core Visual Walkthrough & Engine Pipeline Flow

```
synthetic numeric tags
    |  struct (group, element, value, slice, series)
    v
run_worker(payloads, study_slices)
    |
    +-- missing slice index --------> dropout warning, dropout_count
    |
    +-- identifier element ---------> suppress, value never copied forward
    |
    +-- age / postal quasi-id ------> generalize_age / generalize_postal
    |
    +-- measurement ----------------> Laplace(scale = sensitivity / epsilon)
    |                                 NaN -> 0, +inf -> finite bound
    v
release dict on imaging.tags.anonymized
```

Insert the structural terminal walkthrough recording at docs/assets/terminal-walkthrough.gif before publishing the release notes.

## ⚡ Low-Level OS Mechanics & Network Physics

Laplace noise is drawn from a seeded `random.Random`, not from a blocking entropy device, so a study replays when the seed is fixed. The scale is sensitivity divided by the per-field epsilon. `math` is used for the logarithm that turns a uniform draw into a Laplace sample, and for `isfinite` / `isinf` before a sample is allowed into the mean.

`statistics.mean` and `statistics.pstdev` summarize the released numeric column after suppression. They never see the raw identifier. The ring is a bounded `deque`. Slice dropout is a set difference against `study_slices`, computed in the worker, not by sleeping until a late slice arrives. The event loop does not spawn a thread per slice.

## ⚖️ Architecture Trade-offs & Pragmatic Decisions

Suppression removes the identifier. Generalization coarsens age and postal tags before any noise is added, which is the HIPAA Safe Harbor shape for those quasi-identifiers on this synthetic matrix. Laplace noise is then applied only to measurement elements, with the epsilon written on the release. Combining both is deliberate: generalization is deterministic and auditable; noise is the part that spends budget.

The seed is an argument so a compliance replay can regenerate the same release. Production traffic would use a fresh seed per study. This process does not open a PACS association and does not parse a DICOM file. Callers that have a real header must strip it before `pack_tag`.

## 🚀 Local Installation & Benchmarking

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python src/main.py
python src/test_harness.py
```

```python
import asyncio

from src.main import DicomDifferentialPrivacyMatrix


async def demo() -> None:
    engine = DicomDifferentialPrivacyMatrix(epsilon=0.8, seed=17)
    payload = engine.pack_tag(16, 4112, 90.0, 0, 3)
    await engine.run_worker([payload], study_slices=1)


asyncio.run(demo())
```

`requirements.txt` documents a standard-library runtime. `pip install -r requirements.txt` succeeds with nothing to fetch.

## 🖥️ Terminal Diagnostic Output Preview

```
WARNING [dicom.privacy] slice dropout missing=1 study_slices=4
WARNING [dicom.privacy] infinite measurement bounded to 4095.000000
INFO [dicom.privacy] release stream=imaging.tags.anonymized epsilon_spent=0.800000 suppressed=1 generalized=2 noised=4 dropout=1 inbound_residual=0
```

`python src/main.py` exits 0. The stdout release records `epsilon` 0.8 and a `laplace` action on measurement tags. No name or record number is present in that JSON.

## 📊 Empirical Benchmarking Performance Report

Measured by `python src/test_harness.py` with a deterministic seed, 8000 iterations, `time.perf_counter_ns` latency in microseconds, and `tracemalloc` peak.

| Metric | Measured |
| --- | ---: |
| Status | PASS |
| Iterations | 8000 |
| Average latency | 147.461 µs |
| Empirical P99 | 214.062 µs |
| Scenario latency | 180.727 µs |
| tracemalloc peak | 487842 bytes |

## 🛡️ Edge-Case Resilience & SOC2/Regulatory Compliance

A missing slice increments `dropout_count` and logs the gap against `study_slices`. It does not invent pixel data. `math.inf` is bounded to the finite measurement ceiling (4095 in the scenario) and a warning is logged. NaN measurements are bounded to 0. Both paths raise or bound through `EngineKernelException` when the tag itself is illegal, and neither path copies an identifier into the release ring.

The matrix is synthetic numeric tags. It is aligned with the HIPAA Safe Harbor practice of removing direct identifiers and generalizing quasi-identifiers. It is not a determination that a dataset is de-identified, and it is not a substitute for a privacy office review. SOC 2 processing integrity is the control in view: epsilon spent is reported, suppression is counted, and the raw identifier is not in the outbound record.
