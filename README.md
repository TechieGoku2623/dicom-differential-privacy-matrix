# DICOM Differential Privacy Matrix

A high-throughput, low-latency asynchronous engine engineered to resolve identifier leakage in synthetic numeric DICOM tag matrices by dropping direct-identifier tags, generalizing age and postal values, and adding Laplace noise under sequential epsilon accounting.

Website: https://github.com/TechieGoku2623/dicom-differential-privacy-matrix

Topics: `python` `asyncio` `healthcare` `dicom` `differential-privacy` `hipaa` `medical-imaging`


## 🏗️ Systems Architecture & Event Topology

`DicomDifferentialPrivacyMatrix` accepts packed numeric tags only: a tag id integer, a float, a slice index, a series number, and a missing-slice flag. There is no patient-name field and no medical-record-number string. `pack_tag` / `unpack_tag` are the struct boundary. `run` is the coroutine that releases a study.

Each record is placed on an `asyncio.Queue` and read back before `ingest`. Released measurements are packed again onto a bounded outbound queue. That queue is the in-process stand-in for the Kinesis stream `imaging.tags.anonymized`. This process does not open a Kinesis client. Kafka is not the stream name here. Redis is not used as a privacy ledger. TimescaleDB is not written; the release dict is the record a downstream insert would consume.

Epsilon is a constructor argument. Each noised measurement adds that epsilon to `epsilon_spent` (sequential composition, a sum). Generalization and suppression do not spend budget. Shared release state, including the `random.Random` stream, is guarded by an `asyncio.Lock`. `logging.basicConfig` is called only from `__main__.py`.

```
synthetic numeric tags
    |  struct <I d I I B>  tag, value, slice, series, missing
    v
asyncio.Queue ---- production name: Kinesis imaging.tags.anonymized
    |            this process: in-memory queue, no AWS client
    v
missing flag? ---- dropout += 1, emit nothing
    |
non-finite? ------ EngineKernelException (NaN or inf)
    |
direct id? ------- suppressed += 1, value not copied
    |
age / postal ----- 5-year bins, cap 90; first 3 postal digits
    |
measurement ------ clip to [lo, hi], Laplace(scale = (hi-lo) / epsilon)
    v
release dict: suppressed, generalized, noised, epsilon_spent, dropout
```

## 📊 Core Visual Walkthrough & Engine Pipeline Flow

![Terminal walkthrough](docs/assets/terminal-walkthrough.gif)

```
record
  |
  +-- missing == True -----------------> dropout, no released row
  |
  +-- value is NaN or +/- inf ---------> raise EngineKernelException
  |
  +-- tag in DIRECT_IDENTIFIER_TAGS ---> drop, do not copy the number
  |
  +-- age tag 0x00101010 ---------------> floor(age/5)*5, or 90 if age >= 90
  |
  +-- postal tag 0x00101040 ------------> first three digits of the integer
  |
  +-- other tag ------------------------> clip, then
                                         u ~ uniform (-0.5, 0.5) excluding 0
                                         noise = -scale * sgn(u) * log(1-2|u|)
                                         epsilon_spent += epsilon
  v
JSON object on the anonymized stream name
```

Sensitivity of a clipped measurement is `hi - lo`. The scale passed to the Laplace inverse CDF is that sensitivity divided by epsilon. The same seed replays the same noise. A second measurement draws the next sample and adds epsilon again.

Insert the structural terminal walkthrough recording at docs/assets/terminal-walkthrough.gif before publishing the release notes.

## ⚡ Low-Level OS Mechanics & Network Physics

Laplace noise is drawn from a seeded `random.Random`, not from a blocking entropy device, so a study replays when the seed is fixed. `math.log` and `math.copysign` turn the uniform draw into a Laplace sample. `math.isnan` and `math.isinf` reject a present measurement before any noise is added. A missing slice is skipped before that check, so a placeholder on a dropped slice does not have to be finite.

`statistics.fmean` summarizes the released numeric column after suppression. It never sees the raw identifier. The ring is a bounded `deque`. The outbound queue drops the oldest frame when it is full, so a long session cannot grow without bound. The event loop does not spawn a thread per slice and does not open a DICOM association.

## ⚖️ Architecture Trade-offs & Pragmatic Decisions

Noise is not re-clipped into `[lo, hi]`. Re-clipping would shrink the public range and bias the mechanism. A released measurement can sit outside the clip interval. That is the cost of using the classic Laplace mechanism instead of a truncated one.

Epsilon is charged only when a measurement is noised. Age bins and three-digit postal truncation are deterministic and are not added to the sum. A reviewer who treats those transforms as privacy mechanisms that should themselves have been noised will find the ledger too small. The ledger is the sum of the Laplace epsilons actually spent.

## 🚀 Local Installation & Benchmarking

```bash
python3 -m venv venv
source venv/bin/activate
pip install -e ".[dev]"
python -m dicom_differential_privacy_matrix
python -m dicom_differential_privacy_matrix.harness
```

```python
import asyncio

from dicom_differential_privacy_matrix import DicomDifferentialPrivacyMatrix


async def demo() -> None:
    engine = DicomDifferentialPrivacyMatrix(epsilon=0.8, seed=17)
    report = await engine.run(
        [
            {
                "tag": 0x00101010,
                "value": 47.0,
                "slice_index": 0,
                "series": 3,
                "missing": False,
            }
        ]
    )
    spent = float(report["epsilon_spent"])
    generalized = int(report["generalized"])
    print(spent, generalized, report["released"][0]["value"])


asyncio.run(demo())
```

`requirements.txt` documents a standard-library runtime. `pip install -r requirements.txt` succeeds with nothing to fetch. Install the package with `pip install .`.

## 🖥️ Terminal Diagnostic Output Preview

```
2026-10-02T03:20:08+0000 WARNING [dicom_differential_privacy_matrix.engine] slice dropout tag=2145386512 slice_index=2
2026-10-02T03:20:08+0000 INFO [dicom_differential_privacy_matrix.engine] stream=imaging.tags.anonymized suppressed=1 generalized=2 noised=2 epsilon_spent=1.600000 dropout=1
2026-10-02T03:20:08+0000 INFO [__main__] {'stream': 'imaging.tags.anonymized', 'suppressed': 1, 'generalized': 2, 'noised': 2, 'epsilon_spent': 1.6, 'dropout': 1, 'released': [{'tag': 1052688, 'value': 90.0, 'slice_index': 0, 'series': 3, 'action': 'generalize-age', 'laplace_scale': 0.0}, {'tag': 1052736, 'value': 941.0, 'slice_index': 0, 'series': 3, 'action': 'generalize-postal', 'laplace_scale': 0.0}, {'tag': 2145386512, 'value': 2030.1579639856068, 'slice_index': 0, 'series': 3, 'action': 'laplace', 'laplace_scale': 5118.75}, {'tag': 1597472, 'value': 1.607896250701533, 'slice_index': 1, 'series': 3, 'action': 'laplace', 'laplace_scale': 1.25}], 'mean_released': 765.6914650590771, 'stdev_released': 817.005395546332, 'ring_depth': 4}
```

`python -m dicom_differential_privacy_matrix` exits 0. Tag `1052688` is age `0x00101010` binned to 90. Tag `1052736` is postal `0x00101040` truncated from 94107 to 941. The accession number is absent from `released`. Pixel scale `5118.75` is `4095 / 0.8`. The exposure sample left `[0, 1]` because noise is not re-clipped.

## 📊 Empirical Benchmarking Performance Report

Measured by `PYTHONPATH=src python -m dicom_differential_privacy_matrix.harness` with `random.Random(514)`, 5000 iterations, `time.perf_counter_ns` latency in microseconds, and `tracemalloc` peak.

| Metric | Measured |
| --- | ---: |
| Status | PASS |
| Iterations | 5000 |
| Average latency | 82.7 µs |
| Empirical P99 | 109.286 µs |
| Scenario latency | 136.093 µs |
| tracemalloc peak | 295113 bytes |

## 🛡️ Edge-Case Resilience & SOC2/Regulatory Compliance

`math.inf` and NaN on a present tag raise `EngineKernelException` before epsilon is spent and before a row is appended. A missing-slice flag increments `dropout` and emits no measurement for that record. The rest of the study is still released.

Direct-identifier tag ids (patient id, other patient ids, birth date, accession, institution id, and the numeric patient-name slot) are dropped. The module never constructs a patient name or an MRN string. Age is generalized into 5-year bins and capped at 90. A postal integer is truncated to its first three digits.

The matrix is aligned with the HIPAA Safe Harbor shape of removing direct identifiers, aggregating ages of 90 and above, and shortening geography to three digits. It does not apply the Safe Harbor small-population ZIP exception, and it is not a determination that a dataset is de-identified. It is not a substitute for a privacy-office review. Processing integrity is the operational control: epsilon spent is the sum of the per-measurement budgets, suppression is counted, and the raw identifier is not in the outbound record.
