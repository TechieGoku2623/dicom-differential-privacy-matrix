"""Command-line release of one synthetic numeric tag study."""

from __future__ import annotations

import asyncio
import logging

from .engine import (
    ACCESSION_TAG,
    AGE_TAG,
    EXPOSURE_TAG,
    PIXEL_TAG,
    POSTAL_TAG,
    DicomDifferentialPrivacyMatrix,
)


def _tag(
    tag: int,
    value: float,
    slice_index: int,
    *,
    missing: bool = False,
) -> dict[str, object]:
    return {
        "tag": tag,
        "value": value,
        "slice_index": slice_index,
        "series": 3,
        "missing": missing,
    }


def main() -> int:
    """Release a synthetic study and log the accounting dict."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    engine = DicomDifferentialPrivacyMatrix(epsilon=0.8, seed=17)
    records = [
        _tag(ACCESSION_TAG, 4_821_993.0, 0),
        _tag(AGE_TAG, 95.0, 0),
        _tag(POSTAL_TAG, 94_107.0, 0),
        _tag(PIXEL_TAG, 1_800.0, 0),
        _tag(EXPOSURE_TAG, 0.42, 1),
        _tag(PIXEL_TAG, 0.0, 2, missing=True),
    ]
    report = asyncio.run(engine.run(records))
    logging.getLogger(__name__).info("%s", report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
