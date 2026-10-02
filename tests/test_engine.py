"""Wire, release, and edge-case tests. No network."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import unittest

from dicom_differential_privacy_matrix import (
    DicomDifferentialPrivacyMatrix,
    EngineKernelException,
)
from dicom_differential_privacy_matrix.engine import (
    AGE_TAG,
    EXPOSURE_TAG,
    PATIENT_ID_TAG,
    POSTAL_TAG,
    laplace_noise,
)
from dicom_differential_privacy_matrix.wire import pack_tag, unpack_tag

logging.getLogger("dicom_differential_privacy_matrix").setLevel(logging.CRITICAL)


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
        "series": 1,
        "missing": missing,
    }


class DicomEngineTest(unittest.TestCase):
    def test_wire_roundtrip(self) -> None:
        payload = pack_tag(EXPOSURE_TAG, 0.5, 4, 9, False)
        frame = unpack_tag(payload)
        self.assertEqual(frame["tag"], EXPOSURE_TAG)
        self.assertEqual(frame["value"], 0.5)
        self.assertEqual(frame["slice_index"], 4)
        self.assertEqual(frame["series"], 9)
        self.assertIs(frame["missing"], False)
        self.assertEqual(payload, pack_tag(EXPOSURE_TAG, 0.5, 4, 9, False))
        missing = unpack_tag(pack_tag(AGE_TAG, 0.0, 2, 1, True))
        self.assertIs(missing["missing"], True)
        with self.assertRaises(EngineKernelException):
            unpack_tag(b"\x00\x01")

    def test_happy_path_composition(self) -> None:
        epsilon = 2.0
        seed = 11
        lo, hi = 0.0, 1.0
        raw = 0.4
        report = asyncio.run(
            DicomDifferentialPrivacyMatrix(
                epsilon=epsilon,
                seed=seed,
                bounds={EXPOSURE_TAG: (lo, hi)},
            ).run(
                [
                    _tag(PATIENT_ID_TAG, 8_241_113.0, 0),
                    _tag(AGE_TAG, 47.0, 0),
                    _tag(POSTAL_TAG, 94_107.0, 0),
                    _tag(EXPOSURE_TAG, raw, 1),
                ]
            )
        )
        self.assertEqual(report["suppressed"], 1)
        self.assertEqual(report["generalized"], 2)
        self.assertEqual(report["noised"], 1)
        self.assertEqual(report["dropout"], 0)
        self.assertTrue(math.isclose(float(report["epsilon_spent"]), epsilon))
        self.assertNotIn("8241113", json.dumps(report))
        released = report["released"]
        self.assertEqual(
            [row["value"] for row in released if row["tag"] == AGE_TAG],
            [45.0],
        )
        self.assertEqual(
            [row["value"] for row in released if row["tag"] == POSTAL_TAG],
            [941.0],
        )
        scale = (hi - lo) / epsilon
        expected = raw + laplace_noise(random.Random(seed), scale)
        measured = float(released[-1]["value"])
        self.assertTrue(math.isclose(measured, expected, abs_tol=1e-12))
        self.assertEqual(report["stream"], "imaging.tags.anonymized")

    def test_non_finite_measurements_raise(self) -> None:
        with self.assertRaises(EngineKernelException):
            asyncio.run(
                DicomDifferentialPrivacyMatrix(epsilon=1.0, seed=1).run(
                    [_tag(EXPOSURE_TAG, float("nan"), 0)]
                )
            )
        with self.assertRaises(EngineKernelException):
            asyncio.run(
                DicomDifferentialPrivacyMatrix(epsilon=1.0, seed=1).run(
                    [_tag(EXPOSURE_TAG, math.inf, 0)]
                )
            )

    def test_missing_slice_increments_dropout(self) -> None:
        report = asyncio.run(
            DicomDifferentialPrivacyMatrix(epsilon=1.0, seed=5).run(
                [
                    _tag(EXPOSURE_TAG, 0.4, 0, missing=True),
                    _tag(EXPOSURE_TAG, 0.25, 1, missing=False),
                ]
            )
        )
        self.assertEqual(report["dropout"], 1)
        self.assertEqual(report["noised"], 1)
        self.assertEqual(
            [row["slice_index"] for row in report["released"]],
            [1],
        )
