"""Kernel faults for the DICOM Differential Privacy Matrix."""

from __future__ import annotations


class EngineKernelException(Exception):
    """Raised when a record fails a traceable kernel check."""
