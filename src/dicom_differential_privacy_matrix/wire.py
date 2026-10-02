"""Little-endian numeric DICOM tag frames. No names and no MRNs."""

from __future__ import annotations

import struct

from .exceptions import EngineKernelException

FORMAT: str = "<IdIIB"
FRAME: struct.Struct = struct.Struct(FORMAT)


def pack_tag(
    tag: int,
    value: float,
    slice_index: int,
    series: int,
    missing: bool,
) -> bytes:
    """Pack one numeric tag.

    Layout, little-endian: tag ``uint32``, value ``float64``, slice ``uint32``,
    series ``uint32``, missing flag ``uint8``.
    """
    if isinstance(tag, bool) or not isinstance(tag, int):
        raise EngineKernelException("tag id must be an integer")
    if tag < 0 or tag > 0xFFFFFFFF:
        raise EngineKernelException(f"tag id out of uint32 range: {tag}")
    if slice_index < 0 or slice_index > 0xFFFFFFFF:
        raise EngineKernelException(f"slice index out of range: {slice_index}")
    if series < 0 or series > 0xFFFFFFFF:
        raise EngineKernelException(f"series out of range: {series}")
    flag = 1 if missing else 0
    try:
        return FRAME.pack(tag, float(value), slice_index, series, flag)
    except (struct.error, OverflowError, ValueError) as exc:
        raise EngineKernelException("tag pack failed") from exc


def unpack_tag(payload: bytes) -> dict[str, object]:
    """Unpack one tag. Finite values round-trip exactly."""
    if len(payload) != FRAME.size:
        raise EngineKernelException(f"tag length {len(payload)} != {FRAME.size}")
    try:
        tag, value, slice_index, series, flag = FRAME.unpack(payload)
    except struct.error as exc:
        raise EngineKernelException("tag unpack failed") from exc
    return {
        "tag": int(tag),
        "value": float(value),
        "slice_index": int(slice_index),
        "series": int(series),
        "missing": bool(flag),
    }
