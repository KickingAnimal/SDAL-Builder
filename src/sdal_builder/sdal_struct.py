# src/sdal_builder/sdal_struct.py
from __future__ import annotations
import struct

# --------------------------------------------------------------------------- #
# SDAL 1.7 Big-Endian Struct Definitions
# --------------------------------------------------------------------------- #
# Все структуры определяются в формате Big-Endian (>) для соответствия SDAL.

UINT32 = struct.Struct(">I")
INT32 = struct.Struct(">i")
UINT64 = struct.Struct(">Q")

# KD-Tree Node Payload (uint32 index, int32 lon, int32 lat)
KDTREE_NODE_STRUCT = struct.Struct(">Iii")


def pack_uint64(value: int) -> bytes:
    """Упаковывает int в Big-Endian uint64 для B+-дерева."""
    return UINT64.pack(value)