from __future__ import annotations

import struct
from typing import Iterable, Tuple, List

from scipy.spatial import cKDTree
import bplustree

# --------------------------------------------------------------------------- #
# KD-tree helpers                                                             #
# --------------------------------------------------------------------------- #

def build_kdtree(points: List[Tuple[float, float]]) -> cKDTree:
    """
    Return a KD-tree built from *points* = [(x, y), …].
    """
    return cKDTree(points)

def serialize_kdtree(kd: cKDTree) -> bytes:
    """
    Serialize KD-tree nodes into SDAL-like format.
    
    Format (Big-Endian):
    1. Header: uint32 (Total number of POI nodes)
    2. Nodes: sequence of <uint32 idx><int32 x*1e6><int32 y*1e6>
    """
    buf = bytearray()
    
    poi_count = len(kd.data)
    
    # 1. Header: POI Count (uint32, Big-Endian) - 4 bytes
    # Это дает приложению знать, сколько записей ожидать.
    buf.extend(struct.pack(">I", poi_count)) 

    # 2. Nodes data
    for idx, (x, y) in enumerate(kd.data):
        # >Iii: Big-Endian, uint32 index, int32 lon, int32 lat
        # ИСПРАВЛЕНИЕ: Смена на Big-Endian для всего содержимого (для соответствия SDAL)
        buf.extend(struct.pack(">Iii", idx, int(x * 1e6), int(y * 1e6))) 

    return bytes(buf)

# --------------------------------------------------------------------------- #
# B+-tree helpers                                                             #
# --------------------------------------------------------------------------- #

# ИСПРАВЛЕНИЕ: Смена на Big-Endian (>)
_pack_u64 = struct.Struct(">Q").pack  # Big-endian uint64

def build_bplustree(offsets: Iterable[Tuple[int, int]], path: str) -> None:
    """
    Build an on-disk B+-tree mapping *way_id* (uint32 int) ➜ *offset* (uint64).
    """
    tree = bplustree.BPlusTree(path, key_size=4, value_size=8, order=50)
    for way_id, offset in offsets:
        tree[way_id] = _pack_u64(offset)
    tree.close()