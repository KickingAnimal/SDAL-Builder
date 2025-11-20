# src/sdal_builder/spatial.py
from __future__ import annotations

from typing import Iterable, Tuple, List

from scipy.spatial import cKDTree
import bplustree

# ИМПОРТ ЦЕНТРАЛИЗОВАННЫХ СТРУКТУР (Big-Endian)
from .sdal_struct import KDTREE_NODE_STRUCT, pack_uint64, UINT32

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
    Serialize KD-tree nodes into SDAL-like format (Big-Endian).
    
    Format (Big-Endian):
    1. Header: uint32 (Total number of POI nodes)
    2. Nodes: sequence of <uint32 idx><int32 x*1e6><int32 y*1e6>
    """
    buf = bytearray()
    
    poi_count = len(kd.data)
    
    # 1. Header: POI Count (uint32, Big-Endian)
    buf.extend(UINT32.pack(poi_count)) 

    # 2. Nodes data
    for idx, (x, y) in enumerate(kd.data):
        # ИСПОЛЬЗУЕМ ЦЕНТРАЛИЗОВАННУЮ СТРУКТУРУ KDTREE_NODE_STRUCT (Big-Endian)
        buf.extend(KDTREE_NODE_STRUCT.pack(idx, int(x * 1e6), int(y * 1e6))) 

    return bytes(buf)

# --------------------------------------------------------------------------- #
# B+-tree helpers                                                             #
# --------------------------------------------------------------------------- #

def build_bplustree(offsets: Iterable[Tuple[int, int]], path: str) -> None:
    """
    Build an on-disk B+-tree mapping *way_id* (uint32 int) ➜ *offset* (uint64).
    """
    tree = bplustree.BPlusTree(path, key_size=4, value_size=8, order=50)
    for way_id, offset in offsets:
        # ИСПОЛЬЗУЕМ ЦЕНТРАЛИЗОВАННЫЙ УПАКОВЩИК UINT64 (Big-Endian)
        tree[way_id] = pack_uint64(offset)
    tree.close()