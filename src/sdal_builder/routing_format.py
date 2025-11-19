# src/sdal_builder/routing_format.py
#
# SDAL 1.7 Routing Parcel Builder

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from typing import List, Tuple, Any
from .constants import NO_COMPRESSION 
# from .encoder import encode_bytes # Импорт encode_bytes перенесен внутрь функции для разрыва циклической зависимости

# ────────────────────────────────────────────────────────────────
# SDAL 1.7 Constants & Helpers
# ────────────────────────────────────────────────────────────────

NTU_PER_DEG = 100_000 

def deg_to_ntu(lat_deg: float, lon_deg: float) -> Tuple[int, int]:
    """
    Convert degrees to NavTech Units (NTU).
    """
    lat_ntu = int(round(lat_deg * NTU_PER_DEG))
    lon_ntu = int(round(lon_deg * NTU_PER_DEG))

    def clamp_32(v: int) -> int:
        if v < -0x80000000: return -0x80000000
        if v > 0x7FFFFFFF: return 0x7FFFFFFF
        return v

    return clamp_32(lat_ntu), clamp_32(lon_ntu)

# ────────────────────────────────────────────────────────────────
# Variable Length Value Encoders
# ────────────────────────────────────────────────────────────────

def encode_type1_varuint(n: int) -> bytes:
    """
    Encodes a Type 1 Variable Length Unsigned Value.
    """
    if n < 0 or n > 1_114_095:
        raise ValueError(f"Type1 value out of range: {n}")

    if n <= 239:
        return bytes([n])
    
    if n <= 4_079:
        x = n - 240
        return bytes([0xF0 | ((x >> 8) & 0x0F), x & 0xFF])
    
    if n <= 65_519:
        x = n - 4_080
        return bytes([0xFF, (x >> 8) & 0xFF, x & 0xFF])

    # 4 bytes
    x = n - 65_520
    return bytes([0xFF, 0xF0 | ((x >> 16) & 0x0F), (x >> 8) & 0xFF, x & 0xFF])

encode_varuint = encode_type1_varuint

def encode_type4_varuint(n: int) -> bytes:
    """
    Encodes a Type 4 Variable Length Unsigned Value.
    """
    if n < 0:
        raise ValueError(f"Type4 value must be non-negative: {n} (Spec requires Unsigned)")

    if n <= 61_439:
        return struct.pack(">H", n)

    x = n - 61_440
    if x > 0xFFFFF:
         raise ValueError(f"Type4 value out of range: {n}")

    b0 = 0xF0 | ((x >> 16) & 0x0F)
    b1 = (x >> 8) & 0xFF
    b2 = x & 0xFF
    return bytes([b0, b1, b2])

encode_type4_varint = encode_type4_varuint

# ────────────────────────────────────────────────────────────────
# Data Structures
# ────────────────────────────────────────────────────────────────

@dataclass
class NodeRecord:
    """
    Represents a Routing Node Record.
    """
    node_id: int
    lat_deg: float
    lon_deg: float
    segment_ids: List[int] = field(default_factory=list)
    
    seg_base_index: int = 0

@dataclass
class SegmentRecord:
    """
    Represents a Routing Segment Record.
    """
    seg_id: int
    from_node_id: int
    to_node_id: int
    length_m: float

    speed_class: int = 0  
    oneway: int = 0       
    
    ramp: int = 0
    roundabout: int = 0
    toll: int = 0
    ferry: int = 0

# ────────────────────────────────────────────────────────────────
# Block Encoders
# ────────────────────────────────────────────────────────────────

def encode_nodes_block(nodes: List[NodeRecord], rect_ntu: Tuple[int, int, int, int], scale_shift: int) -> bytes:
    """
    Encodes the Node Data block.
    """
    min_lat, max_lat, min_lon, max_lon = rect_ntu
    
    anchor_lat = min_lat
    anchor_lon = min_lon

    buf = bytearray()
    
    buf += struct.pack(">ii", anchor_lat, anchor_lon)

    for n in nodes:
        lat_ntu, lon_ntu = deg_to_ntu(n.lat_deg, n.lon_deg)
        
        dlat = (lat_ntu - anchor_lat) >> scale_shift
        dlon = (lon_ntu - anchor_lon) >> scale_shift
        
        if dlat < 0 or dlon < 0:
             raise ValueError(f"Node coordinate delta negative. Point outside parcel rect? Node: {lat_ntu},{lon_ntu} Anchor: {anchor_lat},{anchor_lon}")

        buf += encode_type4_varint(dlat)
        buf += encode_type4_varint(dlon)
        
        deg = len(n.segment_ids)
        buf += encode_varuint(deg)
        
        buf += encode_varuint(n.seg_base_index)

    return bytes(buf)


def encode_segments_block(segments: List[SegmentRecord]) -> bytes:
    """
    Encodes the Segment Data block.
    """
    buf = bytearray()
    for s in segments:
        buf += encode_varuint(s.from_node_id)
        buf += encode_varuint(s.to_node_id)
        
        length_units = int(round(s.length_m))
        buf += encode_varuint(length_units)
        
        flags = (s.oneway & 1) | ((s.speed_class & 0x0F) << 1)
        buf.append(flags)
        
    return bytes(buf)

# ────────────────────────────────────────────────────────────────
# Parcel Encoder 
# ────────────────────────────────────────────────────────────────

def encode_routing_parcel(pid: int, 
                          nodes: List[NodeRecord], 
                          segments: List[SegmentRecord], 
                          region: int, 
                          parcel_type: int, 
                          parcel_desc: int,
                          offset_units: int, 
                          rect_ntu: Tuple[int, int, int, int], 
                          scale_shift: int,
                          size_index: int = 0,
                          compress_type: int = NO_COMPRESSION) -> bytes:
    """
    Encodes a full Routing Parcel using the block structures.
    """
    # РАЗРЫВ ЦИКЛИЧЕСКОЙ ЗАВИСИМОСТИ: Импорт внутри функции
    from .encoder import encode_bytes 
    
    # Encode Data Blocks
    nodes_block = encode_nodes_block(nodes, rect_ntu, scale_shift)
    segments_block = encode_segments_block(segments)

    # Construct Parcel Specific Header 
    min_lat, max_lat, min_lon, max_lon = rect_ntu
    
    header = bytearray()
    
    # DBRect_t rect (4 * 4 bytes Slong_t). SDAL order: min_lon, min_lat, max_lon, max_lat.
    header += struct.pack(">iiii", min_lon, min_lat, max_lon, max_lat) # 16 bytes
    
    # scaleShift (uint8), reserved (uint8)
    header += struct.pack(">B", scale_shift) # 1 byte
    header += struct.pack(">B", 0) # 1 byte reserved
    
    # nodeCount (uint32) - ИСПРАВЛЕНО: 'H' на 'I' для поддержки >65535 узлов
    header += struct.pack(">I", len(nodes)) # 4 bytes
    
    # segCount (uint32)
    header += struct.pack(">I", len(segments)) # 4 bytes
    
    # Total PSH size: 16 + 1 + 1 + 4 + 4 = 26 bytes.

    # Combine Payload
    raw_payload = bytes(header) + nodes_block + segments_block

    # Wrap in SDAL Parcel Header (PclHdr_t) using the CORRECT encoder
    return encode_bytes(
        pid=pid,
        payload=raw_payload,
        offset_units=offset_units,
        region=region,
        parcel_type=parcel_type,
        parcel_desc=parcel_desc,
        size_index=size_index,
        compress_type=compress_type,
    )