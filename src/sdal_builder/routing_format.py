# src/sdal_builder/routing_format.py
from __future__ import annotations

import struct
import io
from dataclasses import dataclass, field
from typing import List, Tuple, Any, Optional

# ИМПОРТ ТОЛЬКО НЕОБХОДИМЫХ КОНСТАНТ 
from .constants import NO_COMPRESSION, ROUTING_PARCEL_ID

# ────────────────────────────────────────────────────────────────
# SDAL 1.7 Constants & Helpers
# ────────────────────────────────────────────────────────────────

NTU_PER_DEG = 100_000  # 1 NTU = 1/100,000 degree

# Размеры структур SDAL 1.7
DBRECT_SIZE = 16
SPTL_PCL_HDR_SIZE = 128 
ROUTING_HDR0_SIZE = 32 # RoutingParcelHeader0_t size
BLOCK_OFFSET_ARRAY_SIZE = 16 # Minimal Block Offset Array (4 x Ulong)

def deg_to_ntu(lat_deg: float, lon_deg: float) -> Tuple[int, int]:
    """
    Convert degrees to NavTech Units (NTU).
    SDAL uses integer values of NTUs.
    """
    lat_ntu = int(round(lat_deg * NTU_PER_DEG))
    lon_ntu = int(round(lon_deg * NTU_PER_DEG))

    def clamp_32(v: int) -> int:
        if v < -0x80000000: return -0x80000000
        if v > 0x7FFFFFFF: return 0x7FFFFFFF
        return v
    
    return clamp_32(lat_ntu), clamp_32(lon_ntu)


# ────────────────────────────────────────────────────────────────
# Variable Length Value (VLV) Encoding (Type 1, 4, 5)
# ────────────────────────────────────────────────────────────────

class BitStream:
    """Helper for bit-level writing (essential for Type 5 VLV)."""
    def __init__(self):
        self.buffer = bytearray()
        self.bit_count = 0

    def write_bits(self, value: int, num_bits: int):
        if num_bits == 0:
            return
        
        while num_bits > 0:
            bits_to_write = min(8 - self.bit_count, num_bits)
            shift = num_bits - bits_to_write
            
            mask = (1 << bits_to_write) - 1
            segment = (value >> shift) & mask
            
            if self.bit_count == 0:
                self.buffer.append(0)
            
            # Записываем сегмент в текущий байт
            self.buffer[-1] |= segment << (8 - self.bit_count - bits_to_write)
            
            self.bit_count += bits_to_write
            num_bits -= bits_to_write
            
            if self.bit_count == 8:
                self.bit_count = 0

    def finalize(self) -> bytes:
        if self.bit_count > 0:
            pass
        return bytes(self.buffer)

def encode_vlv_type1(value: int) -> bytes:
    """Type 1: 1-4 bytes, unsigned. 0xxxxxxx (1 byte), 10xxxxxx xxxxxxxx (2 bytes), etc."""
    if value < 0x80:
        return struct.pack(">B", value)
    elif value < 0x4000:
        return struct.pack(">H", value | 0x8000)
    elif value < 0x200000:
        data = value | 0xC00000
        return struct.pack(">I", data)[1:] 
    else:
        return struct.pack(">I", value | 0xE0000000)

def encode_vlv_type5_signed(value: int, bs: BitStream, bit_length: int = 19):
    """
    Type 5: Variable Length Signed Value (19-bit assumed for deltas).
    Кодирует знак (1 бит) + Magnitude (bit_length - 1 бит).
    """
    if bit_length < 7 or bit_length > 19:
        raise ValueError("Type 5 VLV must be 7 to 19 bits.")
        
    sign_bit = 1 if value < 0 else 0
    magnitude = abs(value)
    
    # 1. Write Sign Bit (1 бит)
    bs.write_bits(sign_bit, 1)
    
    # 2. Write Magnitude (bit_length - 1 бит)
    mag_bits = bit_length - 1
    if magnitude >= (1 << mag_bits):
        magnitude = (1 << mag_bits) - 1
    
    bs.write_bits(magnitude, mag_bits)


# ────────────────────────────────────────────────────────────────
# Data Structures
# ────────────────────────────────────────────────────────────────

@dataclass
class NodeRecord:
    node_id: int
    lat_deg: float
    lon_deg: float
    segment_ids: List[int] = field(default_factory=list)

@dataclass
class SegmentRecord:
    seg_id: int
    from_node_id: int
    to_node_id: int
    length_m: float
    speed_class: int
    oneway: int


# ────────────────────────────────────────────────────────────────
# Encoding Blocks (SDAL 1.7 Compliant BRPPD Simulation)
# ────────────────────────────────────────────────────────────────

def _encode_block_descriptor(block_type: int, entry_count: int) -> bytes:
    """
    BlkDesc_t / BlkDesc2_t: usBlkId (H), ulEntryCount (I).
    """
    return struct.pack(">HI", block_type, entry_count)


def encode_nodes_block(
    nodes: List[NodeRecord],
    rect_ntu: Tuple[int, int, int, int],
    scale_shift: int,
) -> bytes:
    """
    Encodes Node Data Block simulating BRPPD, preceded by BlkDesc.
    """
    
    if not nodes:
        return b""
        
    min_lat_ntu, _, min_lon_ntu, _ = rect_ntu
    
    # Block Header
    header = _encode_block_descriptor(block_type=0x0100, entry_count=len(nodes)) # 0x0100 for Node Data
    
    current_lat = min_lat_ntu
    current_lon = min_lon_ntu
    
    data_buffer = io.BytesIO()
    
    bs = BitStream()

    for n in nodes:
        lat_ntu, lon_ntu = deg_to_ntu(n.lat_deg, n.lon_deg)
        
        # 1. Node ID (VLV Type 1: 1-4 bytes)
        data_buffer.write(encode_vlv_type1(n.node_id))
        
        # 2. Delta Lon/Lat (VLV Type 5: 19-bit)
        delta_lat = lat_ntu - current_lat
        delta_lon = lon_ntu - current_lon

        encode_vlv_type5_signed(delta_lon, bs, 19) 
        encode_vlv_type5_signed(delta_lat, bs, 19)
        
        current_lat = lat_ntu
        current_lon = lon_ntu
    
    data_buffer.write(bs.finalize())
    
    return header + data_buffer.getvalue()


def encode_segments_block(segments: List[SegmentRecord]) -> bytes:
    """
    Encodes Segment Data Block using Type 1 VLV, preceded by BlkDesc.
    """
    if not segments:
        return b""
        
    # Block Header
    header = _encode_block_descriptor(block_type=0x0200, entry_count=len(segments)) # 0x0200 for Segment Data
    
    data_buffer = io.BytesIO()

    for s in segments:
        # 1. Segment ID (VLV Type 1)
        data_buffer.write(encode_vlv_type1(s.seg_id))
        
        # 2. From/To Node ID (VLV Type 1)
        data_buffer.write(encode_vlv_type1(s.from_node_id))
        data_buffer.write(encode_vlv_type1(s.to_node_id))
        
        # 3. Attributes (Length)
        data_buffer.write(struct.pack(">f", s.length_m))
        
    return header + data_buffer.getvalue()


# ────────────────────────────────────────────────────────────────
# Internal Routing Header (RoutingParcelHeader0_t)
# ────────────────────────────────────────────────────────────────

# ИСПРАВЛЕНО:
# Структура теперь явно включает 16 полей, включая разбитые reserved и padding.
# Размер строго 32 байта: 26 байт данных + 6 байт паддинга.
_ROUTING_HDR0_STRUCT = struct.Struct(
    ">H I B B I H H H B B B B H H H I"
)

def _encode_routing_header_0(node_count: int, seg_count: int) -> bytes:
    """
    Encodes RoutingParcelHeader0_t (32 bytes).
    """
    args = (
        0xFFFF,     # 1. usMaxArmToArm (H)
        seg_count,  # 2. ulTotalSegs (I)
        0xFF,       # 3. ucMaxSegsPerNode (B) - было 'b', стало 'B' для 0xFF
        0x00,       # 4. ucRoutingFlags (B)
        node_count, # 5. ulTotalNodes (I)
        0xFFFF,     # 6. usMaxNodes (H)
        0xFFFF,     # 7. usMaxSegs (H)
        0x0001,     # 8. usMaxTileLayers (H)
        0x00, 0x00, 0x00, 0x00, # 9-12. ucReserved[4] (4 x B)
        0xFFFF,     # 13. usMaxCoordDeltaX (H)
        0xFFFF,     # 14. usMaxCoordDeltaY (H)
        # Padding to 32 bytes total:
        # (26 bytes payload + 2 bytes + 4 bytes = 32 bytes)
        0x0000,     # 15. Padding part 1 (H - 2 bytes)
        0x00000000  # 16. Padding part 2 (I - 4 bytes)
    )

    return _ROUTING_HDR0_STRUCT.pack(*args)

# ────────────────────────────────────────────────────────────────
# Block Offset Array (BOA)
# ────────────────────────────────────────────────────────────────
def _encode_block_offset_array(node_data_offset: int, seg_data_offset: int) -> bytes:
    """
    Encodes the Block Offset Array (4 x Ulong, 16 bytes).
    Offsets are relative to the start of this array.
    """
    # Offset 0: Node Data Block Offset (относительно начала BOA)
    # Offset 4: Segment Data Block Offset (относительно начала BOA)
    # Offset 8: Condition Data Block Offset (Placeholder)
    # Offset 12: Reserved (Placeholder)
    return struct.pack(">IIII",
                       node_data_offset, 
                       seg_data_offset,  
                       0,               
                       0)                


# ────────────────────────────────────────────────────────────────
# Main Parcel Encoder (SptlPclHdr_t)
# ────────────────────────────────────────────────────────────────

# ПОЛНЫЙ SPTL_PCL_HDR_T (128 байт)
_SPTL_HDR_STRUCT = struct.Struct(
    # 1. DBRect_t boundingRect, tileRect, ancestorRect (3 * 16 = 48 bytes)
    ">iiiiiiiiiiii" 
    # 2. XrfPclHdr_t (20 bytes)
    # ulPclIDTblOffset(I), usPclIDTblLen(H), ucPclIDTblEntryLen(B), ucReserved(B)
    "IHBB"
    # 3. KD-Tree Offsets/Sizes (32 bytes, ushort * 16)
    "HHHHHHHHHHHHHHHH" 
    # 4. Remaining: Scale/Layer/NodeCount/SegCount (28 bytes)
    # ucScale(B), ucLayer(B), usReserved(H), ulNodeCount(I), ulSegCount(I)
    "BBHII"
    # 5. Padding (16 bytes)
    "4I" 
)


def _encode_spatial_parcel_header_prefix(
    rect_ntu: Tuple[int, int, int, int],
    scale_shift: int,
    node_count: int,
    seg_count: int,
) -> bytes:
    """
    Создает полный заголовок SptlPclHdr_t, соответствующий SDAL 1.7.
    """
    min_lat, max_lat, min_lon, max_lon = rect_ntu

    # 1. DBRect'ы (3 раза)
    dbrect_args = (min_lon, min_lat, max_lon, max_lat) * 3
    
    # 2. XrfPclHdr_t (заглушка, 20 байт)
    xrf_args = (0, 0, 0, 0) 
    
    # 3. KD/B-Tree Offsets (16 x ushort, заглушка, 32 байта)
    kd_btree_offsets = (0,) * 16
    
    # 4. Scale/Layer/NodeCount/SegCount
    rest_args = (scale_shift, 1, 0, node_count, seg_count)
    
    # 5. Padding (4 x Ulong, 16 bytes)
    padding_args = (0,) * 4

    args = dbrect_args + xrf_args + kd_btree_offsets + rest_args + padding_args
    
    return _SPTL_HDR_STRUCT.pack(*args)


def encode_routing_parcel(pid: int, 
                          nodes: List[NodeRecord], 
                          segments: List[SegmentRecord], 
                          region: int,
                          parcel_type: int, 
                          parcel_desc: int, 
                          offset_units: int, 
                          rect_ntu: Tuple[int, int, int, int], 
                          scale_shift: int,
                          compress_type: int = NO_COMPRESSION,
                          size_index: int = 0) -> bytes:
    """
    Encodes a full Routing Parcel, используя SptlPclHdr_t, RoutingParcelHeader0_t, BOA и Блоки.
    """
    from .encoder import encode_bytes 
    
    # 1. Encode Data Blocks (Node & Segment Data + BlkDesc)
    nodes_block = encode_nodes_block(nodes, rect_ntu, scale_shift)
    segments_block = encode_segments_block(segments)
    
    # 2. Calculate offsets for BOA
    # BOA начинается сразу после Routing Header 0. 
    node_data_offset_from_boa = BLOCK_OFFSET_ARRAY_SIZE
    
    # Segment Block начинается после Node Block.
    seg_data_offset_from_boa = node_data_offset_from_boa + len(nodes_block)
    
    # 3. Construct Headers and BOA
    spatial_header = _encode_spatial_parcel_header_prefix(
        rect_ntu,
        scale_shift,
        len(nodes),
        len(segments),
    )
    
    routing_header_0 = _encode_routing_header_0(len(nodes), len(segments))
    
    block_offset_array = _encode_block_offset_array(
        node_data_offset_from_boa,
        seg_data_offset_from_boa
    )

    # 4. Combine Payload (SptlHdr + RoutingHdr0 + BOA + Data Blocks)
    raw_payload = (
        spatial_header + 
        routing_header_0 + 
        block_offset_array + 
        nodes_block + 
        segments_block
    )
    
    # 5. Wrap in standard PclHdr_t
    return encode_bytes(
        pid=ROUTING_PARCEL_ID, 
        payload=raw_payload,
        region=region,
        parcel_type=parcel_type,
        parcel_desc=parcel_desc,
        offset_units=offset_units,
        compress_type=compress_type,
        size_index=size_index
    )