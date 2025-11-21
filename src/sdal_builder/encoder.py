# src/sdal_builder/encoder.py
import io
import struct
from typing import List, Tuple, Optional
from .routing_format import deg_to_ntu 
from .constants import NO_COMPRESSION, SZIP_COMPRESSION, UNCOMPRESSED_FLAG, GLB_KD_TREE_PID, ROUTING_PARCEL_ID

"""
encoder.py — SDAL/PSF parcel encoder с SDAL 1.7–style PclHdr_t и NTU координатами.
"""

# ────────────────────────────────────────────────────────────────
# Constants
# ────────────────────────────────────────────────────────────────

PCL_HEADER_SIZE = 20  # bytes, 20-byte PclHdr_t
MAX_USHORT = 0xFFFF # 65535

# Канонический PclHdr_t (11 полей, 20 байт): I H B B B B H H H H H
_PCL_STRUCT = struct.Struct(">I H B B B B H H H H H")


# ────────────────────────────────────────────────────────────────
# Parcel Encoding
# ────────────────────────────────────────────────────────────────

def encode_bytes(
    pid: int,
    payload: bytes,
    *,
    region: int = 1,
    parcel_type: int = 0,
    parcel_desc: int = 0, 
    offset_units: Optional[int] = None,
    compress_type: int = NO_COMPRESSION,
    size_index: int = 0,
    external_to_region: bool = False,
    redundancy: bool = False,
    block_size: int = 4096,
) -> bytes:
    """
    Encodes a payload into a full SDAL parcel (PclHdr_t + data).
    """
    if offset_units is None:
        offset_units = 0

    if pid == 0:
        final_pid = (offset_units & 0xFFFFFF) | (size_index << 24)
        if external_to_region: final_pid |= 1 << 27
        if redundancy: final_pid |= 1 << 28
    else:
        final_pid = pid

    payload_len = len(payload)
    compressed_payload = payload
    compressed_size_bytes = payload_len
    
    # 2. Вычисление полей PclHdr_t (20 bytes)
    
    us_parcel_desc = parcel_desc
    b_endian_swap = 0 # Big-Endian = 0
    us_compress_type = compress_type
    
    # ucCmpDataSizeHi / usCmpDataSizeLo (Размер в битах)
    if compress_type != NO_COMPRESSION:
        compressed_size_bits = compressed_size_bytes * 8
        cmp_size_hi = (compressed_size_bits >> 16) & 0xFF 
        cmp_size_lo = compressed_size_bits & MAX_USHORT
    else:
        # ИСПРАВЛЕНИЕ ПЕРЕПОЛНЕНИЯ: Устанавливаем в 0 для больших несжатых парселов
        cmp_size_hi = 0
        cmp_size_lo = 0

    # usCmpData (H) - 9th field: Смещение до данных (относительно начала PclHdr_t)
    us_cmp_data_offset = PCL_HEADER_SIZE
    
    # usCmpDataUncompSize (H) - 10th field: Полный несжатый размер (Header + Payload), ограниченный 16 битами.
    total_uncompressed_size = PCL_HEADER_SIZE + payload_len
    us_cmp_data_uncomp_size = total_uncompressed_size if total_uncompressed_size <= MAX_USHORT else MAX_USHORT
    
    # 3. Упаковка PclHdr_t (11 аргументов)
    header = _PCL_STRUCT.pack(
        final_pid,                  # 1. ParcelID_t parcelid (I)
        us_parcel_desc,             # 2. Ushort_t usParcelDesc (H)
        parcel_type,                # 3. Uchar_t ucParcelType (B)
        region,                     # 4. RegionID_t region (B)
        b_endian_swap,              # 5. Bool_t bEndianSwap (B)
        cmp_size_hi,                # 6. Uchar_t ucCmpDataSizeHi (B)
        cmp_size_lo,                # 7. Ushort_t usCmpDataSizeLo (H)
        us_compress_type,           # 8. Ushort_t usCompressType (H)
        us_cmp_data_offset,         # 9. Ushort_t usCmpData (H)
        us_cmp_data_uncomp_size,    # 10. Ushort_t usCmpDataUncompSize (H)
        0                           # 11. Ushort_t usExtensionOffset (H)
    )

    return header + compressed_payload


def encode_strings(
    pid: int,
    strings: List[str],
    *,
    compress_type: int = NO_COMPRESSION, 
    region: int = 1,
    parcel_type: int = 0,
    parcel_desc: int = 0,
    offset_units: Optional[int] = None,
    size_index: int = 0,
    external_to_region: bool = False,
    redundancy: bool = False,
    block_size: int = 4096,
) -> bytes:
    """
    Encodes a list of strings into a single parcel with offsets table.
    """
    
    # 1. Сборка данных строк
    string_buffer = io.BytesIO()
    for s in strings:
        s_bytes = s.encode('ascii', 'replace') + b'\x00'
        string_buffer.write(s_bytes)
    
    string_data = string_buffer.getvalue()
    
    # 2. Сборка таблицы смещений
    offsets_table_buffer = io.BytesIO()
    offsets_table_buffer.write(struct.pack(">I", len(strings))) # ulStringCount
    
    current_string_offset = 0
    for s in strings:
        offsets_table_buffer.write(struct.pack(">I", current_string_offset))
        current_string_offset += len(s.encode('ascii', 'replace') + b'\x00')

    payload = offsets_table_buffer.getvalue() + string_data

    return encode_bytes(
        pid,
        payload,
        compress_type=compress_type,
        region=region,
        parcel_type=parcel_type,
        parcel_desc=parcel_desc,
        offset_units=offset_units,
        size_index=size_index,
        external_to_region=external_to_region,
        redundancy=redundancy,
        block_size=block_size,
    )


def encode_cartography(
    pid: int,
    records: List[Tuple[int, List[Tuple[float, float]]]],
    *,
    compress_type: int = NO_COMPRESSION,
    region: int = 1,
    parcel_type: int = 0,
    parcel_desc: int = 0,
    offset_units: Optional[int] = None,
    rect_ntu: Tuple[int, int, int, int] = (0, 0, 0, 0),
    size_index: int = 0,
    external_to_region: bool = False,
    redundancy: bool = False,
    block_size: int = 4096,
) -> bytes:
    """
    Encodes a Cartography Parcel (PID 110) payload with a header containing DBRect.
    """
    buf = io.BytesIO()
    # DBRect Header: min_lon, min_lat, max_lon, max_lat, count (I I I I H)
    buf.write(struct.pack(">iiiiH", rect_ntu[2], rect_ntu[0], rect_ntu[3], rect_ntu[1], len(records)))
    
    # Write records
    for way_id, coords in records:
        buf.write(struct.pack(">I", way_id))
        buf.write(struct.pack(">H", len(coords)))
        for lon, lat in coords:
            lat_ntu, lon_ntu = deg_to_ntu(lat, lon)
            buf.write(struct.pack(">ii", lon_ntu, lat_ntu))
            
    payload = buf.getvalue()
    
    return encode_bytes(
        pid, payload, compress_type=compress_type, region=region, parcel_type=parcel_type,
        parcel_desc=parcel_desc, offset_units=offset_units, size_index=size_index,
        external_to_region=external_to_region, redundancy=redundancy, block_size=block_size,
    )


def encode_btree(
    pid: int,
    offsets: List[Tuple[int, int]],
    *,
    compress_type: int = NO_COMPRESSION,
    region: int = 1,
    parcel_type: int = 0,
    parcel_desc: int = 0,
    offset_units: Optional[int] = None,
    size_index: int = 0,
    external_to_region: bool = False,
    redundancy: bool = False,
    block_size: int = 4096,
) -> bytes:
    """
    Encodes a B-tree or POI index parcel payload: (id, offset) pairs.
    """
    buf = io.BytesIO()
    buf.write(struct.pack(">IH", len(offsets), 1))

    # N * (I Q) for (ID, offset_uint64)
    for id_val, offset_val in offsets:
        buf.write(struct.pack(">IQ", id_val, offset_val))

    payload = buf.getvalue()

    return encode_bytes(
        pid, payload, compress_type=compress_type, region=region, parcel_type=parcel_type,
        parcel_desc=parcel_desc, offset_units=offset_units, size_index=size_index,
        external_to_region=external_to_region, redundancy=redundancy, block_size=block_size,
    )


def encode_poi_index(
    pid: int,
    offsets: List[Tuple[int, int]],
    *args, **kwargs
) -> bytes:
    """
    POI index (poi_id, byte_offset) pairs (такой же layout, как encode_btree).
    """
    return encode_btree(pid, offsets, *args, **kwargs)