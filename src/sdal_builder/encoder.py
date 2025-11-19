import io
import struct
import unicodedata 
from typing import List, Tuple, Optional
from .constants import NO_COMPRESSION, SZIP_COMPRESSION, HUFFMAN_TABLE, EOF
from .routing_format import (
    NodeRecord,
    SegmentRecord,
    deg_to_ntu,
)

"""
encoder.py — SDAL/PSF parcel encoder с SDAL 1.7–style PclHdr_t и NTU координатами.
"""

# ────────────────────────────────────────────────────────────────
# Constants and Helpers
# ────────────────────────────────────────────────────────────────

PCL_HEADER_SIZE = 20
NTU_PER_DEG = 100_000

_PCL_STRUCT = struct.Struct(">I H B B B B H H H H H")


# ────────────────────────────────────────────────────────────────
# Transliteration and Safe Encoding
# ────────────────────────────────────────────────────────────────

def _transliterate_to_latin1(s: str) -> str:
    """
    Выполняет безопасную транслитерацию и очистку строки, 
    чтобы гарантировать, что результат будет корректно кодироваться в Latin-1 
    и содержать только символы, для которых есть коды в HUFFMAN_TABLE.
    
    Это решает проблему с многобайтовыми UTF-8 последовательностями.
    """
    # 1. Нормализация NFKD для разложения диакритических знаков (например, 'á' -> 'a' + 'акцент')
    normalized = unicodedata.normalize('NFKD', s)
    
    # 2. Кодирование в ASCII: это безопасно удаляет или заменяет все символы, 
    # которые не могут быть транслитерированы в базовый ASCII.
    # Это ключевой шаг, который устраняет байт 226 (0xE2).
    safe_ascii = normalized.encode('ascii', 'ignore').decode('ascii')
    
    # 3. Возвращаем строку, которая теперь состоит только из ASCII-символов. 
    # При кодировании в Latin-1 (ISO-8859-1) они остаются в диапазоне 0-127 
    # и попадают в кодовое пространство статического Хаффмана.
    return safe_ascii.encode('latin-1').decode('latin-1')


# ────────────────────────────────────────────────────────────────
# Huffman Compression (SZIP) Logic
# ────────────────────────────────────────────────────────────────

def _pack_bits_to_bytes(bit_string: str) -> bytes:
    """
    Преобразует строку битов ('0' и '1') в байты, дополняя последний байт нулями.
    """
    padding_len = (8 - (len(bit_string) % 8)) % 8
    
    padded_bit_string = bit_string + '0' * padding_len
    
    res = bytearray()
    for i in range(0, len(padded_bit_string), 8):
        byte_bits = padded_bit_string[i:i+8]
        res.append(int(byte_bits, 2)) 
    
    return bytes(res)


def huffman_compress(payload: bytes) -> bytes:
    """
    Реализует статическое сжатие Хаффмана (SZIP) согласно SDAL 1.7, 
    используя HUFFMAN_TABLE.
    """
    bit_string = ""
    
    # 1. Кодирование payload
    for byte_val in payload:
        code = HUFFMAN_TABLE.get(byte_val)
        if code is None:
            raise ValueError(f"Missing Huffman code for byte value: {byte_val}. Data cleansing failed.")
        bit_string += code

    # 2. Добавление маркера конца файла (EOF)
    eof_code = HUFFMAN_TABLE.get(EOF)
    if eof_code is None:
        raise ValueError("Missing Huffman EOF code in constants.HUFFMAN_TABLE")
    bit_string += eof_code
    
    # 3. Упаковка битов в байты
    return _pack_bits_to_bytes(bit_string)


# ────────────────────────────────────────────────────────────────
# ParcelID_t helpers
# ────────────────────────────────────────────────────────────────

def make_parcelid(
    *,
    offset_units: int,
    size_index: int = 0,
    external_to_region: bool = False,
    redundancy: bool = False,
) -> int:
    """
    Build a 32-bit ParcelID_t as described in SDAL 1.7.
    """
    if not (0 <= offset_units < (1 << 24)):
        raise ValueError("offset_units must fit in 24 bits")
    if not (0 <= size_index < 64):
        raise ValueError("size_index must be in [0, 63]")

    val = 0
    if external_to_region:
        val |= 1 << 31
    if redundancy:
        val |= 1 << 30
    val |= (size_index & 0x3F) << 24
    val |= offset_units & 0xFFFFFF
    return val & 0xFFFFFFFF


# ────────────────────────────────────────────────────────────────
# PclHdr_t builder
# ────────────────────────────────────────────────────────────────

def _build_pcl_header(
    *,
    parcelid: int,
    payload_len: int,
    compressed_payload_len: int,
    region: int = 1,
    parcel_type: int = 0,
    parcel_desc: int = 0,
    compress_type: int = NO_COMPRESSION,
    data_offset: int = PCL_HEADER_SIZE,
) -> bytes:
    """
    Construct a 20-byte PclHdr_t header for an (uncompressed) payload.
    """
    if payload_len < 0:
        raise ValueError("payload_len must be >= 0")

    bEndianSwap = 0

    # Сжатый размер в битах
    size_bits = compressed_payload_len * 8
    ucCmpDataSizeHi = (size_bits >> 16) & 0xFF
    usCmpDataSizeLo = size_bits & 0xFFFF

    usParcelDesc = parcel_desc & 0xFFFF
    ucParcelType = parcel_type & 0xFF
    region_id = region & 0xFF
    usCompressType = compress_type & 0xFFFF

    usCmpData = data_offset & 0xFFFF
    usCmpDataUncompSize = payload_len & 0xFFFF
    usExtensionOffset = 0

    return _PCL_STRUCT.pack(
        parcelid & 0xFFFFFFFF,
        usParcelDesc,
        ucParcelType,
        region_id,
        bEndianSwap,
        ucCmpDataSizeHi,
        usCmpDataSizeLo,
        usCompressType,
        usCmpData,
        usCmpDataUncompSize,
        usExtensionOffset,
    )


# ────────────────────────────────────────────────────────────────
# Generic parcel encoder
# ────────────────────────────────────────────────────────────────

def encode_bytes(
    pid: int,
    payload: bytes,
    *,
    region: int = 1,
    parcel_type: int = 0,
    parcel_desc: int = 0,
    compress_type: int = NO_COMPRESSION,
    offset_units: Optional[int] = None,
    size_index: int = 0,
    external_to_region: bool = False,
    redundancy: bool = False,
    block_size: int = 4096,
) -> bytes:
    """
    Оборачивает произвольный payload в SDAL/PSF parcel (PclHdr_t + данные).
    """
    if offset_units is None:
        offset_units = 0

    if parcel_desc == 0:
        parcel_desc = pid & 0xFFFF

    # 1. Сжатие
    uncompressed_len = len(payload)
    if compress_type == SZIP_COMPRESSION:
        compressed_payload = huffman_compress(payload)
        compressed_len = len(compressed_payload)
    else:
        compressed_payload = payload
        compressed_len = uncompressed_len

    # 2. ParcelID
    parcelid = make_parcelid(
        offset_units=offset_units,
        size_index=size_index,
        external_to_region=external_to_region,
        redundancy=redundancy,
    )

    # 3. PclHdr_t
    header = _build_pcl_header(
        parcelid=parcelid,
        payload_len=uncompressed_len,
        compressed_payload_len=compressed_len,
        region=region,
        parcel_type=parcel_type,
        parcel_desc=parcel_desc,
        compress_type=compress_type,
        data_offset=PCL_HEADER_SIZE,
    )
    
    return header + compressed_payload


# ────────────────────────────────────────────────────────────────
# Higher-level helpers
# ────────────────────────────────────────────────────────────────

def encode_strings(
    pid: int,
    strings: List[str],
    *,
    region: int = 1,
    parcel_type: int = 0,
    parcel_desc: int = 0,
    offset_units: Optional[int] = None,
    size_index: int = 0,
    external_to_region: bool = False,
    redundancy: bool = False,
    block_size: int = 4096,
    compress_type: int = NO_COMPRESSION,
) -> bytes:
    """
    Простейший strings-parcel.
    Использует безопасную транслитерацию и Latin-1.
    """
    buf = io.BytesIO()
    for s in strings:
        # Применяем безопасную транслитерацию
        safe_string = _transliterate_to_latin1(s)
        # Кодируем в однобайтовый Latin-1
        data = safe_string.encode("latin-1")
        buf.write(data)
        buf.write(b"\x00")
    payload = buf.getvalue()
    return encode_bytes(
        pid,
        payload,
        region=region,
        parcel_type=parcel_type,
        parcel_desc=parcel_desc,
        offset_units=offset_units,
        size_index=size_index,
        external_to_region=external_to_region,
        redundancy=redundancy,
        block_size=block_size,
        compress_type=compress_type,
    )


def _ntu_from_deg(deg: float) -> int:
    """
    Вспомогательное: deg -> NTU (1e5/deg) с клиппингом в 32-бит.
    """
    val = int(round(deg * NTU_PER_DEG))
    if val < -0x80000000:
        val = -0x80000000
    elif val > 0x7FFFFFFF:
        val = 0x7FFFFFFF
    return val


def encode_cartography(
    pid: int,
    records: List[Tuple[int, List[Tuple[float, float]]]],
    *,
    region: int = 1,
    parcel_type: int = 1,  # cartographic parcel
    parcel_desc: int = 0,
    offset_units: Optional[int] = None,
    size_index: int = 0,
    external_to_region: bool = False,
    redundancy: bool = False,
    block_size: int = 4096,
    rect_ntu: Optional[Tuple[int, int, int, int]] = None, # (min_lat, max_lat, min_lon, max_lon)
    compress_type: int = NO_COMPRESSION,
) -> bytes:
    """
    Простой cartographic parcel (полилинии в NTU) для XXX1.SDL.
    """
    
    if len(records) > 0xFFFF:
        raise ValueError("Too many records for CARTO parcel (max 65535)")

    # 1. Определяем DBRect
    if rect_ntu is not None:
        min_lat_ntu, max_lat_ntu, min_lon_ntu, max_lon_ntu = rect_ntu
    else:
        # Fallback 
        min_lat_ntu = max_lat_ntu = min_lon_ntu = max_lon_ntu = 0

    # DBRect_t в SDAL: (min_lon, min_lat, max_lon, max_lat)
    bbox_bytes = struct.pack(
        ">iiii",
        min_lon_ntu, 
        min_lat_ntu, 
        max_lon_ntu, 
        max_lat_ntu, 
    )

    # 2. Формируем Payload
    buf = io.BytesIO()
    buf.write(bbox_bytes)
    buf.write(struct.pack(">H", len(records)))

    for way_id, coords in records:
        buf.write(struct.pack(">I", int(way_id)))
        buf.write(struct.pack(">H", len(coords)))
        for lon, lat in coords:
            buf.write(struct.pack(">ii", _ntu_from_deg(lon), _ntu_from_deg(lat)))

    payload = buf.getvalue()
    return encode_bytes(
        pid,
        payload,
        region=region,
        parcel_type=parcel_type,
        parcel_desc=parcel_desc,
        offset_units=offset_units,
        size_index=size_index,
        external_to_region=external_to_region,
        redundancy=redundancy,
        block_size=block_size,
        compress_type=compress_type,
    )


def encode_btree(
    pid: int,
    offsets: List[Tuple[int, int]],
    *,
    region: int = 1,
    parcel_type: int = 0,
    parcel_desc: int = 0,
    offset_units: Optional[int] = None,
    size_index: int = 0,
    external_to_region: bool = False,
    redundancy: bool = False,
    block_size: int = 4096,
    compress_type: int = NO_COMPRESSION,
) -> bytes:
    """
    Простой (id, offset) table как stand-in для B+-tree index.
    """
    buf = io.BytesIO()
    buf.write(struct.pack(">I", len(offsets)))
    for ent_id, off in offsets:
        buf.write(struct.pack(">II", int(ent_id), int(off)))
    payload = buf.getvalue()
    return encode_bytes(
        pid,
        payload,
        region=region,
        parcel_type=parcel_type,
        parcel_desc=parcel_desc,
        offset_units=offset_units,
        size_index=size_index,
        external_to_region=external_to_region,
        redundancy=redundancy,
        block_size=block_size,
        compress_type=compress_type,
    )


def encode_poi_index(
    pid: int,
    offsets: List[Tuple[int, int]],
    *,
    region: int = 1,
    parcel_type: int = 0,
    parcel_desc: int = 0,
    offset_units: Optional[int] = None,
    size_index: int = 0,
    external_to_region: bool = False,
    redundancy: bool = False,
    block_size: int = 4096,
    compress_type: int = NO_COMPRESSION,
) -> bytes:
    """
    POI index как (poi_id, byte_offset) pairs (такой же layout, как encode_btree).
    """
    return encode_btree(
        pid,
        offsets,
        region=region,
        parcel_type=parcel_type,
        parcel_desc=parcel_desc,
        offset_units=offset_units,
        size_index=size_index,
        external_to_region=external_to_region,
        redundancy=redundancy,
        block_size=block_size,
        compress_type=compress_type,
    )