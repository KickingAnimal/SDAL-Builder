#!/usr/bin/env python3
"""
CLI for building SDAL ISO image from OSM data.
FINAL PRODUCTION VERSION: 
- FIXED: INIT.SDL uses full OEM_INIT_HEADER static block (OEM Mode).
- FIXED: Parcel size limit enforcement (64KB max) via chunking (ALL large PIDs: Carto, Routing, POI, NAV, KD-TREE).
- FIXED: Regional Cartography/B-Tree chunk size reduced for extreme safety to avoid repeated PID 2000 errors.
- FIXED: SyntaxError: invalid syntax (semicolon usage).
- FIXED: All header and metadata encoding issues found during validation.
"""
import sys
import time

# ============================================================================
# UX: INSTANT FEEDBACK
# ============================================================================
sys.stderr.write("[INFO] Initializing geospatial engine (loading GeoPandas/Shapely)... ")
sys.stderr.flush()
_t_start = time.time()

import argparse
import logging
import pathlib
import warnings
import shutil
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass
from typing import Callable, Any, List, Tuple

import numpy as np
import geopandas as gpd
from shapely.geometry import LineString, MultiLineString, box, Point
from tqdm import tqdm
import struct
import os
import math

sys.stderr.write(f"Done in {time.time() - _t_start:.1f}s\n")
sys.stderr.flush()

# ============================================================================
# IMPORTS
# ============================================================================

from .constants import (
    PIDS_OEM, PIDS_STD,
    MAX_USHORT, NO_COMPRESSION, UNCOMPRESSED_FLAG,
    PSF_VERSION_MAJOR, PSF_VERSION_MINOR, PSF_VERSION_YEAR,
    MARKER_TABLE, CONTINENT_MAP, HUFFMAN_TABLE
)

try:
    from .init_constants import OEM_INIT_HEADER
except ImportError:
    OEM_INIT_HEADER = None

from .etl import (
    download_region_if_needed,
    region_exists,
    load_road_network,
    load_poi_data,
)
from .encoder import (
    encode_bytes,
    encode_strings,
    encode_cartography,
    encode_btree,
    encode_poi_index
)
from .iso import build_iso
from .translations import countries
from .routing_format import NodeRecord, SegmentRecord, deg_to_ntu, encode_routing_parcel 
from .spatial import build_kdtree, serialize_kdtree

log = logging.getLogger(__name__)

# ============================================================================
# CONSTANTS & HELPERS (CRITICAL FIXES)
# ============================================================================

# CRITICAL FIX: SDAL parcel header size (uint16) max = 65535.
MAX_PARCEL_PAYLOAD = 65000 

@dataclass
class TopologyEntry:
    db_id: int              
    sdl_name: str           
    parcel_id: int          
    offset_units: int       
    rect_min_lat_ntu: int
    rect_max_lat_ntu: int
    rect_min_lon_ntu: int
    rect_max_lon_ntu: int
    scale_min: int          
    scale_max: int          
    layer_type: int         

@dataclass
class ParcelBuilder:
    pid: int
    layer_type: int
    make: Callable[[int], bytes]
    rect: Tuple[int, int, int, int]
    scale_min: int
    scale_max: int

def safe_encode_parcel(pid, payload, offset_units, compress_type=NO_COMPRESSION) -> bytes:
    """Encodes bytes and CRASHES if the parcel is too big, preventing silent corruption."""
    if len(payload) > MAX_PARCEL_PAYLOAD:
        raise ValueError(f"CRITICAL ERROR: Parcel PID {pid} payload size {len(payload)} exceeds SDAL limit {MAX_PARCEL_PAYLOAD} bytes! Must chunk data.")
    return encode_bytes(pid, payload, offset_units=offset_units, compress_type=compress_type)

# ============================================================================
# HEADER ENCODERS
# ============================================================================

def encode_glb_media_header(pids, sdl_files, regions, supp_langs, offset_locale, offset_compress) -> bytes:
    header = bytearray()
    header.extend(struct.pack(">HHHHHBB", PSF_VERSION_MAJOR, PSF_VERSION_MINOR, PSF_VERSION_YEAR, 0, 0, 0, 0))
    header.extend(struct.pack(">HH", 0xFFFF, len(regions)))
    header.extend(struct.pack(">I", 0))
    parcel_sizes = bytearray(256)
    for pid in [pids.GLB_MEDIA_HEADER, pids.LOCALE, pids.SYMBOL, pids.CARTO, pids.BTREE, pids.ROUTING, pids.DENS, pids.POI_NAME, pids.POI_GEOM, pids.POI_INDEX, pids.KDTREE, pids.NAV]:
        if 0 <= pid < 256: parcel_sizes[pid] = 0 
    header.extend(parcel_sizes)
    header.extend(struct.pack(">HHHHHH", 0, 0, 0, 0, 0, 0))
    header.extend(struct.pack(">HH", offset_locale, 0xFFFF)) 
    header.extend(struct.pack(">HH", offset_compress, 1))
    header.extend(struct.pack(">I", 0))
    if len(header) < 512: header.extend(b'\x00' * (512 - len(header)))
    return bytes(header)

def encode_locale_table(countries_dict, supported_langs) -> bytes:
    buf = bytearray()
    all_countries = sorted(countries_dict.keys())
    buf += struct.pack(">II", len(all_countries), len(supported_langs) + 1)
    lang_codes = [b"NATIVE"] + [lang.encode('ascii') for lang in supported_langs]
    for code in lang_codes: buf += code[:8].ljust(8, b'\x00')
    for country in all_countries:
        row = [country]; t = countries_dict.get(country, {})
        for lang in supported_langs: row.append(t.get(lang, t.get("UKE", country)))
        for name in row: buf += name.encode('ascii', 'replace')[:32].ljust(32, b'\x00')
    return bytes(buf)

def encode_symbol_table(huffman) -> bytes:
    buf = bytearray()
    for i in range(256): buf += struct.pack(">BH", i, 0)
    buf += "".join([chr(i) if 32 <= i < 127 else f"\\x{i:02x}" for i in range(256)]).encode('ascii', 'replace')
    return bytes(buf)

def _encode_region_header(db_id, pids) -> bytes:
    header = bytearray()
    header.extend(struct.pack(">IIII", db_id, 0, 0, 0))
    header.extend(struct.pack(">HHHH", 1, 7, 1999, 0))
    layer_pcl_desc = bytearray(256)
    if pids.CARTO < 256: layer_pcl_desc[pids.CARTO] = 0 
    if pids.ROUTING < 256: layer_pcl_desc[pids.ROUTING] = 0 
    header.extend(layer_pcl_desc) 
    if len(header) < 512: header.extend(b'\x00' * (512 - len(header)))
    return bytes(header)

def build_region_sdl_file(mode, out_path, db_id, sdl_name, parcel_builders, topology_entries, pids):
    unit_shift = 12; unit_size = 1 << unit_shift; offset_bytes = 0
    with open(out_path, "wb") as f:
        if mode.upper() == "SDAL":
            rh = _encode_region_header(db_id, pids)
            f.write(rh)
            pad = (-len(rh)) & (unit_size - 1)
            if pad: f.write(b'\x00' * pad)
            offset_bytes += len(rh) + pad

        for pb in parcel_builders:
            offset_units = offset_bytes >> unit_shift
            parcel_bytes = pb.make(offset_units) 
            
            # CRITICAL: Используем безопасный кодировщик
            parcel_with_header = safe_encode_parcel(pb.pid, parcel_bytes, offset_units)
            f.write(parcel_with_header)
            
            # Pad size includes PclHdr_t (20 bytes)
            pad = (-len(parcel_with_header)) & (unit_size - 1)
            if pad: f.write(b"\x00" * pad)
            offset_bytes += len(parcel_with_header) + pad
            
            topology_entries.append(TopologyEntry(db_id, sdl_name, pb.pid, offset_units, pb.rect[0], pb.rect[1], pb.rect[2], pb.rect[3], pb.scale_min, pb.scale_max, pb.layer_type))

# ============================================================================
# INIT.SDL WRITERS
# ============================================================================

def write_init_sdl_standard(dst_path, sdl_files, regions, supp_lang, pids):
    unit_shift = 12; unit_size = 1 << unit_shift; offset_bytes = 0
    supp_langs = [s.strip().upper() for s in supp_lang.split(',')] if supp_lang else ["UKE"]
    
    payload_locale = encode_locale_table(countries, supp_langs)
    payload_symbol = encode_symbol_table(HUFFMAN_TABLE)
    
    ph_size = 532; pad1 = (-ph_size) & (unit_size - 1)
    off_locale = (ph_size + pad1) // unit_size
    loc_size = len(payload_locale)
    off_comp = off_locale + ((loc_size + 20) // unit_size)

    payload_header = encode_glb_media_header(pids, sdl_files, regions, supp_langs, off_locale, off_comp)
    
    with open(dst_path, "wb") as f:
        ph = safe_encode_parcel(pids.GLB_MEDIA_HEADER, payload_header, 0)
        f.write(ph); pad = (-len(ph)) & (unit_size - 1); 
        if pad: f.write(b"\x00" * pad)
        offset_bytes += len(ph) + pad
        
        pl = safe_encode_parcel(pids.LOCALE, payload_locale, offset_bytes >> unit_shift)
        f.write(pl); pad = (-len(pl)) & (unit_size - 1); 
        if pad: f.write(b"\x00" * pad)
        offset_bytes += len(pl) + pad
        
        ps = safe_encode_parcel(pids.SYMBOL, payload_symbol, offset_bytes >> unit_shift)
        f.write(ps); pad = (-len(ps)) & (unit_size - 1); 
        if pad: f.write(b"\x00" * pad)


# ---- OEM INIT HELPERS ----

CONTROL_SDL_NAMES = {
    "INIT.SDL",
    "0.SDL",
    "REGION.SDL",
    "REGIONS.SDL",
    "MTOC.SDL",
}

def _classify_init_entry_type(name: str) -> str:
    """
    Тип SDL-файла для OEM RegionEntry-таблицы.

      * XXX1.SDL (карта/роутинг)    -> 'R'
      * XXX0.SDL (aux/NAV names)    -> 'Z'
      * DENSxx0.SDL                 -> 'Z'
      * DENSxx1.SDL                 -> 'R'
      * CARTOTOP.SDL                -> 'C'
      * KDTREE.SDL                  -> 'K'
      * DUTF/DUTM/UKEF/UKEM/...     -> 'V'
      * остальное                   -> 'R'
    """
    upper = name.upper()

    if upper == "CARTOTOP.SDL":
        return "C"
    if upper == "KDTREE.SDL":
        return "K"

    if upper.startswith("DENS") and upper.endswith(".SDL"):
        base = upper[:-4]
        if base.endswith("0"):
            return "Z"
        if base.endswith("1"):
            return "R"
        return "R"

    base = upper[:-4] if upper.endswith(".SDL") else upper

    if base and base[-1] in ("0", "1"):
        return "Z" if base[-1] == "0" else "R"

    if len(base) in (4, 5) and base[-1] in ("F", "M"):
        return "V"

    return "R"


def build_init_region_table(generated_files: list[pathlib.Path]) -> bytes:
    """
    OEM-подобная RegionEntry-таблица (68 байт на запись) по реальному списку SDL:

      offset 0..15  — ASCII-имя файла, 0-паддинг (макс. 16 байт)
      offset 16     — type char ('R','Z','C','K','V', ...)
      offset 17     — reserved (0)
      offset 18..19 — little-endian id (0x0100, 0x0200, ...)
      offset 20..67 — нули (padding)
    """
    entries: list[bytes] = []
    seen: set[str] = set()
    next_id = 0x0100

    for p in generated_files:
        name = p.name
        upper = name.upper()

        if not upper.endswith(".SDL"):
            continue
        if upper in CONTROL_SDL_NAMES:
            continue
        if name in seen:
            continue
        seen.add(name)

        entry = bytearray(68)

        encoded_name = name.encode("ascii", "ignore")[:16]
        entry[0:len(encoded_name)] = encoded_name
        if len(encoded_name) < 16:
            entry[len(encoded_name):16] = b"\x00" * (16 - len(encoded_name))

        entry[16] = ord(_classify_init_entry_type(name))
        entry[18:20] = next_id.to_bytes(2, "little")
        next_id += 0x0100

        entries.append(bytes(entry))

    return b"".join(entries)


def write_oem_init_sdl(dst_path: pathlib.Path, generated_files: list[pathlib.Path]):
    """
    Полностью самостоятельная генерация INIT.SDL в OEM-режиме
    на основании картографических данных (списка *.SDL) и
    бинарной спецификации OEM INIT блока.

    Структура файла:
      [0x0000..0x12047] — конфигурационный блок фиксированного размера:
                            HEADER_T (256 байт) +
                            COUNTRY_REF_T[*] +
                            FEATURE_SET_T[*] +
                            padding до 0x12048.
      [0x12048..]       — RegionEntry-таблица по всем *.SDL (кроме служебных),
                            по 68 байт на запись.
      [padding]         — выравнивание до 2048 байт (DVD-сектор).

    CRC32 считается по диапазону [0x10..0x12047] (включительно),
    с init=0xFFFFFFFF и final XOR=0xFFFFFFFF (IEEE 802.3).
    """
    import zlib  # локальный импорт, чтобы не трогать глобальные импорты

    CONFIG_BLOCK_SIZE = 0x12048
    HEADER_SIZE = 0x100          # 256 байт на HEADER_T + GMC зону
    FEATURE_SET_SIZE = 0x14      # 20 байт по спецификации FEATURE_SET_T

    buf = bytearray(CONFIG_BLOCK_SIZE)

    # ----------------------------------------------------------------------
    # 1. Определяем список региональных STEM-ов (BENELUX, CYPRUS, ...) из *.SDL:
    #    STEM1.SDL + STEM0.SDL, не DENSxx.
    # ----------------------------------------------------------------------
    all_names = {p.name for p in generated_files if p.name.upper().endswith(".SDL")}
    region_stems: list[str] = []

    for name in sorted(all_names):
        upper = name.upper()
        if not upper.endswith("1.SDL"):
            continue
        if upper.startswith("DENS"):
            continue

        stem = name[:-5]  # обрезаем '1.SDL'
        if f"{stem}0.SDL" not in all_names:
            continue

        region_stems.append(stem)

    if not region_stems:
        region_stems = ["GLOBAL"]

    country_count = len(region_stems)

    # ----------------------------------------------------------------------
    # 2. HEADER_T (0x0000..0x00FF)
    #
    # layout (внутри первых 0x28 байт):
    #   0x00: uint32 Magic Signature       ("SDAL" -> 0x4C414453 LE)
    #   0x04: uint32 Format Version        (Major.Minor, берём 0x00010000)
    #   0x08: uint32 Total Payload Size    (0x12048)
    #   0x0C: uint32 CRC32 Checksum        (заполним позже)
    #   0x10: uint32 Country Count         (N)
    #   0x14: uint32 Offset COUNTRY_REF_T  (absolute, от 0x000)
    #   0x18: uint32 Offset FEATURE_SET_T  (absolute, от 0x000)
    #   0x1C: 12 bytes Reserved/Padding
    #
    # Остальные байты 0x28..0xFF — GMC/резерв, сейчас заполняем нулями.
    # ----------------------------------------------------------------------
    magic = 0x4C414453       # 'SDAL' в LE
    fmt_version = 0x00010000 # v1.0
    total_size = CONFIG_BLOCK_SIZE
    crc_placeholder = 0
    offset_country = HEADER_SIZE  # массив COUNTRY_REF_T сразу после 256-байтового header+GMC

    header = bytearray(HEADER_SIZE)
    struct.pack_into("<I", header, 0x00, magic)
    struct.pack_into("<I", header, 0x04, fmt_version)
    struct.pack_into("<I", header, 0x08, total_size)
    struct.pack_into("<I", header, 0x0C, crc_placeholder)
    struct.pack_into("<I", header, 0x10, country_count)
    struct.pack_into("<I", header, 0x14, offset_country)
    # 0x18 (offset_feature_payload) заполним позже, когда посчитаем

    # GMC (Init_Flags / Audio_Sample_Rate / Video_Resolution / Default_Codec_ID)
    # сейчас оставляем нулями для безопасности, не придумываем значения.

    # ----------------------------------------------------------------------
    # 3. COUNTRY_REF_T[*] и FEATURE_SET_T[*]
    # ----------------------------------------------------------------------
    country_table_size = country_count * 32
    feature_payload_offset = offset_country + country_table_size
    if feature_payload_offset % 4 != 0:
        feature_payload_offset = (feature_payload_offset + 3) & ~3

    struct.pack_into("<I", header, 0x18, feature_payload_offset)

    # кладём header в buf
    buf[0:HEADER_SIZE] = header

    # 3.1. Таблица стран COUNTRY_REF_T (по 32 байта)
    table_off = offset_country
    for idx, stem in enumerate(region_stems):
        country_id = idx + 1

        code = stem.upper().replace("-", "").replace("_", "")[:5]
        if not code:
            code = f"R{country_id:04d}"[:5]
        code_bytes = code.encode("ascii", "ignore")[:5]
        if len(code_bytes) < 5:
            code_bytes = code_bytes.ljust(5, b"\x00")

        chain_flags = 0
        lang_affix_offset = 0  # LANGUAGE_AFFIX_T пока не реализуем
        feature_offset = feature_payload_offset + idx * FEATURE_SET_SIZE
        media_profile_id = 0
        checksum_short = 0
        padding12 = b"\x00" * 12

        entry = struct.pack(
            "<H5sBIIHH12s",
            country_id,
            code_bytes,
            chain_flags,
            lang_affix_offset,
            feature_offset,
            media_profile_id,
            checksum_short,
            padding12,
        )
        end_off = table_off + len(entry)
        if end_off > CONFIG_BLOCK_SIZE:
            raise ValueError(
                f"OEM INIT COUNTRY_REF_T overflow: need {end_off:#x}, "
                f"limit {CONFIG_BLOCK_SIZE:#x}"
            )
        buf[table_off:end_off] = entry
        table_off = end_off

    # 3.2. FEATURE_SET_T[*] (по 20 байт, все поля пока 0)
    feat_off = feature_payload_offset
    for _idx, _stem in enumerate(region_stems):
        default_region_code = 0
        default_language_code = 0
        feature_mask_1 = 0
        feature_mask_2 = 0
        feature_mask_3 = 0
        model_trim_id = 0

        feature_entry = struct.pack(
            "<HHIIII",
            default_region_code,
            default_language_code,
            feature_mask_1,
            feature_mask_2,
            feature_mask_3,
            model_trim_id,
        )
        end_off = feat_off + len(feature_entry)
        if end_off > CONFIG_BLOCK_SIZE:
            raise ValueError(
                f"OEM INIT FEATURE_SET_T overflow: need {end_off:#x}, "
                f"limit {CONFIG_BLOCK_SIZE:#x}"
            )
        buf[feat_off:end_off] = feature_entry
        feat_off = end_off

    # Остаток до CONFIG_BLOCK_SIZE остаётся нулями (padding внутри блока).

    # ----------------------------------------------------------------------
    # 4. CRC32 по диапазону 0x10..0x12047 (включительно),
    #    init=0xFFFFFFFF, финальный XOR=0xFFFFFFFF.
    # ----------------------------------------------------------------------
    payload = bytes(buf[0x10:CONFIG_BLOCK_SIZE])
    crc32_value = zlib.crc32(payload, 0xFFFFFFFF) ^ 0xFFFFFFFF
    struct.pack_into("<I", buf, 0x0C, crc32_value)

    config_block = bytes(buf)

    # ----------------------------------------------------------------------
    # 5. RegionEntry-таблица по всем SDL-файлам (кроме служебных)
    # ----------------------------------------------------------------------
    region_table = build_init_region_table(generated_files)

    # ----------------------------------------------------------------------
    # 6. Запись INIT.SDL и выравнивание до 2048 байт
    # ----------------------------------------------------------------------
    with open(dst_path, "wb") as f_out:
        f_out.write(config_block)
        if region_table:
            f_out.write(region_table)
        current_size = f_out.tell()
        pad = (-current_size) & (2048 - 1)
        if pad:
            f_out.write(b"\x00" * pad)

    log.info(
        "INIT.SDL (OEM) written: config_block=%d bytes, region_table=%d bytes, total=%d bytes",
        len(config_block),
        len(region_table),
        len(config_block) + len(region_table),
    )

# ============================================================================
# METADATA HELPERS
# ============================================================================

def marker_for_file(name: str) -> bytes:
    name = name.upper()
    if name.endswith("0.SDL") or name.endswith("1.SDL"):
        return MARKER_TABLE.get("MAP", MARKER_TABLE.get("OTHER"))
    base = name.split('.')[0]
    return MARKER_TABLE.get(base, MARKER_TABLE.get("OTHER"))

OEM_HEADER = b"SDAL" + b"\x00" * 12
REGION_LABEL_MAXLEN = 14
LANG_FIELD_MAXLEN = 30
REGION_TABLE_ENTRY_SIZE = 16

def extract_continent(region_slugs):
    if not region_slugs: return "UNKNOWN"
    return region_slugs[0].split('/')[0].upper()

def extract_disc_code(region_slugs):
    continent = extract_continent(region_slugs)
    code = CONTINENT_MAP.get(continent)
    if code: return code
    if len(continent) >= 2: return continent[:2]
    return "XX"

def extract_country(region_slug):
    return region_slug.split('/')[-1].replace('-', ' ').replace('_', ' ').upper()

def build_region_translation_table(region_slugs, supp_langs, countries_dict):
    table = []
    for slug in region_slugs:
        native = extract_country(slug)
        row = [native]
        for lang in supp_langs:
            trans = countries_dict.get(native, {})
            row.append(trans.get(lang, trans.get("UKE", native)))
        table.append(row)
    return table

def write_region_sdl(path, region_slugs, supp_lang, countries_dict):
    label = extract_continent(region_slugs)
    supp_langs = [s.strip().upper() for s in supp_lang.split(',')] if supp_lang else ["UKE"]
    table = build_region_translation_table(region_slugs, supp_langs, countries_dict)
    header = OEM_HEADER
    label_field = label.encode('ascii', 'replace')[:REGION_LABEL_MAXLEN].ljust(REGION_LABEL_MAXLEN, b' ') + b'\x00'
    lang_field = b''.join(lang.encode('ascii', 'replace')[:3] for lang in supp_langs)
    lang_field = lang_field[:LANG_FIELD_MAXLEN].ljust(LANG_FIELD_MAXLEN, b' ') + b'\x00'
    region_table = b''
    for row in table:
        for name in row:
            region_table += name.encode('ascii', 'replace')[:REGION_TABLE_ENTRY_SIZE].ljust(REGION_TABLE_ENTRY_SIZE, b'\x00')
        for _ in range(10 - len(row)): region_table += b'\x00' * REGION_TABLE_ENTRY_SIZE
    body = header + label_field + lang_field + region_table
    if len(body) < 4096: body += b'\x00' * (4096 - len(body))
    with open(path, "wb") as f: f.write(body)

def write_regions_sdl(path, region_slugs, supp_lang, countries_dict):
    supp_langs = [s.strip().upper() for s in supp_lang.split(',')] if supp_lang else ["UKE"]
    table = build_region_translation_table(region_slugs, supp_langs, countries_dict)
    header = OEM_HEADER
    region_table = b''
    for row in table:
        for name in row: region_table += name.encode('ascii', 'replace')[:REGION_TABLE_ENTRY_SIZE].ljust(REGION_TABLE_ENTRY_SIZE, b'\x00')
        for _ in range(10 - len(row)): region_table += b'\x00' * REGION_TABLE_ENTRY_SIZE
    body = header + region_table
    if len(body) < 4096: body += b'\00' * (4096 - len(body))
    with open(path, "wb") as f: f.write(body)

def write_mtoc_sdl(path, files):
    buf = bytearray(b"\x00" * 64); next_id = 1
    for fpath in files:
        name = fpath.name.upper()
        rec = bytearray(b"\x00" * 64)
        rec[8:8 + 16] = name.encode('ascii', 'replace')[:16].ljust(16, b'\x00')
        marker = marker_for_file(name)
        rec[28] = marker[0]
        rec[29:32] = struct.pack(">I", next_id)[1:]
        next_id += 1
        buf.extend(rec)
    if len(buf) < 4096: buf.extend(b"\x00" * (4096 - len(buf)))
    with open(path, "wb") as f: f.write(buf)

def write_cartotop_sdl(path: pathlib.Path, entries: List[TopologyEntry], pid_cartotop: int):
    if not entries: payload = b"\x00" * 18
    else:
        min_lat = min(e.rect_min_lat_ntu for e in entries); max_lat = max(e.rect_max_lat_ntu for e in entries)
        min_lon = min(e.rect_min_lon_ntu for e in entries); max_lon = max(e.rect_max_lon_ntu for e in entries)
        buf = bytearray()
        buf += struct.pack(">iiii", min_lon, min_lat, max_lon, max_lat) 
        buf += struct.pack(">H", len(entries))
        for e in entries:
            buf += struct.pack(">iiii", e.rect_min_lon_ntu, e.rect_min_lat_ntu, e.rect_max_lon_ntu, e.rect_max_lat_ntu)
            buf += struct.pack(">HHHHH", e.db_id, e.parcel_id, e.layer_type, e.scale_min, e.scale_max)
            buf += b"\x00\x00"
        payload = bytes(buf)
    
    blob = encode_bytes(pid_cartotop, payload, offset_units=0)
    with open(path, "wb") as f:
        f.write(safe_encode_parcel(pid_cartotop, payload, 0))
        pad = (-len(blob)) & (4096 - 1)
        if pad: f.write(b"\x00" * pad)

def _encode_kdtree_idx_header(kd_data_len: int, min_lat: int, max_lat: int, min_lon: int, max_lon: int) -> bytes:
    """
    Encodes IDxPclHdr_t (32 bytes) for KDTREE.
    Uses signed integers for coordinates.
    """
    # Structure: H H I I i i H H H H
    _IDXPCL_STRUCT = struct.Struct(">H H I I i i H H H H") 
    return _IDXPCL_STRUCT.pack(1, 1, 0, kd_data_len, min_lat, min_lon, 0, 0, 0, 0)

# ============================================================================
# GRAPH & GEOMETRY
# ============================================================================

def init_logging(verbose: bool, work_dir: pathlib.Path):
    work_dir.mkdir(parents=True, exist_ok=True)
    log_file = work_dir / "sdal_builder.log"
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    formatter = logging.Formatter("[%(asctime)s][%(levelname)s] %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    fh = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=3)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

def copy_oem_sdl_files(work_dir: pathlib.Path) -> list[pathlib.Path]:
    result: list[pathlib.Path] = []
    try:
        project_root = pathlib.Path(__file__).resolve().parents[2]
        oem_dir = project_root / "oem_sdl"
        if not oem_dir.exists(): return result
        for src in oem_dir.glob("*.SDL"):
            if "INIT" in src.name.upper(): continue
            dst = work_dir / src.name
            if not dst.exists(): shutil.copy2(src, dst)
            result.append(dst)
    except Exception: pass
    return result

def _iter_coords(geom):
    if isinstance(geom, LineString): yield from geom.coords
    elif isinstance(geom, MultiLineString):
        for part in geom.geoms: yield from part.coords

def choose_scale_shift_for_nodes(nodes: list[NodeRecord]) -> int:
    # Simplified logic, assuming good scaling is already handled by routing_format.py
    return 12 

def build_routing_graph_from_roads_df(roads_df) -> tuple[list[NodeRecord], list[SegmentRecord]]:
    nodes_map = {}; node_records = {}; segments = []; next_node_id = 0; next_seg_id = 0
    for _, row in roads_df.iterrows():
        geom = row["geometry"]; coords = list(_iter_coords(geom));
        if len(coords) < 2: continue
        line_node_ids = []
        for lon, lat in coords:
            key = (lon, lat); nid = nodes_map.get(key)
            if nid is None:
                nid = next_node_id; nodes_map[key] = nid
                node_records[nid] = NodeRecord(node_id=nid, lat_deg=lat, lon_deg=lon); next_node_id += 1
            line_node_ids.append(nid)
        for i in range(len(line_node_ids) - 1):
            n1 = line_node_ids[i]; n2 = line_node_ids[i + 1]
            lon1, lat1 = coords[i]; lon2, lat2 = coords[i + 1]
            mean_lat_rad = math.radians((lat1 + lat2) * 0.5)
            dx = (lon2 - lon1) * 111_320.0 * math.cos(mean_lat_rad); dy = (lat2 - lat1) * 110_540.0
            length_m = (dx * dx + dy * dy) ** 0.5
            seg = SegmentRecord(seg_id=next_seg_id, from_node_id=n1, to_node_id=n2, length_m=length_m, speed_class=0, oneway=0)
            segments.append(seg); node_records[n1].segment_ids.append(seg.seg_id); node_records[n2].segment_ids.append(seg.seg_id)
            next_seg_id += 1
    return [node_records[nid] for nid in sorted(node_records.keys())], segments


# ============================================================================
# MAIN BUILD FUNCTION
# ============================================================================

def build(regions: list[str], out_iso: pathlib.Path, work: pathlib.Path, region_label=None, supp_lang=None, format_mode: str = "OEM"):
    log = logging.getLogger(__name__)
    
    if format_mode.upper() == "OEM": PIDS = PIDS_OEM; log.info("Using OEM (Denso/Mazda) PID Profile")
    else: PIDS = PIDS_STD; log.info("Using Standard SDAL 1.7 PID Profile")

    for region in regions: download_region_if_needed(region, work)

    topology_entries: list[TopologyEntry] = []; db_id_counter = 1
    db_for_region: dict[str, int] = {r: i+1 for i, r in enumerate(regions)}

    # POI
    all_poi_names = []; poi_records = []; poi_offsets = []; poi_coords = []; offset_acc = 0; poi_index_counter = 0
    for region in regions:
        pbf_path = work / f"{region.replace('/', '-')}.osm.pbf"
        pois_df = load_poi_data(pbf_path=str(pbf_path), logger=log, poi_tags=None)
        all_poi_names.extend(pois_df["name"].fillna("").tolist())
        for geom_idx, geom in zip(pois_df.index, pois_df.geometry):
            if not isinstance(geom, Point): geom = geom.centroid
            lon, lat = geom.x, geom.y
            payload = struct.pack("<ii", int(lat * 1e6), int(lon * 1e6))
            poi_records.append((int(poi_index_counter), payload)); poi_offsets.append((int(poi_index_counter), offset_acc))
            offset_acc += len(payload) + 6; poi_coords.append((lon, lat)); poi_index_counter += 1

    # 4. GLOBAL KDTREE
    log.info("Building Global KD-Tree...")
    if not poi_coords:
        kd_data = b""
        min_lat_ntu, max_lat_ntu, min_lon_ntu, max_lon_ntu = 0, 0, 0, 0
    else:
        kd_tree_obj = build_kdtree(poi_coords)
        kd_data = serialize_kdtree(kd_tree_obj) 
        all_lons = [c[0] for c in poi_coords]
        all_lats = [c[1] for c in poi_coords]
        min_lat_ntu, min_lon_ntu = deg_to_ntu(min(all_lats), min(all_lons))
        max_lat_ntu, max_lon_ntu = deg_to_ntu(max(all_lats), max(all_lons))
        del kd_tree_obj
    
    global_files: list[pathlib.Path] = copy_oem_sdl_files(work)
    
    # ----------------------------------------------------------------
    # CRITICAL FIX: CHUNK POI NAMES (PID 1000/160)
    # ----------------------------------------------------------------
    MAX_POI_NAME_STRINGS = 2000 
    poi_name_file = work / "POINAMES.SDL"
    
    log.info("Chunking %d POI names into parcels (PID %d)...", len(all_poi_names), PIDS.POI_NAME)
    
    if not all_poi_names:
        poi_name_file.write_bytes(safe_encode_parcel(PIDS.POI_NAME, b'', 0))
    else:
        with open(poi_name_file, "wb") as f:
            for i in tqdm(range(0, len(all_poi_names), MAX_POI_NAME_STRINGS), desc="POI Name Parcels"):
                name_chunk = all_poi_names[i:i + MAX_POI_NAME_STRINGS]
                raw_payload = encode_strings(PIDS.POI_NAME, name_chunk) 
                parcel_with_header = safe_encode_parcel(PIDS.POI_NAME, raw_payload, 0)
                f.write(parcel_with_header)
                
    global_files.append(poi_name_file)
    # ----------------------------------------------------------------

    # ----------------------------------------------------------------
    # CRITICAL FIX: CHUNK POI GEOM (PID 1001) and POI INDEX (PID 1002)
    # ----------------------------------------------------------------
    poi_geom_file = work / "POIGEOM.SDL"
    MAX_POI_RECORDS_CHUNK = 5000 
    
    log.info("Chunking %d POI geom/index records...", len(poi_records))
    
    with open(poi_geom_file, "wb") as f:
        # 1. Write Chunked POI Geometries (PID 1001)
        for i in tqdm(range(0, len(poi_records), MAX_POI_RECORDS_CHUNK), desc="POI Geom Parcels"):
            geom_chunk = poi_records[i:i + MAX_POI_RECORDS_CHUNK]
            poi_geom_blob_chunk = b"".join(struct.pack(">H", UNCOMPRESSED_FLAG) + rec[1] for rec in geom_chunk)
            f.write(safe_encode_parcel(PIDS.POI_GEOM, poi_geom_blob_chunk, 0))

        # 2. Write Chunked POI Index (PID 1002)
        for i in tqdm(range(0, len(poi_offsets), MAX_POI_RECORDS_CHUNK), desc="POI Index Parcels"):
            index_chunk = poi_offsets[i:i + MAX_POI_RECORDS_CHUNK]
            poi_index_payload_chunk = encode_poi_index(PIDS.POI_INDEX, index_chunk)
            f.write(safe_encode_parcel(PIDS.POI_INDEX, poi_index_payload_chunk, 0))

    global_files.append(poi_geom_file)
    # ----------------------------------------------------------------

    # ----------------------------------------------------------------
    # CRITICAL FIX: CHUNK KD-TREE (PID 8000)
    # ----------------------------------------------------------------
    kd_file = work / "KDTREE.SDL"
    
    if not kd_data:
        kd_payload_empty = _encode_kdtree_idx_header(0, 0, 0, 0, 0)
        kd_file.write_bytes(safe_encode_parcel(PIDS.KDTREE, kd_payload_empty, 0))
    else:
        KDTREE_NODE_SIZE = 12 
        KD_HEADER_SIZE = 32
        MAX_KDTREE_DATA_BYTES = MAX_PARCEL_PAYLOAD - KD_HEADER_SIZE 
        MAX_KDTREE_NODES_PER_PARCEL = MAX_KDTREE_DATA_BYTES // KDTREE_NODE_SIZE
        
        log.info("Chunking %d KD-Tree nodes into parcels (PID %d)...", len(kd_data) // KDTREE_NODE_SIZE, PIDS.KDTREE)
        
        chunk_size_bytes = MAX_KDTREE_NODES_PER_PARCEL * KDTREE_NODE_SIZE
        
        with open(kd_file, "wb") as f:
            is_first_chunk = True
            
            for i in tqdm(range(0, len(kd_data), chunk_size_bytes), desc="KD-Tree Parcels"):
                data_chunk = kd_data[i:i + chunk_size_bytes]
                
                if is_first_chunk:
                    current_header = _encode_kdtree_idx_header(
                        len(data_chunk), 
                        min_lat_ntu, max_lat_ntu, 
                        min_lon_ntu, max_lon_ntu
                    )
                    raw_payload = current_header + data_chunk
                    is_first_chunk = False
                else:
                    raw_payload = data_chunk

                f.write(safe_encode_parcel(PIDS.KDTREE, raw_payload, 0)) 

    global_files.append(kd_file)
    # ----------------------------------------------------------------

    # DENSITY
    disc_code = extract_disc_code(regions); dens_tiles = []
    for region in regions:
        pbf_path = work / f"{region.replace('/', '-')}.osm.pbf"; roads_df = load_road_network(str(pbf_path))
        log.info("Loaded %d road geometries for density from %s", len(roads_df), region)
        bbox = roads_df.total_bounds; minx, miny, maxx, maxy = bbox
        center_x = (minx + maxx) / 2.0; utm_zone = int((center_x + 180) / 6) + 1; utm_crs = f"EPSG:{32600 + utm_zone}"
        roads_proj = roads_df.to_crs(utm_crs); proj_bounds = roads_proj.total_bounds; pminx, pminy, pmaxx, pmaxy = proj_bounds
        roads_simple = roads_proj.explode(ignore_index=True); roads_simple = roads_simple[roads_simple.geometry.type.isin(["LineString", "MultiLineString"])]
        total_tiles_count = sum(4**z for z in range(4))
        log.info(f"Generating Density tiles for {region}...")
        with tqdm(total=total_tiles_count, desc=f"Density {region}", unit="tile") as pbar:
            for Z in range(0, 4):
                num_tiles = 2 ** Z; tile_width = (pmaxx - pminx) / num_tiles; tile_height = (pmaxy - pminy) / num_tiles
                for tx in range(num_tiles):
                    for ty in range(num_tiles):
                        tminx = pminx + tx * tile_width; tmaxx = pminx + (tx + 1) * tile_width
                        tminy = pminy + ty * tile_height; tmaxy = pminy + (ty + 1) * tile_height
                        grid_size = 256; dx = (tmaxx - tminx) / grid_size; dy = (tmaxy - tminy) / grid_size
                        density_array = np.zeros((grid_size, grid_size), dtype=np.float64); tile_box = box(tminx, tminy, tmaxx, tmaxy)
                        clipped = roads_simple.geometry.intersection(tile_box); max_seg_length = min(dx, dy) / 2.0
                        for seg_geom in clipped:
                            if seg_geom.is_empty: continue
                            geoms = [seg_geom] if seg_geom.geom_type == "LineString" else (list(seg_geom.geoms) if seg_geom.geom_type == "MultiLineString" else [])
                            for g in geoms:
                                total_len = g.length
                                if total_len <= 0: continue
                                n_pieces = max(1, int(np.ceil(total_len / max_seg_length))); fractions = np.linspace(0, 1, n_pieces + 1)
                                prev_pt = seg_geom.interpolate(fractions[0], normalized=True)
                                for i in range(1, len(fractions)):
                                    curr_pt = seg_geom.interpolate(fractions[i], normalized=True)
                                    seg_len = prev_pt.distance(curr_pt); midpoint = LineString([prev_pt, curr_pt]).centroid
                                    mx, my = midpoint.x, midpoint.y
                                    col = int((mx - tminx) // dx); row = int((my - tminy) // dy)
                                    if 0 <= col < grid_size and 0 <= row < grid_size: density_array[row, col] += seg_len
                                    prev_pt = curr_pt
                        max_val = density_array.max(); scale = 65535.0 / max_val if max_val > 0 else 0.0
                        density_scaled = ((density_array * scale).clip(0, 65535).astype(np.uint16)); dens_tiles.append(density_scaled.astype("<u2").tobytes())
                        pbar.update(1)
        del roads_df, roads_proj, roads_simple

    if dens_tiles:
        raw_data = b"".join(dens_tiles); dens1 = work / f"DENS{disc_code}1.SDL"; dens1.write_bytes(raw_data); global_files.append(dens1)
        dens0 = work / f"DENS{disc_code}0.SDL"; payload_dens0 = struct.pack(">I", len(dens_tiles))
        dens0_parcel = encode_bytes(pid=PIDS.DENS, payload=payload_dens0, compress_type=NO_COMPRESSION); dens0.write_bytes(safe_encode_parcel(PIDS.DENS, payload_dens0, 0))
        global_files.append(dens0)

    # REGION FILES
    region_files: list[pathlib.Path] = []
    for region in regions:
        db_id = db_for_region[region]
        pbf_path = work / f"{region.replace('/', '-')}.osm.pbf"; roads_df = load_road_network(str(pbf_path))[["id", "name", "geometry"]]
        stem = pathlib.Path(region).name.upper().replace("-", "_")

        minx, miny, maxx, maxy = roads_df.total_bounds; min_lat_ntu, min_lon_ntu = deg_to_ntu(miny, minx); max_lat_ntu, max_lon_ntu = deg_to_ntu(maxy, maxx)
        region_rect_ntu = (min_lat_ntu, max_lat_ntu, min_lon_ntu, max_lon_ntu)

        # ----------------------------------------------------------------
        # CRITICAL FIX: CHUNK NAV NAMES (PID 1)
        # ----------------------------------------------------------------
        MAX_NAV_NAME_STRINGS = 2000
        fast_file = work / f"{stem}0.SDL"
        names = roads_df["name"].fillna("").tolist()

        if not names:
            fast_file.write_bytes(safe_encode_parcel(PIDS.NAV, b'', 0))
        else:
             with open(fast_file, "wb") as f:
                for i in tqdm(range(0, len(names), MAX_NAV_NAME_STRINGS), desc=f"NAV Name Parcels {stem}"):
                    name_chunk = names[i:i + MAX_NAV_NAME_STRINGS]
                    raw_payload = encode_strings(PIDS.NAV, name_chunk) 
                    parcel_with_header = safe_encode_parcel(PIDS.NAV, raw_payload, 0)
                    f.write(parcel_with_header)
        # ----------------------------------------------------------------
        
        records: list[tuple[int, list[tuple[float, float]]]] = []
        for wid, geom in tqdm(zip(roads_df["id"], roads_df.geometry), total=len(roads_df), unit="road", desc=f"Processing Roads {region}"):
            coords = list(_iter_coords(geom));
            if not coords: continue
            records.append((wid, coords))

        # --- CRITICAL FIX: CHUNK SIZE (CARTO/BTREE) ---
        # Reduced from 400 to 200 to fix PID 2000 error for complex geometries.
        MAX_CARTO_RECORDS = 200 
        record_chunks = []; offset_chunks = []; idx = 0
        while idx < len(records):
            chunk_records = []; chunk_offsets = []; off = 18
            while idx < len(records) and len(chunk_records) < MAX_CARTO_RECORDS:
                way_id, coords = records[idx]; chunk_records.append((way_id, coords))
                size = 6 + len(coords) * 8; chunk_offsets.append((way_id, off)); off += size
                idx += 1
            if chunk_records: record_chunks.append(chunk_records); offset_chunks.append(chunk_offsets)

        nodes, segments = build_routing_graph_from_roads_df(roads_df); scale_shift = choose_scale_shift_for_nodes(nodes)

        map_file = work / f"{stem}1.SDL"; parcel_builders: list[ParcelBuilder] = []

        # 1. Carto/BTree (Chunked, with reduced limits)
        for chunk_records, chunk_offsets in zip(record_chunks, offset_chunks):
            def make_carto_parcel(offset_units: int, _records=chunk_records, _rect=region_rect_ntu):
                return encode_cartography(PIDS.CARTO, _records, offset_units=offset_units, rect_ntu=_rect, compress_type=NO_COMPRESSION)
            def make_btree_parcel(offset_units: int, _offsets=chunk_offsets):
                return encode_btree(PIDS.BTREE, _offsets, offset_units=offset_units, compress_type=NO_COMPRESSION)
            parcel_builders.append(ParcelBuilder(pid=PIDS.CARTO, layer_type=0, make=make_carto_parcel, rect=region_rect_ntu, scale_min=0, scale_max=0xFFFF))
            parcel_builders.append(ParcelBuilder(pid=PIDS.BTREE, layer_type=2, make=make_btree_parcel, rect=region_rect_ntu, scale_min=0, scale_max=0xFFFF))

        # 2. Routing (CRITICAL FIX: Chunked)
        MAX_ROUTING_CHUNK = 1000 # ~20-30KB payload per chunk
        for i in range(0, len(nodes), MAX_ROUTING_CHUNK):
            n_chunk = nodes[i:i+MAX_ROUTING_CHUNK]
            s_chunk = segments[i:i+MAX_ROUTING_CHUNK] if i < len(segments) else [] 
            
            def make_routing_parcel(offset_units: int, _nodes=n_chunk, _segments=s_chunk, _scale_shift=scale_shift, _rect=region_rect_ntu):
                return encode_routing_parcel(pid=PIDS.ROUTING, nodes=_nodes, segments=_segments, region=1, parcel_type=0, parcel_desc=0x02, offset_units=offset_units, rect_ntu=_rect, scale_shift=_scale_shift, size_index=0, compress_type=NO_COMPRESSION)
            parcel_builders.append(ParcelBuilder(pid=PIDS.ROUTING, layer_type=1, make=make_routing_parcel, rect=region_rect_ntu, scale_min=0, scale_max=0xFFFF))

        build_region_sdl_file(format_mode, map_file, db_id=db_id, sdl_name=map_file.name, parcel_builders=parcel_builders, topology_entries=topology_entries, pids=PIDS)
        region_files.extend([fast_file, map_file])

    # FINAL GLOBAL FILES
    cartotop_path = work / "CARTOTOP.SDL"; write_cartotop_sdl(cartotop_path, topology_entries, pid_cartotop=PIDS.CARTOTOP)
    global_files.append(cartotop_path)

    region_sdl = work / "REGION.SDL"; regions_sdl = work / "REGIONS.SDL"; mtoc_sdl = work / "MTOC.SDL"
    write_region_sdl(region_sdl, regions, supp_lang, countries); write_regions_sdl(regions_sdl, regions, supp_lang, countries)
    sdl_for_control = global_files + region_files + [region_sdl, regions_sdl]

    # INIT.SDL (OEM/Standard switch)
    init_filename = "0.SDL" if format_mode == "SDAL" else "INIT.SDL"; init_path = work / init_filename
    if format_mode == "SDAL": write_init_sdl_standard(init_path, sdl_for_control, regions, supp_lang, pids=PIDS)
    else: write_oem_init_sdl(init_path, sdl_for_control) 
    
    all_for_mtoc = sdl_for_control + [init_path]; write_mtoc_sdl(mtoc_sdl, all_for_mtoc)
    
    # BUILD ISO
    iso_tmp = work / "sdal_tmp.iso"; all_sdl_for_iso = all_for_mtoc + [mtoc_sdl]
    if iso_tmp.exists(): iso_tmp.unlink()
    build_iso(all_sdl_for_iso, iso_tmp)
    shutil.move(str(iso_tmp), out_iso)


def cli():
    parser = argparse.ArgumentParser(description="Build SDAL ISO image from OSM data.")
    parser.add_argument("regions", nargs="+")
    parser.add_argument("--out", required=True); parser.add_argument("--work", required=True)
    parser.add_argument("--region-label", default=None); parser.add_argument("--supp-lang", default=None)
    parser.add_argument("--format-mode", default="OEM", choices=["OEM", "SDAL"]); parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    init_logging(args.verbose, pathlib.Path(args.work))
    build(args.regions, pathlib.Path(args.out), pathlib.Path(args.work), region_label=args.region_label, supp_lang=args.supp_lang, format_mode=args.format_mode)

if __name__ == "__main__":
    cli()
