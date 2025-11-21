# src/sdal_builder/main.py
#!/usr/bin/env python3
"""
CLI for building SDAL ISO image from OSM data.
"""
import argparse
import logging
import pathlib
import sys
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

# Импортируем все необходимые модули и константы
from .constants import *
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
    encode_poi_index,
    # szip_compress удален
    MAX_USHORT,
    PCL_HEADER_SIZE
)
from .iso import build_iso
from .translations import countries
from .routing_format import NodeRecord, SegmentRecord, deg_to_ntu, encode_routing_parcel 
from .spatial import build_kdtree, serialize_kdtree

log = logging.getLogger(__name__)

# ────────────────────────────────────────────────────────────────
# CARTOTOP / Topology Structures
# ────────────────────────────────────────────────────────────────

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
    compress: int = NO_COMPRESSION 

# ────────────────────────────────────────────────────────────────
# Global Header Encoding (GlbMediaHeader_t, CompressInfo_t)
# ────────────────────────────────────────────────────────────────

def encode_compress_info() -> bytes:
    """
    Encodes CompressInfo_t.
    """
    # usCompressInfoCount (H) = 1, usCompressInfoReserved (H) = 0
    # Структура: I H I B B (8+2+4+1+1) = 16 байт
    element = struct.pack(">IHIBB", 0, 0, 0, 0, 0)
    
    return struct.pack(">HH", 1, 0) + element


def encode_glb_media_header(
    sdl_files: list[pathlib.Path], 
    regions: list[str], 
    supp_langs: list[str],
    offset_locale: int, 
    offset_compress: int
) -> bytes:
    """
    Encodes GlbMediaHeader_t (PID 19).
    Структура 512 байт, включая все необходимые поля Global Media Index.
    """
    
    header = bytearray()
    
    # 1. PSF Version/IDs (H H H H H B B) [12 bytes]
    header.extend(struct.pack(">HHHHHBB", PSF_VERSION_MAJOR, PSF_VERSION_MINOR, PSF_VERSION_YEAR, 0, 0, 0, 0))
    
    # usMaxPclCount (H), usMaxRegions (H) [4 bytes]
    header.extend(struct.pack(">HH", 0xFFFF, len(regions)))
    
    # ulMapIDTblOffset (I) [4 bytes]
    header.extend(struct.pack(">I", 0))
    
    # 2. ucaParcelSizes[256] (256 bytes)
    parcel_sizes = bytearray(256)
    # List of PIDs we generate, all using size index 0
    generated_pids = [
        GLB_MEDIA_HEADER_PID, LOCALE_PARCEL_ID, SYMBOL_PARCEL_ID, 
        CARTO_PARCEL_ID, BTREE_PARCEL_ID, ROUTING_PARCEL_ID, 
        DENS_PARCEL_ID, POI_NAME_PARCEL_ID, POI_GEOM_PARCEL_ID, 
        POI_INDEX_PARCEL_ID, GLB_KD_TREE_PID, NAV_PARCEL_ID
    ]
    
    for pid in generated_pids:
        if 0 <= pid < 256:
            parcel_sizes[pid] = 0 # Size Index 0 for 4096 byte unit
            
    header.extend(parcel_sizes)
    
    # 3. Global Media Index Pointers (H*6 + H*2 + H*2 + I) 
    
    # Metadata Index Pointers (6 x H, 12 bytes)
    header.extend(struct.pack(">HHHHHH", 
                             0, # usMetadataLevelTableOffset (Placeholder 0)
                             0, # usMetadataLevelTableCount (Placeholder 0)
                             0, # usChainIDIndexOffset (Placeholder 0)
                             0, # usChainIDParcelCount (Placeholder 0)
                             0, # usFeatTypeIndexOffset (Placeholder 0)
                             0  # usFeatTypeIndexLen (Placeholder 0)
                             ))
    
    # Locale Index (4 bytes)
    header.extend(struct.pack(">HH", offset_locale, 0xFFFF)) 
    
    # Compress Info Index (4 bytes)
    header.extend(struct.pack(">HH", offset_compress, 1))
    
    # ulFileSize (I) [4 bytes]
    header.extend(struct.pack(">I", 0))
    
    # Padding to 512 bytes
    if len(header) < 512:
         header.extend(b'\x00' * (512 - len(header)))

    return bytes(header)


def encode_locale_table(countries_dict: dict, supported_langs: list[str]) -> bytes:
    """
    Кодирует словарь стран в бинарный формат для LOCALE_PARCEL.
    """
    buf = bytearray()
    
    all_countries = sorted(countries_dict.keys())
    # Header: ulCountryCount (I), ulLangCount (I)
    buf += struct.pack(">II", len(all_countries), len(supported_langs) + 1)
    
    lang_codes = [b"NATIVE"] + [lang.encode('ascii') for lang in supported_langs]
    for code in lang_codes:
        buf += code[:8].ljust(8, b'\x00')
    
    ENTRY_SIZE = 32
    for country_native_name in all_countries:
        row = [country_native_name]
        translations = countries_dict.get(country_native_name, {})
        for lang in supported_langs:
            translated_name = translations.get(lang, translations.get("UKE", country_native_name))
            row.append(translated_name)

        for name in row:
            buf += name.encode('ascii', 'replace')[:ENTRY_SIZE].ljust(ENTRY_SIZE, b'\x00')
            
    return bytes(buf)


def encode_symbol_table(huffman_table: dict) -> bytes:
    """
    Кодирует таблицу Huffman-кодов (stubbed).
    """
    buf = bytearray()
    
    # Упрощенная заглушка для Symbol Table (PID 101) - 256 * 3 + 256 байт
    for i in range(256):
        code_len = 0 # Length 0
        buf += struct.pack(">BH", i, code_len)

    symbols = "".join([chr(i) if 32 <= i < 127 else f"\\x{i:02x}" for i in range(256)])
    buf += symbols.encode('ascii', 'replace')
    
    return bytes(buf)


# ────────────────────────────────────────────────────────────────
# Region Header (RgnHdr_t) for SDAL mode
# ────────────────────────────────────────────────────────────────

_RGN_HDR_SIZE = 512 

def _encode_region_header(db_id: int) -> bytes:
    """
    Encodes a minimal, structurally correct 512-byte Region Header (RgnHdr_t).
    """
    header = bytearray()
    
    # 1. RgnHdr_t core fields (4 * I + 4 * H) [24 bytes]
    # ulDbId (I), ulReserved (I), ulRegionOffsetUnits (I), ulRegionLengthUnits (I)
    header.extend(struct.pack(">IIII", db_id, 0, 0, 0))
    
    # usRgnHdrMajorVer, usRgnHdrMinorVer, usRgnHdrYear, usReserved (4 x H)
    header.extend(struct.pack(">HHHH", 1, 7, 1999, 0))
    
    # 2. ucLayerPclDesc[256] (Layer Parcel Descriptor Table) (256 bytes)
    layer_pcl_desc = bytearray(256)
    # Layer 0 (Carto) and Layer 1 (Routing) use size index 0 by convention
    if CARTO_PARCEL_ID < 256: layer_pcl_desc[CARTO_PARCEL_ID] = 0 
    if ROUTING_PARCEL_ID < 256: layer_pcl_desc[ROUTING_PARCEL_ID] = 0 
    header.extend(layer_pcl_desc) # 256 bytes
    
    # 3. Padding/Reserved (Remaining bytes to 512)
    if len(header) < _RGN_HDR_SIZE:
        header.extend(b'\x00' * (_RGN_HDR_SIZE - len(header)))
        
    return bytes(header)


# ────────────────────────────────────────────────────────────────
# Region File Building Functions (Switchable)
# ────────────────────────────────────────────────────────────────

def build_region_sdl_file_sdal(out_path: pathlib.Path, 
                               db_id: int, 
                               sdl_name: str, 
                               parcel_builders: List[ParcelBuilder],
                               topology_entries: List[TopologyEntry]):
    """
    Builds a regional .SDL file in SDAL mode: RgnHdr_t (512B + padding) + Parcels.
    """
    unit_shift = 12
    unit_size = 1 << unit_shift
    
    # 1. Write RgnHdr_t (512 bytes)
    region_header = _encode_region_header(db_id)
    
    region_header_size = len(region_header)
    # RgnHdr_t is written and padded to the next unit boundary (4096 bytes)
    pad_to_unit = (-region_header_size) & (unit_size - 1)
    
    # Offset of the first actual parcel payload (in bytes)
    offset_bytes = region_header_size + pad_to_unit
    
    with open(out_path, "wb") as f:
        f.write(region_header)
        if pad_to_unit:
            f.write(b'\x00' * pad_to_unit)

        # 2. Write Data Parcels (PclHdr_t + Payload)
        for pb in parcel_builders:
            # Offset units calculation respects the header/padding at the start of the file
            offset_units = offset_bytes >> unit_shift
            
            parcel_bytes = pb.make(offset_units) 
            
            f.write(parcel_bytes)
            pad = (-len(parcel_bytes)) & (unit_size - 1)
            if pad:
                f.write(b"\x00" * pad)
            offset_bytes += len(parcel_bytes) + pad
            
            topology_entries.append(
                # Topology Entry's offset is based on the whole file, including RgnHdr_t.
                TopologyEntry(
                    db_id=db_id,
                    sdl_name=sdl_name,
                    parcel_id=pb.pid,
                    offset_units=offset_units,
                    rect_min_lat_ntu=pb.rect[0],
                    rect_max_lat_ntu=pb.rect[1],
                    rect_min_lon_ntu=pb.rect[2],
                    rect_max_lon_ntu=pb.rect[3],
                    scale_min=pb.scale_min,
                    scale_max=pb.scale_max,
                    layer_type=pb.layer_type,
                )
            )

def build_region_sdl_file_oem(out_path: pathlib.Path, 
                               db_id: int, 
                               sdl_name: str, 
                               parcel_builders: List[ParcelBuilder],
                               topology_entries: List[TopologyEntry]):
    """
    Builds a regional .SDL file in OEM mode (current version): 
    Parcels start immediately at the file beginning (Offset 0).
    """
    unit_shift = 12
    unit_size = 1 << unit_shift
    offset_bytes = 0
    
    with open(out_path, "wb") as f:
        for pb in parcel_builders:
            offset_units = offset_bytes >> unit_shift
            
            parcel_bytes = pb.make(offset_units)
            
            f.write(parcel_bytes)
            pad = (-len(parcel_bytes)) & (unit_size - 1)
            if pad:
                f.write(b"\x00" * pad)
            offset_bytes += len(parcel_bytes) + pad
            
            topology_entries.append(
                TopologyEntry(
                    db_id=db_id,
                    sdl_name=sdl_name,
                    parcel_id=pb.pid,
                    offset_units=offset_units,
                    rect_min_lat_ntu=pb.rect[0],
                    rect_max_lat_ntu=pb.rect[1],
                    rect_min_lon_ntu=pb.rect[2],
                    rect_max_lon_ntu=pb.rect[3],
                    scale_min=pb.scale_min,
                    scale_max=pb.scale_max,
                    layer_type=pb.layer_type,
                )
            )


def build_region_sdl_file(mode: str, *args, **kwargs):
    """Router function based on format mode."""
    if mode.upper() == "SDAL":
        return build_region_sdl_file_sdal(*args, **kwargs)
    else: # OEM is default
        return build_region_sdl_file_oem(*args, **kwargs)
        
# ────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────

def write_init_sdl(dst_path: pathlib.Path, sdl_files: list[pathlib.Path], regions: list[str], supp_lang: str | None):
    """
    Generate INIT.SDL compliant with SDAL 1.7 Global Media File layout.
    """
    unit_shift = 12
    unit_size = 1 << unit_shift
    offset_bytes = 0
    
    if not supp_lang:
        supp_langs = ["UKE"]
    else:
        supp_langs = [s.strip().upper() for s in supp_lang.split(',')]

    
    # 1. PID 100 Parcel (Locale/Translation Table)
    payload_locale = encode_locale_table(countries, supp_langs)
    
    # 2. PID 101 Parcel (Symbol Table)
    payload_symbol = encode_symbol_table(HUFFMAN_TABLE)
    
    # 3. CompressInfo_t (Встроенный в GlbMediaHeader Index)
    payload_compress = encode_compress_info()
    
    # --- Предварительная сборка для получения смещений ---
    # Parcel Header size is 20 bytes. Payload size is 512 bytes. Total 532 bytes.
    # The first parcel (PID 19) is at offset 0 (0 units).
    
    # Parcel 19 size is 20 (header) + 512 (payload) = 532 bytes. Padded to unit_size (4096)
    parcel_header_size = 532
    pad1 = (-parcel_header_size) & (unit_size - 1)
    
    # Rough starting byte offset for PID 100 (Locale)
    offset_locale_bytes_rough = parcel_header_size + pad1
    offset_locale_units = offset_locale_bytes_rough // unit_size
    
    locale_parcel_size_rough = len(encode_bytes(LOCALE_PARCEL_ID, payload_locale, compress_type=NO_COMPRESSION, offset_units=offset_locale_units))
    
    # Rough starting byte offset for PID 101 (Symbol)
    offset_symbol_bytes_rough = offset_locale_bytes_rough + locale_parcel_size_rough
    # CompressInfo is conceptually located after PID 100/101, but the pointer 
    # refers to the payload (which is static payload_compress).
    
    # We rely on the initial parcel offset of 0 (0 units) for PID 19.
    offset_compress_units = offset_locale_units + (locale_parcel_size_rough // unit_size)

    # --- 1. PID 19 (GlbMediaHeader) ---
    # Pointers inside GlbMediaHeader_t refer to offsets in units relative to the start of the file.
    payload_header = encode_glb_media_header(
        sdl_files, regions, supp_langs, 
        # Locales starts at offset_locale_units (Parcel 100)
        offset_locale=offset_locale_units, 
        # CompressInfo starts after PID 100/101 (This is the next parcel offset)
        offset_compress=offset_compress_units
    )
    
    parcel_header = encode_bytes(
        GLB_MEDIA_HEADER_PID, payload_header, offset_units=0, region=0, 
        parcel_type=0, parcel_desc=0, compress_type=NO_COMPRESSION, size_index=0
    )
    
    
    with open(dst_path, "wb") as f:
        # 1. PID 19 (GlbMediaHeader) - Offset 0
        f.write(parcel_header)
        pad = (-len(parcel_header)) & (unit_size - 1)
        if pad: f.write(b"\x00" * pad)
        offset_bytes += len(parcel_header) + pad
        
        # 2. PID 100 (Locale Table)
        parcel_locale = encode_bytes(
            LOCALE_PARCEL_ID, payload_locale, offset_units=(offset_bytes >> unit_shift), region=0, 
            parcel_type=0, parcel_desc=0, compress_type=NO_COMPRESSION, size_index=0
        )
        f.write(parcel_locale)
        pad = (-len(parcel_locale)) & (unit_size - 1)
        if pad: f.write(b"\x00" * pad)
        offset_bytes += len(parcel_locale) + pad
        
        # 3. PID 101 (Symbol Table)
        parcel_symbol = encode_bytes(
            SYMBOL_PARCEL_ID, payload_symbol, offset_units=(offset_bytes >> unit_shift), region=0, 
            parcel_type=0, parcel_desc=0, compress_type=NO_COMPRESSION, size_index=0
        )
        f.write(parcel_symbol)
        pad = (-len(parcel_symbol)) & (unit_size - 1)
        if pad: f.write(b"\x00" * pad)


def marker_for_file(name: str) -> bytes:
    """Decide OEM-style marker byte per file name."""
    name = name.upper()
    if name.endswith("0.SDL") or name.endswith("1.SDL"):
        return MARKER_TABLE.get("MAP", MARKER_TABLE.get("OTHER"))
    return MARKER_TABLE.get(name.split('.')[0], MARKER_TABLE.get("OTHER"))


OEM_HEADER = b"SDAL" + b"\x00" * 12
REGION_LABEL_MAXLEN = 14
LANG_FIELD_MAXLEN = 30
REGION_TABLE_ENTRY_SIZE = 16


def extract_continent(region_slugs):
    if not region_slugs:
        return "UNKNOWN"
    return region_slugs[0].split('/')[0].upper()


def extract_disc_code(region_slugs):
    """Return disc-level code (for DENSxx naming) from region slugs."""
    continent = extract_continent(region_slugs)
    
    code = CONTINENT_MAP.get(continent)
    if code:
        return code
    
    if len(continent) >= 2:
        return continent[:2]
    return "XX"


def extract_country(region_slug):
    return region_slug.split('/')[-1].replace('-', ' ').replace('_', ' ').upper()


def build_region_translation_table(region_slugs, supp_langs, countries_dict):
    table = []
    for slug in region_slugs:
        native = extract_country(slug)
        row = [native]
        for lang in supp_langs:
            row.append(
                countries_dict.get(native, {}).get(lang, countries_dict.get(native, {}).get("UKE", native))
            )
        table.append(row)
    return table


def write_region_sdl(path, region_slugs, supp_lang, countries_dict):
    """Write REGION.SDL in full OEM style. (Kept OEM as per request)"""
    label = extract_continent(region_slugs)
    if not supp_lang:
        supp_langs = ["UKE"]
    else:
        supp_langs = [s.strip().upper() for s in supp_lang.split(',')]

    table = build_region_translation_table(region_slugs, supp_langs, countries_dict)

    header = OEM_HEADER
    label_field = label.encode('ascii', 'replace')[:REGION_LABEL_MAXLEN].ljust(
        REGION_LABEL_MAXLEN, b' '
    ) + b'\x00'
    lang_field = b''.join(lang.encode('ascii', 'replace')[:3] for lang in supp_langs)
    lang_field = lang_field[:LANG_FIELD_MAXLEN].ljust(LANG_FIELD_MAXLEN, b' ') + b'\x00'

    region_table = b''
    for row in table:
        for name in row:
            region_table += name.encode('ascii', 'replace')[:REGION_TABLE_ENTRY_SIZE].ljust(
                REGION_TABLE_ENTRY_SIZE, b'\x00'
            )
        for _ in range(10 - len(row)):
            region_table += b'\x00' * REGION_TABLE_ENTRY_SIZE

    body = header + label_field + lang_field + region_table
    if len(body) < 4096:
        body += b'\x00' * (4096 - len(body))
    with open(path, "wb") as f:
        f.write(body)


def write_regions_sdl(path, region_slugs, supp_lang, countries_dict):
    """Write REGIONS.SDL: simplified table of regions. (Kept OEM as per request)"""
    if not supp_lang:
        supp_langs = ["UKE"]
    else:
        supp_langs = [s.strip().upper() for s in supp_lang.split(',')]
    table = build_region_translation_table(region_slugs, supp_langs, countries_dict)

    header = OEM_HEADER
    region_table = b''
    for row in table:
        for name in row:
            region_table += name.encode('ascii', 'replace')[:REGION_TABLE_ENTRY_SIZE].ljust(
                REGION_TABLE_ENTRY_SIZE, b'\x00'
            )
        for _ in range(10 - len(row)):
            region_table += b'\x00' * REGION_TABLE_ENTRY_SIZE

    body = header + region_table
    if len(body) < 4096:
        body += b'\x00' * (4096 - len(body))
    with open(path, "wb") as f:
        f.write(body)


def write_mtoc_sdl(path, files):
    """Write MTOC.SDL with OEM-like records. (Kept OEM as per request)"""
    buf = bytearray(b"\x00" * 64)
    next_id = 1

    for fpath in files:
        name = fpath.name.upper()
        rec = bytearray(b"\x00" * 64)
        rec[8:8 + 16] = name.encode('ascii', 'replace')[:16].ljust(16, b'\x00')
        marker = marker_for_file(name)
        rec[28] = marker[0]
        rec[29:32] = struct.pack(">I", next_id)[1:]
        next_id += 1
        buf.extend(rec)

    if len(buf) < 4096:
        buf.extend(b"\x00" * (4096 - len(buf)))
    with open(path, "wb") as f:
        f.write(buf)


def write_cartotop_sdl(path: pathlib.Path, entries: List[TopologyEntry]):
    """Writes CARTOTOP.SDL containing a single parcel with the global index."""
    if not entries:
        payload = b"\x00" * 18
    else:
        min_lat = min(e.rect_min_lat_ntu for e in entries)
        max_lat = max(e.rect_max_lat_ntu for e in entries)
        min_lon = min(e.rect_min_lon_ntu for e in entries)
        max_lon = max(e.rect_max_lon_ntu for e in entries)

        buf = bytearray()
        buf += struct.pack(">iiii", min_lon, min_lat, max_lon, max_lat) 
        buf += struct.pack(">H", len(entries))
        
        for e in entries:
            buf += struct.pack(">iiii", 
                               e.rect_min_lon_ntu, e.rect_min_lat_ntu,
                               e.rect_max_lon_ntu, e.rect_max_lat_ntu)
            buf += struct.pack(">HHHHH",
                               e.db_id,
                               e.parcel_id,
                               e.layer_type,
                               e.scale_min,
                               e.scale_max)
            buf += b"\x00\x00"
            
        payload = bytes(buf)

    blob = encode_bytes(
        pid=CARTOTOP_PARCEL_ID, 
        payload=payload,
        offset_units=0, 
        region=0,
        parcel_type=0,
        parcel_desc=0,
        compress_type=NO_COMPRESSION,
        size_index=0
    )
    
    with open(path, "wb") as f:
        f.write(blob)
        pad = (-len(blob)) & (4096 - 1)
        if pad:
            f.write(b"\x00" * pad)


def _encode_kdtree_idx_header(kd_data_len: int, min_lat: int, max_lat: int, min_lon: int, max_lon: int) -> bytes:
    """
    Encodes a minimal, structurally correct IDxPclHdr_t (32 bytes).
    Structure: usIndexID(H), usIndexType(H), ulIndexOffset(I), ulIndexLength(I), min_lat(I), min_lon(I), reserved[4] (H*4)
    """
    # Структура: H H I I I I 4H
    _IDXPCL_STRUCT = struct.Struct(">H H I I I I H H H H") 
    
    ul_index_offset = 0 
    ul_index_length = kd_data_len
    
    us_index_id = 1 
    us_index_type = 1 

    args = (
        us_index_id,      
        us_index_type,    
        ul_index_offset,  
        ul_index_length,  
        min_lat,          
        min_lon,          
        0, 0, 0, 0        
    )
    
    return _IDXPCL_STRUCT.pack(*args)

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
    """Copy OEM SDL files from the project oem_sdl/ folder into work_dir."""
    result: list[pathlib.Path] = []
    try:
        project_root = pathlib.Path(__file__).resolve().parents[2]
    except Exception:
        return result
    oem_dir = project_root / "oem_sdl"
    if not oem_dir.exists():
        return result
    for src in oem_dir.glob("*.SDL"):
        if src.name.upper() == "INIT.SDL":
            continue
        dst = work_dir / src.name
        if not dst.exists():
            shutil.copy2(src, dst)
        result.append(dst)
    return result


def _iter_coords(geom):
    """Yield (lon, lat) pairs from LineString or MultiLineString."""
    if isinstance(geom, LineString):
        for x, y in geom.coords:
            yield (x, y)
    elif isinstance(geom, MultiLineString):
        for part in geom.geoms:
            for x, y in part.coords:
                yield (x, y)


def build_region_sdl_file(mode: str, out_path: pathlib.Path, 
                          db_id: int, 
                          sdl_name: str, 
                          parcel_builders: List[ParcelBuilder],
                          topology_entries: List[TopologyEntry]):
    """Router function based on format mode."""
    if mode.upper() == "SDAL":
        return build_region_sdl_file_sdal(out_path, db_id, sdl_name, parcel_builders, topology_entries)
    else: # OEM is default
        return build_region_sdl_file_oem(out_path, db_id, sdl_name, parcel_builders, topology_entries)


def build_region_sdl_file_sdal(out_path: pathlib.Path, 
                               db_id: int, 
                               sdl_name: str, 
                               parcel_builders: List[ParcelBuilder],
                               topology_entries: List[TopologyEntry]):
    """
    Builds a regional .SDL file in SDAL mode: RgnHdr_t (512B + padding) + Parcels.
    """
    unit_shift = 12
    unit_size = 1 << unit_shift
    
    # 1. Write RgnHdr_t (512 bytes)
    region_header = _encode_region_header(db_id)
    
    region_header_size = len(region_header)
    # RgnHdr_t is written and padded to the next unit boundary (4096 bytes)
    pad_to_unit = (-region_header_size) & (unit_size - 1)
    
    # Offset of the first actual parcel payload (in bytes)
    offset_bytes = region_header_size + pad_to_unit
    
    with open(out_path, "wb") as f:
        f.write(region_header)
        if pad_to_unit:
            f.write(b'\x00' * pad_to_unit)

        # 2. Write Data Parcels (PclHdr_t + Payload)
        for pb in parcel_builders:
            # Offset units calculation respects the header/padding at the start of the file
            offset_units = offset_bytes >> unit_shift
            
            parcel_bytes = pb.make(offset_units) 
            
            f.write(parcel_bytes)
            pad = (-len(parcel_bytes)) & (unit_size - 1)
            if pad:
                f.write(b"\x00" * pad)
            offset_bytes += len(parcel_bytes) + pad
            
            topology_entries.append(
                # Topology Entry's offset is based on the whole file, including RgnHdr_t.
                TopologyEntry(
                    db_id=db_id,
                    sdl_name=sdl_name,
                    parcel_id=pb.pid,
                    offset_units=offset_units,
                    rect_min_lat_ntu=pb.rect[0],
                    rect_max_lat_ntu=pb.rect[1],
                    rect_min_lon_ntu=pb.rect[2],
                    rect_max_lon_ntu=pb.rect[3],
                    scale_min=pb.scale_min,
                    scale_max=pb.scale_max,
                    layer_type=pb.layer_type,
                )
            )

def build_region_sdl_file_oem(out_path: pathlib.Path, 
                               db_id: int, 
                               sdl_name: str, 
                               parcel_builders: List[ParcelBuilder],
                               topology_entries: List[TopologyEntry]):
    """
    Builds a regional .SDL file in OEM mode (current version): 
    Parcels start immediately at the file beginning (Offset 0).
    """
    unit_shift = 12
    unit_size = 1 << unit_shift
    offset_bytes = 0
    
    with open(out_path, "wb") as f:
        for pb in parcel_builders:
            offset_units = offset_bytes >> unit_shift
            
            parcel_bytes = pb.make(offset_units)
            
            f.write(parcel_bytes)
            pad = (-len(parcel_bytes)) & (unit_size - 1)
            if pad:
                f.write(b"\x00" * pad)
            offset_bytes += len(parcel_bytes) + pad
            
            topology_entries.append(
                TopologyEntry(
                    db_id=db_id,
                    sdl_name=sdl_name,
                    parcel_id=pb.pid,
                    offset_units=offset_units,
                    rect_min_lat_ntu=pb.rect[0],
                    rect_max_lat_ntu=pb.rect[1],
                    rect_min_lon_ntu=pb.rect[2],
                    rect_max_lon_ntu=pb.rect[3],
                    scale_min=pb.scale_min,
                    scale_max=pb.scale_max,
                    layer_type=pb.layer_type,
                )
            )


def choose_scale_shift_for_nodes(nodes: list[NodeRecord]) -> int:
    """Choose a scaleShift."""
    if not nodes:
        return 0
    has_any = False
    min_lat_ntu = max_lat_ntu = 0
    min_lon_ntu = max_lon_ntu = 0
    for n in nodes:
        lat_ntu, lon_ntu = deg_to_ntu(n.lat_deg, n.lon_deg)
        if not has_any:
            min_lat_ntu = max_lat_ntu = lat_ntu
            min_lon_ntu = max_lon_ntu = lon_ntu
            has_any = True
        else:
            min_lat_ntu = min(min_lat_ntu, lat_ntu)
            max_lat_ntu = max(max_lat_ntu, lat_ntu)
            min_lon_ntu = min(min_lon_ntu, lon_ntu)
            max_lon_ntu = max(max_lon_ntu, lon_ntu)
    if not has_any:
        return 0
    max_delta = max(max_lat_ntu - min_lat_ntu, max_lon_ntu - min_lon_ntu)
    for s in range(0, 16):
        if (max_delta >> s) <= 0xFFFFF:
            return s
    return 15


def build_routing_graph_from_roads_df(roads_df) -> tuple[list[NodeRecord], list[SegmentRecord]]:
    """Derive a simple routing graph."""
    nodes_map: dict[tuple[float, float], int] = {}
    node_records: dict[int, NodeRecord] = {}
    segments: list[SegmentRecord] = []
    next_node_id = 0
    next_seg_id = 0

    for _, row in roads_df.iterrows():
        geom = row["geometry"]
        coords = list(_iter_coords(geom))
        if len(coords) < 2:
            continue

        line_node_ids: list[int] = []
        for lon, lat in coords:
            key = (lon, lat)
            nid = nodes_map.get(key)
            if nid is None:
                nid = next_node_id
                nodes_map[key] = nid
                node_records[nid] = NodeRecord(
                    node_id=nid,
                    lat_deg=lat,
                    lon_deg=lon,
                )
                next_node_id += 1
            line_node_ids.append(nid)

        for i in range(len(line_node_ids) - 1):
            n1 = line_node_ids[i]
            n2 = line_node_ids[i + 1]
            lon1, lat1 = coords[i]
            lon2, lat2 = coords[i + 1]
            mean_lat_rad = math.radians((lat1 + lat2) * 0.5)
            dx = (lon2 - lon1) * 111_320.0 * math.cos(mean_lat_rad)
            dy = (lat2 - lat1) * 110_540.0
            length_m = (dx * dx + dy * dy) ** 0.5

            seg = SegmentRecord(
                seg_id=next_seg_id,
                from_node_id=n1,
                to_node_id=n2,
                length_m=length_m,
                speed_class=0,
                oneway=0,
            )
            segments.append(seg)
            node_records[n1].segment_ids.append(seg.seg_id)
            node_records[n2].segment_ids.append(seg.seg_id)
            next_seg_id += 1

    node_list = [node_records[nid] for nid in sorted(node_records.keys())]
    return node_list, segments


def build(
    regions: list[str], out_iso: pathlib.Path, work: pathlib.Path,
    region_label=None, supp_lang=None, format_mode: str = "OEM"
):
    log = logging.getLogger(__name__)

    for region in regions:
        if not region_exists(region):
            log.error(f"Region slug '{region}' not found or not downloadable from Geofabrik.")
            sys.exit(1)

    warnings.filterwarnings("ignore", category=FutureWarning, module="pyrosm.networks")
    warnings.filterwarnings("ignore", message="Geometry is in a geographic CRS.*", category=UserWarning)

    work.mkdir(parents=True, exist_ok=True)

    # 1) Download all regions
    for region in regions:
        pbf_path = work / f"{region.replace('/', '-')}.osm.pbf"
        download_region_if_needed(region, work)
        
        # Проверка целостности файла после загрузки
        min_size_mb = 10 
        min_size_bytes = min_size_mb * 1024 * 1024
        
        if not pbf_path.exists() or pbf_path.stat().st_size < min_size_bytes:
            current_size_mb = pbf_path.stat().st_size / (1024 * 1024) if pbf_path.exists() else 0.0
            log.error(f"PBF file for {region} is missing or too small ({current_size_mb:.2f} MB). Expected minimum is {min_size_mb} MB.")
            log.error("Download likely failed. Please clean your work directory (`rm -rf {work}`) and try running the script again.")
            sys.exit(1)


    # Global Topology Registry
    topology_entries: list[TopologyEntry] = []
    
    # DB ID assignment
    db_id_counter = 1
    db_for_region: dict[str, int] = {}
    for region in regions:
        db_for_region[region] = db_id_counter
        db_id_counter += 1

    # 2) Global POI name list
    all_poi_names = []
    poi_records = []
    poi_offsets = []
    poi_coords = []
    offset_acc = 0
    poi_index_counter = 0
    
    for region in regions:
        pbf_path = work / f"{region.replace('/', '-')}.osm.pbf"
        pois_df = load_poi_data(pbf_path=str(pbf_path), logger=log, poi_tags=None)
        log.info("Loaded %d POIs from %s", len(pois_df), region)
        
        all_poi_names.extend(pois_df["name"].fillna("").tolist())
        
        for geom_idx, geom in zip(pois_df.index, pois_df.geometry):
            if not isinstance(geom, Point):
                geom = geom.centroid
            lon, lat = geom.x, geom.y
            
            payload = struct.pack("<ii", int(lat * 1e6), int(lon * 1e6))
            poi_records.append((int(poi_index_counter), payload))
            poi_offsets.append((int(poi_index_counter), offset_acc))
            offset_acc += len(payload) + 6
            
            poi_coords.append((lon, lat))
            
            poi_index_counter += 1


    # 2.5) Build Global KD-Tree from POI coordinates
    log.info("Building Global KD-Tree from %d POI geometries...", len(poi_coords))
    if not poi_coords:
        kd_payload = b""
        min_lat_ntu, max_lat_ntu, min_lon_ntu, max_lon_ntu = 0, 0, 0, 0
    else:
        # Build KD-Tree (using simplified serialization for now)
        kd_tree_obj = build_kdtree(poi_coords)
        kd_data = serialize_kdtree(kd_tree_obj) 
        
        # Calculate bounding box for IDxPclHdr_t
        all_lons = [c[0] for c in poi_coords]
        all_lats = [c[1] for c in poi_coords]
        min_lat, min_lon = min(all_lats), min(all_lons)
        max_lat, max_lon = max(all_lats), max(all_lons)
        min_lat_ntu, min_lon_ntu = deg_to_ntu(min_lat, min_lon)
        max_lat_ntu, max_lon_ntu = deg_to_ntu(max_lat, max_lon)
        
        # Construct IDxPclHdr_t (32 bytes) + KD Data
        kd_header = _encode_kdtree_idx_header(
            len(kd_data),
            min_lat_ntu, max_lat_ntu, min_lon_ntu, max_lon_ntu # Bounding box
        )
        kd_payload = kd_header + kd_data
        
        log.info("Serialized KD-Tree payload size: %.2f MB", len(kd_payload) / 1e6)
        del kd_tree_obj
    

    # 3) Encode POI names (NO_COMPRESSION)
    global_files: list[pathlib.Path] = []
    global_files.extend(copy_oem_sdl_files(work))

    poi_name_file = work / "POINAMES.SDL"
    # ФУНКЦИОНАЛЬНОЕ ИСПРАВЛЕНИЕ: Используем NO_COMPRESSION
    poi_name_file.write_bytes(encode_strings(POI_NAME_PARCEL_ID, all_poi_names, compress_type=NO_COMPRESSION))
    global_files.append(poi_name_file)

    # 4) Encode POI geometry & index
    poi_geom_file = work / "POIGEOM.SDL"
    
    # Payload POI Geometry (бинарные данные) - NO_COMPRESSION
    poi_geom_blob = encode_bytes(
        POI_GEOM_PARCEL_ID, 
        b"".join(struct.pack(">H", UNCOMPRESSED_FLAG) + rec[1] for rec in poi_records), 
        offset_units=0, region=0, parcel_type=0, parcel_desc=0,
        compress_type=NO_COMPRESSION,
        size_index=0
    )
    
    # Payload POI Index (бинарные данные) - NO_COMPRESSION
    poi_index_blob = encode_poi_index(POI_INDEX_PARCEL_ID, poi_offsets, compress_type=NO_COMPRESSION)
    
    with open(poi_geom_file, "wb") as f:
        f.write(poi_geom_blob)
        f.write(encode_bytes(POI_INDEX_PARCEL_ID, poi_index_blob, offset_units=0, region=0, parcel_type=0, parcel_desc=0, compress_type=NO_COMPRESSION, size_index=0))
    global_files.append(poi_geom_file)

    # 5) KDTREE & placeholders
    # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Используем kd_payload, включающий IDxPclHdr_t
    for name, pid, data in [
        ("KDTREE.SDL", GLB_KD_TREE_PID, kd_payload), 
    ]:
        path = work / name
        path.write_bytes(encode_bytes(pid, data, offset_units=0, region=0, parcel_type=0, parcel_desc=0, compress_type=NO_COMPRESSION, size_index=0))
        global_files.append(path)

    # --- Density overlays ---
    disc_code = extract_disc_code(regions)
    dens_tiles = []

    for region in regions:
        pbf_path = work / f"{region.replace('/', '-')}.osm.pbf"
        roads_df = load_road_network(str(pbf_path))
        log.info("Loaded %d road geometries for density from %s", len(roads_df), region)

        bbox = roads_df.total_bounds
        minx, miny, maxx, maxy = bbox
        
        # Approximate UTM zone
        center_x = (minx + maxx) / 2.0
        utm_zone = int((center_x + 180) / 6) + 1
        utm_crs = f"EPSG:{32600 + utm_zone}"
        roads_proj = roads_df.to_crs(utm_crs)
        proj_bounds = roads_proj.total_bounds
        pminx, pminy, pmaxx, pmaxy = proj_bounds

        roads_simple = roads_proj.explode(ignore_index=True)
        roads_simple = roads_simple[
            roads_simple.geometry.type.isin(["LineString", "MultiLineString"])
        ]

        log.info(f"Generating Density tiles for {region}...")
        for Z in range(0, 4):
            num_tiles = 2 ** Z
            tile_width = (pmaxx - pminx) / num_tiles
            tile_height = (pmaxy - pminy) / num_tiles

            for tx in range(num_tiles):
                for ty in range(num_tiles):
                    tminx = pminx + tx * tile_width
                    tmaxx = pminx + (tx + 1) * tile_width
                    tminy = pminy + ty * tile_height
                    tmaxy = pminy + (ty + 1) * tile_height
                    
                    grid_size = 256
                    dx = (tmaxx - tminx) / grid_size
                    dy = (tmaxy - tminy) / grid_size

                    density_array = np.zeros((grid_size, grid_size), dtype=np.float64)
                    tile_box = box(tminx, tminy, tmaxx, tmaxy)
                    clipped = roads_simple.geometry.intersection(tile_box)

                    max_seg_length = min(dx, dy) / 2.0
                    
                    for seg_geom in tqdm(
                        clipped,
                        desc=f"Rasterizing DENSITY: {region} | Zoom {Z} Tile ({tx},{ty})",
                        unit="geom",
                    ):
                        if seg_geom.is_empty: continue
                        if seg_geom.geom_type == "LineString": geoms = [seg_geom]
                        elif seg_geom.geom_type == "MultiLineString": geoms = list(seg_geom.geoms)
                        else: continue

                        for g in geoms:
                            total_len = g.length
                            if total_len <= 0: continue
                            n_pieces = max(1, int(np.ceil(total_len / max_seg_length)))
                            fractions = np.linspace(0, 1, n_pieces + 1)
                            prev_pt = seg_geom.interpolate(fractions[0], normalized=True)
                            for i in range(1, len(fractions)):
                                curr_pt = seg_geom.interpolate(fractions[i], normalized=True)
                                seg_len = prev_pt.distance(curr_pt)
                                midpoint = LineString([prev_pt, curr_pt]).centroid
                                mx, my = midpoint.x, midpoint.y
                                col = int((mx - tminx) // dx)
                                row = int((my - tminy) // dy)
                                if 0 <= col < grid_size and 0 <= row < grid_size:
                                    density_array[row, col] += seg_len
                                prev_pt = curr_pt
                    
                    max_val = density_array.max()
                    scale = 65535.0 / max_val if max_val > 0 else 0.0
                    density_scaled = ((density_array * scale).clip(0, 65535).astype(np.uint16))
                    dens_tiles.append(density_scaled.astype("<u2").tobytes())
        del roads_df, roads_proj, roads_simple

    # ----------------------------------------------------------------
    # 6) Density overlays: Aggregation and file split
    # ----------------------------------------------------------------
    if dens_tiles:
        raw_data = b"".join(dens_tiles)
        
        # DENSXXX1.SDL: Raw data (large file)
        dens1 = work / f"DENS{disc_code}1.SDL"
        dens1.write_bytes(raw_data)
        global_files.append(dens1)

        # DENSXXX0.SDL: Index/control file (small file)
        dens0 = work / f"DENS{disc_code}0.SDL"
        payload_dens0 = struct.pack(">I", len(dens_tiles))
        
        dens0_parcel = encode_bytes(
            pid=DENS_PARCEL_ID, 
            payload=payload_dens0,
            offset_units=0, region=0, parcel_type=0, parcel_desc=0,
            compress_type=NO_COMPRESSION,
            size_index=0
        )
        
        dens0.write_bytes(dens0_parcel) 
        global_files.append(dens0)
        
        log.info(f"Generated DENS files: {dens0.name} (Header/Index), {dens1.name} (Raw Data)")
    # ----------------------------------------------------------------

    # 7) Per-region 0/1 cartography files (XXX0.SDL / XXX1.SDL)
    region_files: list[pathlib.Path] = []
    for region in regions:
        db_id = db_for_region[region]
        pbf_path = work / f"{region.replace('/', '-')}.osm.pbf"
        roads_df = load_road_network(str(pbf_path))[["id", "name", "geometry"]]
        stem = pathlib.Path(region).name.upper().replace("-", "_")

        minx, miny, maxx, maxy = roads_df.total_bounds
        min_lat_ntu, min_lon_ntu = deg_to_ntu(miny, minx)
        max_lat_ntu, max_lon_ntu = deg_to_ntu(maxy, maxx)
        region_rect_ntu = (min_lat_ntu, max_lat_ntu, min_lon_ntu, max_lon_ntu)

        # XXX0.SDL — "fast" names (Textual data)
        fast_file = work / f"{stem}0.SDL"
        names = roads_df["name"].fillna("").tolist()
        # ФУНКЦИОНАЛЬНОЕ ИСПРАВЛЕНИЕ: Используем NO_COMPRESSION
        fast_file.write_bytes(encode_strings(NAV_PARCEL_ID, names, compress_type=NO_COMPRESSION))

        # Cartography records + B-tree offsets
        records: list[tuple[int, list[tuple[float, float]]]] = []
        for wid, geom in tqdm(zip(roads_df["id"], roads_df.geometry), total=len(roads_df), unit="road"):
            coords = list(_iter_coords(geom))
            if not coords: continue
            records.append((wid, coords))

        MAX_CARTO_RECORDS = 65535
        record_chunks: list[list[tuple[int, list[tuple[float, float]]]]] = []
        offset_chunks: list[list[tuple[int, int]]] = []

        idx = 0
        while idx < len(records):
            chunk_records = []
            chunk_offsets = []
            off = 18
            while idx < len(records) and len(chunk_records) < MAX_CARTO_RECORDS:
                way_id, coords = records[idx]
                chunk_records.append((way_id, coords))
                size = 6 + len(coords) * 8
                chunk_offsets.append((way_id, off))
                off += size
                idx += 1
            if chunk_records:
                record_chunks.append(chunk_records)
                offset_chunks.append(chunk_offsets)

        nodes, segments = build_routing_graph_from_roads_df(roads_df)
        scale_shift = choose_scale_shift_for_nodes(nodes)

        # XXX1.SDL
        map_file = work / f"{stem}1.SDL"
        parcel_builders: list[ParcelBuilder] = []

        for chunk_records, chunk_offsets in zip(record_chunks, offset_chunks):
            
            def make_carto_parcel(offset_units: int, _records=chunk_records, _rect=region_rect_ntu):
                return encode_cartography(
                    CARTO_PARCEL_ID, _records, offset_units=offset_units, rect_ntu=_rect,
                    compress_type=NO_COMPRESSION,
                )

            def make_btree_parcel(offset_units: int, _offsets=chunk_offsets):
                return encode_btree(
                    BTREE_PARCEL_ID, _offsets, offset_units=offset_units,
                    compress_type=NO_COMPRESSION,
                )

            parcel_builders.append(ParcelBuilder(
                pid=CARTO_PARCEL_ID, layer_type=0, make=make_carto_parcel, rect=region_rect_ntu,
                scale_min=0, scale_max=0xFFFF, compress=NO_COMPRESSION
            ))
            
            parcel_builders.append(ParcelBuilder(
                pid=BTREE_PARCEL_ID, layer_type=2, make=make_btree_parcel, rect=region_rect_ntu,
                scale_min=0, scale_max=0xFFFF, compress=NO_COMPRESSION
            ))

        def make_routing_parcel(offset_units: int, _nodes=nodes, _segments=segments, _scale_shift=scale_shift, _rect=region_rect_ntu):
            return encode_routing_parcel(
                pid=ROUTING_PARCEL_ID, nodes=_nodes, segments=_segments, region=1,
                parcel_type=0, parcel_desc=0x02, offset_units=offset_units,
                rect_ntu=_rect, scale_shift=_scale_shift, size_index=0,
                compress_type=NO_COMPRESSION
            )

        parcel_builders.append(ParcelBuilder(
            pid=ROUTING_PARCEL_ID, layer_type=1, make=make_routing_parcel, rect=region_rect_ntu,
            scale_min=0, scale_max=0xFFFF, compress=NO_COMPRESSION
        ))

        # Используем роутер для выбора режима построения файла
        build_region_sdl_file(
            format_mode, map_file, db_id=db_id, sdl_name=map_file.name,
            parcel_builders=parcel_builders, topology_entries=topology_entries
        )

        region_files.extend([fast_file, map_file])

    # 8) Write CARTOTOP
    cartotop_path = work / "CARTOTOP.SDL"
    write_cartotop_sdl(cartotop_path, topology_entries)
    global_files.append(cartotop_path)

    # 9) REGION/REGIONS/INIT/MTOC
    region_sdl = work / "REGION.SDL"
    regions_sdl = work / "REGIONS.SDL"
    mtoc_sdl = work / "MTOC.SDL"
    init_sdl = work / "INIT.SDL"

    write_region_sdl(region_sdl, regions, supp_lang, countries)
    write_regions_sdl(regions_sdl, regions, supp_lang, countries)

    sdl_for_control = global_files + region_files + [region_sdl, regions_sdl]

    write_init_sdl(init_sdl, sdl_for_control, regions, supp_lang)

    all_for_mtoc = sdl_for_control + [init_sdl]
    write_mtoc_sdl(mtoc_sdl, all_for_mtoc)

    # 10) Build ISO
    iso_tmp = work / "sdal_tmp.iso"
    if iso_tmp.exists():
        iso_tmp.unlink()

    all_sdl_for_iso = all_for_mtoc + [mtoc_sdl]
    
    from .iso import build_iso 
    build_iso(
        sdl_files=all_sdl_for_iso,
        out_iso=iso_tmp,
    )

    shutil.move(str(iso_tmp), out_iso)


def cli():
    parser = argparse.ArgumentParser(description="Build SDAL ISO image from OSM data.")
    parser.add_argument(
        "regions",
        nargs="+",
        help="Region slugs compatible with Geofabrik, e.g. 'europe/cyprus'",
    )
    parser.add_argument("--out", required=True, help="Output ISO path")
    parser.add_argument("--work", required=True, help="Working directory")
    parser.add_argument("--region-label", default=None, help="(ignored, label always continent)")
    parser.add_argument(
        "--supp-lang",
        default=None,
        help="Supported language codes as CSV (e.g., 'DAN,DUT,ENG'). Default: UKE",
    )
    parser.add_argument(
        "--format-mode",
        default="OEM",
        choices=["OEM", "SDAL"],
        help="Map file structuring mode. OEM uses custom header/index; SDAL uses RgnHdr_t.",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args()
    init_logging(args.verbose, pathlib.Path(args.work))
    build(
        args.regions,
        pathlib.Path(args.out),
        pathlib.Path(args.work),
        region_label=args.region_label,
        supp_lang=args.supp_lang,
        format_mode=args.format_mode,
    )


if __name__ == "__main__":
    cli()