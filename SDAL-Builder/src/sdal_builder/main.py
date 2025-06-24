#!/usr/bin/env python3
"""
CLI for building SDAL ISO images per region, embedding PID 0 in MTOC.SDL,
generating multi‐tile density overlays *grouped per OEM region*,
and including all OSM POIs,
without blowing out memory by holding everything in a single GeoDataFrame.
"""
import argparse
import logging
import pathlib
import sys
import json
import warnings
import shutil
from logging.handlers import RotatingFileHandler

import requests
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString, MultiLineString, box, Point
from tqdm import tqdm
import struct
import os

from .constants import (
    CARTO_PARCEL_ID,
    NAV_PARCEL_ID,
    KDTREE_PARCEL_ID,
    BTREE_PARCEL_ID,
    DENS_PARCEL_ID,
    POI_NAME_PARCEL_ID,
    POI_GEOM_PARCEL_ID,
    POI_INDEX_PARCEL_ID,
)
from .etl import load_road_network, load_poi_data
from .encoder import encode_strings, encode_road_records, encode_bytes
from .spatial import build_kdtree, serialize_kdtree, build_bplustree, dump_bplustree
from .iso import write_iso
from .translations import countries

# --- OEM group definition (edit to match your disc structure) ---
GROUPS = {
    "BE": "BENELUX", "NL": "BENELUX", "LU": "BENELUX",
    "DK": "DENSWE", "SE": "DENSWE", "NO": "DENSWE", "FI": "DENSWE",
    "FR": "FRANCE",
    "DE": "GERMANY",
    "IT": "ITALY",
    "ES": "IBERIA", "PT": "IBERIA",
    "GB": "UK", "IE": "UK",
    "AT": "SWIAUS", "CH": "SWIAUS",
    # fallback: unmapped gets code itself (e.g. CY → CY)
}

### ---- MARKER TABLE (type → byte, see SDAL) ----
MARKER_TABLE = {
    "REGION": b'R',   # Main region
    "STUB":   b'Z',   # Zero/empty stub
    "KDTREE": b'K',
    "CARTO":  b'C',
    "VOICE":  b'V',
    "LANG":   b'L',
    # extend as needed for others
}

# --------- OEM REGION/REGIONS SDL LOGIC: PATCHED ---------

SDAL_MAGIC = b'SDAL'
OEM_HEADER = b'SDAL\x00\x02\x00\x06\x00\x14\x00\x03\x00\x0e'
REGION_LABEL_MAXLEN = 0x0E
LANG_CODES_MAXLEN = 0x1E
REGION_TABLE_ENTRY_SIZE = 0x10
SECTOR_SIZE = 4096

def oem_pad(data: bytes) -> bytes:
    padding = (SECTOR_SIZE - (len(data) % SECTOR_SIZE)) % SECTOR_SIZE
    return data + (b'\x00' * padding)

def extract_continent(region_slugs):
    # "europe/cyprus" -> "EUROPE"
    if not region_slugs:
        return "UNKNOWN"
    return region_slugs[0].split('/')[0].upper()

def extract_country(region_slug):
    # "europe/cyprus" -> "CYPRUS"
    return region_slug.split('/')[-1].replace('-', ' ').replace('_', ' ').upper()

def build_region_translation_table(region_slugs, supp_langs, countries):
    """
    For each region, returns a list of strings:
      [native, LANG1, LANG2, ...]
    First is always native (from slug). Then fill each lang (fallback UKE or native).
    """
    table = []
    for slug in region_slugs:
        native = extract_country(slug)
        row = [native]
        for lang in supp_langs:
            t = countries.get(native, {}).get(lang)
            if t:
                row.append(t)
            else:
                row.append(countries.get(native, {}).get('UKE', native))
        table.append(row)
    return table

def write_region_sdl(path, region_slugs, supp_lang, countries):
    """
    Write REGION.SDL in full OEM style:
      [header][label][lang_codes][region table][padding]
    """
    # Label: always the CONTINENT (not region name!)
    label = extract_continent(region_slugs)
    # Languages: default to UKE, or take from arg
    if not supp_lang:
        supp_langs = ["UKE"]
    else:
        supp_langs = [s.strip().upper() for s in supp_lang.split(',')]

    # Table: one row per region, [native, lang1, lang2, ...]
    table = build_region_translation_table(region_slugs, supp_langs, countries)

    # --- Build Binary Layout ---
    header = OEM_HEADER
    # Label, 14 bytes + NUL
    label_field = label.encode('ascii', 'replace')[:REGION_LABEL_MAXLEN].ljust(REGION_LABEL_MAXLEN, b' ') + b'\x00'
    # Lang codes, concat, max 30 bytes + NUL
    lang_field = b''.join(lang.encode('ascii') for lang in supp_langs)[:LANG_CODES_MAXLEN].ljust(LANG_CODES_MAXLEN, b' ') + b'\x00'
    # Table: 16 bytes per entry, padded
    region_table = b''
    for row in table:
        for name in row:
            region_table += name.encode('utf-8')[:REGION_TABLE_ENTRY_SIZE].ljust(REGION_TABLE_ENTRY_SIZE, b'\x00')
        for _ in range(10 - len(row)):
            region_table += b'\x00' * REGION_TABLE_ENTRY_SIZE

    body = header + label_field + lang_field + region_table
    with open(path, "wb") as f:
        f.write(oem_pad(body))

def write_regions_sdl(path, region_slugs, supp_lang, countries):
    """
    Write REGIONS.SDL in OEM style:
      [header][region table][padding]
    No label/langs, only region table.
    """
    if not supp_lang:
        supp_langs = ["UKE"]
    else:
        supp_langs = [s.strip().upper() for s in supp_lang.split(',')]
    table = build_region_translation_table(region_slugs, supp_langs, countries)
    header = OEM_HEADER
    region_table = b''
    for row in table:
        for name in row:
            region_table += name.encode('utf-8')[:REGION_TABLE_ENTRY_SIZE].ljust(REGION_TABLE_ENTRY_SIZE, b'\x00')
        for _ in range(10 - len(row)):
            region_table += b'\x00' * REGION_TABLE_ENTRY_SIZE
    body = header + region_table
    with open(path, "wb") as f:
        f.write(oem_pad(body))

# ----------- OEM MTOC WRITER -----------

def write_oem_mtoc_sdl(work_dir: pathlib.Path):
    """
    Write MTOC.SDL in genuine OEM style:
        - 64 bytes of zero padding
        - Entries: 32 bytes each (16-byte filename, 1 marker, 15 reserved)
        - File size 4KB (4096 bytes)
    """
    # All .SDL files (including MTOC, REGION, REGIONS) in dir, sorted
    files = sorted(str(f.name) for f in work_dir.glob("*.SDL"))
    records = []
    for fname in files:
        name_bytes = fname.encode("ascii")[:16]
        name_bytes = name_bytes.ljust(16, b'\x00')
        marker = marker_for_file(fname)
        rec = name_bytes + marker + b'\x00' * 15
        records.append(rec)
    # 64 bytes of zero padding at start
    body = b'\x00' * 64 + b''.join(records)
    body = body.ljust(4096, b'\x00')
    (work_dir / "MTOC.SDL").write_bytes(body)

# --------- END PATCH -----------

def init_logging(verbose: bool, work_dir: pathlib.Path):
    work_dir.mkdir(parents=True, exist_ok=True)
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s | %(levelname)s | %(message)s"
    datefmt = "%H:%M:%S"
    logging.basicConfig(
        level=level,
        format=fmt,
        datefmt=datefmt,
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    fh = RotatingFileHandler(work_dir / "run.log", maxBytes=5_000_000, backupCount=3)
    fh.setFormatter(logging.Formatter(fmt, datefmt))
    logging.getLogger().addHandler(fh)

def _iter_coords(geom):
    if isinstance(geom, LineString):
        return list(geom.coords)
    if isinstance(geom, MultiLineString):
        coords = []
        for part in geom.geoms:
            coords.extend(part.coords)
        return coords
    return []

def fetch(region: str, dest: pathlib.Path) -> pathlib.Path:
    url = f"https://download.geofabrik.de/{region}-latest.osm.pbf"
    log = logging.getLogger(__name__)
    if dest.exists():
        log.info("Using cached PBF: %s", dest)
        return dest
    log.info("Downloading %s …", url)
    r = requests.get(url, stream=True, timeout=30)
    r.raise_for_status()
    total = int(r.headers.get("content-length", 0))
    with open(dest, "wb") as f, tqdm(total=total, unit="B", unit_scale=True) as bar:
        for chunk in r.iter_content(8192):
            f.write(chunk)
            bar.update(len(chunk))
    log.info("Saved %s (%.1f MB)", dest, dest.stat().st_size / 1e6)
    return dest

def region_exists(region: str) -> bool:
    url = f"https://download.geofabrik.de/{region}-latest.osm.pbf"
    try:
        r = requests.head(url, allow_redirects=True, timeout=10)
        return r.status_code == 200
    except Exception:
        return False

def marker_for_file(fname: str):
    name = fname.upper()
    if name == "MTOC.SDL":      return MARKER_TABLE["STUB"]
    if name == "KDTREE.SDL":    return MARKER_TABLE["KDTREE"]
    if name == "CARTOTOP.SDL":  return MARKER_TABLE["CARTO"]
    if name in ("REGION.SDL", "REGIONS.SDL"): return MARKER_TABLE["STUB"]
    if name.startswith("DENS"): return MARKER_TABLE["REGION"]
    if name.endswith("0.SDL") or name.endswith("1.SDL"):
        return MARKER_TABLE["REGION"]
    if name.endswith("F.SDL") or name.endswith("M.SDL"):
        return MARKER_TABLE["REGION"]
    if name.startswith("VOICE"):
        return MARKER_TABLE["VOICE"]
    if name.startswith("LANG"):
        return MARKER_TABLE["LANG"]
    return MARKER_TABLE["STUB"]

def list_sdl_files(work_dir: pathlib.Path):
    # All .SDL files, including MTOC/REGION/REGIONS, sorted (ISO order)
    return sorted(str(f.name) for f in work_dir.glob("*.SDL"))

# --------- MAIN BUILD FUNCTION ---------

def build(
    regions: list[str], out_iso: pathlib.Path, work: pathlib.Path,
    region_label=None, supp_lang=None
):
    log = logging.getLogger(__name__)

    for region in regions:
        if not region_exists(region):
            log.error(f"Region slug '{region}' not found or not downloadable from Geofabrik.")
            sys.exit(1)

    warnings.filterwarnings("ignore", category=FutureWarning, module="pyrosm.networks")
    warnings.filterwarnings("ignore", message="Geometry is in a geographic CRS.*", category=UserWarning)

    work.mkdir(parents=True, exist_ok=True)

    try:
        oem_dir = pathlib.Path(__file__).resolve().parents[2] / "oem_sdl"
        global_files: list[pathlib.Path] = []
        if oem_dir.exists():
            for s in oem_dir.glob("*.SDL"):
                dst = work / s.name
                if not dst.exists():
                    shutil.copy2(s, dst)
                global_files.append(dst)

        all_centroids: list[tuple[float, float]] = []
        for region in regions:
            pbf_path = work / f"{region.replace('/', '-')}.osm.pbf"
            pbf = fetch(region, pbf_path)
            log.info("Parsing road network for %s with Pyrosm", region)
            roads_df = load_road_network(str(pbf))
            centroids = [(pt.x, pt.y) for pt in roads_df.geometry.centroid]
            all_centroids.extend(centroids)
            del roads_df
        log.info("Total combined centroids: %d", len(all_centroids))
        log.info("Building global KD-tree")
        kd = build_kdtree(all_centroids)
        kd_blob = serialize_kdtree(kd)

        all_poi_names: list[str] = []
        poi_records: list[tuple[int, bytes]] = []
        poi_offsets: list[tuple[int, int]] = []
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
                poi_index_counter += 1
            del pois_df
        log.info("Total combined POIs: %d", len(all_poi_names))
        poi_name_file = work / "POINAMES.SDL"
        poi_name_bytes = encode_strings(POI_NAME_PARCEL_ID, all_poi_names)
        poi_name_file.write_bytes(poi_name_bytes)
        global_files.append(poi_name_file)
        poi_idx_path = work / "POI.bpt"
        build_bplustree(poi_offsets, str(poi_idx_path))
        poi_index_blob = dump_bplustree(str(poi_idx_path))
        poi_geom_file = work / "POIGEOM.SDL"
        with open(poi_geom_file, "wb") as f:
            for pid, payload in poi_records:
                f.write(encode_bytes(POI_GEOM_PARCEL_ID, payload))
            f.write(encode_bytes(POI_INDEX_PARCEL_ID, poi_index_blob))
        global_files.append(poi_geom_file)

        for name, pid, data in [
            ("CARTOTOP.SDL", CARTO_PARCEL_ID, b""),
            ("KDTREE.SDL", KDTREE_PARCEL_ID, kd_blob),
        ]:
            path = work / name
            path.write_bytes(encode_bytes(pid, data))
            global_files.append(path)

        dens_buckets = {}
        for region in regions:
            cc = region.split("/")[-1][:2].upper()
            group = GROUPS.get(cc, cc)
            pbf_path = work / f"{region.replace('/', '-')}.osm.pbf"
            roads_df = load_road_network(str(pbf_path))
            log.info("Loaded %d road geometries for density from %s", len(roads_df), region)
            bbox = roads_df.total_bounds
            minx, miny, maxx, maxy = bbox
            center_x = (minx + maxx) / 2.0
            center_y = (miny + maxy) / 2.0
            utm_zone = int((center_x + 180) / 6) + 1
            utm_crs = f"EPSG:{32600 + utm_zone}"
            roads_proj = roads_df.to_crs(utm_crs)
            proj_bounds = roads_proj.total_bounds
            pminx, pminy, pmaxx, pmaxy = proj_bounds
            roads_simple = roads_proj.explode(ignore_index=True)
            roads_simple = roads_simple[
                roads_simple.geometry.type.isin(["LineString", "MultiLineString"])
            ]
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
                            desc=f"Rasterizing {region} Z{Z} tile ({tx},{ty})",
                            leave=False,
                        ):
                            if seg_geom.is_empty:
                                continue
                            total_len = seg_geom.length
                            if total_len == 0:
                                continue
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
                        density_scaled = (
                            (density_array * scale).clip(0, 65535).astype(np.uint16)
                        )
                        raw_bytes = density_scaled.astype("<u2").tobytes()
                        dens_buckets.setdefault(group, []).append(raw_bytes)
            del roads_df, roads_proj, roads_simple

        for group, tile_blobs in dens_buckets.items():
            n = len(tile_blobs)
            if n == 0:
                continue
            midpoint = (n + 1) // 2
            header_tiles = tile_blobs[:midpoint]
            body_tiles = tile_blobs[midpoint:]
            dens0 = work / f"DENS{group}0.SDL"
            dens1 = work / f"DENS{group}1.SDL"
            dens0.write_bytes(b"".join(header_tiles))
            dens1.write_bytes(b"".join(body_tiles))
            global_files.extend([dens0, dens1])

        region_files: list[pathlib.Path] = []
        for region in regions:
            pbf_path = work / f"{region.replace('/', '-')}.osm.pbf"
            roads_df = load_road_network(str(pbf_path))[["id", "name", "geometry"]]
            stem = pathlib.Path(region).name.upper().replace("-", "_")
            fast = work / f"{stem}F.SDL"
            names = roads_df["name"].fillna("").tolist()
            fast.write_bytes(encode_strings(NAV_PARCEL_ID, names))
            records = []
            offsets = []
            off = 0
            for wid, geom in tqdm(zip(roads_df["id"], roads_df.geometry), total=len(roads_df), unit="road"):
                coords = _iter_coords(geom)
                records.append((wid, coords))
                size = 6 + len(coords) * 16
                offsets.append((wid, off))
                off += size
            idx_path = work / f"{stem}.bpt"
            build_bplustree(offsets, str(idx_path))
            bt_blob = dump_bplustree(str(idx_path))
            fast.write_bytes(encode_bytes(BTREE_PARCEL_ID, bt_blob))
            region_files.append(fast)
            mapf = work / f"{stem}M.SDL"
            mapf.write_bytes(encode_road_records(CARTO_PARCEL_ID, records))
            mapf.write_bytes(encode_bytes(KDTREE_PARCEL_ID, kd_blob))
            region_files.append(mapf)
            del roads_df

        # --- FINAL OEM INDEX FILES ---
        # Write REGION/REGIONS files before MTOC
        write_region_sdl(str(work / "REGION.SDL"), regions, supp_lang, countries)
        write_regions_sdl(str(work / "REGIONS.SDL"), regions, supp_lang, countries)
        write_oem_mtoc_sdl(work)

        # Always include MTOC/REGION/REGIONS in ISO
        special_files = [
            work / "MTOC.SDL",
            work / "REGION.SDL",
            work / "REGIONS.SDL"
        ]
        all_iso_files = special_files + global_files + region_files

        write_iso(all_iso_files, out_iso)
        log.info("ISO built: %s", out_iso)

    except Exception:
        log.exception("Build failed")
        raise

def cli():
    parser = argparse.ArgumentParser(description="Build SDAL ISO per region, group DENS overlays per OEM logic")
    parser.add_argument(
        "regions",
        nargs="+",
        help="Geofabrik region slugs (e.g. europe/cyprus or europe/united-kingdom)",
    )
    parser.add_argument("--out", default="sdal.iso", help="Output ISO path")
    parser.add_argument("--work", default="build/tmp", help="Working directory")
    parser.add_argument("--region-label", default=None, help="(ignored, label always continent)")
    parser.add_argument("--supp-lang", default=None, help="Supported language codes as CSV (e.g., 'DAN,DUT,ENG'). Default: UKE")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args()
    init_logging(args.verbose, pathlib.Path(args.work))
    build(
        args.regions,
        pathlib.Path(args.out),
        pathlib.Path(args.work),
        region_label=args.region_label,
        supp_lang=args.supp_lang,
    )

if __name__ == "__main__":
    cli()
