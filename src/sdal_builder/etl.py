from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, Set, Union

import geopandas as gpd
import pandas as pd
import warnings
from tqdm import tqdm
import requests
import os

from .sdal_osmium_stream import extract_driving_roads   # road helper

LOG = logging.getLogger(__name__)

# ────────────────────────────────────────────────────────────────
# POIs (Теги и загрузчик)
# ────────────────────────────────────────────────────────────────

# ВОССТАНОВЛЕННЫЙ ОРИГИНАЛЬНЫЙ НАБОР POI-ТЕГОВ
DEFAULT_POI_TAGS: Set[str] = {
    "amenity", "shop", "tourism", "leisure", "historic",
    "office", "craft", "man_made", "healthcare", "sport",
    "emergency", "public_transport", "railway", "aeroway", "natural",
}


def load_all_pois(
    pbf_path: Union[str, Path],
    logger: logging.Logger = LOG,
    poi_tags: Iterable[str] | None = None,
) -> gpd.GeoDataFrame:
    """
    Load POIs from an OSM extract using Pyrosm.get_pois.

    - Подавляет pandas PerformanceWarning от Pyrosm.
    - Консолидирует DataFrame in-place, чтобы не было фрагментации.
    """
    try:
        from pyrosm import OSM
        import pandas as pd
    except ImportError:
        logger.error("ERROR: pyrosm is required for POI loading. Install with 'pip install pyrosm'")
        raise

    warnings.filterwarnings(
        "ignore",
        category=pd.errors.PerformanceWarning,
        module="pyrosm.pois",
    )

    tags = set(poi_tags) if poi_tags else DEFAULT_POI_TAGS
    tag_filter = {k: True for k in tags}

    logger.info("Loading POIs with Pyrosm.get_pois (Vectorized Filter)... This may take a while on large PBFs.")
    poi_gdf = OSM(str(pbf_path)).get_pois(custom_filter=tag_filter)

    # Defragment DataFrame (иначе Pyrosm иногда оставляет «дырки»)
    try:
        # ИСПРАВЛЕНИЕ: Удаляем вызов консолидации, так как он может быть нестабилен
        # poi_gdf._consolidate_inplace()  # type: ignore[attr-defined]
        pass
    except Exception:
        pass

    logger.info("Loaded %d POIs after filtering", len(poi_gdf))
    return poi_gdf


def load_poi_data(
    pbf_path: Union[str, Path],
    logger: logging.Logger = LOG,
    poi_tags: Iterable[str] | None = None,
) -> gpd.GeoDataFrame:
    """
    Main entry used by src/sdal_builder/main.py.
    Forwards to load_all_pois, eliminating any dependency on legacy extract_pois.
    """
    return load_all_pois(pbf_path, logger, poi_tags)


# ────────────────────────────────────────────────────────────────
# Road network
# ────────────────────────────────────────────────────────────────

def load_road_network(pbf_path: Union[str, Path]) -> gpd.GeoDataFrame:
    """
    Load drivable road network from .osm.pbf using streaming osmium helper.
    """
    pbf_path = str(pbf_path)
    LOG.info("Loading drivable road network from %s", pbf_path)
    roads_df = extract_driving_roads(pbf_path)
    LOG.info("Loaded %d road geometries", len(roads_df))
    return roads_df


# ────────────────────────────────────────────────────────────────
# Geofabrik: download and region existence (with progress)
# ────────────────────────────────────────────────────────────────

def download_region_if_needed(region: str, work_dir: Path) -> Path:
    """
    Скачивает OSM PBF с Geofabrik, если он ещё не лежит в work_dir.
    """
    import requests
    from tqdm import tqdm

    work_dir.mkdir(parents=True, exist_ok=True)
    safe_name = region.strip("/").replace("/", "-")
    dest = work_dir / f"{safe_name}.osm.pbf"

    url = f"https://download.geofabrik.de/{region}-latest.osm.pbf"
    log = LOG

    if dest.exists() and dest.stat().st_size > 0:
        log.info("Using cached PBF: %s (%.1f MB)", dest, dest.stat().st_size / 1e6)
        return dest

    log.info("Downloading Geofabrik PBF %s -> %s", url, dest)

    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0)) or None

        with open(dest, "wb") as f, tqdm(
            total=total,
            unit="B",
            unit_scale=True,
            desc=f"GET {safe_name}.osm.pbf",
        ) as bar:
            for chunk in r.iter_content(8192):
                if not chunk:
                    continue
                f.write(chunk)
                bar.update(len(chunk))

    log.info("Saved %s (%.1f MB)", dest, dest.stat().st_size / 1e6)
    return dest


def region_exists(region: str) -> bool:
    """Проверяет доступность региона на Geofabrik."""
    import requests

    url = f"https://download.geofabrik.de/{region}-latest.osm.pbf"
    try:
        r = requests.head(url, allow_redirects=True, timeout=10)
        return r.status_code == 200
    except Exception as exc:
        LOG.warning("HEAD %s failed: %s", url, exc)
        return False