from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, Set, Union

import geopandas as gpd
from .sdal_osmium_stream import extract_driving_roads   # road helper

LOG = logging.getLogger(__name__)

# ────────────────────────────────────────────────────────────────
# Road network
# ────────────────────────────────────────────────────────────────
def load_road_network(pbf_path: Union[str, Path]) -> gpd.GeoDataFrame:
    """
    Return a GeoDataFrame of drivable roads from an OSM .pbf extract.
    Uses the streaming _RoadHandler in sdal_osmium_stream.py.
    """
    LOG.info("Loading drivable road network …")
    roads_df = extract_driving_roads(str(pbf_path))
    LOG.info("Loaded %d road geometries", len(roads_df))
    return roads_df

# ────────────────────────────────────────────────────────────────
# POIs via Pyrosm
# ────────────────────────────────────────────────────────────────
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
    Suppresses pandas fragmentation warnings at the source.
    Consolidates the DataFrame in-place afterwards for safety.
    """
    from pyrosm import OSM
    import warnings
    import pandas as pd

    # Suppress pandas PerformanceWarning from Pyrosm's prepare_geodataframe
    warnings.filterwarnings(
        "ignore",
        category=pd.errors.PerformanceWarning,
        module="pyrosm.pois"
    )

    tags = set(poi_tags) if poi_tags else DEFAULT_POI_TAGS
    tag_filter = {k: True for k in tags}

    logger.info("Loading POIs with Pyrosm.get_pois …")
    poi_gdf = OSM(str(pbf_path)).get_pois(custom_filter=tag_filter)

    # Defragment the DataFrame to ensure no future warnings
    poi_gdf._consolidate_inplace()  # type: ignore[attr-defined]

    logger.info("Loaded %d POIs after filtering", len(poi_gdf))
    return poi_gdf


def load_poi_data(
    pbf_path: Union[str, Path],
    logger: logging.Logger = LOG,
    poi_tags: Iterable[str] | None = None,
) -> gpd.GeoDataFrame:
    """
    Main entry used by src/sdal_builder/main.py.
    Forwards to load_all_pois, eliminating any dependency on extract_pois/_POIHandler.
    """
    return load_all_pois(pbf_path, logger, poi_tags)
