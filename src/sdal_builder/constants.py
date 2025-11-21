# src/sdal_builder/constants.py

"""
SDAL constants, PID Sets, Markers, and Huffman table.
Stores both OEM (Denso) and Standard SDAL PID configurations.
"""

from dataclasses import dataclass

# ================================================================
# SYSTEM CONSTANTS & FLAGS
# ================================================================

EOF = "_EOF"
MAX_USHORT = 0xFFFF
PCL_HEADER_SIZE = 20

# Compression Flags
NO_COMPRESSION = 0
SZIP_COMPRESSION = 4
UNCOMPRESSED_FLAG = 0

# ================================================================
# PARCEL ID CONFIGURATION (PID SETS)
# ================================================================

@dataclass(frozen=True)
class PIDProfile:
    """Holds the Parcel ID mapping for a specific format mode."""
    GLB_MEDIA_HEADER: int = 0x0013  # 19 (Fixed by spec)
    LOCALE: int = 0x0064            # 100
    SYMBOL: int = 0x0065            # 101
    
    CARTO: int = 0
    ROUTING: int = 0
    BTREE: int = 0
    DENS: int = 0
    
    POI_NAME: int = 0
    POI_GEOM: int = 0
    POI_INDEX: int = 0
    
    NAV: int = 0
    CARTOTOP: int = 0
    KDTREE: int = 0

# --- OEM MODE (Denso / Mazda / Saab) ---
PIDS_OEM = PIDProfile(
    CARTO=2000,       # 0x07D0
    ROUTING=4000,     # 0x0FA0
    BTREE=3000,       # 0x0BB8
    DENS=3072,        # 0x0C00
    
    POI_NAME=1000,    # 0x03E8
    POI_GEOM=1001,    # 0x03E9
    POI_INDEX=1002,   # 0x03EA
    
    NAV=1,            # 0x0001
    CARTOTOP=2000,    # Same as Carto (Global context)
    KDTREE=8000       # 0x1F40
)

# --- STANDARD SDAL MODE (Spec 1.7 typical examples) ---
PIDS_STD = PIDProfile(
    CARTO=110,
    ROUTING=130,
    BTREE=122,
    DENS=140,
    
    POI_NAME=160,
    POI_GEOM=161,
    POI_INDEX=162,
    
    NAV=111,
    CARTOTOP=120,
    KDTREE=255
)

# ================================================================
# VERSIONS
# ================================================================

PSF_VERSION_MAJOR = 1
PSF_VERSION_MINOR = 7
PSF_VERSION_YEAR  = 1999

# ================================================================
# LOOKUP TABLES (MARKERS & REGIONS)
# ================================================================

# Markers for MTOC.SDL.
MARKER_TABLE = {
    "MAP":      b'\x01',
    "REGION":   b'\x02',
    "POINAMES": b'\x08',
    "POIGEOM":  b'\x08',
    "POI":      b'\x08',
    "DENS":     b'\x0D',
    "INDEX":    b'\x02',
    "INIT":     b'\x00',
    "AUDIO":    b'\x0A',
    "STUB":     b'\x00',
    "OTHER":    b'\x00'
}

# Region mapping (Geofabrik / ISO) -> Disc Group Name
GROUPS = {
    # EUROPE
    "BE": "BENELUX", "NL": "BENELUX", "LU": "BENELUX",
    "DK": "DENSWE", "SE": "DENSWE", "NO": "DENSWE", "FI": "DENSWE",
    "FR": "FRANCE",
    "DE": "GERMANY",
    "IT": "ITALY",
    "ES": "IBERIA", "PT": "IBERIA",
    "GB": "UK", "IE": "UK",
    "AT": "SWIAUS", "CH": "SWIAUS",
    "CZ": "CEUROPE", "PL": "CEUROPE", "SK": "CEUROPE", "HU": "CEUROPE",
    "GR": "GREBALK", "BG": "GREBALK", "RO": "GREBALK",
    "TR": "TURKEY", "RU": "RUSSIA", "UA": "RUSSIA",
    
    # NORTH AMERICA
    "US": "NAFTA", "CA": "NAFTA", "MX": "NAFTA",
    
    # SOUTH AMERICA
    "BR": "S_AMERICA", "AR": "S_AMERICA", "CO": "S_AMERICA", "CL": "S_AMERICA",
    
    # ASIA / OCEANIA
    "JP": "JAPAN", "KR": "KOREA", "CN": "CHINA", "IN": "INDIA",
    "AU": "AUS_NZ", "NZ": "AUS_NZ"
}

# Continent mapping for headers
CONTINENT_MAP = {
    "EUROPE": "EU",
    "NORTH-AMERICA": "NA",
    "SOUTH-AMERICA": "SA",
    "ASIA": "AS",
    "AFRICA": "AF",
    "AUSTRALIA": "AU"
}

# ================================================================
# HUFFMAN TABLE (FULL 256 ENTRIES)
# ================================================================

HUFFMAN_TABLE = {
    i: bin(i)[2:].zfill(8) for i in range(256)
}

HUFFMAN_TABLE.update({
    0: "000", 1: "001", 2: "0100", 3: "0101", 
    4: "01100", 5: "01101", 6: "01110", 7: "01111",
    32: "101", 33: "111"
})