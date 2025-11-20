# src/sdal_builder/validate_sdal_iso.py
#!/usr/bin/env python3
"""
validate_sdal_iso.py

Light‑weight structural validator for SDAL / PSF style ISO images.

It uses the canonical SDAL 1.7 PclHdr_t C-structure.
"""

import sys
import io
from typing import Tuple, Dict, Any

try:
    from pycdlib import PyCdlib
except ImportError:  # pragma: no cover - runtime dependency
    print("ERROR: pip install pycdlib", file=sys.stderr)
    sys.exit(1)

import struct

# Canonical PclHdr_t C-structure (20 bytes, Big-Endian)
# I H B B B B H H H H H
# ulParcelId (I), usPayloadSize (H), ucRegion (B), ucParcelType (B), ucParcelDesc (B), ucCompressType (B), usCmpDataSizeHi (H), usCmpDataSizeLo (H), usCmpDataUncompSize (H), usExtensionOffset (H)
_PCL_STRUCT = struct.Struct(">I H B B B B H H H H H")
_PCL_HEADER_LEN = 20

# Compression codes (для отчета)
COMPRESSION_CODES = {
    0x01: "NO_COMPRESSION",
    0x04: "SZIP_COMPRESSION",
    0x00: "UNKNOWN(0x00)",
}

def decode_parcelid(pid: int) -> Dict[str, Any]:
    """Декодирует 32-битный ParcelID_t."""
    return {
        "offset_units": (pid & 0x00FFFFFF),
        "size_index": (pid >> 24) & 0x7,
        "external_to_region": bool(pid & (1 << 27)),
        "redundancy": bool(pid & (1 << 28)),
    }


def validate_sdl_struct(filename: str, data: bytes, block_size: int = 2048) -> bool:
    """
    Validates the structure of a single SDL file (which contains one or more parcels).
    """
    offset = 0
    file_ok = True

    while offset < len(data):
        if len(data) - offset < _PCL_HEADER_LEN:
            print(f"  [ERROR] File end reached unexpectedly at offset {offset}. Remaining bytes: {len(data) - offset}")
            file_ok = False
            break

        header_bytes = data[offset : offset + _PCL_HEADER_LEN]
        
        try:
            (
                ulParcelId, usPayloadSize, ucRegion, ucParcelType, ucParcelDesc, ucCompressType, 
                usCmpDataSizeHi, usCmpDataSizeLo, usCmpDataUncompSize, usExtensionOffset
            ) = _PCL_STRUCT.unpack(header_bytes)
        except struct.error as e:
            print(f"  [ERROR] Cannot unpack PclHdr_t at offset {offset}: {e}")
            file_ok = False
            break

        pid_info = decode_parcelid(ulParcelId)
        
        # Combined compressed data size (ulCmpDataSize)
        ulCmpDataSize = (usCmpDataSizeHi << 16) | usCmpDataSizeLo
        
        # Report
        print(f"* {filename} @ {offset}: PID={ulParcelId} (OffsetUnits={pid_info['offset_units']})")
        print(f"  Region={ucRegion}, Type={ucParcelType}, Desc={ucParcelDesc}, Compress={COMPRESSION_CODES.get(ucCompressType, f'0x{ucCompressType:02x}')}")
        print(f"  CompSize={ulCmpDataSize}, UncompSize={usCmpDataUncompSize}, PayloadSizeH={usPayloadSize}")

        # Validation Logic
        parcel_ok = True
        
        # 1. Check if compressed payload fits in file
        expected_total_size = _PCL_HEADER_LEN + ulCmpDataSize
        if offset + expected_total_size > len(data):
            print(f"  [CRITICAL ERROR] Compressed payload ({ulCmpDataSize}B) extends beyond file end ({len(data)}B).")
            parcel_ok = False
        
        # 2. Check compressed size vs uncompressed size consistency (Simple check)
        if ucCompressType == 0x04 and ulCmpDataSize > usCmpDataUncompSize and usCmpDataUncompSize != 0:
             print(f"  [WARNING] Compressed size ({ulCmpDataSize}) > Uncompressed size ({usCmpDataUncompSize}) for SZIP parcel.")
        
        # 3. Check padding/advancement
        # Advance by header + compressed payload
        offset += _PCL_HEADER_LEN + ulCmpDataSize
        
        # Handle padding to a block boundary (e.g., 4096 bytes)
        padding = (offset % block_size)
        if padding != 0:
            pad_size = block_size - padding
            if pad_size > 0:
                print(f"  [INFO] Skipping {pad_size} bytes of alignment padding (to {block_size} boundary).")
            offset += pad_size
            
        if not parcel_ok:
            file_ok = False

    if file_ok:
        print(f"** {filename}: OK ({len(data)} bytes, {offset // block_size} blocks)")
    return file_ok


def main(iso_path: str) -> int:
    block_size = 4096
    
    try:
        iso = PyCdlib()
        iso.open(iso_path)
    except Exception as e:
        print(f"ERROR: Could not open ISO file {iso_path}: {e}", file=sys.stderr)
        return 1

    root = iso.list_children(iso_path="/")
    sdl_entries = [
        child
        for child in root
        if getattr(child, "file_identifier", b"").upper().endswith(b".SDL;1")
    ]

    if not sdl_entries:
        print("No SDL files found in ISO.")
        iso.close()
        return 0

    ok = True
    for child in sdl_entries:
        name_raw = child.file_identifier.decode("ascii", errors="ignore")
        name = name_raw.split(";")[0]

        buf = io.BytesIO()
        iso.get_file_from_iso_fp(buf, iso_path=f"/{name_raw}")
        data = buf.getvalue()

        if name.upper() in ("MTOC.SDL", "REGION.SDL", "REGIONS.SDL", "INIT.SDL"):
            print(f"* {name}: (skipped – special index file)")
            continue

        if not validate_sdl_struct(name, data, block_size):
            ok = False

    iso.close()
    return 0 if ok else 1


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python validate_sdal_iso.py <path_to_iso>", file=sys.stderr)
        sys.exit(1)
    sys.exit(main(sys.argv[1]))