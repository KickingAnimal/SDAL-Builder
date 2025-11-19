#!/usr/bin/env python3
"""
validate_sdal_iso.py

Light‑weight structural validator for SDAL / PSF style ISO images.

It understands the parcel header (PclHdr_t) used by encoder.py:

  uint32 parcelid;
  uint16 usParcelDesc;
  uint8  ucParcelType;
  uint8  region;
  uint8  bEndianSwap;
  uint8  ucCmpDataSizeHi;
  uint16 usCmpDataSizeLo;
  uint16 usCompressType;
  uint16 usCmpData;
  uint16 usCmpDataUncompSize;
  uint16 usExtensionOffset;

and performs the following checks:

  * walks all *.SDL files in the root of the ISO
  * for every non‑special SDL (not MTOC/REGION/REGIONS/INIT):
      - validates that the header fits in the file
      - validates that the compressed payload fits in the file
      - advances by either (header + payload) or usCmpDataUncompSize
        (to tolerate padding to a block boundary)
  * decodes the ParcelID_t fields (ext/redundancy/size_index/offset_units)
    for convenience in the report.
"""

import sys
import io
from typing import Tuple

try:
    from pycdlib import PyCdlib
except ImportError:  # pragma: no cover - runtime dependency
    print("ERROR: pip install pycdlib", file=sys.stderr)
    sys.exit(1)

import struct


# >I H B B B B H H H H H  == 20 bytes
# ИСПРАВЛЕНО: Сменено с Little-Endian ('<') на Big-Endian ('>') для соответствия builder'у
_PCL_STRUCT = struct.Struct(">I H B B B B H H H H H")
PCL_HEADER_SIZE = _PCL_STRUCT.size


def decode_parcelid(parcelid: int) -> Tuple[int, int, int, int]:
    """
    Decode a PSF‑style ParcelID_t into:

        (external, redundancy, size_index, offset_units)
    """
    parcelid &= 0xFFFFFFFF
    external = (parcelid >> 31) & 0x1
    redundancy = (parcelid >> 30) & 0x1
    size_index = (parcelid >> 24) & 0x3F
    offset_units = parcelid & 0xFFFFFF
    return external, redundancy, size_index, offset_units


def _iter_parcels(data: bytes):
    """
    Yield (parcel_no, header_tuple, payload_start, payload_end) for every
    parcel that appears structurally valid.

    header_tuple is the unpacked PclHdr_t:
      (parcelid, usParcelDesc, ucParcelType, region, bEndianSwap,
       ucCmpDataSizeHi, usCmpDataSizeLo, usCompressType,
       usCmpData, usCmpDataUncompSize, usExtensionOffset)
    """
    ptr = 0
    parcel_no = 0
    n = len(data)

    while ptr + PCL_HEADER_SIZE <= n:
        try:
            # Используем Big-Endian для распаковки
            header = _PCL_STRUCT.unpack_from(data, ptr)
        except struct.error:
            break

        (
            parcelid,
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
        ) = header

        if usCmpData < PCL_HEADER_SIZE:
            # Clearly invalid – payload cannot start before the header ends.
            yield parcel_no, header, None, None
            break

        # Reconstruct compressed size in bytes (cmp_bits includes all 24 bits)
        cmp_bits = ((ucCmpDataSizeHi << 16) | usCmpDataSizeLo) & 0xFFFFFF
        cmp_bytes = (cmp_bits + 7) // 8  # round up from bits to bytes

        payload_start = ptr + usCmpData
        payload_end = payload_start + cmp_bytes

        if payload_start < 0 or payload_end > n:
            # Declared payload would run out of file bounds.
            yield parcel_no, header, None, None
            break

        yield parcel_no, header, payload_start, payload_end

        # Minimal step = payload offset (usCmpData) + compressed payload length (cmp_bytes).
        min_step = usCmpData + cmp_bytes
        step = min_step

        # If usCmpDataUncompSize is large enough we interpret it as the
        # "on‑disc" size of the parcel including padding to a block
        # boundary (e.g., 4096 bytes).
        if usCmpDataUncompSize and usCmpDataUncompSize >= min_step:
            step = usCmpDataUncompSize

        ptr += step
        parcel_no += 1


def validate_sdl_struct(name: str, data: bytes) -> bool:
    """
    Validate a single SDL file (except for special index files).
    """
    ok = True
    print(f"* {name}: size={len(data)} bytes")

    for parcel_no, header, payload_start, payload_end in _iter_parcels(data):
        (
            parcelid,
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
        ) = header

        if payload_start is None:
            print(
                f"  Parcel {parcel_no}: FAIL – invalid header "
                f"(usCmpData={usCmpData}, usCmpDataUncompSize={usCmpDataUncompSize})"
            )
            ok = False
            break

        cmp_bits = ((ucCmpDataSizeHi << 16) | usCmpDataSizeLo) & 0xFFFFFF
        cmp_bytes = (cmp_bits + 7) // 8

        ext, red, sz_idx, off_units = decode_parcelid(parcelid)

        print(
            f"  Parcel {parcel_no}: OK "
            f"(parcelid=0x{parcelid:08X}, ext={ext}, red={red}, "
            f"sz_idx={sz_idx}, off_units={off_units}, "
            f"type={ucParcelType}, region={region_id}, "
            f"cmp_bytes={cmp_bytes}, cmp_type={usCompressType}, desc={usParcelDesc})"
        )

    return ok


def validate_sdal_iso_ext(path: str) -> bool:
    """
    Validate all SDL files inside the given ISO image.
    """
    iso = PyCdlib()
    iso.open(path)

    ok = True

    # Walk root directory, list all *.SDL files.
    root = iso.list_children(iso_path="/")
    sdl_entries = [
        child
        for child in root
        if getattr(child, "file_identifier", b"").upper().endswith(b".SDL;1")
    ]

    if not sdl_entries:
        print("No SDL files found in ISO.")
        iso.close()
        return False

    for child in sdl_entries:
        name_raw = child.file_identifier.decode("ascii", errors="ignore")
        # Strip ;1 version suffix.
        name = name_raw.split(";")[0]

        # Read file contents.
        buf = io.BytesIO()
        iso.get_file_from_iso_fp(buf, iso_path=f"/{name_raw}")
        data = buf.getvalue()

        # For now we skip structural validation of the index files themselves,
        # as their internal formats are handled separately by the builder:
        #  * MTOC.SDL
        #  * REGION.SDL
        #  * REGIONS.SDL
        #  * INIT.SDL
        #  * CARTOTOP.SDL (новый файл с нестандартным PID)
        if name.upper() in ("MTOC.SDL", "REGION.SDL", "REGIONS.SDL", "INIT.SDL", "CARTOTOP.SDL"):
            print(f"* {name}: (skipped – special index file)")
            continue

        if not validate_sdl_struct(name, data):
            ok = False

    iso.close()
    return ok


def main(argv=None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    if len(argv) != 1:
        print("Usage: validate_sdal_iso.py <path/to/sdal.iso>")
        return 1

    iso_path = argv[0]
    return 0 if validate_sdal_iso_ext(iso_path) else 2


if __name__ == "__main__":
    raise SystemExit(main())