#!/usr/bin/env python3
import sys, io, zlib, json

try:
    import bitstruct
except ImportError:
    print("ERROR: pip install bitstruct")
    sys.exit(1)

try:
    from pycdlib import PyCdlib
except ImportError:
    print("ERROR: pip install pycdlib")
    sys.exit(1)

FMT    = 'u16u32u32u16u16u8u8'
HDR_LEN = bitstruct.calcsize(FMT) // 8

def get_root_sdl_files(iso):
    recs = iso.list_children(iso_path='/')
    return [
        r.file_identifier().decode().rstrip(";1")
        for r in recs
        if r.is_file() and r.file_identifier().decode().upper().endswith('.SDL;1')
    ]

def read_iso_file(iso, fname):
    buf = io.BytesIO()
    iso.get_file_from_iso_fp(buf, iso_path=f"/{fname};1" if not fname.endswith(";1") else f"/{fname}")
    return buf.getvalue()

def read_mtoc(mtoc_bytes):
    try:
        d = json.loads(mtoc_bytes.rstrip(b"\0"))
        names = [e['name'] for e in d['files']]
        return names
    except Exception as e:
        print(f"  [ERROR] Failed to parse MTOC.SDL as JSON: {e}")
        return []

def read_region_tbl(region_bytes):
    # Table of little-endian 32-bit offsets
    if len(region_bytes) < 4:
        return []
    count = len(region_bytes) // 4
    offsets = [int.from_bytes(region_bytes[i*4:(i+1)*4], "little") for i in range(count)]
    return offsets

def validate_sdal_iso_ext(iso_path):
    iso = PyCdlib()
    iso.open(iso_path)
    ok = True

    # --- Gather files
    sdl_files = get_root_sdl_files(iso)
    print(f"\nFiles in ISO root: {sdl_files}")

    # --- MTOC.SDL check
    mtoc_bytes = read_iso_file(iso, "MTOC.SDL")
    mtoc_names = read_mtoc(mtoc_bytes)
    print(f"\nMTOC.SDL files: {mtoc_names}")

    # 1. Every root SDL file should be in MTOC.SDL (except MTOC.SDL itself, REGION.SDL, REGIONS.SDL)
    missing_in_mtoc = sorted(set(sdl_files) - set(mtoc_names))
    extra_in_mtoc   = sorted(set(mtoc_names) - set(sdl_files))
    if missing_in_mtoc:
        print("  [FAIL] Files in ISO but not in MTOC.SDL:", missing_in_mtoc)
        ok = False
    if extra_in_mtoc:
        print("  [FAIL] Files in MTOC.SDL but not in ISO:", extra_in_mtoc)
        ok = False

    # 2. REGION.SDL & REGIONS.SDL checks
    for idx_file in ["REGION.SDL", "REGIONS.SDL"]:
        try:
            idx_bytes = read_iso_file(iso, idx_file)
            offsets = read_region_tbl(idx_bytes)
            if len(offsets) < len(mtoc_names):
                print(f"  [WARN] {idx_file} has fewer offsets than files in MTOC.SDL ({len(offsets)} vs {len(mtoc_names)})")
            elif len(offsets) > len(mtoc_names):
                print(f"  [WARN] {idx_file} has more offsets than files in MTOC.SDL ({len(offsets)} vs {len(mtoc_names)})")
            else:
                print(f"  {idx_file} offset count matches MTOC.SDL ({len(offsets)} files)")
        except Exception as e:
            print(f"  [ERROR] Failed to read {idx_file}: {e}")
            ok = False

    # 3. Parcel validation (existing logic, improved file name printing)
    for fname in sdl_files:
        print(f"\n=== Validating {fname}.SDL ===")
        data = read_iso_file(iso, fname + ".SDL")
        ptr, parcel_no, total = 0, 1, len(data)
        while ptr < total:
            if total - ptr < HDR_LEN:
                print(f"  Parcel {parcel_no}: FAIL – incomplete header ({total-ptr} bytes left)")
                ok = False
                break
            header = data[ptr:ptr+HDR_LEN]
            pid, length, crc, *_ = bitstruct.unpack(FMT, header)
            start, end = ptr + HDR_LEN, ptr + HDR_LEN + length
            if end > total:
                print(f"  Parcel {parcel_no}: FAIL – length mismatch (hdr={length}, remain={total-start})")
                ok = False
                break
            payload = data[start:end]
            calc_crc = zlib.crc32(payload) & 0xFFFFFFFF
            if calc_crc != crc:
                print(f"  Parcel {parcel_no}: FAIL – CRC mismatch (hdr={crc:08x}, calc={calc_crc:08x})")
                ok = False
                break
            print(f"  Parcel {parcel_no}: OK (pid={pid}, size={length})")
            ptr, parcel_no = end, parcel_no + 1

    iso.close()
    return ok

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: validate_sdal_iso.py <path/to/sdal.iso>")
        sys.exit(1)
    sys.exit(0 if validate_sdal_iso_ext(sys.argv[1]) else 2)
