import sys
import struct
import io
import logging
import os
import re
from typing import Tuple, Set, Optional

try:
    from pycdlib import PyCdlib
except ImportError:
    print("❌ ERROR: The 'pycdlib' library was not found.")
    print("Install it: pip install pycdlib")
    sys.exit(1)

try:
    from tqdm import tqdm
except ImportError:
    # TQDM Mock if not installed
    class TQDM_Mock:
        def __init__(self, *args, **kwargs): pass
        def update(self, *args, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, exc_type, exc_val, exc_tb): pass
    tqdm = TQDM_Mock

# --- Logging Setup ---
class Color:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'

logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger("VALIDATOR")

def fail(msg): log.error(f"{Color.RED}[FAIL] {msg}{Color.RESET}")
def pass_check(msg): log.info(f"{Color.GREEN}[OK] {msg}{Color.RESET}")
def warn(msg): log.warning(f"{Color.YELLOW}[WARN] {msg}{Color.RESET}")
def info(msg): log.info(f"{Color.BLUE}[INFO] {msg}{Color.RESET}") 

# --- Constants ---
BLOCK_SIZE = 4096
PCL_HEADER_LEN = 20 # Length of the Parcel Header

# PIDs
GLB_MEDIA_HEADER_PID = 0x13
NAV_PARCEL_ID        = 0x0001

# --- Global State for Validation Mode ---
VALIDATION_MODE = 'OEM' 

# --- Constants for INIT.SDL Header checks ---
OEM_INIT_HEADER_ID = 0x0002A000  # Typical OEM ID (e.g., Saab)
SDAL_ASCII_HEADER_ID = 0x5344414C # 'SDAL' (Observed in user's log)
SDAL_STRICT_ASCII_START = b' ' # Strict SDAL 1.7 standard start byte

# -----------------------------------------------------------------------------
# ISO HELPER
# -----------------------------------------------------------------------------

def get_file_content(iso, filename) -> bytes:
    """Retrieves file content from ISO image."""
    candidates = [f"/{filename}", f"/{filename};1"]
    for path in candidates:
        try:
            with io.BytesIO() as f:
                iso.get_file_from_iso_fp(f, iso_path=path)
                return f.getvalue()
        except Exception:
            continue
    raise FileNotFoundError(f"File {filename} not found in ISO root.")

# -----------------------------------------------------------------------------
# STRUCTURAL HELPERS
# -----------------------------------------------------------------------------

def read_parcel_header_fast(view: memoryview, offset: int) -> Optional[Tuple[int, int, int]]:
    """
    Reads a 20-byte parcel header and returns:
    (8-bit PID, 32-bit ulParcelId, Total Length of parcel including header)
    """
    if offset + PCL_HEADER_LEN > len(view):
        return None
    
    # >I H B B B B H H H H H
    ul_parcel_id, _, _, _, _, _, size_hi, size_lo, _, _, _ = \
        struct.unpack_from(">IHBBBBHHHHH", view, offset)

    # 1. Decode Parcel ID (8 bits)
    pid = ul_parcel_id & 0xFF
    
    # 2. Calculate Data Size (24 bits combined)
    data_size = (size_hi << 16) | size_lo
    
    total_len = PCL_HEADER_LEN + data_size
    return pid, ul_parcel_id, total_len


# -----------------------------------------------------------------------------
# MODE-AWARE CHECKERS
# -----------------------------------------------------------------------------

def _check_padding(filename: str, view: memoryview, start_offset: int, end_offset: int, is_final_junk: bool = False) -> bool:
    """Checks for non-zero padding/junk based on the global mode."""
    global VALIDATION_MODE
    
    if start_offset >= end_offset:
        return True

    padding_to_check = view[start_offset:end_offset]
    
    # Check if any byte is non-zero
    if any(b != 0 for b in padding_to_check):
        if VALIDATION_MODE == 'SDAL17':
            # Strict mode: FAIL on non-zero padding
            fail(f"{filename}: Non-zero padding/junk found from 0x{start_offset:X} to 0x{end_offset:X}. (SDAL 1.7 requires zero padding)")
            return False 
        else: # OEM Mode
            # OEM mode: WARN, but PASS
            warn_msg = "Trailing junk found." if is_final_junk else "Non-zero padding/junk found."
            warn(f"{filename}: {warn_msg} (OEM is tolerant, PASS). Length: {end_offset - start_offset}. Start: {padding_to_check[:16].hex()}...")
            return True 
    
    # Always PASS for zero padding
    return True

def validate_parcel_chain(filename: str, data: bytes) -> Tuple[bool, Set[int]]:
    """Walks the SDL file parcel by parcel, checking structure and padding."""
    file_size = len(data)
    view = memoryview(data)
    offset = 0
    parcel_count = 0
    pids_found = set()
    is_valid = True
    
    desc = f"⛓️  Checking {filename} ({VALIDATION_MODE})"
    with tqdm(total=file_size, unit='B', unit_scale=True, desc=desc, leave=True, mininterval=0.1) as pbar:
        
        while offset < file_size:
            parcel_start = -1
            
            # 1. Search for the next valid parcel header within the current block
            search_limit = min(file_size, offset + BLOCK_SIZE) 
            
            for i in range(offset, search_limit):
                if i + PCL_HEADER_LEN > file_size:
                    break
                    
                temp_res = read_parcel_header_fast(view, i)
                
                if temp_res:
                    pid, ul_parcel_id, total_len = temp_res
                    
                    # Check for valid PID and boundary check
                    if pid != 0 and (i + total_len <= file_size):
                        parcel_start = i
                        break
            
            # 2. Process found or missing parcel
            if parcel_start != -1:
                # Found the next parcel
                skipped_bytes = parcel_start - offset
                
                if skipped_bytes > 0:
                    # Check skipped bytes (junk/padding)
                    if not _check_padding(filename, view, offset, parcel_start):
                        is_valid = False
                        break # Critical failure in SDAL17 mode
                
                offset = parcel_start 
                
                # Reread header
                pid, ul_parcel_id, total_len = read_parcel_header_fast(view, offset)
                
                # Check for ISO block padding (if total size is not BLOCK_SIZE aligned)
                pad = (-total_len) & (BLOCK_SIZE - 1)
                padding_start = offset + total_len
                
                if pad > 0:
                    # Check padding bytes
                    if not _check_padding(filename, view, padding_start, padding_start + pad):
                        is_valid = False
                        break # Critical failure in SDAL17 mode

                step = total_len + pad
                pbar.update(step)
                offset += step
                pids_found.add(pid)
                parcel_count += 1

            else:
                # 3. Could not find a valid header: must be EOF or critical junk.
                if offset < file_size:
                    remaining = file_size - offset
                    
                    # Check remaining data as final junk
                    if not _check_padding(filename, view, offset, file_size, is_final_junk=True):
                        is_valid = False
                    
                    pbar.update(remaining)
                
                break # Exit the loop
            
    # Final check reporting
    if is_valid:
        # Check for critical NAV Parcel (PID 1) (optional informational check)
        if NAV_PARCEL_ID in pids_found and filename.upper().endswith('1.SDL'):
            pass_check(f"{filename}: Chain OK. {parcel_count} parcels. Critical NAV Parcel (PID {NAV_PARCEL_ID}) present.")
        else:
            pass_check(f"{filename}: Chain OK. {parcel_count} parcels. PIDs: {sorted(list(pids_found))}")
    else:
        fail(f"** {filename}: Structure FAILED in {VALIDATION_MODE} mode.")
        
    return is_valid, pids_found


# -----------------------------------------------------------------------------
# FILE SPECIFIC VALIDATORS (Mode-aware INIT.SDL)
# -----------------------------------------------------------------------------

def validate_init_sdl(iso) -> bool:
    """Checks the INIT.SDL header and attempts to parse the file list."""
    global VALIDATION_MODE
    info("📄 Checking INIT.SDL...")
    try:
        content = get_file_content(iso, "INIT.SDL")
    except Exception:
        fail("INIT.SDL missing!")
        return False

    is_ok = True
    if len(content) < 4:
        fail("INIT.SDL is too short for any header.")
        return False
        
    ul_parcel_id = struct.unpack_from(">I", content, 0)[0]

    # --- 1. Header Check ---
    if VALIDATION_MODE == 'OEM':
        if ul_parcel_id in (OEM_INIT_HEADER_ID, SDAL_ASCII_HEADER_ID):
             pass_check(f"INIT.SDL header: Detected OEM/SDAL ID (0x{ul_parcel_id:X}). Allowed in OEM mode.")
        elif content.startswith(SDAL_STRICT_ASCII_START):
             pass_check("INIT.SDL header: Detected ASCII (' ') start. Allowed in OEM mode.")
        else:
             warn(f"INIT.SDL header: Unknown ID (0x{ul_parcel_id:X}). Allowed in OEM mode.")
    else: # SDAL17 mode (Strict)
        if content.startswith(SDAL_STRICT_ASCII_START):
            pass_check("INIT.SDL header: Strict SDAL 1.7 ASCII (' ') start.")
        else:
            fail(f"INIT.SDL header invalid. Expected ASCII (' ') start. Found binary ID: 0x{ul_parcel_id:X}")
            is_ok = False
    
    # --- 2. File List Parsing FIX (using regex) ---
    try:
        # Decode the whole content, ignoring non-ASCII characters, which is common in OEM files.
        text_full = content.decode('ascii', 'ignore')
        
        # Regex to find file names (1-15 chars followed by .SDL)
        file_name_pattern = re.compile(r'([A-Z0-9]{1,15}\.SDL)', re.IGNORECASE)
        
        found_files = set()
        
        # Search for all matches in the text content
        for match in file_name_pattern.finditer(text_full):
            found_files.add(match.group(1).upper().split(';')[0])
        
        # Remove known non-file list entries that might get picked up (like CARTOTOP, KDTREE, etc.)
        # This list comes from analyzing the INIT.SDL structure (benelux_tl/tm, italy_tm, etc.)
        metadata_tags_to_remove = ["TL", "TM", "V", "R", "K", "C", "Z"] 

        # Filter out potential false positives like BENELUX_TL, FRANCE_TM etc.
        filtered_files = {f for f in found_files if not any(f.endswith(tag) for tag in metadata_tags_to_remove)}
        
        if not filtered_files:
            warn("INIT.SDL: Could not parse file list.")
        else:
            info(f"INIT references {len(filtered_files)} files: {sorted(list(filtered_files))}")
            
    except Exception as e:
        warn(f"Error parsing INIT.SDL footer: {e}")
        
    return is_ok

def validate_cartotop(iso):
    """Checks existence and simple bounds structure of CARTOTOP.SDL."""
    info("🌍 Checking CARTOTOP.SDL...")
    try:
        data = get_file_content(iso, "CARTOTOP.SDL")
    except:
        fail("CARTOTOP.SDL missing!")
        return

    if len(data) < 38:
        fail("CARTOTOP.SDL too small.")
        return

    try:
        # PclHdr_t (20 bytes) + DBRect_t (16 bytes) + H (2 bytes count) = 38 bytes
        chunk = data[20:38]
        min_lon, min_lat, max_lon, max_lat, count = struct.unpack(">iiiiH", chunk)
        pass_check(f"CARTOTOP Bounds: ({min_lat},{min_lon}) to ({max_lat},{max_lon}). Regions: {count}")
    except Exception as e:
        fail(f"CARTOTOP parse error: {e}")

def validate_kdtree(iso):
    """Checks existence and simple bounds structure of KDTREE.SDL."""
    info("🌳 Checking KDTREE.SDL...")
    try:
        data = get_file_content(iso, "KDTREE.SDL")
    except:
        fail("KDTREE.SDL missing.")
        return

    PCL_HEADER_LEN = 20
    IDXPCL_HEADER_LEN = 28
    
    if len(data) < PCL_HEADER_LEN + IDXPCL_HEADER_LEN:
        warn("KDTREE.SDL is too small to contain IDxPclHdr_t.")
        return

    try:
        # IDxPclHdr_t starts after PclHdr_t (20 bytes)
        idx_data = data[PCL_HEADER_LEN:PCL_HEADER_LEN + IDXPCL_HEADER_LEN]
        # Format: H H I I i i H H H H (28 bytes)
        _, _, _, _, min_lat, min_lon, _, _, _, _ = struct.unpack(">HHIIiiHHHH", idx_data)
        pass_check(f"KDTREE Valid Bounds: {min_lat}, {min_lon}")
    except Exception as e:
        warn(f"KDTREE bounds check failed: {e}")

def validate_mtoc(iso):
    """Checks existence and reports entry count for MTOC.SDL."""
    info("📋 Checking MTOC.SDL...")
    try:
        data = get_file_content(iso, "MTOC.SDL")
    except:
        fail("MTOC.SDL missing.")
        return

    # MTOC entries are 64 bytes each
    count = len(data) // 64
    if count == 0:
        fail("MTOC is empty.")
        return
    pass_check(f"MTOC has {count} entries.")


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def main():
    global VALIDATION_MODE
    
    if len(sys.argv) < 2:
        print("Usage: python3 validate_sdal_iso.py <path_to_iso> [mode]")
        print("Modes: 'SDAL17' (strict standard), 'OEM' (tolerant/OEM-specific checks). Default: OEM")
        sys.exit(1)

    iso_path = sys.argv[1]
    
    # 1. Check and set optional mode argument
    if len(sys.argv) > 2:
        mode_arg = sys.argv[2].upper()
        if mode_arg in ('SDAL17', 'OEM'):
            VALIDATION_MODE = mode_arg
            info(f"Validation mode set to: {VALIDATION_MODE}")
        else:
            warn(f"Unknown mode: {mode_arg}. Using default mode: '{VALIDATION_MODE}'.")
    else:
         info(f"Using default validation mode: {VALIDATION_MODE}")


    if not os.path.exists(iso_path):
        fail(f"File not found: {iso_path}")
        sys.exit(1)

    info(f"Opening ISO: {iso_path}")
    iso = PyCdlib()
    try:
        iso.open(iso_path)
    except Exception as e:
        fail(f"ISO Error: {e}")
        sys.exit(1)

    # 2. List Root Files (Fixed: uses .file_identifier())
    sdl_entries = []
    try:
        for child in iso.list_children(iso_path='/'):
            # Must call .file_identifier() as a function
            if child.file_identifier() not in [b'.', b'..']:
                if child.file_identifier().decode('ascii', errors='ignore').upper().endswith(".SDL;1"):
                    sdl_entries.append(child)
    except Exception as e:
        fail(f"Cannot list root directory: {e}")
        iso.close()
        sys.exit(1)

    # 3. Global Validators
    all_files_ok = validate_init_sdl(iso) 
    validate_mtoc(iso)
    validate_cartotop(iso)
    validate_kdtree(iso)

    # 4. Check Map Files
    for child in sdl_entries:
        name_raw = child.file_identifier().decode("ascii", errors="ignore")
        name = name_raw.split(";")[0]
        
        name_upper = name.upper()

        # Filter 1: Skip critical index/meta files
        if name_upper in (
            "MTOC.SDL", "REGION.SDL", "REGIONS.SDL", "INIT.SDL", 
            "CARTOTOP.SDL", "KDTREE.SDL",
        ):
            info(f"* {name}: (Structural validation skipped. Index/Meta file.)")
            continue
            
        # Filter 2: Skip Density (DENS), Font (*F.SDL), Media (*M.SDL), Data (*D.SDL) files
        if name_upper.startswith("DENS") or name_upper.endswith(("F.SDL", "M.SDL", "D.SDL")):
             info(f"* {name}: (Structural validation skipped. Media/Density file.)")
             continue
        
        # Read file contents.
        buf = io.BytesIO()
        try:
            iso.get_file_from_iso_fp(buf, iso_path=f"/{name_raw}")
        except Exception as e:
            fail(f"Could not read {name}: {e}")
            all_files_ok = False
            continue
            
        data = buf.getvalue()

        # Check map structure
        is_valid, _ = validate_parcel_chain(name, data)
        if not is_valid:
            all_files_ok = False

    iso.close()
    
    final_status = "PASSED" if all_files_ok else "FAILED"
    info(f"*** ISO VALIDATION COMPLETE: {final_status} in {VALIDATION_MODE} mode. ***")
    sys.exit(0 if all_files_ok else 1)

if __name__ == "__main__":
    main()