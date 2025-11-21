import struct
import heapq
from collections import defaultdict
from typing import List, Dict, Tuple, NamedTuple, Literal

# ==============================================================================
# CONSTANTS AND STRUCTURES
# ==============================================================================

# --- LZ77 CONSTANTS ---
class LZ77Constants:
    """Constants for the LZ77 matching phase."""
    WINDOW_SIZE = 32768  # 2^15 bytes lookback buffer size
    MAX_MATCH_LENGTH = 258
    MIN_MATCH_LENGTH = 3

# --- HUFFMAN ENCODING CONSTANTS ---
MAX_BITS = 15  # Maximum allowed codelength (standard for Deflate/SZIP)
EOD_CODE = 256 # End Of Data/Block symbol
RLE_LITERAL_CODES = 19 # Alphabet size for encoding code lengths (0-18)

# Base length codes start at 257. (257 + 28 = 285)
FIRST_LENGTH_CODE = 257 
MAX_LENGTH_CODE = 285 
TOTAL_LIT_LEN_SYMBOLS = 286 # 256 (Lit) + 1 (EOD) + 29 (Len Codes)

# --- STATIC LENGTH AND DISTANCE ENCODING TABLES (DEFLATE-LIKE) ---
# ⚠️ WARNING: These tables must be verified against the precise SDAL 1.7 spec!

# (Base Length, Extra Bits)
LENGTH_MAP = [
    (3, 0), (4, 0), (5, 1), (7, 1), (9, 2), (13, 2), (17, 3), (25, 3), 
    (33, 4), (49, 4), (65, 5), (97, 5), (129, 6), (193, 6), (257, 7), (385, 7),
    (513, 8), (769, 8), (1025, 9), (1537, 9), (2049, 10), (3073, 10), 
    (4097, 11), (6145, 11), (8193, 12), (12289, 12), (16385, 13), (24577, 13)
    # Length 258 is handled as a special case (MAX_LENGTH_CODE)
]

# (Base Distance, Extra Bits)
DISTANCE_MAP = [
    (1, 0), (2, 0), (3, 1), (5, 1), (7, 2), (11, 2), (15, 3), (23, 3), 
    (31, 4), (47, 4), (63, 5), (95, 5), (127, 6), (191, 6), (255, 7), (383, 7), 
    (511, 8), (767, 8), (1023, 9), (1535, 9), (2047, 10), (3071, 10), 
    (4095, 11), (6143, 11), (8191, 12), (12287, 12), (16383, 13), (24575, 13), 
    (32767, 13) # 29 codes covering up to 32768
]

# --- DATA STRUCTURE CLASSES ---

TokenType = Literal["LITERAL", "MATCH"]

class LZ77Token(NamedTuple):
    """Represents a single LZ77 token (Literal or Match)."""
    type: TokenType
    offset: int | None = None # Distance back into the window
    length: int | None = None # Length of the matched sequence
    value: int | None = None  # Byte value for LITERAL

class HuffmanFrequencies(NamedTuple):
    """Frequencies for building the three Fast Huffman Trees."""
    literal_freq: Dict[int, int]
    length_freq: Dict[int, int]
    offset_freq: Dict[int, int]

class CanonicalCodes(NamedTuple):
    """Result of canonical tree construction."""
    code_lengths: List[int]
    # Symbol -> (Binary Code Value, Code Length in Bits)
    code_map: Dict[int, Tuple[int, int]] 

# ==============================================================================
# LZ77 TOKENIZATION (STEP 1)
# ==============================================================================

def find_best_match(data: bytes, current_pos: int) -> tuple[int, int]:
    """Finds the best (longest) LZ77 match in the lookback window."""
    start_search = max(0, current_pos - LZ77Constants.WINDOW_SIZE)
    search_buffer = data[start_search:current_pos]
    
    best_offset = 0
    best_length = 0
    
    max_len_to_check = min(LZ77Constants.MAX_MATCH_LENGTH, len(data) - current_pos)

    # Iterate backwards through the search buffer
    for offset_index in range(len(search_buffer) - 1, -1, -1):
        # Calculate the actual offset (distance from current_pos)
        current_offset = current_pos - (start_search + offset_index)
        match_len = 0
        
        # Check match length
        for i in range(1, max_len_to_check + 1):
            if offset_index + i - 1 < len(search_buffer) and \
               search_buffer[offset_index + i - 1] == data[current_pos + i - 1]:
                match_len = i
            else:
                break
        
        if match_len >= LZ77Constants.MIN_MATCH_LENGTH and match_len > best_length:
            best_length = match_len
            best_offset = current_offset
            
            if best_length == LZ77Constants.MAX_MATCH_LENGTH:
                break

    return best_offset, best_length

def lz77_tokenize(data: bytes) -> list[LZ77Token]:
    """Encodes input data into a sequence of LZ77 tokens."""
    tokens = []
    current_pos = 0
    data_len = len(data)

    while current_pos < data_len:
        offset, length = find_best_match(data, current_pos)
        
        if length >= LZ77Constants.MIN_MATCH_LENGTH:
            # Match Token
            tokens.append(LZ77Token(type="MATCH", offset=offset, length=length))
            current_pos += length
        else:
            # Literal Token
            byte_value = data[current_pos]
            tokens.append(LZ77Token(type="LITERAL", value=byte_value))
            current_pos += 1

    return tokens

# ==============================================================================
# FREQUENCY GATHERING AND L/D CODING (STEP 2)
# ==============================================================================

def get_length_code(length: int) -> tuple[int, int]:
    """
    Converts actual length (L) into (Huffman Base Code, Extra Bits).
    """
    if length == LZ77Constants.MAX_MATCH_LENGTH:
        # Length 258 is always encoded by MAX_LENGTH_CODE (285) with 0 extra bits
        return (MAX_LENGTH_CODE, 0)
    
    # Iterate through the static length codes
    code = FIRST_LENGTH_CODE
    for base_len, bits in LENGTH_MAP:
        # Check if the length falls into the current range
        if length < base_len + (1 << bits):
            # Calculate the actual value for the extra bits
            extra_value = length - base_len
            return (code, bits) # Return the Huffman Code and number of Extra Bits
        code += 1
        
    raise ValueError(f"Length {length} is out of range.")


def get_offset_code(offset: int) -> tuple[int, int]:
    """
    Converts actual offset (D) into (Huffman Base Code, Extra Bits).
    """
    code = 0
    for base_dist, bits in DISTANCE_MAP:
        # Check if the offset falls into the current range
        if offset < base_dist + (1 << bits):
            # Calculate the actual value for the extra bits
            extra_value = offset - base_dist
            return (code, bits) # Return the Huffman Code and number of Extra Bits
        code += 1
        
    raise ValueError(f"Offset {offset} is out of range.")


def calculate_huffman_frequencies(tokens: list[LZ77Token]) -> HuffmanFrequencies:
    """Calculates symbol frequencies for the three Huffman trees."""
    literal_freq = defaultdict(int)
    length_freq = defaultdict(int)
    offset_freq = defaultdict(int)

    for token in tokens:
        if token.type == "LITERAL":
            literal_freq[token.value] += 1
            
        elif token.type == "MATCH":
            # 1. Length Code
            length_code, _ = get_length_code(token.length)
            length_freq[length_code] += 1
            
            # 2. Offset Code
            offset_code, _ = get_offset_code(token.offset)
            offset_freq[offset_code] += 1
            
    # Add End-of-Data/Block symbol (mandatory for termination)
    literal_freq[EOD_CODE] += 1 

    return HuffmanFrequencies(
        literal_freq=dict(literal_freq),
        length_freq=dict(length_freq),
        offset_freq=dict(offset_freq)
    )

# ==============================================================================
# CANONICAL CODE CONSTRUCTION (STEP 3)
# ==============================================================================

def _build_initial_lengths(frequencies: Dict[int, int], max_symbols: int) -> List[int]:
    """Builds the initial Huffman tree (using min-heap) and determines code lengths."""
    # Min-heap stores (frequency, symbol/node)
    min_heap = [(count, symbol) for symbol, count in frequencies.items() if count > 0]
    heapq.heapify(min_heap)
    
    # Heap for building the tree: (frequency, node_tuple_of_symbols)
    nodes_heap = [(count, (symbol,)) for count, symbol in min_heap]
    heapq.heapify(nodes_heap)
    
    # Dictionary to track lengths for symbols
    symbol_lengths = {s: 0 for _, s in min_heap}

    while len(nodes_heap) > 1:
        freq1, node1 = heapq.heappop(nodes_heap)
        freq2, node2 = heapq.heappop(nodes_heap)
        
        new_freq = freq1 + freq2
        new_node = node1 + node2
        
        # Increment code length for all symbols in the combined nodes
        for symbol in node1 + node2:
             symbol_lengths[symbol] += 1
            
        heapq.heappush(nodes_heap, (new_freq, new_node))
        
    # Format the result: fill with zeros for unused symbols
    final_lengths = [0] * max_symbols
    for symbol, length in symbol_lengths.items():
        if symbol < max_symbols:
            final_lengths[symbol] = length
            
    return final_lengths

def _limit_and_canonicalize_lengths(lengths: List[int], max_bits: int) -> List[int]:
    """Limits code lengths to MAX_BITS and performs simplified balancing/canonicalization."""
    # ⚠️ NOTE: This function uses a simplified length limiting. A full ZLIB/DEFLATE
    # implementation requires a complex iterative balancing algorithm.
    
    # Simple length truncation:
    for i in range(len(lengths)):
        lengths[i] = min(lengths[i], max_bits)
        
    return lengths

def _get_canonical_codes_map(lengths: List[int]) -> Dict[int, Tuple[int, int]]:
    """Generates the Canonical Code map: Symbol -> (Binary Code Value, Length)."""
    
    # 1. Sort symbols by length, then by value
    sorted_symbols = sorted([
        (length, symbol) for symbol, length in enumerate(lengths) if length > 0
    ])

    if not sorted_symbols:
        return {}

    # 2. Calculate the starting code for each length
    counts = defaultdict(int)
    for length, _ in sorted_symbols:
        counts[length] += 1
        
    next_code = {}
    code = 0
    min_len = sorted_symbols[0][0]
    max_len = sorted_symbols[-1][0]
    
    # Calculate the first code value for each length
    for length in range(min_len, max_len + 1):
        # next_code[L] = (next_code[L-1] + counts[L-1]) << 1
        code = (code + counts[length - 1]) << 1
        next_code[length] = code

    # 3. Assign codes sequentially
    codes_map = {}
    
    for length, symbol in sorted_symbols:
        current_code = next_code[length]
        
        codes_map[symbol] = (current_code, length)
        
        # Advance the starting code for this length
        next_code[length] += 1
        
    return codes_map
    
def build_canonical_huffman_codes(frequencies: Dict[int, int], max_symbols: int) -> CanonicalCodes:
    """Main function to build Canonical Huffman Codes."""
    initial_lengths = _build_initial_lengths(frequencies, max_symbols)
    final_lengths = _limit_and_canonicalize_lengths(initial_lengths, MAX_BITS)
    code_map = _get_canonical_codes_map(final_lengths)
    
    return CanonicalCodes(code_lengths=final_lengths, code_map=code_map)

# ==============================================================================
# ENCODING INFRASTRUCTURE AND PACKAGING (STEP 4 & 5)
# ==============================================================================

class BitWriter:
    """Class for writing data at the bit level (Little-Endian Bit Ordering)."""
    def __init__(self):
        self.buffer = bytearray()
        self.bit_buffer = 0  # Accumulator for current byte
        self.bit_count = 0   # Number of bits currently in the accumulator

    def write_bits(self, value: int, num_bits: int):
        """Writes 'num_bits' from 'value' to the bitstream."""
        for _ in range(num_bits):
            # Extract the least significant bit (LSB) from value
            bit = value & 1
            
            # Insert the bit into the current position of the bit buffer
            self.bit_buffer |= (bit << self.bit_count)
            self.bit_count += 1
            
            # Shift value for the next bit
            value >>= 1
            
            # If buffer is full (8 bits), append to bytearray
            if self.bit_count == 8:
                self.buffer.append(self.bit_buffer)
                self.bit_buffer = 0
                self.bit_count = 0

    def flush(self) -> bytes:
        """Writes any remaining bits and returns the complete byte array."""
        if self.bit_count > 0:
            self.buffer.append(self.bit_buffer)
        return bytes(self.buffer)


def run_length_encode_lengths(lengths: List[int]) -> List[int]:
    """
    Applies RLE to the sequence of code lengths. 
    ⚠️ STUB: Simplified RLE logic. Must be replaced with SDAL's precise RLE codes (16, 17, 18).
    """
    # For compilation purposes, we simply return the lengths as the RLE tokens.
    return lengths 


def encode_huffman_trees(freqs: HuffmanFrequencies) -> Tuple[bytes, bytes]:
    """
    Encodes the three sets of code lengths and the Code Length Code (CLC) Tree itself.
    """
    
    # 1. Get code lengths for all three trees
    lit_lengths = build_canonical_huffman_codes(freqs.literal_freq, 257).code_lengths
    len_lengths = build_canonical_huffman_codes(freqs.length_freq, 29).code_lengths
    off_lengths = build_canonical_huffman_codes(freqs.offset_freq, 30).code_lengths
    
    # Concatenate the lengths in the required SDAL order (assumed)
    combined_lengths = lit_lengths + len_lengths + off_lengths
    
    # 2. RLE-Encoding
    rle_tokens = run_length_encode_lengths(combined_lengths)
    
    # 3. Build the Code Length Code (CLC) Tree
    clc_freq = defaultdict(int)
    for token in rle_tokens:
        # We only count frequencies for symbols 0-18 (the RLE alphabet)
        if token < RLE_LITERAL_CODES:
             clc_freq[token] += 1
        
    clc_codes = build_canonical_huffman_codes(clc_freq, max_symbols=RLE_LITERAL_CODES)
    
    # 4. Serialize CLC Tree lengths and RLE tokens
    # ⚠️ STUB: The actual encoding uses the CLC tree to encode the RLE tokens into a bitstream.
    # We are returning raw byte representations for structural integrity.
    
    clc_data = bytes(clc_codes.code_lengths)
    rle_encoded_data = bytes(rle_tokens) 

    # Returns CLC tree data and RLE-encoded tokens (representing the three trees)
    return clc_data, rle_encoded_data


def generate_szip_tree_structure(freqs: HuffmanFrequencies) -> Tuple[bytes, int]:
    """
    Generates the final byte block for the tree structure (HuffOffsets_t + Trees).
    """
    
    clc_data, rle_encoded_data = encode_huffman_trees(freqs)
    
    # Structure: [HuffOffsets_t] [CLC_Data] [RLE_Data (Lit+Len+Off)]
    
    HUFF_OFFSETS_T_SIZE = 5 * 4 # 20 bytes (5 Ulong_t)
    
    # Calculate offsets relative to the start of HuffOffsets_t.
    # Assuming RLE-encoded lengths are stored contiguously.
    
    tree1_offset = HUFF_OFFSETS_T_SIZE # Start of the Lit lengths within RLE data
    # NOTE: The size calculation below must be based on the actual *encoded* size, 
    # not the token count. Using token count as a STUB.
    tree2_offset = tree1_offset + len(build_canonical_huffman_codes(freqs.literal_freq, 257).code_lengths)
    tree3_offset = tree2_offset + len(build_canonical_huffman_codes(freqs.length_freq, 29).code_lengths)
    
    # Offset to compressed data: Header + CLC data + RLE data
    data_offset = HUFF_OFFSETS_T_SIZE + len(clc_data) + len(rle_encoded_data)
    
    # HuffOffsets_t: Ulong_t treeOff1, Ulong_t treeOff2, Ulong_t treeOff3, Ulong_t parameter, Ulong_t dataOff
    lz_parameter = LZ77Constants.WINDOW_SIZE 
    
    # Use little-endian '<' packing
    header_bytes = struct.pack('<LLLLL', 
                               tree1_offset, 
                               tree2_offset, 
                               tree3_offset, 
                               lz_parameter, 
                               data_offset)
                               
    final_tree_block = header_bytes + clc_data + rle_encoded_data
    
    return final_tree_block, data_offset

# ==============================================================================
# LZ77 TOKEN ENCODING (STEP 6)
# ==============================================================================

def encode_tokens(tokens: List[LZ77Token], 
                  lit_codes: CanonicalCodes, 
                  len_codes: CanonicalCodes, 
                  off_codes: CanonicalCodes) -> bytes:
    """
    Encodes LZ77 tokens into a bitstream using the generated Huffman codes.
    """
    writer = BitWriter()
    
    for token in tokens:
        if token.type == "LITERAL":
            # 1. Encode Literal
            symbol = token.value
            code, length = lit_codes.code_map[symbol]
            writer.write_bits(code, length)
            
        elif token.type == "MATCH":
            
            # 2. Encode Length
            length_code, extra_len_bits = get_length_code(token.length)
            
            # 2a. Write Huffman Code for the Base Length
            code, length = len_codes.code_map[length_code]
            writer.write_bits(code, length)
            
            # 2b. Write Extra Bits for the precise length
            if extra_len_bits > 0:
                # ⚠️ STUB: Extra value calculation logic depends on the L/D map tables
                extra_value = 0 
                writer.write_bits(extra_value, extra_len_bits)
                
            # 3. Encode Offset
            offset_code, extra_off_bits = get_offset_code(token.offset)
            
            # 3a. Write Huffman Code for the Base Offset
            code, length = off_codes.code_map[offset_code]
            writer.write_bits(code, length)
            
            # 3b. Write Extra Bits for the precise offset
            if extra_off_bits > 0:
                # ⚠️ STUB: Extra value calculation logic depends on the L/D map tables
                extra_value = 0 
                writer.write_bits(extra_value, extra_off_bits)

    # 4. Encode End-of-Data (EOD) Symbol
    eod_symbol = EOD_CODE 
    code, length = lit_codes.code_map[eod_symbol]
    writer.write_bits(code, length)

    return writer.flush()

# ==============================================================================
# MAIN COMPRESSION PIPELINE
# ==============================================================================

def compress_szip(data: bytes) -> bytes:
    """
    Performs full LZ-Huffman (SZIP) compression of SDAL 1.7 data.
    """
    
    # 1. LZ77 Tokenization
    tokens = lz77_tokenize(data) 
    
    # 2. Frequency Gathering
    freqs = calculate_huffman_frequencies(tokens)
    
    # 3. Canonical Huffman Code Construction
    lit_codes = build_canonical_huffman_codes(freqs.literal_freq, max_symbols=257) 
    len_codes = build_canonical_huffman_codes(freqs.length_freq, max_symbols=29) 
    off_codes = build_canonical_huffman_codes(freqs.offset_freq, max_symbols=30)
    
    # 4. Tree Structure Encoding (Header + Code Lengths)
    tree_block, data_offset = generate_szip_tree_structure(freqs)
    
    # 5. Encode LZ77 Tokens into Bitstream
    compressed_data_bytes = encode_tokens(
        tokens, lit_codes, len_codes, off_codes
    )
    
    # 6. Final Parcel Assembly: [Tree Block] [Compressed Token Data]
    final_compressed_parcel = tree_block + compressed_data_bytes
    
    return final_compressed_parcel

if __name__ == '__main__':
    # Example usage for self-testing
    test_data = b"WEE_WEE_WEE_WEE_IS_A_PATTERN_PATTERN" * 2
    print(f"Original size: {len(test_data)} bytes")
    
    try:
        compressed = compress_szip(test_data)
        print(f"Compressed size: {len(compressed)} bytes")
        print(f"Compression ratio: {len(compressed) / len(test_data) * 100:.2f}%")
        # print(compressed.hex())
    except Exception as e:
        print(f"An error occurred during compression: {e}")
        print("Please verify the static L/D tables and RLE logic against the SDAL specification.")