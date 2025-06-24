
"""Density tile merger – combines multiple DENSCxx*.SDL fragments produced
by the builder into the two‑file SDAL parcel layout expected by old head‑units.

Very naive: the *first* tile becomes header (meta‑info), the rest is concatenated
into the body.  Works because each tile already contains full parcel headers
— only the payloads are joined.
"""

from typing import List, Tuple


def merge_tiles(tiles: List[bytes]) -> Tuple[bytes, bytes]:
    if not tiles:
        raise ValueError("No tiles to merge")
    if len(tiles) == 1:
        # nothing to merge – header + empty body
        return tiles[0], b""
    header = tiles[0]
    body = b"".join(tiles[1:])
    return header, body
