"""XBLOCK (extended data tree block) for PST NDB layer.

An XBLOCK connects multiple data blocks into a data tree, used when
a node's data exceeds a single block (e.g. multi-page Heap-on-Node).

See [MS-PST] 2.2.2.8.3.2.
"""

import struct

XBLOCK_BTYPE = 0x01
XBLOCK_LEVEL = 0x01  # Level 1 = references data blocks (level 0)


def build_xblock(bid_list, total_data_bytes):
    """Build an XBLOCK data tree block.

    Args:
        bid_list: List of data block BIDs (leaf blocks) in order.
        total_data_bytes: Total bytes of raw data across all referenced blocks.

    Returns:
        Raw bytes for the XBLOCK block data (before block packing/trailer).
    """
    cEnt = len(bid_list)

    # Header: btype(1) + cLevel(1) + cEnt(2) + lcbTotal(4) = 8 bytes
    header = struct.pack('<BB H I',
                         XBLOCK_BTYPE,
                         XBLOCK_LEVEL,
                         cEnt,
                         total_data_bytes)

    # BID array: 8 bytes each
    bid_data = b''
    for bid in bid_list:
        bid_data += struct.pack('<Q', bid)

    return header + bid_data
