"""Subnode BTree (SL/SI blocks) for PST NDB layer.

Subnodes store child data blocks within a parent node.
Used for folder TCs (hierarchy, contents, assoc) and message TCs (recipients, attachments).

The subnode BTree is stored as a data block (not a page).
See [MS-PST] 2.2.2.8.3.
"""

import struct

# Block types for subnode blocks
SLBLOCK_TYPE = 0x02  # Subnode Leaf Block

# SLENTRY (24 bytes, Unicode): nid(8) + bidData(8) + bidSub(8)
SLENTRY_SIZE = 24


def pack_sl_entry(nid, bid_data, bid_sub=0):
    """Pack a Subnode Leaf Entry (24 bytes, Unicode)."""
    return struct.pack('<QQQ', nid, bid_data, bid_sub)


def build_sl_block(entries):
    """Build a Subnode Leaf Block.

    Args:
        entries: List of (nid, bid_data, bid_sub) tuples.

    Returns:
        Raw bytes for the SL block data (before block packing/trailer).
    """
    # SLBLOCK header: btype(1) + cLevel(1) + cEnt(2) + dwPadding(4) = 8 bytes
    header = struct.pack('<BB H I',
                         SLBLOCK_TYPE,  # btype = SL
                         0,  # cLevel = 0 (leaf)
                         len(entries),  # cEnt
                         0)  # dwPadding

    entry_data = b''
    for nid, bid_data, bid_sub in entries:
        entry_data += pack_sl_entry(nid, bid_data, bid_sub)

    return header + entry_data
