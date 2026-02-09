"""BTree-on-Heap (BTH) implementation.

BTH stores sorted key-value pairs inside a Heap-on-Node.
Used by Property Context for property storage.

See [MS-PST] 2.3.2.
"""

import struct
from .heap import HeapOnNode, HN_CLIENT_BTH

# BTHHEADER: 8 bytes
BTH_HEADER_SIZE = 8


def build_bth_data(entries, key_size=2, data_size=6):
    """Build BTH structures inside a HeapOnNode.

    For a simple single-level BTH (all data fits in one leaf page):
    1. Allocate BTHHEADER on heap
    2. Allocate leaf data records on heap

    Args:
        entries: List of (key_bytes, data_bytes) tuples, sorted by key.
        key_size: Size of each key in bytes (default 2 for property ID).
        data_size: Size of each data value in bytes (default 6 for PC entries).

    Returns:
        HeapOnNode with BTH data ready to build.
    """
    hn = HeapOnNode(client_sig=HN_CLIENT_BTH)

    if entries:
        # Build leaf record data
        leaf_data = b''
        for key, value in entries:
            assert len(key) == key_size, f"Key size mismatch: {len(key)} != {key_size}"
            assert len(value) == data_size, f"Data size mismatch: {len(value)} != {data_size}"
            leaf_data += key + value

        leaf_hid = hn.allocate(leaf_data)
    else:
        leaf_hid = 0  # Empty BTH

    # BTHHEADER: cbKey(1) + cbEnt(1) + bIdxLevels(1) + hidRoot(4)
    # Actually per spec: bType(1)=0xB5 + cbKey(1) + cbEnt(1) + bIdxLevels(1) + hidRoot(4) = 8 bytes
    bth_header = struct.pack('<BB BB I',
                             0xB5,  # bType
                             key_size,  # cbKey
                             data_size,  # cbEnt
                             0,  # bIdxLevels (0 = leaf only)
                             leaf_hid)  # hidRoot

    header_hid = hn.allocate(bth_header)
    hn.set_user_root(header_hid)

    return hn
