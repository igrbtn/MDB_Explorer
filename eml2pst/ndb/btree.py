"""B-tree page structures for PST NDB layer.

The NDB uses two B-trees:
- NBT (Node B-Tree): maps NID → (bidData, bidSub, nidParent)
- BBT (Block B-Tree): maps BID → (ib, cb, cRef)

Each page is exactly 512 bytes. See [MS-PST] 2.2.2.7.
"""

import struct
from ..crc import compute_crc

PAGE_SIZE = 512
PAGE_TRAILER_SIZE = 16
PAGE_DATA_SIZE = PAGE_SIZE - PAGE_TRAILER_SIZE  # 496 bytes

# Page types
PTTYPE_BBT = 0x80
PTTYPE_NBT = 0x81
PTTYPE_FMP = 0x82
PTTYPE_FPM = 0x83
PTTYPE_AMAP = 0x84
PTTYPE_FMAP = 0x85
PTTYPE_FPMAP = 0x86

# Entry sizes
NBTENTRY_SIZE = 32  # Unicode NBT leaf entry
BBTENTRY_SIZE = 24  # Unicode BBT leaf entry
BTENTRY_SIZE = 24  # Interior node entry (both trees)

# Max entries per page
# 488 bytes usable for entries (496 - 8 bytes metadata)
ENTRIES_AREA = 488
MAX_NBT_LEAF = ENTRIES_AREA // NBTENTRY_SIZE  # 15
MAX_BBT_LEAF = ENTRIES_AREA // BBTENTRY_SIZE  # 20
MAX_INTERIOR = ENTRIES_AREA // BTENTRY_SIZE  # 20


def pack_nbt_entry(nid, bid_data, bid_sub=0, nid_parent=0):
    """Pack an NBTENTRY (32 bytes, Unicode).

    nid(8) + bidData(8) + bidSub(8) + nidParent(4) + dwPadding(4)
    """
    return struct.pack('<QQQ II', nid, bid_data, bid_sub, nid_parent, 0)


def pack_bbt_entry(bid, ib, cb, c_ref=2):
    """Pack a BBTENTRY (24 bytes, Unicode).

    BREF(16: bid+ib) + cb(2) + cRef(2) + dwPadding(4)
    """
    return struct.pack('<QQ HH I', bid, ib, cb, c_ref, 0)


def pack_bt_entry(key, bid, ib):
    """Pack a BTENTRY (24 bytes) for interior B-tree nodes.

    btkey(8) + BREF(16: bid + ib)
    """
    return struct.pack('<Q QQ', key, bid, ib)


def build_btpage(entries, ptype, bid, c_level=0):
    """Build a single 512-byte B-tree page.

    Args:
        entries: List of packed entry bytes (must fit in one page).
        ptype: Page type (PTTYPE_NBT or PTTYPE_BBT).
        bid: Page BID.
        c_level: Tree depth (0 = leaf).

    Returns:
        512 bytes of page data.
    """
    if not entries:
        if c_level > 0:
            entry_size = BTENTRY_SIZE
        elif ptype == PTTYPE_NBT:
            entry_size = NBTENTRY_SIZE
        else:
            entry_size = BBTENTRY_SIZE
    else:
        entry_size = len(entries[0])

    max_entries = ENTRIES_AREA // entry_size if entry_size > 0 else 0

    # Build the entries area (488 bytes for data + 8 bytes metadata)
    entries_data = b''.join(entries)
    # Pad entries to 488 bytes
    entries_area = entries_data[:ENTRIES_AREA].ljust(ENTRIES_AREA, b'\x00')

    # Metadata: cEnt(1) + cEntMax(1) + cbEnt(1) + cLevel(1) + dwPadding(4) = 8 bytes
    metadata = struct.pack('<BBBB I',
                           len(entries),  # cEnt
                           max_entries,  # cEntMax
                           entry_size,  # cbEnt
                           c_level,  # cLevel
                           0)  # dwPadding

    page_data = entries_area + metadata
    assert len(page_data) == 496, f"Page data is {len(page_data)} bytes, expected 496"

    # Compute CRC over the 496 bytes of page data
    crc = compute_crc(page_data)

    # Build page trailer
    trailer = struct.pack('<BB H I Q',
                          ptype,  # ptype
                          ptype,  # ptypeRepeat
                          0,  # wSig (always 0 for pages)
                          crc,  # dwCRC
                          bid)  # bid

    page = page_data + trailer
    assert len(page) == 512
    return page


def build_btree_pages(entries, ptype, alloc_bid_fn, alloc_offset_fn):
    """Build a potentially multi-level B-tree from entries.

    If entries fit in one leaf page, returns a single page.
    Otherwise splits into multiple leaf pages with an interior root.

    Args:
        entries: List of packed entry bytes, sorted by key.
        ptype: Page type (PTTYPE_NBT or PTTYPE_BBT).
        alloc_bid_fn: Callable that returns a new page BID.
        alloc_offset_fn: Callable(bid) that assigns and returns the file offset for a page.

    Returns:
        List of (bid, offset, page_bytes) for all pages.
        The last entry is the root page.
    """
    if not entries:
        entry_size = NBTENTRY_SIZE if ptype == PTTYPE_NBT else BBTENTRY_SIZE
    else:
        entry_size = len(entries[0])

    max_per_page = ENTRIES_AREA // entry_size

    if len(entries) <= max_per_page:
        # Single leaf page
        bid = alloc_bid_fn()
        offset = alloc_offset_fn(bid)
        page = build_btpage(entries, ptype, bid, c_level=0)
        return [(bid, offset, page)]

    # Multi-level: split entries into leaf pages, then build interior root
    pages = []
    interior_entries = []

    for i in range(0, len(entries), max_per_page):
        chunk = entries[i:i + max_per_page]
        leaf_bid = alloc_bid_fn()
        leaf_offset = alloc_offset_fn(leaf_bid)
        leaf_page = build_btpage(chunk, ptype, leaf_bid, c_level=0)
        pages.append((leaf_bid, leaf_offset, leaf_page))

        # Interior entry: key = first key in this leaf, BREF = (bid, offset)
        first_key = struct.unpack('<Q', chunk[0][:8])[0]
        interior_entries.append(pack_bt_entry(first_key, leaf_bid, leaf_offset))

    # Build interior root page
    root_bid = alloc_bid_fn()
    root_offset = alloc_offset_fn(root_bid)
    root_page = build_btpage(interior_entries, ptype, root_bid, c_level=1)
    pages.append((root_bid, root_offset, root_page))

    return pages
