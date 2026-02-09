"""Heap-on-Node (HN) implementation.

HN provides a variable-size allocator within a data block.
It's the foundation for Property Context and Table Context.

Supports multi-page HN where allocations span multiple data blocks
connected via an XBLOCK data tree.

See [MS-PST] 2.3.1.
"""

import struct
from ..ndb.block import MAX_BLOCK_DATA

# HN heap signature (always 0xEC for any HN block)
HN_SIG = 0xEC

# HN client signatures (bClientSig)
HN_CLIENT_TC = 0x7C  # Table Context
HN_CLIENT_BTH = 0xB5  # BTree-on-Heap
HN_CLIENT_PC = 0xBC  # Property Context

# HNHDR: 12 bytes (page 0 only)
HNHDR_SIZE = 12

# HNPAGEHDR: 2 bytes (pages after page 0)
HNPAGEHDR_SIZE = 2

# Maximum individual allocation on an HN page per [MS-PST] 2.3.1
MAX_HN_ALLOC = 3580


def make_hid(block_index, index):
    """Create a Heap ID (HID).

    HID layout (32-bit):
    - bits 0-4: hidType (always 0 for HN allocations)
    - bits 5-15: hidIndex (1-based allocation index within the page)
    - bits 16-31: hidBlockIndex (0-based data block index)
    """
    return ((block_index & 0xFFFF) << 16) | ((index & 0x7FF) << 5) | 0


def hid_index(hid):
    return (hid >> 5) & 0x7FF


def hid_block_index(hid):
    return (hid >> 16) & 0xFFFF


class HeapOnNode:
    """Builds a Heap-on-Node data structure, potentially spanning multiple pages.

    Usage:
        hn = HeapOnNode(client_sig=HN_CLIENT_BTH)
        hid1 = hn.allocate(some_data)
        hid2 = hn.allocate(more_data)
        pages = hn.build()  # list of bytes, one per page
    """

    def __init__(self, client_sig=HN_CLIENT_BTH, user_root_hid=0):
        self.client_sig = client_sig
        self.user_root_hid = user_root_hid
        # Per-page allocation tracking: list of lists of bytes objects
        self._pages = [[]]
        self._current_page = 0

    def _page_header_size(self, page_index):
        """Header size for a given page (12 for page 0, 2 for subsequent)."""
        return HNHDR_SIZE if page_index == 0 else HNPAGEHDR_SIZE

    def _can_fit(self, page_index, allocs, new_data_len):
        """Check if adding new_data to a page with existing allocs fits."""
        header_sz = self._page_header_size(page_index)
        current_data = sum(len(a) for a in allocs)
        new_count = len(allocs) + 1
        # HNPAGEMAP: cAlloc(2) + cFree(2) + (cAlloc+1)*2 offsets
        pagemap_sz = 4 + (new_count + 1) * 2
        total = header_sz + current_data + new_data_len + pagemap_sz
        return total <= MAX_BLOCK_DATA

    def allocate(self, data: bytes) -> int:
        """Allocate data on the heap and return its HID."""
        page = self._current_page
        allocs = self._pages[page]

        if not self._can_fit(page, allocs, len(data)) and len(allocs) > 0:
            # Start a new page
            self._pages.append([])
            self._current_page += 1
            page = self._current_page
            allocs = self._pages[page]

        idx = len(allocs) + 1  # 1-based within this page
        hid = make_hid(page, idx)
        allocs.append(data)
        return hid

    def set_user_root(self, hid):
        """Set the user root HID (typically the BTH header)."""
        self.user_root_hid = hid

    def build(self) -> list:
        """Build the raw data blocks for all HN pages.

        Returns:
            List of bytes objects, one per page/block (without block trailers).
        """
        result = []
        for page_idx, allocations in enumerate(self._pages):
            if page_idx == 0:
                result.append(self._build_page0(allocations))
            else:
                result.append(self._build_page_n(allocations))
        return result

    def _build_page0(self, allocations):
        """Build page 0 with full HNHDR (12 bytes)."""
        num_alloc = len(allocations)
        data_start = HNHDR_SIZE

        # Compute allocation offsets (relative to data_start)
        offsets = []
        current = 0
        for alloc in allocations:
            offsets.append(current)
            current += len(alloc)
        offsets.append(current)  # Sentinel

        data_area_size = current
        ib_hnpm = data_start + data_area_size

        # HNHDR (12 bytes)
        hnhdr = struct.pack('<H BB I I',
                            ib_hnpm,
                            HN_SIG,
                            self.client_sig,
                            self.user_root_hid,
                            0)  # dwFill

        data_area = b''.join(allocations)

        # HNPAGEMAP
        pagemap = struct.pack('<HH', num_alloc, 0)
        for off in offsets:
            pagemap += struct.pack('<H', off + data_start)

        return hnhdr + data_area + pagemap

    def _build_page_n(self, allocations):
        """Build page N>0 with HNPAGEHDR (2 bytes)."""
        num_alloc = len(allocations)
        data_start = HNPAGEHDR_SIZE

        offsets = []
        current = 0
        for alloc in allocations:
            offsets.append(current)
            current += len(alloc)
        offsets.append(current)

        data_area_size = current
        ib_hnpm = data_start + data_area_size

        # HNPAGEHDR (2 bytes): just ibHnpm
        header = struct.pack('<H', ib_hnpm)

        data_area = b''.join(allocations)

        pagemap = struct.pack('<HH', num_alloc, 0)
        for off in offsets:
            pagemap += struct.pack('<H', off + data_start)

        return header + data_area + pagemap
