"""Heap-on-Node (HN) implementation.

HN provides a variable-size allocator within a data block.
It's the foundation for Property Context and Table Context.

See [MS-PST] 2.3.1.
"""

import struct

# HN heap signature (always 0xEC for any HN block)
HN_SIG = 0xEC

# HN client signatures (bClientSig)
HN_CLIENT_TC = 0x7C  # Table Context
HN_CLIENT_BTH = 0xB5  # BTree-on-Heap
HN_CLIENT_PC = 0xBC  # Property Context

# HNHDR: 12 bytes
HNHDR_SIZE = 12

# HNPAGEMAP: 2 + 2 + (cAlloc+1)*2 bytes
# Minimum HNPAGEMAP: 2 bytes (cAlloc=0, cFree=0) + 2 bytes (rgibAlloc[0])

# Maximum data in a single HN page (first block)
# Block max = 8176, minus HNHDR(12), minus page map overhead
# Practical: ~8100 bytes for first page


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
    """Builds a Heap-on-Node data block.

    Usage:
        hn = HeapOnNode(client_sig=HN_CLIENT_BTH)
        hid1 = hn.allocate(some_data)
        hid2 = hn.allocate(more_data)
        raw_block = hn.build()
    """

    def __init__(self, client_sig=HN_CLIENT_BTH, user_root_hid=0):
        self.client_sig = client_sig
        self.user_root_hid = user_root_hid
        self._allocations = []  # list of bytes objects
        self._block_index = 0

    def allocate(self, data: bytes) -> int:
        """Allocate data on the heap and return its HID."""
        idx = len(self._allocations) + 1  # 1-based
        hid = make_hid(self._block_index, idx)
        self._allocations.append(data)
        return hid

    def set_user_root(self, hid):
        """Set the user root HID (typically the BTH header)."""
        self.user_root_hid = hid

    def build(self) -> bytes:
        """Build the raw data block containing the HN structure.

        Returns:
            Raw bytes for the data block (without block trailer).
        """
        # Calculate layout:
        # [HNHDR 12 bytes] [allocation 1] [allocation 2] ... [HNPAGEMAP]
        #
        # HNHDR:
        #   ibHnpm (2): offset of HNPAGEMAP from start of block
        #   bSig (1): HN_CLIENT_xxx
        #   bClientSig (1): client signature
        #   hidUserRoot (4): HID of user root
        #   dwFill (4): fill (0)
        #
        # HNPAGEMAP:
        #   cAlloc (2): number of allocations
        #   cFree (2): number of free entries
        #   rgibAlloc[cAlloc+1] (2 each): offsets of allocations

        num_alloc = len(self._allocations)

        # Calculate page map size
        pagemap_size = 4 + (num_alloc + 1) * 2  # cAlloc(2) + cFree(2) + (n+1)*2

        # Calculate data area: starts after HNHDR
        data_start = HNHDR_SIZE

        # Compute allocation offsets (relative to data_start)
        offsets = []
        current = 0
        for alloc in self._allocations:
            offsets.append(current)
            current += len(alloc)
        offsets.append(current)  # Final sentinel offset

        # Total data area
        data_area_size = current

        # Page map offset from start of block
        ib_hnpm = data_start + data_area_size

        # Build HNHDR (12 bytes)
        hnhdr = struct.pack('<H BB I I',
                            ib_hnpm,  # ibHnpm
                            HN_SIG,  # bSig (always 0xEC)
                            self.client_sig,  # bClientSig
                            self.user_root_hid,  # hidUserRoot
                            0)  # dwFill

        # Build data area
        data_area = b''.join(self._allocations)

        # Build HNPAGEMAP
        pagemap = struct.pack('<HH', num_alloc, 0)  # cAlloc, cFree
        for off in offsets:
            pagemap += struct.pack('<H', off + data_start)  # absolute offsets from block start

        # Assemble
        block_data = hnhdr + data_area + pagemap

        return block_data
