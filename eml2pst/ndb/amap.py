"""Allocation Map (AMap) page for PST NDB layer.

The AMap tracks which bytes in the PST file are allocated.
Each bit in the AMap represents one 64-byte slot.
See [MS-PST] 2.2.2.7.4.
"""

import struct
from ..crc import compute_crc
from .btree import PAGE_SIZE, PTTYPE_AMAP

# AMap covers file bytes starting after the header
# First AMap starts at offset 0x4400 and covers bytes from 0x4400
# Each AMap page has 496 data bytes = 3968 bits = covers 3968 * 64 = 253952 bytes
AMAP_DATA_SIZE = 496  # 512 - 16 (page trailer)
AMAP_BITS = AMAP_DATA_SIZE * 8  # 3968 bits
AMAP_COVERAGE = AMAP_BITS * 64  # 253952 bytes per AMap page

# First AMap offset in file
FIRST_AMAP_OFFSET = 0x4400


def build_amap_page(allocated_ranges, amap_offset, file_base_offset, bid):
    """Build a 512-byte AMap page.

    Args:
        allocated_ranges: List of (offset, size) tuples for allocated regions.
        amap_offset: File offset of this AMap page.
        file_base_offset: File offset that bit 0 of this AMap represents.
        bid: Page BID for this AMap.

    Returns:
        512 bytes of AMap page data.
    """
    bitmap = bytearray(AMAP_DATA_SIZE)

    for alloc_offset, alloc_size in allocated_ranges:
        # Calculate which bits this allocation covers
        if alloc_offset < file_base_offset:
            continue
        rel_offset = alloc_offset - file_base_offset
        start_slot = rel_offset // 64
        # Round up the size to 64-byte slots
        num_slots = (alloc_size + 63) // 64

        for slot in range(start_slot, start_slot + num_slots):
            if 0 <= slot < AMAP_BITS:
                byte_idx = slot // 8
                bit_idx = slot % 8
                bitmap[byte_idx] |= (1 << bit_idx)

    # Mark the AMap page itself as allocated
    amap_rel = (amap_offset - file_base_offset) // 64
    amap_slots = PAGE_SIZE // 64  # 8 slots
    for slot in range(amap_rel, amap_rel + amap_slots):
        if 0 <= slot < AMAP_BITS:
            byte_idx = slot // 8
            bit_idx = slot % 8
            bitmap[byte_idx] |= (1 << bit_idx)

    # Compute CRC and build page trailer
    crc = compute_crc(bytes(bitmap))
    trailer = struct.pack('<BB H I Q',
                          PTTYPE_AMAP,  # ptype
                          PTTYPE_AMAP,  # ptypeRepeat
                          0,  # wSig
                          crc,  # dwCRC
                          bid)  # bid

    page = bytes(bitmap) + trailer
    assert len(page) == 512
    return page


def compute_amap_free(allocated_ranges, amap_offset, file_base_offset):
    """Compute free space in an AMap page's coverage area."""
    total_allocated = 0
    coverage_end = file_base_offset + AMAP_COVERAGE

    for alloc_offset, alloc_size in allocated_ranges:
        if alloc_offset >= coverage_end or alloc_offset + alloc_size <= file_base_offset:
            continue
        # Clip to coverage area
        start = max(alloc_offset, file_base_offset)
        end = min(alloc_offset + alloc_size, coverage_end)
        total_allocated += end - start

    # AMap page itself is allocated
    total_allocated += PAGE_SIZE

    return AMAP_COVERAGE - total_allocated
