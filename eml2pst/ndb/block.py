"""Data block structures for PST NDB layer.

Blocks are the fundamental data storage units. Each block is 64-byte aligned
and ends with a BLOCKTRAILER. Maximum raw data per block is 8176 bytes
(8192 - 16 byte trailer).

See [MS-PST] 2.2.2.8.
"""

import struct
from ..crc import compute_crc

BLOCK_ALIGN = 64
MAX_BLOCK_DATA = 8176  # 8192 - 16 (trailer size)
BLOCK_TRAILER_SIZE = 16


def block_signature(ib: int, bid: int) -> int:
    """Compute block signature per [MS-PST] 5.5.

    Implements the ComputeSig algorithm from the specification:
    ib ^= bid; ib >>= 16; bid >>= 16; ib ^= bid; return ib & 0xFFFF
    """
    ib ^= bid
    ib >>= 16
    bid >>= 16
    ib ^= bid
    return ib & 0xFFFF


def pack_block(data: bytes, bid: int, ib: int) -> bytes:
    """Pack a data block with trailer and 64-byte alignment.

    Args:
        data: Raw data bytes (must be <= MAX_BLOCK_DATA).
        bid: Block ID for this block.
        ib: File offset where this block will be written.

    Returns:
        64-byte aligned block bytes with trailer.
    """
    assert len(data) <= MAX_BLOCK_DATA, f"Block data too large: {len(data)}"

    cb = len(data)
    w_sig = block_signature(ib, bid)
    crc = compute_crc(data)

    # BLOCKTRAILER: cb(2) + wSig(2) + dwCRC(4) + bid(8) = 16 bytes
    trailer = struct.pack('<HHI Q', cb, w_sig, crc, bid)

    # Total = data + padding + trailer, aligned to 64 bytes
    # The total block size (data + trailer) is padded to 64-byte boundary
    total_raw = len(data) + BLOCK_TRAILER_SIZE
    # Round up to 64 byte boundary
    total_aligned = ((total_raw + BLOCK_ALIGN - 1) // BLOCK_ALIGN) * BLOCK_ALIGN
    padding_size = total_aligned - total_raw

    return data + (b'\x00' * padding_size) + trailer


def block_total_size(data_len: int) -> int:
    """Calculate total on-disk size of a block with given data length."""
    total_raw = data_len + BLOCK_TRAILER_SIZE
    return ((total_raw + BLOCK_ALIGN - 1) // BLOCK_ALIGN) * BLOCK_ALIGN
