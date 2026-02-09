"""PST file header (Unicode format, 564 bytes).

Implements the HEADER structure from [MS-PST] 2.2.2.6.
Unicode PST: wVer >= 23, 8-byte BIDs and IBs.
"""

import struct
from ..crc import compute_crc

# Header constants
MAGIC = b'\x21\x42\x44\x4E'  # "!BDN"
MAGIC_CLIENT = b'\x53\x4D'  # "SM"
WVER_UNICODE = 23
WVER_CLIENT = 19
BPLATFORM_CREATE = 0x01  # Windows
BPLATFORM_ACCESS = 0x01

HEADER_SIZE = 564  # Total header bytes written (padded to 564 for alignment)
CRYPT_NONE = 0x00
CRYPT_PERMUTE = 0x01

# BREF: 16 bytes (bid:8 + ib:8)
BREF_FMT = '<QQ'
BREF_SIZE = 16

# ROOT structure: 72 bytes (Unicode)
ROOT_SIZE = 72


def pack_bref(bid, ib):
    """Pack a BREF structure (16 bytes): BID + IB."""
    return struct.pack(BREF_FMT, bid, ib)


def pack_root(file_eof, ib_amap_last, cb_amap_free, cb_pmap_free,
              bref_nbt, bref_bbt, f_amap_valid=1):
    """Pack the ROOT structure (72 bytes).

    Args:
        file_eof: Total PST file size.
        ib_amap_last: Offset of last AMap page.
        cb_amap_free: Free bytes in all AMaps.
        cb_pmap_free: Free bytes in all PMaps.
        bref_nbt: (bid, ib) tuple for NBT root page.
        bref_bbt: (bid, ib) tuple for BBT root page.
        f_amap_valid: 1 if AMap is valid.
    """
    data = struct.pack('<I', 0)  # dwReserved
    data += struct.pack('<Q', file_eof)
    data += struct.pack('<Q', ib_amap_last)
    data += struct.pack('<Q', cb_amap_free)
    data += struct.pack('<Q', cb_pmap_free)
    data += pack_bref(*bref_nbt)
    data += pack_bref(*bref_bbt)
    data += struct.pack('<BBH', f_amap_valid, 0, 0)
    assert len(data) == ROOT_SIZE
    return data


def build_header(root_data, bid_next_p, bid_next_b, unique=1,
                 crypt_method=CRYPT_NONE):
    """Build a complete 564-byte Unicode PST header.

    Args:
        root_data: 72-byte ROOT structure.
        bid_next_p: Next page BID to allocate.
        bid_next_b: Next data block BID to allocate.
        unique: Monotonically increasing unique value.
        crypt_method: Encryption method (0=none).

    Returns:
        564 bytes of header data.
    """
    buf = bytearray(564)

    # Magic and version fields
    struct.pack_into('<4s', buf, 0x00, MAGIC)
    # dwCRCPartial placeholder at 0x04 (filled later)
    struct.pack_into('<2s', buf, 0x08, MAGIC_CLIENT)
    struct.pack_into('<H', buf, 0x0A, WVER_UNICODE)
    struct.pack_into('<H', buf, 0x0C, WVER_CLIENT)
    struct.pack_into('<B', buf, 0x0E, BPLATFORM_CREATE)
    struct.pack_into('<B', buf, 0x0F, BPLATFORM_ACCESS)
    # dwReserved1, dwReserved2 at 0x10, 0x14 = 0
    # bidUnused at 0x18 = 0

    struct.pack_into('<Q', buf, 0x20, bid_next_p)
    struct.pack_into('<I', buf, 0x28, unique)

    # rgnid[32] at 0x2C - NID counters per type (128 bytes)
    # Initialize to reasonable starting values
    # Index 0 (NID_TYPE_NONE) = 0
    # We set all to 0 initially; the PST file builder manages NID allocation
    # rgnid is 32 x 4 bytes = 128 bytes at 0x2C..0xAB

    # qwUnused at 0xAC = 0 (8 bytes)

    # ROOT at 0xB4 (72 bytes)
    buf[0xB4:0xB4 + ROOT_SIZE] = root_data

    # dwAlign at 0xFC = 0

    # rgbFM at 0x100: deprecated free map, 128 bytes of 0xFF
    buf[0x100:0x180] = b'\xFF' * 128

    # rgbFP at 0x180: deprecated free page map, 128 bytes of 0xFF
    buf[0x180:0x200] = b'\xFF' * 128

    # bSentinel at 0x200 = 0x80
    struct.pack_into('<B', buf, 0x200, 0x80)

    # bCryptMethod at 0x201
    struct.pack_into('<B', buf, 0x201, crypt_method)

    # rgbReserved at 0x202 = 0 (2 bytes)

    # bidNextB at 0x204
    struct.pack_into('<Q', buf, 0x204, bid_next_b)

    # dwCRCFull at 0x20C (filled below)
    # rgbReserved2 at 0x210 (3 bytes) = 0
    # bReserved at 0x213 = 0
    # rgbReserved3 at 0x214 (32 bytes) = 0

    # Compute CRCs
    # dwCRCPartial: CRC of 471 bytes starting at offset 0x08
    crc_partial = compute_crc(bytes(buf[0x08:0x08 + 471]))
    struct.pack_into('<I', buf, 0x04, crc_partial)

    # dwCRCFull: CRC of 516 bytes starting at offset 0x08
    crc_full = compute_crc(bytes(buf[0x08:0x08 + 516]))
    struct.pack_into('<I', buf, 0x20C, crc_full)

    return bytes(buf)
