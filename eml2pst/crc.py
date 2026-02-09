"""MS-PST CRC-32 calculation.

Implements the CRC algorithm specified in [MS-PST] 5.3.
This is a standard CRC-32 with polynomial 0xEDB88320 (reflected).
"""

import struct

# CRC-32 lookup table (256 entries, polynomial 0xEDB88320)
_CRC_TABLE = None


def _build_crc_table():
    global _CRC_TABLE
    if _CRC_TABLE is not None:
        return
    table = []
    for i in range(256):
        crc = i
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xEDB88320
            else:
                crc >>= 1
        table.append(crc & 0xFFFFFFFF)
    _CRC_TABLE = table


def compute_crc(data: bytes) -> int:
    """Compute CRC-32 per MS-PST specification.

    Args:
        data: Bytes to compute CRC over.

    Returns:
        32-bit CRC value.
    """
    _build_crc_table()
    crc = 0
    for b in data:
        crc = (_CRC_TABLE[(crc ^ b) & 0xFF] ^ (crc >> 8)) & 0xFFFFFFFF
    return crc
