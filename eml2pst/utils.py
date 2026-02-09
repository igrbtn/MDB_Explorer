"""Utility functions for PST file creation."""

import struct
from datetime import datetime, timezone

# Windows FILETIME epoch: January 1, 1601
_FILETIME_EPOCH = datetime(1601, 1, 1, tzinfo=timezone.utc)
_TICKS_PER_SECOND = 10_000_000  # 100-nanosecond intervals


def datetime_to_filetime(dt: datetime) -> int:
    """Convert a Python datetime to a Windows FILETIME (64-bit integer).

    FILETIME = number of 100-nanosecond intervals since January 1, 1601 UTC.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = dt - _FILETIME_EPOCH
    return int(delta.total_seconds() * _TICKS_PER_SECOND)


def filetime_now() -> int:
    """Return current time as a Windows FILETIME."""
    return datetime_to_filetime(datetime.now(timezone.utc))


def pack_filetime(ft: int) -> bytes:
    """Pack a FILETIME as 8 bytes little-endian."""
    return struct.pack('<Q', ft)


def encode_unicode(s: str) -> bytes:
    """Encode a string as UTF-16LE with null terminator (for PT_UNICODE)."""
    return s.encode('utf-16-le') + b'\x00\x00'


def encode_string8(s: str) -> bytes:
    """Encode a string as UTF-8 with null terminator (for PT_STRING8)."""
    return s.encode('utf-8') + b'\x00'


def align(value: int, boundary: int) -> int:
    """Round up value to the next multiple of boundary."""
    remainder = value % boundary
    if remainder == 0:
        return value
    return value + (boundary - remainder)


def pad_to(data: bytes, boundary: int, fill: int = 0x00) -> bytes:
    """Pad data to the next multiple of boundary bytes."""
    padded_len = align(len(data), boundary)
    return data + bytes([fill]) * (padded_len - len(data))


def make_entry_id(nid: int) -> bytes:
    """Create a minimal PST internal entry ID from a NID.

    PST entry IDs are 24 bytes:
    - 4 bytes flags (0)
    - 16 bytes provider UID (store record key)
    - 4 bytes NID

    For internal references we use a simplified form.
    """
    return struct.pack('<I', 0) + b'\x00' * 16 + struct.pack('<I', nid)
