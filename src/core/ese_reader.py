#!/usr/bin/env python3
"""
ESE Database Reader for Exchange EDB Files

Based on Windows ESE (Extensible Storage Engine) documentation.
Provides high-level interface for reading Exchange mailbox data.
"""

import struct
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass
from enum import IntEnum

try:
    import pyesedb
    HAS_PYESEDB = True
except ImportError:
    HAS_PYESEDB = False


class ESEColumnType(IntEnum):
    """ESE Column Types (JET_coltyp)"""
    Nil = 0
    Bit = 1
    UnsignedByte = 2
    Short = 3
    Long = 4
    Currency = 5
    IEEESingle = 6
    IEEEDouble = 7
    DateTime = 8
    Binary = 9
    Text = 10
    LongBinary = 11
    LongText = 12
    SLV = 13
    UnsignedLong = 14
    LongLong = 15
    GUID = 16
    UnsignedShort = 17


@dataclass
class ColumnInfo:
    """Column metadata"""
    name: str
    index: int
    type: ESEColumnType
    is_long_value: bool = False


@dataclass
class EmailMessage:
    """Extracted email message data"""
    record_index: int
    date_created: Optional[datetime] = None
    date_received: Optional[datetime] = None
    date_sent: Optional[datetime] = None
    subject: str = ""
    body: str = ""
    sender: str = ""
    recipients: str = ""
    message_class: str = ""
    size: int = 0
    has_attachments: bool = False
    is_read: bool = False
    importance: int = 0
    raw_property_blob: bytes = b""
    raw_native_body: bytes = b""


class ESEReader:
    """
    High-level reader for Exchange EDB databases.

    Uses pyesedb for low-level ESE access and provides
    email-specific extraction methods.
    """

    def __init__(self, db_path: str):
        if not HAS_PYESEDB:
            raise ImportError("pyesedb is required for ESE database access")

        self.db_path = db_path
        self.db = None
        self.tables: Dict[str, Any] = {}
        self.message_table = None
        self.columns: Dict[str, ColumnInfo] = {}

    def open(self) -> bool:
        """Open the database file"""
        try:
            self.db = pyesedb.file()
            self.db.open(self.db_path)
            self._load_tables()
            return True
        except Exception as e:
            print(f"Error opening database: {e}")
            return False

    def close(self):
        """Close the database"""
        if self.db:
            self.db.close()
            self.db = None

    def _load_tables(self):
        """Load all tables from database"""
        self.tables = {}
        for i in range(self.db.get_number_of_tables()):
            table = self.db.get_table(i)
            if table:
                self.tables[table.name] = table

    def get_message_tables(self) -> List[str]:
        """Get list of Message_XXX tables (mailboxes)"""
        return [name for name in self.tables.keys()
                if name.startswith('Message_') and name != 'Message']

    def select_message_table(self, table_name: str) -> bool:
        """Select a message table for reading"""
        if table_name not in self.tables:
            return False

        self.message_table = self.tables[table_name]
        self._load_columns()
        return True

    def _load_columns(self):
        """Load column metadata for current message table"""
        self.columns = {}
        if not self.message_table:
            return

        for j in range(self.message_table.get_number_of_columns()):
            col = self.message_table.get_column(j)
            if col:
                try:
                    col_type = ESEColumnType(col.type)
                except ValueError:
                    col_type = ESEColumnType.Binary

                is_lv = col_type in [ESEColumnType.LongBinary, ESEColumnType.LongText]
                self.columns[col.name] = ColumnInfo(
                    name=col.name,
                    index=j,
                    type=col_type,
                    is_long_value=is_lv
                )

    def get_record_count(self) -> int:
        """Get number of records in current message table"""
        if not self.message_table:
            return 0
        return self.message_table.get_number_of_records()

    def get_column_value(self, record, column_name: str) -> Optional[bytes]:
        """Get raw value from a column"""
        if column_name not in self.columns:
            return None

        col_info = self.columns[column_name]

        try:
            if col_info.is_long_value and record.is_long_value(col_info.index):
                lv = record.get_value_data_as_long_value(col_info.index)
                return lv.get_data() if lv else None
            else:
                return record.get_value_data(col_info.index)
        except:
            return None

    @staticmethod
    def filetime_to_datetime(data: bytes) -> Optional[datetime]:
        """
        Convert Windows FILETIME to Python datetime.

        FILETIME is 100-nanosecond intervals since January 1, 1601.
        """
        if not data or len(data) != 8:
            return None

        try:
            filetime = struct.unpack('<Q', data)[0]
            if filetime == 0:
                return None

            # Windows FILETIME epoch is January 1, 1601
            epoch = datetime(1601, 1, 1)
            return epoch + timedelta(microseconds=filetime / 10)
        except:
            return None

    @staticmethod
    def decode_utf16(data: bytes) -> str:
        """Decode UTF-16-LE string"""
        if not data:
            return ""
        try:
            return data.decode('utf-16-le', errors='ignore').strip('\x00')
        except:
            return ""

    @staticmethod
    def decode_long(data: bytes) -> int:
        """Decode 32-bit signed integer"""
        if not data or len(data) != 4:
            return 0
        return struct.unpack('<i', data)[0]

    @staticmethod
    def decode_longlong(data: bytes) -> int:
        """Decode 64-bit signed integer"""
        if not data or len(data) != 8:
            return 0
        return struct.unpack('<q', data)[0]

    @staticmethod
    def decode_bit(data: bytes) -> bool:
        """Decode boolean bit"""
        if not data:
            return False
        return data[0] != 0

    def read_message(self, record_index: int) -> Optional[EmailMessage]:
        """
        Read a single email message from the database.

        Args:
            record_index: Index of the record to read

        Returns:
            EmailMessage object or None if failed
        """
        if not self.message_table:
            return None

        try:
            record = self.message_table.get_record(record_index)
            if not record:
                return None

            msg = EmailMessage(record_index=record_index)

            # Extract dates
            date_created = self.get_column_value(record, 'DateCreated')
            msg.date_created = self.filetime_to_datetime(date_created)

            date_received = self.get_column_value(record, 'DateReceived')
            msg.date_received = self.filetime_to_datetime(date_received)

            date_sent = self.get_column_value(record, 'DateSent')
            msg.date_sent = self.filetime_to_datetime(date_sent)

            # Extract size
            size_data = self.get_column_value(record, 'Size')
            msg.size = self.decode_longlong(size_data)

            # Extract flags
            has_attach = self.get_column_value(record, 'HasAttachments')
            msg.has_attachments = self.decode_bit(has_attach)

            is_read = self.get_column_value(record, 'IsRead')
            msg.is_read = self.decode_bit(is_read)

            importance = self.get_column_value(record, 'Importance')
            msg.importance = self.decode_long(importance) if importance else 0

            # Extract message class
            msg_class = self.get_column_value(record, 'MessageClass')
            if msg_class:
                msg.message_class = self.decode_utf16(msg_class)

            # Extract recipients
            recipients = self.get_column_value(record, 'DisplayTo')
            if recipients:
                msg.recipients = self.decode_utf16(recipients)

            # Store raw blobs for body extraction
            msg.raw_property_blob = self.get_column_value(record, 'PropertyBlob') or b""
            msg.raw_native_body = self.get_column_value(record, 'NativeBody') or b""

            return msg

        except Exception as e:
            print(f"Error reading record {record_index}: {e}")
            return None

    def iter_messages(self, start: int = 0, limit: int = None):
        """
        Iterate over messages in the current table.

        Args:
            start: Starting record index
            limit: Maximum number of records to return

        Yields:
            EmailMessage objects
        """
        if not self.message_table:
            return

        total = self.get_record_count()
        end = total if limit is None else min(start + limit, total)

        for i in range(start, end):
            msg = self.read_message(i)
            if msg:
                yield msg


def extract_subject_from_property_blob(data: bytes) -> str:
    """
    Extract subject from PropertyBlob.

    The PropertyBlob contains MAPI properties. Subject is stored after
    "administratorM" (or similar sender pattern) followed by a length byte.

    Note: Exchange uses compression for repeated patterns in subjects.
    This function extracts the visible text; some compressed subjects
    may appear truncated.
    """
    if not data or len(data) < 10:
        return ""

    # Find "torM" pattern (end of "administratorM" or similar)
    tor_pos = data.find(b'torM')
    if tor_pos < 0:
        # Try alternative: look for sender pattern ending in M
        for pattern in [b'stM', b'erM', b'orM']:
            pos = data.find(pattern)
            if pos > 0:
                tor_pos = pos + len(pattern) - 1  # Point to M
                break

    if tor_pos < 0:
        return ""

    # Subject starts after M + length byte
    subject_start = tor_pos + 5  # torM + length_byte
    if subject_start >= len(data):
        return ""

    # Find end marker "HH" or control sequence
    end_pos = len(data)
    hh_pos = data.find(b'HH', subject_start)
    if hh_pos > 0:
        end_pos = hh_pos

    # Also look for 0x1a or high control bytes as end
    for i in range(subject_start, min(subject_start + 100, len(data))):
        if data[i] == 0x1a or (data[i] >= 0x80 and i > subject_start + 3):
            end_pos = min(end_pos, i)
            break

    subject_bytes = data[subject_start:end_pos]

    # Extract printable characters, handling compression markers
    subject_parts = []
    current_word = []

    for i, b in enumerate(subject_bytes):
        # Printable character (but not "!" which is compression marker)
        if 0x20 <= b <= 0x7e and b != ord('!'):
            current_word.append(chr(b))
        # "!" followed by null+digit is compression - skip the marker
        elif b == ord('!'):
            # Flush current word
            if current_word:
                subject_parts.append(''.join(current_word))
                current_word = []
        # Null or control - word boundary
        elif b == 0x00 or b < 0x20:
            if current_word:
                subject_parts.append(''.join(current_word))
                current_word = []
            # If followed by digit after null, add the digit
            if b == 0x00 and i + 1 < len(subject_bytes):
                next_b = subject_bytes[i + 1]
                if 0x30 <= next_b <= 0x39:  # Digit
                    subject_parts.append(chr(next_b))

    if current_word:
        subject_parts.append(''.join(current_word))

    subject = ''.join(subject_parts)

    # Clean up
    subject = subject.strip()

    return subject


def extract_sender_from_property_blob(data: bytes) -> str:
    """
    Extract sender name/email from PropertyBlob.

    Looks for administrator/sender name patterns and email addresses.
    """
    if not data or len(data) < 10:
        return ""

    import re

    # First look for email address
    text = data.decode('utf-8', errors='ignore')
    emails = re.findall(r'<?([\w\.-]+@[\w\.-]+\.\w+)>?', text)

    # Find readable strings
    strings = []
    current = []

    for b in data:
        if 32 <= b <= 126:
            current.append(chr(b))
        else:
            if len(current) >= 3:
                strings.append(''.join(current))
            current = []

    if current and len(current) >= 3:
        strings.append(''.join(current))

    # Look for sender name patterns
    for s in strings:
        # Pattern: "ministratorM" -> "Administrator"
        if 'ministrator' in s.lower():
            return "Administrator"

        # Pattern: "M.Name" where Name is sender
        if s.startswith('M.') and len(s) > 2:
            name = s[2:]
            if name and not name.startswith(('IPM', '<', '/')):
                return name

    # Return email if found
    if emails:
        return emails[0]

    return ""


def extract_message_id_from_property_blob(data: bytes) -> str:
    """
    Extract Message-ID from PropertyBlob.

    Message-ID format: <xxxxx@domain>
    """
    if not data:
        return ""

    import re
    text = data.decode('utf-8', errors='ignore')

    # Look for Message-ID pattern
    match = re.search(r'<([a-f0-9]+@[\w\.-]+)>', text)
    if match:
        return f"<{match.group(1)}>"

    return ""


# Convenience function
def open_edb(path: str) -> ESEReader:
    """Open an EDB database and return reader"""
    reader = ESEReader(path)
    if reader.open():
        return reader
    return None
