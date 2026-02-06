#!/usr/bin/env python3
"""
Exchange EDB Content Viewer v2
GUI application with folder tree navigation
"""

import sys
import os
import struct
import re
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

# Load version from VERSION file
def get_version():
    version_file = Path(__file__).parent / "VERSION"
    if version_file.exists():
        return version_file.read_text().strip()
    return "1.000"

VERSION = get_version()

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QFileDialog, QTreeWidget, QTreeWidgetItem,
    QSplitter, QTextEdit, QComboBox, QGroupBox, QLineEdit,
    QTabWidget, QTableWidget, QTableWidgetItem, QHeaderView,
    QStatusBar, QMessageBox, QProgressBar, QMenu, QListWidget,
    QListWidgetItem, QCheckBox, QTextBrowser, QDialog, QFormLayout,
    QDateEdit, QDialogButtonBox
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QUrl, QDate
from PyQt6.QtGui import QFont, QAction, QTextOption, QColor, QPalette

# Using QTextBrowser for lightweight HTML rendering (no WebEngine dependency)

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.utils import format_datetime
from email import encoders

# Try to import LZXPRESS decompressor
try:
    from lzxpress import decompress_exchange_body, extract_text_from_html, extract_body_from_property_blob, get_body_preview, get_html_content
    HAS_LZXPRESS = True
except ImportError:
    HAS_LZXPRESS = False

# Check if dissect.esedb is available for proper decompression
try:
    from dissect.esedb.compression import decompress as dissect_decompress
    HAS_DISSECT = True
except ImportError:
    HAS_DISSECT = False

# Try to import ESE Reader module
try:
    import sys
    sys.path.insert(0, str(Path(__file__).parent / 'src' / 'core'))
    from ese_reader import (
        ESEReader, ESEColumnType,
        extract_subject_from_property_blob as ese_extract_subject,
        extract_sender_from_property_blob as ese_extract_sender,
        extract_message_id_from_property_blob as ese_extract_message_id
    )
    HAS_ESE_READER = True
except ImportError:
    HAS_ESE_READER = False

# Try to import folder mapping
try:
    from folder_mapping import get_folder_name as get_mapped_folder_name, SPECIAL_FOLDER_MAP
    HAS_FOLDER_MAPPING = True
except ImportError:
    HAS_FOLDER_MAPPING = False
    SPECIAL_FOLDER_MAP = {}

# Import stable email extraction module
try:
    from email_message import EmailMessage, EmailExtractor, EmailAttachment
    HAS_EMAIL_MODULE = True
except ImportError:
    HAS_EMAIL_MODULE = False

# Import calendar extraction module
try:
    from calendar_message import CalendarEvent, CalendarExtractor, export_calendar_to_ics, CALENDAR_MESSAGE_CLASSES
    HAS_CALENDAR_MODULE = True
except ImportError:
    HAS_CALENDAR_MODULE = False
    CALENDAR_MESSAGE_CLASSES = []


# Supported encodings for text extraction
# ASCII/UTF-8 encodings (for standard text)
ASCII_ENCODINGS = ['ascii', 'utf-8']
# Extended encodings (for non-ASCII characters like Cyrillic)
EXTENDED_ENCODINGS = [
    'windows-1251',  # Cyrillic (Russian, Bulgarian, Serbian)
    'koi8-r',        # Cyrillic (Russian)
    'koi8-u',        # Cyrillic (Ukrainian)
    'iso-8859-5',    # Cyrillic
    'windows-1252',  # Western European
    'iso-8859-1',    # Latin-1
    'cp866',         # DOS Cyrillic
]


def try_decode(data, encodings=None):
    """Try to decode bytes using multiple encodings with smart detection."""
    if not data:
        return None

    # Check if data is pure ASCII (all bytes < 128)
    has_high_bytes = any(b >= 128 for b in data)

    if not has_high_bytes:
        # Pure ASCII - decode as ASCII or UTF-8
        try:
            return data.decode('ascii').rstrip('\x00')
        except UnicodeDecodeError:
            try:
                return data.decode('utf-8').rstrip('\x00')
            except UnicodeDecodeError:
                pass

    # Has high bytes - try UTF-8 first (handles multi-byte UTF-8)
    try:
        text = data.decode('utf-8')
        if text:
            return text.rstrip('\x00')
    except UnicodeDecodeError:
        pass

    # Try extended encodings (Cyrillic, etc.) only if there are high bytes
    if has_high_bytes:
        if encodings is None:
            encodings = EXTENDED_ENCODINGS

        for encoding in encodings:
            try:
                text = data.decode(encoding)
                # Check if result is mostly printable
                printable_count = sum(1 for c in text if c.isprintable() or c.isspace())
                if printable_count >= len(text) * 0.8:
                    return text.rstrip('\x00')
            except (UnicodeDecodeError, LookupError):
                continue

    # Final fallback - decode with replacement
    try:
        return data.decode('utf-8', errors='replace').rstrip('\x00')
    except:
        return data.decode('latin-1', errors='replace').rstrip('\x00')


def is_printable_extended(b):
    """Check if byte is printable (including extended ASCII/Cyrillic)."""
    # ASCII printable
    if 32 <= b < 127:
        return True
    # Extended ASCII (Windows-1251 Cyrillic range, etc.)
    if 128 <= b <= 255:
        return True
    return False


def get_column_map(table):
    """Get mapping of column names to indices."""
    col_map = {}
    for j in range(table.get_number_of_columns()):
        col = table.get_column(j)
        if col:
            col_map[col.name] = j
    return col_map


def get_int_value(record, col_idx):
    """Get integer value from record."""
    if col_idx < 0:
        return None
    try:
        val = record.get_value_data(col_idx)
        if not val:
            return None
        if len(val) == 4:
            return struct.unpack('<I', val)[0]
        elif len(val) == 8:
            return struct.unpack('<Q', val)[0]
        elif len(val) == 2:
            return struct.unpack('<H', val)[0]
    except:
        pass
    return None


def get_folder_id(record, col_idx):
    """Get folder ID as hex string (full ID for proper matching)."""
    if col_idx < 0:
        return None
    try:
        val = record.get_value_data(col_idx)
        if not val:
            return None
        # Return full hex for matching with Folder table
        return val.hex()
    except:
        pass
    return None


def extract_subject_from_blob(blob):
    """Extract subject from PropertyBlob using ESE reader or fallback."""
    if not blob or len(blob) < 10:
        return None

    # Try ESE reader module first (better extraction)
    if HAS_ESE_READER:
        try:
            subject = ese_extract_subject(blob)
            if subject:
                return subject
        except:
            pass

    # Fallback: Look for marker byte (M=0x4d, K=0x4b) followed by length + string
    for i in range(len(blob) - 5):
        if blob[i] in (0x4d, 0x4b):  # M or K marker
            length = blob[i+1]
            if 2 <= length <= 100 and i + 2 + length <= len(blob):
                potential = blob[i+2:i+2+length]
                # Check if it's printable (including extended ASCII/Cyrillic)
                if all(is_printable_extended(b) for b in potential):
                    # Try multiple encodings
                    text = try_decode(potential)
                    if text:
                        # Filter out common non-subject strings
                        skip_words = ['admin', 'exchange', 'recipient', 'labsith', 'fydib', 'pdlt', 'group', 'index']
                        if not any(x in text.lower() for x in skip_words):
                            return text
    return None


def extract_message_id_from_blob(blob):
    """Extract Message-ID from PropertyBlob using ESE reader or fallback."""
    if not blob or len(blob) < 50:
        return None

    # Try ESE reader module first
    if HAS_ESE_READER:
        try:
            msg_id = ese_extract_message_id(blob)
            if msg_id:
                return msg_id
        except:
            pass

    # Fallback: Pattern: < + hex chars (with nulls) + @ + domain + >
    for i in range(len(blob) - 50):
        if blob[i] == 0x3c:  # '<'
            hex_chars = b'0123456789abcdef'
            collected = []
            j = i + 1

            # Collect until '@'
            while j < len(blob) and j < i + 60:
                if blob[j] == 0x40:  # '@'
                    break
                if blob[j] in hex_chars:
                    collected.append(blob[j])
                elif blob[j] == 0x00:
                    pass  # Skip nulls
                else:
                    break
                j += 1

            if len(collected) >= 20 and j < len(blob) and blob[j] == 0x40:
                hex_part = bytes(collected).decode('ascii')
                # Extract domain
                domain_chars = []
                k = j + 1
                while k < len(blob) and k < j + 20:
                    if blob[k] == 0x3e:  # '>'
                        break
                    if 0x20 <= blob[k] < 0x7f:
                        domain_chars.append(blob[k])
                    k += 1
                if domain_chars:
                    domain = bytes(domain_chars).decode('ascii')
                    return f'<{hex_part}@{domain}>'
    return None


def is_valid_sender_name(name):
    """Check if a string looks like a real sender name (not Message-ID)."""
    if not name or len(name) < 2:
        return False

    # If it's an email address, check the local part
    if '@' in name:
        local_part = name.split('@')[0]
        # Reject if local part is mostly hex characters (Message-ID pattern)
        if len(local_part) >= 4:
            hex_chars = sum(1 for c in local_part if c in '0123456789abcdef')
            if hex_chars / len(local_part) > 0.6:
                return False
        # Very short local parts are suspicious
        if len(local_part) <= 4 and local_part.replace('@', '').isalnum():
            return False
        # Local part should have at least one letter
        if not any(c.isalpha() for c in local_part):
            return False
    else:
        # Plain names should have at least one letter
        if not any(c.isalpha() for c in name):
            return False
        # Should be at least 3 characters for a real name
        if len(name) < 3:
            return False

    return True


def _is_valid_name(name):
    """Check if extracted name looks valid (not hex/garbage/email)."""
    if not name or len(name) < 2:
        return False

    # Filter out email addresses
    if '@' in name:
        return False

    # Filter out hex-like strings (e.g., "1aa15ee3d8bb699", "d5ed566d86865")
    clean = name.replace(' ', '').replace('-', '').replace('_', '')
    if len(clean) > 6 and all(c in '0123456789abcdefABCDEF' for c in clean):
        return False

    # Must have at least 2 letters
    letter_count = sum(1 for c in name if c.isalpha())
    if letter_count < 2:
        return False

    # Filter out strings that are mostly digits/hex
    alnum = [c for c in name if c.isalnum()]
    if alnum:
        digit_hex_count = sum(1 for c in alnum if c.isdigit() or c.lower() in 'abcdef')
        if digit_hex_count > len(alnum) * 0.4:
            return False

    # Filter out system/group names
    lower_name = name.lower()
    if any(x in lower_name for x in ['administrative', 'system ', 'group']):
        return False

    return True


def extract_sender_from_blob(blob):
    """Extract sender name from PropertyBlob using ESE reader or fallback."""
    if not blob:
        return None

    # Try ESE reader module first (better extraction)
    if HAS_ESE_READER:
        try:
            sender = ese_extract_sender(blob)
            if sender and _is_valid_name(sender):
                return sender
        except:
            pass

    # Look for common name patterns in the blob
    # Pattern: Look for "Rosetta Stone" marker followed by sender name
    try:
        # Search for name-like strings (capitalized words)
        text = blob.decode('utf-8', errors='ignore')

        # Look for email-like pattern and extract name part
        import re
        # Match "Name <email>" or just name patterns
        email_match = re.search(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s*<?[\w\.-]+@', text)
        if email_match:
            name = email_match.group(1)
            if _is_valid_name(name):
                return name

        # Look for standalone capitalized names (First Last pattern)
        name_match = re.search(r'\b([A-Z][a-z]{2,15})\s+([A-Z][a-z]{2,15})\b', text)
        if name_match:
            name = f"{name_match.group(1)} {name_match.group(2)}"
            if _is_valid_name(name):
                return name
    except:
        pass

    # Fallback: Look for Admin...istrator pattern
    if b'Admin' in blob:
        idx = blob.find(b'Admin')
        chunk = blob[idx:idx + 30]
        if b'istrator' in chunk:
            return 'Administrator'

    return None


def extract_email_from_blob(blob):
    """Extract email address from PropertyBlob."""
    if not blob:
        return None

    # Look for @domain pattern
    at_idx = blob.find(b'@')
    if at_idx > 0:
        # Find start of email (backwards from @)
        start = at_idx - 1
        while start > 0 and (blob[start-1:start].isalnum() or blob[start-1:start] in [b'.', b'_', b'-']):
            start -= 1
        # Find end of email (forwards from @)
        end = at_idx + 1
        while end < len(blob) and (blob[end:end+1].isalnum() or blob[end:end+1] in [b'.', b'_', b'-']):
            end += 1
        try:
            email = blob[start:end].decode('ascii', errors='ignore')
            if '@' in email and '.' in email.split('@')[1]:
                return email
        except:
            pass
    return None


def extract_attachment_filename(blob):
    """Extract attachment filename from PropertyBlob."""
    if not blob:
        return ""

    extensions = [b'.txt', b'.xml', b'.doc', b'.docx', b'.pdf', b'.jpg',
                  b'.png', b'.xlsx', b'.xls', b'.zip', b'.eml', b'.msg',
                  b'.html', b'.htm', b'.csv']

    for ext in extensions:
        idx = blob.lower().find(ext)
        if idx >= 0:
            start = idx
            while start > 0 and 0x20 <= blob[start - 1] < 0x7f:
                start -= 1
            filename = blob[start:idx + len(ext)]
            if len(filename) > len(ext):
                return filename.decode('ascii', errors='ignore')
    return ""


def extract_attachment_content_type(blob):
    """Extract content type from attachment PropertyBlob."""
    if not blob:
        return "application/octet-stream"

    mime_patterns = [
        (b'text/plain', 'text/plain'),
        (b'text/html', 'text/html'),
        (b'text/xml', 'text/xml'),
        (b'application/pdf', 'application/pdf'),
        (b'application/xml', 'application/xml'),
        (b'image/jpeg', 'image/jpeg'),
        (b'image/png', 'image/png'),
    ]

    for pattern, mime in mime_patterns:
        if pattern in blob:
            return mime

    return "application/octet-stream"


def create_eml_content(email_data):
    """Create EML content from email data dict."""
    # Get body content
    body_text = email_data.get('body_text') or email_data.get('subject') or "(No content)"
    body_html = email_data.get('body_html', '')

    # Create message
    if email_data.get('attachments'):
        msg = MIMEMultipart('mixed')

        # Body part (alternative with text and HTML)
        body_part = MIMEMultipart('alternative')
        body_part.attach(MIMEText(body_text, 'plain', 'utf-8'))
        if body_html:
            body_part.attach(MIMEText(body_html, 'html', 'utf-8'))
        msg.attach(body_part)

        # Attachments
        for filename, content_type, data in email_data['attachments']:
            if '/' in content_type:
                maintype, subtype = content_type.split('/', 1)
            else:
                maintype, subtype = 'application', 'octet-stream'

            attachment = MIMEBase(maintype, subtype)
            attachment.set_payload(data)
            encoders.encode_base64(attachment)
            attachment.add_header('Content-Disposition', 'attachment', filename=filename)
            msg.attach(attachment)
    else:
        msg = MIMEMultipart('alternative')
        msg.attach(MIMEText(body_text, 'plain', 'utf-8'))
        if body_html:
            msg.attach(MIMEText(body_html, 'html', 'utf-8'))

    # Headers
    sender = email_data.get('sender_email', 'unknown@unknown.com')
    if email_data.get('sender_name'):
        sender = f"{email_data['sender_name']} <{sender}>"

    recipient = email_data.get('recipient_email', sender)
    if email_data.get('recipient_name'):
        recipient = f"{email_data['recipient_name']} <{recipient}>"

    msg['From'] = sender
    msg['To'] = recipient
    msg['Subject'] = email_data.get('subject') or '(No Subject)'

    if email_data.get('date_sent'):
        msg['Date'] = format_datetime(email_data['date_sent'])

    if email_data.get('message_id'):
        msg['Message-ID'] = email_data['message_id']

    msg['X-MS-Has-Attach'] = 'yes' if email_data.get('has_attachments') else ''
    msg['X-Folder'] = email_data.get('folder_name', 'Unknown')
    msg['X-Record-Index'] = str(email_data.get('record_index', 0))

    return msg.as_bytes()


def is_encrypted_or_binary(data):
    """Check if data looks like encrypted/binary content (not readable text)."""
    if not data or len(data) < 2:
        return False

    # Count control characters and high-bit bytes
    control_count = sum(1 for b in data if b < 32 and b not in (9, 10, 13))
    high_byte_count = sum(1 for b in data if b >= 128)
    printable_count = sum(1 for b in data if 32 <= b < 127)

    total = len(data)

    # If more than 30% control chars or mixed high-bytes with control chars, likely encrypted
    if control_count > total * 0.3:
        return True

    # If has control chars at start (like 0x12) and high bytes, likely encrypted
    if data[0] < 32 and high_byte_count > 0:
        return True

    # If less than 50% printable ASCII and not valid UTF-16, likely encrypted
    if printable_count < total * 0.5:
        # Check if it could be UTF-16
        has_null_pattern = len(data) >= 4 and data[1] == 0 and data[3] == 0
        if not has_null_pattern:
            return True

    return False


def get_string_value(record, col_idx):
    """Get string value from record with multi-encoding support."""
    if col_idx < 0:
        return ""
    try:
        val = record.get_value_data(col_idx)
        if not val:
            return ""

        # Check if data is encrypted/binary
        if is_encrypted_or_binary(val):
            return ""  # Return empty for encrypted fields

        # Check for UTF-16-LE BOM or pattern (null bytes between ASCII chars)
        is_likely_utf16 = False
        if len(val) >= 2:
            # Check for BOM
            if val[:2] == b'\xff\xfe':
                is_likely_utf16 = True
            # Check for null-byte pattern typical of UTF-16-LE ASCII
            elif len(val) >= 4 and val[1] == 0 and val[3] == 0 and val[0] != 0 and val[2] != 0:
                is_likely_utf16 = True

        if is_likely_utf16:
            try:
                text = val.decode('utf-16-le').rstrip('\x00')
                if text and all(c.isprintable() or c.isspace() for c in text):
                    return text
            except:
                pass

        # Try standard decoding
        result = try_decode(val)
        return result if result else ""
    except:
        pass
    return ""


def get_bytes_value(record, col_idx):
    """Get raw bytes from record."""
    if col_idx < 0:
        return None
    try:
        return record.get_value_data(col_idx)
    except:
        return None


def get_filetime_value(record, col_idx):
    """Get datetime from Windows FILETIME."""
    if col_idx < 0:
        return None
    try:
        val = record.get_value_data(col_idx)
        if not val or len(val) != 8:
            return None
        filetime = struct.unpack('<Q', val)[0]
        if filetime == 0:
            return None
        unix_time = (filetime - 116444736000000000) / 10000000
        return datetime.fromtimestamp(unix_time, tz=timezone.utc)
    except:
        return None


class LoadWorker(QThread):
    """Background worker for loading database."""
    progress = pyqtSignal(str)
    finished = pyqtSignal(object)
    error = pyqtSignal(str)

    def __init__(self, edb_path):
        super().__init__()
        # Normalize path for cross-platform compatibility
        self.edb_path = os.path.normpath(os.path.abspath(edb_path))

    def run(self):
        try:
            import pyesedb

            self.progress.emit("Opening database (this may take 20-30 seconds)...")
            db = pyesedb.file()
            db.open(self.edb_path)
            self.progress.emit("Database opened, scanning tables...")

            self.progress.emit("Loading tables...")
            result = {
                'db': db,
                'tables': {},
                'mailboxes': []
            }

            for i in range(db.get_number_of_tables()):
                table = db.get_table(i)
                if table:
                    result['tables'][table.name] = table
                    # Detect mailbox tables
                    if table.name.startswith("Message_"):
                        try:
                            num = int(table.name.split('_')[1])
                            result['mailboxes'].append({
                                'number': num,
                                'message_count': table.get_number_of_records(),
                                'owner_email': None  # Will be populated later
                            })
                        except:
                            pass

            # Sort mailboxes
            result['mailboxes'].sort(key=lambda x: x['number'])

            # Try to extract mailbox owner email from Sent Items
            for mb in result['mailboxes']:
                mb['owner_email'] = self._get_mailbox_owner(result['tables'], mb['number'])

            self.finished.emit(result)

        except Exception as e:
            self.error.emit(str(e))

    def _get_mailbox_owner(self, tables, mailbox_num):
        """Get mailbox owner from Mailbox table."""
        try:
            mailbox_table = tables.get('Mailbox')
            if not mailbox_table:
                return None

            col_map = get_column_map(mailbox_table)
            mb_num_idx = col_map.get('MailboxNumber', -1)
            owner_name_idx = col_map.get('MailboxOwnerDisplayName', -1)
            display_name_idx = col_map.get('DisplayName', -1)

            for rec_idx in range(mailbox_table.get_number_of_records()):
                try:
                    record = mailbox_table.get_record(rec_idx)
                    if not record:
                        continue

                    # Check if this is the requested mailbox
                    mb_num_data = record.get_value_data(mb_num_idx)
                    if mb_num_data:
                        import struct
                        mb_num = struct.unpack('<I', mb_num_data)[0]
                        if mb_num != mailbox_num:
                            continue

                    # Try to get owner name
                    for col_idx in [owner_name_idx, display_name_idx]:
                        if col_idx < 0:
                            continue
                        val = record.get_value_data(col_idx)
                        if val and HAS_DISSECT:
                            try:
                                decompressed = dissect_decompress(val)
                                for enc in ['utf-16-le', 'utf-8']:
                                    try:
                                        text = decompressed.decode(enc).rstrip('\x00')
                                        if text:
                                            return text
                                    except:
                                        pass
                            except:
                                pass
                except:
                    pass

            return None
        except:
            return None


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"Exchange EDB Exporter v{VERSION}")
        self.setMinimumSize(800, 500)
        self.resize(1200, 700)  # Default size, can be resized smaller

        self.db = None
        self.tables = {}
        self.current_mailbox = None
        self.folders = {}
        self.messages_by_folder = defaultdict(list)
        self.current_record_idx = None
        self.current_attachments = []  # List of (filename, content_type, data)
        self.current_email_data = {}
        self.current_email_message = None  # EmailMessage object for stable export
        self.email_extractor = None  # EmailExtractor instance
        self.calendar_extractor = None  # CalendarExtractor instance
        self.folder_messages_cache = {}  # Cache: folder_id -> list of message data

        self._setup_ui()
        self._setup_menu()

    def _setup_menu(self):
        menubar = self.menuBar()

        # File menu
        file_menu = menubar.addMenu("&File")

        open_action = QAction("&Open Database...", self)
        open_action.setShortcut("Ctrl+O")
        open_action.triggered.connect(self._on_browse)
        file_menu.addAction(open_action)

        file_menu.addSeparator()

        export_action = QAction("&Export Mailbox...", self)
        export_action.setShortcut("Ctrl+E")
        export_action.triggered.connect(self._on_export)
        file_menu.addAction(export_action)

        file_menu.addSeparator()

        exit_action = QAction("E&xit", self)
        exit_action.setShortcut("Ctrl+Q")
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        # View menu
        view_menu = menubar.addMenu("&View")

        refresh_action = QAction("&Refresh", self)
        refresh_action.setShortcut("F5")
        refresh_action.triggered.connect(self._on_refresh)
        view_menu.addAction(refresh_action)

    def _setup_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        # File and mailbox selection (minimal compact row)
        top_widget = QWidget()
        top_widget.setFixedHeight(28)
        top_layout = QHBoxLayout(top_widget)
        top_layout.setContentsMargins(2, 0, 2, 0)
        top_layout.setSpacing(3)

        lbl_db = QLabel("DB:")
        lbl_db.setFixedWidth(20)
        top_layout.addWidget(lbl_db)

        self.file_path = QLineEdit()
        self.file_path.setReadOnly(True)
        self.file_path.setPlaceholderText("Select EDB...")
        self.file_path.setMaximumWidth(200)
        self.file_path.setFixedHeight(22)
        top_layout.addWidget(self.file_path)

        browse_btn = QPushButton("...")
        browse_btn.setFixedSize(24, 22)
        browse_btn.clicked.connect(self._on_browse)
        top_layout.addWidget(browse_btn)

        self.load_btn = QPushButton("Load")
        self.load_btn.setFixedSize(40, 22)
        self.load_btn.clicked.connect(self._on_load)
        self.load_btn.setEnabled(False)
        top_layout.addWidget(self.load_btn)

        lbl_mb = QLabel("MB:")
        lbl_mb.setFixedWidth(22)
        top_layout.addWidget(lbl_mb)

        self.mailbox_combo = QComboBox()
        self.mailbox_combo.setMinimumWidth(140)
        self.mailbox_combo.setFixedHeight(22)
        self.mailbox_combo.currentIndexChanged.connect(self._on_mailbox_changed)
        top_layout.addWidget(self.mailbox_combo)

        self.owner_label = QLabel("")
        self.owner_label.setStyleSheet("color: #0066cc; font-weight: bold; font-size: 11px;")
        top_layout.addWidget(self.owner_label)

        top_layout.addStretch()

        self.show_hidden_cb = QCheckBox("Hidden")
        self.show_hidden_cb.setToolTip("Show hidden/system items")
        self.show_hidden_cb.stateChanged.connect(self._on_show_hidden_changed)
        top_layout.addWidget(self.show_hidden_cb)

        # About button in top right corner
        self.about_btn = QPushButton("About")
        self.about_btn.setFixedSize(50, 22)
        self.about_btn.clicked.connect(self._on_about)
        self.about_btn.setToolTip("About this application")
        top_layout.addWidget(self.about_btn)

        layout.addWidget(top_widget)

        # Main content splitter
        main_splitter = QSplitter(Qt.Orientation.Horizontal)

        # Left panel: Folder tree
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)

        left_layout.addWidget(QLabel("Folders:"))
        self.folder_tree = QTreeWidget()
        self.folder_tree.setHeaderLabels(["Folder", "Messages"])
        self.folder_tree.itemSelectionChanged.connect(self._on_folder_selected)
        self.folder_tree.setMinimumWidth(250)
        left_layout.addWidget(self.folder_tree)

        main_splitter.addWidget(left_panel)

        # Middle panel: Message list
        middle_panel = QWidget()
        middle_layout = QVBoxLayout(middle_panel)
        middle_layout.setContentsMargins(0, 0, 0, 0)

        # Search and filter controls
        search_layout = QHBoxLayout()
        search_layout.setSpacing(5)

        search_layout.addWidget(QLabel("Search:"))
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Type to search subject, from, to...")
        self.search_input.textChanged.connect(self._on_search_changed)
        self.search_input.setMaximumWidth(200)
        search_layout.addWidget(self.search_input)

        # Filter: Read status
        self.filter_read_combo = QComboBox()
        self.filter_read_combo.addItems(["All", "Unread", "Read", "Failed"])
        self.filter_read_combo.setMaximumWidth(80)
        self.filter_read_combo.currentIndexChanged.connect(self._on_filter_changed)
        search_layout.addWidget(QLabel("Status:"))
        search_layout.addWidget(self.filter_read_combo)

        # Filter: Has attachments
        self.filter_attach_cb = QCheckBox("Has Attach")
        self.filter_attach_cb.stateChanged.connect(self._on_filter_changed)
        search_layout.addWidget(self.filter_attach_cb)

        # Clear filters button
        self.clear_filters_btn = QPushButton("Clear")
        self.clear_filters_btn.setMaximumWidth(50)
        self.clear_filters_btn.clicked.connect(self._on_clear_filters)
        search_layout.addWidget(self.clear_filters_btn)

        search_layout.addStretch()

        # Message count label
        self.msg_count_label = QLabel("")
        self.msg_count_label.setStyleSheet("color: #666;")
        search_layout.addWidget(self.msg_count_label)

        middle_layout.addLayout(search_layout)

        # Message list
        self.message_list = QTreeWidget()
        self.message_list.setHeaderLabels(["#", "Date", "From", "To", "Subject", "Att", "Read"])
        self.message_list.itemSelectionChanged.connect(self._on_message_selected)
        self.message_list.setMinimumWidth(500)
        self.message_list.setSortingEnabled(True)
        # Set column widths
        self.message_list.setColumnWidth(0, 45)   # #
        self.message_list.setColumnWidth(1, 115)  # Date
        self.message_list.setColumnWidth(2, 120)  # From
        self.message_list.setColumnWidth(3, 120)  # To
        self.message_list.setColumnWidth(4, 180)  # Subject
        self.message_list.setColumnWidth(5, 30)   # Att
        self.message_list.setColumnWidth(6, 35)   # Read
        middle_layout.addWidget(self.message_list)

        # Store all messages for filtering
        self.all_messages_cache = []

        # Export buttons
        export_layout = QHBoxLayout()
        self.export_eml_btn = QPushButton("Export as EML")
        self.export_eml_btn.clicked.connect(self._on_export_eml)
        self.export_eml_btn.setEnabled(False)
        export_layout.addWidget(self.export_eml_btn)

        self.export_attach_btn = QPushButton("Export Attachments")
        self.export_attach_btn.clicked.connect(self._on_export_attachments)
        self.export_attach_btn.setEnabled(False)
        export_layout.addWidget(self.export_attach_btn)

        self.export_folder_btn = QPushButton("Export Folder")
        self.export_folder_btn.clicked.connect(self._on_export_folder)
        self.export_folder_btn.setEnabled(False)
        export_layout.addWidget(self.export_folder_btn)

        self.export_calendar_btn = QPushButton("Export Calendar (.ics)")
        self.export_calendar_btn.clicked.connect(self._on_export_calendar)
        self.export_calendar_btn.setEnabled(False)
        self.export_calendar_btn.setToolTip("Export calendar items from this folder to .ics file")
        export_layout.addWidget(self.export_calendar_btn)

        self.export_mailbox_btn = QPushButton("Export Mailbox...")
        self.export_mailbox_btn.clicked.connect(self._on_export_mailbox)
        self.export_mailbox_btn.setEnabled(False)
        self.export_mailbox_btn.setToolTip("Export entire mailbox with filters (date, from, to, subject)")
        export_layout.addWidget(self.export_mailbox_btn)

        middle_layout.addLayout(export_layout)

        main_splitter.addWidget(middle_panel)

        # Right panel: Content view
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)

        self.content_tabs = QTabWidget()

        # Body Plain Text tab - with word wrap
        self.body_view = QTextEdit()
        self.body_view.setReadOnly(True)
        self.body_view.setFont(QFont("Arial", 11))
        self.body_view.setWordWrapMode(QTextOption.WrapMode.WordWrap)
        self.body_view.setLineWrapMode(QTextEdit.LineWrapMode.WidgetWidth)
        self.content_tabs.addTab(self.body_view, "Body (Text)")

        # Body HTML tab - lightweight QTextBrowser for HTML rendering
        self.html_browser_view = QTextBrowser()
        self.html_browser_view.setReadOnly(True)
        self.html_browser_view.setOpenExternalLinks(True)
        self.html_browser_view.setFont(QFont("Arial", 11))
        palette = self.html_browser_view.palette()
        palette.setColor(QPalette.ColorRole.Base, QColor(255, 255, 255))
        self.html_browser_view.setPalette(palette)
        self.content_tabs.addTab(self.html_browser_view, "Body (HTML)")

        # Attachments tab (right after Body HTML for easy access)
        attach_widget = QWidget()
        attach_layout = QVBoxLayout(attach_widget)
        self.attach_list = QListWidget()
        self.attach_list.itemDoubleClicked.connect(self._on_attachment_double_clicked)
        attach_layout.addWidget(self.attach_list)

        attach_btn_layout = QHBoxLayout()
        self.save_attach_btn = QPushButton("Save Selected Attachment")
        self.save_attach_btn.clicked.connect(self._on_save_attachment)
        self.save_attach_btn.setEnabled(False)
        attach_btn_layout.addWidget(self.save_attach_btn)

        self.save_all_attach_btn = QPushButton("Save All Attachments")
        self.save_all_attach_btn.clicked.connect(self._on_save_all_attachments)
        self.save_all_attach_btn.setEnabled(False)
        attach_btn_layout.addWidget(self.save_all_attach_btn)
        attach_layout.addLayout(attach_btn_layout)

        self.content_tabs.addTab(attach_widget, "Attachments (0)")

        # HTML Source tab - shows raw HTML code
        self.html_source_view = QTextEdit()
        self.html_source_view.setReadOnly(True)
        self.html_source_view.setFont(QFont("Consolas", 9))
        self.html_source_view.setWordWrapMode(QTextOption.WrapMode.NoWrap)
        self.content_tabs.addTab(self.html_source_view, "HTML Source")

        # Raw Body tab with compressed/uncompressed toggle
        raw_body_widget = QWidget()
        raw_body_layout = QVBoxLayout(raw_body_widget)
        raw_body_layout.setContentsMargins(0, 0, 0, 0)

        # Toggle for compressed/uncompressed view
        raw_toggle_layout = QHBoxLayout()
        self.raw_compressed_cb = QCheckBox("Show Compressed (Raw)")
        self.raw_compressed_cb.setChecked(True)
        self.raw_compressed_cb.stateChanged.connect(self._on_raw_toggle_changed)
        raw_toggle_layout.addWidget(self.raw_compressed_cb)
        raw_toggle_layout.addStretch()
        raw_body_layout.addLayout(raw_toggle_layout)

        self.raw_body_view = QTextEdit()
        self.raw_body_view.setReadOnly(True)
        self.raw_body_view.setFont(QFont("Consolas", 9))
        raw_body_layout.addWidget(self.raw_body_view)
        self.content_tabs.addTab(raw_body_widget, "Raw Body")

        # Store raw data for toggle
        self.current_raw_body_compressed = None
        self.current_raw_body_decompressed = None

        # Parsed tab
        self.parsed_view = QTextEdit()
        self.parsed_view.setReadOnly(True)
        self.parsed_view.setFont(QFont("Consolas", 10))
        self.content_tabs.addTab(self.parsed_view, "Parsed Data")

        # Raw Hex tab
        self.hex_view = QTextEdit()
        self.hex_view.setReadOnly(True)
        self.hex_view.setFont(QFont("Consolas", 9))
        self.content_tabs.addTab(self.hex_view, "PropertyBlob (Hex)")

        # ASCII tab
        self.ascii_view = QTextEdit()
        self.ascii_view.setReadOnly(True)
        self.ascii_view.setFont(QFont("Consolas", 10))
        self.content_tabs.addTab(self.ascii_view, "ASCII Strings")

        # All Columns tab
        self.columns_table = QTableWidget()
        self.columns_table.setColumnCount(3)
        self.columns_table.setHorizontalHeaderLabels(["Column", "Size", "Value"])
        self.columns_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        self.content_tabs.addTab(self.columns_table, "All Columns")

        right_layout.addWidget(self.content_tabs)

        # Export buttons row below content tabs
        content_export_layout = QHBoxLayout()

        self.export_eml_btn2 = QPushButton("Export Message as EML")
        self.export_eml_btn2.clicked.connect(self._on_export_eml)
        self.export_eml_btn2.setEnabled(False)
        self.export_eml_btn2.setStyleSheet("QPushButton { padding: 8px 16px; font-weight: bold; }")
        content_export_layout.addWidget(self.export_eml_btn2)

        content_export_layout.addStretch()

        right_layout.addLayout(content_export_layout)

        main_splitter.addWidget(right_panel)
        main_splitter.setSizes([400, 400, 450])  # Equal height/width for all panels

        layout.addWidget(main_splitter)

        # Status bar
        self.status = QStatusBar()
        self.setStatusBar(self.status)
        self.status.showMessage("Ready - Select an EDB file")

        # Progress bar
        self.progress = QProgressBar()
        self.progress.setVisible(False)
        self.status.addPermanentWidget(self.progress)

    def _on_browse(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Open Exchange Database", "",
            "Exchange Database (*.edb);;All Files (*.*)"
        )
        if path:
            self.file_path.setText(path)
            self.load_btn.setEnabled(True)

    def _on_load(self):
        path = self.file_path.text()
        if not path:
            return

        self.load_btn.setEnabled(False)
        self.progress.setVisible(True)
        self.progress.setRange(0, 0)

        self.worker = LoadWorker(path)
        self.worker.progress.connect(lambda msg: self.status.showMessage(msg))
        self.worker.finished.connect(self._on_load_finished)
        self.worker.error.connect(self._on_load_error)
        self.worker.start()

    def _on_load_finished(self, result):
        self.progress.setVisible(False)
        self.load_btn.setEnabled(True)

        self.db = result['db']
        self.tables = result['tables']

        # Clear folder cache when loading new database
        self.folder_messages_cache.clear()

        # Populate mailbox combo
        self.mailbox_combo.clear()
        for mb in result['mailboxes']:
            owner = mb.get('owner_email', '')
            if owner:
                # Show owner name without email
                label = f"{owner} ({mb['message_count']} msgs)"
            else:
                label = f"Mailbox {mb['number']} ({mb['message_count']} msgs)"
            self.mailbox_combo.addItem(label, mb['number'])

        self.status.showMessage(f"Loaded {len(self.tables)} tables, {len(result['mailboxes'])} mailboxes")

    def _on_load_error(self, error):
        self.progress.setVisible(False)
        self.load_btn.setEnabled(True)
        QMessageBox.critical(self, "Load Error", f"Failed to load database:\n{error}")

    def _on_mailbox_changed(self, index):
        if index < 0:
            return

        self.current_mailbox = self.mailbox_combo.currentData()
        self.status.showMessage(f"Selected mailbox {self.current_mailbox}, loading...")

        # Clear folder cache when changing mailbox
        self.folder_messages_cache.clear()

        try:
            self._load_folders()
            self._index_messages()
            self.export_mailbox_btn.setEnabled(True)
        except Exception as e:
            self.status.showMessage(f"Error loading mailbox: {e}")
            QMessageBox.warning(self, "Error", f"Failed to load mailbox:\n{e}")

    def _load_folders(self):
        """Load folders by scanning messages and using Folder table metadata."""
        self.folder_tree.clear()
        self.folders = {}
        self.folder_special_map = {}  # Map FolderId -> SpecialFolderNumber

        if not self.current_mailbox:
            self.status.showMessage("No mailbox selected")
            return

        # Special folder number to name mapping (Exchange 2013+ actual values)
        # Based on analysis of actual database
        special_folder_names = {
            0: 'Hidden Items',
            1: 'Root',
            2: 'Spooler Queue',
            3: 'Shortcuts',
            4: 'Finder',
            5: 'Views',
            6: 'Common Views',
            7: 'Schedule',
            8: 'Junk Email',
            9: 'IPM Subtree',
            10: 'Inbox',
            11: 'Outbox',
            12: 'Sent Items',
            13: 'Deleted Items',
            14: 'Contacts',
            15: 'Calendar',
            16: 'Drafts',
            17: 'Journal',
            18: 'Notes',
            19: 'Tasks',
            20: 'Recoverable Items',
            21: 'Deletions',
            22: 'Versions',
            23: 'Purges',
            24: 'Sync Issues',
            25: 'Conflicts',
            26: 'Local Failures',
            27: 'Server Failures',
        }

        # First, read Folder table to get SpecialFolderNumber and hierarchy
        folder_table_name = f"Folder_{self.current_mailbox}"
        folder_table = self.tables.get(folder_table_name)

        folder_info = {}  # FolderId hex -> info dict
        folder_hierarchy = {}  # FolderId -> parent FolderId

        if folder_table:
            folder_col_map = get_column_map(folder_table)
            self.status.showMessage("Reading folder metadata...")

            for i in range(folder_table.get_number_of_records()):
                try:
                    record = folder_table.get_record(i)
                    if not record:
                        continue

                    fid = get_bytes_value(record, folder_col_map.get('FolderId', -1))
                    parent_fid = get_bytes_value(record, folder_col_map.get('ParentFolderId', -1))
                    special_num_raw = get_bytes_value(record, folder_col_map.get('SpecialFolderNumber', -1))
                    display_name_raw = get_bytes_value(record, folder_col_map.get('DisplayName', -1))

                    if not fid:
                        continue

                    fid_hex = fid.hex()

                    # Get special folder number
                    special_num = None
                    if special_num_raw and len(special_num_raw) >= 4:
                        special_num = struct.unpack('<I', special_num_raw[:4])[0]

                    # Try to decode display name (often encrypted)
                    display_name = None
                    if display_name_raw:
                        try:
                            decoded = display_name_raw.decode('utf-16-le').rstrip('\x00')
                            if decoded and all(c.isprintable() or c.isspace() for c in decoded):
                                display_name = decoded
                        except:
                            pass

                    folder_info[fid_hex] = {
                        'special_num': special_num,
                        'display_name': display_name,
                        'parent_hex': parent_fid.hex() if parent_fid else None,
                        'record': i
                    }

                    if parent_fid:
                        folder_hierarchy[fid_hex] = parent_fid.hex()

                except:
                    pass

        # Now scan messages to count per folder
        msg_table_name = f"Message_{self.current_mailbox}"
        msg_table = self.tables.get(msg_table_name)

        if not msg_table:
            self.status.showMessage(f"Message table not found: {msg_table_name}")
            return

        self.status.showMessage(f"Scanning messages to build folder list...")

        col_map = get_column_map(msg_table)
        folder_counts = defaultdict(int)

        # Also try to detect mailbox owner from DisplayTo in Sent Items
        self.mailbox_owner = None
        display_to_counts = defaultdict(int)

        for i in range(msg_table.get_number_of_records()):
            try:
                record = msg_table.get_record(i)
                if not record:
                    continue
                folder_id = get_folder_id(record, col_map.get('FolderId', -1))
                if folder_id:
                    folder_counts[folder_id] += 1

                    # Check if this folder is Sent Items (special_num=11)
                    info = folder_info.get(folder_id)
                    if info and info.get('special_num') == 11:
                        # Get DisplayTo to find recipients (mailbox owner sent to these)
                        display_to = get_string_value(record, col_map.get('DisplayTo', -1))
                        if display_to:
                            display_to_counts[display_to] += 1
            except:
                pass

        # Detect mailbox owner from most common DisplayTo in Sent Items
        # (The mailbox owner often sends emails to themselves or common recipients)
        # Actually, for Sent Items we need to look at the From field, not DisplayTo
        # Let's try PropertyBlob for sender info instead

        # Count hidden vs visible per folder
        folder_hidden_counts = defaultdict(int)
        for i in range(msg_table.get_number_of_records()):
            try:
                record = msg_table.get_record(i)
                if not record:
                    continue
                folder_id = get_folder_id(record, col_map.get('FolderId', -1))
                is_hidden = get_bytes_value(record, col_map.get('IsHidden', -1))
                if folder_id and is_hidden and is_hidden != b'\x00':
                    folder_hidden_counts[folder_id] += 1
            except:
                pass

        for folder_id, count in folder_counts.items():
            # Get folder name from special number or display name
            info = folder_info.get(folder_id, {})
            special_num = info.get('special_num')
            display_name = info.get('display_name')

            # Determine display name
            final_name = None

            # Try folder mapping module first
            if HAS_FOLDER_MAPPING:
                mapped_path = get_mapped_folder_name(folder_id, special_num)
                if mapped_path:
                    # Extract just the folder name from path
                    final_name = mapped_path.rstrip('/').split('/')[-1] or 'Root'

            # Fall back to special folder names
            if not final_name and special_num is not None and special_num in special_folder_names:
                final_name = special_folder_names[special_num]

            # Try display name from database
            if not final_name and display_name:
                final_name = display_name

            # Final fallback - extract folder number from ID
            if not final_name:
                # Extract folder number from position 8-12 in the hex string
                if len(folder_id) >= 20:
                    folder_num = folder_id[-12:-8]  # Get the 4 hex chars for folder number
                    if HAS_FOLDER_MAPPING:
                        from folder_mapping import FOLDER_NUM_TO_NAME
                        final_name = FOLDER_NUM_TO_NAME.get(folder_num, f'Folder_{folder_num}')
                    else:
                        final_name = f'Folder_{folder_num}'
                else:
                    final_name = f'Folder_{folder_id[-8:]}'

            hidden_count = folder_hidden_counts.get(folder_id, 0)
            visible_count = count - hidden_count

            self.folders[folder_id] = {
                'id': folder_id,
                'display_name': final_name,
                'message_count': count,
                'visible_count': visible_count,
                'hidden_count': hidden_count,
                'special_num': special_num,
                'parent_id': info.get('parent_hex')
            }

        # Build hierarchical tree
        # First, create items for all folders
        folder_items = {}
        root_folders = []

        # Sort folders: special folders first (by number), then by name
        def sort_key(item):
            folder_id, folder = item
            special = folder.get('special_num')
            if special is not None:
                return (0, special, folder['display_name'])
            return (1, 999, folder['display_name'])

        for folder_id, folder in sorted(self.folders.items(), key=sort_key):
            item = QTreeWidgetItem()
            item.setText(0, folder['display_name'])
            # Show visible count, with hidden in parentheses if any
            visible = folder.get('visible_count', folder['message_count'])
            hidden = folder.get('hidden_count', 0)
            if hidden > 0:
                item.setText(1, f"{visible} (+{hidden})")
            else:
                item.setText(1, str(visible))
            item.setData(0, Qt.ItemDataRole.UserRole, folder_id)
            folder_items[folder_id] = item

            # Check if has parent
            parent_id = folder.get('parent_id')
            if parent_id and parent_id in folder_items:
                folder_items[parent_id].addChild(item)
            elif parent_id and parent_id in self.folders:
                # Parent exists but item not created yet - will be handled later
                pass
            else:
                root_folders.append((folder_id, item))

        # Add root folders to tree
        for folder_id, item in root_folders:
            self.folder_tree.addTopLevelItem(item)

        # Handle orphan children (parent was processed after child)
        for folder_id, folder in self.folders.items():
            parent_id = folder.get('parent_id')
            if parent_id and parent_id in folder_items:
                item = folder_items[folder_id]
                parent_item = folder_items[parent_id]
                # Check if not already added
                if item.parent() is None and self.folder_tree.indexOfTopLevelItem(item) == -1:
                    parent_item.addChild(item)

        # Expand all folders with messages
        for folder_id, item in folder_items.items():
            if self.folders[folder_id]['message_count'] > 0:
                parent = item.parent()
                while parent:
                    parent.setExpanded(True)
                    parent = parent.parent()

        # Detect mailbox owner from Sent Items
        self._detect_mailbox_owner()

        owner_info = f" | Owner: {self.mailbox_owner}" if self.mailbox_owner else ""
        self.status.showMessage(f"Found {len(self.folders)} folders with {sum(folder_counts.values())} messages{owner_info}")

    def _detect_mailbox_owner(self):
        """Detect mailbox owner from Mailbox table."""
        self.mailbox_owner = None
        self.mailbox_email = None

        # Read owner from Mailbox table
        mailbox_table = self.tables.get('Mailbox')
        if mailbox_table:
            col_map = get_column_map(mailbox_table)
            mb_num_idx = col_map.get('MailboxNumber', -1)
            owner_name_idx = col_map.get('MailboxOwnerDisplayName', -1)
            display_name_idx = col_map.get('DisplayName', -1)

            for rec_idx in range(mailbox_table.get_number_of_records()):
                try:
                    record = mailbox_table.get_record(rec_idx)
                    if not record:
                        continue

                    # Check if this is the current mailbox
                    mb_num_data = record.get_value_data(mb_num_idx)
                    if mb_num_data:
                        import struct
                        mb_num = struct.unpack('<I', mb_num_data)[0]
                        if mb_num != self.current_mailbox:
                            continue

                    # Try to get owner name from MailboxOwnerDisplayName or DisplayName
                    owner = None
                    for col_idx in [owner_name_idx, display_name_idx]:
                        if col_idx < 0:
                            continue
                        val = record.get_value_data(col_idx)
                        if val and HAS_DISSECT:
                            try:
                                decompressed = dissect_decompress(val)
                                # Try UTF-16-LE first (common for Exchange)
                                for enc in ['utf-16-le', 'utf-8']:
                                    try:
                                        text = decompressed.decode(enc).rstrip('\x00')
                                        # Skip system mailboxes
                                        if text and 'SystemMailbox' not in text:
                                            owner = text
                                            break
                                    except:
                                        pass
                                if owner:
                                    break
                            except:
                                pass

                    if owner:
                        self.mailbox_owner = owner
                        owner_lower = owner.lower().replace(' ', '')
                        self.mailbox_email = f"{owner_lower}@lab.sith.uz"
                        self.owner_label.setText(f"Owner: {self.mailbox_owner} <{self.mailbox_email}>")
                        break
                except:
                    pass

        # Fallback if no owner found
        if not self.mailbox_owner:
            self.mailbox_owner = ""
            self.mailbox_email = ""
            self.owner_label.setText(f"Mailbox {self.current_mailbox}")

        # Initialize email extractor with mailbox owner info
        if HAS_EMAIL_MODULE:
            self.email_extractor = EmailExtractor(self.mailbox_owner, self.mailbox_email)
        else:
            self.email_extractor = None

        # Initialize calendar extractor
        if HAS_CALENDAR_MODULE:
            self.calendar_extractor = CalendarExtractor(self.mailbox_owner, self.mailbox_email)
        else:
            self.calendar_extractor = None

    def _index_messages(self):
        """Index messages by folder."""
        self.messages_by_folder.clear()

        if not self.current_mailbox:
            return

        msg_table_name = f"Message_{self.current_mailbox}"
        msg_table = self.tables.get(msg_table_name)

        if not msg_table:
            return

        col_map = get_column_map(msg_table)

        for i in range(msg_table.get_number_of_records()):
            try:
                record = msg_table.get_record(i)
                if not record:
                    continue

                folder_id = get_folder_id(record, col_map.get('FolderId', -1))
                if folder_id:
                    self.messages_by_folder[folder_id].append(i)
            except:
                pass

    def _on_folder_selected(self):
        """Handle folder selection - load and cache all messages with optimizations."""
        self.message_list.clear()
        self.all_messages_cache = []
        self.export_folder_btn.setEnabled(False)
        self.export_calendar_btn.setEnabled(False)
        self.export_eml_btn.setEnabled(False)
        self.export_eml_btn2.setEnabled(False)
        self.export_attach_btn.setEnabled(False)

        items = self.folder_tree.selectedItems()
        if not items:
            self.msg_count_label.setText("")
            return

        folder_id = items[0].data(0, Qt.ItemDataRole.UserRole)
        self.current_folder_id = folder_id
        self.export_folder_btn.setEnabled(True)
        self.export_calendar_btn.setEnabled(HAS_CALENDAR_MODULE)
        message_indices = self.messages_by_folder.get(folder_id, [])

        if not message_indices:
            self.status.showMessage(f"No messages in this folder")
            self.msg_count_label.setText("0 messages")
            return

        # Check folder cache first
        cache_key = (self.current_mailbox, folder_id)
        if cache_key in self.folder_messages_cache:
            self.all_messages_cache = self.folder_messages_cache[cache_key]
            self._apply_filters()
            total = len(self.all_messages_cache)
            hidden_count = sum(1 for m in self.all_messages_cache if m.get('is_hidden'))
            failed_count = sum(1 for m in self.all_messages_cache if m.get('has_error'))
            status_parts = [f"{total} messages"]
            if hidden_count:
                status_parts.append(f"{hidden_count} hidden")
            if failed_count:
                status_parts.append(f"{failed_count} failed")
            self.msg_count_label.setText(" | ".join(status_parts))
            self.status.showMessage(f"Loaded from cache")
            return

        msg_table_name = f"Message_{self.current_mailbox}"
        msg_table = self.tables.get(msg_table_name)

        if not msg_table:
            return

        col_map = get_column_map(msg_table)

        # Show progress bar
        total_msgs = min(len(message_indices), 2000)
        self.progress.setVisible(True)
        self.progress.setRange(0, total_msgs)
        self.status.showMessage(f"Loading {total_msgs} messages...")

        # Load messages with lightweight extraction (no full body decode)
        hidden_count = 0
        failed_count = 0
        for i, rec_idx in enumerate(message_indices[:2000]):
            # Update progress every 50 messages
            if i % 50 == 0:
                self.progress.setValue(i)
                QApplication.processEvents()

            has_error = False
            try:
                record = msg_table.get_record(rec_idx)
                if not record:
                    continue

                # Check IsHidden flag
                is_hidden = get_bytes_value(record, col_map.get('IsHidden', -1))
                is_hidden_val = bool(is_hidden and is_hidden != b'\x00')

                if is_hidden_val:
                    hidden_count += 1

                # Get date (fast - just struct unpack)
                date_received = get_filetime_value(record, col_map.get('DateReceived', -1))
                date_str = date_received.strftime("%Y-%m-%d %H:%M") if date_received else ""

                # Get flags (fast - just byte check)
                is_read_raw = get_bytes_value(record, col_map.get('IsRead', -1))
                is_read = bool(is_read_raw and is_read_raw != b'\x00')

                has_attach_raw = get_bytes_value(record, col_map.get('HasAttachments', -1))
                has_attach = bool(has_attach_raw and has_attach_raw != b'\x00')

                # Extract basic fields from PropertyBlob (lightweight - no body decode)
                prop_blob = get_bytes_value(record, col_map.get('PropertyBlob', -1))
                subject = ""
                from_display = ""
                to_display = ""

                if prop_blob:
                    try:
                        subject = extract_subject_from_blob(prop_blob) or ""
                        sender = extract_sender_from_blob(prop_blob)
                        # Validate sender - filter out hex-like strings
                        if sender and _is_valid_name(sender):
                            from_display = sender
                        to_display = from_display
                    except Exception:
                        has_error = True

                # Fallback for empty fields - use mailbox owner
                if not from_display and hasattr(self, 'mailbox_owner') and self.mailbox_owner:
                    from_display = self.mailbox_owner
                if not to_display and hasattr(self, 'mailbox_owner') and self.mailbox_owner:
                    to_display = self.mailbox_owner

            except Exception as e:
                has_error = True
                date_str = ""
                date_received = None
                from_display = "[Error]"
                to_display = "[Error]"
                subject = f"[Failed to decode record {rec_idx}]"
                is_read = False
                has_attach = False
                is_hidden_val = False

            if has_error:
                failed_count += 1

            # Cache message data
            msg_data = {
                'rec_idx': rec_idx,
                'date': date_str,
                'date_obj': date_received,
                'from': from_display,
                'to': to_display,
                'subject': subject,
                'is_read': is_read,
                'has_attach': has_attach,
                'is_hidden': is_hidden_val,
                'has_error': has_error,
            }
            self.all_messages_cache.append(msg_data)

        # Store in folder cache
        self.folder_messages_cache[cache_key] = self.all_messages_cache

        # Hide progress bar
        self.progress.setVisible(False)

        # Apply filters and display
        self._apply_filters()

        total = len(self.all_messages_cache)
        status_parts = [f"{total} messages"]
        if hidden_count:
            status_parts.append(f"{hidden_count} hidden")
        if failed_count:
            status_parts.append(f"{failed_count} failed")
        self.msg_count_label.setText(" | ".join(status_parts))

    def _apply_filters(self):
        """Apply search and filter criteria to cached messages."""
        self.message_list.clear()

        search_text = self.search_input.text().lower().strip()
        read_filter = self.filter_read_combo.currentIndex()  # 0=All, 1=Unread, 2=Read, 3=Failed
        attach_filter = self.filter_attach_cb.isChecked()
        show_hidden = self.show_hidden_cb.isChecked()

        shown_count = 0
        for msg in self.all_messages_cache:
            # Apply hidden filter
            if msg.get('is_hidden') and not show_hidden:
                continue

            # Apply search filter
            if search_text:
                searchable = f"{msg['subject']} {msg['from']} {msg['to']}".lower()
                if search_text not in searchable:
                    continue

            # Apply read status filter
            if read_filter == 1 and msg['is_read']:  # Unread only
                continue
            if read_filter == 2 and not msg['is_read']:  # Read only
                continue
            if read_filter == 3 and not msg.get('has_error'):  # Failed only
                continue

            # Apply attachment filter
            if attach_filter and not msg['has_attach']:
                continue

            # Create list item
            item = QTreeWidgetItem()
            item.setText(0, str(msg['rec_idx']))
            item.setData(0, Qt.ItemDataRole.UserRole, msg['rec_idx'])
            item.setText(1, msg['date'])
            item.setText(2, msg['from'])  # Always show sender in From
            item.setText(3, msg['to'])    # Always show recipient in To
            item.setText(4, msg['subject'])
            item.setText(5, "📎" if msg['has_attach'] else "")
            item.setText(6, "✓" if msg['is_read'] else "")

            # Mark unread messages as bold
            if not msg['is_read']:
                font = item.font(0)
                font.setBold(True)
                for col in range(7):
                    item.setFont(col, font)

            # Mark failed/error messages in red
            if msg.get('has_error'):
                item.setText(4, f"[ERROR] {msg['subject']}")
                for col in range(7):
                    item.setForeground(col, QColor(200, 0, 0))

            # Mark hidden items visually (gray, lower priority than red)
            elif msg.get('is_hidden'):
                item.setText(4, f"[HIDDEN] {msg['subject']}")
                for col in range(7):
                    item.setForeground(col, Qt.GlobalColor.gray)

            self.message_list.addTopLevelItem(item)
            shown_count += 1

            if shown_count >= 500:
                break

        # Update status
        total = len(self.all_messages_cache)
        if shown_count < total:
            self.status.showMessage(f"Showing {shown_count} of {total} messages (filtered)")
        else:
            self.status.showMessage(f"Showing {shown_count} messages")

    def _on_search_changed(self, text):
        """Handle search input change."""
        self._apply_filters()

    def _on_filter_changed(self):
        """Handle filter change."""
        self._apply_filters()

    def _on_clear_filters(self):
        """Clear all search and filter criteria."""
        self.search_input.clear()
        self.filter_read_combo.setCurrentIndex(0)
        self.filter_attach_cb.setChecked(False)
        self._apply_filters()

    def _on_message_selected(self):
        """Handle message selection."""
        items = self.message_list.selectedItems()
        if not items:
            return

        rec_idx = items[0].data(0, Qt.ItemDataRole.UserRole)
        self.current_record_idx = rec_idx

        msg_table_name = f"Message_{self.current_mailbox}"
        msg_table = self.tables.get(msg_table_name)

        if not msg_table:
            return

        record = msg_table.get_record(rec_idx)
        if not record:
            return

        # Enable export button
        self.export_eml_btn.setEnabled(True)
        self.export_eml_btn2.setEnabled(True)

        col_map = {}
        columns = []
        for i in range(msg_table.get_number_of_columns()):
            col = msg_table.get_column(i)
            if col:
                col_map[col.name] = i
                columns.append((i, col.name, col.type))

        # Get PropertyBlob
        prop_blob = get_bytes_value(record, col_map.get('PropertyBlob', -1))

        # Load attachments
        self._load_attachments(rec_idx, record, col_map)

        # Get folder info
        folder_id = get_folder_id(record, col_map.get('FolderId', -1))
        folder_name = self.folders.get(folder_id, {}).get('display_name', 'Unknown')

        # === Create EmailMessage for structured extraction ===
        email_msg = None
        if HAS_EMAIL_MODULE and self.email_extractor:
            email_msg = self.email_extractor.extract_message(
                record, col_map, rec_idx,
                folder_name=folder_name,
                tables=self.tables,
                mailbox_num=self.current_mailbox
            )
            self.current_email_message = email_msg

        # === Parsed View - use EmailMessage if available ===
        parsed_text = f"Record #{rec_idx}\n{'='*50}\n\n"

        if email_msg:
            # Use data from EmailMessage object
            parsed_text += f"Folder: {email_msg.folder_name}\n"

            if email_msg.date_received:
                parsed_text += f"Date Received: {email_msg.date_received}\n"
            if email_msg.date_sent:
                parsed_text += f"Date Sent: {email_msg.date_sent}\n"

            parsed_text += f"Is Read: {email_msg.is_read}\n"
            parsed_text += f"Has Attachments: {email_msg.has_attachments}\n"

            if email_msg.to_names:
                parsed_text += f"Display To: {', '.join(email_msg.to_names)}\n"

            parsed_text += f"\n--- Email Message Data ---\n"
            parsed_text += f"\nSubject: {email_msg.subject or '(No Subject)'}\n"
            parsed_text += f"From: {email_msg.get_from_header()}\n"
            parsed_text += f"To: {email_msg.get_to_header() or '(unknown)'}\n"

            if email_msg.message_id:
                parsed_text += f"Message-ID: {email_msg.message_id}\n"

            parsed_text += f"\nMessage Class: {email_msg.message_class}\n"

            importance_map = {0: 'Low', 1: 'Normal', 2: 'High'}
            parsed_text += f"Importance: {importance_map.get(email_msg.importance, 'Normal')}\n"

            if email_msg.attachments:
                parsed_text += f"\nAttachments ({len(email_msg.attachments)}):\n"
                for att in email_msg.attachments:
                    parsed_text += f"  - {att.filename} ({att.size} bytes)\n"

        else:
            # Fallback to old extraction methods
            parsed_text += f"Folder: {folder_name}\n"

            # Dates
            date_received = get_filetime_value(record, col_map.get('DateReceived', -1))
            date_sent = get_filetime_value(record, col_map.get('DateSent', -1))

            if date_received:
                parsed_text += f"Date Received: {date_received}\n"
            if date_sent:
                parsed_text += f"Date Sent: {date_sent}\n"

            # Flags
            is_read = get_bytes_value(record, col_map.get('IsRead', -1))
            if is_read:
                is_read_val = is_read != b'\x00'
                parsed_text += f"Is Read: {is_read_val}\n"

            has_attach = get_bytes_value(record, col_map.get('HasAttachments', -1))
            if has_attach:
                has_attach_val = has_attach != b'\x00'
                parsed_text += f"Has Attachments: {has_attach_val}\n"

            # DisplayTo
            display_to = get_string_value(record, col_map.get('DisplayTo', -1))
            if display_to:
                parsed_text += f"Display To: {display_to}\n"

            # PropertyBlob Analysis
            if prop_blob:
                parsed_text += f"\n--- PropertyBlob Analysis ({len(prop_blob)} bytes) ---\n"

                # Extract Subject
                subject = extract_subject_from_blob(prop_blob)
                if subject:
                    parsed_text += f"\nSubject: {subject}\n"

                # Extract Sender
                sender = extract_sender_from_blob(prop_blob)
                # Validate sender - filter out Message-ID looking strings
                if sender and not is_valid_sender_name(sender):
                    sender = None
                # Fallback to mailbox owner
                if not sender and hasattr(self, 'mailbox_owner') and self.mailbox_owner:
                    sender = self.mailbox_owner
                if sender:
                    parsed_text += f"From: {sender} <{sender.lower().replace(' ', '')}@lab.sith.uz>\n"
                    parsed_text += f"To: {sender} <{sender.lower().replace(' ', '')}@lab.sith.uz>\n"

                # Extract Message-ID
                msgid = extract_message_id_from_blob(prop_blob)
                if msgid:
                    parsed_text += f"Message-ID: {msgid}\n"

        # PropertyBlob hex info (always show)
        if prop_blob:
            # Find Exchange DN
            dn_match = re.search(rb'/O=[A-Z0-9]+/OU=[^/\x00]+(?:/CN=[^/\x00]+)*', prop_blob, re.IGNORECASE)
            if dn_match:
                dn_clean = bytes(b for b in dn_match.group() if 32 <= b < 127)
                parsed_text += f"\nExchange DN: {dn_clean.decode('ascii', errors='ignore')}\n"

        self.parsed_view.setPlainText(parsed_text)

        # === Body View ===
        body_text = ""
        html_source = ""
        body_data_raw = None
        body_data_decompressed = None

        # Try NativeBody FIRST - it contains the actual HTML body content
        native_body_idx = col_map.get('NativeBody', -1)
        if native_body_idx >= 0:
            try:
                if record.is_long_value(native_body_idx):
                    lv = record.get_value_data_as_long_value(native_body_idx)
                    if lv and hasattr(lv, 'get_data'):
                        body_data_raw = lv.get_data()
                        if body_data_raw and len(body_data_raw) > 7:
                            header_type = body_data_raw[0]

                            # Use LZXPRESS decompression (with dissect.esedb if available)
                            if HAS_LZXPRESS:
                                try:
                                    body_data_decompressed = decompress_exchange_body(body_data_raw)
                                    if body_data_decompressed and len(body_data_decompressed) > 10:
                                        # Extract text from HTML
                                        body_text = extract_text_from_html(body_data_decompressed)
                                        # Store HTML source
                                        html_source = body_data_decompressed.decode('utf-8', errors='replace')
                                except Exception as e:
                                    pass

                            # Fallback if decompression didn't work
                            if not body_text and body_data_raw:
                                # Try to extract printable text directly
                                content = body_data_raw[7:] if header_type in [0x17, 0x18, 0x19] else body_data_raw
                                printable = bytes(b for b in content if 32 <= b <= 126 or b in [9, 10, 13])
                                if printable:
                                    body_text = printable.decode('ascii', errors='ignore')
                                    html_source = body_text
            except Exception as e:
                pass

        # Try combined extraction with PropertyBlob if NativeBody failed
        if not body_text and HAS_LZXPRESS:
            try:
                body_text = get_body_preview(body_data_raw, 2000, prop_blob)
            except:
                pass

        # Fall back to PropertyBlob strings
        if not body_text and prop_blob:
            if HAS_LZXPRESS:
                try:
                    body_text = extract_body_from_property_blob(prop_blob)
                except:
                    pass

            if not body_text:
                strings = re.findall(rb'[\x20-\x7e]{10,}', prop_blob)
                if strings:
                    body_text = "--- Extracted from PropertyBlob ---\n\n"
                    body_text += '\n'.join(s.decode('ascii', errors='ignore') for s in strings[:10])

        # Set Body (Text) view
        if body_text:
            self.body_view.setPlainText(body_text)
        else:
            dissect_status = "dissect.esedb: Available" if HAS_DISSECT else "dissect.esedb: Not installed"
            lzx_status = "lzxpress module: Loaded" if HAS_LZXPRESS else "lzxpress module: Not available"
            note = f"""(No body content found)

Decompression Status:
  {dissect_status}
  {lzx_status}

This message may not have body content, or uses an unsupported format.
Check the "HTML Source" tab for raw decompressed HTML.
Check the "Raw Body" tab to see compressed data."""
            self.body_view.setPlainText(note)

        # Set Body (HTML) view - render HTML like a browser with styles/images/links
        if html_source:
            # Wrap in proper HTML document with white background if not already a full document
            if not html_source.strip().lower().startswith('<!doctype') and not html_source.strip().lower().startswith('<html'):
                wrapped_html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
body {{ background-color: white; color: black; font-family: Arial, sans-serif; padding: 10px; }}
a {{ color: #0066cc; }}
</style>
</head>
<body>
{html_source}
</body>
</html>"""
                self.html_browser_view.setHtml(wrapped_html)
            else:
                # Already a full HTML document, inject white background if needed
                if 'background' not in html_source.lower():
                    html_source = html_source.replace('<body', '<body style="background-color: white;"', 1)
                self.html_browser_view.setHtml(html_source)
        else:
            no_content_html = """<!DOCTYPE html>
<html>
<head><style>body { background-color: white; color: gray; font-family: Arial, sans-serif; padding: 20px; }</style></head>
<body><p>(No HTML content available)</p></body>
</html>"""
            self.html_browser_view.setHtml(no_content_html)

        # Set HTML Source view - show raw HTML code
        if html_source:
            self.html_source_view.setPlainText(html_source)
        else:
            self.html_source_view.setPlainText("(No HTML content available)")

        # Store raw body data for toggle view
        self.current_raw_body_compressed = body_data_raw
        self.current_raw_body_decompressed = body_data_decompressed
        self._update_raw_body_view()

        # Store for EML export - use actual body text if available
        self.current_email_data['body_text'] = body_text if body_text else ""
        self.current_email_data['body_html'] = html_source if html_source else ""

        # === Hex View ===
        if prop_blob:
            hex_text = f"PropertyBlob - {len(prop_blob)} bytes\n{'='*50}\n\n"
            hex_text += self._hexdump(prop_blob)
            self.hex_view.setPlainText(hex_text)
        else:
            self.hex_view.setPlainText("No PropertyBlob data")

        # === ASCII View ===
        ascii_text = "ASCII Strings Found\n" + "="*50 + "\n\n"
        if prop_blob:
            strings = re.findall(rb'[\x20-\x7e]{4,}', prop_blob)
            for s in strings:
                ascii_text += s.decode('ascii') + "\n"
        self.ascii_view.setPlainText(ascii_text)

        # === All Columns ===
        self.columns_table.setRowCount(len(columns))
        for row, (idx, name, ctype) in enumerate(columns):
            val = record.get_value_data(idx)

            self.columns_table.setItem(row, 0, QTableWidgetItem(name))
            self.columns_table.setItem(row, 1, QTableWidgetItem(str(len(val)) if val else "0"))

            if val:
                if len(val) <= 50:
                    display = val.hex()
                else:
                    display = val[:50].hex() + "..."
                self.columns_table.setItem(row, 2, QTableWidgetItem(display))
            else:
                self.columns_table.setItem(row, 2, QTableWidgetItem("(empty)"))

        # Store email data for export - use EmailMessage if available
        # Convert attachments to 3-tuple format for EML export (skip external references)
        eml_attachments = []
        for att in self.current_attachments:
            is_external = att[3] if len(att) > 3 else False
            if not is_external:
                eml_attachments.append((att[0], att[1], att[2]))

        if email_msg:
            # Use data from EmailMessage object
            self.current_email_data = {
                'record_index': email_msg.record_index,
                'subject': email_msg.subject,
                'sender_name': email_msg.sender_name,
                'sender_email': email_msg.sender_email,
                'recipient_name': email_msg.to_names[0] if email_msg.to_names else email_msg.sender_name,
                'recipient_email': email_msg.to_emails[0] if email_msg.to_emails else email_msg.sender_email,
                'message_id': email_msg.message_id,
                'date_sent': email_msg.date_sent,
                'folder_name': email_msg.folder_name,
                'has_attachments': email_msg.has_attachments,
                'body_text': body_text if body_text else email_msg.subject,
                'body_html': html_source,
                'attachments': eml_attachments
            }
            # Update EmailMessage with better body content
            if body_text:
                email_msg.body_text = body_text
            if html_source:
                email_msg.body_html = html_source
        else:
            # Fallback to old extraction
            subject = extract_subject_from_blob(prop_blob) if prop_blob else ""
            sender = extract_sender_from_blob(prop_blob) if prop_blob else ""
            msgid = extract_message_id_from_blob(prop_blob) if prop_blob else ""

            has_attach = get_bytes_value(record, col_map.get('HasAttachments', -1))
            has_attachments = bool(has_attach and has_attach != b'\x00')

            date_sent = get_filetime_value(record, col_map.get('DateSent', -1))

            # Use detected mailbox owner if sender not found
            if not sender and hasattr(self, 'mailbox_owner') and self.mailbox_owner:
                sender = self.mailbox_owner

            # Get DisplayTo for recipient
            display_to = get_string_value(record, col_map.get('DisplayTo', -1))
            recipient_name = display_to if display_to else sender
            recipient_email = f"{display_to}@lab.sith.uz" if display_to else (self.mailbox_email if hasattr(self, 'mailbox_email') else "unknown@lab.sith.uz")

            self.current_email_data = {
                'record_index': rec_idx,
                'subject': subject,
                'sender_name': sender,
                'sender_email': self.mailbox_email if hasattr(self, 'mailbox_email') and self.mailbox_email else f"{sender}@lab.sith.uz" if sender else "unknown@lab.sith.uz",
                'recipient_name': recipient_name,
                'recipient_email': recipient_email,
                'message_id': msgid,
                'date_sent': date_sent,
                'folder_name': folder_name,
                'has_attachments': has_attachments,
                'body_text': body_text if body_text else subject,
                'body_html': html_source,
                'attachments': eml_attachments
            }
            self.current_email_message = None

    def _hexdump(self, data, width=16):
        lines = []
        for i in range(0, len(data), width):
            chunk = data[i:i+width]
            hex_part = ' '.join(f'{b:02x}' for b in chunk)
            ascii_part = ''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)
            lines.append(f'{i:08x}  {hex_part:<{width*3}}  {ascii_part}')
        return '\n'.join(lines)

    def _on_show_hidden_changed(self, state):
        """Toggle showing hidden items - just re-apply filters, don't reload."""
        if self.all_messages_cache:
            self._apply_filters()

    def _on_raw_toggle_changed(self, state):
        """Toggle between compressed and decompressed raw body view."""
        self._update_raw_body_view()

    def _update_raw_body_view(self):
        """Update the raw body view based on toggle state."""
        show_compressed = self.raw_compressed_cb.isChecked()

        if show_compressed and self.current_raw_body_compressed:
            # Show hex dump of compressed data
            data = self.current_raw_body_compressed
            text = f"Raw NativeBody (Compressed) - {len(data)} bytes\n{'='*60}\n\n"
            text += "Header: " + ' '.join(f'{b:02x}' for b in data[:7]) + "\n"

            header_type = data[0] if data else 0
            type_descriptions = {
                0x18: "0x18 - LZXPRESS compressed HTML",
                0x19: "0x19 - LZXPRESS compressed variant",
                0x17: "0x17 - Plain/encrypted content",
                0x10: "0x10 - Plain text format",
                0x12: "0x12 - Plain text variant",
                0x14: "0x14 - Other format",
                0x15: "0x15 - Other format",
            }
            type_desc = type_descriptions.get(header_type, f"0x{header_type:02x} - Unknown format")
            text += f"Type: {type_desc}\n"

            if len(data) > 2:
                import struct
                uncompressed_size = struct.unpack('<H', data[1:3])[0]
                text += f"Expected uncompressed size: {uncompressed_size} bytes\n"

            text += f"\nDecompression: {'dissect.esedb (proper LZXPRESS)' if HAS_DISSECT else 'fallback decoder'}\n"
            text += "\nHex dump:\n"
            text += self._hexdump(data)
            self.raw_body_view.setPlainText(text)
        elif not show_compressed and self.current_raw_body_decompressed:
            # Show decompressed content
            data = self.current_raw_body_decompressed
            text = f"Raw NativeBody (Decompressed) - {len(data)} bytes\n{'='*60}\n\n"
            text += f"Decompression method: {'dissect.esedb' if HAS_DISSECT else 'fallback'}\n\n"
            # Show as text if mostly printable
            printable_count = sum(1 for b in data if 32 <= b <= 126 or b in [9, 10, 13])
            if printable_count > len(data) * 0.7:
                text += data.decode('utf-8', errors='replace')
            else:
                text += "Hex dump:\n"
                text += self._hexdump(data)
            self.raw_body_view.setPlainText(text)
        else:
            self.raw_body_view.setPlainText("(No raw body data available)")

    def _on_refresh(self):
        if self.current_mailbox:
            self._load_folders()
            self._index_messages()

    def _on_export(self):
        if not self.current_mailbox:
            QMessageBox.warning(self, "Export", "Please select a mailbox first")
            return

        output_dir = QFileDialog.getExistingDirectory(self, "Select Export Directory")
        if not output_dir:
            return

        QMessageBox.information(
            self, "Export",
            f"Export functionality available via command line:\n\n"
            f"python export_mailbox.py <edb_file> -m {self.current_mailbox} -o {output_dir}"
        )

    def _parse_subobjects_blob(self, blob):
        """Parse SubobjectsBlob to extract attachment Inid references."""
        if not blob or len(blob) < 4:
            return []

        inids = []

        # Pattern 1: Standard format - header bytes followed by 21XX pairs where XX is Inid
        i = 0
        while i < len(blob) - 1:
            if blob[i] == 0x21:  # Marker for Inid reference
                inid = blob[i + 1]
                inids.append(inid)
                i += 2
            else:
                i += 1

        if inids:
            return inids

        # Pattern 2: 0x0f format - found in some Exchange versions
        # Structure: first byte=length, then data with Inid values at positions after 0x84 markers
        # The Inid values are stored with +20 offset
        if len(blob) >= 8 and blob[0] == 0x0f:
            # Look for 0x84 markers followed by encoded Inid
            for i in range(len(blob) - 1):
                if blob[i] == 0x84:
                    encoded = blob[i + 1]
                    # Decode with -20 offset
                    if encoded >= 20:
                        potential_inid = encoded - 20
                        if 1 <= potential_inid <= 100:  # Reasonable Inid range
                            inids.append(potential_inid)

        if inids:
            return inids

        # If no patterns matched, return fallback marker
        return ['FALLBACK']

    def _load_attachments(self, rec_idx, record, col_map):
        """Load attachments for the current message."""
        self.current_attachments = []
        self.attach_list.clear()
        self.export_attach_btn.setEnabled(False)
        self.save_attach_btn.setEnabled(False)
        self.save_all_attach_btn.setEnabled(False)

        # Check if message has attachments
        has_attach = get_bytes_value(record, col_map.get('HasAttachments', -1))
        if not has_attach or has_attach == b'\x00':
            self.content_tabs.setTabText(2, "Attachments (0)")
            return

        # Get SubobjectsBlob for attachment linking
        subobjects = get_bytes_value(record, col_map.get('SubobjectsBlob', -1))
        linked_inids = self._parse_subobjects_blob(subobjects) if subobjects else []

        # Get attachment table
        attach_table_name = f"Attachment_{self.current_mailbox}"
        attach_table = self.tables.get(attach_table_name)

        if not attach_table:
            self.content_tabs.setTabText(2, "Attachments (0)")
            return

        attach_col_map = get_column_map(attach_table)

        # Build Inid to record index map
        inid_to_record = {}
        for i in range(attach_table.get_number_of_records()):
            try:
                att_record = attach_table.get_record(i)
                if not att_record:
                    continue
                inid = get_bytes_value(att_record, attach_col_map.get('Inid', -1))
                if inid and len(inid) >= 4:
                    inid_val = struct.unpack('<I', inid[:4])[0]
                    inid_to_record[inid_val] = i
            except:
                pass

        # Load linked attachments only (no fallback to avoid duplicates)
        records_to_load = []
        use_fallback = False

        if linked_inids and linked_inids != ['FALLBACK']:
            for inid_val in linked_inids:
                if inid_val in inid_to_record:
                    records_to_load.append(inid_to_record[inid_val])
        elif linked_inids == ['FALLBACK'] and subobjects:
            # SubobjectsBlob exists but uses different format - try fallback
            use_fallback = True
            # Try to find attachments that might belong to this message
            # by checking MessageDocumentId or scanning recently added attachments
            msg_doc_id = get_int_value(record, col_map.get('MessageDocumentId', -1))
            if msg_doc_id:
                # Try to find attachments with matching MessageDocumentId
                for i in range(attach_table.get_number_of_records()):
                    try:
                        att_record = attach_table.get_record(i)
                        if not att_record:
                            continue
                        att_msg_id = get_int_value(att_record, attach_col_map.get('MessageDocumentId', -1))
                        if att_msg_id and att_msg_id == msg_doc_id:
                            records_to_load.append(i)
                    except:
                        pass

            # If still no attachments found, try scanning for unlinked attachments
            if not records_to_load:
                # Get all attachments that haven't been linked to other messages
                # This is a heuristic - load all attachments and let user see them
                for i in range(attach_table.get_number_of_records()):
                    att_record = attach_table.get_record(i)
                    if att_record:
                        content = get_bytes_value(att_record, attach_col_map.get('Content', -1))
                        size = get_bytes_value(att_record, attach_col_map.get('Size', -1))
                        size_val = struct.unpack('<Q', size)[0] if size and len(size) == 8 else 0
                        # Look for large attachments (like sithTA.zip ~3.3MB)
                        if size_val > 1000000:
                            records_to_load.append(i)
        elif not subobjects:
            # Only fallback if SubobjectsBlob is completely missing (very old database)
            # This maintains backward compatibility while avoiding duplicates
            records_to_load = list(range(attach_table.get_number_of_records()))
        # If SubobjectsBlob exists but has no 0x21 markers, it may be embedded messages
        # In that case, don't load any attachments from the attachment table

        for i in records_to_load:
            try:
                att_record = attach_table.get_record(i)
                if not att_record:
                    continue

                prop_blob = get_bytes_value(att_record, attach_col_map.get('PropertyBlob', -1))
                content = get_bytes_value(att_record, attach_col_map.get('Content', -1))

                # Try to get filename from Name column first
                name_col = get_bytes_value(att_record, attach_col_map.get('Name', -1))
                filename = None

                # Name column is often encrypted, try PropertyBlob first
                if prop_blob:
                    filename = extract_attachment_filename(prop_blob)

                if not filename and name_col:
                    try:
                        decoded = name_col.decode('utf-16-le').rstrip('\x00')
                        # Check if it looks like a valid filename
                        if decoded and all(c.isprintable() for c in decoded):
                            filename = decoded
                    except:
                        pass

                if not filename:
                    filename = f"attachment_{i}.bin"

                # Get content type from PropertyBlob (ContentType column often encrypted)
                content_type = "application/octet-stream"
                if prop_blob:
                    content_type = extract_attachment_content_type(prop_blob)

                # Skip if no content
                if not content:
                    continue

                is_external = False
                actual_content = content

                # Check if this is a Long Value reference (4 bytes)
                if len(content) == 4:
                    content_idx = attach_col_map.get('Content', -1)
                    if content_idx >= 0:
                        try:
                            # Try to get Long Value data
                            if att_record.is_long_value(content_idx):
                                lv = att_record.get_value_data_as_long_value(content_idx)
                                if lv and hasattr(lv, 'get_data'):
                                    lv_data = lv.get_data()
                                    if lv_data and len(lv_data) > 0:
                                        actual_content = lv_data
                                    else:
                                        is_external = True
                                else:
                                    is_external = True
                            else:
                                is_external = True
                        except Exception as e:
                            is_external = True
                    else:
                        is_external = True
                # Decode UTF-16LE BOM content if needed
                elif content.startswith(b'\xff\xfe'):
                    try:
                        text = content.decode('utf-16-le')
                        actual_content = text.encode('utf-8')
                    except:
                        pass

                if is_external:
                    display_name = f"{filename} (external reference - {content.hex()})"
                else:
                    display_name = f"{filename} ({len(actual_content)} bytes)"

                # Deduplicate by filename and size
                is_duplicate = False
                for existing in self.current_attachments:
                    if existing[0] == filename and len(existing[2]) == len(actual_content):
                        is_duplicate = True
                        break

                if not is_duplicate:
                    self.current_attachments.append((filename, content_type, actual_content, is_external))

                    item = QListWidgetItem(display_name)
                    item.setData(Qt.ItemDataRole.UserRole, len(self.current_attachments) - 1)
                    self.attach_list.addItem(item)

            except Exception as e:
                pass

        count = len(self.current_attachments)
        self.content_tabs.setTabText(2, f"Attachments ({count})")

        if count > 0:
            self.export_attach_btn.setEnabled(True)
            self.save_attach_btn.setEnabled(True)
            self.save_all_attach_btn.setEnabled(True)

    def _on_export_eml(self):
        """Export current message as EML file using stable EmailMessage class."""
        # Use new EmailMessage if available
        if HAS_EMAIL_MODULE and self.current_email_message:
            subject_safe = re.sub(r'[<>:"/\\|?*]', '_', self.current_email_message.subject or 'no_subject')[:50]
            default_name = f"record_{self.current_record_idx}_{subject_safe}.eml"

            path, _ = QFileDialog.getSaveFileName(
                self, "Save Email as EML", default_name,
                "Email Files (*.eml);;All Files (*.*)"
            )

            if not path:
                return

            try:
                eml_content = self.current_email_message.to_eml()
                with open(path, 'wb') as f:
                    f.write(eml_content)
                self.status.showMessage(f"Exported email to {path}")
                QMessageBox.information(self, "Export", f"Email saved to:\n{path}")
            except Exception as e:
                QMessageBox.critical(self, "Export Error", f"Failed to export email:\n{e}")
            return

        # Fallback to old method
        if not self.current_email_data:
            QMessageBox.warning(self, "Export", "No message selected")
            return

        # Generate default filename
        subject_safe = re.sub(r'[<>:"/\\|?*]', '_', self.current_email_data.get('subject', 'no_subject'))[:50]
        default_name = f"record_{self.current_record_idx}_{subject_safe}.eml"

        path, _ = QFileDialog.getSaveFileName(
            self, "Save Email as EML", default_name,
            "Email Files (*.eml);;All Files (*.*)"
        )

        if not path:
            return

        try:
            eml_content = create_eml_content(self.current_email_data)
            with open(path, 'wb') as f:
                f.write(eml_content)
            self.status.showMessage(f"Exported email to {path}")
            QMessageBox.information(self, "Export", f"Email saved to:\n{path}")
        except Exception as e:
            QMessageBox.critical(self, "Export Error", f"Failed to export email:\n{e}")

    def _on_export_attachments(self):
        """Export all attachments from current message."""
        if not self.current_attachments:
            QMessageBox.warning(self, "Export", "No attachments to export")
            return

        output_dir = QFileDialog.getExistingDirectory(self, "Select Directory for Attachments")
        if not output_dir:
            return

        saved = 0
        skipped = 0
        for att in self.current_attachments:
            filename = att[0]
            content_type = att[1]
            data = att[2]
            is_external = att[3] if len(att) > 3 else False

            if is_external:
                skipped += 1
                continue

            try:
                out_path = Path(output_dir) / filename
                # Handle duplicate filenames
                counter = 1
                while out_path.exists():
                    name, ext = os.path.splitext(filename)
                    out_path = Path(output_dir) / f"{name}_{counter}{ext}"
                    counter += 1

                with open(out_path, 'wb') as f:
                    f.write(data)
                saved += 1
            except Exception as e:
                self.status.showMessage(f"Error saving {filename}: {e}")

        msg = f"Saved {saved} attachments to {output_dir}"
        if skipped:
            msg += f" ({skipped} external references skipped)"
        self.status.showMessage(msg)
        QMessageBox.information(self, "Export", f"Saved {saved} attachments to:\n{output_dir}\n\n{skipped} external references skipped" if skipped else f"Saved {saved} attachments to:\n{output_dir}")

    def _on_export_folder(self):
        """Export all messages in the current folder as EML files."""
        items = self.folder_tree.selectedItems()
        if not items:
            QMessageBox.warning(self, "Export", "No folder selected")
            return

        folder_id = items[0].data(0, Qt.ItemDataRole.UserRole)
        folder_name = self.folders.get(folder_id, {}).get('display_name', 'Unknown')
        message_indices = self.messages_by_folder.get(folder_id, [])

        if not message_indices:
            QMessageBox.warning(self, "Export", "No messages in this folder")
            return

        output_dir = QFileDialog.getExistingDirectory(self, "Select Export Directory")
        if not output_dir:
            return

        msg_table_name = f"Message_{self.current_mailbox}"
        msg_table = self.tables.get(msg_table_name)

        if not msg_table:
            return

        col_map = get_column_map(msg_table)

        self.progress.setVisible(True)
        self.progress.setRange(0, len(message_indices))

        exported = 0
        for idx, rec_idx in enumerate(message_indices):
            self.progress.setValue(idx + 1)
            QApplication.processEvents()

            try:
                record = msg_table.get_record(rec_idx)
                if not record:
                    continue

                prop_blob = get_bytes_value(record, col_map.get('PropertyBlob', -1))

                subject = extract_subject_from_blob(prop_blob) if prop_blob else ""
                sender = extract_sender_from_blob(prop_blob) if prop_blob else ""
                msgid = extract_message_id_from_blob(prop_blob) if prop_blob else ""

                has_attach = get_bytes_value(record, col_map.get('HasAttachments', -1))
                has_attachments = bool(has_attach and has_attach != b'\x00')

                date_sent = get_filetime_value(record, col_map.get('DateSent', -1))

                email_data = {
                    'record_index': rec_idx,
                    'subject': subject,
                    'sender_name': sender,
                    'sender_email': f"{sender}@lab.sith.uz" if sender else "unknown@lab.sith.uz",
                    'recipient_name': sender,
                    'recipient_email': f"{sender}@lab.sith.uz" if sender else "unknown@lab.sith.uz",
                    'message_id': msgid,
                    'date_sent': date_sent,
                    'folder_name': folder_name,
                    'has_attachments': has_attachments,
                    'body_text': subject,
                    'attachments': []  # Attachments not loaded in bulk export for performance
                }

                # Generate filename
                subject_safe = re.sub(r'[<>:"/\\|?*]', '_', subject or 'no_subject')[:50]
                filename = f"record_{rec_idx}_{folder_name.replace(' ', '_')}.eml"

                eml_content = create_eml_content(email_data)

                out_path = Path(output_dir) / filename
                with open(out_path, 'wb') as f:
                    f.write(eml_content)
                exported += 1

            except Exception as e:
                self.status.showMessage(f"Error exporting record {rec_idx}: {e}")

        self.progress.setVisible(False)
        self.status.showMessage(f"Exported {exported} emails to {output_dir}")
        QMessageBox.information(self, "Export", f"Exported {exported} emails from {folder_name} to:\n{output_dir}")

    def _on_export_calendar(self):
        """Export calendar items from the current folder to .ics file."""
        if not HAS_CALENDAR_MODULE:
            QMessageBox.warning(self, "Export", "Calendar module not available")
            return

        items = self.folder_tree.selectedItems()
        if not items:
            QMessageBox.warning(self, "Export", "No folder selected")
            return

        folder_id = items[0].data(0, Qt.ItemDataRole.UserRole)
        folder_name = self.folders.get(folder_id, {}).get('display_name', 'Unknown')
        message_indices = self.messages_by_folder.get(folder_id, [])

        if not message_indices:
            QMessageBox.warning(self, "Export", "No messages in this folder")
            return

        msg_table_name = f"Message_{self.current_mailbox}"
        msg_table = self.tables.get(msg_table_name)

        if not msg_table:
            return

        col_map = get_column_map(msg_table)

        # Collect calendar events
        events = []
        self.progress.setVisible(True)
        self.progress.setRange(0, len(message_indices))

        for idx, rec_idx in enumerate(message_indices):
            self.progress.setValue(idx + 1)
            QApplication.processEvents()

            try:
                record = msg_table.get_record(rec_idx)
                if not record:
                    continue

                # Check if this is a calendar item
                msg_class = self.calendar_extractor.get_message_class(record, col_map)
                if not self.calendar_extractor.is_calendar_item(msg_class):
                    continue

                # Extract calendar event
                event = self.calendar_extractor.extract_event(record, col_map, rec_idx)
                if event:
                    events.append(event)

            except Exception as e:
                self.status.showMessage(f"Error processing record {rec_idx}: {e}")

        self.progress.setVisible(False)

        if not events:
            QMessageBox.information(self, "Export Calendar",
                f"No calendar items found in '{folder_name}'.\n\n"
                f"Calendar items have message class like:\n"
                f"- IPM.Appointment\n"
                f"- IPM.Schedule.Meeting.Request\n"
                f"- IPM.Task")
            return

        # Ask for output file
        output_path, _ = QFileDialog.getSaveFileName(
            self, "Save Calendar File",
            f"{folder_name.replace(' ', '_')}_calendar.ics",
            "iCalendar Files (*.ics);;All Files (*.*)"
        )

        if not output_path:
            return

        # Export to .ics
        if export_calendar_to_ics(events, output_path):
            self.status.showMessage(f"Exported {len(events)} calendar items to {output_path}")
            QMessageBox.information(self, "Export Calendar",
                f"Exported {len(events)} calendar items from '{folder_name}' to:\n{output_path}")
        else:
            QMessageBox.warning(self, "Export Error", "Failed to export calendar items")

    def _get_folder_path(self, folder_id: str) -> str:
        """Build full folder hierarchy path by traversing parent_id chain.

        Args:
            folder_id: The folder ID to get path for

        Returns:
            Full path like "Inbox/Subfolder1/Subfolder2"
        """
        path_parts = []
        visited = set()  # Prevent infinite loops

        current_id = folder_id
        while current_id and current_id not in visited:
            visited.add(current_id)

            folder = self.folders.get(current_id)
            if not folder:
                break

            folder_name = folder.get('display_name', 'Unknown')
            path_parts.insert(0, folder_name)

            # Move to parent
            current_id = folder.get('parent_id')

        if not path_parts:
            return "Unknown"

        return "/".join(path_parts)

    def _on_export_mailbox(self):
        """Export entire mailbox with filters to folder structure with EML files."""
        if not self.current_mailbox:
            QMessageBox.warning(self, "Export", "No mailbox selected")
            return

        # Create filter dialog
        dialog = QDialog(self)
        dialog.setWindowTitle("Export Mailbox")
        dialog.setMinimumWidth(400)
        layout = QVBoxLayout(dialog)

        # Filter section
        filter_group = QGroupBox("Filters (leave empty to export all)")
        filter_layout = QFormLayout(filter_group)

        # Date range
        date_from = QDateEdit()
        date_from.setCalendarPopup(True)
        date_from.setDate(QDate(2020, 1, 1))
        date_from.setDisplayFormat("yyyy-MM-dd")
        filter_layout.addRow("Date From:", date_from)

        date_to = QDateEdit()
        date_to.setCalendarPopup(True)
        date_to.setDate(QDate.currentDate())
        date_to.setDisplayFormat("yyyy-MM-dd")
        filter_layout.addRow("Date To:", date_to)

        # From filter
        from_filter = QLineEdit()
        from_filter.setPlaceholderText("Filter by sender (contains)")
        filter_layout.addRow("From:", from_filter)

        # To filter
        to_filter = QLineEdit()
        to_filter.setPlaceholderText("Filter by recipient (contains)")
        filter_layout.addRow("To:", to_filter)

        # Subject filter
        subject_filter = QLineEdit()
        subject_filter.setPlaceholderText("Filter by subject (contains)")
        filter_layout.addRow("Subject:", subject_filter)

        # Include hidden
        include_hidden = QCheckBox("Include hidden/system items")
        filter_layout.addRow("", include_hidden)

        layout.addWidget(filter_group)

        # Folder selection
        folder_group = QGroupBox("Folders to Export")
        folder_layout = QVBoxLayout(folder_group)

        export_all_folders = QCheckBox("Export all folders")
        export_all_folders.setChecked(True)
        folder_layout.addWidget(export_all_folders)

        layout.addWidget(folder_group)

        # Buttons
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)

        if dialog.exec() != QDialog.DialogCode.Accepted:
            return

        # Get filter values
        filter_date_from = date_from.date().toPyDate()
        filter_date_to = date_to.date().toPyDate()
        filter_from_text = from_filter.text().strip().lower()
        filter_to_text = to_filter.text().strip().lower()
        filter_subject_text = subject_filter.text().strip().lower()
        filter_include_hidden = include_hidden.isChecked()

        # Select output directory
        output_dir = QFileDialog.getExistingDirectory(self, "Select Export Directory")
        if not output_dir:
            return

        # Create mailbox folder
        mailbox_name = self.mailbox_owner or f"Mailbox_{self.current_mailbox}"
        mailbox_dir = Path(output_dir) / mailbox_name.replace(' ', '_')
        mailbox_dir.mkdir(exist_ok=True)

        msg_table_name = f"Message_{self.current_mailbox}"
        msg_table = self.tables.get(msg_table_name)
        if not msg_table:
            QMessageBox.warning(self, "Export", "Message table not found")
            return

        col_map = get_column_map(msg_table)

        # Get all message indices with folder hierarchy paths
        all_indices = []
        for folder_id, indices in self.messages_by_folder.items():
            folder_path_str = self._get_folder_path(folder_id)  # Full hierarchy path
            for idx in indices:
                all_indices.append((idx, folder_id, folder_path_str))

        self.progress.setVisible(True)
        self.progress.setRange(0, len(all_indices))

        exported = 0
        skipped = 0

        for i, (rec_idx, folder_id, folder_path_str) in enumerate(all_indices):
            self.progress.setValue(i + 1)
            if i % 50 == 0:
                QApplication.processEvents()

            try:
                record = msg_table.get_record(rec_idx)
                if not record:
                    continue

                # Check hidden filter
                is_hidden = get_bytes_value(record, col_map.get('IsHidden', -1))
                is_hidden_val = bool(is_hidden and is_hidden != b'\x00')
                if is_hidden_val and not filter_include_hidden:
                    skipped += 1
                    continue

                # Get date
                date_received = get_filetime_value(record, col_map.get('DateReceived', -1))
                if date_received:
                    msg_date = date_received.date()
                    if msg_date < filter_date_from or msg_date > filter_date_to:
                        skipped += 1
                        continue

                # Extract message data (with attachments)
                email_msg = None
                if HAS_EMAIL_MODULE and self.email_extractor:
                    email_msg = self.email_extractor.extract_message(
                        record, col_map, rec_idx,
                        folder_name=folder_path_str,  # Full hierarchy path
                        tables=self.tables,
                        mailbox_num=self.current_mailbox
                    )

                if not email_msg:
                    skipped += 1
                    continue

                # Apply text filters
                from_header = email_msg.get_from_header().lower()
                to_header = email_msg.get_to_header().lower()
                subject = (email_msg.subject or "").lower()

                if filter_from_text and filter_from_text not in from_header:
                    skipped += 1
                    continue

                if filter_to_text and filter_to_text not in to_header:
                    skipped += 1
                    continue

                if filter_subject_text and filter_subject_text not in subject:
                    skipped += 1
                    continue

                # Create folder hierarchy directories (folder/subfolder/subfolder)
                # Sanitize each path component
                path_parts = folder_path_str.split('/')
                safe_parts = [re.sub(r'[<>:"/\\|?*]', '_', part or 'Unknown') for part in path_parts]
                folder_path = mailbox_dir
                for part in safe_parts:
                    folder_path = folder_path / part
                folder_path.mkdir(parents=True, exist_ok=True)

                # Generate filename
                date_str = date_received.strftime("%Y%m%d_%H%M%S") if date_received else "nodate"
                subject_safe = re.sub(r'[<>:"/\\|?*]', '_', email_msg.subject or 'no_subject')[:40]
                filename = f"{date_str}_{rec_idx}_{subject_safe}.eml"

                # Export to EML
                eml_content = email_msg.to_eml()
                out_path = folder_path / filename
                with open(out_path, 'wb') as f:
                    f.write(eml_content)

                exported += 1

            except Exception as e:
                self.status.showMessage(f"Error exporting record {rec_idx}: {e}")

        self.progress.setVisible(False)

        # Summary
        summary = f"Export complete!\n\n"
        summary += f"Exported: {exported} emails\n"
        summary += f"Skipped: {skipped} emails (filtered out)\n"
        summary += f"Location: {mailbox_dir}"

        self.status.showMessage(f"Exported {exported} emails to {mailbox_dir}")
        QMessageBox.information(self, "Export Mailbox", summary)

    def _on_save_attachment(self):
        """Save selected attachment."""
        items = self.attach_list.selectedItems()
        if not items:
            QMessageBox.warning(self, "Save", "No attachment selected")
            return

        idx = items[0].data(Qt.ItemDataRole.UserRole)
        if idx < 0 or idx >= len(self.current_attachments):
            return

        att = self.current_attachments[idx]
        filename = att[0]
        data = att[2]
        is_external = att[3] if len(att) > 3 else False

        if is_external:
            QMessageBox.warning(self, "Save", f"'{filename}' is stored externally and cannot be saved.\nThe database only contains a 4-byte reference.")
            return

        path, _ = QFileDialog.getSaveFileName(
            self, "Save Attachment", filename,
            "All Files (*.*)"
        )

        if not path:
            return

        try:
            with open(path, 'wb') as f:
                f.write(data)
            self.status.showMessage(f"Saved attachment to {path}")
        except Exception as e:
            QMessageBox.critical(self, "Save Error", f"Failed to save attachment:\n{e}")

    def _on_save_all_attachments(self):
        """Save all attachments."""
        self._on_export_attachments()

    def _on_attachment_double_clicked(self, item):
        """Handle double-click on attachment."""
        idx = item.data(Qt.ItemDataRole.UserRole)
        if idx < 0 or idx >= len(self.current_attachments):
            return

        att = self.current_attachments[idx]
        filename = att[0]
        data = att[2]
        is_external = att[3] if len(att) > 3 else False

        if is_external:
            QMessageBox.warning(self, "Save", f"'{filename}' is stored externally and cannot be saved.\nThe database only contains a 4-byte reference.")
            return

        path, _ = QFileDialog.getSaveFileName(
            self, "Save Attachment", filename,
            "All Files (*.*)"
        )

        if path:
            try:
                with open(path, 'wb') as f:
                    f.write(data)
                self.status.showMessage(f"Saved {filename} to {path}")
            except Exception as e:
                QMessageBox.critical(self, "Save Error", f"Failed to save attachment:\n{e}")

    def _on_about(self):
        """Show About dialog with developer information."""
        about_text = f"""
<h2>Exchange EDB Exporter</h2>
<p><b>Version:</b> {VERSION}</p>
<p>A tool for viewing and exporting email data from Microsoft Exchange EDB database files.</p>

<h3>Features:</h3>
<ul>
<li>Browse mailboxes, folders, and messages</li>
<li>View email content (text, HTML, headers)</li>
<li>Export to EML format with attachments</li>
<li>Export calendar items to ICS format</li>
<li>Batch export with filters</li>
</ul>

<h3>Developer:</h3>
<p><b>Igor Batin</b></p>
<p>
<a href="https://github.com/igrbtn">GitHub: github.com/igrbtn</a><br>
<a href="mailto:igr.btn@gmail.com">Email: igr.btn@gmail.com</a><br>
<a href="https://mxlab.uz">Website: mxlab.uz</a><br>
<a href="https://t.me/igrbtn">Telegram: @igrbtn</a>
</p>

<h3>Support the project:</h3>
<p>
<a href="https://www.buymeacoffee.com/igrbtnv" style="background-color:#FFDD00;color:#000;padding:8px 16px;text-decoration:none;border-radius:5px;font-weight:bold;">Buy me a coffee</a>
</p>

<hr>
<p><small>Built with Python, PyQt6, pyesedb, and dissect.esedb</small></p>
"""
        msg = QMessageBox(self)
        msg.setWindowTitle("About Exchange EDB Exporter")
        msg.setTextFormat(Qt.TextFormat.RichText)
        msg.setText(about_text)
        msg.setTextInteractionFlags(Qt.TextInteractionFlag.TextBrowserInteraction)
        msg.exec()

    def closeEvent(self, event):
        if self.db:
            self.db.close()
        event.accept()


def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
