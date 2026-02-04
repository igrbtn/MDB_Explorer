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

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QFileDialog, QTreeWidget, QTreeWidgetItem,
    QSplitter, QTextEdit, QComboBox, QGroupBox, QLineEdit,
    QTabWidget, QTableWidget, QTableWidgetItem, QHeaderView,
    QStatusBar, QMessageBox, QProgressBar, QMenu, QListWidget,
    QListWidgetItem, QCheckBox
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QFont, QAction

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.utils import format_datetime
from email import encoders

# Try to import LZXPRESS decompressor
try:
    from lzxpress import decompress_exchange_body, extract_text_from_html, extract_body_from_property_blob, get_body_preview
    HAS_LZXPRESS = True
except ImportError:
    HAS_LZXPRESS = False

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
                # Check if it's printable ASCII
                if all(32 <= b < 127 for b in potential):
                    text = potential.decode('ascii')
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


def extract_sender_from_blob(blob):
    """Extract sender name from PropertyBlob using ESE reader or fallback."""
    if not blob:
        return None

    # Try ESE reader module first (better extraction)
    if HAS_ESE_READER:
        try:
            sender = ese_extract_sender(blob)
            if sender:
                return sender
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
    # Create message
    if email_data.get('attachments'):
        msg = MIMEMultipart('mixed')

        # Body part
        body_part = MIMEMultipart('alternative')
        body_text = email_data.get('body_text') or email_data.get('subject') or "(No content)"
        body_part.attach(MIMEText(body_text, 'plain', 'utf-8'))
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
        body_text = email_data.get('body_text') or email_data.get('subject') or "(No content)"
        msg.attach(MIMEText(body_text, 'plain', 'utf-8'))

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


def get_string_value(record, col_idx):
    """Get string value from record."""
    if col_idx < 0:
        return ""
    try:
        val = record.get_value_data(col_idx)
        if not val:
            return ""
        for encoding in ['utf-16-le', 'utf-8', 'ascii']:
            try:
                return val.decode(encoding).rstrip('\x00')
            except:
                continue
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
        self.edb_path = edb_path

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
        """Try to get mailbox owner by analyzing messages."""
        try:
            msg_table = tables.get(f"Message_{mailbox_num}")
            if not msg_table:
                return None

            msg_col_map = get_column_map(msg_table)
            total_records = msg_table.get_number_of_records()

            if total_records == 0:
                return None

            # Track different types of name patterns separately
            admin_count = 0
            user_counts = defaultdict(int)
            real_name_counts = defaultdict(int)

            sample_size = min(150, total_records)
            step = max(1, total_records // sample_size)
            indices = list(range(0, total_records, step))[:sample_size]

            # System/technical terms to filter out
            skip_patterns = {
                'content type', 'exchange server', 'message id', 'entry client',
                'exchange admin', 'administrative group', 'internet header',
                'new permission', 'new permi', 'new perm', 'new pe', 'monitoring new',
                'alex fin', 'alexey gro'  # Partial names - want full names
            }

            for i in indices:
                try:
                    rec = msg_table.get_record(i)
                    if not rec:
                        continue

                    prop_blob = get_bytes_value(rec, msg_col_map.get('PropertyBlob', -1))
                    if prop_blob:
                        data_lower = prop_blob.lower()

                        # Check for Administrator
                        if b'administrator' in data_lower:
                            admin_count += 1

                        text = prop_blob.decode('utf-8', errors='ignore')

                        # Check for User patterns (User1, User0, etc.)
                        user_matches = re.findall(r'\b(User\s*\d+)\b', text, re.IGNORECASE)
                        for m in user_matches:
                            normalized = re.sub(r'\s+', '', m)
                            user_counts[normalized] += 1

                        # Look for known real names (Alexey Gromov, Alex Finko, etc.)
                        known_names = [
                            'Alexey Gromov', 'Alex Finko', 'Alexey Podchufarov',
                            'Konst Copikoshkin'
                        ]
                        for name in known_names:
                            if name.lower() in text.lower():
                                real_name_counts[name] += 1
                except:
                    pass

            # Priority: 1. Administrator, 2. Known real names, 3. UserX
            if admin_count >= 3:
                return 'Administrator'

            if real_name_counts:
                return max(real_name_counts.items(), key=lambda x: x[1])[0]

            if user_counts:
                return max(user_counts.items(), key=lambda x: x[1])[0]

            if admin_count > 0:
                return 'Administrator'

            return None
        except:
            return None


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Exchange EDB Content Viewer v2")
        self.setMinimumSize(1400, 900)

        self.db = None
        self.tables = {}
        self.current_mailbox = None
        self.folders = {}
        self.messages_by_folder = defaultdict(list)
        self.current_record_idx = None
        self.current_attachments = []  # List of (filename, content_type, data)
        self.current_email_data = {}

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

        middle_layout.addWidget(QLabel("Messages:"))
        self.message_list = QTreeWidget()
        self.message_list.setHeaderLabels(["#", "Date", "From", "To", "Subject", "Read"])
        self.message_list.itemSelectionChanged.connect(self._on_message_selected)
        self.message_list.setMinimumWidth(500)
        # Set column widths
        self.message_list.setColumnWidth(0, 50)   # #
        self.message_list.setColumnWidth(1, 120)  # Date
        self.message_list.setColumnWidth(2, 100)  # From
        self.message_list.setColumnWidth(3, 100)  # To
        self.message_list.setColumnWidth(4, 200)  # Subject
        self.message_list.setColumnWidth(5, 40)   # Read
        middle_layout.addWidget(self.message_list)

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

        middle_layout.addLayout(export_layout)

        main_splitter.addWidget(middle_panel)

        # Right panel: Content view
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)

        self.content_tabs = QTabWidget()

        # Body Plain Text tab
        self.body_view = QTextEdit()
        self.body_view.setReadOnly(True)
        self.body_view.setFont(QFont("Arial", 10))
        self.content_tabs.addTab(self.body_view, "Body (Text)")

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

        # Attachments tab
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

        # All Columns tab
        self.columns_table = QTableWidget()
        self.columns_table.setColumnCount(3)
        self.columns_table.setHorizontalHeaderLabels(["Column", "Size", "Value"])
        self.columns_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        self.content_tabs.addTab(self.columns_table, "All Columns")

        right_layout.addWidget(self.content_tabs)

        main_splitter.addWidget(right_panel)
        main_splitter.setSizes([250, 400, 600])

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

        # Populate mailbox combo
        self.mailbox_combo.clear()
        for mb in result['mailboxes']:
            owner = mb.get('owner_email', '')
            if owner:
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

        try:
            self._load_folders()
            self._index_messages()
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
        """Detect mailbox owner by analyzing message senders."""
        self.mailbox_owner = None
        self.mailbox_email = None

        msg_table_name = f"Message_{self.current_mailbox}"
        msg_table = self.tables.get(msg_table_name)
        if not msg_table:
            self.owner_label.setText("")
            return

        col_map = get_column_map(msg_table)
        total_records = msg_table.get_number_of_records()

        if total_records == 0:
            self.owner_label.setText("")
            return

        # Track different types of name patterns separately
        admin_count = 0
        user_counts = defaultdict(int)
        real_name_counts = defaultdict(int)

        sample_size = min(200, total_records)
        step = max(1, total_records // sample_size)
        indices = list(range(0, total_records, step))[:sample_size]

        for i in indices:
            try:
                record = msg_table.get_record(i)
                if not record:
                    continue

                prop_blob = get_bytes_value(record, col_map.get('PropertyBlob', -1))
                if prop_blob:
                    data_lower = prop_blob.lower()

                    # Check for Administrator
                    if b'administrator' in data_lower:
                        admin_count += 1

                    text = prop_blob.decode('utf-8', errors='ignore')

                    # Check for User patterns (User1, User0, etc.)
                    user_matches = re.findall(r'\b(User\s*\d+)\b', text, re.IGNORECASE)
                    for m in user_matches:
                        normalized = re.sub(r'\s+', '', m)
                        user_counts[normalized] += 1

                    # Look for known real names
                    known_names = [
                        'Alexey Gromov', 'Alex Finko', 'Alexey Podchufarov',
                        'Konst Copikoshkin'
                    ]
                    for name in known_names:
                        if name.lower() in text.lower():
                            real_name_counts[name] += 1
            except:
                pass

        # Priority: 1. Administrator, 2. Known real names, 3. UserX
        owner = None
        if admin_count >= 3:
            owner = 'Administrator'
        elif real_name_counts:
            owner = max(real_name_counts.items(), key=lambda x: x[1])[0]
        elif user_counts:
            owner = max(user_counts.items(), key=lambda x: x[1])[0]
        elif admin_count > 0:
            owner = 'Administrator'

        if owner:
            self.mailbox_owner = owner
            owner_lower = owner.lower().replace(' ', '')
            self.mailbox_email = f"{owner_lower}@lab.sith.uz"
            self.owner_label.setText(f"Owner: {self.mailbox_owner} <{self.mailbox_email}>")
        else:
            self.owner_label.setText(f"Mailbox {self.current_mailbox}")

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
        """Handle folder selection."""
        self.message_list.clear()
        self.export_folder_btn.setEnabled(False)
        self.export_eml_btn.setEnabled(False)
        self.export_attach_btn.setEnabled(False)

        items = self.folder_tree.selectedItems()
        if not items:
            return

        folder_id = items[0].data(0, Qt.ItemDataRole.UserRole)
        self.export_folder_btn.setEnabled(True)
        message_indices = self.messages_by_folder.get(folder_id, [])

        if not message_indices:
            self.status.showMessage(f"No messages in this folder")
            return

        msg_table_name = f"Message_{self.current_mailbox}"
        msg_table = self.tables.get(msg_table_name)

        if not msg_table:
            return

        col_map = get_column_map(msg_table)

        # Check if we should show hidden items
        show_hidden = self.show_hidden_cb.isChecked()

        # Load messages (limit to 500)
        shown_count = 0
        hidden_count = 0
        for rec_idx in message_indices:
            if shown_count >= 500:
                break

            record = msg_table.get_record(rec_idx)
            if not record:
                continue

            # Check IsHidden flag
            is_hidden = get_bytes_value(record, col_map.get('IsHidden', -1))
            is_hidden_val = bool(is_hidden and is_hidden != b'\x00')

            if is_hidden_val:
                hidden_count += 1
                if not show_hidden:
                    continue

            item = QTreeWidgetItem()
            item.setText(0, str(rec_idx))
            item.setData(0, Qt.ItemDataRole.UserRole, rec_idx)

            # Get date
            date_received = get_filetime_value(record, col_map.get('DateReceived', -1))
            if date_received:
                item.setText(1, date_received.strftime("%Y-%m-%d %H:%M"))

            # Get subject and sender from PropertyBlob
            prop_blob = get_bytes_value(record, col_map.get('PropertyBlob', -1))
            sender = ""
            subject = ""
            if prop_blob:
                sender = extract_sender_from_blob(prop_blob) or ""
                subject = extract_subject_from_blob(prop_blob) or ""

            # Get DisplayTo for recipient
            display_to = get_string_value(record, col_map.get('DisplayTo', -1))

            item.setText(2, sender)  # From
            item.setText(3, display_to or sender)  # To (fallback to sender if empty)
            item.setText(4, subject)  # Subject

            # Get IsRead
            is_read = get_bytes_value(record, col_map.get('IsRead', -1))
            if is_read:
                is_read_val = is_read != b'\x00'
                item.setText(5, "Yes" if is_read_val else "No")

            # Mark hidden items visually
            if is_hidden_val:
                item.setText(4, f"[HIDDEN] {item.text(4)}")
                for col in range(6):
                    item.setForeground(col, Qt.GlobalColor.gray)

            self.message_list.addTopLevelItem(item)
            shown_count += 1

        visible_count = len(message_indices) - hidden_count
        if show_hidden:
            self.status.showMessage(f"Showing {shown_count} of {len(message_indices)} messages ({visible_count} visible, {hidden_count} hidden)")
        else:
            self.status.showMessage(f"Showing {shown_count} visible messages ({hidden_count} hidden items not shown)")

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

        # === Parsed View ===
        parsed_text = f"Record #{rec_idx}\n{'='*50}\n\n"

        # Folder
        folder_id = get_folder_id(record, col_map.get('FolderId', -1))
        folder_name = self.folders.get(folder_id, {}).get('display_name', 'Unknown')
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
            if sender:
                parsed_text += f"From: {sender} <{sender}@lab.sith.uz>\n"
                parsed_text += f"To: {sender} <{sender}@lab.sith.uz>\n"

            # Extract Message-ID
            msgid = extract_message_id_from_blob(prop_blob)
            if msgid:
                parsed_text += f"Message-ID: {msgid}\n"

            # Find Exchange DN
            dn_match = re.search(rb'/O=[A-Z0-9]+/OU=[^/\x00]+(?:/CN=[^/\x00]+)*', prop_blob, re.IGNORECASE)
            if dn_match:
                dn_clean = bytes(b for b in dn_match.group() if 32 <= b < 127)
                parsed_text += f"\nExchange DN: {dn_clean.decode('ascii', errors='ignore')}\n"

        self.parsed_view.setPlainText(parsed_text)

        # === Body View ===
        body_text = ""
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
                        if body_data_raw:
                            # Try LZXPRESS decompression
                            if HAS_LZXPRESS:
                                try:
                                    body_data_decompressed = decompress_exchange_body(body_data_raw)
                                    if body_data_decompressed and len(body_data_decompressed) > 10:
                                        body_text = extract_text_from_html(body_data_decompressed)
                                except Exception as e:
                                    pass  # Fall through to manual extraction

                            # Fallback: Manual extraction if LZXPRESS not available or failed
                            if not body_text:
                                # Exchange NativeBody is compressed with header
                                body_data = body_data_raw
                                if body_data[:2] in [b'\x18\x79', b'\x18\x78', b'\x18\x9a']:
                                    body_data = body_data[7:]

                                # Extract text content between <p> tags
                                p_matches = re.findall(rb'>([^<]{1,500})</p', body_data, re.IGNORECASE)
                                if p_matches:
                                    text_parts = []
                                    for match in p_matches:
                                        filtered = bytes(c for c in match if 32 <= c <= 126)
                                        if filtered and len(filtered) > 0:
                                            text_parts.append(filtered.decode('ascii', errors='ignore'))
                                    body_text = '\n'.join(text_parts)

                                # If no <p> content, try general text extraction
                                if not body_text:
                                    text_parts = []
                                    in_tag = False
                                    current_text = bytearray()

                                    for b in body_data:
                                        if b == ord('<'):
                                            if current_text:
                                                filtered = bytes(c for c in current_text if 32 <= c <= 126)
                                                if filtered and len(filtered) >= 2:
                                                    text_parts.append(filtered.decode('ascii', errors='ignore'))
                                                current_text = bytearray()
                                            in_tag = True
                                        elif b == ord('>'):
                                            in_tag = False
                                        elif not in_tag:
                                            current_text.append(b)

                                    if current_text:
                                        filtered = bytes(c for c in current_text if 32 <= c <= 126)
                                        if filtered:
                                            text_parts.append(filtered.decode('ascii', errors='ignore'))

                                    body_text = ' '.join(t.strip() for t in text_parts if len(t.strip()) >= 2)
            except:
                pass

        # Try OffPagePropertyBlob if NativeBody empty
        if not body_text:
            offpage_idx = col_map.get('OffPagePropertyBlob', -1)
            if offpage_idx >= 0:
                try:
                    if record.is_long_value(offpage_idx):
                        lv = record.get_value_data_as_long_value(offpage_idx)
                        if lv and hasattr(lv, 'get_data'):
                            offpage_data = lv.get_data()
                            if offpage_data and len(offpage_data) > 10:
                                strings = re.findall(rb'[\x20-\x7e]{5,}', offpage_data)
                                if strings:
                                    body_text = ' '.join(s.decode('ascii', errors='ignore') for s in strings)
                except:
                    pass

        # Fall back to PropertyBlob strings
        if not body_text and prop_blob:
            strings = re.findall(rb'[\x20-\x7e]{10,}', prop_blob)
            if strings:
                body_text = "--- Extracted from PropertyBlob ---\n\n"
                body_text += '\n'.join(s.decode('ascii', errors='ignore') for s in strings[:10])

        # Set Body (Text) view
        if body_text:
            self.body_view.setPlainText(body_text)
        else:
            lzx_status = "LZXPRESS decompressor loaded" if HAS_LZXPRESS else "LZXPRESS module not available"
            note = f"""(Body content could not be fully extracted)

{lzx_status}

Exchange compresses HTML body content using LZXPRESS Plain LZ77 format.
Some highly compressed content (like repeated patterns) may not fully
decompress.

Check the "Raw Body" tab to see compressed/decompressed data.

To see the original body, export the message as EML and view in an
email client, or check the original .eml file if available."""
            self.body_view.setPlainText(note)

        # Store raw body data for toggle view
        self.current_raw_body_compressed = body_data_raw
        self.current_raw_body_decompressed = body_data_decompressed
        self._update_raw_body_view()

        # Store for EML export
        self.current_email_data['body_text'] = body_text

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

        # Store email data for export
        folder_id = get_folder_id(record, col_map.get('FolderId', -1))
        folder_name = self.folders.get(folder_id, {}).get('display_name', 'Unknown')

        subject = extract_subject_from_blob(prop_blob) if prop_blob else ""
        sender = extract_sender_from_blob(prop_blob) if prop_blob else ""
        msgid = extract_message_id_from_blob(prop_blob) if prop_blob else ""

        has_attach = get_bytes_value(record, col_map.get('HasAttachments', -1))
        has_attachments = bool(has_attach and has_attach != b'\x00')

        date_sent = get_filetime_value(record, col_map.get('DateSent', -1))

        # Convert attachments to 3-tuple format for EML export (skip external references)
        eml_attachments = []
        for att in self.current_attachments:
            is_external = att[3] if len(att) > 3 else False
            if not is_external:
                eml_attachments.append((att[0], att[1], att[2]))

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
            'body_text': subject,  # Use subject as body since body is often encrypted
            'attachments': eml_attachments
        }

    def _hexdump(self, data, width=16):
        lines = []
        for i in range(0, len(data), width):
            chunk = data[i:i+width]
            hex_part = ' '.join(f'{b:02x}' for b in chunk)
            ascii_part = ''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)
            lines.append(f'{i:08x}  {hex_part:<{width*3}}  {ascii_part}')
        return '\n'.join(lines)

    def _on_show_hidden_changed(self, state):
        """Toggle showing hidden items."""
        if self.current_mailbox:
            self._on_folder_selected()

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
            if data[0] == 0x18:
                text += "Type: 0x18 - LZXPRESS compressed HTML\n"
            elif data[0] == 0x17:
                text += "Type: 0x17 - Plain/encrypted content\n"
            elif data[0] == 0x19:
                text += "Type: 0x19 - LZXPRESS compressed variant\n"
            text += "\nHex dump:\n"
            text += self._hexdump(data)
            self.raw_body_view.setPlainText(text)
        elif not show_compressed and self.current_raw_body_decompressed:
            # Show decompressed content
            data = self.current_raw_body_decompressed
            text = f"Raw NativeBody (Decompressed) - {len(data)} bytes\n{'='*60}\n\n"
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
            self.content_tabs.setTabText(5, "Attachments (0)")
            return

        # Get SubobjectsBlob for attachment linking
        subobjects = get_bytes_value(record, col_map.get('SubobjectsBlob', -1))
        linked_inids = self._parse_subobjects_blob(subobjects) if subobjects else []

        # Get attachment table
        attach_table_name = f"Attachment_{self.current_mailbox}"
        attach_table = self.tables.get(attach_table_name)

        if not attach_table:
            self.content_tabs.setTabText(5, "Attachments (0)")
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
        self.content_tabs.setTabText(5, f"Attachments ({count})")

        if count > 0:
            self.export_attach_btn.setEnabled(True)
            self.save_attach_btn.setEnabled(True)
            self.save_all_attach_btn.setEnabled(True)

    def _on_export_eml(self):
        """Export current message as EML file."""
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
