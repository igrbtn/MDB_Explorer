#!/usr/bin/env python3
"""
Exchange EDB Content Viewer v2
GUI application with folder tree navigation
"""

import sys
import os
import struct
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict


class Profiler:
    """Simple profiler that tracks timing of named operations."""
    def __init__(self):
        self._starts = {}
        self._stats = {}  # name -> {'count': int, 'total': float, 'last': float}
        self._log = []  # List of (timestamp, name, elapsed_ms)

    def start(self, name):
        self._starts[name] = time.perf_counter()

    def stop(self, name):
        if name not in self._starts:
            return
        elapsed = time.perf_counter() - self._starts.pop(name)
        if name not in self._stats:
            self._stats[name] = {'count': 0, 'total': 0.0, 'last': 0.0}
        self._stats[name]['count'] += 1
        self._stats[name]['total'] += elapsed
        self._stats[name]['last'] = elapsed
        self._log.append((time.time(), name, elapsed * 1000))

    def get_stats(self):
        """Return list of (name, count, total_s, avg_ms, last_ms) sorted by total desc."""
        result = []
        for name, s in self._stats.items():
            avg_ms = (s['total'] / s['count'] * 1000) if s['count'] > 0 else 0
            result.append((name, s['count'], s['total'], avg_ms, s['last'] * 1000))
        result.sort(key=lambda x: x[2], reverse=True)
        return result

    def get_log(self):
        """Return chronological log of all operations."""
        return list(self._log)

    def export_csv(self, filepath):
        """Export stats and log to CSV file."""
        import csv
        with open(filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["=== PROFILER STATS ==="])
            writer.writerow(["Operation", "Calls", "Total (s)", "Avg (ms)", "Last (ms)"])
            for name, count, total, avg_ms, last_ms in self.get_stats():
                writer.writerow([name, count, round(total, 3), round(avg_ms, 1), round(last_ms, 1)])
            writer.writerow([])
            writer.writerow(["=== OPERATION LOG ==="])
            writer.writerow(["Timestamp", "Operation", "Duration (ms)"])
            for ts, name, elapsed_ms in self._log:
                dt = datetime.fromtimestamp(ts).strftime("%H:%M:%S.%f")[:-3]
                writer.writerow([dt, name, round(elapsed_ms, 2)])

    def clear(self):
        self._starts.clear()
        self._stats.clear()
        self._log.clear()


profiler = Profiler()

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
    QDateEdit, QDialogButtonBox, QGridLayout
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QUrl, QDate
from PyQt6.QtGui import QFont, QAction, QTextOption, QColor, QPalette, QIcon

# Using QTextBrowser for lightweight HTML rendering (no WebEngine dependency)


# Try to import LZXPRESS decompressor
try:
    from core.lzxpress import decompress_exchange_body, extract_text_from_html, extract_body_from_property_blob, get_body_preview, get_html_content
    HAS_LZXPRESS = True
except ImportError:
    HAS_LZXPRESS = False

# Check if dissect.esedb is available for proper decompression
try:
    from dissect.esedb.compression import decompress as dissect_decompress
    HAS_DISSECT = True
except ImportError:
    HAS_DISSECT = False


# Try to import folder mapping
try:
    from core.folder_mapping import get_folder_name as get_mapped_folder_name, SPECIAL_FOLDER_MAP
    HAS_FOLDER_MAPPING = True
except ImportError:
    HAS_FOLDER_MAPPING = False
    SPECIAL_FOLDER_MAP = {}

# Import stable email extraction module
try:
    from exporters.email_message import EmailMessage, EmailExtractor, EmailAttachment
    HAS_EMAIL_MODULE = True
except ImportError:
    HAS_EMAIL_MODULE = False

# Import calendar extraction module
try:
    from exporters.calendar_message import CalendarEvent, CalendarExtractor, export_calendar_to_ics, CALENDAR_MESSAGE_CLASSES
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
            profiler.start("DB Open")

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
            result['file_size'] = os.path.getsize(self.edb_path)
            result['total_messages'] = sum(mb['message_count'] for mb in result['mailboxes'])

            # Try to extract mailbox owner email from Sent Items
            for mb in result['mailboxes']:
                mb['owner_email'] = self._get_mailbox_owner(result['tables'], mb['number'])

            profiler.stop("DB Open")
            self.finished.emit(result)

        except Exception as e:
            profiler.stop("DB Open")
            self.error.emit(str(e))

    def _get_mailbox_owner(self, tables, mailbox_num):
        """Get mailbox owner from Mailbox table."""
        profiler.start("Get Mailbox Owner")
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
                                            profiler.stop("Get Mailbox Owner")
                                            return text
                                    except:
                                        pass
                            except:
                                pass
                except:
                    pass

            profiler.stop("Get Mailbox Owner")
            return None
        except:
            profiler.stop("Get Mailbox Owner")
            return None


class ProfilerDialog(QDialog):
    """Floating profiler window showing operation timing stats."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Profiler")
        self.setMinimumSize(500, 300)
        self.resize(600, 400)
        self.setWindowFlags(self.windowFlags() | Qt.WindowType.Tool)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(4, 4, 4, 4)

        self.table = QTableWidget()
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels(["Operation", "Calls", "Total (s)", "Avg (ms)", "Last (ms)"])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setSortingEnabled(True)
        layout.addWidget(self.table)

        btn_layout = QHBoxLayout()
        clear_btn = QPushButton("Clear")
        clear_btn.clicked.connect(self._on_clear)
        btn_layout.addWidget(clear_btn)
        export_btn = QPushButton("Export CSV")
        export_btn.clicked.connect(self._on_export_csv)
        btn_layout.addWidget(export_btn)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)

        from PyQt6.QtCore import QTimer
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.refresh)
        self.timer.start(2000)

    def refresh(self):
        stats = profiler.get_stats()
        self.table.setSortingEnabled(False)
        self.table.setRowCount(len(stats))
        for row, (name, count, total, avg_ms, last_ms) in enumerate(stats):
            self.table.setItem(row, 0, QTableWidgetItem(name))
            item_count = QTableWidgetItem()
            item_count.setData(Qt.ItemDataRole.DisplayRole, count)
            self.table.setItem(row, 1, item_count)
            item_total = QTableWidgetItem()
            item_total.setData(Qt.ItemDataRole.DisplayRole, round(total, 3))
            self.table.setItem(row, 2, item_total)
            item_avg = QTableWidgetItem()
            item_avg.setData(Qt.ItemDataRole.DisplayRole, round(avg_ms, 1))
            self.table.setItem(row, 3, item_avg)
            item_last = QTableWidgetItem()
            item_last.setData(Qt.ItemDataRole.DisplayRole, round(last_ms, 1))
            self.table.setItem(row, 4, item_last)
        self.table.setSortingEnabled(True)

    def _on_clear(self):
        profiler.clear()
        self.refresh()

    def _on_export_csv(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Export Profiler Data", "profiler_stats.csv",
            "CSV Files (*.csv);;All Files (*.*)"
        )
        if path:
            profiler.export_csv(path)
            QMessageBox.information(self, "Export", f"Profiler data exported to:\n{path}")

    def showEvent(self, event):
        super().showEvent(event)
        self.refresh()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"Exchange EDB Exporter v{VERSION}")
        icon_path = Path(__file__).parent / "assets" / "icon.png"
        if icon_path.exists():
            self.setWindowIcon(QIcon(str(icon_path)))
        self.setMinimumSize(1000, 500)
        self.resize(1400, 700)  # Default size, can be resized smaller

        self.db = None
        self.tables = {}
        self.current_mailbox = None
        self.folders = {}
        self.messages_by_folder = defaultdict(list)
        self.current_record_idx = None
        self.current_attachments = []  # List of (filename, content_type, data)
        self.current_email_message = None  # EmailMessage object for export
        self.current_msg_type = 'email'  # 'email', 'calendar', or 'contact'
        self.current_cal_event = None
        self.current_contact = None
        self.email_extractor = None  # EmailExtractor instance
        self.calendar_extractor = None  # CalendarExtractor instance
        self.folder_messages_cache = {}  # Cache: folder_id -> list of message data
        self._cached_msg_col_map = None
        self._cached_msg_columns = None
        self._cached_attach_col_map = None
        self._cached_inid_to_record = None
        self._cached_msgdocid_to_attach = None
        self.debug_mode = False
        self.profiler_dialog = None

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

        view_menu.addSeparator()

        # Column visibility toggles
        self.show_from_email_action = QAction("Show From Email Column", self)
        self.show_from_email_action.setCheckable(True)
        self.show_from_email_action.setChecked(False)
        self.show_from_email_action.triggered.connect(self._toggle_from_email_column)
        view_menu.addAction(self.show_from_email_action)

        self.show_to_email_action = QAction("Show To Email Column", self)
        self.show_to_email_action.setCheckable(True)
        self.show_to_email_action.setChecked(False)
        self.show_to_email_action.triggered.connect(self._toggle_to_email_column)
        view_menu.addAction(self.show_to_email_action)

    def _setup_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        # File and mailbox selection (two rows)
        top_widget = QWidget()
        top_widget.setFixedHeight(54)
        top_vlayout = QVBoxLayout(top_widget)
        top_vlayout.setContentsMargins(2, 0, 2, 0)
        top_vlayout.setSpacing(2)

        # Row 1: DB path, Load, Hidden, About
        row1_layout = QHBoxLayout()
        row1_layout.setSpacing(3)

        lbl_db = QLabel("DB:")
        lbl_db.setFixedWidth(20)
        row1_layout.addWidget(lbl_db)

        self.file_path = QLineEdit()
        self.file_path.setReadOnly(True)
        self.file_path.setPlaceholderText("Select EDB...")
        self.file_path.setMaximumWidth(200)
        self.file_path.setFixedHeight(22)
        self.file_path.mousePressEvent = lambda e: self._on_browse()
        row1_layout.addWidget(self.file_path)

        browse_btn = QPushButton("...")
        browse_btn.setFixedSize(30, 22)
        browse_btn.clicked.connect(self._on_browse)
        row1_layout.addWidget(browse_btn)

        self.load_btn = QPushButton("Load")
        self.load_btn.setFixedSize(50, 22)
        self.load_btn.clicked.connect(self._on_load)
        self.load_btn.setEnabled(False)
        row1_layout.addWidget(self.load_btn)

        self.db_info_label = QLabel("")
        self.db_info_label.setStyleSheet("color: #969696; font-size: 11px;")
        row1_layout.addWidget(self.db_info_label)

        row1_layout.addStretch()

        # About button in top right corner
        self.about_btn = QPushButton("About")
        self.about_btn.setFixedSize(70, 22)
        self.about_btn.clicked.connect(self._on_about)
        self.about_btn.setToolTip("About this application")
        self.about_btn.setStyleSheet("QPushButton { font-weight: bold; background-color: #094771; color: #ffffff; border: 1px solid #007acc; }")
        row1_layout.addWidget(self.about_btn)

        top_vlayout.addLayout(row1_layout)

        # Row 2: Mailbox selection
        row2_layout = QHBoxLayout()
        row2_layout.setSpacing(3)

        lbl_mb = QLabel("MB:")
        lbl_mb.setFixedWidth(22)
        row2_layout.addWidget(lbl_mb)

        self.mailbox_combo = QComboBox()
        self.mailbox_combo.setMaximumWidth(284)
        self.mailbox_combo.setMinimumWidth(284)
        self.mailbox_combo.setFixedHeight(22)
        self.mailbox_combo.currentIndexChanged.connect(self._on_mailbox_changed)
        row2_layout.addWidget(self.mailbox_combo)

        self.owner_label = QLabel("")
        self.owner_label.setStyleSheet("color: #55aaff; font-weight: bold; font-size: 11px;")
        row2_layout.addWidget(self.owner_label)

        row2_layout.addStretch()

        top_vlayout.addLayout(row2_layout)

        layout.addWidget(top_widget)

        # Main content splitter
        main_splitter = QSplitter(Qt.Orientation.Horizontal)

        # Left panel: Folder tree
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)

        # Export buttons above folder tree
        left_btn_layout = QHBoxLayout()
        left_btn_layout.setSpacing(4)

        self.export_folder_btn = QPushButton("Export Folder")
        self.export_folder_btn.clicked.connect(self._on_export_folder)
        self.export_folder_btn.setEnabled(False)
        left_btn_layout.addWidget(self.export_folder_btn)

        self.export_mailbox_btn = QPushButton("Export Mailbox...")
        self.export_mailbox_btn.clicked.connect(self._on_export_mailbox)
        self.export_mailbox_btn.setEnabled(False)
        self.export_mailbox_btn.setToolTip("Export entire mailbox with filters (date, from, to, subject)")
        left_btn_layout.addWidget(self.export_mailbox_btn)

        left_layout.addLayout(left_btn_layout)

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

        # Filter: Show hidden/system items
        self.show_hidden_cb = QCheckBox("Hidden")
        self.show_hidden_cb.setToolTip("Show hidden/system items")
        self.show_hidden_cb.stateChanged.connect(self._on_show_hidden_changed)
        search_layout.addWidget(self.show_hidden_cb)

        # Clear filters button
        self.clear_filters_btn = QPushButton("Clear")
        self.clear_filters_btn.setMaximumWidth(70)
        self.clear_filters_btn.clicked.connect(self._on_clear_filters)
        search_layout.addWidget(self.clear_filters_btn)

        search_layout.addStretch()

        # Message count label
        self.msg_count_label = QLabel("")
        self.msg_count_label.setStyleSheet("color: #969696;")
        search_layout.addWidget(self.msg_count_label)

        middle_layout.addLayout(search_layout)

        # Message list - columns: #, Date, From, To, FromEmail, ToEmail, Subject, Att, Read
        self.message_list = QTreeWidget()
        self.message_list.setHeaderLabels(["#", "Date", "From", "To", "From Email", "To Email", "Subject", "Att", "Read"])
        self.message_list.itemSelectionChanged.connect(self._on_message_selected)
        self.message_list.setMinimumWidth(500)
        self.message_list.setSortingEnabled(True)
        # Set column widths
        self.message_list.setColumnWidth(0, 45)   # #
        self.message_list.setColumnWidth(1, 115)  # Date
        self.message_list.setColumnWidth(2, 120)  # From
        self.message_list.setColumnWidth(3, 120)  # To
        self.message_list.setColumnWidth(4, 150)  # From Email (hidden by default)
        self.message_list.setColumnWidth(5, 150)  # To Email (hidden by default)
        self.message_list.setColumnWidth(6, 180)  # Subject
        self.message_list.setColumnWidth(7, 30)   # Att
        self.message_list.setColumnWidth(8, 35)   # Read
        # Hide email columns by default
        self.message_list.setColumnHidden(4, True)  # From Email
        self.message_list.setColumnHidden(5, True)  # To Email
        middle_layout.addWidget(self.message_list)

        # Store all messages for filtering
        self.all_messages_cache = []

        main_splitter.addWidget(middle_panel)

        # Right panel: Content view
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)

        # Export buttons row at top of right panel
        right_export_layout = QHBoxLayout()
        right_export_layout.setSpacing(4)

        self.export_eml_btn2 = QPushButton("Export Message (.eml)")
        self.export_eml_btn2.clicked.connect(self._on_export_message)
        self.export_eml_btn2.setEnabled(False)
        self.export_eml_btn2.setStyleSheet("QPushButton { padding: 4px 12px; font-weight: bold; background-color: #094771; color: #ffffff; border: 1px solid #007acc; }")
        right_export_layout.addWidget(self.export_eml_btn2)

        self.export_attach_btn = QPushButton("Export Attachments")
        self.export_attach_btn.clicked.connect(self._on_export_attachments)
        self.export_attach_btn.setEnabled(False)
        right_export_layout.addWidget(self.export_attach_btn)

        right_export_layout.addStretch()
        right_layout.addLayout(right_export_layout)

        # Message header section (Outlook-style labels above tabs)
        header_widget = QWidget()
        header_widget.setStyleSheet("QWidget { background-color: #252526; border-bottom: 1px solid #3e3e42; }")
        header_layout = QGridLayout(header_widget)
        header_layout.setContentsMargins(8, 6, 8, 6)
        header_layout.setSpacing(2)

        # Create labels with bold field names
        label_style = "font-weight: bold; color: #969696;"
        value_style = "color: #d4d4d4;"

        # Row 0: From / Organizer / Name
        self.label_row0 = QLabel("From:")
        self.label_row0.setStyleSheet(label_style)
        self.header_from = QLabel("(none)")
        self.header_from.setStyleSheet(value_style)
        self.header_from.setWordWrap(True)
        header_layout.addWidget(self.label_row0, 0, 0)
        header_layout.addWidget(self.header_from, 0, 1)

        # Row 1: To / Attendees / Email
        self.label_row1 = QLabel("To:")
        self.label_row1.setStyleSheet(label_style)
        self.header_to = QLabel("(none)")
        self.header_to.setStyleSheet(value_style)
        self.header_to.setWordWrap(True)
        header_layout.addWidget(self.label_row1, 1, 0)
        header_layout.addWidget(self.header_to, 1, 1)

        # Row 2: Cc / Location / Phone
        self.label_row2 = QLabel("Cc:")
        self.label_row2.setStyleSheet(label_style)
        self.header_cc = QLabel("(none)")
        self.header_cc.setStyleSheet(value_style)
        self.header_cc.setWordWrap(True)
        header_layout.addWidget(self.label_row2, 2, 0)
        header_layout.addWidget(self.header_cc, 2, 1)

        # Row 3: Bcc / (hidden) / Company
        self.label_row3 = QLabel("Bcc:")
        self.label_row3.setStyleSheet(label_style)
        self.header_bcc = QLabel("(none)")
        self.header_bcc.setStyleSheet(value_style)
        self.header_bcc.setWordWrap(True)
        header_layout.addWidget(self.label_row3, 3, 0)
        header_layout.addWidget(self.header_bcc, 3, 1)

        # Row 4: Subject / Subject / Title
        self.label_row4 = QLabel("Subject:")
        self.label_row4.setStyleSheet(label_style)
        self.header_subject = QLabel("(No Subject)")
        self.header_subject.setStyleSheet(value_style + " font-weight: bold; color: #ffffff;")
        self.header_subject.setWordWrap(True)
        header_layout.addWidget(self.label_row4, 4, 0)
        header_layout.addWidget(self.header_subject, 4, 1)

        # Row 5: Date / Start Time / Created
        self.label_row5 = QLabel("Date:")
        self.label_row5.setStyleSheet(label_style)
        self.header_date = QLabel("(none)")
        self.header_date.setStyleSheet(value_style)
        header_layout.addWidget(self.label_row5, 5, 0)
        header_layout.addWidget(self.header_date, 5, 1)

        # Make value column stretch
        header_layout.setColumnStretch(1, 1)

        right_layout.addWidget(header_widget)

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
        self.html_browser_view.setStyleSheet("QTextBrowser { background-color: #ffffff; color: #000000; }")
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

        main_splitter.addWidget(right_panel)
        # Set stretch factors to maintain proportions on resize (10%, 30%, 60%)
        main_splitter.setStretchFactor(0, 1)   # Folders 10%
        main_splitter.setStretchFactor(1, 3)   # Messages 30%
        main_splitter.setStretchFactor(2, 6)   # Message 60%
        main_splitter.setSizes([200, 400, 800])  # Initial sizes

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
            self._full_db_path = path
            self.file_path.setText(os.path.basename(path))
            self.file_path.setCursorPosition(0)
            self.file_path.setToolTip(path)
            self.load_btn.setEnabled(True)

    def _on_load(self):
        path = getattr(self, '_full_db_path', self.file_path.text())
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

        # Populate mailbox combo without auto-selecting
        self.mailbox_combo.blockSignals(True)
        self.mailbox_combo.clear()
        self.mailbox_combo.addItem("-- Select Mailbox --", None)
        for mb in result['mailboxes']:
            owner = mb.get('owner_email', '')
            if owner:
                label = f"{owner} ({mb['message_count']} msgs)"
            else:
                label = f"Mailbox {mb['number']} ({mb['message_count']} msgs)"
            self.mailbox_combo.addItem(label, mb['number'])
        self.mailbox_combo.setCurrentIndex(0)
        self.mailbox_combo.blockSignals(False)

        # Display database statistics
        file_size = result.get('file_size', 0)
        if file_size >= 1024 * 1024 * 1024:
            size_str = f"{file_size / (1024**3):.1f} GB"
        elif file_size >= 1024 * 1024:
            size_str = f"{file_size / (1024**2):.1f} MB"
        elif file_size >= 1024:
            size_str = f"{file_size / 1024:.1f} KB"
        else:
            size_str = f"{file_size} B"
        total_msgs = result.get('total_messages', 0)
        mb_count = len(result['mailboxes'])
        tbl_count = len(self.tables)
        self.db_info_label.setText(f"{size_str} | {mb_count} mailboxes | {total_msgs} messages | {tbl_count} tables")

        self.status.showMessage(f"Loaded {tbl_count} tables, {mb_count} mailboxes")

    def _on_load_error(self, error):
        self.progress.setVisible(False)
        self.load_btn.setEnabled(True)
        QMessageBox.critical(self, "Load Error", f"Failed to load database:\n{error}")

    def _on_mailbox_changed(self, index):
        if index < 0:
            return

        self.current_mailbox = self.mailbox_combo.currentData()
        if self.current_mailbox is None:
            return
        self.status.showMessage(f"Selected mailbox {self.current_mailbox}, loading...")

        # Clear caches when changing mailbox
        self.folder_messages_cache.clear()
        self._cached_msg_col_map = None
        self._cached_msg_columns = None
        self._cached_attach_col_map = None
        self._cached_inid_to_record = None
        self._cached_msgdocid_to_attach = None

        try:
            self._load_folders()
            self._index_messages()
            self._build_mailbox_caches()
            self.export_mailbox_btn.setEnabled(True)
        except Exception as e:
            self.status.showMessage(f"Error loading mailbox: {e}")
            QMessageBox.warning(self, "Error", f"Failed to load mailbox:\n{e}")

    def _build_mailbox_caches(self):
        """Pre-build column maps and attachment index for current mailbox."""
        profiler.start("Build Caches")

        # Cache Message table column map
        msg_table_name = f"Message_{self.current_mailbox}"
        msg_table = self.tables.get(msg_table_name)
        if msg_table:
            col_map = {}
            columns = []
            for i in range(msg_table.get_number_of_columns()):
                col = msg_table.get_column(i)
                if col:
                    col_map[col.name] = i
                    columns.append((i, col.name, col.type))
            self._cached_msg_col_map = col_map
            self._cached_msg_columns = columns

        # Cache Attachment table column map and Inid-to-record index
        attach_table_name = f"Attachment_{self.current_mailbox}"
        attach_table = self.tables.get(attach_table_name)
        if attach_table:
            attach_col_map = get_column_map(attach_table)
            self._cached_attach_col_map = attach_col_map

            inid_to_record = {}
            msgdocid_to_attach = {}
            for i in range(attach_table.get_number_of_records()):
                try:
                    att_record = attach_table.get_record(i)
                    if not att_record:
                        continue
                    inid = get_bytes_value(att_record, attach_col_map.get('Inid', -1))
                    if inid and len(inid) >= 4:
                        inid_val = struct.unpack('<I', inid[:4])[0]
                        inid_to_record[inid_val] = i
                    # Also index by MessageDocumentId for fallback lookups
                    att_msg_id = get_int_value(att_record, attach_col_map.get('MessageDocumentId', -1))
                    if att_msg_id:
                        if att_msg_id not in msgdocid_to_attach:
                            msgdocid_to_attach[att_msg_id] = []
                        msgdocid_to_attach[att_msg_id].append(i)
                except:
                    pass
            self._cached_inid_to_record = inid_to_record
            self._cached_msgdocid_to_attach = msgdocid_to_attach

        profiler.stop("Build Caches")

    def _load_folders(self):
        """Load folders by scanning messages and using Folder table metadata."""
        profiler.start("Load Folders")
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
                        from core.folder_mapping import FOLDER_NUM_TO_NAME
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
                        self.mailbox_email = f"{owner_lower}@unknown"
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
        profiler.stop("Load Folders")

    def _index_messages(self):
        """Index messages by folder with progress indication."""
        profiler.start("Index Messages")
        self.messages_by_folder.clear()

        if not self.current_mailbox:
            return

        msg_table_name = f"Message_{self.current_mailbox}"
        msg_table = self.tables.get(msg_table_name)

        if not msg_table:
            return

        col_map = get_column_map(msg_table)
        total_records = msg_table.get_number_of_records()

        # Show progress
        self.progress.setVisible(True)
        self.progress.setRange(0, total_records)
        self.status.showMessage(f"Indexing {total_records} messages...")

        for i in range(total_records):
            # Update progress every 100 messages
            if i % 100 == 0:
                self.progress.setValue(i)
                QApplication.processEvents()

            try:
                record = msg_table.get_record(i)
                if not record:
                    continue

                folder_id = get_folder_id(record, col_map.get('FolderId', -1))
                if folder_id:
                    self.messages_by_folder[folder_id].append(i)
            except:
                pass

        self.progress.setVisible(False)
        profiler.stop("Index Messages")
        self.status.showMessage(f"Indexed {total_records} messages into {len(self.messages_by_folder)} folders")

    def _on_folder_selected(self):
        """Handle folder selection - load and cache all messages with optimizations."""
        profiler.start("Load Folder Messages")
        self.message_list.clear()
        self.all_messages_cache = []
        self.export_folder_btn.setEnabled(False)
        self.export_eml_btn2.setEnabled(False)
        self.export_attach_btn.setEnabled(False)

        items = self.folder_tree.selectedItems()
        if not items:
            self.msg_count_label.setText("")
            return

        folder_id = items[0].data(0, Qt.ItemDataRole.UserRole)
        self.current_folder_id = folder_id
        self.export_folder_btn.setEnabled(True)
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

                # Full message extraction for accurate From/To/Subject
                subject = ""
                from_display = ""
                to_display = ""
                from_email = ""
                to_email = ""
                is_read = False
                has_attach = False

                if HAS_EMAIL_MODULE and self.email_extractor:
                    try:
                        email_msg = self.email_extractor.extract_message(
                            record, col_map, rec_idx,
                            headers_only=False  # Full decode for accurate data
                        )
                        if email_msg:
                            subject = email_msg.subject or ""
                            from_display = email_msg.sender_name or ""
                            from_email = email_msg.sender_email or ""
                            to_display = email_msg.to_names[0] if email_msg.to_names else from_display
                            to_email = email_msg.to_emails[0] if email_msg.to_emails else from_email
                            is_read = email_msg.is_read
                            has_attach = email_msg.has_attachments
                    except Exception:
                        has_error = True
                else:
                    # Fallback: basic extraction
                    is_read_raw = get_bytes_value(record, col_map.get('IsRead', -1))
                    is_read = bool(is_read_raw and is_read_raw != b'\x00')
                    has_attach_raw = get_bytes_value(record, col_map.get('HasAttachments', -1))
                    has_attach = bool(has_attach_raw and has_attach_raw != b'\x00')

                # Fallback for empty fields - use mailbox owner
                if not from_display and hasattr(self, 'mailbox_owner') and self.mailbox_owner:
                    from_display = self.mailbox_owner
                if not to_display and hasattr(self, 'mailbox_owner') and self.mailbox_owner:
                    to_display = self.mailbox_owner
                if not from_email and hasattr(self, 'mailbox_email') and self.mailbox_email:
                    from_email = self.mailbox_email
                if not to_email and hasattr(self, 'mailbox_email') and self.mailbox_email:
                    to_email = self.mailbox_email

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

            # Cache message data - use emails as primary display
            msg_data = {
                'rec_idx': rec_idx,
                'date': date_str,
                'date_obj': date_received,
                'from': from_email or from_display,  # Show email, fallback to name
                'to': to_email or to_display,        # Show email, fallback to name
                'from_email': from_email,
                'to_email': to_email,
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
        profiler.stop("Load Folder Messages")

    def _apply_filters(self):
        """Apply search and filter criteria to cached messages."""
        profiler.start("Apply Filters")
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

            # Apply search filter (searches names and emails)
            if search_text:
                searchable = f"{msg['subject']} {msg['from']} {msg['to']} {msg.get('from_email', '')} {msg.get('to_email', '')}".lower()
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

            # Create list item - columns: #, Date, From, To, FromEmail, ToEmail, Subject, Att, Read
            item = QTreeWidgetItem()
            item.setText(0, str(msg['rec_idx']))
            item.setData(0, Qt.ItemDataRole.UserRole, msg['rec_idx'])
            item.setText(1, msg['date'])
            item.setText(2, msg['from'])  # From name
            item.setText(3, msg['to'])    # To name
            item.setText(4, msg.get('from_email', ''))  # From email
            item.setText(5, msg.get('to_email', ''))    # To email
            item.setText(6, msg['subject'])
            item.setText(7, "📎" if msg['has_attach'] else "")
            item.setText(8, "✓" if msg['is_read'] else "")

            # Mark unread messages as bold
            if not msg['is_read']:
                font = item.font(0)
                font.setBold(True)
                for col in range(9):
                    item.setFont(col, font)

            # Mark failed/error messages in red
            if msg.get('has_error'):
                item.setText(6, f"[ERROR] {msg['subject']}")
                for col in range(9):
                    item.setForeground(col, QColor(255, 85, 85))

            # Mark hidden items visually (gray, lower priority than red)
            elif msg.get('is_hidden'):
                item.setText(6, f"[HIDDEN] {msg['subject']}")
                for col in range(9):
                    item.setForeground(col, QColor(128, 128, 128))

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
        profiler.stop("Apply Filters")

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

    def _set_header_mode(self, mode: str):
        """Switch header labels between email/calendar/contact modes."""
        if mode == 'calendar':
            self.label_row0.setText("Organizer:")
            self.label_row1.setText("Attendees:")
            self.label_row2.setText("Location:")
            self.label_row3.setVisible(False)
            self.header_bcc.setVisible(False)
            self.label_row4.setText("Subject:")
            self.label_row5.setText("Start Time:")
        elif mode == 'contact':
            self.label_row0.setText("Name:")
            self.label_row1.setText("Email:")
            self.label_row2.setText("Phone:")
            self.label_row3.setText("Company:")
            self.label_row3.setVisible(True)
            self.header_bcc.setVisible(True)
            self.label_row4.setText("Title:")
            self.label_row5.setText("Created:")
        else:  # 'email'
            self.label_row0.setText("From:")
            self.label_row1.setText("To:")
            self.label_row2.setText("Cc:")
            self.label_row3.setText("Bcc:")
            self.label_row3.setVisible(True)
            self.header_bcc.setVisible(True)
            self.label_row4.setText("Subject:")
            self.label_row5.setText("Date:")

    def _extract_contact_fields(self, email_msg, prop_blob):
        """Extract contact-specific fields from existing data (no extra DB reads)."""
        fields = {
            'name': '', 'email': '', 'phone': '',
            'company': '', 'title': '', 'created': ''
        }

        # Name: from sender or subject
        fields['name'] = email_msg.sender_name or email_msg.subject or ''

        # Created date
        created = email_msg.date_sent or email_msg.date_received
        if created and hasattr(created, 'strftime'):
            fields['created'] = created.strftime("%a, %d %b %Y %H:%M:%S %z")

        # Extract from PropertyBlob text
        if prop_blob:
            blob_text = ''
            try:
                if HAS_DISSECT:
                    from dissect.esedb.compression import decompress as d_decompress
                    decompressed = d_decompress(prop_blob)
                    blob_text = decompressed.decode('utf-8', errors='ignore')
                else:
                    blob_text = prop_blob.decode('utf-8', errors='ignore')
            except:
                blob_text = prop_blob.decode('utf-8', errors='ignore')

            # Email pattern
            email_match = re.search(r'[\w.-]+@[\w.-]+\.\w{2,}', blob_text)
            if email_match:
                fields['email'] = email_match.group()

            # Phone pattern
            phone_match = re.search(r'[\+]?[\d\s\-\(\)]{7,15}', blob_text)
            if phone_match:
                phone = phone_match.group().strip()
                if len(phone) >= 7:
                    fields['phone'] = phone

        # Try HTML body for company/title
        if email_msg.body_html:
            html_text = email_msg.body_html
            company_match = re.search(r'(?:company|organization|org)[:\s]*([^<\n]{2,50})',
                                       html_text, re.IGNORECASE)
            if company_match:
                fields['company'] = company_match.group(1).strip()

            title_match = re.search(r'(?:title|position|job)[:\s]*([^<\n]{2,50})',
                                     html_text, re.IGNORECASE)
            if title_match:
                fields['title'] = title_match.group(1).strip()

        return fields

    def _on_message_selected(self):
        """Handle message selection."""
        profiler.start("Select Message")
        items = self.message_list.selectedItems()
        if not items:
            profiler.stop("Select Message")
            return

        rec_idx = items[0].data(0, Qt.ItemDataRole.UserRole)
        self.current_record_idx = rec_idx

        msg_table_name = f"Message_{self.current_mailbox}"
        msg_table = self.tables.get(msg_table_name)

        if not msg_table:
            return

        profiler.start("SM: Get Record")
        record = msg_table.get_record(rec_idx)
        profiler.stop("SM: Get Record")
        if not record:
            return

        # Enable export button
        self.export_eml_btn2.setEnabled(True)

        # Use cached column map (built once per mailbox)
        if self._cached_msg_col_map:
            col_map = self._cached_msg_col_map
            columns = self._cached_msg_columns
        else:
            col_map = {}
            columns = []
            for i in range(msg_table.get_number_of_columns()):
                col = msg_table.get_column(i)
                if col:
                    col_map[col.name] = i
                    columns.append((i, col.name, col.type))

        # Get PropertyBlob
        profiler.start("SM: PropertyBlob")
        prop_blob = get_bytes_value(record, col_map.get('PropertyBlob', -1))
        profiler.stop("SM: PropertyBlob")

        # Load attachments
        self._load_attachments(rec_idx, record, col_map)

        # Get folder info
        folder_id = get_folder_id(record, col_map.get('FolderId', -1))
        folder_name = self.folders.get(folder_id, {}).get('display_name', 'Unknown')

        # === Create EmailMessage for structured extraction ===
        profiler.start("SM: Extract Email")
        email_msg = None
        if HAS_EMAIL_MODULE and self.email_extractor:
            email_msg = self.email_extractor.extract_message(
                record, col_map, rec_idx,
                folder_name=folder_name,
                tables=self.tables,
                mailbox_num=self.current_mailbox
            )
            self.current_email_message = email_msg
        profiler.stop("SM: Extract Email")

        # === Detect message type (use CalendarExtractor for proper LZXPRESS decompression) ===
        msg_class = ''
        if HAS_CALENDAR_MODULE and hasattr(self, 'calendar_extractor') and self.calendar_extractor:
            msg_class = self.calendar_extractor.get_message_class(record, col_map)
        is_calendar = (bool(msg_class) and HAS_CALENDAR_MODULE
                       and hasattr(self, 'calendar_extractor') and self.calendar_extractor
                       and self.calendar_extractor.is_calendar_item(msg_class))
        is_contact = msg_class.upper().startswith('IPM.CONTACT') if msg_class else False

        cal_event = None
        contact = None
        if is_calendar:
            cal_event = self.calendar_extractor.extract_event(record, col_map, rec_idx)
        if is_contact and email_msg:
            contact = self._extract_contact_fields(email_msg, prop_blob)

        # Store for export button
        self.current_cal_event = cal_event
        self.current_contact = contact
        if is_calendar:
            self.current_msg_type = 'calendar'
            self.export_eml_btn2.setText("Export Event (.ics)")
        elif is_contact:
            self.current_msg_type = 'contact'
            self.export_eml_btn2.setText("Export Contact (.vcf)")
        else:
            self.current_msg_type = 'email'
            self.export_eml_btn2.setText("Export Message (.eml)")

        # === Update Header Labels ===
        profiler.start("SM: Update Headers")

        if is_calendar and cal_event:
            self._set_header_mode('calendar')
            # Organizer
            organizer = cal_event.organizer_name or (email_msg.sender_name if email_msg else '') or '(none)'
            if cal_event.organizer_email:
                organizer = f"{organizer} <{cal_event.organizer_email}>"
            self.header_from.setText(organizer)
            # Attendees
            attendee_strs = []
            for att in cal_event.attendees:
                if att.name and att.email:
                    attendee_strs.append(f"{att.name} <{att.email}>")
                elif att.email:
                    attendee_strs.append(att.email)
            self.header_to.setText(", ".join(attendee_strs) if attendee_strs else "(none)")
            # Location
            self.header_cc.setText(cal_event.location or "(none)")
            self.header_bcc.setText("")
            # Subject
            self.header_subject.setText(cal_event.subject or (email_msg.subject if email_msg else '') or "(No Subject)")
            # Start Time
            if cal_event.start_time:
                start_str = cal_event.start_time.strftime("%a, %d %b %Y %H:%M:%S %z")
                if cal_event.end_time:
                    end_str = cal_event.end_time.strftime("%H:%M:%S")
                    start_str += f" - {end_str}"
                self.header_date.setText(start_str)
            else:
                msg_date = email_msg.date_sent or email_msg.date_received if email_msg else None
                self.header_date.setText(msg_date.strftime("%a, %d %b %Y %H:%M:%S %z") if msg_date and hasattr(msg_date, 'strftime') else "(none)")

        elif is_contact and contact:
            self._set_header_mode('contact')
            self.header_from.setText(contact['name'] or "(none)")
            self.header_to.setText(contact['email'] or "(none)")
            self.header_cc.setText(contact['phone'] or "(none)")
            self.header_bcc.setText(contact['company'] or "(none)")
            self.header_subject.setText(contact['title'] or "(none)")
            self.header_date.setText(contact['created'] or "(none)")

        elif email_msg:
            self._set_header_mode('email')
            # From:
            from_header = email_msg.get_from_header()
            self.header_from.setText(from_header if from_header else "(none)")
            # To:
            to_header = email_msg.get_to_header()
            self.header_to.setText(to_header if to_header else "(none)")
            # Cc:
            cc_header = email_msg.get_cc_header()
            self.header_cc.setText(cc_header if cc_header else "(none)")
            # Bcc:
            bcc_recipients = []
            for i, email in enumerate(email_msg.bcc_emails):
                name = email_msg.bcc_names[i] if i < len(email_msg.bcc_names) else ""
                if name:
                    bcc_recipients.append(f"{name} <{email}>")
                else:
                    bcc_recipients.append(email)
            bcc_header = ", ".join(bcc_recipients) if bcc_recipients else "(none)"
            self.header_bcc.setText(bcc_header)
            # Subject:
            self.header_subject.setText(email_msg.subject or "(No Subject)")
            # Date:
            msg_date = email_msg.date_sent or email_msg.date_received
            if msg_date:
                date_str = msg_date.strftime("%a, %d %b %Y %H:%M:%S %z") if hasattr(msg_date, 'strftime') else str(msg_date)
            else:
                date_str = "(none)"
            self.header_date.setText(date_str)
        else:
            self._set_header_mode('email')
            # Fallback - basic info from DB columns
            sender = self.mailbox_owner if hasattr(self, 'mailbox_owner') and self.mailbox_owner else ""
            self.header_from.setText(sender if sender else "(none)")

            display_to = get_string_value(record, col_map.get('DisplayTo', -1))
            self.header_to.setText(display_to if display_to else "(none)")
            self.header_cc.setText("(none)")
            self.header_bcc.setText("(none)")

            self.header_subject.setText("(No Subject)")

            date_sent = get_filetime_value(record, col_map.get('DateSent', -1))
            date_received = get_filetime_value(record, col_map.get('DateReceived', -1))
            msg_date = date_sent or date_received
            if msg_date:
                date_str = msg_date.strftime("%a, %d %b %Y %H:%M:%S %z") if hasattr(msg_date, 'strftime') else str(msg_date)
            else:
                date_str = "(none)"
            self.header_date.setText(date_str)

        profiler.stop("SM: Update Headers")

        # === Parsed View - type-specific message display ===
        profiler.start("SM: Parsed View")
        parsed_text = ""

        if is_calendar and cal_event:
            # Calendar event view
            parsed_text += "=" * 60 + "\n"
            parsed_text += "  CALENDAR EVENT\n"
            parsed_text += "=" * 60 + "\n\n"

            parsed_text += f"Subject:     {cal_event.subject or '(none)'}\n"
            parsed_text += f"Location:    {cal_event.location or '(none)'}\n\n"

            if cal_event.start_time:
                parsed_text += f"Start:       {cal_event.start_time.strftime('%a, %d %b %Y %H:%M:%S %z')}\n"
            if cal_event.end_time:
                parsed_text += f"End:         {cal_event.end_time.strftime('%a, %d %b %Y %H:%M:%S %z')}\n"
            if cal_event.all_day:
                parsed_text += f"All Day:     Yes\n"

            parsed_text += f"\nOrganizer:   {cal_event.organizer_name or '(none)'}"
            if cal_event.organizer_email:
                parsed_text += f" <{cal_event.organizer_email}>"
            parsed_text += "\n"

            if cal_event.attendees:
                parsed_text += f"\nAttendees ({len(cal_event.attendees)}):\n"
                for att in cal_event.attendees:
                    status_str = f" [{att.status}]" if att.status != "NEEDS-ACTION" else ""
                    if att.name:
                        parsed_text += f"  - {att.name} <{att.email}>{status_str}\n"
                    else:
                        parsed_text += f"  - {att.email}{status_str}\n"

            parsed_text += f"\n--- Event Details ---\n"
            parsed_text += f"Status:        {cal_event.status}\n"
            parsed_text += f"Busy Status:   {cal_event.busy_status}\n"
            parsed_text += f"Importance:    {cal_event.importance}\n"
            parsed_text += f"Message Class: {cal_event.message_class}\n"
            parsed_text += f"Record:        #{rec_idx}\n"
            parsed_text += f"Folder:        {folder_name}\n"

            if cal_event.is_recurring:
                parsed_text += f"Recurring:     Yes\n"
                if cal_event.recurrence_rule:
                    parsed_text += f"Rule:          {cal_event.recurrence_rule}\n"

            if cal_event.has_reminder:
                parsed_text += f"Reminder:      {cal_event.reminder_minutes} minutes before\n"

            if cal_event.description:
                parsed_text += f"\n--- Description ---\n{cal_event.description}\n"

        elif is_contact and contact:
            # Contact card view
            parsed_text += "=" * 60 + "\n"
            parsed_text += "  CONTACT CARD\n"
            parsed_text += "=" * 60 + "\n\n"

            parsed_text += f"Name:        {contact.get('name', '(none)')}\n"
            parsed_text += f"Email:       {contact.get('email', '(none)')}\n"
            parsed_text += f"Phone:       {contact.get('phone', '(none)')}\n"
            parsed_text += f"Company:     {contact.get('company', '(none)')}\n"
            parsed_text += f"Title:       {contact.get('title', '(none)')}\n"
            parsed_text += f"Created:     {contact.get('created', '(none)')}\n"

            parsed_text += f"\n--- Record Details ---\n"
            parsed_text += f"Message Class: {email_msg.message_class if email_msg else msg_class}\n"
            parsed_text += f"Record:        #{rec_idx}\n"
            parsed_text += f"Folder:        {folder_name}\n"

        elif email_msg:
            # Standard email view
            parsed_text += "=" * 60 + "\n"

            # From:
            from_header = email_msg.get_from_header()
            parsed_text += f"From:      {from_header if from_header else '(none)'}\n"

            # To: (can be none, one or multiple)
            to_header = email_msg.get_to_header()
            parsed_text += f"To:        {to_header if to_header else '(none)'}\n"

            # Cc: (Copy - can be none, one or multiple)
            cc_header = email_msg.get_cc_header()
            parsed_text += f"Cc:        {cc_header if cc_header else '(none)'}\n"

            # Bcc: (can be none, one or multiple)
            bcc_recipients = []
            for i, email in enumerate(email_msg.bcc_emails):
                name = email_msg.bcc_names[i] if i < len(email_msg.bcc_names) else ""
                if name:
                    bcc_recipients.append(f"{name} <{email}>")
                else:
                    bcc_recipients.append(email)
            bcc_header = ", ".join(bcc_recipients) if bcc_recipients else ""
            parsed_text += f"Bcc:       {bcc_header if bcc_header else '(none)'}\n"

            # Subject:
            parsed_text += f"Subject:   {email_msg.subject or '(No Subject)'}\n"

            # Date:
            msg_date = email_msg.date_sent or email_msg.date_received
            if msg_date:
                date_str = msg_date.strftime("%a, %d %b %Y %H:%M:%S %z") if hasattr(msg_date, 'strftime') else str(msg_date)
            else:
                date_str = "(none)"
            parsed_text += f"Date:      {date_str}\n"

            parsed_text += "=" * 60 + "\n\n"

            # Additional metadata
            parsed_text += f"--- Message Details ---\n"
            parsed_text += f"Folder: {email_msg.folder_name}\n"
            parsed_text += f"Record: #{rec_idx}\n"
            parsed_text += f"Read: {'Yes' if email_msg.is_read else 'No'}\n"

            importance_map = {0: 'Low', 1: 'Normal', 2: 'High'}
            parsed_text += f"Importance: {importance_map.get(email_msg.importance, 'Normal')}\n"
            parsed_text += f"Message Class: {email_msg.message_class}\n"

            if email_msg.message_id:
                parsed_text += f"Message-ID: {email_msg.message_id}\n"

            if email_msg.attachments:
                parsed_text += f"\n--- Attachments ({len(email_msg.attachments)}) ---\n"
                for att in email_msg.attachments:
                    parsed_text += f"  - {att.filename} ({att.size} bytes)\n"

        else:
            # Fallback - minimal info from DB columns
            parsed_text += "=" * 60 + "\n"
            sender = self.mailbox_owner if hasattr(self, 'mailbox_owner') and self.mailbox_owner else ""
            parsed_text += f"From:      {sender if sender else '(none)'}\n"

            display_to = get_string_value(record, col_map.get('DisplayTo', -1))
            parsed_text += f"To:        {display_to if display_to else '(none)'}\n"
            parsed_text += f"Subject:   (No Subject)\n"

            date_sent = get_filetime_value(record, col_map.get('DateSent', -1))
            date_received = get_filetime_value(record, col_map.get('DateReceived', -1))
            msg_date = date_sent or date_received
            if msg_date:
                date_str = msg_date.strftime("%a, %d %b %Y %H:%M:%S %z") if hasattr(msg_date, 'strftime') else str(msg_date)
            else:
                date_str = "(none)"
            parsed_text += f"Date:      {date_str}\n"

            parsed_text += "=" * 60 + "\n\n"
            parsed_text += f"--- Message Details ---\n"
            parsed_text += f"Folder: {folder_name}\n"
            parsed_text += f"Record: #{rec_idx}\n"

        # PropertyBlob hex info (always show)
        if prop_blob:
            # Find Exchange DN
            dn_match = re.search(rb'/O=[A-Z0-9]+/OU=[^/\x00]+(?:/CN=[^/\x00]+)*', prop_blob, re.IGNORECASE)
            if dn_match:
                dn_clean = bytes(b for b in dn_match.group() if 32 <= b < 127)
                parsed_text += f"\nExchange DN: {dn_clean.decode('ascii', errors='ignore')}\n"

        self.parsed_view.setPlainText(parsed_text)
        profiler.stop("SM: Parsed View")

        # === Body View ===
        profiler.start("SM: Body Decode")
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
                                    profiler.start("Decompress Body")
                                    body_data_decompressed = decompress_exchange_body(body_data_raw)
                                    if body_data_decompressed and len(body_data_decompressed) > 10:
                                        # Extract text from HTML
                                        body_text = extract_text_from_html(body_data_decompressed)
                                        # Store HTML source
                                        html_source = body_data_decompressed.decode('utf-8', errors='replace')
                                except Exception as e:
                                    pass
                                finally:
                                    profiler.stop("Decompress Body")

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

        profiler.stop("SM: Body Decode")

        # Set Body (Text) view
        profiler.start("SM: Render Views")
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

        # Update EmailMessage with rendered body content for export
        if email_msg:
            if body_text:
                email_msg.body_text = body_text
            if html_source:
                email_msg.body_html = html_source

        profiler.stop("SM: Render Views")

        # === Hex View ===
        profiler.start("SM: Hex/ASCII/Cols")
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

        profiler.stop("SM: Hex/ASCII/Cols")
        profiler.stop("Select Message")

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
            # Clear folder cache to force reload
            self.folder_messages_cache.clear()
            self._load_folders()
            self._index_messages()

    def _toggle_from_email_column(self):
        """Toggle From Email column visibility."""
        hidden = self.message_list.isColumnHidden(4)
        self.message_list.setColumnHidden(4, not hidden)
        self.show_from_email_action.setChecked(hidden)

    def _toggle_to_email_column(self):
        """Toggle To Email column visibility."""
        hidden = self.message_list.isColumnHidden(5)
        self.message_list.setColumnHidden(5, not hidden)
        self.show_to_email_action.setChecked(hidden)

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
        profiler.start("Load Attachments")
        self.current_attachments = []
        self.attach_list.clear()
        self.export_attach_btn.setEnabled(False)
        self.save_attach_btn.setEnabled(False)
        self.save_all_attach_btn.setEnabled(False)

        # Check if message has attachments
        has_attach = get_bytes_value(record, col_map.get('HasAttachments', -1))
        if not has_attach or has_attach == b'\x00':
            self.content_tabs.setTabText(2, "Attachments (0)")
            profiler.stop("Load Attachments")
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

        # Use cached maps (built once per mailbox)
        attach_col_map = self._cached_attach_col_map if self._cached_attach_col_map else get_column_map(attach_table)
        inid_to_record = self._cached_inid_to_record if self._cached_inid_to_record else {}

        # Load linked attachments only (no fallback to avoid duplicates)
        profiler.start("LA: Find Records")
        records_to_load = []
        use_fallback = False

        if linked_inids and linked_inids != ['FALLBACK']:
            for inid_val in linked_inids:
                if inid_val in inid_to_record:
                    records_to_load.append(inid_to_record[inid_val])
        elif linked_inids == ['FALLBACK'] and subobjects:
            # SubobjectsBlob exists but uses different format - use cached MessageDocumentId lookup
            use_fallback = True
            msg_doc_id = get_int_value(record, col_map.get('MessageDocumentId', -1))
            if msg_doc_id and self._cached_msgdocid_to_attach:
                records_to_load = list(self._cached_msgdocid_to_attach.get(msg_doc_id, []))
        elif not subobjects:
            # No SubobjectsBlob - use cached MessageDocumentId lookup instead of scanning all records
            msg_doc_id = get_int_value(record, col_map.get('MessageDocumentId', -1))
            if msg_doc_id and self._cached_msgdocid_to_attach:
                records_to_load = list(self._cached_msgdocid_to_attach.get(msg_doc_id, []))
        # If SubobjectsBlob exists but has no 0x21 markers, it may be embedded messages
        # In that case, don't load any attachments from the attachment table
        profiler.stop("LA: Find Records")

        profiler.start(f"LA: Read {len(records_to_load)} records")
        for i in records_to_load:
            try:
                att_record = attach_table.get_record(i)
                if not att_record:
                    continue

                # Only read PropertyBlob and Name for metadata - NOT Content (it's a Long Value, very slow)
                prop_blob = get_bytes_value(att_record, attach_col_map.get('PropertyBlob', -1))

                # Get filename
                filename = None
                if prop_blob:
                    filename = extract_attachment_filename(prop_blob)

                if not filename:
                    name_col = get_bytes_value(att_record, attach_col_map.get('Name', -1))
                    if name_col:
                        try:
                            decoded = name_col.decode('utf-16-le').rstrip('\x00')
                            if decoded and all(c.isprintable() for c in decoded):
                                filename = decoded
                        except:
                            pass

                if not filename:
                    filename = f"attachment_{i}.bin"

                # Get content type from PropertyBlob
                content_type = "application/octet-stream"
                if prop_blob:
                    content_type = extract_attachment_content_type(prop_blob)

                # Get size from Size column (quick integer read, not the actual content)
                size_data = get_bytes_value(att_record, attach_col_map.get('Size', -1))
                content_size = 0
                if size_data:
                    if len(size_data) == 8:
                        content_size = struct.unpack('<Q', size_data)[0]
                    elif len(size_data) == 4:
                        content_size = struct.unpack('<I', size_data)[0]

                display_name = f"{filename} ({content_size} bytes)" if content_size > 0 else f"{filename}"

                # Deduplicate by filename
                is_duplicate = False
                for existing in self.current_attachments:
                    if existing[0] == filename:
                        is_duplicate = True
                        break

                if not is_duplicate:
                    # Store metadata only - data loaded on demand via _get_attachment_data()
                    self.current_attachments.append((filename, content_type, content_size, False, i))

                    item = QListWidgetItem(display_name)
                    item.setData(Qt.ItemDataRole.UserRole, len(self.current_attachments) - 1)
                    self.attach_list.addItem(item)

            except Exception as e:
                pass

        profiler.stop(f"LA: Read {len(records_to_load)} records")

        count = len(self.current_attachments)
        self.content_tabs.setTabText(2, f"Attachments ({count})")

        if count > 0:
            self.export_attach_btn.setEnabled(True)
            self.save_attach_btn.setEnabled(True)
            self.save_all_attach_btn.setEnabled(True)
        profiler.stop("Load Attachments")

    def _get_attachment_data(self, att_record_idx):
        """Load attachment binary data on demand from the attachment table."""
        attach_table_name = f"Attachment_{self.current_mailbox}"
        attach_table = self.tables.get(attach_table_name)
        if not attach_table:
            return None

        attach_col_map = self._cached_attach_col_map if self._cached_attach_col_map else get_column_map(attach_table)

        try:
            att_record = attach_table.get_record(att_record_idx)
            if not att_record:
                return None

            content = get_bytes_value(att_record, attach_col_map.get('Content', -1))
            if not content:
                return None

            # Long Value reference
            if len(content) == 4:
                content_idx = attach_col_map.get('Content', -1)
                if content_idx >= 0 and att_record.is_long_value(content_idx):
                    lv = att_record.get_value_data_as_long_value(content_idx)
                    if lv and hasattr(lv, 'get_data'):
                        lv_data = lv.get_data()
                        if lv_data and len(lv_data) > 0:
                            return lv_data
                return None

            # UTF-16LE BOM content
            if content.startswith(b'\xff\xfe'):
                try:
                    return content.decode('utf-16-le').encode('utf-8')
                except:
                    pass

            return content
        except:
            return None

    def _on_export_message(self):
        """Smart export: EML for emails, ICS for calendar, VCF for contacts."""
        if self.current_msg_type == 'calendar':
            self._on_export_single_event()
        elif self.current_msg_type == 'contact':
            self._on_export_single_contact()
        else:
            self._on_export_eml()

    def _on_export_single_event(self):
        """Export current calendar event as .ics file."""
        profiler.start("Export EML")
        if not self.current_cal_event:
            QMessageBox.warning(self, "Export", "No calendar event selected")
            profiler.stop("Export EML")
            return

        subject_safe = re.sub(r'[<>:"/\\|?*]', '_', self.current_cal_event.subject or 'event')[:50]
        default_name = f"record_{self.current_record_idx}_{subject_safe}.ics"

        path, _ = QFileDialog.getSaveFileName(
            self, "Save Calendar Event", default_name,
            "iCalendar Files (*.ics);;All Files (*.*)"
        )
        if not path:
            profiler.stop("Export EML")
            return

        try:
            ics_content = self.current_cal_event.to_ics()
            with open(path, 'w', encoding='utf-8') as f:
                f.write(ics_content)
            self.status.showMessage(f"Exported event to {path}")
            QMessageBox.information(self, "Export", f"Calendar event saved to:\n{path}")
        except Exception as e:
            QMessageBox.critical(self, "Export Error", f"Failed to export event:\n{e}")
        profiler.stop("Export EML")

    def _on_export_single_contact(self):
        """Export current contact as .vcf file."""
        profiler.start("Export EML")
        if not self.current_contact:
            QMessageBox.warning(self, "Export", "No contact selected")
            profiler.stop("Export EML")
            return

        name_safe = re.sub(r'[<>:"/\\|?*]', '_', self.current_contact.get('name', 'contact'))[:50]
        default_name = f"record_{self.current_record_idx}_{name_safe}.vcf"

        path, _ = QFileDialog.getSaveFileName(
            self, "Save Contact", default_name,
            "vCard Files (*.vcf);;All Files (*.*)"
        )
        if not path:
            profiler.stop("Export EML")
            return

        try:
            vcard = self._build_vcard(self.current_contact)
            if not vcard:
                QMessageBox.warning(self, "Export", "Could not build vCard - no name found")
                profiler.stop("Export EML")
                return
            with open(path, 'w', encoding='utf-8') as f:
                f.write(vcard)
            self.status.showMessage(f"Exported contact to {path}")
            QMessageBox.information(self, "Export", f"Contact saved to:\n{path}")
        except Exception as e:
            QMessageBox.critical(self, "Export Error", f"Failed to export contact:\n{e}")
        profiler.stop("Export EML")

    def _on_export_eml(self):
        """Export current message as EML file using stable EmailMessage class."""
        profiler.start("Export EML")
        if not self.current_email_message:
            QMessageBox.warning(self, "Export", "No message selected")
            profiler.stop("Export EML")
            return

        subject_safe = re.sub(r'[<>:"/\\|?*]', '_', self.current_email_message.subject or 'no_subject')[:50]
        default_name = f"record_{self.current_record_idx}_{subject_safe}.eml"

        path, _ = QFileDialog.getSaveFileName(
            self, "Save Email as EML", default_name,
            "Email Files (*.eml);;All Files (*.*)"
        )

        if not path:
            profiler.stop("Export EML")
            return

        try:
            eml_content = self.current_email_message.to_eml()
            with open(path, 'wb') as f:
                f.write(eml_content)
            self.status.showMessage(f"Exported email to {path}")
            QMessageBox.information(self, "Export", f"Email saved to:\n{path}")
        except Exception as e:
            QMessageBox.critical(self, "Export Error", f"Failed to export email:\n{e}")
        profiler.stop("Export EML")

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
            is_external = att[3] if len(att) > 3 else False

            if is_external:
                skipped += 1
                continue

            try:
                data = self._get_attachment_data(att[4])
                if not data:
                    skipped += 1
                    continue

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
        profiler.start("Export Folder")
        items = self.folder_tree.selectedItems()
        if not items:
            QMessageBox.warning(self, "Export", "No folder selected")
            profiler.stop("Export Folder")
            return

        folder_id = items[0].data(0, Qt.ItemDataRole.UserRole)
        folder_name = self.folders.get(folder_id, {}).get('display_name', 'Unknown')
        message_indices = self.messages_by_folder.get(folder_id, [])

        if not message_indices:
            QMessageBox.warning(self, "Export", "No messages in this folder")
            profiler.stop("Export Folder")
            return

        output_dir = QFileDialog.getExistingDirectory(self, "Select Export Directory")
        if not output_dir:
            profiler.stop("Export Folder")
            return

        msg_table_name = f"Message_{self.current_mailbox}"
        msg_table = self.tables.get(msg_table_name)

        if not msg_table:
            profiler.stop("Export Folder")
            return

        col_map = get_column_map(msg_table)

        self.progress.setVisible(True)
        self.progress.setRange(0, len(message_indices))

        exported_eml = 0
        exported_ics = 0
        exported_vcf = 0
        for idx, rec_idx in enumerate(message_indices):
            self.progress.setValue(idx + 1)
            QApplication.processEvents()

            try:
                record = msg_table.get_record(rec_idx)
                if not record:
                    continue

                prop_blob = get_bytes_value(record, col_map.get('PropertyBlob', -1))

                # Detect message type
                msg_class = ''
                if HAS_CALENDAR_MODULE and hasattr(self, 'calendar_extractor') and self.calendar_extractor:
                    msg_class = self.calendar_extractor.get_message_class(record, col_map)
                is_cal = (bool(msg_class) and HAS_CALENDAR_MODULE
                          and self.calendar_extractor.is_calendar_item(msg_class))
                is_vcf = msg_class.upper().startswith('IPM.CONTACT') if msg_class else False

                date_sent = get_filetime_value(record, col_map.get('DateSent', -1))
                date_str = date_sent.strftime("%Y%m%d_%H%M%S") if date_sent else "nodate"

                # Extract full EmailMessage for EML/VCF export
                email_msg = None
                if not is_cal and HAS_EMAIL_MODULE and self.email_extractor:
                    email_msg = self.email_extractor.extract_message(
                        record, col_map, rec_idx, folder_name=folder_name,
                        tables=self.tables, mailbox_num=self.current_mailbox)

                # Get subject from EmailMessage
                subject = email_msg.subject if email_msg else ''
                subject_safe = re.sub(r'[<>:"/\\|?*]', '_', subject or 'no_subject')[:50]

                if is_cal:
                    # Export as ICS
                    cal_event = self.calendar_extractor.extract_event(record, col_map, rec_idx)
                    if cal_event:
                        subject_safe = re.sub(r'[<>:"/\\|?*]', '_', cal_event.subject or 'event')[:50]
                        filename = f"{date_str}_{rec_idx}_{subject_safe}.ics"
                        out_path = Path(output_dir) / filename
                        with open(out_path, 'w', encoding='utf-8') as f:
                            f.write(cal_event.to_ics())
                        exported_ics += 1
                elif is_vcf:
                    # Export as VCF
                    contact = self._extract_contact_fields(email_msg, prop_blob)
                    vcard = self._build_vcard(contact)
                    if vcard:
                        name_safe = re.sub(r'[<>:"/\\|?*]', '_', contact.get('name', 'contact'))[:40]
                        filename = f"{date_str}_{rec_idx}_{name_safe}.vcf"
                        out_path = Path(output_dir) / filename
                        with open(out_path, 'w', encoding='utf-8') as f:
                            f.write(vcard)
                        exported_vcf += 1
                elif email_msg:
                    # Export as EML using EmailMessage.to_eml()
                    filename = f"{date_str}_{rec_idx}_{subject_safe}.eml"
                    eml_content = email_msg.to_eml()
                    out_path = Path(output_dir) / filename
                    with open(out_path, 'wb') as f:
                        f.write(eml_content)
                    exported_eml += 1

            except Exception as e:
                self.status.showMessage(f"Error exporting record {rec_idx}: {e}")

        self.progress.setVisible(False)
        total = exported_eml + exported_ics + exported_vcf
        parts = []
        if exported_eml:
            parts.append(f"{exported_eml} emails (.eml)")
        if exported_ics:
            parts.append(f"{exported_ics} events (.ics)")
        if exported_vcf:
            parts.append(f"{exported_vcf} contacts (.vcf)")
        detail = ", ".join(parts) if parts else "0 items"
        self.status.showMessage(f"Exported {total} items to {output_dir}")
        QMessageBox.information(self, "Export", f"Exported {detail} from {folder_name} to:\n{output_dir}")
        profiler.stop("Export Folder")

    def _on_export_calendar(self):
        """Export calendar items from the current folder to .ics file."""
        profiler.start("Export Calendar")
        if not HAS_CALENDAR_MODULE:
            QMessageBox.warning(self, "Export", "Calendar module not available")
            profiler.stop("Export Calendar")
            return

        items = self.folder_tree.selectedItems()
        if not items:
            QMessageBox.warning(self, "Export", "No folder selected")
            profiler.stop("Export Calendar")
            return

        folder_id = items[0].data(0, Qt.ItemDataRole.UserRole)
        folder_name = self.folders.get(folder_id, {}).get('display_name', 'Unknown')
        message_indices = self.messages_by_folder.get(folder_id, [])

        if not message_indices:
            QMessageBox.warning(self, "Export", "No messages in this folder")
            profiler.stop("Export Calendar")
            return

        msg_table_name = f"Message_{self.current_mailbox}"
        msg_table = self.tables.get(msg_table_name)

        if not msg_table:
            profiler.stop("Export Calendar")
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
        profiler.stop("Export Calendar")

    def _on_export_contacts(self):
        """Export contacts from the current folder to .vcf file."""
        profiler.start("Export Contacts")

        items = self.folder_tree.selectedItems()
        if not items:
            QMessageBox.warning(self, "Export", "No folder selected")
            profiler.stop("Export Contacts")
            return

        folder_id = items[0].data(0, Qt.ItemDataRole.UserRole)
        folder_name = self.folders.get(folder_id, {}).get('display_name', 'Unknown')
        message_indices = self.messages_by_folder.get(folder_id, [])

        if not message_indices:
            QMessageBox.warning(self, "Export", "No messages in this folder")
            profiler.stop("Export Contacts")
            return

        msg_table_name = f"Message_{self.current_mailbox}"
        msg_table = self.tables.get(msg_table_name)

        if not msg_table:
            profiler.stop("Export Contacts")
            return

        col_map = self._cached_msg_col_map if self._cached_msg_col_map else get_column_map(msg_table)

        # Collect contacts
        vcards = []
        self.progress.setVisible(True)
        self.progress.setRange(0, len(message_indices))

        for idx, rec_idx in enumerate(message_indices):
            self.progress.setValue(idx + 1)
            if idx % 20 == 0:
                QApplication.processEvents()

            try:
                record = msg_table.get_record(rec_idx)
                if not record:
                    continue

                # Check if this is a contact item
                msg_class = ''
                if HAS_CALENDAR_MODULE and self.calendar_extractor:
                    msg_class = self.calendar_extractor.get_message_class(record, col_map)
                if not msg_class:
                    continue
                if not msg_class.upper().startswith('IPM.CONTACT'):
                    continue

                # Extract contact fields using EmailExtractor + _extract_contact_fields
                email_msg = None
                if HAS_EMAIL_MODULE and self.email_extractor:
                    email_msg = self.email_extractor.extract_message(
                        record, col_map, rec_idx,
                        folder_name=folder_name,
                        tables=self.tables,
                        mailbox_num=self.current_mailbox
                    )

                prop_blob = get_bytes_value(record, col_map.get('PropertyBlob', -1))

                if email_msg:
                    contact = self._extract_contact_fields(email_msg, prop_blob)
                else:
                    contact = {'name': '', 'email': '', 'phone': '',
                               'company': '', 'title': '', 'created': ''}

                # Build vCard
                vcard = self._build_vcard(contact)
                if vcard:
                    vcards.append(vcard)

            except Exception as e:
                self.status.showMessage(f"Error processing record {rec_idx}: {e}")

        self.progress.setVisible(False)

        if not vcards:
            QMessageBox.information(self, "Export Contacts",
                f"No contacts found in '{folder_name}'.\n\n"
                f"Contact items have message class: IPM.Contact")
            profiler.stop("Export Contacts")
            return

        # Ask for output file
        output_path, _ = QFileDialog.getSaveFileName(
            self, "Save Contacts File",
            f"{folder_name.replace(' ', '_')}_contacts.vcf",
            "vCard Files (*.vcf);;All Files (*.*)"
        )

        if not output_path:
            profiler.stop("Export Contacts")
            return

        # Write all vCards to single file
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write('\r\n'.join(vcards))

            self.status.showMessage(f"Exported {len(vcards)} contacts to {output_path}")
            QMessageBox.information(self, "Export Contacts",
                f"Exported {len(vcards)} contacts from '{folder_name}' to:\n{output_path}")
        except Exception as e:
            QMessageBox.warning(self, "Export Error", f"Failed to export contacts: {e}")

        profiler.stop("Export Contacts")

    def _build_vcard(self, contact: dict) -> str:
        """Build a vCard 3.0 string from contact fields."""
        name = contact.get('name', '').strip()
        if not name:
            return ''

        lines = []
        lines.append("BEGIN:VCARD")
        lines.append("VERSION:3.0")

        # Full name
        lines.append(f"FN:{self._vcard_escape(name)}")

        # Structured name (try to split into last/first)
        parts = name.split()
        if len(parts) >= 2:
            lines.append(f"N:{self._vcard_escape(parts[-1])};{self._vcard_escape(' '.join(parts[:-1]))}")
        else:
            lines.append(f"N:{self._vcard_escape(name)};;;")

        # Email
        email = contact.get('email', '').strip()
        if email:
            lines.append(f"EMAIL;TYPE=INTERNET:{email}")

        # Phone
        phone = contact.get('phone', '').strip()
        if phone:
            lines.append(f"TEL;TYPE=WORK:{phone}")

        # Company
        company = contact.get('company', '').strip()
        if company:
            lines.append(f"ORG:{self._vcard_escape(company)}")

        # Title
        title = contact.get('title', '').strip()
        if title:
            lines.append(f"TITLE:{self._vcard_escape(title)}")

        lines.append("END:VCARD")
        return '\r\n'.join(lines)

    @staticmethod
    def _vcard_escape(text: str) -> str:
        """Escape special characters for vCard format."""
        if not text:
            return ''
        text = text.replace('\\', '\\\\')
        text = text.replace(',', '\\,')
        text = text.replace(';', '\\;')
        text = text.replace('\n', '\\n')
        return text

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
        profiler.start("Export Mailbox")
        if not self.current_mailbox:
            QMessageBox.warning(self, "Export", "No mailbox selected")
            profiler.stop("Export Mailbox")
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
            profiler.stop("Export Mailbox")
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

        exported_eml = 0
        exported_ics = 0
        exported_vcf = 0
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

                # Detect message type
                msg_class = ''
                if HAS_CALENDAR_MODULE and self.calendar_extractor:
                    msg_class = self.calendar_extractor.get_message_class(record, col_map)
                is_cal = (bool(msg_class) and HAS_CALENDAR_MODULE
                          and self.calendar_extractor.is_calendar_item(msg_class))
                is_vcf = msg_class.upper().startswith('IPM.CONTACT') if msg_class else False

                date_str = date_received.strftime("%Y%m%d_%H%M%S") if date_received else "nodate"
                subject_safe = re.sub(r'[<>:"/\\|?*]', '_', email_msg.subject or 'no_subject')[:40]

                if is_cal:
                    # Export as ICS
                    cal_event = self.calendar_extractor.extract_event(record, col_map, rec_idx)
                    if cal_event:
                        filename = f"{date_str}_{rec_idx}_{subject_safe}.ics"
                        out_path = folder_path / filename
                        with open(out_path, 'w', encoding='utf-8') as f:
                            f.write(cal_event.to_ics())
                        exported_ics += 1
                elif is_vcf:
                    # Export as VCF
                    contact = self._extract_contact_fields(email_msg, get_bytes_value(record, col_map.get('PropertyBlob', -1)))
                    vcard = self._build_vcard(contact)
                    if vcard:
                        name_safe = re.sub(r'[<>:"/\\|?*]', '_', contact.get('name', 'contact'))[:40]
                        filename = f"{date_str}_{rec_idx}_{name_safe}.vcf"
                        out_path = folder_path / filename
                        with open(out_path, 'w', encoding='utf-8') as f:
                            f.write(vcard)
                        exported_vcf += 1
                else:
                    # Export as EML
                    filename = f"{date_str}_{rec_idx}_{subject_safe}.eml"
                    eml_content = email_msg.to_eml()
                    out_path = folder_path / filename
                    with open(out_path, 'wb') as f:
                        f.write(eml_content)
                    exported_eml += 1

            except Exception as e:
                self.status.showMessage(f"Error exporting record {rec_idx}: {e}")

        self.progress.setVisible(False)

        # Summary
        total = exported_eml + exported_ics + exported_vcf
        parts = []
        if exported_eml:
            parts.append(f"{exported_eml} emails (.eml)")
        if exported_ics:
            parts.append(f"{exported_ics} events (.ics)")
        if exported_vcf:
            parts.append(f"{exported_vcf} contacts (.vcf)")
        detail = ", ".join(parts) if parts else "0 items"

        summary = f"Export complete!\n\n"
        summary += f"Exported: {detail}\n"
        summary += f"Skipped: {skipped} (filtered out)\n"
        summary += f"Location: {mailbox_dir}"

        self.status.showMessage(f"Exported {total} items to {mailbox_dir}")
        QMessageBox.information(self, "Export Mailbox", summary)
        profiler.stop("Export Mailbox")

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
            data = self._get_attachment_data(att[4])
            if not data:
                QMessageBox.warning(self, "Save", "Could not read attachment data")
                return
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
                data = self._get_attachment_data(att[4])
                if not data:
                    QMessageBox.warning(self, "Save", "Could not read attachment data")
                    return
                with open(path, 'wb') as f:
                    f.write(data)
                self.status.showMessage(f"Saved {filename} to {path}")
            except Exception as e:
                QMessageBox.critical(self, "Save Error", f"Failed to save attachment:\n{e}")

    def _on_about(self):
        """Show About dialog with developer information and debug toggle."""
        dlg = QDialog(self)
        dlg.setWindowTitle("About Exchange EDB Exporter")
        dlg.setMinimumWidth(420)
        layout = QVBoxLayout(dlg)

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
        text_browser = QTextBrowser()
        text_browser.setHtml(about_text)
        text_browser.setOpenExternalLinks(True)
        text_browser.setStyleSheet("QTextBrowser { background-color: #252526; color: #d4d4d4; border: none; }")
        layout.addWidget(text_browser)

        debug_cb = QCheckBox("Enable Debug Profiler")
        debug_cb.setChecked(self.debug_mode)
        def _toggle_debug(checked):
            self.debug_mode = checked
            if checked:
                if not self.profiler_dialog:
                    self.profiler_dialog = ProfilerDialog(self)
                self.profiler_dialog.show()
                self.profiler_dialog.raise_()
            else:
                if self.profiler_dialog:
                    self.profiler_dialog.hide()
        debug_cb.toggled.connect(_toggle_debug)
        layout.addWidget(debug_cb)

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(dlg.accept)
        layout.addWidget(close_btn)

        dlg.exec()

    def closeEvent(self, event):
        if self.db:
            self.db.close()
        event.accept()


def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    # Dark mode palette for cross-platform consistency
    dark_palette = QPalette()
    dark_palette.setColor(QPalette.ColorRole.Window, QColor(45, 45, 48))
    dark_palette.setColor(QPalette.ColorRole.WindowText, QColor(212, 212, 212))
    dark_palette.setColor(QPalette.ColorRole.Base, QColor(30, 30, 30))
    dark_palette.setColor(QPalette.ColorRole.AlternateBase, QColor(45, 45, 48))
    dark_palette.setColor(QPalette.ColorRole.ToolTipBase, QColor(50, 50, 50))
    dark_palette.setColor(QPalette.ColorRole.ToolTipText, QColor(212, 212, 212))
    dark_palette.setColor(QPalette.ColorRole.Text, QColor(212, 212, 212))
    dark_palette.setColor(QPalette.ColorRole.Button, QColor(55, 55, 58))
    dark_palette.setColor(QPalette.ColorRole.ButtonText, QColor(212, 212, 212))
    dark_palette.setColor(QPalette.ColorRole.BrightText, QColor(255, 255, 255))
    dark_palette.setColor(QPalette.ColorRole.Link, QColor(85, 170, 255))
    dark_palette.setColor(QPalette.ColorRole.Highlight, QColor(42, 130, 218))
    dark_palette.setColor(QPalette.ColorRole.HighlightedText, QColor(255, 255, 255))
    dark_palette.setColor(QPalette.ColorRole.PlaceholderText, QColor(128, 128, 128))
    # Disabled state
    dark_palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.WindowText, QColor(128, 128, 128))
    dark_palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.Text, QColor(128, 128, 128))
    dark_palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.ButtonText, QColor(128, 128, 128))
    app.setPalette(dark_palette)

    # Global stylesheet for consistent dark look
    app.setStyleSheet("""
        QMainWindow, QWidget {
            background-color: #2d2d30;
            color: #d4d4d4;
        }
        QMenuBar {
            background-color: #2d2d30;
            color: #d4d4d4;
        }
        QMenuBar::item:selected {
            background-color: #3e3e42;
        }
        QMenu {
            background-color: #2d2d30;
            color: #d4d4d4;
            border: 1px solid #3e3e42;
        }
        QMenu::item:selected {
            background-color: #094771;
        }
        QTreeWidget, QTableWidget, QListWidget, QTextEdit, QTextBrowser {
            background-color: #1e1e1e;
            color: #d4d4d4;
            border: none;
            selection-background-color: #094771;
            selection-color: #ffffff;
        }
        QHeaderView::section {
            background-color: #2d2d30;
            color: #d4d4d4;
            border: 1px solid #3e3e42;
            padding: 4px;
        }
        QTabWidget::pane {
            border: 1px solid #3e3e42;
            background-color: #1e1e1e;
        }
        QTabBar::tab {
            background-color: #2d2d30;
            color: #969696;
            border: 1px solid #3e3e42;
            padding: 6px 12px;
            margin-right: 2px;
        }
        QTabBar::tab:selected {
            background-color: #1e1e1e;
            color: #d4d4d4;
            border-bottom-color: #1e1e1e;
        }
        QTabBar::tab:hover {
            background-color: #3e3e42;
            color: #d4d4d4;
        }
        QPushButton {
            background-color: #3e3e42;
            color: #d4d4d4;
            border: 1px solid #555558;
            padding: 4px 10px;
            border-radius: 2px;
        }
        QPushButton:hover {
            background-color: #4e4e52;
        }
        QPushButton:pressed {
            background-color: #094771;
        }
        QPushButton:disabled {
            background-color: #2d2d30;
            color: #666;
            border-color: #3e3e42;
        }
        QLineEdit, QComboBox {
            background-color: #3c3c3c;
            color: #d4d4d4;
            border: 1px solid #555558;
            padding: 2px 4px;
            border-radius: 2px;
        }
        QComboBox::drop-down {
            border-left: 1px solid #555558;
        }
        QComboBox QAbstractItemView {
            background-color: #2d2d30;
            color: #d4d4d4;
            selection-background-color: #094771;
        }
        QCheckBox {
            color: #d4d4d4;
        }
        QLabel {
            color: #d4d4d4;
        }
        QStatusBar {
            background-color: #007acc;
            color: #ffffff;
        }
        QProgressBar {
            background-color: #3c3c3c;
            border: 1px solid #555558;
            text-align: center;
            color: #d4d4d4;
        }
        QProgressBar::chunk {
            background-color: #007acc;
        }
        QSplitter::handle {
            background-color: #3e3e42;
        }
        QScrollBar:vertical {
            background-color: #1e1e1e;
            width: 12px;
        }
        QScrollBar::handle:vertical {
            background-color: #555558;
            min-height: 20px;
            border-radius: 3px;
        }
        QScrollBar::handle:vertical:hover {
            background-color: #6e6e72;
        }
        QScrollBar:horizontal {
            background-color: #1e1e1e;
            height: 12px;
        }
        QScrollBar::handle:horizontal {
            background-color: #555558;
            min-width: 20px;
            border-radius: 3px;
        }
        QScrollBar::handle:horizontal:hover {
            background-color: #6e6e72;
        }
        QScrollBar::add-line, QScrollBar::sub-line {
            height: 0; width: 0;
        }
        QMessageBox {
            background-color: #2d2d30;
        }
        QDialog {
            background-color: #2d2d30;
            color: #d4d4d4;
        }
    """)

    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
