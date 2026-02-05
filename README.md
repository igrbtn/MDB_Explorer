# Exchange EDB Viewer & Exporter

A Python GUI application for viewing and exporting emails from Microsoft Exchange EDB (Extensible Storage Engine Database) files.

## Features

- **Browse mailbox folder structure** with proper folder names
- **View messages** with From, To, Subject, Date fields
- **Display email body** (text and HTML views with QWebEngineView)
- **Extract and save attachments** (including large Long Value attachments)
- **Export emails as EML format** (with body, headers, and attachments)
- **Export entire folders** to EML files
- **Export calendar items** to iCalendar (.ics) format
- **Search and filter** messages by subject, from, to, read status, attachments
- **Mailbox owner detection** from Mailbox table (properly decompressed)
- **Multi-encoding support** (UTF-8, Cyrillic Windows-1251, KOI8-R, etc.)
- Support for hidden/system items toggle

## Requirements

```
Python 3.8+
PyQt6
PyQt6-WebEngine (for HTML rendering)
pyesedb (libesedb-python)
dissect.esedb (for LZXPRESS decompression)
```

## Installation

```bash
pip install PyQt6 PyQt6-WebEngine libesedb-python dissect.esedb
```

Or use the install script:
```bash
./install.sh   # Linux/macOS
install.bat    # Windows
```

## Usage

```bash
python gui_viewer_v2.py [path_to_edb_file]
```

Or launch without arguments and use the Browse button to select an EDB file.

## Project Structure

```
edb_exporter/
├── gui_viewer_v2.py      # Main GUI application
├── email_message.py      # Email extraction and EML export
├── calendar_message.py   # Calendar extraction and ICS export
├── lzxpress.py          # LZXPRESS decompression utilities
├── folder_mapping.py    # Folder name mapping
├── cli.py               # Command-line interface
├── src/
│   └── core/
│       └── ese_reader.py  # ESE database utilities
├── requirements.txt
├── install.sh / install.bat
└── run.sh / run.bat
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         GUI Layer (PyQt6)                           │
├─────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────────┐ │
│  │ File Select  │  │   Mailbox    │  │      Owner Label           │ │
│  │   Toolbar    │  │   Dropdown   │  │   (from Mailbox table)     │ │
│  └──────────────┘  └──────────────┘  └────────────────────────────┘ │
├─────────────────────────────────────────────────────────────────────┤
│  ┌───────────┐  ┌──────────────────────┐  ┌───────────────────────┐ │
│  │  Folder   │  │    Message List      │  │   Content Viewer      │ │
│  │   Tree    │  │  ┌────────────────┐  │  │  ┌─────────────────┐  │ │
│  │           │  │  │ Search/Filter  │  │  │  │ Body (Text)     │  │ │
│  │ - Inbox   │  │  └────────────────┘  │  │  │ Body (HTML)     │  │ │
│  │ - Sent    │  │  # Date From To Subj │  │  │ Parsed Data     │  │ │
│  │ - Drafts  │  │  1 2026 Admin...     │  │  │ Attachments     │  │ │
│  │ - ...     │  │  2 2026 User...      │  │  │ All Columns     │  │ │
│  └───────────┘  └──────────────────────┘  └───────────────────────┘ │
├─────────────────────────────────────────────────────────────────────┤
│  Export: [EML] [Attachments] [Folder] [Calendar (.ics)]             │
└─────────────────────────────────────────────────────────────────────┘
```

## Key Modules

### `email_message.py`
- `EmailMessage` - dataclass for email data
- `EmailExtractor` - extracts emails from EDB records
- `EmailAttachment` - attachment data structure
- Handles PropertyBlob parsing, body decompression, EML export

### `calendar_message.py`
- `CalendarEvent` - dataclass for calendar events
- `CalendarExtractor` - extracts calendar items from EDB
- `export_calendar_to_ics()` - exports to iCalendar format
- Supports IPM.Appointment, IPM.Schedule.Meeting.*, IPM.Task

### `lzxpress.py`
- LZXPRESS decompression for NativeBody (HTML content)
- Uses `dissect.esedb` for accurate decompression
- Fallback decoder for systems without dissect

## Exchange EDB Database Structure

### Key Tables (XXX = mailbox number)

| Table | Description |
|-------|-------------|
| `Mailbox` | Mailbox metadata, owner display name |
| `Folder_XXX` | Folder hierarchy and metadata |
| `Message_XXX` | Email messages and calendar items |
| `Attachment_XXX` | File attachments |

### Special Folder Numbers

| Number | Folder |
|--------|--------|
| 10 | Inbox |
| 11 | Outbox |
| 12 | Sent Items |
| 13 | Deleted Items |
| 14 | Contacts |
| 15 | Calendar |
| 16 | Drafts |

### Data Compression

- **PropertyBlob**: Contains subject, sender, message-id (pattern-based extraction)
- **NativeBody**: HTML body compressed with MS-XCA LZXPRESS
- **Mailbox columns**: Compressed with ESE 7-bit encoding (decompressed via dissect.esedb)

## License

MIT License
