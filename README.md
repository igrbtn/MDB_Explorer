# Exchange EDB Viewer & Exporter

A Python GUI application for viewing and exporting emails from Microsoft Exchange EDB (Extensible Storage Engine Database) files.

## Features

- Browse mailbox folder structure with proper folder names
- View messages with From, To, Subject, Date fields
- Display email body (text and HTML views)
- Extract and save attachments (including large Long Value attachments)
- Export emails as EML format
- Export entire folders
- Support for hidden/system items toggle
- Mailbox owner detection from Sent Items

## Requirements

```
Python 3.8+
PyQt6
pyesedb (libesedb-python)
```

## Installation

```bash
pip install PyQt6
pip install libesedb-python
```

## Usage

```bash
python gui_viewer_v2.py [path_to_edb_file]
```

Or launch without arguments and use the Browse button to select an EDB file.

---

# Application Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         GUI Layer (PyQt6)                            │
├─────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────────┐ │
│  │ File Select  │  │   Mailbox    │  │      Owner Label           │ │
│  │   Toolbar    │  │   Dropdown   │  │   (from Sent Items)        │ │
│  └──────────────┘  └──────────────┘  └────────────────────────────┘ │
├─────────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────────┐  ┌─────────────────────────┐  │
│  │   Folder    │  │  Message List   │  │    Content Viewer       │  │
│  │    Tree     │  │  (From/To/Subj) │  │  ┌─────────────────────┐│  │
│  │             │  │                 │  │  │ Body (Text)         ││  │
│  │  - Inbox    │  │  #  Date  From  │  │  │ Body (HTML)         ││  │
│  │  - Sent     │  │  1  2026  Admin │  │  │ Parsed Data         ││  │
│  │  - Drafts   │  │  2  2026  User  │  │  │ PropertyBlob (Hex)  ││  │
│  │  - Deleted  │  │                 │  │  │ ASCII Strings       ││  │
│  │  - Calendar │  │                 │  │  │ Attachments         ││  │
│  │  - Contacts │  │                 │  │  │ All Columns         ││  │
│  │  ...        │  │                 │  │  └─────────────────────┘│  │
│  └─────────────┘  └─────────────────┘  └─────────────────────────┘  │
├─────────────────────────────────────────────────────────────────────┤
│                          Status Bar                                  │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                      Core Data Layer                                 │
├─────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐    ┌──────────────────┐                       │
│  │   LoadWorker     │    │   Data Models    │                       │
│  │   (QThread)      │    │                  │                       │
│  │                  │    │  - Folder        │                       │
│  │  - Open EDB      │    │  - Message       │                       │
│  │  - Scan tables   │    │  - Attachment    │                       │
│  │  - Detect MBs    │    │  - EmailData     │                       │
│  │  - Get owner     │    │                  │                       │
│  └──────────────────┘    └──────────────────┘                       │
├─────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                    pyesedb Library                            │   │
│  │                                                               │   │
│  │  - ESE database parsing                                       │   │
│  │  - Table/Column/Record access                                 │   │
│  │  - Long Value (LV) retrieval via get_value_data_as_long_value │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

## Key Components

### GUI Components (`gui_viewer_v2.py`)

| Component | Description |
|-----------|-------------|
| `MainWindow` | Main application window with splitter layout |
| `LoadWorker` | Background thread for loading EDB files |
| `folder_tree` | QTreeWidget showing folder hierarchy |
| `message_list` | QTreeWidget showing messages in selected folder |
| `content_tabs` | QTabWidget with multiple content views |

### Data Flow

```
EDB File → pyesedb → Tables Dict → Folder/Message Indexing → GUI Display
                                          ↓
                              Attachment extraction (Long Value)
                                          ↓
                                    EML Export
```

---

# Microsoft Exchange EDB Database Structure

## Overview

Exchange Server stores mailbox data in EDB files using Microsoft's **Extensible Storage Engine (ESE)**, also known as JET Blue. The database contains multiple tables organized by mailbox number.

## Database Layout

```
Exchange EDB File
│
├── System Tables
│   ├── MSysObjects        (Table catalog)
│   ├── MSysObjids         (Object IDs)
│   ├── MSysLocales        (Locale info)
│   └── MSysDatabaseMaintenance
│
├── Global Tables
│   ├── GlobalLocaleIds
│   ├── Mailbox            (Mailbox metadata)
│   └── MailboxIdentity
│
└── Per-Mailbox Tables (XXX = mailbox number, e.g., 103)
    ├── Folder_XXX         (Folder definitions)
    ├── Message_XXX        (Email messages)
    ├── Attachment_XXX     (File attachments)
    ├── Recipient_XXX      (Recipients)
    └── ...other tables
```

## Key Tables

### Folder_XXX Table

Stores folder hierarchy and metadata.

| Column | Type | Description |
|--------|------|-------------|
| `FolderId` | Binary(26) | Unique folder identifier |
| `ParentFolderId` | Binary(26) | Parent folder reference |
| `DisplayName` | LongText | Folder name (UTF-16LE, often encrypted) |
| `SpecialFolderNumber` | Long | Standard folder type (see below) |
| `MessageCount` | Long | Number of messages |

**Special Folder Numbers:**
```
1  = Root
9  = IPM Subtree (Top of Information Store)
10 = Inbox
11 = Outbox
12 = Sent Items
13 = Deleted Items
14 = Contacts
15 = Calendar
16 = Drafts
17 = Journal
18 = Notes
19 = Tasks
20 = Recoverable Items
```

### Message_XXX Table

Stores email messages and calendar items.

| Column | Type | Description |
|--------|------|-------------|
| `MessageDocumentId` | Long | Unique message ID |
| `FolderId` | Binary(26) | Parent folder |
| `MessageClass` | LongText | Item type (IPM.Note, IPM.Appointment, etc.) |
| `DateReceived` | DateTime | FILETIME format |
| `DateSent` | DateTime | FILETIME format |
| `IsRead` | Bit | Read status |
| `IsHidden` | Bit | Hidden/system item flag |
| `HasAttachments` | Bit | Attachment indicator |
| `DisplayTo` | LongText | Recipients display |
| `PropertyBlob` | LongBinary | MAPI properties (compressed) |
| `NativeBody` | LongBinary | HTML body (LZXPRESS compressed) |
| `SubobjectsBlob` | LongBinary | Attachment references |

### Attachment_XXX Table

Stores file attachments.

| Column | Type | Description |
|--------|------|-------------|
| `Inid` | LongLong | Unique attachment ID |
| `AttachmentId` | Binary | Full attachment identifier |
| `Content` | LongBinary | Attachment data or LV reference |
| `Size` | LongLong | Attachment size in bytes |
| `Name` | LongText | Filename (often encrypted) |
| `ContentType` | LongText | MIME type |
| `PropertyBlob` | LongBinary | Attachment properties |

## Data Storage Mechanisms

### PropertyBlob Format

MAPI properties are stored in a compressed binary format:

```
PropertyBlob Structure:
┌─────────────┬─────────────┬─────────────┬─────────────┐
│  Property   │  Property   │   Value     │    ...      │
│    Tag      │    Type     │   Data      │             │
│  (2 bytes)  │  (2 bytes)  │  (varies)   │             │
└─────────────┴─────────────┴─────────────┴─────────────┘

Common markers:
- 0x4D (M) + length byte + string = Subject/text field
- 0x4B (K) + length byte + string = Alternative text
```

### Long Value (LV) Storage

Large binary data (>255 bytes) is stored in ESE's Long Value B+ tree:

```
Normal column value:
┌─────────────────────┐
│  Inline data        │
│  (up to 255 bytes)  │
└─────────────────────┘

Long Value reference:
┌─────────────────────┐      ┌─────────────────────┐
│  4-byte LV ID       │ ───> │  Actual data in     │
│  (reference)        │      │  LV B+ tree         │
└─────────────────────┘      └─────────────────────┘

Access via pyesedb:
  record.is_long_value(column_idx)  → True/False
  record.get_value_data_as_long_value(column_idx).get_data()  → bytes
```

### SubobjectsBlob Format

Links messages to their attachments:

```
Format 1 (0x21 markers):
┌──────────┬──────────┬──────────┬──────────┐
│  Header  │  0x21    │  Inid    │  0x21    │ ...
│  bytes   │ (marker) │ (1 byte) │ (marker) │
└──────────┴──────────┴──────────┴──────────┘

Format 2 (0x0F format - Exchange 2013+):
┌──────────┬──────────┬──────────┬──────────┐
│   0x0F   │  Header  │  0x84    │  Inid+20 │ ...
│ (length) │  bytes   │ (marker) │ (encoded)│
└──────────┴──────────┴──────────┴──────────┘

Inid values link to Attachment_XXX.Inid column
```

### PropertyBlob Body Encoding

The PropertyBlob stores subject and body text using a custom compression format:

```
PropertyBlob Structure:
┌──────────────────────────────────────────────────────────────┐
│  Header bytes                                                │
├──────────────────────────────────────────────────────────────┤
│  ... metadata (GUIDs, addresses, etc.) ...                   │
├──────────────────────────────────────────────────────────────┤
│  Sender name ending with 'M' marker (e.g., "Rosetta StoneM") │
├──────────────────────────────────────────────────────────────┤
│  Subject (repeat-encoded) - see format below                 │
├──────────────────────────────────────────────────────────────┤
│  ... more metadata (message-ID, etc.) ...                    │
├──────────────────────────────────────────────────────────────┤
│  Body text (may use back-references to subject)              │
└──────────────────────────────────────────────────────────────┘

Subject/Body Repeat Encoding:
┌─────────────────────────────────────────────────────────────────┐
│  [Length] [Char1][00][00] [Space] [Char2][00][00] [Space] ...   │
└─────────────────────────────────────────────────────────────────┘

First byte after 'M' marker = expected output length

Pattern: char + 00 00 = repeat char 4 times total
  - Example: 0x41 0x00 0x00 → "AAAA"
  - Example: 0x31 0x00 0x00 → "1111"

Pattern: char + 48 48 = alternate repeat marker (same effect)
  - Example: 0x43 0x48 0x48 → "CCCC"

Space (0x20) is always literal, never repeated.

Example encoding:
  Subject "AAAA BBBB CCCC" (14 chars) is stored as:
  0e 41 00 00 20 42 00 00 20 a8 01 43 48 48
  │  │  └──┘  │  │  └──┘  │  └──┘  │  └──┘
  │  │   │    │  │   │    │   │    │   └─ repeat pattern
  │  │   │    │  │   │    │   │    └─ 'C'
  │  │   │    │  │   │    │   └─ control bytes
  │  │   │    │  │   │    └─ space
  │  │   │    │  │   └─ repeat → "BBBB"
  │  │   │    │  └─ 'B'
  │  │   │    └─ space
  │  │   └─ repeat → "AAAA"
  │  └─ 'A'
  └─ length = 14 chars
```

### NativeBody Compression

HTML body content uses Exchange LZXPRESS compression with the same repeat pattern:

```
┌─────────────────────────────────────────────────┐
│  Header (7 bytes)                               │
│  ┌─────┬─────────────┬───────────────────────┐  │
│  │0x18 │ Uncompressed│ Flags/Reserved        │  │
│  │     │ Size (2B)   │ (4 bytes)             │  │
│  └─────┴─────────────┴───────────────────────┘  │
├─────────────────────────────────────────────────┤
│  Compressed HTML data                           │
│  - Literal bytes (printable ASCII)              │
│  - Repeat pattern: char + 00 00 = repeat 4x     │
│  - Back-references (high-bit control bytes)     │
│  - LZ77-style offset/length encoding            │
└─────────────────────────────────────────────────┘

Decompression implemented in lzxpress.py
```

### FolderId Format

```
Full FolderId (26 bytes):
┌─────────────────────────────┬─────────────────────────┐
│  Mailbox Prefix (6 bytes)   │  Folder ID (20 bytes)   │
└─────────────────────────────┴─────────────────────────┘

Folder ID breakdown (last 20 chars hex):
Position:  0-7      8-11     12-19
          ┌────────┬────────┬────────┐
          │Prefix  │Folder  │Type/   │
          │        │Number  │Flags   │
          └────────┴────────┴────────┘

Example: 00000000010c00000100
         ^^^^^^^^         = Prefix (00000000)
                 ^^^^     = Folder 010c = Inbox
                     ^^^^ = Type flags
```

## DateTime Format

Exchange uses Windows FILETIME (100-nanosecond intervals since Jan 1, 1601):

```python
def filetime_to_datetime(filetime_bytes):
    ft = struct.unpack('<Q', filetime_bytes)[0]
    unix_time = (ft - 116444736000000000) / 10000000
    return datetime.fromtimestamp(unix_time, tz=timezone.utc)
```

---

# Known Limitations

1. **Body Compression**: NativeBody uses LZXPRESS compression. Repeated patterns may not fully decompress.

2. **Encrypted Fields**: Some fields (DisplayName, Name) may be encrypted in newer Exchange versions.

3. **Large Databases**: Loading large EDB files (>10GB) may be slow.

4. **Offline Only**: EDB files must be dismounted/offline to read.

---

# File Structure

```
edb_exporter/
├── gui_viewer_v2.py       # Main GUI application
├── lzxpress.py            # Body text extraction & decompression
│                          # - extract_body_from_property_blob()
│                          # - decompress_exchange_body()
│                          # - extract_text_from_html()
│                          # - decode_repeat_pattern()
├── folder_mapping.py      # Exchange folder ID mappings
├── exchange_decompress.py # Legacy body decompression utilities
├── analyze_mailbox.py     # Mailbox analysis tool
├── extract_long_values.py # Attachment extraction
├── src/
│   └── core/
│       ├── ese_reader.py      # Low-level ESE database access
│       ├── edb_reader.py      # EDB file reader wrapper
│       ├── mailbox_parser.py  # Mailbox data parser
│       └── message.py         # Message data model
└── README.md              # This file
```

---

# References

- [Microsoft ESE Database Format](https://docs.microsoft.com/en-us/windows/win32/extensible-storage-engine/extensible-storage-engine)
- [libesedb Documentation](https://github.com/libyal/libesedb)
- [MAPI Property Tags](https://docs.microsoft.com/en-us/office/client-developer/outlook/mapi/mapi-property-tags)
- [Exchange Store Schema](https://docs.microsoft.com/en-us/exchange/architecture/mailbox-servers/managed-store)
