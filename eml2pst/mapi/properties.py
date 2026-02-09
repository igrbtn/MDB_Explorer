"""MAPI property tags, types, and constants for PST files."""

import struct

# --- Property Types (low 2 bytes of property tag) ---
PT_UNSPECIFIED = 0x0000
PT_SHORT = 0x0002  # 16-bit integer
PT_LONG = 0x0003  # 32-bit integer
PT_BOOLEAN = 0x000B  # 8-bit boolean (in 4-byte slot)
PT_SYSTIME = 0x0040  # FILETIME (8 bytes)
PT_STRING8 = 0x001E  # 8-bit string (null-terminated)
PT_UNICODE = 0x001F  # UTF-16LE string (null-terminated)
PT_BINARY = 0x0102  # Binary blob
PT_LONG_LONG = 0x0014  # 64-bit integer
PT_GUID = 0x0048  # 16-byte GUID
PT_MV_BINARY = 0x1102  # Multi-valued binary
PT_MV_UNICODE = 0x101F  # Multi-valued Unicode string

# Fixed-size property data lengths (for inline storage in PC/TC)
PROP_TYPE_SIZES = {
    PT_SHORT: 2,
    PT_LONG: 4,
    PT_BOOLEAN: 4,  # stored as 4 bytes in PST
    PT_LONG_LONG: 8,
    PT_SYSTIME: 8,
    PT_GUID: 16,
}


def is_fixed_type(prop_type):
    return prop_type in PROP_TYPE_SIZES


def fixed_size(prop_type):
    return PROP_TYPE_SIZES.get(prop_type, 0)


def is_variable_type(prop_type):
    return prop_type in (PT_STRING8, PT_UNICODE, PT_BINARY,
                         PT_MV_BINARY, PT_MV_UNICODE)


def prop_tag(prop_id, prop_type):
    return (prop_id << 16) | prop_type


def prop_id(tag):
    return (tag >> 16) & 0xFFFF


def prop_type(tag):
    return tag & 0xFFFF


# --- Message Store Properties ---
PR_RECORD_KEY = prop_tag(0x0FF9, PT_BINARY)
PR_DISPLAY_NAME = prop_tag(0x3001, PT_UNICODE)
PR_IPM_SUBTREE_ENTRYID = prop_tag(0x35E0, PT_BINARY)
PR_IPM_WASTEBASKET_ENTRYID = prop_tag(0x35E3, PT_BINARY)
PR_FINDER_ENTRYID = prop_tag(0x35E7, PT_BINARY)
PR_STORE_SUPPORT_MASK = prop_tag(0x340D, PT_LONG)
PR_STORE_STATE = prop_tag(0x340E, PT_LONG)
PR_VALID_FOLDER_MASK = prop_tag(0x35DF, PT_LONG)
PR_PST_PASSWORD = prop_tag(0x67FF, PT_LONG)

# --- Folder Properties ---
PR_CONTENT_COUNT = prop_tag(0x3602, PT_LONG)
PR_CONTENT_UNREAD_COUNT = prop_tag(0x3603, PT_LONG)
PR_SUBFOLDERS = prop_tag(0x360A, PT_BOOLEAN)
PR_CONTAINER_CLASS = prop_tag(0x3613, PT_UNICODE)

# --- Message Properties ---
PR_SUBJECT = prop_tag(0x0037, PT_UNICODE)
PR_SUBJECT_PREFIX = prop_tag(0x003D, PT_UNICODE)
PR_NORMALIZED_SUBJECT = prop_tag(0x0E1D, PT_UNICODE)
PR_BODY = prop_tag(0x1000, PT_UNICODE)
PR_HTML = prop_tag(0x1013, PT_BINARY)
PR_MESSAGE_CLASS = prop_tag(0x001A, PT_UNICODE)
PR_MESSAGE_FLAGS = prop_tag(0x0E07, PT_LONG)
PR_MESSAGE_SIZE = prop_tag(0x0E08, PT_LONG)
PR_MESSAGE_STATUS = prop_tag(0x0E17, PT_LONG)
PR_IMPORTANCE = prop_tag(0x0017, PT_LONG)
PR_PRIORITY = prop_tag(0x0026, PT_LONG)
PR_SENSITIVITY = prop_tag(0x0036, PT_LONG)
PR_HASATTACH = prop_tag(0x0E1B, PT_BOOLEAN)
PR_MESSAGE_DELIVERY_TIME = prop_tag(0x0E06, PT_SYSTIME)
PR_CLIENT_SUBMIT_TIME = prop_tag(0x0039, PT_SYSTIME)
PR_CREATION_TIME = prop_tag(0x3007, PT_SYSTIME)
PR_LAST_MODIFICATION_TIME = prop_tag(0x3008, PT_SYSTIME)
PR_INTERNET_CPID = prop_tag(0x3FDE, PT_LONG)  # Internet code page (65001 = UTF-8)
PR_MESSAGE_CODEPAGE = prop_tag(0x3FFD, PT_LONG)  # Message code page

# --- Sender Properties ---
PR_SENDER_NAME = prop_tag(0x0C1A, PT_UNICODE)
PR_SENDER_EMAIL_ADDRESS = prop_tag(0x0C1F, PT_UNICODE)
PR_SENDER_ADDRTYPE = prop_tag(0x0C1E, PT_UNICODE)
PR_SENT_REPRESENTING_NAME = prop_tag(0x0042, PT_UNICODE)
PR_SENT_REPRESENTING_EMAIL = prop_tag(0x0065, PT_UNICODE)
PR_SENT_REPRESENTING_ADDRTYPE = prop_tag(0x0064, PT_UNICODE)

# --- Recipient Properties ---
PR_DISPLAY_NAME_W = prop_tag(0x3001, PT_UNICODE)
PR_EMAIL_ADDRESS = prop_tag(0x3003, PT_UNICODE)
PR_ADDRTYPE = prop_tag(0x3002, PT_UNICODE)
PR_RECIPIENT_TYPE = prop_tag(0x0C15, PT_LONG)
PR_ROWID = prop_tag(0x3000, PT_LONG)

# Recipient types
MAPI_TO = 1
MAPI_CC = 2
MAPI_BCC = 3

# --- Attachment Properties ---
PR_ATTACH_NUM = prop_tag(0x0E21, PT_LONG)
PR_ATTACH_METHOD = prop_tag(0x3705, PT_LONG)
PR_ATTACH_FILENAME = prop_tag(0x3704, PT_UNICODE)
PR_ATTACH_LONG_FILENAME = prop_tag(0x3707, PT_UNICODE)
PR_ATTACH_SIZE = prop_tag(0x0E20, PT_LONG)
PR_ATTACH_DATA_BIN = prop_tag(0x3701, PT_BINARY)
PR_ATTACH_MIME_TAG = prop_tag(0x370E, PT_UNICODE)
PR_RENDERING_POSITION = prop_tag(0x370B, PT_LONG)

# Attachment methods
ATTACH_BY_VALUE = 1

# --- Common Entry ID / NID Properties ---
PR_ENTRYID = prop_tag(0x0FFF, PT_BINARY)
PR_PARENT_ENTRYID = prop_tag(0x0E09, PT_BINARY)
PR_CHANGE_KEY = prop_tag(0x65E2, PT_BINARY)

# --- Message Flags ---
MSGFLAG_READ = 0x0001
MSGFLAG_UNMODIFIED = 0x0002
MSGFLAG_HASATTACH = 0x0010

# --- Store Support Mask ---
STORE_ENTRYID_UNIQUE = 0x00000001
STORE_READONLY = 0x00000002
STORE_SEARCH_OK = 0x00000004
STORE_MODIFY_OK = 0x00000008
STORE_CREATE_OK = 0x00000010
STORE_ATTACH_OK = 0x00000020
STORE_OLE_OK = 0x00000040
STORE_UNICODE_OK = 0x00040000

# Default store support mask for a writable PST
DEFAULT_STORE_SUPPORT_MASK = (
    STORE_ENTRYID_UNIQUE | STORE_SEARCH_OK | STORE_MODIFY_OK |
    STORE_CREATE_OK | STORE_ATTACH_OK | STORE_OLE_OK | STORE_UNICODE_OK
)

# Valid folder mask bits
FOLDER_IPM_SUBTREE_VALID = 0x00000001
FOLDER_IPM_INBOX_VALID = 0x00000002
FOLDER_IPM_OUTBOX_VALID = 0x00000004
FOLDER_IPM_WASTEBASKET_VALID = 0x00000008
FOLDER_IPM_SENTMAIL_VALID = 0x00000010
FOLDER_IPM_VIEWS_VALID = 0x00000020
FOLDER_COMMON_VIEWS_VALID = 0x00000040
FOLDER_FINDER_VALID = 0x00000080

# --- NID Types ---
NID_TYPE_NONE = 0x00
NID_TYPE_INTERNAL = 0x01
NID_TYPE_NORMAL_FOLDER = 0x02
NID_TYPE_SEARCH_FOLDER = 0x03
NID_TYPE_NORMAL_MESSAGE = 0x04
NID_TYPE_ATTACHMENT = 0x05
NID_TYPE_SEARCH_UPDATE_QUEUE = 0x06
NID_TYPE_SEARCH_CRITERIA_OBJECT = 0x07
NID_TYPE_ASSOC_MESSAGE = 0x08
NID_TYPE_CONTENTS_TABLE_INDEX = 0x0A
NID_TYPE_RECEIVE_FOLDER_TABLE = 0x0B
NID_TYPE_OUTGOING_QUEUE_TABLE = 0x0C
NID_TYPE_HIERARCHY_TABLE = 0x0D
NID_TYPE_CONTENTS_TABLE = 0x0E
NID_TYPE_ASSOC_CONTENTS_TABLE = 0x0F
NID_TYPE_SEARCH_CONTENTS_TABLE = 0x10
NID_TYPE_ATTACHMENT_TABLE = 0x11
NID_TYPE_RECIPIENT_TABLE = 0x12
NID_TYPE_SEARCH_TABLE_INDEX = 0x13
NID_TYPE_LTP = 0x1F

# --- Special Internal NIDs ---
NID_MESSAGE_STORE = 0x21  # type=INTERNAL, index=1
NID_NAME_TO_ID_MAP = 0x61  # type=INTERNAL, index=3
NID_NORMAL_FOLDER_TEMPLATE = 0xA1  # type=INTERNAL, index=5
NID_SEARCH_FOLDER_TEMPLATE = 0xC1  # type=INTERNAL, index=6
NID_ROOT_FOLDER = 0x122  # type=NORMAL_FOLDER, index=9
NID_SEARCH_MANAGEMENT_QUEUE = 0x1E1  # type=INTERNAL
NID_SEARCH_ACTIVITY_LIST = 0x201  # type=INTERNAL
NID_SEARCH_DOMAIN_OBJECT = 0x261  # type=INTERNAL
NID_SEARCH_GATHERER_QUEUE = 0x281  # type=INTERNAL
NID_SEARCH_GATHERER_DESCRIPTOR = 0x2A1  # type=INTERNAL
NID_SEARCH_GATHERER_FOLDER_QUEUE = 0x321  # type=INTERNAL


def make_nid(nid_type, nid_index):
    return (nid_index << 5) | nid_type


def nid_type(nid):
    return nid & 0x1F


def nid_index(nid):
    return (nid >> 5) & 0x7FFFFFF


# --- LTP Internal Properties (for TC row index) ---
PidTagLtpRowId = prop_tag(0x67F2, PT_LONG)   # Required first TCOLDESC: dwRowID at offset 0
PidTagLtpRowVer = prop_tag(0x67F3, PT_LONG)   # Optional row version

# --- Table Context Column Descriptors (for TC) ---
# Columns for Hierarchy Table (folder's subfolder list)
HIERARCHY_TC_COLUMNS = [
    # (prop_tag, offset_in_row, size, iBit_index)
    (PR_DISPLAY_NAME, PT_UNICODE),
    (PR_CONTENT_COUNT, PT_LONG),
    (PR_CONTENT_UNREAD_COUNT, PT_LONG),
    (PR_SUBFOLDERS, PT_BOOLEAN),
    (PR_CONTAINER_CLASS, PT_UNICODE),
]

# Columns for Contents Table (folder's message list)
CONTENTS_TC_COLUMNS = [
    (PR_SUBJECT, PT_UNICODE),
    (PR_MESSAGE_CLASS, PT_UNICODE),
    (PR_MESSAGE_FLAGS, PT_LONG),
    (PR_MESSAGE_SIZE, PT_LONG),
    (PR_MESSAGE_DELIVERY_TIME, PT_SYSTIME),
    (PR_IMPORTANCE, PT_LONG),
    (PR_SENSITIVITY, PT_LONG),
    (PR_HASATTACH, PT_BOOLEAN),
    (PR_SENDER_NAME, PT_UNICODE),
]
