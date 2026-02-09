"""Message Store node (NID 0x21) and Name-to-ID Map (NID 0x61).

The Message Store is the root object of a PST file.
The Name-to-ID Map maps named properties to numeric IDs.

See [MS-PST] 2.4.3 and 2.4.7.
"""

import os
import struct
from ..ltp.pc import build_pc_node
from ..mapi.properties import (
    NID_MESSAGE_STORE, NID_ROOT_FOLDER,
    PR_RECORD_KEY, PR_DISPLAY_NAME,
    PR_IPM_SUBTREE_ENTRYID,
    PR_STORE_SUPPORT_MASK, PR_VALID_FOLDER_MASK,
    PR_PST_PASSWORD,
    DEFAULT_STORE_SUPPORT_MASK,
    FOLDER_IPM_SUBTREE_VALID,
    PT_LONG, PT_BINARY, prop_tag,
)


def make_entry_id(record_key, nid):
    """Create a PST entry ID from a record key and NID.

    PST entry IDs are 24 bytes:
    - 4 bytes flags (0)
    - 16 bytes provider UID (must match store's PR_RECORD_KEY)
    - 4 bytes NID
    """
    return struct.pack('<I', 0) + record_key[:16] + struct.pack('<I', nid)


def build_message_store(display_name="Personal Folders"):
    """Build the Message Store Property Context data block.

    Returns:
        Tuple of (raw_bytes, record_key) where record_key is the
        16-byte store UID used in entry IDs.
    """
    record_key = os.urandom(16)

    properties = [
        (PR_RECORD_KEY, record_key),
        (PR_DISPLAY_NAME, display_name),
        (PR_IPM_SUBTREE_ENTRYID, make_entry_id(record_key, NID_ROOT_FOLDER)),
        (PR_STORE_SUPPORT_MASK, DEFAULT_STORE_SUPPORT_MASK),
        (PR_VALID_FOLDER_MASK, FOLDER_IPM_SUBTREE_VALID),
        (PR_PST_PASSWORD, 0),
    ]

    return build_pc_node(properties), record_key


def build_name_to_id_map():
    """Build the Name-to-ID Map Property Context (NID 0x61).

    See [MS-PST] 2.4.7.
    """
    PS_MAPI = bytes([
        0x28, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
        0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46,
    ])
    PS_PUBLIC_STRINGS = bytes([
        0x29, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
        0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46,
    ])

    guid_stream = PS_MAPI + PS_PUBLIC_STRINGS

    properties = [
        (prop_tag(0x0001, PT_LONG), 251),  # Bucket count
        (prop_tag(0x0002, PT_BINARY), guid_stream),  # GUID stream
        (prop_tag(0x0003, PT_BINARY), b'\x00' * 8),  # Entry stream (minimal)
        (prop_tag(0x0004, PT_BINARY), b'\x00' * 4),  # String stream (minimal)
    ]

    return build_pc_node(properties)
