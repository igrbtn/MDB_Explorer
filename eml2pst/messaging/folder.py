"""Folder objects for PST Messaging layer.

Each folder has 4 nodes:
- Folder PC (nid): Property Context with folder properties
- Hierarchy TC (nid | 0x0D): Table Context listing subfolders
- Contents TC (nid | 0x0E): Table Context listing messages
- Associated Contents TC (nid | 0x0F): Table Context for FAI messages

See [MS-PST] 2.4.4.
"""

from ..ltp.pc import build_pc_node
from ..ltp.tc import build_tc_node
from ..mapi.properties import (
    NID_TYPE_HIERARCHY_TABLE, NID_TYPE_CONTENTS_TABLE,
    NID_TYPE_ASSOC_CONTENTS_TABLE,
    PR_DISPLAY_NAME, PR_CONTENT_COUNT, PR_CONTENT_UNREAD_COUNT,
    PR_SUBFOLDERS, PR_CONTAINER_CLASS,
    PR_CREATION_TIME, PR_LAST_MODIFICATION_TIME,
    PR_SUBJECT, PR_MESSAGE_CLASS, PR_MESSAGE_FLAGS,
    PR_MESSAGE_SIZE, PR_MESSAGE_DELIVERY_TIME,
    PR_IMPORTANCE, PR_SENSITIVITY, PR_HASATTACH, PR_SENDER_NAME,
)
from ..utils import filetime_now


def build_folder_pc(display_name, content_count=0, has_subfolders=False,
                    container_class="IPF.Note"):
    """Build the folder Property Context.

    Returns:
        Raw bytes for folder PC data block.
    """
    now = filetime_now()
    properties = [
        (PR_DISPLAY_NAME, display_name),
        (PR_CONTENT_COUNT, content_count),
        (PR_CONTENT_UNREAD_COUNT, 0),
        (PR_SUBFOLDERS, has_subfolders),
        (PR_CONTAINER_CLASS, container_class),
        (PR_CREATION_TIME, now),
        (PR_LAST_MODIFICATION_TIME, now),
    ]
    return build_pc_node(properties)


def build_hierarchy_tc(subfolder_rows=None):
    """Build the Hierarchy Table Context (subfolder list).

    Args:
        subfolder_rows: List of dicts with subfolder properties.
            Each dict should have: _nid, PR_DISPLAY_NAME, etc.

    Returns:
        Raw bytes for hierarchy TC data block.
    """
    column_tags = [
        PR_DISPLAY_NAME,
        PR_CONTENT_COUNT,
        PR_CONTENT_UNREAD_COUNT,
        PR_SUBFOLDERS,
    ]
    rows = subfolder_rows or []
    return build_tc_node(column_tags, rows)


def build_contents_tc(message_rows=None):
    """Build the Contents Table Context (message list).

    Args:
        message_rows: List of dicts with message summary properties.
            Each dict should have: _nid, PR_SUBJECT, PR_MESSAGE_CLASS, etc.

    Returns:
        Raw bytes for contents TC data block.
    """
    column_tags = [
        PR_SUBJECT,
        PR_MESSAGE_CLASS,
        PR_MESSAGE_FLAGS,
        PR_MESSAGE_SIZE,
        PR_MESSAGE_DELIVERY_TIME,
        PR_IMPORTANCE,
        PR_HASATTACH,
        PR_SENDER_NAME,
    ]
    rows = message_rows or []
    return build_tc_node(column_tags, rows)


def build_assoc_contents_tc():
    """Build an empty Associated Contents Table Context.

    Returns:
        Raw bytes for associated contents TC data block.
    """
    return build_tc_node([], [])


def folder_nid_hierarchy(folder_nid):
    """Get hierarchy TC NID for a folder."""
    return (folder_nid & ~0x1F) | NID_TYPE_HIERARCHY_TABLE


def folder_nid_contents(folder_nid):
    """Get contents TC NID for a folder."""
    return (folder_nid & ~0x1F) | NID_TYPE_CONTENTS_TABLE


def folder_nid_assoc(folder_nid):
    """Get associated contents TC NID for a folder."""
    return (folder_nid & ~0x1F) | NID_TYPE_ASSOC_CONTENTS_TABLE
