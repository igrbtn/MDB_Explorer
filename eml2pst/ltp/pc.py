"""Property Context (PC) implementation.

A PC is a BTH that stores MAPI properties for an object (message, folder, store).
Each entry maps a property ID (2 bytes) to its value.

See [MS-PST] 2.3.3.
"""

import struct
from .bth import build_bth_data
from .heap import HeapOnNode, HN_CLIENT_PC, MAX_HN_ALLOC
from ..mapi.properties import (
    prop_id, prop_type, is_fixed_type, fixed_size, is_variable_type,
    PT_LONG, PT_SHORT, PT_BOOLEAN, PT_SYSTIME, PT_LONG_LONG,
    PT_STRING8, PT_UNICODE, PT_BINARY, PT_GUID,
)

# NID type for property value subnodes (per [MS-PST] NID_TYPE_LTP = 0x1F)
_NID_TYPE_LTP = 0x1F


def build_pc_node(properties):
    """Build a Property Context data block.

    Args:
        properties: List of (prop_tag, value) tuples where value is
                   already the appropriate Python type:
                   - int for PT_LONG, PT_SHORT, PT_BOOLEAN, PT_LONG_LONG
                   - int (FILETIME) for PT_SYSTIME
                   - bytes for PT_BINARY, PT_GUID
                   - str for PT_STRING8, PT_UNICODE

    Returns:
        Tuple of (pages, subnodes) where:
            pages: List of bytes objects (HN pages, one per data block)
            subnodes: List of (nid, data_bytes) for values > MAX_HN_ALLOC
    """
    hn = HeapOnNode(client_sig=HN_CLIENT_PC)
    subnodes = []

    # BTH entry format for PC:
    #   key: wPropId (2 bytes) - property ID
    #   data: wPropType(2) + dwValueHnid(4) = 6 bytes
    #
    # dwValueHnid is either:
    #   - Inline value (fixed-size <= 4 bytes)
    #   - HID (heap allocation, low 5 bits = 0)
    #   - NID (subnode reference, low 5 bits != 0) for values > MAX_HN_ALLOC

    bth_entries = []

    for tag, value in sorted(properties, key=lambda x: prop_id(x[0])):
        pid = prop_id(tag)
        ptype = prop_type(tag)

        key = struct.pack('<H', pid)

        if is_fixed_type(ptype):
            fsize = fixed_size(ptype)
            if fsize <= 4:
                if ptype == PT_LONG:
                    dw = struct.pack('<I', value & 0xFFFFFFFF)
                elif ptype == PT_SHORT:
                    dw = struct.pack('<H', value & 0xFFFF) + b'\x00\x00'
                elif ptype == PT_BOOLEAN:
                    dw = struct.pack('<I', 1 if value else 0)
                else:
                    dw = struct.pack('<I', value & 0xFFFFFFFF)
                data = struct.pack('<H', ptype) + dw
            else:
                if ptype == PT_SYSTIME:
                    heap_data = struct.pack('<Q', value)
                elif ptype == PT_LONG_LONG:
                    heap_data = struct.pack('<Q', value)
                elif ptype == PT_GUID:
                    heap_data = value if isinstance(value, bytes) else bytes(16)
                else:
                    heap_data = struct.pack('<Q', value)
                hid = hn.allocate(heap_data)
                data = struct.pack('<H I', ptype, hid)
        elif is_variable_type(ptype):
            if ptype == PT_UNICODE:
                encoded = value.encode('utf-16-le') if isinstance(value, str) else value
            elif ptype == PT_STRING8:
                encoded = value.encode('utf-8') if isinstance(value, str) else value
            elif ptype == PT_BINARY:
                encoded = value if isinstance(value, bytes) else bytes(value)
            else:
                encoded = value if isinstance(value, bytes) else b''

            if len(encoded) > MAX_HN_ALLOC:
                # Too large for heap — store as subnode
                nid = (pid << 5) | _NID_TYPE_LTP
                subnodes.append((nid, encoded))
                data = struct.pack('<H I', ptype, nid)
            else:
                hid = hn.allocate(encoded)
                data = struct.pack('<H I', ptype, hid)
        else:
            encoded = value if isinstance(value, bytes) else b''
            if len(encoded) > MAX_HN_ALLOC:
                nid = (pid << 5) | _NID_TYPE_LTP
                subnodes.append((nid, encoded))
                data = struct.pack('<H I', ptype, nid)
            else:
                hid = hn.allocate(encoded)
                data = struct.pack('<H I', ptype, hid)

        bth_entries.append((key, data))

    if bth_entries:
        leaf_data = b''
        for key, value in bth_entries:
            leaf_data += key + value
        leaf_hid = hn.allocate(leaf_data)
    else:
        leaf_hid = 0

    bth_header = struct.pack('<BB BB I',
                             0xB5, 2, 6, 0, leaf_hid)
    header_hid = hn.allocate(bth_header)
    hn.set_user_root(header_hid)

    return hn.build(), subnodes
