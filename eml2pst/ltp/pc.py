"""Property Context (PC) implementation.

A PC is a BTH that stores MAPI properties for an object (message, folder, store).
Each entry maps a property ID (2 bytes) to its value.

See [MS-PST] 2.3.3.
"""

import struct
from .bth import build_bth_data
from .heap import HeapOnNode, HN_CLIENT_PC
from ..mapi.properties import (
    prop_id, prop_type, is_fixed_type, fixed_size, is_variable_type,
    PT_LONG, PT_SHORT, PT_BOOLEAN, PT_SYSTIME, PT_LONG_LONG,
    PT_STRING8, PT_UNICODE, PT_BINARY, PT_GUID,
)


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
        Raw bytes for the data block (ready to be packed as a block).
    """
    hn = HeapOnNode(client_sig=HN_CLIENT_PC)

    # Separate into BTH entries (key=propID, data=encoded value reference)
    # For fixed-size properties: data is stored inline in the BTH entry
    # For variable-size: data is allocated on the heap, BTH stores HID
    #
    # BTH entry format for PC:
    #   key: wPropId (2 bytes) - property ID
    #   data: wPropType(2) + dwValueHnid(4) = 6 bytes
    #
    # For fixed-size props <= 4 bytes: dwValueHnid = inline value
    # For fixed-size props > 4 bytes: dwValueHnid = HID to heap allocation
    # For variable-size props: dwValueHnid = HID to heap allocation

    bth_entries = []

    for tag, value in sorted(properties, key=lambda x: prop_id(x[0])):
        pid = prop_id(tag)
        ptype = prop_type(tag)

        key = struct.pack('<H', pid)

        if is_fixed_type(ptype):
            fsize = fixed_size(ptype)
            if fsize <= 4:
                # Inline value in dwValueHnid
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
                # Allocate on heap
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
            # Encode and allocate on heap
            if ptype == PT_UNICODE:
                if isinstance(value, str):
                    heap_data = value.encode('utf-16-le')
                else:
                    heap_data = value
            elif ptype == PT_STRING8:
                if isinstance(value, str):
                    heap_data = value.encode('utf-8')
                else:
                    heap_data = value
            elif ptype == PT_BINARY:
                heap_data = value if isinstance(value, bytes) else bytes(value)
            else:
                heap_data = value if isinstance(value, bytes) else b''

            hid = hn.allocate(heap_data)
            data = struct.pack('<H I', ptype, hid)
        else:
            # Unknown type, store as binary
            heap_data = value if isinstance(value, bytes) else b''
            hid = hn.allocate(heap_data)
            data = struct.pack('<H I', ptype, hid)

        bth_entries.append((key, data))

    # Now build the BTH inside the HN
    if bth_entries:
        leaf_data = b''
        for key, value in bth_entries:
            leaf_data += key + value

        leaf_hid = hn.allocate(leaf_data)
    else:
        leaf_hid = 0

    # BTHHEADER
    bth_header = struct.pack('<BB BB I',
                             0xB5,  # bType
                             2,  # cbKey (propID = 2 bytes)
                             6,  # cbEnt (propType(2) + value(4) = 6 bytes)
                             0,  # bIdxLevels
                             leaf_hid)  # hidRoot

    header_hid = hn.allocate(bth_header)
    hn.set_user_root(header_hid)

    return hn.build()
