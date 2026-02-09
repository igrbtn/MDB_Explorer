"""Table Context (TC) implementation.

A TC is a 2D table (rows x columns) stored in a Heap-on-Node.
Used for folder hierarchy, contents, recipients, and attachments.

See [MS-PST] 2.3.4.
"""

import struct
from .heap import HeapOnNode, HN_CLIENT_TC
from ..mapi.properties import (
    prop_id, prop_type, is_fixed_type, fixed_size,
    PT_LONG, PT_SHORT, PT_BOOLEAN, PT_SYSTIME, PT_UNICODE, PT_STRING8,
    PT_BINARY, PT_LONG_LONG,
    PidTagLtpRowId,
)

# TCINFO header
# bType(1) + cCols(1) + rgib[4](2 each=8) + hidRowIndex(4) + hnidRows(4) + hidIndex(4)
# = 1 + 1 + 8 + 4 + 4 + 4 = 22 bytes, then TCOLDESC array follows
TCINFO_FIXED_SIZE = 22

# TCOLDESC: tag(4) + ibData(2) + cbData(1) + iBit(1) = 8 bytes
TCOLDESC_SIZE = 8


def _column_storage_size(ptype):
    """Get the storage size for a column in the row data."""
    if is_fixed_type(ptype):
        return fixed_size(ptype)
    else:
        # Variable-size columns store a 4-byte HNID in the fixed area
        return 4


def build_tc_node(column_defs, rows):
    """Build a Table Context data block.

    Args:
        column_defs: List of property tags defining the columns.
        rows: List of dicts mapping prop_tag → value for each row.
              Values should be the appropriate Python type.

    Returns:
        Raw bytes for the TC data block.
    """
    hn = HeapOnNode(client_sig=HN_CLIENT_TC)

    # Filter out PidTagLtpRowId if caller passed it — we always add it ourselves
    column_defs = [t for t in column_defs if t != PidTagLtpRowId]
    num_user_cols = len(column_defs)

    # Total columns = PidTagLtpRowId + user columns
    total_cols = num_user_cols + 1

    # CEB (Cell Existence Bitmap) size: ceil(total_cols / 8) bytes
    ceb_size = (total_cols + 7) // 8

    # Sort user columns into type groups per [MS-PST] 2.3.4.2:
    # Group 1: Fixed >=4-byte columns (PT_LONG=4, PT_BOOLEAN=4, PT_SYSTIME=8, etc.)
    # Group 2: Fixed 2-byte columns (PT_SHORT)
    # Group 3: Fixed 1-byte columns (none currently used)
    # Group 4: Variable-size columns (PT_UNICODE, PT_STRING8, PT_BINARY as 4-byte HNID)
    group_4b = []  # fixed >= 4 bytes
    group_2b = []  # fixed 2 bytes
    group_1b = []  # fixed 1 byte
    group_var = []  # variable-size (stored as 4-byte HNID)

    for tag in column_defs:
        ptype = prop_type(tag)
        if is_fixed_type(ptype):
            sz = fixed_size(ptype)
            if sz >= 4:
                group_4b.append(tag)
            elif sz == 2:
                group_2b.append(tag)
            else:
                group_1b.append(tag)
        else:
            group_var.append(tag)

    # Sort each group by tag for deterministic ordering
    group_4b.sort()
    group_2b.sort()
    group_1b.sort()
    group_var.sort()

    # Build complete column list:
    # First entry MUST be PidTagLtpRowId at offset 0 (describes dwRowID)
    # Then sorted user columns starting at offset 4
    columns = [{
        'tag': PidTagLtpRowId,
        'ib_data': 0,
        'cb_data': 4,
        'i_bit': 0,
    }]

    sorted_tags = group_4b + group_2b + group_1b + group_var
    current_offset = 4  # User columns start after dwRowID (4 bytes)

    # Track group boundary offsets
    offset_after_4b = 4
    offset_after_2b = 4
    offset_after_1b = 4

    for i, tag in enumerate(sorted_tags):
        ptype = prop_type(tag)
        cb = _column_storage_size(ptype)
        columns.append({
            'tag': tag,
            'ib_data': current_offset,
            'cb_data': cb,
            'i_bit': i + 1,  # +1 because iBit 0 is PidTagLtpRowId
        })
        current_offset += cb

        if i < len(group_4b):
            offset_after_4b = current_offset
        elif i < len(group_4b) + len(group_2b):
            offset_after_2b = current_offset
        elif i < len(group_4b) + len(group_2b) + len(group_1b):
            offset_after_1b = current_offset

    # If a group is empty, its boundary equals the previous group's boundary
    if not group_2b:
        offset_after_2b = offset_after_4b
    if not group_1b:
        offset_after_1b = offset_after_2b

    # rgib boundaries per [MS-PST] 2.3.4.2:
    # rgib[TCI_4b] = start of >=4-byte fixed group (always 0 — includes dwRowID)
    # rgib[TCI_2b] = end of >=4-byte group
    # rgib[TCI_1b] = end of 2-byte group
    # rgib[TCI_bm] = end of CEB = total row stride (libpff uses this as row size)
    rgib_0 = 0
    rgib_1 = offset_after_4b
    rgib_2 = offset_after_2b
    rgib_3 = current_offset + ceb_size  # Total row size including CEB

    # Row size = rgib[TCI_bm] (libpff uses rgib_3 directly as the row stride)
    row_size = rgib_3

    # Build TCOLDESC array (PidTagLtpRowId first, then user columns)
    coldesc_data = b''
    for col in columns:
        coldesc_data += struct.pack('<I H BB',
                                    col['tag'],
                                    col['ib_data'],
                                    col['cb_data'],
                                    col['i_bit'])

    # Build row data
    all_row_data = b''
    for row_idx, row in enumerate(rows):
        row_bytes = bytearray(row_size)

        # dwRowID at offset 0 (4 bytes) - use row index as ID
        nid_value = row.get('_nid', row_idx)
        struct.pack_into('<I', row_bytes, 0, nid_value)

        # CEB: mark PidTagLtpRowId (iBit 0) as always present
        ceb = bytearray(ceb_size)
        ceb[0] |= (1 << 7)  # iBit 0 → byte 0, bit 7

        # Fill in user column values
        for col in columns[1:]:  # Skip PidTagLtpRowId (already written as dwRowID)
            tag = col['tag']
            if tag not in row:
                continue

            value = row[tag]
            ptype = prop_type(tag)
            offset = col['ib_data']

            # Mark column as present in CEB
            bit_idx = col['i_bit']
            ceb[bit_idx // 8] |= (1 << (7 - (bit_idx % 8)))

            if is_fixed_type(ptype):
                if ptype == PT_LONG:
                    struct.pack_into('<I', row_bytes, offset, value & 0xFFFFFFFF)
                elif ptype == PT_SHORT:
                    struct.pack_into('<H', row_bytes, offset, value & 0xFFFF)
                elif ptype == PT_BOOLEAN:
                    struct.pack_into('<I', row_bytes, offset, 1 if value else 0)
                elif ptype == PT_SYSTIME:
                    struct.pack_into('<Q', row_bytes, offset, value)
                elif ptype == PT_LONG_LONG:
                    struct.pack_into('<Q', row_bytes, offset, value)
            else:
                # Variable-size: allocate on heap, store HID
                if ptype == PT_UNICODE:
                    heap_data = value.encode('utf-16-le') if isinstance(value, str) else value
                elif ptype == PT_STRING8:
                    heap_data = value.encode('utf-8') if isinstance(value, str) else value
                elif ptype == PT_BINARY:
                    heap_data = value if isinstance(value, bytes) else b''
                else:
                    heap_data = value if isinstance(value, bytes) else b''

                hid = hn.allocate(heap_data)
                struct.pack_into('<I', row_bytes, offset, hid)

        # Write CEB at end of column data (offset = current_offset, before rgib_3 boundary)
        ceb_offset = current_offset
        row_bytes[ceb_offset:ceb_offset + ceb_size] = ceb
        all_row_data += bytes(row_bytes)

    # Allocate row data on heap (or store as HNID)
    if all_row_data:
        rows_hid = hn.allocate(all_row_data)
    else:
        rows_hid = 0

    # Build Row Index BTH: maps dwRowID (4 bytes) → row index (4 bytes)
    # Required by [MS-PST] 2.3.4.3 for row lookup
    row_index_hid = 0
    if rows:
        # Collect (dwRowID, row_index) pairs, sorted by dwRowID
        ri_pairs = []
        for row_idx, row in enumerate(rows):
            nid_value = row.get('_nid', row_idx)
            ri_pairs.append((nid_value, row_idx))
        ri_pairs.sort(key=lambda x: x[0])

        # Pack leaf entries: key(4) + data(4) per entry
        ri_leaf_data = b''
        for rid, ridx in ri_pairs:
            ri_leaf_data += struct.pack('<I', rid) + struct.pack('<I', ridx)

        ri_leaf_hid = hn.allocate(ri_leaf_data)

        # BTHHEADER: bType(1)=0xB5, cbKey(1)=4, cbEnt(1)=4, bIdxLevels(1)=0, hidRoot(4)
        ri_bth_header = struct.pack('<BB BB I',
                                     0xB5, 4, 4, 0, ri_leaf_hid)
        row_index_hid = hn.allocate(ri_bth_header)

    # Build TCINFO header
    tcinfo = struct.pack('<BB HHHH I I I',
                         0x7C,  # bType = TC
                         total_cols,  # cCols (includes PidTagLtpRowId)
                         rgib_0,  # rgib[TCI_4b]
                         rgib_1,  # rgib[TCI_2b]
                         rgib_2,  # rgib[TCI_1b]
                         rgib_3,  # rgib[TCI_bm]
                         row_index_hid,  # hidRowIndex
                         rows_hid,  # hnidRows
                         0)  # hidIndex (deprecated)

    tcinfo_data = tcinfo + coldesc_data

    # Allocate TCINFO on heap
    tcinfo_hid = hn.allocate(tcinfo_data)
    hn.set_user_root(tcinfo_hid)

    return hn.build()
