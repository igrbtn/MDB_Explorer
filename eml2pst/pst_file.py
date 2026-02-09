"""PST file assembly — orchestrates all layers to produce a valid .pst file.

This module ties together NDB, LTP, and Messaging layers to write
a complete Unicode PST file from in-memory data.
"""

import struct
from pathlib import Path

from .crc import compute_crc
from .ndb.header import build_header, pack_root, HEADER_SIZE
from .ndb.block import pack_block, block_total_size
from .ndb.btree import (
    build_btree_pages, pack_nbt_entry, pack_bbt_entry,
    PTTYPE_NBT, PTTYPE_BBT, PAGE_SIZE,
)
from .ndb.amap import build_amap_page, FIRST_AMAP_OFFSET, compute_amap_free
from .ndb.subnode import build_sl_block
from .messaging.store import build_message_store, build_name_to_id_map
from .messaging.folder import (
    build_folder_pc, build_hierarchy_tc, build_contents_tc,
    build_assoc_contents_tc, folder_nid_hierarchy,
    folder_nid_contents, folder_nid_assoc,
)
from .messaging.message import (
    build_message_pc, build_recipients_tc, build_attachments_tc,
    message_nid_recipients, message_nid_attachments,
)
from .mapi.properties import (
    NID_MESSAGE_STORE, NID_NAME_TO_ID_MAP, NID_ROOT_FOLDER,
    NID_TYPE_NORMAL_FOLDER, NID_TYPE_NORMAL_MESSAGE,
    NID_TYPE_INTERNAL,
    PR_DISPLAY_NAME, PR_CONTENT_COUNT, PR_CONTENT_UNREAD_COUNT,
    PR_SUBFOLDERS,
    PR_SUBJECT, PR_MESSAGE_CLASS, PR_MESSAGE_FLAGS,
    PR_MESSAGE_SIZE, PR_MESSAGE_DELIVERY_TIME,
    PR_IMPORTANCE, PR_HASATTACH, PR_SENDER_NAME,
    PR_SENSITIVITY,
    MSGFLAG_READ, MSGFLAG_HASATTACH,
    make_nid,
)
from .ltp.pc import build_pc_node


class PSTFileBuilder:
    """Builds a complete PST file from folders and messages."""

    def __init__(self, display_name="Personal Folders"):
        self.display_name = display_name
        self._next_nid_index = 32  # Start after reserved NIDs
        # BID bit 1 (i-bit) distinguishes leaf (0) from internal (1) blocks.
        # Leaf BIDs (data blocks): 4, 8, 12, 16, ... (BID % 4 == 0)
        # Internal BIDs (SLBLOCK etc): 6, 10, 14, 18, ... (BID % 4 == 2)
        self._next_leaf_bid = 4
        self._next_internal_bid = 6
        self._next_page_bid = 5  # Odd BIDs are for pages

        # Collected nodes: list of (nid, data_bid, subnode_bid, parent_nid)
        self._nodes = []
        # Collected data blocks: list of (bid, data_bytes)
        self._data_blocks = []
        # Folder structure for building hierarchy
        self._folders = {}  # nid -> {name, parent_nid, subfolder_nids, message_nids}
        self._messages = {}  # msg_nid -> parsed_eml dict
        self._root_nid = NID_ROOT_FOLDER
        # Pre-initialize root folder so add_folder() can register subfolders
        self._folders[self._root_nid] = {
            'name': 'Top of Personal Folders',
            'parent_nid': self._root_nid,  # Root folder is self-referential
            'subfolder_nids': [],
            'message_nids': [],
        }

    def _alloc_data_bid(self):
        """Allocate a leaf data block BID (i-bit = 0, BID % 4 == 0)."""
        bid = self._next_leaf_bid
        self._next_leaf_bid += 4
        return bid

    def _alloc_internal_bid(self):
        """Allocate an internal block BID (i-bit = 1, BID % 4 == 2)."""
        bid = self._next_internal_bid
        self._next_internal_bid += 4
        return bid

    def _alloc_page_bid(self):
        """Allocate a page BID (odd number)."""
        bid = self._next_page_bid
        if bid % 2 == 0:
            bid += 1
        self._next_page_bid = bid + 2
        return bid

    def _alloc_nid(self, nid_type):
        """Allocate a new NID of the given type."""
        idx = self._next_nid_index
        self._next_nid_index += 1
        return make_nid(nid_type, idx)

    def _store_data_block(self, data):
        """Store data as a leaf block (i-bit=0), return the BID."""
        bid = self._alloc_data_bid()
        self._data_blocks.append((bid, data))
        return bid

    def _store_internal_block(self, data):
        """Store data as an internal block (i-bit=1, for SLBLOCK/XBLOCK), return the BID."""
        bid = self._alloc_internal_bid()
        self._data_blocks.append((bid, data))
        return bid

    def _add_node(self, nid, data, sub_bid=0, parent_nid=0):
        """Add a node with its data block."""
        bid = self._store_data_block(data)
        self._nodes.append((nid, bid, sub_bid, parent_nid))
        return bid

    def add_folder(self, name, parent_nid=None):
        """Add a folder to the PST.

        Args:
            name: Folder display name.
            parent_nid: Parent folder NID (None = root).

        Returns:
            NID of the new folder.
        """
        if parent_nid is None:
            parent_nid = self._root_nid

        folder_nid = self._alloc_nid(NID_TYPE_NORMAL_FOLDER)

        self._folders[folder_nid] = {
            'name': name,
            'parent_nid': parent_nid,
            'subfolder_nids': [],
            'message_nids': [],
        }

        # Register as subfolder of parent
        if parent_nid in self._folders:
            self._folders[parent_nid]['subfolder_nids'].append(folder_nid)

        return folder_nid

    def add_message(self, folder_nid, parsed_eml):
        """Add a message to a folder.

        Args:
            folder_nid: NID of the target folder.
            parsed_eml: Dict from eml_parser.parse_eml_file().

        Returns:
            NID of the new message.
        """
        msg_nid = self._alloc_nid(NID_TYPE_NORMAL_MESSAGE)

        if folder_nid in self._folders:
            self._folders[folder_nid]['message_nids'].append(msg_nid)

        # Store parsed EML for Contents TC row building
        self._messages[msg_nid] = parsed_eml

        # Build message PC data
        msg_data = build_message_pc(parsed_eml)

        # Build subnodes (recipients TC, attachments TC) as SLBLOCK
        sl_entries = []

        if parsed_eml.get('recipients'):
            recip_data = build_recipients_tc(parsed_eml['recipients'])
            recip_bid = self._store_data_block(recip_data)
            recip_nid = message_nid_recipients(msg_nid)
            sl_entries.append((recip_nid, recip_bid, 0))

        if parsed_eml.get('attachments'):
            attach_data = build_attachments_tc(parsed_eml['attachments'])
            attach_bid = self._store_data_block(attach_data)
            attach_nid = message_nid_attachments(msg_nid)
            sl_entries.append((attach_nid, attach_bid, 0))

            # Store attachment data as separate blocks
            for att in parsed_eml['attachments']:
                if att.get('data'):
                    self._store_data_block(att['data'])

        # Create SLBLOCK if there are subnodes (must use internal BID)
        sub_bid = 0
        if sl_entries:
            sl_data = build_sl_block(sl_entries)
            sub_bid = self._store_internal_block(sl_data)

        # Add message node with bidSub pointing to SLBLOCK
        self._add_node(msg_nid, msg_data, sub_bid=sub_bid, parent_nid=folder_nid)

        return msg_nid

    def _build_internal_nodes(self):
        """Build the required internal nodes (store, root folder, name map)."""
        # 1. Message Store (NID 0x21)
        store_data, self._record_key = build_message_store(self.display_name)
        self._add_node(NID_MESSAGE_STORE, store_data)

        # 2. Name-to-ID Map (NID 0x61) — required structure with GUID/entry/string streams
        namemap_data = build_name_to_id_map()
        self._add_node(NID_NAME_TO_ID_MAP, namemap_data)

    def _build_folder_nodes(self):
        """Build all folder nodes (PC + 3 TCs as separate top-level NBT entries)."""
        for folder_nid, finfo in self._folders.items():
            has_subs = len(finfo['subfolder_nids']) > 0
            msg_count = len(finfo['message_nids'])

            # Folder PC
            pc_data = build_folder_pc(
                finfo['name'],
                content_count=msg_count,
                has_subfolders=has_subs,
            )

            # Build the 3 TC data blocks (separate top-level NBT entries)
            # Hierarchy TC (subfolder list)
            sub_rows = []
            for sub_nid in finfo['subfolder_nids']:
                sub_info = self._folders.get(sub_nid, {})
                sub_rows.append({
                    '_nid': sub_nid,
                    PR_DISPLAY_NAME: sub_info.get('name', ''),
                    PR_CONTENT_COUNT: len(sub_info.get('message_nids', [])),
                    PR_CONTENT_UNREAD_COUNT: 0,
                    PR_SUBFOLDERS: len(sub_info.get('subfolder_nids', [])) > 0,
                })
            hier_nid = folder_nid_hierarchy(folder_nid)
            hier_data = build_hierarchy_tc(sub_rows)
            hier_bid = self._store_data_block(hier_data)

            # Contents TC (message list)
            msg_rows = []
            for msg_nid in finfo['message_nids']:
                parsed = self._messages.get(msg_nid, {})
                flags = MSGFLAG_READ
                if parsed.get('has_attachments', False):
                    flags |= MSGFLAG_HASATTACH
                row = {
                    '_nid': msg_nid,
                    PR_SUBJECT: parsed.get('subject', ''),
                    PR_MESSAGE_CLASS: parsed.get('message_class', 'IPM.Note'),
                    PR_MESSAGE_FLAGS: flags,
                    PR_MESSAGE_SIZE: parsed.get('message_size', 0),
                    PR_IMPORTANCE: parsed.get('importance', 1),
                    PR_HASATTACH: parsed.get('has_attachments', False),
                    PR_SENDER_NAME: parsed.get('sender_name', ''),
                }
                if parsed.get('delivery_time'):
                    row[PR_MESSAGE_DELIVERY_TIME] = parsed['delivery_time']
                msg_rows.append(row)
            contents_nid = folder_nid_contents(folder_nid)
            contents_data = build_contents_tc(msg_rows)
            contents_bid = self._store_data_block(contents_data)

            # Associated Contents TC (empty)
            assoc_nid = folder_nid_assoc(folder_nid)
            assoc_data = build_assoc_contents_tc()
            assoc_bid = self._store_data_block(assoc_data)

            # Add folder PC node (no subnodes — TCs are separate NBT entries)
            self._add_node(folder_nid, pc_data, sub_bid=0,
                           parent_nid=finfo['parent_nid'])

            # Add 3 TC nodes as separate top-level NBT entries (as Outlook does)
            self._nodes.append((hier_nid, hier_bid, 0, 0))
            self._nodes.append((contents_nid, contents_bid, 0, 0))
            self._nodes.append((assoc_nid, assoc_bid, 0, 0))

    def write(self, output_path):
        """Assemble and write the complete PST file.

        Args:
            output_path: Path for the output .pst file.
        """
        output_path = Path(output_path)

        # Build internal structures
        self._build_internal_nodes()
        self._build_folder_nodes()

        # Phase 1: Assign file offsets to all data blocks
        # Layout: [Header 0x4400] [AMap page 512] [data blocks...] [BTree pages...]
        current_offset = FIRST_AMAP_OFFSET + PAGE_SIZE  # After AMap page

        block_positions = {}  # bid -> (offset, raw_size)
        for bid, data in self._data_blocks:
            total = block_total_size(len(data))
            block_positions[bid] = (current_offset, len(data))
            current_offset += total

        # Align to page boundary for B-tree pages
        if current_offset % PAGE_SIZE != 0:
            current_offset = ((current_offset + PAGE_SIZE - 1) // PAGE_SIZE) * PAGE_SIZE

        # Phase 2: Build NBT entries
        nbt_entries = []
        for nid, data_bid, sub_bid, parent_nid in self._nodes:
            entry = pack_nbt_entry(nid, data_bid, sub_bid, parent_nid)
            nbt_entries.append(entry)
        nbt_entries.sort(key=lambda e: struct.unpack('<Q', e[:8])[0])

        # Phase 3: Build BBT entries
        bbt_entries = []
        for bid, data in self._data_blocks:
            if bid in block_positions:
                offset, raw_size = block_positions[bid]
                entry = pack_bbt_entry(bid, offset, raw_size)
                bbt_entries.append(entry)
        bbt_entries.sort(key=lambda e: struct.unpack('<Q', e[:8])[0])

        # Phase 4: Build B-tree pages (potentially multi-level)
        page_offset_cursor = [current_offset]  # mutable for closure
        page_offsets = {}  # bid -> offset

        def alloc_page_offset(bid):
            offset = page_offset_cursor[0]
            page_offsets[bid] = offset
            page_offset_cursor[0] += PAGE_SIZE
            return offset

        nbt_pages = build_btree_pages(
            nbt_entries, PTTYPE_NBT,
            self._alloc_page_bid, alloc_page_offset
        )
        bbt_pages = build_btree_pages(
            bbt_entries, PTTYPE_BBT,
            self._alloc_page_bid, alloc_page_offset
        )

        # Root pages are the last in each list
        nbt_root_bid, nbt_root_offset, _ = nbt_pages[-1]
        bbt_root_bid, bbt_root_offset, _ = bbt_pages[-1]

        file_eof = page_offset_cursor[0]

        # Phase 5: Build AMap
        allocated_ranges = []
        allocated_ranges.append((0, FIRST_AMAP_OFFSET))  # Header area
        for bid, data in self._data_blocks:
            if bid in block_positions:
                offset, raw_size = block_positions[bid]
                total = block_total_size(raw_size)
                allocated_ranges.append((offset, total))
        for _, pg_offset, _ in nbt_pages + bbt_pages:
            allocated_ranges.append((pg_offset, PAGE_SIZE))

        amap_bid = self._alloc_page_bid()
        amap_page = build_amap_page(
            allocated_ranges, FIRST_AMAP_OFFSET,
            FIRST_AMAP_OFFSET, amap_bid
        )
        amap_free = compute_amap_free(
            allocated_ranges, FIRST_AMAP_OFFSET, FIRST_AMAP_OFFSET
        )

        # Phase 6: Build header
        root = pack_root(
            file_eof=file_eof,
            ib_amap_last=FIRST_AMAP_OFFSET,
            cb_amap_free=amap_free,
            cb_pmap_free=0,
            bref_nbt=(nbt_root_bid, nbt_root_offset),
            bref_bbt=(bbt_root_bid, bbt_root_offset),
            f_amap_valid=1,
        )

        header = build_header(
            root_data=root,
            bid_next_p=self._next_page_bid,
            bid_next_b=max(self._next_leaf_bid, self._next_internal_bid),
            unique=1,
        )

        # Phase 7: Write everything to file
        with open(output_path, 'wb') as f:
            # Header (padded to FIRST_AMAP_OFFSET)
            f.write(header)
            f.write(b'\x00' * (FIRST_AMAP_OFFSET - len(header)))

            # AMap page
            f.write(amap_page)

            # Data blocks (in order of offset)
            sorted_blocks = sorted(
                [(bid, data) for bid, data in self._data_blocks if bid in block_positions],
                key=lambda x: block_positions[x[0]][0]
            )

            expected_offset = FIRST_AMAP_OFFSET + PAGE_SIZE
            for bid, data in sorted_blocks:
                offset, _ = block_positions[bid]
                if offset > expected_offset:
                    f.write(b'\x00' * (offset - expected_offset))
                packed = pack_block(data, bid, offset)
                f.write(packed)
                expected_offset = offset + len(packed)

            # B-tree pages (in order of offset)
            all_bt_pages = sorted(
                nbt_pages + bbt_pages,
                key=lambda x: x[1]  # sort by offset
            )

            for pg_bid, pg_offset, pg_data in all_bt_pages:
                if pg_offset > expected_offset:
                    f.write(b'\x00' * (pg_offset - expected_offset))
                f.write(pg_data)
                expected_offset = pg_offset + len(pg_data)

        return output_path
