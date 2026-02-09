"""PST file assembly — orchestrates all layers to produce a valid .pst file.

This module ties together NDB, LTP, and Messaging layers to write
a complete Unicode PST file from in-memory data.
"""

import struct
from pathlib import Path

from .crc import compute_crc
from .ndb.header import build_header, pack_root, HEADER_SIZE
from .ndb.block import pack_block, block_total_size, MAX_BLOCK_DATA
from .ndb.xblock import build_xblock
from .ndb.btree import (
    build_btree_pages, pack_nbt_entry, pack_bbt_entry,
    PTTYPE_NBT, PTTYPE_BBT, PAGE_SIZE,
)
from .ndb.amap import build_amap_page, FIRST_AMAP_OFFSET, AMAP_COVERAGE, compute_amap_free
from .ndb.subnode import build_sl_block
from .messaging.store import build_message_store, build_name_to_id_map
from .messaging.folder import (
    build_folder_pc, build_hierarchy_tc, build_contents_tc,
    build_assoc_contents_tc, folder_nid_hierarchy,
    folder_nid_contents, folder_nid_assoc,
)
from .messaging.message import (
    build_message_pc, build_recipients_tc, build_attachments_tc,
    build_attachment_pc, attachment_subnode_nid,
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


def _skip_amap_pages(offset, size):
    """Advance offset so [offset, offset+size) doesn't overlap any AMap page.

    AMap pages are at fixed positions: FIRST_AMAP_OFFSET + n * AMAP_COVERAGE.
    """
    while True:
        if offset < FIRST_AMAP_OFFSET:
            return offset
        rel = offset - FIRST_AMAP_OFFSET
        n = rel // AMAP_COVERAGE
        amap_pos = FIRST_AMAP_OFFSET + n * AMAP_COVERAGE
        # Check if we overlap with the AMap page at amap_pos
        if offset < amap_pos + PAGE_SIZE:
            offset = amap_pos + PAGE_SIZE
            continue
        # Check if the item extends into the next AMap page's position
        next_amap = FIRST_AMAP_OFFSET + (n + 1) * AMAP_COVERAGE
        if offset + size > next_amap:
            offset = next_amap + PAGE_SIZE
            continue
        return offset


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

    def _store_node_pages(self, pages):
        """Store HN pages. Single page -> leaf BID, multi-page -> XBLOCK internal BID."""
        if len(pages) == 1:
            return self._store_data_block(pages[0])
        bids = []
        total = 0
        for page in pages:
            bids.append(self._store_data_block(page))
            total += len(page)
        return self._store_internal_block(build_xblock(bids, total))

    def _store_subnode_data(self, data):
        """Store subnode data. Small -> leaf BID, large -> chunked + XBLOCK internal BID."""
        if len(data) <= MAX_BLOCK_DATA:
            return self._store_data_block(data)
        bids = []
        for i in range(0, len(data), MAX_BLOCK_DATA):
            bids.append(self._store_data_block(data[i:i + MAX_BLOCK_DATA]))
        return self._store_internal_block(build_xblock(bids, len(data)))

    def _build_sl_bid(self, sl_entries):
        """Build SLBLOCK from entries and return internal BID, or 0 if empty."""
        if not sl_entries:
            return 0
        return self._store_internal_block(build_sl_block(sl_entries))

    def _store_tc_or_pc(self, pages, subnodes):
        """Store a PC or TC node's pages and subnodes, return (data_bid, sub_bid)."""
        data_bid = self._store_node_pages(pages)
        sub_bid = 0
        if subnodes:
            sl = [(sn, self._store_subnode_data(sd), 0) for sn, sd in subnodes]
            sub_bid = self._build_sl_bid(sl)
        return data_bid, sub_bid

    def _add_node(self, nid, pages, subnodes=None, extra_sl_entries=None, parent_nid=0):
        """Add a node with its HN pages and optional subnodes.

        Args:
            nid: Node ID.
            pages: List of bytes (HN pages from build_pc_node/build_tc_node).
            subnodes: List of (nid, data_bytes) for large values.
            extra_sl_entries: Additional (nid, bid_data, bid_sub) SL entries.
            parent_nid: Parent node ID.
        """
        data_bid = self._store_node_pages(pages)

        sl_entries = list(extra_sl_entries or [])
        if subnodes:
            for sub_nid, sub_data in subnodes:
                sub_bid = self._store_subnode_data(sub_data)
                sl_entries.append((sub_nid, sub_bid, 0))

        sub_bid = self._build_sl_bid(sl_entries)
        self._nodes.append((nid, data_bid, sub_bid, parent_nid))
        return data_bid

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

        # Build message PC (returns (pages, subnodes))
        msg_pages, msg_subnodes = build_message_pc(parsed_eml)

        # Build SL entries for recipients/attachments TCs
        extra_sl = []

        if parsed_eml.get('recipients'):
            recip_pages, recip_subnodes = build_recipients_tc(parsed_eml['recipients'])
            recip_bid, recip_sub_bid = self._store_tc_or_pc(recip_pages, recip_subnodes)
            recip_nid = message_nid_recipients(msg_nid)
            extra_sl.append((recip_nid, recip_bid, recip_sub_bid))

        if parsed_eml.get('attachments'):
            attach_pages, attach_subnodes = build_attachments_tc(parsed_eml['attachments'])
            attach_bid, attach_sub_bid = self._store_tc_or_pc(attach_pages, attach_subnodes)
            attach_nid = message_nid_attachments(msg_nid)
            extra_sl.append((attach_nid, attach_bid, attach_sub_bid))

            # Per [MS-PST] 2.4.6.2: each attachment gets its own subnode PC
            for i, att in enumerate(parsed_eml['attachments']):
                att_pages, att_subnodes = build_attachment_pc(att, i)
                att_bid, att_sub_bid = self._store_tc_or_pc(att_pages, att_subnodes)
                att_nid = attachment_subnode_nid(i)
                extra_sl.append((att_nid, att_bid, att_sub_bid))

        # Add message node with PC pages, subnodes, and extra SL entries
        self._add_node(msg_nid, msg_pages, msg_subnodes,
                       extra_sl_entries=extra_sl, parent_nid=folder_nid)

        return msg_nid

    def _build_internal_nodes(self):
        """Build the required internal nodes (store, root folder, name map)."""
        # 1. Message Store (NID 0x21)
        (store_pages, store_subnodes), self._record_key = build_message_store(self.display_name)
        self._add_node(NID_MESSAGE_STORE, store_pages, store_subnodes)

        # 2. Name-to-ID Map (NID 0x61) — required structure with GUID/entry/string streams
        namemap_pages, namemap_subnodes = build_name_to_id_map()
        self._add_node(NID_NAME_TO_ID_MAP, namemap_pages, namemap_subnodes)

    def _build_folder_nodes(self):
        """Build all folder nodes (PC + 3 TCs as separate top-level NBT entries)."""
        for folder_nid, finfo in self._folders.items():
            has_subs = len(finfo['subfolder_nids']) > 0
            msg_count = len(finfo['message_nids'])

            # Folder PC (returns (pages, subnodes))
            pc_pages, pc_subnodes = build_folder_pc(
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
            hier_pages, hier_subnodes = build_hierarchy_tc(sub_rows)
            hier_bid, hier_sub_bid = self._store_tc_or_pc(hier_pages, hier_subnodes)

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
            contents_pages, contents_subnodes = build_contents_tc(msg_rows)
            contents_bid, contents_sub_bid = self._store_tc_or_pc(
                contents_pages, contents_subnodes)

            # Associated Contents TC (empty)
            assoc_nid = folder_nid_assoc(folder_nid)
            assoc_pages, assoc_subnodes = build_assoc_contents_tc()
            assoc_bid, assoc_sub_bid = self._store_tc_or_pc(assoc_pages, assoc_subnodes)

            # Add folder PC node
            self._add_node(folder_nid, pc_pages, pc_subnodes,
                           parent_nid=finfo['parent_nid'])

            # Add 3 TC nodes as separate top-level NBT entries (as Outlook does)
            self._nodes.append((hier_nid, hier_bid, hier_sub_bid, 0))
            self._nodes.append((contents_nid, contents_bid, contents_sub_bid, 0))
            self._nodes.append((assoc_nid, assoc_bid, assoc_sub_bid, 0))

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
        # Layout: [Header 0x4400] [AMap pages at fixed intervals] [data blocks...] [BTree pages...]
        current_offset = FIRST_AMAP_OFFSET + PAGE_SIZE  # After first AMap page

        block_positions = {}  # bid -> (offset, raw_size)
        for bid, data in self._data_blocks:
            total = block_total_size(len(data))
            current_offset = _skip_amap_pages(current_offset, total)
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
            offset = _skip_amap_pages(page_offset_cursor[0], PAGE_SIZE)
            page_offsets[bid] = offset
            page_offset_cursor[0] = offset + PAGE_SIZE
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

        # Phase 5: Build AMap pages (one per AMAP_COVERAGE region)
        allocated_ranges = []
        for bid, data in self._data_blocks:
            if bid in block_positions:
                offset, raw_size = block_positions[bid]
                total = block_total_size(raw_size)
                allocated_ranges.append((offset, total))
        for _, pg_offset, _ in nbt_pages + bbt_pages:
            allocated_ranges.append((pg_offset, PAGE_SIZE))

        num_amaps = max(1, (file_eof - FIRST_AMAP_OFFSET + AMAP_COVERAGE - 1) // AMAP_COVERAGE)
        amap_pages_list = []  # list of (offset, page_bytes)
        total_amap_free = 0
        last_amap_offset = FIRST_AMAP_OFFSET

        for i in range(num_amaps):
            amap_offset = FIRST_AMAP_OFFSET + i * AMAP_COVERAGE
            amap_bid = self._alloc_page_bid()
            page = build_amap_page(
                allocated_ranges, amap_offset, amap_offset, amap_bid
            )
            free = compute_amap_free(
                allocated_ranges, amap_offset, amap_offset
            )
            amap_pages_list.append((amap_offset, page))
            total_amap_free += free
            last_amap_offset = amap_offset

        # Phase 6: Build header
        root = pack_root(
            file_eof=file_eof,
            ib_amap_last=last_amap_offset,
            cb_amap_free=total_amap_free,
            cb_pmap_free=0,
            bref_nbt=(nbt_root_bid, nbt_root_offset),
            bref_bbt=(bbt_root_bid, bbt_root_offset),
            f_amap_valid=2,
        )

        header = build_header(
            root_data=root,
            bid_next_p=self._next_page_bid,
            bid_next_b=max(self._next_leaf_bid, self._next_internal_bid),
            unique=1,
        )

        # Phase 7: Write all items sorted by file offset
        with open(output_path, 'wb') as f:
            # Header (padded to FIRST_AMAP_OFFSET)
            f.write(header)
            f.write(b'\x00' * (FIRST_AMAP_OFFSET - len(header)))

            # Collect all items: (offset, data_bytes)
            write_items = []
            for amap_offset, amap_data in amap_pages_list:
                write_items.append((amap_offset, amap_data))
            for bid, data in self._data_blocks:
                if bid in block_positions:
                    offset, _ = block_positions[bid]
                    packed = pack_block(data, bid, offset)
                    write_items.append((offset, packed))
            for pg_bid, pg_offset, pg_data in nbt_pages + bbt_pages:
                write_items.append((pg_offset, pg_data))

            write_items.sort(key=lambda x: x[0])

            expected_offset = FIRST_AMAP_OFFSET
            for offset, data in write_items:
                if offset > expected_offset:
                    f.write(b'\x00' * (offset - expected_offset))
                f.write(data)
                expected_offset = offset + len(data)

        return output_path
