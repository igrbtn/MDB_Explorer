#!/usr/bin/env python3
"""
Exchange EDB Exporter - Command Line Interface

A comprehensive CLI for viewing and exporting data from Microsoft Exchange EDB files.

Usage:
    python cli.py <edb_file> <command> [options]

Commands:
    list-mailboxes     List all mailboxes
    list-folders       List folders in a mailbox
    list-emails        List emails (with search/filter)
    export-email       Export single email to EML
    export-folder      Export folder to EML files
    export-mailbox     Export entire mailbox
    export-calendar    Export calendar items to ICS
    info               Show database information
"""

import sys
import os
import argparse
import struct
import csv
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List, Dict

# Try imports
try:
    import pyesedb
except ImportError:
    print("ERROR: pyesedb not installed. Run: pip install libesedb-python")
    sys.exit(1)

try:
    from dissect.esedb.compression import decompress as dissect_decompress
    HAS_DISSECT = True
except ImportError:
    HAS_DISSECT = False
    print("WARNING: dissect.esedb not installed. Some features may not work.")
    print("         Run: pip install dissect.esedb")

# Import local modules
try:
    from email_message import EmailMessage, EmailExtractor
    HAS_EMAIL_MODULE = True
except ImportError:
    HAS_EMAIL_MODULE = False

try:
    from calendar_message import CalendarExtractor, export_calendar_to_ics, CALENDAR_MESSAGE_CLASSES
    HAS_CALENDAR_MODULE = True
except ImportError:
    HAS_CALENDAR_MODULE = False
    CALENDAR_MESSAGE_CLASSES = []


def get_column_map(table) -> Dict[str, int]:
    """Get mapping of column names to indices."""
    col_map = {}
    for j in range(table.get_number_of_columns()):
        col = table.get_column(j)
        if col:
            col_map[col.name] = j
    return col_map


def get_filetime(record, col_idx: int) -> Optional[datetime]:
    """Convert FILETIME column to datetime."""
    if col_idx < 0:
        return None
    try:
        val = record.get_value_data(col_idx)
        if not val or len(val) != 8:
            return None
        filetime = struct.unpack('<Q', val)[0]
        if filetime == 0:
            return None
        EPOCH_DIFF = 116444736000000000
        timestamp = (filetime - EPOCH_DIFF) / 10000000
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    except:
        return None


def decompress_text(data: bytes) -> str:
    """Decompress and decode text data."""
    if not data:
        return ""

    if HAS_DISSECT:
        try:
            decompressed = dissect_decompress(data)
            # Try UTF-16-LE first (common for Exchange)
            try:
                return decompressed.decode('utf-16-le').rstrip('\x00')
            except:
                pass
            # Try UTF-8
            try:
                return decompressed.decode('utf-8').rstrip('\x00')
            except:
                pass
        except:
            pass

    # Fallback
    try:
        return data.decode('utf-8', errors='ignore').rstrip('\x00')
    except:
        return ""


class EDBExporter:
    """Main class for EDB export operations."""

    def __init__(self, edb_path: str, verbose: bool = False):
        self.edb_path = edb_path
        self.verbose = verbose
        self.db = None
        self.tables = {}

    def open(self):
        """Open the database."""
        if not os.path.exists(self.edb_path):
            raise FileNotFoundError(f"File not found: {self.edb_path}")

        self.db = pyesedb.file()
        self.db.open(self.edb_path)

        # Load tables
        for i in range(self.db.get_number_of_tables()):
            table = self.db.get_table(i)
            if table:
                self.tables[table.name] = table

        if self.verbose:
            print(f"Opened database with {len(self.tables)} tables")

    def close(self):
        """Close the database."""
        if self.db:
            self.db.close()
            self.db = None

    def get_mailboxes(self) -> List[Dict]:
        """Get list of mailboxes with owner names."""
        mailboxes = []

        mailbox_table = self.tables.get("Mailbox")
        if not mailbox_table:
            return mailboxes

        col_map = get_column_map(mailbox_table)

        for i in range(mailbox_table.get_number_of_records()):
            try:
                rec = mailbox_table.get_record(i)
                if not rec:
                    continue

                # Get mailbox number
                num_idx = col_map.get('MailboxNumber', -1)
                num_val = rec.get_value_data(num_idx) if num_idx >= 0 else None
                if not num_val or len(num_val) < 4:
                    continue
                mailbox_num = struct.unpack('<I', num_val[:4])[0]

                # Get owner name
                owner_idx = col_map.get('MailboxOwnerDisplayName', -1)
                owner_val = rec.get_value_data(owner_idx) if owner_idx >= 0 else None
                owner_name = decompress_text(owner_val) if owner_val else ""

                # Get message count from Message table
                msg_table = self.tables.get(f"Message_{mailbox_num}")
                msg_count = msg_table.get_number_of_records() if msg_table else 0

                # Get attachment count
                att_table = self.tables.get(f"Attachment_{mailbox_num}")
                att_count = att_table.get_number_of_records() if att_table else 0

                # Get folder count
                folder_table = self.tables.get(f"Folder_{mailbox_num}")
                folder_count = folder_table.get_number_of_records() if folder_table else 0

                mailboxes.append({
                    'number': mailbox_num,
                    'owner': owner_name,
                    'messages': msg_count,
                    'attachments': att_count,
                    'folders': folder_count
                })
            except Exception as e:
                if self.verbose:
                    print(f"Error reading mailbox record {i}: {e}")

        return sorted(mailboxes, key=lambda x: x['number'])

    def get_folders(self, mailbox_num: int) -> List[Dict]:
        """Get list of folders in a mailbox."""
        folders = []

        folder_table = self.tables.get(f"Folder_{mailbox_num}")
        if not folder_table:
            return folders

        col_map = get_column_map(folder_table)

        for i in range(folder_table.get_number_of_records()):
            try:
                rec = folder_table.get_record(i)
                if not rec:
                    continue

                # Get folder ID
                fid_idx = col_map.get('FolderId', -1)
                fid_val = rec.get_value_data(fid_idx) if fid_idx >= 0 else None
                folder_id = fid_val.hex() if fid_val else ""

                # Get display name
                name_idx = col_map.get('DisplayName', -1)
                name_val = rec.get_value_data(name_idx) if name_idx >= 0 else None
                display_name = decompress_text(name_val) if name_val else ""

                # Get message count
                count_idx = col_map.get('MessageCount', -1)
                count_val = rec.get_value_data(count_idx) if count_idx >= 0 else None
                msg_count = struct.unpack('<I', count_val[:4])[0] if count_val and len(count_val) >= 4 else 0

                # Get parent folder ID
                parent_idx = col_map.get('ParentFolderId', -1)
                parent_val = rec.get_value_data(parent_idx) if parent_idx >= 0 else None
                parent_id = parent_val.hex() if parent_val else ""

                # Get special folder number
                special_idx = col_map.get('SpecialFolderNumber', -1)
                special_val = rec.get_value_data(special_idx) if special_idx >= 0 else None
                special_num = struct.unpack('<I', special_val[:4])[0] if special_val and len(special_val) >= 4 else 0

                folders.append({
                    'id': folder_id,
                    'name': display_name or f"Folder_{i}",
                    'messages': msg_count,
                    'parent_id': parent_id,
                    'special': special_num,
                    'record': i
                })
            except Exception as e:
                if self.verbose:
                    print(f"Error reading folder record {i}: {e}")

        return folders

    def get_emails(self, mailbox_num: int, folder_id: str = None,
                   search: str = None, limit: int = None,
                   date_from: datetime = None, date_to: datetime = None) -> List[Dict]:
        """Get list of emails with optional filtering."""
        emails = []

        msg_table = self.tables.get(f"Message_{mailbox_num}")
        if not msg_table:
            return emails

        col_map = get_column_map(msg_table)

        # Get mailbox owner for extractor
        mailboxes = self.get_mailboxes()
        owner = next((m['owner'] for m in mailboxes if m['number'] == mailbox_num), "")

        extractor = EmailExtractor(mailbox_owner=owner) if HAS_EMAIL_MODULE else None

        for i in range(msg_table.get_number_of_records()):
            try:
                rec = msg_table.get_record(i)
                if not rec:
                    continue

                # Filter by folder
                if folder_id:
                    fid_idx = col_map.get('FolderId', -1)
                    fid_val = rec.get_value_data(fid_idx) if fid_idx >= 0 else None
                    if fid_val and fid_val.hex() != folder_id:
                        continue

                # Get date
                date_received = get_filetime(rec, col_map.get('DateReceived', -1))

                # Filter by date
                if date_from and date_received and date_received < date_from:
                    continue
                if date_to and date_received and date_received > date_to:
                    continue

                # Extract email data
                email_data = {
                    'record': i,
                    'date': date_received,
                    'subject': '',
                    'from': '',
                    'to': '',
                    'has_attachments': False,
                    'is_read': False
                }

                if extractor:
                    email_msg = extractor.extract_message(rec, col_map, i)
                    email_data['subject'] = email_msg.subject
                    email_data['from'] = email_msg.get_from_header()
                    email_data['to'] = email_msg.get_to_header()
                    email_data['has_attachments'] = email_msg.has_attachments
                    email_data['is_read'] = email_msg.is_read
                else:
                    # Basic extraction without module
                    has_att_idx = col_map.get('HasAttachments', -1)
                    has_att = rec.get_value_data(has_att_idx) if has_att_idx >= 0 else None
                    email_data['has_attachments'] = bool(has_att and has_att != b'\x00')

                    is_read_idx = col_map.get('IsRead', -1)
                    is_read = rec.get_value_data(is_read_idx) if is_read_idx >= 0 else None
                    email_data['is_read'] = bool(is_read and is_read != b'\x00')

                # Search filter
                if search:
                    search_lower = search.lower()
                    if (search_lower not in email_data['subject'].lower() and
                        search_lower not in email_data['from'].lower() and
                        search_lower not in email_data['to'].lower()):
                        continue

                emails.append(email_data)

                if limit and len(emails) >= limit:
                    break

            except Exception as e:
                if self.verbose:
                    print(f"Error reading message record {i}: {e}")

        return emails

    def export_email(self, mailbox_num: int, record_idx: int, output_path: str) -> bool:
        """Export a single email to EML file."""
        if not HAS_EMAIL_MODULE:
            print("ERROR: email_message module not available")
            return False

        msg_table = self.tables.get(f"Message_{mailbox_num}")
        if not msg_table:
            print(f"ERROR: Message table not found for mailbox {mailbox_num}")
            return False

        col_map = get_column_map(msg_table)

        # Get mailbox owner
        mailboxes = self.get_mailboxes()
        owner = next((m['owner'] for m in mailboxes if m['number'] == mailbox_num), "")

        extractor = EmailExtractor(mailbox_owner=owner)

        try:
            rec = msg_table.get_record(record_idx)
            if not rec:
                print(f"ERROR: Record {record_idx} not found")
                return False

            email_msg = extractor.extract_message(
                rec, col_map, record_idx,
                tables=self.tables, mailbox_num=mailbox_num
            )

            eml_content = email_msg.to_eml()

            with open(output_path, 'wb') as f:
                f.write(eml_content)

            return True
        except Exception as e:
            print(f"ERROR: {e}")
            return False

    def export_folder(self, mailbox_num: int, folder_id: str, output_dir: str,
                      date_from: datetime = None, date_to: datetime = None) -> int:
        """Export all emails in a folder to EML files."""
        if not HAS_EMAIL_MODULE:
            print("ERROR: email_message module not available")
            return 0

        msg_table = self.tables.get(f"Message_{mailbox_num}")
        if not msg_table:
            print(f"ERROR: Message table not found for mailbox {mailbox_num}")
            return 0

        col_map = get_column_map(msg_table)

        # Get mailbox owner
        mailboxes = self.get_mailboxes()
        owner = next((m['owner'] for m in mailboxes if m['number'] == mailbox_num), "")

        extractor = EmailExtractor(mailbox_owner=owner)

        # Create output directory
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        exported = 0

        for i in range(msg_table.get_number_of_records()):
            try:
                rec = msg_table.get_record(i)
                if not rec:
                    continue

                # Filter by folder
                if folder_id:
                    fid_idx = col_map.get('FolderId', -1)
                    fid_val = rec.get_value_data(fid_idx) if fid_idx >= 0 else None
                    if fid_val and fid_val.hex() != folder_id:
                        continue

                # Filter by date
                date_received = get_filetime(rec, col_map.get('DateReceived', -1))
                if date_from and date_received and date_received < date_from:
                    continue
                if date_to and date_received and date_received > date_to:
                    continue

                email_msg = extractor.extract_message(
                    rec, col_map, i,
                    tables=self.tables, mailbox_num=mailbox_num
                )

                # Generate filename
                date_str = date_received.strftime("%Y%m%d_%H%M%S") if date_received else "nodate"
                subject_safe = re.sub(r'[<>:"/\\|?*]', '_', email_msg.subject or 'no_subject')[:40]
                filename = f"{date_str}_{i}_{subject_safe}.eml"

                eml_content = email_msg.to_eml()

                with open(Path(output_dir) / filename, 'wb') as f:
                    f.write(eml_content)

                exported += 1

                if self.verbose and exported % 50 == 0:
                    print(f"  Exported {exported} emails...")

            except Exception as e:
                if self.verbose:
                    print(f"Error exporting record {i}: {e}")

        return exported

    def export_mailbox(self, mailbox_num: int, output_dir: str,
                       date_from: datetime = None, date_to: datetime = None) -> int:
        """Export entire mailbox with folder structure."""
        if not HAS_EMAIL_MODULE:
            print("ERROR: email_message module not available")
            return 0

        # Get folders
        folders = self.get_folders(mailbox_num)
        folder_map = {f['id']: f for f in folders}

        # Build folder paths
        def get_folder_path(folder_id: str) -> str:
            parts = []
            visited = set()
            current_id = folder_id
            while current_id and current_id not in visited:
                visited.add(current_id)
                folder = folder_map.get(current_id)
                if not folder:
                    break
                parts.insert(0, folder['name'])
                current_id = folder.get('parent_id', '')
            return '/'.join(parts) if parts else 'Unknown'

        msg_table = self.tables.get(f"Message_{mailbox_num}")
        if not msg_table:
            print(f"ERROR: Message table not found for mailbox {mailbox_num}")
            return 0

        col_map = get_column_map(msg_table)

        # Get mailbox owner
        mailboxes = self.get_mailboxes()
        owner = next((m['owner'] for m in mailboxes if m['number'] == mailbox_num), "")

        extractor = EmailExtractor(mailbox_owner=owner)

        # Create base output directory
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        exported = 0

        for i in range(msg_table.get_number_of_records()):
            try:
                rec = msg_table.get_record(i)
                if not rec:
                    continue

                # Get folder ID
                fid_idx = col_map.get('FolderId', -1)
                fid_val = rec.get_value_data(fid_idx) if fid_idx >= 0 else None
                folder_id = fid_val.hex() if fid_val else ""

                # Filter by date
                date_received = get_filetime(rec, col_map.get('DateReceived', -1))
                if date_from and date_received and date_received < date_from:
                    continue
                if date_to and date_received and date_received > date_to:
                    continue

                # Get folder path
                folder_path = get_folder_path(folder_id)
                safe_parts = [re.sub(r'[<>:"/\\|?*]', '_', p) for p in folder_path.split('/')]

                email_msg = extractor.extract_message(
                    rec, col_map, i,
                    folder_name=folder_path,
                    tables=self.tables, mailbox_num=mailbox_num
                )

                # Create folder directory
                folder_dir = Path(output_dir)
                for part in safe_parts:
                    folder_dir = folder_dir / part
                folder_dir.mkdir(parents=True, exist_ok=True)

                # Generate filename
                date_str = date_received.strftime("%Y%m%d_%H%M%S") if date_received else "nodate"
                subject_safe = re.sub(r'[<>:"/\\|?*]', '_', email_msg.subject or 'no_subject')[:40]
                filename = f"{date_str}_{i}_{subject_safe}.eml"

                eml_content = email_msg.to_eml()

                with open(folder_dir / filename, 'wb') as f:
                    f.write(eml_content)

                exported += 1

                if self.verbose and exported % 50 == 0:
                    print(f"  Exported {exported} emails...")

            except Exception as e:
                if self.verbose:
                    print(f"Error exporting record {i}: {e}")

        return exported

    def export_calendar(self, mailbox_num: int, output_path: str) -> int:
        """Export calendar items to ICS file."""
        if not HAS_CALENDAR_MODULE:
            print("ERROR: calendar_message module not available")
            return 0

        msg_table = self.tables.get(f"Message_{mailbox_num}")
        if not msg_table:
            print(f"ERROR: Message table not found for mailbox {mailbox_num}")
            return 0

        col_map = get_column_map(msg_table)

        # Get mailbox owner
        mailboxes = self.get_mailboxes()
        owner = next((m['owner'] for m in mailboxes if m['number'] == mailbox_num), "")

        extractor = CalendarExtractor(mailbox_owner=owner)

        events = []

        for i in range(msg_table.get_number_of_records()):
            try:
                rec = msg_table.get_record(i)
                if not rec:
                    continue

                # Check if calendar item
                msg_class = extractor.get_message_class(rec, col_map)
                if not extractor.is_calendar_item(msg_class):
                    continue

                event = extractor.extract_event(rec, col_map, i)
                if event:
                    events.append(event)

            except Exception as e:
                if self.verbose:
                    print(f"Error reading calendar record {i}: {e}")

        if events:
            if export_calendar_to_ics(events, output_path):
                return len(events)

        return 0


def cmd_list_mailboxes(args):
    """List mailboxes command."""
    exporter = EDBExporter(args.edb_file, args.verbose)
    exporter.open()

    try:
        mailboxes = exporter.get_mailboxes()

        if args.csv:
            # Output to CSV
            with open(args.csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['Number', 'Owner', 'Messages', 'Attachments', 'Folders'])
                for mb in mailboxes:
                    writer.writerow([mb['number'], mb['owner'], mb['messages'],
                                   mb['attachments'], mb['folders']])
            print(f"Saved {len(mailboxes)} mailboxes to {args.csv}")
        else:
            # Print to console
            print(f"\n{'#':<6} {'Owner':<35} {'Messages':<10} {'Attachments':<12} {'Folders':<8}")
            print("-" * 80)
            for mb in mailboxes:
                print(f"{mb['number']:<6} {mb['owner'][:34]:<35} {mb['messages']:<10} "
                      f"{mb['attachments']:<12} {mb['folders']:<8}")
            print(f"\nTotal: {len(mailboxes)} mailboxes")
    finally:
        exporter.close()


def cmd_list_folders(args):
    """List folders command."""
    exporter = EDBExporter(args.edb_file, args.verbose)
    exporter.open()

    try:
        folders = exporter.get_folders(args.mailbox)

        if args.csv:
            with open(args.csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['ID', 'Name', 'Messages', 'Special', 'ParentID'])
                for fld in folders:
                    writer.writerow([fld['id'][:16], fld['name'], fld['messages'],
                                   fld['special'], fld['parent_id'][:16] if fld['parent_id'] else ''])
            print(f"Saved {len(folders)} folders to {args.csv}")
        else:
            print(f"\n{'Name':<40} {'Messages':<10} {'Special':<8} {'ID':<18}")
            print("-" * 80)
            for fld in folders:
                print(f"{fld['name'][:39]:<40} {fld['messages']:<10} {fld['special']:<8} {fld['id'][:16]}")
            print(f"\nTotal: {len(folders)} folders")
    finally:
        exporter.close()


def cmd_list_emails(args):
    """List/search emails command."""
    exporter = EDBExporter(args.edb_file, args.verbose)
    exporter.open()

    try:
        # Parse dates
        date_from = datetime.strptime(args.date_from, "%Y-%m-%d").replace(tzinfo=timezone.utc) if args.date_from else None
        date_to = datetime.strptime(args.date_to, "%Y-%m-%d").replace(tzinfo=timezone.utc) if args.date_to else None

        emails = exporter.get_emails(
            args.mailbox,
            folder_id=args.folder,
            search=args.search,
            limit=args.limit,
            date_from=date_from,
            date_to=date_to
        )

        if args.csv:
            with open(args.csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['Record', 'Date', 'From', 'To', 'Subject', 'Attachments', 'Read'])
                for email in emails:
                    writer.writerow([
                        email['record'],
                        email['date'].strftime("%Y-%m-%d %H:%M") if email['date'] else '',
                        email['from'],
                        email['to'],
                        email['subject'],
                        'Yes' if email['has_attachments'] else 'No',
                        'Yes' if email['is_read'] else 'No'
                    ])
            print(f"Saved {len(emails)} emails to {args.csv}")
        else:
            print(f"\n{'#':<6} {'Date':<18} {'From':<25} {'Subject':<35}")
            print("-" * 90)
            for email in emails:
                date_str = email['date'].strftime("%Y-%m-%d %H:%M") if email['date'] else 'N/A'
                att = " [A]" if email['has_attachments'] else ""
                print(f"{email['record']:<6} {date_str:<18} {email['from'][:24]:<25} "
                      f"{email['subject'][:34]}{att}")
            print(f"\nTotal: {len(emails)} emails")
    finally:
        exporter.close()


def cmd_export_email(args):
    """Export single email command."""
    exporter = EDBExporter(args.edb_file, args.verbose)
    exporter.open()

    try:
        output = args.output or f"email_{args.record}.eml"
        if exporter.export_email(args.mailbox, args.record, output):
            print(f"Exported email to {output}")
        else:
            print("Export failed")
            sys.exit(1)
    finally:
        exporter.close()


def cmd_export_folder(args):
    """Export folder command."""
    exporter = EDBExporter(args.edb_file, args.verbose)
    exporter.open()

    try:
        date_from = datetime.strptime(args.date_from, "%Y-%m-%d").replace(tzinfo=timezone.utc) if args.date_from else None
        date_to = datetime.strptime(args.date_to, "%Y-%m-%d").replace(tzinfo=timezone.utc) if args.date_to else None

        output = args.output or f"folder_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        count = exporter.export_folder(args.mailbox, args.folder, output, date_from, date_to)
        print(f"Exported {count} emails to {output}")
    finally:
        exporter.close()


def cmd_export_mailbox(args):
    """Export mailbox command."""
    exporter = EDBExporter(args.edb_file, args.verbose)
    exporter.open()

    try:
        date_from = datetime.strptime(args.date_from, "%Y-%m-%d").replace(tzinfo=timezone.utc) if args.date_from else None
        date_to = datetime.strptime(args.date_to, "%Y-%m-%d").replace(tzinfo=timezone.utc) if args.date_to else None

        # Get mailbox owner for folder name
        mailboxes = exporter.get_mailboxes()
        owner = next((m['owner'] for m in mailboxes if m['number'] == args.mailbox), f"Mailbox_{args.mailbox}")

        output = args.output or owner.replace(' ', '_')
        count = exporter.export_mailbox(args.mailbox, output, date_from, date_to)
        print(f"Exported {count} emails to {output}")
    finally:
        exporter.close()


def cmd_export_calendar(args):
    """Export calendar command."""
    exporter = EDBExporter(args.edb_file, args.verbose)
    exporter.open()

    try:
        output = args.output or f"calendar_{args.mailbox}.ics"
        count = exporter.export_calendar(args.mailbox, output)
        if count > 0:
            print(f"Exported {count} calendar items to {output}")
        else:
            print("No calendar items found")
    finally:
        exporter.close()


def cmd_info(args):
    """Show database info command."""
    exporter = EDBExporter(args.edb_file, args.verbose)
    exporter.open()

    try:
        print(f"\n=== DATABASE INFO ===")
        print(f"File: {args.edb_file}")
        print(f"Tables: {len(exporter.tables)}")

        mailboxes = exporter.get_mailboxes()
        total_messages = sum(m['messages'] for m in mailboxes)
        total_attachments = sum(m['attachments'] for m in mailboxes)

        print(f"\nMailboxes: {len(mailboxes)}")
        print(f"Total messages: {total_messages}")
        print(f"Total attachments: {total_attachments}")

        print(f"\n=== MAILBOXES ===")
        for mb in mailboxes[:10]:
            print(f"  {mb['number']}: {mb['owner']} ({mb['messages']} msgs)")
        if len(mailboxes) > 10:
            print(f"  ... and {len(mailboxes) - 10} more")
    finally:
        exporter.close()


def main():
    parser = argparse.ArgumentParser(
        description="Exchange EDB Exporter - Command Line Interface",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('edb_file', help='Path to EDB database file')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')

    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # list-mailboxes
    p_mb = subparsers.add_parser('list-mailboxes', help='List all mailboxes')
    p_mb.add_argument('--csv', help='Save to CSV file')

    # list-folders
    p_fld = subparsers.add_parser('list-folders', help='List folders in mailbox')
    p_fld.add_argument('-m', '--mailbox', type=int, required=True, help='Mailbox number')
    p_fld.add_argument('--csv', help='Save to CSV file')

    # list-emails
    p_emails = subparsers.add_parser('list-emails', help='List/search emails')
    p_emails.add_argument('-m', '--mailbox', type=int, required=True, help='Mailbox number')
    p_emails.add_argument('-f', '--folder', help='Filter by folder ID')
    p_emails.add_argument('-s', '--search', help='Search in subject/from/to')
    p_emails.add_argument('-n', '--limit', type=int, help='Limit results')
    p_emails.add_argument('--date-from', help='Filter from date (YYYY-MM-DD)')
    p_emails.add_argument('--date-to', help='Filter to date (YYYY-MM-DD)')
    p_emails.add_argument('--csv', help='Save to CSV file')

    # export-email
    p_exp_email = subparsers.add_parser('export-email', help='Export single email')
    p_exp_email.add_argument('-m', '--mailbox', type=int, required=True, help='Mailbox number')
    p_exp_email.add_argument('-r', '--record', type=int, required=True, help='Record index')
    p_exp_email.add_argument('-o', '--output', help='Output file path')

    # export-folder
    p_exp_fld = subparsers.add_parser('export-folder', help='Export folder to EML files')
    p_exp_fld.add_argument('-m', '--mailbox', type=int, required=True, help='Mailbox number')
    p_exp_fld.add_argument('-f', '--folder', required=True, help='Folder ID')
    p_exp_fld.add_argument('-o', '--output', help='Output directory')
    p_exp_fld.add_argument('--date-from', help='Filter from date (YYYY-MM-DD)')
    p_exp_fld.add_argument('--date-to', help='Filter to date (YYYY-MM-DD)')

    # export-mailbox
    p_exp_mb = subparsers.add_parser('export-mailbox', help='Export entire mailbox')
    p_exp_mb.add_argument('-m', '--mailbox', type=int, required=True, help='Mailbox number')
    p_exp_mb.add_argument('-o', '--output', help='Output directory')
    p_exp_mb.add_argument('--date-from', help='Filter from date (YYYY-MM-DD)')
    p_exp_mb.add_argument('--date-to', help='Filter to date (YYYY-MM-DD)')

    # export-calendar
    p_exp_cal = subparsers.add_parser('export-calendar', help='Export calendar to ICS')
    p_exp_cal.add_argument('-m', '--mailbox', type=int, required=True, help='Mailbox number')
    p_exp_cal.add_argument('-o', '--output', help='Output file path')

    # info
    p_info = subparsers.add_parser('info', help='Show database information')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        print("\nExamples:")
        print("  python cli.py database.edb info")
        print("  python cli.py database.edb list-mailboxes")
        print("  python cli.py database.edb list-mailboxes --csv mailboxes.csv")
        print("  python cli.py database.edb list-folders -m 103")
        print("  python cli.py database.edb list-emails -m 103 -s \"invoice\" --csv results.csv")
        print("  python cli.py database.edb export-email -m 103 -r 318 -o email.eml")
        print("  python cli.py database.edb export-mailbox -m 103 -o ./exported")
        print("  python cli.py database.edb export-calendar -m 103 -o calendar.ics")
        sys.exit(0)

    # Execute command
    commands = {
        'list-mailboxes': cmd_list_mailboxes,
        'list-folders': cmd_list_folders,
        'list-emails': cmd_list_emails,
        'export-email': cmd_export_email,
        'export-folder': cmd_export_folder,
        'export-mailbox': cmd_export_mailbox,
        'export-calendar': cmd_export_calendar,
        'info': cmd_info
    }

    try:
        commands[args.command](args)
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
