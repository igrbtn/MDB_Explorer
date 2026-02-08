#!/usr/bin/env python3
"""
Email Message Module for Exchange EDB Export

Provides stable extraction and export of email messages from Exchange EDB databases.
Creates proper RFC 5322 compliant EML files with all headers and body content.
"""

import struct
import re
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.utils import format_datetime, formataddr
from email import encoders
from dataclasses import dataclass, field
from typing import Optional, List, Tuple


@dataclass
class EmailAttachment:
    """Represents an email attachment."""
    filename: str
    content_type: str
    data: bytes
    size: int = 0
    is_inline: bool = False
    content_id: str = ""

    def __post_init__(self):
        self.size = len(self.data) if self.data else 0


@dataclass
class EmailMessage:
    """
    Represents a complete email message extracted from Exchange EDB.

    Contains all headers, body content (text and HTML), and attachments.
    Can be exported to standard EML format.
    """
    # Identifiers
    record_index: int = 0
    message_document_id: int = 0
    message_id: str = ""

    # Envelope
    sender_name: str = ""
    sender_email: str = ""
    to_names: List[str] = field(default_factory=list)
    to_emails: List[str] = field(default_factory=list)
    cc_names: List[str] = field(default_factory=list)
    cc_emails: List[str] = field(default_factory=list)
    bcc_names: List[str] = field(default_factory=list)
    bcc_emails: List[str] = field(default_factory=list)
    reply_to: str = ""

    # Subject and dates
    subject: str = ""
    date_sent: Optional[datetime] = None
    date_received: Optional[datetime] = None

    # Body content
    body_text: str = ""
    body_html: str = ""

    # Flags
    is_read: bool = False
    has_attachments: bool = False
    importance: int = 1  # 0=low, 1=normal, 2=high
    sensitivity: int = 0  # 0=normal, 1=personal, 2=private, 3=confidential

    # Metadata
    folder_name: str = ""
    message_class: str = "IPM.Note"

    # Attachments
    attachments: List[EmailAttachment] = field(default_factory=list)

    # Raw data for debugging
    _raw_property_blob: bytes = field(default=b'', repr=False)

    def get_from_header(self) -> str:
        """Get formatted From header."""
        if self.sender_name and self.sender_email:
            return formataddr((self.sender_name, self.sender_email))
        elif self.sender_email:
            return self.sender_email
        elif self.sender_name:
            return f"{self.sender_name} <{self.sender_name.lower().replace(' ', '')}@unknown>"
        return "unknown@unknown"

    def get_to_header(self) -> str:
        """Get formatted To header."""
        recipients = []
        for i, email in enumerate(self.to_emails):
            name = self.to_names[i] if i < len(self.to_names) else ""
            if name:
                recipients.append(formataddr((name, email)))
            else:
                recipients.append(email)
        return ", ".join(recipients) if recipients else ""

    def get_cc_header(self) -> str:
        """Get formatted Cc header."""
        recipients = []
        for i, email in enumerate(self.cc_emails):
            name = self.cc_names[i] if i < len(self.cc_names) else ""
            if name:
                recipients.append(formataddr((name, email)))
            else:
                recipients.append(email)
        return ", ".join(recipients) if recipients else ""

    def get_importance_header(self) -> str:
        """Get X-Priority header value."""
        if self.importance == 0:
            return "5"  # Low
        elif self.importance == 2:
            return "1"  # High
        return "3"  # Normal

    def to_eml(self) -> bytes:
        """
        Export message to RFC 5322 compliant EML format.

        Returns:
            EML content as bytes
        """
        # Determine message structure
        has_html = bool(self.body_html and self.body_html.strip())
        has_text = bool(self.body_text and self.body_text.strip())
        has_attachments = bool(self.attachments)

        if has_attachments:
            # Mixed: body + attachments
            msg = MIMEMultipart('mixed')

            # Body part
            if has_html and has_text:
                body_part = MIMEMultipart('alternative')
                body_part.attach(MIMEText(self.body_text, 'plain', 'utf-8'))
                body_part.attach(MIMEText(self.body_html, 'html', 'utf-8'))
                msg.attach(body_part)
            elif has_html:
                msg.attach(MIMEText(self.body_html, 'html', 'utf-8'))
            elif has_text:
                msg.attach(MIMEText(self.body_text, 'plain', 'utf-8'))
            else:
                msg.attach(MIMEText(self.subject or "(No content)", 'plain', 'utf-8'))

            # Attachments
            for att in self.attachments:
                if att.data:
                    maintype, subtype = att.content_type.split('/', 1) if '/' in att.content_type else ('application', 'octet-stream')
                    attachment = MIMEBase(maintype, subtype)
                    attachment.set_payload(att.data)
                    encoders.encode_base64(attachment)
                    attachment.add_header('Content-Disposition', 'attachment', filename=att.filename)
                    if att.content_id:
                        attachment.add_header('Content-ID', f'<{att.content_id}>')
                    msg.attach(attachment)
        else:
            # No attachments
            if has_html and has_text:
                msg = MIMEMultipart('alternative')
                msg.attach(MIMEText(self.body_text, 'plain', 'utf-8'))
                msg.attach(MIMEText(self.body_html, 'html', 'utf-8'))
            elif has_html:
                msg = MIMEText(self.body_html, 'html', 'utf-8')
            elif has_text:
                msg = MIMEText(self.body_text, 'plain', 'utf-8')
            else:
                msg = MIMEText(self.subject or "(No content)", 'plain', 'utf-8')

        # Set headers
        msg['From'] = self.get_from_header()

        to_header = self.get_to_header()
        if to_header:
            msg['To'] = to_header

        cc_header = self.get_cc_header()
        if cc_header:
            msg['Cc'] = cc_header

        msg['Subject'] = self.subject or "(No Subject)"

        if self.date_sent:
            msg['Date'] = format_datetime(self.date_sent)
        elif self.date_received:
            msg['Date'] = format_datetime(self.date_received)

        if self.message_id:
            msg['Message-ID'] = self.message_id

        if self.reply_to:
            msg['Reply-To'] = self.reply_to

        # Extended headers
        msg['X-Priority'] = self.get_importance_header()
        msg['X-MS-Has-Attach'] = 'yes' if self.has_attachments else 'no'
        msg['X-MS-Exchange-MessageClass'] = self.message_class
        msg['X-Folder'] = self.folder_name
        msg['X-Record-Index'] = str(self.record_index)

        if self.importance == 2:
            msg['Importance'] = 'high'
        elif self.importance == 0:
            msg['Importance'] = 'low'

        if self.sensitivity > 0:
            sensitivity_map = {1: 'Personal', 2: 'Private', 3: 'Company-Confidential'}
            msg['Sensitivity'] = sensitivity_map.get(self.sensitivity, 'Normal')

        return msg.as_bytes()

    def get_summary(self) -> str:
        """Get a text summary of the message."""
        lines = [
            f"Record: #{self.record_index}",
            f"Message-ID: {self.message_id or '(none)'}",
            f"From: {self.get_from_header()}",
            f"To: {self.get_to_header() or '(none)'}",
        ]

        if self.get_cc_header():
            lines.append(f"Cc: {self.get_cc_header()}")

        lines.extend([
            f"Subject: {self.subject or '(No Subject)'}",
            f"Date: {self.date_sent or self.date_received or '(unknown)'}",
            f"Folder: {self.folder_name}",
            f"Read: {self.is_read}",
            f"Attachments: {len(self.attachments)}",
        ])

        return "\n".join(lines)


class EmailExtractor:
    """
    Extracts email messages from Exchange EDB database records.

    Handles decryption detection, encoding conversion, and data extraction
    from PropertyBlob and other columns.
    """

    # Supported encodings for text extraction
    ENCODINGS = ['utf-8', 'ascii', 'windows-1251', 'koi8-r', 'iso-8859-1']

    def __init__(self, mailbox_owner: str = "", mailbox_email: str = ""):
        """
        Initialize extractor.

        Args:
            mailbox_owner: Default sender name for fallback
            mailbox_email: Default sender email for fallback
        """
        self.mailbox_owner = mailbox_owner
        self.mailbox_email = mailbox_email or f"{mailbox_owner.lower().replace(' ', '')}@unknown" if mailbox_owner else ""

    def is_encrypted(self, data: bytes) -> bool:
        """Check if data appears to be encrypted/binary."""
        if not data or len(data) < 2:
            return False

        control_count = sum(1 for b in data if b < 32 and b not in (9, 10, 13))
        high_byte_count = sum(1 for b in data if b >= 128)

        # Encrypted if starts with control char and has high bytes
        if data[0] < 32 and high_byte_count > 0:
            return True

        # Encrypted if >30% control characters
        if control_count > len(data) * 0.3:
            return True

        return False

    def try_decode(self, data: bytes) -> str:
        """Try to decode bytes using multiple encodings."""
        if not data:
            return ""

        if self.is_encrypted(data):
            return ""

        # Check for UTF-16-LE pattern
        if len(data) >= 4 and data[1] == 0 and data[3] == 0:
            try:
                text = data.decode('utf-16-le').rstrip('\x00')
                if text and all(c.isprintable() or c.isspace() for c in text):
                    return text
            except:
                pass

        # Try standard encodings
        for encoding in self.ENCODINGS:
            try:
                text = data.decode(encoding)
                printable = sum(1 for c in text if c.isprintable() or c.isspace())
                if printable >= len(text) * 0.8:
                    return text.rstrip('\x00')
            except:
                continue

        return ""

    def extract_from_property_blob(self, blob: bytes, field: str) -> str:
        """
        Extract a specific field from PropertyBlob.

        Args:
            blob: PropertyBlob data
            field: Field to extract ('subject', 'sender', 'message_id')

        Returns:
            Extracted string value
        """
        if not blob:
            return ""

        if field == 'sender':
            return self._extract_sender(blob)
        elif field == 'subject':
            return self._extract_subject(blob)
        elif field == 'message_id':
            return self._extract_message_id(blob)

        return ""

    def _extract_sender(self, blob: bytes) -> str:
        """
        Extract sender name from PropertyBlob using M marker pattern.

        PropertyBlob structure contains:
        - M + length_byte + sender_name (e.g., M\x0d + "Rosetta Stone")
        - The sender name is typically at a fixed offset after decompression
        """
        if not blob or len(blob) < 50:
            return ""

        # Try to decompress blob first
        try:
            from dissect.esedb.compression import decompress as dissect_decompress
            blob = dissect_decompress(blob)
        except:
            pass

        # Search for M marker pattern with name-like content
        # Look for M + length (5-30) + ASCII text that looks like a name
        for i in range(len(blob) - 10):
            if blob[i] == ord('M') and 5 <= blob[i+1] <= 40:
                length = blob[i+1]
                if i + 2 + length > len(blob):
                    continue

                text_data = blob[i+2:i+2+length]

                # Check if this looks like a name (mostly letters and spaces)
                if not text_data:
                    continue

                # Skip if it looks like email (has @) - we want name, not email
                if b'@' in text_data:
                    continue

                # Skip if it looks like Message-ID
                if text_data.startswith(b'<'):
                    continue

                # Skip folder/system names
                skip_patterns = [b'Junk', b'Inbox', b'Sent', b'Deleted', b'Drafts',
                                b'Microsoft', b'Exchange', b'System', b'Recovery',
                                b'Calendar', b'Contacts', b'Tasks', b'/O=', b'/OU=',
                                b'CN=', b'Rule', b'http', b'schema']
                if any(p in text_data for p in skip_patterns):
                    continue

                # Check if mostly ASCII letters and spaces
                printable = sum(1 for b in text_data if (65 <= b <= 90) or (97 <= b <= 122) or b == 32)
                if printable >= length * 0.7:
                    try:
                        name = text_data.decode('ascii', errors='ignore').strip()
                        # Must have at least one space (First Last name pattern) or be a single word
                        if name and (len(name) >= 3):
                            # Validate it looks like a name
                            if all(c.isalpha() or c.isspace() for c in name):
                                return name
                    except:
                        pass

        return ""

    def _extract_sender_email(self, blob: bytes) -> str:
        """
        Extract sender email from PropertyBlob using M marker pattern.

        Looks for M + length + email@domain pattern.
        """
        if not blob or len(blob) < 50:
            return ""

        # Try to decompress blob first
        try:
            from dissect.esedb.compression import decompress as dissect_decompress
            blob = dissect_decompress(blob)
        except:
            pass

        # Search for M marker pattern with email content
        for i in range(len(blob) - 10):
            if blob[i] == ord('M') and 10 <= blob[i+1] <= 60:
                length = blob[i+1]
                if i + 2 + length > len(blob):
                    continue

                text_data = blob[i+2:i+2+length]

                # Check if this looks like an email (has @)
                if b'@' not in text_data:
                    continue

                # Skip Message-ID
                if text_data.startswith(b'<'):
                    continue

                try:
                    email = text_data.decode('ascii', errors='ignore').strip()
                    # Validate email format
                    if '@' in email and '.' in email.split('@')[1]:
                        # Clean up any trailing garbage
                        if email.endswith('audit'):
                            email = email[:-5]
                        return email
                except:
                    pass

        return ""

    def _extract_recipient_from_display_to(self, display_to: bytes) -> str:
        """
        Extract recipient name from DisplayTo column.

        DisplayTo is often compressed with LZXPRESS, then UTF-16 LE encoded.
        """
        if not display_to or len(display_to) < 4:
            return ""

        text = ""

        # Try to decompress
        try:
            from dissect.esedb.compression import decompress as dissect_decompress
            decompressed = dissect_decompress(display_to)
            # Decode as UTF-16 LE
            text = decompressed.decode('utf-16-le', errors='ignore').rstrip('\x00')
        except:
            pass

        # Try direct UTF-16 decode if decompress failed
        if not text:
            try:
                if len(display_to) >= 2 and display_to[1] == 0:
                    text = display_to.decode('utf-16-le', errors='ignore').rstrip('\x00')
            except:
                pass

        if not text:
            return ""

        # Clean up the extracted text
        # Remove 'audit' suffix
        if text.endswith('audit'):
            text = text[:-5]

        # Handle LDAP paths like "lab.sith.uz/ADPortal/Users/Name"
        if '/' in text:
            parts = text.split('/')
            for part in reversed(parts):
                # Skip audit suffixes
                if part.endswith('audit'):
                    part = part[:-5]
                # Skip empty parts
                if not part:
                    continue
                # Skip domain parts (have dots but no spaces, like lab.sith.uz)
                if '.' in part and ' ' not in part:
                    continue
                # Skip AD-specific parts
                if part.startswith('AD') or part.startswith('OU=') or part.startswith('CN='):
                    continue
                # Found a valid name
                if part:
                    return part.strip()

        # If it's just a domain name (has dots, no spaces), return empty
        if '.' in text and ' ' not in text:
            return ""

        return text.strip()

    def _parse_rfc822_headers(self, body_text: str) -> dict:
        """Parse RFC822 headers from message body to get real From/To."""
        headers = {}
        if not body_text:
            return headers

        lines = body_text.split('\n')
        current_header = None
        current_value = []

        for line in lines[:50]:  # Only check first 50 lines for headers
            # Empty line marks end of headers
            if not line.strip():
                if current_header:
                    headers[current_header] = ' '.join(current_value).strip()
                break

            # Check for header line
            if ':' in line and not line.startswith(' ') and not line.startswith('\t'):
                # Save previous header
                if current_header:
                    headers[current_header] = ' '.join(current_value).strip()

                parts = line.split(':', 1)
                current_header = parts[0].strip()
                current_value = [parts[1].strip()] if len(parts) > 1 else []
            elif current_header and (line.startswith(' ') or line.startswith('\t')):
                # Continuation line
                current_value.append(line.strip())

        return headers

    def _extract_name_from_header(self, header_value: str) -> tuple:
        """Extract name and email from header like 'Name <email@domain>'."""
        if not header_value:
            return "", ""

        import re
        # Match "Name <email>" pattern
        match = re.match(r'^([^<]+)\s*<([^>]+)>', header_value)
        if match:
            return match.group(1).strip(), match.group(2).strip()

        # Match just email
        if '@' in header_value:
            email = header_value.strip()
            name = email.split('@')[0]
            return name, email

        return header_value.strip(), ""

    def _extract_subject(self, blob: bytes, sender_name: str = "") -> str:
        """
        Extract subject from PropertyBlob.

        PropertyBlob structure: ... <sender_name> M <length> <subject_data> ...
        The sender name is followed by M marker (0x4d), then a length byte,
        then the subject bytes. Works on raw blob (no decompression needed).
        """
        if not blob or len(blob) < 50:
            return ""

        # Strategy: find "<sender_name>M" pattern in raw blob, extract subject after it
        if sender_name and len(sender_name) >= 3:
            # Try full sender name + M, then progressively shorter suffixes
            sender_bytes = sender_name.encode('ascii', errors='ignore')
            for suffix_len in range(len(sender_bytes), max(2, len(sender_bytes) - 6), -1):
                pattern = sender_bytes[-suffix_len:] + b'M'
                pos = blob.find(pattern)
                if pos >= 0:
                    subject_start = pos + len(pattern)
                    result = self._extract_subject_at_position(blob, subject_start)
                    if result:
                        return result
                    break  # Found pattern but no valid subject, try fallback

        # Fallback: scan M/K markers for first readable non-system text
        skip_words = [b'admin', b'exchange', b'recipient', b'fydib',
                      b'pdlt', b'group', b'index', b'system', b'mailbox',
                      b'/O=', b'/OU=', b'CN=', b'EX:', b'http']
        sender_lower = sender_name.lower().encode('ascii', errors='ignore') if sender_name else b''

        for i in range(len(blob) - 5):
            if blob[i] not in (0x4d, 0x4b):  # M or K marker
                continue
            length = blob[i + 1]
            if length < 2 or length > 100:
                continue
            if i + 2 + length > len(blob):
                continue
            potential = blob[i + 2:i + 2 + length]
            # Must be printable
            if not all(32 <= b <= 126 for b in potential):
                continue
            text_lower = potential.lower()
            # Skip system strings
            if any(w in text_lower for w in skip_words):
                continue
            # Skip emails and Message-IDs
            if b'@' in potential or potential.startswith(b'<'):
                continue
            # Skip if it matches sender name
            if sender_lower and text_lower.strip() == sender_lower.strip():
                continue
            text = potential.decode('ascii', errors='ignore')
            if len(text) >= 2:
                return text

        return ""

    def _extract_subject_at_position(self, blob: bytes, pos: int) -> str:
        """Extract subject data starting at position (right after senderM marker)."""
        if pos >= len(blob) - 2:
            return ""

        length = blob[pos]
        if length < 2 or length > 100:
            return ""
        if pos + 1 + length > len(blob):
            return ""

        subject_data = blob[pos:pos + 1 + length]
        content = subject_data[1:]  # Skip length byte

        # Check for repeat pattern encoding (AAAA BBBB style)
        if self._looks_like_repeat_encoding(content):
            return self._decode_repeat_pattern(subject_data)

        # Skip Message-IDs
        if content and content[0] == 0x3c:  # '<'
            return ""

        # Extract printable text
        return self._extract_printable_text(content)

    def _extract_printable_text(self, data: bytes) -> str:
        """Extract printable ASCII text from bytes."""
        if not data:
            return ""

        # Remove null bytes and extract printable chars
        cleaned = bytes(b for b in data if b != 0 and 32 <= b < 127)
        if cleaned:
            return cleaned.decode('ascii', errors='ignore')
        return ""

    def _looks_like_repeat_encoding(self, data: bytes) -> bool:
        """Check if data uses repeat pattern encoding (char + 00 00)."""
        if not data or len(data) < 4:
            return False

        # Count char + 00 00 patterns
        pattern_count = 0
        i = 0
        while i < len(data) - 2:
            if (32 < data[i] <= 126 and  # Printable non-space
                data[i+1] == 0x00 and data[i+2] == 0x00):
                pattern_count += 1
                i += 3
            else:
                i += 1

        # If we have at least 2 such patterns, it's likely repeat encoding
        return pattern_count >= 2

    def _decode_repeat_pattern(self, data: bytes) -> str:
        """
        Decode repeat pattern encoding (char + 00 00 = char*4).

        Format (first byte is expected output length):
        - char + 00 00 = repeat char 4 times (e.g., 41 00 00 = "AAAA")
        - 0x20 = space (literal)
        - high_byte (0x80+) + byte = back-reference

        Example: AAAA BBBB CCCC (14 chars)
        0e 41 00 00 20 42 00 00 20 a8 01 43...
        where a8 01 is back-ref, 43='C' indicates "CCCC"
        """
        if not data or len(data) < 2:
            return ""

        expected_len = data[0]
        output = []
        i = 1  # Start after length byte

        while i < len(data) and len(''.join(output)) < expected_len + 5:
            b = data[i]

            # Primary pattern: printable char + 00 00 = repeat 4 times
            if (0x30 <= b <= 0x7a and  # 0-9, A-Z, a-z
                i + 2 < len(data) and
                data[i+1] == 0x00 and data[i+2] == 0x00):
                output.append(chr(b) * 4)
                i += 3
                continue

            # Space = literal
            if b == 0x20:
                output.append(' ')
                i += 1
                continue

            # 00 00 sequence - skip it and check next byte
            if b == 0x00 and i + 1 < len(data) and data[i+1] == 0x00:
                i += 2
                continue

            # High byte (0x80+) = back-reference
            # Look ahead for the next printable char (indicates repeated char)
            if b >= 0x80:
                # Scan ahead for the indicator character
                found_char = False
                for k in range(i + 1, min(i + 5, len(data))):
                    c = data[k]
                    # Found a letter (A-Z) or digit - this indicates the repeated char
                    if 0x41 <= c <= 0x5a or 0x30 <= c <= 0x39:
                        output.append(chr(c) * 4)
                        i = k + 1
                        found_char = True
                        break
                    # Stop at space (next word) or 00 00 pattern
                    if c == 0x20:
                        break
                    if c == 0x00 and k + 1 < len(data) and data[k+1] == 0x00:
                        break
                if not found_char:
                    i += 2 if i + 1 < len(data) else 1
                continue

            # Control byte (0x01-0x1f) - skip
            if b < 0x20:
                i += 1
                continue

            # Other printable (0x40-0x7e) that's not followed by 00 00
            # Usually part of back-ref or garbage - skip
            if 0x40 <= b <= 0x7e:
                i += 1
                continue

            i += 1

        result = ''.join(output)
        result = ' '.join(result.split())  # Normalize spaces

        # Trim to expected length
        if expected_len > 0 and len(result) > expected_len:
            result = result[:expected_len]

        return result.strip()

    def _extract_message_id(self, blob: bytes) -> str:
        """Extract Message-ID from PropertyBlob."""
        # Look for <...@...> pattern
        for i in range(len(blob) - 20):
            if blob[i] == 0x3c:  # '<'
                # Find closing '>'
                for j in range(i+1, min(i+100, len(blob))):
                    if blob[j] == 0x3e:  # '>'
                        potential = blob[i:j+1]
                        # Clean up nulls
                        cleaned = bytes(b for b in potential if b != 0)
                        try:
                            msg_id = cleaned.decode('ascii')
                            if '@' in msg_id and msg_id.startswith('<') and msg_id.endswith('>'):
                                return msg_id
                        except:
                            pass
                        break
        return ""

    def extract_message(self, record, col_map: dict, rec_idx: int,
                       folder_name: str = "", tables: dict = None,
                       mailbox_num: int = 0, headers_only: bool = False) -> EmailMessage:
        """
        Extract a complete EmailMessage from a database record.

        Args:
            record: pyesedb record object
            col_map: Column name to index mapping
            rec_idx: Record index
            folder_name: Name of the folder
            tables: Dictionary of all tables (for attachments)
            mailbox_num: Mailbox number (for attachments)
            headers_only: If True, skip body and attachment extraction (fast mode for list views)

        Returns:
            EmailMessage object
        """
        msg = EmailMessage()
        msg.record_index = rec_idx
        msg.folder_name = folder_name

        # Get PropertyBlob
        prop_blob = self._get_bytes(record, col_map.get('PropertyBlob', -1))
        msg._raw_property_blob = prop_blob or b''

        # Extract basic fields
        msg.message_document_id = self._get_int(record, col_map.get('MessageDocumentId', -1)) or 0

        # Dates
        msg.date_received = self._get_filetime(record, col_map.get('DateReceived', -1))
        msg.date_sent = self._get_filetime(record, col_map.get('DateSent', -1))

        # Flags
        msg.is_read = self._get_bool(record, col_map.get('IsRead', -1))
        msg.has_attachments = self._get_bool(record, col_map.get('HasAttachments', -1))
        msg.importance = self._get_int(record, col_map.get('Importance', -1)) or 1
        msg.sensitivity = self._get_int(record, col_map.get('Sensitivity', -1)) or 0

        # Message class
        msg_class = self._get_string(record, col_map.get('MessageClass', -1))
        if msg_class:
            msg.message_class = msg_class

        # Extract sender/email from PropertyBlob first
        if prop_blob:
            msg.sender_name = self._extract_sender(prop_blob)
            msg.sender_email = self._extract_sender_email(prop_blob)
            msg.message_id = self._extract_message_id(prop_blob)

        # Extract recipient from DisplayTo column
        display_to = self._get_bytes(record, col_map.get('DisplayTo', -1))
        if display_to:
            recipient_name = self._extract_recipient_from_display_to(display_to)
            if recipient_name:
                msg.to_names = [recipient_name]
                clean_name = recipient_name.lower().replace(' ', '')
                msg.to_emails = [f"{clean_name}@unknown"]

        # Get body content - extract BEFORE subject so we can use RFC822 sender
        native_body = self._get_long_value(record, col_map.get('NativeBody', -1))
        body_text = ""
        if native_body:
            msg.body_html, body_text = self._extract_body(native_body, prop_blob)
            msg.body_text = body_text

        # Parse RFC822 headers from body to get real sender for this email
        if body_text:
            headers = self._parse_rfc822_headers(body_text)
            if 'From' in headers:
                name, email = self._extract_name_from_header(headers['From'])
                if name:
                    msg.sender_name = name
                if email:
                    msg.sender_email = email

        # Fallback sender to mailbox owner if not found
        if not msg.sender_name and self.mailbox_owner:
            msg.sender_name = self.mailbox_owner

        # Now extract subject using this email's actual sender name
        if prop_blob:
            msg.subject = self._extract_subject(prop_blob, msg.sender_name)

        # Build sender email if missing
        if msg.sender_name and not msg.sender_email:
            msg.sender_email = f"{msg.sender_name.lower().replace(' ', '')}@unknown"
        elif not msg.sender_email and self.mailbox_email:
            msg.sender_email = self.mailbox_email

        # If no body found yet, try PropertyBlob
        if not msg.body_text and prop_blob:
            msg.body_text = self._extract_body_from_property_blob(prop_blob)

        # Load attachments (skip in headers_only mode for speed)
        if not headers_only and msg.has_attachments and tables and mailbox_num:
            msg.attachments = self._extract_attachments(record, col_map, tables, mailbox_num)

        return msg

    def _get_bytes(self, record, col_idx: int) -> Optional[bytes]:
        """Get raw bytes from record column."""
        if col_idx < 0:
            return None
        try:
            return record.get_value_data(col_idx)
        except:
            return None

    def _get_int(self, record, col_idx: int) -> Optional[int]:
        """Get integer value from record column."""
        if col_idx < 0:
            return None
        try:
            val = record.get_value_data(col_idx)
            if not val:
                return None
            if len(val) == 4:
                return struct.unpack('<I', val)[0]
            elif len(val) == 8:
                return struct.unpack('<Q', val)[0]
            elif len(val) == 2:
                return struct.unpack('<H', val)[0]
            elif len(val) == 1:
                return val[0]
        except:
            pass
        return None

    def _get_bool(self, record, col_idx: int) -> bool:
        """Get boolean value from record column."""
        val = self._get_bytes(record, col_idx)
        return bool(val and val != b'\x00')

    def _get_string(self, record, col_idx: int) -> str:
        """Get string value from record column."""
        val = self._get_bytes(record, col_idx)
        if not val:
            return ""
        return self.try_decode(val)

    def _get_filetime(self, record, col_idx: int) -> Optional[datetime]:
        """Get datetime from Windows FILETIME column."""
        if col_idx < 0:
            return None
        try:
            val = record.get_value_data(col_idx)
            if not val or len(val) != 8:
                return None
            filetime = struct.unpack('<Q', val)[0]
            if filetime == 0:
                return None
            unix_time = (filetime - 116444736000000000) / 10000000
            return datetime.fromtimestamp(unix_time, tz=timezone.utc)
        except:
            return None

    def _get_long_value(self, record, col_idx: int) -> Optional[bytes]:
        """Get Long Value data from record column."""
        if col_idx < 0:
            return None
        try:
            if record.is_long_value(col_idx):
                lv = record.get_value_data_as_long_value(col_idx)
                if lv and hasattr(lv, 'get_data'):
                    return lv.get_data()
        except:
            pass
        return None

    def _extract_body(self, native_body: bytes, prop_blob: bytes = None) -> Tuple[str, str]:
        """
        Extract HTML and plain text body from NativeBody.

        Returns:
            Tuple of (html_body, text_body)
        """
        if not native_body or len(native_body) < 7:
            return "", ""

        html_body = ""
        text_body = ""

        # Try LZXPRESS decompression
        try:
            from core.lzxpress import decompress_exchange_body, extract_text_from_html
            decompressed = decompress_exchange_body(native_body)
            if decompressed:
                html_body = decompressed.decode('utf-8', errors='replace')
                text_body = extract_text_from_html(decompressed)
        except ImportError:
            # Fallback: try to extract printable content
            header_type = native_body[0]
            content = native_body[7:] if header_type in [0x17, 0x18, 0x19] else native_body
            printable = bytes(b for b in content if 32 <= b <= 126 or b in [9, 10, 13])
            if printable:
                text_body = printable.decode('ascii', errors='ignore')
        except:
            pass

        return html_body, text_body

    def _extract_body_from_property_blob(self, blob: bytes) -> str:
        """Extract body text from PropertyBlob as fallback."""
        try:
            from core.lzxpress import extract_body_from_property_blob
            return extract_body_from_property_blob(blob)
        except:
            # Simple fallback: extract printable strings
            strings = re.findall(rb'[\x20-\x7e]{10,}', blob)
            if strings:
                return '\n'.join(s.decode('ascii', errors='ignore') for s in strings[:5])
        return ""

    def _extract_attachments(self, record, col_map: dict, tables: dict,
                            mailbox_num: int) -> List[EmailAttachment]:
        """Extract attachments for the message."""
        attachments = []

        attach_table = tables.get(f"Attachment_{mailbox_num}")
        if not attach_table:
            return attachments

        # Get attachment column map
        attach_col_map = {}
        for j in range(attach_table.get_number_of_columns()):
            col = attach_table.get_column(j)
            if col:
                attach_col_map[col.name] = j

        # Get SubobjectsBlob for attachment linking
        subobjects = self._get_bytes(record, col_map.get('SubobjectsBlob', -1))
        linked_inids = self._parse_subobjects(subobjects) if subobjects else []

        # Build Inid to record map
        inid_map = {}
        for i in range(attach_table.get_number_of_records()):
            try:
                att_rec = attach_table.get_record(i)
                if not att_rec:
                    continue
                inid = self._get_bytes(att_rec, attach_col_map.get('Inid', -1))
                if inid and len(inid) >= 4:
                    inid_val = struct.unpack('<I', inid[:4])[0]
                    inid_map[inid_val] = i
            except:
                pass

        # Load linked attachments
        for inid_val in linked_inids:
            if inid_val not in inid_map:
                continue

            try:
                att_rec = attach_table.get_record(inid_map[inid_val])
                if not att_rec:
                    continue

                # Get content
                content = self._get_bytes(att_rec, attach_col_map.get('Content', -1))
                if not content:
                    continue

                # Check for Long Value
                if len(content) == 4:
                    content_idx = attach_col_map.get('Content', -1)
                    if content_idx >= 0:
                        try:
                            if att_rec.is_long_value(content_idx):
                                lv = att_rec.get_value_data_as_long_value(content_idx)
                                if lv and hasattr(lv, 'get_data'):
                                    lv_data = lv.get_data()
                                    if lv_data:
                                        content = lv_data
                        except:
                            continue

                # Get filename
                prop_blob = self._get_bytes(att_rec, attach_col_map.get('PropertyBlob', -1))
                filename = self._extract_attachment_filename(prop_blob) or f"attachment_{inid_val}.bin"

                # Get content type
                content_type = self._extract_content_type(prop_blob) or "application/octet-stream"

                attachments.append(EmailAttachment(
                    filename=filename,
                    content_type=content_type,
                    data=content
                ))
            except:
                pass

        return attachments

    def _parse_subobjects(self, blob: bytes) -> List[int]:
        """Parse SubobjectsBlob to get attachment Inid values.

        SubobjectsBlob may be compressed (starts with certain markers like 0x0f).
        After decompression, attachments are marked with 0x21 followed by Inid byte.
        """
        if not blob:
            return []

        # Try to decompress the blob first (some SubobjectsBlobs are compressed)
        data = blob
        try:
            from dissect.esedb.compression import decompress as dissect_decompress
            decompressed = dissect_decompress(blob)
            if decompressed:
                data = decompressed
        except:
            pass  # Use raw blob if decompression fails

        # Parse using 0x21 pattern: 0x21 followed by Inid byte
        inids = []
        i = 0
        while i < len(data) - 1:
            if data[i] == 0x21:
                inids.append(data[i + 1])
                i += 2
            else:
                i += 1

        return inids

    def _extract_attachment_filename(self, blob: bytes) -> str:
        """Extract filename from attachment PropertyBlob."""
        if not blob:
            return ""

        extensions = [b'.txt', b'.xml', b'.doc', b'.docx', b'.pdf', b'.jpg',
                      b'.png', b'.xlsx', b'.xls', b'.zip', b'.eml', b'.msg']

        for ext in extensions:
            idx = blob.lower().find(ext)
            if idx >= 0:
                start = idx
                while start > 0 and 0x20 <= blob[start - 1] < 0x7f:
                    start -= 1
                filename = blob[start:idx + len(ext)]
                if len(filename) > len(ext):
                    return filename.decode('ascii', errors='ignore')

        return ""

    def _extract_content_type(self, blob: bytes) -> str:
        """Extract content type from attachment PropertyBlob."""
        if not blob:
            return ""

        mime_types = [
            (b'text/plain', 'text/plain'),
            (b'text/html', 'text/html'),
            (b'application/pdf', 'application/pdf'),
            (b'image/jpeg', 'image/jpeg'),
            (b'image/png', 'image/png'),
        ]

        for pattern, mime in mime_types:
            if pattern in blob:
                return mime

        return "application/octet-stream"
