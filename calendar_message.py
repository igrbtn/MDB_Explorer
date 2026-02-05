#!/usr/bin/env python3
"""
Calendar Message Extraction Module for Exchange EDB databases.
Extracts calendar/appointment data and exports to iCalendar (.ics) format.
"""

import struct
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional, List
from email.utils import formataddr
import uuid
import re

# Try to import dissect for decompression
try:
    from dissect.esedb.compression import decompress as dissect_decompress
    HAS_DISSECT = True
except ImportError:
    HAS_DISSECT = False


# Calendar message class patterns
CALENDAR_MESSAGE_CLASSES = [
    'IPM.Appointment',
    'IPM.Schedule.Meeting.Request',
    'IPM.Schedule.Meeting.Resp.Pos',
    'IPM.Schedule.Meeting.Resp.Neg',
    'IPM.Schedule.Meeting.Resp.Tent',
    'IPM.Schedule.Meeting.Canceled',
    'IPM.Schedule.Meeting.Notification.Forward',
    'IPM.OLE.CLASS.{00061055-0000-0000-C000-000000000046}',  # Recurring appointment
    'IPM.Task',  # Tasks (can also be exported)
]


@dataclass
class CalendarAttendee:
    """Represents a calendar event attendee."""
    name: str = ""
    email: str = ""
    role: str = "REQ-PARTICIPANT"  # REQ-PARTICIPANT, OPT-PARTICIPANT, CHAIR
    status: str = "NEEDS-ACTION"   # ACCEPTED, DECLINED, TENTATIVE, NEEDS-ACTION


@dataclass
class CalendarEvent:
    """Represents a calendar event/appointment."""
    # Core fields
    uid: str = ""
    subject: str = ""
    description: str = ""
    location: str = ""

    # Time fields
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    all_day: bool = False

    # Organizer
    organizer_name: str = ""
    organizer_email: str = ""

    # Attendees
    attendees: List[CalendarAttendee] = field(default_factory=list)

    # Recurrence (simplified)
    is_recurring: bool = False
    recurrence_rule: str = ""  # RRULE format

    # Status
    status: str = "CONFIRMED"  # TENTATIVE, CONFIRMED, CANCELLED
    busy_status: str = "BUSY"  # FREE, TENTATIVE, BUSY, OOF

    # Reminder
    reminder_minutes: int = 0
    has_reminder: bool = False

    # Metadata
    message_class: str = ""
    record_index: int = 0
    created: Optional[datetime] = None
    modified: Optional[datetime] = None

    # Categories/labels
    categories: List[str] = field(default_factory=list)
    importance: str = "NORMAL"  # LOW, NORMAL, HIGH

    def to_ics(self) -> str:
        """Export event to iCalendar format."""
        lines = []

        # Header
        lines.append("BEGIN:VCALENDAR")
        lines.append("VERSION:2.0")
        lines.append("PRODID:-//Exchange EDB Exporter//EN")
        lines.append("CALSCALE:GREGORIAN")
        lines.append("METHOD:PUBLISH")

        # Event
        lines.append("BEGIN:VEVENT")

        # UID
        uid = self.uid or str(uuid.uuid4())
        lines.append(f"UID:{uid}")

        # Timestamps
        if self.created:
            lines.append(f"DTSTAMP:{self._format_datetime(self.created)}")
        else:
            lines.append(f"DTSTAMP:{self._format_datetime(datetime.now(timezone.utc))}")

        if self.modified:
            lines.append(f"LAST-MODIFIED:{self._format_datetime(self.modified)}")

        # Start/End times
        if self.start_time:
            if self.all_day:
                lines.append(f"DTSTART;VALUE=DATE:{self.start_time.strftime('%Y%m%d')}")
            else:
                lines.append(f"DTSTART:{self._format_datetime(self.start_time)}")

        if self.end_time:
            if self.all_day:
                lines.append(f"DTEND;VALUE=DATE:{self.end_time.strftime('%Y%m%d')}")
            else:
                lines.append(f"DTEND:{self._format_datetime(self.end_time)}")

        # Subject/Summary
        if self.subject:
            lines.append(f"SUMMARY:{self._escape_text(self.subject)}")

        # Description
        if self.description:
            lines.append(f"DESCRIPTION:{self._escape_text(self.description)}")

        # Location
        if self.location:
            lines.append(f"LOCATION:{self._escape_text(self.location)}")

        # Organizer
        if self.organizer_email:
            if self.organizer_name:
                lines.append(f"ORGANIZER;CN={self._escape_text(self.organizer_name)}:mailto:{self.organizer_email}")
            else:
                lines.append(f"ORGANIZER:mailto:{self.organizer_email}")

        # Attendees
        for attendee in self.attendees:
            if attendee.email:
                parts = [f"ATTENDEE"]
                if attendee.name:
                    parts.append(f"CN={self._escape_text(attendee.name)}")
                parts.append(f"ROLE={attendee.role}")
                parts.append(f"PARTSTAT={attendee.status}")
                parts.append(f"RSVP=TRUE:mailto:{attendee.email}")
                lines.append(";".join(parts))

        # Status
        lines.append(f"STATUS:{self.status}")

        # Transparency (busy status)
        if self.busy_status == "FREE":
            lines.append("TRANSP:TRANSPARENT")
        else:
            lines.append("TRANSP:OPAQUE")

        # Categories
        if self.categories:
            lines.append(f"CATEGORIES:{','.join(self.categories)}")

        # Priority/Importance
        if self.importance == "HIGH":
            lines.append("PRIORITY:1")
        elif self.importance == "LOW":
            lines.append("PRIORITY:9")
        else:
            lines.append("PRIORITY:5")

        # Recurrence
        if self.is_recurring and self.recurrence_rule:
            lines.append(f"RRULE:{self.recurrence_rule}")

        # Reminder/Alarm
        if self.has_reminder and self.reminder_minutes > 0:
            lines.append("BEGIN:VALARM")
            lines.append("ACTION:DISPLAY")
            lines.append(f"TRIGGER:-PT{self.reminder_minutes}M")
            lines.append(f"DESCRIPTION:Reminder: {self._escape_text(self.subject)}")
            lines.append("END:VALARM")

        lines.append("END:VEVENT")
        lines.append("END:VCALENDAR")

        return "\r\n".join(lines)

    def _format_datetime(self, dt: datetime) -> str:
        """Format datetime for iCalendar (UTC)."""
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        utc_dt = dt.astimezone(timezone.utc)
        return utc_dt.strftime("%Y%m%dT%H%M%SZ")

    def _escape_text(self, text: str) -> str:
        """Escape text for iCalendar format."""
        if not text:
            return ""
        # Escape special characters
        text = text.replace("\\", "\\\\")
        text = text.replace(";", "\\;")
        text = text.replace(",", "\\,")
        text = text.replace("\n", "\\n")
        text = text.replace("\r", "")
        return text


class CalendarExtractor:
    """Extracts calendar events from Exchange EDB database."""

    def __init__(self, mailbox_owner: str = "", mailbox_email: str = ""):
        self.mailbox_owner = mailbox_owner
        self.mailbox_email = mailbox_email

    def is_calendar_item(self, message_class: str) -> bool:
        """Check if message class represents a calendar item."""
        if not message_class:
            return False

        msg_class_upper = message_class.upper()
        for pattern in CALENDAR_MESSAGE_CLASSES:
            if msg_class_upper.startswith(pattern.upper()):
                return True

        return False

    def get_message_class(self, record, col_map: dict) -> str:
        """Get decompressed message class from record."""
        msg_class_idx = col_map.get('MessageClass', -1)
        if msg_class_idx < 0:
            return ""

        try:
            val = record.get_value_data(msg_class_idx)
            if not val:
                return ""

            # Try decompression
            if HAS_DISSECT:
                try:
                    decompressed = dissect_decompress(val)
                    for enc in ['utf-16-le', 'utf-8', 'ascii']:
                        try:
                            text = decompressed.decode(enc).rstrip('\x00')
                            if text:
                                return text
                        except:
                            pass
                except:
                    pass

            # Fallback to direct decode
            try:
                return val.decode('utf-8', errors='ignore').rstrip('\x00')
            except:
                return ""
        except:
            return ""

    def extract_event(self, record, col_map: dict, rec_idx: int) -> Optional[CalendarEvent]:
        """Extract calendar event from database record."""
        event = CalendarEvent()
        event.record_index = rec_idx

        # Get message class
        event.message_class = self.get_message_class(record, col_map)

        # Get timestamps
        event.created = self._get_filetime(record, col_map.get('DateCreated', -1))
        event.modified = self._get_filetime(record, col_map.get('LastModificationTime', -1))

        # Try to get calendar-specific times (may be in PropertyBlob)
        date_received = self._get_filetime(record, col_map.get('DateReceived', -1))
        date_sent = self._get_filetime(record, col_map.get('DateSent', -1))

        # Get PropertyBlob for detailed extraction
        prop_blob = self._get_bytes(record, col_map.get('PropertyBlob', -1))
        if prop_blob:
            self._extract_from_property_blob(event, prop_blob)

        # Use sent time as fallback start time if not extracted
        if not event.start_time and date_sent:
            event.start_time = date_sent
            event.end_time = date_sent + timedelta(hours=1)  # Default 1 hour duration

        # Set organizer from mailbox owner if not set
        if not event.organizer_name and self.mailbox_owner:
            event.organizer_name = self.mailbox_owner
            event.organizer_email = self.mailbox_email

        # Generate UID if not set
        if not event.uid:
            event.uid = f"{rec_idx}-{uuid.uuid4()}@exchange.local"

        return event

    def _extract_from_property_blob(self, event: CalendarEvent, blob: bytes):
        """Extract calendar properties from PropertyBlob."""
        if not blob or len(blob) < 10:
            return

        # Extract subject (reuse email extraction logic)
        event.subject = self._extract_text_field(blob, 'subject')

        # Extract location
        event.location = self._extract_text_field(blob, 'location')

        # Extract description/body
        event.description = self._extract_text_field(blob, 'body')

        # Look for time patterns in blob
        self._extract_times_from_blob(event, blob)

        # Look for attendee patterns
        self._extract_attendees_from_blob(event, blob)

    def _extract_text_field(self, blob: bytes, field_type: str) -> str:
        """Extract a text field from PropertyBlob."""
        # Similar to email subject extraction
        # Look for patterns based on field type

        if field_type == 'subject':
            # Use same logic as email subject extraction
            patterns = [b'StoneM', b'toneM', b'atorM']
            for pattern in patterns:
                pos = blob.find(pattern)
                if pos >= 0:
                    start = pos + len(pattern)
                    if start < len(blob):
                        length = blob[start]
                        if length > 0 and start + 1 + length <= len(blob):
                            data = blob[start + 1:start + 1 + length]
                            try:
                                return data.decode('utf-8', errors='ignore').strip()
                            except:
                                pass

        elif field_type == 'location':
            # Look for location markers
            for marker in [b'Location', b'LOCATION', b'location']:
                pos = blob.find(marker)
                if pos >= 0:
                    # Try to extract text after marker
                    start = pos + len(marker)
                    end = min(start + 200, len(blob))
                    segment = blob[start:end]
                    # Find printable text
                    text = self._extract_printable_text(segment, max_len=100)
                    if text:
                        return text

        return ""

    def _extract_printable_text(self, data: bytes, max_len: int = 200) -> str:
        """Extract printable ASCII text from bytes."""
        result = []
        for b in data:
            if 32 <= b < 127:
                result.append(chr(b))
            elif result and b == 0:
                break  # Null terminator

        text = ''.join(result).strip()
        return text[:max_len] if text else ""

    def _extract_times_from_blob(self, event: CalendarEvent, blob: bytes):
        """Try to extract start/end times from PropertyBlob."""
        # FILETIME values are 8 bytes, representing 100-nanosecond intervals since 1601-01-01
        # Look for potential FILETIME values in reasonable date ranges

        # This is a simplified approach - real implementation would parse MAPI properties
        pass

    def _extract_attendees_from_blob(self, event: CalendarEvent, blob: bytes):
        """Try to extract attendees from PropertyBlob."""
        # Look for email patterns
        email_pattern = rb'[\w.-]+@[\w.-]+\.\w+'

        try:
            # Decode blob for regex
            text = blob.decode('utf-8', errors='ignore')
            emails = re.findall(r'[\w.-]+@[\w.-]+\.\w+', text)

            for email in emails[:10]:  # Limit to 10 attendees
                if email != self.mailbox_email:  # Skip organizer
                    attendee = CalendarAttendee(email=email)
                    # Try to find name near email
                    event.attendees.append(attendee)
        except:
            pass

    def _get_bytes(self, record, col_idx: int) -> Optional[bytes]:
        """Get raw bytes from record column."""
        if col_idx < 0:
            return None
        try:
            return record.get_value_data(col_idx)
        except:
            return None

    def _get_filetime(self, record, col_idx: int) -> Optional[datetime]:
        """Get datetime from FILETIME column."""
        if col_idx < 0:
            return None
        try:
            val = record.get_value_data(col_idx)
            if not val or len(val) != 8:
                return None

            filetime = struct.unpack('<Q', val)[0]
            if filetime == 0:
                return None

            # Convert FILETIME to datetime
            # FILETIME is 100-nanosecond intervals since 1601-01-01
            EPOCH_DIFF = 116444736000000000  # Difference between 1601 and 1970 in 100ns
            timestamp = (filetime - EPOCH_DIFF) / 10000000

            return datetime.fromtimestamp(timestamp, tz=timezone.utc)
        except:
            return None


def export_calendar_to_ics(events: List[CalendarEvent], output_path: str) -> bool:
    """Export multiple calendar events to a single .ics file."""
    try:
        lines = []
        lines.append("BEGIN:VCALENDAR")
        lines.append("VERSION:2.0")
        lines.append("PRODID:-//Exchange EDB Exporter//EN")
        lines.append("CALSCALE:GREGORIAN")
        lines.append("METHOD:PUBLISH")

        for event in events:
            # Get individual event content without VCALENDAR wrapper
            event_ics = event.to_ics()
            # Extract just the VEVENT part
            start = event_ics.find("BEGIN:VEVENT")
            end = event_ics.find("END:VEVENT") + len("END:VEVENT")
            if start >= 0 and end > start:
                lines.append(event_ics[start:end])

        lines.append("END:VCALENDAR")

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("\r\n".join(lines))

        return True
    except Exception as e:
        print(f"Error exporting calendar: {e}")
        return False
