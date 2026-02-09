"""EML file parser — extracts MAPI-compatible properties from .eml files.

Uses Python's built-in email module to parse RFC 5322 messages and
converts them into property dicts for the messaging layer.
"""

import email
import email.policy
import email.utils
from datetime import datetime, timezone
from pathlib import Path

from .utils import datetime_to_filetime
from .mapi.properties import MAPI_TO, MAPI_CC, MAPI_BCC


def parse_eml_file(filepath):
    """Parse an EML file and extract properties for PST message creation.

    Args:
        filepath: Path to the .eml file.

    Returns:
        Dict with keys:
            subject, body_text, body_html, message_class,
            sender_name, sender_email, delivery_time, submit_time,
            importance, sensitivity, has_attachments,
            recipients: [{name, email, recipient_type}],
            attachments: [{filename, data, mime_type, size}]
    """
    filepath = Path(filepath)
    with open(filepath, 'rb') as f:
        msg = email.message_from_binary_file(f, policy=email.policy.compat32)

    result = {
        'subject': (msg.get('Subject') or '(No Subject)'),
        'message_class': 'IPM.Note',
        'body_text': None,
        'body_html': None,
        'sender_name': '',
        'sender_email': '',
        'delivery_time': None,
        'submit_time': None,
        'importance': 1,  # Normal
        'priority': 0,
        'sensitivity': 0,
        'has_attachments': False,
        'recipients': [],
        'attachments': [],
    }

    # Parse sender
    from_header = msg.get('From') or ''
    if from_header:
        parsed = email.utils.parseaddr(from_header)
        result['sender_name'] = parsed[0] or parsed[1]
        result['sender_email'] = parsed[1]

    # Parse date
    date_header = msg.get('Date') or ''
    if date_header:
        parsed_date = email.utils.parsedate_to_datetime(date_header)
        ft = datetime_to_filetime(parsed_date)
        result['delivery_time'] = ft
        result['submit_time'] = ft

    if result['delivery_time'] is None:
        from .utils import filetime_now
        result['delivery_time'] = filetime_now()
        result['submit_time'] = result['delivery_time']

    # Parse importance / priority
    importance = (msg.get('Importance') or '').lower()
    x_priority = (msg.get('X-Priority') or '').strip()
    if importance == 'high' or x_priority in ('1', '2'):
        result['importance'] = 2
        result['priority'] = -1
    elif importance == 'low' or x_priority in ('4', '5'):
        result['importance'] = 0
        result['priority'] = 1

    # Parse recipients
    for header, rtype in [('To', MAPI_TO), ('Cc', MAPI_CC), ('Bcc', MAPI_BCC)]:
        addr_header = msg.get(header) or ''
        if addr_header:
            addrs = email.utils.getaddresses([addr_header])
            for name, addr in addrs:
                if addr:
                    result['recipients'].append({
                        'name': name or addr,
                        'email': addr,
                        'recipient_type': rtype,
                    })

    # Parse body and attachments
    if msg.is_multipart():
        _process_multipart(msg, result)
    else:
        content_type = msg.get_content_type()
        charset = msg.get_content_charset() or 'utf-8'
        if content_type == 'text/plain':
            result['body_text'] = _decode_payload(msg, charset)
        elif content_type == 'text/html':
            result['body_html'] = _decode_payload(msg, charset)
        else:
            # Single non-text part — treat as attachment
            _add_attachment(msg, result)

    result['has_attachments'] = len(result['attachments']) > 0

    return result


def _process_multipart(msg, result):
    """Recursively process multipart MIME message."""
    for part in msg.walk():
        content_type = part.get_content_type()
        content_disposition = str(part.get('Content-Disposition', ''))
        charset = part.get_content_charset() or 'utf-8'

        if part.is_multipart():
            continue

        if 'attachment' in content_disposition:
            _add_attachment(part, result)
        elif content_type == 'text/plain' and result['body_text'] is None:
            result['body_text'] = _decode_payload(part, charset)
        elif content_type == 'text/html' and result['body_html'] is None:
            result['body_html'] = _decode_payload(part, charset)
        elif content_type.startswith('image/') or content_type.startswith('application/'):
            _add_attachment(part, result)


def _decode_payload(part, charset='utf-8'):
    """Decode a MIME part's payload to string."""
    payload = part.get_payload(decode=True)
    if payload is None:
        return ''
    try:
        return payload.decode(charset)
    except (UnicodeDecodeError, LookupError):
        return payload.decode('utf-8', errors='replace')


def _add_attachment(part, result):
    """Extract attachment data from a MIME part."""
    payload = part.get_payload(decode=True)
    if payload is None:
        return

    filename = part.get_filename()
    if not filename:
        ext = part.get_content_type().split('/')[-1]
        filename = f'attachment_{len(result["attachments"])}.{ext}'

    result['attachments'].append({
        'filename': filename,
        'data': payload,
        'mime_type': part.get_content_type(),
        'size': len(payload),
    })
