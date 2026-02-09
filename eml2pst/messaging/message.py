"""Message objects for PST Messaging layer.

A message has:
- Message PC: Property Context with all message properties
- Recipients TC (subnode): Table of recipients
- Attachments TC (subnode): Table of attachments

See [MS-PST] 2.4.5.
"""

from ..ltp.pc import build_pc_node
from ..ltp.tc import build_tc_node
from ..mapi.properties import (
    NID_TYPE_RECIPIENT_TABLE, NID_TYPE_ATTACHMENT_TABLE,
    PR_SUBJECT, PR_BODY, PR_HTML, PR_MESSAGE_CLASS,
    PR_MESSAGE_FLAGS, PR_MESSAGE_SIZE, PR_IMPORTANCE,
    PR_PRIORITY, PR_SENSITIVITY, PR_HASATTACH,
    PR_MESSAGE_DELIVERY_TIME, PR_CLIENT_SUBMIT_TIME,
    PR_CREATION_TIME, PR_LAST_MODIFICATION_TIME,
    PR_SENDER_NAME, PR_SENDER_EMAIL_ADDRESS, PR_SENDER_ADDRTYPE,
    PR_SENT_REPRESENTING_NAME, PR_SENT_REPRESENTING_EMAIL,
    PR_SENT_REPRESENTING_ADDRTYPE,
    PR_DISPLAY_NAME_W, PR_EMAIL_ADDRESS, PR_ADDRTYPE,
    PR_RECIPIENT_TYPE, PR_ROWID,
    PR_ATTACH_NUM, PR_ATTACH_METHOD, PR_ATTACH_FILENAME,
    PR_ATTACH_LONG_FILENAME, PR_ATTACH_SIZE, PR_ATTACH_DATA_BIN,
    PR_ATTACH_MIME_TAG, PR_RENDERING_POSITION,
    MSGFLAG_READ, MSGFLAG_HASATTACH, ATTACH_BY_VALUE,
    MAPI_TO, MAPI_CC, MAPI_BCC,
)
from ..utils import filetime_now


def build_message_pc(parsed_eml):
    """Build a message Property Context from parsed EML data.

    Args:
        parsed_eml: Dict with keys:
            subject, body_text, body_html, message_class,
            sender_name, sender_email, delivery_time,
            importance, sensitivity, has_attachments

    Returns:
        Raw bytes for message PC data block.
    """
    now = filetime_now()
    props = []

    subject = parsed_eml.get('subject', '(No Subject)')
    props.append((PR_SUBJECT, subject))
    props.append((PR_MESSAGE_CLASS, parsed_eml.get('message_class', 'IPM.Note')))

    flags = MSGFLAG_READ
    if parsed_eml.get('has_attachments', False):
        flags |= MSGFLAG_HASATTACH
    props.append((PR_MESSAGE_FLAGS, flags))

    if parsed_eml.get('body_text'):
        props.append((PR_BODY, parsed_eml['body_text']))
    if parsed_eml.get('body_html'):
        html = parsed_eml['body_html']
        if isinstance(html, str):
            html = html.encode('utf-8')
        props.append((PR_HTML, html))

    props.append((PR_IMPORTANCE, parsed_eml.get('importance', 1)))  # 1 = Normal
    props.append((PR_PRIORITY, parsed_eml.get('priority', 0)))  # 0 = Normal
    props.append((PR_SENSITIVITY, parsed_eml.get('sensitivity', 0)))  # 0 = Normal
    props.append((PR_HASATTACH, parsed_eml.get('has_attachments', False)))

    delivery_time = parsed_eml.get('delivery_time', now)
    submit_time = parsed_eml.get('submit_time', delivery_time)
    props.append((PR_MESSAGE_DELIVERY_TIME, delivery_time))
    props.append((PR_CLIENT_SUBMIT_TIME, submit_time))
    props.append((PR_CREATION_TIME, now))
    props.append((PR_LAST_MODIFICATION_TIME, now))

    body_text = parsed_eml.get('body_text') or ''
    body_size = len(body_text.encode('utf-16-le'))
    props.append((PR_MESSAGE_SIZE, body_size))

    # Sender
    sender_name = parsed_eml.get('sender_name', '')
    sender_email = parsed_eml.get('sender_email', '')
    if sender_name:
        props.append((PR_SENDER_NAME, sender_name))
        props.append((PR_SENT_REPRESENTING_NAME, sender_name))
    if sender_email:
        props.append((PR_SENDER_EMAIL_ADDRESS, sender_email))
        props.append((PR_SENDER_ADDRTYPE, 'SMTP'))
        props.append((PR_SENT_REPRESENTING_EMAIL, sender_email))
        props.append((PR_SENT_REPRESENTING_ADDRTYPE, 'SMTP'))

    return build_pc_node(props)


def build_recipients_tc(recipients):
    """Build a Recipients Table Context.

    Args:
        recipients: List of dicts with keys:
            name, email, recipient_type (MAPI_TO/CC/BCC)

    Returns:
        Raw bytes for recipients TC data block.
    """
    column_tags = [
        PR_DISPLAY_NAME_W,
        PR_EMAIL_ADDRESS,
        PR_ADDRTYPE,
        PR_RECIPIENT_TYPE,
        PR_ROWID,
    ]

    rows = []
    for i, recip in enumerate(recipients):
        row = {
            '_nid': i,
            PR_DISPLAY_NAME_W: recip.get('name', recip.get('email', '')),
            PR_EMAIL_ADDRESS: recip.get('email', ''),
            PR_ADDRTYPE: 'SMTP',
            PR_RECIPIENT_TYPE: recip.get('recipient_type', MAPI_TO),
            PR_ROWID: i,
        }
        rows.append(row)

    return build_tc_node(column_tags, rows)


def build_attachments_tc(attachments):
    """Build an Attachments Table Context.

    Args:
        attachments: List of dicts with keys:
            filename, data (bytes), mime_type, size

    Returns:
        Raw bytes for attachments TC data block.
    """
    column_tags = [
        PR_ATTACH_NUM,
        PR_ATTACH_METHOD,
        PR_ATTACH_LONG_FILENAME,
        PR_ATTACH_SIZE,
        PR_ATTACH_MIME_TAG,
        PR_RENDERING_POSITION,
    ]

    rows = []
    for i, att in enumerate(attachments):
        row = {
            '_nid': i,
            PR_ATTACH_NUM: i,
            PR_ATTACH_METHOD: ATTACH_BY_VALUE,
            PR_ATTACH_LONG_FILENAME: att.get('filename', f'attachment_{i}'),
            PR_ATTACH_SIZE: att.get('size', len(att.get('data', b''))),
            PR_ATTACH_MIME_TAG: att.get('mime_type', 'application/octet-stream'),
            PR_RENDERING_POSITION: 0xFFFFFFFF,
        }
        rows.append(row)

    return build_tc_node(column_tags, rows)


def message_nid_recipients(msg_nid):
    """Get recipients TC local descriptor NID for a message.

    Per [MS-PST] and libpff, this is the well-known fixed NID 0x0692.
    """
    return 0x0692


def message_nid_attachments(msg_nid):
    """Get attachments TC local descriptor NID for a message.

    Per [MS-PST] and libpff, this is the well-known fixed NID 0x0671.
    """
    return 0x0671
