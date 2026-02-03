#!/usr/bin/env python3
"""
Exchange NativeBody Decompression and Text Extraction

Exchange stores HTML body content with compression. The format uses:
- 7-byte header (type marker, uncompressed size, flags)
- Compressed content with literal bytes and back-references

This module provides practical text extraction from Exchange NativeBody data,
handling the compression artifacts gracefully.
"""

import struct
import re


def decompress_exchange_body(data: bytes) -> bytes:
    """
    Process Exchange NativeBody data and extract readable HTML.

    Exchange header format (7 bytes):
    - Byte 0: 0x18 or 0x19 (compression type marker)
    - Bytes 1-2: Uncompressed size (little-endian 16-bit)
    - Bytes 3-6: Flags/reserved

    Args:
        data: Raw NativeBody data from Exchange

    Returns:
        Processed HTML content with compression artifacts cleaned up
    """
    if not data or len(data) < 7:
        return data

    # Check for Exchange compression header
    if data[0] not in [0x18, 0x19]:
        # Not compressed or different format
        return data

    # Skip 7-byte header
    content = data[7:]

    # Build output by keeping printable bytes and reconstructing HTML structure
    output = bytearray()
    i = 0

    while i < len(content):
        b = content[i]

        # Keep printable ASCII and common whitespace
        if 0x20 <= b <= 0x7e or b in [0x09, 0x0a, 0x0d]:
            output.append(b)
            i += 1
        # Handle null bytes - often separate tokens
        elif b == 0x00:
            # Don't add space if previous char is already whitespace or tag char
            if output and output[-1] not in [0x20, 0x3c, 0x3e, 0x0a, 0x0d]:
                output.append(0x20)  # Replace with space
            i += 1
        # Skip control bytes
        else:
            i += 1

    return bytes(output)


def extract_text_from_html(html_bytes: bytes) -> str:
    """
    Extract visible text content from HTML bytes.

    Handles HTML with compression artifacts by:
    1. Removing script/style/comments
    2. Extracting text between tags
    3. Filtering to readable content

    Args:
        html_bytes: HTML content (possibly with compression artifacts)

    Returns:
        Extracted text content
    """
    try:
        html = html_bytes.decode('utf-8', errors='ignore')
    except:
        html = html_bytes.decode('latin-1', errors='ignore')

    # Remove script and style content
    html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<!--.*?-->', '', html, flags=re.DOTALL)

    # Find content between paragraph tags specifically
    # This is where actual email body content usually is
    body_parts = []

    # Look for <p>content</p> patterns (including mangled tags like "p>content")
    p_matches = re.findall(r'(?:<p[^>]*>|p>)([^<]*?)(?:</p|<|$)', html, re.IGNORECASE)
    for match in p_matches:
        text = match.strip()
        # Filter to meaningful content
        if text and len(text) >= 1:
            # Skip if it's just HTML artifacts or pure punctuation
            if not re.match(r'^[\s@#;:,.\-_{}()\[\]]+$', text):
                # Keep even single characters or numbers as they may be compressed body content
                body_parts.append(text)

    # Also look for content in div with specific wrapper classes
    div_matches = re.findall(r'<div[^>]*(?:wrapper|content|body)[^>]*>([^<]+)<', html, re.IGNORECASE)
    for match in div_matches:
        text = match.strip()
        if text and len(text) >= 1 and not re.match(r'^[\s@#;:,.\-_{}()\[\]]+$', text):
            body_parts.append(text)

    # Look for content after "p>" without proper opening tag (compressed data artifact)
    p_mangled = re.findall(r'(?:^|[^<])p>([^\s<][^<]*?)(?:<|/p|\s*$)', html, re.IGNORECASE)
    for match in p_mangled:
        text = match.strip()
        if text and len(text) >= 1 and text not in body_parts:
            if not re.match(r'^[\s@#;:,.\-_{}()\[\]]+$', text):
                body_parts.append(text)

    if body_parts:
        return '\n'.join(body_parts)

    # Fallback: General text extraction between any tags
    text_parts = []
    in_tag = False
    current = []
    skip_content = False

    for c in html:
        if c == '<':
            if current:
                text = ''.join(current).strip()
                # Filter meaningful text
                if text and len(text) >= 2:
                    # Skip CSS-like content
                    if not any(x in text.lower() for x in ['margin', 'padding', 'font-', 'color:', 'style', 'display']):
                        text_parts.append(text)
                current = []
            in_tag = True
        elif c == '>':
            in_tag = False
        elif not in_tag and not skip_content:
            if c.isprintable() or c in '\r\n\t':
                current.append(c)

    if current:
        text = ''.join(current).strip()
        if text and len(text) >= 2:
            text_parts.append(text)

    # Clean up and join
    result = '\n'.join(t for t in text_parts if t)

    # Clean up multiple spaces/newlines
    result = re.sub(r'[ \t]+', ' ', result)
    result = re.sub(r'\n\s*\n', '\n\n', result)

    return result.strip()


def get_body_preview(data: bytes, max_length: int = 500) -> str:
    """
    Get a preview of the email body content.

    Args:
        data: Raw NativeBody data
        max_length: Maximum preview length

    Returns:
        Text preview of the body content
    """
    if not data:
        return ""

    # Process the data
    processed = decompress_exchange_body(data)

    # Extract text
    text = extract_text_from_html(processed)

    if text and len(text) > max_length:
        text = text[:max_length] + "..."

    return text


if __name__ == '__main__':
    # Test with Exchange data
    import pyesedb

    db = pyesedb.file()
    db.open('/Users/igorbatin/Documents/VaibPro/MDB_exporter/Mailbox Database 0058949847 3/Mailbox Database 0058949847.edb')

    tables = {}
    for i in range(db.get_number_of_tables()):
        t = db.get_table(i)
        if t:
            tables[t.name] = t

    msg_table = tables['Message_103']
    col_map = {}
    for j in range(msg_table.get_number_of_columns()):
        col = msg_table.get_column(j)
        if col:
            col_map[col.name] = j

    # Test messages
    for rec_idx in [306, 308, 314]:
        rec = msg_table.get_record(rec_idx)
        native_idx = col_map.get('NativeBody', -1)

        if rec.is_long_value(native_idx):
            lv = rec.get_value_data_as_long_value(native_idx)
            if lv:
                raw_data = lv.get_data()
                print(f"\n{'='*60}")
                print(f"Message {rec_idx}")
                print(f"{'='*60}")
                print(f"Raw data: {len(raw_data)} bytes")

                processed = decompress_exchange_body(raw_data)
                print(f"Processed: {len(processed)} bytes")

                text = extract_text_from_html(processed)
                print(f"Extracted text: {text if text else '(none)'}")

    db.close()
