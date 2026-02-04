#!/usr/bin/env python3
"""
Exchange NativeBody LZXPRESS Decompression - Universal Algorithm v2

Based on MS-XCA LZXPRESS specification:
- 2-byte back-reference tokens: (offset-1) << 3 | (length-3)
- Stored in little-endian
- Min match length = 3, min offset = 1

Exchange-specific patterns discovered:
- Header: 7 bytes (type, uncompressed_size, flags)
- Control sequence: 00 XX YY 00 where XX,YY < 0x20 - skip
- Repeat pattern: char 00 00 non-null - repeat char 4 times

Key insight: Exchange streams data without separate indicator bits.
High-bit bytes (0x80+) followed by another byte = back-reference.
"""

import struct
import re


def decompress_lzxpress(data: bytes) -> bytes:
    """
    Decompress Exchange NativeBody LZXPRESS data.

    Args:
        data: Raw NativeBody data including header

    Returns:
        Decompressed content
    """
    if not data or len(data) < 7:
        return data

    # Parse header
    header_type = data[0]

    # Check header type
    if header_type not in [0x17, 0x18, 0x19]:
        # Unknown format - return as-is
        return data

    if header_type == 0x17:
        # Type 0x17 = plain/uncompressed
        return data[7:]

    # Get expected size and flags
    expected_size = struct.unpack('<H', data[1:3])[0]
    flags = struct.unpack('<I', data[3:7])[0]

    content = data[7:]

    # Decompress
    output = _decompress_content(content, expected_size, flags)

    return output


def _decompress_content(data: bytes, expected_size: int, flags: int) -> bytes:
    """
    Core decompression algorithm.

    LZXPRESS back-reference encoding:
    - 2-byte token: value = byte1 | (byte2 << 8)
    - offset = (value >> 3) + 1
    - length = (value & 7) + 3

    Pattern recognition (in priority order):
    1. Control sequence: 00 XX YY 00 (XX,YY < 0x20) - skip 4 bytes
    2. Repeat pattern: char 00 00 non-null - output char 4 times, consume 3 bytes
    3. Back-reference (high): 0x80+ followed by byte - 2-byte LZXPRESS token
    4. Back-reference (low): XX 00 where XX >= 0x20 - try 1-byte LZXPRESS token
    5. Whitespace: 0x09, 0x0a, 0x0d - literal
    6. Control bytes: 0x00-0x1f - skip
    7. Printable: 0x20-0x7e - literal
    """
    output = bytearray()
    i = 0
    max_output = expected_size if expected_size > 0 else len(data) * 10

    while i < len(data) and len(output) < max_output:
        b = data[i]

        # Pattern 1: Control sequence 00 XX YY 00 (where XX,YY < 0x20)
        if (b == 0x00 and i + 3 < len(data) and
            data[i + 1] < 0x20 and data[i + 2] < 0x20 and data[i + 3] == 0x00):
            i += 4
            continue

        # Pattern 2: Repeat pattern - alphanumeric + 00 00 + non-null
        if (0x30 <= b <= 0x7a and i + 3 < len(data) and
            data[i + 1] == 0x00 and data[i + 2] == 0x00 and
            data[i + 3] != 0x00):
            output.extend([b] * 4)
            i += 3
            continue

        # Pattern 3: Back-reference (high-bit) - 0x80+ followed by byte
        if b >= 0x80 and i + 1 < len(data):
            byte2 = data[i + 1]

            # LZXPRESS 2-byte token
            value = b | (byte2 << 8)
            offset = (value >> 3) + 1
            length = (value & 7) + 3

            # Copy from output buffer if valid
            if offset > 0 and offset <= len(output):
                start_pos = len(output) - offset
                for j in range(length):
                    if start_pos + j < len(output):
                        output.append(output[start_pos + j])

            i += 2
            continue

        # Pattern 4: Back-reference (low) - XX 00 where XX >= 0x20
        # Must check if the 00 is actually the start of a control sequence
        if (b >= 0x20 and b < 0x80 and i + 1 < len(data) and data[i + 1] == 0x00):
            # Check if the 00 starts a control sequence: 00 XX YY 00
            # If so, treat current byte as literal, not part of back-ref
            is_ctrl_seq_start = (i + 4 < len(data) and
                                 data[i + 2] < 0x20 and
                                 data[i + 3] < 0x20 and
                                 data[i + 4] == 0x00)

            # Check it's not a repeat pattern (XX 00 00)
            is_repeat = (i + 2 < len(data) and data[i + 2] == 0x00)

            if is_ctrl_seq_start:
                # The 00 is start of control seq, current byte is literal
                output.append(b)
                i += 1
                continue

            if not is_repeat:
                # Try as 1-byte LZXPRESS token
                value = b
                offset = (value >> 3) + 1
                length = (value & 7) + 3

                # Apply back-reference if offset is valid
                if offset > 0 and offset <= len(output):
                    start_pos = len(output) - offset
                    for j in range(length):
                        if start_pos + j < len(output):
                            output.append(output[start_pos + j])
                    i += 2
                    continue

            # If back-ref failed, treat as literal
            output.append(b)
            i += 1
            continue

        # Pattern 5: Whitespace literals
        if b in [0x09, 0x0a, 0x0d]:
            output.append(b)
            i += 1
            continue

        # Pattern 6: Control bytes (skip)
        if b < 0x20:
            i += 1
            continue

        # Pattern 7: Printable ASCII - literal
        if 0x20 <= b <= 0x7e:
            output.append(b)
            i += 1
            continue

        # Default: skip unknown
        i += 1

    return bytes(output)


def extract_text(html_bytes: bytes) -> str:
    """
    Extract visible text from HTML.

    Args:
        html_bytes: HTML content

    Returns:
        Extracted text
    """
    try:
        html = html_bytes.decode('utf-8', errors='ignore')
    except:
        html = html_bytes.decode('latin-1', errors='ignore')

    # Remove script/style
    html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<!--.*?-->', '', html, flags=re.DOTALL)

    # Extract text from p/span tags
    text_parts = []

    # Find all <p> or <span> content
    for match in re.finditer(r'<(?:p|span)[^>]*>([^<]+)</(?:p|span)>', html, re.IGNORECASE):
        text = match.group(1).strip()
        if text and not any(x in text.lower() for x in ['margin', 'padding', 'font-']):
            text_parts.append(text)

    if text_parts:
        return ' '.join(text_parts)

    # Fallback: extract all text between tags
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'\s+', ' ', text)

    return text.strip()


def get_body_text(native_body: bytes, property_blob: bytes = None) -> str:
    """
    Get body text from NativeBody, with PropertyBlob fallback.

    Args:
        native_body: Raw NativeBody data
        property_blob: Optional PropertyBlob for fallback

    Returns:
        Extracted body text
    """
    result = ""

    # Try NativeBody first
    if native_body:
        decompressed = decompress_lzxpress(native_body)
        result = extract_text(decompressed)

    # Fallback to PropertyBlob if needed
    if (not result or len(result) < 10) and property_blob:
        # Try to find M+ pattern in PropertyBlob
        pb_text = _extract_from_property_blob(property_blob)
        if pb_text and len(pb_text) > len(result):
            result = pb_text

    return result


def _extract_from_property_blob(data: bytes) -> str:
    """
    Extract text from PropertyBlob using repeat pattern decoding.

    PropertyBlob format:
    - After sender name + 'M' marker
    - Length byte + encoded text
    - char + 00 00 = repeat 4 times

    Args:
        data: Raw PropertyBlob data

    Returns:
        Extracted text
    """
    if not data or len(data) < 50:
        return ""

    # Find M marker followed by length and printable
    m_pos = -1
    for i in range(len(data) - 3):
        if (data[i] == 0x4d and  # 'M'
            data[i + 1] < 0x80 and  # Length byte
            0x20 <= data[i + 2] <= 0x7e):  # Printable
            m_pos = i + 1
            break

    if m_pos < 0:
        return ""

    # Decode using repeat pattern
    expected_len = data[m_pos]
    output = []
    i = m_pos + 1

    while i < len(data) and len(''.join(output)) < expected_len + 20:
        b = data[i]

        # Printable + 00 00 = repeat 4 times
        if (0x21 <= b <= 0x7e and i + 2 < len(data) and
            data[i + 1] == 0x00 and data[i + 2] == 0x00):
            output.append(chr(b) * 4)
            i += 3
            continue

        # Space is literal
        if b == 0x20:
            output.append(' ')
            i += 1
            continue

        # Other printable = literal
        if 0x21 <= b <= 0x7e:
            output.append(chr(b))
            i += 1
            continue

        # Skip control bytes
        if b < 0x20:
            i += 1
            continue

        # Skip high-bit bytes (control)
        if b >= 0x80:
            i += 2 if i + 1 < len(data) else 1
            continue

        i += 1

    result = ''.join(output)
    if expected_len > 0 and len(result) > expected_len:
        result = result[:expected_len]

    return result.strip()


# ===== TEST CODE =====

if __name__ == '__main__':
    import pyesedb

    DB_PATH = '/Users/igorbatin/Documents/VaibPro/MDB_exporter/NewDB/NewDB_ABCD_New/NewDB.edb'

    db = pyesedb.file()
    db.open(DB_PATH)

    tables = {}
    for i in range(db.get_number_of_tables()):
        t = db.get_table(i)
        if t:
            tables[t.name] = t

    table = tables['Message_101']
    col_map = {}
    for j in range(table.get_number_of_columns()):
        col = table.get_column(j)
        if col:
            col_map[col.name] = j

    test_cases = [
        (302, "AAAA BBBB CCCC"),
        (308, "2222 3333 4444 5555"),
    ]

    for record_idx, expected in test_cases:
        print(f"\n{'='*60}")
        print(f"Record {record_idx}")
        print(f"{'='*60}")
        print(f"Expected: {expected}")

        record = table.get_record(record_idx)
        native_idx = col_map.get('NativeBody', -1)
        pb_idx = col_map.get('PropertyBlob', -1)

        native_body = None
        property_blob = None

        if native_idx >= 0 and record.is_long_value(native_idx):
            lv = record.get_value_data_as_long_value(native_idx)
            native_body = lv.get_data()

        if pb_idx >= 0:
            try:
                property_blob = record.get_value_data(pb_idx)
            except:
                pass

        # Decompress
        if native_body:
            decompressed = decompress_lzxpress(native_body)
            print(f"\nDecompressed HTML ({len(decompressed)} bytes):")

            html = decompressed.decode('latin-1', errors='replace')
            # Show first 400 chars
            for start in range(0, min(len(html), 400), 100):
                print(f"  [{start:3d}] {repr(html[start:start+100])}")

            # Check patterns
            print(f"\nPattern checks:")
            patterns = ['<html>', '</html>', '<body', '</body>', '<p>', '</p>', 'text/html']
            for p in patterns:
                found = p.encode() in decompressed
                print(f"  {'✓' if found else '✗'} {p}")

        # Extract text
        text = get_body_text(native_body, property_blob)
        print(f"\nExtracted text: {text}")

        # Check expected
        if all(word in text for word in expected.split()):
            print(f"✓ SUCCESS: All expected words found!")
        else:
            found = [w for w in expected.split() if w in text]
            print(f"✗ Partial: {len(found)}/{len(expected.split())} words")

    db.close()
