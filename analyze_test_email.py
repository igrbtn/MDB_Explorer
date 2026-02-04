#!/usr/bin/env python3
"""
Analyze test emails to understand compression patterns.

Usage:
    python3 analyze_test_email.py <path_to_edb> [table_name]

Example:
    python3 analyze_test_email.py /path/to/NewDB.edb Message_101
"""

import sys
import pyesedb
import re

def hex_dump(data, width=16, max_lines=50):
    """Format data as hex dump with ASCII."""
    lines = []
    for i in range(0, min(len(data), width * max_lines), width):
        chunk = data[i:i+width]
        hex_str = ' '.join(f'{b:02x}' for b in chunk)
        ascii_str = ''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)
        lines.append(f'{i:04x}: {hex_str:<{width*3}}  {ascii_str}')
    return '\n'.join(lines)


def find_test_patterns(data, patterns):
    """Find test patterns in data."""
    results = []
    for pattern in patterns:
        pattern_bytes = pattern.encode('latin1')
        pos = 0
        while True:
            idx = data.find(pattern_bytes, pos)
            if idx < 0:
                break
            results.append((idx, pattern))
            pos = idx + 1
    return sorted(results, key=lambda x: x[0])


def analyze_control_bytes(data, start_pos, end_pos):
    """Analyze control bytes in a section."""
    controls = []
    i = start_pos
    while i < end_pos:
        b = data[i]
        if b >= 0x80:
            # High-bit control
            ctrl_bytes = [b]
            j = i + 1
            while j < end_pos and j < i + 5:
                next_b = data[j]
                if next_b >= 0x80 or next_b < 0x20:
                    ctrl_bytes.append(next_b)
                    j += 1
                else:
                    break
            controls.append((i, ctrl_bytes))
            i = j
        elif b < 0x20 and b not in [0x09, 0x0a, 0x0d]:
            # Low control byte
            if i + 1 < end_pos:
                controls.append((i, [b, data[i+1]]))
                i += 2
            else:
                i += 1
        else:
            i += 1
    return controls


def analyze_record(rec, col_map, patterns_to_find):
    """Analyze a single record."""
    print("\n" + "=" * 70)

    # Get PropertyBlob
    pb_idx = col_map.get('PropertyBlob', -1)
    pb_data = None
    if pb_idx >= 0:
        try:
            pb_data = rec.get_value_data(pb_idx)
        except:
            pass

    if not pb_data:
        print("No PropertyBlob data")
        return

    print(f"PropertyBlob: {len(pb_data)} bytes")

    # Find all readable strings
    strings = []
    current = []
    start = 0
    for i, b in enumerate(pb_data):
        if 32 <= b <= 126:
            if not current:
                start = i
            current.append(chr(b))
        else:
            if len(current) >= 3:
                strings.append((start, ''.join(current)))
            current = []
    if len(current) >= 3:
        strings.append((start, ''.join(current)))

    print("\nReadable strings:")
    for pos, text in strings:
        # Highlight our test patterns
        highlighted = text
        for pattern in patterns_to_find:
            if pattern in text:
                highlighted = f"*** {text} ***"
                break
        print(f"  0x{pos:04x}: {highlighted}")

    # Find test patterns
    print("\nTest pattern locations:")
    found_patterns = find_test_patterns(pb_data, patterns_to_find)
    for pos, pattern in found_patterns:
        print(f"  '{pattern}' at 0x{pos:04x}")
        # Show context
        start = max(0, pos - 5)
        end = min(len(pb_data), pos + len(pattern) + 10)
        context = pb_data[start:end]
        print(f"    Context: {context.hex()}")

    # Analyze control bytes after first pattern occurrence
    if found_patterns:
        first_pos = found_patterns[0][0]
        # Find body section (usually after M+ or similar marker)
        body_start = pb_data.find(b'M+', first_pos)
        if body_start < 0:
            body_start = first_pos + 50

        print(f"\nControl bytes analysis (from 0x{body_start:04x}):")
        controls = analyze_control_bytes(pb_data, body_start, min(body_start + 200, len(pb_data)))
        for pos, ctrl_bytes in controls[:20]:
            hex_ctrl = ' '.join(f'{b:02x}' for b in ctrl_bytes)
            # Try to interpret
            if len(ctrl_bytes) >= 2:
                last_byte = ctrl_bytes[-1]
                print(f"  0x{pos:04x}: [{hex_ctrl}] - last byte (length?): {last_byte}")

    # Full hex dump of body section
    print("\nBody section hex dump:")
    # Find where body might start
    m_plus = pb_data.find(b'M+')
    if m_plus >= 0:
        print(f"(Starting from M+ at 0x{m_plus:04x})")
        print(hex_dump(pb_data[m_plus:], max_lines=30))
    else:
        # Just show last 300 bytes
        start = max(0, len(pb_data) - 300)
        print(f"(Last 300 bytes from 0x{start:04x})")
        print(hex_dump(pb_data[start:], max_lines=30))


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 analyze_test_email.py <path_to_edb> [table_name] [patterns...]")
        print("\nExample:")
        print("  python3 analyze_test_email.py /path/to/NewDB.edb Message_101 ALPHA BETA")
        return

    db_path = sys.argv[1]
    table_name = sys.argv[2] if len(sys.argv) > 2 else None
    patterns = sys.argv[3:] if len(sys.argv) > 3 else ['ALPHA', 'BETA', 'GAMMA', 'REPEAT', 'TEST']

    print(f"Opening: {db_path}")
    print(f"Looking for patterns: {patterns}")

    db = pyesedb.file()
    db.open(db_path)

    # Find message tables
    message_tables = []
    for i in range(db.get_number_of_tables()):
        t = db.get_table(i)
        if t and t.name.startswith('Message_'):
            if table_name is None or t.name == table_name:
                message_tables.append(t.name)

    print(f"\nMessage tables: {message_tables}")

    for table_name in message_tables:
        table = db.get_table_by_name(table_name)
        if not table:
            continue

        record_count = table.get_number_of_records()
        print(f"\n{'='*70}")
        print(f"Table: {table_name} ({record_count} records)")
        print('='*70)

        # Get column map
        col_map = {}
        for j in range(table.get_number_of_columns()):
            col = table.get_column(j)
            if col:
                col_map[col.name] = j

        # Search for records containing our patterns
        found_records = []
        for rec_idx in range(record_count):
            rec = table.get_record(rec_idx)
            if not rec:
                continue

            pb_idx = col_map.get('PropertyBlob', -1)
            if pb_idx < 0:
                continue

            try:
                pb_data = rec.get_value_data(pb_idx)
                if pb_data:
                    for pattern in patterns:
                        if pattern.encode('latin1') in pb_data:
                            found_records.append(rec_idx)
                            break
            except:
                pass

        print(f"\nRecords containing test patterns: {found_records}")

        # Analyze each found record
        for rec_idx in found_records[-5:]:  # Last 5 matching records
            print(f"\n{'='*70}")
            print(f"Record {rec_idx}")
            rec = table.get_record(rec_idx)
            analyze_record(rec, col_map, patterns)

    db.close()


if __name__ == '__main__':
    main()
