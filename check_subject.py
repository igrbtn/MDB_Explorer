#!/usr/bin/env python3
"""
Check if the subject contains the referenced text.
Also look for patterns with repeated words.
"""

import pyesedb

# Check the NewDB lorem ipsum email
DB_PATH = '/Users/igorbatin/Documents/VaibPro/MDB_exporter/NewDB/NewDB_oneuser_rstone_loremipsum/NewDB.edb'

db = pyesedb.file()
db.open(DB_PATH)

table = db.get_table_by_name('Message_101')

# Get column map
col_map = {}
for j in range(table.get_number_of_columns()):
    col = table.get_column(j)
    if col:
        col_map[col.name] = j

print("=" * 70)
print("Checking all columns in record 292 (lorem ipsum email)")
print("=" * 70)

# Get record 292
rec = table.get_record(292)

# Show all columns with readable text
for col_name, col_idx in sorted(col_map.items()):
    try:
        if rec.is_long_value(col_idx):
            lv = rec.get_value_data_as_long_value(col_idx)
            data = lv.get_data() if lv else None
        else:
            data = rec.get_value_data(col_idx)

        if data and len(data) > 0:
            # Check for "Lorem" or "Ipsum"
            if b'Lorem' in data or b'Ipsum' in data or b'lorem' in data or b'ipsum' in data:
                print(f"\n{col_name}: Found lorem/ipsum!")
                print(f"  Length: {len(data)} bytes")
                # Show readable content
                text = data.decode('utf-8', errors='replace')
                # Extract readable strings
                import re
                strings = re.findall(r'[\x20-\x7e]{4,}', text)
                for s in strings[:10]:
                    print(f"  - {s[:80]}")

            # Also show the column if it has short readable text
            if len(data) < 100:
                try:
                    text = data.decode('utf-16-le', errors='replace').strip('\x00')
                    if text and len(text) >= 3 and any(c.isalpha() for c in text):
                        print(f"\n{col_name}: {text[:50]}")
                except:
                    pass

    except Exception as e:
        pass

print("\n" + "=" * 70)
print("Looking for Subject column specifically")
print("=" * 70)

# The subject might be in a different column or embedded in PropertyBlob
# Let's look at MessageClass and other metadata columns
for col_name in ['MessageClass', 'Subject', 'DisplayTo', 'DisplayFrom', 'DisplayCc', 'DisplayBcc']:
    if col_name in col_map:
        col_idx = col_map[col_name]
        try:
            data = rec.get_value_data(col_idx)
            if data:
                # Try different encodings
                try:
                    text = data.decode('utf-16-le', errors='replace').strip('\x00')
                    if text:
                        print(f"{col_name}: {text}")
                except:
                    pass
        except:
            pass

# Now examine the PropertyBlob structure to find subject
print("\n" + "=" * 70)
print("PropertyBlob structure analysis")
print("=" * 70)

pb_idx = col_map.get('PropertyBlob', -1)
if pb_idx >= 0:
    pb_data = rec.get_value_data(pb_idx)
    if pb_data:
        # Look for subject markers (MAPI property PR_SUBJECT = 0x0037)
        # or look for readable strings that could be subject

        # Find all readable strings and their positions
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

        if current and len(current) >= 3:
            strings.append((start, ''.join(current)))

        print("\nReadable strings in PropertyBlob:")
        for pos, text in strings[:30]:
            print(f"  0x{pos:04x}: {text}")

        # Look specifically for where "Lorem" and "Rosetta" appear
        lorem_pos = pb_data.find(b'Lorem')
        rosetta_pos = pb_data.find(b'Rosetta')

        print(f"\n'Lorem' position: 0x{lorem_pos:04x}" if lorem_pos >= 0 else "\n'Lorem' not found")
        print(f"'Rosetta' position: 0x{rosetta_pos:04x}" if rosetta_pos >= 0 else "'Rosetta' not found")

        # What's around Lorem?
        if lorem_pos >= 0:
            print(f"\nContext around 'Lorem':")
            start = max(0, lorem_pos - 20)
            end = min(len(pb_data), lorem_pos + 30)
            context = pb_data[start:end]
            hex_str = context.hex()
            print(f"  Hex: {hex_str}")
            # Readable parts
            text = ''.join(chr(b) if 32 <= b < 127 else '.' for b in context)
            print(f"  ASCII: {text}")

db.close()

# Also check the original database record 323
print("\n" + "=" * 70)
print("Checking original database record 323 (quick brown fox)")
print("=" * 70)

db2 = pyesedb.file()
db2.open('/Users/igorbatin/Documents/VaibPro/MDB_exporter/Mailbox Database 0058949847/Mailbox Database 0058949847.edb')

table2 = db2.get_table_by_name('Message_103')
col_map2 = {}
for j in range(table2.get_number_of_columns()):
    col = table2.get_column(j)
    if col:
        col_map2[col.name] = j

rec2 = table2.get_record(323)

pb_idx2 = col_map2.get('PropertyBlob', -1)
if pb_idx2 >= 0:
    pb_data2 = rec2.get_value_data(pb_idx2)
    if pb_data2:
        # Find all readable strings
        strings = []
        current = []
        start = 0

        for i, b in enumerate(pb_data2):
            if 32 <= b <= 126:
                if not current:
                    start = i
                current.append(chr(b))
            else:
                if len(current) >= 3:
                    strings.append((start, ''.join(current)))
                current = []

        if current and len(current) >= 3:
            strings.append((start, ''.join(current)))

        print("\nReadable strings in PropertyBlob (record 323):")
        for pos, text in strings[:30]:
            print(f"  0x{pos:04x}: {text}")

        # Check what's before M+The
        mplus_pos = pb_data2.find(b'M+The')
        if mplus_pos >= 0:
            print(f"\n'M+The' position: 0x{mplus_pos:04x}")
            print(f"Context before:")
            start = max(0, mplus_pos - 50)
            context = pb_data2[start:mplus_pos+10]
            text = ''.join(chr(b) if 32 <= b < 127 else '.' for b in context)
            print(f"  {text}")

db2.close()
