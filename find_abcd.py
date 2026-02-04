#!/usr/bin/env python3
"""
Find and analyze the ABCD test email.
"""

import pyesedb

DB_PATH = '/Users/igorbatin/Documents/VaibPro/MDB_exporter/NewDB/NewDB_ABCD/NewDB.edb'

db = pyesedb.file()
db.open(DB_PATH)

# Find all Message tables
for i in range(db.get_number_of_tables()):
    t = db.get_table(i)
    if t and t.name.startswith('Message_'):
        table = t
        print(f"\nTable: {table.name}, Records: {table.get_number_of_records()}")

        col_map = {}
        for j in range(table.get_number_of_columns()):
            col = table.get_column(j)
            if col:
                col_map[col.name] = j

        # Search for 'aaa' or 'bbb' pattern in all records
        for rec_idx in range(table.get_number_of_records()):
            rec = table.get_record(rec_idx)
            if not rec:
                continue

            pb_idx = col_map.get('PropertyBlob', -1)
            if pb_idx < 0:
                continue

            try:
                pb_data = rec.get_value_data(pb_idx)
                if pb_data:
                    # Search for our test patterns
                    if b'aaa' in pb_data.lower() or b'bbb' in pb_data.lower():
                        print(f"\n{'='*70}")
                        print(f"FOUND! Record {rec_idx}")
                        print(f"{'='*70}")
                        print(f"PropertyBlob size: {len(pb_data)} bytes")

                        # Find all readable strings
                        strings = []
                        current = []
                        start = 0
                        for idx, b in enumerate(pb_data):
                            if 32 <= b <= 126:
                                if not current:
                                    start = idx
                                current.append(chr(b))
                            else:
                                if len(current) >= 2:
                                    strings.append((start, ''.join(current)))
                                current = []
                        if len(current) >= 2:
                            strings.append((start, ''.join(current)))

                        print("\nReadable strings:")
                        for pos, text in strings:
                            if 'aaa' in text.lower() or 'bbb' in text.lower() or 'ccc' in text.lower() or 'ddd' in text.lower():
                                print(f"  *** 0x{pos:04x}: {text} ***")
                            else:
                                print(f"  0x{pos:04x}: {text}")

                        # Full hex dump
                        print("\nFull hex dump:")
                        for idx in range(0, len(pb_data), 16):
                            chunk = pb_data[idx:idx+16]
                            hex_str = ' '.join(f'{b:02x}' for b in chunk)
                            ascii_str = ''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)
                            print(f"  {idx:04x}: {hex_str:<48}  {ascii_str}")

                        # Also get NativeBody
                        nb_idx = col_map.get('NativeBody', -1)
                        if nb_idx >= 0:
                            try:
                                if rec.is_long_value(nb_idx):
                                    lv = rec.get_value_data_as_long_value(nb_idx)
                                    nb_data = lv.get_data() if lv else None
                                else:
                                    nb_data = rec.get_value_data(nb_idx)

                                if nb_data:
                                    print(f"\nNativeBody size: {len(nb_data)} bytes")
                                    print("NativeBody hex dump:")
                                    for idx in range(0, min(len(nb_data), 500), 16):
                                        chunk = nb_data[idx:idx+16]
                                        hex_str = ' '.join(f'{b:02x}' for b in chunk)
                                        ascii_str = ''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)
                                        print(f"  {idx:04x}: {hex_str:<48}  {ascii_str}")
                            except Exception as e:
                                print(f"NativeBody error: {e}")

            except Exception as e:
                pass

db.close()
