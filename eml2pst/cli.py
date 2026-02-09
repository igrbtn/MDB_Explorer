"""Command-line interface for eml2pst."""

import argparse
import base64
import json
import sys
from pathlib import Path

from .pst_file import PSTFileBuilder
from .eml_parser import parse_eml_file, parse_eml_bytes


def _process_stdin(builder, stats):
    """Read JSONL from stdin and add messages to the builder.

    Each line is a JSON object with:
        folder: Folder path (e.g. "Inbox", "Inbox/Projects")
        eml:    Base64-encoded EML content
        eml_file: Path to EML file on disk (alternative to eml)
    """
    # Cache: folder path -> NID
    folder_nids = {}

    def get_or_create_folder(folder_path):
        """Get or create a folder by path (e.g. 'Inbox/Projects/2024')."""
        if not folder_path:
            return builder._root_nid
        if folder_path in folder_nids:
            return folder_nids[folder_path]

        parts = folder_path.replace('\\', '/').split('/')
        current_path = ''
        parent_nid = None

        for part in parts:
            current_path = f"{current_path}/{part}" if current_path else part
            if current_path not in folder_nids:
                nid = builder.add_folder(part, parent_nid)
                folder_nids[current_path] = nid
                stats['folders'] += 1
            parent_nid = folder_nids[current_path]

        return folder_nids[folder_path]

    for line_num, line in enumerate(sys.stdin, 1):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as e:
            stats['errors'] += 1
            print(f"  ! Line {line_num}: invalid JSON: {e}", file=sys.stderr)
            continue

        folder_path = obj.get('folder', '')
        try:
            folder_nid = get_or_create_folder(folder_path)

            if 'eml' in obj:
                eml_data = base64.b64decode(obj['eml'])
                parsed = parse_eml_bytes(eml_data)
            elif 'eml_file' in obj:
                parsed = parse_eml_file(obj['eml_file'])
            else:
                stats['errors'] += 1
                print(f"  ! Line {line_num}: missing 'eml' or 'eml_file'",
                      file=sys.stderr)
                continue

            builder.add_message(folder_nid, parsed)
            stats['messages'] += 1
            subject = parsed.get('subject', '(No Subject)')[:60]
            print(f"  + [{folder_path or 'Root'}] {subject}", file=sys.stderr)
        except Exception as e:
            stats['errors'] += 1
            print(f"  ! Line {line_num}: {e}", file=sys.stderr)


def _process_directory(builder, input_dir, stats):
    """Recursively process a directory tree of EML files."""
    def recurse(dir_path, parent_nid=None):
        if parent_nid is not None:
            folder_nid = builder.add_folder(dir_path.name, parent_nid)
            stats['folders'] += 1
        else:
            folder_nid = None

        target_nid = folder_nid if folder_nid is not None else builder._root_nid

        eml_files = sorted(dir_path.glob('*.eml'))
        for eml_path in eml_files:
            try:
                parsed = parse_eml_file(eml_path)
                builder.add_message(target_nid, parsed)
                stats['messages'] += 1
                print(f"  + {eml_path.name}", file=sys.stderr)
            except Exception as e:
                stats['errors'] += 1
                print(f"  ! Error parsing {eml_path.name}: {e}", file=sys.stderr)

        subdirs = sorted([d for d in dir_path.iterdir() if d.is_dir()])
        for subdir in subdirs:
            recurse(subdir, target_nid)

    print(f"Scanning {input_dir}...", file=sys.stderr)
    recurse(input_dir)


def main():
    parser = argparse.ArgumentParser(
        prog='eml2pst',
        description='Convert EML files and directory trees to PST format.',
    )
    parser.add_argument(
        'input_dir',
        type=Path,
        nargs='?',
        default=None,
        help='Directory containing EML files (subdirectories become PST folders)',
    )
    parser.add_argument(
        '-o', '--output',
        type=Path,
        default=Path('output.pst'),
        help='Output PST file path (default: output.pst)',
    )
    parser.add_argument(
        '-n', '--name',
        default='Personal Folders',
        help='Display name for the PST store (default: Personal Folders)',
    )
    parser.add_argument(
        '--stdin',
        action='store_true',
        help='Read JSONL from stdin: {"folder":"path","eml":"<base64>"}',
    )

    args = parser.parse_args()

    if not args.stdin and args.input_dir is None:
        parser.error("either input_dir or --stdin is required")

    if args.input_dir and not args.stdin and not args.input_dir.is_dir():
        print(f"Error: '{args.input_dir}' is not a directory", file=sys.stderr)
        sys.exit(1)

    builder = PSTFileBuilder(display_name=args.name)
    stats = {'folders': 0, 'messages': 0, 'errors': 0}

    if args.stdin:
        print("Reading JSONL from stdin...", file=sys.stderr)
        _process_stdin(builder, stats)
    else:
        _process_directory(builder, args.input_dir, stats)

    print(f"\nWriting PST file to {args.output}...", file=sys.stderr)
    builder.write(args.output)

    print(f"\nDone: {stats['messages']} messages, {stats['folders']} folders",
          file=sys.stderr)
    if stats['errors']:
        print(f"  ({stats['errors']} errors)", file=sys.stderr)


if __name__ == '__main__':
    main()
