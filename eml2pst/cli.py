"""Command-line interface for eml2pst."""

import argparse
import sys
from pathlib import Path

from .pst_file import PSTFileBuilder
from .eml_parser import parse_eml_file


def main():
    parser = argparse.ArgumentParser(
        prog='eml2pst',
        description='Convert EML files and directory trees to PST format.',
    )
    parser.add_argument(
        'input_dir',
        type=Path,
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

    args = parser.parse_args()

    if not args.input_dir.is_dir():
        print(f"Error: '{args.input_dir}' is not a directory", file=sys.stderr)
        sys.exit(1)

    builder = PSTFileBuilder(display_name=args.name)
    stats = {'folders': 0, 'messages': 0, 'errors': 0}

    def process_directory(dir_path, parent_nid=None):
        """Recursively process a directory tree."""
        # Create a folder for this directory (skip for root input_dir)
        if parent_nid is not None:
            folder_nid = builder.add_folder(dir_path.name, parent_nid)
            stats['folders'] += 1
        else:
            folder_nid = None  # Messages go to root folder

        target_nid = folder_nid if folder_nid is not None else builder._root_nid

        # Process EML files in this directory
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

        # Recurse into subdirectories
        subdirs = sorted([d for d in dir_path.iterdir() if d.is_dir()])
        for subdir in subdirs:
            process_directory(subdir, target_nid)

    print(f"Scanning {args.input_dir}...", file=sys.stderr)
    process_directory(args.input_dir)

    print(f"\nWriting PST file to {args.output}...", file=sys.stderr)
    builder.write(args.output)

    print(f"\nDone: {stats['messages']} messages, {stats['folders']} folders", file=sys.stderr)
    if stats['errors']:
        print(f"  ({stats['errors']} errors)", file=sys.stderr)


if __name__ == '__main__':
    main()
