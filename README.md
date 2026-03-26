# Instagram HTML Sorter

A powerful Python tool to process and organize Instagram HTML exports.

## Features

- Parses raw Instagram HTML exports or ZIP files
- Merges multi-part chats
- Deduplicates messages and media (SHA-256)
- Organizes media by date and chat
- Generates offline chat viewer (HTML)
- Fully resumable processing
- Cross-platform (Windows, Linux, macOS)

## Usage

```bash
python instagram_html_sort.py --input /path/to/export --output /path/to/output
```

## Install Dependencies

```bash
pip install -r requirements.txt
```

## Output

- `Chats/` → processed chats with viewer
- `Media/` → organized media files
- `manifest.json` → run summary

## Notes

- Works with large exports
- Handles corrupted or partial data
