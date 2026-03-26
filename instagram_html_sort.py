#!/usr/bin/env python3
"""
Instagram HTML Export Sorter & Chat Viewer Builder
Version: 3.0.0  —  Enterprise Edition

What this does
--------------
  - Accepts a raw Instagram HTML export folder OR a .zip file
  - Groups multi-part chat HTML chunks by conversation folder
  - Merges, deduplicates, and chronologically sorts every message
  - Copies media into a date-organised folder tree (Year / Month / Chat)
  - Stamps JPEG files with correct EXIF timestamps
  - Deduplicates media globally by SHA-256 — zero wasted disk space
  - Writes per-chat transcript.json + a standalone offline chat_viewer.html
  - Fully resumable: processed chunks are skipped on re-run
  - Per-chat error isolation: one broken chat never kills the rest
  - Thread-safe throughout — safe for parallel media hashing & copying
  - Windows, macOS, and Linux compatible

Usage
-----
  python instagram_html_sort.py --input /path/to/export --output /path/to/out
  python instagram_html_sort.py                        # launches GUI picker
  python instagram_html_sort.py --input export.zip --output out --workers 8
  python instagram_html_sort.py --input export/ --output out --dry-run
  python instagram_html_sort.py --input export/ --output out --verbose

Dependencies
------------
  pip install beautifulsoup4 lxml piexif tqdm
  (lxml is optional but much faster than html.parser)
"""

from __future__ import annotations

import argparse
import calendar
import hashlib
import html as html_lib
import json
import logging
import os
import re
import shutil
import struct
import sys
import tempfile
import threading
import time
import traceback
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse

from bs4 import BeautifulSoup, SoupStrainer, Tag

# ── Optional dependencies ────────────────────────────────────────────────────

try:
    import piexif
    _HAS_PIEXIF = True
except ImportError:
    piexif = None          # type: ignore[assignment]
    _HAS_PIEXIF = False

try:
    from tqdm import tqdm
    _HAS_TQDM = True
except ImportError:
    tqdm = None            # type: ignore[assignment]
    _HAS_TQDM = False

try:
    import tkinter as tk
    from tkinter import filedialog, messagebox, ttk
    _HAS_TK = True
except ImportError:
    tk = filedialog = messagebox = ttk = None  # type: ignore[assignment]
    _HAS_TK = False

# ── Constants ────────────────────────────────────────────────────────────────

VERSION = "3.0.0"

SUPPORTED_MEDIA_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp",
    ".mp4", ".mov", ".m4v", ".avi", ".mkv",
}
IMAGE_EXTENSIONS     = {".jpg", ".jpeg"}
ALL_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}
VIDEO_EXTENSIONS     = {".mp4", ".mov", ".m4v", ".avi", ".mkv"}

TIMESTAMP_FORMATS = [
    "%b %d, %Y %I:%M %p",
    "%B %d, %Y %I:%M %p",
    "%b %d, %Y %I:%M%p",
    "%B %d, %Y %I:%M%p",
    "%b %d, %Y %H:%M",
    "%B %d, %Y %H:%M",
]

REACTION_RE   = re.compile(r"^Reacted\s+(.+?)\s+to your message\s*$", re.IGNORECASE)
REPLY_HINT_RE = re.compile(r"(replied to (your|the) message|reply to (your|the) message)", re.IGNORECASE)
MSG_CHUNK_RE  = re.compile(r"message[_-]?(\d+)$", re.IGNORECASE)
MEDIA_IDX_RE  = re.compile(r"_(\d{6})_")

# Windows reserved device names — forbidden as filenames/folder names
_WIN_RESERVED = frozenset({
    "CON", "PRN", "AUX", "NUL",
    "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
    "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
})

# ── Data model ───────────────────────────────────────────────────────────────

@dataclass
class Attachment:
    kind: str                           # "local_media" | "external_link"
    original: str                       # raw URL / path from HTML
    resolved_path: Optional[str] = None # absolute path on disk (local_media only)
    copied_path:   Optional[str] = None # absolute path in output tree
    sha256:        Optional[str] = None # hex digest of source file
    duplicated_of: Optional[str] = None # sha256 of the first copy if dupe


@dataclass
class MessageRecord:
    chat_name:        str
    source_html:      str
    source_file_rank: int
    order_index:      int
    sender:           str
    timestamp_text:   str
    timestamp:        Optional[str]
    raw_text:         str
    display_text:     str
    message_type:     str = "message"
    attachments:      List[Attachment] = field(default_factory=list)


@dataclass
class ChatBundle:
    chat_name:    str
    group_key:    str
    source_files: List[str]           = field(default_factory=list)
    messages:     List[MessageRecord] = field(default_factory=list)


@dataclass
class RunStats:
    """Accumulates run-wide counters. All mutations via a lock — thread-safe."""
    chats_total:    int = 0
    chats_done:     int = 0
    chats_failed:   int = 0
    messages_total: int = 0
    media_copied:   int = 0
    media_dupes:    int = 0
    media_missing:  int = 0
    media_errors:   int = 0
    parse_errors:   int = 0
    _lock: threading.Lock = field(
        default_factory=threading.Lock, repr=False, compare=False
    )

    def add(self, **kwargs: int) -> None:
        with self._lock:
            for k, v in kwargs.items():
                setattr(self, k, getattr(self, k) + v)


# ── Thread-safe helpers ──────────────────────────────────────────────────────

class MediaDigestCache:
    """Thread-safe in-memory SHA-256 cache keyed by absolute path string."""

    def __init__(self) -> None:
        self._cache: Dict[str, str] = {}
        self._lock  = threading.Lock()

    def get(self, path: Path) -> Optional[str]:
        with self._lock:
            return self._cache.get(str(path))

    def set(self, path: Path, digest: str) -> None:
        with self._lock:
            self._cache[str(path)] = digest

    def __len__(self) -> int:
        with self._lock:
            return len(self._cache)


class SkippedLog:
    """Thread-safe append-only log for files that could not be processed."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()

    def write(self, line: str) -> None:
        with self._lock:
            try:
                with self._path.open("a", encoding="utf-8") as f:
                    f.write(line.rstrip("\n") + "\n")
            except OSError:
                pass  # never crash because the log failed


class MediaIndex:
    """Thread-safe wrapper around the global {sha256: relative_path} dict."""

    def __init__(self, initial: Dict[str, str]) -> None:
        self._data = dict(initial)
        self._lock = threading.Lock()

    def get(self, digest: str) -> Optional[str]:
        with self._lock:
            return self._data.get(digest)

    def set(self, digest: str, rel_path: str) -> None:
        with self._lock:
            self._data[digest] = rel_path

    def snapshot(self) -> Dict[str, str]:
        with self._lock:
            return dict(self._data)

    def next_index(self) -> int:
        with self._lock:
            indices = [
                int(m.group(1))
                for rel in self._data.values()
                if (m := MEDIA_IDX_RE.search(Path(rel).name))
            ]
            return max(indices, default=0)

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)


# ── Filesystem utilities ──────────────────────────────────────────────────────

def sanitize_name(value: str, fallback: str = "unknown") -> str:
    """
    Return a safe folder / file name that works on Windows, macOS, and Linux.

    Key rules
    ---------
    - Removes all characters forbidden on Windows: < > : " / \\ | ? * and
      ASCII control characters (0x00-0x1f)
    - Collapses whitespace and consecutive underscores
    - **Strips trailing dots AND underscores** — Windows silently refuses to
      create directories whose names end with a dot (e.g. "Shah.........").
      This was the direct cause of the FileNotFoundError crash.
    - Strips leading dots (avoids hidden-file semantics on Unix)
    - Blocks Windows reserved device names (CON, NUL, COM1-9, LPT1-9)
    - Truncates to 100 characters to stay safe on deeply-nested paths
    """
    value = (value or "").strip()
    if not value:
        return fallback

    # Replace every forbidden/problematic character with underscore
    # Covers: Windows forbidden chars + ASCII control chars
    value = re.sub(r'[<>:"/\\|?*\x00-\x1f\x7f]', "_", value)

    # Collapse whitespace → underscore
    value = re.sub(r"\s+", "_", value)

    # Collapse runs of underscores and dots
    value = re.sub(r"_+", "_", value)
    value = re.sub(r"\.{2,}", ".", value)

    # !! THE CRITICAL FIX:
    # Strip trailing dots and underscores — Windows rejects folder names
    # ending with '.' (e.g. "Shah........." → crash).
    value = value.rstrip("._")

    # Strip leading dots / underscores (no hidden files, no leading separators)
    value = value.lstrip("._")

    # Truncate
    value = value[:100]

    if not value:
        return fallback

    # Block Windows reserved device names regardless of extension
    stem = value.split(".")[0].upper()
    if stem in _WIN_RESERVED:
        value = f"{value}_"

    return value if value else fallback


def ensure_dir(path: Path) -> Path:
    """
    Create directory and all parents. Raises a clear RuntimeError if the
    directory still does not exist after the call (e.g. bad Windows path).
    """
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise RuntimeError(
            f"Cannot create directory '{path}': {exc}\n"
            "This usually means the path contains characters that your OS or "
            "filesystem forbids. Check for trailing dots, reserved names (NUL, "
            "CON …), or path lengths over 260 characters on Windows."
        ) from exc

    if not path.is_dir():
        raise RuntimeError(
            f"mkdir succeeded but '{path}' is still not a directory. "
            "This is unexpected — check filesystem permissions."
        )
    return path


def safe_dir(path: Path, logger: logging.Logger) -> Optional[Path]:
    """
    Like ensure_dir but logs the error and returns None instead of raising.
    Use this when a missing directory should skip the item rather than abort.
    """
    try:
        return ensure_dir(path)
    except RuntimeError as exc:
        logger.error(str(exc))
        return None


def save_json(path: Path, obj: Any) -> None:
    """
    Atomic JSON write:
      1. Ensure the parent directory exists
      2. Write to a sibling .tmp file (with a UUID suffix to avoid collisions)
      3. Rename into place (atomic on all major OSes)
      4. Clean up the .tmp file if anything goes wrong

    Using a UUID suffix (instead of .with_suffix('.tmp')) means two threads
    writing to the same directory never clobber each other's temp file.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    # Verify the parent actually exists (mkdir can silently fail on Windows
    # for bad path names; we want an early, clear error here)
    if not path.parent.is_dir():
        raise RuntimeError(
            f"save_json: parent directory does not exist and could not be "
            f"created: '{path.parent}'\n"
            "Check for illegal characters (trailing dots, reserved names, etc.)"
        )

    tmp = path.parent / f".{path.stem}_{uuid.uuid4().hex[:8]}.tmp"
    try:
        tmp.write_text(
            json.dumps(obj, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        tmp.replace(path)
    except Exception:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def get_file_hash(filepath: Path) -> str:
    """SHA-256 hash. Uses the fast C-level file_digest when available (Python ≥ 3.11)."""
    if hasattr(hashlib, "file_digest"):
        with open(filepath, "rb") as f:
            return hashlib.file_digest(f, "sha256").hexdigest()
    h   = hashlib.sha256()
    buf = bytearray(256 * 1024)
    mv  = memoryview(buf)
    with open(filepath, "rb", buffering=0) as f:
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    return h.hexdigest()


def elapsed_str(seconds: float) -> str:
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m}m {s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h {m:02d}m {s:02d}s"


# ── Pure utilities ────────────────────────────────────────────────────────────

def normalize_sender(raw: str, chat_title: str) -> str:
    text = (raw or "").strip()
    # Zero-width / invisible Unicode spacers Instagram sometimes emits
    if not text or text in {"\u2800", "\u200b", "\ufeff", "\u00ad"}:
        return "You"
    return text


def parse_timestamp(ts_text: str) -> Optional[datetime]:
    ts_text = (ts_text or "").strip()
    for fmt in TIMESTAMP_FORMATS:
        try:
            return datetime.strptime(ts_text, fmt)
        except ValueError:
            continue
    return None


def month_folder(dt: datetime) -> str:
    return f"{dt.month:02d}_{calendar.month_name[dt.month]}"


def is_probably_media_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in SUPPORTED_MEDIA_EXTENSIONS)


def html_to_text(node: Tag) -> str:
    text    = node.get_text("\n", strip=False)
    lines   = [line.strip() for line in text.splitlines()]
    compact = "\n".join(line for line in lines if line)
    return html_lib.unescape(compact).strip()


def timestamp_to_iso(ts: Optional[datetime]) -> Optional[str]:
    return ts.isoformat(timespec="seconds") if ts else None


def iso_to_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return None


# ── Logging setup ─────────────────────────────────────────────────────────────

def setup_logger(output_root: Path, verbose: bool = False) -> logging.Logger:
    ensure_dir(output_root)
    logger = logging.getLogger("instagram_sorter")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%H:%M:%S"
    )

    fh = logging.FileHandler(output_root / "run.log", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def log_banner(logger: logging.Logger, text: str, char: str = "=", width: int = 60) -> None:
    bar = char * width
    logger.info(bar)
    if text:
        logger.info(f"  {text}")
        logger.info(bar)


# ── Path resolution ───────────────────────────────────────────────────────────

def candidate_paths(
    raw_url: str,
    html_path: Path,
    export_root: Path,
    base_href: Optional[str],
) -> List[Path]:
    raw_url = unquote((raw_url or "").strip())
    if not raw_url or urlparse(raw_url).scheme in {"http", "https"}:
        return []

    raw_path   = raw_url.lstrip("/")
    candidates: List[Path] = [
        (export_root       / raw_path).resolve(),
        (html_path.parent  / raw_path).resolve(),
    ]
    for parent in html_path.parents:
        candidates.append((parent / raw_path).resolve())

    if base_href:
        base_rel = base_href.strip().lstrip("/")
        candidates += [
            (export_root       / base_rel / raw_path).resolve(),
            (html_path.parent  / base_rel / raw_path).resolve(),
        ]
        for parent in html_path.parents:
            candidates.append((parent / base_rel / raw_path).resolve())

    marker = "your_instagram_activity/"
    if marker in raw_path:
        trimmed = raw_path[raw_path.index(marker):]
        candidates.append((export_root / trimmed).resolve())

    seen:   set[str]  = set()
    unique: List[Path] = []
    for c in candidates:
        key = str(c)
        if key not in seen:
            seen.add(key)
            unique.append(c)
    return unique


def resolve_local_path(
    raw_url: str,
    html_path: Path,
    export_root: Path,
    base_href: Optional[str],
) -> Optional[Path]:
    for candidate in candidate_paths(raw_url, html_path, export_root, base_href):
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


# ── ZIP extraction ────────────────────────────────────────────────────────────

def safe_extract_zip(zip_path: Path, temp_dir: Path, logger: logging.Logger) -> Path:
    """
    Validate and extract a ZIP file with:
      - Path-traversal attack prevention (zip-slip guard)
      - Disk-space pre-flight check (needs 120 % of uncompressed size free)
      - Progress logging every 500 files
    """
    logger.info(f"  Validating ZIP: {zip_path.name}")
    if not zipfile.is_zipfile(zip_path):
        raise ValueError(f"Not a valid ZIP file: {zip_path}")

    total_size    = 0
    temp_resolved = str(temp_dir.resolve())

    with zipfile.ZipFile(zip_path, "r") as zf:
        infolist = zf.infolist()
        logger.info(f"  ZIP contains {len(infolist):,} entries. Running safety checks...")

        for info in infolist:
            target = (temp_dir / info.filename).resolve()
            if not str(target).startswith(temp_resolved):
                raise ValueError(
                    f"Unsafe ZIP path (potential zip-slip attack): {info.filename!r}"
                )
            total_size += info.file_size

        size_mb = total_size / (1024 ** 2)
        free_mb = shutil.disk_usage(temp_dir).free / (1024 ** 2)
        logger.info(f"  Uncompressed: {size_mb:,.1f} MB  |  Free disk: {free_mb:,.1f} MB")

        if shutil.disk_usage(temp_dir).free < int(total_size * 1.2):
            raise OSError(
                f"Insufficient disk space. Need ~{size_mb * 1.2:,.0f} MB, "
                f"only {free_mb:,.0f} MB available."
            )

        logger.info(f"  Extracting {len(infolist):,} files...")
        for i, info in enumerate(infolist, 1):
            if i % 500 == 0 or i == len(infolist):
                logger.info(f"    Extracted {i:,}/{len(infolist):,}...")
            zf.extract(info, temp_dir)

    logger.info(f"  Extraction complete → {temp_dir}")
    return temp_dir


# ── HTML discovery & grouping ─────────────────────────────────────────────────

def discover_html_files(root: Path, logger: logging.Logger) -> List[Path]:
    logger.info(f"  Scanning {root} for HTML files...")
    html_files = [p for ext in ("*.html", "*.htm") for p in root.rglob(ext)]
    valid_files = sorted(
        set(
            p for p in html_files
            if "Output" not in p.parts and "output" not in p.parts
        )
    )
    logger.info(f"  Found {len(valid_files):,} HTML file(s).")
    return valid_files


def detect_chat_name(soup: BeautifulSoup, html_path: Path) -> str:
    if soup.title:
        title = soup.title.get_text(" ", strip=True)
        if title:
            return title
    h1 = soup.find("h1")
    if h1:
        name = h1.get_text(" ", strip=True)
        if name:
            return name
    return html_path.parent.name or html_path.stem


def chat_group_key(html_path: Path, export_root: Path, chat_name: str) -> str:
    try:
        return html_path.parent.relative_to(export_root).as_posix()
    except Exception:
        return f"{chat_name}::{html_path.parent.resolve()}"


def extract_chunk_rank(html_path: Path) -> int:
    m = MSG_CHUNK_RE.search(html_path.stem)
    return int(m.group(1)) if m else 0


def sort_chunk_files(files: List[Path]) -> List[Path]:
    def key(path: Path) -> Tuple[int, int, str]:
        rank = extract_chunk_rank(path)
        if rank > 0:
            return (0, -rank, path.name.lower())
        try:
            return (1, int(path.stat().st_mtime_ns), path.name.lower())
        except Exception:
            return (2, 0, path.name.lower())
    return sorted(files, key=key)


def group_html_files(root: Path, logger: logging.Logger) -> Dict[str, List[Path]]:
    groups: Dict[str, List[Path]] = {}
    files = discover_html_files(root, logger)
    if not files:
        return groups

    logger.info(f"  Routing {len(files):,} files into conversation groups...")
    for i, html_path in enumerate(files, 1):
        if i % 100 == 0 or i == len(files):
            logger.info(f"    Grouped {i:,}/{len(files):,}...")
        try:
            raw       = html_path.read_text(encoding="utf-8", errors="ignore")
            soup      = BeautifulSoup(raw, "html.parser", parse_only=SoupStrainer("title"))
            chat_name = detect_chat_name(soup, html_path)
        except Exception:
            chat_name = html_path.stem

        key = chat_group_key(html_path, root, chat_name)
        groups.setdefault(key, []).append(html_path)

    multi_chunk = [(k, v) for k, v in groups.items() if len(v) > 1]
    logger.info(
        f"  Grouping done: {len(groups):,} conversation(s)  "
        f"({len(multi_chunk)} multi-part)."
    )
    if multi_chunk:
        top = sorted(multi_chunk, key=lambda x: -len(x[1]))[:5]
        for key, fls in top:
            logger.info(f"    Multi-part: '{Path(key).name}' → {len(fls)} chunks")
        if len(multi_chunk) > 5:
            logger.info(f"    … and {len(multi_chunk) - 5} more multi-part chats.")

    return groups


# ── Message parsing ───────────────────────────────────────────────────────────

def infer_message_kind(raw_text: str) -> Tuple[str, str]:
    text = (raw_text or "").strip()
    if not text:
        return "empty", ""
    m = REACTION_RE.match(text)
    if m:
        emoji = m.group(1).strip()
        return "reaction", f"{emoji} reacted to your message"
    if REPLY_HINT_RE.search(text):
        return "reply", text
    return "message", text


def extract_attachments(
    body: Tag,
    html_path: Path,
    export_root: Path,
    base_href: Optional[str],
) -> Tuple[List[Attachment], str]:
    attachments: List[Attachment] = []
    seen_urls: set[Tuple[str, str]] = set()

    for tag in body.find_all(["a", "img", "video", "source"]):
        url = unquote((tag.get("href") or tag.get("src") or "").strip())
        if not url:
            continue
        dedup_key = (tag.name, url)
        if dedup_key in seen_urls:
            continue
        seen_urls.add(dedup_key)

        if tag.name in {"img", "video", "source"} or is_probably_media_url(url):
            local = resolve_local_path(url, html_path, export_root, base_href)
            if local:
                attachments.append(Attachment(kind="local_media", original=url, resolved_path=str(local)))
            else:
                attachments.append(Attachment(kind="external_link", original=url))
        else:
            attachments.append(Attachment(kind="external_link", original=url))

    # Strip tags from a clone to extract clean text
    body_clone = BeautifulSoup(str(body), "html.parser")
    for tag in body_clone.find_all(["a", "img", "video", "source"]):
        if tag.name == "a":
            anchor_text = tag.get_text(" ", strip=True)
            href        = tag.get("href", "")
            if anchor_text and anchor_text != href:
                tag.replace_with(anchor_text)
            else:
                tag.decompose()
        else:
            tag.decompose()

    return attachments, html_to_text(body_clone)


def parse_html_file(
    html_path: Path,
    export_root: Path,
    chat_name: str,
    source_rank: int,
    logger: logging.Logger,
) -> List[MessageRecord]:
    """
    Parse all messages from a single HTML chunk.
    Returns [] on any error — never raises, logs instead.
    """
    try:
        raw = html_path.read_text(encoding="utf-8", errors="ignore")
    except OSError as exc:
        logger.error(f"    Cannot read {html_path.name}: {exc}")
        return []

    base_match = re.search(r'<base\s+href="([^"]+)"', raw, re.IGNORECASE)
    base_href  = base_match.group(1) if base_match else None

    strainer = SoupStrainer("div", class_=lambda c: c and "pam" in c and "_a6-g" in c)
    try:
        soup = BeautifulSoup(raw, "lxml", parse_only=strainer)
    except Exception:
        try:
            soup = BeautifulSoup(raw, "html.parser", parse_only=strainer)
        except Exception as exc:
            logger.error(f"    HTML parse failed for {html_path.name}: {exc}")
            return []

    message_blocks = soup.find_all("div", recursive=False)
    if not message_blocks:
        message_blocks = soup.find_all(
            "div", class_=lambda c: c and "pam" in c and "_a6-g" in c
        )

    if not message_blocks:
        logger.warning(f"    No message blocks found in {html_path.name}.")
        return []

    parsed: List[MessageRecord] = []
    # Instagram exports newest-first; reverse to get chronological order
    for idx, block in enumerate(reversed(message_blocks)):
        try:
            h2       = block.find("h2")
            ts_div   = block.find("div", class_="_a6-o")
            body_div = block.find("div", class_="_a6-p")

            if not (h2 and ts_div and body_div):
                continue

            sender  = normalize_sender(h2.get_text(" ", strip=True), chat_name)
            ts_text = ts_div.get_text(" ", strip=True)
            ts      = parse_timestamp(ts_text)

            if ts is None:
                try:
                    ts = datetime.fromtimestamp(html_path.stat().st_mtime)
                except Exception:
                    ts = datetime.now()

            attachments, raw_text = extract_attachments(body_div, html_path, export_root, base_href)
            kind, display_text    = infer_message_kind(raw_text)

            parsed.append(MessageRecord(
                chat_name        = chat_name,
                source_html      = str(html_path),
                source_file_rank = source_rank,
                order_index      = idx,
                sender           = sender,
                timestamp_text   = ts_text,
                timestamp        = timestamp_to_iso(ts),
                raw_text         = raw_text,
                display_text     = display_text,
                message_type     = kind,
                attachments      = attachments,
            ))

        except Exception as exc:
            logger.debug(
                f"    Skipped block #{idx} in {html_path.name}: "
                f"{type(exc).__name__}: {exc}"
            )

    logger.debug(f"    Parsed {len(parsed):,} messages from {html_path.name}")
    return parsed


# ── Message deduplication ─────────────────────────────────────────────────────

def message_signature(msg: MessageRecord) -> str:
    att_sig = [
        f"{a.kind}:{a.original}:{a.resolved_path or ''}:{a.copied_path or ''}"
        for a in msg.attachments
    ]
    raw = "|".join([
        msg.sender, msg.timestamp_text, msg.raw_text,
        msg.display_text, msg.message_type, ";".join(att_sig),
    ])
    return hashlib.sha256(raw.encode("utf-8", errors="ignore")).hexdigest()


def merge_sorted_messages(
    messages: List[MessageRecord],
    logger: Optional[logging.Logger] = None,
) -> List[MessageRecord]:
    seen:   set[str]           = set()
    merged: List[MessageRecord] = []
    dupes = 0

    for msg in messages:
        sig = message_signature(msg)
        if sig in seen:
            dupes += 1
        else:
            seen.add(sig)
            merged.append(msg)

    if dupes and logger:
        logger.info(f"    Removed {dupes:,} duplicate message(s).")

    return sorted(
        merged,
        key=lambda m: (
            iso_to_timestamp(m.timestamp) or datetime.min,
            m.source_file_rank,
            m.order_index,
        ),
    )


# ── Media processing ──────────────────────────────────────────────────────────

def copy_and_stamp_media(src: Path, dst: Path, ts: datetime, logger: logging.Logger) -> None:
    ensure_dir(dst.parent)
    try:
        shutil.copy2(src, dst)
    except OSError as exc:
        raise OSError(f"File copy failed {src} → {dst}: {exc}") from exc

    ts_epoch = ts.timestamp()

    if dst.suffix.lower() in IMAGE_EXTENSIONS and _HAS_PIEXIF:
        try:
            with open(dst, "rb") as f:
                header = f.read(2)
            if header == b"\xff\xd8":
                exif_dict = piexif.load(str(dst))
                if "0th"  not in exif_dict: exif_dict["0th"]  = {}
                if "Exif" not in exif_dict: exif_dict["Exif"] = {}
                dt_bytes = ts.strftime("%Y:%m:%d %H:%M:%S").encode("utf-8")
                exif_dict["0th"][piexif.ImageIFD.DateTime]          = dt_bytes
                exif_dict["Exif"][piexif.ExifIFD.DateTimeOriginal]  = dt_bytes
                exif_dict["Exif"][piexif.ExifIFD.DateTimeDigitized] = dt_bytes
                piexif.insert(piexif.dump(exif_dict), str(dst))
        except (ValueError, struct.error):
            pass
        except Exception as exc:
            logger.debug(f"    EXIF stamp skipped for {dst.name}: {type(exc).__name__}: {exc}")

    try:
        os.utime(dst, (ts_epoch, ts_epoch))
    except OSError as exc:
        logger.debug(f"    utime failed for {dst.name}: {exc}")


def build_output_path(
    output_root:   Path,
    chat_name:     str,
    ts:            datetime,
    original_name: str,
    index:         int,
) -> Path:
    month_dir = ensure_dir(
        output_root / "Media" / str(ts.year) / month_folder(ts) / sanitize_name(chat_name)
    )
    safe_stem = sanitize_name(Path(original_name).stem, "media")
    ext       = Path(original_name).suffix.lower() or ".bin"
    filename  = f"{ts.strftime('%Y-%m-%d_%H-%M-%S')}_{index:06d}_{safe_stem}{ext}"
    return month_dir / filename


def process_single_media(
    msg:          MessageRecord,
    att:          Attachment,
    output_root:  Path,
    chat_name:    str,
    media_idx:    int,
    digest_cache: MediaDigestCache,
    media_index:  MediaIndex,
    skipped_log:  SkippedLog,
    logger:       logging.Logger,
    dry_run:      bool = False,
) -> Tuple[str, Attachment, Optional[str], Optional[str]]:
    """
    Process one media attachment. Thread-safe.

    Returns (status, att, digest, rel_path) where status is one of:
      "copied"    — new file successfully copied into output tree
      "duplicate" — file already in media_index; att.duplicated_of set
      "missing"   — source file not found on disk
      "error"     — unexpected failure
    """
    if not att.resolved_path:
        return "missing", att, None, None

    src = Path(att.resolved_path)
    if not src.exists() or not src.is_file():
        skipped_log.write(f"{att.resolved_path}\tmissing source\t{msg.source_html}")
        return "missing", att, None, None

    # Hash (with cache)
    try:
        digest = digest_cache.get(src)
        if digest is None:
            digest = get_file_hash(src)
            digest_cache.set(src, digest)
        att.sha256 = digest
    except OSError as exc:
        logger.debug(f"    Hash failed for {src.name}: {exc}")
        skipped_log.write(f"{att.resolved_path}\thash error\t{exc}")
        return "error", att, None, None

    # Duplicate check
    existing_rel = media_index.get(digest)
    if existing_rel:
        existing_abs = output_root / existing_rel
        if existing_abs.exists():
            att.duplicated_of = digest
            att.copied_path   = str(existing_abs)
            return "duplicate", att, digest, existing_rel

    if dry_run:
        return "copied", att, digest, "<dry-run>"

    try:
        ts  = iso_to_timestamp(msg.timestamp) or datetime.fromtimestamp(src.stat().st_mtime)
        dst = build_output_path(output_root, chat_name, ts, src.name, media_idx)

        # Guard: if a different file landed at the same destination path, add a hash suffix
        if dst.exists():
            existing_digest = get_file_hash(dst)
            if existing_digest != digest:
                stem, suffix = dst.stem, dst.suffix
                dst = dst.with_name(f"{stem}_{digest[:8]}{suffix}")

        copy_and_stamp_media(src, dst, ts, logger)
        att.copied_path = str(dst)
        rel_path        = str(dst.relative_to(output_root))
        media_index.set(digest, rel_path)
        return "copied", att, digest, rel_path

    except Exception as exc:
        logger.debug(f"    Media copy error {src.name}: {type(exc).__name__}: {exc}")
        skipped_log.write(f"{att.resolved_path}\tcopy error\t{exc}")
        return "error", att, None, None


# ── State persistence ─────────────────────────────────────────────────────────

def attachment_to_dict(att: Attachment) -> Dict[str, Any]:
    return asdict(att)


def attachment_from_dict(data: Dict[str, Any]) -> Attachment:
    valid_keys = {"kind", "original", "resolved_path", "copied_path", "sha256", "duplicated_of"}
    kwargs = {k: data.get(k) for k in valid_keys}
    kwargs.setdefault("kind", "external_link")
    kwargs.setdefault("original", "")
    return Attachment(**kwargs)  # type: ignore[arg-type]


def message_to_dict(msg: MessageRecord) -> Dict[str, Any]:
    return {
        "chat_name":        msg.chat_name,
        "source_html":      msg.source_html,
        "source_file_rank": msg.source_file_rank,
        "order_index":      msg.order_index,
        "sender":           msg.sender,
        "timestamp_text":   msg.timestamp_text,
        "timestamp":        msg.timestamp,
        "raw_text":         msg.raw_text,
        "display_text":     msg.display_text,
        "message_type":     msg.message_type,
        "attachments":      [attachment_to_dict(a) for a in msg.attachments],
    }


def message_from_dict(data: Dict[str, Any]) -> MessageRecord:
    return MessageRecord(
        chat_name        = data.get("chat_name", ""),
        source_html      = data.get("source_html", ""),
        source_file_rank = int(data.get("source_file_rank", 0)),
        order_index      = int(data.get("order_index", 0)),
        sender           = data.get("sender", "You"),
        timestamp_text   = data.get("timestamp_text", ""),
        timestamp        = data.get("timestamp"),
        raw_text         = data.get("raw_text", ""),
        display_text     = data.get("display_text") or data.get("raw_text", ""),
        message_type     = data.get("message_type", "message"),
        attachments      = [attachment_from_dict(a) for a in data.get("attachments", [])],
    )


def bundle_to_state(bundle: ChatBundle, processed_sources: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "chat_name":         bundle.chat_name,
        "group_key":         bundle.group_key,
        "source_files":      bundle.source_files,
        "processed_sources": processed_sources,
        "messages":          [message_to_dict(m) for m in bundle.messages],
    }


def load_chat_state(chat_dir: Path) -> Tuple[Dict[str, Any], List[MessageRecord], str]:
    state = load_json(chat_dir / "chat_state.json", {})
    if not isinstance(state, dict):
        state = {}
    return (
        state.get("processed_sources", {}),
        [message_from_dict(m) for m in state.get("messages", [])],
        state.get("chat_name", chat_dir.name),
    )


# ── Chat viewer HTML ──────────────────────────────────────────────────────────

def render_chat_viewer(bundle: ChatBundle, output_html: Path) -> None:
    ensure_dir(output_html.parent)

    lightweight: List[Dict[str, Any]] = []
    for m in bundle.messages:
        is_me    = 1 if m.sender == "You" else 0
        t_obj    = iso_to_timestamp(m.timestamp)
        time_str = t_obj.strftime("%b %d, %Y  %I:%M %p") if t_obj else m.timestamp_text

        media_list: List[Dict[str, str]] = []
        for a in m.attachments:
            if a.kind == "local_media" and a.copied_path:
                try:
                    rel = os.path.relpath(a.copied_path, output_html.parent).replace("\\", "/")
                    media_list.append({"type": "local", "url": rel})
                except ValueError:
                    # relpath fails across Windows drives; fall back to absolute
                    media_list.append({"type": "local", "url": a.copied_path.replace("\\", "/")})
            elif a.kind == "external_link" and a.original:
                media_list.append({"type": "ext", "url": a.original})

        lightweight.append({
            "s": is_me,
            "n": m.sender if not is_me else "",
            "t": time_str,
            "x": html_lib.escape(m.display_text or m.raw_text),
            "k": m.message_type[0],
            "a": media_list,
        })

    total_msg    = len(lightweight)
    json_payload = json.dumps(lightweight, separators=(",", ":"), ensure_ascii=False)
    chat_title   = html_lib.escape(bundle.chat_name)

    html = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{chat_title} — Chat Viewer</title>
<style>
:root {{
  --bg:#0d1117; --panel:#161b22; --panel2:#1f2633; --panel3:#252d3a;
  --accent:#2d6cdf; --accent-dim:rgba(45,108,223,.18);
  --text:#eef2ff; --muted:#9aa4b2; --border:rgba(255,255,255,.08);
  --reply-bar:#6fc2ff; --search-bg:#1a2230; --green:#3fb950;
}}
*,*::before,*::after {{ box-sizing:border-box; margin:0; padding:0; }}
html,body {{ height:100%; background:var(--bg); color:var(--text);
             font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif; }}

.header {{
  position:sticky; top:0; z-index:20;
  background:rgba(22,27,34,.97); backdrop-filter:blur(8px);
  border-bottom:1px solid var(--border); padding:14px 20px 12px;
}}
.header-top {{ display:flex; align-items:center; gap:12px; }}
.header h1 {{ font-size:18px; font-weight:700; flex:1;
              white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
.header .meta {{ color:var(--muted); font-size:12px; margin-top:5px; }}
.badge {{ background:var(--accent); color:#fff; font-size:11px; font-weight:700;
          padding:2px 8px; border-radius:999px; flex-shrink:0; }}

.search-wrap {{ margin-top:10px; position:relative; }}
#search-input {{
  width:100%; padding:8px 80px 8px 12px;
  background:var(--search-bg); border:1px solid var(--border);
  border-radius:8px; color:var(--text); font-size:13px; outline:none;
}}
#search-input:focus {{ border-color:var(--accent); }}
#search-input::placeholder {{ color:var(--muted); }}
.search-nav {{ position:absolute; right:8px; top:50%; transform:translateY(-50%);
               display:flex; gap:4px; }}
.search-nav button {{
  background:none; border:1px solid var(--border); color:var(--muted);
  cursor:pointer; padding:3px 7px; font-size:12px; border-radius:5px;
  transition:color .15s,border-color .15s;
}}
.search-nav button:hover {{ color:var(--text); border-color:var(--accent); }}
#search-count {{ color:var(--muted); font-size:11px; margin-top:4px; min-height:16px; }}

.container {{ max-width:900px; margin:0 auto; padding:18px 14px 80px;
              display:flex; flex-direction:column; gap:10px; }}

.bubble {{
  max-width:82%; border:1px solid var(--border); border-radius:18px;
  padding:11px 14px; font-size:14px; line-height:1.5;
  white-space:pre-wrap; word-break:break-word;
  box-shadow:0 4px 16px rgba(0,0,0,.2); transition:outline .1s;
}}
.bubble.me    {{ align-self:flex-end; background:linear-gradient(160deg,#1f4ea8,#2d6cdf);
                 border-color:rgba(45,108,223,.4); }}
.bubble.them  {{ align-self:flex-start; background:var(--panel); }}
.bubble.system {{ align-self:center; background:var(--panel3);
                  border-style:dashed; padding:7px 16px;
                  border-radius:999px; font-size:12px; color:var(--muted); text-align:center; }}
.bubble.highlight {{ outline:2px solid var(--accent); outline-offset:3px; }}

.msg-meta {{ display:flex; align-items:baseline; gap:8px; margin-bottom:5px; }}
.sender   {{ font-weight:700; font-size:13px; }}
.time     {{ color:rgba(255,255,255,.4); font-size:11px; }}
.reply-hint {{ font-size:11px; color:var(--reply-bar); margin-bottom:5px;
               padding-left:8px; border-left:2px solid var(--reply-bar); }}

.media-grid {{ margin-top:8px; display:flex; flex-wrap:wrap; gap:8px; }}
.media-grid a img, .media-grid img {{
  max-width:260px; max-height:260px; border-radius:10px;
  border:1px solid var(--border); object-fit:cover; cursor:zoom-in;
}}
.media-grid video {{
  max-width:320px; max-height:280px; border-radius:10px;
  border:1px solid var(--border);
}}
.media-link {{
  color:#6fc2ff; font-size:12px; text-decoration:none;
  display:inline-flex; align-items:center; gap:5px; padding:4px 8px;
  background:var(--accent-dim); border-radius:6px; overflow-wrap:anywhere;
}}
.media-link:hover {{ text-decoration:underline; }}

#lightbox {{
  display:none; position:fixed; inset:0; z-index:100;
  background:rgba(0,0,0,.92); cursor:zoom-out;
  justify-content:center; align-items:center;
}}
#lightbox.open {{ display:flex; }}
#lightbox img {{ max-width:92vw; max-height:92vh; border-radius:10px; }}

#totop {{
  display:none; position:fixed; bottom:24px; right:20px; z-index:50;
  background:var(--accent); color:#fff; border:none; cursor:pointer;
  width:40px; height:40px; border-radius:50%; font-size:20px;
  box-shadow:0 4px 14px rgba(0,0,0,.4); align-items:center; justify-content:center;
}}
#totop.visible {{ display:flex; }}

.date-divider {{
  align-self:center; color:var(--muted); font-size:11px;
  padding:4px 14px; background:var(--panel3);
  border-radius:999px; border:1px solid var(--border); user-select:none;
}}

#sentinel {{ height:60px; width:100%; }}
</style>
</head>
<body>

<div class="header">
  <div class="header-top">
    <h1>{chat_title}</h1>
    <span class="badge">{total_msg:,} messages</span>
  </div>
  <div class="meta">Instagram HTML export &mdash; instagram_html_sort v{VERSION}</div>
  <div class="search-wrap">
    <input id="search-input" type="search"
           placeholder="Search messages… (Enter = next, Shift+Enter = prev)"
           spellcheck="false" autocomplete="off">
    <div class="search-nav">
      <button id="prev-btn" title="Previous match">▲</button>
      <button id="next-btn" title="Next match">▼</button>
    </div>
  </div>
  <div id="search-count"></div>
</div>

<div class="container" id="chat-container"></div>
<div id="sentinel"></div>
<div id="lightbox"><img id="lb-img" src="" alt=""></div>
<button id="totop" title="Back to top">↑</button>

<script>
const chatData = {json_payload};

const IMAGE_EXTS = new Set(['jpg','jpeg','png','gif','webp','bmp']);
const VIDEO_EXTS = new Set(['mp4','mov','m4v','avi','mkv']);

const container = document.getElementById('chat-container');
const sentinel  = document.getElementById('sentinel');
const lightbox  = document.getElementById('lightbox');
const lbImg     = document.getElementById('lb-img');
const totop     = document.getElementById('totop');
const searchIn  = document.getElementById('search-input');
const searchCnt = document.getElementById('search-count');

let currentIndex = 0;
const CHUNK = 60;
let allBubbles = [];
let searchHits  = [];
let hitCursor   = -1;

function ext(url) {{
  return (url.split('.').pop().split('?')[0] || '').toLowerCase();
}}

function buildMediaNode(a) {{
  const e = ext(a.url);
  if (IMAGE_EXTS.has(e)) {{
    const link = document.createElement('a');
    link.href = a.url; link.target = '_blank';
    const img = document.createElement('img');
    img.src = a.url; img.loading = 'lazy';
    img.addEventListener('click', ev => {{ ev.preventDefault(); openLightbox(a.url); }});
    link.appendChild(img);
    return link;
  }}
  if (VIDEO_EXTS.has(e)) {{
    const v = document.createElement('video');
    v.src = a.url; v.controls = true; v.preload = 'none';
    return v;
  }}
  const link = document.createElement('a');
  link.className = 'media-link';
  link.href = a.url; link.target = '_blank';
  link.textContent = (a.type === 'ext' ? '🔗 ' : '📎 ') + (a.url.length > 60 ? a.url.slice(0,60)+'…' : a.url);
  return link;
}}

function buildBubble(msg, globalIdx) {{
  const frag = document.createDocumentFragment();

  // Date divider when day changes
  if (globalIdx > 0) {{
    const prev = chatData[globalIdx - 1];
    const prevDay = (prev.t || '').slice(0, 12);
    const thisDay = (msg.t  || '').slice(0, 12);
    if (prevDay && thisDay && prevDay !== thisDay) {{
      const div = document.createElement('div');
      div.className = 'date-divider';
      div.textContent = thisDay;
      frag.appendChild(div);
    }}
  }}

  const bubble = document.createElement('div');

  if (msg.k === 'r') {{
    bubble.className = 'bubble system';
    const who = msg.s === 1 ? 'You' : msg.n;
    const emoji = msg.x.replace(/reacted */i,'').replace(/ *to your message/i,'');
    bubble.textContent = who + ' reacted ' + emoji + ' · ' + msg.t;
  }} else {{
    bubble.className = msg.s === 1 ? 'bubble me' : 'bubble them';

    const meta = document.createElement('div');
    meta.className = 'msg-meta';
    if (msg.s === 0) {{
      const snd = document.createElement('span');
      snd.className = 'sender'; snd.textContent = msg.n;
      meta.appendChild(snd);
    }}
    const ts = document.createElement('span');
    ts.className = 'time'; ts.textContent = msg.t;
    meta.appendChild(ts);
    bubble.appendChild(meta);

    if (msg.k === 'p') {{
      const hint = document.createElement('div');
      hint.className = 'reply-hint';
      hint.textContent = '↩ replied to a message';
      bubble.appendChild(hint);
    }}

    if (msg.x) {{
      const body = document.createElement('div');
      body.innerHTML = msg.x;
      bubble.appendChild(body);
    }}

    if (msg.a && msg.a.length > 0) {{
      const grid = document.createElement('div');
      grid.className = 'media-grid';
      msg.a.forEach(a => grid.appendChild(buildMediaNode(a)));
      bubble.appendChild(grid);
    }}
  }}

  bubble.dataset.idx = globalIdx;
  frag.appendChild(bubble);
  return {{ frag, bubble }};
}}

function renderChunk() {{
  if (currentIndex >= chatData.length) return;
  const end    = Math.min(currentIndex + CHUNK, chatData.length);
  const docFrag = document.createDocumentFragment();
  for (let i = currentIndex; i < end; i++) {{
    const {{ frag, bubble }} = buildBubble(chatData[i], i);
    allBubbles[i] = bubble;
    docFrag.appendChild(frag);
  }}
  container.appendChild(docFrag);
  currentIndex = end;
  if (searchIn.value.trim()) runSearch(searchIn.value.trim(), false);
}}

// Infinite scroll
const ioObserver = new IntersectionObserver(entries => {{
  if (entries[0].isIntersecting && currentIndex < chatData.length) renderChunk();
}}, {{ rootMargin: '200px' }});
ioObserver.observe(sentinel);
renderChunk();

// Lightbox
function openLightbox(src) {{ lbImg.src = src; lightbox.classList.add('open'); }}
lightbox.addEventListener('click', () => {{ lightbox.classList.remove('open'); lbImg.src=''; }});
document.addEventListener('keydown', e => {{
  if (e.key === 'Escape') lightbox.classList.remove('open');
}});

// Scroll-to-top
window.addEventListener('scroll', () => {{
  totop.classList.toggle('visible', window.scrollY > 600);
}}, {{ passive: true }});
totop.addEventListener('click', () => window.scrollTo({{ top:0, behavior:'smooth' }}));

// Search
function clearHighlights() {{
  document.querySelectorAll('.bubble.highlight').forEach(b => b.classList.remove('highlight'));
}}

function runSearch(query, moveToNext) {{
  clearHighlights();
  searchHits = [];
  hitCursor  = -1;
  if (!query) {{ searchCnt.textContent = ''; return; }}

  const lq = query.toLowerCase();
  chatData.forEach((msg, i) => {{
    const haystack = [(msg.x||''), (msg.n||''), (msg.t||'')].join(' ').toLowerCase();
    if (haystack.includes(lq)) searchHits.push(i);
  }});

  if (!searchHits.length) {{
    searchCnt.textContent = 'No results';
    return;
  }}

  searchCnt.textContent = searchHits.length + ' result' + (searchHits.length > 1 ? 's' : '');
  if (moveToNext !== false) jumpToHit(0);
}}

function jumpToHit(idx) {{
  if (!searchHits.length) return;
  hitCursor = ((idx % searchHits.length) + searchHits.length) % searchHits.length;
  const msgIdx = searchHits[hitCursor];

  // Force-render up to that message
  while (currentIndex <= msgIdx && currentIndex < chatData.length) renderChunk();

  const bubble = allBubbles[msgIdx];
  if (bubble) {{
    clearHighlights();
    bubble.classList.add('highlight');
    bubble.scrollIntoView({{ behavior: 'smooth', block: 'center' }});
    searchCnt.textContent =
      (hitCursor + 1) + ' / ' + searchHits.length + ' result' +
      (searchHits.length > 1 ? 's' : '');
  }}
}}

searchIn.addEventListener('input', e => runSearch(e.target.value.trim()));
searchIn.addEventListener('keydown', e => {{
  if (e.key === 'Enter') {{
    e.preventDefault();
    jumpToHit(e.shiftKey ? hitCursor - 1 : hitCursor + 1);
  }}
}});
document.getElementById('next-btn').addEventListener('click', () => jumpToHit(hitCursor + 1));
document.getElementById('prev-btn').addEventListener('click', () => jumpToHit(hitCursor - 1));
</script>
</body>
</html>"""

    output_html.write_text(html, encoding="utf-8")


# ── Input resolution ──────────────────────────────────────────────────────────

def resolve_input_root(
    selected: Path, logger: logging.Logger
) -> Tuple[Path, Optional[tempfile.TemporaryDirectory]]:
    if selected.is_file() and selected.suffix.lower() == ".zip":
        temp_ctx: tempfile.TemporaryDirectory = tempfile.TemporaryDirectory(
            prefix="instagram_export_"
        )
        logger.info("ZIP detected — extracting to temporary directory...")
        safe_extract_zip(selected, Path(temp_ctx.name), logger)
        return Path(temp_ctx.name), temp_ctx
    if selected.is_dir():
        return selected, None
    raise ValueError(
        f"Input must be a folder or a .zip file, got: {selected}"
    )


# ── Main pipeline ─────────────────────────────────────────────────────────────

def process_export(
    input_path:  Path,
    output_root: Path,
    workers:     int  = 0,
    dry_run:     bool = False,
    verbose:     bool = False,
) -> None:
    """
    End-to-end processing of an Instagram HTML export.

    Parameters
    ----------
    input_path  : Path to an export folder or a .zip file.
    output_root : Destination directory (created if absent).
    workers     : Thread-pool size. 0 = auto (cpu_count).
    dry_run     : Parse and log everything, but write zero files.
    verbose     : Enable DEBUG-level logging.
    """
    run_start = time.monotonic()
    ensure_dir(output_root)
    logger      = setup_logger(output_root, verbose=verbose)
    skipped_log = SkippedLog(output_root / "skipped_files.log")
    media_index_path = output_root / "media_index.json"

    log_banner(logger, f"INSTAGRAM SORTER  v{VERSION}  {'— DRY RUN ' if dry_run else ''}— START")
    logger.info(f"  Input   : {input_path}")
    logger.info(f"  Output  : {output_root}")
    logger.info(f"  Dry run : {dry_run}")
    logger.info(f"  Verbose : {verbose}")
    logger.info(f"  piexif  : {'yes' if _HAS_PIEXIF else 'NO — EXIF stamping disabled'}")
    logger.info(f"  tqdm    : {'yes' if _HAS_TQDM else 'NO — plain logging only'}")

    try:
        export_root, temp_ctx = resolve_input_root(input_path, logger)
    except Exception as exc:
        logger.error(f"FATAL: Cannot resolve input: {exc}")
        raise

    try:
        _run_pipeline(
            export_root      = export_root,
            output_root      = output_root,
            media_index_path = media_index_path,
            skipped_log      = skipped_log,
            logger           = logger,
            workers          = workers,
            dry_run          = dry_run,
            run_start        = run_start,
        )
    except KeyboardInterrupt:
        logger.warning("\nInterrupted (Ctrl-C). Partial results saved — re-run to resume.")
        raise
    except Exception:
        logger.error("FATAL unhandled exception:")
        logger.error(traceback.format_exc())
        raise
    finally:
        if temp_ctx:
            logger.info("Cleaning up temporary extraction directory...")
            try:
                temp_ctx.cleanup()
            except Exception:
                pass


def _run_pipeline(
    export_root:       Path,
    output_root:       Path,
    media_index_path:  Path,
    skipped_log:       SkippedLog,
    logger:            logging.Logger,
    workers:           int,
    dry_run:           bool,
    run_start:         float,
) -> None:
    stats = RunStats()

    # ── Phase 1: Discovery ───────────────────────────────────────────────────
    log_banner(logger, "PHASE 1 — DISCOVERY", char="-", width=50)
    groups = group_html_files(export_root, logger)

    if not groups:
        logger.error("No HTML files found. Verify your export path.")
        raise FileNotFoundError("No HTML files found in the selected export.")

    stats.chats_total = len(groups)

    raw_index   = load_json(media_index_path, {})
    media_index = MediaIndex(raw_index if isinstance(raw_index, dict) else {})
    if len(media_index):
        logger.info(f"  Loaded existing media index: {len(media_index):,} entries.")

    n_workers    = workers if workers > 0 else (os.cpu_count() or 4)
    digest_cache = MediaDigestCache()
    executor     = ThreadPoolExecutor(max_workers=n_workers, thread_name_prefix="media")
    logger.info(f"  Thread pool: {n_workers} worker(s)")

    # ── Phase 2: Per-chat processing ─────────────────────────────────────────
    log_banner(logger, "PHASE 2 — PROCESSING CHATS", char="-", width=50)

    bundles:     List[ChatBundle] = []
    failed_chats: List[str]       = []
    group_items = list(groups.items())

    progress = (
        tqdm(group_items, desc="Chats", unit="chat", dynamic_ncols=True)
        if _HAS_TQDM else group_items
    )

    for chat_num, (group_key, files) in enumerate(progress, 1):
        files = sort_chunk_files(files)

        # Detect chat name from first chunk
        chat_name = Path(group_key.split("::")[-1]).name or f"chat_{chat_num}"
        try:
            raw       = files[0].read_text(encoding="utf-8", errors="ignore")
            soup      = BeautifulSoup(raw, "html.parser", parse_only=SoupStrainer("title"))
            chat_name = detect_chat_name(soup, files[0])
        except Exception:
            pass

        safe_chat_name = sanitize_name(chat_name, fallback=f"chat_{chat_num:04d}")

        logger.info(
            f"\n[{chat_num}/{len(groups)}] '{chat_name}'  "
            f"(folder: '{safe_chat_name}', {len(files)} chunk{'s' if len(files)>1 else ''})"
        )

        # ── Per-chat isolation: wrap the whole chat in try/except ────────────
        # This is the key change: one broken chat logs the error and
        # continues to the next — it never aborts the whole run.
        try:
            _process_single_chat(
                chat_num         = chat_num,
                total_chats      = len(groups),
                group_key        = group_key,
                files            = files,
                chat_name        = chat_name,
                safe_chat_name   = safe_chat_name,
                export_root      = export_root,
                output_root      = output_root,
                media_index      = media_index,
                media_index_path = media_index_path,
                digest_cache     = digest_cache,
                skipped_log      = skipped_log,
                executor         = executor,
                stats            = stats,
                logger           = logger,
                dry_run          = dry_run,
                bundles          = bundles,
            )

        except Exception:
            # Log the full traceback but keep going
            tb = traceback.format_exc()
            logger.error(
                f"  !! Chat '{chat_name}' failed with an unhandled error. "
                f"Skipping and continuing with remaining chats."
            )
            logger.error(tb)
            skipped_log.write(f"CHAT FAILED\t{chat_name}\t{group_key}\n{tb}")
            failed_chats.append(chat_name)
            stats.add(chats_failed=1)

    # ── Shutdown & final artefacts ────────────────────────────────────────────
    executor.shutdown(wait=True)

    if not dry_run:
        save_json(media_index_path, media_index.snapshot())
        save_json(
            output_root / "manifest.json",
            {
                "version":      VERSION,
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "elapsed_sec":  round(time.monotonic() - run_start, 1),
                "stats": {
                    "chats":         stats.chats_done,
                    "chats_failed":  stats.chats_failed,
                    "messages":      stats.messages_total,
                    "media_copied":  stats.media_copied,
                    "media_dupes":   stats.media_dupes,
                    "media_missing": stats.media_missing,
                    "media_errors":  stats.media_errors,
                    "parse_errors":  stats.parse_errors,
                },
                "failed_chats": failed_chats,
                "chats": [
                    {
                        "chat_name":     b.chat_name,
                        "message_count": len(b.messages),
                        "source_files":  len(b.source_files),
                    }
                    for b in bundles
                ],
            },
        )

    elapsed = elapsed_str(time.monotonic() - run_start)
    log_banner(logger, "COMPLETE", width=60)
    logger.info(f"  Elapsed         : {elapsed}")
    logger.info(f"  Chats processed : {stats.chats_done:,} / {stats.chats_total:,}")
    if failed_chats:
        logger.warning(f"  Chats FAILED    : {len(failed_chats):,}  ← see skipped_files.log")
        for name in failed_chats:
            logger.warning(f"    - {name}")
    logger.info(f"  Total messages  : {stats.messages_total:,}")
    logger.info(f"  Media copied    : {stats.media_copied:,}")
    logger.info(f"  Media dupes     : {stats.media_dupes:,}  (saved disk space)")
    logger.info(f"  Media missing   : {stats.media_missing:,}")
    logger.info(f"  Errors          : {stats.media_errors + stats.parse_errors:,}")
    if dry_run:
        logger.info("  (DRY RUN — no files written)")
    log_banner(logger, "", width=60)


def _process_single_chat(
    chat_num:         int,
    total_chats:      int,
    group_key:        str,
    files:            List[Path],
    chat_name:        str,
    safe_chat_name:   str,
    export_root:      Path,
    output_root:      Path,
    media_index:      MediaIndex,
    media_index_path: Path,
    digest_cache:     MediaDigestCache,
    skipped_log:      SkippedLog,
    executor:         ThreadPoolExecutor,
    stats:            RunStats,
    logger:           logging.Logger,
    dry_run:          bool,
    bundles:          List[ChatBundle],
) -> None:
    """Process all HTML chunks for one conversation. Raises on fatal errors."""

    chat_dir = ensure_dir(output_root / "Chats" / safe_chat_name)
    processed_sources, existing_messages, _ = load_chat_state(chat_dir)

    bundle = ChatBundle(
        chat_name    = chat_name,
        group_key    = group_key,
        source_files = [str(p) for p in files],
        messages     = list(existing_messages),
    )

    chat_new_msg     = 0
    chat_media_copied = 0
    chat_media_dupes  = 0
    chat_errors       = 0

    for chunk_idx, html_path in enumerate(files):
        # ── Resume check ──────────────────────────────────────────────────
        try:
            st       = html_path.stat()
            file_sig = f"{st.st_mtime_ns}:{st.st_size}"
        except OSError:
            file_sig = html_path.name

        prior = processed_sources.get(str(html_path), {})
        if prior.get("signature") == file_sig and prior.get("status") == "done":
            logger.info(
                f"  [{chunk_idx+1}/{len(files)}] {html_path.name}  ← already done, skipping."
            )
            continue

        logger.info(f"  [{chunk_idx+1}/{len(files)}] Parsing {html_path.name}...")

        # ── Parse ─────────────────────────────────────────────────────────
        try:
            parsed_messages = parse_html_file(
                html_path    = html_path,
                export_root  = export_root,
                chat_name    = chat_name,
                source_rank  = extract_chunk_rank(html_path),
                logger       = logger,
            )
        except Exception as exc:
            logger.error(f"    Parse error: {exc}")
            skipped_log.write(f"{html_path}\tparse error\t{exc}")
            chat_errors += 1
            stats.add(parse_errors=1)
            continue

        n_att = sum(len(m.attachments) for m in parsed_messages)
        logger.info(
            f"    {len(parsed_messages):,} messages  |  {n_att:,} attachment reference(s)"
        )

        # ── Media ─────────────────────────────────────────────────────────
        media_jobs = [
            (msg, att)
            for msg in parsed_messages
            for att in msg.attachments
            if att.kind == "local_media" and att.resolved_path
        ]

        if media_jobs:
            logger.info(f"    Queuing {len(media_jobs):,} media file(s)...")
            next_base = media_index.next_index()

            futures = {
                executor.submit(
                    process_single_media,
                    msg, att, output_root, chat_name,
                    next_base + i,
                    digest_cache, media_index, skipped_log, logger, dry_run,
                ): att
                for i, (msg, att) in enumerate(media_jobs)
            }

            done_count = 0
            for future in as_completed(futures):
                done_count += 1
                if done_count % 100 == 0:
                    logger.info(f"    Media: {done_count:,}/{len(media_jobs):,} done...")
                try:
                    status, att, digest, rel_path = future.result()
                except Exception as exc:
                    logger.error(f"    Worker error: {exc}")
                    chat_errors += 1
                    stats.add(media_errors=1)
                    continue

                if status == "copied":
                    chat_media_copied += 1
                    stats.add(media_copied=1)
                elif status == "duplicate":
                    chat_media_dupes += 1
                    stats.add(media_dupes=1)
                elif status == "missing":
                    stats.add(media_missing=1)
                elif status == "error":
                    chat_errors += 1
                    stats.add(media_errors=1)

            logger.info(
                f"    Media: {chat_media_copied} copied, {chat_media_dupes} dupes, "
                f"{chat_errors} errors."
            )

        chat_new_msg += len(parsed_messages)
        bundle.messages.extend(parsed_messages)
        bundle.messages = merge_sorted_messages(bundle.messages, logger)

        # ── Checkpoint ────────────────────────────────────────────────────
        processed_sources[str(html_path)] = {"signature": file_sig, "status": "done"}
        if not dry_run:
            try:
                save_json(chat_dir / "chat_state.json", bundle_to_state(bundle, processed_sources))
                save_json(media_index_path, media_index.snapshot())
            except Exception as exc:
                # Log but don't crash — worst case we redo this chunk on next run
                logger.warning(f"    Checkpoint save failed (will retry next run): {exc}")

    # ── Finalise ──────────────────────────────────────────────────────────────
    if not dry_run:
        try:
            save_json(chat_dir / "transcript.json", [message_to_dict(m) for m in bundle.messages])
            render_chat_viewer(bundle, chat_dir / "chat_viewer.html")
        except Exception as exc:
            logger.error(f"  Failed to write final output for '{chat_name}': {exc}")

    stats.add(messages_total=len(bundle.messages), chats_done=1)
    bundles.append(bundle)

    logger.info(
        f"  ✓ '{chat_name}' done: {len(bundle.messages):,} messages, "
        f"{chat_media_copied} media copied, {chat_media_dupes} dupes."
    )


# ── GUI ───────────────────────────────────────────────────────────────────────

def choose_paths_gui() -> Tuple[Path, Path]:
    if not _HAS_TK:
        raise RuntimeError(
            "tkinter is not available. Use --input and --output arguments instead."
        )
    root = tk.Tk()
    root.title(f"Instagram HTML Sorter  v{VERSION}")
    root.geometry("600x270")
    root.resizable(False, False)

    selected_input:  List[Optional[Path]] = [None]
    selected_output: List[Optional[Path]] = [None]

    def set_in(cmd: Any) -> None:
        p = cmd(title="Select Instagram export (folder or ZIP)")
        if p:
            selected_input[0] = Path(p)
            input_var.set(f"Input:   {p}")

    def set_out() -> None:
        p = filedialog.askdirectory(title="Select output folder")
        if p:
            selected_output[0] = Path(p)
            output_var.set(f"Output:  {p}")

    def run() -> None:
        if selected_input[0] and selected_output[0]:
            root.destroy()
        else:
            messagebox.showerror("Missing selection", "Please select both input and output.")

    main = ttk.Frame(root, padding=20)
    main.pack(fill="both", expand=True)

    input_var  = tk.StringVar(value="Input:   none selected")
    output_var = tk.StringVar(value="Output:  none selected")
    ttk.Label(main, textvariable=input_var,  font=("", 10)).pack(anchor="w", pady=(0, 4))
    ttk.Label(main, textvariable=output_var, font=("", 10)).pack(anchor="w", pady=(0, 16))

    btn_row = ttk.Frame(main)
    btn_row.pack(fill="x")
    ttk.Button(btn_row, text="Select ZIP",    command=lambda: set_in(filedialog.askopenfilename)).pack(side="left", padx=(0, 8))
    ttk.Button(btn_row, text="Select Folder", command=lambda: set_in(filedialog.askdirectory)).pack(side="left", padx=(0, 8))
    ttk.Button(btn_row, text="Choose Output", command=set_out).pack(side="left", padx=(0, 8))
    ttk.Button(btn_row, text="▶  Start",      command=run).pack(side="right")

    ttk.Separator(main).pack(fill="x", pady=14)
    ttk.Label(
        main,
        text="Tip: output folder will contain Chats/, Media/, manifest.json, and run.log.",
        foreground="gray", font=("", 9),
    ).pack(anchor="w")

    root.mainloop()

    if not selected_input[0] or not selected_output[0]:
        sys.exit("No paths selected — exiting.")
    return selected_input[0], selected_output[0]


# ── CLI entry point ───────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="instagram_html_sort",
        description=(
            f"Instagram HTML export sorter & offline chat viewer builder  (v{VERSION}).\n"
            "Omit --input / --output to open the GUI picker."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--input",   type=Path, metavar="PATH",
                        help="Export folder or .zip file")
    parser.add_argument("--output",  type=Path, metavar="PATH",
                        help="Destination directory")
    parser.add_argument("--workers", type=int, default=0, metavar="N",
                        help="Media-processing thread count (default: auto = cpu_count)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and log everything but write no files")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Enable DEBUG-level logging")
    args = parser.parse_args()

    if args.input and args.output:
        process_export(
            input_path  = args.input,
            output_root = args.output,
            workers     = args.workers,
            dry_run     = args.dry_run,
            verbose     = args.verbose,
        )
    elif args.input or args.output:
        parser.error("Supply both --input and --output, or neither (to use the GUI).")
    else:
        input_path, output_root = choose_paths_gui()
        process_export(
            input_path  = input_path,
            output_root = output_root,
            workers     = args.workers,
            dry_run     = args.dry_run,
            verbose     = args.verbose,
        )


if __name__ == "__main__":
    main()
