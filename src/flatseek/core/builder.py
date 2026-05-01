"""ETL pipeline: CSV → hash-partitioned trigram inverted index.

Index layout:
  {output_dir}/
    index/{aa}/{bb}/idx.bin   binary posting list file (append-mode per prefix)
    docs/docs_{start:010d}.zlib  compressed JSON doc chunks
    stats.json

Index entry format (binary, sequential in .idx files):
  H  term_len   (2 bytes, little-endian)
  *  term_bytes
  I  doclist_len (4 bytes, little-endian)
  *  doclist     (delta-encoded varint doc_ids)

Terms indexed per cell value:
  {col}:{token}      exact token scoped to column
  {col}:~{trigram}   3-gram for wildcard queries on column
  {token}            global exact (tokens >= 3 chars)
  ~{trigram}         global 3-gram (cross-column wildcard)
"""

import os
import io
import csv
import gc
import gzip
import bz2
import json
import struct
import hashlib
import zlib
import re
import time
import sys
import threading
from array import array as _array
from collections import defaultdict
from datetime import datetime
from functools import lru_cache
from itertools import islice

# orjson is 3-5x faster than stdlib json for doc store serialisation.
# Used only for doc chunk encode/decode (hot path). Config files still use
# stdlib json so they remain human-readable and indent-formatted.
try:
    import orjson as _orjson
    def _doc_dumps(obj: dict) -> bytes:
        return _orjson.dumps(obj)
    def _doc_loads(data: bytes) -> dict:
        return _orjson.loads(data)
except ImportError:
    def _doc_dumps(obj: dict) -> bytes:
        return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    def _doc_loads(data: bytes) -> dict:
        return json.loads(data)

print("Latest nih!!")

# ─── Compressed source file helpers ──────────────────────────────────────────
#
# Supported transparent compression wrappers for source data files.
# The index itself is never compressed with these — only source CSV/JSON input.

_COMPRESS_OPENERS = {".gz": gzip.open, ".bz2": bz2.open}


def _compress_ext(path):
    """Return '.gz', '.bz2', or '' if the file has no compression wrapper."""
    for cext in _COMPRESS_OPENERS:
        if path.endswith(cext):
            return cext
    return ""


def _inner_ext(path):
    """Return the data extension, stripping any compression suffix.

    Examples:
        data.csv.gz  → .csv
        data.jsonl   → .jsonl
        data.csv     → .csv
    """
    cext = _compress_ext(path)
    if cext:
        return os.path.splitext(os.path.basename(path)[: -len(cext)])[1].lower()
    return os.path.splitext(path)[1].lower()


def _open_source(path, binary=False, newline=None):
    """Open a source data file for reading, transparently decompressing if needed.

    Returns a file-like object.  Caller is responsible for closing it (use as
    context manager or call .close()).

    Pass newline="" when opening CSV files so csv.DictReader handles line
    endings correctly (Python docs requirement — prevents double-translation
    of \\r\\n by both TextIOWrapper and csv module).
    """
    cext = _compress_ext(path)
    if cext:
        opener = _COMPRESS_OPENERS[cext]
        if binary:
            return opener(path, "rb")
        kw = dict(encoding="utf-8", errors="replace")
        if newline is not None:
            kw["newline"] = newline
        return opener(path, "rt", **kw)
    if binary:
        return open(path, "rb")
    kw = dict(encoding="utf-8", errors="replace", buffering=8 * 1024 * 1024)
    if newline is not None:
        kw["newline"] = newline
    return open(path, "r", **kw)


# Pre-compiled struct packers for the hot path in _flush_terms.
# struct.pack("<H", n) parses the format string every call; Struct.pack skips that.
_PACK_H = struct.Struct("<H").pack   # uint16 — term length prefix
_PACK_I = struct.Struct("<I").pack   # uint32 — doclist length prefix

DOC_CHUNK_SIZE      = 100_000   # rows per doc chunk file (must match query_engine)
CHECKPOINT_ROWS     = 50_000    # flush terms+docs and save checkpoint every N rows
BUFFER_FLUSH_BYTES  = 512_000   # flush a prefix buffer when it hits this size (~512 KB)

# Minimum size a prefix buffer must reach before it is included in the background
# checkpoint write.  Below this it stays in memory and accumulates across checkpoints.
# This is the primary fix for the IOPS bottleneck: without it, every checkpoint
# writes all 65536 prefix files (even ~175-byte ones) — 65536×8 workers = 524K
# file-open/write/close ops per cycle, each costing 50-100 μs on macOS APFS.
# At 8 KB threshold: each prefix is written every ~46 checkpoints (not every one),
# reducing file ops from 524K to ~11K per cycle — ~47× fewer IOPS.
# Memory cost: at most 8 KB × 65536 prefixes = 512 MB per worker (in practice less).
CHECKPOINT_WRITE_BYTES = 8 * 1024
# Hard cap on in-memory buffer accumulation (normal/non-daemon mode).
# If self._buffers total exceeds this, _bg_flush_io lowers the per-buffer
# write threshold enough to drain back down to this level, regardless of the
# adaptive CHECKPOINT_WRITE_BYTES value.  Keeps the buffer working set inside
# L3 cache to prevent encode-time blowup as buffers accumulate across checkpoints.
MEM_FLUSH_BYTES = 50 * 1024 * 1024  # 50 MB

# ── Segment-append (S3-friendly) ──────────────────────────────────────────────
# When a prefix's idx file exceeds this size, subsequent checkpoints write new
# immutable segment files (idx_seg0001.bin, idx_seg0002.bin ...) instead of
# reading and recompressing the entire accumulated file on every checkpoint.
# This eliminates the O(N²) recompression bottleneck on S3 and large indexes.
# At finalize, all segments are merged into one idx.bin with a single compress.
SEGMENT_SWITCH_SIZE = 5 * 1024 * 1024  # 5 MB — switch to segments after this

# ── Daemon (in-memory) mode ────────────────────────────────────────────────
# In daemon mode the checkpoint never writes to disk.  A lazy-writer thread
# drains buffers to disk in the background every LAZY_WRITE_INTERVAL seconds.
# Only buffers larger than LAZY_WRITE_BYTES are written; smaller ones wait.
LAZY_WRITE_INTERVAL = 10         # seconds between lazy-writer cycles
LAZY_WRITE_BYTES    = 512 * 1024  # drain buffers ≥ 512 KB each cycle
# Safety cap: if total in-memory buffer bytes exceed this, the lazy writer
# lowers the threshold to MAX_MEM_BYTES / 4 and flushes more aggressively.
LAZY_MAX_MEM_BYTES  = 6 * 1024 * 1024 * 1024  # 6 GB default safety cap

ZLIB_LEVEL = 6               # default zlib level for doc store and pre-compress
ZLIB_LEVEL_IDX = 9           # index postings compress better — use max level

# Max threads used for parallel buffer flushes in finalize()
FLUSH_THREADS = min(8, ((__import__("os").cpu_count() or 2)))

# ── Bucket-batch finalize ───────────────────────────────────────────────────
# At finalize, instead of flushing 47K individual prefix files (47K open+write+close
# syscalls = ~10s syscall overhead on APFS), group buffers by bucket directory
# and write each bucket's all prefixes in ONE sequential pass per thread.
# This reduces ~47K file ops → ~256 bucket ops (one per thread per bucket).
# Each thread handles N buckets; each bucket writes its ~185 prefixes sequentially.
BATCH_SIZE = 32  # prefixes per batch write within a bucket (not file-per-prefix)

# Optional zstd for doc store — ~7x compression vs ~6x zlib, 7x faster decompress.
# Install with: pip install zstandard
import threading as _threading
_zstd_tls = _threading.local()  # thread-local zstd contexts (ZstdCompressor/Decompressor
                                 # are NOT thread-safe — shared instances → segfault)
try:
    import zstandard as _zstd
    HAS_ZSTD = True
except ImportError:
    HAS_ZSTD = False

_ZSTD_MAGIC = b'\x28\xb5\x2f\xfd'   # first 4 bytes of every zstd frame


def _compress_doc(data: bytes) -> bytes:
    """Compress doc chunk bytes. Uses zstd if available, zlib otherwise."""
    if HAS_ZSTD:
        if not hasattr(_zstd_tls, "cctx"):
            _zstd_tls.cctx = _zstd.ZstdCompressor(level=9)
        return _zstd_tls.cctx.compress(data)
    return zlib.compress(data, ZLIB_LEVEL)


def _decompress_doc(data: bytes) -> bytes:
    """Decompress doc chunk bytes. Auto-detects zstd or zlib."""
    if data[:4] == _ZSTD_MAGIC:
        if not hasattr(_zstd_tls, "dctx"):
            _zstd_tls.dctx = _zstd.ZstdDecompressor()
        return _zstd_tls.dctx.decompress(data)
    return zlib.decompress(data)

# Storage/semantic types where trigrams are useful (wildcard search).
# Includes both new storage types (TEXT) and legacy semantic names (name, email,
# address, city, province, country, username, phone, birthday, date, string, other).
# Types not listed here use exact token only — no trigrams generated.
_TRIGRAM_TYPES = {
    # New storage types
    "TEXT",
    # Legacy semantic names (backward compat with existing indexes)
    "string", "other",
}

# Only generate trigrams for tokens up to this length.
# Long tokens (full email address, full address string) produce many
# low-value trigrams — their sub-tokens (domain, local part, street name, etc.) already
# cover all practical wildcard patterns and are under this limit.
MAX_TRIGRAM_TOKEN_LEN = 15


# ─── Binary encoding ─────────────────────────────────────────────────────────

def encode_doclist(doc_ids):
    """Delta-encode sorted unique doc_ids as varints.

    Deltas are always non-negative since doc_ids are appended in monotonically
    increasing order. Duplicates (same term from multiple columns) are skipped.
    """
    if not doc_ids:
        return b""
    doc_ids = sorted(set(doc_ids))   # ensure sorted + unique; set() dedupes
    out = bytearray()
    prev = 0
    last = -1
    for did in doc_ids:
        if did == last:
            continue
        d = did - prev
        last = did
        prev = did
        while d >= 0x80:
            out.append((d & 0x7F) | 0x80)
            d >>= 7
        out.append(d)
    return bytes(out)


# Bounded cache for the 65536 possible prefix strings ("00/00" … "ff/ff").
# Unlike lru_cache(term → prefix), this caches the STRING OBJECT by its 16-bit
# bucket index — so at most 65536 entries are ever created.  After warmup every
# call is a cheap dict lookup.  This avoids creating 5M+ new f-string objects
# per checkpoint (which caused GC pressure and degradation with bare CRC32).
_PREFIX_CACHE: dict = {}

def term_hash_b(term_b: bytes) -> str:
    """Map pre-encoded term bytes → 'aa/bb' directory prefix (65536 buckets).

    Takes bytes directly so callers that already have term.encode() don't pay
    for a second encode call.  The prefix string is cached by 16-bit bucket
    index (bounded to 65536 entries) — same string object reused across all
    terms that share a prefix, no repeated f-string allocation.
    """
    h = zlib.crc32(term_b) & 0xFFFFFFFF
    k = h >> 16
    s = _PREFIX_CACHE.get(k)
    if s is None:
        _PREFIX_CACHE[k] = s = f"{(h >> 24) & 0xFF:02x}/{(h >> 16) & 0xFF:02x}"
    return s

def term_hash(term: str) -> str:
    """Map term string → 'aa/bb' directory prefix.  Used by query_engine."""
    return term_hash_b(term.encode())


# ─── Value normalization ──────────────────────────────────────────────────────

def normalize_date(val):
    """Normalize date string to YYYYMMDD.

    Accepts -, /, |, . as date-part separators (e.g. 1982-03-28, 28|03|1982).
    """
    v = val.strip().replace(" ", "")
    m = re.match(r"^(\d{4})[-/|.](\d{2})[-/|.](\d{2})$", v)
    if m:
        return m.group(1) + m.group(2) + m.group(3)
    m = re.match(r"^(\d{2})[-/|.](\d{2})[-/|.](\d{4})$", v)
    if m:
        return m.group(3) + m.group(2) + m.group(1)
    if re.match(r"^\d{8}$", v):
        return v
    return v


def normalize_phone(val):
    """Strip non-digits, normalize 62xx → 08xx."""
    digits = re.sub(r"[^\d]", "", val)
    if digits.startswith("62") and len(digits) > 3:
        digits = "0" + digits[2:]
    return digits


def normalize_value(val, col_key):
    """Normalize a value based on column name patterns.

    Normalization is determined by column NAME (not storage type), so that
    DATE-stored birthday columns and generic date columns both get date normalization,
    and KEYWORD-stored phone columns get phone normalization.
    """
    if not val or not val.strip():
        return val
    # Normalize date columns (birthday, date, created_at, etc.)
    # These are identified by column name patterns, regardless of storage type.
    col_lower = col_key.lower()
    if col_lower in (
        {"birthday", "dob", "birth_date", "birthdate", "date_of_birth",
         "tgl_lahir", "tanggal_lahir", "lahir", "birth", "born",
         "tgl_bergabung", "joined", "join_date", "created_at", "updated_at",
         "tgl_daftar", "register_date", "tgl_registrasi", "tanggal_daftar",
         "tgl_buat", "tgl_update", "tanggal", "date"}):
        return normalize_date(val)
    # Normalize phone columns (phone, hp, no_hp, etc.)
    if col_lower in (
        {"phone", "hp", "handphone", "telp", "telephone", "mobile",
         "no_hp", "nomor_hp", "nomor_telepon", "nohp", "telepon",
         "cell", "cellphone", "no_telp", "phone_number"}):
        return normalize_phone(val)
    return val


# ─── Progress bar ────────────────────────────────────────────────────────────

def _count_rows(path):
    """Fast row count: count newlines in binary mode, subtract 1 for header.

    NOTE: This is an *estimate* for CSV files. If rows contain embedded newlines
    inside quoted fields, the count will be higher than the actual row count.
    The 'Done' line at the end always shows the real number of rows indexed.

    Returns None for JSON arrays or compressed files (unknown without full parse/decompress).
    """
    # Compressed files cannot be byte-scanned without decompressing — return None (spinner).
    if _compress_ext(path):
        return None
    ext = _inner_ext(path)
    if ext in (".csv", ".jsonl", ".ndjson"):
        with open(path, "rb") as f:
            count = sum(buf.count(b"\n") for buf in iter(lambda: f.read(1 << 20), b""))
        return max(0, count - 1)  # subtract header line for CSV
    return None  # JSON array: unknown upfront


def _fmt_duration(secs):
    secs = int(secs)
    if secs < 60:
        return f"{secs}s"
    if secs < 3600:
        return f"{secs // 60}m{secs % 60:02d}s"
    return f"{secs // 3600}h{(secs % 3600) // 60:02d}m"


def _progress(current, total, t_start, width=30, instant_rate=None):
    """Print an in-place progress bar with ETA.

    instant_rate: sliding-window rows/s (last ~30 s).  When provided it is
    shown as the main rate; the cumulative average is omitted to save space.
    When None (no window data yet) the cumulative average is shown instead.
    """
    elapsed = time.time() - t_start
    avg_rate = current / elapsed if elapsed > 0 else 0
    rate = instant_rate if instant_rate is not None else avg_rate

    if total:
        pct = current / total
        filled = int(width * pct)
        bar = "█" * filled + "░" * (width - filled)
        eta = _fmt_duration((total - current) / rate) if rate > 0 else "?"
        line = (
            f"  [{bar}] {pct:5.1%} "
            f"| {current:>12,} / {total:,} "
            f"| {rate:>9,.0f} rows/s "
            f"| {_fmt_duration(elapsed)} elapsed"
            f"| ETA {eta}"
        )
    else:
        # JSON array — no total known
        spinner = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"[current // 50_000 % 10]
        line = (
            f"  [{spinner}] "
            f"| {current:>12,} rows "
            f"| {rate:>9,.0f} rows/s "
            f"| {_fmt_duration(elapsed)} elapsed"
        )

    # Truncate to terminal width
    try:
        tw = os.get_terminal_size().columns - 1
    except OSError:
        tw = 100
    sys.stderr.write("\r" + line[:tw])
    sys.stderr.flush()


# ─── Build estimate (pre-sampling) ───────────────────────────────────────────

def _sample_estimate(path, col_schema, headers, delimiter, columns, n=5_000):
    """Sample the first N rows to estimate indexing speed and storage size.

    Builds a real mini-index into a temp directory (no shortcuts) so the
    measured byte counts reflect actual on-disk layout.

    Returns:
        {"rows_per_sec": float, "idx_bytes_per_row": float,
         "doc_bytes_per_row": float}
        or None on failure.
    """
    import tempfile
    try:
        t0 = time.time()
        with tempfile.TemporaryDirectory() as tmp:
            mini = IndexBuilder(tmp)
            count = 0
            with _open_source(path) as f:
                reader = csv.DictReader(f, fieldnames=columns or None,
                                        delimiter=delimiter)
                for row in islice(reader, n):
                    mini.add_row(row, headers, col_schema=col_schema)
                    count += 1

            if count == 0:
                return None

            elapsed = time.time() - t0

            # Force flush everything so temp dir reflects real on-disk sizes
            mini._flush_terms()
            for prefix in list(mini._buffers):
                mini._flush_prefix_buffer(prefix)
            mini._flush_docs()

            idx_bytes = sum(
                os.path.getsize(os.path.join(r, fn))
                for r, _, fs in os.walk(os.path.join(tmp, "index"))
                for fn in fs
            )
            doc_bytes = sum(
                os.path.getsize(os.path.join(r, fn))
                for r, _, fs in os.walk(os.path.join(tmp, "docs"))
                for fn in fs
            )

        return {
            "rows_per_sec":     count / elapsed,
            "idx_bytes_per_row": idx_bytes / count,
            "doc_bytes_per_row": doc_bytes / count,
        }
    except Exception:
        return None


def _print_estimate(est, total_rows, skip_rows=0, output_dir=None):
    """Print a human-readable build estimate box, including a disk-space check."""
    import shutil

    remaining = (total_rows - skip_rows) if total_rows else None

    rps   = est["rows_per_sec"]
    i_bpr = est["idx_bytes_per_row"]
    d_bpr = est["doc_bytes_per_row"]

    lines = [f"  Estimate  (sampled {5_000:,} rows)"]
    lines.append(f"    Speed  : ~{rps:>10,.0f} rows/s")

    total_bytes = None
    if remaining:
        eta_s   = remaining / rps
        idx_gb  = remaining * i_bpr / 1e9
        doc_gb  = remaining * d_bpr / 1e9
        total_bytes = remaining * (i_bpr + d_bpr)
        lines.append(f"    ETA    : ~{_fmt_duration(eta_s):>10}  ({remaining:,} rows remaining)")
        lines.append(f"    Index  : ~{idx_gb:>7.1f} GB  (upper bound)")
        lines.append(f"    Docs   : ~{doc_gb:>7.1f} GB")
        lines.append(f"    Total  : ~{idx_gb + doc_gb:>7.1f} GB")
    else:
        lines.append(f"    ETA    : (row count still being computed)")
        lines.append(f"    Index  : ~{i_bpr / 1e3:.1f} KB/row")
        lines.append(f"    Docs   : ~{d_bpr / 1e3:.1f} KB/row")

    # ── Disk space check ──────────────────────────────────────────────────────
    check_path = output_dir or "."
    try:
        usage   = shutil.disk_usage(check_path)
        free_gb = usage.free / 1e9
        total_disk_gb = usage.total / 1e9
        lines.append(f"    Disk   :  {free_gb:.1f} GB free of {total_disk_gb:.1f} GB  ({check_path})")

        if total_bytes is not None:
            needed_gb = total_bytes / 1e9
            # Add 10% safety margin
            if usage.free < total_bytes * 1.10:
                short_gb = needed_gb * 1.10 - free_gb
                lines.append(f"")
                lines.append(f"  ⚠  WARNING: Not enough disk space!")
                lines.append(f"     Needed : ~{needed_gb:.1f} GB  (+10% margin = {needed_gb*1.10:.1f} GB)")
                lines.append(f"     Free   :  {free_gb:.1f} GB")
                lines.append(f"     Short  :  {short_gb:.1f} GB")
                lines.append(f"     Free up space or point -o to a larger volume before building.")
            elif usage.free < total_bytes * 1.25:
                lines.append(f"  ⚠  Low disk: only {free_gb:.1f} GB free, estimated need ~{needed_gb:.1f} GB. Proceed with caution.")
    except Exception:
        pass   # disk_usage can fail on some network/virtual filesystems

    print("\n".join(lines))


# ─── Parallel-build byte-range reader ────────────────────────────────────────

class _ByteRangeReader(io.RawIOBase):
    """Binary file reader bounded to [byte_start, byte_end) with header injection.

    Used for single-file parallel builds: each worker gets a non-overlapping
    byte slice.  The original file's header line is injected at the front of
    every worker's stream so pandas can resolve column names even when the
    worker starts mid-file.  Seeking is O(1) — no rows are read-and-discarded.
    """
    def __init__(self, path, byte_start, byte_end, header_bytes=b""):
        super().__init__()
        self._f    = open(path, "rb")
        self._f.seek(byte_start)
        self._end  = byte_end
        self._hdr  = header_bytes
        self._hpos = 0

    def readable(self):
        return True

    def readinto(self, buf):
        mv    = memoryview(buf)
        total = 0
        n     = len(buf)

        # 1. Drain injected header first
        if self._hpos < len(self._hdr):
            take = min(n, len(self._hdr) - self._hpos)
            mv[:take] = self._hdr[self._hpos:self._hpos + take]
            self._hpos += take
            total += take
            if total == n:
                return total

        # 2. Stream from file up to byte_end
        rem = self._end - self._f.tell()
        if rem <= 0:
            return total or 0
        data = self._f.read(min(n - total, rem))
        if data:
            mv[total:total + len(data)] = data
            total += len(data)
        return total or 0

    def close(self):
        if not self.closed:
            self._f.close()
        super().close()


def _split_file_byte_ranges(path, n_workers):
    """Compute n_workers non-overlapping byte ranges for a CSV file.

    Returns (header_bytes, [(start, end), ...]) where all ranges cover the
    data portion (after the header line).  Each (start, end) is aligned to a
    line boundary so no row is split across two workers.
    """
    size = os.path.getsize(path)
    with open(path, "rb") as f:
        header_bytes = f.readline()
        if not header_bytes.endswith(b"\n"):
            header_bytes += b"\n"
        data_start = f.tell()
        data_size  = size - data_start

        boundaries = [data_start]
        for i in range(1, n_workers):
            target = data_start + data_size * i // n_workers
            f.seek(target)
            f.readline()         # skip partial line → align to next full line
            boundaries.append(f.tell())
        boundaries.append(size)

    ranges = [(boundaries[i], boundaries[i + 1]) for i in range(n_workers)]
    return header_bytes, ranges


# ─── Tokenization ────────────────────────────────────────────────────────────

_SPLIT_RE  = re.compile(r"[\s_\-,./\\|]+")
_DIGITS_RE = re.compile(r"[^\d]")

def tokenize(value):
    """Extract searchable tokens from a cell value."""
    v = value.strip()
    if not v:
        return []
    vl = v.lower()

    seen = set()
    result = []

    def _add(t):
        if len(t) >= 2 and t not in seen:
            seen.add(t)
            result.append(t)

    _add(vl)
    for tok in _SPLIT_RE.split(vl):
        _add(tok)

    if "@" in vl:
        local, domain = vl.split("@", 1)
        _add(local)
        _add(domain)

    # Digit suffixes/prefixes — only run when value actually contains digits.
    # Otherwise _DIGITS_RE.sub allocates a new empty string per token-free row.
    has_digit = False
    for c in v:
        if '0' <= c <= '9':
            has_digit = True
            break
    if has_digit:
        digits = _DIGITS_RE.sub("", v)
        if len(digits) >= 4:
            _add(digits)
            for n in (4, 6, 8):
                if len(digits) > n:
                    _add(digits[-n:])   # suffixes: last 4/6/8 digits
                    _add(digits[:n])    # prefixes: first 4/6/8 digits

    return result


def make_trigrams(token):
    """Yield all 3-grams in a token."""
    for i in range(len(token) - 2):
        yield token[i:i+3]


# ─── IndexBuilder ─────────────────────────────────────────────────────────────

def _doc_fingerprint(doc, dedup_fields=None):
    """Return a 64-bit int fingerprint for a doc dict.

    Args:
        doc:          canonical doc dict (post-normalization, post col_key mapping)
        dedup_fields: list of canonical col_keys to hash, or None = all non-meta fields

    Returns an int (first 8 bytes of MD5 as little-endian uint64) instead of a
    hex string.  Storing ints in the seen-set uses ~28 bytes per entry in CPython
    vs ~56 bytes for a 32-char hex string — halves memory at 10M+ row builds.
    """
    if dedup_fields:
        parts = [(k, str(doc.get(k, ""))) for k in sorted(dedup_fields)]
    else:
        parts = sorted((k, str(v)) for k, v in doc.items() if not k.startswith("_"))
    # Use JSON encoding so field values containing "|" or "=" don't collide with
    # separator characters.  "|".join(f"{k}={v}") was ambiguous: a value of "x|b=y"
    # in field "a" produced the same payload as separate fields a="x", b="y".
    payload = json.dumps(parts, ensure_ascii=False, separators=(",", ":")).encode()
    return int.from_bytes(hashlib.md5(payload).digest()[:8], "little")


class IndexBuilder:
    def __init__(self, output_dir, column_map=None, start_doc_id=0, dataset=None,
                 checkpoint_cb=None, delimiter=",", columns=None, worker_id=None,
                 dedup_fields=None, doc_id_end=None, daemon=False):
        """
        Args:
            output_dir:    where to write index/, docs/, stats.json
            column_map:    from classify.build_column_map()
            start_doc_id:  continue from a previous build (from manifest)
            dataset:       optional dataset label injected as _dataset field on every doc
            checkpoint_cb: callable(rows_processed, doc_counter) — called after each
                           periodic flush so the caller can persist resume state
            delimiter:     CSV field delimiter (default ','; use '#' for '#'-separated files)
            columns:       explicit list of column names for headerless files; when set
                           the first row of every CSV is treated as data, not a header
            worker_id:     integer worker ID for distributed builds; each worker writes to
                           idx_w{worker_id}.bin instead of idx.bin so there are no conflicts
                           when multiple workers share the same output volume
            doc_id_end:    inclusive upper bound for doc_ids this worker may assign
                           (from the build plan). If doc_counter exceeds this, the build
                           stops immediately — prevents silent data corruption from
                           overlapping doc_id ranges when the plan underestimates row count.
            daemon:        if True, never flush prefix buffers to disk at checkpoint — all
                           encoded data accumulates in RAM.  A background lazy-writer thread
                           drains buffers above LAZY_WRITE_BYTES to disk every
                           LAZY_WRITE_INTERVAL seconds so RAM stays bounded while the
                           indexing hot-loop is never blocked by disk I/O.  Finalize()
                           flushes everything remaining.  Requires enough free RAM to hold
                           all accumulated buffers between lazy-writer cycles.
        """
        self.output_dir = output_dir
        self.index_dir = os.path.join(output_dir, "index")
        self.docs_dir = os.path.join(output_dir, "docs")
        self.column_map = column_map or {}
        self.dataset = dataset  # injected as _dataset on every doc
        self.delimiter = delimiter
        self.columns = columns  # explicit header names for headerless files
        self._worker_id = worker_id
        # Hard upper bound on doc_ids: if exceeded, the plan underestimated the
        # row count and workers would write to overlapping chunk files. Catching
        # this at the first checkpoint (200K rows in) beats finding out 32 hours later.
        self._doc_id_end = doc_id_end

        # Build-time dedup: hash fingerprints of already-seen docs.
        # None = dedup disabled; [] = hash all non-meta fields; [col,...] = hash only those.
        self._dedup_fields = dedup_fields   # None means disabled
        self._seen_hashes: set = set()
        self._dedup_skipped = 0             # rows skipped due to dedup
        # Each worker writes its own shard file; query engine reads all *.bin files
        self._idx_filename = f"idx_w{worker_id}.bin" if worker_id is not None else "idx.bin"
        # Workers write per-worker stats to avoid concurrent overwrites of stats.json.
        # The auto-spawn orchestrator merges them after all workers finish.
        self._stats_path = (
            os.path.join(output_dir, f"stats_w{worker_id}.json")
            if worker_id is not None
            else os.path.join(output_dir, "stats.json")
        )
        # Disable pre-sampling estimate by default; enable with estimate=True in build()
        self._estimate_enabled = False

        os.makedirs(self.index_dir, exist_ok=True)
        os.makedirs(self.docs_dir, exist_ok=True)

        self.doc_counter = start_doc_id  # global monotonic doc id (continues across builds)
        self.total_docs = 0              # docs flushed in this session

        self._pending_docs = {}      # doc_id → row dict (unflushed)
        # array('I') stores doc_ids as raw C uint32 (4 bytes each) instead of
        # Python int objects (28 bytes each + GC overhead). For 20M doc_id entries
        # per 200K-row checkpoint this cuts memory from ~560 MB to ~80 MB and
        # eliminates millions of PyObject allocations — the main cause of GC-driven
        # slowdown over long builds.
        self._pending_terms = defaultdict(lambda: _array("I"))
        self._rows_since_checkpoint = 0   # rows since last terms+docs flush + checkpoint save
        self._rows_processed = 0          # cumulative rows processed in current file

        # Hot-path cache: col_key → (col_prefix, col_tg_prefix) bytes-stable strings.
        # Avoids re-concatenating "name:" / "name:~" on every cell of every row.
        # 1.4M re-concats per 100K rows × 14 cols on the blockchain dataset; this
        # collapses them to one concat per column for the lifetime of the build.
        self._col_prefix_cache: dict = {}
        # Per-build tokenize memoization for short repeating cell values
        # (status, country, level, type, lang ...).  Bounded by skipping values
        # longer than 64 chars at insertion time.
        self._tokenize_cache: dict = {}

        # Per-worker byte-level resume: expose the active _ByteRangeReader so the
        # checkpoint callback can record the exact file position after each chunk.
        # Updated by process_file(); None when not in byte-range mode.
        self._current_byte_reader = None   # active _ByteRangeReader (or None)
        self._last_safe_byte_pos  = None   # _f.tell() after last COMPLETE chunk
        self._rows_at_safe_pos    = 0      # _rows_processed at _last_safe_byte_pos

        self._buffers = {}           # prefix → bytearray (encoded entries)
        self._total_entries = 0

        self.columns_seen = {}       # canonical_key → semantic_type
        self._checkpoint_cb = checkpoint_cb

        # Background checkpoint flush: main thread swaps out data dicts and
        # continues indexing immediately while a daemon thread writes to disk.
        self._flush_thread = None    # currently running background flush thread
        self._flush_thread_exc = None  # exception captured from bg thread (if any)

        # ── Adaptive checkpoint tuning ────────────────────────────────────────
        # As the index grows, more prefix buckets become active, so each checkpoint
        # writes to more files (IOPS grows). The adaptive logic monitors the ratio of
        # bg-thread I/O time to main-thread processing time and raises thresholds
        # when the build becomes I/O-bound, keeping rows/s from degrading.
        #
        # _checkpoint_write_bytes: only buffers >= this are written to disk per
        #   checkpoint; smaller ones accumulate in RAM (fewer file opens).
        # _rows_per_checkpoint: swap pending_terms every N rows (longer window →
        #   less frequent checkpoints → less total I/O overhead).
        #
        # Both start at their constant defaults and can only grow (never shrink)
        # so that each adaptation reduces I/O pressure without reverting.
        self._checkpoint_write_bytes = CHECKPOINT_WRITE_BYTES
        self._rows_per_checkpoint    = CHECKPOINT_ROWS
        self._last_bg_duration       = 0.0   # written by bg thread, read by main
        self._last_checkpoint_t      = time.monotonic()  # start of last checkpoint window
        self._checkpoint_count       = 0     # how many checkpoints fired so far

        # ── Per-checkpoint debug stats (populated by bg thread, read by main) ──
        # Broken down into encode time, disk time, buffer counts/bytes so that
        # the checkpoint print shows exactly which variable is growing and why.
        self._last_bg_stats: dict = {
            "t_encode": 0.0,   # seconds spent encoding pending_terms → _buffers
            "t_disk":   0.0,   # seconds spent writing prefix files to disk
            "n_disk":   0,     # number of prefix buffers written to disk
            "n_mem":    0,     # number of prefix buffers kept in RAM after checkpoint
            "mem_bytes": 0,    # total bytes in _buffers after checkpoint
        }

        # ── Daemon (in-memory) mode ───────────────────────────────────────────
        # daemon=True: checkpoint bg thread only encodes — never writes to disk.
        # A separate lazy-writer thread drains large buffers to disk in the
        # background so RAM stays bounded without ever blocking the hot-loop.
        self._daemon            = daemon
        self._buffers_lock      = threading.Lock()   # guards _buffers in daemon mode
        self._bg_encoding_flag  = threading.Event()  # set while bg thread holds _buffers
        self._lazy_writer_stop  = threading.Event()  # set by finalize() to stop writer
        self._lazy_writer_thread = None
        if daemon:
            # In daemon mode: checkpoint never writes to disk → _checkpoint_write_bytes
            # set to a very large value so the extraction step in _bg_flush_io finds
            # nothing to write and returns immediately.
            self._checkpoint_write_bytes = 2 ** 31
            self._lazy_writer_thread = threading.Thread(
                target=self._lazy_writer_loop, daemon=True,
                name="flatseek-lazy-writer",
            )
            self._lazy_writer_thread.start()

        # Cache which index prefixes are zlib-compressed (written by 'flatseek compress').
        # On the first flush to a prefix we read 2 bytes; on subsequent flushes we
        # skip the read entirely — halving file-open count per bg flush.
        self._compressed_prefixes: set = set()
        # Cache prefixes known to be NOT compressed so we never read 2 magic bytes
        # again for a plain (non-compressed) prefix after the first write.
        # Without this, every append to an existing idx.bin opens the file twice:
        # once to read the magic, once to append — 130K extra opens per checkpoint.
        self._known_plain_prefixes: set = set()

        # Cache directories known to exist — skip os.makedirs syscall for known
        # prefixes. Once a prefix dir is created (first flush), it stays forever.
        # Without this, every _flush_buf_to_disk calls makedirs even for the
        # thousands of prefixes already written in previous checkpoints.
        self._known_dirs: set = set()

        # Pressure WAL: when total in-memory buffer bytes exceed MEM_FLUSH_BYTES,
        # we must drain memory but cannot afford to write 65K tiny files (each
        # file open costs ~1.5ms on HDD → 27K files = 40s).  Instead we write
        # one sequential WAL file per checkpoint that spills, which is a single
        # ~25MB sequential write (~0.25s on HDD).  finalize() reads them back and
        # merges into the regular prefix structure.
        self._pressure_wal_paths: list = []

        # Segment counter per prefix: how many segment files written for this prefix.
        # Used by segment-append to name new segment files (idx_seg{counter:04d}.bin).
        self._seg_counters: dict = {}

        # Incremental on-disk size trackers — incremented at every flush (O(1), no
        # os.walk needed).  Updated by both main thread and background flush thread;
        # GIL makes simple += on Python ints safe for display-only counters.
        self._idx_bytes_flushed: int = 0
        self._doc_bytes_flushed: int = 0

    # ─── Column info ──────────────────────────────────────────────────────────

    def _col_info(self, col_name, file_rel):
        """Return (semantic_type, canonical_key) for a column, using column_map if available."""
        if file_rel and file_rel in self.column_map:
            info = self.column_map[file_rel].get(col_name)
            if info and isinstance(info, dict):
                return info.get("semantic_type", "other"), info.get("canonical_key", _norm(col_name))
        for file_cols in self.column_map.values():
            if col_name in file_cols:
                info = file_cols[col_name]
                if isinstance(info, dict):   # skip _headerless / _columns / _delimiter
                    return info.get("semantic_type", "other"), info.get("canonical_key", _norm(col_name))
        return "other", _norm(col_name)

    # ─── Indexing helpers ─────────────────────────────────────────────────────

    def _index_value(self, doc_id, col_key, value, sem_type):
        """Add tokens (and trigrams if applicable) for a single value to pending_terms."""
        # Per-build memoization: short repeated values (status, country, level...)
        # tokenize identically every row. Caching the token list cuts ~3M tokenize
        # calls down to a few hundred on long builds. Keyed by raw value to keep
        # collisions zero (case-sensitive columns coexist).
        tcache = self._tokenize_cache
        cached = tcache.get(value)
        if cached is None:
            tokens = tokenize(value)
            # Cache only short-ish values — long unique strings (signatures, urls)
            # rarely repeat and would just bloat the cache.
            if len(value) <= 64:
                tcache[value] = tokens
        else:
            tokens = cached
        want_trigrams = sem_type in _TRIGRAM_TYPES
        pt = self._pending_terms

        # Always index the exact lowercased value scoped to this column, even when
        # tokenize() drops it (min-length filter ≥ 2 cuts single-char codes like
        # gender "L"→"l", "P"→"p"). This guarantees gender:L / gender:f etc. work.
        vl_exact = value.strip().lower()
        # Cached per-column prefix strings — built once, reused for every cell.
        cached = self._col_prefix_cache.get(col_key)
        if cached is None:
            col_prefix   = col_key + ":"
            col_tg_prefix = col_prefix + "~"
            self._col_prefix_cache[col_key] = (col_prefix, col_tg_prefix)
        else:
            col_prefix, col_tg_prefix = cached
        exact_col_term = col_prefix + vl_exact
        if vl_exact:
            pt[exact_col_term].append(doc_id)

        for tok in tokens:
            col_term = col_prefix + tok     # avoids f-string format overhead per token
            if col_term != exact_col_term:
                pt[col_term].append(doc_id)
            # Always index each token globally (not just for trigram-enabled types).
            # This ensures short tokens like "ml" (< 3 chars, no trigrams) are still
            # searchable as bare terms via global fallback in _resolve.
            if len(tok) >= 2:
                pt[tok].append(doc_id)
            if want_trigrams and len(tok) <= MAX_TRIGRAM_TOKEN_LEN:
                for tg in make_trigrams(tok):
                    pt[col_tg_prefix + tg].append(doc_id)

    # ─── Row accumulation ─────────────────────────────────────────────────────

    def _build_col_schema(self, headers, file_rel):
        """Pre-compute {col: (sem_type, col_key)} for all headers in a file.

        Called once per file before the row loop so _col_info is never invoked
        inside the hot path (which would otherwise cost 3 dict-lookups per cell).
        Also pre-populates columns_seen so that overhead is skipped in the loop.
        """
        schema = {}
        for col in headers:
            sem_type, col_key = self._col_info(col, file_rel)
            schema[col] = (sem_type, col_key)
            self.columns_seen[col_key] = sem_type
        return schema

    def add_row(self, row, headers, file_rel=None, col_schema=None):
        doc_id = self.doc_counter
        self.doc_counter += 1

        doc = {}

        # Expand nested arrays/objects before indexing
        # {"tags": ["api","cdn"], "address": {"city": "NYC"}}
        #   → {"tags[0]": "api", "tags[1]": "cdn", "address.city": "NYC", ...}
        expanded_row = _expand_record(dict(row))

        # Inject reserved metadata fields
        if self.dataset:
            doc["_dataset"] = self.dataset
            self._index_value(doc_id, "_dataset", self.dataset, "other")

        if file_rel:
            doc["_source"] = file_rel
            self._index_value(doc_id, "_source", file_rel, "other")

        _index = self._index_value
        for col in headers:
            val = row.get(col, "")
            if col_schema is not None:
                sem_type, col_key = col_schema[col]
            else:
                sem_type, col_key = self._col_info(col, file_rel)
                self.columns_seen[col_key] = sem_type

            norm_val = normalize_value(val, col_key)

            # Store first non-empty value for each canonical key.
            # Multiple CSV columns may share one canonical key (e.g. first_name,
            # last_name, full_name all → "name"). We index ALL of them so
            # search covers every column, but only store the first in the doc so
            # the displayed result shows the most complete value.
            if norm_val and norm_val.strip():
                if col_key not in doc:
                    doc[col_key] = norm_val
                elif not doc[col_key]:
                    doc[col_key] = norm_val   # fill empty slot if first was blank
                _index(doc_id, col_key, norm_val, sem_type)

        # Index expanded sub-fields (tags[0], address.city, etc.)
        for exp_key, exp_val in expanded_row.items():
            if exp_key in row:
                continue  # skip parent keys that are in the original row
            if exp_key not in self.columns_seen:
                # Infer type from expanded key name
                exp_sem_type, _ = self._col_info(exp_key, file_rel)
                self.columns_seen[exp_key] = exp_sem_type
            if exp_val and str(exp_val).strip():
                # Determine semantic type for the expanded field
                exp_sem_type = self.columns_seen.get(exp_key, "other")
                _index(doc_id, exp_key, str(exp_val), exp_sem_type)
                # Also store in doc dict so range queries can read the value
                if exp_key not in doc:
                    doc[exp_key] = str(exp_val)

        # Build-time dedup: fingerprint the canonical doc and skip if seen before.
        # Checked AFTER building `doc` so the hash is on normalised values, not raw row.
        # doc_counter is NOT incremented for skipped rows (keeps doc_ids dense).
        if self._dedup_fields is not None:
            fp = _doc_fingerprint(doc, self._dedup_fields if self._dedup_fields else None)
            if fp in self._seen_hashes:
                self._dedup_skipped += 1
                self.doc_counter -= 1   # undo the increment at the top of add_row
                return
            self._seen_hashes.add(fp)

        self._pending_docs[doc_id] = doc
        self._rows_processed += 1
        self._rows_since_checkpoint += 1

        # Checkpoint every CHECKPOINT_ROWS rows.
        # GIL-aware strategy: do all Python-heavy work (term encoding) in the MAIN
        # thread synchronously, then hand only the pre-encoded buffers + docs to a
        # background thread for disk I/O. Disk writes release the GIL, so the bg
        # thread barely competes with the next indexing window.
        #
        # Previous approach (encode in bg thread) caused main thread to run at ~50%
        # speed due to GIL contention with the bg thread's 20M-iteration encode loop.
        if self._rows_since_checkpoint >= self._rows_per_checkpoint:
            # ── Range guard (worker mode) ─────────────────────────────────────
            # If doc_counter has exceeded the plan-assigned doc_id_end, the plan
            # underestimated the row count. Workers with overlapping doc_id ranges
            # would write to the same doc chunk files → silent data corruption.
            # Stop immediately so the user sees a clear error within minutes,
            # not hours. Delete build_plan.json + stats_w*.json and rebuild.
            if self._doc_id_end is not None and self.doc_counter > self._doc_id_end + 1:
                raise RuntimeError(
                    f"\n\nWorker {self._worker_id}: doc_id counter ({self.doc_counter:,}) "
                    f"exceeded plan-assigned range end ({self._doc_id_end:,}).\n"
                    f"The build plan underestimated the row count — doc chunks would\n"
                    f"overlap with other workers, causing silent data corruption.\n\n"
                    f"Fix: delete build_plan.json and stats_w*.json, then rebuild.\n"
                    f"The new plan will use a larger doc_id range with 10% headroom."
                )

            _t0 = time.monotonic()

            # ── Measure main-thread window ────────────────────────────────────
            # How long did the main thread spend processing the current batch?
            # This is the budget the bg thread has to finish before the NEXT join.
            _main_duration = _t0 - self._last_checkpoint_t

            # Wait for previous disk-I/O thread (releases GIL during writes, fast).
            _t_join = 0.0
            if self._flush_thread is not None:
                self._flush_thread.join()
                _t_join = time.monotonic() - _t0
                # Propagate any exception the bg thread captured (e.g. disk full).
                if self._flush_thread_exc is not None:
                    exc = self._flush_thread_exc
                    self._flush_thread_exc = None
                    raise RuntimeError(
                        f"Background flush failed — stopping build to prevent data loss.\n"
                        f"Cause: {exc}\n"
                        f"Check disk space (df -h) and permissions on the output directory."
                    ) from exc
                # No gc.collect() here: gc.disable() is set for the whole build loop.
                # pending_terms / buffers are dicts with no cyclic refs — refcount
                # frees them when the bg thread drops its args after join(). A full
                # collect would walk the entire (large) accumulated heap every 50K rows,
                # easily 1-3 s per call. The single gc.collect() in finalize() cleans up
                # any true cycles after the hot loop is done.

            # ── Adaptive I/O tuning ───────────────────────────────────────────
            # If the bg thread ran longer than the main processing window, the main
            # thread stalled waiting (_t_join > 0).  Also adapt proactively if bg
            # already consumed > 70% of the main window — we're close to the edge.
            # Strategy: raise _checkpoint_write_bytes (fewer prefix files per flush)
            # to reduce disk time.  We intentionally do NOT grow _rows_per_checkpoint:
            # larger batches fill self._buffers dict beyond L3 cache (~50-100 MB),
            # causing Python dict cache-miss slowdown that makes encode time blow up
            # super-linearly (50K rows→15s, 112K rows→40s).  Keep the batch size fixed.
            _bg_dur = self._last_bg_duration
            _adapt_msg = ""
            if _t_join > 0.1 or (_main_duration > 0 and _bg_dur > _main_duration * 0.70):
                _old_wb = self._checkpoint_write_bytes
                # In daemon mode, disk writes are handled by the lazy writer —
                # the checkpoint write-bytes threshold is already at 2 GB so
                # there is nothing to adapt on that axis.
                if not self._daemon:
                    # Double write threshold (write fewer tiny buckets per checkpoint)
                    self._checkpoint_write_bytes = min(
                        self._checkpoint_write_bytes * 2,
                        2 * 1024 * 1024,  # cap at 2 MB — bounds in-memory buffer accumulation
                    )
                if self._checkpoint_write_bytes != _old_wb:
                    _wb_str = ("∞" if self._daemon
                               else f"{self._checkpoint_write_bytes // 1024}KB")
                    _adapt_msg = (
                        f"  \033[33m[adapt]\033[0m"
                        f" write_bytes={_wb_str}"
                        f"  ckpt_rows={self._rows_per_checkpoint:,}"
                    )

            # ── Swap: hand old pending_terms + docs to bg thread ─────────────────
            # flush_terms (encode) + disk I/O both run in the bg thread concurrently
            # with the next hot-loop — main thread never blocks on encoding anymore.
            _old_pending_terms = self._pending_terms
            _docs              = self._pending_docs

            self._pending_terms = defaultdict(lambda: _array("I"))
            self._pending_docs  = {}
            # self._buffers intentionally NOT cleared — bg thread extends it;
            # small buffers accumulate across checkpoints (fewer IOPS).
            self._rows_since_checkpoint = 0
            self._last_checkpoint_t = time.monotonic()

            self._checkpoint_count += 1
            _st = self._last_bg_stats
            try:
                import resource as _resource
                _rss_mb = _resource.getrusage(_resource.RUSAGE_SELF).ru_maxrss >> 20
            except Exception:
                _rss_mb = 0

            _ckpt_line = (
                f"\r  \033[36m[ckpt #{self._checkpoint_count}]\033[0m"
                f"  main={_main_duration:.1f}s"
                f"  encode={_st['t_encode']:.1f}s"
                f"  disk={_st['t_disk']:.1f}s"
                f"  wait={_t_join:.2f}s"
                f"  |  terms={len(_old_pending_terms):,}"
                f"  disk_bufs={_st['n_disk']:,}"
                f"  mem_bufs={_st['n_mem']:,}"
                f"  mem_buf={_st['mem_bytes'] >> 20}MB"
                f"  |  rss={_rss_mb}MB"
                + (f"  wb=∞" if self._daemon else f"  wb={self._checkpoint_write_bytes >> 10}KB")
                + f"  ckpt={self._rows_per_checkpoint:,}"
            )
            sys.stderr.write(_ckpt_line + (_adapt_msg or "") + "\n")
            sys.stderr.flush()

            self._write_partial_stats()

            if self._checkpoint_cb:
                self._checkpoint_cb(self._rows_processed, self.doc_counter)

            # ── Bg thread: encode + extract large bufs + disk I/O ───────────────
            self._flush_thread = threading.Thread(
                target=self._bg_flush_io, args=(_docs, _old_pending_terms), daemon=True
            )
            self._flush_thread.start()

    def _write_partial_stats(self):
        """Write mid-build stats so QueryEngine can initialise even if interrupted.

        Workers write to their own stats_w{N}.json shard (no concurrent write race).
        The orchestrator merges available shard files into a partial stats.json every
        ~10 s during the polling loop.  finalize() writes the authoritative final stats.
        """

        stats_path = self._stats_path
        prev_cols = {}
        # prev_entries = baseline from PREVIOUS completed sessions only
        prev_entries = 0
        if os.path.exists(stats_path):
            try:
                with open(stats_path) as f:
                    prev = json.load(f)
                prev_cols = prev.get("columns", {})
                if prev.get("_partial"):
                    # We wrote this earlier in the same session — read the baseline
                    prev_entries = prev.get("_prev_entries", 0)
                else:
                    # Written by a previous finalize() — authoritative
                    prev_entries = prev.get("total_entries", 0)
            except Exception:
                pass

        idx_mb  = round(self._idx_bytes_flushed / 1024**2, 1)
        doc_mb  = round(self._doc_bytes_flushed / 1024**2, 1)
        with open(stats_path, "w") as f:
            json.dump({
                "total_docs":    self.doc_counter,
                "rows_indexed":  self.total_docs,
                "total_entries": prev_entries + self._total_entries,
                "_prev_entries": prev_entries,  # baseline so finalize() doesn't double-count
                "doc_chunk_size": DOC_CHUNK_SIZE,
                "index_files":   0,   # accurate after finalize()
                "index_size_mb": idx_mb,
                "docs_size_mb":  doc_mb,
                "total_size_mb": round(idx_mb + doc_mb, 1),
                "columns": {**prev_cols, **self.columns_seen},
                "_partial": True,     # marker; removed by finalize()
            }, f, indent=2)

    def _chunk_start(self, doc_id):
        return (doc_id // DOC_CHUNK_SIZE) * DOC_CHUNK_SIZE

    def _doc_path(self, chunk_start):
        """2-level hex path like the index: docs/{aa}/{bb}/docs_{N:010d}.zlib"""
        n = chunk_start // DOC_CHUNK_SIZE
        aa = f"{(n >> 8) & 0xFF:02x}"
        bb = f"{n & 0xFF:02x}"
        return os.path.join(self.docs_dir, aa, bb, f"docs_{chunk_start:010d}.zlib")

    def _flush_docs(self):
        if not self._pending_docs:
            return
        ids = sorted(self._pending_docs)

        # Group by chunk boundary (pending docs may span multiple chunks)
        by_chunk = defaultdict(dict)
        for did in ids:
            by_chunk[self._chunk_start(did)][str(did)] = self._pending_docs[did]

        for chunk_start, new_data in by_chunk.items():
            path = self._doc_path(chunk_start)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            # Merge with existing chunk — needed when incremental builds share a chunk
            if os.path.isfile(path):
                try:
                    with open(path, "rb") as f:
                        existing = _doc_loads(_decompress_doc(f.read()))
                    existing.update(new_data)
                    merged = existing
                except (zlib.error, Exception) as _e:
                    print(f"  Warning: corrupted doc chunk {os.path.basename(path)}, rebuilding ({_e})")
                    merged = new_data
            else:
                merged = new_data
            # Collapse expanded fields (arrays, nested) before writing to doc store
            merged = {doc_id: _collapse_record(rec) for doc_id, rec in merged.items()}
            raw = _doc_dumps(merged)
            compressed = _compress_doc(raw)
            tmp = path + f".{os.getpid()}.tmp"
            with open(tmp, "wb") as f:
                f.write(compressed)
            os.replace(tmp, path)
            self._doc_bytes_flushed += len(compressed)

        self.total_docs += len(ids)
        self._pending_docs.clear()

    def _bg_flush_io(self, docs, pending_terms):
        """Background checkpoint flush — encodes terms and writes to disk.

        Runs in a daemon thread while the main thread immediately starts the
        next hot-loop.  Does THREE things in sequence:

          1. _flush_terms(pending_terms) — encode str→doclist entries into
             self._buffers.  Runs concurrently with the next hot-loop (main
             thread only writes to self._pending_terms during this time, which
             is completely disjoint from self._buffers).  Previously this ran
             in the main thread (blocking); moving it here hides 2-3 s of
             Python work behind the hot-loop, doubling effective throughput.

          2. Extract prefix buffers that have grown past CHECKPOINT_WRITE_BYTES
             — these are large enough to justify the file-open cost.  Tiny
             buffers stay in self._buffers and accumulate across checkpoints,
             reducing IOPS from 65536×n_workers per cycle to a small fraction.

          3. _bg_flush_io_inner(docs, large_bufs) — atomic disk writes (GIL
             released for every write, so main thread runs at full speed).

        Thread-safety: self._buffers is only accessed by the bg thread between
        checkpoints (main thread never touches it in the hot-loop).  The join()
        at the next checkpoint provides the happens-before guarantee, so no
        locks are needed.

        Any exception is stored in self._flush_thread_exc and re-raised in the
        main thread on the next join, ensuring disk errors stop the build fast.
        """
        _t_bg_start = time.monotonic()
        _t_encode = _t_disk = 0.0
        _n_disk = _n_mem = _mem_bytes = 0
        try:
            # ── Step 1: encode pending_terms → self._buffers ────────────────────
            # In daemon mode, set the encoding flag so the lazy-writer thread
            # knows not to touch _buffers while we're extending them.
            if self._daemon:
                self._bg_encoding_flag.set()
            _t1 = time.monotonic()
            self._flush_terms(pending_terms)
            _t_encode = time.monotonic() - _t1
            if self._daemon:
                self._bg_encoding_flag.clear()

            # ── Step 2 + 3: drain buffers, then flush to disk ──────────────────
            write_threshold = self._checkpoint_write_bytes
            _t2 = time.monotonic()

            if not self._daemon and self._buffers and \
                    sum(len(b) for b in self._buffers.values()) > MEM_FLUSH_BYTES:
                # Memory pressure: write ALL buffers as ONE sequential WAL file
                # instead of N tiny random files.  Writing 27K files to HDD costs
                # ~40s (27K × 1.5ms/file); one 25MB sequential write costs ~0.25s.
                # finalize() reads WAL files back and merges into prefix structure.
                self._write_pressure_wal()
                large_bufs: dict = {}
                _n_disk    = -(len(self._pressure_wal_paths))  # negative = WAL hint
                _n_mem     = 0
                _mem_bytes = 0
            else:
                # Normal path: write only buffers that exceeded the adaptive threshold.
                large_bufs = {p: buf for p, buf in self._buffers.items()
                              if len(buf) >= write_threshold}
                for p in large_bufs:
                    del self._buffers[p]  # small ones accumulate across checkpoints
                _n_disk    = len(large_bufs)
                _n_mem     = len(self._buffers)
                _mem_bytes = sum(len(b) for b in self._buffers.values())

            # ── Step 3: write docs + any large index buffers to disk ────────────
            # In daemon/WAL mode large_bufs is empty so only docs are written.
            self._bg_flush_io_inner(docs, large_bufs)
            _t_disk = time.monotonic() - _t2

        except Exception as exc:
            self._flush_thread_exc = exc
        finally:
            if self._daemon:
                self._bg_encoding_flag.clear()  # ensure cleared even on exception
            # Record detailed breakdown for main-thread debug display.
            self._last_bg_stats = {
                "t_encode": _t_encode,
                "t_disk":   _t_disk,
                "n_disk":   _n_disk,
                "n_mem":    _n_mem,
                "mem_bytes": _mem_bytes,
            }
            self._last_bg_duration = time.monotonic() - _t_bg_start

    # ─── Pressure WAL (Write-Ahead Log) ──────────────────────────────────────

    def _write_pressure_wal(self):
        """Spill ALL in-memory prefix buffers to one sequential WAL file.

        Called from the bg checkpoint thread when total buffer bytes exceed
        MEM_FLUSH_BYTES.  Writing 27K small files to HDD costs ~40s; writing
        one ~25MB sequential file costs ~0.25s.  finalize() calls
        _merge_pressure_wals() to read these files and scatter back to the
        regular per-prefix idx files.

        WAL entry format (little-endian):
          [5 bytes  : prefix string "xx/yy"]
          [4 bytes  : uint32 data length   ]
          [N bytes  : raw buffer data      ]
        """
        if not self._buffers:
            return
        wal_dir = os.path.join(self.output_dir, "_wal")
        os.makedirs(wal_dir, exist_ok=True)
        # Unique name: worker id + checkpoint counter
        base = os.path.splitext(self._idx_filename)[0]  # e.g. "idx_w3"
        wal_path = os.path.join(
            wal_dir, f"{base}_c{self._checkpoint_count:06d}.wal"
        )
        # Write sequentially: one file, all prefixes concatenated
        _pack_header = struct.Struct("<5sI").pack
        with open(wal_path, "wb") as f:
            for prefix, buf in self._buffers.items():
                if not buf:
                    continue
                f.write(_pack_header(prefix.encode(), len(buf)))
                f.write(buf)
        self._pressure_wal_paths.append(wal_path)
        # Keep dict structure (all 65K keys) but reset contents.
        # Avoids freeing + re-allocating 65K bytearray objects on next checkpoint,
        # which causes GC pressure and slower encode on the first post-WAL pass.
        for prefix in self._buffers:
            self._buffers[prefix] = bytearray()

    def _merge_pressure_wals(self):
        """Read WAL files written by _write_pressure_wal and append to prefix files.

        Called from finalize() after all encoding is done.  Reads each WAL,
        groups entries by prefix, then writes to the regular prefix idx files
        using the same _flush_buf_to_disk path (handles compressed files, etc.).
        WAL files are deleted after successful merge.
        """
        if not self._pressure_wal_paths:
            return
        sys.stderr.write(
            f"  Merging {len(self._pressure_wal_paths)} pressure WAL file(s)...\n"
        )
        _struct_hdr = struct.Struct("<5sI")
        hdr_size = _struct_hdr.size  # 9 bytes
        merged: dict = {}  # prefix → bytearray
        for wal_path in self._pressure_wal_paths:
            with open(wal_path, "rb") as f:
                while True:
                    hdr = f.read(hdr_size)
                    if not hdr:
                        break
                    if len(hdr) < hdr_size:
                        break  # truncated
                    prefix_b, dlen = _struct_hdr.unpack(hdr)
                    prefix = prefix_b.decode()
                    data = f.read(dlen)
                    if prefix not in merged:
                        merged[prefix] = bytearray()
                    merged[prefix] += data
        # Scatter merged data to the regular prefix idx files
        n = len(merged)
        done = 0
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=FLUSH_THREADS) as pool:
            futs = {pool.submit(self._flush_buf_to_disk, p, merged): p
                    for p in merged}
            for fut in as_completed(futs):
                fut.result()
                done += 1
                if done % 2000 == 0:
                    sys.stderr.write(f"\r  WAL merge: {done:,}/{n:,} prefixes")
                    sys.stderr.flush()
        # Remove WAL files now that data is safely in prefix files
        for wal_path in self._pressure_wal_paths:
            try:
                os.unlink(wal_path)
            except OSError:
                pass
        self._pressure_wal_paths.clear()
        sys.stderr.write("\r" + " " * 50 + "\r")

    # ─── Daemon lazy writer ────────────────────────────────────────────────────

    def _lazy_writer_loop(self):
        """Daemon-mode background thread: drains large in-memory buffers to disk.

        Runs every LAZY_WRITE_INTERVAL seconds.  Skips a cycle if the checkpoint
        bg thread is currently encoding (_bg_encoding_flag is set) to avoid
        concurrent modification of _buffers without a heavy lock.

        Threshold starts at LAZY_WRITE_BYTES (512 KB) but drops proportionally
        when total buffer memory exceeds LAZY_MAX_MEM_BYTES — writes more
        aggressively to keep RAM bounded.
        """
        while not self._lazy_writer_stop.wait(timeout=LAZY_WRITE_INTERVAL):
            self._lazy_writer_cycle()

    def _lazy_writer_cycle(self, force_all=False):
        """One drain cycle: extract large buffers, write to disk outside the lock."""
        # Skip if the checkpoint bg thread is encoding into _buffers right now.
        # The encoding flag is held for the duration of _flush_terms (~2-5 s),
        # which is much shorter than LAZY_WRITE_INTERVAL (10 s), so we'll catch
        # the next window quickly.
        if self._bg_encoding_flag.is_set():
            return

        # Compute effective write threshold.  If total buffer memory is near the
        # safety cap, lower the threshold so we flush more buffers this cycle.
        total_bytes = sum(len(b) for b in self._buffers.values())
        if force_all:
            threshold = 0
        elif total_bytes > LAZY_MAX_MEM_BYTES:
            # Aggressive: flush everything above 1/4 of the per-bucket average
            avg = total_bytes // max(len(self._buffers), 1)
            threshold = max(avg // 4, 1024)
        else:
            threshold = LAZY_WRITE_BYTES

        # Extract qualifying buffers under the encoding flag guard.
        # We re-check the flag here: if the encoding thread started between our
        # is_set() check above and now, skip the cycle (safe, next cycle catches it).
        if self._bg_encoding_flag.is_set():
            return

        to_write = {p: bytearray(buf) for p, buf in self._buffers.items()
                    if len(buf) >= threshold}
        for p in to_write:
            del self._buffers[p]

        if not to_write:
            return

        # Write outside any lock — disk I/O is independent per prefix.
        # Pass each buffer in its own single-entry dict so _flush_buf_to_disk
        # can .clear() it without touching self._buffers.
        written_b = 0
        for prefix, buf in to_write.items():
            n = len(buf)
            self._flush_buf_to_disk(prefix, to_write)
            written_b += n

        if written_b:
            tag = "\033[35m[lazy]\033[0m"
            sys.stderr.write(
                f"  {tag} wrote {len(to_write):,} bufs"
                f" ({written_b >> 20}MB → disk)"
                f"  mem_bufs={len(self._buffers):,}"
                f"  mem_buf={(sum(len(b) for b in self._buffers.values())) >> 20}MB\n"
            )
            sys.stderr.flush()

    def _bg_flush_io_inner(self, docs, buffers):
        # ── 1. Flush doc chunks FIRST (fast, makes data searchable immediately) ──
        if docs:
            ids = sorted(docs)
            by_chunk = defaultdict(dict)
            for did in ids:
                by_chunk[self._chunk_start(did)][str(did)] = docs[did]
            for chunk_start, new_data in by_chunk.items():
                path = self._doc_path(chunk_start)
                doc_dir = os.path.dirname(path)
                if doc_dir not in self._known_dirs:
                    os.makedirs(doc_dir, exist_ok=True)
                    self._known_dirs.add(doc_dir)
                if os.path.isfile(path):
                    try:
                        with open(path, "rb") as f:
                            existing = _doc_loads(_decompress_doc(f.read()))
                        existing.update(new_data)
                        merged = existing
                    except (zlib.error, Exception) as _e:
                        print(f"  Warning: corrupted doc chunk {os.path.basename(path)}, rebuilding ({_e})")
                        merged = new_data
                else:
                    merged = new_data
                raw = _doc_dumps(merged)
                compressed = _compress_doc(raw)
                tmp = path + f".{os.getpid()}.tmp"
                with open(tmp, "wb") as f:
                    f.write(compressed)
                os.replace(tmp, path)
                self._doc_bytes_flushed += len(compressed)
            self.total_docs += len(ids)

        # ── 2. Flush pre-encoded prefix buffers to disk ───────────────────────
        # Use a thread pool so multiple prefix files are written concurrently.
        # Each write is independent (different file), so no locking needed.
        # Capped at FLUSH_THREADS to avoid overwhelming the OS file cache.
        prefixes = list(buffers)
        if len(prefixes) <= 4:
            # Not worth the thread-pool overhead for a handful of prefixes.
            for prefix in prefixes:
                self._flush_buf_to_disk(prefix, buffers)
        else:
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=FLUSH_THREADS) as ex:
                list(ex.map(lambda p: self._flush_buf_to_disk(p, buffers), prefixes))
        # NOTE: gc.collect() is NOT called here. It was moved to the main thread
        # (see checkpoint logic in add_row). Calling gc.collect() in a background
        # thread acquires the GIL for the entire collection duration, pausing the
        # main indexing thread — which defeats the purpose of background I/O.

    def _flush_buf_to_disk(self, prefix, buffers):
        """Write a prefix buffer (from an explicit dict) to its idx file.

        Segment-append strategy (S3-friendly):
          - For compressed prefixes: write a NEW immutable segment file every
            checkpoint instead of read+decompress+recompress (eliminates O(N²)).
          - For plain prefixes: append to existing file (no recompression cost).
          - At finalize(): all segments are merged into one compressed idx.bin.

        Magic-check result is cached in self._compressed_prefixes so subsequent
        flushes to the same prefix skip the extra open()+read(2) entirely.
        """
        buf = buffers.get(prefix)
        if not buf:
            return
        dir_path = os.path.join(self.index_dir, prefix)
        path = os.path.join(dir_path, self._idx_filename)

        n_raw = len(buf)   # capture before clear(); used to update the running counter

        if prefix in self._compressed_prefixes:
            # Compressed prefix: write new immutable segment file (no recompression).
            # Each checkpoint creates idx_seg{counter:04d}.bin — final merge happens
            # at the end in _finalize_merge_segments().  This eliminates the O(N²)
            # read+decompress+recompress cycle that dominates finalize time on S3.
            if dir_path not in self._known_dirs:
                os.makedirs(dir_path, exist_ok=True)
                self._known_dirs.add(dir_path)
            seg_num = self._seg_counters.get(prefix, 0) + 1
            self._seg_counters[prefix] = seg_num
            seg_path = os.path.join(dir_path, f"idx_seg{seg_num:04d}.bin")
            with open(seg_path, "wb") as f:
                f.write(buf)
            buf.clear()
            self._idx_bytes_flushed += n_raw
            return

        if prefix in self._known_plain_prefixes:
            # Known non-compressed — dir exists, skip makedirs + magic check.
            # Only switch to segment files if existing file is already large.
            existing_size = 0
            try:
                existing_size = os.path.getsize(path)
            except OSError:
                pass
            if existing_size >= SEGMENT_SWITCH_SIZE:
                # Switch to segment files — avoids ever-growing append that would
                # eventually require recompression at finalize.
                if dir_path not in self._known_dirs:
                    os.makedirs(dir_path, exist_ok=True)
                    self._known_dirs.add(dir_path)
                seg_num = self._seg_counters.get(prefix, 0) + 1
                self._seg_counters[prefix] = seg_num
                seg_path = os.path.join(dir_path, f"idx_seg{seg_num:04d}.bin")
                with open(seg_path, "wb") as f:
                    f.write(buf)
                buf.clear()
                self._idx_bytes_flushed += n_raw
                return
            with open(path, "ab") as f:
                f.write(buf)
            buf.clear()
            self._idx_bytes_flushed += n_raw
            return

        # First time seeing this prefix — create dir if needed, check compression.
        if dir_path not in self._known_dirs:
            os.makedirs(dir_path, exist_ok=True)
            self._known_dirs.add(dir_path)

        if os.path.isfile(path):
            # File exists but compression status unknown yet — check magic once,
            # then cache so we never open this file twice again for this prefix.
            with open(path, "rb") as f:
                magic = f.read(2)
            if len(magic) >= 2 and magic[0] == 0x78 and magic[1] in (0x01, 0x5e, 0x9c, 0xda):
                self._compressed_prefixes.add(prefix)
                # Compressed prefix, first time: write as first segment
                seg_num = self._seg_counters.get(prefix, 0) + 1
                self._seg_counters[prefix] = seg_num
                seg_path = os.path.join(dir_path, f"idx_seg{seg_num:04d}.bin")
                with open(seg_path, "wb") as f:
                    f.write(buf)
                buf.clear()
                self._idx_bytes_flushed += n_raw
                return
            self._known_plain_prefixes.add(prefix)

        with open(path, "ab") as f:
            f.write(buf)
        buf.clear()
        self._idx_bytes_flushed += n_raw

    def _flush_terms(self, pending_terms=None):
        """Encode pending_terms into self._buffers.

        pending_terms: the dict to encode.  When called from the bg thread,
        this is the old pending_terms snapshot handed off at checkpoint.
        When called from finalize() (main thread), pass None to use
        self._pending_terms.
        """
        pt = pending_terms if pending_terms is not None else self._pending_terms
        if not pt:
            return
        # Debug: check first term's doc_ids type
        first_term = next(iter(pt.keys()), None)
        if first_term is not None:
            first_doc_ids = pt[first_term]
            if first_doc_ids and len(first_doc_ids) > 0:
                sample_did = first_doc_ids[0]
                if not isinstance(sample_did, int):
                    sys.stderr.write(f"\n  [BUG] doc_id is {type(sample_did).__name__}: {sample_did!r}\n")
                    sys.stderr.flush()
        # Hoist frequently-accessed names into locals — avoids repeated global /
        # attribute lookups inside a loop that may iterate millions of times.
        buffers       = self._buffers
        pack_h        = _PACK_H
        pack_i        = _PACK_I
        enc_doclist   = encode_doclist
        th            = term_hash_b          # takes bytes — avoids double encode
        flush_buf     = self._flush_prefix_buffer
        threshold     = BUFFER_FLUSH_BYTES
        total_entries = self._total_entries

        for term, doc_ids in pt.items():
            term_b  = term.encode()          # utf-8 is default, no kwarg needed
            doclist = enc_doclist(doc_ids)
            prefix  = th(term_b)             # pass bytes directly

            buf = buffers.get(prefix)
            if buf is None:
                buffers[prefix] = buf = bytearray()

            # Extend directly — avoids building an intermediate `entry` bytes
            # object (4 allocations + 3 concatenations per term → 1 extend per field).
            buf += pack_h(len(term_b))
            buf += term_b
            buf += pack_i(len(doclist))
            buf += doclist
            total_entries += 1

            if len(buf) >= threshold:
                flush_buf(prefix)

        self._total_entries = total_entries
        pt.clear()

    def _flush_prefix_buffer(self, prefix):
        buf = self._buffers.get(prefix)
        if not buf:
            return
        dir_path = os.path.join(self.index_dir, prefix)
        path = os.path.join(dir_path, self._idx_filename)

        n_raw = len(buf)   # capture before clear()

        try:
            if prefix in self._compressed_prefixes:
                # Compressed prefix: write new immutable segment (no recompression).
                if dir_path not in self._known_dirs:
                    os.makedirs(dir_path, exist_ok=True)
                    self._known_dirs.add(dir_path)
                seg_num = self._seg_counters.get(prefix, 0) + 1
                self._seg_counters[prefix] = seg_num
                seg_path = os.path.join(dir_path, f"idx_seg{seg_num:04d}.bin")
                with open(seg_path, "wb") as f:
                    f.write(buf)
                buf.clear()
                self._idx_bytes_flushed += n_raw
                return

            if prefix in self._known_plain_prefixes:
                # Dir and file exist — skip makedirs and magic check entirely.
                # Switch to segment files if existing file is already large.
                existing_size = 0
                try:
                    existing_size = os.path.getsize(path)
                except OSError:
                    pass
                if existing_size >= SEGMENT_SWITCH_SIZE:
                    if dir_path not in self._known_dirs:
                        os.makedirs(dir_path, exist_ok=True)
                        self._known_dirs.add(dir_path)
                    seg_num = self._seg_counters.get(prefix, 0) + 1
                    self._seg_counters[prefix] = seg_num
                    seg_path = os.path.join(dir_path, f"idx_seg{seg_num:04d}.bin")
                    with open(seg_path, "wb") as f:
                        f.write(buf)
                    buf.clear()
                    self._idx_bytes_flushed += n_raw
                    return
                with open(path, "ab") as f:
                    f.write(buf)
                buf.clear()
                self._idx_bytes_flushed += n_raw
                return

            if dir_path not in self._known_dirs:
                os.makedirs(dir_path, exist_ok=True)
                self._known_dirs.add(dir_path)

            if os.path.isfile(path):
                # First write to this prefix during this build session — detect compression.
                with open(path, "rb") as f:
                    magic = f.read(2)
                if len(magic) >= 2 and magic[0] == 0x78 and magic[1] in (0x01, 0x5e, 0x9c, 0xda):
                    self._compressed_prefixes.add(prefix)
                    # Compressed prefix, first time: write as first segment
                    seg_num = self._seg_counters.get(prefix, 0) + 1
                    self._seg_counters[prefix] = seg_num
                    seg_path = os.path.join(dir_path, f"idx_seg{seg_num:04d}.bin")
                    with open(seg_path, "wb") as f:
                        f.write(buf)
                    buf.clear()
                    self._idx_bytes_flushed += n_raw
                    return
                self._known_plain_prefixes.add(prefix)

            with open(path, "ab") as f:
                f.write(buf)
            buf.clear()
            self._idx_bytes_flushed += n_raw
        except Exception as e:
            # Log error but don't re-raise — one failed prefix shouldn't stop
            # the rest of the flush or prevent stats from being written.
            sys.stderr.write(f"\n  Error flushing prefix '{prefix}': {e}\n")
            sys.stderr.flush()
            # Fall through: do NOT clear buf so it can be retried on next finalize
            self._buffers[prefix] = buf

    def _finalize_merge_segments(self):
        """Merge all segment files (idx_seg*.bin) into one compressed idx.bin.

        This is called exactly once at the end of finalize(), after all checkpoint
        buffers have been flushed.  Each prefix that had segment files gets all its
        segments concatenated and recompressed into a single idx.bin file, then the
        segment files are deleted.

        Returns the total number of segment files merged.
        """
        import glob as _glob

        total_segs = 0
        for prefix, seg_count in list(self._seg_counters.items()):
            if seg_count == 0:
                continue
            dir_path = os.path.join(self.index_dir, prefix)
            idx_path = os.path.join(dir_path, self._idx_filename)

            # Collect all segment files for this prefix
            seg_files = sorted(
                f for f in os.listdir(dir_path)
                if f.startswith("idx_seg") and f.endswith(".bin")
            )
            if not seg_files:
                continue

            # Concatenate all segment contents
            merged_data = b""
            for seg_file in seg_files:
                seg_path = os.path.join(dir_path, seg_file)
                with open(seg_path, "rb") as f:
                    merged_data += f.read()
                total_segs += 1

            # Compress the merged data into final idx.bin
            compressed = zlib.compress(merged_data, ZLIB_LEVEL_IDX)
            with open(idx_path, "wb") as f:
                f.write(compressed)

            # Delete segment files
            for seg_file in seg_files:
                seg_path = os.path.join(dir_path, seg_file)
                try:
                    os.remove(seg_path)
                except OSError:
                    pass

            # Update compressed_prefixes so future builds of the same prefix know
            # it's compressed (idx.bin is now compressed instead of segment files)
            self._compressed_prefixes.add(prefix)
            # Also record that it has plain (non-compressed) content as the base file
            self._known_plain_prefixes.discard(prefix)

            # Clear seg counter
            self._seg_counters[prefix] = 0

        return total_segs

    # ─── File processing ──────────────────────────────────────────────────────

    def process_file(self, path, base_dir=None, skip_rows=0, byte_range=None,
                     estimated_total=None):
        """Index a file, optionally skipping the first skip_rows rows (for resume).

        Returns the number of rows newly indexed (excluding skipped rows).
        """
        file_rel = os.path.relpath(path, base_dir) if base_dir else os.path.basename(path)
        ext      = _inner_ext(path)          # real data extension, stripped of .gz/.bz2
        is_compressed = bool(_compress_ext(path))

        # Byte-range parallel split is impossible on compressed files (not seekable).
        # If a caller somehow passes byte_range for a compressed file, ignore it.
        if is_compressed and byte_range is not None:
            byte_range = None

        # Resolve effective columns: explicit arg > column_map metadata > None (use file header)
        effective_columns = self.columns
        effective_delimiter = self.delimiter
        if effective_columns is None and file_rel in self.column_map:
            file_meta = self.column_map[file_rel]
            if file_meta.get("_headerless"):
                effective_columns  = file_meta.get("_columns")
                effective_delimiter = file_meta.get("_delimiter", self.delimiter)

        size_mb   = os.path.getsize(path) / 1024 ** 2
        ext_lower = ext   # kept for compat with code below that uses ext_lower

        # ── Peek headers once so col_schema is available before the main loop ──
        # This also lets us run _sample_estimate before indexing starts.
        peek_headers = []
        if ext == ".csv":
            try:
                with _open_source(path, newline="") as _pf:
                    _pr = csv.DictReader(_pf, fieldnames=effective_columns or None,
                                         delimiter=effective_delimiter)
                    peek_headers = list(_pr.fieldnames or [])
            except Exception:
                pass
        _peek_col_schema = self._build_col_schema(peek_headers, file_rel) if peek_headers else {}

        # ── Row-counting (background for large files) ──────────────────────────
        # Large files start counting in a background thread so indexing begins
        # immediately. Progress bar starts in spinner mode and switches to
        # progress + ETA as soon as the count is ready.
        # Workers operating on a byte slice skip full-file counting; they use the
        # plan-provided estimated_total instead.
        _total_box = [None]   # mutable box; _tick closure reads it
        _count_thread = None
        if estimated_total is not None:
            _total_box[0] = estimated_total   # from plan (worker byte-range mode)
        elif byte_range is not None:
            pass   # slice of unknown length — total stays None, bar shows spinner
        elif size_mb <= 50 or ext_lower not in (".csv", ".jsonl", ".ndjson"):
            _total_box[0] = _count_rows(path)
        else:
            def _bg_count():
                _total_box[0] = _count_rows(path)
            _count_thread = threading.Thread(target=_bg_count, daemon=True)
            _count_thread.start()

        # ── Pre-sampling estimate ──────────────────────────────────────────────
        # Opt-in only (build --estimate). Skipped for workers (byte-range slices
        # are not representative) and on resume (speed already measured).
        _est = None
        if self._estimate_enabled and ext == ".csv" and peek_headers \
                and not skip_rows and size_mb > 1 and byte_range is None:
            sys.stderr.write("  Sampling for estimates...\r")
            sys.stderr.flush()
            _est = _sample_estimate(path, _peek_col_schema, peek_headers,
                                    effective_delimiter, effective_columns)
            sys.stderr.write(" " * 40 + "\r")
            sys.stderr.flush()

        total_str = f"~{_total_box[0]:,}" if (_total_box[0] and ext_lower == ".csv") \
                    else (f"{_total_box[0]:,}" if _total_box[0] else "counting...")

        if is_compressed:
            cname    = _compress_ext(path).lstrip(".")   # "gz" or "bz2"
            size_str = f"{cname}, {size_mb:.1f} MB compressed, rows unknown (streaming)"
        elif skip_rows:
            size_str = f"{size_mb:.1f} MB, {total_str} rows, resuming from row {skip_rows:,}"
        else:
            size_str = f"{size_mb:.1f} MB, {total_str} rows"

        print(f"\n  {file_rel}  ({size_str})")

        if _est:
            # If row count isn't ready yet, wait briefly (the sample took ~1s, count
            # has been running in parallel the whole time — usually already done).
            if _count_thread and _count_thread.is_alive():
                _count_thread.join(timeout=3)
            _print_estimate(_est, _total_box[0], skip_rows, self.output_dir)

        # Reset per-file row counter (starts at skip_rows so checkpoint offsets are correct)
        self._rows_processed = skip_rows

        count = 0   # rows indexed in this call (not counting skipped)
        t_start = time.time()
        REPORT_EVERY = 10_000  # update bar every N rows
        _rate_window: list = []  # (timestamp, count) pairs for 30-s sliding-window rate

        def _tick():
            if count % REPORT_EVERY == 0:
                total = _total_box[0]
                effective_total = total - skip_rows if total else None

                # Sliding-window rate: keep the last 30 s of (t, count) samples.
                now_t = time.time()
                _rate_window.append((now_t, count))
                while len(_rate_window) > 1 and _rate_window[0][0] < now_t - 30.0:
                    _rate_window.pop(0)
                instant = None
                if len(_rate_window) >= 2:
                    dt = _rate_window[-1][0] - _rate_window[0][0]
                    dr = _rate_window[-1][1] - _rate_window[0][1]
                    if dt > 0:
                        instant = dr / dt

                if self._worker_id is not None:
                    # Worker mode: write progress file for parent aggregation.
                    # Do NOT print to stderr — multiple workers writing \r bars garbles output.
                    try:
                        ppath = os.path.join(self.output_dir,
                                             f"progress_w{self._worker_id}.json")
                        _st = self._last_bg_stats
                        _prog = {
                            "rows":         count,
                            "total":        effective_total or 0,
                            "instant_rate": instant or 0,
                            # ── ckpt debug fields (read by parent poll loop) ──
                            "ckpt":         self._checkpoint_count,
                            "encode":       round(_st["t_encode"], 2),
                            "disk":         round(_st["t_disk"], 2),
                            "wait":         round(self._last_bg_duration - _st["t_encode"] - _st["t_disk"], 2),
                            "disk_bufs":    _st["n_disk"],
                            "mem_bufs":     _st["n_mem"],
                            "mem_buf_mb":   _st["mem_bytes"] >> 20,
                            "wb_kb":        (0 if self._daemon
                                             else self._checkpoint_write_bytes >> 10),
                            "ckpt_rows":    self._rows_per_checkpoint,
                            "daemon":       self._daemon,
                        }
                        with open(ppath, "w") as _pf:
                            _pf.write(json.dumps(_prog))
                    except Exception:
                        pass
                else:
                    _progress(count, effective_total, t_start, instant_rate=instant)

        if ext == ".csv":
            # Prefer pandas for CSV parsing — it reads at C speed (~3-5× faster
            # than csv.DictReader). Falls back to csv.DictReader if not installed.
            try:
                import pandas as _pd
                _HAS_PANDAS = True
            except ImportError:
                _HAS_PANDAS = False

            # col_schema already built from peek_headers — reuse it
            col_schema  = _peek_col_schema
            headers     = peek_headers

            if _HAS_PANDAS and not effective_columns and not is_compressed:
                # pandas path: read in chunks, convert each chunk to dicts.
                # Skip leading rows at chunk level (not row-by-row) so large
                # resumes don't iterate every skipped row individually.
                #
                # byte_range mode (single-file parallel): use _ByteRangeReader to
                # seek directly to this worker's byte slice (O(1), no row scanning).
                # The reader injects the header line so pandas resolves column names.
                try:
                    if byte_range is not None:
                        byte_start, byte_end = byte_range
                        with open(path, "rb") as _hf:
                            _hdr = _hf.readline()
                            if not _hdr.endswith(b"\n"):
                                _hdr += b"\n"
                        _raw_reader = _ByteRangeReader(path, byte_start, byte_end, _hdr)
                        _csv_src = io.BufferedReader(_raw_reader)
                        # Expose reader so checkpoint callback can snapshot file position
                        self._current_byte_reader = _raw_reader
                        self._last_safe_byte_pos  = _raw_reader._f.tell()
                        self._rows_at_safe_pos    = self._rows_processed
                    else:
                        _raw_reader = None
                        _csv_src = path

                    reader_iter = _pd.read_csv(
                        _csv_src,
                        delimiter=effective_delimiter,
                        dtype=str,
                        keep_default_na=False,
                        chunksize=10_000,
                        encoding="utf-8",
                        encoding_errors="replace",
                        on_bad_lines="skip",
                    )
                    total_skipped = 0
                    for chunk in reader_iter:
                        # Chunk-level skip: drop entire chunks without touching rows
                        if total_skipped + len(chunk) <= skip_rows:
                            total_skipped += len(chunk)
                            # Still advance the safe byte position so resume is accurate
                            if _raw_reader is not None:
                                self._last_safe_byte_pos = _raw_reader._f.tell()
                                self._rows_at_safe_pos   = self._rows_processed
                            continue
                        if total_skipped < skip_rows:
                            chunk = chunk.iloc[skip_rows - total_skipped:]
                            total_skipped = skip_rows

                        for row_dict in chunk.to_dict("records"):
                            try:
                                self.add_row(row_dict, headers, file_rel, col_schema)
                                count += 1
                                _tick()
                            except Exception as err:
                                print(f"Error: {err}")

                        # After each complete chunk: snapshot file position so the
                        # checkpoint callback records the last safe resume point.
                        # _f.tell() here = start of NEXT chunk = safe O(1) seek target.
                        if _raw_reader is not None:
                            self._last_safe_byte_pos = _raw_reader._f.tell()
                            self._rows_at_safe_pos   = self._rows_processed

                    if _raw_reader is not None:
                        _raw_reader.close()
                        self._current_byte_reader = None
                except Exception as err:
                    print(f"  pandas read failed ({err}), falling back to csv.DictReader")
                    _HAS_PANDAS = False

            if not _HAS_PANDAS or effective_columns:
                # csv.DictReader path.
                # Raise field_size_limit so long fields don't throw csv.Error mid-loop.
                # Wrap iteration in while/next so a single bad line triggers 'continue',
                # not a break of the entire loop.
                csv.field_size_limit(2 ** 27)   # 128 MB per field — effectively unlimited

                # Byte-range workers: read the SAME byte slice, not the full file.
                # Opening the full file here would read all N rows instead of this
                # worker's slice, blowing past the pre-allocated doc_id range.
                if byte_range is not None:
                    _br_start, _br_end = byte_range
                    with open(path, "rb") as _hf2:
                        _hdr2 = _hf2.readline()
                        if not _hdr2.endswith(b"\n"):
                            _hdr2 += b"\n"
                    _raw2 = _ByteRangeReader(path, _br_start, _br_end, _hdr2)
                    # TextIOWrapper requires BufferedIOBase, not RawIOBase directly
                    _f_ctx: io.IOBase = io.TextIOWrapper(io.BufferedReader(_raw2),
                                                         encoding="utf-8",
                                                         errors="replace",
                                                         newline="")
                else:
                    _f_ctx = _open_source(path, newline="")

                try:
                    reader = csv.DictReader(_f_ctx, fieldnames=effective_columns or None,
                                            delimiter=effective_delimiter)
                    if skip_rows:
                        for _ in islice(reader, skip_rows):
                            pass
                    _iter = iter(reader)
                    while True:
                        try:
                            row = next(_iter)
                        except StopIteration:
                            break
                        except Exception as err:
                            print(f"  Row parse error (skipping 1 row): {err}")
                            continue
                        try:
                            if len(row) != len(headers):
                                continue
                            self.add_row(row, headers, file_rel, col_schema)
                            count += 1
                            _tick()
                        except Exception as err:
                            print(f"Error: {err}")
                finally:
                    _f_ctx.close()

        elif ext in (".json", ".jsonl", ".ndjson"):
            headers = None
            col_schema = None
            skipped = 0
            _json_iter = _iter_json_file(path)
            while True:
                try:
                    hdrs, row = next(_json_iter)
                except StopIteration:
                    break
                except Exception as err:
                    print(f"  Row parse error (skipping 1 row): {err}")
                    continue
                try:
                    if headers is None:
                        headers = hdrs
                        col_schema = self._build_col_schema(headers, file_rel)
                    if skipped < skip_rows:
                        skipped += 1
                        continue
                    self.add_row(row, headers, file_rel, col_schema)
                    count += 1
                    _tick()
                except Exception as err:
                    print(f"Error: {err}")

        else:
            print("    Skipped (unsupported format)")
            return 0

        elapsed = time.time() - t_start
        rate = count / elapsed if elapsed > 0 else 0

        # Final progress update for parent aggregation (workers with < REPORT_EVERY rows
        # never triggered _tick(), so we write one last update with the real count).
        if self._worker_id is not None:
            try:
                ppath = os.path.join(self.output_dir,
                                     f"progress_w{self._worker_id}.json")
                _st = self._last_bg_stats
                with open(ppath, "w") as _pf:
                    _pf.write(json.dumps({
                        "rows": count, "total": count, "instant_rate": 0,
                        "ckpt":       self._checkpoint_count,
                        "encode":     round(_st["t_encode"], 2),
                        "disk":       round(_st["t_disk"], 2),
                        "wait":       round(max(self._last_bg_duration
                                                - _st["t_encode"] - _st["t_disk"], 0), 2),
                        "disk_bufs":  _st["n_disk"],
                        "mem_bufs":   _st["n_mem"],
                        "mem_buf_mb": _st["mem_bytes"] >> 20,
                        "wb_kb":      (0 if self._daemon
                                       else self._checkpoint_write_bytes >> 10),
                        "ckpt_rows":  self._rows_per_checkpoint,
                        "daemon":     self._daemon,
                    }))
            except Exception:
                pass

        sys.stderr.write("\r" + " " * 100 + "\r")  # clear progress line
        sys.stderr.flush()
        print(f"  Done: {count:,} rows in {_fmt_duration(elapsed)} ({rate:,.0f} rows/s)")
        return count

    # ─── Finalize ─────────────────────────────────────────────────────────────

    def finalize(self, progress_callback=None):
        """Finalize the index — flush all remaining data to disk.

        Args:
            progress_callback: optional callable(flush_done, flush_total, message)
                               called after each batch of prefixes is flushed so the
                               caller can track flush progress in real-time.
        """
        import sys
        from concurrent.futures import ThreadPoolExecutor, as_completed

        sys.stderr.write(f"\n  [finalize] starting — pending_docs={len(self._pending_docs)}, "
                         f"pending_terms={len(self._pending_terms)}, "
                         f"buffers={len(self._buffers)}, total_docs={self.total_docs}\n")
        sys.stderr.flush()

        # Stop the lazy writer first (daemon mode) so it doesn't race with us.
        if self._lazy_writer_thread is not None:
            self._lazy_writer_stop.set()
            self._lazy_writer_thread.join(timeout=30)
            self._lazy_writer_thread = None

        # Wait for any in-flight background checkpoint flush before touching
        # the index files ourselves — avoids concurrent writes to idx.bin.
        if self._flush_thread is not None:
            self._flush_thread.join()
            self._flush_thread = None
            if self._flush_thread_exc is not None:
                exc = self._flush_thread_exc
                self._flush_thread_exc = None
                raise RuntimeError(
                    f"Background flush failed — index may be incomplete.\n"
                    f"Cause: {exc}\n"
                    f"Check disk space (df -h) and permissions on the output directory."
                ) from exc

        # Merge any pressure WAL files written during the build (non-daemon mode).
        # These contain buffer data that was too small to write per-prefix-file but
        # needed to be spilled from RAM.  Must happen before _flush_terms() which
        # may add more data to the same prefix files.
        self._merge_pressure_wals()

        print("\nFlushing remaining data...")
        t0 = time.time()
        self._flush_docs()
        sys.stderr.write(f"  [finalize] after _flush_docs: total_docs={self.total_docs}, pending_docs={len(self._pending_docs)}\n")
        sys.stderr.flush()
        self._flush_terms()
        sys.stderr.write(f"  [finalize] after _flush_terms: _total_entries={self._total_entries}, buffers={len(self._buffers)}\n")
        sys.stderr.flush()

        # Flush prefix buffers in parallel — each bucket is independent
        prefixes = list(self._buffers)
        n_bufs = len(prefixes)
        done = 0

        def _upd_flush(flush_done, flush_total):
            """Write flush progress to progress_w{N}.json so orchestrator can display it."""
            if self._worker_id is None:
                return
            try:
                ppath = os.path.join(self.output_dir,
                                     f"progress_w{self._worker_id}.json")
                _st = self._last_bg_stats
                with open(ppath, "w") as _pf:
                    _pf.write(json.dumps({
                        "rows":        self.total_docs,
                        "total":       self.total_docs,
                        "instant_rate": 0,
                        "ckpt":        self._checkpoint_count,
                        "encode":      round(_st["t_encode"], 2),
                        "disk":        round(_st["t_disk"], 2),
                        "wait":        0.0,
                        "disk_bufs":   flush_done,
                        "mem_bufs":    flush_total - flush_done,
                        "mem_buf_mb":  _st["mem_bytes"] >> 20,
                        "wb_kb":       0,
                        "ckpt_rows":   self._rows_per_checkpoint,
                        "daemon":      self._daemon,
                        "flushing":    True,
                        "flush_done":  flush_done,
                        "flush_total": flush_total,
                    }))
            except Exception:
                pass

        _upd_flush(0, n_bufs)  # signal start of flush phase

        # ── Bucket-batch flush: group by bucket (2-char prefix) ─────────────────
        # Old approach: 47K individual file ops (open+write+close per prefix).
        # New approach: ~256 bucket ops — each bucket writes all its ~185 prefix
        # files in one sequential pass with dirs/files created once per bucket.
        # Net effect: 47K syscalls → ~256 bucket syscalls + sequential disk writes.

        # Group prefixes by bucket (first 2 chars of hex prefix)
        _buckets: dict = {}
        for _p in prefixes:
            _bucket = _p[:2]
            if _bucket not in _buckets:
                _buckets[_bucket] = []
            _buckets[_bucket].append(_p)

        _bucket_list = list(_buckets.items())  # [(bucket, [prefixes]), ...]
        _n_buckets = len(_bucket_list)
        _done_lock = _threading.Lock()

        def _flush_bucket(batch_bucket, batch_prefixes):
            """Flush all prefixes for one bucket in one sequential pass."""
            nonlocal done
            # One dir create per bucket (not per prefix)
            _dir_path = os.path.join(self.index_dir, batch_bucket)
            try:
                os.makedirs(_dir_path, exist_ok=True)
            except Exception as _e:
                pass
            _known = self._known_dirs
            if _dir_path not in _known:
                _known.add(_dir_path)
            _prefix: str
            _buf: bytearray
            _pending_small = []  # (prefix_rel, buf) for batch write
            _written = 0

            _prefix_dir = os.path.join(_dir_path, _prefix_rel)
            _prefix_created = False

            for _i, _prefix in enumerate(sorted(batch_prefixes)):
                _buf = self._buffers.get(_prefix)
                if not _buf:
                    continue
                _prefix_rel = _prefix[3:]  # "xx/yy" → "yy"
                if _prefix_dir != os.path.join(_dir_path, _prefix_rel):
                    _prefix_dir = os.path.join(_dir_path, _prefix_rel)
                    _prefix_created = False
                _file_path = os.path.join(_prefix_dir, self._idx_filename)

                # Write buffer to disk
                try:
                    if not _prefix_created:
                        os.makedirs(_prefix_dir, exist_ok=True)
                        _prefix_created = True
                    with open(_file_path, "wb") as _f:
                        _f.write(_buf)
                    _buf.clear()
                    self._idx_bytes_flushed += len(_buf) if hasattr(_buf, '__len__') else 0
                except Exception:
                    pass
                _written += 1

                # Batch progress update every BATCH_SIZE prefixes
                if _written % BATCH_SIZE == 0:
                    with _done_lock:
                        done += BATCH_SIZE
                        if done % 5000 == 0:
                            sys.stderr.write(f"\r  Writing index buffers: {done:,}/{n_bufs:,}")
                            sys.stderr.flush()
                            _upd_flush(done, n_bufs)

            with _done_lock:
                done += _written
                _upd_flush(done, n_bufs)

        # Parallel: each thread handles N buckets (not N prefixes)
        with ThreadPoolExecutor(max_workers=FLUSH_THREADS) as _pool:
            _futs = {_pool.submit(_flush_bucket, _bkt, _prefs): (_bkt, _prefs)
                     for _bkt, _prefs in _bucket_list}
            for _fut in as_completed(_futs):
                try:
                    _fut.result()
                except Exception as _e:
                    pass  # errors logged inside _flush_bucket

        sys.stderr.write(f"  [finalize] buffers flushed: {done}/{n_bufs} prefixes, "
                         f"{_n_buckets} buckets, idx_bytes={self._idx_bytes_flushed:,}\n")
        sys.stderr.flush()
        self._buffers.clear()
        sys.stderr.write("\r" + " " * 60 + "\r")
        sys.stderr.flush()
        print(f"  Flush done in {_fmt_duration(time.time() - t0)}")

        # ── Merge segment files into final idx.bin ───────────────────────────────
        # All segment files (idx_seg*.bin) are merged into one compressed idx.bin,
        # then the segment files are deleted.  This is the ONE time we pay the
        # O(N) recompression cost — instead of O(N²) at every checkpoint.
        if self._seg_counters:
            t_seg = time.time()
            segs_merged = self._finalize_merge_segments()
            sys.stderr.write(f"  [finalize] merged {segs_merged} segment files "
                             f"in {_fmt_duration(time.time() - t_seg)}\n")
            sys.stderr.flush()

        # Count index files — includes all *.bin variants (worker shards)
        idx_size = idx_files = 0
        for root, _, files in os.walk(self.index_dir):
            for f in files:
                if f.endswith(".bin"):
                    idx_size += os.path.getsize(os.path.join(root, f))
                    idx_files += 1

        # Count doc store — must walk subdirs (2-level hierarchy)
        doc_size = 0
        for root, _, files in os.walk(self.docs_dir):
            for f in files:
                if f.endswith(".zlib"):
                    doc_size += os.path.getsize(os.path.join(root, f))

        # Load existing stats to merge columns from previous runs.
        # In worker mode, stats_path is stats_w{N}.json — orchestrator merges later.
        # If the file is a mid-build partial snapshot (_partial=True) written by
        # _write_partial_stats(), use _prev_entries as the baseline so we don't
        # double-count self._total_entries which was already included there.
        stats_path = self._stats_path
        prev_cols = {}
        prev_entries = 0
        if os.path.exists(stats_path):
            with open(stats_path) as f:
                prev = json.load(f)
            prev_cols = prev.get("columns", {})
            if prev.get("_partial"):
                prev_entries = prev.get("_prev_entries", 0)
            else:
                prev_entries = prev.get("total_entries", 0)

        merged_cols = {**prev_cols, **self.columns_seen}

        # rows_indexed = actual rows processed in this session (not the doc_counter value,
        # which starts at start_doc_id and is inflated in parallel/incremental builds).
        rows_indexed = self.doc_counter - (self.doc_counter - self.total_docs)
        # Simpler: total_docs is incremented once per doc flushed = actual row count.
        rows_indexed = self.total_docs

        stats = {
            # total_docs: the final doc_counter value, which represents the highest
            # doc_id + 1 seen.  For parallel builds this is per-worker; merge_worker_stats
            # sums rows_indexed (actual row count) instead.
            "total_docs": self.doc_counter,
            "rows_indexed": rows_indexed,   # actual rows in this session
            "total_entries": prev_entries + self._total_entries,
            "doc_chunk_size": DOC_CHUNK_SIZE,
            "index_files": idx_files,
            "index_size_mb": round(idx_size / 1024**2, 1),
            "docs_size_mb": round(doc_size / 1024**2, 1),
            "total_size_mb": round((idx_size + doc_size) / 1024**2, 1),
            "columns": merged_cols,
        }

        with open(stats_path, "w") as f:
            json.dump(stats, f, indent=2)

        # ── Write term sets for fast bucket-level term existence check ─────────
        self._write_bucket_term_sets()

        # ── Write doc_values columnar storage for fast aggregations ─────────────
        self._write_doc_values()

        print(f"  Docs:  {stats['total_docs']:,} | {stats['docs_size_mb']:.1f} MB")
        print(f"  Index: {idx_files:,} files | {stats['index_size_mb']:.1f} MB")
        print(f"  Total: {stats['total_size_mb']:.1f} MB")
        return stats

    # ─── Columnar doc_values for fast aggregations ───────────────────────────────

    def _write_doc_values(self):
        """Write per-field doc_values (columnar storage) for fast aggregations.

        For INT/FLOAT fields: stores sorted [doc_id, value] pairs → O(log N) range queries
        For KEYWORD fields: stores term→doc_count map → O(1) terms aggregation

        Storage format:
          dv/{field}/numeric.bin   — binary [doc_id_le, value_le] pairs, sorted by value
          dv/{field}/terms.bin     — binary [(term_bytes, count_le), ...] sorted by count

        Complexity:
          Before: O(N) full scan for any aggregation
          After:  O(log N) for range queries, O(K) for terms (K = unique terms)
        """
        import struct as _struct

        from collections import Counter
        import sys as _sys
        dv_dir = os.path.join(self.output_dir, "dv")
        os.makedirs(dv_dir, exist_ok=True)

        _pack_i   = _struct.Struct("<I").pack
        _pack_f   = _struct.Struct("<d").pack
        _pack_h   = _struct.Struct("<H").pack
        _pack_q   = _struct.Struct("<Q").pack

        # Identify numeric, keyword, and array fields from columns_seen
        numeric_fields = {
            f for f, t in self.columns_seen.items()
            if t in ("INT", "FLOAT")
        }
        keyword_fields = {
            f for f, t in self.columns_seen.items()
            if t == "KEYWORD"
        }
        array_fields = {
            f for f, t in self.columns_seen.items()
            if t == "ARRAY"
        }

        if not numeric_fields and not keyword_fields and not array_fields:
            _sys.stderr.write(f"  [doc_values] WARNING: no numeric/keyword/array fields found — columns_seen={dict(self.columns_seen)}\n")
            _sys.stderr.flush()
            return

        # Collect all values per field by streaming chunks
        numeric_data: dict = {}     # field → list of (value, doc_id)
        keyword_data: dict = {}     # field → Counter of term→count (KEYWORD + ARRAY elements)

        for field in numeric_fields:
            numeric_data[field] = []
        for field in keyword_fields | array_fields:
            keyword_data[field] = Counter()

        for chunk_start, chunk in self._iter_chunks_for_build():
            for doc_id, doc in chunk.items():
                for field in numeric_fields:
                    v = doc.get(field)
                    if v is None:
                        continue
                    try:
                        fval = float(str(v).replace(",", ""))
                        numeric_data[field].append((fval, doc_id))
                    except (ValueError, TypeError):
                        pass
                for field in keyword_fields:
                    v = doc.get(field)
                    if v is None:
                        continue
                    keyword_data[field][str(v)] += 1
                for field in array_fields:
                    v = doc.get(field)
                    if v is None:
                        continue
                    try:
                        arr = json.loads(v) if isinstance(v, str) else v
                        if isinstance(arr, list):
                            for item in arr:
                                keyword_data[field][str(item)] += 1
                    except (ValueError, TypeError):
                        pass

        # Write numeric doc_values
        for field, pairs in numeric_data.items():
            if not pairs:
                continue
            pairs.sort(key=lambda x: x[0])  # sort by value
            field_dir = os.path.join(dv_dir, field)
            os.makedirs(field_dir, exist_ok=True)
            path = os.path.join(field_dir, "numeric.bin")
            with open(path, "wb") as f:
                for value, doc_id in pairs:
                    f.write(_pack_q(doc_id))   # 8-byte doc_id
                    f.write(_pack_f(value))    # 8-byte float value

        # Write keyword doc_values (term→count for fast terms aggregation)
        for field, counter in keyword_data.items():
            if not counter:
                continue
            field_dir = os.path.join(dv_dir, field)
            os.makedirs(field_dir, exist_ok=True)
            path = os.path.join(field_dir, "terms.bin")
            with open(path, "wb") as f:
                for term, count in counter.most_common():
                    term_bytes = term.encode("utf-8")
                    f.write(_pack_h(len(term_bytes)))   # 2-byte term length
                    f.write(term_bytes)                  # variable-length term
                    f.write(_pack_i(count))             # 4-byte count

    def _write_bucket_term_sets(self):
        """Write per-bucket term sets for O(1) fast-reject of non-existent terms.

        Scans each bucket's *.bin files after flush, extracts all indexed terms,
        and writes a pickled Python set as {bucket_dir}/terms.set.

        At query time, flatseek loads the term set before reading posting files
        — if a term is not in the set, the posting file read is skipped entirely.
        This gives 2x–4x speedup for queries with terms that don't exist, which
        is the common case for broad wildcard patterns.

        Storage overhead: ~1 byte per term (very compact for terms that share
        prefixes due to the hash-partitioned bucket layout).
        """
        import pickle
        bucket_count = 0
        for bucket_dir in os.listdir(self.index_dir):
            bucket_path = os.path.join(self.index_dir, bucket_dir)
            if not os.path.isdir(bucket_path):
                continue

            all_terms = set()
            for bin_file in os.listdir(bucket_path):
                if not bin_file.endswith(".bin"):
                    continue
                bin_path = os.path.join(bucket_path, bin_file)
                try:
                    with open(bin_path, "rb") as f:
                        data = f.read()
                    if not data:
                        continue
                    # Decompress if compressed
                    if len(data) >= 2 and data[0] == 0x78 and data[1] in (0x01, 0x5e, 0x9c, 0xda):
                        try:
                            data = zlib.decompress(data)
                        except Exception:
                            continue

                    offset = 0
                    while offset + 2 <= len(data):
                        term_len = struct.unpack_from("<H", data, offset)[0]; offset += 2
                        if offset + term_len > len(data):
                            break
                        try:
                            term = data[offset:offset + term_len].decode("utf-8")
                            all_terms.add(term)
                        except UnicodeDecodeError:
                            pass
                        offset += term_len
                        if offset + 4 > len(data):
                            break
                        pl_len = struct.unpack_from("<I", data, offset)[0]; offset += 4
                        offset += pl_len
                except Exception:
                    continue

            if all_terms:
                set_path = os.path.join(bucket_path, "terms.set")
                try:
                    with open(set_path, "wb") as f:
                        pickle.dump(all_terms, f)
                    bucket_count += 1
                except Exception:
                    pass

        if bucket_count:
            sys.stderr.write(f"  Bloom/sets: {bucket_count} bucket term sets written\n")
            sys.stderr.flush()

    def _iter_chunks_for_build(self):
        """Yield (chunk_start, chunk_dict) by re-reading flushed chunk files from disk.

        Used by _write_doc_values() after finalize() has flushed all pending docs.
        """
        import glob as _glob
        docs_dir = self.docs_dir
        pattern = os.path.join(docs_dir, "**", "*.zlib")
        files = _glob.glob(pattern, recursive=True)
        if not files:
            pattern2 = os.path.join(docs_dir, "chunks_*.zlib")
            files = _glob.glob(pattern2)
        if not files:
            return

        for path in sorted(files):
            try:
                m = re.match(r"^docs_(\d+)\.zlib$", os.path.basename(path))
                if not m:
                    m = re.match(r"^chunks_(\d+)\.zlib$", os.path.basename(path))
                    if not m:
                        continue
                chunk_start = int(m.group(1))
                with open(path, "rb") as fh:
                    blob = fh.read()
                raw = _decompress_doc(blob)
                chunk = {int(k): v for k, v in _doc_loads(raw).items()}
                yield chunk_start, chunk
            except Exception:
                pass

    # ─── Helpers ─────────────────────────────────────────────────────────────

def _norm(s):
    return s.lower().strip().replace(" ", "_").replace("-", "_")


def find_data_files(directory):
    """Find CSV and JSON data files, skipping output/work directories.

    Recognises plain and compressed variants (.gz, .bz2).
    """
    skip = {"data", "data2", "testdata", "tmp", ".git", "__pycache__", ".venv", "env"}
    exts = {
        ".csv", ".json", ".jsonl", ".ndjson",
        ".csv.gz",   ".csv.bz2",
        ".json.gz",  ".json.bz2",
        ".jsonl.gz", ".jsonl.bz2",
        ".ndjson.gz", ".ndjson.bz2",
    }
    result = []
    for root, dirs, files in os.walk(directory):
        dirs[:] = [d for d in dirs if d not in skip]
        for f in sorted(files):
            if any(f.endswith(e) for e in exts):
                result.append(os.path.join(root, f))
    return result


def _json_val_to_str(v):
    """Convert any JSON value to a string suitable for indexing.

    Handles:
    - None → ""
    - dict/list → json.dumps
    - string that looks like JSON/Python-literal array/object → parse and re-dumps
    - plain string/number/bool → str(value)

    Fast path: a string only triggers parser attempts when its first char is
    `[` or `{`. The vast majority of CSV cell values are plain hex/text/numbers
    where no parsing is needed — the previous unconditional `ast.literal_eval`
    fallback compiled an AST per cell (~1.3M `compile` calls on a 100K-row
    blockchain CSV) and dominated build time.
    """
    if v is None:
        return ""
    t = type(v)
    if t is dict or t is list:
        return json.dumps(v, ensure_ascii=False)
    if t is not str:
        return str(v)
    # String fast path: skip parser attempts unless the value looks structured.
    if not v or v[0] not in ('[', '{'):
        return v
    # Looks structured — try strict JSON first (fast C parser, no compile).
    try:
        parsed = json.loads(v)
        if isinstance(parsed, (dict, list)):
            return json.dumps(parsed, ensure_ascii=False)
    except (ValueError, TypeError):
        pass
    # Fall back to Python-literal only when single-quote syntax is present
    # (e.g. "['api','cdn']" written by str(list)).  Avoids the compile step
    # for malformed JSON that has no chance of being a Python literal either.
    if "'" in v:
        try:
            import ast
            parsed = ast.literal_eval(v)
            if isinstance(parsed, (dict, list)):
                return json.dumps(parsed, ensure_ascii=False)
        except (ValueError, SyntaxError, TypeError):
            pass
    return v


def _expand_record(obj, prefix="", sep="."):
    """Recursively expand a dict into flat dot-path keys.

    {"tags": ["api","cdn"], "address": {"city": "NYC"}}
        ↓
    {"tags": '["api","cdn"]', "tags[0]": "api", "tags[1]": "cdn", "address.city": "NYC"}

    - Simple arrays stored as full JSON string for clean display (tags: ["api","cdn"])
    - Each element also expanded for search (tags[0], tags[1] etc.)
    - Nested arrays/dicts expanded recursively for deep field search

    Handles both JSON arrays and Python-style single-quoted arrays.
    """
    result = {}
    for key, val in obj.items():
        if type(key) is not str:
            key = str(key)
        full_key = f"{prefix}{key}" if prefix else key
        # Dispatch by input type — avoids the str-roundtrip + json.loads probe
        # that previously ran on every cell (the dominant cost on text-heavy CSVs).
        t = type(val)
        if t is dict:
            parsed = val
        elif t is list:
            parsed = val
        elif val is None:
            result[full_key] = ""
            continue
        elif t is str:
            # Only attempt to parse strings that look structured.
            if val and val[0] in ('[', '{'):
                try:
                    parsed = json.loads(val)
                except (ValueError, TypeError):
                    parsed = None
                    if "'" in val:
                        try:
                            import ast
                            parsed = ast.literal_eval(val)
                        except (ValueError, SyntaxError, TypeError):
                            parsed = None
                if not isinstance(parsed, (dict, list)):
                    # Not actually structured — store as plain string and skip recursion
                    result[full_key] = val
                    continue
            else:
                result[full_key] = val
                continue
        else:
            result[full_key] = str(val)
            continue

        # `parsed` is now a dict or list. Recurse / expand.
        if isinstance(parsed, dict):
            result.update(_expand_record(parsed, full_key + sep, sep))
        else:  # list
            result[full_key] = json.dumps(parsed, ensure_ascii=False)
            for i, item in enumerate(parsed):
                arr_key = f"{full_key}[{i}]"
                if type(item) is dict:
                    result.update(_expand_record(item, arr_key + sep, sep))
                elif type(item) is list:
                    result[arr_key] = json.dumps(item, ensure_ascii=False)
                else:
                    result[arr_key] = "" if item is None else str(item)
    return result


def _collapse_record(expanded_record, sep="."):
    """Reverse _expand_record: collapse dot-notation and array-index notation
    back into nested dicts and arrays.

    {"tags": '["api","cdn"]', "tags[0]": "api", "tags[1]": "cdn",
     "address.city": "NYC", "address": '{"city":"NYC"}'}
        ↓
    {"tags": ["api", "cdn"], "address": {"city": "NYC"}}

    Only collapses fields that were expanded by _expand_record. All other
    fields (primitives, already-clean nested) pass through unchanged.
    """
    import re as _re

    result = {}
    # Track keys that were expanded (array indices and dot-notation) to skip in final pass
    expanded_keys = set()  # keys like "tags[0]", "address.city" that should not appear in result

    # First pass: identify all array-indexed keys (tags[0], tags[1], etc.)
    # and reconstruct arrays from their parent JSON string
    array_index_keys = sorted(
        k for k in expanded_record.keys()
        if _re.match(r"^(.+)\[(\d+)\]$", k)
    )
    expanded_arrays = set()  # parent keys that are arrays (e.g., "tags")

    for key in array_index_keys:
        m = _re.match(r"^(.+)\[(\d+)\]$", key)
        assert m
        parent = m.group(1)
        idx = int(m.group(2))
        expanded_keys.add(key)

        # Find the actual array parent key (e.g., "info.metadata.a.tags" for key "info.metadata.a.tags[0]")
        # parent="info.metadata.a", but actual array key="info.metadata.a.tags"
        actual_parent = None
        arr = None
        for try_parent in [parent, key.rsplit('[', 1)[0]]:
            if try_parent in expanded_record:
                try:
                    arr = json.loads(expanded_record[try_parent])
                    if isinstance(arr, list):
                        actual_parent = try_parent
                        break
                except (json.JSONDecodeError, TypeError):
                    pass

        if actual_parent is None:
            continue

        if actual_parent not in expanded_arrays:
            expanded_arrays.add(actual_parent)
            result[actual_parent] = [None] * len(arr)
        if actual_parent in result and idx < len(result[actual_parent]):
            val = expanded_record[key]
            try:
                parsed = json.loads(val)
                if isinstance(parsed, (dict, list)):
                    val = parsed
            except (json.JSONDecodeError, TypeError):
                pass
            result[actual_parent][idx] = val

    # Second pass: handle dot-notation nested keys (address.city, info.metadata.a.value, etc.)
    # Skip keys that are array indices (items[0].name) - handled in first pass
    dot_keys = sorted(
        k for k in expanded_record.keys()
        if sep in k and k not in expanded_arrays and not _re.match(r"^(.+)\[(\d+)\]$", k)
    )
    for key in dot_keys:
        expanded_keys.add(key)
        parts = key.split(sep)  # split on ALL separators for deep paths
        if len(parts) < 2:
            continue
        # Build nested structure: walk the path, creating dicts as needed
        d = result
        for i in range(len(parts) - 1):
            part = parts[i]
            if part not in d:
                d[part] = {}
            # If intermediate parent key exists in expanded_record as JSON, merge its structure
            parent_key = sep.join(parts[:i+1])
            if parent_key in expanded_record:
                try:
                    parent_val = json.loads(expanded_record[parent_key])
                    if isinstance(parent_val, dict) and isinstance(d[part], dict):
                        for k, v in parent_val.items():
                            if k not in d[part]:
                                d[part][k] = v
                except (json.JSONDecodeError, TypeError):
                    pass
            # If d[part] is already a non-dict (string from a previous shorter path),
            # we can't continue nesting — stop here and fall back to storing as-is
            if not isinstance(d[part], dict):
                break
            d = d[part]
        else:
            # Only set the value if we completed the full path without breaking
            d[parts[-1]] = expanded_record[key]
            continue
        # Fallback: if we broke out of the loop, store as top-level flat key
        result[key] = expanded_record[key]

    # Third pass: add all other keys that weren't part of expansion
    for key, val in expanded_record.items():
        if key in expanded_keys:
            continue
        if key in expanded_arrays:
            continue
        # Skip keys that match array index pattern (items[0], etc.) - these are expanded elements
        if _re.match(r"^(.+)\[(\d+)\]$", key):
            continue
        if sep in key:
            parts = key.split(sep, 1)
            parent = parts[0]
            if parent in expanded_arrays:
                continue
        if key not in result:
            result[key] = expanded_record[key]

    return result


_ES_BULK_ACTIONS = frozenset({"index", "create", "update", "delete"})


def _is_es_bulk_action(obj):
    """True when obj is an Elasticsearch bulk API action line."""
    return isinstance(obj, dict) and len(obj) == 1 and next(iter(obj)) in _ES_BULK_ACTIONS


def _iter_json_single_obj(data):
    """Yield (headers, row) from an already-parsed single JSON value.

    Handles:
      - list of dicts (JSON array)
      - ES search result   {"hits": {"hits": [{"_source": {...}}]}}
      - wrapped array      {"data": [...]}
      - plain flat dict    {"key": "val", ...}  → treated as 1-row
    """
    if isinstance(data, list):
        records = data
    elif isinstance(data, dict):
        hits_wrapper = data.get("hits", {})
        if isinstance(hits_wrapper, dict) and isinstance(hits_wrapper.get("hits"), list):
            # ES search result
            records = []
            for hit in hits_wrapper["hits"]:
                if not isinstance(hit, dict):
                    continue
                src = hit.get("_source")
                records.append(src if isinstance(src, dict) else hit)
        else:
            # Look for the first list-of-dicts value (wrapped array)
            list_val = None
            for v in data.values():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    list_val = v
                    break
            records = list_val if list_val is not None else [data]
    else:
        return

    headers = None
    for rec in records:
        if not isinstance(rec, dict):
            continue
        row = _expand_record(rec)
        if headers is None:
            headers = list(row.keys())
        yield headers, row


def _iter_es_jsonl(f):
    """Yield (headers, row) from Elasticsearch JSONL (elasticdump format).

    Each line: {"_source": {...}, "_id": "...", "_index": "..."}
    """
    headers = None
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(obj, dict):
            continue
        src = obj.get("_source")
        data = src if isinstance(src, dict) else obj
        row = _expand_record(data)
        if headers is None:
            headers = list(row.keys())
        yield headers, row


def _iter_es_bulk(f):
    """Yield (headers, row) from Elasticsearch bulk API format.

    Alternating lines: action {"index":{...}} / document {...}
    Uses _is_es_bulk_action to stay in sync even after malformed lines.
    """
    headers = None
    expect_doc = False
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            expect_doc = False
            continue
        if not isinstance(obj, dict):
            expect_doc = False
            continue
        if _is_es_bulk_action(obj):
            expect_doc = True
            continue
        if not expect_doc:
            continue
        expect_doc = False
        src = obj.get("_source")
        data = src if isinstance(src, dict) else obj
        row = _expand_record(data)
        if headers is None:
            headers = list(row.keys())
        yield headers, row


def _iter_regular_jsonl(f):
    """Yield (headers, row) from regular JSONL (one JSON object per line)."""
    headers = None
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(obj, dict):
            continue
        row = _expand_record(obj)
        if headers is None:
            headers = list(row.keys())
        yield headers, row


def _iter_json_file(path):
    """Yield (headers, row_dict) from a JSON or JSONL file.

    Auto-detects format:
      1. JSON array          [{...}, ...]
      2. Regular JSONL       one flat JSON object per line
      3. ES JSONL            {"_source":{...}, "_id":"...", ...}  per line  (elasticdump)
      4. ES bulk API         alternating {"index":{...}} / {doc} lines
      5. ES search result    {"hits": {"hits": [{"_source":{...}}]}}
      6. Wrapped array       {"data": [...]}  — uses first list-of-dicts value

    Works with both seekable (plain) and non-seekable (gzip/bz2) streams.
    JSONL variants are streamed without re-opening; whole-file JSON formats
    (array, pretty-printed object) re-open the file once.
    """
    # ── First pass: read just enough to detect the format ────────────────────
    with _open_source(path) as f:
        first_char = f.read(1)
        if not first_char:
            return

        if first_char == "{":
            # Read the first complete line to distinguish JSONL from pretty JSON.
            # Prepend first_char — f.read(1) already consumed the opening '{'.
            first_line = first_char + f.readline().rstrip("\n")

            try:
                first_obj = json.loads(first_line)
                first_line_complete = True
            except json.JSONDecodeError:
                first_line_complete = False
                first_obj = None

            if first_line_complete and isinstance(first_obj, dict):
                # Peek: is there a second non-empty line?
                next_line = ""
                while True:
                    nl = f.readline()
                    if not nl:
                        break
                    nl = nl.strip()
                    if nl:
                        next_line = nl
                        break

                if next_line:
                    # ── JSONL — stream the rest without re-opening ─────────────
                    # Chain already-read lines back before the remaining file.
                    def _chain(f=f, first=first_line, second=next_line):
                        yield first
                        yield second
                        yield from f

                    if "_source" in first_obj:
                        yield from _iter_es_jsonl(_chain())    # Format 3: ES JSONL
                    elif _is_es_bulk_action(first_obj):
                        yield from _iter_es_bulk(_chain())     # Format 4: ES bulk
                    else:
                        yield from _iter_regular_jsonl(_chain())  # Format 2: JSONL
                    return
                # else: single minified object — fall through to re-open below

    # ── Second pass: whole-file formats (array, pretty-printed, single obj) ──
    # Re-open so json.load() reads from the start.  Acceptable: these formats
    # are typically smaller and must be fully loaded into memory anyway.
    with _open_source(path) as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"    Warning: could not parse {path}: {e}")
            return
        yield from _iter_json_single_obj(data)  # Formats 1, 5, 6, or single {}

# ─── Manifest (tracks indexed files to prevent re-index) ─────────────────────

def plan(csv_src, output_dir, n_workers, delimiter=",", columns=None):
    """Generate a build_plan.json that splits work across n_workers.

    Each worker gets:
      - A non-overlapping list of files to index
      - A non-overlapping doc_id range (pre-computed from row counts)
      - A unique worker_id used to name its index shard (idx_w{id}.bin)

    Files are assigned to workers using a greedy load-balancing algorithm that
    tries to equalise the total row count per worker (bin-packing by row count).

    Run AFTER this to execute each shard (locally or in separate containers):
      python main.py build <csv_src> -o <output_dir> --plan build_plan.json --worker-id 0
      python main.py build <csv_src> -o <output_dir> --plan build_plan.json --worker-id 1
      ...
    """
    os.makedirs(output_dir, exist_ok=True)

    csv_src = os.path.abspath(csv_src)
    if os.path.isfile(csv_src):
        all_files = [csv_src]
        csv_base = os.path.dirname(csv_src)
    else:
        all_files = find_data_files(csv_src)
        csv_base = csv_src

    if not all_files:
        print(f"No data files found in {csv_src}")
        return None

    print(f"Counting rows in {len(all_files)} file(s)...")
    file_infos = []
    for path in sorted(all_files):
        rel = os.path.relpath(path, csv_base)
        row_count = _count_rows(path) or 0
        size_mb = os.path.getsize(path) / 1024**2
        file_infos.append({"rel": rel, "abs": path, "rows": row_count, "size_mb": round(size_mb, 1)})
        print(f"  {rel}: ~{row_count:,} rows  ({size_mb:.1f} MB)")

    # ── Single-file mode: split by byte ranges instead of file distribution ──
    # Avoids reading-and-discarding millions of rows to "skip" to a position.
    # Each worker seeks to its byte offset in O(1) via _ByteRangeReader.
    # Compressed files are not seekable — fall through to normal distribution.
    if len(all_files) == 1 and n_workers > 1 and not _compress_ext(all_files[0]):
        path  = all_files[0]
        rel   = file_infos[0]["rel"]
        total_rows = file_infos[0]["rows"]
        print(f"\nSingle file — splitting into {n_workers} byte-range chunks (O(1) seek)...")
        _, byte_ranges = _split_file_byte_ranges(path, n_workers)
        file_size = os.path.getsize(path)
        with open(path, "rb") as _hf:
            _hf.readline()
            data_start = _hf.tell()
        data_size = file_size - data_start

        # If _count_rows returned 0 (JSONL, JSON array, or any format without a
        # reliable newline count), fall back to a byte-size estimate.
        # Assume ~100 bytes per row on average — very conservative so we always
        # over-allocate rather than under-allocate doc_id space.  Getting this wrong
        # means workers share doc chunk files → silent data corruption (overwrites).
        if not total_rows:
            total_rows = max(1, data_size // 100)
            print(f"  Row count unknown — estimating ~{total_rows:,} rows from file size "
                  f"(100 bytes/row assumption). Actual may differ; 5% headroom added.")

        # Align each worker's doc_id start to a chunk boundary so workers write
        # to completely separate doc chunk files — eliminates concurrent-write races.
        base_id = _load_manifest(output_dir).get("next_doc_id", 0)
        # Round up to next chunk boundary so first worker's chunk is clean
        next_id = ((base_id + DOC_CHUNK_SIZE - 1) // DOC_CHUNK_SIZE) * DOC_CHUNK_SIZE
        assignments = []
        for w, (bstart, bend) in enumerate(byte_ranges):
            byte_frac = (bend - bstart) / max(data_size, 1)
            est_rows  = int(total_rows * byte_frac)
            headroom  = max(100_000, int(est_rows * 0.10))   # 10% headroom, min 100K
            doc_end   = next_id + est_rows + headroom - 1
            assignments.append({
                "worker_id":      w,
                "doc_id_start":   next_id,
                "doc_id_end":     doc_end,
                "estimated_rows": est_rows,
                "files":          [rel],
                "byte_start":     bstart,
                "byte_end":       bend,
            })
            # Advance to next chunk boundary past this worker's range
            next_id = ((doc_end + 1 + DOC_CHUNK_SIZE - 1) // DOC_CHUNK_SIZE) * DOC_CHUNK_SIZE

        plan_data = {
            "n_workers":  n_workers,
            "csv_base":   csv_base,
            "output_dir": output_dir,
            "delimiter":  delimiter,
            "columns":    columns,
            "single_file_split": True,
            "assignments": assignments,
        }
        # Validate: no overlapping doc_id ranges between workers.
        _validate_plan_ranges(assignments)

        plan_path = os.path.join(output_dir, "build_plan.json")
        with open(plan_path, "w") as f:
            json.dump(plan_data, f, indent=2)

        print(f"\nPlan written: {plan_path}")
        for a in assignments:
            mb = (a["byte_end"] - a["byte_start"]) / 1024**2
            range_size = a["doc_id_end"] - a["doc_id_start"] + 1
            print(f"  Worker {a['worker_id']}: bytes [{a['byte_start']:,}…{a['byte_end']:,}]"
                  f"  ({mb:.0f} MB)  ~{a['estimated_rows']:,} rows"
                  f"  doc_ids [{a['doc_id_start']:,}…{a['doc_id_end']:,}]  (range={range_size:,})")
        _print_parallel_run_hint(csv_src, output_dir, plan_path, n_workers)
        return plan_data

    # ── Multi-file mode: greedy bin-packing by row count ────────────────────
    worker_rows   = [0] * n_workers
    worker_files  = [[] for _ in range(n_workers)]

    for fi in sorted(file_infos, key=lambda x: -x["rows"]):   # largest first
        w = min(range(n_workers), key=lambda i: worker_rows[i])
        worker_files[w].append(fi)
        worker_rows[w] += fi["rows"]

    # Assign non-overlapping doc_id ranges aligned to chunk boundaries.
    # Chunk-alignment ensures each worker writes to completely separate doc chunk
    # files — eliminates concurrent-write races on shared zlib files.
    assignments = []
    base_id = _load_manifest(output_dir).get("next_doc_id", 0)
    next_id = ((base_id + DOC_CHUNK_SIZE - 1) // DOC_CHUNK_SIZE) * DOC_CHUNK_SIZE
    for w in range(n_workers):
        files  = worker_files[w]
        n_rows = worker_rows[w]
        # Add 10% headroom for rows that were under-counted (embedded newlines etc.)
        # Minimum 100K to guard against zero row-count estimates on unknown formats.
        headroom  = max(100_000, int(n_rows * 0.10))
        doc_start = next_id
        doc_end   = next_id + n_rows + headroom - 1
        assignments.append({
            "worker_id":   w,
            "doc_id_start": doc_start,
            "doc_id_end":   doc_end,
            "estimated_rows": n_rows,
            "files": [f["rel"] for f in files],
        })
        next_id = ((doc_end + 1 + DOC_CHUNK_SIZE - 1) // DOC_CHUNK_SIZE) * DOC_CHUNK_SIZE

    plan_data = {
        "n_workers": n_workers,
        "csv_base":  csv_base,
        "output_dir": output_dir,
        "delimiter": delimiter,
        "columns": columns,
        "assignments": assignments,
    }

    # Validate: no overlapping doc_id ranges between workers.
    # Overlaps would cause concurrent writes to the same doc chunk files → corruption.
    _validate_plan_ranges(assignments)

    plan_path = os.path.join(output_dir, "build_plan.json")
    with open(plan_path, "w") as f:
        json.dump(plan_data, f, indent=2)

    print(f"\nPlan written: {plan_path}")
    total_rows = sum(worker_rows)
    for a in assignments:
        range_size = a["doc_id_end"] - a["doc_id_start"] + 1
        print(f"  Worker {a['worker_id']}: {len(a['files'])} file(s)"
              f"  ~{a['estimated_rows']:,} rows"
              f"  doc_ids [{a['doc_id_start']:,} … {a['doc_id_end']:,}]  (range={range_size:,})")
    print(f"\nTotal: ~{total_rows:,} rows across {n_workers} worker(s)")
    _print_parallel_run_hint(csv_src, output_dir, plan_path, n_workers)
    return plan_data


def _validate_plan_ranges(assignments):
    """Raise ValueError if any two workers have overlapping doc_id ranges.

    Overlapping ranges cause multiple workers to write to the same doc chunk
    files concurrently, producing silent data corruption (last writer wins).
    """
    ranges = sorted(
        (a["doc_id_start"], a["doc_id_end"], a["worker_id"])
        for a in assignments
    )
    for i in range(len(ranges) - 1):
        s0, e0, w0 = ranges[i]
        s1, e1, w1 = ranges[i + 1]
        if e0 >= s1:
            raise ValueError(
                f"BUG: doc_id ranges overlap between worker {w0} and worker {w1}: "
                f"worker {w0} ends at {e0:,}, worker {w1} starts at {s1:,}.\n"
                f"This would cause concurrent writes to the same doc chunk files.\n"
                f"Delete build_plan.json and stats_w*.json, then re-run 'plan'."
            )


def _print_parallel_run_hint(csv_src, output_dir, plan_path, n_workers):
    print(f"\nRun each worker (or use 'build --workers {n_workers}' to auto-spawn):")
    for w in range(n_workers):
        print(f"  python main.py build {csv_src} -o {output_dir}"
              f" --plan {plan_path} --worker-id {w}")


def _partial_merge_stats(output_dir, n_workers, rows_so_far):
    """Write a partial stats.json from whatever worker shard files exist so far.

    called periodically during the polling loop so `flatseek stats` returns meaningful
    data even during a long multi-hour parallel build.
    Marks the result with _partial=True; merge_worker_stats() overwrites it with
    the authoritative final version when all workers complete.
    """
    merged_cols   = {}
    total_entries = 0
    idx_mb = 0.0
    doc_mb = 0.0
    for w in range(n_workers):
        wpath = os.path.join(output_dir, f"stats_w{w}.json")
        try:
            with open(wpath) as f:
                ws = json.load(f)
            total_entries += ws.get("total_entries", 0)
            idx_mb        += ws.get("index_size_mb", 0)
            doc_mb        += ws.get("docs_size_mb", 0)
            merged_cols.update(ws.get("columns", {}))
        except Exception:
            pass   # worker hasn't written yet — skip silently

    stats_path = os.path.join(output_dir, "stats.json")
    # Preserve columns from any previous completed build
    if os.path.exists(stats_path):
        try:
            with open(stats_path) as f:
                old = json.load(f)
            if not old.get("_partial"):
                merged_cols = {**old.get("columns", {}), **merged_cols}
        except Exception:
            pass

    partial = {
        "_partial":      True,
        "total_docs":    rows_so_far,
        "rows_indexed":  rows_so_far,
        "total_entries": total_entries,
        "index_size_mb": round(idx_mb, 1),
        "docs_size_mb":  round(doc_mb, 1),
        "total_size_mb": round(idx_mb + doc_mb, 1),
        "columns":       merged_cols,
    }
    try:
        with open(stats_path, "w") as f:
            json.dump(partial, f, indent=2)
    except Exception:
        pass


def merge_worker_stats(output_dir, n_workers):
    """Merge stats_w*.json from parallel workers into the final stats.json.

    Called by the orchestrator after all workers have completed.  Reads each
    worker's per-worker stats file, sums doc counts / entries, merges column
    maps, then writes a single unified stats.json and cleans up the shard files.
    """
    rows_indexed  = 0   # actual rows processed across all workers
    total_entries = 0
    merged_cols   = {}
    max_doc_id    = 0   # highest doc_counter seen (for display)

    # Walk the index + docs dirs to get authoritative on-disk sizes
    idx_size = idx_files = 0
    index_dir = os.path.join(output_dir, "index")
    for root, _, files in os.walk(index_dir):
        for f in files:
            if f.endswith(".bin"):
                idx_size += os.path.getsize(os.path.join(root, f))
                idx_files += 1

    doc_size = 0
    docs_dir = os.path.join(output_dir, "docs")
    for root, _, files in os.walk(docs_dir):
        for f in files:
            if f.endswith(".zlib"):
                doc_size += os.path.getsize(os.path.join(root, f))

    for w in range(n_workers):
        wpath = os.path.join(output_dir, f"stats_w{w}.json")
        if not os.path.exists(wpath):
            print(f"  WARNING: stats_w{w}.json missing — worker {w} may have failed")
            continue
        with open(wpath) as f:
            ws = json.load(f)
        # rows_indexed = actual rows per worker (not doc_counter which starts at a large offset)
        rows_indexed  += ws.get("rows_indexed", ws.get("total_docs", 0))
        total_entries += ws.get("total_entries", 0)
        max_doc_id     = max(max_doc_id, ws.get("total_docs", 0))
        merged_cols.update(ws.get("columns", {}))

    # Merge with any pre-existing stats (from non-parallel previous builds)
    stats_path = os.path.join(output_dir, "stats.json")
    if os.path.exists(stats_path):
        with open(stats_path) as f:
            old = json.load(f)
        merged_cols = {**old.get("columns", {}), **merged_cols}
        # If there's a finalized (non-partial) previous build, add its counts
        if not old.get("_partial"):
            rows_indexed  += old.get("rows_indexed", old.get("total_docs", 0))
            total_entries += old.get("total_entries", 0)

    stats = {
        "total_docs":    rows_indexed,   # actual rows (used for "X docs in index")
        "rows_indexed":  rows_indexed,
        "total_entries": total_entries,
        "doc_chunk_size": DOC_CHUNK_SIZE,
        "index_files":   idx_files,
        "index_size_mb": round(idx_size / 1024**2, 1),
        "docs_size_mb":  round(doc_size / 1024**2, 1),
        "total_size_mb": round((idx_size + doc_size) / 1024**2, 1),
        "columns":       merged_cols,
    }

    with open(stats_path, "w") as f:
        json.dump(stats, f, indent=2)

    # Clean up per-worker stats files
    for w in range(n_workers):
        wpath = os.path.join(output_dir, f"stats_w{w}.json")
        if os.path.exists(wpath):
            os.remove(wpath)

    print(f"  Merged stats: {rows_indexed:,} docs, {total_entries:,} entries, "
          f"{stats['total_size_mb']:.1f} MB")
    return stats


def _load_manifest(output_dir):
    path = os.path.join(output_dir, "manifest.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {"files": {}, "next_doc_id": 0}


def _save_manifest(manifest, output_dir):
    with open(os.path.join(output_dir, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)


def _file_fingerprint(path):
    s = os.stat(path)
    return s.st_mtime, s.st_size


def _is_unchanged(path, entry):
    mtime, size = _file_fingerprint(path)
    return mtime == entry.get("mtime") and size == entry.get("size_bytes")


def _auto_classify(files, csv_base, col_map_file, delimiter=",", columns=None, type_overrides=None):
    """Classify new files only (from an explicit file list), merge with existing column_map.json."""
    from flatseek.core.classify import classify_file

    existing = {}
    if os.path.exists(col_map_file):
        with open(col_map_file) as f:
            existing = json.load(f)

    new_classified = 0
    for path in files:
        rel = os.path.relpath(path, csv_base)
        if rel not in existing:
            result = classify_file(path, delimiter=delimiter,
                                   columns=columns, type_overrides=type_overrides)
            existing[rel] = result
            new_classified += 1
            print(f"  Classify {rel}:")
            for col, info in result.items():
                if col.startswith("_"):   # skip _headerless, _columns, _delimiter metadata
                    continue
                flag = "✓" if info["confidence"] >= 0.8 else "?"
                print(f"    {col!r:35s} → {info['semantic_type']:12s} ({info['confidence']:.0%}) {flag}")

    if new_classified:
        os.makedirs(os.path.dirname(os.path.abspath(col_map_file)), exist_ok=True)
        with open(col_map_file, "w") as f:
            json.dump(existing, f, indent=2, ensure_ascii=False)
        print(f"  Saved column map: {col_map_file}\n")
    else:
        print(f"  Column map up to date ({len(existing)} file(s) already classified)")

    return existing


def build(csv_src, output_dir, column_map_path=None, dataset=None, delimiter=",",
          columns=None, type_overrides=None, worker_id=None, plan_path=None,
          estimate=False, dedup_fields=None, daemon=False):
    """Build index from csv_src, which may be a single file or a directory.

    Distributed / parallel mode:
      Pass worker_id (int) and plan_path (path to build_plan.json generated by plan()).
      Each worker writes its index shard to idx_w{worker_id}.bin files; the query
      engine reads all *.bin files so no merge step is needed.
    """
    os.makedirs(output_dir, exist_ok=True)
    t0 = time.time()

    # ── Distributed mode: read assignment from plan ───────────────────────────
    byte_range_override = None   # (byte_start, byte_end) for single-file splits
    if plan_path is not None and worker_id is not None:
        with open(plan_path) as f:
            plan_data = json.load(f)
        assignment = next(
            (a for a in plan_data["assignments"] if a["worker_id"] == worker_id), None
        )
        if assignment is None:
            print(f"Error: worker_id={worker_id} not found in plan {plan_path}")
            return
        csv_base  = plan_data["csv_base"]
        all_files = [os.path.join(csv_base, rel) for rel in assignment["files"]]
        start_doc_id_override = assignment["doc_id_start"]
        doc_id_end_override   = assignment.get("doc_id_end")
        delimiter  = plan_data.get("delimiter", delimiter)
        columns    = plan_data.get("columns") or columns
        if "byte_start" in assignment:
            byte_range_override = (assignment["byte_start"], assignment["byte_end"])
        est_rows = assignment.get("estimated_rows")
        est_rows_int = est_rows if isinstance(est_rows, int) else None
        est_rows_str = f"~{est_rows:,}" if isinstance(est_rows, int) else "?"
        doc_range_size = (doc_id_end_override - start_doc_id_override + 1) if doc_id_end_override else 0
        print(f"Worker {worker_id}: {len(all_files)} file(s)"
              f"  {est_rows_str} rows  doc_ids [{start_doc_id_override:,}…{doc_id_end_override:,}]  (range={doc_range_size:,})"
              + (f"  bytes [{byte_range_override[0]:,}…{byte_range_override[1]:,}]"
                 if byte_range_override else ""))

        # Per-worker checkpoint: written at each periodic flush so a restart can
        # skip already-indexed rows instead of re-processing from scratch.
        _wckpt_path = os.path.join(output_dir, f"checkpoint_w{worker_id}.json")
        _wckpt: dict = {}
        if os.path.isfile(_wckpt_path):
            try:
                with open(_wckpt_path) as _cf:
                    _wckpt = json.load(_cf)
            except Exception:
                _wckpt = {}

        # On restart (partial stats file detected): use checkpoint to resume if available,
        # otherwise wipe the stale idx shard to avoid duplicate index entries.
        _stats_w = os.path.join(output_dir, f"stats_w{worker_id}.json")
        if os.path.isfile(_stats_w):
            try:
                with open(_stats_w) as _sf:
                    _is_partial = json.load(_sf).get("_partial", False)
            except Exception:
                _is_partial = False
            if _is_partial:
                if _wckpt:
                    # Resume: advance start_doc_id to the checkpoint value.
                    _ckpt_doc = _wckpt.get("doc_id_next")
                    if _ckpt_doc is not None:
                        start_doc_id_override = _ckpt_doc
                    # For byte-range workers: seek directly to the saved byte offset
                    # instead of scanning skip_rows rows.  byte_offset = start of
                    # the next unread chunk at checkpoint time (O(1) fseek on resume).
                    _ckpt_byte = _wckpt.get("byte_offset")
                    if _ckpt_byte is not None and byte_range_override is not None:
                        byte_range_override = (_ckpt_byte, byte_range_override[1])
                    print(f"Worker {worker_id}: restart — resuming from checkpoint "
                          f"row {_wckpt.get('rows_done', 0):,}, doc #{start_doc_id_override:,}"
                          + (f", byte {_ckpt_byte:,}" if _ckpt_byte else ""))
                else:
                    # No checkpoint: must re-index from scratch; wipe stale shard first.
                    _shard_name = f"idx_w{worker_id}.bin"
                    _idx_dir    = os.path.join(output_dir, "index")
                    _n_deleted  = 0
                    for _root, _dirs, _files in os.walk(_idx_dir):
                        for _fname in _files:
                            if _fname == _shard_name:
                                os.remove(os.path.join(_root, _fname))
                                _n_deleted += 1
                    print(f"Worker {worker_id}: restart — no checkpoint, cleared {_n_deleted} stale shard file(s)")
    else:
        _wckpt_path = None
        _wckpt = {}

        est_rows_int = None
        start_doc_id_override = None
        doc_id_end_override   = None
        # Resolve input: single file or directory
        csv_src = os.path.abspath(csv_src)
        if os.path.isfile(csv_src):
            all_files = [csv_src]
            csv_base = os.path.dirname(csv_src)
        elif os.path.isdir(csv_src):
            all_files = find_data_files(csv_src)
            csv_base  = csv_src
            # Multi-index: build each file into its own sub-directory so the
            # query engine can discover and query them independently (or together).
            for path in all_files:
                stem = os.path.splitext(os.path.basename(path))[0]
                sub_output = os.path.join(output_dir, stem)
                print(f"\n{'─'*55}")
                print(f"  Sub-index [{stem}]  →  {sub_output}")
                print(f"{'─'*55}")
                build(path, sub_output,
                      column_map_path=None,   # each sub-index gets its own col_map
                      dataset=dataset,
                      delimiter=delimiter,
                      columns=columns,
                      type_overrides=type_overrides,
                      worker_id=None,
                      plan_path=None,
                      estimate=estimate)
            return
        else:
            print(f"Error: {csv_src!r} is not a file or directory")
            return

    if not all_files:
        print(f"No data files found in {csv_src}")
        return

    # 1. Auto-classify new files
    col_map_file = column_map_path or os.path.join(output_dir, "column_map.json")
    print("── Classify ──────────────────────────────────────────")
    column_map = _auto_classify(all_files, csv_base, col_map_file, delimiter=delimiter,
                                columns=columns, type_overrides=type_overrides)

    # In worker mode (plan_path set): skip manifest-based checkpoint resume.
    # Workers run concurrently and would race on manifest.json; their doc_id
    # ranges come from the plan, not the checkpoint.  Manifest writes are also
    # suppressed for workers — the orchestrator handles post-build bookkeeping.
    _is_worker = (plan_path is not None and worker_id is not None)

    # 2. Load manifest — tracks what's already indexed + any in-progress checkpoint
    manifest = _load_manifest(output_dir)
    already = manifest["files"]

    # Detect mid-file checkpoint from a previous interrupted run.
    # Skip in worker mode — workers use plan-assigned doc_id ranges, not the manifest.
    resume_rel    = None
    resume_rows   = 0
    resume_doc_id = None
    if not _is_worker:
        ckpt = manifest.get("checkpoint") or {}
        resume_rel    = ckpt.get("file_rel")
        resume_rows   = ckpt.get("rows_done", 0)
        resume_doc_id = ckpt.get("doc_id_next")

    # 3. Separate new/changed from unchanged
    to_index, skipped = [], []
    for path in all_files:
        rel = os.path.relpath(path, csv_base)
        if rel in already and _is_unchanged(path, already[rel]):
            skipped.append(rel)
        else:
            to_index.append(path)

    # Workers resuming from a checkpoint: skip files already completed in the
    # previous interrupted run (manifest doesn't track per-worker completions).
    if _is_worker and _wckpt.get("files_done"):
        _done_rels = set(_wckpt["files_done"])
        to_index = [p for p in to_index if os.path.relpath(p, csv_base) not in _done_rels]

    print("── Files ─────────────────────────────────────────────")
    if skipped:
        print(f"  Already indexed ({len(skipped)} file(s), skipping):")
        for r in skipped:
            rows = already[r].get("rows", "?")
            print(f"    {r}  ({rows:,} rows)" if isinstance(rows, int) else f"    {r}  ({rows} rows)")

    if not to_index:
        print("\nNothing to index — all files are up to date.")
        # Stale checkpoint with no work left — clean it up
        if not _is_worker and ckpt:
            manifest.pop("checkpoint", None)
            _save_manifest(manifest, output_dir)
        return

    print(f"\n  To index: {len(to_index)} file(s)")

    # 4. Determine starting doc_id
    # Priority: plan override > checkpoint > manifest
    start_doc_id = start_doc_id_override if start_doc_id_override is not None \
                   else manifest["next_doc_id"]
    if not _is_worker and resume_rel and resume_doc_id is not None:
        # Check that the resume target is actually in to_index (file unchanged since interrupt)
        resume_paths = {os.path.relpath(p, csv_base): p for p in to_index}
        if resume_rel in resume_paths:
            start_doc_id = resume_doc_id
            print(f"  Resuming interrupted build: {resume_rel} at row {resume_rows:,}"
                  f"  (doc #{start_doc_id:,})")
        else:
            # File was completed or removed since the interrupt — discard stale checkpoint
            resume_rel = None
            resume_rows = 0

    if dataset:
        print(f"  Dataset label: {dataset!r}")

    if dedup_fields is not None:
        label = ", ".join(dedup_fields) if dedup_fields else "all fields"
        print(f"  Dedup (build-time): enabled  ({label})")

    print("── Indexing ──────────────────────────────────────────")
    builder = IndexBuilder(output_dir, column_map, start_doc_id=start_doc_id, dataset=dataset,
                          delimiter=delimiter, columns=columns, worker_id=worker_id,
                          dedup_fields=dedup_fields, doc_id_end=doc_id_end_override,
                          daemon=daemon)
    builder._estimate_enabled = estimate and not _is_worker
    total_new = 0

    # Disable Python's automatic GC during the indexing loop.
    # The GC normally runs unpredictably whenever generation thresholds are hit.
    # At millions of rows this can cause surprise full (gen2) pauses of several
    # seconds that get worse as the heap grows — manifesting as the visible
    # throughput drop from 10k → 6k → 2k rows/s.
    # gc.collect() runs explicitly in the main thread after each checkpoint join()
    # so collections happen at a well-defined safe point (while the main thread is
    # already paused waiting for the bg I/O thread) with zero extra disruption.
    gc.disable()

    for path in to_index:
        rel = os.path.relpath(path, csv_base)
        doc_start = builder.doc_counter

        # Non-worker: resume from manifest checkpoint (mid-file resume)
        skip_rows = resume_rows if rel == resume_rel else 0

        # Worker: resume from per-worker checkpoint if this file is the current one.
        # If a byte_offset was saved, byte_range_override is already updated to seek
        # directly there (O(1)) — skip_rows covers only the partial last chunk
        # (rows between rows_at_byte_off and rows_done, at most chunksize rows).
        if _is_worker and _wckpt and _wckpt.get("file_rel") == rel:
            _ckpt_byte = _wckpt.get("byte_offset")
            if _ckpt_byte is not None and byte_range_override is not None:
                # byte_range already updated to start at _ckpt_byte; re-index only
                # the partial last chunk (rows between chunk boundary and checkpoint).
                skip_rows = _wckpt.get("rows_done", 0) - _wckpt.get("rows_at_byte_off", 0)
                skip_rows = max(0, skip_rows)
            else:
                skip_rows = _wckpt.get("rows_done", 0)
            if skip_rows:
                print(f"Worker {worker_id}: skipping {skip_rows:,} rows (partial chunk) in {rel}")
            elif _ckpt_byte:
                print(f"Worker {worker_id}: resuming at byte {_ckpt_byte:,} in {rel} (no row scan needed)")

        if not _is_worker:
            # Write checkpoint before starting this file so we can resume if interrupted
            manifest["checkpoint"] = {
                "file_rel": rel,
                "rows_done": skip_rows,
                "doc_id_next": builder.doc_counter,
            }
            _save_manifest(manifest, output_dir)

        # Callback: update checkpoint after each periodic flush.
        def _on_checkpoint(rows_done, doc_id_next, _rel=rel):
            manifest["checkpoint"] = {
                "file_rel": _rel,
                "rows_done": rows_done,
                "doc_id_next": doc_id_next,
            }
            manifest["next_doc_id"] = doc_id_next
            _save_manifest(manifest, output_dir)

        def _worker_on_checkpoint(rows_done, doc_id_next, _rel=rel,
                                  _wcp=_wckpt_path, _files_done=list(_wckpt.get("files_done", []))):
            # Snapshot current byte position (set by process_file after each chunk).
            # Saves _last_safe_byte_pos = start of the NEXT unread chunk, so resume
            # can O(1)-seek directly there instead of scanning skip_rows rows.
            _byte_off  = builder._last_safe_byte_pos
            _rows_byte = builder._rows_at_safe_pos
            entry = {
                "files_done":     _files_done,
                "file_rel":       _rel,
                "rows_done":      rows_done,
                "doc_id_next":    doc_id_next,
            }
            if _byte_off is not None:
                entry["byte_offset"]      = _byte_off
                entry["rows_at_byte_off"] = _rows_byte
            with open(_wcp, "w") as _cf:
                json.dump(entry, _cf)

        if _is_worker:
            builder._checkpoint_cb = _worker_on_checkpoint
        else:
            builder._checkpoint_cb = _on_checkpoint

        rows = builder.process_file(path, csv_base, skip_rows=skip_rows,
                                    byte_range=byte_range_override,
                                    estimated_total=est_rows_int if _is_worker else None)
        total_new += rows

        # File complete: record in manifest and remove checkpoint
        stat = os.stat(path)
        entry = {
            "mtime": stat.st_mtime,
            "size_bytes": stat.st_size,
            "rows": skip_rows + rows,    # total rows in file (including previously skipped)
            "doc_start": doc_start,
            "doc_end": builder.doc_counter - 1,
            "indexed_at": datetime.now().isoformat(timespec="seconds"),
        }
        if dataset:
            entry["dataset"] = dataset
        already[rel] = entry
        manifest["next_doc_id"] = builder.doc_counter
        manifest.pop("checkpoint", None)  # clear — file is fully done
        if not _is_worker:
            _save_manifest(manifest, output_dir)

        # Worker: mark this file done in the checkpoint for next-file skipping
        if _is_worker and _wckpt_path:
            _wckpt.setdefault("files_done", [])
            if rel not in _wckpt["files_done"]:
                _wckpt["files_done"].append(rel)
            _wckpt.pop("file_rel", None)
            _wckpt.pop("rows_done", None)
            with open(_wckpt_path, "w") as _cf:
                json.dump(_wckpt, _cf)

        # Only the first file in to_index can be a resume target (non-worker)
        resume_rel = None
        resume_rows = 0

    print("── Finalizing ────────────────────────────────────────")
    gc.enable()
    gc.collect()   # one full collection now that the hot loop is done
    builder.finalize()
    # Worker done: remove per-worker checkpoint — stats_w{N}.json (non-partial) is
    # the authoritative completion marker; checkpoint_w{N}.json is no longer needed.
    if _is_worker and _wckpt_path and os.path.isfile(_wckpt_path):
        os.remove(_wckpt_path)
    dedup_msg = (f"  ({builder._dedup_skipped:,} duplicate rows skipped)"
                 if builder._dedup_skipped else "")
    print(f"\nDone: {total_new:,} new rows in {_fmt_duration(time.time() - t0)}"
          f"  (total index: {builder.doc_counter:,} docs){dedup_msg}")
