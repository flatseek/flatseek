"""Query engine for the trigram inverted index.

Features:
  - Exact match:   search("raydium", column="program")
  - Wildcard:      search("%garuda%", column="callsign")  or  search("smi*")
  - AND:           search_and([("program", "raydium"), ("status", "success")])
  - Pagination:    page=0, page_size=20  — O(1), no rescan

Wildcard rules:
  %term% or *term*  → trigram lookup + intersect
  term%  or term*   → trigram lookup (trailing ok)
  %term  or *term   → trigram lookup (leading — same cost as middle)
  no wildcard       → exact token lookup

Doc chunks are loaded lazily (only the chunks containing requested doc_ids).
"""

import glob as _glob
import logging
import os
import re
import json
import struct
import zlib
from collections import defaultdict
from datetime import date as _date

try:
    import orjson as _orjson
    def _doc_loads(data: bytes) -> dict:
        return _orjson.loads(data)
except ImportError:
    def _doc_loads(data: bytes) -> dict:
        return json.loads(data)

_ZSTD_MAGIC = b'\x28\xb5\x2f\xfd'

import threading as _threading
_zstd_tls = _threading.local()  # thread-local — ZstdDecompressor is NOT thread-safe
try:
    import zstandard as _zstd
    _HAS_ZSTD = True
except ImportError:
    _HAS_ZSTD = False


def _decompress_doc(data: bytes) -> bytes:
    if data[:4] == _ZSTD_MAGIC:
        if not _HAS_ZSTD:
            raise RuntimeError(
                "Doc store was compressed with zstd but zstandard is not installed. "
                "Run: pip install zstandard"
            )
        if not hasattr(_zstd_tls, "dctx"):
            _zstd_tls.dctx = _zstd.ZstdDecompressor()
        return _zstd_tls.dctx.decompress(data)
    return zlib.decompress(data)


# ─── Encryption helpers ────────────────────────────────────────────────────────
#
# Format: b'FLATSEEK\x01' (9 B magic) + nonce (12 B) + ciphertext + auth_tag (16 B)
# Algorithm: ChaCha20-Poly1305 — fast on all hardware, no AES-NI dependency.
# Key derivation: PBKDF2-HMAC-SHA256, 600k iterations, 32-byte key.
# Salt (32 B) is stored in <data_dir>/encryption.json alongside the key id.
#
# Ordering convention: compress first, then encrypt.
# On read: decrypt first (detect magic), then decompress (detect zlib/zstd/raw).

_ENC_MAGIC    = b'FLATSEEK\x01'   # 9 bytes — unique to flatseek; cannot collide with zlib/zstd magic
_ENC_NONCE_LEN = 12
_ENC_HEADER_LEN = len(_ENC_MAGIC) + _ENC_NONCE_LEN   # 21 bytes


def _require_cryptography():
    try:
        from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
        from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
        from cryptography.hazmat.primitives import hashes
        return ChaCha20Poly1305, PBKDF2HMAC, hashes
    except ImportError:
        raise RuntimeError(
            "Encryption requires the 'cryptography' package.\n"
            "Install it with: pip install cryptography"
        )


def derive_key(passphrase: str, salt: bytes) -> bytes:
    """Derive a 32-byte ChaCha20 key from a passphrase using PBKDF2-HMAC-SHA256."""
    _, PBKDF2HMAC, hashes = _require_cryptography()
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=600_000,
    )
    return kdf.derive(passphrase.encode("utf-8"))


def encrypt_bytes(data: bytes, key: bytes) -> bytes:
    """Encrypt data with ChaCha20-Poly1305. Returns magic+nonce+ciphertext+tag."""
    import os as _os
    ChaCha20Poly1305, _, __ = _require_cryptography()
    nonce = _os.urandom(_ENC_NONCE_LEN)
    ct = ChaCha20Poly1305(key).encrypt(nonce, data, None)
    return _ENC_MAGIC + nonce + ct


def decrypt_bytes(data: bytes, key: bytes) -> bytes:
    """Decrypt a flatseek-encrypted blob. Raises ValueError on wrong key / tampered data."""
    ChaCha20Poly1305, _, __ = _require_cryptography()
    if not data.startswith(_ENC_MAGIC):
        raise ValueError("Not a flatseek-encrypted blob (missing magic header)")
    nonce = data[len(_ENC_MAGIC): _ENC_HEADER_LEN]
    ct    = data[_ENC_HEADER_LEN:]
    try:
        return ChaCha20Poly1305(key).decrypt(nonce, ct, None)
    except Exception:
        raise ValueError("Decryption failed — wrong key or corrupted data")


def is_encrypted(data: bytes) -> bool:
    return data[:len(_ENC_MAGIC)] == _ENC_MAGIC


def load_encryption_key(index_dir: str, passphrase: str) -> bytes:
    """Load salt from encryption.json (inside index_dir) and derive key from passphrase.

    Raises FileNotFoundError if encryption.json is missing (index not encrypted).
    """
    enc_path = os.path.join(index_dir, "encryption.json")
    if not os.path.isfile(enc_path):
        raise FileNotFoundError(
            f"No encryption.json found in {index_dir}. "
            "Index is not encrypted, or the file was deleted."
        )
    with open(enc_path) as f:
        meta = json.load(f)
    salt = bytes.fromhex(meta["salt"])
    return derive_key(passphrase, salt)


# ─── Decoding ─────────────────────────────────────────────────────────────────

def decode_doclist(data):
    """Decode delta-encoded varint doc_ids."""
    if not data:
        return []
    ids, i, prev = [], 0, 0
    while i < len(data):
        val = shift = 0
        while i < len(data):
            b = data[i]; i += 1
            val |= (b & 0x7F) << shift
            if not (b & 0x80):
                break
            shift += 7
        prev += val
        ids.append(prev)
    return ids


_PREFIX_CACHE: dict = {}
_POSTING_CACHE_MAX = 8192   # max terms cached per QueryEngine instance

def term_hash(term):
    """Must match builder.term_hash exactly — determines which bucket file to read."""
    h = zlib.crc32(term.encode()) & 0xFFFFFFFF
    k = h >> 16
    s = _PREFIX_CACHE.get(k)
    if s is None:
        _PREFIX_CACHE[k] = s = f"{(h >> 24) & 0xFF:02x}/{(h >> 16) & 0xFF:02x}"
    return s


# ─── Index discovery ──────────────────────────────────────────────────────────

def _discover_index_dirs(root):
    """Return list of index directories at or under root.

    Rules:
    - root itself has index/ → [root]   (single index, current behaviour)
    - root has subdirs each with index/ → sorted list of those subdirs (multi-index)
    - Nothing found → []
    """
    root = os.path.abspath(root)
    if os.path.isdir(os.path.join(root, "index")):
        return [root]
    subs = []
    try:
        for name in sorted(os.listdir(root)):
            sub = os.path.join(root, name)
            if os.path.isdir(sub) and os.path.isdir(os.path.join(sub, "index")):
                subs.append(sub)
    except Exception:
        pass
    return subs


# ─── Helpers ──────────────────────────────────────────────────────────────────

# Module-level: array-index suffix `tags[0]`. Compiled once.
_ARRAY_IDX_RE = re.compile(r"^(.+?)\[(\d+)\]$")

# Module-level cache of pre-parsed field paths.  Keyed by the flat dot-path,
# value is either:
#   • the literal `_PLAIN`  — `field` is a top-level key, no walking needed
#   • a tuple of segments    — `(("profile", None), ("location", None), ("city", None))`
#                              `(("info", None), ("metadata", None), ("a", None), ("tags", 0))`
# Aggregation and range/wildcard hot paths consult this cache instead of
# re-splitting + re-regex'ing on every doc.
_PLAIN = object()
_PATH_CACHE: dict = {}


def _parse_field_path(field):
    """Pre-parse a dot-path key into a tuple of (key, array_idx_or_None) segs.

    Returns `_PLAIN` for a plain top-level key, otherwise the segment tuple.
    """
    cached = _PATH_CACHE.get(field, None)
    if cached is not None:
        return cached
    if not field or ("." not in field and "[" not in field):
        _PATH_CACHE[field] = _PLAIN
        return _PLAIN
    out = []
    for part in field.split("."):
        m = _ARRAY_IDX_RE.match(part)
        if m:
            out.append((m.group(1), int(m.group(2))))
        else:
            out.append((part, None))
    parsed = tuple(out)
    _PATH_CACHE[field] = parsed
    return parsed


def _walk_path(doc, parsed):
    """Walk a pre-parsed segment tuple through a (possibly stringified-JSON) doc.

    Optimized: avoids json.loads for strings that clearly aren't JSON objects/arrays.
    """
    cur = doc
    for key, idx in parsed:
        if isinstance(cur, dict):
            cur = cur.get(key)
        elif isinstance(cur, str):
            # Fast path: only attempt JSON parse for strings that look like JSON.
            # This skips ~90% of intermediate string values (plain text, numbers, etc.)
            if not cur or cur[0] not in ('{', '['):
                return None
            try:
                obj = json.loads(cur.replace("'", '"'))
                cur = obj.get(key) if isinstance(obj, dict) else None
            except Exception:
                return None
        else:
            return None
        if cur is None:
            return None
        if idx is not None:
            if isinstance(cur, list):
                cur = cur[idx] if 0 <= idx < len(cur) else None
            elif isinstance(cur, str):
                if not cur or cur[0] not in ('{', '['):
                    return None
                try:
                    arr = json.loads(cur.replace("'", '"'))
                    cur = arr[idx] if (isinstance(arr, list) and 0 <= idx < len(arr)) else None
                except Exception:
                    return None
            else:
                return None
            if cur is None:
                return None
    return cur


def _get_nested_value(doc, field):
    """Read a (possibly dot-pathed, array-indexed) field from a doc.

    The doc store keeps fields in their collapsed/nested form (see
    `_collapse_record` in builder).  Query-time keys are flat dot-paths
    (e.g. `info.metadata.a.value`) because the trigram index is keyed that way.
    This walks the doc using the flat-path key so range queries and
    aggregations can find scalars inside nested objects without re-flattening
    the doc store.

    Pre-parsing is cached in `_PATH_CACHE`; per-call overhead for repeated
    field names is one dict lookup.
    """
    if doc is None or not field:
        return None
    # Fast path — direct stored key (top-level column, array container).
    direct = doc.get(field)
    if direct is not None:
        return direct
    parsed = _parse_field_path(field)
    if parsed is _PLAIN:
        return None
    return _walk_path(doc, parsed)


# ─── QueryEngine ──────────────────────────────────────────────────────────────

class QueryEngine:
    def __init__(self, data_dir):
        self.data_dir = os.path.abspath(data_dir)

        dirs = _discover_index_dirs(self.data_dir)
        if not dirs:
            raise FileNotFoundError(
                f"No index found in {data_dir}. Run: flatseek build <csv_dir>")

        if len(dirs) == 1 and dirs[0] == self.data_dir:
            # ── Single-index mode (unchanged behaviour) ───────────────────────
            self._sub_engines = None
            self.index_dir = os.path.join(self.data_dir, "index")
            self.docs_dir  = os.path.join(self.data_dir, "docs")
            stats_path = os.path.join(self.data_dir, "stats.json")
            if not os.path.exists(stats_path):
                raise FileNotFoundError(
                    f"No index found in {data_dir}. Run: flatseek build <csv_dir>")
            with open(stats_path) as f:
                self.stats = json.load(f)
        else:
            # ── Multi-index mode: root contains per-file sub-indexes ──────────
            self._sub_engines = [QueryEngine(d) for d in dirs]
            self.index_dir = None
            self.docs_dir  = None
            # Merge stats for summary display
            merged_cols: dict = {}
            total_docs = total_entries = 0
            idx_mb = doc_mb = 0.0
            for eng in self._sub_engines:
                s = eng.stats
                total_docs    += s.get("total_docs", 0)
                total_entries += s.get("total_entries", 0)
                idx_mb        += s.get("index_size_mb", 0)
                doc_mb        += s.get("docs_size_mb", 0)
                merged_cols.update(s.get("columns", {}))
            self.stats = {
                "total_docs":    total_docs,
                "total_entries": total_entries,
                "index_size_mb": round(idx_mb, 1),
                "docs_size_mb":  round(doc_mb, 1),
                "total_size_mb": round(idx_mb + doc_mb, 1),
                "columns":       merged_cols,
            }

        self.doc_chunk_size = self.stats.get("doc_chunk_size", 100_000)
        self._doc_cache = {}      # chunk_start → {doc_id: row_dict}
        self._posting_cache = {}  # term → sorted doc_id list (bounded to _POSTING_CACHE_MAX)
        self._enc_key: bytes | None = None   # set via set_key() before querying encrypted indexes

    def reload_stats(self):
        """Re-read stats.json from disk to pick up changes after a flush."""
        import json
        stats_path = os.path.join(self.data_dir, "stats.json")
        if os.path.exists(stats_path):
            with open(stats_path) as f:
                self.stats = json.load(f)

    def set_key(self, key: bytes):
        """Supply the decryption key for an encrypted index.

        Must be called before the first search on an encrypted index.
        For multi-index mode, propagates to all sub-engines.
        """
        self._enc_key = key
        if self._sub_engines:
            for eng in self._sub_engines:
                eng.set_key(key)

    def _decrypt_if_needed(self, data: bytes) -> bytes:
        """Decrypt data if it carries the cari encryption magic header."""
        if not data or not is_encrypted(data):
            return data
        if self._enc_key is None:
            raise RuntimeError(
                "Index is encrypted. Supply a passphrase with --passphrase or "
                "call engine.set_key(key) before querying."
            )
        return decrypt_bytes(data, self._enc_key)

    # ─── Index file lookup ────────────────────────────────────────────────────

    def _read_posting(self, term):
        """Read posting list for an exact term from the index.

        Reads ALL *.bin files in the bucket directory — supports both single-builder
        (idx.bin) and distributed/parallel builds (idx_w0.bin, idx_w1.bin, …).
        Merges all doc_id lists into a single sorted, deduplicated result.
        """
        prefix = term_hash(term)
        bucket_dir = os.path.join(self.index_dir, prefix)

        if not os.path.isdir(bucket_dir):
            return []

        bin_files = sorted(f for f in os.listdir(bucket_dir) if f.endswith(".bin"))
        if not bin_files:
            return []

        # Check posting cache first (populated below on first read)
        cached = self._posting_cache.get(term)
        if cached is not None:
            return cached

        all_ids = []
        for bin_file in bin_files:
            data = open(os.path.join(bucket_dir, bin_file), "rb").read()
            if not data:
                continue
            # Decrypt if encrypted (must happen before decompression)
            data = self._decrypt_if_needed(data)
            # Auto-detect zlib compression
            if len(data) >= 2 and data[0] == 0x78 and data[1] in (0x01, 0x5e, 0x9c, 0xda):
                data = zlib.decompress(data)

            offset = 0
            while offset < len(data):
                if offset + 2 > len(data):
                    break
                term_len = struct.unpack_from("<H", data, offset)[0]; offset += 2
                if offset + term_len > len(data):
                    break
                stored = data[offset:offset + term_len].decode("utf-8"); offset += term_len
                if offset + 4 > len(data):
                    break
                pl_len = struct.unpack_from("<I", data, offset)[0]; offset += 4
                if stored == term:
                    all_ids.extend(decode_doclist(data[offset:offset + pl_len]))
                offset += pl_len

        result = sorted(set(all_ids)) if all_ids else []
        # Cache with bounded eviction: discard oldest half when full
        pc = self._posting_cache
        if len(pc) >= _POSTING_CACHE_MAX:
            keep = _POSTING_CACHE_MAX // 2
            for old_key in list(pc)[:len(pc) - keep]:
                del pc[old_key]
        pc[term] = result
        return result

    # ─── Term parsing ─────────────────────────────────────────────────────────

    @staticmethod
    def _wildcard_to_re(pattern):
        """Convert a wildcard pattern (* and %) to a compiled regex.

        Anchoring rules (mirrors SQL LIKE / glob semantics):
          "GARUDA%" → ^GARUDA.*    (prefix  — must start with "GARUDA")
          "%raydium" → .*raydium$  (suffix  — must end   with "raydium")
          "%jupiter%"→ .*jupiter.* (infix   — contains "jupiter" anywhere)
          "*jupiter*"→ .*jupiter.* (same with * wildcard)
          "7xMg%"    → ^7xMg.*
        """
        clean = pattern.strip().lower()
        starts_wild = bool(clean) and clean[0]  in ('%', '*')
        ends_wild   = bool(clean) and clean[-1] in ('%', '*')
        parts = re.split(r'[%*]+', clean)
        escaped = '.*'.join(re.escape(p) for p in parts)
        if not starts_wild:
            escaped = '^' + escaped
        if not ends_wild:
            escaped = escaped + '$'
        return re.compile(escaped)

    def _verify_wildcard(self, candidate_ids, pattern, column=None):
        """Post-filter trigram candidates by verifying the wildcard pattern against
        the actual stored document values.

        Trigram intersection is a candidate-generation step — it guarantees all
        trigrams are present but NOT that they appear consecutively. For example,
        "RAYDIUM" contains both trigrams 'ray' and 'diu' yet does not match
        *garuda*. This method eliminates those false positives.

        Args:
            candidate_ids: sorted list of doc_ids from trigram intersection
            pattern:       original wildcard term, e.g. "*garuda*" or "7xMg*"
            column:        canonical column key, or None for cross-column check

        Returns:
            sorted list of verified doc_ids
        """
        if not candidate_ids:
            return []
        rx = self._wildcard_to_re(pattern)
        rx_search = rx.search

        # Group by chunk_start so each chunk is loaded exactly once.
        # candidate_ids is sorted → chunks are processed in ascending order →
        # verified list is implicitly sorted (no extra sort needed).
        by_chunk: dict = {}
        chunk_start_fn = self._chunk_start
        for doc_id in candidate_ids:
            cs = chunk_start_fn(doc_id)
            try:
                by_chunk[cs].append(doc_id)
            except KeyError:
                by_chunk[cs] = [doc_id]

        verified = []
        load_chunk = self._load_chunk
        if column:
            for cs in sorted(by_chunk):
                chunk = load_chunk(cs)
                for doc_id in by_chunk[cs]:
                    val = _get_nested_value(chunk.get(doc_id, {}), column) or ""
                    if val and rx_search(str(val).lower()):
                        verified.append(doc_id)
        else:
            for cs in sorted(by_chunk):
                chunk = load_chunk(cs)
                for doc_id in by_chunk[cs]:
                    for val in chunk.get(doc_id, {}).values():
                        if isinstance(val, str) and rx_search(val.lower()):
                            verified.append(doc_id)
                            break
        return verified

    def _resolve(self, term, column=None, max_docs=None, *, exact=False):
        """Parse a (possibly wildcard) term into a sorted list of matching doc_ids.

        Args:
            term: Search term (may contain * or % wildcards).
            column: Optional column prefix.
            max_docs: If set for a match-all query, return only up to this many IDs
                      (avoids materializing billions of IDs for large indices).
            exact: If True, skip infix fallback (quoted term search).
        """
        term = term.strip()
        has_wildcard = "%" in term or "*" in term
        clean = term.strip("%* ").lower()

        # Handle "*" or "%" (match all) - return all doc IDs
        if not clean and has_wildcard:
            total = self.stats.get("total_docs", 0)
            if max_docs is not None:
                total = min(total, max_docs)
            return list(range(total))

        if not clean:
            return []

        col_prefix = f"{column}:" if column else ""

        if not has_wildcard:
            ids = self._read_posting(f"{col_prefix}{clean}")
            if not ids:
                # Expanded array/object fields use dot-bracket keys (tags[0], address.city).
                # When column-scoped exact match is empty, try each expanded variant.
                # Only applies when a column is specified AND no spaces (not multi-word).
                if column and " " not in clean:
                    result: set = set()
                    for col_key in self.columns():
                        if col_key.startswith(column) and col_key != column:
                            result.update(self._read_posting(f"{col_key}:{clean}"))
                    if result:
                        ids = sorted(result)
                # Fall back to global exact (column-scoped match may not exist for
                # expanded fields like tags[0], address.city; also needed for short
                # tokens without trigrams).
                if not ids:
                    ids = self._read_posting(clean)
            # Multi-word bare phrase (e.g. "aceh utara"): the full value is indexed
            # column-scoped but NOT as a global token.  Fan out across all columns.
            if not ids and not column and " " in clean:
                result: set = set()
                for col_key in self.columns():
                    result.update(self._read_posting(f"{col_key}:{clean}"))
                ids = sorted(result)
            # Fall back to infix match (e.g. "init" → "*init*") if exact match found nothing.
            # Infix is needed so "hang" matches "changing" (substring "hang" at position 2).
            # Skip infix fallback when exact=True (quoted term search).
            if not exact and not ids and len(clean) >= 2:
                ids = self._resolve("*" + clean + "*", column, max_docs)
            return sorted(ids)

        # Wildcard: extract trigrams from each literal segment only.
        # Splitting on wildcards prevents '%'/'*' chars appearing inside trigrams —
        # those chars are never indexed so their posting lists are always empty,
        # which causes a mixed pattern like "rayd%jupiter" to return zero results.
        _lit_segs = [s for s in re.split(r'[%*]+', clean) if s]
        _seen_tg: set = set()
        tgs: list = []
        for _seg in _lit_segs:
            for _i in range(len(_seg) - 2):
                _tg = _seg[_i:_i+3]
                if _tg not in _seen_tg:
                    _seen_tg.add(_tg)
                    tgs.append(_tg)

        if not tgs:
            # All literal segments are < 3 chars — no trigrams available.
            # Fall back to exact lookup only when there are no internal wildcards
            # (i.e. pattern like "bu*" → look for token "bu").  Patterns with
            # internal wildcards like "a%b" are unsupported at this length.
            if '%' in clean or '*' in clean:
                return []
            ids = self._read_posting(f"{col_prefix}{clean}")
            return sorted(ids)

        if column:
            # Column-scoped wildcard: read all trigram posting lists, then intersect
            # smallest→largest so rare trigrams prune the candidate set early.
            tg_lists = []
            for tg in tgs:
                ids = self._read_posting(f"{col_prefix}~{tg}")
                if not ids:
                    return []
                tg_lists.append(ids)
            tg_lists.sort(key=len)   # rarest (smallest) first → fastest intersection
            result = set(tg_lists[0])
            for ids in tg_lists[1:]:
                result &= set(ids)
                if not result:
                    return []
            candidates = sorted(result)
        else:
            # Cross-column wildcard: union trigram intersections across all known columns.
            # Global trigrams are not stored (removed to save space), so we iterate columns.
            result = set()
            for col_key in self.columns():
                tg_lists = []
                for tg in tgs:
                    ids = self._read_posting(f"{col_key}:~{tg}")
                    if not ids:
                        tg_lists = []
                        break
                    tg_lists.append(ids)
                if not tg_lists:
                    continue
                tg_lists.sort(key=len)
                col_result = set(tg_lists[0])
                for ids in tg_lists[1:]:
                    col_result &= set(ids)
                    if not col_result:
                        break
                if col_result:
                    result |= col_result
            candidates = sorted(result)

        # Post-filter: verify candidates actually match the full wildcard pattern.
        # Trigram intersection eliminates most non-matches but can produce false
        # positives when trigrams appear in a value but not consecutively
        # (e.g. "RAYDIUMV2" contains 'ray' and 'diu' but not the substring "garuda").
        return self._verify_wildcard(candidates, term, column)

    # ─── Doc fetching ─────────────────────────────────────────────────────────

    def _chunk_start(self, doc_id):
        return (doc_id // self.doc_chunk_size) * self.doc_chunk_size

    def _iter_chunks(self):
        """Yield (chunk_start, chunk_dict) for every chunk file in docs_dir.

        Uses glob to find files regardless of naming scheme — handles both
        sequential (docs_0000000000.zlib) and sparse/gapped chunk numbering.
        """

        # Glob all .zlib files under docs_dir
        pattern = os.path.join(self.docs_dir, "**", "*.zlib")
        files = _glob.glob(pattern, recursive=True)
        if not files:
            # Fallback: try sequential chunk numbering (chunks_0000000000.zlib, etc.)
            pattern2 = os.path.join(self.docs_dir, "chunks_*.zlib")
            files = _glob.glob(pattern2)
        if not files:
            logger = logging.getLogger(__name__)
            logger.warning(f"_iter_chunks: no .zlib files found in {self.docs_dir}, pattern={pattern}")
            return

        def chunk_key(path):
            m = re.match(r"^.*?docs_(\d+)\.zlib$", path.replace("\\", "/"))
            if m:
                return int(m.group(1))
            m2 = re.match(r"^.*?chunks_(\d+)\.zlib$", path.replace("\\", "/"))
            if m2:
                return int(m2.group(1))
            return 0

        for path in sorted(files, key=chunk_key):
            try:
                basename = os.path.basename(path)
                m = re.match(r"^docs_(\d+)\.zlib$", basename)
                if not m:
                    m = re.match(r"^chunks_(\d+)\.zlib$", basename)
                    if not m:
                        continue
                chunk_start = int(m.group(1))
                with open(path, "rb") as f:
                    blob = f.read()
                blob = self._decrypt_if_needed(blob)
                raw = _decompress_doc(blob)
                chunk = {int(k): v for k, v in _doc_loads(raw).items()}
                yield chunk_start, chunk
            except Exception:
                pass

    def _scan_doc_ids(self, max_ids=None):
        """Iterate chunk files and yield actual stored doc IDs in order.

        Handles gaps from deduplication where IDs are not consecutive.
        Yields up to max_ids IDs (None = unlimited).
        """
        collected = 0
        for chunk_start, chunk in self._iter_chunks():
            for doc_id in sorted(chunk):
                yield doc_id
                collected += 1
                if max_ids is not None and collected >= max_ids:
                    return

    def _fetch_page_from_chunks(self, page, page_size):
        """Fetch a page of docs by iterating chunks directly.

        More reliable than ID-based pagination for indices with gapped IDs
        (e.g. after dedup) or non-sequential numbering.
        """
        start_offset = page * page_size
        end_offset = start_offset + page_size

        docs = []
        offset = 0
        for chunk_start, chunk in self._iter_chunks():
            chunk_ids = sorted(chunk.keys())
            chunk_len = len(chunk_ids)

            # Check if any docs from this chunk fall in our page window
            if offset + chunk_len <= start_offset:
                # Entire chunk is before our window
                offset += chunk_len
                continue

            if offset >= end_offset:
                # We've passed our window
                break

            # Collect docs from this chunk that fall in [start_offset, end_offset)
            for doc_id in chunk_ids:
                if offset >= start_offset and offset < end_offset:
                    doc = {"_id": doc_id, **chunk[doc_id]}
                    self._collapse_expanded_fields(doc)
                    docs.append(doc)
                    if len(docs) >= page_size:
                        return docs
                offset += 1

        return docs

    def _doc_path(self, chunk_start):
        """2-level hex path: docs/{aa}/{bb}/docs_{N:010d}.zlib"""
        n = chunk_start // self.doc_chunk_size
        aa = f"{(n >> 8) & 0xFF:02x}"
        bb = f"{n & 0xFF:02x}"
        return os.path.join(self.docs_dir, aa, bb, f"docs_{chunk_start:010d}.zlib")

    def _load_chunk(self, start):
        if start in self._doc_cache:
            return self._doc_cache[start]

        # Try new 2-level path first, fall back to old flat path (backward compat)
        path = self._doc_path(start)
        if not os.path.isfile(path):
            path = os.path.join(self.docs_dir, f"docs_{start:010d}.zlib")
        if not os.path.isfile(path):
            return {}

        try:
            with open(path, "rb") as f:
                blob = f.read()
            blob = self._decrypt_if_needed(blob)   # no-op if not encrypted
            raw  = _decompress_doc(blob)
            chunk = {int(k): v for k, v in _doc_loads(raw).items()}
        except Exception as e:
            import warnings
            warnings.warn(f"Corrupted doc chunk {os.path.basename(path)}, skipping: {e}")
            chunk = {}
        self._doc_cache[start] = chunk
        return chunk

    def _fetch_docs(self, doc_ids):
        by_chunk = defaultdict(list)
        for did in doc_ids:
            by_chunk[self._chunk_start(did)].append(did)

        docs = []
        for start in sorted(by_chunk):
            chunk = self._load_chunk(start)
            for did in by_chunk[start]:
                if did in chunk:
                    doc = {"_id": did, **chunk[did]}
                    self._collapse_expanded_fields(doc)
                    docs.append(doc)
        return docs

    def _collapse_expanded_fields(self, doc):
        """Collapse expanded array/object fields back into parent structures.

        - tags[0], tags[1] → tags: [val0, val1]
        - info.metadata.a.value → info: {metadata: {a: {value: ...}}}
        - address.city, address.district → address: {city: ..., district: ...}

        Also removes parent keys that have expanded children to avoid redundancy.
        """
        import re

        array_groups = {}   # root -> {index: value}
        object_groups = {}  # root -> {path: value}
        to_delete = set()
        all_dot_keys = []

        # First pass: identify roots and collect dot-keys
        for key in list(doc.keys()):
            if key.startswith('_'):
                continue
            m_array = re.match(r'^(.+)\[(\d+)\]$', key)
            if m_array:
                root = m_array.group(1)
                idx = int(m_array.group(2))
                if root not in array_groups:
                    array_groups[root] = {}
                array_groups[root][idx] = doc[key]
                to_delete.add(key)
                continue

            m_dot = re.match(r'^(.+)\.(.+)$', key)
            if m_dot:
                all_dot_keys.append(key)

        # Find actual roots: keys that have children with more dots
        # e.g. info.metadata.a.value has root=info, child=info.metadata.a, grandchild=info.metadata.a.value
        # We want to identify that 'info' is a root (has deep children) but 'info.metadata.a' is not
        dot_key_roots = set(k.split('.')[0] for k in all_dot_keys)
        deep_roots = set()
        for k in all_dot_keys:
            parts = k.split('.')
            if len(parts) >= 3:
                # This key has structure root.parent.grandparent, so root has deep children
                deep_roots.add(parts[0])

        # Process dot-keys: determine if they should be collapsed or kept as-is
        for key in all_dot_keys:
            parts = key.split('.')
            root = parts[0]

            if root in deep_roots:
                # This is a deep key like info.metadata.a.value - collapse into root
                # path = everything after root (metadata.a.value)
                path = '.'.join(parts[1:])
                if root not in object_groups:
                    object_groups[root] = {}
                object_groups[root][path] = doc[key]
                to_delete.add(key)
            else:
                # Simple dot-path like address.city - collapse into root as-is
                path = '.'.join(parts[1:])
                if root not in object_groups:
                    object_groups[root] = {}
                object_groups[root][path] = doc[key]
                to_delete.add(key)

        # Reconstruct arrays
        for root, items in array_groups.items():
            doc[root] = [items[i] for i in sorted(items.keys())]

        # Reconstruct nested objects
        for root, paths in object_groups.items():
            nested = {}
            for path, value in paths.items():
                parts = path.split('.')
                d = nested
                for p in parts[:-1]:
                    if p not in d:
                        d[p] = {}
                    d = d[p]
                d[parts[-1]] = value
            doc[root] = nested

        # Delete expanded child keys
        for key in to_delete:
            del doc[key]

    # ─── Pagination helper ────────────────────────────────────────────────────

    def _paginate(self, doc_ids, page, page_size, total=None):
        if total is None:
            total = len(doc_ids)
        start = page * page_size
        page_ids = doc_ids[start:start + page_size]
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "results": self._fetch_docs(page_ids),
        }

    # ─── Multi-index fan-out helpers ──────────────────────────────────────────

    def _multi_paginate(self, per_eng, page, page_size):
        """Paginate across (engine, sorted_ids) pairs without loading all docs."""
        total = sum(len(ids) for _, ids in per_eng)
        start = page * page_size
        end   = start + page_size
        results = []
        offset  = 0
        for eng, ids in per_eng:
            n  = len(ids)
            lo = max(0, start - offset)
            hi = min(n, end   - offset)
            if lo < hi:
                docs = eng._fetch_docs(ids[lo:hi])
                idx_name = os.path.basename(eng.data_dir)
                for doc in docs:
                    doc["_index"] = idx_name
                results.extend(docs)
            offset += n
            if offset >= end:
                break
        return {"total": total, "page": page, "page_size": page_size, "results": results}

    # ─── Public API ───────────────────────────────────────────────────────────

    def search(self, term, column=None, page=0, page_size=20):
        """Search with optional wildcard (% or *).

        Args:
            term:      search term, e.g. "raydium" or "GARUDA*" or "*jup*"
            column:    canonical column key to restrict search (e.g. "program", "callsign")
            page:      0-based page number
            page_size: results per page

        Returns:
            {"total": N, "page": N, "page_size": N, "results": [...]}
        """
        is_match_all = not term.strip().strip("%* ")

        if self._sub_engines is not None:
            per_eng = [(eng, eng._resolve(term, column)) for eng in self._sub_engines]
            return self._multi_paginate(per_eng, page, page_size)

        # For match-all on a single index, scan chunks directly to get actual
        # stored docs — skips gaps left by dedup (IDs are not always consecutive).
        if is_match_all:
            real_total = self.stats.get("total_docs", 0)
            docs = self._fetch_page_from_chunks(page, page_size)
            return {"total": real_total, "page": page, "page_size": page_size, "results": docs}

        doc_ids = self._resolve(term, column)
        return self._paginate(doc_ids, page, page_size)

    def search_and(self, conditions, page=0, page_size=20):
        """AND search across multiple (column, term) conditions.

        All conditions must match (intersection). Fails fast if any returns empty.

        Args:
            conditions: list of (column, term) tuples, e.g.:
                        [("program", "raydium"), ("status", "success")]
                        Use None as column for cross-column search.
            page:       0-based page number
            page_size:  results per page

        Returns:
            {"total": N, "page": N, "page_size": N, "results": [...]}
        """
        if self._sub_engines is not None:
            per_eng = []
            for eng in self._sub_engines:
                result = None
                for col, term in conditions:
                    ids = eng._resolve(term, col)
                    if not ids:
                        result = []
                        break
                    s = set(ids)
                    result = s if result is None else result & s
                per_eng.append((eng, sorted(result) if result else []))
            return self._multi_paginate(per_eng, page, page_size)

        result = None
        for col, term in conditions:
            ids = self._resolve(term, col)
            if not ids:
                return {"total": 0, "page": page, "page_size": page_size, "results": []}
            s = set(ids)
            result = s if result is None else result & s

        if not result:
            return {"total": 0, "page": page, "page_size": page_size, "results": []}

        return self._paginate(sorted(result), page, page_size)

    def query(self, query_str, page=0, page_size=20):
        """Execute a Lucene-style query string.

        In multi-index mode, queries all sub-indexes and merges results.
        Results from sub-indexes are tagged with _index = sub-dir name.

        Args:
            query_str: Lucene query, e.g. "program:raydium AND amount:>1000000"
            page:      0-based page number
            page_size: results per page

        Returns:
            {"total": N, "page": N, "page_size": N, "results": [...], "query": str}

        Raises:
            SyntaxError: if query_str is invalid
        """
        if self._sub_engines is not None:
            return self._query_multi(query_str, page, page_size)

        from flatseek.core.query_parser import parse, execute

        ast = parse(query_str)
        if ast is None:
            return {"total": 0, "page": page, "page_size": page_size, "results": [], "query": query_str}

        doc_ids = sorted(execute(ast, self))
        result = self._paginate(doc_ids, page, page_size)
        result["query"] = query_str
        return result

    def _query_multi(self, query_str, page, page_size):
        """Fan query out to all sub-indexes, merge results, paginate efficiently."""
        from flatseek.core.query_parser import parse, execute

        ast = parse(query_str)
        if ast is None:
            return {"total": 0, "page": page, "page_size": page_size,
                    "results": [], "query": query_str}

        per_eng = []
        for eng in self._sub_engines:
            try:
                ids = sorted(execute(ast, eng))
            except Exception:
                ids = []
            per_eng.append((eng, ids))

        result = self._multi_paginate(per_eng, page, page_size)
        result["query"] = query_str
        return result

    def join(self, query_a, query_b, on, page=0, page_size=20):
        """Cross-dataset join: find docs matching query_a AND query_b linked by a shared field.

        Useful when two datasets share a key (phone, email) but are indexed separately
        as different _dataset labels.

        Args:
            query_a:   Lucene query for the first dataset, e.g. "_dataset:txs AND program:raydium"
            query_b:   Lucene query for the second dataset, e.g. "_dataset:logs AND service:api-gateway"
            on:        canonical field name that links both datasets (e.g. "signer", "trace_id")
            page:      0-based page
            page_size: results per page

        Returns:
            {
              "total": N,
              "page": N,
              "page_size": N,
              "results": [{"_a": doc_a, "_b": doc_b}, ...]
            }
        """
        from flatseek.core.query_parser import parse, execute

        # Resolve both queries to doc_id sets
        ast_a = parse(query_a)
        ast_b = parse(query_b)
        ids_a = sorted(execute(ast_a, self))
        ids_b = sorted(execute(ast_b, self))

        if not ids_a or not ids_b:
            return {"total": 0, "page": page, "page_size": page_size, "results": []}

        # Load docs for both sides, build lookup by join key
        docs_a = self._fetch_docs(ids_a)
        docs_b = self._fetch_docs(ids_b)

        # Index side B by join key value
        b_by_key = {}
        for doc in docs_b:
            key_val = doc.get(on, "")
            if key_val:
                b_by_key.setdefault(key_val, []).append(doc)

        # Join: for each doc in A, find matching docs in B
        pairs = []
        for doc_a in docs_a:
            key_val = doc_a.get(on, "")
            if key_val and key_val in b_by_key:
                for doc_b in b_by_key[key_val]:
                    pairs.append({"_a": doc_a, "_b": doc_b})

        total = len(pairs)
        start = page * page_size
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "results": pairs[start:start + page_size],
        }

    def cross_lookup(self, query, target, link_field, target_field=None,
                     return_fields=None, top_n=10, page=0, page_size=20):
        """Search this index, then look up matching values in a target index.

        Use this to enrich results across two separate indexes.  For example:
        search the 'solana_txs' index for raydium swaps, then fetch fee stats from
        the 'fee_stats' index for every transaction found.

        Args:
            query:         Lucene query run on THIS index.
                           e.g. "program:raydium"  or  "callsign:GARUDA* AND altitude:>30000"
            target:        Another QueryEngine instance to look up in.
            link_field:    Field from this index's results used as the lookup key.
                           e.g. "signer", "trace_id", "campaign_id"
            target_field:  Field in the target index to match against.
                           Defaults to link_field when omitted.
            return_fields: List of fields to include from target results.
                           None = all fields.
            top_n:         How many results from THIS index to process (default 10).
            page / page_size: Pagination over final joined output.

        Returns:
            {
                "total": N,          # number of source docs with at least one match
                "page": N,
                "page_size": N,
                "results": [
                    {
                        "_source":      {doc from this index},
                        "_matches":     [{doc from target, filtered to return_fields}],
                        "_match_count": N,
                    },
                    ...
                ]
            }

        Example:
            qe_txs     = QueryEngine("data/solana_txs")
            qe_logs    = QueryEngine("data/logs")

            result = qe_txs.cross_lookup(
                query         = "program:raydium AND signer:*7xMg*",
                target        = qe_logs,
                link_field    = "signer",
                return_fields = ["trace_id", "level"],
                top_n         = 20,
            )
            for row in result["results"]:
                src = row["_source"]
                for m in row["_matches"]:
                    print(src.get("signer"), m.get("trace_id"), m.get("level"))
        """
        if target_field is None:
            target_field = link_field

        # ── Step 1: search this (source) index ───────────────────────────────
        src_result = self.query(query, page_size=top_n)
        src_docs   = src_result.get("results", [])

        if not src_docs:
            return {"total": 0, "page": page, "page_size": page_size, "results": []}

        # ── Step 2: collect unique link values, preserving source doc order ──
        seen_vals = set()
        src_with_key = []   # [(src_doc, link_val)]
        for doc in src_docs:
            val = str(doc.get(link_field, "")).strip()
            if not val or val in seen_vals:
                continue
            seen_vals.add(val)
            src_with_key.append((doc, val))

        # ── Step 3: look up each link value in the target index ───────────────
        # Quote multi-word values so they're searched as a phrase, not AND terms.
        joined = []
        for src_doc, link_val in src_with_key:
            safe_val = f'"{link_val}"' if " " in link_val else link_val
            tgt_q    = f"{target_field}:{safe_val}"
            tgt_result = target.query(tgt_q, page_size=50)
            tgt_docs   = tgt_result.get("results", [])

            if return_fields:
                rf = set(return_fields)
                tgt_docs = [
                    {k: v for k, v in d.items() if k in rf or k == "_id"}
                    for d in tgt_docs
                ]

            joined.append({
                "_source":      src_doc,
                "_matches":     tgt_docs,
                "_match_count": len(tgt_docs),
            })

        # Only keep rows that have at least one match
        matched = [r for r in joined if r["_match_count"] > 0]

        total = len(matched)
        start = page * page_size
        return {
            "total":     total,
            "page":      page,
            "page_size": page_size,
            "results":   matched[start:start + page_size],
        }

    # ─── Range queries ────────────────────────────────────────────────────────

    def _year_range_ids(self, field, lo_year, hi_year):
        """Union posting lists for all years in [lo_year, hi_year]."""
        result = set()
        for year in range(max(1900, lo_year), min(2100, hi_year) + 1):
            result.update(self._read_posting(f"{field}:{year}"))
        return result

    def _resolve_range(self, field, *args):
        """Resolve a range node ('range', field, op, value) or
        ('range', field, 'between', lo, hi).

        Supports:
          - altitude>30000, altitude:[30000 TO 40000]
          - amount>1000000, amount:[500000 TO 5000000]
          - umur>40, umur<40, umur:[30 TO 50]  (converted to birthday year range)

        Resolution is year-precision: values are matched by the 4-digit year
        token already stored in the index.
        """
        current_year = _date.today().year
        op = args[0]

        # ── umur / age → birthday year range (inverted) ──────────────────────
        _AGE_FIELDS = {'umur', 'age', 'usia'}
        if field.lower() in _AGE_FIELDS:
            # Find birthday column by semantic type
            bday_col = next(
                (c for c, t in self.columns().items() if t == 'birthday'),
                'birthday'
            )
            try:
                if op == 'between':
                    lo_age, hi_age = int(args[1]), int(args[2])
                    lo_y = current_year - hi_age
                    hi_y = current_year - lo_age
                else:
                    age = int(args[1])
                    if op == '>':    lo_y, hi_y = 1900, current_year - age - 1
                    elif op == '>=': lo_y, hi_y = 1900, current_year - age
                    elif op == '<':  lo_y, hi_y = current_year - age + 1, current_year
                    elif op == '<=': lo_y, hi_y = current_year - age, current_year
                    else:            return set()
            except (ValueError, IndexError):
                return set()
            return self._year_range_ids(bday_col, lo_y, hi_y)

        # ── Date / birthday range ─────────────────────────────────────────────
        def _extract_year(v):
            """Extract 4-digit year from a value like '1980', '1980-06-15', '19800615'."""
            v = re.sub(r'[-/|.]', '', str(v).strip())
            m = re.match(r'^(\d{4})', v)
            return int(m.group(1)) if m else None

        def _parse_numeric(v):
            """Try to parse an integer or float string for numeric range queries.
            Returns None for date-like 8-digit YYYYMMDD integers so they fall through
            to the date/year range path instead of being treated as numbers."""
            try:
                v = str(v).strip().replace(",", "")
                num = float(v)
                # Reject pure 8-digit integers that look like YYYYMMDD dates.
                # Only the integer part matters — "20260301.0" is a float, not a date string.
                int_val = int(num)
                if float(int_val) == num:  # no fractional part
                    s = str(int_val)
                    if len(s) == 8 and s.isdigit():
                        m = re.match(r'^(19|20)\d{2}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])$', s)
                        if m:
                            return None
                return num
            except (ValueError, TypeError):
                return None

        # Numeric range: if op is 'between' with numeric args, handle as numeric range
        if op == 'between':
            lo_val = _parse_numeric(args[1])
            hi_val = _parse_numeric(args[2])
            if lo_val is not None and hi_val is not None:
                # Query uses numeric range (e.g. balance:[1000000 TO 99999999])
                # Scan all docs and filter by numeric comparison
                result = set()
                total = self.stats.get("total_docs", 0)
                by_chunk: dict = {}
                chunk_start_fn = self._chunk_start
                for doc_id in range(total):
                    cs = chunk_start_fn(doc_id)
                    if cs not in by_chunk:
                        by_chunk[cs] = []
                    by_chunk[cs].append(doc_id)
                load_chunk = self._load_chunk
                for cs in sorted(by_chunk):
                    chunk = load_chunk(cs)
                    for doc_id in by_chunk[cs]:
                        row = chunk.get(doc_id, {})
                        fval = _get_nested_value(row, field)
                        if fval is not None:
                            try:
                                fval_num = float(fval)
                                if lo_val <= fval_num <= hi_val:
                                    result.add(doc_id)
                            except (ValueError, TypeError):
                                pass
                return result
            # Fall through: treat as year range if parsing failed
            lo_y = _extract_year(args[1])
            hi_y = _extract_year(args[2])
            if lo_y is None or hi_y is None:
                return set()
        else:
            val_y = _extract_year(args[1])
            # If not a year (no 4-digit prefix), or year is unrealistic, treat as numeric comparison
            # e.g. "1000000" extracts as 1000 (first 4 digits) which is not a real year
            if val_y is None or val_y < 1900 or val_y > current_year + 1:
                num_val = _parse_numeric(args[1])
                if num_val is not None:
                    op_sym = op  # '>', '>=', '<', '<='
                    result = set()
                    total = self.stats.get("total_docs", 0)
                    by_chunk: dict = {}
                    chunk_start_fn = self._chunk_start
                    for doc_id in range(total):
                        cs = chunk_start_fn(doc_id)
                        if cs not in by_chunk:
                            by_chunk[cs] = []
                        by_chunk[cs].append(doc_id)
                    load_chunk = self._load_chunk
                    for cs in sorted(by_chunk):
                        chunk = load_chunk(cs)
                        for doc_id in by_chunk[cs]:
                            row = chunk.get(doc_id, {})
                            fval = _get_nested_value(row, field)
                            if fval is not None:
                                try:
                                    fval_num = float(fval)
                                    if op_sym == '>' and fval_num > num_val:
                                        result.add(doc_id)
                                    elif op_sym == '>=' and fval_num >= num_val:
                                        result.add(doc_id)
                                    elif op_sym == '<' and fval_num < num_val:
                                        result.add(doc_id)
                                    elif op_sym == '<=' and fval_num <= num_val:
                                        result.add(doc_id)
                                except (ValueError, TypeError):
                                    pass
                    return result
                return set()
            if op == '>':    lo_y, hi_y = val_y + 1, current_year
            elif op == '>=': lo_y, hi_y = val_y, current_year
            elif op == '<':  lo_y, hi_y = 1900, val_y - 1
            elif op == '<=': lo_y, hi_y = 1900, val_y
            else:            return set()

        return self._year_range_ids(field, lo_y, hi_y)

    # ─── Metadata ─────────────────────────────────────────────────────────────

    def columns(self):
        """Return {canonical_key: semantic_type} dict."""
        return self.stats.get("columns", {})

    def summary(self):
        s = self.stats
        lines = [
            f"Docs:       {s.get('total_docs', '?'):,}",
            f"Index:      {s.get('index_size_mb', '?')} MB ({s.get('index_files', '?')} files)",
            f"Doc store:  {s.get('docs_size_mb', '?')} MB",
            f"Total:      {s.get('total_size_mb', '?')} MB",
            "",
            "Columns:",
        ]
        for col, sem_type in sorted(self.columns().items()):
            lines.append(f"  {col:30s} {sem_type}")
        return "\n".join(lines)

    # ─── Aggregation ─────────────────────────────────────────────────────────

    def aggregate(self, q=None, aggs=None, size=10):
        """Execute streaming aggregation with optional Lucene query filter.

        Memory-bounded: terms Counter capped at 1M unique, cardinality uses
        fixed ~48KB HyperLogLog sketch. Checks RSS every 50 chunks.

        Args:
            q:     Lucene query string to filter docs (None = all docs).
            aggs:  Aggregation definitions, e.g.
                   {"by_city": {"terms": {"field": "city", "size": 10}}}
            size:  Default max buckets for terms aggs (default: 10).

        Returns:
            dict with keys: took, hits.total, aggregations{...}
        """
        import gc
        import hashlib
        import math
        import time as _time
        from collections import Counter

        if aggs is None:
            aggs = {}

        # ── Parse query ───────────────────────────────────────────────────────
        if q:
            from flatseek.core.query_parser import parse, execute
            ast = parse(q)
            if ast is not None:
                matching_ids = set(execute(ast, self))
            else:
                matching_ids = set()
        else:
            matching_ids = None  # sentinel: all docs

        # ── Pre-parse agg config ──────────────────────────────────────────────
        agg_fields = {}
        agg_configs = {}
        agg_types = ("terms", "avg", "min", "max", "sum", "stats",
                     "cardinality", "date_histogram", "histogram")
        for agg_name, agg_def in aggs.items():
            if not isinstance(agg_def, dict):
                continue
            agg_type = None
            agg_config = agg_def
            if agg_name in agg_types:
                agg_type = agg_name
                agg_config = agg_def
            else:
                for k in agg_types:
                    if k in agg_def and isinstance(agg_def[k], dict):
                        agg_type = k
                        agg_config = agg_def[k]
                        break
            if not agg_type:
                continue
            field = agg_config.get("field")
            if not field:
                continue
            agg_fields[agg_type] = field
            agg_configs[agg_type] = agg_config

        # ── Streaming state ──────────────────────────────────────────────────
        terms_counter = Counter()
        terms_overflow = 0
        terms_truncated = False
        date_counter = Counter()
        hist_counter = Counter()

        # Fixed-memory HyperLogLog sketch (~48KB)
        class _CardinalitySketch:
            __slots__ = ("m", "_bitmap", "_zero_count", "_max_reg")
            def __init__(self, m=65536):
                self.m = m
                self._bitmap = [0] * m
                self._zero_count = m
                self._max_reg = 0
            def add(self, value):
                if value is None:
                    return
                h = hashlib.sha256(str(value).encode()).digest()[:8]
                hash_u32 = int.from_bytes(h[:4], "little")
                reg_idx = hash_u32 % self.m
                bits = h[4] & 0x3F
                old = (self._bitmap[reg_idx >> 4] >> ((reg_idx & 0xF) * 4)) & 0xF
                if old == 0:
                    self._zero_count -= 1
                if bits > self._max_reg:
                    self._max_reg = bits
                shift = (reg_idx & 0xF) * 4
                self._bitmap[reg_idx >> 4] = (
                    self._bitmap[reg_idx >> 4] & ~(0xF << shift)) | (bits << shift)
            def count(self):
                if self._zero_count > 0:
                    return int(self.m * math.log(self.m / max(1, self._zero_count)))
                alpha = 0.6735 if self.m == 65536 else (0.7071 if self.m == 32768 else 0.7213)
                raw = alpha * self.m * self.m / sum(
                    1.0 / (1 << max(1, ((self._bitmap[i >> 4] >> ((i & 0xF) * 4)) & 0xF)))
                    for i in range(self.m))
                return int(min(raw, 2 ** (self._max_reg + 1)))

        cardinality_sketch = _CardinalitySketch()

        running_count = 0
        running_sum = 0.0
        running_min = None
        running_max = None

        total_docs = 0
        chunks_since_mem_check = 0
        start = _time.perf_counter()
        MAX_TERMS = 1_000_000
        MEM_CHECK_INTERVAL = 50

        def _parse_date(value):
            if not value:
                return None
            value = str(value).replace("-", "").replace("/", "")
            if len(value) == 8 and value.isdigit():
                return value
            elif len(value) == 10:
                return value[:4] + value[5:7] + value[8:10]
            return value

        def _to_number(val):
            try:
                return float(str(val))
            except Exception:
                return None

        # ── Iterate chunks ───────────────────────────────────────────────────
        def _iter_chunks_for_agg(eng):
            if eng._sub_engines is not None:
                for sub in eng._sub_engines:
                    for cs, chunk in sub._iter_chunks():
                        yield cs, chunk
            else:
                for cs, chunk in eng._iter_chunks():
                    yield cs, chunk

        # Pre-resolve every agg field's parsed path once.  This is the hot-path
        # win for terms aggregations: 50K+ regex+split calls collapse to one
        # parse per distinct field.
        agg_field_paths = {f: _parse_field_path(f) for f in agg_fields.values()}
        # Cache item lists in stable order (avoids dict-iteration overhead per doc).
        agg_items = [(t, f, agg_field_paths[f]) for t, f in agg_fields.items()]

        # Hoist hot frees into locals.
        _walk = _walk_path
        _PLAIN_LOCAL = _PLAIN

        for chunk_start, chunk in _iter_chunks_for_agg(self):
            chunk_len = len(chunk)
            total_docs += chunk_len
            chunks_since_mem_check += 1

            # No sort: terms/stats/avg are commutative; ordering doesn't affect
            # the result and saves O(N log N) per chunk on big indices.
            for doc_id, doc in chunk.items():
                # Filter by query
                if matching_ids is not None and doc_id not in matching_ids:
                    continue

                for agg_type, field, ppath in agg_items:
                    val = doc.get(field)
                    if val is None:
                        if ppath is _PLAIN_LOCAL:
                            continue
                        val = _walk(doc, ppath)
                        if val is None:
                            continue

                    if agg_type == "terms":
                        if not terms_truncated:
                            # Arrays: iterate each element so "graphql" gets its own bucket
                            # val may be a list/tuple, or a string like "['a','b']" from CSV
                            if isinstance(val, (list, tuple)):
                                items = val
                            elif isinstance(val, str) and val.startswith(('[', '(')):
                                try:
                                    items = json.loads(val.replace("'", '"'))
                                except Exception:
                                    items = [val]
                            else:
                                items = [val]
                            for item in items:
                                if len(terms_counter) < MAX_TERMS:
                                    terms_counter[str(item)] += 1
                                else:
                                    terms_truncated = True
                                    terms_overflow += 1

                    elif agg_type == "cardinality":
                        cardinality_sketch.add(val)

                    elif agg_type in ("avg", "min", "max", "sum", "stats"):
                        num = _to_number(val)
                        if num is None:
                            continue
                        running_count += 1
                        running_sum += num
                        if running_min is None or num < running_min:
                            running_min = num
                        if running_max is None or num > running_max:
                            running_max = num

                    elif agg_type == "date_histogram":
                        ms_to_epoch = agg_configs.get("date_histogram", {}).get("ms_to_epoch", False)
                        if ms_to_epoch:
                            try:
                                epoch = int(float(str(val)))
                                if 0 < epoch < 4102444800:
                                    val_str = _time.strftime("%Y%m%d%H%M%S", _time.gmtime(epoch))
                                else:
                                    val_str = str(val)
                            except Exception:
                                val_str = str(val)
                        else:
                            val_str = _parse_date(val) if isinstance(val, str) else str(val)
                        date_counter[val_str] += 1

                    elif agg_type == "histogram":
                        try:
                            num = float(val) if isinstance(val, str) else val
                            interval = agg_configs.get("histogram", {}).get("interval", 1)
                            bucket_key = int(num // interval) * interval
                            hist_counter[bucket_key] += 1
                        except Exception:
                            pass

            # Memory guard
            if chunks_since_mem_check >= MEM_CHECK_INTERVAL:
                chunks_since_mem_check = 0
                gc.collect()

        # ── Build response ───────────────────────────────────────────────────
        aggregations = {}
        for agg_type, agg_config in agg_configs.items():
            # Use the actual field name as the aggregation result key, not agg_type.
            # e.g. {"terms": {"field": "city"}} → result keyed by "city"
            result_key = agg_fields.get(agg_type, agg_type)
            if agg_type == "terms":
                terms_size = agg_config.get("size", size)
                top = terms_counter.most_common(terms_size)
                buckets = [{"key": k, "doc_count": c} for k, c in top]
                unique_count = len(terms_counter)
                if terms_truncated:
                    unique_count = f">{unique_count}"
                aggregations[result_key] = {
                    "buckets": buckets,
                    "sum_other_doc_count": (
                        unique_count - len(buckets) if isinstance(unique_count, int) else 0
                    ),
                }

            elif agg_type == "avg":
                if running_count > 0:
                    aggregations[result_key] = {"value": running_sum / running_count}

            elif agg_type == "min":
                if running_count > 0:
                    aggregations[result_key] = {"value": running_min}

            elif agg_type == "max":
                if running_count > 0:
                    aggregations[result_key] = {"value": running_max}

            elif agg_type == "sum":
                if running_count > 0:
                    aggregations[result_key] = {"value": running_sum}

            elif agg_type == "stats":
                if running_count > 0:
                    aggregations[result_key] = {
                        "count": running_count,
                        "min": running_min,
                        "max": running_max,
                        "avg": running_sum / running_count,
                        "sum": running_sum,
                    }

            elif agg_type == "cardinality":
                aggregations[result_key] = {"value": cardinality_sketch.count()}

            elif agg_type == "date_histogram":
                interval = agg_config.get("interval", "day")
                processed = Counter()
                for k, c in date_counter.items():
                    if interval == "hour" and len(k) >= 8:
                        key = k[:8] + "0000"
                    elif interval == "month" and len(k) >= 6:
                        key = k[:6]
                    elif interval == "year" and len(k) >= 4:
                        key = k[:4]
                    else:
                        key = k[:8] if len(k) >= 8 else k
                    processed[key] += c
                buckets = [
                    {"key_as_string": k, "key": k, "doc_count": c}
                    for k, c in sorted(processed.items())
                ]
                aggregations[result_key] = {"buckets": buckets}

            elif agg_type == "histogram":
                interval = agg_config.get("interval", 1)
                processed = Counter()
                for k, c in hist_counter.items():
                    bucket_key = int(k // interval) * interval
                    processed[bucket_key] += 1
                buckets = [
                    {"key": k, "doc_count": c}
                    for k, c in sorted(processed.items())
                ]
                aggregations[result_key] = {"buckets": buckets}

        return {
            "took": int((_time.perf_counter() - start) * 1000),
            "hits": {"total": total_docs, "hits": []},
            "aggregations": aggregations,
        }
