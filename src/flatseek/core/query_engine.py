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
import mmap as _mmap
import os
import re
import json
import struct
import zlib
import mmap
import time as _time
from collections import defaultdict
from datetime import date as _date
from pathlib import Path

from flatseek.core.storage import StorageAdapter, create_storage_adapter

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

# Default embedded license key (built-in, public).
# Used when FLATSEEK_EMBEDDED_KEY env var is not set.
# This key allows opensource binary to verify tokens signed with default key.
DEFAULT_EMBEDDED_LICENSE_KEY = b"opensourcelicensekey1234567890123456"  # 32 bytes


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
    except Exception as e:
        # Wrong key → re-raise as InvalidToken so route handlers detect it as 401.
        _is_wrong_key = False
        try:
            from cryptography.fernet import InvalidToken
            _is_wrong_key = isinstance(e, InvalidToken)
        except ImportError:
            pass
        if not _is_wrong_key:
            try:
                from cryptography.exceptions import InvalidTag
                _is_wrong_key = isinstance(e, InvalidTag)
            except ImportError:
                pass
        if _is_wrong_key:
            try:
                from cryptography.fernet import InvalidToken as _IT
                raise _IT(str(e)) from e
            except ImportError:
                raise ValueError(f"Decryption failed (wrong key): {e}")
        raise ValueError(f"Decryption failed: {e}")


def is_encrypted(data: bytes) -> bool:
    return data[:len(_ENC_MAGIC)] == _ENC_MAGIC


def load_encryption_key(index_dir: str | None, passphrase: str, meta: dict | None = None) -> bytes:
    """Load salt from encryption.json (inside index_dir) and derive key from passphrase.

    Args:
        index_dir: Directory containing encryption.json (not needed if meta is provided).
        passphrase: The passphrase to derive the key from.
        meta: Optional pre-loaded encryption metadata dict (contains 'salt' key).
              If provided, index_dir is ignored.

    Raises FileNotFoundError if encryption.json is missing (index not encrypted).
    """
    if meta is None:
        if index_dir is None:
            raise FileNotFoundError("Must provide either index_dir or meta")
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


# ─── Embedded verification key (for subscription/passphrase tokens) ─────────────
#
# The binary ships with an embedded HMAC key (stored in _embedded_key field).
# This key is used to verify user-provided passphrase tokens WITHOUT exposing
# the owner's private key in the binary.
#
# Flow:
#   Owner: generate_passphrase(id, expire_ts, export_limit, owner_private_key) → token
#   Binary: verify_passphrase(token, embedded_key) → (id, expire_ts, export_limit)
#
# Security:
#   - Embedded key is a separate HMAC key (not the same as encryption key)
#   - User can forge expiry dates but signature won't match
#   - User cannot forge new tokens (needs private key)


def get_embedded_license_key() -> bytes:
    """Get the embedded license verification key.

    Order:
      1. FLATSEEK_EMBEDDED_KEY env var (for custom builds)
      2. DEFAULT_EMBEDDED_LICENSE_KEY (built-in)

    Returns 32-byte key.
    """
    import os
    key_hex = os.environ.get("FLATSEEK_EMBEDDED_KEY", "")
    if key_hex:
        key = bytes.fromhex(key_hex)
        if len(key) != 32:
            raise ValueError(
                f"FLATSEEK_EMBEDDED_KEY must be 32 bytes, got {len(key)}"
            )
        return key
    return DEFAULT_EMBEDDED_LICENSE_KEY


def verify_license_token(token: str, manifest: dict) -> tuple[str, int, int]:
    """Verify a license token using the embedded key from manifest (crypto layer).

    This is the crypto-layer verification — the embedded_key comes from the
    .fsk manifest, NOT from the binary. The binary's default key is only used
    as fallback if the manifest has no embedded_key.

    Flow:
      manifest._embedded_key = which key was used when packing
      token = base64(id|expire_ts|export_limit|signature)
      signature = HMAC(owner_private_key, id|expire_ts|export_limit)

    Returns (id, expire_ts, export_limit).
    Raises PermissionError on key mismatch (custom key FSK vs opensource binary).

    Args:
        token: base64-encoded license token from generate_passphrase()
        manifest: manifest dict from .fsk (contains _embedded_key)
    Returns:
        (id, expire_ts, export_limit) if valid
    Raises:
        ValueError if token is invalid/tampered
        PermissionError if expired or key mismatch
    """
    # Get embedded key from manifest
    manifest_key = get_embedded_key(manifest)
    if manifest_key is None:
        # Plain .fsk — no license
        raise ValueError("Not a license-protected .fsk")

    # Get binary's key (default or custom)
    binary_key = get_embedded_license_key()

    # Compare manifest key vs binary key
    import base64 as _b64
    if _b64.b64encode(manifest_key).decode() != _b64.b64encode(binary_key).decode():
        # Key mismatch: manifest has different key than binary
        # This means: custom key FSK tried with opensource binary
        raise PermissionError(
            "This .fsk was generated by a private/custom flatseek build.\n"
            "The embedded license key does not match this opensource binary.\n"
            "Please obtain the proper licensed binary from the vendor."
        )

    # Keys match — verify token with that key
    return verify_passphrase(token, manifest_key)


def get_embedded_key(manifest: dict) -> bytes | None:
    """Extract embedded HMAC key from manifest metadata.

    The embedded key is stored as base64 in the manifest's _embedded_key field.
    Returns None if not present (plain/old-format binary).
    """
    import base64 as _b64
    embedded_b64 = manifest.get("_embedded_key")
    if not embedded_b64:
        return None
    try:
        return _b64.b64decode(embedded_b64)
    except Exception:
        return None


# ─── Enclosed encryption (for license/expiration) ─────────────────────────────
#
# Double-layer encryption that cryptographically enforces expiration:
#
#   inner_ct = encrypt({data, expire_ts, K_inner_xored}, K_inner)
#   outer_ct = encrypt(inner_ct, K_user)
#
# K_inner is XORed with HMAC(K_inner, "pos") and stored at a random position
# inside the inner plaintext. To decrypt inner after expiry, you'd need to know
# K_inner to find K_inner - circular dependency enforced by time check.
#
# To decrypt:
#   1. decrypt outer_ct with passphrase → inner_ct
#   2. decrypt inner_ct with K_inner → data (ONLY if not expired)
#
# After expiry: step 2 fails because decrypt checks expire_ts before returning.
# No separate "is_expired" check to patch - enforced by the crypto itself.


def encrypt_outer_enclosed(inner_ct: bytes, passphrase: str, salt: bytes) -> bytes:
    """Encrypt the inner ciphertext with user's passphrase (outer layer).

    K_user = PBKDF2(passphrase, salt, 600k iterations)
    outer_ct = encrypt(inner_ct, K_user)
    """
    k_user = derive_key(passphrase, salt)
    return encrypt_bytes(inner_ct, k_user)


def decrypt_outer_enclosed(outer_ct: bytes, passphrase: str, salt: bytes) -> bytes:
    """Decrypt outer layer to get inner ciphertext. Wrong passphrase → ValueError."""
    k_user = derive_key(passphrase, salt)
    return decrypt_bytes(outer_ct, k_user)


def encrypt_expired_enclosed(data: bytes, expire_ts: int, inner_key: bytes) -> bytes:
    """Encrypt data with inner key, embedding expiration timestamp.

    The plaintext is JSON: {"expire_at": unix_ts_int, "data": <bytes>, "pos": int}
    Where 'pos' is the byte offset where the XORed key hint is stored.
    The key hint is HMAC(inner_key, b"pos") XOR inner_key[:16].

    Returns the ciphertext blob. Expiration is NOT checked here - only when decrypting.
    """
    import json
    import base64 as _b64
    import hmac
    import hashlib

    # Create key hint: HMAC(key, context) XOR key_prefix
    key_hint = hmac.new(inner_key, b"flatseek-enclosed-v1", hashlib.sha256).digest()
    key_prefix = inner_key[:16]
    xored_hint = bytes(a ^ b for a, b in zip(key_hint[:16], key_prefix))

    # Random position for hint (avoiding JSON boundaries)
    import os as _os
    data_len = len(data)
    pos = _os.urandom(4)
    pos_int = int.from_bytes(pos, "big") % max(1, data_len - 32)

    # Build plaintext
    plaintext = json.dumps({
        "expire_at": expire_ts,
        "data": _b64.b64encode(data).decode("ascii"),
        "hint": _b64.b64encode(xored_hint).decode("ascii"),
        "pos": pos_int,
    }).encode("utf-8")
    return encrypt_bytes(plaintext, inner_key)


def decrypt_expired_enclosed(enclosed_ct: bytes, inner_key: bytes) -> bytes:
    """Decrypt enclosed data. Checks expiration BEFORE returning.

    Raises PermissionError if current time > expire_at.
    Raises ValueError if wrong key or tampered data.
    Returns the original data bytes.
    """
    import json
    import base64 as _b64
    from datetime import datetime, timezone

    plaintext = decrypt_bytes(enclosed_ct, inner_key)
    parsed = json.loads(plaintext)

    expire_ts = parsed["expire_at"]
    now_ts = int(datetime.now(timezone.utc).timestamp())
    if now_ts > expire_ts:
        raise PermissionError(
            f"This index expired on {datetime.fromtimestamp(expire_ts, tz=timezone.utc).isoformat()}. "
            "The license has lapsed and the data can no longer be accessed."
        )

    return _b64.b64decode(parsed["data"])


# ─── Passphrase / signed license tokens ─────────────────────────────────────────
#
# Owner generates passphrase with private key:
#   passphrase = base64(id|expire_ts|signature)
#   signature = HMAC-SHA256(key, id|expire_ts)
#
# Binary verifies with embedded key:
#   verify_passphrase(token, key) → (id, expire_ts) or raises
#
# Token format (compact base64):
#   base64(id|expire_ts|signature)
#   id: arbitrary string (email, UUID, counter)
#   expire_ts: Unix timestamp int
#   signature: HMAC-SHA256(key, id|expire_ts)


def generate_passphrase(id: str, expire_ts: int, key: bytes,
                        export_limit: int = 1000) -> str:
    """Generate a passphrase token from id + expiry + export_limit + key.

    Args:
        id: arbitrary identifier (email, UUID, counter)
        expire_ts: Unix timestamp when this token expires
        key: symmetric key for HMAC signing (owner keeps secret)
        export_limit: max rows exportable with this token (default 1000, 0=unlimited)
    Returns:
        base64-encoded token: base64(id|expire_ts|export_limit|base64(signature))
        Signature is base64-encoded to avoid delimiter collision (raw HMAC bytes
        may contain b'|' which would break the rsplit-based parsing in verify).
    """
    import base64 as _b64
    import hmac
    import hashlib

    msg = f"{id}|{expire_ts}|{export_limit}".encode("utf-8")
    sig = hmac.new(key, msg, hashlib.sha256).digest()
    sig_b64 = _b64.b64encode(sig).decode("ascii")
    payload = _b64.b64encode(msg + b"|" + sig_b64.encode("ascii")).decode("ascii")
    return payload


def verify_passphrase(token: str, key: bytes) -> tuple[str, int, int]:
    """Verify a passphrase token and return (id, expire_ts, export_limit).

    Args:
        token: base64-encoded passphrase from generate_passphrase()
        key: symmetric key for HMAC verification (from manifest)
    Returns:
        (id, expire_ts, export_limit) if valid
    Raises:
        ValueError if token is invalid/tampered
        PermissionError if expired
    """
    import base64 as _b64
    import hmac
    import hashlib
    from datetime import datetime, timezone

    try:
        raw = _b64.b64decode(token)
    except Exception:
        raise ValueError("Invalid passphrase format")

    try:
        id_part, ts_part, limit_part, sig_b64 = raw.rsplit(b"|", 3)
        id_str = id_part.decode("utf-8")
        expire_ts = int(ts_part)
        export_limit = int(limit_part)
        signature = _b64.b64decode(sig_b64)
    except (ValueError, UnicodeDecodeError) as e:
        raise ValueError(f"Malformed passphrase: {e}")

    # Verify signature
    msg = f"{id_str}|{expire_ts}|{export_limit}".encode("utf-8")
    expected_sig = hmac.new(key, msg, hashlib.sha256).digest()
    if not hmac.compare_digest(signature, expected_sig):
        raise ValueError("Invalid passphrase signature")

    # Check expiration
    now_ts = int(datetime.now(timezone.utc).timestamp())
    if now_ts > expire_ts:
        raise PermissionError(
            f"Token expired on {datetime.fromtimestamp(expire_ts, tz=timezone.utc).isoformat()}"
        )

    return id_str, expire_ts, export_limit


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


def _decode_wal_chunk(chunk, _S_H, _S_I, term_ids):
    """Decode a WAL entry data blob into term→doc_ids mapping.

    Reusable across mmap and bytes code paths.
    """
    buf_start = 0
    buf_end = len(chunk)
    while buf_start + 6 <= buf_end:
        term_len = _S_H.unpack_from(chunk, buf_start)[0]
        buf_start += 2
        if buf_start + term_len > buf_end:
            break
        term = chunk[buf_start:buf_start + term_len].decode("utf-8", errors="ignore")
        buf_start += term_len
        if buf_start + 4 > buf_end:
            break
        pl_len = _S_I.unpack_from(chunk, buf_start)[0]
        buf_start += 4
        if buf_start + pl_len > buf_end:
            break
        ids = decode_doclist(chunk[buf_start:buf_start + pl_len])
        buf_start += pl_len
        term_ids[term] = term_ids.get(term, []) + ids


_PREFIX_CACHE: dict = {}
_POSTING_CACHE_MAX = 8192   # max terms cached per QueryEngine instance

# Wildcard query safety caps
_MAX_WILDCARD_CANDIDATES = 500_000   # cap candidate expansion from trigram intersection
_MAX_VERIFIED_RESULTS   = 10_000   # stop verification early once we have this many matches

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
    def __init__(self, data_dir, storage: StorageAdapter | None = None):
        self.data_dir = os.path.abspath(data_dir) if not isinstance(data_dir, str) or not data_dir.startswith(("http://", "https://", "hf://")) else data_dir
        self.storage = storage or create_storage_adapter(path=self.data_dir)

        # Check if we're using URL storage adapter or FlatseekFile
        from flatseek.core.storage import URLStorageAdapter
        from flatseek.flatseek_file import FlatseekFileStorageAdapter
        is_url_storage = isinstance(self.storage, URLStorageAdapter)
        is_flatseek_file = isinstance(self.storage, FlatseekFileStorageAdapter)

        if is_flatseek_file:
            # FlatseekFile: files are stored inside the single file
            # Manifest section contains stats.json, column_map.json, manifest.json
            # Index/Docs/DV are accessed via offset tables
            self.index_dir = "index"  # FlatseekFile uses virtual paths
            self.docs_dir = "docs"
            stats_path = "stats.json"  # Virtual path to manifest stats

            if not self.storage.exists(stats_path):
                raise FileNotFoundError(
                    f"No stats found in {data_dir}. File may be corrupted.")
            self.stats = json.loads(self.storage.read_bytes(stats_path))
            self._sub_engines = None
            dirs = []
        elif is_url_storage:
            # URL storage: data_dir is the index name, storage handles URL resolution
            # data_dir can be:
            #   - "adsb" (single index in root folder of a multi-index repo)
            #   - "adsb" when base_url is root folder with multiple indexes
            #   - "parent/adsb" for nested index paths
            index_name = data_dir  # data_dir is the index name/label

            # Check if this is a direct index pattern (URL is a single index repo)
            # For direct index: storage.index_name == data_dir and files are at root level
            # For multi-index: files are in subdirs like adsb/stats.json
            from flatseek.core.storage import URLStorageAdapter
            storage_index_name = self.storage.index_name if isinstance(self.storage, URLStorageAdapter) else None
            is_direct_index = storage_index_name is not None and storage_index_name == index_name

            if is_direct_index:
                # Direct index: files are at root level (stats.json, index/, docs/)
                self.index_dir = "index"
                self.docs_dir = "docs"
                stats_path = "stats.json"
            else:
                # Multi-index folder or root-index: files are at index_name level
                self.index_dir = self.storage.join(index_name, "index")
                self.docs_dir = self.storage.join(index_name, "docs")
                stats_path = self.storage.join(index_name, "stats.json")

                # If stats.json doesn't exist at index_name level, try base_path level
                # This handles case where base_url points directly to an index folder
                if not self.storage.exists(stats_path):
                    if self.storage.base_path:
                        alt_stats = self.storage.join(self.storage.base_path, "stats.json")
                        if self.storage.exists(alt_stats):
                            stats_path = alt_stats
                            self.index_dir = self.storage.join(self.storage.base_path, "index")
                            self.docs_dir = self.storage.join(self.storage.base_path, "docs")

            if not self.storage.exists(stats_path):
                raise FileNotFoundError(
                    f"No index found at {self.storage.base_url}/{index_name}. "
                    f"Check that base_path is correct and stats.json exists.")

            self.stats = json.loads(self.storage.read_bytes(stats_path))
            self._sub_engines = None
            dirs = []
        else:
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
                if not os.path.exists(stats_path) and not self.storage.exists(stats_path):
                    raise FileNotFoundError(
                        f"No index found in {data_dir}. Run: flatseek build <csv_dir>")
                # If stats.json is encrypted (starts with FLATSEEK\x01 magic),
                # the bytes won't be valid JSON. Skip and let reload_stats() re-read
                # after the key is set via set_key().
                try:
                    self.stats = json.loads(self.storage.read_bytes(stats_path))
                except (json.JSONDecodeError, ValueError):
                    # Encrypted or malformed — stats will be loaded by reload_stats()
                    # once a key is provided via set_key()
                    self.stats = {}
            else:
                # ── Multi-index mode: root contains per-file sub-indexes ──────────
                self._sub_engines = [QueryEngine(d, storage) for d in dirs]
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
        self._hot_postings = {}    # term → sorted doc_id list (never evicted, hot fields)
        self._enc_key: bytes | None = None   # set via set_key() before querying encrypted indexes
        self._dv_cache: dict = {}   # field → doc_values data (lazy loaded)
        self._mmap_cache = {}        # path → mmap object (reusable across queries)
        self._term_set_cache = {}    # bucket_prefix → frozenset of all terms in bucket
        # Per-bin-file term->offset index (lazy). Maps term bytes → offset
        # into the (decrypted+decompressed) bucket data where the posting
        # list's length prefix begins. Lets `_read_posting` skip the
        # O(bucket_size) term scan on every call. Built once per file per
        # QE instance on first access; mem cost ~30 bytes per unique term.
        self._term_offset_cache: dict[str, dict[bytes, int]] = {}
        # WAL index: prefix → {wal_file: [offset, ...]} built lazily on first scan
        self._wal_index: dict[str, dict[str, list[int]]] = {}
        self._wal_index_built: set[str] = set()   # WAL files already indexed
        # WAL posting cache: prefix → {term: [doc_id, ...]} — persists across
        # queries. Initialized here (not in clear_doc_cache) so that
        # `_read_wal_postings` is safe to call on a fresh QE that hasn't
        # touched the doc cache yet — otherwise the line
        # `self._wal_posting_cache[prefix] = term_ids` raises
        # AttributeError the first time a search runs against an in-progress
        # build with live WAL files.
        self._wal_posting_cache: dict[str, dict[str, list[int]]] = {}
        # ── Lightweight profiling hooks (enable with env var) ──────────────────
        # Used by `_timed_phase`. Initialized here so `_timed_phase` works on
        # any fresh QE — previously these were set inside `clear_doc_cache`,
        # so calling `_timed_phase` on a QE that hadn't yet run any export
        # raised AttributeError.
        self._profile: bool = os.environ.get("FLATSEEK_PROFILE", "") == "1"
        self._phase_times: dict = {}   # phase → cumulative microseconds

        # TombstoneStore: only for local directory indexes (not .fsk which is read-only)
        self._tombstones = None
        if not is_flatseek_file and not is_url_storage:
            from flatseek.core.tombstone import TombstoneStore
            self._tombstones = TombstoneStore(self.data_dir)
            self._tombstones.replay_wal()

    def _alive_ids(self, doc_ids):
        """Return only non-deleted doc_ids, using tombstone store for O(1) lookup."""
        if self._tombstones is None:
            return list(doc_ids)
        return self._tombstones.filter_alive(list(doc_ids))

    def clear_doc_cache(self):
        """Drop all entries from `_doc_cache`. Intended for long-running
        pagination loops (e.g. streaming export) that touch many chunks
        across pages — without this, every chunk ever read stays in RAM
        forever and the process gets OOM-killed on big result sets.

        Safe to call between pagination pages: each page reads a fresh
        slice of `doc_ids`, so the chunks it loads are not the same as
        the previous page's chunks (no within-page cache reuse).

        Does NOT touch posting caches / hot postings / DV cache / WAL
        posting cache / profiling state — those are bounded separately
        and shouldn't be cleared on every page.
        """
        self._doc_cache.clear()

    def _timed_phase(self, name):
        """Context manager for profiling a phase. Enable with FLATSEEK_PROFILE=1."""
        class _PhaseTimer:
            __slots__ = ("name", "_t0", "_profile", "_phase_times")
            def __init__(self, name, profile, phase_times):
                self.name = name
                self._profile = profile
                self._phase_times = phase_times
                self._t0 = None
            def __enter__(self):
                if self._profile:
                    self._t0 = _time.perf_counter()
                return self
            def __exit__(self, *args):
                if self._profile and self._t0 is not None:
                    elapsed = (_time.perf_counter() - self._t0) * 1_000_000
                    self._phase_times[self.name] = self._phase_times.get(self.name, 0) + elapsed
        return _PhaseTimer(name, self._profile, self._phase_times)

    def reload_stats(self):
        """Re-read stats.json from disk and refresh sizes if needed.

        stats.json is stale during ongoing builds (not updated by WAL flush).
        Recalculate sizes from disk so serve shows accurate numbers during a build.
        For completed builds, stats.json is authoritative and we skip the disk walk.
        """
        import json
        from flatseek.core.storage import LocalStorageAdapter
        from flatseek.flatseek_file import FlatseekFileStorageAdapter
        # For .fsk sources, stats.json is at the root of the archive
        # (data_dir is the virtual "index" dir, not where stats live).
        # For directory sources, stats.json is at the data_dir root.
        if isinstance(self.storage, FlatseekFileStorageAdapter):
            stats_path = "stats.json"
        else:
            stats_path = os.path.join(self.data_dir, "stats.json")
        if self.storage.exists(stats_path):
            raw = self.storage.read_bytes(stats_path)
            # Decrypt if encrypted (starts with FLATSEEK\x01 magic).
            # InvalidKey / corrupted data → re-raise so the route (wrapped in
            # asyncio.to_thread) catches it and returns 401.
            if self._enc_key and is_encrypted(raw):
                raw = decrypt_bytes(raw, self._enc_key)
            self.stats = json.loads(raw)

        # Only recalculate sizes during active builds. Detect active build by
        # presence of WAL files (even if _wal_merged.txt exists from a previous
        # session, new WAL files mean a new build is in progress).
        if isinstance(self.storage, LocalStorageAdapter):
            wal_dir = os.path.join(self.data_dir, "_wal")
            has_wals = False
            try:
                has_wals = any(f.endswith(".wal") for f in os.listdir(wal_dir))
            except OSError:
                pass
            if has_wals:
                index_size = 0
                docs_size = 0
                for root, _, files in os.walk(self.index_dir):
                    for f in files:
                        index_size += os.path.getsize(os.path.join(root, f))
                for root, _, files in os.walk(self.docs_dir):
                    for f in files:
                        docs_size += os.path.getsize(os.path.join(root, f))
                self.stats["index_size_mb"] = round(index_size / 1024 ** 2, 1)
                self.stats["docs_size_mb"] = round(docs_size / 1024 ** 2, 1)
                self.stats["total_size_mb"] = round(
                    self.stats["index_size_mb"] + self.stats["docs_size_mb"], 1
                )

    def set_key(self, key: bytes):
        """Supply the decryption key for an encrypted index.

        Must be called before the first search on an encrypted index.
        For multi-index mode, propagates to all sub-engines.
        """
        self._enc_key = key
        # Propagate to FlatseekFileStorageAdapter if present
        from flatseek.flatseek_file import FlatseekFileStorageAdapter
        if isinstance(self.storage, FlatseekFileStorageAdapter):
            self.storage.set_key(key)
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

    def _mmap_read(self, rel_path: str) -> bytes:
        """Read file contents via storage adapter.

        For local: uses mmap when possible (OS page cache for hot files).
        For remote (S3, Vercel Blob): falls back to direct read.
        """
        try:
            # For local storage, try mmap for hot file performance
            # For remote storage (S3, Blob), mmap doesn't apply, use read_bytes
            from flatseek.core.storage import LocalStorageAdapter
            if isinstance(self.storage, LocalStorageAdapter):
                path = self.storage._resolve(rel_path)
                cached = self._mmap_cache.get(path)
                if cached is not None:
                    return cached
                size = os.path.getsize(path)
                if size == 0:
                    return b""
                with open(path, "rb") as f:
                    mm = _mmap.mmap(f.fileno(), size, access=_mmap.ACCESS_READ)
                self._mmap_cache[path] = mm
                return mm[:]
        except Exception:
            pass
        # Universal fallback: use storage adapter (works for all backends)
        return self.storage.read_bytes(rel_path)

    def _load_bucket_term_set(self, prefix):
        """Load the pickled term set for a bucket (fast-reject cache).

        Returns a frozenset of all terms indexed in the bucket, or None if
        the terms.set file does not exist (old index without term sets).
        """
        if prefix in self._term_set_cache:
            return self._term_set_cache[prefix]

        import pickle
        bucket_dir = os.path.join(self.index_dir, prefix)
        set_path = os.path.join(bucket_dir, "terms.set")
        try:
            if self.storage.exists(set_path):
                data = self.storage.read_bytes(set_path)
                terms = frozenset(pickle.loads(data))
                self._term_set_cache[prefix] = terms
                return terms
        except Exception:
            pass
        # Mark as no-set so we don't re-try every call
        self._term_set_cache[prefix] = None
        return None

    # ─── WAL (Write-Ahead Log) reader ─────────────────────────────────────────

    def _read_wal_postings(self, prefix):
        """Read all WAL files and return {term: [doc_ids]} for a bucket prefix.

        WAL format (written by builder._write_pressure_wal):
          [5 bytes  : prefix string "xx/yy"          ]
          [4 bytes  : uint32 data length              ]
          [N bytes  : raw binary posting data          ]
            ↳ same binary format as idx.bin entries:
              [2 bytes term_len][N bytes term][4 bytes pl_len][delta-varint doc_ids]

        This enables search on ongoing builds where the index hasn't been
        checkpointed yet — the inverted index is still in WAL buffers.
        """
        # ── Fast path: return cached postings for this prefix ───────────────────
        # After the first scan of each WAL file, the posting data is cached here.
        # Subsequent queries on the same prefix hit this cache instantly.
        #
        # Cache safety: only use cache when ALL WAL files have been indexed
        # (meaning _wal_index_built has every .wal file).  During an active build,
        # new WAL files appear continuously; using the cache would return stale
        # results missing the newest entries.
        _wal_dir = os.path.join(self.data_dir, "_wal")
        _all_indexed = True
        try:
            _current_wals = set(f for f in os.listdir(_wal_dir) if f.endswith(".wal"))
            if _current_wals - self._wal_index_built:
                _all_indexed = False
        except (OSError, PermissionError):
            _all_indexed = False

        if _all_indexed and prefix in self._wal_posting_cache:
            return self._wal_posting_cache[prefix]

        # ── Slow path: scan WAL files lazily ─────────────────────────────────────
        # Each WAL file is scanned once, building an in-memory index of
        # prefix → [(offset, data_len), ...].  After indexing, we read and
        # decode only the entries matching |prefix|.
        # Subsequent queries reuse the index without re-scanning.
        #
        # If a persisted index exists (_wal_index.json), load it to skip the
        # full scan on cold-start sessions.
        from flatseek.core.storage import LocalStorageAdapter
        if not isinstance(self.storage, LocalStorageAdapter):
            return {}

        wal_dir = os.path.join(self.data_dir, "_wal")
        if not os.path.isdir(wal_dir):
            return {}

        # Skip WAL files already merged to the checkpointed index
        merged_path = os.path.join(self.data_dir, "_wal_merged.txt")
        try:
            merged_wal: set = set()
            with open(merged_path, "r") as f:
                for line in f:
                    name = line.strip()
                    if name.endswith(".wal"):
                        merged_wal.add(name)
        except (OSError, FileNotFoundError):
            merged_wal = set()

        try:
            wal_files = sorted(f for f in os.listdir(wal_dir) if f.endswith(".wal"))
        except (OSError, PermissionError):
            return {}

        if not wal_files:
            return {}

        term_ids: dict = {}
        _S_H = struct.Struct("<H")
        _S_I = struct.Struct("<I")
        _S_5sI = struct.Struct("<5sI")

        for wal_file in wal_files:
            if wal_file in merged_wal:
                continue  # Already in checkpointed index

            wal_path = os.path.join(wal_dir, wal_file)

            # ── Pass 1: build prefix index (one-time, reads full file) ─────────────
            if wal_file not in self._wal_index_built:
                try:
                    with open(wal_path, "rb") as f:
                        data = f.read()
                except (OSError, PermissionError, EOFError):
                    continue
                idx: dict[str, list[tuple]] = {}
                i = 0
                while i + 9 <= len(data):
                    wal_prefix_bytes, data_len = _S_5sI.unpack_from(data, i)
                    wal_prefix = wal_prefix_bytes.decode("utf-8", errors="ignore")
                    i += 9
                    if i + data_len > len(data):
                        break  # truncated
                    if data_len > 0:  # skip zero-length padding entries
                        idx.setdefault(wal_prefix, []).append((i, data_len))
                    i += data_len
                self._wal_index[wal_file] = idx
                self._wal_index_built.add(wal_file)
                # Decode using the same data we just read (no extra I/O)
                chunk_offsets = idx.get(prefix)
                if chunk_offsets:
                    for entry_start, dl in chunk_offsets:
                        chunk = data[entry_start:entry_start + dl]
                        _decode_wal_chunk(chunk, _S_H, _S_I, term_ids)
                continue

            # ── Pass 2: for already-indexed files, use mmap for selective reads ──
            offsets = self._wal_index.get(wal_file, {}).get(prefix)
            if not offsets:
                continue

            mmap_key = wal_path
            cached = self._mmap_cache.get(mmap_key)
            if cached is None:
                try:
                    fh = open(wal_path, "rb")
                    m = mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)
                    self._mmap_cache[mmap_key] = (m, fh)
                    cached = (m, fh)
                except (OSError, PermissionError, ValueError):
                    cached = None  # force bytes fallback below

            if cached is not None:
                mmap_obj, fh = cached
                for entry_start, dl in offsets:
                    chunk = mmap_obj[entry_start:entry_start + dl]
                    _decode_wal_chunk(chunk, _S_H, _S_I, term_ids)
            else:
                try:
                    with open(wal_path, "rb") as f:
                        data = f.read()
                    for entry_start, dl in offsets:
                        chunk = data[entry_start:entry_start + dl]
                        _decode_wal_chunk(chunk, _S_H, _S_I, term_ids)
                except (OSError, PermissionError):
                    continue

        # Cache so the next query on this prefix is instant
        self._wal_posting_cache[prefix] = term_ids
        return term_ids

    # ─── Index file lookup ────────────────────────────────────────────────────

    def _read_posting(self, term):
        """Read posting list for an exact term from the index.

        Reads ALL *.bin files in the bucket directory — supports both single-builder
        (idx.bin) and distributed/parallel builds (idx_w0.bin, idx_w1.bin, …).
        Merges all doc_id lists into a single sorted, deduplicated result.

        Also reads WAL files from _wal/ to support search on ongoing builds
        where the inverted index hasn't been checkpointed to disk yet.

        Uses mmap for hot files (OS page cache), _hot_postings for
        never-evicted persistent cache, and bucket term sets for O(1) fast-reject.
        """
        # Hot postings: never evicted, common filter terms stay cached
        hot = self._hot_postings.get(term)
        if hot is not None:
            return hot

        prefix = term_hash(term)
        bucket_dir = os.path.join(self.index_dir, prefix)

        from flatseek.core.storage import LocalStorageAdapter
        bucket_exists = (isinstance(self.storage, LocalStorageAdapter)
                         and (self.storage.exists(bucket_dir) or os.path.isdir(bucket_dir)))

        # Fast reject: if bucket has a term-set file and term is not in it,
        # skip all posting file reads entirely (2x–4x speedup for non-existent terms).
        # NOTE: term_set covers only checkpointed index data — WAL data may have terms
        # not yet in term_set, so fast-reject only applies when bucket exists (has index).
        term_set = self._load_bucket_term_set(prefix) if bucket_exists else None
        if term_set is not None and term not in term_set:
            return []

        # Use storage adapter for listing if remote, otherwise fall back to os.listdir
        from flatseek.core.storage import LocalStorageAdapter, URLStorageAdapter
        if isinstance(self.storage, LocalStorageAdapter):
            if bucket_exists:
                bin_files = sorted(f for f in os.listdir(bucket_dir) if f.endswith(".bin"))
            else:
                bin_files = []
        elif isinstance(self.storage, URLStorageAdapter):
            # URL mode optimization: skip listdir entirely.
            # We already know bucket_dir from term_hash, so directly check
            # the known bin file patterns. This avoids an HTTP Tree API call
            # that lists all files when we only need a few specific ones.
            bin_files = []
            for pattern in ("idx_w0.bin", "idx_w1.bin", "idx_w2.bin", "idx_w3.bin",
                           "idx.bin", "idx_w4.bin", "idx_w5.bin", "idx_w6.bin",
                           "idx_w7.bin"):
                if self.storage.exists(os.path.join(bucket_dir, pattern)):
                    bin_files.append(pattern)
        else:
            bin_files = sorted(f for f in self.storage.listdir(bucket_dir) if f.endswith(".bin"))

        # For non-URL storage, listdir may return empty too (e.g. empty bucket dir)
        # Check known patterns as fallback only for non-URL adapters
        if not bin_files and not isinstance(self.storage, URLStorageAdapter):
            for pattern in ("idx_w0.bin", "idx_w1.bin", "idx_w2.bin", "idx_w3.bin",
                           "idx.bin", "idx_w4.bin", "idx_w5.bin", "idx_w6.bin",
                           "idx_w7.bin"):
                bin_path = os.path.join(bucket_dir, pattern)
                if self.storage.exists(bin_path):
                    bin_files.append(pattern)

        # ── Merge WAL postings (for ongoing builds only) ─────────────────────────
        # WAL files contain unflushed index buffers from in-progress builds.
        # Once _wal_merged.txt exists, all WAL data has been checkpointed → skip WAL.
        # Skip WAL entirely when the merged marker exists (build is complete).
        wal_ids: list = []
        merged_marker = os.path.join(self.data_dir, "_wal_merged.txt")
        if not os.path.isfile(merged_marker):
            wal_ids = self._read_wal_postings(prefix).get(term, [])

        # Check bounded LRU cache
        cached = self._posting_cache.get(term)
        if cached is not None:
            return cached

        all_ids = []
        # NOTE: `_build_term_offset_index` infrastructure exists for the
        # O(bucket_size)-scan elimination optimization but is NOT used here
        # because a user reported a silent regression where one specific
        # keyword returned 0 results after the switch (other keywords fine,
        # synthetic data unaffected). Until the root cause is debugged against
        # the user's actual index, this path is reverted to the original
        # linear scan which fails loudly on file errors (decrypt/decompress
        # failures propagate, don't silently disappear).
        for bin_file in bin_files:
            bin_path = os.path.join(bucket_dir, bin_file)
            data = self._mmap_read(bin_path)
            if not data:
                continue
            # Decrypt if encrypted (must happen before decompression)
            data = self._decrypt_if_needed(data)
            # Auto-detect zlib compression
            if len(data) >= 2 and data[0] == 0x78 and data[1] in (0x01, 0x5e, 0x9c, 0xda):
                data = zlib.decompress(data)

            offset = 0
            while offset + 2 <= len(data):
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

        all_ids.extend(wal_ids)

        result = sorted(set(all_ids)) if all_ids else []

        # ── Promote to hot cache ───────────────────────────────────────────────
        # Promote to hot cache if term looks like a common filter (column:value with
        # common column names).  Terms with very large posting lists are excluded.
        _COMMON_COLUMNS = frozenset({"status", "type", "program", "service", "level",
                                      "country", "region", "city", "active", "enabled"})
        col_part = term.split(":", 1)[0].lower() if ":" in term else ""
        if result and (col_part in _COMMON_COLUMNS or len(result) < 50_000):
            self._hot_postings[term] = result
        else:
            # Bounded LRU cache for everything else
            pc = self._posting_cache
            if len(pc) >= _POSTING_CACHE_MAX:
                keep = _POSTING_CACHE_MAX // 2
                for old_key in list(pc)[:len(pc) - keep]:
                    del pc[old_key]
            pc[term] = result
        return result

    def _build_term_offset_index(self, bin_path: str) -> dict[bytes, int]:
        """Build (or fetch cached) per-file term→offset index for a bucket bin file.

        The bin file format is a sequence of:
            [term_len: u16][term: term_len bytes][pl_len: u32][pl_data: pl_len bytes]
        repeated. To find a term without scanning, we parse the file once and
        store `{term_bytes: offset_of_pl_len_start}`. Offset points to the
        start of the `pl_len` u32 (right after the term bytes), so the lookup
        path can `struct.unpack_from("<I", data, offset)` directly to get
        the posting list length and then read pl_data.

        Lazily built on first `_read_posting` access against this file.
        Cached for the lifetime of the QueryEngine (or until the file
        changes on disk). Memory cost is O(unique terms in file), typically
        ~30 bytes per entry — a bucket with 1000 unique terms ≈ 30 KB.

        Built against the **decrypted + decompressed** data — offsets are
        not stable across decryption+decompression so we have to do those
        steps here too. But the index itself is just ints, ~30 bytes each,
        cheap to keep around.
        """
        cached = self._term_offset_cache.get(bin_path)
        if cached is not None:
            return cached

        data = self._mmap_read(bin_path)
        if not data:
            self._term_offset_cache[bin_path] = {}
            return {}

        data = self._decrypt_if_needed(data)
        if len(data) >= 2 and data[0] == 0x78 and data[1] in (0x01, 0x5e, 0x9c, 0xda):
            try:
                data = zlib.decompress(data)
            except Exception:
                self._term_offset_cache[bin_path] = {}
                return {}

        offsets: dict[bytes, int] = {}
        offset = 0
        n = len(data)
        _unpack_h = struct.Struct("<H").unpack_from
        _unpack_i = struct.Struct("<I").unpack_from
        while offset + 2 <= n:
            try:
                term_len = _unpack_h(data, offset)[0]
            except struct.error:
                break
            offset += 2
            if offset + term_len > n:
                break
            term = bytes(data[offset:offset + term_len])
            offset += term_len
            if offset + 4 > n:
                break
            try:
                pl_len = _unpack_i(data, offset)[0]
            except struct.error:
                break
            # Offset points to where pl_len starts — lookup reads it directly.
            offsets[term] = offset
            offset += 4 + pl_len

        self._term_offset_cache[bin_path] = offsets
        return offsets

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
            with self._timed_phase("verify_wildcard"):
                for cs in sorted(by_chunk):
                    chunk = load_chunk(cs)
                    for doc_id in by_chunk[cs]:
                        val = _get_nested_value(chunk.get(doc_id) or {}, column) or ""
                        if val and rx_search(str(val).lower()):
                            verified.append(doc_id)
                            if len(verified) >= _MAX_VERIFIED_RESULTS:
                                return verified
        else:
            with self._timed_phase("verify_wildcard"):
                for cs in sorted(by_chunk):
                    chunk = load_chunk(cs)
                    for doc_id in by_chunk[cs]:
                        doc = chunk.get(doc_id)
                        if not doc:
                            continue
                        for val in doc.values():
                            if isinstance(val, str) and rx_search(val.lower()):
                                verified.append(doc_id)
                                break
                        if len(verified) >= _MAX_VERIFIED_RESULTS:
                            return verified
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
        # Also handle empty string (search("")) as match-all for sorted queries
        if not clean:
            total = self.stats.get("total_docs", 0)
            if max_docs is not None:
                total = min(total, max_docs)
            return list(range(total))

        # Auto-wildcard: bare term with @ or . (e.g. "gmail.com", "user@") →
        # treat as partial match so "@gmail.com" finds all emails containing it
        # without requiring user to type *gmail.com*
        if not column and not has_wildcard and ("@" in term or "." in term):
            has_wildcard = True
            clean = f"*{clean}*"

        col_prefix = f"{column}:" if column else ""

        # Date field shortcuts: dob:month=11, dob:year=1997
        # Resolves to YYYYMMDD range so existing DATE-stored values work without rebuild.
        if not has_wildcard:
            _DATE_MONTH_SHORTCUT = re.match(r"^month=(\d{1,2})$", clean)
            _DATE_YEAR_SHORTCUT  = re.match(r"^year=(\d{4})$", clean)
            if column and _DATE_MONTH_SHORTCUT:
                m = int(_DATE_MONTH_SHORTCUT.group(1))
                if 1 <= m <= 12:
                    _MONTH_DAYS = [0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
                    lo = f"0000{m:02d}01"
                    hi = f"9999{m:02d}{_MONTH_DAYS[m]:02d}"
                    return self._resolve_range(column, "between", lo, hi)
            if column and _DATE_YEAR_SHORTCUT:
                y = int(_DATE_YEAR_SHORTCUT.group(1))
                lo = f"{y}0101"
                hi = f"{y}1231"
                return self._resolve_range(column, "between", lo, hi)

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
            # Extract trigrams from the raw segment as-is (matching what the builder
            # does via make_trigrams).  Stripping non-alphanum chars would cause a
            # mismatch: "dev.io" → indexed ["dev","ev.","v.i",".io"] but query
            # ["dev","evi","vio"] — "evi" never appears in the index.
            # Non-alphanum trigrams (e.g. "ev." from "dev.io") are fine; they
            # still appear in the index and _verify_wildcard eliminates false
            # positives via exact stored-value matching.
            _seg_raw = _seg.lower()
            for _i in range(len(_seg_raw) - 2):
                _tg = _seg_raw[_i:_i+3]
                if _tg not in _seen_tg:
                    _seen_tg.add(_tg)
                    tgs.append(_tg)

        # Email/domain patterns (contain @ or .): skip trigram intersection entirely.
        # Trigrams break on dots/@, so "gmail" and "com" are matched as separate
        # words instead of one substring — leads to false-negative or tiny results.
        # Fall straight to _wildcard_fallback for accurate substring matching.
        _skip_trigrams = not column and bool(re.search(r"[@.]", clean.lstrip("*%")))

        if not tgs:
            # All literal segments are < 3 chars — no trigrams available.
            # Fall back to exact lookup only when there are no internal wildcards
            # (i.e. pattern like "bu*" → look for token "bu").  Patterns with
            # internal wildcards like "a%b" are unsupported at this length.
            if '%' in clean or '*' in clean:
                return []
            ids = self._read_posting(f"{col_prefix}{clean}")
            return sorted(ids)

        if _skip_trigrams:
            # Email-like pattern: trigrams can't handle dot-separated substrands.
            # Go straight to full bucket scan.
            return self._wildcard_fallback(clean, column, has_wildcard, col_prefix)

        if column:
            # Column-scoped wildcard: read all trigram posting lists, then intersect
            # smallest→largest so rare trigrams prune the candidate set early.
            tg_lists = []
            for tg in tgs:
                ids = self._read_posting(f"{col_prefix}~{tg}")
                if not ids:
                    # No trigram index OR intersection empty — try wildcard fallback
                    # for email-like patterns (dots/@ break trigram consecutive matching)
                    if re.search(r"[@.]", clean.lstrip("*")):
                        return self._wildcard_fallback(clean, column, has_wildcard, col_prefix)
                    return []
                tg_lists.append(ids)
            tg_lists.sort(key=len)   # rarest (smallest) first → fastest intersection
            result = set(tg_lists[0])
            for ids in tg_lists[1:]:
                result &= set(ids)
                if not result:
                    return []
            candidates = sorted(result)
            # Safety cap: if too many candidates, fall back to prefix/suffix expansion
            if len(candidates) > _MAX_WILDCARD_CANDIDATES:
                return self._wildcard_fallback(clean, column, has_wildcard, col_prefix)
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
            # If trigram intersection is empty but term has dots/@ (email-like),
            # fall back to regex scan — trigrams fail because "gmail" and "com"
            # are separated by a dot, breaking consecutive char matching.
            if not candidates and (re.search(r"[@.]", clean.lstrip("*"))):
                return self._wildcard_fallback(clean, column, has_wildcard, col_prefix)
            if len(candidates) > _MAX_WILDCARD_CANDIDATES:
                return self._wildcard_fallback(clean, column, has_wildcard, col_prefix)

        # Post-filter: verify candidates actually match the full wildcard pattern.
        # Trigram intersection eliminates most non-matches but can produce false
        # positives when trigrams appear in a value but not consecutively
        # (e.g. "RAYDIUMV2" contains 'ray' and 'diu' but not the substring "garuda").
        return self._verify_wildcard(candidates, term, column)

    # ─── Wildcard fallback (safety cap) ─────────────────────────────────────────

    def _wildcard_fallback(self, clean, column, has_wildcard, col_prefix):
        """Fallback for broad wildcards: use prefix/suffix expansion with early terminate.

        When trigram candidate set exceeds _MAX_WILDCARD_CANDIDATES, this path
        avoids O(N) verification by only expanding from index terms that directly
        match the prefix/suffix pattern — much fewer candidates, sorted by
        postings size so rare terms are checked first.

        Before: O(N docs) verification scan (all candidates loaded from disk)
        After:  O(T index terms matching pattern) — T << N typically
        """
        starts_wild = clean.startswith(('%', '*'))
        ends_wild   = clean.endswith(('%', '*'))
        # Strip anchors for matching against indexed terms
        prefix = clean.lstrip('%*')
        prefix = prefix.rstrip('%*')

        # Email/domain patterns (contain @ or .): treat as both-side wildcard
        # even without explicit * so "gmail.com" matches "user@gmail.com".
        if not starts_wild and not ends_wild and re.search(r"[@.]", prefix):
            starts_wild = ends_wild = True

        if not prefix:
            return []

        # Build a regex that matches indexed tokens (no .* on edges since we're
        # only checking the token content, not requiring full anchored match)
        escaped = re.escape(prefix)
        if starts_wild and ends_wild:
            rx = re.compile(f".*{escaped}.*", re.IGNORECASE)
        elif starts_wild:
            rx = re.compile(f".*{escaped}", re.IGNORECASE)
        elif ends_wild:
            rx = re.compile(f"^{escaped}", re.IGNORECASE)  # token must start with prefix
        else:
            rx = re.compile(f"^{escaped}$", re.IGNORECASE)

        # Collect matching terms by scanning the postings index.
        # This is expensive (reads all bucket dirs), so we limit to top matches.
        all_ids: set = set()
        checked_terms = 0
        max_terms_check = 100_000  # safety limit on index term scanning

        if column:
            cols_to_scan = [column]
        else:
            cols_to_scan = list(self.columns())

        # For email-like patterns (@ or .), per-column bucket scanning misses terms
        # because "email:gmail.com" is stored in bucket(term_hash("email:gmail.com"))
        # = "28/c2", NOT in bucket(term_hash("email")) = "e7/92".  Instead, scan
        # ALL buckets so every indexed term is checked against the pattern.
        _scan_all = not column and bool(re.search(r"[@.]", clean.lstrip("*%")))

        if _scan_all:
            # Wildcard email/domain scan: iterate all first-level buckets.
            # This is slower but necessary for email/domain partial matching.
            for root, dirs, files in os.walk(self.index_dir):
                if checked_terms >= max_terms_check:
                    break
                for bin_file in files:
                    if not bin_file.endswith(".bin"):
                        continue
                    if checked_terms >= max_terms_check:
                        break
                    bin_path = os.path.join(root, bin_file)
                    try:
                        data = self._mmap_read(bin_path)
                        if not data:
                            continue
                        if is_encrypted(data):
                            try:
                                data = self._decrypt_if_needed(data)
                            except Exception:
                                continue
                        if len(data) >= 2 and data[0] == 0x78 and data[1] in (0x01, 0x5e, 0x9c, 0xda):
                            data = zlib.decompress(data)
                    except Exception:
                        continue
                    offset = 0
                    while offset + 2 <= len(data) and checked_terms < max_terms_check:
                        term_len = struct.unpack_from("<H", data, offset)[0]; offset += 2
                        if offset + term_len > len(data):
                            break
                        try:
                            stored = data[offset:offset + term_len].decode("utf-8"); offset += term_len
                        except UnicodeDecodeError:
                            offset += term_len
                            if offset + 4 > len(data):
                                break
                            pl_len = struct.unpack_from("<I", data, offset)[0]; offset += 4
                            continue
                        if offset + 4 > len(data):
                            break
                        pl_len = struct.unpack_from("<I", data, offset)[0]; offset += 4
                        checked_terms += 1
                        # Match against the full stored term (includes column prefix)
                        if rx.match(stored):
                            all_ids.update(decode_doclist(data[offset:offset + pl_len]))
                            if len(all_ids) > _MAX_VERIFIED_RESULTS:
                                return sorted(all_ids)[:_MAX_VERIFIED_RESULTS]
                        offset += pl_len
            return sorted(all_ids)

        for col_key in cols_to_scan:
            if checked_terms >= max_terms_check:
                break
            prefix_hash = term_hash(col_key)
            bucket_dir = os.path.join(self.index_dir, prefix_hash)
            from flatseek.core.storage import LocalStorageAdapter
            if isinstance(self.storage, LocalStorageAdapter):
                if not self.storage.exists(bucket_dir) and not os.path.isdir(bucket_dir):
                    continue
            else:
                if not self.storage.exists(bucket_dir):
                    continue
            # Use storage adapter for listing if remote
            from flatseek.core.storage import LocalStorageAdapter
            if isinstance(self.storage, LocalStorageAdapter):
                bin_files = sorted(f for f in os.listdir(bucket_dir) if f.endswith(".bin"))
            #else:
            #    bin_files = sorted(f for f in self.storage.listdir(bucket_dir) if f.endswith(".bin"))

            # For URL storage, try common file patterns directly if listdir returns empty
            if not bin_files:
                for pattern in ["idx.bin", "idx_w0.bin", "idx_w1.bin", "idx_w2.bin", "idx_w3.bin"]:
                    bin_path = os.path.join(bucket_dir, pattern)
                    if self.storage.exists(bin_path):
                        bin_files.append(pattern)

            for bin_file in bin_files:
                if checked_terms >= max_terms_check:
                    break
                try:
                    bin_path = os.path.join(bucket_dir, bin_file)
                    data = self._mmap_read(bin_path)
                    if not data:
                        continue
                    # Handle encrypted index files
                    if is_encrypted(data):
                        try:
                            data = self._decrypt_if_needed(data)
                        except Exception:
                            continue
                    if len(data) >= 2 and data[0] == 0x78 and data[1] in (0x01, 0x5e, 0x9c, 0xda):
                        data = zlib.decompress(data)
                except Exception:
                    continue
                offset = 0
                while offset + 2 <= len(data) and checked_terms < max_terms_check:
                    term_len = struct.unpack_from("<H", data, offset)[0]; offset += 2
                    if offset + term_len > len(data):
                        break
                    try:
                        stored = data[offset:offset + term_len].decode("utf-8"); offset += term_len
                    except UnicodeDecodeError:
                        # Corrupted or encrypted entry — skip this entry
                        offset += term_len
                        if offset + 4 > len(data):
                            break
                        pl_len = struct.unpack_from("<I", data, offset)[0]; offset += 4
                        continue
                    if offset + 4 > len(data):
                        break
                    pl_len = struct.unpack_from("<I", data, offset)[0]; offset += 4
                    checked_terms += 1
                    # Only check column-scoped terms
                    if not stored.startswith(f"{col_key}:"):
                        continue
                    token = stored[len(col_key) + 1:]
                    if rx.match(token):
                        all_ids.update(decode_doclist(data[offset:offset + pl_len]))
                        if len(all_ids) > _MAX_VERIFIED_RESULTS:
                            # Early termination: we have enough results
                            return sorted(all_ids)[:_MAX_VERIFIED_RESULTS]
                    offset += pl_len

        return sorted(all_ids)

    # ─── Doc fetching ─────────────────────────────────────────────────────────

    def _chunk_start(self, doc_id):
        return (doc_id // self.doc_chunk_size) * self.doc_chunk_size

    def _list_docs_recursive(self, prefix: str = "") -> list[str]:
        """Recursively list all doc files under docs_dir for remote storage.

        S3/Blob don't have glob, so we implement recursive listing via
        listdir with pagination. For URL storage, we try common patterns
        directly since listdir may return empty.
        """
        results = []
        to_visit = [prefix] if prefix else [""]
        while to_visit:
            current = to_visit.pop(0)
            entries = self.storage.listdir(current)
            for entry in entries:
                full_path = self.storage.join(current, entry) if current else entry
                # Check if it's a file (ends with .zlib)
                if full_path.endswith(".zlib"):
                    results.append(full_path)
                else:
                    # It's a directory, add to visit queue
                    to_visit.append(full_path)

        # For URL storage (HuggingFace, GitHub), listdir returns empty.
        # Try common doc chunk patterns directly.
        if not results:
            from flatseek.core.storage import URLStorageAdapter
            if isinstance(self.storage, URLStorageAdapter):
                # Try standard 2-level hex path pattern: docs/{aa}/{bb}/docs_{N:010d}.zlib
                # Chunk size is typically 100000, check first few chunks
                for chunk_start in range(0, 1000000, self.doc_chunk_size):
                    aa = f"{(chunk_start // 256 // 256) & 0xFF:02x}"
                    bb = f"{(chunk_start // 256) % 256:02x}"
                    doc_path = f"{self.docs_dir}/{aa}/{bb}/docs_{chunk_start:010d}.zlib"
                    if self.storage.exists(doc_path):
                        results.append(doc_path)
                    # Also try flat pattern for backwards compat
                    flat_path = f"{self.docs_dir}/docs_{chunk_start:010d}.zlib"
                    if self.storage.exists(flat_path):
                        results.append(flat_path)

        return results

    def _iter_chunks(self, match_chunks=None):
        """Yield (chunk_start, chunk_dict) for every chunk file in docs_dir.

        Uses glob to find files regardless of naming scheme — handles both
        sequential (docs_0000000000.zlib) and sparse/gapped chunk numbering.

        Args:
            match_chunks: Optional set of chunk_start values to KEEP. Chunks
                whose chunk_start is NOT in this set are skipped BEFORE the
                read (so encrypted / single-file storage adapters don't pay
                the decrypt cost for empty chunks). None = no filter.
                Adapters that override _iter_chunks must honor this filter.
        """
        from flatseek.core.storage import LocalStorageAdapter

        # Delegate to storage adapter if it provides its own _iter_chunks
        # (e.g. FlatseekFileStorageAdapter overrides this for single-file mode)
        if hasattr(self.storage, "_iter_chunks"):
            # Adapter may or may not support match_chunks. Detect via signature.
            import inspect as _inspect
            try:
                params = _inspect.signature(self.storage._iter_chunks).parameters
                if "match_chunks" in params:
                    yield from self.storage._iter_chunks(match_chunks=match_chunks)
                else:
                    yield from self.storage._iter_chunks()
            except (TypeError, ValueError):
                # Built-in / C-implemented method — fall back to no filter
                yield from self.storage._iter_chunks()
            return

        # For local storage, use fast glob. For remote, use storage.listdir
        if isinstance(self.storage, LocalStorageAdapter):
            pattern = os.path.join(self.docs_dir, "**", "*.zlib")
            files = _glob.glob(pattern, recursive=True)
            if not files:
                pattern2 = os.path.join(self.docs_dir, "chunks_*.zlib")
                files = _glob.glob(pattern2)
        else:
            # Remote storage: recursive listdir
            files = self._list_docs_recursive()

        if not files:
            logger = logging.getLogger(__name__)
            logger.warning(f"_iter_chunks: no .zlib files found in {self.docs_dir}")
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
                # Skip BEFORE read — saves I/O + decrypt for empty chunks.
                if match_chunks is not None and chunk_start not in match_chunks:
                    continue
                blob = self.storage.read_bytes(path)
                blob = self._decrypt_if_needed(blob)
                raw = _decompress_doc(blob)
                chunk = {int(k): v for k, v in _doc_loads(raw).items()}
                yield chunk_start, chunk
            except Exception as e:
                # Wrong key (InvalidToken from decrypt_bytes) → propagate so
                # callers get 401.  Other errors (bad file, decompress failure)
                # → silently skip this chunk.
                try:
                    from cryptography.fernet import InvalidToken
                except ImportError:
                    InvalidToken = None
                if InvalidToken is not None and isinstance(e, InvalidToken):
                    raise

    def _find_first_chunk_path(self):
        """Return the path of the first chunk file, or None if no chunks."""
        import glob as _glob
        pattern = os.path.join(self.docs_dir, "**", "*.zlib")
        files = _glob.glob(pattern, recursive=True)
        if not files:
            pattern2 = os.path.join(self.docs_dir, "chunks_*.zlib")
            files = _glob.glob(pattern2)
        if not files:
            return None

        def _chunk_num(p):
            m = re.search(r"docs_(\d+)", p.replace("\\", "/"))
            if not m:
                m = re.search(r"chunks_(\d+)", p.replace("\\", "/"))
            if not m:
                m = re.match(r"(\d+)", os.path.basename(p))
            return int(m.group(1)) if m else 0

        return sorted(files, key=_chunk_num)[0]

    def _scan_doc_ids(self, max_ids=None):
        """Iterate chunk files and yield actual stored doc IDs in order.

        Handles gaps from deduplication where IDs are not consecutive.
        Yields up to max_ids IDs (None = unlimited). Skips deleted docs.
        """
        collected = 0
        for chunk_start, chunk in self._iter_chunks():
            for doc_id in sorted(chunk):
                if self._tombstones and self._tombstones.is_deleted(doc_id):
                    continue
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
                # Skip deleted docs
                if self._tombstones and self._tombstones.is_deleted(doc_id):
                    continue
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

    def _load_chunk(self, start, cache=True):
        # Atomic cache read first — also handles warm cache from prior API
        # call. The cache is a plain dict; reads/writes are atomic per-key
        # in CPython, so concurrent parallel calls are safe.
        if start in self._doc_cache:
            return self._doc_cache[start]

        # Try new 2-level path first, fall back to old flat path (backward compat)
        path = self._doc_path(start)
        if not self.storage.exists(path):
            path = os.path.join(self.docs_dir, f"docs_{start:010d}.zlib")
        if not self.storage.exists(path):
            return {}

        try:
            blob = self.storage.read_bytes(path)
            blob = self._decrypt_if_needed(blob)   # no-op if not encrypted
            raw  = _decompress_doc(blob)
            chunk = {int(k): v for k, v in _doc_loads(raw).items()}
        except Exception:
            # Silently skip unreadable chunks. The FSK adapter's
            # `_iter_chunks` path (used by streaming export before parallel
            # pre-read) was silent on this; matching that behavior to avoid
            # spamming the user's console for what may be a benign
            # compressed/uncompressed chunk mix. The data is still returned
            # as an empty dict — callers (export, search) handle missing
            # docs gracefully via the empty fallback.
            chunk = {}
        if cache:
            # Streaming export passes cache=False to avoid unbounded growth
            # when many chunks are touched (parallel reads would otherwise
            # all populate _doc_cache).
            self._doc_cache[start] = chunk
        return chunk

    def _fetch_docs(self, doc_ids):
        # Filter out deleted documents
        doc_ids = self._alive_ids(doc_ids)
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

    # ─── Sorting ────────────────────────────────────────────────────────────

    def _sort_doc_ids(self, doc_ids, sort_spec):
        """Sort doc_ids by field(s) in sort_spec.

        Args:
            doc_ids: iterable of doc_ids to sort
            sort_spec: list of (field, direction) tuples, e.g. [("amount", "desc")]

        Returns:
            sorted list of doc_ids
        """
        if not sort_spec:
            return sorted(doc_ids)

        # Stable sort: process in reverse order so primary sort is applied last
        for field, direction in reversed(sort_spec):
            if field == "_score":
                # Score sorting: keep original doc_id order as tiebreaker
                continue
            doc_ids = self._sort_by_field(doc_ids, field, direction)

        return doc_ids

    def _sort_by_field(self, doc_ids, field, direction):
        """Sort doc_ids by a single field using doc_values if available.

        Args:
            doc_ids: list of doc_ids to sort
            field: field name to sort by
            direction: "asc" or "desc"

        Returns:
            sorted list of doc_ids
        """
        if not doc_ids:
            return doc_ids

        # Check if field has doc_values (numeric or terms)
        pairs = self._load_doc_values(field)

        if pairs is None:
            # No doc_values — scan chunks to get field values
            return self._sort_by_chunk_scan(doc_ids, field, direction)

        # Determine if numeric or keyword doc_values
        # Strip data_dir prefix for FlatseekFileStorageAdapter since stored keys don't include it
        from flatseek.flatseek_file import FlatseekFileStorageAdapter
        numeric_path = os.path.join(self.data_dir, "dv", field, "numeric.bin")
        if isinstance(self.storage, FlatseekFileStorageAdapter):
            if numeric_path.startswith(self.data_dir + "/"):
                numeric_path = numeric_path[len(self.data_dir) + 1:]
        if self.storage.exists(numeric_path):
            return self._sort_by_numeric(doc_ids, pairs, field, direction)
        else:
            # Keyword: sort alphabetically by term
            return self._sort_by_keyword(doc_ids, pairs, field, direction)

    def _sort_by_numeric(self, doc_ids, pairs, field, direction):
        """Sort doc_ids by numeric field using doc_values."""
        # pairs is list of (value, doc_id) sorted by value
        doc_id_set = set(doc_ids)
        value_map = {did: val for val, did in pairs if did in doc_id_set}

        reverse = (direction == "desc")

        def sort_key(did):
            val = value_map.get(did)
            if val is None:
                # Missing values sort last
                return (1, 0 if reverse else float('inf'))
            return (0, val)

        return sorted(doc_ids, key=sort_key, reverse=reverse)

    def _sort_by_keyword(self, doc_ids, pairs, field, direction):
        """Sort doc_ids by keyword/terms field using doc_values."""
        # pairs is list of (term_bytes, count) — need to get actual term values
        # For keywords, we need to scan chunks to get the actual term strings
        return self._sort_by_chunk_scan(doc_ids, field, direction)

    def _sort_by_chunk_scan(self, doc_ids, field, direction):
        """Sort doc_ids by scanning chunks to get field values.

        Used when doc_values are not available (TEXT fields, or keyword without terms.bin).
        """
        if not doc_ids:
            return doc_ids

        # Group doc_ids by chunk
        by_chunk = defaultdict(list)
        for did in doc_ids:
            by_chunk[self._chunk_start(did)].append(did)

        # Build value map from chunks
        value_map = {}
        for chunk_start in sorted(by_chunk):
            chunk = self._load_chunk(chunk_start)
            for did in by_chunk[chunk_start]:
                doc = chunk.get(did)
                if doc is not None:
                    val = _get_nested_value(doc, field)
                    value_map[did] = val
                else:
                    value_map[did] = None

        reverse = (direction == "desc")

        def sort_key(did):
            val = value_map.get(did)
            if val is None:
                return (1, "")  # Missing sorts last
            if isinstance(val, (int, float)):
                return (0, val)
            return (0, str(val))

        return sorted(doc_ids, key=sort_key, reverse=reverse)

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

    def search(self, term, column=None, page=0, page_size=20, sort=None):
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
            per_eng = []
            for eng in self._sub_engines:
                ids = eng._resolve(term, column)
                ids = eng._alive_ids(ids) if hasattr(eng, '_alive_ids') else ids
                per_eng.append((eng, ids))
            return self._multi_paginate(per_eng, page, page_size)

        # For match-all on a single index, scan chunks directly to get actual
        # stored docs — skips gaps left by dedup (IDs are not always consecutive).
        # However: when sort is specified, we must go through _sort_doc_ids first,
        # so skip this fast path for sorted match-all queries.
        if is_match_all and not sort:
            # Validate key early for encrypted indexes (even with page_size=0,
            # which causes _fetch_page_from_chunks to exit before touching chunks).
            if self._enc_key and self.docs_dir:
                first_chunk = self._find_first_chunk_path()
                if first_chunk:
                    blob = self.storage.read_bytes(first_chunk)
                    if is_encrypted(blob):
                        blob = self._decrypt_if_needed(blob)  # raises InvalidToken on wrong key
            real_total = self.stats.get("total_docs", 0)
            docs = self._fetch_page_from_chunks(page, page_size)
            return {"total": real_total, "page": page, "page_size": page_size, "results": docs}

        doc_ids = self._resolve(term, column)
        doc_ids = self._alive_ids(doc_ids)
        if sort:
            doc_ids = self._sort_doc_ids(doc_ids, sort)
        return self._paginate(doc_ids, page, page_size)

    def search_and(self, conditions, page=0, page_size=20):
        """AND search across multiple (column, term) conditions.

        All conditions must match (intersection). Fails fast if any returns empty.
        Conditions are sorted by posting list size before intersecting — rarest
        term is evaluated first so large posting lists are pruned early.

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
                # Resolve all conditions first, sort by size (rarest first) per engine
                resolved = []
                for col, term in conditions:
                    ids = eng._resolve(term, col)
                    if not ids:
                        result = set()
                        break
                    resolved.append((len(ids), set(ids)))
                else:
                    resolved.sort(key=lambda x: x[0])
                    result = resolved[0][1]
                    for _, s in resolved[1:]:
                        result &= s
                        if not result:
                            break
                # Filter out deleted docs
                alive = eng._alive_ids(result) if hasattr(eng, '_alive_ids') else result
                per_eng.append((eng, sorted(alive) if alive else []))
            return self._multi_paginate(per_eng, page, page_size)

        # Resolve all terms first, then sort by posting list size (rarest first)
        # to minimize intersection work — mirrors Tantivy's conjunction optimization.
        resolved = []
        for col, term in conditions:
            ids = self._resolve(term, col)
            if not ids:
                return {"total": 0, "page": page, "page_size": page_size, "results": []}
            resolved.append((len(ids), set(ids)))
        # Sort by size ascending: smallest posting list first
        resolved.sort(key=lambda x: x[0])

        result = resolved[0][1]
        for _, ids in resolved[1:]:
            result &= ids
            if not result:
                break

        if not result:
            return {"total": 0, "page": page, "page_size": page_size, "results": []}

        # Filter out deleted docs
        result = self._alive_ids(result)
        return self._paginate(sorted(result), page, page_size)

    def query(self, query_str, page=0, page_size=20, sort=None):
        """Execute a Lucene-style query string.

        In multi-index mode, queries all sub-indexes and merges results.
        Results from sub-indexes are tagged with _index = sub-dir name.

        Args:
            query_str: Lucene query, e.g. "program:raydium AND amount:>1000000"
            page:      0-based page number
            page_size: results per page
            sort:      list of (field, direction) tuples, e.g. [("amount", "desc")]

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
        # Filter out deleted documents
        doc_ids = self._alive_ids(doc_ids)
        if sort:
            doc_ids = self._sort_doc_ids(doc_ids, sort)
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
                # Filter out deleted docs per sub-engine
                ids = eng._alive_ids(ids) if hasattr(eng, '_alive_ids') else ids
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

    # ─── Doc_values columnar storage for fast aggregations ─────────────────────

    def _load_doc_values(self, field):
        """Load doc_values for a field (lazy, cached).

        Returns:
            For numeric fields: list of (value, doc_id) sorted by value
            For keyword fields: list of (term_bytes, count) sorted by count
        """
        if field in self._dv_cache:
            return self._dv_cache[field]

        import struct as _struct
        _unpack_q = _struct.Struct("<Q").unpack
        _unpack_f = _struct.Struct("<d").unpack
        _unpack_h = _struct.Struct("<H").unpack
        _unpack_i = _struct.Struct("<I").unpack

        dv_path = os.path.join(self.data_dir, "dv", field)
        from flatseek.core.storage import LocalStorageAdapter
        if isinstance(self.storage, LocalStorageAdapter):
            if not self.storage.exists(dv_path) and not os.path.isdir(dv_path):
                self._dv_cache[field] = None
                return None
        else:
            # For FlatseekFileStorageAdapter: stored keys don't include data_dir prefix
            # (e.g. "dv/vote_count/numeric.bin" not "1.2M-movies/dv/vote_count/numeric.bin").
            # Strip the prefix so exists() finds the right key.
            from flatseek.flatseek_file import FlatseekFileStorageAdapter
            lookup_base = dv_path
            if isinstance(self.storage, FlatseekFileStorageAdapter):
                if dv_path.startswith(self.data_dir + "/"):
                    lookup_base = dv_path[len(self.data_dir) + 1:]
            # Check that the field has any doc_values (numeric or terms) before proceeding
            if not self.storage.exists(os.path.join(lookup_base, "numeric.bin")) and \
               not self.storage.exists(os.path.join(lookup_base, "terms.bin")):
                self._dv_cache[field] = None
                return None

        # Detect numeric vs keyword from file presence
        numeric_path = os.path.join(lookup_base, "numeric.bin")
        terms_path = os.path.join(lookup_base, "terms.bin")

        if self.storage.exists(numeric_path):
            # Numeric: load sorted (value, doc_id) pairs
            pairs = []
            data = self.storage.read_bytes(numeric_path)
            i = 0
            while i + 16 <= len(data):
                doc_id = _unpack_q(data[i:i+8])[0]; i += 8
                value  = _unpack_f(data[i:i+8])[0]; i += 8
                pairs.append((value, doc_id))
            self._dv_cache[field] = pairs
            return pairs
        elif self.storage.exists(terms_path):
            # Keyword: load term→count list
            terms = []
            data = self.storage.read_bytes(terms_path)
            i = 0
            while i + 2 <= len(data):
                term_len = _unpack_h(data[i:i+2])[0]; i += 2
                if i + term_len + 4 > len(data):
                    break
                term = data[i:i+term_len].decode("utf-8"); i += term_len
                count = _unpack_i(data[i:i+4])[0]; i += 4
                terms.append((term, count))
            self._dv_cache[field] = terms
            return terms
        else:
            self._dv_cache[field] = None
            return None

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

        # ── DATE field between: dob between 00001101 and 99991130 ─────────────
        # Handles dob:month=11 → between "00001101" "99991130"
        # and dob:year=1997 → between "19970101" "19971231"
        _DATE_FIELDS = None   # lazily resolved
        if op == 'between' and len(args) == 3:
            lo_str, hi_str = str(args[1]), str(args[2])
            if len(lo_str) == 8 and len(hi_str) == 8 and lo_str.isdigit() and hi_str.isdigit():
                # Resolve DATE fields from column type
                if _DATE_FIELDS is None:
                    _DATE_FIELDS = {c for c, t in self.columns().items() if t == 'DATE'}
                if field in _DATE_FIELDS:
                    # Use exact match on year token for year ranges
                    # and full YYYYMMDD comparison for month ranges
                    lo_y_match = re.match(r'^(\d{4})0101$', lo_str)
                    hi_y_match = re.match(r'^(\d{4})1231$', hi_str)
                    if lo_y_match and hi_y_match:
                        # Full year range: 19970101 to 19971231 → union of year tokens
                        lo_y = int(lo_y_match.group(1))
                        hi_y = int(hi_y_match.group(1))
                        return self._year_range_ids(field, lo_y, hi_y)
                    else:
                        # Month range: 00001101 to 99991130 → scan all years,
                        # check month digits (pos 4-5) match lo month
                        month_digit = lo_str[4:6]   # e.g. '11' for November
                        year_start = int(lo_str[:4]) if lo_str[:4] != '0000' else 1900
                        year_end   = int(hi_str[:4]) if hi_str[:4] != '9999' else current_year
                        day_start  = int(lo_str[6:8]) if lo_str[6:8] != '00' else 1
                        day_end    = int(hi_str[6:8]) if hi_str[6:8] != '00' else 31

                        result = set()
                        by_chunk = {}
                        chunk_start_fn = self._chunk_start
                        total = self.stats.get("total_docs", 0)
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
                                if fval and len(fval) == 8 and fval.isdigit():
                                    # Check: year in range, month matches, day in range
                                    try:
                                        yr  = int(fval[0:4])
                                        mon = int(fval[4:6])
                                        day = int(fval[6:8])
                                        if (year_start <= yr <= year_end and
                                                mon == int(month_digit) and
                                                day_start <= day <= day_end):
                                            result.add(doc_id)
                                    except ValueError:
                                        pass
                        return result

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
                # Fast path: use doc_values binary search if available
                pairs = self._load_doc_values(field)
                if pairs is not None:
                    # Binary search: O(log N) find lower/upper bounds
                    # Guard against mixed types (some values may be str if field
                    # was classified numeric but contains non-numeric data)
                    import bisect
                    try:
                        values = [p[0] for p in pairs]
                        # numeric.bin stores (float_value, doc_id) pairs — values[0] is always float
                        # terms.bin stores (term_string, count) pairs — values[0] is str
                        # If values are strings, this field uses terms.bin (KEYWORD/ARRAY/DATE),
                        # not numeric.bin — binary search with float bounds will fail
                        if values and not isinstance(values[0], (int, float)):
                            pairs = None
                        else:
                            lo_idx = bisect.bisect_left(values, lo_val)
                            hi_idx = bisect.bisect_right(values, hi_val)
                            result = {doc_id for _, doc_id in pairs[lo_idx:hi_idx]}
                    except TypeError:
                        # Mixed types in values → fall back to chunk scan
                        pairs = None
                if pairs is None:
                    # Fallback: full chunk scan
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
                    # Fast path: use doc_values binary search if available
                    pairs = self._load_doc_values(field)
                    if pairs is not None:
                        import bisect
                        try:
                            values = [p[0] for p in pairs]
                            # Same guard as BETWEEN path: if values are strings (terms.bin),
                            # binary search with float bounds will TypeError
                            if values and not isinstance(values[0], (int, float)):
                                pairs = None
                            elif op_sym == '>':
                                idx = bisect.bisect_right(values, num_val)
                                result = {doc_id for _, doc_id in pairs[idx:]}
                            elif op_sym == '>=':
                                idx = bisect.bisect_left(values, num_val)
                                result = {doc_id for _, doc_id in pairs[idx:]}
                            elif op_sym == '<':
                                idx = bisect.bisect_left(values, num_val)
                                result = {doc_id for _, doc_id in pairs[:idx]}
                            elif op_sym == '<=':
                                idx = bisect.bisect_right(values, num_val)
                                result = {doc_id for _, doc_id in pairs[:idx]}
                            return result
                        except TypeError:
                            # Mixed types → fall back to chunk scan
                            pairs = None
                    if pairs is None:
                        # Fallback: full chunk scan
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

        # ── Fast path: use doc_values if all aggs are numeric/keyword with dv ──
        # Check if every agg can use doc_values (no query filter + supported type)
        if matching_ids is None:
            all_dv_compatible = True
            dv_able_fields = {}
            for agg_type, field, _ in agg_items:
                if agg_type in ("terms",):
                    dv = self._load_doc_values(field)
                    if dv is not None:
                        dv_able_fields[agg_type] = (field, dv)
                    else:
                        all_dv_compatible = False
                        break
                elif agg_type in ("avg", "min", "max", "sum", "stats"):
                    dv = self._load_doc_values(field)
                    if dv is not None:
                        dv_able_fields[agg_type] = (field, dv)
                    else:
                        all_dv_compatible = False
                        break
                else:
                    all_dv_compatible = False
                    break

            if all_dv_compatible and dv_able_fields:
                # All aggs have doc_values and no filter → use fast columnar path
                aggregations = {}
                with self._timed_phase("agg_dv"):
                    for agg_type, (field, dv) in dv_able_fields.items():
                        if agg_type == "terms":
                            buckets = [{"key": term, "doc_count": count} for term, count in dv]
                            aggregations[field] = {
                                "buckets": buckets[:agg_configs[agg_type].get("size", size)],
                                "sum_other_doc_count": max(0, len(dv) - size),
                            }
                        elif agg_type == "stats":
                            vals = [p[0] for p in dv]
                            count = len(vals)
                            vals_sorted = sorted(vals)
                            min_v = vals_sorted[0]
                            max_v = vals_sorted[-1]
                            avg_v = sum(vals) / count
                            sum_v = sum(vals)
                            aggregations[field] = {
                                "count": count, "min": min_v, "max": max_v,
                                "avg": avg_v, "sum": sum_v,
                            }
                        elif agg_type == "min":
                            aggregations[field] = {"value": min(p[0] for p in dv)}
                        elif agg_type == "max":
                            aggregations[field] = {"value": max(p[0] for p in dv)}
                        elif agg_type == "avg":
                            vals = [p[0] for p in dv]
                            aggregations[field] = {"value": sum(vals) / len(vals)}
                        elif agg_type == "sum":
                            aggregations[field] = {"value": sum(p[0] for p in dv)}

                return {
                    "took": int((_time.perf_counter() - start) * 1000),
                    "hits": {"total": self.stats.get("total_docs", 0), "hits": []},
                    "aggregations": aggregations,
                }

        for chunk_start, chunk in _iter_chunks_for_agg(self):
            chunk_len = len(chunk)
            # Only count towards hits.total if no filter, or doc matches filter
            chunks_since_mem_check += 1

            with self._timed_phase("agg_scan"):
                # No sort: terms/stats/avg are commutative; ordering doesn't affect
                # the result and saves O(N log N) per chunk on big indices.
                for doc_id, doc in chunk.items():
                    # Filter by query
                    if matching_ids is not None and doc_id not in matching_ids:
                        continue
                    # Count matching doc towards total
                    total_docs += 1

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
