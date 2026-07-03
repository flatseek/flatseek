"""Dependency injection for Flatseek API."""

import os
from pathlib import Path
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, AsyncGenerator, Any

from fastapi import Depends, HTTPException, Query

from flatseek.core.storage import StorageAdapter, create_storage_adapter, VercelBlobStorageAdapter, URLStorageAdapter
from flatseek.flatseek_file import FlatseekFileStorageAdapter

# Default data directory
DEFAULT_DATA_DIR = os.environ.get("FLATSEEK_DATA_DIR", "data")

# Import QueryEngine lazily to avoid circular imports
if TYPE_CHECKING:
    from flatseek.core.query_engine import QueryEngine


class IndexManager:
    """Manages QueryEngine instances per index (lazy loading).

    Auto-detects index types from data_dir:
    - Directory indices: data/<name>/ has index/ subdir
    - .fsk archives: data/<name>.fsk or data/<sub>/<name>.fsk
    """

    def __init__(self):
        self._engines: dict[str, Any] = {}
        # Shared default storage adapter
        self._storage: StorageAdapter = create_storage_adapter()
        # Per-bucket URL storage adapters (key = bucket URL)
        self._bucket_storage: dict[str, URLStorageAdapter] = {}

        # Discovered .fsk files in data_dir: index_name → Path
        # Populated lazily on first list_indices() or get_engine() call
        self._fsk_index_map: dict[str, Path] = {}
        # Per-index encryption keys (set by authenticate route)
        self._enc_keys: dict[str, bytes] = {}

        # Single-file mode: FLATSEEK_SINGLE_FILE points to a .fsk file
        self._single_file_path: str | None = os.environ.get("FLATSEEK_SINGLE_FILE")
        self._single_file_engine: "QueryEngine | None" = None
        self._single_file_name: str | None = None
        if self._single_file_path:
            self._init_single_file_engine()

    def _discover_local_fsk(self) -> None:
        """Walk data_dir and register every .fsk file as an index.

        Called lazily on first list/get call when in local-dir mode.
        """
        import base64
        import json

        dir_path = Path(self.data_dir)
        if not dir_path.is_dir():
            return

        for fsk_path in sorted(dir_path.rglob("*.fsk")):
            rel = fsk_path.relative_to(dir_path)
            index_name = str(rel.with_suffix("").as_posix())
            self._fsk_index_map[index_name] = fsk_path

        # Pre-load per-index keys from FLATSEEK_FSK_KEYS (JSON)
        # Format: {"index_name": "base64_encoded_key", ...}
        keys_json = os.environ.get("FLATSEEK_FSK_KEYS", "{}")
        try:
            keys_map: dict[str, str] = json.loads(keys_json)
            for idx, key_b64 in keys_map.items():
                if idx in self._fsk_index_map:
                    try:
                        self._enc_keys[idx] = base64.b64decode(key_b64)
                    except Exception:
                        pass
        except Exception:
            pass

    def _open_fsk_engine(self, index: str) -> "QueryEngine":
        """Lazily open a QueryEngine for a .fsk file."""
        from flatseek.flatseek_file import FlatseekFileStorageAdapter
        from flatseek.core.query_engine import QueryEngine

        fsk_path = self._fsk_index_map[index]
        enc_key = self._enc_keys.get(index)
        storage = FlatseekFileStorageAdapter(fsk_path, enc_key=enc_key)
        engine = QueryEngine(".", storage=storage)
        if enc_key is not None:
            engine._enc_key = enc_key
        return engine

    def _init_single_file_engine(self) -> None:
        """Initialize QueryEngine from a single .fsk file."""
        import base64
        from flatseek.flatseek_file import FlatseekFileStorageAdapter
        from flatseek.core.query_engine import QueryEngine
        p = Path(self._single_file_path)
        self._single_file_name = p.stem

        enc_key = None
        key_b64 = os.environ.get("FLATSEEK_FSK_KEY")
        if key_b64:
            try:
                enc_key = base64.b64decode(key_b64)
            except Exception:
                pass

        storage = FlatseekFileStorageAdapter(p, enc_key=enc_key)
        self._single_file_engine = QueryEngine(".", storage=storage)
        if enc_key is not None:
            self._single_file_engine._enc_key = enc_key

    @property
    def data_dir(self) -> str:
        """Data directory, read dynamically from environment."""
        return os.environ.get("FLATSEEK_DATA_DIR", DEFAULT_DATA_DIR)

    def is_encrypted(self, index: str, bucket_url: str | None = None) -> bool:
        """Check if a specific index is encrypted.

        Each index stores its own encryption.json inside its folder.
        If that file exists, the index is encrypted.
        For bucket URLs, checks via the remote storage adapter.
        """
        # Single-file mode: check storage adapter's encryption status.
        # If key is available (from --passphrase), index is unlocked → return False.
        if self._single_file_engine is not None:
            storage = getattr(self._single_file_engine, "storage", None)
            storage_encrypted = getattr(storage, "_is_encrypted", False) if storage else False
            has_key = getattr(self._single_file_engine, "_enc_key", None) is not None
            if storage_encrypted and has_key:
                return False  # unlocked — no password needed
            return storage_encrypted  # locked without key → prompt for password

        if bucket_url:
            # For bucket URLs, check via storage adapter
            storage = self._get_bucket_storage(bucket_url)
            # Special case: .fsk URL → the storage is FlatseekFileStorageAdapter
            # which already tracks _is_encrypted from the manifest at open
            # time. Don't try to read "encryption.json" (no such sub-path on
            # a .fsk).
            if isinstance(storage, FlatseekFileStorageAdapter):
                return bool(getattr(storage, "_is_encrypted", False))
            # For URL storage, index IS the relative path within the bucket
            enc_path = "encryption.json"
            try:
                storage.read_bytes(enc_path)
                return True
            except (FileNotFoundError, Exception):
                return False

        # Local directory mode: check both .fsk files and directory indices
        # Lazily discover .fsk files if not already done
        if not self._fsk_index_map:
            self._discover_local_fsk()

        if index in self._fsk_index_map:
            # .fsk index: probe encryption status using lightweight header read
            # (avoids full FlatseekFileStorageAdapter init which raises on encrypted files)
            return FlatseekFileStorageAdapter.probe_encrypted(self._fsk_index_map[index])

        # Directory index: check for encryption.json
        possible_paths = [
            os.path.join(self.data_dir, index),
            os.path.join(self.data_dir, index, "..", index),
        ]
        # Also check if data_dir itself is the index dir (unpacked .fsk output)
        if os.path.basename(self.data_dir.rstrip(os.sep)) == index:
            possible_paths.insert(0, self.data_dir)
        for index_path in possible_paths:
            enc_path = os.path.join(index_path, "encryption.json")
            if os.path.isfile(enc_path):
                return True
        return False

    def index_exists(self, index: str) -> bool:
        """Check if an index exists in data_dir."""
        if self._single_file_engine is not None:
            return self._single_file_name == index
        if not self._fsk_index_map:
            self._discover_local_fsk()
        if index in self._fsk_index_map:
            return True
        return os.path.isdir(os.path.join(self.data_dir, index, "index"))

    def _get_bucket_storage(self, bucket_url: str) -> URLStorageAdapter:
        """Get or create a storage adapter for the given bucket URL.

        For encrypted bucket URLs, the adapter is NOT cached — each new session
        must re-probe and re-authenticate so cache isolation is enforced.
        Non-encrypted buckets use a shared cached adapter with global cache.

        Special case: if the URL points at a single .fsk archive (e.g.
        HuggingFace dataset hosting one packed index file), use
        FlatseekFileStorageAdapter with HTTP Range instead of the
        directory-mode URLStorageAdapter. The directory adapter joins
        ``base_url + "/" + rel_path`` which would request non-existent
        sub-paths of the .fsk file.
        """
        import logging
        logger = logging.getLogger(__name__)

        # Auto-detect: .fsk URL → use FlatseekFileStorageAdapter (Range)
        if bucket_url.split("?", 1)[0].rstrip("/").endswith(
                (".fsk", ".flatseek", ".flat")):
            from flatseek.flatseek_file import FlatseekFileStorageAdapter
            logger.warning(
                f"[_get_bucket_storage] {bucket_url} is a .fsk URL, using "
                f"FlatseekFileStorageAdapter (HTTP Range, no full download)"
            )
            return FlatseekFileStorageAdapter(bucket_url)

        # For encrypted buckets, ALWAYS create a fresh adapter (no caching)
        # to ensure per-session cache isolation. The probe below determines this.
        try:
            from flatseek.core.storage import URLStorageAdapter
            probe_adapter = URLStorageAdapter(url=bucket_url, base_path="", cache_enabled=True)
            enc_data = probe_adapter.read_bytes("encryption.json")
            logger.warning(f"[_get_bucket_storage] {bucket_url} is ENCRYPTED")
            # Create fresh adapter for encrypted bucket — do NOT cache in _bucket_storage
            # is_encrypted=True ensures all caches are disabled (key-bound cache only)
            adapter = URLStorageAdapter(url=bucket_url, base_path="", cache_enabled=True, is_encrypted=True)
            logger.warning(f"[_get_bucket_storage] Created UNCACHED encrypted adapter is_encrypted=True")
            return adapter
        except FileNotFoundError:
            logger.warning(f"[_get_bucket_storage] {bucket_url} is NOT encrypted, using cached adapter")
            # Non-encrypted: use shared cached adapter
            adapter = self._bucket_storage.get(bucket_url)
            if adapter is None:
                adapter = URLStorageAdapter(url=bucket_url, base_path="", cache_enabled=True, is_encrypted=False)
                self._bucket_storage[bucket_url] = adapter
                logger.warning(f"[_get_bucket_storage] Created and cached non-encrypted adapter")
            return adapter
        except Exception as e:
            logger.warning(f"[_get_bucket_storage] {bucket_url} check failed: {e}, assuming not encrypted")
            adapter = self._bucket_storage.get(bucket_url)
            if adapter is None:
                adapter = URLStorageAdapter(url=bucket_url, base_path="", cache_enabled=True, is_encrypted=False)
                self._bucket_storage[bucket_url] = adapter
            return adapter

    def _engine_cache_key(self, index: str, bucket_url: str | None) -> str:
        """Generate cache key for engine, optionally including bucket URL."""
        if bucket_url:
            return f"{bucket_url}::{index}"
        return index

    def get_engine(self, index: str, bucket_url: str | None = None) -> "QueryEngine":
        """Get or create QueryEngine for an index.

        Args:
            index: Index name.
            bucket_url: Optional URL storage URL (e.g. HuggingFace dataset URL).
                       When provided, creates a URLStorageAdapter on-the-fly for
                       direct access to that remote index without env vars.
        """
        # Single-file mode: serve the one .fsk file under any index name
        if self._single_file_engine is not None:
            return self._single_file_engine

        # Lazily discover local .fsk files if in local-dir mode
        if not self._fsk_index_map and not bucket_url:
            self._discover_local_fsk()

        # Local .fsk index
        if index in self._fsk_index_map:
            cache_key = self._engine_cache_key(index, bucket_url)
            cached = self._engines.get(cache_key)
            available_key = self._enc_keys.get(index)
            # Re-open if: cached storage was opened without a key but we now have one
            if cached is not None and available_key is not None:
                cached_storage = getattr(cached, "storage", None)
                if cached_storage is not None and getattr(cached_storage, "_enc_key", None) is None:
                    # Was opened without key; re-open with the now-available key
                    self._engines[cache_key] = self._open_fsk_engine(index)
            elif cache_key not in self._engines:
                self._engines[cache_key] = self._open_fsk_engine(index)
            return self._engines[cache_key]

        if not index.replace("_", "").replace("-", "").replace("/", "").replace(".", "").isalnum():
            raise HTTPException(400, f"Invalid index name: {index}")

        cache_key = self._engine_cache_key(index, bucket_url)
        engine = self._engines.get(cache_key)

        if engine is None:
            from flatseek.core.query_engine import QueryEngine

            # Determine which storage to use
            if bucket_url:
                # Check if bucket_url is a HF dataset repo (not a direct .fsk URL)
                # HF dataset repos have .fsk files at root level like public-dataset
                original_url_lower = bucket_url.lower()
                is_hf_dataset_repo = (
                    "huggingface.co" in original_url_lower
                    and "/datasets/" in original_url_lower
                    and not bucket_url.rstrip("/").endswith((".fsk", ".flatseek", ".flat")))
                if is_hf_dataset_repo:
                    # Use FlatseekFileStorageAdapter pointing to the specific .fsk file
                    fsk_url = f"{bucket_url.rstrip('/')}/resolve/main/{index}.fsk"
                    storage = FlatseekFileStorageAdapter(fsk_url)
                else:
                    # Per-request bucket URL → create URLStorageAdapter
                    storage = self._get_bucket_storage(bucket_url)
            elif isinstance(self._storage, URLStorageAdapter):
                # Default URL storage
                storage = self._storage
            else:
                storage = None

            if storage is not None and isinstance(
                    storage, (URLStorageAdapter, FlatseekFileStorageAdapter)
            ):
                # URL-based storage (HuggingFace, GitHub, generic .fsk URL, etc.)
                try:
                    engine = QueryEngine(index, storage=storage)
                    # Never cache engines for encrypted bucket URLs — they hold
                    # per-session decryption state that must not leak to other sessions
                    if bucket_url and self.is_encrypted(index, bucket_url):
                        # Return uncached engine for encrypted bucket
                        return engine
                    self._engines[cache_key] = engine
                except FileNotFoundError:
                    raise HTTPException(404, f"Index not found: {index}")
            else:
                # Local/S3 storage: look for index directory on filesystem
                # If FLATSEEK_FSK_KEY is set (from --passphrase on cmd_serve), apply it.
                env_key = None
                import base64 as _b64
                fsk_key_b64 = os.environ.get("FLATSEEK_FSK_KEY")
                if fsk_key_b64:
                    try:
                        env_key = _b64.b64decode(fsk_key_b64)
                    except Exception:
                        pass

                # Detect if data_dir IS the index dir (e.g. unpacked .fsk output)
                if (os.path.isdir(os.path.join(self.data_dir, "index")) and
                        os.path.isfile(os.path.join(self.data_dir, "stats.json"))):
                    # data_dir is the index itself
                    if index == os.path.basename(self.data_dir.rstrip(os.sep)):
                        engine = QueryEngine(self.data_dir, storage=self._storage)
                        if env_key:
                            engine.set_key(env_key)
                        if not self.is_encrypted(index, bucket_url):
                            self._engines[cache_key] = engine
                        return engine

                possible_paths = [
                    os.path.join(self.data_dir, index),
                    os.path.join(self.data_dir, index, "..", index),
                ]
                for path in possible_paths:
                    if os.path.isdir(os.path.join(path, "index")):
                        engine = QueryEngine(path, storage=self._storage)
                        if env_key:
                            engine.set_key(env_key)
                        # Never cache encrypted local indexes — they require a per-session
                        # key that must not leak to other sessions
                        if not self.is_encrypted(index, bucket_url):
                            self._engines[cache_key] = engine
                        else:
                            # For encrypted indexes, always return a fresh engine to prevent
                            # a previous request's key from leaking into this request.
                            pass
                        break

        if engine is None:
            raise HTTPException(404, f"Index not found: {index}")

        return engine

    def list_indices(self, bucket_url: str | None = None) -> list[str]:
        """List all available indices.

        Args:
            bucket_url: Optional URL storage URL. When provided, lists indices
                       from that remote HuggingFace/GitHub URL instead of local disk.
        """
        # Single-file mode: only one index (the .fsk filename)
        if self._single_file_engine is not None:
            return [self._single_file_name] if self._single_file_name else []

        # Determine which storage to use
        if bucket_url:
            storage = self._get_bucket_storage(bucket_url)
            return self._list_url_indices(storage)
        elif isinstance(self._storage, URLStorageAdapter):
            return self._list_url_indices(self._storage)

        # Local directory mode: merge .fsk files and directory-based indices
        if not os.path.isdir(self.data_dir):
            return []

        indices: set[str] = set()

        # Lazily discover .fsk files
        if not self._fsk_index_map:
            self._discover_local_fsk()
        indices.update(self._fsk_index_map.keys())

        # Directory-based indices
        if (os.path.isfile(os.path.join(self.data_dir, "stats.json")) and
                os.path.isdir(os.path.join(self.data_dir, "index"))):
            indices.add(os.path.basename(self.data_dir.rstrip(os.sep)))

        for name in os.listdir(self.data_dir):
            path = os.path.join(self.data_dir, name)
            if os.path.isdir(os.path.join(path, "index")):
                indices.add(name)
            elif os.path.isdir(path):
                for sub in os.listdir(path):
                    sub_path = os.path.join(path, sub)
                    if os.path.isdir(os.path.join(sub_path, "index")):
                        indices.add(f"{name}/{sub}")

        return sorted(indices)

    def _list_url_indices(self, storage: "URLStorageAdapter") -> list[str]:
        """List indices from URL storage (HuggingFace, GitHub, etc.).

        Handles three URL patterns:
        1. Root folder: base_url points to repo root with multiple index subdirs
           e.g., https://huggingface.co/datasets/owner/flatdata-indexes
           → listdir("") returns subdir names like ['adsb', 'articles', ...]
        2. Direct index: base_url points directly to an index folder
           e.g., https://huggingface.co/datasets/owner/sample-articles
           → listdir("") returns files/dirs at root level like ['docs', 'index', 'stats.json']
        3. Bucket repo: base_url is a HuggingFace bucket (repoType=bucket)
           → HF API tree/ doesn't return top-level dirs; use the web UI URL
           (https://huggingface.co/buckets/owner/repo/) to extract index names
        4. Single .fsk archive: skip directory-walk entirely — there's
           exactly one index (the .fsk file itself).
        """
        from flatseek.core.storage import URLStorageAdapter
        from flatseek.flatseek_file import FlatseekFileStorageAdapter

        # ── Single .fsk archive: skip multi-index detection ─────────────────
        # .fsk is one file → one index, named after the file stem.
        if isinstance(storage, FlatseekFileStorageAdapter):
            # If the URL ended in something.fsk, use the stem. If the
            # .fsk was passed as a local path earlier, fall through to
            # the existing single-file handling below.
            url = getattr(storage, "_url", None) or str(getattr(storage, "path", ""))
            if url and url.split("?", 1)[0].rstrip("/").endswith(
                    (".fsk", ".flatseek", ".flat")):
                from pathlib import PurePosixPath
                stem = PurePosixPath(url.split("?", 1)[0]).stem
                return [stem]

        # ── Bucket repos: use web UI URL to find index names ───────────────────
        # Buckets use /resolve/{index_name} URL pattern, not /resolve/main
        # HF API tree/ returns cached/upload metadata, not top-level index dirs
        # Solution: hit the web UI URL (https://huggingface.co/buckets/owner/repo/)
        # which returns HTML with links to each index tree page
        # `storage._original_url` is a URLStorageAdapter-specific attribute;
        # guard with getattr so FlatseekFileStorageAdapter (which doesn't
        # have it) doesn't crash this code path.
        original_url = getattr(storage, "_original_url", None) or ""
        if "huggingface.co" in original_url and "/buckets/" in original_url:
            try:
                import httpx
                # Use the original URL (without /resolve/) to get HTML listing
                web_url = original_url.rstrip("/")
                resp = httpx.get(web_url, timeout=30.0, follow_redirects=True)
                if resp.status_code == 200:
                    html = resp.text
                    # Extract index names from tree links: /buckets/owner/repo/tree/{index_name}
                    import re
                    # Pattern: /buckets/{owner}/{repo}/tree/{index_name} (alphanumeric + dash/underscore)
                    matches = re.findall(r'/buckets/[^/]+/[^/]+/tree/([a-zA-Z0-9_-]+)', html)
                    if matches:
                        # Dedupe and filter out .cache
                        indices = sorted(set(m for m in matches if m != '.cache'))
                        if indices:
                            return indices
            except Exception:
                pass

        # Not multi-index. Check if this is a direct index (single index repo)
        # Direct index: stats.json and index/ exist at root level
        if storage.exists("stats.json") or storage.exists("index"):
            if isinstance(storage, URLStorageAdapter):
                idx_name = storage.index_name
                if idx_name:
                    return [idx_name]

        # HuggingFace dataset repo with .fsk files directly in root (e.g. public-dataset repo)
        # Use HF API to discover .fsk files
        original_url = getattr(storage, "_original_url", "") or ""
        if "huggingface.co" in original_url and "/datasets/" in original_url:
            try:
                import httpx
                from pathlib import PurePosixPath
                # Parse owner/repo from URL
                parts = original_url.rstrip("/").split("/")
                hf_idx = parts.index("huggingface.co")
                owner = parts[hf_idx + 2]
                repo = parts[hf_idx + 3]
                api_url = f"https://huggingface.co/api/datasets/{owner}/{repo}/tree/main"
                resp = httpx.get(api_url, timeout=30.0)
                if resp.status_code == 200:
                    files = resp.json()
                    fsk_files = [
                        f["path"] for f in files
                        if f.get("type") == "file" and f["path"].endswith((".fsk", ".flatseek", ".flat"))
                    ]
                    if fsk_files:
                        return [PurePosixPath(f).stem for f in fsk_files]
            except Exception:
                pass

        return []


# Global index manager
_index_manager: IndexManager | None = None


def get_index_manager() -> IndexManager:
    """Get the global IndexManager instance.

    The storage adapter is set once at first creation and is NEVER updated
    based on environment variable changes after that. This ensures that
    when one request sets FLATSEEK_STORAGE_URL for a bucket URL, it does
    not affect other concurrent requests that need the default storage.

    Per-request bucket storage is always handled via _get_bucket_storage()
    which creates fresh adapters per bucket URL, isolated per request.
    """
    global _index_manager
    if _index_manager is None:
        _index_manager = IndexManager()
        # Note: do NOT re-create storage here based on env vars.
        # Once created, the manager's storage is fixed for its lifetime.
        # Per-request bucket storage is handled by _get_bucket_storage().
    return _index_manager


async def get_query_engine(
    index: str,
    manager: IndexManager = Depends(get_index_manager),
) -> "QueryEngine":
    """Dependency to get QueryEngine for an index."""
    return manager.get_engine(index)


def require_index(
    index: str,
    manager: IndexManager = Depends(get_index_manager),
) -> str:
    """Dependency that validates index exists."""
    try:
        manager.get_engine(index)
    except Exception as e:
        raise HTTPException(404, str(e)) from e
    return index