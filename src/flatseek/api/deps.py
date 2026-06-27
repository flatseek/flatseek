"""Dependency injection for Flatseek API."""

import os
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, AsyncGenerator, Any

from fastapi import Depends, HTTPException, Query

from flatseek.core.storage import StorageAdapter, create_storage_adapter, VercelBlobStorageAdapter, URLStorageAdapter

# Default data directory
DEFAULT_DATA_DIR = os.environ.get("FLATSEEK_DATA_DIR", "data")

# Import QueryEngine lazily to avoid circular imports
if TYPE_CHECKING:
    from flatseek.core.query_engine import QueryEngine


class IndexManager:
    """Manages QueryEngine instances per index (lazy loading).

    Supports two storage modes:
    - Default: uses shared storage adapter from environment (local, S3, Vercel Blob, URL)
    - Per-request: when bucket= URL param is provided, creates a URLStorageAdapter
      on-the-fly for that specific HuggingFace/GitHub URL. Engines are cached
      separately per (bucket_url, index) to avoid cross-contamination.
    """

    def __init__(self):
        self._engines: dict[str, Any] = {}
        # Shared default storage adapter
        self._storage: StorageAdapter = create_storage_adapter()
        # Per-bucket URL storage adapters (key = bucket URL)
        self._bucket_storage: dict[str, URLStorageAdapter] = {}

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
        if bucket_url:
            # For bucket URLs, check via storage adapter
            storage = self._get_bucket_storage(bucket_url)
            # For URL storage, index IS the relative path within the bucket
            enc_path = "encryption.json"
            import logging
            logger2 = logging.getLogger(__name__)
            logger2.warning(f"[deps] is_encrypted bucket_url={bucket_url} index={index} storage={storage}")
            try:
                enc_data = storage.read_bytes(enc_path)
                logger2.warning(f"[deps] is_encrypted read_bytes success, data={enc_data[:50]}")
                return True
            except (FileNotFoundError, Exception) as e:
                logger2.warning(f"[deps] is_encrypted read_bytes failed: {e}")
                return False

        possible_paths = [
            os.path.join(self.data_dir, index),
            os.path.join(self.data_dir, index, "..", index),
        ]
        for index_path in possible_paths:
            enc_path = os.path.join(index_path, "encryption.json")
            if os.path.isfile(enc_path):
                return True
        return False

    def _get_bucket_storage(self, bucket_url: str) -> URLStorageAdapter:
        """Get or create a URLStorageAdapter for the given bucket URL.

        For encrypted bucket URLs, the adapter is NOT cached — each new session
        must re-probe and re-authenticate so cache isolation is enforced.
        Non-encrypted buckets use a shared cached adapter with global cache.
        """
        import logging
        logger = logging.getLogger(__name__)

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
        if not index.replace("_", "").replace("-", "").replace("/", "").isalnum():
            raise HTTPException(400, f"Invalid index name: {index}")

        cache_key = self._engine_cache_key(index, bucket_url)
        engine = self._engines.get(cache_key)

        if engine is None:
            from flatseek.core.query_engine import QueryEngine

            # Determine which storage to use
            if bucket_url:
                # Per-request bucket URL → create URLStorageAdapter
                storage = self._get_bucket_storage(bucket_url)
            elif isinstance(self._storage, URLStorageAdapter):
                # Default URL storage
                storage = self._storage
            else:
                storage = None

            if storage is not None and isinstance(storage, URLStorageAdapter):
                # URL-based storage (HuggingFace, GitHub, etc.)
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
                possible_paths = [
                    os.path.join(self.data_dir, index),
                    os.path.join(self.data_dir, index, "..", index),
                ]
                for path in possible_paths:
                    if os.path.isdir(os.path.join(path, "index")):
                        engine = QueryEngine(path, storage=self._storage)
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
                       Caches the URLStorageAdapter for reuse in subsequent calls.
        """
        # Determine which storage to use
        if bucket_url:
            storage = self._get_bucket_storage(bucket_url)
            # Cache the bucket URL storage adapter reference so it persists
            return self._list_url_indices(storage)
        elif isinstance(self._storage, URLStorageAdapter):
            return self._list_url_indices(self._storage)

        if not os.path.isdir(self.data_dir):
            return []

        indices = []
        for name in os.listdir(self.data_dir):
            path = os.path.join(self.data_dir, name)
            if os.path.isdir(os.path.join(path, "index")):
                indices.append(name)
            elif os.path.isdir(path):
                for sub in os.listdir(path):
                    sub_path = os.path.join(path, sub)
                    if os.path.isdir(os.path.join(sub_path, "index")):
                        indices.append(f"{name}/{sub}")
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
        """
        from flatseek.core.storage import URLStorageAdapter

        # ── Bucket repos: use web UI URL to find index names ───────────────────
        # Buckets use /resolve/{index_name} URL pattern, not /resolve/main
        # HF API tree/ returns cached/upload metadata, not top-level index dirs
        # Solution: hit the web UI URL (https://huggingface.co/buckets/owner/repo/)
        # which returns HTML with links to each index tree page
        if "huggingface.co" in storage._original_url and "/buckets/" in storage._original_url:
            try:
                import httpx
                # Use the original URL (without /resolve/) to get HTML listing
                web_url = storage._original_url.rstrip("/")
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