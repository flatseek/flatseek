"""Storage abstraction layer for local filesystem, S3, and Vercel Blob."""

from __future__ import annotations

import io
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO, TYPE_CHECKING

if TYPE_CHECKING:
    import boto3

# Environment variable names
ENV_STORAGE_BACKEND = "FLATSEEK_STORAGE_BACKEND"
ENV_BUCKET = "FLATSEEK_BUCKET"
ENV_S3_REGION = "FLATSEEK_S3_REGION"
ENV_S3_ENDPOINT = "FLATSEEK_S3_ENDPOINT"
ENV_BASE_PATH = "FLATSEEK_BASE_PATH"
ENV_STORAGE_URL = "FLATSEEK_STORAGE_URL"


@dataclass
class StorageConfig:
    """Configuration for storage backend."""

    backend: str = "local"  # "local" | "s3" | "vercel-blob" | "url"
    bucket: str = ""  # bucket name
    region: str = ""  # AWS region for S3
    endpoint_url: str = ""  # custom endpoint (for S3-compatible like MinIO, Vercel Blob)
    base_path: str = ""  # prefix within bucket
    url: str = ""  # URL prefix for remote index (github, huggingface, or generic URL)

    @classmethod
    def from_env(cls) -> "StorageConfig":
        """Create config from environment variables."""
        return cls(
            backend=os.environ.get(ENV_STORAGE_BACKEND, "local"),
            bucket=os.environ.get(ENV_BUCKET, ""),
            region=os.environ.get(ENV_S3_REGION, ""),
            endpoint_url=os.environ.get(ENV_S3_ENDPOINT, ""),
            base_path=os.environ.get(ENV_BASE_PATH, ""),
            url=os.environ.get(ENV_STORAGE_URL, ""),
        )

    def rel(self, path: str) -> str:
        """Join base_path with relative path."""
        if self.base_path:
            return f"{self.base_path}/{path}".strip("/")
        return path


class StorageAdapter(ABC):
    """Abstract storage interface for read/write operations."""

    # ─── Read ─────────────────────────────────────────────────────────────────

    @abstractmethod
    def read_bytes(self, rel_path: str) -> bytes:
        """Read entire file as bytes."""
        ...

    @abstractmethod
    def open_read(self, rel_path: str) -> BinaryIO:
        """Open file for streaming read. Caller must close the returned object."""
        ...

    @abstractmethod
    def exists(self, rel_path: str) -> bool:
        """Check if a file exists."""
        ...

    @abstractmethod
    def listdir(self, rel_path: str) -> list[str]:
        """List directory contents. Returns basenames only (no prefix/suffix)."""
        ...

    # ─── Write ─────────────────────────────────────────────────────────────────

    @abstractmethod
    def write_bytes(self, rel_path: str, data: bytes) -> None:
        """Write entire file from bytes."""
        ...

    @abstractmethod
    def open_write(self, rel_path: str) -> BinaryIO:
        """Open file for streaming write. Returns a context manager.

        Usage:
            with adapter.open_write("path/to/file") as f:
                f.write(data)
        """
        ...

    @abstractmethod
    def mkdir(self, rel_path: str) -> None:
        """Create directory (recursive). No-op if already exists."""
        ...

    @abstractmethod
    def delete(self, rel_path: str) -> None:
        """Delete a file. No-op if doesn't exist."""
        ...

    # ─── Utilities ─────────────────────────────────────────────────────────────

    def join(self, *parts: str) -> str:
        """Join path components with / (always forward slash, S3-compatible)."""
        return "/".join(p.strip("/") for p in parts if p.strip("/"))


class LocalStorageAdapter(StorageAdapter):
    """Local filesystem storage adapter using pathlib."""

    def __init__(self, root: str | Path | None = None, base_path: str = ""):
        """
        Args:
            root: Local root directory. Defaults to current working directory.
            base_path: Prefix path within root (for multi-tenant setups).
        """
        self.root = Path(root) if root else Path.cwd()
        self.base_path = base_path

    def _resolve(self, rel_path: str) -> Path:
        parts = rel_path.split("/")
        return self.root / self.base_path / "/" .join(parts) if self.base_path else self.root / rel_path

    def read_bytes(self, rel_path: str) -> bytes:
        return self._resolve(rel_path).read_bytes()

    def open_read(self, rel_path: str) -> BinaryIO:
        return self._resolve(rel_path).open("rb")

    def exists(self, rel_path: str) -> bool:
        return self._resolve(rel_path).is_file()

    def listdir(self, rel_path: str) -> list[str]:
        path = self._resolve(rel_path)
        if path.is_dir():
            return [p.name for p in path.iterdir()]
        # S3-style: list objects with prefix, return immediate children only
        # For local, treat as prefix match
        prefix = str(path) + "/"
        parent = path.parent
        if not parent.is_dir():
            return []
        results = []
        for p in parent.iterdir():
            p_str = str(p)
            if p_str.startswith(prefix):
                name = p_str[len(prefix):].split("/")[0]
                if name and name not in results:
                    print(name)
                    results.append(name)
        return results

    def write_bytes(self, rel_path: str, data: bytes) -> None:
        path = self._resolve(rel_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def open_write(self, rel_path: str) -> BinaryIO:
        path = self._resolve(rel_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        return path.open("wb")

    def mkdir(self, rel_path: str) -> None:
        self._resolve(rel_path).mkdir(parents=True, exist_ok=True)

    def delete(self, rel_path: str) -> None:
        path = self._resolve(rel_path)
        if path.is_file():
            path.unlink()
        elif path.is_dir():
            import shutil
            shutil.rmtree(path)


class S3StorageAdapter(StorageAdapter):
    """S3 storage adapter using boto3."""

    def __init__(
        self,
        bucket: str,
        region: str = "",
        endpoint_url: str = "",
        base_path: str = "",
        aws_access_key_id: str = "",
        aws_secret_access_key: str = "",
    ):
        """
        Args:
            bucket: S3 bucket name.
            region: AWS region (e.g., 'us-east-1').
            endpoint_url: Custom endpoint URL for S3-compatible storage (MinIO, etc.).
            base_path: Prefix within bucket.
            aws_access_key_id: AWS access key. Defaults to env AWS_ACCESS_KEY_ID.
            aws_secret_access_key: AWS secret key. Defaults to env AWS_SECRET_ACCESS_KEY.
        """
        self.bucket = bucket
        self.base_path = base_path
        self._client: "boto3.resources.base.ServiceResource | None" = None
        self._client_kwargs: dict = {
            "region_name": region or None,
            "endpoint_url": endpoint_url or None,
        }
        if aws_access_key_id and aws_secret_access_key:
            self._client_kwargs["aws_access_key_id"] = aws_access_key_id
            self._client_kwargs["aws_secret_access_key"] = aws_secret_access_key

    def _client(self):
        """Lazy-load boto3 client."""
        if self._client is None:
            import boto3
            self._client = boto3.resource("s3", **self._client_kwargs)
        return self._client

    def _key(self, rel_path: str) -> str:
        parts = rel_path.split("/")
        if self.base_path:
            return "/".join([self.base_path] + parts)
        return rel_path

    def read_bytes(self, rel_path: str) -> bytes:
        obj = self._client().Object(self.bucket, self._key(rel_path))
        return obj.get()["Body"].read()

    def open_read(self, rel_path: str) -> BinaryIO:
        obj = self._client().Object(self.bucket, self._key(rel_path))
        return io.BytesIO(obj.get()["Body"].read())

    def exists(self, rel_path: str) -> bool:
        try:
            self._client().Object(self.bucket, self._key(rel_path)).load()
            return True
        except Exception:
            return False

    def listdir(self, rel_path: str) -> list[str]:
        prefix = self._key(rel_path)
        if prefix:
            prefix += "/"
        paginator = self._client().meta.client.get_paginator("list_objects_v2")
        results = set()
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix, Delimiter="/"):
            for obj in page.get("CommonPrefixes", []):
                name = obj["Prefix"][len(prefix):].rstrip("/")
                if name:
                    results.add(name)
            for obj in page.get("Contents", []):
                name = os.path.basename(obj["Key"])
                if name and name != obj["Key"]:
                    results.add(name)
        return sorted(results)

    def write_bytes(self, rel_path: str, data: bytes) -> None:
        obj = self._client().Object(self.bucket, self._key(rel_path))
        obj.put(Body=data)

    def open_write(self, rel_path: str) -> BinaryIO:
        buf = io.BytesIO()
        return _S3WriteBuffer(self._client(), self.bucket, self._key(rel_path), buf)

    def mkdir(self, rel_path: str) -> None:
        # S3 has no real directories; ensure the path exists as a prefix
        # by writing a zero-byte object with trailing slash
        if rel_path:
            key = self._key(rel_path)
            if not key.endswith("/"):
                key += "/"
            try:
                self._client().Object(self.bucket, key).load()
            except Exception:
                self._client().Object(self.bucket, key).put(Body=b"")

    def delete(self, rel_path: str) -> None:
        self._client().Object(self.bucket, self._key(rel_path)).delete()


class _S3WriteBuffer(io.BytesIO):
    """Temporary buffer that uploads to S3 on close()."""

    def __init__(self, client, bucket: str, key: str, initial: io.BytesIO | None = None):
        super().__init__(initial.read() if initial else b"")
        self._client = client
        self._bucket = bucket
        self._key = key

    def close(self):
        if self.getvalue():
            self._client.Bucket(self._bucket).Object(self._key).put(Body=self.getvalue())
        super().close()


class _VercelBlobWriteBuffer(io.BytesIO):
    """Temporary buffer that uploads to Vercel Blob on close()."""

    def __init__(self, client, key: str, headers: dict):
        super().__init__()
        self._client = client
        self._key = key
        self._headers = headers

    def close(self):
        if self.getvalue():
            url = f"{VercelBlobStorageAdapter.API_URL}/?pathname={self._key}"
            resp = self._client.put(url, content=self.getvalue(), headers=self._headers)
            resp.raise_for_status()
        super().close()


class VercelBlobStorageAdapter(StorageAdapter):
    """Vercel Blob storage adapter using direct HTTP API.

    Vercel Blob provides a REST API accessible via https://vercel.com/api/blob.
    This adapter uses httpx for HTTP requests (already a project dependency).
    """

    API_URL = "https://vercel.com/api/blob"
    API_VERSION = "2024-06-06"

    def __init__(
        self,
        bucket: str = "",
        base_path: str = "",
        vercel_token: str = "",
    ):
        """
        Args:
            bucket: Vercel Blob store ID (from BLOB_READ_WRITE_TOKEN).
            base_path: Prefix within blob storage.
            vercel_token: Token for authentication. Defaults to env BLOB_READ_WRITE_TOKEN.
        """
        import os as _os
        token = vercel_token or _os.environ.get("BLOB_READ_WRITE_TOKEN", "")

        # Extract store ID from token if not explicitly provided
        # Token format: vercel_blob_rw_{storeId}_{random}
        if not bucket and token:
            parts = token.split("_")
            if len(parts) >= 4:
                bucket = parts[3].lower()

        self.bucket = bucket or ""
        self.base_path = base_path
        self._token = token
        self.__client: "httpx.Client | None" = None

    def _get_client(self) -> "httpx.Client":
        """Lazy-load httpx client."""
        if self.__client is None:
            import httpx
            self.__client = httpx.Client(
                timeout=60.0,
                headers={
                    "Authorization": f"Bearer {self._token}",
                    "x-api-version": self.API_VERSION,
                }
            )
        return self.__client

    def _key(self, rel_path: str) -> str:
        """Build the blob pathname with base_path prefix.

        Handles both absolute and relative paths, stripping common prefixes.
        """
        import pathlib
        p = pathlib.Path(rel_path)
        parts = p.parts
        base_parts = self.base_path.split("/") if self.base_path else []

        if p.is_absolute():
            # Absolute path - find and strip the base_path from it
            if base_parts:
                for i, part in enumerate(parts):
                    if part == base_parts[0]:
                        # Check if subsequent parts match
                        match = True
                        for j, bp in enumerate(base_parts):
                            if i + j >= len(parts) or parts[i + j] != bp:
                                match = False
                                break
                        if match:
                            rel_path = "/".join(parts[i + len(base_parts):])
                            break
                else:
                    rel_path = str(p.name)
            else:
                rel_path = str(p.name)
        else:
            # Relative path - strip leading ./
            # Also strip common prefixes like ./data/ or ./output/ if base_path is set
            if str(p).startswith("./"):
                rel_path = str(p)[2:]
            else:
                rel_path = str(p)

            # If base_path is set, check if rel_path starts with base_path component
            # and strip it to avoid doubling (e.g., ./data/article_1k with base_path=article_1k)
            if base_parts and not rel_path.startswith(base_parts[0]):
                # Check if path starts with base_path as a directory component
                for i, part in enumerate(rel_path.split("/")):
                    if part == base_parts[0]:
                        rel_path = "/".join(rel_path.split("/")[i+1:])
                        break

        rel_path = rel_path.strip("/")
        if self.base_path:
            return f"{self.base_path}/{rel_path}"
        return rel_path

    def _headers(self) -> dict:
        """Build request headers."""
        return {
            "Authorization": f"Bearer {self._token}",
            "x-api-version": self.API_VERSION,
            "x-vercel-blob-access": "public",
        }

    def read_bytes(self, rel_path: str) -> bytes:
        """Read entire file as bytes via direct blob URL."""
        import httpx
        key = self._key(rel_path)
        # Use direct blob URL format: https://{storeId}.public.blob.vercel-storage.com/{pathname}
        url = f"https://{self.bucket}.public.blob.vercel-storage.com/{key}"
        resp = self._get_client().get(url)
        if resp.status_code == 404:
            raise FileNotFoundError(f"Blob not found: {rel_path}")
        resp.raise_for_status()
        return resp.content

    def open_read(self, rel_path: str) -> BinaryIO:
        """Open file for streaming read."""
        return io.BytesIO(self.read_bytes(rel_path))

    def exists(self, rel_path: str) -> bool:
        """Check if a blob exists via HEAD request on direct blob URL."""
        import httpx
        key = self._key(rel_path)
        url = f"https://{self.bucket}.public.blob.vercel-storage.com/{key}"
        try:
            resp = self._get_client().head(url)
            return resp.status_code == 200
        except httpx.HTTPError:
            return False

    def listdir(self, rel_path: str) -> list[str]:
        """List directory contents via list API."""
        import httpx
        prefix = self._key(rel_path)
        if prefix:
            prefix += "/"
        url = f"{self.API_URL}?prefix={prefix}"
        resp = self._get_client().get(url)
        resp.raise_for_status()
        data = resp.json()
        results = []
        for blob in data.get("blobs", []):
            pathname = blob.get("pathname", "")
            name = os.path.basename(pathname)
            if name and name != pathname:
                results.append(name)
        return sorted(results)

    def write_bytes(self, rel_path: str, data: bytes) -> None:
        """Write entire file via PUT request."""
        import httpx
        key = self._key(rel_path)
        url = f"{self.API_URL}/?pathname={key}"
        headers = {
            **self._headers(),
            "Content-Type": "application/octet-stream",
            "x-allow-overwrite": "1",  # Vercel Blob requires this to overwrite existing blobs
        }
        resp = self._get_client().put(url, content=data, headers=headers)
        resp.raise_for_status()

    def open_write(self, rel_path: str) -> BinaryIO:
        """Open file for streaming write."""
        headers = {**self._headers(), "x-allow-overwrite": "1"}
        return _VercelBlobWriteBuffer(self._get_client(), self._key(rel_path), headers)

    def mkdir(self, rel_path: str) -> None:
        """Create directory marker (folder) via PUT with trailing slash."""
        import httpx
        key = self._key(rel_path)
        if not key.endswith("/"):
            key += "/"
        url = f"{self.API_URL}/?pathname={key}"
        headers = {**self._headers(), "Content-Type": "application/octet-stream"}
        try:
            resp = self._get_client().put(url, content=b"", headers=headers)
            resp.raise_for_status()
        except httpx.HTTPError:
            pass  # Folder may already exist

    def delete(self, rel_path: str) -> None:
        """Delete a blob via DELETE request."""
        import httpx
        url = f"{self.API_URL}/?pathname={self._key(rel_path)}"
        try:
            resp = self._get_client().delete(url)
            if resp.status_code not in (204, 404):
                resp.raise_for_status()
        except httpx.HTTPError:
            pass


class URLStorageAdapter(StorageAdapter):
    """Read-only storage adapter for remote URLs (GitHub releases, HuggingFace, generic URLs).

    Supports:
    - GitHub release URLs: https://github.com/{owner}/{repo}/releases/download/{tag}/{filename}
    - HuggingFace URLs: https://huggingface.co/{repo}/resolve/{revision}/{path}
    - Generic HTTP/HTTPS URLs

    This adapter is READ-ONLY - write operations raise NotImplementedError.

    Files are cached in memory after first read for performance.
    """

    # Class-level cache for all instances (cleared on process restart)
    # Cache structure: encrypted data cached WITH key hash for per-auth-session isolation
    _file_cache: dict[str, bytes] = {}
    _file_cache_keys: dict[str, bytes] = {}  # stores the key used to cache each entry
    _cache_max_size = 50  # max cached files
    _use_cache: bool = True  # set to False to disable all caching
    _process_id: str = str(os.getpid())  # for per-process cache isolation
    _exists_cache: dict[str, bool] = {}
    _exists_cache_max = 500
    _listdir_cache: dict[str, list[str]] = {}  # cache for listdir results
    _listdir_cache_max = 200
    _not_found_cache: dict[str, bool] = {}  # cache for 404 URLs to avoid repeated calls
    _not_found_cache_max = 500

    # HuggingFace raw URL suffix for datasets/models
    _HF_RESOLVE_PATH = "/resolve/main"

    # For bucket repos, the index name IS the revision (no /main/)
    _HF_BUCKET_RESOLVE_PATH = "/resolve"  # adapter appends index name after this

    def __init__(self, url: str, base_path: str = "", cache_enabled: bool = True, is_encrypted: bool = False):
        """
        Args:
            url: Base URL for files. For HuggingFace, auto-adds /resolve/main if not present.
                Examples:
                - https://huggingface.co/datasets/owner/repo (auto → .../resolve/main)
                - https://huggingface.co/datasets/owner/repo/resolve/main (direct raw)
                - https://huggingface.co/datasets/owner/repo/resolve/main/adsb (direct index)
            base_path: Additional path prefix within the URL (for nested index directories).
            cache_enabled: If False, disable file caching.
            is_encrypted: If True, cache entries are key-bound and require matching key.
        """
        self._original_url = url  # Keep original for index name extraction
        self._cache_enabled = cache_enabled
        self._is_encrypted = is_encrypted

        # Auto-add resolve path for HuggingFace URLs if not present
        # Buckets use /resolve/{index_name} (index IS the revision, no /main/)
        # Datasets/models use /resolve/main
        if "huggingface.co" in url and "/resolve/" not in url:
            if "/buckets/" in url:
                # Bucket: index name IS the revision, use /resolve/{index_name}
                url = url.rstrip("/") + self._HF_BUCKET_RESOLVE_PATH
            else:
                # Dataset/model: use /resolve/main
                url = url.rstrip("/") + self._HF_RESOLVE_PATH

        self.base_url = url.rstrip("/")
        self.base_path = base_path.strip("/")

    @property
    def index_name(self) -> str | None:
        """Extract index name from URL for direct index pattern.

        For direct index URLs like https://huggingface.co/datasets/owner/sample-adsb,
        returns 'sample-adsb' (the repo name after /datasets/owner/).

        For root folder URLs like https://huggingface.co/datasets/owner/flatdata-indexes,
        returns None (it's a folder containing multiple indexes).

        The distinction is made by checking if there's a path component AFTER the
        auto-added /resolve/main. If the original URL ends with just the repo name
        (no trailing path), it's treated as a direct index.
        """
        if "huggingface.co" not in self._original_url:
            return None

        original = self._original_url.rstrip("/")

        # If original URL has /resolve/, use the existing logic
        if "/resolve/" in original:
            parts = original.split("/")
            try:
                hf_idx = parts.index("huggingface.co")
                resolve_idx = parts.index("resolve") if "resolve" in parts else -1
                if resolve_idx > 0 and resolve_idx + 2 < len(parts):
                    # Has path after /resolve/main → multi-index
                    return None
                # /resolve/main only → repo name is index
                return parts[hf_idx + 3] if hf_idx + 3 < len(parts) else None
            except (ValueError, IndexError):
                return None

        # No /resolve/ in original URL
        # Check if there's content after the repo name that would indicate multi-index
        parts = original.split("/")
        try:
            hf_idx = parts.index("huggingface.co")
            # parts: [..., 'huggingface.co', 'datasets', 'owner', 'repo']
            # repo is at hf_idx + 3
            if hf_idx + 3 < len(parts):
                repo = parts[hf_idx + 3]
                # Check if there's more path after the repo (would be multi-index)
                if hf_idx + 4 < len(parts):
                    # Has more path components → this is a nested path within repo
                    return None
                # No more path → this is a direct index (single index named repo)
                return repo
        except (ValueError, IndexError):
            return None

        return None

    def _resolve_url(self, rel_path: str) -> str:
        """Build full URL for a given relative path."""
        path = rel_path.strip("/")
        if self.base_path:
            full_path = f"{self.base_path}/{path}"
        else:
            full_path = path
        return f"{self.base_url}/{full_path}"

    def read_bytes(self, rel_path: str, encryption_key: bytes | None = None) -> bytes:
        """Read entire file as bytes from URL.

        For encrypted bucket indexes, caching is only enabled AFTER the session has
        been authenticated (i.e., encryption_key is provided). Unauthenticated
        sessions always hit the network.

        Args:
            rel_path: Relative path within the bucket
            encryption_key: If provided, session is authenticated — key-bound cache is active.
                           If None, session is not authenticated — no caching (must re-auth).
        """
        import httpx
        cache_key = self._cache_key(rel_path)

        # Check cache: only use cache when session is authenticated (encryption_key provided)
        # For unauthenticated sessions (key=None), always fetch from network
        if self._cache_enabled and encryption_key is not None and cache_key in URLStorageAdapter._file_cache:
            stored_key = URLStorageAdapter._file_cache_keys.get(cache_key)
            if stored_key and stored_key == encryption_key:
                # Key matches — session authenticated, return cached data
                return URLStorageAdapter._file_cache[cache_key]
            # Key mismatch — must re-fetch and re-cache with correct key

        url = self._resolve_url(rel_path)
        # Check 404 cache first to avoid repeated failed requests
        # Skip 404 cache for encrypted buckets — each session must re-probe independently
        if self._cache_enabled and not self._is_encrypted and url in URLStorageAdapter._not_found_cache:
            raise FileNotFoundError(f"URL not found (cached): {url}")

        response = httpx.get(url, timeout=60.0, follow_redirects=True)
        if response.status_code == 404:
            # Cache the 404 to avoid repeated calls (skip for encrypted buckets)
            if self._cache_enabled and not self._is_encrypted and len(URLStorageAdapter._not_found_cache) < URLStorageAdapter._not_found_cache_max:
                URLStorageAdapter._not_found_cache[url] = True
            raise FileNotFoundError(f"URL not found: {url}")

        response.raise_for_status()
        data = response.content

        # Cache only for authenticated sessions (encryption_key provided)
        # For unauthenticated sessions (key=None), data is NOT cached
        if self._cache_enabled and encryption_key is not None and len(URLStorageAdapter._file_cache) < URLStorageAdapter._cache_max_size:
            URLStorageAdapter._file_cache[cache_key] = data
            URLStorageAdapter._file_cache_keys[cache_key] = encryption_key

        return data

    def _cache_key(self, rel_path: str) -> str:
        """Generate cache key from base_url + base_path + rel_path."""
        return f"{self.base_url}|{self.base_path}|{rel_path}"

    def open_read(self, rel_path: str) -> BinaryIO:
        """Open file for streaming read."""
        return io.BytesIO(self.read_bytes(rel_path))

    def exists(self, rel_path: str) -> bool:
        """Check if a file exists via GET request (cached after first check).

        Note: existence checks for encryption.json are NOT cached to prevent
        security issues where an unauthenticated probe result would hide the
        encryption status from future authenticated sessions.
        """
        import httpx
        cache_key = self._cache_key(rel_path)

        if self._cache_enabled and cache_key in URLStorageAdapter._exists_cache:
            # Never cache encryption.json existence — it determines auth requirements
            if rel_path == "encryption.json":
                pass  # Fall through to live probe
            else:
                return URLStorageAdapter._exists_cache[cache_key]

        url = self._resolve_url(rel_path)
        try:
            response = httpx.get(url, timeout=10.0, follow_redirects=True)
            result = response.status_code == 200
        except httpx.HTTPError:
            result = False

        # Cache if enabled and within limits
        # IMPORTANT: never cache encryption.json — it determines the auth requirement.
        # If an unauthenticated probe caches False (file not found), future authenticated
        # sessions would incorrectly think the bucket is not encrypted.
        # Conversely, caching True here hides the encryption status from other sessions.
        if self._cache_enabled and len(URLStorageAdapter._exists_cache) < URLStorageAdapter._exists_cache_max:
            if rel_path != "encryption.json":
                URLStorageAdapter._exists_cache[cache_key] = result

        return result

    def listdir(self, rel_path: str) -> list[str]:
        """List directory contents (cached).

        For encrypted buckets, caching is disabled to ensure each session must
        re-probe and re-authenticate independently.
        """
        import httpx

        # Check cache first (skip for encrypted buckets)
        cache_key = f"{self.base_url}|{self.base_path}|{rel_path}"
        if self._cache_enabled and not self._is_encrypted and cache_key in URLStorageAdapter._listdir_cache:
            return URLStorageAdapter._listdir_cache[cache_key]

        results = []

        # HuggingFace: use tree API directly (no index.json fallback)
        if "huggingface.co" in self.base_url:
            tree_result = self._huggingface_tree(rel_path)
            if tree_result is not None:
                results = tree_result

        # GitHub releases: can't list directories, return empty
        elif "github.com" in self.base_url and "/releases/download/" in self.base_url:
            results = []

        # Cache the result if enabled (skip for encrypted buckets)
        if self._cache_enabled and not self._is_encrypted and len(URLStorageAdapter._listdir_cache) < URLStorageAdapter._listdir_cache_max:
            URLStorageAdapter._listdir_cache[cache_key] = results

        return results

    def _huggingface_tree(self, rel_path: str) -> list[str] | None:
        """List HuggingFace repo tree at given rel_path.

        Uses only the HF tree API (/api/.../tree/...) - no fallback to index.json.

        Logic for root (rel_path=""):
        - Check if root has stats.json → this IS a direct index (single index repo)
        - Otherwise, each folder with stats.json is a sub-index
        """
        import httpx
        if rel_path.find("/") > 1: return None        
        print("panggil hugging", rel_path)

        parts = self.base_url.split("/")
        try:
            idx = parts.index("datasets") if "datasets" in parts else (
                  parts.index("models") if "models" in parts else (
                  parts.index("spaces") if "spaces" in parts else -1))
            if idx < 0 or idx + 2 >= len(parts):
                return None

            repo_type = parts[idx]
            owner = parts[idx + 1]
            repo = parts[idx + 2]

            revision = "main"
            if "resolve" in parts:
                resolve_idx = parts.index("resolve")
                if resolve_idx + 1 < len(parts):
                    revision = parts[resolve_idx + 1]


            tree_url = f"https://huggingface.co/api/{repo_type}/{owner}/{repo}/tree/{revision}"
            if rel_path:
                tree_url += f"/{rel_path}"

            response = httpx.get(tree_url, timeout=30.0, follow_redirects=True)
            if response.status_code != 200:
                return None

            data = response.json()
            results = []
            seen = set()

            for item in data:
                item_path = item.get("path", "")
                item_type = item.get("type", "")

                if rel_path:
                    # Listing a specific folder - extract names at this level
                    if "/" in item_path:
                        name = item_path.split("/")[-1]
                    else:
                        name = item_path
                else:
                    # Listing root - determine if it's an index
                    if item_type == "directory":
                        # Check if this folder contains stats.json (it's an index)
                        folder_path = item_path
                        # We know this is a folder - need to check if it has stats.json
                        # For now, just add the folder name and we'll filter later
                        name = folder_path.split("/")[0] if folder_path else ""
                    else:
                        # File at root level - if it's stats.json, this is a direct index
                        name = item_path.split("/")[-1] if item_path else ""

                print("name", name)
                if name and name not in seen:
                    seen.add(name)
                    results.append(name)

            # If root listing and we got files like stats.json at root, this is a direct index
            # Return just the repo name as the index
            if not rel_path and "stats.json" in results:
                return [self.index_name] if self.index_name else []

            # Filter folders to only those that are indexes (have stats.json or index/)
            # For root listing, verify each candidate is actually an index
            if not rel_path:
                verified = []
                for candidate in results:
                    candidate_stats = f"{candidate}/stats.json"
                    candidate_index = f"{candidate}/index"
                    # Use cached exists check
                    cache_key = self._cache_key(candidate_stats)
                    if cache_key in URLStorageAdapter._exists_cache:
                        if URLStorageAdapter._exists_cache[cache_key]:
                            verified.append(candidate)
                    else:
                        # Make a lightweight check
                        check_url = f"{self.base_url}/{candidate_stats}"
                        try:
                            resp = httpx.head(check_url, timeout=10.0, follow_redirects=True)
                            if resp.status_code == 200:
                                URLStorageAdapter._exists_cache[cache_key] = True
                                verified.append(candidate)
                            else:
                                URLStorageAdapter._exists_cache[cache_key] = False
                        except Exception:
                            URLStorageAdapter._exists_cache[cache_key] = False
                return verified if verified else results

            return results if results else None

        except (ValueError, IndexError, httpx.HTTPError):
            return None

    def write_bytes(self, rel_path: str, data: bytes) -> None:
        """Write is not supported for URL adapter."""
        raise NotImplementedError("URLStorageAdapter is read-only")

    def open_write(self, rel_path: str) -> BinaryIO:
        """Write is not supported for URL adapter."""
        raise NotImplementedError("URLStorageAdapter is read-only")

    def mkdir(self, rel_path: str) -> None:
        """Write is not supported for URL adapter."""
        raise NotImplementedError("URLStorageAdapter is read-only")

    def delete(self, rel_path: str) -> None:
        """Write is not supported for URL adapter."""
        raise NotImplementedError("URLStorageAdapter is read-only")


class RangeFile:
    """File-like object that reads from a URL via HTTP Range requests.

    Implements the seek + read protocol that ``FlatseekFileStorageAdapter``
    uses internally (``self._fh.seek(...)`` then ``self._fh.read(n)``), so
    the same adapter code can serve .fsk files from a remote URL (HF, S3,
    Vercel Blob, GitHub releases, …) without downloading the whole file.

    Strategy
    --------
    1. ``seek(pos)`` only updates the in-memory cursor (no HTTP).
    2. ``read(n)`` issues one ``GET`` per missing 4 KB block, with
       ``Range: bytes=<start>-<end>``. Cached blocks are reused.
    3. The file size is probed lazily on first read (a small Range request
       that returns either ``Content-Range: bytes X-Y/Z`` or ``416``).

    Supported by all major providers that flatseek uses (Cloudflare-backed
    HF, S3, Vercel Blob, GitHub releases). For providers that don't honor
    Range, the adapter falls back to downloading the whole file once.

    Thread safety: not thread-safe — create one RangeFile per
    FlatseekFileStorageAdapter instance. Concurrent reads from multiple
    threads would need locking (not currently used by the adapter).
    """
    BLOCK_SIZE = 4096

    def __init__(self, url: str, *, timeout: float = 30.0,
                 headers: dict | None = None):
        self._url = url
        self._pos = 0
        self._size: int | None = None
        self._timeout = timeout
        self._headers = dict(headers or {})
        # Block cache: block_idx → bytes
        self._cache: dict[int, bytes] = {}
        # When the server returns the whole file at once (status 200,
        # not honoring Range), we keep it here and serve all reads from
        # this buffer. Avoids multiple round-trips for non-Range servers.
        self._full_data: bytes | None = None
        # Total size, populated on first read
        self._eof_hit = False  # True once we've seen a partial block

    # ── File-like protocol ────────────────────────────────────────────
    def seek(self, pos: int) -> int:
        if pos < 0:
            raise ValueError(f"negative seek: {pos}")
        self._pos = pos
        return pos

    def tell(self) -> int:
        return self._pos

    def read(self, n: int = -1) -> bytes:
        """Read up to ``n`` bytes from current position.

        ``n=-1`` (or 0) reads until EOF.
        """
        # Fast path: if the server already returned the whole file in
        # cache[0] (it didn't honor Range), serve straight from there.
        if self._full_data is not None:
            if n is None or n < 0:
                n = len(self._full_data) - self._pos
            chunk = self._full_data[self._pos:self._pos + n]
            self._pos += len(chunk)
            return chunk

        if n is None or n < 0:
            # Read everything: probe size first
            if self._size is None:
                self._probe_size()
            n = max(0, (self._size or 0) - self._pos)
        out = bytearray()
        remaining = n
        while remaining > 0:
            block_idx, block_offset = divmod(self._pos, self.BLOCK_SIZE)
            chunk = self._fetch_block(block_idx)
            if chunk is None or chunk == b"":
                break  # EOF
            available = len(chunk) - block_offset
            take = min(available, remaining)
            if take <= 0:
                break
            out.extend(chunk[block_offset:block_offset + take])
            self._pos += take
            remaining -= take
        return bytes(out)

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def close(self) -> None:
        """No-op — the HTTP client holds no persistent state."""
        self._cache.clear()

    # ── Internals ────────────────────────────────────────────────────
    def _probe_size(self) -> int:
        """Determine the file's total size via a tiny Range request.

        Some servers return ``Content-Length`` directly; we use a
        0-byte Range request which is the most universally-supported
        way to learn the total size.
        """
        import httpx
        try:
            resp = httpx.get(
                self._url,
                headers={**self._headers, "Range": "bytes=0-0"},
                timeout=self._timeout,
                follow_redirects=True,
            )
        except Exception as e:
            raise IOError(f"RangeFile probe failed for {self._url}: {e}")
        if resp.status_code == 200:
            # Server didn't honor Range — full content
            size = len(resp.content)
            # Cache the whole file so subsequent reads don't need HTTP
            self._full_data = resp.content
            self._cache[0] = resp.content  # also keep for backward compat
            self._eof_hit = True
        elif resp.status_code == 206:
            cr = resp.headers.get("Content-Range", "")
            # Format: "bytes 0-0/<size>"
            try:
                size = int(cr.rsplit("/", 1)[1])
            except (ValueError, IndexError):
                # Fallback to Content-Length (single byte)
                size = int(resp.headers.get("Content-Length", "0")) + 1
            self._cache[0] = resp.content  # 1 byte
        elif resp.status_code == 416:
            # Range not satisfiable — empty file
            size = 0
        else:
            resp.raise_for_status()
            size = 0
        self._size = size
        return size

    def _fetch_block(self, block_idx: int) -> bytes | None:
        """Fetch a 4 KB block, with cache.

        If we already cached a *partial* block (e.g., a 1-byte block
        from the initial size probe), and the caller now wants the
        full block, re-fetch the whole thing. This way the cache holds
        the "largest version of this block we've seen so far".
        """
        if block_idx in self._cache and len(self._cache[block_idx]) >= self.BLOCK_SIZE:
            return self._cache[block_idx]
        if self._size == 0:
            return b""
        if self._eof_hit and block_idx * self.BLOCK_SIZE >= self._size:
            return None  # we know we're past EOF

        import httpx
        start = block_idx * self.BLOCK_SIZE
        end = start + self.BLOCK_SIZE - 1
        try:
            resp = httpx.get(
                self._url,
                headers={**self._headers, "Range": f"bytes={start}-{end}"},
                timeout=self._timeout,
                follow_redirects=True,
            )
        except Exception as e:
            raise IOError(
                f"RangeFile read failed at offset {start} from {self._url}: {e}"
            )

        if resp.status_code == 200:
            # Server ignored Range and returned the whole file.
            # Cache it as the full-data buffer so future reads serve
            # straight from memory without more round-trips.
            self._size = len(resp.content)
            self._full_data = resp.content
            self._eof_hit = True
            return resp.content
        if resp.status_code == 416:
            # Range Not Satisfiable — past EOF
            self._eof_hit = True
            return b""
        if resp.status_code == 206:
            data = resp.content
            # Cache the block
            self._cache[block_idx] = data
            # Update size from Content-Range (covers cases where we
            # haven't probed first, e.g., for very large files)
            if self._size is None:
                cr = resp.headers.get("Content-Range", "")
                try:
                    self._size = int(cr.rsplit("/", 1)[1])
                except (ValueError, IndexError):
                    pass
            return data
        # 4xx/5xx — let the caller see the error
        resp.raise_for_status()
        return None  # unreachable, for type checkers

    def size(self) -> int:
        """Return total file size, probing if necessary."""
        if self._size is None:
            return self._probe_size()
        return self._size
        raise NotImplementedError("URLStorageAdapter is read-only")


def create_storage_adapter(
    config: StorageConfig | None = None,
    path: str | Path | None = None,
) -> StorageAdapter:
    """Factory to create a StorageAdapter from config.

    Args:
        config: StorageConfig instance. If None, reads from environment.
        path: Optional path hint for auto-detection of .flatseek files.

    Returns:
        LocalStorageAdapter by default (backward compatible).
    """
    if config is None:
        config = StorageConfig.from_env()

    # Auto-detect .flatseek files (including .flat alias)
    if path is not None:
        is_url = isinstance(path, str) and (
            path.startswith("http://") or path.startswith("https://")
        )
        ends_with_fsk = str(path).endswith((".fsk", ".flatseek", ".flat"))
        if is_url and ends_with_fsk:
            # HTTP URL pointing at a .fsk file — use RangeFile-backed
            # FlatseekFileStorageAdapter (no full download). This is
            # the path that lets Flatlens serve a multi-GB .fsk hosted
            # on HF/S3 without downloading the whole archive.
            from flatseek.flatseek_file import FlatseekFileStorageAdapter
            return FlatseekFileStorageAdapter(path)
        p = Path(path)
        if p.is_file() and ends_with_fsk:
            from flatseek.flatseek_file import FlatseekFileStorageAdapter
            return FlatseekFileStorageAdapter(p)

    if config.backend == "local":
        return LocalStorageAdapter(base_path=config.base_path)
    elif config.backend == "s3":
        return S3StorageAdapter(
            bucket=config.bucket,
            region=config.region,
            endpoint_url=config.endpoint_url,
            base_path=config.base_path,
        )
    elif config.backend == "vercel-blob":
        return VercelBlobStorageAdapter(
            bucket=config.bucket,
            base_path=config.base_path,
        )
    elif config.backend == "url":
        # If the URL points at a .fsk archive, use RangeFile-backed
        # FlatseekFileStorageAdapter (no full download). Otherwise fall
        # back to URLStorageAdapter (directory mode) for HuggingFace
        # dataset repos etc.
        if config.url and config.url.split("?", 1)[0].rstrip("/").endswith(
                (".fsk", ".flatseek", ".flat")):
            from flatseek.flatseek_file import FlatseekFileStorageAdapter
            return FlatseekFileStorageAdapter(config.url)
        return URLStorageAdapter(
            url=config.url,
            base_path=config.base_path,
        )
    else:
        # Unknown backend, fall back to local
        return LocalStorageAdapter(base_path=config.base_path)