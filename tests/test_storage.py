"""Tests for storage adapters."""

import io
import os
import tempfile
from pathlib import Path

import pytest

from flatseek.core.storage import (
    LocalStorageAdapter,
    S3StorageAdapter,
    VercelBlobStorageAdapter,
    StorageConfig,
    create_storage_adapter,
)


class TestLocalStorageAdapter:
    """Tests for LocalStorageAdapter."""

    def test_read_write_bytes(self):
        """Test basic read/write bytes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = LocalStorageAdapter(root=tmpdir)

            # Write
            adapter.write_bytes("test.txt", b"hello world")

            # Read
            data = adapter.read_bytes("test.txt")
            assert data == b"hello world"

    def test_exists(self):
        """Test exists check."""
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = LocalStorageAdapter(root=tmpdir)

            assert not adapter.exists("test.txt")

            adapter.write_bytes("test.txt", b"hello")
            assert adapter.exists("test.txt")

    def test_listdir(self):
        """Test directory listing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = LocalStorageAdapter(root=tmpdir)

            # Create nested structure
            adapter.write_bytes("dir1/file1.txt", b"1")
            adapter.write_bytes("dir1/file2.txt", b"2")
            adapter.write_bytes("dir2/file3.txt", b"3")

            # List root
            contents = adapter.listdir("")
            assert "dir1" in contents
            assert "dir2" in contents

            # List dir1
            contents = adapter.listdir("dir1")
            assert "file1.txt" in contents
            assert "file2.txt" in contents

    def test_mkdir(self):
        """Test directory creation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = LocalStorageAdapter(root=tmpdir)

            adapter.mkdir("newdir/subdir")
            assert (Path(tmpdir) / "newdir" / "subdir").is_dir()

    def test_delete(self):
        """Test file deletion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = LocalStorageAdapter(root=tmpdir)

            adapter.write_bytes("test.txt", b"hello")
            assert adapter.exists("test.txt")

            adapter.delete("test.txt")
            assert not adapter.exists("test.txt")

    def test_open_read(self):
        """Test streaming read."""
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = LocalStorageAdapter(root=tmpdir)
            adapter.write_bytes("test.txt", b"hello world")

            with adapter.open_read("test.txt") as f:
                data = f.read()
            assert data == b"hello world"

    def test_open_write(self):
        """Test streaming write."""
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = LocalStorageAdapter(root=tmpdir)

            with adapter.open_write("test.txt") as f:
                f.write(b"hello world")

            data = adapter.read_bytes("test.txt")
            assert data == b"hello world"

    def test_nested_paths(self):
        """Test nested path operations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = LocalStorageAdapter(root=tmpdir)

            adapter.write_bytes("a/b/c/test.txt", b"nested")
            data = adapter.read_bytes("a/b/c/test.txt")
            assert data == b"nested"

            assert adapter.exists("a/b/c/test.txt")

    def test_base_path(self):
        """Test base_path prefix."""
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = LocalStorageAdapter(root=tmpdir, base_path="prefix")

            adapter.write_bytes("test.txt", b"hello")
            data = adapter.read_bytes("test.txt")
            assert data == b"hello"

            # File should be at prefix/test.txt
            assert (Path(tmpdir) / "prefix" / "test.txt").read_bytes() == b"hello"

    def test_join(self):
        """Test path joining."""
        adapter = LocalStorageAdapter()
        assert adapter.join("a", "b", "c") == "a/b/c"
        assert adapter.join("a/", "/b/", "c") == "a/b/c"


class TestStorageConfig:
    """Tests for StorageConfig."""

    def test_from_env_defaults(self):
        """Test default values from env."""
        # Clear env vars
        env_backup = {}
        for key in ["FLATSEEK_STORAGE_BACKEND", "FLATSEEK_BUCKET", "FLATSEEK_S3_REGION", "FLATSEEK_S3_ENDPOINT", "FLATSEEK_BASE_PATH"]:
            env_backup[key] = os.environ.pop(key, None)

        try:
            config = StorageConfig.from_env()
            assert config.backend == "local"
            assert config.bucket == ""
            assert config.region == ""
            assert config.endpoint_url == ""
            assert config.base_path == ""
        finally:
            # Restore env
            for key, val in env_backup.items():
                if val is not None:
                    os.environ[key] = val

    def test_from_env_with_values(self):
        """Test from env with values."""
        env_backup = {}
        for key in ["FLATSEEK_STORAGE_BACKEND", "FLATSEEK_BUCKET", "FLATSEEK_S3_REGION", "FLATSEEK_S3_ENDPOINT", "FLATSEEK_BASE_PATH"]:
            env_backup[key] = os.environ.pop(key, None)

        try:
            os.environ["FLATSEEK_STORAGE_BACKEND"] = "s3"
            os.environ["FLATSEEK_BUCKET"] = "my-bucket"
            os.environ["FLATSEEK_S3_REGION"] = "us-west-2"
            os.environ["FLATSEEK_BASE_PATH"] = "indexes/prod"

            config = StorageConfig.from_env()
            assert config.backend == "s3"
            assert config.bucket == "my-bucket"
            assert config.region == "us-west-2"
            assert config.base_path == "indexes/prod"
        finally:
            for key, val in env_backup.items():
                if val is not None:
                    os.environ[key] = val
                else:
                    os.environ.pop(key, None)

    def test_rel(self):
        """Test rel path joining."""
        config = StorageConfig(base_path="prefix")
        assert config.rel("test.txt") == "prefix/test.txt"

        config = StorageConfig(base_path="")
        assert config.rel("test.txt") == "test.txt"


class TestCreateStorageAdapter:
    """Tests for create_storage_adapter factory."""

    def test_create_local(self):
        """Test creating local adapter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = StorageConfig(backend="local", base_path=tmpdir)
            adapter = create_storage_adapter(config)
            assert isinstance(adapter, LocalStorageAdapter)

    def test_create_s3(self):
        """Test creating S3 adapter (mock)."""
        config = StorageConfig(
            backend="s3",
            bucket="test-bucket",
            region="us-east-1",
        )
        adapter = create_storage_adapter(config)
        assert isinstance(adapter, S3StorageAdapter)
        assert adapter.bucket == "test-bucket"

    def test_create_vercel_blob(self):
        """Test creating Vercel Blob adapter."""
        config = StorageConfig(
            backend="vercel-blob",
            bucket="test-token",
        )
        adapter = create_storage_adapter(config)
        assert isinstance(adapter, VercelBlobStorageAdapter)

    def test_create_unknown_backend(self):
        """Test unknown backend falls back to local."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = StorageConfig(backend="unknown", base_path=tmpdir)
            adapter = create_storage_adapter(config)
            assert isinstance(adapter, LocalStorageAdapter)

    def test_create_from_env_local(self):
        """Test creating from env with local backend."""
        env_backup = {}
        for key in ["FLATSEEK_STORAGE_BACKEND", "FLATSEEK_BUCKET", "FLATSEEK_S3_REGION", "FLATSEEK_S3_ENDPOINT", "FLATSEEK_BASE_PATH"]:
            env_backup[key] = os.environ.pop(key, None)

        try:
            os.environ["FLATSEEK_STORAGE_BACKEND"] = "local"
            os.environ["FLATSEEK_BASE_PATH"] = "/tmp"

            adapter = create_storage_adapter()
            assert isinstance(adapter, LocalStorageAdapter)
        finally:
            for key, val in env_backup.items():
                if val is not None:
                    os.environ[key] = val
                else:
                    os.environ.pop(key, None)


class TestS3StorageAdapter:
    """Tests for S3StorageAdapter (mocked)."""

    def test_bucket_config(self):
        """Test S3 adapter config."""
        adapter = S3StorageAdapter(
            bucket="my-bucket",
            region="eu-west-1",
            endpoint_url="https://custom.endpoint",
            base_path="path/to/index",
        )
        assert adapter.bucket == "my-bucket"
        assert adapter.base_path == "path/to/index"

    def test_key_generation(self):
        """Test S3 key generation."""
        adapter = S3StorageAdapter(
            bucket="my-bucket",
            base_path="prefix",
        )
        assert adapter._key("test.txt") == "prefix/test.txt"

        adapter2 = S3StorageAdapter(
            bucket="my-bucket",
            base_path="",
        )
        assert adapter2._key("test.txt") == "test.txt"


class TestVercelBlobStorageAdapter:
    """Tests for VercelBlobStorageAdapter."""

    def test_token_extraction(self):
        """Test token extraction from URL-like token."""
        adapter = VercelBlobStorageAdapter(
            bucket="abc123hash",
            base_path="my-index",
        )
        # Vercel Blob URL format: https://{token}.public.blob.vercel-storage.com
        assert adapter.bucket == "abc123hash"

    def test_inherits_from_s3(self):
        """Test VercelBlob inherits S3 adapter interface."""
        adapter = VercelBlobStorageAdapter(
            bucket="test-bucket",
            base_path="test",
        )
        # Should have S3-compatible methods
        assert hasattr(adapter, "read_bytes")
        assert hasattr(adapter, "write_bytes")
        assert hasattr(adapter, "listdir")


class TestURLStorageAdapter:
    """Tests for URLStorageAdapter (read-only remote URL adapter)."""

    def test_create_via_factory(self):
        """Test URL adapter creation via factory."""
        from flatseek.core.storage import URLStorageAdapter
        config = StorageConfig(
            backend="url",
            url="https://github.com/user/repo/releases/download/v1.0",
            base_path="article_1k",
        )
        adapter = create_storage_adapter(config)
        assert isinstance(adapter, URLStorageAdapter)
        assert adapter.base_url == "https://github.com/user/repo/releases/download/v1.0"
        assert adapter.base_path == "article_1k"

    def test_url_resolve(self):
        """Test URL resolution."""
        from flatseek.core.storage import URLStorageAdapter
        adapter = URLStorageAdapter(
            url="https://huggingface.co/datasets/user/index/resolve/main",
            base_path="article_1k",
        )
        assert adapter._resolve_url("stats.json") == (
            "https://huggingface.co/datasets/user/index/resolve/main/article_1k/stats.json"
        )

    def test_url_resolve_no_base_path(self):
        """Test URL resolution without base_path."""
        from flatseek.core.storage import URLStorageAdapter
        adapter = URLStorageAdapter(
            url="https://github.com/user/repo/releases/download/v1.0",
        )
        assert adapter._resolve_url("index/stats.json") == (
            "https://github.com/user/repo/releases/download/v1.0/index/stats.json"
        )

    def test_write_not_supported(self):
        """Test that write operations raise NotImplementedError."""
        from flatseek.core.storage import URLStorageAdapter
        adapter = URLStorageAdapter(url="https://example.com")
        with pytest.raises(NotImplementedError):
            adapter.write_bytes("test.txt", b"data")
        with pytest.raises(NotImplementedError):
            adapter.open_write("test.txt")
        with pytest.raises(NotImplementedError):
            adapter.mkdir("dir")
        with pytest.raises(NotImplementedError):
            adapter.delete("test.txt")