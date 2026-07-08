"""Tests for API stats endpoint (GET /_stats)."""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

# Use TestClient for FastAPI route testing
try:
    from fastapi.testclient import TestClient
    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False
    pytest.skip("FastAPI not installed", allow_module_level=True)

from flatseek.api.deps import IndexManager


def _create_minimal_index(data_dir: Path, name: str, stats: dict) -> Path:
    """Create a minimal index structure in data_dir."""
    idx_dir = data_dir / name
    idx_dir.mkdir(parents=True, exist_ok=True)
    (idx_dir / "index").mkdir()
    (idx_dir / "docs").mkdir()
    (idx_dir / "stats.json").write_text(json.dumps(stats))
    return idx_dir


class TestStatsEndpoint:
    """Tests for GET /{index}/_stats endpoint."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        """Set up test environment with a temporary data directory."""
        self.data_dir = tmp_path
        self.env_patch = patch.dict(os.environ, {
            "FLATSEEK_DATA_DIR": str(self.data_dir),
            "FLATSEEK_API_BASE": "http://localhost:8000",
        })
        self.env_patch.start()

        # Create app after setting env
        from flatseek.api.main import app
        self.client = TestClient(app, raise_server_exceptions=False)

    def teardown_method(self):
        self.env_patch.stop()

    def test_stats_returns_description(self):
        """/{index}/_stats should return description from stats.json."""
        _create_minimal_index(self.data_dir, "idx_desc", {
            "total_docs": 100,
            "description": "My test index",
            "columns": {"status": "KEYWORD"},
        })

        response = self.client.get("/idx_desc/_stats")
        assert response.status_code == 200
        data = response.json()
        assert data["description"] == "My test index"

    def test_stats_returns_license_expire(self):
        """/{index}/_stats should return license_expire from stats.json."""
        # Create fresh index with unique name
        idx_dir = self.data_dir / "idx_expire"
        idx_dir.mkdir(parents=True, exist_ok=True)
        (idx_dir / "index").mkdir()
        (idx_dir / "docs").mkdir()
        (idx_dir / "stats.json").write_text(json.dumps({
            "total_docs": 100,
            "license_expire": "2026-12-31 23:59:59 UTC",
            "columns": {},
        }))

        response = self.client.get("/idx_expire/_stats")
        assert response.status_code == 200
        data = response.json()
        assert data["license_expire"] == "2026-12-31 23:59:59 UTC"

    def test_stats_returns_all_fields(self):
        """/{index}/_stats should return all expected fields."""
        idx_dir = self.data_dir / "idx_all"
        idx_dir.mkdir(parents=True, exist_ok=True)
        (idx_dir / "index").mkdir()
        (idx_dir / "docs").mkdir()
        (idx_dir / "stats.json").write_text(json.dumps({
            "total_docs": 500,
            "index_size_mb": 10.5,
            "docs_size_mb": 25.3,
            "description": "Full stats test",
            "columns": {"amount": "FLOAT", "sender": "KEYWORD"},
        }))

        response = self.client.get("/idx_all/_stats")
        assert response.status_code == 200
        data = response.json()

        # Check required fields
        assert data["_index"] == "idx_all"
        assert data["docs"]["count"] == 500
        assert data["store"]["index_size_mb"] == 10.5
        assert data["store"]["docs_size_mb"] == 25.3
        assert data["columns"]["amount"] == "FLOAT"
        assert data["columns"]["sender"] == "KEYWORD"
        assert data["encrypted"] is False
        assert data["description"] == "Full stats test"

    def test_stats_returns_403_for_no_password_on_encrypted(self):
        """GET /_stats on encrypted index without password header should return 403."""
        # Create encrypted index (bucket encryption)
        idx_dir = self.data_dir / "idx_nopass"
        idx_dir.mkdir(parents=True, exist_ok=True)
        (idx_dir / "index").mkdir()
        (idx_dir / "docs").mkdir()
        (idx_dir / "stats.json").write_text(json.dumps({
            "total_docs": 50,
            "columns": {},
        }))
        # Add encryption.json (bucket encryption marker)
        (idx_dir / "encryption.json").write_text(json.dumps({
            "salt": "deadbeef",
            "algorithm": "ChaCha20-Poly1305",
            "kdf": "PBKDF2-HMAC-SHA256",
            "iterations": 1000,
        }))

        response = self.client.get("/idx_nopass/_stats")
        # Should return 403 (Forbidden) because index is encrypted
        # and no X-Index-Password header was provided
        assert response.status_code == 403

    def test_stats_returns_401_for_wrong_passphrase(self):
        """GET /_stats with wrong passphrase should return 401."""
        from flatseek.core.query_engine import load_encryption_key, encrypt_bytes
        import secrets

        # Create encrypted index with REAL encryption
        idx_dir = self.data_dir / "idx_wrongpass"
        idx_dir.mkdir(parents=True, exist_ok=True)
        (idx_dir / "index").mkdir()
        (idx_dir / "docs").mkdir()

        salt = secrets.token_bytes(16)
        correct_pass = "correctpass"
        wrong_pass = "wrongpass"

        # Encrypt stats.json
        enc_meta = {
            "salt": salt.hex(),
            "algorithm": "ChaCha20-Poly1305",
            "kdf": "PBKDF2-HMAC-SHA256",
            "iterations": 1000,
        }
        (idx_dir / "encryption.json").write_text(json.dumps(enc_meta))

        correct_key = load_encryption_key(str(idx_dir), correct_pass, meta=enc_meta)
        stats_bytes = json.dumps({"total_docs": 50, "columns": {}}).encode()
        encrypted_stats = encrypt_bytes(stats_bytes, correct_key)
        (idx_dir / "stats.json").write_bytes(encrypted_stats)

        # Request with wrong passphrase
        response = self.client.get(
            "/idx_wrongpass/_stats",
            headers={"X-Index-Password": wrong_pass}
        )
        # Should return 401 for invalid passphrase
        assert response.status_code == 401


class TestStatsResponseUnit:
    """Unit tests for stats response (without HTTP)."""

    def test_stats_includes_description(self, tmp_path):
        """/_stats should include description from stats.json."""
        idx_dir = tmp_path / "testidx"
        idx_dir.mkdir()
        (idx_dir / "index").mkdir()
        (idx_dir / "docs").mkdir()

        stats = {
            "total_docs": 100,
            "description": "Test index description",
            "columns": {"status": "KEYWORD"},
        }
        (idx_dir / "stats.json").write_text(json.dumps(stats))

        with patch.dict(os.environ, {"FLATSEEK_DATA_DIR": str(tmp_path)}):
            manager = IndexManager()
            engine = manager.get_engine("testidx")
            assert engine.stats.get("description") == "Test index description"

    def test_stats_includes_license_expire(self, tmp_path):
        """/_stats should include license_expire from stats.json."""
        idx_dir = tmp_path / "testidx"
        idx_dir.mkdir()
        (idx_dir / "index").mkdir()
        (idx_dir / "docs").mkdir()

        stats = {
            "total_docs": 100,
            "license_expire": "2026-12-31 23:59:59 UTC",
            "columns": {},
        }
        (idx_dir / "stats.json").write_text(json.dumps(stats))

        with patch.dict(os.environ, {"FLATSEEK_DATA_DIR": str(tmp_path)}):
            manager = IndexManager()
            engine = manager.get_engine("testidx")
            assert engine.stats.get("license_expire") == "2026-12-31 23:59:59 UTC"


class TestInvalidPassphraseErrorCode:
    """Tests for 401 error codes on invalid passphrase."""

    def test_wrong_key_causes_reload_stats_to_fail(self, tmp_path):
        """With wrong passphrase key, reload_stats should raise (causing 401 in API)."""
        idx_dir = tmp_path / "testidx"
        idx_dir.mkdir()
        (idx_dir / "index").mkdir()
        (idx_dir / "docs").mkdir()
        (idx_dir / "stats.json").write_text(json.dumps({"total_docs": 10, "columns": {}}))

        from flatseek.core.query_engine import load_encryption_key, encrypt_bytes
        import secrets

        salt = secrets.token_bytes(32)
        correct_pass = "correctpass123"
        wrong_pass = "wrongpass456"

        enc_meta = {
            "salt": salt.hex(),
            "algorithm": "ChaCha20-Poly1305",
            "kdf": "PBKDF2-HMAC-SHA256",
            "iterations": 1000,
        }
        (idx_dir / "encryption.json").write_text(json.dumps(enc_meta))

        correct_key = load_encryption_key(str(idx_dir), correct_pass, meta=enc_meta)
        stats_bytes = json.dumps({"total_docs": 10, "columns": {}}).encode()
        encrypted_stats = encrypt_bytes(stats_bytes, correct_key)
        (idx_dir / "stats.json").write_bytes(encrypted_stats)

        wrong_key = load_encryption_key(str(idx_dir), wrong_pass, meta=enc_meta)

        with patch.dict(os.environ, {"FLATSEEK_DATA_DIR": str(tmp_path)}):
            manager = IndexManager()
            engine = manager.get_engine("testidx")
            engine.set_key(wrong_key)

            from cryptography.fernet import InvalidToken
            with pytest.raises(InvalidToken):
                engine.reload_stats()
