"""Tests for bucket URL encryption support."""

import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from flatseek.api.deps import IndexManager
from flatseek.core.query_engine import load_encryption_key, derive_key, encrypt_bytes, decrypt_bytes


class TestLoadEncryptionKeyWithMeta:
    """Tests for load_encryption_key with pre-loaded metadata."""

    def test_load_encryption_key_with_meta(self):
        """Test load_encryption_key accepts meta dict instead of index_dir."""
        meta = {
            "salt": "6992d3f4b9ef8d4de2cd20128bcee4ea275489b3ed6ba7094a9385ed9b3ee7f7",
            "algorithm": "ChaCha20-Poly1305",
            "kdf": "PBKDF2-HMAC-SHA256",
            "iterations": 600000
        }
        passphrase = "test123"

        # Should not raise
        key = load_encryption_key(None, passphrase, meta)
        assert len(key) == 32  # ChaCha20 key is 32 bytes

    def test_load_encryption_key_derives_same_key(self):
        """Test that key derivation is consistent."""
        meta = {
            "salt": "6992d3f4b9ef8d4de2cd20128bcee4ea275489b3ed6ba7094a9385ed9b3ee7f7",
            "algorithm": "ChaCha20-Poly1305",
            "kdf": "PBKDF2-HMAC-SHA256",
            "iterations": 600000
        }
        passphrase = "test123"

        key = load_encryption_key(None, passphrase, meta)

        # Verify by encrypting/decrypting
        test_data = b"hello world"
        encrypted = encrypt_bytes(test_data, key)
        decrypted = decrypt_bytes(encrypted, key)
        assert decrypted == test_data


class TestIndexManagerEncryption:
    """Tests for IndexManager encryption detection."""

    def test_is_encrypted_local_index(self):
        """Test is_encrypted for local index checks filesystem."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = IndexManager()
            # Patch os.path.isfile in deps module to intercept the encryption.json check
            orig_isfile = os.path.isfile
            def patched_isfile(path):
                if path == os.path.join(tmpdir, "test-index", "encryption.json"):
                    return True
                if path == os.path.join(tmpdir, "nonexistent", "encryption.json"):
                    return False
                return orig_isfile(path)
            with patch('flatseek.api.deps.os.path.isfile', patched_isfile):
                with patch.dict(os.environ, {"FLATSEEK_DATA_DIR": tmpdir}):
                    assert manager.is_encrypted("test-index") is True
                    assert manager.is_encrypted("nonexistent") is False

    def test_is_encrypted_bucket_url_via_storage(self):
        """Test is_encrypted for HF dataset URL raises PermissionError when layout is private."""
        manager = IndexManager()

        bucket = "https://huggingface.co/datasets/owner/repo"

        # Mock storage adapter (not actually used — HF API is checked first)
        mock_storage = MagicMock()
        mock_storage.read_bytes.side_effect = FileNotFoundError("not found")
        manager._bucket_storage[bucket] = mock_storage

        # HF API returns 401 for fake repo → layout "private" → raises PermissionError
        with pytest.raises(PermissionError):
            manager.is_encrypted("some-index", bucket)

    def test_is_encrypted_bucket_url_with_file(self):
        """Test is_encrypted for bucket URL when HF layout is non-private."""
        manager = IndexManager()

        bucket = "https://huggingface.co/datasets/owner/repo"

        # Mock HF layout detection to return "dir" (not private)
        # so it reaches _get_bucket_storage
        with patch.object(manager, "_detect_hf_layout", return_value="dir"):
            # Mock _get_bucket_storage to return a mock storage with encryption.json
            mock_storage = MagicMock()
            mock_storage.read_bytes.return_value = json.dumps({
                "salt": "abc123",
                "algorithm": "ChaCha20-Poly1305"
            }).encode()
            with patch.object(manager, "_get_bucket_storage", return_value=mock_storage):
                # Layout is "dir" → reads encryption.json → is encrypted
                assert manager.is_encrypted("some-index", bucket) is True


class TestFLATSEEKVerifyConstant:
    """Tests to ensure FLATSEEK_VERIFY_ constant is used correctly."""

    def test_encrypt_decrypt_round_trip(self):
        """Test basic encrypt/decrypt round trip."""
        passphrase = "test123"
        salt = b"12345678901234567890123456789012"  # 32 bytes

        key = derive_key(passphrase, salt)
        assert len(key) == 32

        # Encrypt and decrypt
        original = b"Hello, Flatseek!"
        encrypted = encrypt_bytes(original, key)
        decrypted = decrypt_bytes(encrypted, key)

        assert decrypted == original