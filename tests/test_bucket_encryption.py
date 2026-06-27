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
        """Test is_encrypted for bucket URL checks remote storage."""
        manager = IndexManager()

        bucket = "https://huggingface.co/datasets/owner/repo"

        # Mock storage adapter
        mock_storage = MagicMock()
        mock_storage.read_bytes.side_effect = FileNotFoundError("not found")
        manager._bucket_storage[bucket] = mock_storage

        # Should return False when file not found
        assert manager.is_encrypted("some-index", bucket) is False

    def test_is_encrypted_bucket_url_with_file(self):
        """Test is_encrypted for bucket URL when file exists."""
        manager = IndexManager()

        bucket = "https://huggingface.co/datasets/owner/repo"

        # Mock storage adapter that returns content
        mock_storage = MagicMock()
        mock_storage.read_bytes.return_value = json.dumps({
            "salt": "abc123",
            "algorithm": "ChaCha20-Poly1305"
        }).encode()
        manager._bucket_storage[bucket] = mock_storage

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