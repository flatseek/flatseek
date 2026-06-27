"""Tests for API encryption authentication flows (single-file, unpacked, bucket)."""

import json
import os
import tempfile
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from flatseek.api.deps import IndexManager
from flatseek.core.query_engine import (
    load_encryption_key,
    encrypt_bytes,
    decrypt_bytes,
)


class TestIndexManagerSelfAsIndex:
    """Tests for unpacked .fsk directory (data_dir IS the index)."""

    def test_list_indices_self_as_index(self):
        """list_indices returns dirname when data_dir has stats.json + index/."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create minimal unpacked structure
            os.makedirs(os.path.join(tmpdir, "index", "00"))
            Path(tmpdir, "index", "00", "test.bin").touch()
            Path(tmpdir, "stats.json").write_text('{"total_docs":10}')
            Path(tmpdir, "encryption.json").write_text(
                '{"salt":"aabbcc","algorithm":"ChaCha20-Poly1305","kdf":"PBKDF2-HMAC-SHA256","iterations":1000}'
            )

            with patch.dict(os.environ, {"FLATSEEK_DATA_DIR": tmpdir}):
                manager = IndexManager()
                indices = manager.list_indices()
                assert len(indices) == 1
                assert os.path.basename(tmpdir) in indices

    def test_is_encrypted_self_as_index(self):
        """is_encrypted returns True when encryption.json exists at data_dir root."""
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "encryption.json").write_text(
                '{"salt":"aabbcc","algorithm":"ChaCha20-Poly1305","kdf":"PBKDF2-HMAC-SHA256","iterations":1000}'
            )
            idx_name = os.path.basename(tmpdir)
            with patch.dict(os.environ, {"FLATSEEK_DATA_DIR": tmpdir}):
                manager = IndexManager()
                # Nested path doesn't exist, so it should use data_dir
                assert manager.is_encrypted(idx_name) is True
                # Wrong name
                assert manager.is_encrypted("nonexistent") is False

    def test_is_encrypted_nested_index(self):
        """is_encrypted checks nested path when data_dir is parent of indices."""
        with tempfile.TemporaryDirectory() as tmpdir:
            idx_dir = os.path.join(tmpdir, "myindex")
            os.makedirs(os.path.join(idx_dir, "index"))
            Path(idx_dir, "encryption.json").write_text(
                '{"salt":"aabbcc","algorithm":"ChaCha20-Poly1305","kdf":"PBKDF2-HMAC-SHA256","iterations":1000}'
            )
            with patch.dict(os.environ, {"FLATSEEK_DATA_DIR": tmpdir}):
                manager = IndexManager()
                assert manager.is_encrypted("myindex") is True
                assert manager.is_encrypted("otherindex") is False


class TestIndexManagerSingleFileMode:
    """Tests for single-file .fsk mode with encryption."""

    def test_is_encrypted_false_when_key_provided(self):
        """is_encrypted returns False when FLATSEEK_FSK_KEY is set."""
        with tempfile.TemporaryDirectory() as tmpdir:
            from flatseek.flatseek_file import FlatseekFileStorageAdapter

            # Create a minimal encrypted .fsk for testing
            # We can't easily create one here, so mock the storage
            with patch.dict(os.environ, {"FLATSEEK_DATA_DIR": tmpdir}):
                manager = IndexManager()

                # Mock _single_file_engine
                mock_storage = MagicMock()
                mock_storage._is_encrypted = True  # index IS encrypted
                mock_engine = MagicMock()
                mock_engine.storage = mock_storage
                mock_engine._enc_key = b"x" * 32  # key IS set
                manager._single_file_engine = mock_engine

                # With key → should return False (unlocked)
                assert manager.is_encrypted("any-index") is False

    def test_is_encrypted_true_when_no_key(self):
        """is_encrypted returns True when key is not provided."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(os.environ, {"FLATSEEK_DATA_DIR": tmpdir}):
                manager = IndexManager()

                mock_storage = MagicMock()
                mock_storage._is_encrypted = True
                mock_engine = MagicMock()
                mock_engine.storage = mock_storage
                mock_engine._enc_key = None  # no key
                manager._single_file_engine = mock_engine

                # No key → should return True (locked, needs password)
                assert manager.is_encrypted("any-index") is True

    def test_is_encrypted_false_when_not_encrypted(self):
        """is_encrypted returns False when storage is not encrypted."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(os.environ, {"FLATSEEK_DATA_DIR": tmpdir}):
                manager = IndexManager()

                mock_storage = MagicMock()
                mock_storage._is_encrypted = False  # not encrypted
                mock_engine = MagicMock()
                mock_engine.storage = mock_storage
                mock_engine._enc_key = None
                manager._single_file_engine = mock_engine

                assert manager.is_encrypted("any-index") is False


class TestAuthenticateWithVerifyToken:
    """Tests for verify_token generation and validation."""

    def test_verify_token_round_trip(self):
        """verify_token encrypt/decrypt round-trip validates correctly."""
        meta = {
            "salt": "6992d3f4b9ef8d4de2cd20128bcee4ea275489b3ed6ba7094a9385ed9b3ee7f7",
            "algorithm": "ChaCha20-Poly1305",
            "kdf": "PBKDF2-HMAC-SHA256",
            "iterations": 600000,
        }
        import secrets

        key = load_encryption_key(None, "secret123", meta)
        token = secrets.token_bytes(16)
        ct = encrypt_bytes(b"FLATSEEK_VERIFY_" + token, key)
        dec = decrypt_bytes(ct, key)
        assert dec.startswith(b"FLATSEEK_VERIFY_")

        # Wrong key fails
        wrong_key = load_encryption_key(None, "wrongpass", meta)
        with pytest.raises(Exception):
            decrypt_bytes(ct, wrong_key)
