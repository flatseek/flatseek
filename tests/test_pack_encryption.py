"""Tests for pack/unpack encryption round-trips."""

import json
import os
import shutil
import tempfile
from pathlib import Path

import pytest

from flatseek.core.query_engine import (
    load_encryption_key,
    encrypt_bytes,
    decrypt_bytes,
    is_encrypted,
)

ENC_MAGIC = b"FLATSEEK\x01"


class TestPackUnpackEncryptionRoundTrip:
    """Pack encrypted source → unpack → verify encryption state preserved."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmpdir = tempfile.mkdtemp()
        self.key = load_encryption_key(
            "/Users/judotens/Works/Codes/tenslab/flatseek/testdata.fresh",
            "secret123",
        )
        yield
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _unpack_to(self, src_fsk, label):
        out = os.path.join(self.tmpdir, label)
        from flatseek.flatseek_file import FlatseekUnpacker
        shutil.rmtree(out, ignore_errors=True)
        FlatseekUnpacker(Path(src_fsk), Path(out), enc_key=None).unpack()
        return out

    def _unpack_with_key(self, src_fsk, label):
        out = os.path.join(self.tmpdir, label)
        from flatseek.flatseek_file import FlatseekUnpacker
        shutil.rmtree(out, ignore_errors=True)
        FlatseekUnpacker(Path(src_fsk), Path(out), enc_key=self.key).unpack()
        return out

    def _enc(self, path):
        return open(path, "rb").read(9) == ENC_MAGIC

    def _first_idx_file(self, out):
        for r, _, fs in os.walk(os.path.join(out, "index")):
            for f in sorted(fs)[:1]:
                return os.path.join(r, f)
        return None

    # ── testdata.fsk: OLD format (plaintext manifest, __b64 values) ──────────

    def test_old_unpack_no_key(self):
        """OLD format, no key: index encrypted, JSON encrypted."""
        out = self._unpack_to(
            "/Users/judotens/Works/Codes/tenslab/flatseek/testdata.fsk",
            "old_no_key",
        )
        assert self._enc(f"{out}/stats.json") is True
        idx = self._first_idx_file(out)
        assert idx is not None
        assert self._enc(idx) is True

    def test_old_unpack_with_key(self):
        """OLD format, with key: index plaintext, JSON encrypted."""
        out = self._unpack_with_key(
            "/Users/judotens/Works/Codes/tenslab/flatseek/testdata.fsk",
            "old_with_key",
        )
        assert self._enc(f"{out}/stats.json") is True  # __b64 — always raw bytes
        idx = self._first_idx_file(out)
        assert idx is not None
        assert self._enc(idx) is False  # index decrypted

    # ── testdata.enc.fsk: NEW format (plaintext manifest, hex-encrypted JSONs) ─

    def test_new_unpack_no_key(self):
        """NEW format, no key: index encrypted, JSON plaintext (manifest readable)."""
        out = self._unpack_to(
            "/Users/judotens/Works/Codes/tenslab/flatseek/testdata.enc.fsk",
            "new_no_key",
        )
        assert self._enc(f"{out}/stats.json") is False
        idx = self._first_idx_file(out)
        assert idx is not None
        assert self._enc(idx) is True

    def test_new_unpack_with_key(self):
        """NEW format, with key: index plaintext, JSON plaintext."""
        out = self._unpack_with_key(
            "/Users/judotens/Works/Codes/tenslab/flatseek/testdata.enc.fsk",
            "new_with_key",
        )
        assert self._enc(f"{out}/stats.json") is False
        idx = self._first_idx_file(out)
        assert idx is not None
        assert self._enc(idx) is False


class TestPackEncryptionPreservesState:
    """Re-pack from unpacked dir → verify encryption state is correct."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmpdir = tempfile.mkdtemp()
        self.key = load_encryption_key(
            "/Users/judotens/Works/Codes/tenslab/flatseek/testdata.fresh",
            "secret123",
        )
        yield
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_repack_from_plaintext_unpacked(self):
        """Pack plaintext dir (with encryption.json) + enc_key → JSON files hex-encrypted,
        index sections stored as-is (plaintext, since source was plaintext)."""
        from flatseek.flatseek_file import FlatseekUnpacker, FlatseekPacker

        # Unpack NEW format with key → plaintext files
        src = os.path.join(self.tmpdir, "src")
        FlatseekUnpacker(
            Path("/Users/judotens/Works/Codes/tenslab/flatseek/testdata.enc.fsk"),
            Path(src),
            enc_key=self.key,
        ).unpack()

        # Re-pack with enc_key → JSON files encrypted, index sections plaintext
        dst = os.path.join(self.tmpdir, "repacked.fsk")
        FlatseekPacker(Path(src), Path(dst), enc_key=self.key).pack()

        # Read manifest from packed .fsk to verify JSON is hex-encrypted
        import struct
        from flatseek.flatseek_file import SID_MANIFEST
        with open(dst, "rb") as f:
            f.read(11)  # skip header
            n_secs = struct.unpack("B", f.read(1))[0]
            for _ in range(n_secs):
                sid, sz = struct.unpack(">II", f.read(8))
                data = f.read(sz)
                if sid == SID_MANIFEST:
                    manifest = json.loads(data)
                    # stats must be hex-encoded (encrypted)
                    assert isinstance(manifest.get("stats"), str), \
                        "stats should be hex-encrypted when packing with enc_key"
                    # _encryption_b64 must be present
                    assert "_encryption_b64" in manifest
                    break


class TestEncryptionKeyDerivation:
    """Tests for load_encryption_key with meta dict (no filesystem needed)."""

    def test_load_with_meta(self):
        """load_encryption_key accepts meta dict + passphrase."""
        meta = {
            "salt": "6992d3f4b9ef8d4de2cd20128bcee4ea275489b3ed6ba7094a9385ed9b3ee7f7",
            "algorithm": "ChaCha20-Poly1305",
            "kdf": "PBKDF2-HMAC-SHA256",
            "iterations": 600000,
        }
        key = load_encryption_key(None, "test123", meta)
        assert len(key) == 32  # ChaCha20 key is 32 bytes

    def test_round_trip(self):
        """Encrypt/decrypt round-trip with derived key."""
        meta = {
            "salt": "6992d3f4b9ef8d4de2cd20128bcee4ea275489b3ed6ba7094a9385ed9b3ee7f7",
            "algorithm": "ChaCha20-Poly1305",
            "kdf": "PBKDF2-HMAC-SHA256",
            "iterations": 600000,
        }
        key = load_encryption_key(None, "test123", meta)
        data = b"hello flatseek"
        enc = encrypt_bytes(data, key)
        dec = decrypt_bytes(enc, key)
        assert dec == data

    def test_wrong_passphrase_fails(self):
        """Wrong passphrase produces wrong key → decrypt fails."""
        meta = {
            "salt": "6992d3f4b9ef8d4de2cd20128bcee4ea275489b3ed6ba7094a9385ed9b3ee7f7",
            "algorithm": "ChaCha20-Poly1305",
            "kdf": "PBKDF2-HMAC-SHA256",
            "iterations": 600000,
        }
        key = load_encryption_key(None, "correct", meta)
        enc = encrypt_bytes(b"test", key)
        wrong_key = load_encryption_key(None, "wrongpass", meta)
        with pytest.raises(Exception):
            decrypt_bytes(enc, wrong_key)

    def test_is_encrypted_helper(self):
        """is_encrypted returns True only for FLATSEEK magic prefix."""
        assert is_encrypted(ENC_MAGIC + b"someciphertext") is True
        assert is_encrypted(b"plaintext json data here") is False
        assert is_encrypted(b'{"key":"value"}') is False
