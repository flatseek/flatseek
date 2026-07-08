"""Tests for enclosed encryption (license/expiration enforcement).

These tests cover:
- encrypt_expired_enclosed / decrypt_expired_enclosed
- pack with expiration (enclosed format)
- unpack of enclosed format
- backward compatibility with normal format
"""

import json
import os
import shutil
import struct
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from flatseek.core.query_engine import (
    decrypt_expired_enclosed,
    derive_key,
    encrypt_bytes,
    encrypt_expired_enclosed,
)
from flatseek.flatseek_file import (
    FlatseekPacker,
    FlatseekUnpacker,
)


# ─── Unit tests for crypto primitives ───────────────────────────────────────────

class TestEnclosedCrypto:
    def test_encrypt_decrypt_roundtrip(self):
        """Basic encrypt/decrypt works."""
        inner_key = os.urandom(32)
        plaintext = b"Hello, enclosed world!"
        expire_ts = 9999999999  # far future

        ct = encrypt_expired_enclosed(plaintext, expire_ts, inner_key)
        result = decrypt_expired_enclosed(ct, inner_key)
        assert result == plaintext

    def test_wrong_key_fails(self):
        """Decryption with wrong key fails."""
        inner_key = os.urandom(32)
        wrong_key = os.urandom(32)
        expire_ts = 9999999999

        ct = encrypt_expired_enclosed(b"test data", expire_ts, inner_key)
        with pytest.raises(Exception):  # ChaCha20Poly1305 will raise
            decrypt_expired_enclosed(ct, wrong_key)

    def test_expired_rejected(self):
        """Expired enclosed data is rejected."""
        inner_key = os.urandom(32)
        expired_ts = 1  # 1970-01-01

        ct = encrypt_expired_enclosed(b"test data", expired_ts, inner_key)
        with pytest.raises(PermissionError) as exc:
            decrypt_expired_enclosed(ct, inner_key)
        assert "expired" in str(exc.value).lower()

    def test_expired_near_future_accepted(self):
        """Future expiration is accepted."""
        inner_key = os.urandom(32)
        future_ts = int(datetime.now(timezone.utc).timestamp()) + 86400  # 1 day from now

        ct = encrypt_expired_enclosed(b"test data", future_ts, inner_key)
        result = decrypt_expired_enclosed(ct, inner_key)
        assert result == b"test data"

    def test_different_inner_keys_produce_different_ciphertext(self):
        """Different keys produce different ciphertexts."""
        plaintext = b"same data"
        expire_ts = 9999999999
        key1 = os.urandom(32)
        key2 = os.urandom(32)

        ct1 = encrypt_expired_enclosed(plaintext, expire_ts, key1)
        ct2 = encrypt_expired_enclosed(plaintext, expire_ts, key2)

        assert ct1 != ct2

    def test_very_long_data(self):
        """Handles large data correctly."""
        inner_key = os.urandom(32)
        expire_ts = 9999999999
        large_data = b"x" * (10 * 1024 * 1024)  # 10 MB

        ct = encrypt_expired_enclosed(large_data, expire_ts, inner_key)
        result = decrypt_expired_enclosed(ct, inner_key)
        assert result == large_data


# ─── Tests for FlatseekPacker._pack_enclosed ───────────────────────────────────

class TestEnclosedPack:
    def _make_minimal_index(self, tmp_path: Path) -> Path:
        """Create a minimal fake index directory."""
        os.makedirs(tmp_path / "index")
        os.makedirs(tmp_path / "docs")
        (tmp_path / "stats.json").write_text('{"total_docs": 100}')
        (tmp_path / "column_map.json").write_text('{"columns": {}}')
        (tmp_path / "manifest.json").write_text('{"version": 1}')
        return tmp_path

    def test_pack_normal_no_magic(self, tmp_path):
        """Normal pack (no expiration) produces FLATSEEK04 magic."""
        tmp_in = tmp_path / "input"
        tmp_in.mkdir()
        out_fsk = tmp_path / "output.fsk"

        self._make_minimal_index(tmp_in)
        packer = FlatseekPacker(tmp_in, out_fsk)
        packer.pack()
        data = out_fsk.read_bytes()
        assert data[:10] == b"FLATSEEK04"

    def test_pack_with_expiration_no_magic(self, tmp_path):
        """Pack with expiration produces enclosed format (no FLATSEEK04 magic)."""
        tmp_in = tmp_path / "input"
        tmp_in.mkdir()
        out_fsk = tmp_path / "output.fsk"

        self._make_minimal_index(tmp_in)
        packer = FlatseekPacker(
            tmp_in, out_fsk,
            expire_at=9999999999,
            master_key=b"test_master_key_32_bytes_here!!!",
        )
        packer.pack()
        data = out_fsk.read_bytes()
        # Enclosed format does NOT start with FLATSEEK04
        assert data[:10] != b"FLATSEEK04"

    def test_enclosed_format_structure(self, tmp_path):
        """Enclosed format has correct structure: salt + outer + inner."""
        tmp_in = tmp_path / "input"
        tmp_in.mkdir()
        out_fsk = tmp_path / "output.fsk"

        self._make_minimal_index(tmp_in)
        packer = FlatseekPacker(
            tmp_in, out_fsk,
            expire_at=9999999999,
            master_key=b"test_master_key_32_bytes_here!!!",
        )
        packer.pack()
        data = out_fsk.read_bytes()

        # Structure: salt(32) + outer_len(4) + outer_ct + inner_ct
        assert len(data) > 36
        salt = data[:32]
        outer_len = int.from_bytes(data[32:36], "little")
        assert outer_len > 0
        assert len(data) > 36 + outer_len


# ─── Tests for FlatseekUnpacker._unpack_enclosed ───────────────────────────────

class TestEnclosedUnpack:
    def _make_minimal_index(self, tmp_path: Path) -> Path:
        os.makedirs(tmp_path / "index")
        os.makedirs(tmp_path / "docs")
        (tmp_path / "stats.json").write_text('{"total_docs": 100}')
        (tmp_path / "column_map.json").write_text('{"columns": {}}')
        (tmp_path / "manifest.json").write_text('{"version": 1}')
        return tmp_path

    def test_unpack_normal_format(self, tmp_path):
        """Normal format unpack works (backward compat)."""
        tmp_in = tmp_path / "input"
        tmp_in.mkdir()
        out_fsk = tmp_path / "output.fsk"
        unpack_dir = tmp_path / "unpacked"

        self._make_minimal_index(tmp_in)
        packer = FlatseekPacker(tmp_in, out_fsk)
        packer.pack()

        unpacker = FlatseekUnpacker(out_fsk, unpack_dir)
        unpacker.unpack()
        assert (unpack_dir / "stats.json").exists()

    def test_unpack_enclosed_requires_passphrase(self, tmp_path):
        """Enclosed unpack fails without passphrase."""
        tmp_in = tmp_path / "input"
        tmp_in.mkdir()
        out_fsk = tmp_path / "output.fsk"
        unpack_dir = tmp_path / "unpacked"

        self._make_minimal_index(tmp_in)
        packer = FlatseekPacker(
            tmp_in, out_fsk,
            expire_at=9999999999,
            master_key=b"test_master_key_32_bytes_here!!!",
        )
        packer.pack()

        unpacker = FlatseekUnpacker(out_fsk, unpack_dir)  # no passphrase
        with pytest.raises(ValueError) as exc:
            unpacker.unpack()
        assert "Passphrase required" in str(exc.value)


# ─── Tests for backward compatibility ────────────────────────────────────────────

class TestBackwardCompatibility:
    def _make_minimal_index(self, tmp_path: Path) -> Path:
        os.makedirs(tmp_path / "index")
        os.makedirs(tmp_path / "docs")
        (tmp_path / "stats.json").write_text('{"total_docs": 100}')
        (tmp_path / "column_map.json").write_text('{"columns": {}}')
        (tmp_path / "manifest.json").write_text('{"version": 1}')
        return tmp_path

    def test_old_format_still_works(self, tmp_path):
        """Normal pack/unpack without expiration works exactly as before."""
        tmp_in = tmp_path / "input"
        tmp_in.mkdir()
        out_fsk = tmp_path / "output.fsk"
        unpack_dir = tmp_path / "unpacked"

        # Pack without expiration
        self._make_minimal_index(tmp_in)
        packer = FlatseekPacker(tmp_in, out_fsk)
        packer.pack()

        # Unpack should work
        unpacker = FlatseekUnpacker(out_fsk, unpack_dir)
        unpacker.unpack()

        assert (unpack_dir / "stats.json").exists()
        stats = json.loads((unpack_dir / "stats.json").read_text())
        assert stats["total_docs"] == 100

    def test_format_detection_normal(self, tmp_path):
        """Normal format correctly detected."""
        tmp_in = tmp_path / "input"
        tmp_in.mkdir()
        out_fsk = tmp_path / "output.fsk"

        self._make_minimal_index(tmp_in)
        packer = FlatseekPacker(tmp_in, out_fsk)
        packer.pack()
        data = out_fsk.read_bytes()

        with open(out_fsk, "rb") as f:
            magic = f.read(10)
            assert magic == b"FLATSEEK04"

    def test_format_detection_enclosed(self, tmp_path):
        """Enclosed format correctly detected."""
        tmp_in = tmp_path / "input"
        tmp_in.mkdir()
        out_fsk = tmp_path / "output.fsk"

        self._make_minimal_index(tmp_in)
        packer = FlatseekPacker(
            tmp_in, out_fsk,
            expire_at=9999999999,
            master_key=b"test_master_key_32_bytes_here!!!",
        )
        packer.pack()
        data = out_fsk.read_bytes()

        with open(out_fsk, "rb") as f:
            magic = f.read(10)
            assert magic != b"FLATSEEK04"


class TestDescriptionUrl:
    """Tests for description and url metadata in manifest (not manifest.json file)."""

    def _make_minimal_index(self, tmp_path: Path) -> Path:
        os.makedirs(tmp_path / "index")
        os.makedirs(tmp_path / "docs")
        (tmp_path / "stats.json").write_text('{"total_docs": 100}')
        (tmp_path / "column_map.json").write_text('{"columns": {}}')
        (tmp_path / "manifest.json").write_text('{"version": 1}')
        return tmp_path

    def test_description_url_in_stats_json(self, tmp_path):
        """Description and url are stored in stats.json (build-time metadata)."""
        tmp_in = tmp_path / "input"
        tmp_in.mkdir()
        out_fsk = tmp_path / "output.fsk"

        # Create stats.json with description/url
        (tmp_in / "stats.json").write_text('{"total_docs": 100, "description": "Build Desc", "url": "http://build.url"}')
        os.makedirs(tmp_in / "index")
        os.makedirs(tmp_in / "docs")
        (tmp_in / "column_map.json").write_text('{"columns": {}}')
        (tmp_in / "manifest.json").write_text('{"version": 1}')

        packer = FlatseekPacker(tmp_in, out_fsk)
        packer.pack()

        unpack_dir = tmp_path / "unpacked"
        unpacker = FlatseekUnpacker(out_fsk, unpack_dir)
        unpacker.unpack()

        # Description/url from build are in stats.json
        stats = json.loads((unpack_dir / "stats.json").read_text())
        assert stats.get("description") == "Build Desc"
        assert stats.get("url") == "http://build.url"


class TestExpireAt:
    """Tests for expire_at in manifest section."""

    def _make_minimal_index(self, tmp_path: Path) -> Path:
        os.makedirs(tmp_path / "index")
        os.makedirs(tmp_path / "docs")
        (tmp_path / "stats.json").write_text('{"total_docs": 100}')
        (tmp_path / "column_map.json").write_text('{"columns": {}}')
        (tmp_path / "manifest.json").write_text('{"version": 1}')
        return tmp_path

    def test_expired_rejected_on_unpack(self, tmp_path):
        """Expired enclosed index is rejected on unpack."""
        tmp_in = tmp_path / "input"
        tmp_in.mkdir()
        out_fsk = tmp_path / "output.fsk"

        self._make_minimal_index(tmp_in)
        packer = FlatseekPacker(
            tmp_in, out_fsk,
            expire_at=1,  # expired
            master_key=b"test_master_key_32_bytes_here!!!",
        )
        packer.pack()

        unpack_dir = tmp_path / "unpacked"
        unpacker = FlatseekUnpacker(out_fsk, unpack_dir)

        # Enclosed requires passphrase first
        with pytest.raises(ValueError, match="Passphrase required"):
            unpacker.unpack()
