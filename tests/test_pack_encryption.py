"""Tests for pack/unpack encryption round-trips — fully self-contained, no external files."""

import json
import os
import shutil
import struct
import tempfile
import zlib
from pathlib import Path

import pytest

from flatseek.core.query_engine import (
    load_encryption_key,
    encrypt_bytes,
    decrypt_bytes,
    is_encrypted,
)
from flatseek.flatseek_file import (
    FlatseekPacker,
    FlatseekUnpacker,
    FlatseekHeader,
    SID_MANIFEST,
    HEADER_SIZE,
)

ENC_MAGIC = b"FLATSEEK\x01"

# Known test meta for reproducible key derivation
TEST_META = {
    "salt": "6992d3f4b9ef8d4de2cd20128bcee4ea275489b3ed6ba7094a9385ed9b3ee7f7",
    "algorithm": "ChaCha20-Poly1305",
    "kdf": "PBKDF2-HMAC-SHA256",
    "iterations": 600000,
}
TEST_PASSPHRASE = "test123"


def _make_encryption_json(out_dir: Path) -> None:
    """Write encryption.json + meta.json for a directory."""
    with open(out_dir / "encryption.json", "w") as f:
        json.dump(TEST_META, f)
    with open(out_dir / "meta.json", "w") as f:
        json.dump({"version": "0.1.6", "index_name": "test"}, f)


def _make_stats(out_dir: Path, encrypted: bool = False) -> None:
    """Write stats.json — plaintext dict or encrypted bytes."""
    stats = {"total_docs": 10, "total_bytes": 1024, "fields": ["name", "email"]}
    data = json.dumps(stats).encode()
    if encrypted:
        key = load_encryption_key(None, TEST_PASSPHRASE, TEST_META)
        data = ENC_MAGIC + encrypt_bytes(data, key)
    with open(out_dir / "stats.json", "wb") as f:
        f.write(data)


def _make_index_chunk(path: Path, encrypted: bool = False) -> None:
    """Write a single index .bin chunk — plaintext or encrypted.

    encrypt_bytes() already adds FLATSEEK magic prefix; do NOT prepend manually.
    """
    data = b'{"name": "Alice", "email": "alice@example.com"}'
    if encrypted:
        key = load_encryption_key(None, TEST_PASSPHRASE, TEST_META)
        data = encrypt_bytes(data, key)  # already includes FLATSEEK magic
    with open(path, "wb") as f:
        f.write(data)


def _make_doc_chunk(path: Path, encrypted: bool = False) -> None:
    """Write a single docs .zlib chunk — plaintext or encrypted.

    encrypt_bytes() already adds FLATSEEK magic prefix; do NOT prepend manually.
    """
    data = zlib.compress(b'{"name": "Alice", "city": "Jakarta"}')
    if encrypted:
        key = load_encryption_key(None, TEST_PASSPHRASE, TEST_META)
        data = encrypt_bytes(data, key)  # already includes FLATSEEK magic
    with open(path, "wb") as f:
        f.write(data)


def _enc(path: str) -> bool:
    """Return True if file starts with FLATSEEK magic (encrypted)."""
    with open(path, "rb") as f:
        return f.read(9) == ENC_MAGIC


class TestPackUnpackEncryptionRoundTrip:
    """Pack encrypted source → unpack → verify encryption state preserved."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmpdir = tempfile.mkdtemp()
        self.key = load_encryption_key(None, TEST_PASSPHRASE, TEST_META)
        yield
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _build_src_dir(
        self, name: str, index_enc: bool = False, stats_enc: bool = False, docs_enc: bool = False
    ) -> Path:
        """Build a minimal flatseek index dir with specified encryption states.

        - index_enc=True: index/*.bin files have FLATSEEK magic (section stays encrypted)
        - stats_enc=True: stats.json has FLATSEEK magic (stored as hex in manifest with key)
        - docs_enc=True: docs/*.zlib files have FLATSEEK magic
        """
        src = Path(self.tmpdir) / name
        src.mkdir(parents=True)
        _make_encryption_json(src)
        _make_stats(src, encrypted=stats_enc)

        # Index section: *.bin files
        idx_dir = src / "index"
        idx_dir.mkdir(parents=True)
        _make_index_chunk(idx_dir / "chunk_0.bin", encrypted=index_enc)

        # Docs section: *.zlib files
        docs_dir = src / "docs"
        docs_dir.mkdir(parents=True)
        _make_doc_chunk(docs_dir / "doc_0.zlib", encrypted=docs_enc)

        return src

    def _pack(self, src_dir: Path, dst: Path, enc_key=None) -> None:
        FlatseekPacker(src_dir, dst, enc_key=enc_key).pack()

    def _unpack(self, src_fsk: Path, dst: Path, enc_key=None) -> Path:
        dst = Path(dst)
        shutil.rmtree(dst, ignore_errors=True)
        FlatseekUnpacker(src_fsk, dst, enc_key=enc_key).unpack()
        return dst

    def _read_manifest_stats_type(self, fsk_path: Path) -> type:
        """Read stats type from packed fsk manifest."""
        with open(fsk_path, "rb") as f:
            hdr = FlatseekHeader.unpack(f.read(HEADER_SIZE))
            for s in hdr.sections:
                if s.section_id == SID_MANIFEST:
                    f.seek(s.offset)
                    manifest = json.loads(f.read(s.size))
                    return type(manifest.get("stats"))
        return None

    # ── Plaintext source (no FLATSEEK magic on any file) ──────────────────────

    def test_plaintext_pack_unpack_roundtrip(self):
        """Plaintext source → pack → unpack → all files plaintext."""
        src = self._build_src_dir("plain_src", index_enc=False, stats_enc=False)
        fsk = Path(self.tmpdir) / "plain.fsk"
        self._pack(src, fsk, enc_key=None)

        # Manifest stats is dict (plaintext JSON)
        assert self._read_manifest_stats_type(fsk) is dict

        out = self._unpack(fsk, Path(self.tmpdir) / "plain_out", enc_key=None)
        assert _enc(f"{out}/stats.json") is False
        idx_files = list((out / "index").glob("*.bin"))
        assert len(idx_files) == 1
        assert _enc(str(idx_files[0])) is False
        doc_files = list((out / "docs").glob("*.zlib"))
        assert len(doc_files) == 1

    def test_plaintext_pack_with_key_hex_encrypts_manifest_json(self):
        """Plaintext source + enc_key → manifest JSON files hex-encrypted, sections as-is."""
        src = self._build_src_dir("plain_src_key", index_enc=False, stats_enc=False)
        fsk = Path(self.tmpdir) / "plain_key.fsk"
        self._pack(src, fsk, enc_key=self.key)

        # Manifest stats is hex string (encrypted)
        assert self._read_manifest_stats_type(fsk) is str
        # Index section has no FLATSEEK magic (source was plaintext)
        assert (out := self._unpack(fsk, Path(self.tmpdir) / "plain_key_out", enc_key=None))
        idx_files = list((out / "index").glob("*.bin"))
        assert len(idx_files) == 1
        assert _enc(str(idx_files[0])) is False  # plaintext section → plaintext file

    # ── Encrypted source (files have FLATSEEK magic) ─────────────────────────

    def test_encrypted_pack_unpack_no_key(self):
        """Encrypted source, no key → sections stay encrypted, manifest JSON hex (has key)."""
        src = self._build_src_dir("enc_src", index_enc=True, stats_enc=True, docs_enc=True)
        fsk = Path(self.tmpdir) / "enc.fsk"
        self._pack(src, fsk, enc_key=self.key)  # pack with key → JSON hex-encrypted in manifest

        out = self._unpack(fsk, Path(self.tmpdir) / "enc_out", enc_key=None)
        assert _enc(f"{out}/stats.json") is True  # hex ciphertext written as-is
        idx_files = list((out / "index").glob("*.bin"))
        assert len(idx_files) == 1
        assert _enc(str(idx_files[0])) is True  # encrypted section → encrypted file

    def test_encrypted_pack_unpack_with_key(self):
        """Encrypted source (files have FLATSEEK magic), with key → per-file decrypted."""
        src = self._build_src_dir("enc_src2", index_enc=True, stats_enc=True, docs_enc=True)
        fsk = Path(self.tmpdir) / "enc.fsk"
        self._pack(src, fsk, enc_key=self.key)

        out = self._unpack(fsk, Path(self.tmpdir) / "enc_key_out", enc_key=self.key)
        assert _enc(f"{out}/stats.json") is True  # stats.hex stays as-is (hex ciphertext)
        idx_files = list((out / "index").glob("*.bin"))
        assert len(idx_files) == 1
        # Section doesn't start with magic (section-level enc not used); files inside have
        # per-file FLATSEEK magic → per-file decryption → plaintext output
        assert _enc(str(idx_files[0])) is False  # decrypted by per-file decryption


class TestPackEncryptionPreservesState:
    """Re-pack from unpacked dir → verify encryption state is correct."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmpdir = tempfile.mkdtemp()
        self.key = load_encryption_key(None, TEST_PASSPHRASE, TEST_META)
        yield
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _build_src_dir(self, name: str, index_enc: bool = False, stats_enc: bool = False) -> Path:
        src = Path(self.tmpdir) / name
        src.mkdir(parents=True)
        _make_encryption_json(src)
        _make_stats(src, encrypted=stats_enc)
        idx_dir = src / "index"
        idx_dir.mkdir(parents=True)
        _make_index_chunk(idx_dir / "chunk_0.bin", encrypted=index_enc)
        docs_dir = src / "docs"
        docs_dir.mkdir(parents=True)
        _make_doc_chunk(docs_dir / "doc_0.zlib", encrypted=index_enc)
        return src

    def test_repack_plaintext_with_key_hex_encrypts_manifest(self):
        """Pack plaintext dir (with enc_key) → JSON files hex-encrypted in manifest."""
        src = self._build_src_dir("repack_src", index_enc=False, stats_enc=False)
        dst_fsk = Path(self.tmpdir) / "repacked.fsk"
        FlatseekPacker(src, dst_fsk, enc_key=self.key).pack()

        # Read manifest from packed .fsk
        with open(dst_fsk, "rb") as f:
            hdr = FlatseekHeader.unpack(f.read(HEADER_SIZE))
            for s in hdr.sections:
                if s.section_id == SID_MANIFEST:
                    f.seek(s.offset)
                    manifest = json.loads(f.read(s.size))
                    assert isinstance(manifest.get("stats"), str), \
                        "stats should be hex-encrypted when packing with enc_key"
                    assert "_encryption_b64" in manifest
                    break
            else:
                pytest.fail("Manifest section not found in packed fsk")

    def test_repack_encrypted_preserves_encrypted_state(self):
        """Pack encrypted dir (index has FLATSEEK magic) → index section stays encrypted."""
        src = self._build_src_dir("repack_enc_src", index_enc=True, stats_enc=True)
        dst_fsk = Path(self.tmpdir) / "repacked_enc.fsk"
        FlatseekPacker(src, dst_fsk, enc_key=self.key).pack()

        # Unpack without key → index section stays encrypted
        out = Path(self.tmpdir) / "repacked_enc_out"
        FlatseekUnpacker(dst_fsk, out, enc_key=None).unpack()
        idx_files = list((out / "index").glob("*.bin"))
        assert len(idx_files) == 1
        assert _enc(str(idx_files[0])) is True


class TestEncryptionKeyDerivation:
    """Tests for load_encryption_key with meta dict (no filesystem needed)."""

    def test_load_with_meta(self):
        """load_encryption_key accepts meta dict + passphrase."""
        key = load_encryption_key(None, TEST_PASSPHRASE, TEST_META)
        assert len(key) == 32  # ChaCha20 key is 32 bytes

    def test_round_trip(self):
        """Encrypt/decrypt round-trip with derived key."""
        key = load_encryption_key(None, TEST_PASSPHRASE, TEST_META)
        data = b"hello flatseek"
        enc = encrypt_bytes(data, key)
        dec = decrypt_bytes(enc, key)
        assert dec == data

    def test_wrong_passphrase_fails(self):
        """Wrong passphrase produces wrong key → decrypt fails."""
        key = load_encryption_key(None, TEST_PASSPHRASE, TEST_META)
        enc = encrypt_bytes(b"test", key)
        wrong_key = load_encryption_key(None, "wrongpass", TEST_META)
        with pytest.raises(Exception):
            decrypt_bytes(enc, wrong_key)

    def test_is_encrypted_helper(self):
        """is_encrypted returns True only for FLATSEEK magic prefix."""
        assert is_encrypted(ENC_MAGIC + b"someciphertext") is True
        assert is_encrypted(b"plaintext json data here") is False
        assert is_encrypted(b'{"key":"value"}') is False
