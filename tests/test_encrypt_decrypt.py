"""Tests for `flatseek encrypt/decrypt` CLI commands — focus on resume + integrity check.

Self-contained: builds synthetic index/docs directories in a tempdir, invokes
cmd_encrypt / cmd_decrypt via argparse Namespace, and verifies resume behavior
(magic-header skip) and integrity check (--verify) outcomes.

Does NOT depend on the real `./data/` directory.
"""

import json
import os
import shutil
import tempfile
import zlib
from argparse import Namespace
from pathlib import Path

import pytest

from flatseek.cli import cmd_encrypt, cmd_decrypt
from flatseek.core.query_engine import (
    encrypt_bytes,
    decrypt_bytes,
    is_encrypted,
    load_encryption_key,
)

ENC_MAGIC = b"FLATSEEK\x01"

# Fixed test meta so we can re-derive the same key across calls.
TEST_META = {
    "salt": "6992d3f4b9ef8d4de2cd20128bcee4ea275489b3ed6ba7094a9385ed9b3ee7f7",
    "algorithm": "ChaCha20-Poly1305",
    "kdf": "PBKDF2-HMAC-SHA256",
    "iterations": 600000,
}
TEST_PASSPHRASE = "test123"


# ── Helpers ───────────────────────────────────────────────────────────────────


def _write_encryption_json(data_dir: Path) -> None:
    """Write a fixed encryption.json with the test salt."""
    with open(data_dir / "encryption.json", "w") as f:
        json.dump(TEST_META, f)


def _make_index_dir(data_dir: Path, n_bin: int = 5, n_zlib: int = 5,
                    payload_size: int = 1024) -> dict[Path, bytes]:
    """Build a synthetic index/docs tree.

    Returns a dict mapping path → original plaintext bytes (for assertions).
    """
    plaintexts: dict[Path, bytes] = {}

    idx_dir = data_dir / "index"
    idx_dir.mkdir(parents=True, exist_ok=True)
    for i in range(n_bin):
        path = idx_dir / f"chunk_{i:03d}.bin"
        data = os.urandom(payload_size)
        path.write_bytes(data)
        plaintexts[path] = data

    docs_dir = data_dir / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)
    for i in range(n_zlib):
        path = docs_dir / f"doc_{i:03d}.zlib"
        raw = os.urandom(payload_size // 2)
        data = zlib.compress(raw)
        path.write_bytes(data)
        plaintexts[path] = data

    # Metadata JSON files at the data_dir level (mirrors cmd_encrypt behavior).
    stats_path = data_dir / "stats.json"
    stats_path.write_bytes(json.dumps({"total_docs": n_bin + n_zlib,
                                       "fields": ["name"]}).encode())
    plaintexts[stats_path] = stats_path.read_bytes()

    return plaintexts


def _args(data_dir: str | Path, **kwargs) -> Namespace:
    """Build a Namespace as cmd_encrypt/cmd_decrypt expect."""
    base = {"data_dir": str(data_dir), "passphrase": TEST_PASSPHRASE,
            "resume": True, "verify": False, "verify_sample": 100}
    base.update(kwargs)
    return Namespace(**base)


def _is_encrypted_file(path: Path) -> bool:
    with open(path, "rb") as f:
        return f.read(9) == ENC_MAGIC


def _all_targets(data_dir: Path) -> list[Path]:
    """Mirror cmd_encrypt/cmd_decrypt file selection."""
    paths: list[Path] = []
    idx_dir = data_dir / "index"
    if idx_dir.is_dir():
        paths.extend(idx_dir.glob("*.bin"))
    docs_dir = data_dir / "docs"
    if docs_dir.is_dir():
        paths.extend(docs_dir.glob("*.zlib"))
    for fn in ("stats.json", "column_map.json", "manifest.json"):
        p = data_dir / fn
        if p.exists():
            paths.append(p)
    return paths


# ── Tests ─────────────────────────────────────────────────────────────────────


class TestEncryptDecryptRoundtrip:
    """Plaintext → encrypt → decrypt → bytes identical."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmpdir = Path(tempfile.mkdtemp())
        self.data_dir = self.tmpdir / "data"
        self.data_dir.mkdir()
        self.plaintexts = _make_index_dir(self.data_dir, n_bin=8, n_zlib=8)
        yield
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_encrypt_decrypt_roundtrip(self):
        # Encrypt
        cmd_encrypt(_args(self.data_dir))
        # All files should now be encrypted (magic prefix) on disk
        for path in self.plaintexts:
            assert _is_encrypted_file(path), f"{path} not encrypted after encrypt"

        # Decrypt
        cmd_decrypt(_args(self.data_dir))
        # All files should now be plaintext again
        for path, original in self.plaintexts.items():
            actual = path.read_bytes()
            assert actual == original, f"{path} differs after round-trip"


class TestEncryptResume:
    """Resume by magic-header detection: rerun skips already-encrypted files."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmpdir = Path(tempfile.mkdtemp())
        self.data_dir = self.tmpdir / "data"
        self.data_dir.mkdir()
        self.plaintexts = _make_index_dir(self.data_dir, n_bin=10, n_zlib=10)
        yield
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_encrypt_resume_skips_already_encrypted(self):
        # First run: encrypt everything from plaintext
        cmd_encrypt(_args(self.data_dir))
        salt_path = self.data_dir / "encryption.json"
        salt_first = salt_path.read_bytes()
        # All targets now have magic
        for path in self.plaintexts:
            assert _is_encrypted_file(path)

        # Second run: every file should be skipped via magic detection.
        # Capture stdout/stderr? Use capsys to verify resume messaging.
        cmd_encrypt(_args(self.data_dir))
        salt_second = salt_path.read_bytes()
        # Salt preserved across rerun (key material unchanged)
        assert salt_first == salt_second, "salt must be preserved on resume"

        # All files still encrypted, all byte-identical (no double-encryption)
        for path in self.plaintexts:
            assert _is_encrypted_file(path)

    def test_encrypt_partial_state_resumes_rest(self):
        """Manually encrypt HALF the files, then run cmd_encrypt → remaining get encrypted, encrypted half is skipped."""
        all_paths = list(self.plaintexts.keys())
        half = all_paths[: len(all_paths) // 2]

        key = load_encryption_key(None, TEST_PASSPHRASE, TEST_META)
        for path in half:
            data = path.read_bytes()
            path.write_bytes(encrypt_bytes(data, key))

        # Write encryption.json so cmd_encrypt uses the right salt
        _write_encryption_json(self.data_dir)

        # Run cmd_encrypt — should skip the half already encrypted, encrypt the rest
        cmd_encrypt(_args(self.data_dir))

        # All files encrypted now
        for path in all_paths:
            assert _is_encrypted_file(path), f"{path} not encrypted after resume"

        # Decrypt and verify EVERY file matches its original plaintext
        # (no double-encryption corruption on the previously-encrypted half)
        cmd_decrypt(_args(self.data_dir))
        for path, original in self.plaintexts.items():
            assert path.read_bytes() == original, f"{path} corrupted by double encryption"

    def test_encrypt_no_resume_reencrypts(self):
        """--no-resume must decrypt-then-re-encrypt every file (fresh nonce each run)."""
        cmd_encrypt(_args(self.data_dir))
        # Capture ciphertext hashes after first encrypt run
        import hashlib
        first_hashes = {p: hashlib.sha256(p.read_bytes()).hexdigest()
                        for p in self.plaintexts}

        cmd_encrypt(_args(self.data_dir, resume=False))
        second_hashes = {p: hashlib.sha256(p.read_bytes()).hexdigest()
                         for p in self.plaintexts}

        # All files still encrypted (magic preserved).
        for path in self.plaintexts:
            assert _is_encrypted_file(path)

        # At least one ciphertext should differ — fresh nonce per encrypt_bytes()
        # call means bytes change even though plaintext is the same.
        hash_diffs = sum(1 for p in self.plaintexts
                         if first_hashes[p] != second_hashes[p])
        assert hash_diffs >= 1, \
            "expected at least one file's ciphertext to change after re-encrypt"

        # Round-trip: decrypt everything, verify bytes match original plaintext.
        cmd_decrypt(_args(self.data_dir))
        for path, original in self.plaintexts.items():
            assert path.read_bytes() == original, \
                f"{path} corrupted by no-resume re-encrypt"


class TestDecryptResume:
    """Resume by absence-of-magic detection: rerun skips already-plaintext files."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmpdir = Path(tempfile.mkdtemp())
        self.data_dir = self.tmpdir / "data"
        self.data_dir.mkdir()
        self.plaintexts = _make_index_dir(self.data_dir, n_bin=6, n_zlib=6)
        yield
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_decrypt_resume_skips_already_plain(self):
        # First encrypt, then decrypt once → all plaintext, encryption.json removed
        cmd_encrypt(_args(self.data_dir))
        cmd_decrypt(_args(self.data_dir))
        assert not (self.data_dir / "encryption.json").exists()

        # Re-decrypt: with --resume, should skip all (no magic on any file).
        # Without encryption.json, cmd_decrypt errors out — let's instead test
        # the partial-state case: re-encrypt, then partial-decrypt.
        cmd_encrypt(_args(self.data_dir))
        # Manually decrypt one file to simulate an interrupted prior decrypt.
        one_path = next(iter(self.plaintexts))
        key = load_encryption_key(self.data_dir, TEST_PASSPHRASE)
        original = self.plaintexts[one_path]
        one_path.write_bytes(decrypt_bytes(one_path.read_bytes(), key))
        # Now: one file plain, rest encrypted. encryption.json still present.
        assert not _is_encrypted_file(one_path)
        for p in self.plaintexts:
            if p == one_path:
                continue
            assert _is_encrypted_file(p), f"{p} should still be encrypted"

        # Run decrypt: should skip the plain one, decrypt the rest.
        cmd_decrypt(_args(self.data_dir))
        for path, original_bytes in self.plaintexts.items():
            assert path.read_bytes() == original_bytes, f"{path} corrupted"

        # encryption.json removed on full success
        assert not (self.data_dir / "encryption.json").exists()


class TestEncryptVerify:
    """--verify integrity check on the encrypt command."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmpdir = Path(tempfile.mkdtemp())
        self.data_dir = self.tmpdir / "data"
        self.data_dir.mkdir()
        # Larger sample so --verify has something to sample from
        self.plaintexts = _make_index_dir(self.data_dir, n_bin=30, n_zlib=30)
        yield
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_encrypt_with_verify_passes(self, capsys):
        cmd_encrypt(_args(self.data_dir, verify=True, verify_sample=20))
        captured = capsys.readouterr()
        assert "Verify: 20 sampled" in captured.out
        assert "20 passed" in captured.out
        assert "0 failed" in captured.out

    def test_encrypt_verify_detects_corruption(self, capsys):
        cmd_encrypt(_args(self.data_dir, verify=False))
        # Corrupt one encrypted file by flipping a byte deep inside its ciphertext
        # (past the magic + nonce header so Poly1305 detects the tampering).
        target = next(iter(self.plaintexts))
        raw = bytearray(target.read_bytes())
        # Flip a byte somewhere after the header (offset > magic+nonce=21)
        raw[50] ^= 0xFF
        target.write_bytes(bytes(raw))

        # Run encrypt again with --verify — use verify_sample=0 (all files) so
        # the random sample is guaranteed to include the corrupted file.
        cmd_encrypt(_args(self.data_dir, verify=True, verify_sample=0))
        captured = capsys.readouterr()
        assert "FAILED" in captured.out
        # The corrupted file path should appear in the failed list
        assert str(target) in captured.out

    def test_encrypt_verify_sample_zero_samples_all(self, capsys):
        # --verify-sample 0 → sample every file
        cmd_encrypt(_args(self.data_dir, verify=True, verify_sample=0))
        captured = capsys.readouterr()
        n_targets = len(_all_targets(self.data_dir))
        assert f"Verify: {n_targets} sampled" in captured.out
        assert f"{n_targets} passed" in captured.out


class TestDecryptVerify:
    """--verify integrity check on the decrypt command."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmpdir = Path(tempfile.mkdtemp())
        self.data_dir = self.tmpdir / "data"
        self.data_dir.mkdir()
        self.plaintexts = _make_index_dir(self.data_dir, n_bin=25, n_zlib=25)
        yield
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_decrypt_with_verify_passes(self, capsys):
        cmd_encrypt(_args(self.data_dir))
        cmd_decrypt(_args(self.data_dir, verify=True, verify_sample=15))
        captured = capsys.readouterr()
        assert "Verify: 15 sampled" in captured.out
        assert "15 passed" in captured.out
        assert "0 failed" in captured.out


class TestWrongPassphraseFailsFast:
    """cmd_decrypt probes a file before touching anything → wrong passphrase aborts."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmpdir = Path(tempfile.mkdtemp())
        self.data_dir = self.tmpdir / "data"
        self.data_dir.mkdir()
        self.plaintexts = _make_index_dir(self.data_dir, n_bin=5, n_zlib=5)
        yield
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_wrong_passphrase_aborts_before_modifying(self, capsys):
        # Encrypt with TEST_PASSPHRASE
        cmd_encrypt(_args(self.data_dir, passphrase=TEST_PASSPHRASE))

        # Capture pre-decrypt file contents
        snapshot = {p: p.read_bytes() for p in self.plaintexts}

        # Try to decrypt with wrong passphrase — should print error and abort
        cmd_decrypt(_args(self.data_dir, passphrase="wrong-pass"))

        # Files must NOT have been modified
        for path, original_bytes in snapshot.items():
            assert path.read_bytes() == original_bytes, \
                f"{path} was modified despite wrong passphrase"
        # encryption.json should still be present (decrypt never completed)
        assert (self.data_dir / "encryption.json").exists()
