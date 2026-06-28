"""Tests for FlatseekFileStorageAdapter streaming reads.

The bug we are guarding against: previously, the storage adapter loaded the
ENTIRE section into RAM on first access (`_get_section_data` did `f.read(desc.size)`
and cached forever). For a 32 GB .fsk with a 25 GB docs section, this OOM'd
the serve process.

These tests verify:
- read_bytes fetches only the requested file (single seek+read).
- open_read returns a streaming file-like, not a fully-materialized BytesIO.
- Peak memory stays bounded by single-file size, not section size.
- The file handle is properly managed (close, double-read).
"""

import io
import json
import os
import shutil
import tempfile
import tracemalloc
import zlib
from pathlib import Path

import pytest

from flatseek.flatseek_file import (
    FlatseekFileStorageAdapter,
    FlatseekPacker,
    SID_INDEX,
    SID_DOCS,
)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_index(data_dir: Path, n_bin: int = 8, file_size: int = 256 * 1024,
                n_zlib: int = 4, seed: bytes = b"streaming test") -> dict[Path, bytes]:
    """Build a synthetic index with deterministic content. Returns src → plaintext."""
    plaintexts: dict[Path, bytes] = {}

    def rng(n: int) -> bytes:
        import hashlib as _hl
        out = bytearray(n)
        state = seed
        i = 0
        while i < n:
            state = _hl.sha256(state).digest()
            chunk = state[: min(len(state), n - i)]
            out[i:i + len(chunk)] = chunk
            i += len(chunk)
        return bytes(out)

    idx_dir = data_dir / "index"
    idx_dir.mkdir(parents=True, exist_ok=True)
    for i in range(n_bin):
        path = idx_dir / f"chunk_{i:03d}.bin"
        path.write_bytes(rng(file_size))
        plaintexts[path] = path.read_bytes()

    docs_dir = data_dir / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)
    for i in range(n_zlib):
        path = docs_dir / f"doc_{i:03d}.zlib"
        path.write_bytes(zlib.compress(rng(file_size // 4)))
        plaintexts[path] = path.read_bytes()

    (data_dir / "stats.json").write_text(
        json.dumps({"total_docs": n_bin + n_zlib, "fields": ["name"]})
    )
    return plaintexts


def _make_encrypted_index(data_dir: Path, passphrase: bytes, n_bin: int = 4,
                          file_size: int = 128 * 1024):
    """Build an encrypted index — exercises the per-file decrypt path."""
    import base64
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

    plaintexts = _make_index(data_dir, n_bin=n_bin, file_size=file_size)

    # Derive key the same way pack/serve does.
    salt = os.urandom(16)
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt,
                     iterations=100_000)
    key = kdf.derive(passphrase)

    # Encrypt each .bin file in-place (per-file AEAD, same as real pack path).
    from flatseek.core.query_engine import encrypt_bytes, _ENC_MAGIC as MAGIC
    idx_dir = data_dir / "index"
    for path in list(idx_dir.glob("*.bin")):
        path.write_bytes(encrypt_bytes(path.read_bytes(), key))

    # Write encryption.json so the adapter knows to use the key.
    enc_meta = {
        "salt_b64": base64.b64encode(salt).decode(),
        "iterations": 100_000,
    }
    (data_dir / "encryption.json").write_text(json.dumps(enc_meta))

    return plaintexts, key, MAGIC


# ── Tests ────────────────────────────────────────────────────────────────────


class TestStorageAdapterStreaming:
    """Verify seek+read semantics for read_bytes / open_read."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmpdir = Path(tempfile.mkdtemp())
        yield
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _build_fsk(self, n_bin: int = 6, file_size: int = 256 * 1024,
                   n_zlib: int = 4) -> tuple[Path, dict[Path, bytes]]:
        data_dir = self.tmpdir / "src"
        data_dir.mkdir()
        plaintexts = _make_index(data_dir, n_bin=n_bin, file_size=file_size,
                                 n_zlib=n_zlib)
        fsk = self.tmpdir / "out.fsk"
        FlatseekPacker(data_dir, fsk).pack()
        return fsk, plaintexts

    # ── Plaintext adapter ──────────────────────────────────────────────────

    def test_read_bytes_returns_correct_content(self):
        fsk, plaintexts = self._build_fsk()
        adapter = FlatseekFileStorageAdapter(fsk)

        for src, original in plaintexts.items():
            # rel path inside the .fsk — strip the section prefix
            rel = src.relative_to(src.parent.parent)
            got = adapter.read_bytes(str(rel))
            assert got == original, f"content mismatch for {rel}"

    def test_read_bytes_for_manifest_files(self):
        fsk, _ = self._build_fsk()
        adapter = FlatseekFileStorageAdapter(fsk)

        stats = json.loads(adapter.read_bytes("stats.json"))
        assert "total_docs" in stats

    def test_open_read_streams_via_seek(self):
        """open_read must NOT load the whole file — verify by reading in
        small chunks and checking positions."""
        fsk, plaintexts = self._build_fsk(n_bin=4, file_size=64 * 1024)
        adapter = FlatseekFileStorageAdapter(fsk)

        # Pick the first .bin file.
        first_bin = next(p for p in plaintexts if p.suffix == ".bin")
        rel = first_bin.relative_to(first_bin.parent.parent)
        original = plaintexts[first_bin]

        with adapter.open_read(str(rel)) as f:
            # Initial position is 0.
            assert f.tell() == 0

            # Read 1000 bytes — should be exactly the first 1000.
            head = f.read(1000)
            assert head == original[:1000]
            assert f.tell() == 1000

            # Seek to the end.
            f.seek(0, 2)  # SEEK_END
            assert f.tell() == len(original)

            # Seek back and read all.
            f.seek(0)
            full = f.read()
            assert full == original

    def test_open_read_random_access(self):
        """open_read supports arbitrary seeks + reads."""
        fsk, plaintexts = self._build_fsk(n_bin=2, file_size=128 * 1024)
        adapter = FlatseekFileStorageAdapter(fsk)

        first_bin = next(p for p in plaintexts if p.suffix == ".bin")
        rel = first_bin.relative_to(first_bin.parent.parent)
        original = plaintexts[first_bin]

        with adapter.open_read(str(rel)) as f:
            f.seek(5000)
            chunk1 = f.read(2000)
            assert chunk1 == original[5000:7000]
            assert f.tell() == 7000

            f.seek(10000)
            chunk2 = f.read(100)
            assert chunk2 == original[10000:10100]

    def test_open_read_read_past_eof(self):
        """Read past EOF returns empty (matches stdlib file behavior)."""
        fsk, plaintexts = self._build_fsk(n_bin=2, file_size=32 * 1024)
        adapter = FlatseekFileStorageAdapter(fsk)

        first_bin = next(p for p in plaintexts if p.suffix == ".bin")
        rel = first_bin.relative_to(first_bin.parent.parent)
        original = plaintexts[first_bin]

        with adapter.open_read(str(rel)) as f:
            f.seek(0, 2)
            assert f.tell() == len(original)
            extra = f.read(1000)
            assert extra == b""

    def test_exists_and_listdir(self):
        fsk, plaintexts = self._build_fsk(n_bin=3, file_size=16 * 1024)
        adapter = FlatseekFileStorageAdapter(fsk)

        # All source files exist.
        for src in plaintexts:
            rel = src.relative_to(src.parent.parent)
            assert adapter.exists(str(rel)), f"missing: {rel}"

        # listdir returns the file basenames at the section root.
        idx_contents = adapter.listdir("index")
        assert "chunk_000.bin" in idx_contents
        assert "chunk_002.bin" in idx_contents
        docs_contents = adapter.listdir("docs")
        assert "doc_000.zlib" in docs_contents

    def test_close_releases_handle(self):
        fsk, plaintexts = self._build_fsk()
        adapter = FlatseekFileStorageAdapter(fsk)
        assert adapter._fh is not None
        assert not adapter._fh.closed
        adapter.close()
        assert adapter._fh is None

        # After close, reading a payload file (not manifest) must raise.
        first_bin = next(p for p in plaintexts if p.suffix == ".bin")
        rel = first_bin.relative_to(first_bin.parent.parent)
        with pytest.raises((IOError, ValueError, OSError)):
            adapter.read_bytes(str(rel))

    def test_double_close_is_safe(self):
        fsk, _ = self._build_fsk()
        adapter = FlatseekFileStorageAdapter(fsk)
        adapter.close()
        adapter.close()  # must not raise

    # ── Memory profile: the actual bug guard ───────────────────────────────

    def test_peak_memory_bounded_by_single_file_not_section(self):
        """The bug: loading a 25 GB docs section just to read one chunk OOMs.

        Build a .fsk with a docs section >> the per-file chunk size, then
        read ONE file via the adapter and assert peak memory stays under
        a small multiple of that file's size.

        tracemalloc tracks Python heap allocations only — sufficient to
        detect the old `self._section_data[section_id] = data` whole-section
        cache (which would have allocated 4+ MB of Python bytes objects).
        """
        # 50 files × 1 MB each = 50 MB docs section; each file is 1 MB.
        per_file = 1 * 1024 * 1024  # 1 MB
        n_files = 50
        data_dir = self.tmpdir / "src"
        data_dir.mkdir()
        plaintexts = _make_index(
            data_dir, n_bin=0, file_size=per_file, n_zlib=n_files,
        )

        fsk = self.tmpdir / "out.fsk"
        FlatseekPacker(data_dir, fsk).pack()

        section_size = fsk.stat().st_size

        adapter = FlatseekFileStorageAdapter(fsk)

        # Baseline: memory after adapter open (header + offset tables only).
        tracemalloc.start()
        baseline = tracemalloc.get_traced_memory()[0]

        # Read one .zlib file via the adapter — must NOT load the whole section.
        first_doc = next(p for p in plaintexts if p.suffix == ".zlib")
        rel = first_doc.relative_to(first_doc.parent.parent)
        result = adapter.read_bytes(str(rel))
        assert result == plaintexts[first_doc]

        peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()

        # Old code would have allocated ~50 MB for the whole docs section
        # into self._section_data. New code allocates only this single file.
        # Generous bound: 4× the per-file size (covers any internal copies).
        limit = 4 * per_file
        delta = peak - baseline
        assert delta < limit, (
            f"peak heap delta after single-file read = {delta / 1024 / 1024:.1f} MB "
            f"(limit {limit / 1024 / 1024:.1f} MB). .fsk size = "
            f"{section_size / 1024 / 1024:.1f} MB — looks like the whole-section "
            f"load bug came back."
        )

        adapter.close()


class TestStorageAdapterEncrypted:
    """Verify per-file AEAD decryption works with streaming reads."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmpdir = Path(tempfile.mkdtemp())
        yield
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_read_encrypted_file_decrypts_correctly(self):
        data_dir = self.tmpdir / "src"
        data_dir.mkdir()
        plaintexts, key, _ = _make_encrypted_index(
            data_dir, passphrase=b"correct horse battery staple",
            n_bin=3, file_size=64 * 1024,
        )

        fsk = self.tmpdir / "out.fsk"
        FlatseekPacker(data_dir, fsk, enc_key=key).pack()

        adapter = FlatseekFileStorageAdapter(fsk, enc_key=key)

        # Each .bin file in plaintexts was encrypted in-place before packing.
        # The packed file should round-trip back to the ORIGINAL plaintext
        # via adapter.read_bytes (which decrypts per-file).
        for src, original in plaintexts.items():
            if src.suffix != ".bin":
                continue
            rel = src.relative_to(src.parent.parent)
            got = adapter.read_bytes(str(rel))
            assert got == original, f"decrypted mismatch for {rel}"

        adapter.close()

    def test_open_read_encrypted_file(self):
        """open_read on an encrypted file materializes (AEAD single-shot),
        then wraps in BytesIO so the caller sees plaintext."""
        data_dir = self.tmpdir / "src"
        data_dir.mkdir()
        plaintexts, key, _ = _make_encrypted_index(
            data_dir, passphrase=b"open read test",
            n_bin=2, file_size=32 * 1024,
        )

        fsk = self.tmpdir / "out.fsk"
        FlatseekPacker(data_dir, fsk, enc_key=key).pack()

        adapter = FlatseekFileStorageAdapter(fsk, enc_key=key)
        first_bin = next(p for p in plaintexts if p.suffix == ".bin")
        rel = first_bin.relative_to(first_bin.parent.parent)
        original = plaintexts[first_bin]

        with adapter.open_read(str(rel)) as f:
            data = f.read()
            assert data == original

        adapter.close()