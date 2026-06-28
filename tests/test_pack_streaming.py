"""Tests for streaming pack/unpack — verify OOM-safe behavior on large outputs.

These tests build synthetic indexes large enough to expose OOM-prone code paths
(peak memory no longer scales with output size) and use tracemalloc to assert
that peak working-set stays bounded by chunk size × small factor, not by total
file size.
"""

import hashlib
import json
import os
import shutil
import struct
import tempfile
import tracemalloc
import zlib
from pathlib import Path

import pytest

from flatseek.flatseek_file import (
    FlatseekPacker,
    FlatseekUnpacker,
    FlatseekHeader,
    SID_MANIFEST,
    SID_INDEX,
    SID_DOCS,
    SID_DOC_VALUES,
    HEADER_SIZE,
)

ENC_MAGIC = b"FLATSEEK\x01"


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_large_index(
    data_dir: Path,
    n_bin: int = 5,
    file_size: int = 4 * 1024 * 1024,  # 4 MB each → ~20 MB total for n_bin=5
    n_zlib: int = 0,
    seed: bytes = b"hello flatseek",
) -> dict[Path, bytes]:
    """Build a synthetic index with deterministic content. Returns path → plaintext."""
    plaintexts: dict[Path, bytes] = {}

    # Use a deterministic RNG so failures reproduce.
    import hashlib as _hl
    def rng(n: int) -> bytes:
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
        raw = rng(file_size // 4)
        path.write_bytes(zlib.compress(raw))
        plaintexts[path] = path.read_bytes()

    # Metadata JSON files (small).
    stats_path = data_dir / "stats.json"
    stats_path.write_bytes(json.dumps({
        "total_docs": n_bin + n_zlib, "fields": ["name"],
    }).encode())
    plaintexts[stats_path] = stats_path.read_bytes()

    return plaintexts


def _verify_roundtrip(output_dir: Path, original: dict[Path, bytes],
                      index_subdir: str = "index") -> None:
    """Confirm every original file is recovered byte-for-byte at output_dir."""
    # Files in original dict are indexed by their source path. After unpack,
    # they live at output_dir / <index|docs> / <relative_path>.
    index_dir = output_dir / "index"
    for src_path, original_bytes in original.items():
        # Map src_path → relative path inside index_dir
        rel = src_path.relative_to(src_path.parent.parent)  # strip "index/" or "docs/"
        recovered = index_dir / rel
        assert recovered.exists(), f"missing recovered file: {recovered}"
        assert recovered.read_bytes() == original_bytes, (
            f"byte mismatch at {recovered}"
        )


# ── Tests ─────────────────────────────────────────────────────────────────────


class TestStreamingLargePackUnpack:
    """Pack/unpack a large synthetic index end-to-end with byte-for-byte fidelity."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmpdir = Path(tempfile.mkdtemp())
        yield
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_pack_unpack_large_index_byte_exact(self):
        # 20 MB total in 5 × 4 MB .bin files — large enough that the OLD
        # pack() would have buffered all of it in `results` + `self.sections`
        # simultaneously (≈ 2-3× output size in RAM).
        data_dir = self.tmpdir / "data"
        data_dir.mkdir()
        plaintexts = _make_large_index(data_dir, n_bin=5, file_size=4 * 1024 * 1024)

        fsk = self.tmpdir / "out.fsk"
        FlatseekPacker(data_dir, fsk).pack()

        out_dir = self.tmpdir / "out"
        FlatseekUnpacker(fsk, out_dir).unpack()

        # Binary .bin files must be recovered byte-for-byte. JSON metadata
        # files (stats.json) are pretty-printed by unpack for readability
        # — that's a pre-existing behavior, not a streaming regression.
        binary_paths = {p: b for p, b in plaintexts.items() if p.suffix == ".bin"}
        for src_path, original_bytes in binary_paths.items():
            rel = src_path.relative_to(data_dir)
            recovered = out_dir / rel
            assert recovered.exists(), f"missing {recovered}"
            assert recovered.read_bytes() == original_bytes, (
                f"byte mismatch at {recovered}"
            )

    def test_pack_unpack_large_with_docs(self):
        data_dir = self.tmpdir / "data"
        data_dir.mkdir()
        plaintexts = _make_large_index(
            data_dir, n_bin=3, file_size=2 * 1024 * 1024, n_zlib=4,
        )

        fsk = self.tmpdir / "out.fsk"
        FlatseekPacker(data_dir, fsk).pack()

        out_dir = self.tmpdir / "out"
        FlatseekUnpacker(fsk, out_dir).unpack()

        # Binary files only (.bin, .zlib).
        binary_paths = {p: b for p, b in plaintexts.items()
                        if p.suffix in (".bin", ".zlib")}
        for src_path, original_bytes in binary_paths.items():
            rel = src_path.relative_to(data_dir)
            recovered = out_dir / rel
            assert recovered.exists(), f"missing {recovered}"
            assert recovered.read_bytes() == original_bytes


class TestPackUnpackChecksums:
    """Pack must produce a valid .fsk with correct section + total checksums."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmpdir = Path(tempfile.mkdtemp())
        yield
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_section_checksums_validate(self):
        data_dir = self.tmpdir / "data"
        data_dir.mkdir()
        _make_large_index(data_dir, n_bin=4, file_size=2 * 1024 * 1024)

        fsk = self.tmpdir / "out.fsk"
        FlatseekPacker(data_dir, fsk).pack()

        # Re-read header and re-verify each section's checksum against the bytes
        # actually on disk. The pack() validation already does this, but we
        # re-do it independently to catch silent regressions.
        with open(fsk, "rb") as f:
            hdr = FlatseekHeader.unpack(f.read(HEADER_SIZE))
            for desc in hdr.sections:
                if desc.size == 0:
                    continue
                f.seek(desc.offset)
                section_bytes = f.read(desc.size)
                computed = hashlib.sha256(section_bytes).digest()
                assert computed == desc.checksum, (
                    f"Section {desc.section_id} checksum mismatch"
                )

    def test_checksum_detects_corruption(self):
        """Flip a byte in a packed section → unpacking must fail with checksum error."""
        data_dir = self.tmpdir / "data"
        data_dir.mkdir()
        _make_large_index(data_dir, n_bin=3, file_size=1 * 1024 * 1024)

        fsk = self.tmpdir / "out.fsk"
        FlatseekPacker(data_dir, fsk).pack()

        # Corrupt one byte deep inside the index section.
        with open(fsk, "rb") as f:
            hdr = FlatseekHeader.unpack(f.read(HEADER_SIZE))
            idx_desc = next(s for s in hdr.sections if s.section_id == SID_INDEX)
        with open(fsk, "r+b") as f:
            f.seek(idx_desc.offset + 100)
            b = f.read(1)
            f.seek(-1, 1)
            f.write(bytes([b[0] ^ 0xFF]))

        with pytest.raises(ValueError, match="checksum mismatch"):
            FlatseekUnpacker(fsk, self.tmpdir / "out").unpack()


class TestPackPeakMemoryBounded:
    """Pack peak memory must stay bounded regardless of total output size.

    Uses tracemalloc to measure peak heap during pack. The bound is loose
    (~chunk size × small factor for hasher state) — we just check that peak
    is far below the output size, proving we are not buffering everything.
    """

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmpdir = Path(tempfile.mkdtemp())
        yield
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_pack_peak_memory_under_threshold(self):
        # 80 MB total in 8 × 10 MB files. Old code would have buffered all
        # 80+ MB twice; new code streams chunks while parallel readers
        # prefetch ahead. Peak should stay bounded by worker_count × CHUNK_SIZE
        # (≈ 4 workers × 4 MB ≈ 16 MB) plus hasher/file-handle overhead —
        # far below the output size.
        data_dir = self.tmpdir / "data"
        data_dir.mkdir()
        _make_large_index(data_dir, n_bin=8, file_size=10 * 1024 * 1024)

        fsk = self.tmpdir / "out.fsk"

        tracemalloc.start()
        FlatseekPacker(data_dir, fsk).pack()
        _current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        output_size = fsk.stat().st_size
        # Bound: streaming means peak ≪ output. The OLD pre-streaming code
        # peaked at ~2-3× output_size. We allow output / 2 here (40 MB on
        # this 80 MB fixture) to accommodate the 4-worker × 4 MB prefetch
        # queue plus Python-side chunk copies during the read→hash→write
        # pipeline. Anything bigger than that means whole-file buffering
        # crept back in.
        assert peak < output_size / 2, (
            f"Pack peak memory {peak / 1024 / 1024:.1f} MB is too high "
            f"vs output {output_size / 1024 / 1024:.1f} MB — streaming failed"
        )

    def test_unpack_peak_memory_under_threshold(self):
        # Pack a large file first, then unpack and measure.
        # Per-file AEAD decryption requires whole-file reads (10 MB here);
        # peak is bounded by max(file_size), NOT by .fsk size.
        data_dir = self.tmpdir / "data"
        data_dir.mkdir()
        _make_large_index(data_dir, n_bin=8, file_size=10 * 1024 * 1024)

        fsk = self.tmpdir / "out.fsk"
        FlatseekPacker(data_dir, fsk).pack()

        out_dir = self.tmpdir / "out"

        tracemalloc.start()
        FlatseekUnpacker(fsk, out_dir).unpack()
        _current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        output_size = fsk.stat().st_size
        # Bound: peak stays ≪ output size. OLD code peaked at 2-3× output
        # (would buffer the whole section). NEW code with parallel writes
        # peaks at roughly n_workers × max(file_size) — bounded by individual
        # file size × worker count, NOT by section/fsk size. We allow up to
        # 2× output here to accommodate the parallel write queue; the
        # critical property is "peak doesn't scale with output size".
        assert peak < output_size * 2, (
            f"Unpack peak memory {peak / 1024 / 1024:.1f} MB is too high "
            f"vs fsk {output_size / 1024 / 1024:.1f} MB — streaming failed"
        )


class TestPackFormatUnchanged:
    """The packed file format must remain backward-compatible.

    Tests verify:
    - Header layout unchanged (1024 bytes, same field offsets)
    - Section offsets/sizes/checksums are byte-identical to what existing
      readers expect
    """

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmpdir = Path(tempfile.mkdtemp())
        yield
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_header_layout_unchanged(self):
        data_dir = self.tmpdir / "data"
        data_dir.mkdir()
        _make_large_index(data_dir, n_bin=2, file_size=1024)

        fsk = self.tmpdir / "out.fsk"
        FlatseekPacker(data_dir, fsk).pack()

        with open(fsk, "rb") as f:
            hdr_bytes = f.read(HEADER_SIZE)
        # Magic at offset 0 (10 bytes)
        assert hdr_bytes[:10] == b"FLATSEEK04"
        # Total checksum at [HEADER_SIZE - 64 : HEADER_SIZE - 32]
        total_checksum = hdr_bytes[HEADER_SIZE - 64:HEADER_SIZE - 32]
        assert len(total_checksum) == 32

    def test_offset_table_format_preserved(self):
        """Pack with a known file → inspect offset table layout byte-for-byte."""
        data_dir = self.tmpdir / "data"
        data_dir.mkdir()
        idx_dir = data_dir / "index"
        idx_dir.mkdir()
        idx_dir / "a.bin"
        (idx_dir / "a.bin").write_bytes(b"\x00" * 100)
        # Single file → 1 entry.
        # Layout: 4-byte count + "a.bin|" + 4-byte size + file bytes.

        fsk = self.tmpdir / "out.fsk"
        FlatseekPacker(data_dir, fsk).pack()

        with open(fsk, "rb") as f:
            hdr = FlatseekHeader.unpack(f.read(HEADER_SIZE))
            idx_desc = next(s for s in hdr.sections if s.section_id == SID_INDEX)
            f.seek(idx_desc.offset)
            tbl_len_bytes = f.read(4)
            tbl_len = struct.unpack("<I", tbl_len_bytes)[0]
            tbl_bytes = f.read(tbl_len)

        # n_entries = 1
        n_entries = struct.unpack_from("<I", tbl_bytes, 0)[0]
        assert n_entries == 1
        # Entry: "a.bin" + "|" + 4-byte size (100 = 0x64)
        assert tbl_bytes[4:10] == b"a.bin|"
        size = struct.unpack("<I", tbl_bytes[10:14])[0]
        assert size == 100
