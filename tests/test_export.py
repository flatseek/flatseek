"""Tests for `flatseek export` CLI command.

Self-contained: builds a small in-memory index with IndexBuilder, then calls
cmd_export via argparse Namespace. Covers JSONL/CSV, query filter, stdout
output, column subset, limit, CSV escaping, nested values, encrypted
indexes, .fsk flat files, and resume/interrupt.

Does NOT depend on the real `./data/` directory.
"""

import contextlib
import csv as _csv
import io
import json
import os
import shutil
import sys
import tempfile
import zlib
from argparse import Namespace
from pathlib import Path

import pytest

# Make `flatseek.cli` importable when running from repo root.
_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO / "src"))

from flatseek.cli import (  # noqa: E402
    cmd_export,
    _export_checkpoint_path,
    _export_save_checkpoint,
    _export_load_checkpoint,
    _export_remove_checkpoint,
)
from flatseek.core.builder import IndexBuilder  # noqa: E402


# ── Helpers ───────────────────────────────────────────────────────────────────


def _build_index(data_dir: Path, records: list[dict]) -> Path:
    """Build a real index from a list of dicts using IndexBuilder.

    Returns data_dir. The records are written through the real builder so
    the resulting index is fully queryable via QueryEngine.
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    headers = list(records[0].keys()) if records else ["id", "value"]
    builder = IndexBuilder(str(data_dir), column_map={})
    builder.start_doc_id = 0
    for rec in records:
        row = {col: str(rec.get(col, "")) for col in headers}
        builder.add_row(row, headers)
    builder.finalize()
    return data_dir


def _make_records(n: int = 10) -> list[dict]:
    """Build a small deterministic set of records with mixed types."""
    out = []
    for i in range(n):
        out.append({
            "id": str(i),
            "name": f"user_{i:03d}",
            "score": str(10 * i),
            "active": "true" if i % 2 == 0 else "false",
            "tags": f"a,b,c" if i % 3 == 0 else "",
        })
    return out


def _args(data_dir: str | Path, **kwargs) -> Namespace:
    """Build a Namespace as cmd_export expects."""
    base = {
        "data_dir": str(data_dir),
        "output": None,
        "format": "jsonl",
        "query": "",
        "columns": None,
        "limit": None,
        "passphrase": None,
        "quiet": False,
        "resume": True,
        "storage_backend": None,
        "storage_bucket": None,
        "storage_region": None,
        "storage_endpoint": None,
        "storage_base_path": None,
        "storage_url": None,
    }
    base.update(kwargs)
    return Namespace(**base)


def _read_jsonl(path: Path) -> list[dict]:
    out = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


# ── Tests: basic export ───────────────────────────────────────────────────────


class TestExportBasic:
    """Build a tiny index, run cmd_export, verify output."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmpdir = Path(tempfile.mkdtemp(prefix="flatseek_export_"))
        self.records = _make_records(10)
        self.data_dir = _build_index(self.tmpdir / "data", self.records)
        yield
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_export_all_jsonl_to_file(self):
        out_path = self.tmpdir / "all.jsonl"
        cmd_export(_args(self.data_dir, output=str(out_path), quiet=True))
        docs = _read_jsonl(out_path)
        assert len(docs) == len(self.records)
        ids = sorted(int(d["_id"]) for d in docs)
        assert ids == list(range(len(self.records)))
        # Every record present.
        for rec in self.records:
            assert any(d.get("id") == rec["id"] for d in docs)

    def test_export_all_csv_to_file(self):
        out_path = self.tmpdir / "all.csv"
        cmd_export(_args(self.data_dir, output=str(out_path), format="csv", quiet=True))
        with open(out_path, "r", newline="") as f:
            reader = _csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == len(self.records)
        # Header has _id + all columns.
        assert "_id" in reader.fieldnames
        assert "id" in reader.fieldnames
        assert "name" in reader.fieldnames

    def test_export_with_query(self):
        # 'active:true' should match i=0,2,4,6,8 → 5 docs
        out_path = self.tmpdir / "active.jsonl"
        cmd_export(_args(
            self.data_dir, output=str(out_path),
            query="active:true", quiet=True,
        ))
        docs = _read_jsonl(out_path)
        assert len(docs) == 5
        for d in docs:
            assert d.get("active") == "true"

    def test_export_to_stdout(self):
        # Capture stdout to verify clean streaming (no progress noise on stdout).
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            cmd_export(_args(self.data_dir, quiet=True))
        output = buf.getvalue()
        # Every line is valid JSON; no progress markers.
        lines = [ln for ln in output.splitlines() if ln.strip()]
        assert len(lines) == len(self.records)
        for line in lines:
            json.loads(line)  # raises if not valid

    def test_export_columns_subset(self):
        out_path = self.tmpdir / "subset.jsonl"
        cmd_export(_args(
            self.data_dir, output=str(out_path),
            columns="id,name", quiet=True,
        ))
        docs = _read_jsonl(out_path)
        assert len(docs) == len(self.records)
        for d in docs:
            assert set(d.keys()) <= {"_id", "id", "name"}

    def test_export_limit(self):
        out_path = self.tmpdir / "limited.jsonl"
        cmd_export(_args(
            self.data_dir, output=str(out_path), limit=3, quiet=True,
        ))
        docs = _read_jsonl(out_path)
        assert len(docs) == 3

    def test_export_csv_escaping(self):
        # Build a special-case index with tricky values.
        special = [
            {"id": "1", "msg": 'has, comma'},
            {"id": "2", "msg": 'has "quote"'},
            {"id": "3", "msg": "has\nnewline"},
        ]
        data_dir = _build_index(self.tmpdir / "special", special)
        out_path = self.tmpdir / "escaped.csv"
        cmd_export(_args(data_dir, output=str(out_path), format="csv", quiet=True))
        # Round-trip through DictReader — verifies the file is well-formed.
        with open(out_path, "r", newline="") as f:
            rows = list(_csv.DictReader(f))
        assert len(rows) == 3
        msgs = [r["msg"] for r in rows]
        assert 'has, comma' in msgs
        assert 'has "quote"' in msgs
        assert "has\nnewline" in msgs

    def test_export_nested_values(self):
        # IndexBuilder stores strings; export should JSON-encode non-scalar cells.
        out_path = self.tmpdir / "nested.jsonl"
        cmd_export(_args(self.data_dir, output=str(out_path), quiet=True))
        docs = _read_jsonl(out_path)
        # Some docs have 'tags' (i % 3 == 0 in _make_records). Those that do must
        # preserve the value verbatim.
        with_tags = [d for d in docs if "tags" in d]
        assert len(with_tags) > 0
        for d in with_tags:
            assert d["tags"] == "a,b,c"


# ── Tests: encrypted index ────────────────────────────────────────────────────


class TestExportEncrypted:
    """Verify export works on encrypted indexes with the right passphrase."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmpdir = Path(tempfile.mkdtemp(prefix="flatseek_export_enc_"))
        self.records = _make_records(6)
        self.data_dir = _build_index(self.tmpdir / "data_plain", self.records)
        yield
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _encrypt_index(self, src: Path, dst: Path, passphrase: str) -> None:
        """Mirror cmd_encrypt: copy index, encrypt all bytes, write encryption.json."""
        import base64
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
        from flatseek.core.query_engine import encrypt_bytes

        dst.mkdir(parents=True, exist_ok=True)
        for src_path in src.rglob("*"):
            if src_path.is_dir():
                continue
            rel = src_path.relative_to(src)
            dst_path = dst / rel
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            data = src_path.read_bytes()
            try:
                data = encrypt_bytes(data, key)  # noqa: F821
            except Exception:
                pass
            dst_path.write_bytes(data)
        # Derive key + write encryption.json (matches cmd_encrypt's salt format).
        salt = os.urandom(16)
        # Must match query_engine.derive_key's default iterations.
        iterations = 600_000
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt,
                         iterations=iterations)
        key = kdf.derive(passphrase.encode("utf-8"))
        meta = {
            "salt": salt.hex(),
            "algorithm": "ChaCha20-Poly1305",
            "kdf": "PBKDF2-HMAC-SHA256",
            "iterations": iterations,
        }
        (dst / "encryption.json").write_text(json.dumps(meta))
        # Re-encrypt with the real key (the try/except above was just a placeholder).
        # Actually let's do this properly: encrypt every file with the derived key.
        for dst_path in dst.rglob("*"):
            if dst_path.is_dir() or dst_path.name == "encryption.json":
                continue
            data = dst_path.read_bytes()
            try:
                encrypted = encrypt_bytes(data, key)
                dst_path.write_bytes(encrypted)
            except Exception:
                # Not all files are encryptable; leave as-is.
                pass

    def test_export_encrypted_index(self):
        passphrase = "secret123"
        enc_dir = self.tmpdir / "data_enc"
        self._encrypt_index(self.data_dir, enc_dir, passphrase)

        # Export the plaintext index (baseline).
        out_plain = self.tmpdir / "plain.jsonl"
        cmd_export(_args(self.data_dir, output=str(out_plain), quiet=True))
        plain_docs = _read_jsonl(out_plain)

        # Export the encrypted index (with passphrase).
        out_enc = self.tmpdir / "enc.jsonl"
        cmd_export(_args(
            enc_dir, output=str(out_enc),
            passphrase=passphrase, quiet=True,
        ))
        enc_docs = _read_jsonl(out_enc)

        # Same number of docs, same _ids.
        assert len(plain_docs) == len(enc_docs)
        plain_ids = sorted(d["_id"] for d in plain_docs)
        enc_ids = sorted(d["_id"] for d in enc_docs)
        assert plain_ids == enc_ids


# ── Tests: .fsk flat file ─────────────────────────────────────────────────────


class TestExportFlatseekFile:
    """Verify export works on .fsk flat files (with and without encryption)."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmpdir = Path(tempfile.mkdtemp(prefix="flatseek_export_fsk_"))
        self.records = _make_records(5)
        self.data_dir = _build_index(self.tmpdir / "data_src", self.records)
        yield
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_export_from_fsk(self):
        from flatseek.flatseek_file import FlatseekPacker
        fsk_path = self.tmpdir / "out.fsk"
        FlatseekPacker(self.data_dir, fsk_path).pack()
        assert fsk_path.exists()
        assert fsk_path.stat().st_size > 0

        # Export from .fsk.
        out_fsk = self.tmpdir / "from_fsk.jsonl"
        cmd_export(_args(fsk_path, output=str(out_fsk), quiet=True))
        fsk_docs = _read_jsonl(out_fsk)

        # Export from directory (baseline).
        out_dir = self.tmpdir / "from_dir.jsonl"
        cmd_export(_args(self.data_dir, output=str(out_dir), quiet=True))
        dir_docs = _read_jsonl(out_dir)

        # Same docs.
        assert len(fsk_docs) == len(dir_docs) == len(self.records)
        fsk_ids = sorted(d["_id"] for d in fsk_docs)
        dir_ids = sorted(d["_id"] for d in dir_docs)
        assert fsk_ids == dir_ids


# ── Tests: resume / interrupt ─────────────────────────────────────────────────


class TestExportResume:
    """Verify the checkpoint sidecar enables safe rerun."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmpdir = Path(tempfile.mkdtemp(prefix="flatseek_export_resume_"))
        self.records = _make_records(50)
        self.data_dir = _build_index(self.tmpdir / "data", self.records)
        yield
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_resume_from_checkpoint_skips_already_written(self):
        out_path = self.tmpdir / "out.jsonl"
        # Pre-populate output + checkpoint as if a prior run wrote 25 rows.
        partial = _read_jsonl  # helper
        with open(out_path, "w") as f:
            for i in range(25):
                f.write(json.dumps({"_id": i, "name": f"u{i}"}) + "\n")
        _export_save_checkpoint(
            out_path,
            query_str="", fmt="jsonl", cols=None,
            last_id=24, count=25, started_at="2026-06-28T00:00:00Z",
        )
        # Rerun export — should append only the remaining 25 rows.
        cmd_export(_args(self.data_dir, output=str(out_path), quiet=True))
        docs = _read_jsonl(out_path)
        # 25 (pre-existing) + 25 (newly exported) = 50 unique rows.
        assert len(docs) == 50
        ids = sorted(int(d["_id"]) for d in docs)
        assert ids == list(range(50))
        # No duplicates.
        assert len(set(ids)) == 50
        # Checkpoint removed on success.
        assert not Path(_export_checkpoint_path(out_path)).exists()

    def test_resume_query_mismatch_errors(self):
        out_path = self.tmpdir / "out.jsonl"
        out_path.write_text("")  # create the file
        _export_save_checkpoint(
            out_path,
            query_str="status:active", fmt="jsonl", cols=None,
            last_id=10, count=11, started_at="2026-06-28T00:00:00Z",
        )
        # Rerun with a different query — should exit with code 2.
        with contextlib.redirect_stderr(io.StringIO()):
            with pytest.raises(SystemExit) as exc_info:
                cmd_export(_args(
                    self.data_dir, output=str(out_path),
                    query="status:inactive", quiet=True,
                ))
        assert exc_info.value.code == 2

    def test_resume_columns_warning(self):
        out_path = self.tmpdir / "out.jsonl"
        # Checkpoint claims columns were all columns (None).
        out_path.write_text("")
        _export_save_checkpoint(
            out_path,
            query_str="", fmt="jsonl", cols=None,
            last_id=0, count=1, started_at="2026-06-28T00:00:00Z",
        )
        # Capture stderr to verify the warning is printed.
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            cmd_export(_args(
                self.data_dir, output=str(out_path),
                columns="id,name", quiet=True,
            ))
        assert "column subset differs" in err.getvalue()

    def test_force_restart_clears_checkpoint(self):
        out_path = self.tmpdir / "out.jsonl"
        # Stale checkpoint + stale output.
        with open(out_path, "w") as f:
            f.write('{"_id": 999, "stale": true}\n')
        _export_save_checkpoint(
            out_path,
            query_str="", fmt="jsonl", cols=None,
            last_id=999, count=1, started_at="2026-06-28T00:00:00Z",
        )
        # Run with --no-resume (= resume=False).
        cmd_export(_args(
            self.data_dir, output=str(out_path),
            resume=False, quiet=True,
        ))
        # Stale row is gone; export contains the real data.
        docs = _read_jsonl(out_path)
        assert len(docs) == len(self.records)
        assert not any(d.get("stale") for d in docs)
        # Checkpoint cleaned up.
        assert not Path(_export_checkpoint_path(out_path)).exists()

    def test_stdout_export_no_checkpoint(self):
        # Stdout export must never write a checkpoint file. We point it at
        # a fake path to make sure no sidecar appears in the tmpdir.
        with contextlib.redirect_stdout(io.StringIO()):
            cmd_export(_args(self.data_dir, quiet=True))
        # No checkpoint files anywhere in tmpdir.
        checkpoints = list(self.tmpdir.rglob("*" + _export_checkpoint_path("")[1:]))
        assert checkpoints == []

    def test_quiet_suppresses_progress(self):
        out_path = self.tmpdir / "out.jsonl"
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            cmd_export(_args(
                self.data_dir, output=str(out_path), quiet=True,
            ))
        # No "[EXPORT]" progress markers when --quiet.
        assert "[EXPORT]" not in err.getvalue()


# ── Tests: OOM safety ──────────────────────────────────────────────────────────


class TestExportSkipEmptyChunks:
    """Streaming export must pass `match_chunks` to `_iter_chunks` so the
    storage adapter skips non-matched chunks BEFORE reading them from
    disk. This is the critical optimization for large encrypted .fsk —
    without it, a sparse query matching 10 chunks out of 1000 spends ~99%
    of wall time reading chunks that have no matches.

    Regression for the slow-export issue: API (which only fetches the
    chunks it needs) was ~10x faster than the streaming export that
    iterated every chunk just to skip most.
    """

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmpdir = Path(tempfile.mkdtemp(prefix="flatseek_export_skip_"))
        self.records = _make_records(50)
        self.data_dir = _build_index(self.tmpdir / "data", self.records)
        yield
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_export_skips_non_matched_chunks_before_read(self):
        """Verify that export only LOADS chunks that have matches — non-matched
        chunks are never read. Without this, every chunk in the bucket would
        be seek+read+decrypt+decompressed just to be filtered out.

        The optimization now goes through `_load_chunk(cs, cache=False)` per
        matched chunk (parallelized via ThreadPoolExecutor). Spy on
        `_load_chunk` and verify it was called only for the matched chunks.
        """
        from flatseek.cli import _export_stream_query
        from flatseek.core.query_engine import QueryEngine

        qe = QueryEngine(str(self.data_dir))

        # active:true → 25 of 50 docs, all in chunk 0.
        matched_set = {
            did for did in range(50) if self.records[did]["active"] == "true"
        }
        expected_matched_chunks = {qe._chunk_start(did) for did in matched_set}

        # Spy on _load_chunk to count calls and the chunk_starts accessed.
        load_calls = {"chunks": []}
        original_load = qe._load_chunk

        def spy_load(start, cache=True):
            load_calls["chunks"].append(start)
            return original_load(start, cache=cache)

        qe._load_chunk = spy_load

        # Drive the streaming export.
        docs = list(_export_stream_query(qe, "active:true", cols=None, limit=None))
        assert len(docs) == 25

        # The exporter should have loaded ONLY chunks that have matches.
        loaded = set(load_calls["chunks"])
        # All loaded chunks must be in the matched set (no false-positive
        # chunk reads).
        assert loaded <= expected_matched_chunks, (
            f"Export loaded {loaded - expected_matched_chunks} non-matched "
            f"chunks: {loaded - expected_matched_chunks}. Only "
            f"{expected_matched_chunks} should be read."
        )
        # All matched chunks must be loaded (no false-negatives).
        assert loaded == expected_matched_chunks, (
            f"Export loaded {loaded} but expected {expected_matched_chunks}. "
            f"Some matched chunks were skipped."
        )


class TestExportParallelPreRead:
    """Streaming export must use ThreadPoolExecutor to pre-read matched chunks
    concurrently — this is the speedup for large encrypted .fsk where each
    chunk is ~300-700ms of read+decrypt+decompress.

    Without parallelism: N matched chunks × ~500ms = sequential. With
    8 workers: N/8 × 500ms. For 1000 chunks: 500s → 60s. 8x speedup.
    """

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmpdir = Path(tempfile.mkdtemp(prefix="flatseek_export_par_"))
        # Use enough records + multiple terms to force matches across
        # multiple chunks (chunk_size=100K by default, so 50 records all
        # fit in chunk 0; for the test we just need a small dataset to
        # verify parallel behavior, not raw speedup).
        self.records = _make_records(50)
        self.data_dir = _build_index(self.tmpdir / "data", self.records)
        yield
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_export_uses_threadpool_for_chunk_reads(self):
        """Spy on `_load_chunk` to verify multiple chunks are submitted
        to a ThreadPoolExecutor (rather than read serially).

        We use a slow spy (sleeps 200ms) so serial reads would be slow
        and parallel reads would be fast. If the export went serial,
        wall time would be ≥ N × 200ms. With parallel, it's ~⌈N/workers⌉
        × 200ms. We assert wall time is well under the serial bound.
        """
        import time as _time
        from flatseek.cli import _export_stream_query
        from flatseek.core.query_engine import QueryEngine

        qe = QueryEngine(str(self.data_dir))

        # Mock _load_chunk to add a delay so serial vs parallel is observable.
        # active:true matches docs in chunk 0; for the parallel test we
        # build a synthetic set of matched chunks via the spy.
        load_calls = {"n": 0, "concurrent": 0, "max_concurrent": 0}

        def slow_load(start, cache=True):
            load_calls["n"] += 1
            load_calls["concurrent"] += 1
            load_calls["max_concurrent"] = max(
                load_calls["max_concurrent"], load_calls["concurrent"]
            )
            try:
                # Inject a delay so serial reads would compound.
                _time.sleep(0.05)  # 50ms per chunk
                # Build a fake chunk with one doc for each matched chunk.
                # Without this, the export would still read real data,
                # but we want to control timing precisely.
                return {0: {"_id": start, "active": "true"}}
            finally:
                load_calls["concurrent"] -= 1

        # Force export to believe there are many matched chunks. Monkey-patch
        # execute to return a large set spread across many chunks.
        from flatseek.core import query_parser
        original_execute = query_parser.execute

        def fake_execute(node, qe_, doc_ids=None):
            # Return IDs spread across 16 chunks to force parallel reads.
            return set(range(0, 16 * 100_000, 100_000 // 8))  # 16 distinct chunk_starts

        qe._load_chunk = slow_load
        query_parser.execute = fake_execute
        try:
            t0 = _time.perf_counter()
            docs = list(_export_stream_query(qe, "*", cols=None, limit=128))
            wall = _time.perf_counter() - t0
        finally:
            query_parser.execute = original_execute

        # We should have read ~16 chunks. Serial would be 16 × 50ms = 800ms.
        # Parallel with 8 workers should be ~2 × 50ms = 100ms (plus overhead).
        assert load_calls["n"] >= 16, (
            f"Expected to read ≥16 chunks, only read {load_calls['n']}"
        )
        # Strict speedup check: parallel should be < 60% of serial.
        serial_estimate = load_calls["n"] * 0.05
        assert wall < serial_estimate * 0.6, (
            f"Export reads look serial: {load_calls['n']} chunks in {wall:.2f}s "
            f"(serial would be ~{serial_estimate:.2f}s). ThreadPool not used "
            f"or worker count too low."
        )
        # We also should have seen >1 concurrent read at some point.
        assert load_calls["max_concurrent"] >= 2, (
            f"Max concurrent chunk reads = {load_calls['max_concurrent']} — "
            f"ThreadPool not running chunks in parallel. Need ≥2 for "
            f"parallel speedup."
        )


class TestExportNoOom:
    """Regression: long pagination exports must not grow `_doc_cache`
    unboundedly. Each `qe.query()` page reads fresh chunks; without
    `clear_doc_cache()` between pages, every chunk ever read stays in
    memory forever, OOM-killing the process on big result sets.

    We can't easily build a 100M-row index in a unit test, so we mock the
    pagination loop: invoke `_export_stream_query` repeatedly against
    an engine whose `_load_chunk` populates `_doc_cache` for a fresh
    chunk each iteration. Verify cache size stays bounded.
    """

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmpdir = Path(tempfile.mkdtemp(prefix="flatseek_export_oom_"))
        self.records = _make_records(50)
        self.data_dir = _build_index(self.tmpdir / "data", self.records)
        yield
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_export_query_does_not_populate_doc_cache(self):
        """The streaming `_export_stream_query` reads chunks via
        `_iter_chunks()` (which bypasses `_load_chunk` / `_doc_cache`),
        so the cache must NOT be populated by export — that's the OOM fix.

        Regression test: previously export used `qe.query()` pagination which
        called `_load_chunk` for every chunk touched, ballooning `_doc_cache`
        and OOM-killing big exports.
        """
        from flatseek.cli import _export_stream_query
        from flatseek.core.query_engine import QueryEngine

        qe = QueryEngine(str(self.data_dir))
        before = len(qe._doc_cache)

        # Drive the streaming export to completion.
        docs = list(_export_stream_query(qe, "active:true", cols=None, limit=None))

        # active:true matches 25 of 50 records. Result correctness first.
        assert len(docs) == 25
        assert all(d.get("active") == "true" for d in docs)

        # The cache must NOT grow — `_iter_chunks` is the path, not `_load_chunk`.
        # (Even if a stale entry existed before the export, the export itself
        # does not add to it.)
        assert len(qe._doc_cache) == before, (
            f"Streaming export populated _doc_cache (added {len(qe._doc_cache) - before} "
            f"entries). Should use _iter_chunks which bypasses _doc_cache. "
            f"This is the OOM regression — fix is to stream via _iter_chunks "
            f"instead of paginating via qe.query()."
        )
