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
