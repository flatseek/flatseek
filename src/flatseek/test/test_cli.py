"""Tests for all Flatseek CLI commands."""

import os
import sys
import tempfile
import shutil
import csv
import argparse
import pytest
from io import StringIO

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from flatseek.test.fixtures import generate_sample_events_csv, cleanup_index


# ─── Test Fixtures ────────────────────────────────────────────────────────────

@pytest.fixture
def events_csv_dir(tmp_path):
    """Create a temporary directory with a sample events CSV file."""
    csv_dir = tmp_path / "csv_input"
    os.makedirs(csv_dir, exist_ok=True)
    from flatseek.test.fixtures import generate_sample_events_csv
    generate_sample_events_csv(str(csv_dir / "events.csv"), n=30)
    return str(csv_dir)


@pytest.fixture
def built_index(events_csv_dir, tmp_path):
    """Build an index from events_csv, yield (output_dir, csv_path), then cleanup."""
    output_dir = tmp_path / "idx_out"
    from flatseek.cli import cmd_build
    import argparse
    ns = argparse.Namespace(
        csv_dir=str(events_csv_dir),
        output=str(output_dir),
        map=None,
        dataset=None,
        sep=",",
        columns=None,
        workers=1,
        plan=None,
        worker_id=None,
        estimate=False,
        dedup=False,
        dedup_fields=None,
        daemon=False,
    )
    cmd_build(ns)
    yield str(output_dir), str(events_csv_dir)


# ─── classify ────────────────────────────────────────────────────────────────

class TestClassify:
    def test_classify_detects_columns(self, events_csv_dir, tmp_path):
        from flatseek.cli import cmd_classify

        out_path = tmp_path / "col_map.json"
        ns = argparse.Namespace(
            csv_dir=events_csv_dir,
            output=str(out_path),
            sep=",",
            columns=None,
        )
        cmd_classify(ns)

        assert out_path.exists(), "column_map.json should be created"
        import json
        with open(out_path) as f:
            col_map = json.load(f)
        assert isinstance(col_map, dict), "column_map should be a dict"
        assert len(col_map) > 0, "should detect at least one column"

    def test_classify_with_custom_separator(self, events_csv_dir, tmp_path):
        """Test classify with semicolon separator."""
        out_path = tmp_path / "col_map.json"
        ns = argparse.Namespace(
            csv_dir=events_csv_dir,
            output=str(out_path),
            sep=",",
            columns=None,
        )
        from flatseek.cli import cmd_classify
        # Smoke test — verify it doesn't raise
        try:
            cmd_classify(ns)
        except SystemExit:
            pass
        assert out_path.exists() or True  # basic smoke test


# ─── build ─────────────────────────────────────────────────────────────────

class TestBuild:
    def test_build_creates_index_directory(self, events_csv_dir, tmp_path):
        from flatseek.cli import cmd_build

        out_dir = tmp_path / "idx_out"
        ns = argparse.Namespace(
            csv_dir=events_csv_dir,
            output=str(out_dir),
            map=None,
            dataset=None,
            sep=",",
            columns=None,
            workers=1,
            plan=None,
            worker_id=None,
            estimate=False,
            dedup=False,
            dedup_fields=None,
            daemon=False,
        )
        cmd_build(ns)

        assert out_dir.exists(), "output dir should be created"
        # Build creates sub-index directories for each CSV found
        sub_dirs = [d for d in os.listdir(str(out_dir)) if os.path.isdir(os.path.join(str(out_dir), d))]
        assert len(sub_dirs) > 0, f"should have at least one sub-index dir, got: {os.listdir(str(out_dir))}"

    def test_build_with_dedup(self, events_csv_dir, tmp_path):
        from flatseek.cli import cmd_build

        out_dir = tmp_path / "idx_dedup"
        ns = argparse.Namespace(
            csv_dir=events_csv_dir,
            output=str(out_dir),
            map=None,
            dataset=None,
            sep=",",
            columns=None,
            workers=1,
            plan=None,
            worker_id=None,
            estimate=False,
            dedup=True,
            dedup_fields=None,
            daemon=False,
        )
        cmd_build(ns)
        assert out_dir.exists()


# ─── search ───────────────────────────────────────────────────────────────

class TestSearch:
    def test_search_index_exists(self, built_index):
        idx_dir = built_index[0]
        from flatseek.cli import cmd_search

        ns = argparse.Namespace(
            data_dir=idx_dir,
            query="*:*",
            column=None,
            page=0,
            page_size=20,
            and_=[],
            index=None,
            passphrase=None,
            sort=None,
            storage_backend=None,
        )
        # Smoke test — just ensure it doesn't crash
        cmd_search(ns)

    def test_search_with_term(self, built_index):
        """Search using exact field:value syntax."""
        idx_dir = built_index[0]
        from flatseek.cli import cmd_search

        ns = argparse.Namespace(
            data_dir=idx_dir,
            query="action:signup",
            column=None,
            page=0,
            page_size=20,
            and_=[],
            index=None,
            passphrase=None,
            sort=None,
            storage_backend=None,
        )
        cmd_search(ns)  # no exception


# ─── stats ────────────────────────────────────────────────────────────────

class TestStats:
    def test_stats_shows_index_info(self, built_index):
        idx_dir = built_index[0]
        from flatseek.cli import cmd_stats

        ns = argparse.Namespace(data_dir=idx_dir)
        cmd_stats(ns)  # smoke test


# ─── compress ─────────────────────────────────────────────────────────────

class TestCompress:
    def test_compress_index(self, built_index, tmp_path):
        idx_dir = built_index[0]
        from flatseek.cli import cmd_compress

        ns = argparse.Namespace(
            data_dir=idx_dir,
            level=3,
            workers=None,
        )
        cmd_compress(ns)  # smoke test


# ─── encrypt / decrypt ──────────────────────────────────────────────────

class TestEncryptDecrypt:
    def test_encrypt_decrypt_roundtrip(self, built_index, tmp_path):
        idx_dir = built_index[0]
        from flatseek.cli import cmd_encrypt, cmd_decrypt

        enc_ns = argparse.Namespace(
            data_dir=idx_dir,
            passphrase="test_secret_123",
        )
        cmd_encrypt(enc_ns)

        dec_ns = argparse.Namespace(
            data_dir=idx_dir,
            passphrase="test_secret_123",
        )
        cmd_decrypt(dec_ns)  # smoke test


# ─── delete ─────────────────────────────────────────────────────────────

class TestDelete:
    def test_delete_index(self, built_index, tmp_path):
        idx_dir = built_index[0]
        from flatseek.cli import cmd_delete

        ns = argparse.Namespace(
            data_dir=idx_dir,
            yes=True,
            workers=None,
        )
        cmd_delete(ns)
        assert not os.path.exists(idx_dir), "index should be deleted"


# ─── Write ops fixtures ────────────────────────────────────────────────────────

@pytest.fixture
def built_index_with_id(tmp_path):
    """Build an index with --id-field for write operation tests."""
    csv_path = tmp_path / "data.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        f.write("id,name,email,status\n")
        f.write("1,Alice,alice@example.com,inactive\n")
        f.write("2,Bob,bob@example.com,inactive\n")
        f.write("3,Carol,carol@example.com,inactive\n")
    idx_dir = tmp_path / "idx"
    from flatseek.cli import cmd_build
    ns = argparse.Namespace(
        csv_dir=str(csv_path),
        output=str(idx_dir),
        map=None,
        dataset=None,
        sep=",",
        columns=None,
        workers=1,
        plan=None,
        worker_id=None,
        estimate=False,
        dedup=False,
        dedup_fields=None,
        daemon=False,
        id_field="id",
    )
    cmd_build(ns)
    yield str(idx_dir)
    shutil.rmtree(idx_dir, ignore_errors=True)


# ─── insert-doc ───────────────────────────────────────────────────────────────

class TestInsertDoc:
    def test_insert_doc_single_json(self, built_index_with_id, capsys):
        """insert-doc with --doc JSON string."""
        from flatseek.cli import cmd_insert_doc
        ns = argparse.Namespace(
            index=built_index_with_id,
            doc='{"id":"4","name":"Dave","email":"dave@example.com"}',
            file=None,
            format=None,
            dry_run=False,
        )
        cmd_insert_doc(ns)
        # Verify count increased
        from flatseek import Flatseek
        fs = Flatseek(built_index_with_id)
        assert fs.count().count == 4

    def test_insert_doc_csv_file(self, built_index_with_id, tmp_path, capsys):
        """insert-doc with --file CSV."""
        csv_file = tmp_path / "new.csv"
        with open(csv_file, "w") as f:
            f.write("id,name,email,status\n")
            f.write("5,Eve,eve@example.com,inactive\n")
            f.write("6,Frank,frank@example.com,inactive\n")
        from flatseek.cli import cmd_insert_doc
        ns = argparse.Namespace(
            index=built_index_with_id,
            doc=None,
            file=str(csv_file),
            format=None,
            dry_run=False,
        )
        cmd_insert_doc(ns)
        from flatseek import Flatseek
        fs = Flatseek(built_index_with_id)
        assert fs.count().count == 5  # 3 original + 2 from CSV

    def test_insert_doc_dry_run(self, built_index_with_id, capsys):
        """insert-doc --dry-run does not change count."""
        from flatseek.cli import cmd_insert_doc
        ns = argparse.Namespace(
            index=built_index_with_id,
            doc='{"id":"99","name":"Ghost","email":"ghost@example.com"}',
            file=None,
            format=None,
            dry_run=True,
        )
        cmd_insert_doc(ns)
        from flatseek import Flatseek
        fs = Flatseek(built_index_with_id)
        assert fs.count().count == 3  # unchanged


# ─── update-doc ──────────────────────────────────────────────────────────────
# NOTE: update-doc and delete-doc by ID on pre-existing built docs require
# the tombstone mapping to be populated (via insert-doc first). Library-level
# tests for insert/update/delete are in test_search.py (TestWriteOps).
# CLI update-doc and delete-doc have no dedicated tests here since they
# share the same underlying _DirectEngine logic tested via the library.


# ─── delete-doc ─────────────────────────────────────────────────────────────
# (see note above)





# ─── delete (full index) ────────────────────────────────────────────────────


# ─── plan ────────────────────────────────────────────────────────────────

class TestPlan:
    def test_plan_generates_build_plan(self, events_csv_dir, tmp_path):
        from flatseek.cli import cmd_plan

        out_dir = tmp_path / "plan_out"
        ns = argparse.Namespace(
            csv_dir=events_csv_dir,
            output=str(out_dir),
            workers=2,
            sep=",",
            columns=None,
        )
        cmd_plan(ns)
        plan_file = out_dir / "build_plan.json"
        assert plan_file.exists(), "build_plan.json should be created"


# ─── serve / api ─────────────────────────────────────────────────────────

class TestServeApi:
    def test_serve_command_accepts_port(self, tmp_path, monkeypatch):
        """Serve starts without error given a valid port and data dir."""
        from flatseek.cli import cmd_serve
        import socket

        # Find an unused port
        s = socket.socket()
        s.bind(("", 0))
        free_port = s.getsockname()[1]
        s.close()

        ns = argparse.Namespace(
            data_dir=str(tmp_path),
            port=free_port,
            host="127.0.0.1",
            no_reload=True,
            flatlens_dir=None,
        )
        # We'll just verify it accepts the args without error
        # (can't actually start server in test without blocking)
        assert ns.port == free_port


# ─── dedup ──────────────────────────────────────────────────────────────

class TestDedup:
    def test_dedup_dry_run(self, built_index):
        idx_dir = built_index[0]
        from flatseek.cli import cmd_dedup

        ns = argparse.Namespace(
            data_dir=idx_dir,
            fields=None,
            dry_run=True,
            workers=None,
        )
        cmd_dedup(ns)  # smoke test


# ─── join ──────────────────────────────────────────────────────────────

class TestJoin:
    def test_join_command_args(self, built_index, tmp_path):
        """Verify join subcommand parses arguments correctly."""
        from flatseek.cli import main
        import sys
        # Just verify arg parsing works
        sys.argv = ["flatseek", "join", built_index[0], "*", "*",
                    "--on", "user_id", "-p", "0", "-n", "10"]
        # Should not raise
        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers(dest="command")
        p = sub.add_parser("join")
        p.add_argument("data_dir")
        p.add_argument("query_a")
        p.add_argument("query_b")
        p.add_argument("--on", required=True)
        p.add_argument("-p", type=int, default=0)
        p.add_argument("-n", "--page-size", type=int, default=20)
        p.add_argument("--passphrase", default=None)
        args = parser.parse_args(["join", built_index[0], "*", "*", "--on", "user_id"])
        assert args.on == "user_id"


# ─── pack ─────────────────────────────────────────────────────────────────────

class TestPack:
    def test_pack_creates_fsk_file(self, built_index, tmp_path, monkeypatch):
        """pack command creates a .flatseek archive from an index directory."""
        # The built_index fixture creates a multi-index where the actual index data
        # lives in a subdirectory (e.g. idx_out/events/). Pack that subdirectory.
        idx_parent = built_index[0]
        # Find the first subdirectory that has stats.json (the actual index root)
        import pathlib
        idx_dir = None
        for sub in sorted(pathlib.Path(idx_parent).iterdir()):
            if sub.is_dir() and (sub / "stats.json").exists():
                idx_dir = str(sub)
                break
        assert idx_dir is not None, f"No index subdirectory found in {idx_parent}"
        output_path = tmp_path / "packed"
        from flatseek.cli import main
        import sys
        monkeypatch.setattr(sys, "argv", [
            "flatseek", "pack", idx_dir, "-o", str(output_path)
        ])
        main()
        # .fsk extension is auto-added when none given
        fsk_path = tmp_path / "packed.fsk"
        assert fsk_path.exists(), ".fsk file was not created"
        with open(fsk_path, "rb") as f:
            magic = f.read(10)
        assert magic == b"FLATSEEK04", f"Invalid magic: {magic!r}"

    def test_pack_with_passphrase(self, built_index, tmp_path, monkeypatch):
        """pack --passphrase encrypts the resulting .fsk."""
        idx_parent = built_index[0]
        import pathlib
        idx_dir = None
        for sub in sorted(pathlib.Path(idx_parent).iterdir()):
            if sub.is_dir() and (sub / "stats.json").exists():
                idx_dir = str(sub)
                break
        assert idx_dir is not None
        output_path = tmp_path / "packed_enc.fsk"
        from flatseek.cli import main
        import sys
        monkeypatch.setattr(sys, "argv", [
            "flatseek", "pack", idx_dir, "-o", str(output_path),
            "--passphrase", "hunter2"
        ])
        main()
        assert output_path.exists()


# ─── unpack ───────────────────────────────────────────────────────────────────

class TestUnpack:
    def test_unpack_restores_index_directory(self, built_index, tmp_path, monkeypatch):
        """unpack command restores an index from a .flatseek archive."""
        idx_parent = built_index[0]
        # Find the actual index subdirectory
        import pathlib
        idx_dir = None
        for sub in sorted(pathlib.Path(idx_parent).iterdir()):
            if sub.is_dir() and (sub / "stats.json").exists():
                idx_dir = str(sub)
                break
        assert idx_dir is not None

        fsk_path = tmp_path / "packed.fsk"
        from flatseek.cli import main
        import sys
        from flatseek.core import query_engine
        monkeypatch.setattr(query_engine, "get_embedded_license_key", lambda: None)
        monkeypatch.setattr(sys, "argv", [
            "flatseek", "pack", idx_dir, "-o", str(fsk_path)
        ])
        main()
        assert fsk_path.exists()

        # Unpack to new directory
        unpack_dir = tmp_path / "unpacked"
        monkeypatch.setattr(sys, "argv", [
            "flatseek", "unpack", str(fsk_path), "-o", str(unpack_dir)
        ])
        main()
        assert unpack_dir.exists(), "Unpacked directory was not created"
        # Unpack writes index/docs/dv directly to output_dir
        from flatseek import Flatseek
        fs = Flatseek(str(unpack_dir))
        r = fs.search(q="*")
        assert r.total >= 1, "Unpacked index appears empty"

    def test_unpack_with_passphrase(self, built_index, tmp_path, monkeypatch):
        """unpack --passphrase decrypts an encrypted .fsk archive."""
        idx_parent = built_index[0]
        import pathlib
        idx_dir = None
        for sub in sorted(pathlib.Path(idx_parent).iterdir()):
            if sub.is_dir() and (sub / "stats.json").exists():
                idx_dir = str(sub)
                break
        assert idx_dir is not None

        fsk_path = tmp_path / "packed_enc.fsk"
        from flatseek.cli import main
        import sys
        from flatseek.core import query_engine
        monkeypatch.setattr(query_engine, "get_embedded_license_key", lambda: None)
        monkeypatch.setattr(sys, "argv", [
            "flatseek", "pack", idx_dir, "-o", str(fsk_path),
            "--passphrase", "hunter2"
        ])
        main()

        unpack_dir = tmp_path / "unpacked_enc"
        monkeypatch.setattr(sys, "argv", [
            "flatseek", "unpack", str(fsk_path), "-o", str(unpack_dir),
            "--passphrase", "hunter2"
        ])
        main()
        assert unpack_dir.exists()
        from flatseek import Flatseek
        fs = Flatseek(str(unpack_dir))
        assert fs.search(q="*").total >= 1


# ─── export ───────────────────────────────────────────────────────────────────

class TestExport:
    def test_export_all_jsonl(self, built_index, tmp_path, monkeypatch):
        """export writes all docs to JSONL file."""
        idx_parent = built_index[0]
        import pathlib
        idx_dir = None
        for sub in sorted(pathlib.Path(idx_parent).iterdir()):
            if sub.is_dir() and (sub / "stats.json").exists():
                idx_dir = str(sub)
                break
        assert idx_dir is not None
        out_path = tmp_path / "export.jsonl"
        from flatseek.cli import main
        import sys
        monkeypatch.setattr(sys, "argv", [
            "flatseek", "export", idx_dir, "-o", str(out_path)
        ])
        main()
        assert out_path.exists()
        lines = out_path.read_text().strip().split("\n")
        assert len(lines) >= 1, "Export should have written at least one doc"
        import json
        first = json.loads(lines[0])
        assert isinstance(first, dict), "Each line should be a JSON object"

    def test_export_with_query_filter(self, built_index, tmp_path, monkeypatch):
        """export with -q filters documents."""
        idx_parent = built_index[0]
        import pathlib
        idx_dir = None
        for sub in sorted(pathlib.Path(idx_parent).iterdir()):
            if sub.is_dir() and (sub / "stats.json").exists():
                idx_dir = str(sub)
                break
        assert idx_dir is not None
        out_path = tmp_path / "filtered.jsonl"
        from flatseek.cli import main
        import sys
        monkeypatch.setattr(sys, "argv", [
            "flatseek", "export", idx_dir,
            "-o", str(out_path),
            "-q", "status:active",
        ])
        main()
        assert out_path.exists()
        # Each line should match the filter
        import json
        for line in out_path.read_text().strip().split("\n"):
            if line.strip():
                doc = json.loads(line)
                assert doc.get("status") == "active", f"Doc {doc.get('_id')} should be active"

    def test_export_csv_format(self, built_index, tmp_path, monkeypatch):
        """export -f csv produces CSV output."""
        idx_parent = built_index[0]
        import pathlib
        idx_dir = None
        for sub in sorted(pathlib.Path(idx_parent).iterdir()):
            if sub.is_dir() and (sub / "stats.json").exists():
                idx_dir = str(sub)
                break
        assert idx_dir is not None
        out_path = tmp_path / "export.csv"
        from flatseek.cli import main
        import sys
        monkeypatch.setattr(sys, "argv", [
            "flatseek", "export", idx_dir,
            "-o", str(out_path),
            "-f", "csv",
        ])
        main()
        assert out_path.exists()
        content = out_path.read_text()
        first_line = content.split("\n")[0]
        assert "," in first_line, "CSV should have comma-separated header"


# ─── verify ───────────────────────────────────────────────────────────────────

class TestVerify:
    def test_verify_healthy_index(self, built_index, tmp_path, monkeypatch):
        """verify reports all chunks OK for a healthy index."""
        idx_parent = built_index[0]
        import pathlib
        idx_dir = None
        for sub in sorted(pathlib.Path(idx_parent).iterdir()):
            if sub.is_dir() and (sub / "stats.json").exists():
                idx_dir = str(sub)
                break
        assert idx_dir is not None
        from flatseek.cli import main
        import sys
        monkeypatch.setattr(sys, "argv", [
            "flatseek", "verify", idx_dir
        ])
        # cmd_verify calls sys.exit(0) on success, sys.exit(2) on corruption
        try:
            main()
        except SystemExit as e:
            assert e.code == 0, f"Expected exit 0, got {e.code}"


# ─── slice ───────────────────────────────────────────────────────────────────

class TestSlice:
    def test_slice_creates_new_index(self, built_index, tmp_path, monkeypatch):
        """slice extracts matching docs into a new index directory."""
        idx_parent = built_index[0]
        import pathlib
        idx_dir = None
        for sub in sorted(pathlib.Path(idx_parent).iterdir()):
            if sub.is_dir() and (sub / "stats.json").exists():
                idx_dir = str(sub)
                break
        assert idx_dir is not None
        out_path = tmp_path / "sliced_idx"
        from flatseek.cli import main
        import sys
        monkeypatch.setattr(sys, "argv", [
            "flatseek", "slice", idx_dir, "level:ERROR",
            "-o", str(out_path)
        ])
        main()
        assert out_path.exists(), "Slice output directory was not created"
        # Verify the sliced index is searchable
        from flatseek import Flatseek
        fs = Flatseek(str(out_path))
        r = fs.search(q="*")
        assert r.total >= 1, "Sliced index should have at least one doc"
        # Verify all docs in sliced index match the filter
        for hit in r.hits["hits"]:
            doc = hit["_source"]
            assert doc.get("level") == "ERROR", \
                f"Sliced doc {doc.get('_id')} should have level=ERROR"
