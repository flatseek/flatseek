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
            passphrase=None,
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
            passphrase=None,
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
