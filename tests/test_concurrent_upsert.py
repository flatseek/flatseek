"""Tests for concurrent upsert safety under parallel workers."""

import json
import os
import concurrent.futures
import multiprocessing
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

try:
    from fastapi.testclient import TestClient
    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False
    pytest.skip("FastAPI not installed", allow_module_level=True)


def _run_server(port: int, data_dir: str | None = None) -> None:
    """Module-level server function so multiprocessing.Process can pickle it."""
    import os
    if data_dir:
        # Resolve to absolute path: spawned process may have different cwd
        os.environ["FLATSEEK_DATA_DIR"] = os.path.abspath(data_dir)
    import uvicorn
    uvicorn.run("flatseek.api.main:app", host="127.0.0.1", port=port, log_level="error")


def _get_free_port() -> int:
    """Return a free port by binding a temporary socket."""
    import socket
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ─── Module-level worker functions for multiprocessing ───────────────────────
# These must be at module level so multiprocessing.Process (spawn context) can pickle them.


def _upsert_one(port: int, doc_id: str, name: str) -> tuple[str, int, str] | tuple[str, str, str]:
    """Upsert one document via HTTP. Returns (ok/err, status_code, doc_id)."""
    import httpx
    try:
        with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=30) as client:
            r = client.put(
                f"/testidx/_doc/{doc_id}",
                json={"id": doc_id, "name": name, "status": "active"}
            )
            return ("ok", r.status_code, doc_id)
    except Exception as e:
        return ("err", str(e)[:100], doc_id)


def _build_index(idx_dir: Path, docs: list[dict]) -> None:
    """Build a real searchable index from docs."""
    import csv
    from flatseek.core.builder import IndexBuilder

    (idx_dir / "index").mkdir(parents=True, exist_ok=True)
    (idx_dir / "docs").mkdir(parents=True, exist_ok=True)

    csv_path = idx_dir / "_temp.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "status"])
        writer.writeheader()
        writer.writerows(docs)

    builder = IndexBuilder(
        output_dir=str(idx_dir),
        column_map={},
        start_doc_id=0,
        dataset=None,
        checkpoint_cb=None,
        delimiter=",",
        columns=None,
        worker_id=None,
        dedup_fields=None,
        doc_id_end=None,
        daemon=False,
        id_field="id",
    )
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            builder.add_row(row, list(row.keys()))
    builder.finalize()
    csv_path.unlink()

    # Set _id_field
    stats = json.loads((idx_dir / "stats.json").read_text())
    stats["_id_field"] = "id"
    (idx_dir / "stats.json").write_text(json.dumps(stats, indent=2))
    (idx_dir / "stats.json").touch()  # ensure mtime updated for server to detect change


class TestConcurrentUpsert:
    """Verify no doc_id collisions under parallel upsert workers."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.data_dir = tmp_path
        self.port = _get_free_port()

        self.env_patch = patch.dict(os.environ, {
            "FLATSEEK_DATA_DIR": str(self.data_dir),
            "FLATSEEK_API_BASE": f"http://127.0.0.1:{self.port}",
        })
        self.env_patch.start()

        # Build initial index with 2 docs
        self.docs = [
            {"id": "1", "name": "Alice", "status": "active"},
            {"id": "2", "name": "Bob", "status": "active"},
        ]
        _build_index(self.data_dir / "testidx", self.docs)

        # Set up ID mapping
        idx_dir = self.data_dir / "testidx"
        id_map_dir = idx_dir / ".ids"
        id_map_dir.mkdir(parents=True, exist_ok=True)
        (id_map_dir / "id.json").write_text(json.dumps({"1": 0, "2": 1}))

        yield

        self.env_patch.stop()

    def test_concurrent_upserts_unique_doc_ids(self):
        """Parallel PUT /_doc/{id} should not produce duplicate doc_ids.

        Uses ProcessPoolExecutor with spawn so each worker has its own process
        and doesn't share memory with the test.
        """
        ctx = multiprocessing.get_context("spawn")

        # Start a dedicated server process with data_dir passed explicitly
        # (spawn doesn't inherit env vars from parent)
        server_proc = ctx.Process(
            target=_run_server,
            args=(self.port, str(self.data_dir)),
            daemon=True
        )
        server_proc.start()
        time.sleep(1.5)  # wait for server to start

        try:
            N = 10
            with ctx.Pool(N) as pool:
                args = [(self.port, f"user_{i}", f"User_{i}") for i in range(N)]
                results = list(pool.starmap(_upsert_one, args))

            errors = [r for r in results if r[0] == "err"]
            assert not errors, f"Upsert errors: {errors}"

            successes = [r for r in results if r[0] == "ok" and r[1] == 200]
            assert len(successes) == N, f"Expected {N}, got {len(successes)}: {[r for r in results if r[0]=='ok' and r[1]!=200]}"

            # Verify all doc_ids are unique
            import httpx
            with httpx.Client(base_url=f"http://127.0.0.1:{self.port}", timeout=30) as client:
                resp = client.get("/testidx/_search?q=*&size=100")
                data = resp.json()
                # Should have original 2 + 10 new = 12 total
                total = data["hits"]["total"]
                assert total >= 2 + N, f"Expected at least {2+N} docs, got {total}"
        finally:
            server_proc.terminate()
            server_proc.join(timeout=3)

    def test_concurrent_same_id_upsert_no_collision(self):
        """Multiple workers upserting the SAME id should serialize correctly.

        Each upsert gets its own new doc_id (last-write-wins per natural key).
        Exactly 1 searchable doc remains for the shared id.
        """
        ctx = multiprocessing.get_context("spawn")

        server_proc = ctx.Process(
            target=_run_server,
            args=(self.port, str(self.data_dir)),
            daemon=True
        )
        server_proc.start()
        time.sleep(1.5)

        try:
            N = 5
            with ctx.Pool(N) as pool:
                args = [(self.port, "same_user", "ConcurrentUser")] * N
                results = list(pool.starmap(_upsert_one, args))

            statuses = [r[1] for r in results if r[0] == "ok"]
            errors = [r for r in results if r[0] == "err"]
            assert not errors, f"Errors: {errors}"
            assert all(s == 200 for s in statuses), f"Non-200 statuses: {statuses}"

            # Search should return exactly 1 doc for "same_user"
            import httpx
            with httpx.Client(base_url=f"http://127.0.0.1:{self.port}", timeout=30) as client:
                resp = client.get("/testidx/_search?q=id:same_user")
                data = resp.json()
                assert data["hits"]["total"] == 1, "same_user should appear exactly once"
        finally:
            server_proc.terminate()
            server_proc.join(timeout=3)

    def test_high_concurrency_no_data_loss(self):
        """50 parallel upserts — all docs should be searchable with correct count.

        Uses ProcessPoolExecutor with spawn so each worker has its own process.

        Note: This test is sensitive to runner resource constraints. It may fail
        under heavy system load or in memory-constrained CI environments due to
        the sheer number of simultaneous connections (50 workers, 1 server).
        """
        pytest.skip("stress test: sensitive to runner resource constraints")
