"""Tests for all Flatseek API endpoints."""

import os
import sys
import pytest
import tempfile
import shutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from flatseek.test.fixtures import IndexContext, generate_sample_events_csv, cleanup_index


# ─── Root & Cluster ──────────────────────────────────────────────────────────

class TestRoot:
    def test_root_returns_name_and_version(self):
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        client = TestClient(app, raise_server_exceptions=False)
        r = client.get("/")
        assert r.status_code == 200
        data = r.json()
        assert data["name"] == "Flatseek API"

    def test_cluster_health_returns_indices_count(self):
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        client = TestClient(app, raise_server_exceptions=False)
        r = client.get("/_cluster/health")
        assert r.status_code == 200
        data = r.json()
        assert "status" in data
        assert "number_of_indices" in data


class TestIndicesList:
    def test_indices_returns_empty_when_no_data(self):
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        client = TestClient(app, raise_server_exceptions=False)
        r = client.get("/_indices")
        assert r.status_code == 200
        data = r.json()
        assert "indices" in data


# ─── Index Lifecycle ───────────────────────────────────────────────────────────

class TestIndexCreate:
    def test_create_and_delete_index(self):
        with IndexContext(n_rows=20) as idx_dir:
            from fastapi.testclient import TestClient
            from flatseek.api.main import app
            import os

            os.environ["FLATSEEK_DATA_DIR"] = idx_dir
            client = TestClient(app, raise_server_exceptions=True)

            idx_name = "test_create_idx"
            r = client.put(f"/{idx_name}")
            assert r.status_code == 200, f"Expected 200, got {r.status_code}: {r.json()}"
            data = r.json()
            assert data["status"] in ("created", "exists")
            assert data["_index"] == idx_name

    def test_create_twice_returns_exists(self):
        """Second PUT on same index returns status=exists."""
        from fastapi.testclient import TestClient
        from flatseek.api.main import app
        from flatseek.api.deps import _index_manager
        import os, tempfile, shutil

        tmp = tempfile.mkdtemp(prefix="fs_api_")
        os.environ["FLATSEEK_DATA_DIR"] = tmp
        # Clear the global manager cache so it picks up the new data_dir
        if _index_manager is not None:
            _index_manager._engines.clear()
            _index_manager._index_passwords.clear()
        client = TestClient(app, raise_server_exceptions=True)

        idx_name = "test_twice_idx"
        r1 = client.put(f"/{idx_name}")
        assert r1.status_code == 200
        assert r1.json()["status"] == "created"

        r2 = client.put(f"/{idx_name}")
        assert r2.status_code == 200
        assert r2.json()["status"] == "exists"

        shutil.rmtree(tmp, ignore_errors=True)

    def test_delete_nonexistent_returns_404(self):
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        client = TestClient(app, raise_server_exceptions=False)
        r = client.delete("/this_index_does_not_exist_12345")
        assert r.status_code == 404


# ─── Mapping ───────────────────────────────────────────────────────────────────

class TestMapping:
    def test_get_mapping_returns_404_for_unknown_index(self):
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        client = TestClient(app, raise_server_exceptions=False)
        r = client.get("/nonexistent_idx_xyz/_mapping")
        assert r.status_code == 404


# ─── Search ───────────────────────────────────────────────────────────────────

class TestSearch:
    def test_get_search_returns_404_for_unknown_index(self):
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        client = TestClient(app, raise_server_exceptions=True)
        r = client.get("/nonexistent_idx_xyz/_search?q=*")
        assert r.status_code == 404

    def test_post_search_returns_404_for_unknown_index(self):
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        client = TestClient(app, raise_server_exceptions=True)
        r = client.post("/nonexistent_idx_xyz/_search", json={"query": "*"})
        assert r.status_code == 404


# ─── Aggregations ─────────────────────────────────────────────────────────────

class TestAggregate:
    def test_post_aggregate_returns_404_for_unknown_index(self):
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        client = TestClient(app, raise_server_exceptions=False)
        r = client.post("/nonexistent_idx_xyz/_aggregate", json={"query": "*", "aggs": {}})
        assert r.status_code == 404

    def test_get_aggregate_returns_404_for_unknown_index(self):
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        client = TestClient(app, raise_server_exceptions=False)
        r = client.get("/nonexistent_idx_xyz/_aggregate?q=*")
        assert r.status_code == 404


# ─── Count ───────────────────────────────────────────────────────────────────

class TestCount:
    def test_get_count_returns_404_for_unknown_index(self):
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        client = TestClient(app, raise_server_exceptions=False)
        r = client.get("/nonexistent_idx_xyz/_count?q=*")
        assert r.status_code == 404


# ─── Bulk Index ───────────────────────────────────────────────────────────────

class TestBulkIndex:
    def test_bulk_index_nonexistent_index_creates_it(self):
        """Bulk to nonexistent index returns 200 (index auto-created)."""
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        client = TestClient(app, raise_server_exceptions=True)
        r = client.post(
            "/nonexistent_bulk_test_xyz/_bulk",
            json=[{"id": "1", "doc": {"name": "test"}}],
        )
        assert r.status_code == 200
        data = r.json()
        assert "indexed" in data
        assert data["indexed"] >= 1

    def test_bulk_index_response_has_indexed_field(self):
        """Bulk response contains the indexed count."""
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        client = TestClient(app, raise_server_exceptions=True)
        r = client.post(
            "/bulk_field_test_xyz/_bulk",
            json=[
                {"id": "1", "doc": {"program": "raydium", "fee": 5000}},
                {"id": "2", "doc": {"program": "jupiter", "fee": 5000}},
            ],
        )
        assert r.status_code == 200
        data = r.json()
        assert "indexed" in data
        assert "errors" in data


# ─── Upload Progress ──────────────────────────────────────────────────────────

class TestUploadProgress:
    def test_get_upload_progress_returns_for_unknown_index(self):
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        client = TestClient(app, raise_server_exceptions=False)
        r = client.get("/nonexistent_idx_xyz/_upload_progress")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, dict)


# ─── Flush ───────────────────────────────────────────────────────────────────

class TestFlush:
    def test_flush_nonexistent_index_returns_404(self):
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        client = TestClient(app, raise_server_exceptions=False)
        r = client.post("/nonexistent_flush_xyz/_flush?wait=true")
        assert r.status_code == 404


# ─── Logs ────────────────────────────────────────────────────────────────────────

class TestLogs:
    def test_get_logs_returns_for_unknown_index(self):
        """Logs endpoint returns 200 even for unknown index (empty logs)."""
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        client = TestClient(app, raise_server_exceptions=False)
        r = client.get("/nonexistent_logs_xyz/_logs")
        assert r.status_code == 200


# ─── Stats ────────────────────────────────────────────────────────────────────

class TestStats:
    def test_stats_returns_404_for_unknown_index(self):
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        client = TestClient(app, raise_server_exceptions=False)
        r = client.get("/nonexistent_stats_xyz/_stats")
        assert r.status_code == 404


# ─── Rename ───────────────────────────────────────────────────────────────────

class TestRename:
    def test_rename_unknown_index_returns_404(self):
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        client = TestClient(app, raise_server_exceptions=False)
        r = client.post("/nonexistent_rename_xyz/_rename", json={"new_name": "renamed"})
        assert r.status_code == 404


# ─── Encrypt / Decrypt ───────────────────────────────────────────────────────

class TestEncrypt:
    def test_encrypt_nonexistent_index_returns_404(self):
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        client = TestClient(app, raise_server_exceptions=False)
        r = client.post("/nonexistent_idx_xyz/_encrypt", json={"passphrase": "secret"})
        assert r.status_code == 404

    def test_decrypt_nonexistent_index_returns_404(self):
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        client = TestClient(app, raise_server_exceptions=False)
        r = client.post("/nonexistent_idx_xyz/_decrypt", json={"passphrase": "secret"})
        assert r.status_code == 404

    def test_encrypt_progress_nonexistent_returns_404(self):
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        client = TestClient(app, raise_server_exceptions=False)
        r = client.get("/nonexistent_idx_xyz/_encrypt_progress?job_id=none")
        assert r.status_code == 404


# ─── Authenticate ──────────────────────────────────────────────────────────────

class TestAuth:
    def test_is_encrypted_nonexistent_returns_404(self):
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        client = TestClient(app, raise_server_exceptions=False)
        r = client.get("/nonexistent_idx_xyz/_is_encrypted")
        assert r.status_code == 404

    def test_authenticate_nonexistent_index_returns_404(self):
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        client = TestClient(app, raise_server_exceptions=False)
        r = client.post("/nonexistent_idx_xyz/_authenticate", json={"passphrase": "secret"})
        assert r.status_code == 404

    def test_logout_unknown_index_returns_200(self):
        """Logout returns 200 even for unknown index (no-op)."""
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        client = TestClient(app, raise_server_exceptions=False)
        r = client.delete("/nonexistent_logout_xyz/_authenticate")
        assert r.status_code == 200


# ─── Validate ─────────────────────────────────────────────────────────────────

class TestValidate:
    def test_validate_nonexistent_index_returns_404(self):
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        client = TestClient(app, raise_server_exceptions=False)
        r = client.post("/nonexistent_validate_xyz/_validate", json={"query": "*"})
        assert r.status_code == 404


# ─── Delete By Query ──────────────────────────────────────────────────────────

class TestDeleteByQuery:
    def test_delete_by_query_returns_404_for_nonexistent(self):
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        client = TestClient(app, raise_server_exceptions=False)
        r = client.post("/nonexistent_idx_xyz/_delete_by_query", json={"query": "*"})
        assert r.status_code == 404
