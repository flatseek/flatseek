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
        import os
        orig_data_dir = os.environ.get("FLATSEEK_DATA_DIR")
        try:
            with IndexContext(n_rows=20) as idx_dir:
                from fastapi.testclient import TestClient
                from flatseek.api.main import app

                os.environ["FLATSEEK_DATA_DIR"] = idx_dir
                client = TestClient(app, raise_server_exceptions=True)

                idx_name = "test_create_idx"
                r = client.put(f"/{idx_name}")
                assert r.status_code == 200, f"Expected 200, got {r.status_code}: {r.json()}"
                data = r.json()
                assert data["status"] in ("created", "exists")
                assert data["_index"] == idx_name
        finally:
            if orig_data_dir is None:
                os.environ.pop("FLATSEEK_DATA_DIR", None)
            else:
                os.environ["FLATSEEK_DATA_DIR"] = orig_data_dir

    def test_create_twice_returns_exists(self):
        """Second PUT on same index returns status=exists."""
        from fastapi.testclient import TestClient
        from flatseek.api.main import app
        from flatseek.api.deps import _index_manager
        import os, tempfile, shutil

        orig_data_dir = os.environ.get("FLATSEEK_DATA_DIR")
        tmp = tempfile.mkdtemp(prefix="fs_api_")
        try:
            os.environ["FLATSEEK_DATA_DIR"] = tmp
            # Reset global manager so it picks up the new data_dir
            if _index_manager is not None:
                _index_manager._engines.clear()
            # Force recreation of the global manager with new data_dir
            import flatseek.api.deps as deps
            deps._index_manager = None

            client = TestClient(app, raise_server_exceptions=True)

            idx_name = "test_twice_idx"
            r1 = client.put(f"/{idx_name}")
            assert r1.status_code == 200
            assert r1.json()["status"] == "created"

            r2 = client.put(f"/{idx_name}")
            assert r2.status_code == 200
            assert r2.json()["status"] == "exists"
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
            if orig_data_dir is None:
                os.environ.pop("FLATSEEK_DATA_DIR", None)
            else:
                os.environ["FLATSEEK_DATA_DIR"] = orig_data_dir
            # Reset global manager again so subsequent tests use clean state
            import flatseek.api.deps as deps
            deps._index_manager = None

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


# ─── Write Ops via API ───────────────────────────────────────────────────────

class TestWriteOpsAPI:
    """Test insert/upsert/update/delete/bulk via REST API.

    Uses a temp directory with a pre-built index (built with --id-field).
    FLATSEEK_DATA_DIR is set to the parent of the index so the API can find it.
    """

    @pytest.fixture
    def api_client_with_index(self, tmp_path):
        """Build an index with --id-field and point API at it."""
        import csv, os
        from flatseek.core.builder import build
        from fastapi.testclient import TestClient
        from flatseek.api.main import app

        # Build index with --id-field
        idx_dir = tmp_path / "api_test_idx"
        os.makedirs(str(idx_dir))
        csv_path = idx_dir / "data.csv"
        with open(str(csv_path), "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["id", "name", "email", "status"])
            w.writeheader()
            w.writerow({"id": "1", "name": "Alice", "email": "alice@test.com", "status": "inactive"})
            w.writerow({"id": "2", "name": "Bob", "email": "bob@test.com", "status": "inactive"})
        build(str(csv_path), str(idx_dir), id_field="id")

        # Point API at the parent dir so it finds api_test_idx
        old_env = os.environ.get("FLATSEEK_DATA_DIR")
        os.environ["FLATSEEK_DATA_DIR"] = str(tmp_path)
        import flatseek.api.deps as deps
        deps._index_manager = None  # reset so each test gets a fresh manager
        client = TestClient(app, raise_server_exceptions=True)
        yield client, "api_test_idx"
        if old_env is not None:
            os.environ["FLATSEEK_DATA_DIR"] = old_env
        else:
            os.environ.pop("FLATSEEK_DATA_DIR", None)
        deps._index_manager = None  # clear stale references before next test
        shutil.rmtree(idx_dir, ignore_errors=True)

    def test_insert_via_bulk(self, api_client_with_index):
        """POST /{index}/_bulk_documents with create action."""
        client, idx = api_client_with_index
        r = client.post(
            f"/{idx}/_bulk_documents",
            content=b'{"create": {"_id": "100"}}\n{"id": "100", "name": "Inserted", "email": "ins@test.com"}',
        )
        assert r.status_code == 200
        data = r.json()
        assert data.get("total_created", 0) >= 1

    def test_upsert_via_bulk(self, api_client_with_index):
        """POST /{index}/_bulk_documents with index action (upsert)."""
        client, idx = api_client_with_index
        # Insert first
        client.post(
            f"/{idx}/_bulk_documents",
            content=b'{"index": {"_id": "200"}}\n{"id": "200", "name": "Original", "email": "orig@test.com"}',
        )
        # Upsert
        r = client.post(
            f"/{idx}/_bulk_documents",
            content=b'{"index": {"_id": "200"}}\n{"id": "200", "name": "Updated", "email": "upd@test.com"}',
        )
        assert r.status_code == 200

    def test_delete_by_id_via_api(self, api_client_with_index):
        """DELETE /{index}/_doc/{id} removes the document."""
        client, idx = api_client_with_index
        # Insert first
        client.post(
            f"/{idx}/_bulk_documents",
            content=b'{"create": {"_id": "300"}}\n{"id": "300", "name": "ToDelete", "email": "del@test.com"}',
        )
        # Count before delete
        r1 = client.get(f"/{idx}/_count")
        before = r1.json().get("count", 0)
        # Delete
        r2 = client.delete(f"/{idx}/_doc/300")
        assert r2.status_code == 200
        data = r2.json()
        assert data.get("result") == "deleted"

    def test_update_by_query_api(self, api_client_with_index):
        """PUT /{index}/_doc?q=... updates matching docs."""
        client, idx = api_client_with_index
        # Insert some docs
        for i in range(401, 406):
            client.post(
                f"/{idx}/_bulk_documents",
                content=f'{{"create": {{"_id": "{i}"}}}}\n{{"id": "{i}", "name": "Updatable", "email": "up{i}@test.com"}}'.encode(),
            )
        # Use OR syntax (comma-separated = AND in flatseek)
        r = client.put(
            f"/{idx}/_doc",
            params={"q": "id:401 OR id:402 OR id:403"},
            json={"status": "updated"},
        )
        assert r.status_code == 200
        data = r.json()
        assert data.get("updated", 0) >= 1

    def test_delete_by_query_api(self, api_client_with_index):
        """DELETE /{index}/_doc?q=... removes matching docs."""
        client, idx = api_client_with_index
        # Insert some docs
        for i in range(501, 506):
            client.post(
                f"/{idx}/_bulk_documents",
                content=f'{{"create": {{"_id": "{i}"}}}}\n{{"id": "{i}", "name": "ToDeleteQ", "email": "dq{i}@test.com"}}'.encode(),
            )
        r = client.delete(f"/{idx}/_doc", params={"q": "id:501 OR id:502"})
        assert r.status_code == 200
        data = r.json()
        assert data.get("deleted", 0) >= 1

    # ─── Update (partial merge) ─────────────────────────────────────────────

    def test_put_upsert_new_doc_returns_created(self, api_client_with_index):
        """PUT /{index}/_doc/{id} on new ID returns result=created."""
        client, idx = api_client_with_index
        r = client.put(
            f"/{idx}/_doc/upsert_new_1",
            json={"id": "upsert_new_1", "name": "NewViaPut", "email": "newput@t.com"},
        )
        assert r.status_code == 200
        data = r.json()
        assert data.get("result") == "created"

    def test_put_upsert_existing_returns_updated(self, api_client_with_index):
        """PUT /{index}/_doc/{id} on existing ID returns result=updated."""
        client, idx = api_client_with_index
        # Create first
        client.put(
            f"/{idx}/_doc/upsert_exist_1",
            json={"id": "upsert_exist_1", "name": "Original", "email": "orig@t.com"},
        )
        # Replace
        r = client.put(
            f"/{idx}/_doc/upsert_exist_1",
            json={"id": "upsert_exist_1", "name": "Replaced", "email": "repl@t.com"},
        )
        assert r.status_code == 200
        data = r.json()
        assert data.get("result") == "updated"

    def test_put_upsert_full_replacement(self, api_client_with_index):
        """PUT /{index}/_doc/{id} is a full replacement — old fields are gone."""
        client, idx = api_client_with_index
        client.put(
            f"/{idx}/_doc/replace_1",
            json={"id": "replace_1", "name": "Old", "email": "old@t.com", "status": "inactive"},
        )
        # Replace with fewer fields
        client.put(
            f"/{idx}/_doc/replace_1",
            json={"id": "replace_1", "name": "NewOnly"},
        )
        r = client.get(f"/{idx}/_search?q=id:replace_1")
        hits = r.json().get("hits", {}).get("hits", [])
        assert len(hits) >= 1
        doc = hits[0].get("_source", {})
        assert doc.get("name") == "NewOnly"
        assert "email" not in doc or doc.get("email") is None

    def test_bulk_update_partial_merge(self, api_client_with_index):
        """POST /{index}/_bulk_documents with update action performs partial merge."""
        client, idx = api_client_with_index
        # Insert full doc
        client.put(
            f"/{idx}/_doc/bulkupd_1",
            json={"id": "bulkupd_1", "name": "FullDoc", "email": "full@t.com", "status": "inactive"},
        )
        # Partial update via bulk
        r = client.post(
            f"/{idx}/_bulk_documents",
            content=b'{"update": {"_id": "bulkupd_1"}}\n{"doc": {"status": "active"}}',
        )
        assert r.status_code == 200
        data = r.json()
        assert data.get("total_updated", 0) >= 1
        # Verify field merge: name and email preserved
        r2 = client.get(f"/{idx}/_search?q=id:bulkupd_1")
        doc = r2.json().get("hits", {}).get("hits", [{}])[0].get("_source", {})
        assert doc.get("status") == "active"
        assert doc.get("name") == "FullDoc", "name should be preserved after partial update"
        assert doc.get("email") == "full@t.com", "email should be preserved after partial update"

    def test_bulk_update_not_found_returns_error(self, api_client_with_index):
        """POST /{index}/_bulk_documents update on missing ID returns 404 result."""
        client, idx = api_client_with_index
        r = client.post(
            f"/{idx}/_bulk_documents",
            content=b'{"update": {"_id": "DOES_NOT_EXIST_XYZ"}}\n{"doc": {"status": "active"}}',
        )
        assert r.status_code == 200
        data = r.json()
        assert data.get("errors") is True
        assert any(
            res.get("result") == "not_found" for res in data.get("results", [])
        )

    def test_update_by_query_partial_merge(self, api_client_with_index):
        """PUT /{index}/_doc?q=... with set fields performs partial merge."""
        client, idx = api_client_with_index
        # Insert a doc with multiple fields via upsert
        client.put(
            f"/{idx}/_doc/ubq_1",
            json={"id": "ubq_1", "name": "MergeTest", "email": "merge@t.com", "status": "inactive", "age": "30"},
        )
        # Partial update via update_by_query
        r = client.put(
            f"/{idx}/_doc",
            params={"q": "id:ubq_1"},
            json={"status": "active"},
        )
        assert r.status_code == 200
        data = r.json()
        assert data.get("updated", 0) >= 1
        # Verify other fields preserved
        r2 = client.get(f"/{idx}/_search?q=id:ubq_1")
        doc = r2.json().get("hits", {}).get("hits", [{}])[0].get("_source", {})
        assert doc.get("name") == "MergeTest"
        assert doc.get("email") == "merge@t.com"
        assert doc.get("status") == "active"
        assert doc.get("age") == "30"

