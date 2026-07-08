"""Tests for the Elasticsearch-compatible _bulk API."""

import csv
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

try:
    from fastapi.testclient import TestClient
    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False
    pytest.skip("FastAPI not installed", allow_module_level=True)


def _ndjson(*pairs):
    """Encode action+doc pairs as NDJSON string (Elasticsearch bulk format).

    Example: _ndjson({"index": {"_id": "1"}}, {"email": "alice@example.com", "name": "Alice"})
    → two lines of NDJSON
    """
    lines = []
    for item in pairs:
        lines.append(json.dumps(item))
    return "\n".join(lines) + "\n"


def _create_index_with_docs(data_dir: Path, unique_subdir: str, name: str, docs: list[dict]) -> Path:
    """Create a properly indexed, searchable index from docs using IndexBuilder."""
    idx_dir = data_dir / unique_subdir / name
    idx_dir.mkdir(parents=True, exist_ok=True)
    (idx_dir / "index").mkdir()
    (idx_dir / "docs").mkdir()

    csv_path = idx_dir / "_temp.csv"
    fieldnames = list(docs[0].keys()) if docs else ["id", "name", "status"]
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(docs)

    from flatseek.core.builder import IndexBuilder
    from flatseek.core.tombstone import TombstoneStore

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
        for i, row in enumerate(reader):
            builder.add_row(row, list(row.keys()))
    builder.finalize()
    csv_path.unlink()

    # Create the ID → doc_id mapping so bulk delete/update can resolve _id
    tombstore = TombstoneStore(str(idx_dir))
    for i, doc in enumerate(docs):
        tombstore.set_doc_id_for_field("id", doc["id"], i)

    stats = json.loads((idx_dir / "stats.json").read_text())
    stats["_id_field"] = "id"
    (idx_dir / "stats.json").write_text(json.dumps(stats))

    return idx_dir


class TestBulk:
    """Tests for POST /{index}/_bulk (Elasticsearch-compatible)."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.data_dir = tmp_path
        import uuid
        self.unique_subdir = f"idx_{uuid.uuid4().hex[:8]}"
        self.index_name = f"testidx_{uuid.uuid4().hex[:8]}"

        # Reset IndexManager singleton so each test gets fresh state.
        # This is critical because the manager caches data_dir from the first
        # request and reuses it for all subsequent requests.
        import flatseek.api.deps as deps
        deps._index_manager = None

        self.env_patch = patch.dict(os.environ, {
            "FLATSEEK_DATA_DIR": str(self.data_dir / self.unique_subdir),
            "FLATSEEK_API_BASE": "http://localhost:8000",
        })
        self.env_patch.start()

        # Ensure the unique subdir exists so the index can be created there
        (self.data_dir / self.unique_subdir).mkdir(parents=True, exist_ok=True)

        self.docs = [
            {"id": "1", "name": "Alice",   "email": "alice@example.com",   "status": "active"},
            {"id": "2", "name": "Bob",     "email": "bob@example.com",     "status": "inactive"},
            {"id": "3", "name": "Carol",   "email": "carol@example.com",   "status": "active"},
            {"id": "4", "name": "Dave",    "email": "dave@example.com",    "status": "inactive"},
            {"id": "5", "name": "Eve",     "email": "eve@example.com",     "status": "active"},
        ]
        _create_index_with_docs(self.data_dir, self.unique_subdir, self.index_name, self.docs)

        from flatseek.api.main import app
        self.client = TestClient(app, raise_server_exceptions=False)

        yield

        self.env_patch.stop()
        # Reset singleton so next test gets a fresh manager
        import flatseek.api.deps as deps
        deps._index_manager = None

    # ── index action ─────────────────────────────────────────────────────────

    def test_index_single_doc(self):
        """index action inserts a new document."""
        body = _ndjson(
            {"index": {"_id": "6"}},
            {"id": "6", "name": "Frank", "email": "frank@example.com", "status": "active"},
        )
        response = self.client.post(f"/{self.index_name}/_bulk_documents", content=body)
        assert response.status_code == 200
        data = response.json()
        assert data["errors"] is False
        assert len(data["results"]) == 1
        assert data["results"][0]["operation"] == "index"
        assert data["results"][0]["status"] == 200  # index returns 200, only create returns 201
        assert data["results"][0]["result"] == "created"
        assert data["total_created"] == 1

    def test_index_replaces_existing(self):
        """index action on existing _id replaces the document (upsert)."""
        body = _ndjson(
            {"index": {"_id": "1"}},
            {"id": "1", "name": "Alice-Updated", "email": "alice@example.com", "status": "inactive"},
        )
        response = self.client.post(f"/{self.index_name}/_bulk_documents", content=body)
        assert response.status_code == 200
        data = response.json()
        assert data["errors"] is False
        assert data["results"][0]["operation"] == "index"
        assert data["results"][0]["status"] == 200
        assert data["results"][0]["result"] == "updated"
        assert data["total_updated"] == 1

    # ── create action ─────────────────────────────────────────────────────────

    def test_create_new_doc(self):
        """create action inserts a new document."""
        body = _ndjson(
            {"create": {"_id": "6"}},
            {"id": "6", "name": "Frank", "email": "frank@example.com", "status": "active"},
        )
        response = self.client.post(f"/{self.index_name}/_bulk_documents", content=body)
        assert response.status_code == 200
        data = response.json()
        assert data["errors"] is False
        assert data["results"][0]["result"] == "created"
        assert data["total_created"] == 1

    def test_create_existing_returns_409(self):
        """create action returns 409 if document already exists."""
        body = _ndjson(
            {"create": {"_id": "1"}},
            {"id": "1", "name": "Duplicate", "email": "dup@example.com", "status": "active"},
        )
        response = self.client.post(f"/{self.index_name}/_bulk_documents", content=body)
        assert response.status_code == 200
        data = response.json()
        assert data["errors"] is True
        assert data["results"][0]["status"] == 409
        assert data["results"][0]["result"] == "conflict"

    # ── delete action ─────────────────────────────────────────────────────────

    def test_delete_existing_doc(self):
        """delete action removes a document by _id."""
        body = _ndjson({"delete": {"_id": "1"}})
        response = self.client.post(f"/{self.index_name}/_bulk_documents", content=body)
        assert response.status_code == 200
        data = response.json()
        assert data["errors"] is False
        assert data["results"][0]["operation"] == "delete"
        assert data["results"][0]["status"] == 200
        assert data["results"][0]["result"] == "deleted"
        assert data["total_deleted"] == 1

    def test_delete_missing_returns_404(self):
        """delete action returns 404 if document does not exist."""
        body = _ndjson({"delete": {"_id": "nonexistent"}})
        response = self.client.post(f"/{self.index_name}/_bulk_documents", content=body)
        assert response.status_code == 200
        data = response.json()
        assert data["errors"] is True
        assert data["results"][0]["status"] == 404
        assert data["results"][0]["result"] == "not_found"

    # ── update action ─────────────────────────────────────────────────────────

    def test_update_partial_merge(self):
        """update action merges fields into existing document."""
        body = _ndjson(
            {"update": {"_id": "1"}},
            {"doc": {"status": "verified"}},
        )
        response = self.client.post(f"/{self.index_name}/_bulk_documents", content=body)
        assert response.status_code == 200
        data = response.json()
        assert data["errors"] is False
        assert data["results"][0]["operation"] == "update"
        assert data["results"][0]["status"] == 200
        assert data["results"][0]["result"] == "updated"
        assert data["total_updated"] == 1

    def test_update_missing_returns_404(self):
        """update action returns 404 if document does not exist."""
        body = _ndjson(
            {"update": {"_id": "nonexistent"}},
            {"doc": {"status": "verified"}},
        )
        response = self.client.post(f"/{self.index_name}/_bulk_documents", content=body)
        assert response.status_code == 200
        data = response.json()
        assert data["errors"] is True
        assert data["results"][0]["status"] == 404
        assert data["results"][0]["result"] == "not_found"

    # ── mixed operations ──────────────────────────────────────────────────────

    def test_mixed_operations(self):
        """Multiple operations of different types in one request."""
        # 4 operations: delete existing doc, update existing doc, create new doc (id=6 is new in this test), create new doc (id=7 is new)
        # Frank (id=6) is new — index returns "created"
        body = "\n".join([
            json.dumps({"delete": {"_id": "3"}}),                         # delete Carol
            json.dumps({"update": {"_id": "2"}}),                         # update Bob → merged doc
            json.dumps({"doc": {"status": "verified"}}),
            json.dumps({"index": {"_id": "6"}}),                          # create Frank (new doc in fresh test)
            json.dumps({"id": "6", "name": "Frank", "email": "frank@example.com", "status": "active"}),
            json.dumps({"create": {"_id": "7"}}),                         # create Eve (new doc)
            json.dumps({"id": "7", "name": "Eve", "email": "eve2@example.com", "status": "active"}),
        ]) + "\n"

        response = self.client.post(f"/{self.index_name}/_bulk_documents", content=body)
        assert response.status_code == 200
        data = response.json()
        # 7 lines = 4 operations (delete, update+doc, index+doc, create+doc)
        assert len(data["results"]) == 4
        assert data["total_deleted"] == 1
        assert data["total_created"] == 2  # index Frank + create Eve (both new in fresh test)
        assert data["total_updated"] == 1  # update Bob

    def test_empty_request(self):
        """Empty NDJSON body returns zero results."""
        response = self.client.post(f"/{self.index_name}/_bulk_documents", content="\n")
        assert response.status_code == 200
        data = response.json()
        assert data["results"] == []
        assert data["took"] == 0

    def test_invalid_json_returns_400(self):
        """Malformed JSON in NDJSON body returns 400."""
        response = self.client.post(
            f"/{self.index_name}/_bulk_documents",
            content=b'{"index": {"_id": "1"}}\nnot json at all\n',
        )
        assert response.status_code == 400

    def test_index_action_upserts_all_fields(self):
        """index action (upsert) replaces entire document — old fields disappear."""
        # Index doc with only "name" field — should NOT preserve "email" or "status"
        body = _ndjson(
            {"index": {"_id": "1"}},
            {"id": "1", "name": "Alice-Only"},
        )
        response = self.client.post(f"/{self.index_name}/_bulk_documents", content=body)
        assert response.status_code == 200

        # Verify the doc was replaced (email field gone)
        resp = self.client.get(f"/{self.index_name}/_search?q=id:1")
        hits = resp.json()["hits"]["hits"]
        assert len(hits) == 1
        source = hits[0]["_source"]
        assert source.get("name") == "Alice-Only"
        # email was in original doc, should NOT appear after full replace
        assert "email" not in source
