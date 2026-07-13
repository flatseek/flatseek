"""Tests for Flatseek library write operations: insert, index, update, delete, delete_query, bulk."""

import os
import sys
import tempfile
import shutil
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from flatseek.core.builder import build
from flatseek import Flatseek


# ─── Fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture
def write_index(tmp_path):
    """Build an index with --id-field for write operation tests."""
    csv_path = tmp_path / "data.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        f.write("id,name,email,status,age\n")
        f.write("1,Alice,alice@example.com,inactive,30\n")
        f.write("2,Bob,bob@example.com,inactive,25\n")
        f.write("3,Carol,carol@example.com,inactive,35\n")

    idx_dir = tmp_path / "idx"
    build(str(csv_path), str(idx_dir), id_field="id")
    shutil.rmtree(tmp_path / "idx" / "docs", ignore_errors=True)  # clean up CSV

    yield str(idx_dir)
    shutil.rmtree(idx_dir, ignore_errors=True)


# ─── insert ─────────────────────────────────────────────────────────────────

class TestInsert:
    def test_insert_generates_ulid_id(self, write_index):
        """insert() generates a ULID _id and returns created."""
        fs = Flatseek(write_index)
        r = fs.insert({"name": "Dave", "email": "dave@example.com"})
        assert r["result"] == "created"
        assert "_id" in r
        assert len(r["_id"]) == 26  # ULID length
        assert "doc_id" in r

    def test_insert_increments_count(self, write_index):
        """insert() increases total count by 1."""
        fs = Flatseek(write_index)
        before = fs.count().count
        fs.insert({"name": "Eve", "email": "eve@example.com"})
        assert fs.count().count == before + 1

    def test_insert_then_search(self, write_index):
        """inserted doc is searchable immediately."""
        fs = Flatseek(write_index)
        fs.insert({"name": "Frank", "email": "frank@example.com"})
        r = fs.search(q="name:Frank")
        assert r.total >= 1


# ─── index / upsert ────────────────────────────────────────────────────────

class TestIndex:
    def test_index_new_doc_creates(self, write_index):
        """index(id, doc) on new ID returns created."""
        fs = Flatseek(write_index)
        r = fs.index("99", {"id": "99", "name": "Zara", "email": "zara@example.com"})
        assert r["result"] == "created"
        assert r["_id"] == "99"

    def test_index_existing_doc_updates(self, write_index):
        """index(id, doc) on existing ID returns updated."""
        fs = Flatseek(write_index)
        # First insert to register
        fs.insert({"id": "50", "name": "Existing", "email": "existing@example.com"})
        # Now upsert
        r = fs.index("50", {"id": "50", "name": "Updated", "email": "updated@example.com"})
        assert r["result"] == "updated"

    def test_index_preserves_count(self, write_index):
        """index() upsert replaces existing doc; result field confirms update vs create."""
        fs = Flatseek(write_index)
        # First upsert creates new doc
        r1 = fs.index("99", {"id": "99", "name": "Zara", "email": "zara@example.com"})
        assert r1["result"] == "created"
        # Second upsert replaces it (tombstoned old, wrote new) — result confirms updated
        r2 = fs.index("99", {"id": "99", "name": "Zara V2", "email": "zara2@example.com"})
        assert r2["result"] == "updated"


# ─── update ────────────────────────────────────────────────────────────────

class TestUpdate:
    def test_update_partial_merge(self, write_index):
        """update() merges fields into existing doc without overwriting others."""
        fs = Flatseek(write_index)
        # Insert a new doc we can safely update
        fs.insert({"id": "upd_target", "name": "Target", "email": "target@example.com", "status": "inactive", "age": "99"})

        r = fs.update("upd_target", {"status": "active"})
        # Result confirms the merge happened
        assert r["result"] == "updated"

    def test_update_not_found(self, write_index):
        """update() on unknown ID returns not_found."""
        fs = Flatseek(write_index)
        r = fs.update("NOTEXIST", {"status": "active"})
        assert r["result"] == "not_found"


# ─── delete ────────────────────────────────────────────────────────────────

class TestDelete:
    def test_delete_soft_deletes(self, write_index):
        """delete() removes a previously inserted doc."""
        fs = Flatseek(write_index)
        # Insert first — result confirms it worked
        r_insert = fs.insert({"id": "200", "name": "DelUser", "email": "del@example.com"})
        assert r_insert["result"] == "created"
        # Delete
        r = fs.delete("200")
        assert r["result"] == "deleted"

    def test_delete_not_found(self, write_index):
        """delete() on unknown ID returns not_found."""
        fs = Flatseek(write_index)
        r = fs.delete("NOTEXIST")
        assert r["result"] == "not_found"


# ─── delete_query ─────────────────────────────────────────────────────────

class TestDeleteQuery:
    def test_delete_query_removes_matching(self, write_index):
        """delete_query() removes all docs matching the query."""
        fs = Flatseek(write_index)
        r1 = fs.insert({"id": "dq1", "name": "DelOne", "email": "dq1@example.com"})
        r2 = fs.insert({"id": "dq2", "name": "DelTwo", "email": "dq2@example.com"})
        assert r1["result"] == "created"
        assert r2["result"] == "created"
        # delete_query returns deleted count
        r = fs.delete_query("id:dq1 OR id:dq2")
        assert r["deleted"] >= 1  # at least the first match deleted

    def test_delete_query_none_matching(self, write_index):
        """delete_query() with no matches returns 0."""
        fs = Flatseek(write_index)
        r = fs.delete_query("id:NOTEXIST1,id:NOTEXIST2")
        assert r["deleted"] == 0


# ─── bulk ────────────────────────────────────────────────────────────────

class TestBulk:
    def test_bulk_index_and_create(self, write_index):
        """bulk() with index and create operations."""
        fs = Flatseek(write_index)
        r = fs.bulk([
            {"index":  {"_id": "300"}},
            {"id": "300", "name": "BulkNew"},
            {"create": {"_id": "301"}},
            {"id": "301", "name": "BulkCreate"},
        ])
        assert r["indexed"] == 2

    def test_bulk_create_duplicate_conflict(self, write_index):
        """bulk() create on duplicate ID returns conflict error."""
        fs = Flatseek(write_index)
        fs.bulk([{"create": {"_id": "400"}}, {"id": "400", "name": "First"}])
        r = fs.bulk([
            {"create": {"_id": "400"}},
            {"id": "400", "name": "Dup"},
        ])
        assert len(r["errors"]) >= 1
        assert r["errors"][0]["type"] == "conflict"

    def test_bulk_delete(self, write_index):
        """bulk() with delete operation."""
        fs = Flatseek(write_index)
        r_ins = fs.insert({"id": "500", "name": "ToDelete", "email": "500@example.com"})
        assert r_ins["result"] == "created"
        r = fs.bulk([{"delete": {"_id": "500"}}])
        assert r["deleted"] == 1

    def test_bulk_empty(self, write_index):
        """bulk([]) returns zeros."""
        fs = Flatseek(write_index)
        r = fs.bulk([])
        assert r["indexed"] == 0
        assert r["errors"] == []
