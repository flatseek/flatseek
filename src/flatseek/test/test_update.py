"""Comprehensive tests for Flatseek library update operations.

Covers:
  - update() partial merge
  - update() full replacement via index()
  - update() not found
  - update() preserves non-updated fields
  - update() with JSON/list field values
  - update() followed by searchable verification
  - index() as full replacement (upsert semantics)
  - index() on existing doc (updated result)
  - index() on new doc (created result)
"""

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
def idx(tmp_path):
    """Build an index with --id-field for write operation tests."""
    csv_path = tmp_path / "data.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        f.write("id,name,email,status,age\n")
        f.write("1,Alice,alice@example.com,inactive,30\n")
        f.write("2,Bob,bob@example.com,inactive,25\n")
        f.write("3,Carol,carol@example.com,inactive,35\n")

    idx_dir = tmp_path / "idx"
    build(str(csv_path), str(idx_dir), id_field="id")
    shutil.rmtree(tmp_path / "idx" / "docs", ignore_errors=True)

    yield str(idx_dir)
    shutil.rmtree(idx_dir, ignore_errors=True)


# ─── update() partial merge ─────────────────────────────────────────────────

class TestUpdatePartialMerge:
    """Tests for Flatseek.update() partial-document merge semantics."""

    def test_update_returns_updated_result(self, idx):
        """update() on existing doc returns result=updated."""
        fs = Flatseek(idx)
        fs.insert({"id": "upd1", "name": "Target", "email": "t@t.com", "status": "inactive"})
        r = fs.update("upd1", {"status": "active"})
        assert r["result"] == "updated"

    def test_update_preserves_other_fields(self, idx):
        """Partial update does NOT erase fields not mentioned in the update."""
        fs = Flatseek(idx)
        fs.insert({
            "id": "upd2",
            "name": "KeepMe",
            "email": "keep@example.com",
            "status": "inactive",
            "age": "42",
        })

        fs.update("upd2", {"status": "active"})

        # Verify all original fields are still present
        r = fs.search(q="id:upd2")
        docs = r.docs
        assert len(docs) >= 1
        doc = docs[0]
        assert doc.get("name") == "KeepMe", "name should be preserved"
        assert doc.get("email") == "keep@example.com", "email should be preserved"
        assert doc.get("status") == "active", "updated field should reflect new value"
        assert doc.get("age") == "42", "age should be preserved"

    def test_update_overwrites_same_field(self, idx):
        """update() overwrites the value of an existing field."""
        fs = Flatseek(idx)
        fs.insert({"id": "upd3", "name": "OldName", "email": "old@t.com", "status": "inactive"})
        fs.update("upd3", {"name": "NewName"})
        r = fs.search(q="id:upd3")
        doc = r.docs[0]
        assert doc.get("name") == "NewName"

    def test_update_not_found_returns_not_found(self, idx):
        """update() on unknown ID returns result=not_found."""
        fs = Flatseek(idx)
        r = fs.update("DOES_NOT_EXIST_XYZ", {"status": "active"})
        assert r["result"] == "not_found"

    def test_update_with_json_value(self, idx):
        """update() stores dict values (doc_values auto-deserializes JSON strings)."""
        fs = Flatseek(idx)
        fs.insert({"id": "upd4", "name": "JsonUser", "email": "json@t.com", "status": "inactive"})
        fs.update("upd4", {"metadata": {"key": "value", "n": 123}})
        r = fs.search(q="id:upd4")
        doc = r.docs[0]
        # doc_values auto-deserializes JSON strings back to Python objects
        val = doc.get("metadata", {})
        assert val["key"] == "value"
        assert val["n"] == "123"  # values are stored as strings

    def test_update_with_list_value(self, idx):
        """update() stores list values (doc_values auto-deserializes JSON strings)."""
        fs = Flatseek(idx)
        fs.insert({"id": "upd5", "name": "ListUser", "email": "list@t.com", "status": "inactive"})
        fs.update("upd5", {"tags": ["admin", "vip", "beta"]})
        r = fs.search(q="id:upd5")
        doc = r.docs[0]
        # doc_values auto-deserializes JSON strings back to Python objects
        val = doc.get("tags", [])
        assert val == ["admin", "vip", "beta"]

    def test_update_allocates_new_doc_id(self, idx):
        """update() allocates a new doc_id (tombstone + rewrite); total_docs grows."""
        fs = Flatseek(idx)
        fs.insert({"id": "upd6", "name": "CountTest", "email": "cnt@t.com", "status": "inactive"})
        before_count = fs.count().count
        result = fs.update("upd6", {"status": "active"})
        assert result["result"] == "updated"
        # New doc_id was allocated and differs from the pre-update count
        assert result["new_doc_id"] is not None
        assert result["new_doc_id"] >= before_count  # new id is at least as large as count
        # Alive count (search) stays the same — old doc is tombstoned
        alive_before = len(fs.search(q="id:upd6").docs)
        fs.update("upd6", {"status": "step2"})
        alive_after = len(fs.search(q="id:upd6").docs)
        assert alive_before == alive_after == 1

    def test_update_after_delete_returns_not_found(self, idx):
        """update() on a previously deleted doc returns not_found."""
        fs = Flatseek(idx)
        fs.insert({"id": "upd7", "name": "DelMe", "email": "del@t.com", "status": "inactive"})
        fs.delete("upd7")
        r = fs.update("upd7", {"status": "active"})
        assert r["result"] == "not_found"


# ─── index() upsert (full replace) ─────────────────────────────────────────

class TestIndexUpsert:
    """Tests for Flatseek.index() insert-or-replace semantics."""

    def test_index_new_doc_returns_created(self, idx):
        """index(id, doc) on never-seen ID returns result=created."""
        fs = Flatseek(idx)
        r = fs.index("new1", {"id": "new1", "name": "NewDoc", "email": "new@t.com"})
        assert r["result"] == "created"

    def test_index_existing_doc_returns_updated(self, idx):
        """index(id, doc) on existing ID returns result=updated."""
        fs = Flatseek(idx)
        # Insert first to establish the doc
        fs.insert({"id": "existing1", "name": "Original", "email": "orig@t.com"})
        # Replace via index()
        r = fs.index("existing1", {"id": "existing1", "name": "Replaced", "email": "repl@t.com"})
        assert r["result"] == "updated"

    def test_index_replaces_all_fields(self, idx):
        """index() is a full replacement — old fields not in new doc are gone."""
        fs = Flatseek(idx)
        fs.insert({"id": "replace1", "name": "OldName", "email": "old@t.com", "status": "inactive", "age": "30"})
        fs.index("replace1", {"id": "replace1", "name": "NewName", "email": "new@t.com"})
        r = fs.search(q="id:replace1")
        doc = r.docs[0]
        assert doc.get("name") == "NewName"
        # status and age are gone (not in replacement doc)
        assert "status" not in doc or doc.get("status") is None
        assert "age" not in doc or doc.get("age") is None

    def test_index_preserves_alive_count_on_replace(self, idx):
        """index() replace preserves the alive document count (old doc tombstoned, new doc added)."""
        fs = Flatseek(idx)
        fs.insert({"id": "replace2", "name": "R2", "email": "r2@t.com", "status": "inactive"})
        before_alive = len(fs.search(q="id:replace2").docs)
        result = fs.index("replace2", {"id": "replace2", "name": "R2v2", "email": "r2v2@t.com"})
        assert result["result"] == "updated"
        after_alive = len(fs.search(q="id:replace2").docs)
        assert before_alive == after_alive == 1
        # The new doc has the new name
        doc = fs.search(q="id:replace2").docs[0]
        assert doc.get("name") == "R2v2"


# ─── update() and index() together ──────────────────────────────────────────

class TestUpdateVsIndex:
    """Distinction tests: partial merge (update) vs full replace (index)."""

    def test_update_is_partial_index_is_full(self, idx):
        """update() merges; index() replaces — demonstrate the difference."""
        fs = Flatseek(idx)

        # Insert with 4 fields
        fs.insert({"id": "diff1", "name": "Alice", "email": "a@t.com", "status": "inactive", "age": "30"})

        # Partial update — only status
        fs.update("diff1", {"status": "active"})
        r1 = fs.search(q="id:diff1").docs[0]
        assert r1.get("name") == "Alice"   # preserved
        assert r1.get("status") == "active"  # updated

        # Full replace — only name
        fs.index("diff1", {"id": "diff1", "name": "Bob"})
        r2 = fs.search(q="id:diff1").docs[0]
        assert r2.get("name") == "Bob"     # replaced
        # email, status, age are GONE (not in replacement doc)
        assert r2.get("email") is None or "email" not in r2


# ─── Edge cases ─────────────────────────────────────────────────────────────

class TestUpdateEdgeCases:
    """Boundary conditions for update operations."""

    def test_update_with_empty_fields_dict(self, idx):
        """update() with empty dict is a no-op (still returns updated)."""
        fs = Flatseek(idx)
        fs.insert({"id": "empty1", "name": "NoOp", "email": "no@t.com", "status": "inactive"})
        r = fs.update("empty1", {})
        assert r["result"] == "updated"
        # Fields unchanged
        r2 = fs.search(q="id:empty1").docs[0]
        assert r2.get("name") == "NoOp"

    def test_update_with_special_characters_in_field_value(self, idx):
        """update() correctly handles special characters in string values."""
        fs = Flatseek(idx)
        fs.insert({"id": "spec1", "name": "Normal", "email": "n@t.com", "status": "inactive"})
        fs.update("spec1", {"name": "Na,m e'w;ith=special\"chars\nand\ttabs"})
        r = fs.search(q="id:spec1").docs[0]
        assert r.get("name") == "Na,m e'w;ith=special\"chars\nand\ttabs"

    def test_update_numeric_string_field(self, idx):
        """update() coerces numeric values to strings."""
        fs = Flatseek(idx)
        fs.insert({"id": "num1", "name": "NumUser", "email": "n@t.com", "status": "inactive"})
        fs.update("num1", {"age": 99})  # int — should be stored as str
        r = fs.search(q="id:num1").docs[0]
        assert r.get("age") == "99"

    def test_multiple_updates_in_sequence(self, idx):
        """Multiple sequential updates each merge on top of the previous state."""
        fs = Flatseek(idx)
        fs.insert({"id": "seq1", "name": "Seq", "email": "s@t.com", "status": "inactive", "age": "10"})

        fs.update("seq1", {"status": "step1"})
        fs.update("seq1", {"status": "step2"})
        fs.update("seq1", {"age": "20"})

        r = fs.search(q="id:seq1").docs[0]
        assert r.get("status") == "step2", "status should reflect latest update"
        assert r.get("age") == "20", "age updated in last call"
        assert r.get("name") == "Seq", "name preserved throughout"
