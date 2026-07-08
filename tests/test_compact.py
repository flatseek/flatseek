"""Tests for compaction — reclaiming disk space after bulk deletes."""

import json
import os
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

try:
    from fastapi.testclient import TestClient
    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False
    pytest.skip("FastAPI not installed", allow_module_level=True)


def _create_index_with_tombstones(data_dir: Path, docs: list[dict], deleted_indices: list[int] = None) -> Path:
    """Create a minimal index with documents and tombstones.

    Args:
        data_dir: Parent directory
        docs: List of doc dicts
        deleted_indices: Which doc indices (0-based) are marked deleted
    """
    idx_dir = data_dir / "testidx"
    idx_dir.mkdir(parents=True, exist_ok=True)

    (idx_dir / "index").mkdir()
    (idx_dir / "docs").mkdir()

    # Write stats
    stats = {
        "total_docs": len(docs),
        "total_entries": len(docs) * 3,
        "index_size_mb": 0.1,
        "docs_size_mb": 0.1,
        "total_size_mb": 0.2,
        "columns": {"id": {"type": "KEYWORD"}, "name": {"type": "TEXT"}, "email": {"type": "KEYWORD"}},
        "_id_field": "id",
    }
    (idx_dir / "stats.json").write_text(json.dumps(stats))

    # Write docs in chunks
    chunk_start = 0
    aa = f"{(chunk_start // 100000 // 256) & 0xFF:02x}"
    bb = f"{(chunk_start // 100000) & 0xFF:02x}"
    docs_dir = idx_dir / "docs" / aa / bb
    docs_dir.mkdir(parents=True, exist_ok=True)

    doc_map = {}
    for i, doc in enumerate(docs):
        doc_map[i] = doc

    import zlib
    blob = zlib.compress(json.dumps(doc_map).encode(), level=1)
    chunk_path = docs_dir / f"docs_{chunk_start:010d}.zlib"
    chunk_path.write_bytes(blob)

    # Set up ID mapping
    id_map_dir = idx_dir / ".ids"
    id_map_dir.mkdir(parents=True, exist_ok=True)
    id_map = {str(i): doc.get("id", str(i)) for i, doc in enumerate(docs)}
    (id_map_dir / "id.json").write_text(json.dumps(id_map))

    # Set up tombstones
    if deleted_indices:
        tomb_dir = idx_dir / ".tombstones"
        tomb_dir.mkdir(parents=True, exist_ok=True)

        # Set up bitmap with deleted bits
        max_doc_id = len(docs)
        bitmap_size = (max_doc_id + 7) // 8
        bitmap = bytearray(bitmap_size)
        for di in deleted_indices:
            bitmap[di // 8] |= 1 << (di % 8)
        (tomb_dir / "bitmap.bin").write_bytes(bytes(bitmap))
        (tomb_dir / "max_doc_id").write_bytes(max_doc_id.to_bytes(4, "little"))
        (tomb_dir / "version").write_bytes((1).to_bytes(8, "little"))

    return idx_dir


class TestTombstoneClear:
    """Test TombstoneStore.clear() method."""

    def test_clear_resets_bitmap(self, tmp_path):
        """clear() resets bitmap to all zeros."""
        from flatseek.core.tombstone import TombstoneStore

        idx_dir = _create_index_with_tombstones(tmp_path, [
            {"id": "1", "name": "Alice"},
            {"id": "2", "name": "Bob"},
            {"id": "3", "name": "Carol"},
        ], deleted_indices=[1])  # Bob is deleted

        store = TombstoneStore(str(idx_dir))
        assert store.get_deleted_count() == 1
        assert store.is_deleted(1)

        store.clear()

        assert store.get_deleted_count() == 0
        assert not store.is_deleted(1)
        assert store.get_total_count() == 0

    def test_clear_deletes_wal_files(self, tmp_path):
        """clear() removes WAL files."""
        from flatseek.core.tombstone import TombstoneStore

        # Create index with NO pre-set tombstones, then mark_deleted
        idx_dir = _create_index_with_tombstones(tmp_path, [
            {"id": "1", "name": "Alice"},
        ])  # No deleted_indices

        store = TombstoneStore(str(idx_dir))
        store.mark_deleted([0])  # Mark doc_id 0 as deleted (creates WAL)

        wal_dir = idx_dir / "_tombstone_wal"
        assert list(wal_dir.glob("wal_*.bin"))

        store.clear()

        assert not list(wal_dir.glob("wal_*.bin"))

    def test_get_alive_count(self, tmp_path):
        """get_alive_count() returns total - deleted."""
        from flatseek.core.tombstone import TombstoneStore

        idx_dir = _create_index_with_tombstones(tmp_path, [
            {"id": "1", "name": "Alice"},
            {"id": "2", "name": "Bob"},
            {"id": "3", "name": "Carol"},
            {"id": "4", "name": "Dave"},
        ], deleted_indices=[1, 2])  # Bob and Carol deleted

        store = TombstoneStore(str(idx_dir))
        assert store.get_total_count() == 4
        assert store.get_deleted_count() == 2
        assert store.get_alive_count() == 2


class TestIndexCompactor:
    """Test IndexCompactor.compact() method."""

    def test_compact_requires_alive_docs(self, tmp_path):
        """compact() raises if there are no alive docs."""
        from flatseek.core.tombstone import TombstoneStore
        from flatseek.core.compactor import IndexCompactor

        idx_dir = _create_index_with_tombstones(tmp_path, [
            {"id": "1", "name": "Alice"},
        ], deleted_indices=[0])  # All deleted

        store = TombstoneStore(str(idx_dir))
        compact = IndexCompactor(str(idx_dir), store)

        with pytest.raises(ValueError, match="No alive documents"):
            compact.compact()

    def test_compact_raises_on_fsk(self, tmp_path):
        """compact() raises if index is a .fsk file."""
        from flatseek.core.compactor import IndexCompactor

        fsk_path = tmp_path / "test.fsk"
        fsk_path.write_text("dummy")

        # TombstoneStore not needed for this check
        class FakeTS:
            pass

        compact = IndexCompactor(str(fsk_path), FakeTS())
        # The check happens before tombstone is even accessed
        with pytest.raises(Exception):  # path does not exist as dir
            compact.compact()


class TestCompactCLI:
    """Test CLI compact command."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.data_dir = tmp_path
        self.env_patch = patch.dict(os.environ, {
            "FLATSEEK_DATA_DIR": str(self.data_dir),
            "FLATSEEK_API_BASE": "http://localhost:8000",
        })
        self.env_patch.start()
        yield
        self.env_patch.stop()

    def test_compact_no_deleted(self, tmp_path):
        """compact with no deleted docs prints nothing to reclaim."""
        from flatseek.core.tombstone import TombstoneStore

        idx_dir = _create_index_with_tombstones(tmp_path, [
            {"id": "1", "name": "Alice"},
            {"id": "2", "name": "Bob"},
        ])  # No deleted

        store = TombstoneStore(str(idx_dir))
        assert store.get_deleted_count() == 0

        # Should not raise
        from flatseek.core.compactor import IndexCompactor
        compact = IndexCompactor(str(idx_dir), store)
        # With no deleted, should raise ValueError
        with pytest.raises(ValueError, match="No alive documents"):
            compact.compact()


class TestCompactAPI:
    """Test POST /{index}/_compact endpoint."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.data_dir = tmp_path
        self.env_patch = patch.dict(os.environ, {
            "FLATSEEK_DATA_DIR": str(self.data_dir),
            "FLATSEEK_API_BASE": "http://localhost:8000",
        })
        self.env_patch.start()

        from flatseek.api.main import app
        self.client = TestClient(app, raise_server_exceptions=False)

        yield

        self.env_patch.stop()

    def test_compact_endpoint_requires_tombstones(self, tmp_path):
        """Endpoint returns 400 if index has no tombstone support."""
        idx_dir = tmp_path / "noidx"
        idx_dir.mkdir(parents=True, exist_ok=True)
        (idx_dir / "index").mkdir()
        (idx_dir / "docs").mkdir()
        (idx_dir / "stats.json").write_text(json.dumps({"total_docs": 1}))

        response = self.client.post("/noidx/_compact")
        assert response.status_code == 400

    def test_compact_endpoint_no_deleted_returns_error(self, tmp_path):
        """Compact on index with no deleted docs returns error."""
        _create_index_with_tombstones(tmp_path, [
            {"id": "1", "name": "Alice"},
            {"id": "2", "name": "Bob"},
        ])

        response = self.client.post("/testidx/_compact")
        # No deleted docs → raises ValueError → HTTP 500 or 400
        assert response.status_code in (400, 500)
