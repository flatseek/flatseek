"""Regression tests for live-search-while-building (WAL code path).

Covers the AttributeError on `_wal_posting_cache` that surfaced when
searching a freshly-built index with WAL files in `_wal/` and no prior
call to `clear_doc_cache()`.
"""
from __future__ import annotations

import json
import struct
import tempfile
import shutil
from pathlib import Path

from flatseek.core.query_engine import QueryEngine


def _make_minimal_index(data_dir: Path) -> None:
    """Create the smallest possible on-disk index layout that QE accepts."""
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "_wal").mkdir(exist_ok=True)
    (data_dir / "index").mkdir(exist_ok=True)
    (data_dir / "docs").mkdir(exist_ok=True)
    (data_dir / "stats.json").write_text(json.dumps({
        "total_docs": 0, "total_entries": 0,
        "index_size_mb": 0, "docs_size_mb": 0, "total_size_mb": 0,
        "columns": [],
    }))


def _write_wal(data_dir: Path, prefix: str, term: bytes, doc_id: int) -> Path:
    """Write one WAL entry: <5s prefix><I data_len><data>.

    Inside <data> is a single entry of <H term_len><term><I dl_len><doclist>.
    doclist is varint delta-encoded, so a single doc_id N becomes bytes [N].
    """
    doclist = bytes([doc_id])
    inner = struct.pack("<H", len(term)) + term + struct.pack("<I", len(doclist)) + doclist
    prefix_bytes = prefix.encode("ascii")[:5].ljust(5, b"_")[:5]
    payload = prefix_bytes + struct.pack("<I", len(inner)) + inner
    wal_path = data_dir / "_wal" / "test.wal"
    wal_path.write_bytes(payload)
    return wal_path


class TestWalSearchCaches:
    def setup_method(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="wal_search_test_"))
        self.data_dir = self.tmp / "data"
        _make_minimal_index(self.data_dir)

    def teardown_method(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_fresh_qe_initializes_wal_posting_cache(self):
        """Regression: a fresh QE that hasn't called clear_doc_cache() yet
        must have `_wal_posting_cache` defined so `_read_wal_postings`
        can write to it without raising AttributeError."""
        qe = QueryEngine(str(self.data_dir))
        assert hasattr(qe, "_wal_posting_cache"), (
            "QueryEngine.__init__ must initialize _wal_posting_cache — "
            "without this, `_read_wal_postings` AttributeError's on the "
            "first live-search-while-building query."
        )
        assert qe._wal_posting_cache == {}

    def test_wal_posting_cache_not_cleared_by_clear_doc_cache(self):
        """`clear_doc_cache()` is called by streaming export between pages.
        It must NOT clobber the WAL posting cache (the cache persists
        across queries to keep repeated-prefix WAL lookups O(1))."""
        qe = QueryEngine(str(self.data_dir))
        qe._wal_posting_cache["aa/bb"] = {"hello": [1, 2, 3]}
        qe.clear_doc_cache()
        assert qe._wal_posting_cache == {"aa/bb": {"hello": [1, 2, 3]}}, \
            "clear_doc_cache must preserve WAL posting cache"

    def test_read_wal_postings_with_fresh_qe(self):
        """End-to-end: seed one WAL entry on disk, query the prefix from a
        fresh QE, and verify the decoded postings come back without an
        AttributeError. This is the exact scenario reported in the bug:
        no prior pagination, no prior `clear_doc_cache()` call."""
        _write_wal(self.data_dir, "aa/bb", b"hello", doc_id=42)
        qe = QueryEngine(str(self.data_dir))
        # Must not raise AttributeError on the first call.
        result = qe._read_wal_postings("aa/bb")
        assert "hello" in result, f"expected 'hello' term, got {list(result)}"
        assert 42 in result["hello"], f"expected doc_id 42, got {result['hello']}"
        # Cache populated as a side effect.
        assert "aa/bb" in qe._wal_posting_cache
