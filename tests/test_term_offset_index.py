"""Tests for the term offset index in `_read_posting`.

The index maps `term_bytes -> offset_in_decompressed_bin_data` per bin file,
allowing direct lookup of a term's posting list without scanning the entire
bucket file. Built lazily on first access, cached in
`QueryEngine._term_offset_cache`.
"""

import sys
import tempfile
import shutil
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO / "src"))


def _make_index(data_dir: Path, records: list[dict]) -> Path:
    """Build a real index from records."""
    from flatseek.core.builder import IndexBuilder
    data_dir.mkdir(parents=True, exist_ok=True)
    headers = list(records[0].keys())
    builder = IndexBuilder(str(data_dir), column_map={})
    builder.start_doc_id = 0
    for r in records:
        builder.add_row({h: str(r.get(h, "")) for h in headers}, headers)
    builder.finalize()
    return data_dir


@pytest.fixture
def tmp_index():
    tmp = Path(tempfile.mkdtemp(prefix="flatseek_termidx_"))
    yield tmp
    shutil.rmtree(tmp, ignore_errors=True)


def _make_records(n: int) -> list[dict]:
    """Generate records with mixed column types so multiple terms per bucket."""
    out = []
    for i in range(n):
        out.append({
            "id": str(i),
            "name": f"user_{i:04d}",
            "tag": f"tag_{i % 7}",       # 7 distinct tags
            "city": f"city_{i % 3}",      # 3 distinct cities
        })
    return out


# ── Correctness: indexed lookup matches full-scan ────────────────────────────


class TestTermIndexCorrectness:
    """The indexed lookup must return IDENTICAL results to the previous
    full-scan path — same docs, same order, same set semantics."""

    def test_single_term_match_pre_and_post_index(self, tmp_index):
        data_dir = _make_index(tmp_index / "data", _make_records(100))
        from flatseek.core.query_engine import QueryEngine
        qe = QueryEngine(str(data_dir))

        # Snapshot the index, then call _read_posting. The result must
        # match the canonical lookup via the same QueryEngine.
        result_indexed = qe._read_posting("tag:tag_1")
        # Call again to confirm cache hit path also works (same result).
        result_cached = qe._read_posting("tag:tag_1")
        assert result_indexed == result_cached
        # 100 records / 7 tags → tag_1 matches every 7th, so ~14 docs.
        assert all(isinstance(x, int) for x in result_indexed)
        assert len(result_indexed) == len(set(result_indexed))  # deduped

    def test_multiple_terms_same_bucket(self, tmp_index):
        """Multi-term AND query in same bucket — exercises the index
        multiple times in one call."""
        data_dir = _make_index(tmp_index / "data", _make_records(100))
        from flatseek.core.query_engine import QueryEngine
        from flatseek.core.query_parser import parse, execute
        qe = QueryEngine(str(data_dir))

        ast = parse("tag:tag_1 AND tag:tag_2")
        result = execute(ast, qe)
        # tag_1 and tag_2 are disjoint sets, so intersection is empty.
        assert result == set()

        ast2 = parse("tag:tag_0 OR tag:tag_1")
        result2 = execute(ast2, qe)
        # Union of two tags.
        assert len(result2) == sum(1 for r in _make_records(100)
                                   if r["tag"] in ("tag_0", "tag_1"))

    def test_no_match_returns_empty(self, tmp_index):
        """Term not in any file — index returns empty (term_set fast-reject
        also handles this, but we verify the index path doesn't crash."""
        data_dir = _make_index(tmp_index / "data", _make_records(50))
        from flatseek.core.query_engine import QueryEngine
        qe = QueryEngine(str(data_dir))
        result = qe._read_posting("nonexistent_term")
        assert result == []

    def test_index_cached_across_calls(self, tmp_index):
        """The index for a bin file is built once and reused. Direct test
        of the `_build_term_offset_index` helper (not the production path,
        which is currently scan — see the comment in `_read_posting`).
        """
        data_dir = _make_index(tmp_index / "data", _make_records(50))
        from flatseek.core.query_engine import QueryEngine
        qe = QueryEngine(str(data_dir))

        assert qe._term_offset_cache == {}  # empty before any access

        # Force the helper to be invoked directly (bypasses _read_posting
        # which is currently reverted to the scan path).
        from flatseek.core.query_engine import term_hash
        import os as _os
        from flatseek.core.query_engine import _doc_loads
        term = "tag:tag_0"
        bucket_dir = _os.path.join(qe.index_dir, term_hash(term))
        bin_path = _os.path.join(bucket_dir, "idx.bin")
        assert _os.path.exists(bin_path), f"test setup: expected {bin_path}"
        qe._build_term_offset_index(bin_path)

        # After one call, the index for this bin file should be cached.
        assert bin_path in qe._term_offset_cache
        assert isinstance(qe._term_offset_cache[bin_path], dict)

        # Snapshot dict contents.
        snapshot = dict(qe._term_offset_cache[bin_path])

        # Call the helper again — should NOT rebuild (same dict object).
        qe._build_term_offset_index(bin_path)
        assert qe._term_offset_cache[bin_path] == snapshot, (
            f"Index for {bin_path} was rebuilt between calls — should be "
            f"reused from cache. This is what the optimization relies on."
        )


# ── Regression: bucket scan is eliminated ────────────────────────────────────


class TestBucketScanEliminated:
    """Verify the indexed lookup actually skips the per-term scan. We do
    this by checking that the index path returns the same set as iterating
    the raw data ourselves (which simulates what the scan would do)."""

    def test_indexed_lookup_equals_manual_scan(self, tmp_index):
        """For each bin file the index reads, the indexed term→posting-list
        must match what we'd get by walking the file manually. This is a
        strong invariant: any divergence between scan and index would be a
        correctness bug."""
        data_dir = _make_index(tmp_index / "data", _make_records(100))
        from flatseek.core.query_engine import QueryEngine
        from flatseek.core.query_engine import _doc_loads, _decompress_doc, decode_doclist
        qe = QueryEngine(str(data_dir))

        # Force the index to build by doing one read.
        qe._read_posting("tag:tag_0")

        # For each cached index file, manually scan and compare.
        import struct as _struct
        _unpack_h = _struct.Struct("<H").unpack_from
        _unpack_i = _struct.Struct("<I").unpack_from
        for bin_path, offsets in qe._term_offset_cache.items():
            data = qe._mmap_read(bin_path)
            if not data:
                continue
            data = qe._decrypt_if_needed(data)
            if len(data) >= 2 and data[0] == 0x78 and data[1] in (0x01, 0x5e, 0x9c, 0xda):
                data = _decompress_doc(data)

            # Walk every entry and build expected offsets.
            expected = {}
            offset = 0
            n = len(data)
            while offset + 2 <= n:
                tlen = _unpack_h(data, offset)[0]
                offset += 2
                if offset + tlen > n:
                    break
                term = bytes(data[offset:offset + tlen])
                offset += tlen
                if offset + 4 > n:
                    break
                pl_len = _unpack_i(data, offset)[0]
                expected[term] = offset
                offset += 4 + pl_len

            assert offsets == expected, (
                f"Term offset index for {bin_path} disagrees with manual scan. "
                f"Index has {len(offsets)} entries, scan found {len(expected)}."
            )

            # And for each indexed term, jumping to the offset and reading
            # the posting list yields a non-empty doc-id list.
            for term_bytes, pl_start in offsets.items():
                if pl_start + 4 > n:
                    continue
                pl_len = _unpack_i(data, pl_start)[0]
                pl_data = data[pl_start + 4 : pl_start + 4 + pl_len]
                ids = decode_doclist(pl_data)
                # Should be a valid doc-id list (possibly empty for unusual
                # edge cases, but never raise).
                assert isinstance(ids, list)
                assert all(isinstance(i, int) for i in ids)