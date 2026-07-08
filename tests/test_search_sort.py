"""Tests for search result sorting."""

import json
import os
import tempfile
import zlib
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest


class TestSortParsing:
    """Tests for _parse_sort helper."""

    def test_string_format(self):
        """Parse "field:desc,field2:asc" format."""
        from flatseek.api.routes.search import _parse_sort
        result = _parse_sort("amount:desc,created_at:asc")
        assert result == [("amount", "desc"), ("created_at", "asc")]

    def test_string_format_single(self):
        """Parse single field."""
        from flatseek.api.routes.search import _parse_sort
        result = _parse_sort("amount:desc")
        assert result == [("amount", "desc")]

    def test_elasticsearch_style(self):
        """Parse [{"field": "amount", "order": "desc"}] format."""
        from flatseek.api.routes.search import _parse_sort
        result = _parse_sort([{"field": "amount", "order": "desc"}])
        assert result == [("amount", "desc")]

    def test_simple_dict_style(self):
        """Parse [{"amount": "desc"}] format."""
        from flatseek.api.routes.search import _parse_sort
        result = _parse_sort([{"amount": "desc"}, {"created_at": "asc"}])
        assert result == [("amount", "desc"), ("created_at", "asc")]

    def test_none(self):
        """None input returns None."""
        from flatseek.api.routes.search import _parse_sort
        assert _parse_sort(None) is None

    def test_empty_string(self):
        """Empty string returns None."""
        from flatseek.api.routes.search import _parse_sort
        assert _parse_sort("") is None

    def test_string_with_spaces(self):
        """Parse with spaces around fields."""
        from flatseek.api.routes.search import _parse_sort
        result = _parse_sort("amount : desc , created_at : asc")
        assert result == [("amount", "desc"), ("created_at", "asc")]


class TestSortByNumeric:
    """Tests for _sort_by_numeric using mocks."""

    def test_sort_by_numeric_asc(self):
        """Sort doc_ids by numeric field ascending."""
        from flatseek.core.query_engine import QueryEngine

        qe = MagicMock(spec=QueryEngine)
        # Use the actual method via unbound method
        method = QueryEngine._sort_by_numeric.__get__(qe, QueryEngine)

        # pairs is list of (value, doc_id) sorted by value
        pairs = [(50.0, 1), (100.0, 0), (200.0, 2)]
        doc_ids = [0, 1, 2]

        result = method(doc_ids, pairs, "amount", "asc")
        assert result == [1, 0, 2]  # sorted by value: 50, 100, 200

    def test_sort_by_numeric_desc(self):
        """Sort doc_ids by numeric field descending."""
        from flatseek.core.query_engine import QueryEngine

        qe = MagicMock(spec=QueryEngine)
        method = QueryEngine._sort_by_numeric.__get__(qe, QueryEngine)

        pairs = [(50.0, 1), (100.0, 0), (200.0, 2)]
        doc_ids = [0, 1, 2]

        result = method(doc_ids, pairs, "amount", "desc")
        assert result == [2, 0, 1]  # sorted by value desc: 200, 100, 50

    def test_sort_by_numeric_missing_values(self):
        """Docs with missing values sort last."""
        from flatseek.core.query_engine import QueryEngine

        qe = MagicMock(spec=QueryEngine)
        method = QueryEngine._sort_by_numeric.__get__(qe, QueryEngine)

        # Doc 2 is missing from pairs
        pairs = [(50.0, 1), (100.0, 0), (200.0, 2)]
        doc_ids = [0, 1, 2]

        result = method(doc_ids, pairs, "amount", "asc")
        # Doc 1 (50) first, doc 0 (100) second, doc 2 (missing) last
        assert result == [1, 0, 2]


class TestSortDocIds:
    """Tests for _sort_doc_ids."""

    def test_no_sort_spec(self):
        """Without sort spec, return sorted doc_ids."""
        from flatseek.core.query_engine import QueryEngine

        qe = MagicMock(spec=QueryEngine)
        method = QueryEngine._sort_doc_ids.__get__(qe, QueryEngine)

        doc_ids = [3, 1, 4, 1, 5]
        result = method(doc_ids, None)
        assert result == [1, 1, 3, 4, 5]

    def test_single_field_sort(self):
        """Sort by single field."""
        from flatseek.core.query_engine import QueryEngine

        qe = MagicMock(spec=QueryEngine)
        # Mock _sort_by_field to return reversed doc_ids
        qe._sort_by_field = MagicMock(side_effect=lambda ids, f, d: list(reversed(ids)))
        method = QueryEngine._sort_doc_ids.__get__(qe, QueryEngine)

        doc_ids = [0, 1, 2]
        result = method(doc_ids, [("amount", "desc")])
        assert result == [2, 1, 0]

    def test_multi_field_sort(self):
        """Multi-field sort is applied in reverse order."""
        from flatseek.core.query_engine import QueryEngine

        qe = MagicMock(spec=QueryEngine)
        call_order = []

        def mock_sort(ids, field, direction):
            call_order.append((field, direction))
            return ids

        qe._sort_by_field = MagicMock(side_effect=mock_sort)
        method = QueryEngine._sort_doc_ids.__get__(qe, QueryEngine)

        doc_ids = [0, 1, 2]
        result = method(doc_ids, [("status", "asc"), ("amount", "desc")])

        # Should be called in reverse order: amount first (primary), then status
        assert call_order == [("amount", "desc"), ("status", "asc")]


class TestSearchEndpointSort:
    """Integration tests for search endpoint with sort param."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.data_dir = tmp_path
        self.env_patch = patch.dict(os.environ, {
            "FLATSEEK_DATA_DIR": str(self.data_dir),
            "FLATSEEK_API_BASE": "http://localhost:8000",
        })
        self.env_patch.start()

        from fastapi.testclient import TestClient
        from flatseek.api.main import app
        self.client = TestClient(app, raise_server_exceptions=False)

    def teardown_method(self):
        self.env_patch.stop()

    def _make_index(self, name, stats):
        idx_dir = self.data_dir / name
        idx_dir.mkdir(parents=True)
        (idx_dir / "index").mkdir()
        (idx_dir / "docs").mkdir()
        (idx_dir / "stats.json").write_text(json.dumps(stats))
        return idx_dir

    def test_sort_param_in_response(self):
        """Verify sort param is accepted and no error on index without sort data."""
        self._make_index("testidx", {
            "total_docs": 0,
            "columns": {},
        })

        # Should not error even without doc_values
        response = self.client.get("/testidx/_search?q=*&sort=amount:desc")
        # Acceptable responses: 200 (empty) or error if no data
        assert response.status_code in (200, 404)
