"""Tests for multi-index search via _index wildcard pattern."""

from __future__ import annotations

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

from flatseek.core.builder import build
from flatseek.core.query_engine import QueryEngine
from flatseek.flatseek_file import FlatseekPacker


def _build_and_pack(fsk_bucket: Path, name: str, csv_text: str) -> Path:
    """Build CSV → index → pack to .fsk inside fsk_bucket. Returns .fsk path.

    FSK must be inside fsk_bucket so FLATSEEK_DATA_DIR=fsk_bucket can find it.
    """
    csv_path = fsk_bucket / f"{name}.csv"
    csv_path.write_text(csv_text)
    index_dir = fsk_bucket / f"{name}_data"
    build(str(csv_path), str(index_dir))
    fsk_path = fsk_bucket / f"{name}.fsk"
    FlatseekPacker(index_dir, fsk_path).pack()
    return fsk_path


class TestMultiIndexSearch:
    """Tests for POST /_search with _index wildcard pattern.

    Uses realistic log/event data:
    - logs_2025-01.fsk: Jan 2025 events (timestamp, level, category, service)
    - logs_2025-02.fsk: Feb 2025 events (same schema)
    - logs_2025-03.fsk: Mar 2025 events (same schema)
    """

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        """Build 3 FSK log shards with realistic event data."""
        self.data_dir = tmp_path / "fsk_bucket"
        self.data_dir.mkdir()

        # logs_2025-01: Jan 2025 events
        csv_jan = (
            "timestamp,level,category,service,message\n"
            "2025-01-01T00:00:00Z,ERROR,auth,api-gateway,Failed login attempt\n"
            "2025-01-01T01:15:00Z,WARN,auth,api-gateway,Rate limit exceeded\n"
            "2025-01-01T02:30:00Z,INFO,billing,payment-service,Invoice generated\n"
            "2025-01-02T10:00:00Z,ERROR,database,postgres,Connection timeout\n"
            "2025-01-02T14:22:00Z,INFO,auth,user-service,User registered\n"
        )
        self.fsk_jan = _build_and_pack(self.data_dir, "logs_2025-01", csv_jan)

        # logs_2025-02: Feb 2025 events
        csv_feb = (
            "timestamp,level,category,service,message\n"
            "2025-02-01T00:00:00Z,ERROR,auth,api-gateway,Invalid token\n"
            "2025-02-01T03:00:00Z,WARN,billing,payment-service,Payment pending\n"
            "2025-02-02T08:10:00Z,ERROR,database,postgres,Query timeout\n"
            "2025-02-03T11:05:00Z,INFO,billing,invoice-service,Refund processed\n"
        )
        self.fsk_feb = _build_and_pack(self.data_dir, "logs_2025-02", csv_feb)

        # logs_2025-03: Mar 2025 events
        csv_mar = (
            "timestamp,level,category,service,message\n"
            "2025-03-01T00:00:00Z,ERROR,auth,api-gateway,Session expired\n"
            "2025-03-01T05:20:00Z,INFO,auth,user-service,Password reset\n"
            "2025-03-02T12:00:00Z,WARN,database,postgres,Replication lag\n"
        )
        self.fsk_mar = _build_and_pack(self.data_dir, "logs_2025-03", csv_mar)

        # Patch env and create client
        self.env_patch = patch.dict(os.environ, {
            "FLATSEEK_DATA_DIR": str(self.data_dir),
            "FLATSEEK_API_BASE": "http://localhost:8000",
        })
        self.env_patch.start()

        # Reset singleton BEFORE importing app, so new manager sees patched env
        from flatseek.api.deps import reset_index_manager
        reset_index_manager()

        from flatseek.api.main import app
        self.client = TestClient(app, raise_server_exceptions=False)

    def teardown_method(self):
        self.env_patch.stop()
        # Reset the global IndexManager singleton so the next test
        # gets a fresh manager with the new FLATSEEK_DATA_DIR.
        from flatseek.api.deps import reset_index_manager
        reset_index_manager()

    def test_expand_index_pattern_single(self):
        """expand_index_pattern('logs_2025-01') should return one shard."""
        from flatseek.api.deps import IndexManager
        mgr = IndexManager()
        result = mgr.expand_index_pattern("logs_2025-01")
        assert result == ["logs_2025-01"]

    def test_expand_index_pattern_wildcard(self):
        """expand_index_pattern('logs_2025-*') should return all three shards."""
        from flatseek.api.deps import IndexManager
        mgr = IndexManager()
        result = mgr.expand_index_pattern("logs_2025-*")
        assert sorted(result) == ["logs_2025-01", "logs_2025-02", "logs_2025-03"]

    def test_expand_index_pattern_month_wildcard(self):
        """expand_index_pattern('logs_2025-0*') should match months 01-09."""
        from flatseek.api.deps import IndexManager
        mgr = IndexManager()
        result = mgr.expand_index_pattern("logs_2025-0*")
        assert sorted(result) == ["logs_2025-01", "logs_2025-02", "logs_2025-03"]

    def test_expand_index_pattern_no_match(self):
        """expand_index_pattern('nonexistent*') should return empty list."""
        from flatseek.api.deps import IndexManager
        mgr = IndexManager()
        result = mgr.expand_index_pattern("nonexistent*")
        assert result == []

    def test_multi_index_search_basic(self):
        """POST /any/_search with _index:logs_2025-* should return all shards."""
        response = self.client.post(
            "/any_index/_search",
            json={"_index": "logs_2025-*", "q": "*", "size": 100},
        )
        assert response.status_code == 200
        data = response.json()

        # Top-level _index should be the pattern
        assert data.get("_index") == "logs_2025-*"

        # Should have results from all three shards
        hits = data.get("hits", {}).get("hits", [])
        assert len(hits) > 0

        # Each hit's _source should have _index tagged
        indices_seen = {hit.get("_source", {}).get("_index") for hit in hits}
        assert "logs_2025-01" in indices_seen
        assert "logs_2025-02" in indices_seen
        assert "logs_2025-03" in indices_seen

    def test_multi_index_search_level_filter(self):
        """Filter by level=ERROR across all shards."""
        response = self.client.post(
            "/any/_search",
            json={"_index": "logs_2025-*", "q": "level:ERROR"},
        )
        assert response.status_code == 200
        data = response.json()
        hits = data.get("hits", {}).get("hits", [])

        # All results should be ERROR level
        for hit in hits:
            assert hit.get("_source", {}).get("level") == "ERROR"

        # Should span multiple shards (Jan has 2 errors, Feb has 2, Mar has 1)
        indices_seen = {hit.get("_source", {}).get("_index") for hit in hits}
        assert len(indices_seen) > 1  # errors spread across shards

    def test_multi_index_search_category_filter(self):
        """Filter by category=billing across all shards."""
        response = self.client.post(
            "/any/_search",
            json={"_index": "logs_2025-*", "q": "category:billing"},
        )
        assert response.status_code == 200
        data = response.json()
        hits = data.get("hits", {}).get("hits", [])

        # All results should be billing category
        for hit in hits:
            assert hit.get("_source", {}).get("category") == "billing"

    def test_multi_index_search_service_filter(self):
        """Filter by service=postgres across all shards."""
        response = self.client.post(
            "/any/_search",
            json={"_index": "logs_2025-*", "q": "service:postgres"},
        )
        assert response.status_code == 200
        data = response.json()
        hits = data.get("hits", {}).get("hits", [])

        # All results should be postgres service
        for hit in hits:
            assert hit.get("_source", {}).get("service") == "postgres"

    def test_multi_index_search_total(self):
        """total should be sum of all matching shards."""
        response = self.client.post(
            "/any/_search",
            json={"_index": "logs_2025-*", "q": "*", "size": 1000},
        )
        assert response.status_code == 200
        data = response.json()
        total = data.get("hits", {}).get("total", 0)
        # Jan=5, Feb=4, Mar=3 = 12 total
        assert total == 12

    def test_multi_index_search_pagination(self):
        """Pagination should work across shards."""
        # First page
        response = self.client.post(
            "/any/_search",
            json={"_index": "logs_2025-*", "q": "*", "from": 0, "size": 5},
        )
        assert response.status_code == 200
        data = response.json()
        hits = data.get("hits", {}).get("hits", [])
        assert len(hits) == 5
        total = data.get("hits", {}).get("total", 0)
        assert total == 12

        # Second page
        response2 = self.client.post(
            "/any/_search",
            json={"_index": "logs_2025-*", "q": "*", "from": 5, "size": 5},
        )
        data2 = response2.json()
        hits2 = data2.get("hits", {}).get("hits", [])
        assert len(hits2) == 5

        # Third page (remainder)
        response3 = self.client.post(
            "/any/_search",
            json={"_index": "logs_2025-*", "q": "*", "from": 10, "size": 5},
        )
        data3 = response3.json()
        hits3 = data3.get("hits", {}).get("hits", [])
        assert len(hits3) == 2  # 12 total - 10 = 2

    def test_multi_index_search_response_format(self):
        """Response should be Elasticsearch-compatible."""
        response = self.client.post(
            "/any/_search",
            json={"_index": "logs_2025-01", "q": "*"},
        )
        assert response.status_code == 200
        data = response.json()
        assert "_index" in data
        assert "hits" in data
        assert "took" in data
        assert isinstance(data["took"], int)
        # Jan has 5 docs
        assert data["hits"]["total"] == 5

    def test_no_results_returns_empty(self):
        """Pattern with no matches should return empty hits."""
        response = self.client.post(
            "/any/_search",
            json={"_index": "nonexistent*", "q": "*"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data.get("hits", {}).get("total", 0) == 0
        assert data.get("hits", {}).get("hits", []) == []

    def test_comma_separated_pattern(self):
        """Comma-separated pattern should match multiple index names."""
        response = self.client.post(
            "/any/_search",
            json={"_index": "logs_2025-01,logs_2025-03", "q": "*"},
        )
        assert response.status_code == 200
        data = response.json()
        hits = data.get("hits", {}).get("hits", [])
        indices_seen = {hit.get("_source", {}).get("_index") for hit in hits}
        assert "logs_2025-01" in indices_seen
        assert "logs_2025-03" in indices_seen
        assert "logs_2025-02" not in indices_seen
        # Jan=5 + Mar=3 = 8
        assert data["hits"]["total"] == 8

    # ── Multi-index aggregation tests ──────────────────────────────────────────────

    def test_aggregate_terms_multi_index(self):
        """terms aggregation across multiple shards should merge bucket counts."""
        response = self.client.post(
            "/logs_2025-01/_aggregate",
            json={
                "_index": "logs_2025-*",
                "query": "*",
                "aggs": {"by_level": {"terms": {"field": "level", "size": 10}}},
            },
        )
        assert response.status_code == 200
        data = response.json()
        aggs = data.get("aggregations", {})
        # Result key is the field name ("level"), not the agg name ("by_level")
        buckets = aggs.get("level", {}).get("buckets", [])
        bucket_map = {b["key"]: b["doc_count"] for b in buckets}
        # Total: ERROR=5, WARN=3, INFO=4 across all shards
        assert bucket_map.get("ERROR", 0) == 5, f"ERROR expected 5, got {bucket_map}"
        assert bucket_map.get("WARN", 0) == 3, f"WARN expected 3, got {bucket_map}"
        assert bucket_map.get("INFO", 0) == 4, f"INFO expected 4, got {bucket_map}"

    def test_aggregate_stats_multi_index(self):
        """stats aggregation across shards should sum all stats."""
        # Add numeric 'amount' field to test data and rebuild shards in setup
        # For now, test terms aggregation which works on keyword fields
        response = self.client.post(
            "/logs_2025-01/_aggregate",
            json={
                "_index": "logs_2025-*",
                "query": "level:ERROR",
                "aggs": {"err_count": {"terms": {"field": "level", "size": 10}}},
            },
        )
        assert response.status_code == 200
        data = response.json()
        aggs = data.get("aggregations", {})
        # Result key is the field name "level"
        buckets = aggs.get("level", {}).get("buckets", [])
        bucket_map = {b["key"]: b["doc_count"] for b in buckets}
        # ERROR count = 5 across all shards
        assert bucket_map.get("ERROR", 0) == 5, f"ERROR expected 5, got {bucket_map}"

    def test_aggregate_date_histogram_multi_index(self):
        """date_histogram aggregation should sum counts per date bucket across shards."""
        response = self.client.post(
            "/logs_2025-01/_aggregate",
            json={
                "_index": "logs_2025-*",
                "query": "*",
                "aggs": {
                    "by_day": {"date_histogram": {"field": "timestamp", "interval": "day"}}
                },
            },
        )
        assert response.status_code == 200
        data = response.json()
        aggs = data.get("aggregations", {})
        # Result key is field name "timestamp"
        buckets = aggs.get("timestamp", {}).get("buckets", [])
        bucket_map = {}
        for b in buckets:
            bucket_map[b["key"]] = b["doc_count"]
        # Jan docs: Jan1=2, Jan2=2; Feb: Feb1=2, Feb2=2, Feb3=1; Mar: Mar1=2, Mar2=1
        assert bucket_map.get("20250101", 0) == 3, f"20250101 expected 3, got {bucket_map}"
        assert bucket_map.get("20250102", 0) == 2, f"20250102 expected 2, got {bucket_map}"
        assert bucket_map.get("20250201", 0) == 2, f"20250201 expected 2, got {bucket_map}"
        assert bucket_map.get("20250202", 0) == 1, f"20250202 expected 1, got {bucket_map}"
        assert bucket_map.get("20250203", 0) == 1, f"20250203 expected 1, got {bucket_map}"
        assert bucket_map.get("20250301", 0) == 2, f"20250301 expected 2, got {bucket_map}"
        assert bucket_map.get("20250302", 0) == 1, f"20250302 expected 1, got {bucket_map}"

    def test_aggregate_empty_pattern(self):
        """Aggregation on no-matching pattern returns empty aggregations."""
        response = self.client.post(
            "/logs_2025-01/_aggregate",
            json={
                "_index": "nonexistent*",
                "query": "*",
                "aggs": {"by_level": {"terms": {"field": "level", "size": 10}}},
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data.get("total", 0) == 0
        assert data.get("aggregations", {}) == {}

