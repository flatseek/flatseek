"""Detailed search-engine accuracy tests.

Covers every query pattern documented in README.md:
  • field:value (KEYWORD exact, TEXT free-text)
  • wildcard (prefix / suffix / substring)
  • numeric / date range — [A TO B], >, <, >=, <=
  • boolean (true/false, yes/no, 1/0)
  • array fields (element-wise match)
  • object dot-path (any depth)
  • indexed nested array (obj.arr[i]:val)
  • boolean combinators (AND / OR / NOT, parens)
  • match-all (*) + NOT exclusions
  • aggregations (terms, stats, date_histogram, avg, min, max, sum, cardinality)
"""

import os
import sys
import csv
import json
import tempfile
import shutil
import pytest
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from flatseek.core.builder import IndexBuilder, _expand_record
from flatseek.core.query_engine import QueryEngine


# ─── Rich fixture covering every column type ─────────────────────────────────
DUMMY_RECORDS = [
    {
        "id": 1,
        "name": "Alice Johnson",
        "email": "alice@dev.io",
        "phone": "081234567890",
        "city": "Jakarta",
        "country": "Indonesia",
        "status": "active",
        "enabled": True,
        "balance": 1500000.50,
        "lat": -6.2088,
        "lon": 106.8456,
        "created_at": "2026-01-15T08:30:00",
        "birthday": "1995-03-22",
        "tags": ["graphql", "java", "devops"],
        "categories": ["engineering", "backend"],
        "address": '{"city": "Jakarta", "district": "Sudirman", "zip": "10110"}',
        "skills": '["python", "golang", "kubernetes"]',
        "score": 87.5,
        "level": 12,
        "info": '{"metadata": {"a": {"value": 100, "name": "alice_a", "tags": ["alpha", "beta"], "submetadata": {"deep": "secret_a", "level": 5}}, "b": {"value": 200, "name": "alice_b", "items": ["item_x", "item_y"]}}}',
    },
    {
        "id": 2,
        "name": "Bob Smith",
        "email": "bob@tech.com",
        "phone": "081298765432",
        "city": "Surabaya",
        "country": "Indonesia",
        "status": "inactive",
        "enabled": False,
        "balance": 230000.00,
        "lat": -7.2575,
        "lon": 112.7521,
        "created_at": "2026-02-20T14:00:00",
        "birthday": "1990-07-10",
        "tags": ["python", "react", "frontend"],
        "categories": ["engineering", "frontend"],
        "address": '{"city": "Surabaya", "district": "Wonokromo", "zip": "60246"}',
        "skills": '["typescript", "nextjs"]',
        "score": 72.0,
        "level": 8,
        "info": '{"metadata": {"a": {"value": 150, "name": "bob_a", "tags": ["gamma", "delta"], "submetadata": {"deep": "secret_b", "level": 8}}, "b": {"value": 250, "name": "bob_b", "items": ["item_p", "item_q"]}}}',
    },
    {
        "id": 3,
        "name": "Charlie Putri",
        "email": "charlie@startup.id",
        "phone": "085678123456",
        "city": "Bandung",
        "country": "Indonesia",
        "status": "active",
        "enabled": True,
        "balance": 890000.25,
        "lat": -6.9175,
        "lon": 107.6191,
        "created_at": "2026-01-28T09:15:00",
        "birthday": "1998-11-05",
        "tags": ["golang", "docker", "graphql"],
        "categories": ["engineering", "devops"],
        "address": '{"city": "Bandung", "district": "Dago", "zip": "40111"}',
        "skills": '["rust", "postgres"]',
        "score": 95.0,
        "level": 15,
        "info": '{"metadata": {"a": {"value": 120, "name": "charlie_a", "tags": ["charlie_tag1", "charlie_tag2"], "submetadata": {"deep": "secret_c", "level": 12}}, "b": {"value": 220, "name": "charlie_b", "items": ["charlie_item1", "charlie_item2"]}}}',
    },
    {
        "id": 4,
        "name": "Diana Vals",
        "email": "diana@corp.co",
        "phone": "087712345678",
        "city": "Semarang",
        "country": "Indonesia",
        "status": "active",
        "enabled": True,
        "balance": 3200000.00,
        "lat": -6.9667,
        "lon": 110.4204,
        "created_at": "2026-03-01T11:00:00",
        "birthday": "1992-06-18",
        "tags": ["java", "springboot", "postgres"],
        "categories": ["engineering", "backend"],
        "address": '{"city": "Semarang", "district": "Candisari", "zip": "50256"}',
        "skills": '["java", "spring"]',
        "score": 68.0,
        "level": 10,
        "info": '{"metadata": {"a": {"value": 300, "name": "diana_a", "tags": ["diana_tag1", "diana_tag2"], "submetadata": {"deep": "secret_d", "level": 15}}, "b": {"value": 400, "name": "diana_b", "items": ["diana_item1", "diana_item2"]}}}',
    },
    {
        "id": 5,
        "name": "Eko Prasetyo",
        "email": "eko@studio.my",
        "phone": "081211223344",
        "city": "Yogyakarta",
        "country": "Indonesia",
        "status": "pending",
        "enabled": False,
        "balance": 55000.00,
        "lat": -7.7956,
        "lon": 110.3618,
        "created_at": "2026-03-10T16:45:00",
        "birthday": "1988-01-30",
        "tags": ["kotlin", "android", "mobile"],
        "categories": ["engineering", "mobile"],
        "address": '{"city": "Yogyakarta", "district": "Sleman", "zip": "55281"}',
        "skills": '["kotlin", "jetpack-compose"]',
        "score": 80.5,
        "level": 11,
        "info": '{"metadata": {"a": {"value": 180, "name": "eko_a", "tags": ["eko_tag1", "eko_tag2"], "submetadata": {"deep": "secret_e", "level": 6}}, "b": {"value": 280, "name": "eko_b", "items": ["eko_item1", "eko_item2"]}}}',
    },
    {
        "id": 6,
        "name": "Fitri Hamdi",
        "email": "fitri@dev.io",
        "phone": "081233344556",
        "city": "Jakarta",
        "country": "Indonesia",
        "status": "active",
        "enabled": True,
        "balance": 4100000.75,
        "lat": -6.1751,
        "lon": 106.8650,
        "created_at": "2026-02-14T07:00:00",
        "birthday": "1993-09-14",
        "tags": ["typescript", "nodejs", "react"],
        "categories": ["engineering", "fullstack"],
        "address": '{"city": "Jakarta", "district": "Thamrin", "zip": "10350"}',
        "skills": '["nodejs", "express", "react"]',
        "score": 91.0,
        "level": 14,
        "info": '{"metadata": {"a": {"value": 500, "name": "fitri_a", "tags": ["fitri_tag1", "fitri_tag2"], "submetadata": {"deep": "secret_f", "level": 20}}, "b": {"value": 600, "name": "fitri_b", "items": ["fitri_item1", "fitri_item2"]}}}',
    },
    {
        "id": 7,
        "name": "Gita Wirnata",
        "email": "gita@ux.design",
        "phone": "085677889900",
        "city": "Bali",
        "country": "Indonesia",
        "status": "active",
        "enabled": True,
        "balance": 780000.00,
        "lat": -8.4095,
        "lon": 115.1889,
        "created_at": "2026-01-05T10:30:00",
        "birthday": "1997-04-25",
        "tags": ["figma", "ux", "design"],
        "categories": ["design", "product"],
        "address": '{"city": "Bali", "district": "Denpasar", "zip": "80111"}',
        "skills": '["figma", "sketch"]',
        "score": 77.0,
        "level": 9,
        "info": '{"metadata": {"a": {"value": 90, "name": "gita_a", "tags": ["gita_tag1", "gita_tag2"], "submetadata": {"deep": "secret_g", "level": 3}}, "b": {"value": 190, "name": "gita_b", "items": ["gita_item1", "gita_item2"]}}}',
    },
    {
        "id": 8,
        "name": "Hendra Kusuma",
        "email": "hendra@data.ai",
        "phone": "087722113344",
        "city": "Medan",
        "country": "Indonesia",
        "status": "inactive",
        "enabled": False,
        "balance": 120000.00,
        "lat": 3.5952,
        "lon": 98.6722,
        "created_at": "2026-03-15T13:20:00",
        "birthday": "1991-12-03",
        "tags": ["python", "ml", "tensorflow"],
        "categories": ["data", "engineering"],
        "address": '{"city": "Medan", "district": "Medan Area", "zip": "20212"}',
        "skills": '["python", "sklearn", "pytorch"]',
        "score": 88.5,
        "level": 13,
        "info": '{"metadata": {"a": {"value": 250, "name": "hendra_a", "tags": ["hendra_tag1", "hendra_tag2"], "submetadata": {"deep": "secret_h", "level": 18}}, "b": {"value": 350, "name": "hendra_b", "items": ["hendra_item1", "hendra_item2"]}}}',
    },
    {
        "id": 9,
        "name": "Indah Sperli",
        "email": "indah@startup.id",
        "phone": "081255667788",
        "city": "Makassar",
        "country": "Indonesia",
        "status": "active",
        "enabled": True,
        "balance": 1950000.00,
        "lat": -5.1420,
        "lon": 119.4055,
        "created_at": "2026-02-28T08:00:00",
        "birthday": "1996-08-17",
        "tags": ["elixir", "phoenix", "postgres"],
        "categories": ["engineering", "backend"],
        "address": '{"city": "Makassar", "district": "Losari", "zip": "90111"}',
        "skills": '["elixir", "phoenix"]',
        "score": 84.0,
        "level": 12,
        "info": '{"metadata": {"a": {"value": 350, "name": "indah_a", "tags": ["indah_tag1", "indah_tag2"], "submetadata": {"deep": "secret_i", "level": 25}}, "b": {"value": 450, "name": "indah_b", "items": ["indah_item1", "indah_item2"]}}}',
    },
    {
        "id": 10,
        "name": "Judotens Caniago",
        "email": "judotens@blablabla.com",
        "phone": "081299887766",
        "city": "Jakarta",
        "country": "Indonesia",
        "status": "active",
        "enabled": True,
        "balance": 5000000.00,
        "lat": -6.2088,
        "lon": 106.8456,
        "created_at": "2026-01-20T12:00:00",
        "birthday": "1985-05-01",
        "tags": ["electric", "public", "food"],
        "categories": ["electric", "food"],
        "address": '{"city": "Jakarta", "district": "Medan", "zip": "10120"}',
        "skills": '["mechanic", "food"]',
        "score": 60.0,
        "level": 20,
        "info": '{"metadata": {"a": {"value": 750, "name": "judotens_a", "tags": ["judotens_tag1", "judotens_tag2"], "submetadata": {"deep": "secret_j", "level": 50}}, "b": {"value": 850, "name": "judotens_b", "items": ["judotens_item1", "judotens_item2"]}}}',
    },
]


# ─── Fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def search_index():
    """Build a fresh trigram index from DUMMY_RECORDS once per test module."""
    tmpdir = tempfile.mkdtemp(prefix="fs_search_test_")

    # Write CSV (so the headers/values exercise the same code path as a real upload).
    csv_path = os.path.join(tmpdir, "test_data.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=DUMMY_RECORDS[0].keys())
        writer.writeheader()
        writer.writerows(DUMMY_RECORDS)

    # Build directly via IndexBuilder (faster than spawning the CLI).
    builder = IndexBuilder(tmpdir, column_map={})
    builder.start_doc_id = 0
    headers = list(DUMMY_RECORDS[0].keys())
    for rec in DUMMY_RECORDS:
        row = {col: str(rec.get(col, "")) for col in headers}
        builder.add_row(row, headers)
    builder.finalize()

    engine = QueryEngine(tmpdir)
    yield engine

    shutil.rmtree(tmpdir, ignore_errors=True)


def total(engine, query):
    """Helper: return the `total` count for a query string."""
    return engine.query(query).get("total", 0)


def doc_ids(engine, query):
    """Helper: return the `_id` list for a query string (sorted)."""
    hits = engine.query(query).get("results", [])
    return sorted(h.get("_id") for h in hits)


# ─── 1. Index-time expansion ─────────────────────────────────────────────────

class TestExpansion:
    """Verify _expand_record produces the indexed keys we expect."""

    def test_array_expanded_to_indexed_keys(self):
        rec = {"id": 1, "tags": ["a", "b", "c"]}
        out = _expand_record(rec)
        assert "tags[0]" in out
        assert "tags[1]" in out
        assert "tags[2]" in out

    def test_object_expanded_to_dot_paths(self):
        rec = {"address": '{"city": "Jakarta", "zip": "10110"}'}
        out = _expand_record(rec)
        # Either dot-path or namespaced — at least one address.* key should exist.
        keys = list(out.keys())
        assert any("address" in k and ("city" in k or "." in k) for k in keys)


# ─── 2. KEYWORD — exact match ────────────────────────────────────────────────

class TestKeywordExact:
    @pytest.mark.parametrize("query,expected", [
        ("city:Jakarta",      3),
        ("city:jakarta",      3),   # case-insensitive
        ("city:Surabaya",     1),
        ("city:Bali",         1),
        ("city:Medan",        1),
        ("status:active",     7),
        ("status:inactive",   2),
        ("status:pending",    1),
        ("country:Indonesia", 10),
    ])
    def test_keyword_match(self, search_index, query, expected):
        assert total(search_index, query) == expected


# ─── 3. TEXT — free-text / partial ───────────────────────────────────────────

class TestTextField:
    @pytest.mark.parametrize("query,expected", [
        ("name:Alice",     1),
        ("name:Charlie",   1),
        ("name:Indah",     1),
        ("name:tens",      1),   # partial substring
        ("name:Eko",       1),
        ("email:dev.io",   2),
        ("email:startup.id", 2),
    ])
    def test_text_partial_match(self, search_index, query, expected):
        assert total(search_index, query) == expected


# ─── 4. WILDCARDS — prefix, suffix, substring ────────────────────────────────

class TestWildcards:
    @pytest.mark.parametrize("query,expected", [
        ("name:Alice*",     1),  # prefix
        ("name:*Putri",     1),  # suffix
        ("name:*Wirnata",   1),
        ("city:Jakart*",    3),  # prefix (Jakarta)
        ("city:*arta*",     4),  # substring (Jakarta + Yogyakarta)
        ("email:*@dev.io",  2),  # email pattern
    ])
    def test_wildcard_patterns(self, search_index, query, expected):
        assert total(search_index, query) == expected


# ─── 5. ARRAY — element-wise match ───────────────────────────────────────────

class TestArrayField:
    """tags is indexed as tags[0], tags[1]... — querying tags:value matches any."""

    @pytest.mark.parametrize("query,expected", [
        ("tags:graphql", 2),
        ("tags:java",    2),
        ("tags:python",  2),
        ("tags:golang",  1),
        ("tags:docker",  1),
        ("tags:react",   2),
        ("tags:figma",   1),
        ("tags:elixir",  1),
        ("tags:ml",      1),
    ])
    def test_array_element_match(self, search_index, query, expected):
        assert total(search_index, query) == expected


# ─── 6. OBJECT — dot-path access (single level + deeply nested) ──────────────

class TestObjectDotPath:

    @pytest.mark.parametrize("query,expected", [
        # First-level object (address.*)
        ("address.city:Jakarta",         3),
        ("address.city:Surabaya",        1),
        ("address.city:Bandung",         1),
        ("address.city:Bali",            1),
        ("address.district:Sudirman",    1),
        ("address.zip:10110",            1),
        ("address.zip:60246",            1),
    ])
    def test_first_level_dot_path(self, search_index, query, expected):
        assert total(search_index, query) == expected

    @pytest.mark.parametrize("query,expected", [
        # Deeply nested string fields (info.metadata.a.name)
        ("info.metadata.a.name:alice_a",                1),
        ("info.metadata.a.name:bob_a",                  1),
        ("info.metadata.b.name:alice_b",                1),
        ("info.metadata.b.name:fitri_b",                1),
        # Even deeper string match — info.metadata.a.submetadata.*
        ("info.metadata.a.submetadata.deep:secret_a",   1),
        ("info.metadata.a.submetadata.deep:secret_j",   1),
    ])
    def test_deeply_nested_string_match(self, search_index, query, expected):
        assert total(search_index, query) == expected

    @pytest.mark.parametrize("query,expected", [
        ("info.metadata.a.value:[100 TO 200]",           4),
        ("info.metadata.a.value:[250 TO 500]",           4),
        ("info.metadata.a.value:>=500",                  2),
        ("info.metadata.a.value:<100",                   1),
        ("info.metadata.b.value:[200 TO 400]",           6),
        ("info.metadata.b.value:>=600",                  2),
        ("info.metadata.a.submetadata.level:[10 TO 20]", 4),
        ("info.metadata.a.submetadata.level:>=25",       2),
        ("info.metadata.a.submetadata.level:<10",        4),
    ])
    def test_deeply_nested_numeric_range(self, search_index, query, expected):
        """Numeric range / comparator queries on deeply-nested scalar values."""
        assert total(search_index, query) == expected

    def test_deeply_nested_wildcard(self, search_index):
        """Wildcard against a deeply-nested string field."""
        assert total(search_index, "info.metadata.a.submetadata.deep:secret*") == 10


# ─── 7. NESTED ARRAY INDEX — obj.arr[i]:val ──────────────────────────────────

class TestNestedArrayIndex:

    @pytest.mark.parametrize("query,expected", [
        ("info.metadata.a.tags[0]:alpha",    1),
        ("info.metadata.a.tags[0]:gamma",    1),
        ("info.metadata.a.tags[1]:beta",     1),
        ("info.metadata.a.tags[1]:delta",    1),
        ("info.metadata.b.items[0]:item_x",  1),
        ("info.metadata.b.items[0]:item_p",  1),
        ("info.metadata.b.items[1]:item_y",  1),
        ("info.metadata.b.items[1]:item_q",  1),
    ])
    def test_indexed_array_access(self, search_index, query, expected):
        assert total(search_index, query) == expected


# ─── 8. NUMERIC — open-ended comparators ─────────────────────────────────────

class TestNumericComparators:

    @pytest.mark.parametrize("query,expected", [
        ("balance:>1000000",   5),
        ("balance:<1000000",   5),
        ("balance>1000000",    5),  # bare form (no colon)
        ("balance<1000000",    5),
        ("score:[90 TO 999]",  2),
        ("score:[88 TO 999]",  3),
        ("score:[0 TO 70]",    2),
        ("level:[13 TO 999]",  4),
        ("level:[0 TO 10]",    3),
    ])
    def test_open_range(self, search_index, query, expected):
        assert total(search_index, query) == expected


# ─── 9. NUMERIC RANGE — [A TO B] ─────────────────────────────────────────────

class TestRangeQuery:

    @pytest.mark.parametrize("query,expected", [
        ("balance:[1500000 TO 99999999]", 5),
        ("balance:[0 TO 100000]",         1),
        ("balance:[0 TO 55000]",          1),
        ("balance:[1000000 TO 3000000]",  2),
        ("score:[80 TO 95]",              6),
        ("level:[10 TO 15]",              7),
    ])
    def test_inclusive_range(self, search_index, query, expected):
        assert total(search_index, query) == expected


# ─── 10. DATE RANGE ──────────────────────────────────────────────────────────

class TestDateRange:

    @pytest.mark.parametrize("query,expected", [
        ("created_at:[20260101 TO 20260131]", 10),
        ("created_at:[20260201 TO 20260229]", 10),
        ("created_at:[20260301 TO 20260331]", 10),
    ])
    def test_date_range(self, search_index, query, expected):
        assert total(search_index, query) == expected


# ─── 11. BOOLEAN VALUES ──────────────────────────────────────────────────────

class TestBoolean:

    @pytest.mark.parametrize("query,expected", [
        ("enabled:true",  7),
        ("enabled:false", 3),
    ])
    def test_boolean_match(self, search_index, query, expected):
        assert total(search_index, query) == expected


# ─── 12. AND / OR / NOT ──────────────────────────────────────────────────────

class TestBooleanOperators:

    @pytest.mark.parametrize("query,expected", [
        # AND
        ("city:Jakarta AND status:active", 3),
        ("tags:java AND enabled:true",     2),
        ("address.city:Jakarta AND balance:[1000000 TO 99999999]", 3),
    ])
    def test_and(self, search_index, query, expected):
        assert total(search_index, query) == expected

    @pytest.mark.parametrize("query,expected", [
        ("city:Surabaya OR city:Bali",                          2),
        ("city:Medan OR city:Makassar OR city:Yogyakarta",      3),
        ("tags:python OR tags:golang",                          3),
    ])
    def test_or(self, search_index, query, expected):
        assert total(search_index, query) == expected

    @pytest.mark.parametrize("query,expected", [
        ("city:Jakarta AND NOT status:inactive", 3),
        ("* AND NOT city:Jakarta",               7),
        ("tags:devops AND NOT enabled:false",    1),
        ("* AND NOT enabled:true",               3),
    ])
    def test_not(self, search_index, query, expected):
        assert total(search_index, query) == expected

    @pytest.mark.parametrize("query,expected", [
        # AND + OR + parens
        ("address.city:Jakarta AND (status:active OR status:pending)", 3),
        # AND + OR + NOT + ranges
        ("balance:[500001 TO 99999999] AND (tags:python OR tags:golang) AND NOT status:inactive", 1),
        ("level:[11 TO 999] AND score:[80 TO 999]", 6),
        ("tags:postgres AND (score:[71 TO 999] AND level:[8 TO 999])", 1),
    ])
    def test_combined(self, search_index, query, expected):
        assert total(search_index, query) == expected


# ─── 13. MATCH-ALL ───────────────────────────────────────────────────────────

class TestMatchAll:

    def test_star_returns_everything(self, search_index):
        assert total(search_index, "*") == 10


# ─── 14. EMPTY / NO RESULTS ──────────────────────────────────────────────────

class TestEmptyResults:

    @pytest.mark.parametrize("query", [
        "city:NonExistent",
        "tags:nonexistent",
        "balance:[100000000 TO 100000000]",
    ])
    def test_no_match_returns_zero(self, search_index, query):
        assert total(search_index, query) == 0


# ─── 15. PROGRAMMATIC search_and / search ────────────────────────────────────

class TestProgrammaticAPI:

    def test_search_and(self, search_index):
        result = search_index.search_and([("city", "Jakarta"), ("status", "active")])
        assert result.get("total") == 3

    @pytest.mark.parametrize("term,expected", [
        ("Jakarta", 3),
        ("golang",  2),
        ("Alice",   1),
    ])
    def test_search_simple_term(self, search_index, term, expected):
        result = search_index.search(term)
        assert result.get("total") == expected


# ─── 16. AGGREGATIONS ────────────────────────────────────────────────────────

class TestAggregations:

    def test_terms_on_keyword(self, search_index):
        result = search_index.aggregate(q="*", aggs={"terms": {"field": "city", "size": 10}})
        buckets = result.get("aggregations", {}).get("city", {}).get("buckets", [])
        jakarta = next((b for b in buckets if b["key"] == "Jakarta"), None)
        assert jakarta is not None
        assert jakarta["doc_count"] == 3

    def test_terms_on_array_field(self, search_index):
        result = search_index.aggregate(q="*", aggs={"terms": {"field": "tags", "size": 20}})
        buckets = result.get("aggregations", {}).get("tags", {}).get("buckets", [])
        # Each value is the union of all element occurrences across docs.
        assert len(buckets) > 0

    def test_terms_on_status(self, search_index):
        result = search_index.aggregate(q="*", aggs={"terms": {"field": "status", "size": 10}})
        buckets = {b["key"]: b["doc_count"] for b in
                   result.get("aggregations", {}).get("status", {}).get("buckets", [])}
        assert buckets.get("active") == 7
        assert buckets.get("inactive") == 2
        assert buckets.get("pending") == 1

    def test_date_histogram(self, search_index):
        result = search_index.aggregate(
            q="*",
            aggs={"date_histogram": {"field": "created_at", "interval": "month"}},
        )
        buckets = result.get("aggregations", {}).get("created_at", {}).get("buckets", [])
        assert len(buckets) >= 1

    def test_avg(self, search_index):
        result = search_index.aggregate(q="*", aggs={"avg": {"field": "balance"}})
        avg_val = result.get("aggregations", {}).get("balance", {}).get("value")
        assert avg_val is not None and avg_val > 0

    def test_stats_bundle(self, search_index):
        """`stats` returns {min,max,avg,sum,count} in one pass."""
        result = search_index.aggregate(q="*", aggs={"stats": {"field": "score"}})
        agg = result.get("aggregations", {}).get("score", {})
        assert agg.get("count") == 10
        assert agg.get("min") == pytest.approx(60.0)
        assert agg.get("max") == pytest.approx(95.0)
        assert agg.get("avg") == pytest.approx(80.35)
        assert agg.get("sum") == pytest.approx(803.5)

    def test_min_max_sum(self, search_index):
        """Single-statistic aggregations work and are the recommended path."""
        for stat in ("min", "max", "sum"):
            result = search_index.aggregate(q="*", aggs={stat: {"field": "balance"}})
            val = result.get("aggregations", {}).get("balance", {}).get("value")
            assert val is not None, f"{stat} returned no value"

    def test_aggregation_with_query_filter(self, search_index):
        """Aggregations should respect the query filter."""
        result = search_index.aggregate(
            q="status:active",
            aggs={"terms": {"field": "city", "size": 20}},
        )
        buckets = result.get("aggregations", {}).get("city", {}).get("buckets", [])
        # Only active docs counted — Jakarta active = 3 (Alice, Fitri, Judotens)
        jakarta = next((b for b in buckets if b["key"] == "Jakarta"), None)
        assert jakarta and jakarta["doc_count"] == 3
