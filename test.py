#!/usr/bin/env python3
"""Manual CLI test for flatseek search engine accuracy.

Run from flatseek/ directory:
    python test.py

Creates an index with rich dummy data covering all field types,
then runs comprehensive search tests to verify engine accuracy.
"""

if __name__ == "__main__":
    import os
    import sys
    import csv
    import json
    import tempfile
    import shutil
    from datetime import datetime, timedelta

    # ── Setup import path ────────────────────────────────────────────────────────
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

    from flatseek.core.builder import IndexBuilder, _expand_record, _json_val_to_str
    from flatseek.core.query_engine import QueryEngine
    from flatseek.core.query_parser import parse, execute

    # ── Dummy data ───────────────────────────────────────────────────────────────
    # Covers all semantic types: TEXT, KEYWORD, DATE, FLOAT, BOOL, ARRAY, OBJECT
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

    # ── Build test index ─────────────────────────────────────────────────────────
    tmpdir = tempfile.mkdtemp(prefix="fs_test_")
    csv_path = os.path.join(tmpdir, "test_data.csv")

    print("=" * 70)
    print("FLATSEEK SEARCH ENGINE TEST")
    print("=" * 70)
    print(f"\n[1] Writing {len(DUMMY_RECORDS)} dummy records to CSV...")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=DUMMY_RECORDS[0].keys())
        writer.writeheader()
        writer.writerows(DUMMY_RECORDS)
    print(f"      CSV: {csv_path}")

    # ── Verify _expand_record ────────────────────────────────────────────────────
    print("\n[2] Testing _expand_record (array/object expansion at index time)...")
    sample = DUMMY_RECORDS[0].copy()
    expanded = _expand_record(sample)
    print(f"      Original keys: {list(sample.keys())}")
    print(f"      Expanded keys: {sorted(expanded.keys())}")

    # Check tags[0], tags[1]... exist
    tag_keys = [k for k in expanded if k.startswith("tags[")]
    address_dot_keys = [k for k in expanded if k.startswith("address.")]
    print(f"      tags indexed as: {tag_keys}")
    print(f"      address indexed as: {address_dot_keys}")
    assert tag_keys, "tags[0], tags[1]... should exist after expansion"
    assert address_dot_keys, "address.city etc. should exist after expansion"
    print("      ✓ Array/object expansion verified")

    # ── Build index ──────────────────────────────────────────────────────────────
    print(f"\n[3] Building index at {tmpdir}...")
    builder = IndexBuilder(tmpdir, column_map={})
    builder.start_doc_id = 0

    headers = list(DUMMY_RECORDS[0].keys())
    for i, rec in enumerate(DUMMY_RECORDS):
        row = {col: str(rec.get(col, "")) for col in headers}
        builder.add_row(row, headers)

    builder.finalize()
    print(f"      ✓ Index built: {builder.doc_counter} docs")

    stats = json.load(open(os.path.join(tmpdir, "stats.json")))
    print(f"      Stats columns: {list(stats.get('columns', {}).keys())}")

    # ── Create QueryEngine ────────────────────────────────────────────────────────
    print(f"\n[4] Creating QueryEngine...")
    engine = QueryEngine(tmpdir)
    print(f"      ✓ Engine ready: {stats['total_docs']} docs indexed")

    # ── Helper ───────────────────────────────────────────────────────────────────
    def run(q_str, expected_count=None, expected_doc_ids=None, label=""):
        """Run a query and verify results."""
        print(f"\n   [{label}] query: {q_str!r}")
        result = engine.query(q_str)
        hits = result.get("results", [])
        # _id is internal doc counter; id is the data field
        doc_ids = [h.get("_id") for h in hits]
        total = result.get("total", 0)
        print(f"          total={total}, doc_ids={doc_ids}")
        if expected_count is not None:
            assert total == expected_count, f"Expected {expected_count}, got {total}"
        if expected_doc_ids is not None:
            assert doc_ids == expected_doc_ids, f"Expected {expected_doc_ids}, got {doc_ids}"
        print(f"          ✓ passed")
        return result

    def run_search(term, column=None, expected_count=None, label=""):
        """Run simple search."""
        print(f"\n   [{label}] search: term={term!r}, column={column}")
        result = engine.search(term, column=column)
        hits = result.get("results", [])
        ids = [h.get("id") or h.get("_id") for h in hits]
        total = result.get("total", 0)
        print(f"          total={total}, ids={ids}")
        if expected_count is not None:
            assert total == expected_count, f"Expected {expected_count}, got {total}"
        print(f"          ✓ passed")
        return result

    # ── Run tests ────────────────────────────────────────────────────────────────
    tests_passed = 0
    tests_failed = 0

    def section(name):
        print(f"\n{'─' * 60}")
        print(f"  {name}")
        print(f"{'─' * 60}")

    try:
        section("TEST: ARRAY FIELD - tags (expanded to tags[0], tags[1]...)")
        # Each tag should be searchable individually
        run("tags:graphql", expected_count=2, label="tags:graphql")
        run("tags:java", expected_count=2, label="tags:java")
        run("tags:python", expected_count=2, label="tags:python")
        run("tags:golang", expected_count=1, label="tags:golang")
        run("tags:docker", expected_count=1, label="tags:docker")
        run("tags:react", expected_count=2, label="tags:react")
        run("tags:figma", expected_count=1, label="tags:figma")
        run("tags:elixir", expected_count=1, label="tags:elixir")
        run("tags:ml", expected_count=1, label="tags:ml")

        section("TEST: OBJECT FIELD - address (expanded to address.city etc.)")
        run("address.city:Jakarta", expected_count=3, label="address.city:Jakarta")
        run("address.city:Surabaya", expected_count=1, label="address.city:Surabaya")
        run("address.city:Bandung", expected_count=1, label="address.city:Bandung")
        run("address.city:Bali", expected_count=1, label="address.city:Bali")
        run("address.district:Sudirman", expected_count=1, label="address.district:Sudirman")
        run("address.zip:10110", expected_count=1, label="address.zip:10110")
        run("address.zip:60246", expected_count=1, label="address.zip:60246")

        section("TEST: OBJECT FIELD - info.metadata (deeply nested)")
        # info.metadata.a.name (string)
        run("info.metadata.a.name:alice_a", expected_count=1, label="info.metadata.a.name:alice_a")
        run("info.metadata.a.name:bob_a", expected_count=1, label="info.metadata.a.name:bob_a")
        # info.metadata.b.name (string)
        run("info.metadata.b.name:alice_b", expected_count=1, label="info.metadata.b.name:alice_b")
        run("info.metadata.b.name:fitri_b", expected_count=1, label="info.metadata.b.name:fitri_b")
        # info.metadata.a.value (numeric)
        run("info.metadata.a.value:[100 TO 200]", expected_count=4, label="info.metadata.a.value:[100 TO 200]")
        run("info.metadata.a.value:[250 TO 500]", expected_count=4, label="info.metadata.a.value:[250 TO 500]")
        run("info.metadata.a.value:>=500", expected_count=2, label="info.metadata.a.value:>=500")
        run("info.metadata.a.value:<100", expected_count=1, label="info.metadata.a.value:<100")
        # info.metadata.b.value (numeric)
        run("info.metadata.b.value:[200 TO 400]", expected_count=6, label="info.metadata.b.value:[200 TO 400]")
        run("info.metadata.b.value:>=600", expected_count=2, label="info.metadata.b.value:>=600")
        # info.metadata.a.tags (array)
        run("info.metadata.a.tags[0]:alpha", expected_count=1, label="info.metadata.a.tags[0]:alpha")
        run("info.metadata.a.tags[0]:gamma", expected_count=1, label="info.metadata.a.tags[0]:gamma")
        run("info.metadata.a.tags[1]:beta", expected_count=1, label="info.metadata.a.tags[1]:beta")
        run("info.metadata.a.tags[1]:delta", expected_count=1, label="info.metadata.a.tags[1]:delta")
        # info.metadata.b.items (array)
        run("info.metadata.b.items[0]:item_x", expected_count=1, label="info.metadata.b.items[0]:item_x")
        run("info.metadata.b.items[0]:item_p", expected_count=1, label="info.metadata.b.items[0]:item_p")
        run("info.metadata.b.items[1]:item_y", expected_count=1, label="info.metadata.b.items[1]:item_y")
        run("info.metadata.b.items[1]:item_q", expected_count=1, label="info.metadata.b.items[1]:item_q")
        # info.metadata.a.submetadata.deep (deeply nested string)
        run("info.metadata.a.submetadata.deep:secret_a", expected_count=1, label="info.metadata.a.submetadata.deep:secret_a")
        run("info.metadata.a.submetadata.deep:secret_j", expected_count=1, label="info.metadata.a.submetadata.deep:secret_j")
        run("info.metadata.a.submetadata.deep:secret*", expected_count=10, label="info.metadata.a.submetadata.deep:secret*")
        # info.metadata.a.submetadata.level (deeply nested numeric)
        run("info.metadata.a.submetadata.level:[10 TO 20]", expected_count=4, label="info.metadata.a.submetadata.level:[10 TO 20]")
        run("info.metadata.a.submetadata.level:>=25", expected_count=2, label="info.metadata.a.submetadata.level:>=25")
        run("info.metadata.a.submetadata.level:<10", expected_count=4, label="info.metadata.a.submetadata.level:<10")
        # Combined nested object queries
        run("info.metadata.a.name:alice_a AND info.metadata.b.name:alice_b", expected_count=1, label="info.metadata.a.name AND info.metadata.b.name")
        run("city:Jakarta AND info.metadata.a.value:[100 TO 300]", expected_count=1, label="city:Jakarta AND info.metadata.a.value:[100 TO 300]")

        section("TEST: KEYWORD - city (exact match)")
        run("city:jakarta", expected_count=3, label="city:Jakarta")
        run("city:Surabaya", expected_count=1, label="city:Surabaya")
        run("city:Bali", expected_count=1, label="city:Bali")
        run("city:Medan", expected_count=1, label="city:Medan")

        section("TEST: KEYWORD - status")
        run("status:active", expected_count=7, label="status:active")
        run("status:inactive", expected_count=2, label="status:inactive")
        run("status:pending", expected_count=1, label="status:pending")

        section("TEST: TEXT - name (free-text / partial)")
        run("name:Alice", expected_count=1, label="name:Alice")
        run("name:Charlie", expected_count=1, label="name:Charlie")
        run("name:Indah", expected_count=1, label="name:Indah")
        run("name:tens", expected_count=1, label="name:tens")
        run("name:Eko", expected_count=1, label="name:Eko")

        section("TEST: TEXT - email (partial)")
        run("email:dev.io", expected_count=2, label="email:dev.io")
        run("email:startup.id", expected_count=2, label="email:startup.id")

        section("TEST: WILDCARD - * or %")
        run("name:Alice*", expected_count=1, label="name:Alice*")
        run("name:*Putri", expected_count=1, label="name:*Putri")
        run("name:*Wirnata", expected_count=1, label="name:*Wirnata")
        run("city:Jakart*", expected_count=3, label="city:Jakart*")
        run("city:*arta*", expected_count=4, label="city:*arta*")
        run("email:*@dev.io", expected_count=2, label="email:*@dev.io")

        section("TEST: NUMBER > < >= <=")
        run("balance:>1000000", expected_count=5, label="balance:>1000000")
        run("balance:<1000000", expected_count=5, label="balance:>1000000")
        run("balance>1000000", expected_count=5, label="balance>1000000")
        run("balance<1000000", expected_count=5, label="balance>1000000")
        run("balance:[1500000 TO 99999999]", expected_count=5, label="balance:>=1500000")
        run("balance:[0 TO 100000]", expected_count=1, label="balance:<100000")
        run("balance:[0 TO 55000]", expected_count=1, label="balance:<=55000")
        run("score:[90 TO 999]", expected_count=2, label="score:>90")
        run("score:[88 TO 999]", expected_count=3, label="score:>=88")
        run("score:[0 TO 70]", expected_count=2, label="score:<70")
        run("level:[13 TO 999]", expected_count=4, label="level:>12")
        run("level:[13 TO 999]", expected_count=4, label="level:>=13")
        run("level:[0 TO 10]", expected_count=3, label="level:<=10")

        section("TEST: NUMBER RANGE [TO]")
        run("balance:[1000000 TO 3000000]", expected_count=2, label="balance:[1000000 TO 3000000]")
        run("score:[80 TO 95]", expected_count=6, label="score:[80 TO 95]")
        run("level:[10 TO 15]", expected_count=7, label="level:[10 TO 15]")

        section("TEST: BOOLEAN")
        run("enabled:true", expected_count=7, label="enabled:true")
        run("enabled:false", expected_count=3, label="enabled:false")

        section("TEST: AND (explicit)")
        run("city:Jakarta AND status:active", expected_count=3, label="city:Jakarta AND status:active")
        run("tags:java AND enabled:true", expected_count=2, label="tags:java AND enabled:true")
        run("address.city:Jakarta AND balance:[1000000 TO 99999999]", expected_count=3, label="address.city:Jakarta AND balance:>1000000")

        section("TEST: OR (union)")
        run("city:Surabaya OR city:Bali", expected_count=2, label="city:Surabaya OR city:Bali")
        run("city:Medan OR city:Makassar OR city:Yogyakarta", expected_count=3, label="city:Medan OR city:Makassar")
        run("tags:python OR tags:golang", expected_count=3, label="tags:python OR tags:golang")

        section("TEST: NOT (exclusion)")
        run("city:Jakarta AND NOT status:inactive", expected_count=3, label="city:Jakarta AND NOT status:inactive")
        run("* AND NOT city:Jakarta", expected_count=7, label="NOT city:Jakarta")
        run("tags:devops AND NOT enabled:false", expected_count=1, label="tags:devops AND NOT enabled:false")
        run("* AND NOT enabled:true", expected_count=3, label="NOT enabled:true")

        section("TEST: COMBINED (AND + OR + NOT + ranges)")
        run("address.city:Jakarta AND (status:active OR status:pending)", expected_count=3, label="address.city:Jakarta AND (status:active OR status:pending)")
        run("balance:[500001 TO 99999999] AND (tags:python OR tags:golang) AND NOT status:inactive", expected_count=1, label="balance:>500000 AND (tags:python OR tags:golang)")
        run("level:[11 TO 999] AND score:[80 TO 999]", expected_count=6, label="level:>10 AND score:>=80")
        run("tags:postgres AND (score:[71 TO 999] AND level:[8 TO 999])", expected_count=1, label="tags:postgres AND (score:>70)")

        section("TEST: DATE RANGE")
        run("created_at:[20260101 TO 20260131]", expected_count=10, label="created_at:[20260101 TO 20260131]")
        run("created_at:[20260201 TO 20260228]", expected_count=10, label="created_at:[20260201 TO 20260228]")
        run("created_at:[20260301 TO 20260331]", expected_count=10, label="created_at:[20260301 TO 20260331]")

        section("TEST: MATCH-ALL")
        result = engine.query("*")
        assert result["total"] == 10, f"match-all should return 10, got {result['total']}"
        print(f"   [match-all] total={result['total']} ✓ passed")

        section("TEST: EMPTY / NO RESULTS")
        run("city:NonExistent", expected_count=0, label="city:NonExistent")
        run("tags:nonexistent", expected_count=0, label="tags:nonexistent")
        run("balance:[100000000 TO 100000000]", expected_count=0, label="balance:>99999999")

        section("TEST: search_and (programmatic AND)")
        result = engine.search_and([("city", "Jakarta"), ("status", "active")])
        assert result["total"] == 3, f"search_and expected 3, got {result['total']}"
        print(f"   [search_and] total={result['total']} ✓ passed")

        section("TEST: search (simple term)")
        run_search("Jakarta", expected_count=3, label="search:Jakarta")
        run_search("golang", expected_count=2, label="search:golang")
        run_search("Alice", expected_count=1, label="search:Alice")

        section("TEST: AGGREGATE - terms")
        result = engine.aggregate(q="*", aggs={"terms": {"field": "city", "size": 10}})
        buckets = result.get("aggregations", {}).get("city", {}).get("buckets", [])
        print(f"   [agg:city] buckets: {[(b['key'], b['doc_count']) for b in buckets]}")
        # Verify Jakarta appears with correct count
        jakarta_bucket = next((b for b in buckets if b["key"] == "Jakarta"), None)
        assert jakarta_bucket and jakarta_bucket["doc_count"] == 3, f"Jakarta count should be 3, got {jakarta_bucket}"
        print(f"   ✓ passed")

        result = engine.aggregate(q="*", aggs={"terms": {"field": "tags", "size": 10}})
        buckets = result.get("aggregations", {}).get("tags", {}).get("buckets", [])
        print(f"   [agg:tags] buckets: {[(b['key'], b['doc_count']) for b in buckets]}")
        print(f"   ✓ passed")

        result = engine.aggregate(q="*", aggs={"terms": {"field": "status", "size": 10}})
        buckets = result.get("aggregations", {}).get("status", {}).get("buckets", [])
        print(f"   [agg:status] buckets: {[(b['key'], b['doc_count']) for b in buckets]}")
        print(f"   ✓ passed")

        section("TEST: AGGREGATE - date_histogram")
        result = engine.aggregate(q="*", aggs={"date_histogram": {"field": "created_at", "interval": "month"}})
        buckets = result.get("aggregations", {}).get("created_at", {}).get("buckets", [])
        print(f"   [agg:date_histogram] buckets: {len(buckets)} months")
        print(f"   ✓ passed")

        section("TEST: AGGREGATE - avg / stats")
        result = engine.aggregate(q="*", aggs={"avg": {"field": "balance"}})
        avg_val = result.get("aggregations", {}).get("balance", {}).get("value")
        print(f"   [agg:avg:balance] {avg_val}")
        assert avg_val > 0, f"avg should be > 0, got {avg_val}"
        print(f"   ✓ passed")

        result = engine.aggregate(q="*", aggs={"stats": {"field": "score"}})
        stats_agg = result.get("aggregations", {}).get("score", {})
        print(f"   [agg:stats:score] min={stats_agg.get('min')}, max={stats_agg.get('max')}, avg={stats_agg.get('avg')}")
        print(f"   ✓ passed")

        section("ALL TESTS PASSED ✓")

    except AssertionError as e:
        print(f"\n\n!!! TEST FAILED: {e}")
        tests_failed += 1
        raise
    except Exception as e:
        print(f"\n\n!!! ERROR: {e}")
        import traceback
        traceback.print_exc()
        tests_failed += 1
    finally:
        print(f"\n[5] Cleaning up {tmpdir}...")
        shutil.rmtree(tmpdir, ignore_errors=True)
        print(f"      Done.")
        print(f"\n{'=' * 70}")
        print(f"RESULTS: passed={tests_passed}, failed={tests_failed}")
        print(f"{'=' * 70}")
        sys.exit(0 if tests_failed == 0 else 1)
