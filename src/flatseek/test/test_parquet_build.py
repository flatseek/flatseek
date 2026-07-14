"""Tests for building flatseek index directly from Parquet files."""

import os
import sys
import tempfile
import shutil
import pytest
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from flatseek import Flatseek
from flatseek.cli import cmd_build


def _create_parquet_file(path, records):
    """Create a Parquet file from a list of dict records using PyArrow."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_pylist(records)
    pq.write_table(table, path)


def _build_index_from_parquet(pq_path, output_dir):
    """Build a flatseek index from a Parquet file. Returns the output_dir."""
    ns = argparse.Namespace(
        csv_dir=pq_path,
        output=output_dir,
        map=None,
        dataset=None,
        sep=",",
        columns=None,
        workers=1,
        plan=None,
        worker_id=None,
        estimate=False,
        dedup=False,
        dedup_fields=None,
        daemon=False,
    )
    cmd_build(ns)
    return output_dir


class TestParquetBuild:
    """Build index from Parquet and verify search works."""

    def setup_method(self):
        """Create a temp directory for each test."""
        self.tmpdir = tempfile.mkdtemp(prefix="fs_parquet_test_")

    def teardown_method(self):
        """Clean up temp directory."""
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_parquet_basic_build_and_search(self):
        """Create a Parquet file, build index, verify basic search works."""
        pq_path = os.path.join(self.tmpdir, "people.parquet")
        records = [
            {"id": 1, "name": "Alice Johnson", "email": "alice@example.com",
             "city": "Jakarta", "country": "Indonesia", "age": 30, "active": True},
            {"id": 2, "name": "Bob Smith", "email": "bob@example.com",
             "city": "Surabaya", "country": "Indonesia", "age": 25, "active": False},
            {"id": 3, "name": "Carol White", "email": "carol@example.com",
             "city": "Bandung", "country": "Indonesia", "age": 35, "active": True},
            {"id": 4, "name": "David Brown", "email": "david@example.com",
             "city": "Jakarta", "country": "Indonesia", "age": 28, "active": True},
            {"id": 5, "name": "Eve Davis", "email": "eve@example.com",
             "city": "Yogyakarta", "country": "Indonesia", "age": 32, "active": False},
        ]
        _create_parquet_file(pq_path, records)

        index_path = os.path.join(self.tmpdir, "people.fsk")
        _build_index_from_parquet(pq_path, index_path)

        fs = Flatseek(index_path)

        # Verify total docs
        stats = fs.stats()
        assert stats["total_docs"] == 5, f"Expected 5 docs, got {stats['total_docs']}"

        # Keyword search — exact field:value
        result = fs.search("city:Jakarta")
        assert result.hits["total"] == 2, f"Expected 2 hits for Jakarta, got {result.hits['total']}"
        names = {hit["_source"]["name"] for hit in result.hits["hits"]}
        assert names == {"Alice Johnson", "David Brown"}, f"Unexpected names: {names}"

        # Email search
        result = fs.search("email:bob@example.com")
        assert result.hits["total"] == 1
        assert result.hits["hits"][0]["_source"]["name"] == "Bob Smith"

        # Active status (boolean)
        result = fs.search("active:true")
        assert result.hits["total"] == 3, f"Expected 3 active users, got {result.hits['total']}"

        # Wildcard search (prefix wildcard on TEXT field)
        result = fs.search("name:Alice*")
        assert result.hits["total"] == 1, f"Expected 1 hit for Alice*, got {result.hits['total']}"
        assert result.hits["hits"][0]["_source"]["name"] == "Alice Johnson"

        # Free text search (global)
        result = fs.search("Jakarta")
        assert result.hits["total"] == 2, f"Expected 2 hits for 'Jakarta', got {result.hits['total']}"

    def test_parquet_multiple_files_build(self):
        """Build index from a directory containing multiple Parquet files."""
        # Create two parquet files
        records1 = [
            {"id": 1, "name": "Alice", "dept": "Engineering"},
            {"id": 2, "name": "Bob", "dept": "Engineering"},
        ]
        records2 = [
            {"id": 3, "name": "Carol", "dept": "Marketing"},
            {"id": 4, "name": "Dave", "dept": "Marketing"},
        ]
        pq1 = os.path.join(self.tmpdir, "team1.parquet")
        pq2 = os.path.join(self.tmpdir, "team2.parquet")
        _create_parquet_file(pq1, records1)
        _create_parquet_file(pq2, records2)

        # Multi-index mode: each file becomes its own sub-index
        index_path = os.path.join(self.tmpdir, "team.fsk")
        _build_index_from_parquet(self.tmpdir, index_path)

        fs = Flatseek(index_path)

        # Should have docs from both files
        stats = fs.stats()
        assert stats["total_docs"] == 4, f"Expected 4 docs, got {stats['total_docs']}"

        # Search across all docs
        result = fs.search("Engineering")
        assert result.total == 2

        result = fs.search("Marketing")
        assert result.total == 2

    def test_parquet_mixed_with_csv_build(self):
        """Build index from a directory containing both Parquet and CSV files."""
        import csv

        # Create a Parquet file
        pq_path = os.path.join(self.tmpdir, "people.parquet")
        records = [
            {"id": 1, "name": "Alice", "city": "Jakarta"},
            {"id": 2, "name": "Bob", "city": "Surabaya"},
        ]
        _create_parquet_file(pq_path, records)

        # Create a CSV file
        csv_path = os.path.join(self.tmpdir, "extra.csv")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name", "city"])
            writer.writeheader()
            writer.writerow({"id": 3, "name": "Carol", "city": "Bandung"})
            writer.writerow({"id": 4, "name": "Dave", "city": "Medan"})

        index_path = os.path.join(self.tmpdir, "mixed.fsk")
        _build_index_from_parquet(self.tmpdir, index_path)

        fs = Flatseek(index_path)

        stats = fs.stats()
        assert stats["total_docs"] == 4, f"Expected 4 docs, got {stats['total_docs']}"

        # Search should find docs from both sources
        result = fs.search("Jakarta")
        assert result.total == 1

        result = fs.search("Bandung")
        assert result.total == 1

    def test_parquet_numeric_types(self):
        """Verify numeric types (INT, FLOAT) are correctly indexed from Parquet."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        # Create table with explicit PyArrow types
        ids = pa.array([1, 2, 3, 4, 5])
        ages = pa.array([30, 25, 35, 28, 32], type=pa.int32())
        scores = pa.array([87.5, 92.1, 78.3, 95.0, 84.7], type=pa.float64())
        balances = pa.array([1500000.50, 2300000.00, 980000.25, 3100000.75, 1750000.00],
                            type=pa.float64())
        names = pa.array(["Alice", "Bob", "Carol", "David", "Eve"])

        table = pa.Table.from_arrays(
            [ids, names, ages, scores, balances],
            names=["id", "name", "age", "score", "balance"]
        )
        pq_path = os.path.join(self.tmpdir, "numeric.parquet")
        pq.write_table(table, pq_path)

        index_path = os.path.join(self.tmpdir, "numeric.fsk")
        _build_index_from_parquet(pq_path, index_path)

        fs = Flatseek(index_path)

        stats = fs.stats()
        assert stats["total_docs"] == 5

        # Free text search on numbers works
        result = fs.search("87.5")
        assert result.total == 1

        result = fs.search("30")
        assert result.total == 1

    def test_parquet_boolean_handling(self):
        """Verify boolean values are correctly indexed from Parquet."""
        records = [
            {"id": 1, "name": "Alice", "active": True},
            {"id": 2, "name": "Bob", "active": False},
            {"id": 3, "name": "Carol", "active": True},
        ]
        pq_path = os.path.join(self.tmpdir, "bool_test.parquet")
        _create_parquet_file(pq_path, records)

        index_path = os.path.join(self.tmpdir, "bool_test.fsk")
        _build_index_from_parquet(pq_path, index_path)

        fs = Flatseek(index_path)

        result = fs.search("active:true")
        assert result.total == 2, f"Expected 2 active, got {result.total}"

        result = fs.search("active:false")
        assert result.total == 1, f"Expected 1 inactive, got {result.total}"

    def test_parquet_nested_json(self):
        """Verify nested JSON in Parquet string columns is expanded correctly."""
        records = [
            {
                "id": 1,
                "name": "Alice",
                "address": '{"city": "Jakarta", "district": "Sudirman", "zip": "10110"}',
            },
            {
                "id": 2,
                "name": "Bob",
                "address": '{"city": "Surabaya", "district": "Driyorejo", "zip": "60111"}',
            },
        ]
        pq_path = os.path.join(self.tmpdir, "nested.parquet")
        _create_parquet_file(pq_path, records)

        index_path = os.path.join(self.tmpdir, "nested.fsk")
        _build_index_from_parquet(pq_path, index_path)

        fs = Flatseek(index_path)

        # Dot-path search on nested field
        result = fs.search("address.city:Jakarta")
        assert result.hits["total"] == 1
        assert result.hits["hits"][0]["_source"]["name"] == "Alice"

        result = fs.search("address.city:Surabaya")
        assert result.hits["total"] == 1
        assert result.hits["hits"][0]["_source"]["name"] == "Bob"

    def test_parquet_chunked_reading(self):
        """Verify chunked reading works correctly for large Parquet files."""
        # Create a Parquet file with enough rows to span multiple batches
        records = [
            {"id": i, "name": f"User_{i:04d}", "city": f"City_{i % 10}"}
            for i in range(1, 501)
        ]
        pq_path = os.path.join(self.tmpdir, "chunked.parquet")
        _create_parquet_file(pq_path, records)

        index_path = os.path.join(self.tmpdir, "chunked.fsk")
        _build_index_from_parquet(pq_path, index_path)

        fs = Flatseek(index_path)

        stats = fs.stats()
        assert stats["total_docs"] == 500, f"Expected 500 docs, got {stats['total_docs']}"

        # Verify some specific records
        result = fs.search("User_0001")
        assert result.total == 1

        result = fs.search("city:City_0")
        assert result.total == 50, f"Expected ~50 hits for City_0, got {result.total}"

    def test_parquet_scanner(self):
        """Test scanner.py correctly detects Parquet files."""
        from flatseek.core.scanner import discover_csvs, scan_parquet_file

        records = [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "email": "bob@example.com"},
        ]
        pq_path = os.path.join(self.tmpdir, "scan_test.parquet")
        _create_parquet_file(pq_path, records)

        # Test discover_csvs finds .parquet files
        files = discover_csvs(self.tmpdir)
        assert any(f.endswith(".parquet") for f in files), \
            f"discover_csvs should find .parquet files: {files}"

        # Test scan_parquet_file returns correct schema
        schema = scan_parquet_file(pq_path)
        assert schema is not None, "scan_parquet_file should return schema for valid parquet"
        assert "headers" in schema
        assert "id" in schema["headers"]
        assert "name" in schema["headers"]
        assert "email" in schema["headers"]
        assert "columns" in schema
        assert schema["row_estimate"] == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
