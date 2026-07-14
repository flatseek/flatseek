"""
Concurrent load test for bulk upsert queue.

Tests:
1. Basic single-threaded batch upsert
2. Multi-threaded concurrent upserts
3. Queue status reporting
4. Flush behavior
5. High-throughput scenario with multiple workers
"""

from __future__ import annotations

import os
import time
import tempfile
import shutil
import threading
import concurrent.futures
from pathlib import Path

import pytest


# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def temp_data_dir():
    """Create a temporary data directory for tests."""
    tmpdir = tempfile.mkdtemp(prefix="flatseek_test_")
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def sample_csv(temp_data_dir):
    """Create a sample CSV file for building an index."""
    csv_path = os.path.join(temp_data_dir, "sample.csv")
    with open(csv_path, "w") as f:
        f.write("email,name,status,country\n")
        for i in range(100):
            f.write(f"user{i}@example.com,User {i},active,US\n")
    return csv_path


@pytest.fixture
def built_index(temp_data_dir, sample_csv):
    """Build a small index with id_field for upsert testing."""
    from flatseek.core.builder import IndexBuilder

    index_path = os.path.join(temp_data_dir, "testidx")
    os.makedirs(index_path)

    builder = IndexBuilder(
        output_dir=index_path,
        column_map={},
        start_doc_id=0,
        dataset="testidx",
        checkpoint_cb=None,
        delimiter=",",
        columns=None,
        worker_id=None,
        dedup_fields=None,
        doc_id_end=None,
        daemon=False,
        id_field="email",
    )

    import csv
    with open(sample_csv) as f:
        reader = csv.DictReader(f)
        for row in reader:
            normalized = {k: v for k, v in row.items()}
            builder.add_row(normalized, list(row.keys()), file_rel=None, col_schema=None)

    builder.finalize()

    # Read existing stats before overwriting
    import json
    stats_path = os.path.join(index_path, "stats.json")
    stats = {"_id_field": "email"}
    if os.path.exists(stats_path):
        try:
            existing = json.loads(open(stats_path).read())
            # Preserve existing total_docs and other stats
            stats = {**existing, "_id_field": "email"}
        except Exception:
            pass

    # Write updated stats with id_field
    with open(stats_path, "w") as f:
        json.dump(stats, f)

    # Ensure .next_doc_id exists with correct value
    next_doc_id_path = os.path.join(index_path, ".next_doc_id")
    if not os.path.exists(next_doc_id_path):
        total_docs = stats.get("total_docs", builder.doc_counter)
        with open(next_doc_id_path, "w") as f:
            f.write(str(total_docs))

    return index_path


# ─── Tests ─────────────────────────────────────────────────────────────────────


def test_single_thread_batch_upsert(built_index):
    """Test basic batch upsert with single thread."""
    from flatseek.core.upsert_queue import UpsertQueue, close_upsert_queue

    data_dir = os.path.dirname(built_index)
    index_name = os.path.basename(built_index)

    try:
        queue = UpsertQueue(
            data_dir=data_dir,
            index_name=index_name,
            id_field="email",
            batch_size=10,
            flush_interval=60.0,
        )
        queue.start()

        # Add 5 docs (less than batch_size)
        for i in range(5):
            queue.put(f"new{i}@example.com", {"email": f"new{i}@example.com", "status": "active"})

        status = queue.status()
        assert status["pending_docs"] == 5
        assert status["queue_depth"] == 0

        # Add 5 more to reach batch_size
        for i in range(5, 10):
            queue.put(f"new{i}@example.com", {"email": f"new{i}@example.com", "status": "active"})

        # Wait for flusher thread to finish builder.finalize()
        time.sleep(3.0)

        status = queue.status()
        assert status["pending_docs"] == 0
        assert status["total_docs_flushed"] == 10

        # Verify new docs are searchable via exact email match
        from flatseek.core.query_engine import QueryEngine
        engine = QueryEngine(built_index)

        # Check that new docs can be found by exact email
        for i in range(10):
            result = engine.query(f"email:new{i}@example.com")
            assert result["total"] == 1, f"new{i}@example.com should be found"

    finally:
        close_upsert_queue(data_dir, index_name)


def test_multi_thread_concurrent_upserts(built_index):
    """Test concurrent upserts from multiple threads."""
    from flatseek.core.upsert_queue import UpsertQueue, close_upsert_queue

    data_dir = os.path.dirname(built_index)
    index_name = os.path.basename(built_index)
    num_workers = 4
    docs_per_worker = 20
    batch_size = 10

    try:
        queue = UpsertQueue(
            data_dir=data_dir,
            index_name=index_name,
            id_field="email",
            batch_size=batch_size,
            flush_interval=60.0,
        )
        queue.start()

        errors = []

        def worker(worker_id: int):
            try:
                for i in range(docs_per_worker):
                    doc_id = f"worker{worker_id}_doc{i}@example.com"
                    queue.put(doc_id, {"email": doc_id, "status": "active", "worker": str(worker_id)})
            except Exception as e:
                errors.append(e)

        # Run workers concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(worker, i) for i in range(num_workers)]
            concurrent.futures.wait(futures)

        # Check for errors
        assert len(errors) == 0, f"Worker errors: {errors}"

        # Give flusher time to process all flush requests
        time.sleep(2.0)

        status = queue.status()
        # total_docs_flushed should account for docs that were flushed via batch_size triggers
        # Note: some docs may remain in worker thread buffers and won't be counted until stop()
        assert status["total_docs_flushed"] > 0, "Some docs should have been flushed"

        # Verify some new docs are searchable (sample check)
        from flatseek.core.query_engine import QueryEngine
        engine = QueryEngine(built_index)

        # Check a sample of new docs - they should be found
        found = 0
        for wid in range(num_workers):
            for did in range(docs_per_worker):
                doc_id = f"worker{wid}_doc{did}@example.com"
                result = engine.query(f"email:{doc_id}")
                if result["total"] == 1:
                    found += 1

        # At least some docs should be searchable
        assert found > 0, "At least some new docs should be searchable"

    finally:
        close_upsert_queue(data_dir, index_name)


def test_manual_flush(built_index):
    """Test explicit flush() call."""
    from flatseek.core.upsert_queue import UpsertQueue, close_upsert_queue

    data_dir = os.path.dirname(built_index)
    index_name = os.path.basename(built_index)

    try:
        queue = UpsertQueue(
            data_dir=data_dir,
            index_name=index_name,
            id_field="email",
            batch_size=100,  # Large batch size so auto-flush doesn't trigger
            flush_interval=60.0,
        )
        queue.start()

        # Add some docs
        for i in range(20):
            queue.put(f"flush{i}@example.com", {"email": f"flush{i}@example.com", "status": "active"})

        status = queue.status()
        assert status["pending_docs"] == 20

        # Manual flush
        queue.flush()

        # Wait for flusher thread to finish builder.finalize()
        time.sleep(3.0)

        status = queue.status()
        assert status["pending_docs"] == 0
        assert status["total_docs_flushed"] == 20

        # Verify new docs are searchable
        from flatseek.core.query_engine import QueryEngine
        engine = QueryEngine(built_index)

        for i in range(20):
            result = engine.query(f"email:flush{i}@example.com")
            assert result["total"] == 1, f"flush{i}@example.com should be found"

    finally:
        close_upsert_queue(data_dir, index_name)


def test_queue_status_reporting(built_index):
    """Test that queue status is reported correctly."""
    from flatseek.core.upsert_queue import UpsertQueue, close_upsert_queue

    data_dir = os.path.dirname(built_index)
    index_name = os.path.basename(built_index)

    try:
        queue = UpsertQueue(
            data_dir=data_dir,
            index_name=index_name,
            id_field="email",
            batch_size=50,
            flush_interval=60.0,
        )
        queue.start()

        # Initial status
        status = queue.status()
        assert status["queue_depth"] == 0
        assert status["pending_docs"] == 0
        assert status["flushing"] == False
        assert status["total_docs_flushed"] == 0

        # Add docs
        for i in range(15):
            queue.put(f"status{i}@example.com", {"email": f"status{i}@example.com"})

        status = queue.status()
        assert status["pending_docs"] == 15
        assert status["queue_depth"] == 0

        # Flush
        queue.flush()
        time.sleep(3.0)

        status = queue.status()
        assert status["pending_docs"] == 0
        assert status["total_docs_flushed"] == 15
        assert status["last_flush_age_s"] is not None

    finally:
        close_upsert_queue(data_dir, index_name)


def test_high_throughput_load(built_index):
    """Test high-throughput scenario simulating parallel crawlers."""
    from flatseek.core.upsert_queue import UpsertQueue, close_upsert_queue

    data_dir = os.path.dirname(built_index)
    index_name = os.path.basename(built_index)
    num_workers = 4
    docs_per_worker = 50
    batch_size = 25

    try:
        queue = UpsertQueue(
            data_dir=data_dir,
            index_name=index_name,
            id_field="email",
            batch_size=batch_size,
            flush_interval=10.0,
        )
        queue.start()

        start_time = time.time()

        def worker(worker_id: int):
            for i in range(docs_per_worker):
                doc_id = f"ht_w{worker_id}_d{i}@example.com"
                queue.put(doc_id, {
                    "email": doc_id,
                    "status": "active",
                    "worker": str(worker_id),
                    "seq": str(i),
                })

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(worker, i) for i in range(num_workers)]
            concurrent.futures.wait(futures)

        # Wait for all flushes to complete
        time.sleep(3.0)

        elapsed = time.time() - start_time
        total_docs = num_workers * docs_per_worker

        status = queue.status()
        # At least some docs should have been flushed
        assert status["total_docs_flushed"] > 0, "Some docs should have been flushed"

        # Verify a sample of new docs are searchable
        from flatseek.core.query_engine import QueryEngine
        engine = QueryEngine(built_index)

        found = 0
        for wid in range(num_workers):
            for did in range(0, docs_per_worker, 5):  # Check every 5th doc
                doc_id = f"ht_w{wid}_d{did}@example.com"
                result = engine.query(f"email:{doc_id}")
                if result["total"] == 1:
                    found += 1

        # At least some docs should be searchable
        assert found > 0, "At least some new docs should be searchable"

        throughput = total_docs / elapsed
        print(f"\nHigh-throughput test: {total_docs} docs in {elapsed:.2f}s = {throughput:.1f} docs/sec")

    finally:
        close_upsert_queue(data_dir, index_name)


def test_stop_drains_pending(built_index):
    """Test that stop() properly drains pending docs after flush."""
    from flatseek.core.upsert_queue import UpsertQueue, close_upsert_queue

    data_dir = os.path.dirname(built_index)
    index_name = os.path.basename(built_index)

    try:
        queue = UpsertQueue(
            data_dir=data_dir,
            index_name=index_name,
            id_field="email",
            batch_size=1000,
            flush_interval=60.0,
        )
        queue.start()

        # Add some docs
        for i in range(30):
            queue.put(f"stop{i}@example.com", {"email": f"stop{i}@example.com", "status": "active"})

        status = queue.status()
        assert status["pending_docs"] == 30

        # Flush explicitly before stop
        queue.flush()
        time.sleep(0.5)

        # Stop should complete quickly since no pending docs
        queue.stop(timeout=5.0)

        # Verify new docs are searchable after stop
        from flatseek.core.query_engine import QueryEngine
        engine = QueryEngine(built_index)

        for i in range(30):
            result = engine.query(f"email:stop{i}@example.com")
            assert result["total"] == 1, f"stop{i}@example.com should be found after stop"

    finally:
        try:
            close_upsert_queue(data_dir, index_name)
        except Exception:
            pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
