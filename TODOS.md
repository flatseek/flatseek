# TODOs

Backlog of features / fixes that are NOT in the current release line.
Each item has: status, brief description, why, and how to approach.

---

## [x] `.fsk` over HTTP Range — load .fsk directly from HF/S3 without full download

**Status**: Implemented in v0.1.8 (2026-06-30).

**Result**: wikipedia.fsk (1.3 GB) open: >20 s → ~4 s. First query returns
real results (667 hits for "article") instead of 0 hits. Key changes:

- `RangeFile` now uses persistent `httpx.Client(http2=True)` with 256 KB
  block size and 16 concurrent requests via ThreadPoolExecutor.
- Graceful fallback to HTTP/1.1 if `h2` is not installed.
- `FlatseekFileStorageAdapter.listdir` fixed for `index/` bucket subdirs
  (was causing `_read_posting` to return empty results on remote .fsk).
- `pyproject.toml`: `httpx` → `httpx[http2]`.

---

## [x] Serve multiple `.fsk` files as separate indices

**Status**: Implemented (v0.1.9+). Discovery via `_discover_local_fsk()` in
`api/deps.py` automatically registers all `.fsk` files in `data_dir` as
separate indices. No CLI flag needed — `flatseek serve ./fsk_bucket/` where
`fsk_bucket/` contains `a.fsk`, `b.fsk`, `c.fsk` → three indices named
`a`, `b`, `c`. Each is queried independently.

---

## [x] Multi-index search — `_index` wildcard pattern

**Status**: Implemented (v0.1.10).

**What was implemented**:

1. **`_index` wildcard pattern** in search and aggregate bodies (`search.py`,
   `aggregate.py`): accepts glob patterns (`logs_*`, `a?`, `a*,b*`) and fans
   out to all matching `.fsk` shards in parallel (ThreadPoolExecutor, max 8
   workers). Results are merged, paginated, and tagged with `_index`.

2. **Time-pruning** (`deps.py:expand_index_pattern`): `start`/`end` params
   in YYYYMMDD format extract date segments from index names (e.g. `logs_2025-01`
   → `202501`) and prune indices outside the range before opening engines.

3. **Aggregate date_histogram doc_values fast path** (`query_engine.py`):
   `date_histogram` now uses `terms.bin` doc_values directly when no query
   filter is present — O(1) instead of O(n) chunk scan.

4. **`calendar_interval` aliases** (`query_engine.py`): `1d`/`7d`/`30d`/`90d`
   normalized to `day`, `1w` to `week`, `1M` to `month`, `1y` to `year`.

5. **`IndexManager` singleton isolation** (`deps.py`): added `_ensure_fresh()`
   and `reset_index_manager()` to handle FLATSEEK_DATA_DIR changes between
   test runs.

6. **`tests/test_multi_index_search.py`**: 17 passing tests covering
   pattern expansion, multi-index search/filter/pagination, and aggregate
   terms/date_histogram across shards.

---

## [x] Cross-lookup — join two indexes on a shared key field

**Status**: Implemented (v0.1.10).

`Flatseek.cross_lookup()` enriches source index results with matching documents
from a target index, joined on a shared key field. Available in all three
interfaces: Python library, CLI (`cross-lookup`), and REST API.

---

## [x] Write operations — insert/upsert/update/delete/bulk in library + CLI

**Status**: Implemented (v0.1.10).

`Flatseek` client now supports: `insert()`, `index()` (upsert),
`update()`, `delete()`, `delete_query()`, and `bulk()` in the library.
CLI: `insert-doc`, `update-doc`, `delete-doc`. REST API: `PUT /{index}/_doc/{id}`,
`DELETE /{index}/_doc`, `POST /{index}/_bulk_documents`.

---

## [x] Elasticsearch-compatible bulk API

**Status**: Implemented (v0.1.9).

`POST /{index}/_bulk_documents` provides an Elasticsearch-compatible
bulk API accepting NDJSON. Compatible with the official `elasticsearch-py`
client.

Supports four operation types: `index` (upsert), `create` (insert-only),
`update` (partial merge), `delete`. Returns ES-style response with per-item
status codes and totals.

---

## [ ] Parquet build — build index directly from Parquet files

**Status**: Deferred (v0.10+)

**Why**: Data analysts receive data in Parquet chunks from Spark/dbt/DuckDB
pipelines. Currently FlatSeek only builds from CSV/JSON/JSONL. Parquet is
columnar and self-describing (schema is embedded) — easier to consume than
CSV (no inference needed).

**Scope**:

1. **Scanner** (`core/scanner.py`):
   - Add `.parquet` to `_DATA_EXTS`
   - Detect schema via PyArrow without loading full file into memory
   - Sample values for type detection (needed for classify.py)

2. **Builder** (`core/builder.py`):
   - Add Parquet reading branch in `_open_source()` / `process_file()`
   - Use `pyarrow.parquet.ParquetFile` for chunked reading (per-file, not
     per-row-group — treat each `.parquet` as one chunk like CSV files)
   - Type mapping: Parquet types → FlatSeek semantic types (INT/FLOAT/BOOL/
     KEYWORD/TEXT/EMAIL/PHONE)
   - Nested data: already handled by `_expand_record()` (dotted notation)

3. **CLI** (`cli.py`):
   - Auto-detect `.parquet` files in scanner — no new flag needed
   - Build: `flatseek build ./parquet_bucket/ -o myindex.fsk --single-file`

4. **Export to Parquet** (separate concern, lower priority):
   - Add `--format parquet` to `cmd_export()`
   - Use PyArrow for chunk-based streaming write

**Dependencies**: `pyarrow` as optional dependency (same pattern as
`zstandard`).

**Files to touch**:
- `src/flatseek/core/scanner.py` — `.parquet` extension, schema detection
- `src/flatseek/core/builder.py` — Parquet reading branch
- `tests/test_parquet_build.py` — new test file
- `docs/cli.md` — build from Parquet usage

**Effort**: ~200 lines.

---

## [ ] High-throughput batch upsert queue for parallel crawlers

**Status**: Deferred (post v0.2.0)

**Why**: Current upsert is correct under concurrency but throughput is
limited by per-request disk I/O. A crawler doing 100+ upserts/sec creates
100 separate finalize() calls.

**Problem with current approach**:

Each upsert call does:
```
1. Acquire file lock
2. Read counter
3. Increment counter
4. Release lock
5. Create IndexBuilder
6. add_row() + finalize() → disk I/O
7. Return response
```

Step 5-6 serializes ALL upserts through disk. 100 concurrent crawlers =
100 sequential disk writes = throughput bottleneck.

**Approach**:

Per-worker in-memory buffer with periodic flush:
```
ThreadLocal builder per worker (avoids lock contention on builder state)
    ↓
Shared queue: Queue of doc batches [(id_field, field_value, doc_dict), ...]
    ↓
Background flusher thread: every N docs OR every T seconds, flush batch
    ↓
Atomic rename to make new docs searchable
```

**API addition**:
```
POST /{index}/_bulk_upsert
[{"id": "email@example.com", "doc": {...}}, ...]
→ Accepts batch, returns after queuing (async)
GET /{index}/_bulk_upsert/status
→ Returns queue depth, flush status
```

**Files to touch**:
- `src/flatseek/core/upsert_queue.py` — new: worker-local builder + shared queue + flusher
- `src/flatseek/api/routes/documents.py` — add `_bulk_upsert` endpoint
- `tests/test_bulk_upsert.py` — concurrent load test

**Effort**: ~400 lines.
