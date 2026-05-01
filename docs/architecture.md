# Flatseek Architecture

A structural and behavioral map of the Flatseek codebase. Assumes familiarity with search engine internals; no concept explanations or marketing language.

---

## Table of Contents

- [1. Project Structure](#1-project-structure)
- [2. Layered Architecture](#2-layered-architecture)
- [3. Module Breakdown](#3-module-breakdown)
  - [`cli.py`](#clijpy) · [`core/builder.py`](#corebuilderpy) · [`core/query_engine.py`](#corequery_enginepy) · [`core/query_parser.py`](#corequery_parserpy) · [`core/scanner.py`](#corescannerpy) · [`core/classify.py`](#coreclassifypy) · [`core/chat.py`](#corechatpy) · [`client.py`](#clientpy) · [`api/main.py`](#apimainpy) · [`api/deps.py`](#apidepspy) · [`api/routes/index.py`](#apiroutesindexpy) · [`api/routes/search.py`](#apiroutessearchpy) · [`api/routes/aggregate.py`](#apiroutesaggregatepy)
- [4. Data Flow](#4-data-flow)
  - [Indexing Flow](#indexing-flow) · [Query Flow](#query-flow)
- [5. Runtime Behavior](#5-runtime-behavior)
  - [Memory-Mapped I/O](#memory-mapped-io) · [Memory Usage](#memory-usage) · [Checkpoint Strategy](#checkpoint-strategy) · [Segment-Append](#segment-append)
- [6. Design Patterns](#6-design-patterns)
  - [Streaming](#streaming) · [Columnar Storage](#columnar-storage) · [Trigram Inverted Index](#trigram-inverted-index) · [Memory-Mapped Files](#memory-mapped-files) · [Late Materialization](#late-materialization) · [Segment-Append](#segment-append) · [Dual-Mode Client](#dual-mode-client) · [Lazy Initialization](#lazy-initialization)
- [7. Dependency Graph](#7-dependency-graph)
- [8. Extensibility Points](#8-extensibility-points)

---

## 1. Project Structure {#project-structure}

```
flatseek/                          # Project root
├── pyproject.toml                 # Build config (hatch), entry points, deps
├── README.md
├── docs/
│   ├── internals.md              # Deep teardown (indexing, storage, query, agg)
│   └── architecture.md           # This document
├── src/flatseek/
│   ├── __init__.py               # Version only (`__version__ = "0.1.2"`)
│   ├── cli.py                    # Entry point: build, serve, chat, stats, inspect
│   ├── client.py                 # Dual-mode client: API (httpx) vs direct (QueryEngine)
│   ├── core/
│   │   ├── __init__.py
│   │   ├── builder.py            # ETL pipeline: CSV/JSON → trigram index
│   │   ├── query_engine.py      # Query execution, doc fetching, aggregations
│   │   ├── query_parser.py      # Lucene-style recursive descent parser
│   │   ├── scanner.py            # File discovery, type detection, schema linking
│   │   ├── classify.py           # Column semantic classification (GEO, ID, AMT, etc.)
│   │   └── chat.py               # Natural language query interface (Ollama-compatible)
│   ├── api/
│   │   ├── __init__.py           # Re-exports FastAPI app
│   │   ├── main.py               # FastAPI factory, dashboard mounting, root routes
│   │   ├── deps.py               # IndexManager singleton, FastAPI dependencies
│   │   ├── schemas.py            # Pydantic models, ES-compatible factories
│   │   └── routes/
│   │       ├── index.py          # _doc, _bulk, _flush, _encrypt, _upload_from_url, _rename
│   │       ├── search.py         # _search, _doc/{id}, _msearch, _count, _validate
│   │       └── aggregate.py      # _aggregate (terms, stats, cardinality)
│   └── test/
│       ├── fixtures.py            # generate_sample_events_csv, IndexContext, APITestClient
│       └── test_query_parser.py
└── tests/                         # pytest integration tests
```

**Entry point wiring** (from `pyproject.toml`):
- `flatseek` console script → `flatseek.cli:main`
- `flatseek-generate` → `flatseek.generate:main` (dummy dataset generator)

---

## 2. Layered Architecture {#layered-architecture}

```
┌─────────────────────────────────────────────────────────┐
│  CLI  (flatseek build / serve / chat / inspect / stats) │
│       cmd_build() → parallel subprocess workers         │
└───────────────────────┬─────────────────────────────────┘
                        │ subprocess / HTTP
┌───────────────────────▼─────────────────────────────────┐
│  API  (FastAPI + httpx client)                          │
│  routes/index.py   — ingestion, bulk, encrypt, flush   │
│  routes/search.py  — Lucene query dispatch              │
│  routes/aggregate.py — delegated to QueryEngine.agg     │
│  api/deps.py       — IndexManager (lazy engine per idx) │
└───────────────────────┬─────────────────────────────────┘
                        │ local subprocess call
┌───────────────────────▼─────────────────────────────────┐
│  CLIENT  (flatseek/client.py)                           │
│  Flatseek(url) → _HTTPClient (httpx)                    │
│  Flatseek(path) → _DirectEngine (QueryEngine wrapper)   │
└───────────────────────┬─────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────┐
│  QUERY ENGINE  (core/query_engine.py)                   │
│  .query(Lucene DSL)  .search(wildcard)  .aggregate()   │
│  .join()  .cross_lookup()  .stats  .columns()          │
│  3-tier cache: _hot_postings | _posting_cache | mmap   │
└───────────────────────┬─────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────┐
│  BUILDER  (core/builder.py)                             │
│  _ByteRangeReader → tokenizer → trigram → CRC32 bucket  │
│  Checkpoint → segment-append → finalize_merge          │
└───────────────────────┬─────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────┐
│  STORAGE  (mmap'd .bin files on disk)                  │
│  idx.bin (postings) | numeric.bin/terms.bin (docvals)  │
│  docs_{N}.zlib (chunked doc store) | stats.json        │
└─────────────────────────────────────────────────────────┘
```

**Call direction**: CLI → API → Client → QueryEngine → Builder → Storage. The API and Client layers are symmetric; API is used by external HTTP callers, Client is used by local Python callers.

---

## 3. Module Breakdown {#module-breakdown}

### `cli.py`

**Purpose**: Single entry point for all user-facing operations.

**Commands**:

| Command | Action |
|---------|--------|
| `build` | Spawns parallel `python -m flatseek.core.builder` workers via subprocess |
| `serve` | Starts FastAPI + mounts flatlens dashboard at `/dashboard` |
| `chat` | Opens REPL using `core/chat.py` |
| `inspect` | Dumps index stats, columns, encryption status |
| `stats` | Prints total docs, index size, column types |
| `classify` | Infers semantic types for columns |

**`cmd_build()` orchestration**:
1. `scanner.discover_csvs()` → list of input files
2. `scanner.scan_file()` → per-file schema + sample rows
3. `scanner.link_columns()` → cross-file column unification
4. `classify.classify_file()` → semantic type per column
5. Compute total rows, decide parallel split
6. Write `build_plan.json` (byte ranges per worker)
7. Spawn N subprocess workers, each calling `IndexBuilder`
8. `IndexBuilder.finalize()` merges seg files per worker
9. Final pass: merge all `idx_w{N}.bin` into `idx.bin`
10. `builder._write_doc_values()` and `builder._write_bucket_term_sets()`

### `core/builder.py`

**Purpose**: ETL pipeline — converts CSV/JSON records into a trigram inverted index.

**`_ByteRangeReader(io.RawIOBase)`** — custom raw I/O for parallel CSV splitting. `readinto(buf)` overridden to read only its assigned byte range. Used so multiple workers can process a single CSV without coordination.

**`IndexBuilder`** — main orchestrator.

| Method | Responsibility |
|--------|---------------|
| `tokenize(value)` | Extracts search tokens (full value, split on `_SPLIT_RE`, email parts, digit suffixes) |
| `make_trigrams(token)` | Yields 3-grams; tokens capped at 15 chars |
| `_index_value()` | Adds tokens + trigrams to `pending_terms` dict |
| `_expand_record()` | Flattens arrays/objects (`tags` → `tags[0]`, `tags[1]`) |
| `add_row()` | Accumulates docs, triggers checkpoint at 50K rows |
| `_flush_terms()` | Encodes `pending_terms` to `bytearray` buffers |
| `_flush_buf_to_disk()` | Segment-append: writes buf to `PREFIX/HH/HH/seg_{N}.bin` |
| `_bg_flush_io()` | Background thread for checkpoint I/O |
| `_write_pressure_wal()` | Flushes pending_terms to WAL when memory pressure detected |
| `_lazy_writer_loop()` | Daemon-mode background writer (non-blocking build) |
| `_finalize_merge_segments()` | Merges seg files into compressed `idx.bin` |
| `_write_doc_values()` | Writes columnar `dv/{field}/numeric.bin` and `dv/{field}/terms.bin` |
| `_write_bucket_term_sets()` | Writes `terms.set` for fast-rejection bloom filter |
| `process_file()` | CSV/JSON/JSONL with pandas 10K-row chunk fallback |
| `finalize()` | Final flush, segment merge, stats write |

**Key constants**:

```python
DOC_CHUNK_SIZE = 100_000     # docs per docs_{N}.zlib chunk
CHECKPOINT_ROWS = 50_000     # trigger checkpoint
BUFFER_FLUSH_BYTES = 512_000  # segment switch threshold
CHECKPOINT_WRITE_BYTES = 8_1024  # minimum buffer to trigger flush
SEGMENT_SWITCH_SIZE = 5_242_880  # 5 MB — large prefix switch
MAX_TRIGRAM_TOKEN_LEN = 15   # token length cap
ZLIB_LEVEL = 6               # doc compression
ZLIB_LEVEL_IDX = 9           # index compression
FLUSH_THREADS = min(8, cpu_count())
```

**I/O profile**: High-frequency small writes (50K-row checkpoints) accumulated in 512KB buffer → segment-append to `PREFIX/HH/HH/seg_N.bin`. CRC32 hash bucketing creates 65,536 sub-directories.

### `core/query_engine.py` {#corequery_enginepy}

**Purpose**: Disk-first query execution over mmap'd index files.

**Posting list format**: `[count][doc_id_0][doc_id_1][...]` where doc_ids are delta-encoded varints. `decode_doclist(data)` decodes in one pass.

**3-tier cache**:

```
_hot_postings {}     — pinned in memory, common filter terms (never evicted)
_posting_cache {}     — bounded LRU, max 8192 terms, 256MB cap
_mmap_cache {}       — reused mmap handles
```

| Method | What it does |
|--------|-------------|
| `_mmap_read(path)` | mmap with fallback to regular read |
| `_load_bucket_term_set()` | Fast-reject frozenset loaded from `PREFIX/HH/HH/terms.set` |
| `_read_posting(term_hash)` | 3-tier lookup: hot → LRU cache → mmap |
| `_verify_wildcard(doc_ids, regex)` | Post-filter trigram candidates by regex |
| `_resolve(term)` | Exact/wildcard term resolution, infix fallback |
| `_wildcard_fallback()` | Scan index terms when too many candidates |
| `_load_doc_values(field)` | Returns reader for `dv/{field}/numeric.bin` or `terms.bin` |
| `_resolve_range(field, op, value)` | Binary search (numeric) or chunk scan (terms) |
| `_chunk_start(chunk_id)` | Returns byte offset for `docs_{N:010d}.zlib` |
| `_load_chunk(chunk_id)` | Decompresses single chunk, returns `list[dict]` |
| `_fetch_docs(doc_ids)` | Returns full documents for result set |
| `aggregate(q, aggs, size)` | Streaming HLL aggregation |

**Query routing** (mirrors `routes/search.py`):
- `:` (field:value) or `AND/OR/NOT` → Lucene DSL → `query_engine.query()`
- `*` or `%` wildcard without field → `query_engine.search()`
- `*` (match all) → `query_engine.search("", page=0, page_size=N)`

**Encryption**: `load_encryption_key(index_dir, passphrase)` → `set_key(key)` on engine → all subsequent reads decrypt via ChaCha20-Poly1305.

### `core/query_parser.py` {#corequery_parserpy}

**Purpose**: Parses Lucene query syntax into an AST, then executes against `QueryEngine`.

Recursive descent parser producing AST nodes: `term`, `range`, `and`, `andnot`, `or`, `not`.

`tokenize(text)` produces tokens: `('AND',)`, `('TERM', field, value, quoted)`, `('RANGE', field, op, val)`, `('RANGE', field, 'between', lo, hi)`.

`_Parser._primary()` handles grouping `()`, negation `-`, phrase quotes `"..."`.

`execute(node, qe)` dispatches AST nodes to `QueryEngine.query()` / `search_and()` / `search()`.

### `core/scanner.py` {#corescannerpy}

**Purpose**: File discovery and schema detection.

`discover_csvs(directory)` — recursive glob for CSV/JSON/JSONL, handles `.gz`/`.bz2`.

`detect_type(samples)` — priority cascade:

```
ARRAY > OBJECT > DATE > BOOL > INT > FLOAT > TEXT > KEYWORD
```

`link_columns(file_schemas)` — fuzzy column name matching across files.

### `core/classify.py` {#coreclassifypy}

**Purpose**: Infers semantic types (GEO, ID, AMT, TMSG, etc.) from column names and sample values.

Key lookup tables (precomputed at module load):
- `_ALIAS_EXACT` — direct name map
- `_ALIAS_PARTIAL` — suffix/prefix patterns
- `_PATTERNS` — compiled regex per type
- `_VALUE_SETS` — known value sets per type

`_BoundedTextReader` — hard-limits decompressed bytes to prevent decompression bombs when sampling compressed files.

### `core/chat.py` {#corechatpy}

**Purpose**: Natural language to query conversion via Ollama-compatible API.

`ChatInterface` wraps OpenAI-compatible chat completions (default: Ollama at `http://localhost:11434`). `_system_prompt()` instructs LLM to emit JSON conditions.

`chat(nl_query)` → `_extract_query()` → `qe.search()` or `qe.search_and()`.

`interactive()` — REPL loop with history.

### `client.py` {#clientpy}

**Purpose**: Dual-mode client for Python programmatic access.

| Class | Mode | Wraps |
|-------|------|-------|
| `_HTTPClient` | API | httpx, exposes all HTTP endpoints |
| `_DirectEngine` | Direct | `QueryEngine` local file access |
| `Flatseek` | Auto-detect | URL → `_HTTPClient`; local path → `_DirectEngine` |

`_CardinalitySketch` — HyperLogLog with 65536 × 6-bit registers (~48KB), expected error ~1.6%.

### `api/main.py` {#apimainpy}

`create_app()` — FastAPI factory. Attaches routers from `routes/index`, `routes/search`, `routes/aggregate`.

`attach_dashboard()` — mounts flatlens (web UI) at `/dashboard` using `_find_flatlens()` (searches `node_modules`, `../flatlens`, `src/flatlens`, etc.) and `_copy_and_patch()` (patches `API_BASE` and logo href in a temp copy).

Root routes:
- `GET /` — server info
- `GET /_cluster/health` — ES-compatible health
- `GET /_indices` — list index names

### `api/deps.py` {#apidepspy}

`IndexManager` — lazy singleton. Holds `_engines` dict (index name → `QueryEngine` instance) and `_passwords` dict (for encrypted indexes). `get_index_manager()` returns the singleton.

### `api/routes/index.py` {#apiroutesindexpy}

State: `_index_builders` (in-memory builders), `_bulk_executor` (ThreadPoolExecutor), `_encrypt_jobs`, `_index_logs`, `_upload_progress`.

All indexing operations. Key endpoints:

| Endpoint | Method | Function |
|----------|--------|----------|
| `/{index}/_doc` | POST | Single doc indexing |
| `/{index}/_bulk` | POST | Bulk with resume token |
| `/_upload_from_url` | POST | Fetch + index from remote URL |
| `/{index}/_flush` | POST | Finalize and flush (blocking or async) |
| `/{index}/_encrypt` | PUT | ChaCha20-Poly1305 encryption, background job |
| `/{index}/_decrypt` | POST | Decrypt in-place |
| `/{index}/_authenticate` | POST | Submit passphrase for encrypted index |
| `/{index}/_mapping` | PUT/GET | Create/update or retrieve column mapping |
| `/{index}/_stats` | GET | Docs, size, columns, encryption status |
| `DELETE /{index}` | DELETE | Async index deletion |

`_normalize_doc()` handles Elasticsearch dump format `{"_source": {...}}`. `_index_batch()` runs in ThreadPoolExecutor, normalizes + adds docs.

### `api/routes/search.py` {#apiroutessearchpy}

| Endpoint | Method | Function |
|----------|--------|----------|
| `/{index}/_search` | POST/GET | Lucene query with query routing |
| `/{index}/_doc/{doc_id}` | GET | Single document |
| `/{index}/_msearch` | POST | Multi-search (up to 10 queries) |
| `/{index}/_count` | GET | Count matching docs |
| `/{index}/_validate` | POST | Parse query without executing |
| `/_debug/chunks/{index}` | GET | Debug: list chunk files |

**Query routing logic** (mirrors `QueryEngine`):

```
query contains ":" OR "AND"/"OR"/"NOT"  → engine.query()
query == "*"                            → engine.search("", ...)
query contains "*" or "%"               → engine.search()
otherwise                               → engine.query()
```

### `api/routes/aggregate.py` {#apiroutesaggregatepy}

`POST/GET /{index}/_aggregate` — delegates to `QueryEngine.aggregate()` via `asyncio.to_thread()` (non-blocking).

Supported aggregation types: `terms`, `stats`, `cardinality`.

---

## 4. Data Flow {#data-flow}

### Indexing Flow {#indexing-flow}

```
CSV/JSON/JSONL file
        │
        ▼
scanner.discover_csvs()     ← glob + decompress detection
        │
        ▼
scanner.scan_file()          ← header + 200 sample rows
        │
        ▼
classify.classify_file()     ← semantic type inference
        │
        ▼
build_plan.json              ← byte ranges per worker (CLI only)
        │
        ▼
_ByteRangeReader             ← non-overlapping range per worker
        │
        ▼
IndexBuilder.tokenize()      ← value → tokens + trigrams
        │
        ▼
IndexBuilder._index_value()   ← pending_terms[term_hash].append(doc_id)
        │
        ▼
IndexBuilder.add_row()        ← checkpoint at 50K rows
        │
        ▼
_flush_buf_to_disk()          ← segment-append to PREFIX/HH/HH/seg_N.bin
        │
        ▼
_finalize_merge_segments()   ← merge seg_N.bin → idx.bin (compressed)
        │
        ▼
_write_doc_values()           ← dv/{field}/numeric.bin, terms.bin
        │
        ▼
_write_bucket_term_sets()     ← PREFIX/HH/HH/terms.set (fast-reject)
        │
        ▼
stats.json                    ← total_docs, columns, sizes
```

### Query Flow {#query-flow}

```
client.search(q="program:raydium AND amount:>1000000")
        │
        ▼
query_parser.parse()          ← recursive descent → AST
        │
        ▼
query_parser.execute(ast, qe)  ← AST → QueryEngine calls
        │
        ├── _resolve("raydium")     → posting list (3-tier cache)
        ├── _resolve_range("amount", ">", 1000000) → doc_values binary search
        │
        ▼
AND intersection               ← skip-pointer merged doc ID lists
        │
        ▼
_fetch_docs(doc_ids)          ← decompress docs_{N}.zlib chunks
        │
        ▼
collapse_expanded_fields()    ← rebuild arrays from flat keys (tags[0], tags[1])
        │
        ▼
Response { hits: [{_id, _score, _source}, ...] }
```

---

## 5. Runtime Behavior {#runtime-behavior}

### Memory-Mapped I/O {#memory-mapped-io}

`query_engine._mmap_read(path)` uses `mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)`. OS page cache keeps hot index data in memory. Cold pages are faulted in on demand.

Fallback: if `mmap` fails (permissions, unusual filesystem), falls back to `f.read()` into `bytearray`.

### Memory Usage {#memory-usage}

| Component | Approx Cost |
|-----------|-------------|
| `pending_terms` dict | ~200 bytes per term per 50K rows |
| `array('I')` pending doc IDs | 4 bytes per doc ID (vs ~28 for Python int) |
| HLL sketch (aggregate) | 48 KB fixed |
| Posting cache LRU | Max 8192 terms, 256 MB cap |
| Doc chunk (`docs_{N}.zlib`) | Up to 100K docs, compressed |

### Checkpoint Strategy {#checkpoint-strategy}

Checkpoint fires when either:
- `added_rows >= CHECKPOINT_ROWS` (50K)
- `bytes_in_buffer >= CHECKPOINT_WRITE_BYTES` (8 KB)

Small buffers are held in memory; checkpoint only triggers I/O when buffer is substantial enough to amortize filesystem overhead.

### Segment-Append

When `bytes_in_buffer >= SEGMENT_SWITCH_SIZE` (5 MB), buffer switches to next segment file to avoid O(N) prefix I/O on append-only storage.

---

## 6. Design Patterns {#design-patterns}

### Streaming {#streaming}
Chunked CSV reading via pandas (`read_csv(..., chunksize=10000)`) to bound peak memory. Aggregation streams over doc_values without loading all matching doc IDs.

### Columnar Storage {#columnar-storage}
`dv/{field}/numeric.bin` (sorted uint64) and `dv/{field}/terms.bin` (sorted terms + uint32 doc ID list) enable range query binary search and aggregation without scanning doc chunks.

### Trigram Inverted Index {#trigram-inverted-index}
Posting lists keyed by trigram hash. Wildcard queries (`*term*`) expand to matching trigrams → candidate doc IDs → post-filter by regex. Exact queries use term hash → direct posting lookup.

### Memory-Mapped Files {#memory-mapped-files}
OS page cache as index cache. Zero-copy reads for hot posting lists. `_hot_postings` dict pins common filter terms in-process.

### Late Materialization {#late-materialization}
Search first identifies matching doc IDs via trigram inverted index. Full document bodies are loaded only when needed (result set construction). Aggregation operates directly on doc_values without materializing full documents.

### Segment-Append
Checkpoints accumulated in memory buffers and written as complete segments. Merged into final `idx.bin` at `finalize()`. Avoids read-modify-write on checkpoint.

### Dual-Mode Client {#dual-mode-client}
Single `Flatseek` class auto-detects URL (HTTP API) vs local path (direct QueryEngine). Same Python API for both modes.

### Lazy Initialization {#lazy-initialization}
`IndexManager` creates `QueryEngine` instances on first access per index. Builders created on first `_bulk` or `_doc` call.

---

## 7. Dependency Graph {#dependency-graph}

```
cli.py
  └── core.builder (subprocess workers)
  └── core.scanner
  └── core.classify

client.py
  └── core.query_engine (direct mode)
  └── httpx (API mode)

api/main.py
  └── api/deps (IndexManager)
  └── api/routes/index
  └── api/routes/search
  └── api/routes/aggregate

api/routes/index.py
  └── core.builder (IndexBuilder)
  └── core.scanner
  └── core.classify

api/routes/search.py
  └── core.query_engine (QueryEngine)
  └── core.query_parser (parse, execute)

api/routes/aggregate.py
  └── core.query_engine (aggregate)

api/deps.py
  └── core.query_engine (QueryEngine)

core/query_engine.py
  └── core.query_parser (parse)

core/chat.py
  └── core.query_engine (QueryEngine)

core/scanner.py
  └── core.classify
```

**Leaf modules** (no internal deps): `core/classify.py`, `core/chat.py`

**Core modules** (heavily depended on): `core/query_engine.py`, `core/builder.py`

---

## 8. Extensibility Points {#extensibility-points}

### Adding a New Aggregation Type {#adding-new-aggregation-type}

1. Implement in `QueryEngine.aggregate()` — add a new `elif agg_type == "new_agg":` branch.
2. Add the schema in `api/routes/aggregate.py` Pydantic models.
3. Document the JSON structure in the endpoint docstring.

### Adding a New Query Operator {#adding-new-query-operator}

1. Add token type to `query_parser.tokenize()` in `core/query_parser.py`.
2. Add AST node type and parsing rule in `_Parser` methods.
3. Add execution branch in `query_parser.execute()` calling the appropriate `QueryEngine` method.
4. Add routing condition in `routes/search.py` query routing to route new operator to `engine.query()`.

### Adding a New Storage Format {#adding-new-storage-format}

1. Implement a reader class with `search(query)`, `aggregate(...)` interface matching `QueryEngine`.
2. Update `IndexManager.get_engine()` to detect and instantiate the new format.
3. Add format detection in `query_engine._discover_index_dirs()`.

### Adding a New Field Type {#adding-new-field-type}

1. Add classification logic in `core/classify.py` (`detect_type()` cascade).
2. Add doc_values writer branch in `builder._write_doc_values()`.
3. Add doc_values reader branch in `query_engine._load_doc_values()`.

### Adding a New Encryption Scheme {#adding-new-encryption-scheme}

1. Add encrypt/decrypt in `builder.py` (follow `_encrypt_index()` pattern in `routes/index.py`).
2. Add key loading in `query_engine.py` (`load_encryption_key()`, `set_key()`).
3. Wire password handling in `api/routes/search.py` and `api/routes/aggregate.py` (check `manager.is_encrypted()` → `request.headers.get("x-index-password")`).
4. Add new endpoints in `routes/index.py` (follow `_encrypt` / `_decrypt` pattern).

### Adding a New Input Format {#adding-new-input-format}

1. Add file extension detection in `builder.process_file()` (`elif path.endswith(".newfmt")`).
2. Add parser branch (JSON, CSV, Parquet, etc.) with chunked reading if large.
3. For pandas-supported formats, add to the pandas `read_*` branch; otherwise use `csv.DictReader` fallback.