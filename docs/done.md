# Flatseek Implementation Matrix

> Version: 0.1.10 | Generated: 2026-07-14

---

## Index Management

| Feature | CLI | Library | REST API |
|---|---|---|---|
| Build index from CSV | ✅ `cli.py:678` | ✅ `builder.py:672` | — |
| Build index (parallel) | ✅ `build --plan` | ✅ `builder.py:672` | — |
| Build index from JSONL | ✅ `cli.py:678` | ✅ `builder.py:672` | — |
| Build index from Parquet | ✅ `scanner.py` (detects .parquet) | ✅ `builder.py` (PyArrow) | — |
| Classify CSV columns | ✅ `cli.py:321` | `detect_semantic_types()` | — |
| Generate build plan | ✅ `cli.py:768` | N/A | — |
| Generate dummy data | ✅ `cli.py:5478` | N/A | — |
| Create empty index | — | — | ✅ `index.py:1118` |
| Delete index | ✅ `cli.py:4587` | — | ✅ `index.py:1004` |
| Rename index | — | — | ✅ `index.py:1070` |
| Index stats | ✅ `cli.py:2569` | ✅ `client.py:202` | ✅ `index.py:2077` |
| Index mapping | — | ✅ `client.py:205` | ✅ `index.py:1940` |
| WAL merge | `cli.py:2824` (wal subcommand) | N/A | — |
| WAL info | `cli.py:2824` (wal subcommand) | N/A | — |
| Pack to `.flatseek` | ✅ `cli.py:6147` (inline) | `FlatseekFileStorageAdapter` | — |
| Unpack `.flatseek` | ✅ `cli.py:6212` (inline) | `create_flatseek_storage()` | — |
| Compress index | ✅ `cli.py:5007` | — | — |
| Compact (reclaim space) | ✅ `cli.py:4740` | — | ✅ `documents.py:752` |
| Verify integrity | ✅ `cli.py:3485` | `is_encrypted()` | — |

---

## Encryption & Security

| Feature | CLI | Library | REST API |
|---|---|---|---|
| Encrypt index | ✅ `cli.py:3086` | `encrypt_bytes()` | ✅ `index.py:1142` |
| Decrypt index | ✅ `cli.py:3284` | `decrypt_bytes()` | ✅ `index.py:1291` |
| Authenticate (passphrase) | `--passphrase` flag | `load_encryption_key()` | ✅ `index.py:1405` |
| Check if encrypted | — | `is_encrypted()` | ✅ `index.py:1393` |
| Generate passphrase token | `generate-passphrase` | N/A | — |
| Generate license key | `generate-license-key` | N/A | — |
| Enclosed encryption | `pack --license-key` | N/A | — |

---

## Search

| Feature | CLI | Library | REST API |
|---|---|---|---|
| Full-text search | ✅ `cli.py:1510` | ✅ `query_engine.py:2404` | ✅ `search.py:235` |
| Search (GET) | — | — | ✅ `search.py:518` |
| Multi-index search | ✅ `search --index` | ✅ `Flatseek.search_multi()` | ✅ `POST /_search` + `_index` |
| Cross-lookup (join 2 indexes) | ✅ `cross-lookup` | ✅ `Flatseek.cross_lookup()` | ✅ `POST /_search` + `_cross_lookup` |
| Wildcard trigram | `%%` / `*` | Same | Same |
| Field filters | `field:value` | Same | Same |
| Numeric range | `amount:>1000` | Same | Same |
| Boolean AND | `AND` | Same | Same |
| Boolean OR | `OR` | Same | Same |
| Boolean NOT | `-field:value` | Same | Same |
| Phrase search | `"exact phrase"` | Same | Same |
| Sort | ✅ `--sort field:desc` | `sort` param | `sort` param |
| Pagination | ✅ `-p/--page -n/--page-size` | `page`, `page_size` | `from`, `size` |
| Multi-search (batch) | — | — | ✅ `search.py:604` |
| Validate query | — | — | ✅ `search.py:751` |
| Debug chunk listing | — | — | ✅ `search.py:182` |
| Cross-dataset join | ✅ `cli.py:2527` | — | — |
| Upload from URL | — | — | ✅ `index.py:699` |
| Preview from URL | — | — | ✅ `index.py:605` |

---

## Aggregation

| Feature | CLI | Library | REST API |
|---|---|---|---|
| Terms aggregation | — | ✅ `query_engine.py:2944` | ✅ `aggregate.py:59` |
| Stats aggregation | — | ✅ `query_engine.py:2944` | ✅ `aggregate.py:59` |
| Date histogram | — | ✅ `query_engine.py:2944` | ✅ `aggregate.py:59` |
| Numeric histogram | — | ✅ `query_engine.py:2944` | ✅ `aggregate.py:59` |
| Cardinality (HLL) | — | ✅ `client.py:96` | — |
| Min / Max / Sum / Avg | — | ✅ `query_engine.py:2944` | ✅ `aggregate.py:59` |
| Aggregation + query filter | — | ✅ `query_engine.py:2944` | — |

---

## Document Operations

| Feature | CLI | Library | REST API |
|---|---|---|---|
| Upsert by natural key | ✅ `cli.py:4842` | ✅ `client.py:292` | ✅ `documents.py:159` |
| Insert (no ID) | ✅ `cli.py:4842` | ✅ `client.py:340` | — |
| Partial update by ID | ✅ `cli.py:4842` | ✅ `client.py:357` | — |
| Update by query | ✅ `cli.py:4842` | — | ✅ `documents.py:266` |
| Delete by natural key | ✅ `cli.py:4776` | ✅ `client.py:404` | ✅ `documents.py:91` |
| Delete by Lucene query | ✅ `cli.py:4776` | ✅ `client.py:428` | ✅ `documents.py:127` |
| Get by numeric doc_id | — | — | ✅ `search.py:544` |
| Bulk — ES NDJSON | ✅ `cli.py:4842` | ✅ `client.py:475` | ✅ `documents.py:416` |
| Bulk upsert queue | ✅ `UpsertQueue` via `batch_upsert` | ✅ `UpsertQueue` (`core/upsert_queue.py`) | ✅ `POST /{index}/_bulk_upsert` |
| Bulk — JSON array | — | — | ✅ `index.py:496` |
| Count | `stats` | ✅ `client.py:195` | ✅ `search.py:689` |
| Export to JSONL | ✅ `cli.py:2133` | — | — |
| Export to CSV | `export -f csv` | — | — |
| Deduplicate docs | ✅ `cli.py:2576` | — | — |
| Slice index by query | ✅ `cli.py:3788` | — | — |

---

## Storage Backends

`?bucket=` param tersedia di hampir semua endpoint REST API (search, count, aggregate, stats, mapping, flush, encrypt, authenticate, dll).

| Feature | CLI | Library | REST API |
|---|---|---|---|
| Local filesystem | ✅ (default) | `LocalStorageAdapter` | ✅ (default) |
| Amazon S3 | `--storage-backend s3` | `S3StorageAdapter` | ✅ `?bucket=s3://...` |
| Vercel Blob | `--storage-backend vercel-blob` | `VercelBlobStorageAdapter` | ✅ `?bucket=vercel://...` |
| Remote URL (HTTP Range) | `--storage-backend url` | `URLStorageAdapter` | ✅ `?bucket=https://...` |
| `.fsk` over HTTP Range | `--storage-backend url` | `FlatseekFileStorageAdapter` (`flatseek_file.py:1402`) | ✅ `?bucket=https://.../data.fsk` |
| HuggingFace datasets | `--storage-backend url` | `URLStorageAdapter` | ✅ `?bucket=hf://datasets/...` |

---

## API Server

| Feature | CLI | Library | REST API |
|---|---|---|---|
| Start API + Dashboard | ✅ `cli.py:5178` | `create_app()` | N/A |
| Start API only | ✅ `cli.py:5347` | `create_app()` | N/A |
| Dashboard only | ✅ `cli.py:5397` | N/A | N/A |
| Health check | — | — | ✅ `main.py:301` |
| List indices | — | ✅ `client.py:640` | ✅ `main.py:313` |
| Flush index | — | ✅ `client.py:631` | ✅ `index.py:837` |
| Index logs | — | — | ✅ `index.py:1046` |
| Upload progress | — | — | ✅ `index.py:940` |
| Reset dashboard URL | `--reset-dashboard` | — | — |

---

## Utilities

| Feature | CLI | Library | REST API |
|---|---|---|---|
| Natural language chat | ✅ `cli.py:2562` | — | — |
| Count docs | `stats` | ✅ `client.py:195` | ✅ `search.py:689` |
| Classify text | — | `classify_text()` | — |
| Scan with regex | — | `scanner.scan()` | — |

---

## Module Map

| Module | Line | Purpose |
|---|---|---|
| [`cli.py`](src/flatseek/cli.py) | 6306 | All CLI commands |
| [`client.py`](src/flatseek/client.py) | 1 | Dual-mode client: `_DirectEngine` (163) + `_HTTPClient` (547) + `Flatseek` (665) |
| [`core/query_engine.py`](src/flatseek/core/query_engine.py) | 875 | Core search engine (`QueryEngine`) |
| [`core/builder.py`](src/flatseek/core/builder.py) | 672 | Index builder (`IndexBuilder`) |
| [`core/storage.py`](src/flatseek/core/storage.py) | 54 | Storage adapters: `StorageAdapter`(54), `LocalStorageAdapter`(113), `S3StorageAdapter`(180), `VercelBlobStorageAdapter`(313), `URLStorageAdapter`(510) |
| [`flatseek_file.py`](src/flatseek/flatseek_file.py) | 1402 | `FlatseekFileStorageAdapter` for .fsk files + pack/unpack logic |
| [`core/classify.py`](src/flatseek/core/classify.py) | — | Column semantic type detection |
| [`core/scanner.py`](src/flatseek/core/scanner.py) | — | Regex scanning |
| [`core/compactor.py`](src/flatseek/core/compactor.py) | — | Compaction / WAL merge |
| [`core/tombstone.py`](src/flatseek/core/tombstone.py) | — | Soft-delete / tombstone store |
| [`api/main.py`](src/flatseek/api/main.py) | — | FastAPI app factory |
| [`api/routes/index.py`](src/flatseek/api/routes/index.py) | — | Index management routes |
| [`api/routes/search.py`](src/flatseek/api/routes/search.py) | — | Search, count, validate, multi-search |
| [`api/routes/aggregate.py`](src/flatseek/api/routes/aggregate.py) | — | Aggregation routes |
| [`api/routes/documents.py`](src/flatseek/api/routes/documents.py) | — | Document delete/update/upsert routes |
| [`api/deps.py`](src/flatseek/api/deps.py) | 22 | Dependency injection (`IndexManager`) |

---

## Version History

| Version | Key Additions |
|---|---|
| 0.1.10 | Write ops (insert/upsert/update/delete/bulk) in library + CLI, `_id` field (ULID), separator-split fix, cross-lookup (join 2 indexes), multi-index wildcard search (`_index:pattern`), sort + pagination, bulk upsert queue, Parquet build, comprehensive test suite |
| 0.1.9 | Multi-index search, time-pruning, date_histogram fast path, calendar_interval aliases |
| 0.1.8 | `.fsk` over HTTP Range (no full download) |
| 0.1.7 | Document-level delete/update, sort, license/enclosed encryption, bulk improvements |
| 0.1.6 | Encryption (ChaCha20-Poly1305), WAL management |
| 0.1.5 | Parallel build (`--plan`), deduplication |
| 0.1.0 | Initial release — build, search, aggregations, S3/URL storage |
