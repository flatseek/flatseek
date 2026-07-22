# Flatseek — Feature Gaps

> Based on `docs/done.md` v0.1.10 | Generated: 2026-07-13

Features exist in ONE method but are **MISSING** from others (CLI / Library / REST API).

---

## REST API Exists, Library & CLI Missing

| Feature | REST API | Missing in Library | Missing in CLI | Notes |
|---------|----------|--------------------|----------------|-------|
| Multi-search (batch) | ✅ `search.py:610` | `_HTTPClient` has no multi_search method | ❌ no `flatseek msearch` | |
| Validate query | ✅ `search.py:757` | No validation method | ❌ no command | |
| Debug chunk listing | ✅ `search.py:183` | Not exposed | ❌ no command | |
| Upload from URL | ✅ `index.py:700` | `_HTTPClient` has no upload method | ❌ no command | |
| Preview from URL | ✅ `index.py:606` | `_HTTPClient` has no preview | ❌ no command | |
| Index logs | ✅ `index.py:1046` | `_HTTPClient` has no logs method | ❌ no command | |
| Upload progress | ✅ `index.py:940` | `_HTTPClient` has no progress | ❌ no command | |
| Health check | ✅ `main.py:301` | `_HTTPClient` has no health | ❌ no command | |
| Reset dashboard URL | ❌ (CLI only) | N/A | ❌ Library missing | |
| Create empty index | ✅ `index.py:1118` | `_HTTPClient.index()` requires body, no `create_index()` | ❌ no command | |
| Rename index | ✅ `index.py:1070` | `_HTTPClient` has no rename | ❌ no command | |

---

## Library Exists, CLI & REST API Missing

| Feature | Library | Missing in CLI | Missing in REST | Notes |
|---------|---------|----------------|-----------------|-------|
| Cardinality (HLL) | ✅ `client.py:96` | ❌ no CLI command | ❌ aggregate doesn't expose | `_CardinalitySketch` class |
| Aggregation + query filter | ✅ `query_engine.py:3275` | ❌ no command | ❌ no endpoint | Library only |

---

## CLI Exists, Library & REST API Missing

| Feature | CLI | Missing in Library | Missing in REST | Notes |
|---------|-----|--------------------|----------------|-------|
| Generate dummy data | ✅ `cli.py:5478` | N/A | N/A | Schema presets: ecommerce, logs, blockchain, etc. |
| Compress index | ✅ `cli.py:5007` | N/A | ❌ no endpoint | zlib in-place |
| Verify integrity | ✅ `cli.py:3485` | `is_encrypted()` but no full verify | ❌ no endpoint | Check corrupt chunks |
| WAL merge | ✅ `cli.py:2824` | N/A | ❌ no endpoint | |
| WAL info | ✅ `cli.py:2824` | N/A | ❌ no endpoint | |
| Export to JSONL | ✅ `cli.py:2133` | N/A | ❌ no endpoint | |
| Export to CSV | ✅ `cli.py:2133` | N/A | ❌ no endpoint | |
| Deduplicate docs | ✅ `cli.py:2576` | N/A | ❌ no endpoint | |
| Slice index by query | ✅ `cli.py:3788` | N/A | ❌ no endpoint | Extract matching subset |
| Cross-dataset join | ✅ `cli.py:2527` | N/A | ❌ no endpoint | `--on` shared field |
| Generate passphrase token | `generate-passphrase` | N/A | ❌ no endpoint | Subscription distribution |
| Generate license key | `generate-license-key` | N/A | ❌ no endpoint | |
| Natural language chat | ✅ `cli.py:2562` | N/A | N/A | Ollama integration |
| Multi-index search | ✅ `search --index` | ❌ no Library method | ✅ REST exists | CLI + REST only |
| Classify CSV columns | ✅ `cli.py:321` | `detect_semantic_types()` without line ref | ❌ no endpoint | |

---

## Storage Backend Gaps

| Feature | CLI | Library | REST API | Gap |
|---------|-----|---------|----------|-----|
| S3 storage | ✅ `--storage-backend s3` | ✅ `S3StorageAdapter` | ❌ not exposed | Library exists, REST has no S3 path |
| Vercel Blob | ✅ `--storage-backend vercel-blob` | ✅ `VercelBlobStorageAdapter` | ❌ not exposed | Library exists, REST has no Vercel path |
| Remote URL (HTTP Range) | ✅ `--storage-backend url` | ✅ `URLStorageAdapter` | ❌ not exposed | Library exists, REST has no URL endpoint |
| `.fsk` over HTTP Range | ✅ `--storage-backend url` | ✅ `FlatseekFileStorageAdapter` | ❌ not exposed | Library exists, REST has no endpoint |
| HuggingFace datasets | ✅ `--storage-backend url` | ✅ `URLStorageAdapter` | ❌ not exposed | Library exists, REST has no HuggingFace endpoint |

**Note:** The `?bucket=` param in REST API supports HuggingFace URLs via `_get_bucket_storage()` in `deps.py`, but there is no dedicated S3/Vercel/URL path exposed as an endpoint. Storage adapters are used internally by `IndexManager` only.

---

## Feature Exists in 2 Methods, Missing in the Third

| Feature | Exists in | Missing in | Notes |
|---------|-----------|-----------|-------|
| Update by query | Library + REST | ❌ CLI | Needs `flatseek update-doc --query --set` |
| Bulk — JSON array | REST only | ❌ Library + CLI | `_HTTPClient.bulk()` exists but format differs from `POST /{index}/_bulk` |
| Count | CLI + Library + REST | — | All exist ✅ |
| Index stats | CLI + Library + REST | — | All exist ✅ |
| Encrypt/Decrypt | CLI + REST | Library `encrypt_bytes()` without full wrapping | |
| Authenticate | REST + Library | CLI `--passphrase` pass-through only | |
| Flush index | Library + REST | ❌ CLI | Needs `flatseek flush` command |
| List indices | Library + REST | ❌ CLI | Needs `flatseek indices` command |
| Compact | CLI + REST | ❌ Library | `IndexCompactor` exists but not exposed in client.py |
| Check if encrypted | Library + REST | ❌ CLI | Needs `flatseek is-encrypted` command |

---

## Gap Priority Summary

### 🔴 High — Many Requests / Core Features

1. **REST: S3/Vercel/URL storage path** — storage adapter is internal, no dedicated endpoint
2. **REST: Bulk — JSON array** — `_HTTPClient.bulk()` exists but format differs from REST
3. **Library: `_HTTPClient.bulk_search`** — multi-index batch search via HTTP

### 🟡 Medium — Feature Completeness

4. **CLI: indices / list** — `flatseek indices` does not exist
5. **CLI: flush** — `flatseek flush` does not exist
6. **CLI: is-encrypted** — check encryption status from CLI
7. **REST: Aggregation + query filter** — library has it, REST does not
8. **CLI: Validate query** — `flatseek validate "query"` does not exist
9. **CLI: Update by query** — `flatseek update-doc --query --set` does not exist

### 🟢 Low — Nice to Have

10. **Library: Cardinality in aggregate response** — `_CardinalitySketch` not exposed via `qe.aggregate()`
11. **CLI: compress --parallel** — exists, just needs testing
12. **Library: WAL management** — not needed (WAL is internal to builder)
