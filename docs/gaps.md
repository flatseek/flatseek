# Flatseek — Fitur yang Belum Diimplement

> Berdasarkan `docs/done.md` v0.1.10 | Generated: 2026-07-13

Fitur-fitur di bawah ini ada di SATU metode tapi **BELUM ada** di metode lain (CLI / Library / REST API).

---

## Fitur REST API Ada, tapi Library & CLI Belum

| Fitur | REST API | Belum Ada di Library | Belum Ada di CLI | Notes |
|---|---|---|---|---|
| Multi-search (batch) | ✅ `search.py:610` | `_HTTPClient` tidak punya method multi_search | ❌ tidak ada `flatseek msearch` | |
| Validate query | ✅ `search.py:757` | Tidak ada method validasi | ❌ tidak ada command | |
| Debug chunk listing | ✅ `search.py:183` | Tidak ada | ❌ tidak ada command | |
| Upload from URL | ✅ `index.py:700` | `_HTTPClient` tidak punya | ❌ tidak ada command | |
| Preview from URL | ✅ `index.py:606` | `_HTTPClient` tidak punya | ❌ tidak ada command | |
| Index logs | ✅ `index.py:1046` | `_HTTPClient` tidak punya | ❌ tidak ada command | |
| Upload progress | ✅ `index.py:940` | `_HTTPClient` tidak punya | ❌ tidak ada command | |
| Health check | ✅ `main.py:301` | `_HTTPClient` tidak punya | ❌ tidak ada command | |
| Reset dashboard URL | ❌ (CLI only) | N/A | ❌ tidak ada Library | |
| Create empty index | ✅ `index.py:1118` | `_HTTPClient.index()` butuh body, tidak ada `create_index()` | ❌ tidak ada command | |
| Rename index | ✅ `index.py:1070` | `_HTTPClient` tidak punya | ❌ tidak ada command | |

---

## Fitur Library Ada, tapi CLI & REST API Belum

| Fitur | Library | Belum Ada di CLI | Belum Ada di REST | Notes |
|---|---|---|---|---|
| Cardinality (HLL) | ✅ `client.py:96` | ❌ CLI tidak ada | ❌ aggregate tidak expose | `_CardinalitySketch` class |
| Aggregation + query filter | ✅ `query_engine.py:3275` | ❌ tidak ada command | ❌ tidak ada endpoint | Library only |

---

## Fitur CLI Ada, tapi Library & REST API Belum

| Fitur | CLI | Belum Ada di Library | Belum Ada di REST | Notes |
|---|---|---|---|---|
| Generate dummy data | ✅ `cli.py:5478` | N/A | N/A | Schema presets: ecommerce, logs, blockchain, dll |
| Compress index | ✅ `cli.py:5007` | N/A | ❌ tidak ada endpoint | zlib in-place |
| Verify integrity | ✅ `cli.py:3485` | `is_encrypted()` tapi tidak ada verify lengkap | ❌ tidak ada endpoint | Check corrupt chunks |
| WAL merge | ✅ `cli.py:2824` | N/A | ❌ tidak ada endpoint | |
| WAL info | ✅ `cli.py:2824` | N/A | ❌ tidak ada endpoint | |
| Export to JSONL | ✅ `cli.py:2133` | N/A | ❌ tidak ada endpoint | |
| Export to CSV | ✅ `cli.py:2133` | N/A | ❌ tidak ada endpoint | |
| Deduplicate docs | ✅ `cli.py:2576` | N/A | ❌ tidak ada endpoint | |
| Slice index by query | ✅ `cli.py:3788` | N/A | ❌ tidak ada endpoint | Extract matching subset |
| Cross-dataset join | ✅ `cli.py:2527` | N/A | ❌ tidak ada endpoint | `--on` shared field |
| Generate passphrase token | `generate-passphrase` | N/A | ❌ tidak ada endpoint | Subscription distribution |
| Generate license key | `generate-license-key` | N/A | ❌ tidak ada endpoint | |
| Natural language chat | ✅ `cli.py:2562` | N/A | N/A | Ollama integration |
| Multi-index search | ✅ `search --index` | ❌ tidak ada Library method | ✅ REST ada | CLI + REST only |
| Classify CSV columns | ✅ `cli.py:321` | `detect_semantic_types()` tanpa line ref | ❌ tidak ada endpoint | |

---

## Fitur Storage Backend — Masih Ada Gap

| Fitur | CLI | Library | REST API | Gap |
|---|---|---|---|---|
| S3 storage | ✅ `--storage-backend s3` | ✅ `S3StorageAdapter` | ❌ tidak ada | Library ada, REST tidak expose S3 path |
| Vercel Blob | ✅ `--storage-backend vercel-blob` | ✅ `VercelBlobStorageAdapter` | ❌ tidak ada | Library ada, REST tidak expose |
| Remote URL (HTTP Range) | ✅ `--storage-backend url` | ✅ `URLStorageAdapter` | ❌ tidak ada | Library ada, REST tidak expose |
| `.fsk` over HTTP Range | ✅ `--storage-backend url` | ✅ `FlatseekFileStorageAdapter` | ❌ tidak ada | Library ada, REST tidak expose |
| HuggingFace datasets | ✅ `--storage-backend url` | ✅ `URLStorageAdapter` | ❌ tidak ada | Library ada, REST tidak expose |

**Catatan:** `?bucket=` param di REST API support HuggingFace URL via `_get_bucket_storage()` di `deps.py`, tapi tidak ada path khusus S3/Vercel/URL yang di-expose sebagai endpoint. Storage adapter hanya dipakai secara internal oleh `IndexManager`.

---

## Fitur Ada di 2 Metode, Belum di Metode Ketiga

| Fitur | Ada di | Belum di | Notes |
|---|---|---|---|
| Update by query | Library + REST | ❌ CLI | Perlu `flatseek update-doc --query --set` |
| Bulk — JSON array | REST only | ❌ Library + CLI | `_HTTPClient.bulk()` ada tapi beda format |
| Count | CLI + Library + REST | — | Semua sudah ada ✅ |
| Index stats | CLI + Library + REST | — | Semua sudah ada ✅ |
| Encrypt/Decrypt | CLI + REST | Library `encrypt_bytes()` tanpa full wrapping | |
| Authenticate | REST + Library | CLI `--passphrase` pass-through only | |
| Flush index | Library + REST | ❌ CLI | Perlu `flatseek flush` command |
| List indices | Library + REST | ❌ CLI | Perlu `flatseek indices` command |
| Compact | CLI + REST | ❌ Library | `IndexCompactor` ada tapi tidak di-expose di client.py |
| Check if encrypted | Library + REST | ❌ CLI | Perlu `flatseek is-encrypted` command |

---

## Ringkasan Gap Priority

### 🔴 High — Banyak Request / Core Features
1. **REST: S3/Vercel/URL storage path** — storage adapter internal, tidak ada dedicated endpoint
2. **REST: Bulk — JSON array** — `_HTTPClient` punya `bulk()` tapi beda format dari `POST /{index}/_bulk`
3. **Library: `_HTTPClient.bulk_search`** — multi-index batch search via HTTP

### 🟡 Medium — Kelengkapan Fitur
4. **CLI: indices / list** — `flatseek indices` belum ada
5. **CLI: flush** — `flatseek flush` belum ada
6. **CLI: is-encrypted** — check encryption status dari CLI
7. **REST: Aggregation + query filter** — library ada, REST tidak
8. **CLI: Validate query** — `flatseek validate "query"` belum ada
9. **CLI: Update by query** — `flatseek update-doc --query --set` belum ada

### 🟢 Low — Nice to Have
10. **Library: Cardinality di aggregate response** — `_CardinalitySketch` tidak di-expose via `qe.aggregate()`
11. **CLI: compress --parallel** — sudah ada, tinggal test
12. **Library: WAL management** — tidak perlu (WAL internal builder)
