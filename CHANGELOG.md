# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.10] - 2026-07-13

### Feature: Comprehensive update tests — library + REST API

**New test file: `src/flatseek/test/test_update.py`** (17 tests)

Library-level update operations tested:
- `update()` partial merge preserves non-updated fields
- `update()` returns `updated`/`not_found` correctly
- `update()` with JSON/list/dict field values
- `update()` after delete returns `not_found`
- `index()` upsert: `created` for new docs, `updated` for existing
- `index()` full replacement semantics
- Multiple sequential updates
- `update()` vs `index()` distinction (merge vs replace)

**New REST API tests in `test_api.py` WriteOpsAPI (7 tests)**

- `PUT /{index}/_doc/{id}` upsert (created/updated result verification)
- `PUT /{index}/_doc?q=...` partial update-by-query merge verification
- `POST /{index}/_bulk_documents` update partial merge with field preservation
- `DELETE /{index}/_doc?q=...` delete-by-query

**Bugfixes (3 bugs in update code paths):**

1. `client.py update()` — fetched document *after* tombstones were applied,
   so `_fetch_docs` returned empty. Fixed: moved fetch before tombstone.

2. `documents.py bulk route` — same fetch-after-tombstone bug. Same fix applied.

3. `documents.py bulk route` — missing `id_field` in `IndexBuilder` caused
   `stats.json` to lose `_id_field` on subsequent writes, breaking all
   write ops after the first reload. Fixed: pass `id_field=id_field`.

4. `documents.py bulk route` — newly-written documents invisible because
   engine's `_doc_cache` held stale chunks. Fixed: call `clear_doc_cache()`
   after `builder.finalize()`.

5. `documents.py bulk route` — fetched existing doc via
   `query("_id:{resolved_id}")` which failed for inserted docs (ULID strings
   as `_id`, not integer doc_ids). Fixed: use `_fetch_docs([resolved_id])`
   with integer doc_id directly.

---

## [0.1.10] - 2026-07-13

### Feature: Cross-lookup (join two indexes on shared key)

`Flatseek.cross_lookup()` — enrich source index results with matching documents
from a target index, joined on a shared key field. Available in all three interfaces:

```python
# Python library
r = Flatseek.cross_lookup(
    source_dir="./data/users",
    target_dir="./data/txs",
    query="status:active",
    on="user_id",
    left_fields=["email", "name"],
    right_fields=["tstamp", "amount"],
)

# CLI
flatseek cross-lookup ./data/users ./data/txs \
  "status:active" --on user_id \
  --left-fields "email,name" \
  --right-fields "tstamp,amount"

# REST API
curl -X POST http://localhost:8000/users/_cross_lookup \
  -d '{"target": "txs", "query": "status:active", "on": "user_id"}'
```

Output: `_left` (source doc), `_right` (list of matching target docs), `_match_count`.

### Feature: Multi-index search with wildcard, date_histogram fast path

`MultiIndexEngine` for querying multiple `.fsk` files as one logical index.
`QueryEngine._query_multi` now supports `_index:pattern` routing:

```bash
# Query only indexes matching "ads*"
fs.search("error AND _index:ads*")
```

- Wildcard pattern routing to sub-indexes via `_index:ads*` / `_index:ads?`
- `date_histogram` aggregation now has a fast path for time-series data
- `calendar_interval` aliases: `calendar_interval=month` → `interval=30d`

**API:**
- `GET /{index}/_search` — `date_histogram` with fast path
- `GET /_search` — multi-index with `_index:pattern`

**CLI:**
- `flatseek search "error" --index "logs_2024*"`

### Feature: insert/upsert/update/delete/bulk — library and CLI

**Python library** (`from flatseek import Flatseek`):

```python
from flatseek import Flatseek
fs = Flatseek("./index_dir")

fs.insert({"name": "Alice"})                         # auto-generates ULID _id
fs.index("user@example.com", {"name": "Bob"})       # upsert by natural key
fs.update("user@example.com", {"status": "active"}) # partial update
fs.delete("user@example.com")                       # soft-delete by key
fs.delete_query("status:inactive")                  # soft-delete by Lucene query
fs.bulk([
    {"index": {"_id": "x"}}, {"name": "Alice"},
    {"create": {"_id": "y"}}, {"name": "Bob"},
    {"update": {"_id": "x"}}, {"doc": {"status": "active"}},
    {"delete": {"_id": "y"}},
])
```

**`_id` field:** Documents now have a `_id` field (like Elasticsearch):
- `insert()` generates a ULID string (26 chars, time-sortable, globally unique)
- `index(id, doc)` uses the provided `id` as `_id`
- `_id` is stored as a document field, queryable and searchable

**CLI:**

```bash
flatseek insert-doc ./index --doc '{"id":"1","name":"Alice"}'
flatseek insert-doc ./index --file docs.jsonl
flatseek insert-doc ./index --file data.csv --format csv
```

### Bugfix: `_expand_record` separator-split only for ARRAY columns

Previously, all string fields containing commas (or `;|#`) were
automatically split into `field[0]`, `field[1]`, etc. This broke prose
fields like article bodies ("AI, ML, DL are related").

Fix: separator-split now only applies to columns with semantic type
`ARRAY`. TEXT and KEYWORD fields are stored as-is.

### Tests: sort, pagination, pack, unpack, export, verify, slice

**`test_search.py` — TestSort (5 tests)**
- Sort by numeric field (asc/desc), multi-field sort, sort + pagination correctness

**`test_search.py` — TestPagination (5 tests)**
- Page 0/1 no-overlap, beyond-results returns empty, total reflects all matches,
  large page_size returns all

**`test_cli.py` — TestPack (2 tests)**
- `flatseek pack` creates valid `.fsk` with FLATSEEK04 magic header
- `flatseek pack --passphrase` encrypts the archive

**`test_cli.py` — TestUnpack (2 tests)**
- `flatseek unpack` restores a searchable index from `.fsk`
- `flatseek unpack --passphrase` decrypts and restores encrypted archive

**`test_cli.py` — TestExport (3 tests)**
- `flatseek export` writes all docs to JSONL
- `flatseek export -q` filters output by query
- `flatseek export -f csv` produces CSV format

**`test_cli.py` — TestVerify (1 test)**
- `flatseek verify` reports all chunks OK and exits 0

**`test_cli.py` — TestSlice (1 test)**
- `flatseek slice` creates a new index containing only query-matched docs

### Bugfix: `delete_query` used wrong field to look up tombstone

`delete_query` called `doc.get("_id")` to find the natural key for tombstone
lookup. For inserted documents `_id` is a ULID string, but the tombstone
mapping is keyed by `id_field` name + natural key value. Fix: use
`doc.get(id_field)` instead.

### Bugfix: `test_cli.py::TestSearch` missing Namespace attributes

`cmd_search` accesses `args.index`, `args.sort`, and `args.storage_backend`
from the Namespace. The test's `argparse.Namespace` was missing these attributes.
Fix: added `index=None`, `sort=None`, `storage_backend=None` to the test.

## [0.1.9] - 2026-07-07

### Feature: Elasticsearch-compatible bulk API

`POST /{index}/_bulk_documents` provides an Elasticsearch-compatible bulk
API accepting NDJSON (newline-delimited JSON) with four operation types:

- `index` — insert or replace document by `_id` (upsert, ES `index` action)
- `create` — insert only, fails with 409 if `_id` already exists (ES `create`)
- `update` — partial update by `_id`, merges fields into existing doc (ES `update`)
- `delete` — delete by `_id` (ES `delete`)

**Request format (NDJSON):**
```
{"index": {"_id": "1"}}
{"email": "alice@example.com", "name": "Alice"}
{"create": {"_id": "2"}}
{"email": "bob@example.com", "name": "Bob"}
{"update": {"_id": "1"}}
{"doc": {"status": "verified"}}
{"delete": {"_id": "3"}}
```

Compatible with the official `elasticsearch-py` client. Returns per-item
status codes (200/201/404/409), `took` milliseconds, and totals.

**Internal reliability fixes (included in this feature):**
- TombstoneStore: write 1 null byte to `bitmap.bin` before `mmap()` — fixes `ValueError` crash on macOS.
- upsert counter: seed from `stats.json["total_docs"]` when `.next_doc_id` is absent — prevents new upserts from overwriting existing doc_ids 0,1.
- upsert lock scope: hold `fcntl.LOCK_EX` through `builder.finalize()` — prevents concurrent workers from corrupting the same chunk file.

### Feature: Document-level delete and update by query

Add CRUD operations for documents — delete and update by ID or Lucene query.

**Tombstone storage:** Documents are never physically removed from the index.
Instead, a persistent bitmap marks deleted doc_ids, and a WAL ensures
crash-consistency. Query results automatically exclude deleted documents.

**Optimistic locking:** Delete and update operations use version tracking to
detect concurrent modifications. Conflicting operations return HTTP 409 or
retry automatically up to 3 times.

**API:**
- `DELETE /{index}/_doc/{id}` — Delete document by natural key
- `DELETE /{index}/_doc?q=...` — Delete all documents matching Lucene query
- `PUT /{index}/_doc/{id}` — Upsert (insert or replace) document by natural key
- `PUT /{index}/_doc?q=...` — Update all documents matching Lucene query
- `GET /{index}/_stats` — Now includes `docs.deleted` count

**CLI:**
- `flatseek delete-doc <index> --id=<value>` — Delete by natural key
- `flatseek delete-doc <index> --query="email:*@gmail.com"` — Delete by query
- `flatseek update-doc <index> --id=<value> --set=status=verified` — Update by ID
- `flatseek update-doc <index> --query="email:*@gmail.com" --set=status=verified` — Update by query

**Build with natural key:**
- `flatseek build data/ -o index/ --id-field email` — Enables delete/update by email

**`.fsk` files:** Packed archives are read-only. Delete/update operations return
HTTP 409 with message: "Cannot modify packed .fsk. Unpack first."

### Feature: `--id-field` for natural key tracking

Documents can now be identified by a natural key field (e.g. email, user_id)
instead of internal doc_id. The field is stored in `stats.json._id_field` and
used for upsert/delete-by-ID operations.

### Feature: Compaction to reclaim disk space after bulk deletes

Deleted documents are marked as tombstones but occupy disk space. Compaction
rewrites the index to include only alive documents, freeing the space.

**API:**
- `POST /{index}/_compact` — Rewrite index without deleted documents

**CLI:**
- `flatseek compact <index>` — Reclaim disk space after bulk deletes

**Returns:** `{result: "ok", deleted_removed: N, alive_rewritten: M, old_size_mb: X, new_size_mb: Y}`

**Note:** Compaction rewrites all index files (docs + postings). For large
indexes, run during off-peak hours. `.fsk` files cannot be compacted —
unpack first.

### Feature: Sort search results by field

Add sorting to search results with field-based and score-based sort.

**API:**
- GET `/{index}/_search?sort=amount:desc`
- POST body: `"sort": [{"amount": "desc"}, {"created_at": "asc"}]`

**Supports:**
- Numeric field sorting via doc_values (fast path)
- Text/keyword field sorting via chunk scan (fallback)
- Multi-field sorting (stable, applied in reverse order)
- Missing values sort last
- Pagination with sort works correctly

**CLI:**
- `flatseek search data/ "query" --sort amount:desc`

### Bug fix: HuggingFace bucket URL routing for all dataset types

Fixed correct storage adapter selection (dir-style `URLStorageAdapter` vs
fsk-style `FlatseekFileStorageAdapter`) and proper authentication flows
for all HF dataset types:

- **Plain directory**: `https://huggingface.co/datasets/owner/repo` → 200
- **Encrypted directory**: prompt via `x-index-password` → 401 on wrong/missing → 200 on success
- **Plain `.fsk`**: served directly → 200
- **Enclosed FSK (active)**: passphrase → decrypt outer layer → 200
- **Enclosed FSK (expired)**: `PermissionError` → 403
- **License FSK**: `HF_TOKEN` verified against `_K_inner_encrypted` → 200; wrong token → 401
- **Direct `.fsk` URL**: HTTP Range + `HF_TOKEN` if private → 200
- **Bucket repo**: `/resolve/{index}` pattern (not `/resolve/main/{index}`) → 200

HF API-first layout detection (`/api/datasets/{owner}/{repo}/tree/main`)
avoids blind `.fsk` probing. `PermissionError` correctly returns 401
instead of 403 for encrypted indices.

### Bug fix: `create_storage_adapter` ignores `FLATSEEK_STORAGE_URL` when backend defaults to "local"

When `FLATSEEK_STORAGE_URL` was set but `FLATSEEK_STORAGE_BACKEND` was not,
the factory defaulted to `backend="local"` and completely ignored the URL.
Fix: auto-upgrade `backend="local"` → `"url"` when `FLATSEEK_STORAGE_URL`
is present, so the URL is actually used without requiring both env vars.

### Bug fix: `get_engine` default storage bypasses HF `.fsk` detection

When serving a HF bucket index without an explicit `?bucket=` parameter,
`get_engine` passed the bare `URLStorageAdapter` to `QueryEngine` directly.
For HF bucket repos, this fails because `URLStorageAdapter` cannot resolve
`.fsk` files (needs `FlatseekFileStorageAdapter` with HTTP Range).

Fix: apply the same HF bucket/dataset detection logic to the default
`self._storage` path that the `bucket=` parameter path already uses,
ensuring `_search` and `_indices` consume the same env-configured storage.

### Bug fix: `flatseek serve` hangs on non-TTY stdin

`cmd_serve` called `input("  Install flatlens now? [Y/n] ")` which blocks
forever when stdin is a pipe (no TTY). This caused all 4
`serve_unblocks` regression tests to fail, and would also hang
`flatseek serve` when run from systemd, scripts, or any non-interactive
context.

Fix: guard with `sys.stdin.isatty()` — non-TTY environments
automatically skip the flatlens install prompt and proceed to server
startup.

### CI: pip install with `--no-cache-dir`

Avoid cached package issues in CI by adding `--no-cache-dir` to all
`pip install` commands in `.github/workflows/test.yml`.

### CLI: search/export/slice/verify via remote `.fsk` URL with spinner

`flatseek search`, `export`, `slice`, and `verify` now accept a
`https://` URL pointing directly at a `.fsk` file on HuggingFace,
S3, or any HTTP server. HTTP Range requests are used — no full
download required.

For HuggingFace URLs, the CLI reads `HF_TOKEN` from the
`huggingface_hub` token cache (`~/.cache/huggingface/token`) when
`HF_TOKEN` env var is not set, enabling access to private repos
without extra env configuration.

A spinner (`⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏`) is shown while reading the
remote index and while fetching search results.

### Feature: License token mode with ChaCha20 full-section encryption + expiry

License mode allows distributing `.fsk` files with cryptographic expiry enforcement.
Each section is encrypted with a per-file `K_inner` key. The `K_inner` is only
exposed after HMAC verification of a user token against the manifest's embedded key.

Crypto flow:
- `K_inner` (random 32 bytes) generated per pack
- Token = `HMAC(owner_key, id|expire_ts|export_limit)`, signed with owner's private key
- Runtime: verify token → derive `K_user = HMAC(embedded_key, "flatseek-license-v1")`
  → decrypt `_K_inner_encrypted` with `K_user` → decrypt sections with `K_inner`
- Expiry enforced at crypto layer — cannot be bypassed by patching the binary

Token can be renewed without re-downloading the `.fsk` (new token, same file).

### Feature: Enclosed format with master-key and cryptographic expiry

Enclosed format encrypts the entire `.fsk` blob with a master key and embeds
an `expire_at` timestamp in the encrypted outer layer. Decryption fails
cryptographically when the current time exceeds `expire_at` — no separate expiry
check that could be patched.

Format: `salt(32) | outer_ct_len(4) | outer_ct | inner_ct`
- `outer_ct = encrypt({"k": K_inner, "expire_at": ts}, K_user)`
- `inner_ct = encrypt(fsk_plaintext, K_inner)`
- `K_user = PBKDF2(passphrase, salt, 600k)`

### Feature: Offset table encrypted in license/enclosed formats

The index offset table (which maps file paths to byte offsets in the archive) is
now encrypted inside the encrypted blob with `K_inner` — not stored as plaintext
outside. Without `K_inner`, an attacker cannot determine which files exist or
where they are located in the archive.

### Feature: CLI banner on invalid/expired token

When `flatseek search` or `export` encounters an invalid/expired token for a
license-protected or enclosed `.fsk` file, the CLI now displays an "Index Info"
banner showing the license expiration before exiting with an error — instead of
just the error message alone.

CLI commands affected: `search`, `export`.

### Feature: `GET /_stats` returns `description` and `license_expire`

The `/_stats` API endpoint now includes `description` and `license_expire`
fields in the response, surfaced from either `stats.json` (for regular indexes)
or the `.fsk` manifest (for license/enclosed `.fsk` files).

Schema changes (`IndexStats`):
- Added `description: str | None` — index description from build
- Added `license_expire: str | None` — formatted expiration timestamp

### Feature: `IndexBuilder` supports `license_expire` field

`IndexBuilder` and the `build()` function now accept a `license_expire` parameter.
When set, it is written to `stats.json` as `license_expire` and returned via
`GET /_stats`. This allows the expiration info to be displayed even for
non-encrypted indexes built with an expiration watermark.

## [0.1.8] - 2026-06-30

### Performance: RangeFile HTTP optimization for remote `.fsk`

Serving a `.fsk` index from HuggingFace, S3, or any HTTP URL is now
**5× faster** on open and **dramatically faster** on first query.

Key changes to `RangeFile` (`core/storage.py`):

- **`BLOCK_SIZE`: 4 KB → 256 KB.** Reading a 1.6 MB index offset table
  now needs ~7 requests instead of ~425 (at 4 KB/block).
- **Persistent `httpx.Client`** with HTTP/2 — single TCP/TLS connection,
  request multiplexing. Previously each block fetch created a brand-new
  client (new handshake every request).
- **`MAX_CONCURRENT = 16`** concurrent block fetches via a thread pool,
  exploiting HTTP/2 multiplexing. Server-side stream limits are respected
  with automatic retry (up to 3×) on stream closure.
- **Graceful HTTP/2 fallback** — if `h2` is not installed, falls back
  to HTTP/1.1 with keep-alive (still better than per-request client
  creation).

**Performance on wikipedia.fsk (1.3 GB from HuggingFace):**

| Metric | Before | After |
|--------|--------|-------|
| Open time | >20 s | ~4 s |
| 1.6 MB index offset table | ~425 serial requests (~7 min) | ~7 concurrent requests (~2 s) |

Requires `httpx[http2]` — updated in `pyproject.toml` dependencies.

### Bug fix: `FlatseekFileStorageAdapter.listdir` for `index/` subdirs

`_read_posting` relies on `listdir(bucket_dir)` to enumerate `.bin` files
inside each index bucket. For local files this uses `os.listdir`; for
remote `.fsk` (via `FlatseekFileStorageAdapter`) it previously returned
an empty list because `listdir` wasn't correctly filtering the section-prefixed
offset table keys. Now returns the correct file list — search over remote
`.fsk` archives returns real results.

### Bug fixes & polish on top of v0.1.8

These accumulated on top of the original v0.1.8 release; kept in 0.1.8
to avoid a patch-version churn for what are mostly bug fixes + perf.

#### Encryption: clearer errors, never block serve

- **Locked-manifest detection.** When an encrypted `.fsk` is opened
  without the right passphrase, the storage adapter now reports
  `is_manifest_locked()` and `locked_fields()`. `cmd_search` prints a
  WARNING and returns 0 results instead of crashing with
  `AttributeError: 'str' object has no attribute 'get'`. The API still
  returns 403 from the search route and prompts Flatlens for the
  password — the original "Flatlens prompts" flow is preserved.
- **`flatseek serve` never blocks on passphrase prompt.** A
  `--passphrase`/`FLATSEEK_PASSPHRASE`/TTY prompt would have hung
  server startup if the operator didn't pre-unlock. Now the server
  starts immediately, prints a WARNING, and lets Flatlens prompt the
  client at request time.
- **Passphrase resolution chain.** All commands now look for the
  passphrase in priority order: `--passphrase` CLI flag →
  `FLATSEEK_PASSPHRASE` env var → interactive `getpass` prompt (TTY
  only). The env-var path lets users set it once in their shell rc
  (`export FLATSEEK_PASSPHRASE='…'`) and avoid the shell-history leak
  of putting the secret on the command line.

#### `flatseek slice` performance fixes (work on 1.5M-bin indexes)

- **State file no longer bloats.** Previously the state file stored
  the full list of `bins_written` (up to 75 MB for 1.5M bins) and
  re-saved it every ~50 batches → O(N²) total work. State now stores
  only counts; on resume the filesystem walk reconstructs what's done.
  State file size: ~75 MB → ~12 KB.
- **Skip empty bins.** Low-selectivity queries on high-cardinality
  columns (e.g. `msg:error` on logs with 1.5M unique terms) left
  ~99% of bins with zero surviving postings. We now skip writing
  those bins entirely — query routing handles missing bucket files
  correctly (their terms just don't appear in the index).
- **zlib level 1 instead of 9.** For tiny bin files (<1KB typical)
  size difference is <5% but compress speed is 3-5× faster.
- **Combined 1-pass bin processing.** Previously the rewrite pass
  was followed by a "copy verbatim" pass for unchanged bins; now
  each worker task decides inline (rewrite vs copy vs skip).
- **Batched bins per worker task.** Reduces ThreadPool task-scheduling
  overhead on 1.5M+ bin workloads.

Combined throughput: ~100 bins/sec → ~11,000 bins/sec on the same
machine (113× speedup). For a 1.5M-bin slice: ~4 hours → ~3 minutes.

## [0.1.8] - 2026-06-29

### New: `flatseek verify` — chunk-level integrity check

- **`flatseek verify <path>`** walks every doc chunk (read → decrypt →
  decompress → parse) and reports load failures. Supports both index
  directories and single-file archives (`.fsk` / `.flatseek` / `.flat`),
  in plaintext or encrypted form.
- Useful for diagnosing encryption interrupted mid-run, on-disk bit-rot,
  compression-format mismatch, and partial plaintext in an encrypted index.
- Options: `--passphrase` for encrypted sources, `--mode {all,sample}` +
  `--sample-size N` for spot checks, `--show N` to control how many
  corrupt chunks are listed.
- Exits with code 0 on success, 2 on any chunk failure.
- New test file `tests/test_verify.py` (11 cases) covers all 4
  source/encryption combinations, extension aliases, wrong-passphrase
  detection, and sample mode.

### New: `flatseek slice` — extract a query-matched subset as a new index

- **`flatseek slice <source> <query> -o <output>`** materializes a new
  standalone flatseek index containing only the docs matching a Lucene
  query. Use cases: per-region index partitioning, privacy-filtered
  sub-indexes for individual customers, demo subsets.
- **Output format** auto-detected from extension: `.fsk` /
  `.flatseek` / `.flat` → single file; anything else → directory mirror.
- **Streaming** — groups matching IDs by chunk_start, then reads one chunk
  at a time via `qe._iter_chunks(match_chunks=…)`. Memory bounded by
  `chunk_size × n_workers + 1` regardless of source size.
- **doc_ids preserved** — a doc with id 42 in the source has id 42 in
  the slice. Downstream joins and lookups keep working trivially.
- **Encryption inheritance**: encrypted source → encrypted output with a
  fresh salt (cryptographically independent from source). Override with
  `--force-plaintext` or `--out-passphrase NEW` (re-key with new
  passphrase + new salt).
- **Atomic write**: writes to a temp directory first, then renames to
  the final path on success. A mid-flight crash leaves no partial output.
- **Empty result** is a clean exit — no output written (no empty `.fsk`
  or directory that looks valid but has no data).
- **V1 limitations**: text-search only. Doc-values (`dv/<field>/*`) are
  NOT regenerated, so range queries on numeric fields won't work in
  the slice. `index/<bucket>/terms.set` files are NOT pre-built
  (lazy-built on first query for each bucket, with a small first-hit
  latency penalty). Both are documented follow-ups.
- New test file `tests/test_slice.py` (10 cases) covers plaintext +
  encrypted × dir + .fsk, force-plaintext override, doc-id preservation,
  posting-list filter correctness, AND query, empty result, and FTS
  prefix queries.

### Bug fixes — `QueryEngine` init-in-wrong-place pattern

Two attributes were being initialized in `clear_doc_cache()` instead of
`__init__()`, breaking any code path that called the referencing method
on a fresh QE that hadn't yet run an export-style pagination loop:

- **`_wal_posting_cache`** — set in `clear_doc_cache`, written by
  `_read_wal_postings`. Caused `AttributeError: 'QueryEngine' object
  has no attribute '_wal_posting_cache'` on the first live-search
  query against an ongoing build (WAL present, no prior
  `clear_doc_cache` call).
- **`_profile` and `_phase_times`** — set in `clear_doc_cache`, read by
  `_timed_phase` (used inside the wildcard-verification path of
  `query()`). Caused `AttributeError: 'QueryEngine' object has no
  attribute '_profile'` from the API search route when a query
  contained a wildcard (e.g. `name:user*`).
- Both attributes moved to `__init__`. `clear_doc_cache` no longer
  initializes them and its docstring updated to call out which caches
  it does NOT touch.
- New regression test `tests/test_wal_search.py` (3 cases) locks in
  fresh-QE initialization for `_wal_posting_cache`. The FTS prefix
  test in `tests/test_slice.py` exercises the `_profile` path.

### Bug fix — `cmd_verify` for `.fsk` sources

- The original `cmd_verify` had two broken `.fsk` paths:
  - **Path resolution** reconstructed doc paths from `_file_offsets`
    keys with the wrong prefix format, leading to
    `FileNotFoundError: File not found in flatseek: docs/docs_*.zlib`
    for every chunk. Replaced with `storage._list_docs_recursive()`
    (the canonical source of paths inside the archive).
  - **QueryEngine crash on encrypted `.fsk`**: encrypted `.fsk`
    stores `stats.json` as a JSON string wrapping hex-encoded
    ciphertext, so `json.loads(...)` returns a `str` instead of a
    `dict`, crashing `QueryEngine.__init__` on `self.stats.get(...)`.
    `cmd_verify` no longer constructs a QE for `.fsk` — verify
    doesn't need one, it just iterates the docs section.
- Encrypted **index directory** also broken: `_apply_passphrase()`
    derived the key and stored it on the QE but didn't return it,
    so the local `enc_key` variable stayed `None` at chunk-read time
    and every encrypted chunk reported as "incorrect header check".
    Fix: `_apply_passphrase` now returns the derived key.
- New tests in `tests/test_verify.py` exercise all four
  source/encryption combinations for `verify`.

### Performance — `FlatseekFileStorageAdapter._iter_chunks(match_chunks=…)`

- Added optional `match_chunks` parameter. Chunks not in the keep set
  are skipped **before** the seek+read+decrypt+decompress step on
  `.fsk` sources. Critical for sparse queries on large encrypted
  `.fsk`: a query matching 10 chunks out of 1000 would otherwise spend
  ~99% of wall time reading chunks with no matches.
- `qe._iter_chunks` already plumbed this through; `cmd_export`
  (memory-safe streaming query iterator) and `cmd_slice` both benefit.

### `flatseek export` — memory-safe streaming (already in main, but
documented for the v0.1.8 release line)

- `_export_stream_query` rewritten to use
  `qe._iter_chunks(match_chunks=…)` plus a thread pool with bounded
  memory (`n_workers + 1` chunks in flight max). Replaces the prior
  pagination approach which loaded every chunk touched in a page into
  `_doc_cache`, OOM-killing the process on large result sets.

### Tests

- `tests/test_verify.py` (new, 11 cases)
- `tests/test_slice.py` (new, 10 cases)
- `tests/test_wal_search.py` (new, 3 cases — regression for
  `_wal_posting_cache`)
- `tests/test_term_offset_index.py` (new, ~205 lines — exercises the
  `_build_term_offset_index` lazy optimization, currently unused in
  production pending a silent-regression debug; left in place as a
  characterization test)
- `tests/test_export.py` extended (+221 lines) for the streaming
  rewrite coverage.

### Total

- 126 tests pass, no regressions.
- 4 new commands (well, 2 new + 2 commands made actually work).
- Version bumped across `pyproject.toml`, `__init__.py`,
  `cli.py`, `client.py`, `api/main.py`.

## [0.1.7] - 2026-06-28

### Resume & Integrity — encrypt/decrypt

- **`flatseek encrypt/decrypt` now safely resumes an interrupted run.**
  By default, re-running on a partially-encrypted directory skips
  already-processed files via the `FLATSEEK\x01` magic-header detection.
  The salt in `encryption.json` is preserved across reruns, so key
  material stays stable and previously-encrypted files remain readable.
- **`--no-resume`** flag forces re-encryption of every file. For files that
  are already encrypted, this first decrypts with the current key then
  re-encrypts with a fresh nonce (no double-encryption corruption).
- **`--verify`** flag: after the run, decrypts a random sample of
  encrypted files back to verify the ChaCha20-Poly1305 auth tag. Reports
  N passed / K failed with file paths. `--verify-sample N` controls the
  sample size (default 100; 0 = all files).
- **`cmd_decrypt` probe step** now catches `cryptography.fernet.InvalidToken`
  on wrong passphrase (previously only `ValueError` was caught, which let
  `InvalidToken` bubble up uncaught and crash the CLI).

### Streaming pack/unpack — OOM-safe on very large indexes

- **`flatseek pack`** now streams sections incrementally to the output file.
  Each section's bytes pass through a single `sha256` hasher + 4 MB write
  chunk, never accumulating in memory. Total pack peak memory is bounded by
  `CHUNK_SIZE × ~2` (≈ 8 MB) regardless of total output size.
- **`flatseek unpack`** now reads just the offset-table prefix of each
  section, then seeks to each file's absolute offset in the `.fsk` and
  copies its bytes directly to disk. No `section_data` accumulation, no
  `write_tasks` list. Peak memory is bounded by `max(file_size) × ~2`
  (per-file AEAD still requires whole-file reads).
- **`pack._validate`** also streams — hashes each section in 4 MB blocks.
- **File format unchanged** — same 1024-byte header layout, same section
  descriptors, same checksum semantics. Existing `.fsk` files still work.
- **Pre-existing bug fix:** duplicate `_validate` method shadowed the real
  validation with a no-op stub. Removed; real checksum verification now runs.
- New test file `tests/test_pack_streaming.py` covers byte-exact roundtrip
  on 20 MB synthetic indexes, checksum validation, corruption detection, and
  peak-memory bounds via `tracemalloc`.

### Export — JSONL & CSV streaming with resume

- **New subcommand `flatseek export`** streams documents from an index to
  **JSONL** (default) or **CSV**, optionally filtered by the same Lucene-like
  query syntax used by `search`. Reads chunks one at a time from disk, so
  very large exports are OOM-safe (peak memory bounded by chunk size, not
  total size).
- **Output targets.** `-o PATH` writes to a file; omit `-o` to stream to
  stdout (clean for piping to `jq`, `csvkit`, `awk`, …). Progress goes to
  stderr only when writing to a file.
- **No query → export all.** Without `--query`, every document in the index
  is exported in storage order.
- **Column subset** via `--columns col1,col2,…`. **Row cap** via `--limit N`.
- **Resume / interrupt.** A sidecar checkpoint file
  `{output}.flatseek-export-checkpoint.json` records `last_doc_id` plus the
  query, format, and column set. Re-running the same command picks up where
  it left off and appends only the missing rows. SIGINT/SIGTERM flush the
  checkpoint before exiting. Stdout exports cannot be resumed (silently
  ignored). `--force-restart` (`--no-resume`) discards existing output +
  checkpoint.
- **Mismatch handling.** Query or format mismatch with the checkpoint exits
  with code 2 and points at `--force-restart`. Column mismatch emits a
  warning and continues.
- **Storage support.** Works on directory indexes, `.fsk` flat files
  (`FlatseekFileStorageAdapter`), encrypted indexes (`--passphrase`), and
  remote storage backends (S3 / Vercel Blob / URL — same flags as `search`).
- Documented in `docs/cli.md` under `flatseek export`.
- New test file `tests/test_export.py` covers JSONL/CSV, query, stdout,
  columns, limit, CSV escaping, nested values, encrypted index, `.fsk`
  flat file, resume from checkpoint, query/format mismatch, column
  mismatch warning, `--force-restart`, stdout-no-resume, and `--quiet`.

## [0.1.6] - 2026-06-27

### Pack & Unpack — Portable `.fsk` Format

- **`flatseek pack <dir> -o out.fsk`**: Pack index directory into single portable `.fsk` binary file
  - Preserves all index data, docs, and doc_values in one archive
  - `--passphrase` encrypts the packed file (ChaCha20-Poly1305)
  - Manifest stored as plaintext JSON — enables key derivation without circular dependency
  - Encryption state preserved: encrypted source + `--passphrase` → encrypted `.fsk`
- **`flatseek unpack file.fsk -o <dir>`**: Unpack `.fsk` file back to index directory
  - Handles both OLD format (encrypted manifest, `__b64` values) and NEW format (plaintext manifest, encrypted sections)
  - `--passphrase` decrypts encrypted `.fsk` files
  - `encryption.json` restored from manifest metadata

### API Encryption Auth — Single-File & Unpacked Index Modes

- **`GET /{index}/_is_encrypted`**: Fixed path detection for unpacked directories (data_dir IS the index) and single-file `.fsk` mode
- **`POST /{index}/_authenticate`**: Fixed authentication for:
  - **Single-file `.fsk`**: reads manifest `_encryption_b64` instead of filesystem `encryption.json`
  - **Unpacked directories**: handles self-as-index path (`data_dir` = index root)
  - **Plaintext files**: key derivation success is sufficient proof (no `verify_token` needed)
- **`is_encrypted()` in IndexManager**: Returns correctly for single-file mode via storage adapter's `_is_encrypted` flag; returns `false` when key provided via `--passphrase`

### Serve API — Encryption Flow

- **`flatseek serve` with `--passphrase`**: Key stored in `FLATSEEK_FSK_KEY` env → `is_encrypted` returns `false` → no password prompt in Flatlens
- **`flatseek serve` without `--passphrase`**: Key not stored → `is_encrypted` returns `true` → Flatlens prompts for password
- **Encrypted `.fsk` without `--passphrase`**: Server starts with warning, `is_encrypted=true`, user authenticates via API

### Bug Fixes

- **`query_engine.py`**: `decrypt_bytes` was never imported → `NameError` silently swallowed → `_iter_chunks` returned no data for encrypted indexes
- **`query_engine.py`**: `total_docs` counted all documents instead of only matching ones → `aggregate` always showed wrong hit counts
- **`deps.py`**: `list_indices()` and `get_engine()` didn't detect self-as-index directories → served wrong paths for unpacked `.fsk` dirs


### Live Search During Ongoing Builds
- **WAL-Aware Query Engine**: Search now reads from both `index/` and `_wal/` directories
  - Solves the "search returns 0 while aggregate has data" issue during ongoing builds
  - No more waiting for build to complete before searching
- **Automatic WAL Flush Thread**: Builder now flushes WAL to index every 60 seconds
  - Makes index searchable in near real-time while building
  - Survives builder restart (uses `_wal_merged.txt` marker)
- **New CLI Commands**:
  - `flatseek wal info` — show WAL file statistics
  - `flatseek wal merge` — merge WAL files (rescue tool for builds without flush thread)

### CLI Improvements
- `wal merge` now detects already-merged files and syncs segment counters

## [0.1.4] - 2026-05-08

### Hosting & UX
- **HuggingPost** — billion-row indexes hosted on HuggingFace (free)
  - Public datasets: `https://huggingface.co/buckets/flatseek/flatdata` (adsb, article, sosmed, standard — 10M rows each)
- **Flatlens** — visual search dashboard at [flatlens.flatseek.io](https://flatlens.flatseek.io)
  - Direct links with bucket+index pre-filled: `flatlens.flatseek.io/?bucket=...&index=...`

### Security Fixes
- **Critical**: Server-global password storage removed (`IndexManager._index_passwords`)
  - Password MUST now come from `x-index-password` HTTP header every request
  - Prevents cross-session data leakage where Browser A's auth bleeds to Browser B
- `_stats` and `_mapping` endpoints now return 403 for encrypted indexes without password
- Engines for encrypted bucket URLs are never cached (fresh engine per request)
- `_authenticate` now verifies password by decrypting a known file before marking as authenticated

### HuggingFace Bucket URL Support
- **New URL pattern**: `https://huggingface.co/buckets/{owner}/{repo}` is now supported
  - Buckets use `/resolve/{index_name}` (index IS the revision), not `/resolve/main`
- **Index listing** for buckets: HTML scraping of HF web UI to extract index names
- `_list_url_indices` handles three patterns:
  - Multi-index repos (root folder with index subdirs)
  - Direct index repos (single index named after repo)
  - **NEW** Bucket repos (uses web UI parsing for index discovery)

### API Improvements
- Bucket URL parameter (`?bucket=<url>`) support across all search/aggregate endpoints
- Debug endpoint `GET /_debug/chunks/{index}` shows chunk file paths for URL storage
- `_aggregate` endpoint supports both POST body and GET query params

### CLI Improvements
- `flatseek build --bucket <url>` for building indexes from HuggingFace bucket URLs

## [0.1.3] - 2026-05-07
### Changed
- Lazy dashboard mounting (not loaded until first access)
- Version bump

## [0.1.2] - 2026-05-06
### Added
- Initial release documentation