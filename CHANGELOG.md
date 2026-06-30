# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.8] - 2026-06-30

### Bug fixes & polish on top of v0.1.8

These accumulated on top of the original v0.1.8 release; kept in 0.1.8
to avoid a 0.1.9 bump for what are mostly bug fixes + perf.

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