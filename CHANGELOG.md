# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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