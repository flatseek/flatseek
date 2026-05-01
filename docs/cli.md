# CLI Reference

---

## Table of Contents

- [Overview](#overview)
- [Command Overview](#command-overview)
- [`flatseek build`](#flatseek-build)
- [`flatseek classify`](#flatseek-classify)
- [`flatseek plan`](#flatseek-plan)
- [`flatseek generate`](#flatseek-generate)
- [`flatseek search`](#flatseek-search)
- [`flatseek join`](#flatseek-join)
- [`flatseek chat`](#flatseek-chat)
- [`flatseek stats`](#flatseek-stats)
- [`flatseek serve`](#flatseek-serve)
- [`flatseek api`](#flatseek-api)
- [`flatseek dashboard`](#flatseek-dashboard)
- [`flatseek compress`](#flatseek-compress)
- [`flatseek encrypt`](#flatseek-encrypt)
- [`flatseek decrypt`](#flatseek-decrypt)
- [`flatseek delete`](#flatseek-delete)
- [`flatseek dedup`](#flatseek-dedup)
- [Index Directory Layout](#index-directory-layout)
- [Cross-Command Behavior](#cross-command-behavior)
- [Common Workflows](#common-workflows)

---

## Overview

The Flatseek CLI (`flatseek`) manages the full lifecycle: build, query, serve, and maintain indexes. File-based — no server or cluster required.

| Use case | Command |
|----------|---------|
| Build index | `flatseek build` |
| Query | `flatseek search` |
| Serve API + dashboard | `flatseek serve` |
| Classification / planning | `flatseek classify`, `flatseek plan` |
| Maintenance | `flatseek compress`, `encrypt`, `decrypt`, `dedup`, `delete` |

**CLI vs API vs Python client**: CLI is zero-config and disk-based. The REST API serves remote/http clients. The Python client (`from flatseek import Flatseek`) is for programmatic access within Python code.

---

## Command Overview {#command-overview}

| Command | Description |
|---------|-------------|
| `build` | Build trigram index from CSV/JSON |
| `classify` | Detect column semantic types → `column_map.json` |
| `plan` | Generate `build_plan.json` for parallel builds |
| `generate` | Generate dummy dataset (benchmarking) |
| `search` | Query the index from CLI |
| `join` | Cross-dataset join on shared field |
| `chat` | Natural-language query REPL (Ollama) |
| `stats` | Print index stats and column schema |
| `serve` | Start API server + flatlens dashboard |
| `api` | Start API server only (no dashboard) |
| `dashboard` | Start flatlens dashboard only |
| `compress` | Compress index files in-place with zlib |
| `encrypt` | Encrypt index in-place (ChaCha20-Poly1305) |
| `decrypt` | Decrypt index in-place |
| `delete` | Delete index directory (parallel rm -rf) |
| `dedup` | Remove duplicate docs from index |

---

## `flatseek build`

Build trigram index from one or more CSV/JSON/JSONL files.

#### What it does

1. Detects column types (value sampling + semantic classification)
2. Tokenizes values → generates trigrams → CRC32 hash bucketing
3. Writes posting lists to `index/{aa}/{bb}/idx.bin`
4. Writes doc chunks to `docs/{aa}/{bb}/docs_{N}.zlib`
5. At `finalize()`, merges segments → writes doc_values + stats

**Parallel mode** (`-w N`): auto-generates `build_plan.json`, spawns N subprocess workers, merges results. Resume-safe — interrupted builds restart from last completed worker.

**Daemon mode** (`--daemon`): never flushes buffers to disk during checkpoint. All data stays in RAM; lazy writer drains buffers ≥512 KB every 10 s. Finalize writes remaining buffers at end. Use when RAM is ample (≥8 GB free) and indexing speed is critical.

#### Syntax

```
flatseek build <csv_dir> [-o OUTPUT] [options]
```

#### Arguments

| Name | Type | Required | Description |
|------|------|---------|-------------|
| `csv_dir` | string | Yes | CSV file or directory containing CSV files |

#### Options

| Flag | Default | Description |
|------|---------|-------------|
| `-o`, `--output` | `./data` | Output directory |
| `-m`, `--map` | auto-detect | Path to `column_map.json` |
| `--dataset` | None | Dataset label (stored in stats) |
| `-s`, `--sep` | `,` | CSV delimiter character |
| `--columns` | None | Comma-sep column names for headerless files (first row = data) |
| `-w`, `--workers` | `1` | Parallel worker count. `>1` auto-splits and spawns subprocess workers |
| `--plan` | None | Path to `build_plan.json` (for distributed workers) |
| `--worker-id` | None | Worker index (0-based, used with `--plan`) |
| `--estimate` | `False` | Sample 5,000 rows upfront to estimate speed, ETA, storage |
| `--dedup` | `False` | Skip duplicate rows during indexing (hash all non-meta fields) |
| `--dedup-fields` | None | Dedup on specific columns only (e.g. `phone,nik`) |
| `--daemon` | `False` | RAM-buffered mode: never write prefix buffers to disk at checkpoint |

#### Examples

```bash
# Single-file build
flatseek build ./transactions.csv -o ./data

# Parallel build (8 workers, auto-plan)
flatseek build ./transactions.csv -o ./data -w 8

# Estimate before committing
flatseek build ./transactions.csv -o ./data --estimate

# Dedup on specific key column
flatseek build ./transactions.csv -o ./data --dedup-fields tx_id

# Daemon mode (all RAM, max speed)
flatseek build ./transactions.csv -o ./data --daemon
```

#### Notes

- **Resume**: if `build_plan.json` exists and worker count matches, interrupted builds resume from last completed worker
- **Stale plan detection**: if `build_plan.json` has `estimated_rows=0` for all workers (byte-range split failed on compressed file), plan is regenerated automatically
- **`--dedup` vs `dedup` command**: `--dedup` catches duplicates within a build session/worker shard. Use `flatseek dedup` for cross-worker dedup after parallel build

---

## `flatseek classify`

Detect column semantic types from a directory of CSV files and write `column_map.json`.

#### What it does

1. Samples up to 200 rows per column
2. Runs value-based detection (type priority cascade) + semantic name matching
3. Writes `{filename: {col: type, ...}, ...}` to `column_map.json`

Used to pre-classify before a parallel build (avoids concurrent writes to `column_map.json` by N workers).

#### Syntax

```
flatseek classify <csv_dir> [-o OUTPUT] [options]
```

#### Options

| Flag | Default | Description |
|------|---------|-------------|
| `-o`, `--output` | `./data/column_map.json` | Output path |
| `-s`, `--sep` | `,` | CSV delimiter |
| `--columns` | None | Column names for headerless files |

#### Examples

```bash
flatseek classify ./raw_data -o ./data/column_map.json
flatseek classify ./raw_data --columns id,name,status,amount
```

---

## `flatseek plan`

Generate `build_plan.json` for parallel/distributed indexing. Does not execute the build.

#### What it does

1. Scans input files, estimates total rows
2. Computes byte-range splits per worker
3. Writes `build_plan.json` to output directory

Workers use `build --plan build_plan.json --worker-id N` to pick up their slice.

#### Syntax

```
flatseek plan <csv_dir> [-o OUTPUT] [options]
```

#### Options

| Flag | Default | Description |
|------|---------|-------------|
| `-o`, `--output` | `./data` | Output directory |
| `-n`, `--workers` | `4` | Number of parallel workers |
| `-s`, `--sep` | `,` | CSV delimiter |
| `--columns` | None | Column names for headerless files |

#### Examples

```bash
flatseek plan ./transactions.csv -o ./data -n 8
```

---

## `flatseek generate`

Generate a dummy dataset for benchmarking. Depends on `flatbench` sibling repository.

#### What it does

Calls `flatbench.generators.generate_dataset()` with the given schema and row count.

#### Syntax

```
flatseek generate -o OUTPUT [options]
```

#### Options

| Flag | Default | Description |
|------|---------|-------------|
| `-o`, `--output` | (required) | Output file path |
| `-r`, `--rows` | `100000` | Number of rows |
| `-s`, `--schema` | `standard` | Schema: `standard`, `ecommerce`, `logs`, `nested`, `sparse`, `article`, `adsb`, `campaign`, `devops`, `sosmed`, `blockchain` |
| `-f`, `--format` | `csv` | Output format: `csv`, `jsonl` |
| `--seed` | None | Random seed |

#### Examples

```bash
flatseek generate -o ./benchmark_data.csv -r 1000000 -s blockchain
flatseek generate -o ./data.jsonl -r 500000 -s ecommerce -f jsonl
```

---

## `flatseek search`

Query the index from the command line.

#### What it does

1. Loads `QueryEngine` on the data directory
2. Parses Lucene query string (from `-c`/`--and` convenience flags or raw query arg)
3. Executes via `qe.query()`
4. Prints paginated results

#### Syntax

```
flatseek search <data_dir> [query] [options]
```

#### Arguments

| Name | Type | Required | Description |
|------|------|---------|-------------|
| `data_dir` | string | Yes | Index directory (build output) |
| `query` | string | No | Search term (use `%` for wildcard, `%%` for CLI-safe wildcard) |

#### Options

| Flag | Default | Description |
|------|---------|-------------|
| `-c`, `--column` | None | Restrict to specific column |
| `-p`, `--page` | `0` | Page number (0-based) |
| `-n`, `--page-size` | `20` | Results per page |
| `--and` | (repeatable) | Additional `col:term` AND conditions |
| `--passphrase` | None | Decryption passphrase (prompted if index is encrypted) |

#### Examples

```bash
flatseek search ./data "program:raydium"
flatseek search ./data "signer:*7xMg*"
flatseek search ./data -c status active
flatseek search ./data "" --and program:raydium --and amount:>1000000
flatseek search ./data "status:success" -p 2 -n 50
```

#### Notes

- `%` in the shell may be interpreted by zsh/glob — use `%%` or quote the query: `"signer:*7xMg*"`
- `--and` values must be in `col:term` format, repeat for multiple conditions

---

## `flatseek join`

Cross-dataset join on a shared field value.

#### What it does

1. Resolves two queries (A and B) against the index
2. Matches documents from both result sets on the shared `on` field
3. Returns paired documents as results

#### Syntax

```
flatseek join <data_dir> <query_a> <query_b> --on FIELD [options]
```

#### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--on` | (required) | Shared field to join on |
| `-p`, `--page` | `0` | Page number |
| `-n`, `--page-size` | `20` | Results per page |
| `--passphrase` | None | Decryption passphrase |

#### Examples

```bash
flatseek join ./data "_dataset:txs AND program:raydium" "_dataset:logs AND service:api" --on signer
```

---

## `flatseek chat`

Interactive natural-language query REPL. Requires Ollama running locally.

#### What it does

1. Loads `QueryEngine` on the data directory
2. Starts `ChatInterface` with specified model
3. REPL loop: user types NL → LLM converts to JSON conditions → `qe.search()` / `qe.search_and()` → results

#### Syntax

```
flatseek chat <data_dir> [options]
```

#### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--model` | `qwen2.5-coder` | Ollama model name |
| `--api-base` | `http://localhost:11434/v1` | Ollama API base URL |

#### Examples

```bash
flatseek chat ./data
flatseek chat ./data --model llama3.3
```

---

## `flatseek stats`

Print index statistics and column schema.

#### What it does

Calls `QueryEngine(data_dir).summary()`, which reads `stats.json` and returns total docs, index size, column names, inferred types, and encryption status.

#### Syntax

```
flatseek stats <data_dir>
```

#### Examples

```bash
flatseek stats ./data
```

---

## `flatseek serve`

Start the FastAPI server + flatlens dashboard.

#### What it does

1. Sets `FLATSEEK_DATA_DIR`, `FLATSEEK_PORT`, `FLATSEEK_API_BASE` env vars
2. Auto-detects or auto-installs flatlens dashboard
3. Launches uvicorn on `host:port` with `flatseek.api.main:app`
4. Opens dashboard URL in browser

#### Syntax

```
flatseek serve [options]
```

#### Options

| Flag | Default | Description |
|------|---------|-------------|
| `-d`, `--data` | current directory | Data directory to serve |
| `-p`, `--port` | `8000` | Port number |
| `--host` | `0.0.0.0` | Host to bind |
| `--no-reload` | `False` | Disable uvicorn auto-reload |
| `--flatlens-dir` | auto-detect | Path to flatlens installation |

#### Examples

```bash
flatseek serve -d ./data
flatseek serve -d ./data -p 9000 --no-reload
```

---

## `flatseek api`

Start the API server only (no dashboard).

#### What it does

Same as `serve` but does not set `FLATSEEK_API_BASE` and skips flatlens mounting.

#### Syntax

```
flatseek api [options]
```

#### Options

| Flag | Default | Description |
|------|---------|-------------|
| `-d`, `--data` | current directory | Data directory to serve |
| `-p`, `--port` | `8000` | Port number |
| `--host` | `0.0.0.0` | Host to bind |
| `--no-reload` | `False` | Disable auto-reload |

#### Examples

```bash
flatseek api -d ./data -p 8001
```

---

## `flatseek dashboard`

Start flatlens dashboard only (requires a running API).

#### What it does

1. Finds or auto-installs flatlens
2. Patches `js/api.js` with the provided `--api-base`
3. Starts `npx http-server` on the given port

#### Syntax

```
flatseek dashboard [options]
```

#### Options

| Flag | Default | Description |
|------|---------|-------------|
| `-p`, `--port` | `8080` | Dashboard port |
| `--api-base` | `http://localhost:8000` | API base URL |
| `--flatlens-dir` | auto-detect | Path to flatlens installation |

#### Examples

```bash
flatseek dashboard --api-base http://localhost:8000 -p 8080
```

---

## `flatseek compress`

Compress all `idx.bin` files in-place using zlib.

#### What it does

1. Walks `index/` directory for all `*.bin` files
2. Skips files already compressed (detected by zlib magic bytes `0x78 0x01/0x5e/0x9c/0xda`)
3. Compresses remaining files in parallel with `ThreadPoolExecutor`
4. Rewrites in-place with `os.replace()`

Resume-safe: re-running continues from already-compressed files.

#### Syntax

```
flatseek compress <data_dir> [options]
```

#### Options

| Flag | Default | Description |
|------|---------|-------------|
| `-l`, `--level` | `6` | zlib compression level 1–9 |
| `-w`, `--workers` | `min(8, cpu_count)` | Parallel workers |

#### Examples

```bash
flatseek compress ./data
flatseek compress ./data -l 9 -w 4
```

#### Notes

- Run after build, before `encrypt`. Compression before encryption yields better ratio (encryption preserves bytes, not compressibility)
- Do NOT run before incremental builds — builder appends raw bytes and cannot extend compressed files

---

## `flatseek encrypt`

Encrypt all index and doc files in-place with ChaCha20-Poly1305.

#### What it does

1. Prompts for passphrase (or uses `--passphrase`)
2. Derives key via PBKDF2-HMAC-SHA256 (600,000 iterations, 32-byte salt)
3. Stores salt in `encryption.json` (do not delete)
4. Encrypts all `index/*.bin` and `docs/*.zlib` in parallel
5. Magic header `FLATSEEK\x01` enables detection on read

#### Syntax

```
flatseek encrypt <data_dir> [options]
```

#### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--passphrase` | prompted | Encryption passphrase |

#### Examples

```bash
flatseek encrypt ./data
flatseek encrypt ./data --passphrase mysecret
```

#### Notes

- **No recovery** without the passphrase
- Encrypted indexes cannot accept incremental builds — run `decrypt`, rebuild, then `encrypt` again
- Run after `compress` for best results (encryption preserves byte size, not compressibility)

---

## `flatseek decrypt`

Decrypt an encrypted index in-place.

#### What it does

1. Prompts for passphrase (or uses `--passphrase`)
2. Derives key via `load_encryption_key()`
3. Verifies passphrase against a probe file before touching anything (fail-fast)
4. Decrypts all files in parallel
5. Removes `encryption.json` on success

#### Syntax

```
flatseek decrypt <data_dir> [options]
```

#### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--passphrase` | prompted | Decryption passphrase |

#### Examples

```bash
flatseek decrypt ./data
flatseek decrypt ./data --passphrase mysecret
```

---

## `flatseek delete`

Delete an index directory fast using parallel `rm -rf`.

#### What it does

1. Renames `data_dir` → `data_dir.deleting_{pid}` immediately (atomic, frees the original path name)
2. Walks 2 levels deep to collect ~256+ entry chunks
3. Spawns sliding-window of `rm -rf` subprocess workers (C-level unlink is faster than Python `os.unlink()` for large trees)
4. Cleans up staging directory

Resume-safe: re-running detects and resumes any leftover `.deleting_*` directories.

#### Syntax

```
flatseek delete <data_dir> [options]
```

#### Options

| Flag | Default | Description |
|------|---------|-------------|
| `-y`, `--yes` | `False` | Skip confirmation prompt |
| `-w`, `--workers` | `min(16, cpu_count)` | Parallel rm -rf workers |

#### Examples

```bash
flatseek delete ./data
flatseek delete ./data -y
```

---

## `flatseek dedup`

Remove duplicate documents from the index after a build.

#### What it does

4-step process:

1. **Scan** — reads all `docs/*.zlib` chunks, computes fingerprint per doc (hash of specified fields, or all non-meta fields if `--fields` not given)
2. **Checkpoint** — saves `dead_ids` to `dedup_checkpoint.json` (resume support)
3. **Rewrite doc chunks** — removes dead doc IDs from doc store, parallelized
4. **Rewrite posting lists** — removes dead doc IDs from all `*.bin` index files, parallelized
5. **Update stats** — sets `total_docs` to `kept`, records `_dedup_removed`

Resume: if `dedup_checkpoint.json` exists, scan step is skipped and only rewrite steps run.

#### Syntax

```
flatseek dedup <data_dir> [options]
```

#### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--fields` | all non-meta fields | Canonical column keys to use for fingerprint |
| `--dry-run` | `False` | Report duplicates without writing changes |
| `-w`, `--workers` | `min(8, cpu_count)` | Parallel workers for rewrite phase |

#### Examples

```bash
# Dry run first
flatseek dedup ./data --dry-run

# Dedup on specific key
flatseek dedup ./data --fields tx_id,signer

# Full dedup (all fields)
flatseek dedup ./data
```

#### Notes

- Works on indexes built with parallel workers (including resumed builds)
- Checkpoint is deleted on successful completion
- Chunk rewrite is parallel; posting list rewrite is parallel; scan is single-threaded but resumable

---

## Index Directory Layout {#index-directory-layout}

Commands that operate on an index (`search`, `stats`, `compress`, `encrypt`, `dedup`, `delete`) expect this structure:

```
<data_dir>/
  index/                  # Trigram postings (65,536 bucket dirs)
    {aa}/{bb}/
      idx.bin            # Compressed posting lists
      idx_w0.bin         # Worker shards (parallel builds)
      idx_seg*.bin       # Immutable segments (large prefixes)
      terms.set          # Fast-reject frozenset
  docs/                  # Document chunks
    {aa}/{bb}/
      docs_{N}.zlib      # 100K docs each, zlib/zstd compressed
  dv/                    # Doc values (columnar)
    {field}/
      numeric.bin        # (doc_id: u64, value: f64), sorted by value
      terms.bin          # (term, count), sorted by count
  stats.json              # total_docs, columns, sizes
  column_map.json         # Column type overrides
  manifest.json           # next_doc_id, build metadata
  build_plan.json         # Parallel build plan (if applicable)
  encryption.json          # Salt + algorithm (if encrypted)
  dedup_checkpoint.json   # Dedup resume checkpoint (if applicable)
```

---

## Cross-Command Behavior

### Column Mapping

`build`, `classify`, and `plan` all share the column resolution logic via `_resolve_columns()`:

1. `--columns` flag → use as-is (no type override, first row = data)
2. `column_map.json` exists and matches column count → skip prompt, use stored
3. Column count changed since classification → re-classify
4. Otherwise → interactive prompt (skipped when stdin is not a tty)

Pre-classify with `flatseek classify` before parallel builds to avoid concurrent writes to `column_map.json`.

### Parallelism

- `-w N` on `build` → auto-generates `build_plan.json`, spawns N subprocess workers via `subprocess.Popen`
- Worker stdout/stderr redirected to `worker_{N}.log` (avoids tty conflicts in progress bar display)
- Parent poll loop aggregates progress from `progress_w{N}.json` files, shows sliding-window rate
- SIGTERM propagation ensures all workers die if parent is interrupted
- `--estimate` only runs in parent process; workers use plan-assigned doc_id ranges

### Output Formats

| Command | Output |
|---------|--------|
| `search` | CLI text (doc per line, pagination info) |
| `stats` | CLI text summary (`qe.summary()`) |
| `join` | CLI text (paired [A]/[B] sections) |
| `chat` | REPL (interactive) |
| All others | Single-line status + counts |

---

## Common Workflows

### Basic workflow

```bash
# Build
flatseek build ./data.csv -o ./data

# Serve
flatseek serve -d ./data

# Query
flatseek search ./data "program:raydium AND amount:>1000000"
```

### Parallel build

```bash
# Classify first (avoids concurrent writes)
flatseek classify ./raw_data -o ./data/column_map.json

# Plan
flatseek plan ./raw_data -o ./data -n 8

# Build (workers pick up plan automatically)
flatseek build ./raw_data -o ./data -w 8

# (optional) compress → encrypt
flatseek compress ./data -l 9
flatseek encrypt ./data
```

### Post-build deduplication

```bash
# Check for duplicates first
flatseek dedup ./data --dry-run

# Remove duplicates
flatseek dedup ./data --fields tx_id

# Compress and encrypt
flatseek compress ./data -w 4
flatseek encrypt ./data
```

### Benchmarking

```bash
# Generate a large dataset
flatseek generate -o ./benchmark.csv -r 5000000 -s blockchain

# Build with estimate
flatseek build ./benchmark.csv -o ./data --estimate -w 8

# Query
flatseek search ./data "program:raydium" -n 100
```