# CLI Reference

All FlatSeek CLI commands. Run `flatseek --help` for the full list.

---

## build — Index data

```bash
flatseek build ./data.csv -o ./data
flatseek build ./csv_folder/ -o ./data
flatseek build ./data.json -o ./data
```

**Options:**

| Flag | Description |
|------|-------------|
| `-o PATH` | Output directory or `.fsk` file |
| `--single-file` | Output as single `.fsk` archive |
| `-m, --map PATH` | Column type overrides (`column_map.json`) |
| `--columns NAMES` | Column names for headerless files |
| `--dataset NAME` | Dataset label added as `_dataset` field |
| `--id-field NAME` | Natural key field (enables write operations) |
| `-s, --sep CHAR` | Delimiter (default: `,`) |
| `-w, --workers N` | Parallel workers (default: 1) |
| `--dedup` | Skip duplicate rows |
| `--estimate` | Sample 5,000 rows first, report speed/ETA |
| `--plan PATH` | Build plan for distributed workers |
| `--worker-id N` | Worker index for distributed builds |

**Examples:**

```bash
# Single FSK file
flatseek build ./data.csv -o ./data.fsk --single-file

# With ID field (enables insert/upsert/update/delete)
flatseek build ./data.csv -o ./data --id-field id

# Parallel build
flatseek build ./data.csv -o ./data -w 8

# TSV
flatseek build ./data.tsv -o ./data -s $'\t'
```

---

## insert-doc — Insert / upsert documents

```bash
# Single document (JSON)
flatseek insert-doc ./data --doc '{"id":"1","name":"Alice","email":"alice@example.com"}'

# CSV file
flatseek insert-doc ./data --file ./new_rows.csv

# JSONL file
flatseek insert-doc ./data --file ./new_docs.jsonl

# Auto-detect format from extension
flatseek insert-doc ./data --file ./new_docs.jsonl
flatseek insert-doc ./data --file ./new_rows.csv

# Force format
flatseek insert-doc ./data --file ./data.tsv --format csv

# Dry run (show without inserting)
flatseek insert-doc ./data --doc '{"id":"1","name":"Alice"}' --dry-run
```

**Behaviour:**
- If `--id-field` was set at build time and the doc has that field: **upsert** (replace existing)
- Otherwise: **insert** with auto-generated ULID `_id`
- `--file` supports both CSV and JSONL; format is auto-detected from extension

---

## update-doc — Update documents

```bash
# By query (all matching docs)
flatseek update-doc ./data -q "status:pending" --set "status=verified"

# By ID (requires --id-field at build time)
flatseek update-doc ./data --id "TXN-123" --set "status=verified,processed_by=admin"
```

`--set` is a **partial update** — only named fields are changed; all others untouched.

---

## delete-doc — Delete documents

```bash
# By query
flatseek delete-doc ./data -q "status:spam"

# By ID
flatseek delete-doc ./data --id "TXN-123"
```

Soft-delete (tombstoned, removable via `compact`).

---

## delete — Delete full index

```bash
flatseek delete ./data -y
```

Fast parallel `rm -rf`. Cannot be undone.

---

## compact — Flush WAL changes

```bash
flatseek compact ./data
```

Applies all WAL changes (updates/deletes) to the index permanently and clears the WAL.

---

## search — Search

```bash
flatseek search ./data "program:raydium AND amount:>1000000"
```

**Options:**

| Flag | Description |
|------|-------------|
| `data_dir` | Index directory, `.fsk` file, or remote URL |
| `query` | Search term (Lucene syntax) |
| `-c, --column NAME` | Restrict to column |
| `-p, --page N` | Page number, 0-based |
| `-n, --page-size N` | Results per page |
| `--and COL:TERM` | AND condition (repeatable) |
| `--index PATTERN` | Multi-index glob pattern |
| `--sort FIELD:asc\|desc` | Sort by field |
| `--passphrase PASS` | Decryption passphrase |

**Examples:**

```bash
# Basic
flatseek search ./data "ERROR"

# Wildcards
flatseek search ./data "GARUDA*"           # prefix
flatseek search ./data "*raydium*"         # infix
flatseek search ./data "RAYDIUM%%"         # suffix (trigram)

# Boolean
flatseek search ./data "program:raydium NOT status:failed"
flatseek search ./data "ERROR OR WARN"

# Range
flatseek search ./data "amount:[1000000 TO *]"

# Sort
flatseek search ./data "ERROR" --sort timestamp:desc
flatseek search ./data "ERROR" --sort "amount:desc,timestamp:asc"

# Pagination
flatseek search ./data "ERROR" -p 0 -n 20
flatseek search ./data "ERROR" -p 1 -n 20

# FSK file
flatseek search ./data.fsk "ERROR"

# Remote URL
flatseek search "https://example.com/data.fsk" "ERROR"

# Multi-index via --index
flatseek search ./fsk_bucket "ERROR" --index "logs_2025-*"
flatseek search ./fsk_bucket "ERROR" --index "logs_2025-01,logs_2025-02"

# Query embedded _index field
flatseek search ./fsk_bucket "_index:ads* AND ERROR"
```

---

## aggregate — Aggregations

```bash
flatseek aggregate ./data -q "status:success" \
  --agg "by_level:terms,level,10" \
  --agg "fee_stats:stats,fee"
```

See [docs/aggregate.md](aggregate.md) for full API reference.

---

## cross-lookup — Join two indexes on a shared key

Enrich source index results with lookups from a target index:

```bash
flatseek cross-lookup ./data/users ./data/txs \
  "status:active" \
  --on user_id \
  --left-fields "email,name" \
  --right-fields "tstamp,amount" \
  -n 20
```

**Options:**

| Flag | Description |
|------|-------------|
| `source_dir` | Source (left) index directory |
| `target_dir` | Target (right) index directory |
| `query` | Lucene query for source index |
| `--on FIELD` | Shared field name to join on (required) |
| `--left-fields` | Comma-separated fields to keep from source (default: all) |
| `--right-fields` | Comma-separated fields to keep from target (default: all) |
| `-n, --top-n` | Max results from source to process (default: 10) |
| `-p, --page` | Page number (default: 0) |
| `--page-size` | Results per page (default: 20) |

---

## stats — Index statistics

```bash
flatseek stats ./data
```

---

## export — Export to JSONL/CSV

```bash
flatseek export ./data -o out.jsonl
flatseek export ./data -f csv -o out.csv
```

**Options:**

| Flag | Description |
|------|-------------|
| `data_dir` | Index directory, `.fsk` file, or remote URL |
| `-o, --output PATH` | Output file (default: stdout) |
| `-f, --format jsonl\|csv` | Output format |
| `-q, --query Q` | Lucene query filter |
| `-c, --columns NAMES` | Column list |
| `-n, --limit N` | Max rows to export |
| `--index PATTERN` | Multi-FSK pattern |
| `--passphrase PASS` | Decryption passphrase |
| `--quiet` | Suppress progress |
| `--resume` / `--no-resume` | Resume or restart |

**Examples:**

```bash
# JSONL to stdout
flatseek export ./data -q "level:ERROR"

# CSV to file
flatseek export ./data -f csv -o out.csv

# Limit rows
flatseek export ./data -n 1000 -o out.jsonl

# FSK
flatseek export ./data.fsk -o out.jsonl

# Remote
flatseek export "https://example.com/data.fsk" -o out.jsonl

# Multi-FSK bucket
flatseek export ./fsk_bucket -q "*" --index "logs_2025-*" -o out.jsonl
```

---

## dedup — Remove duplicates

```bash
flatseek dedup ./data --fields "email,phone"
```

Uses `--fields` as fingerprint key. Duplicates keep the last-occurring document.

---

## slice — Extract matching subset

```bash
flatseek slice ./data -q "level:ERROR" -o sliced.fsk
```

---

## classify — Detect column semantic types

```bash
flatseek classify ./data.csv -o column_map.json
```

Outputs a `column_map.json` for use with `build --map`.

---

## plan — Generate build plan

```bash
flatseek plan ./data.csv -o build_plan.json
```

Generates a parallel/distributed build plan for use with `build --plan`.

---

## generate — Generate sample data

```bash
flatseek generate ecommerce --output ./data.csv --rows 10000
flatseek generate logs --output ./logs.csv --rows 50000
flatseek generate blockchain --output ./txs.csv --rows 100000
```

Schema presets: `ecommerce`, `logs`, `blockchain`.

---

## compress — Compress index files

```bash
flatseek compress ./data -l 6
```

Compresses index files in-place using zlib. Lower levels = faster, higher = smaller.

---

## verify — Verify index integrity

```bash
flatseek verify ./data
```

Checks for corrupt chunks and missing files.

---

## encrypt — Encrypt index

```bash
flatseek encrypt ./data --passphrase mypass
```

Encrypts index files in-place.

---

## decrypt — Decrypt index

```bash
flatseek decrypt ./data --passphrase mypass
```

Decrypts index files in-place.

---

## pack — Create .fsk archive

```bash
flatseek pack ./data -o ./data.fsk
```

**Options:**

| Flag | Description |
|------|-------------|
| `-o, --output PATH` | Output `.fsk` file |
| `--passphrase PASS` | Password-protect |
| `--enclosed` | Enclosed/subscription format |
| `--license-id ID` | License ID for enclosed |
| `--expire-ts TIMESTAMP` | Expiration timestamp |
| `--key HEX_KEY` | Custom encryption key |

**Examples:**

```bash
# Plain FSK
flatseek pack ./data -o ./data.fsk

# Password-protected
flatseek pack ./data -o ./protected.fsk --passphrase mypass

# Enclosed (subscription)
flatseek pack ./data -o ./enclosed.fsk --passphrase mypass --enclosed \
  --license-id LIC-123 --expire-ts 1767740800

# Custom key
flatseek pack ./data -o ./data.fsk --key $(python -c "import secrets; print(secrets.token_hex(32))")
```

---

## unpack — Extract .fsk archive

```bash
flatseek unpack ./data.fsk -o ./data
```

---

## wal — WAL management

```bash
flatseek wal ./data info
flatseek wal ./data merge
```

Manage Write-Ahead Log files from ongoing builds.

---

## serve — Start API server

```bash
flatseek serve ./data
flatseek serve ./data -p 9000
flatseek serve ./fsk_bucket
```

**Options:**

| Flag | Description |
|------|-------------|
| `data_dir` | Data directory, `.fsk` file, or omit for current dir |
| `-p, --port N` | Port (default: 8000) |
| `--storage-backend local\|s3\|vercel-blob\|url` | Storage backend |
| `--storage-bucket NAME` | S3/Vercel bucket |
| `--storage-region REGION` | AWS region |
| `--storage-url URL` | URL prefix for URL backend |
| `--passphrase PASS` | Default encryption passphrase |
| `--flatlens-dir PATH` | Flatlens dashboard path |

Dashboard starts automatically at `http://localhost:8000/dashboard`.

---

## api — Start API server only (no dashboard)

```bash
flatseek api ./data -p 9000
```

---

## generate-passphrase — Generate passphrase token

```bash
flatseek generate-passphrase
```

Generates a secure random passphrase for use with `--passphrase`.

---

## generate-license-key — Generate license key

```bash
flatseek generate-license-key
```

Generates a license key for enclosed/subscription FSK files.
