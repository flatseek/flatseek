# Indexing Guide

---

## Overview

Indexing converts flat files (CSV, JSON, JSONL) into a disk-based search index. The output is a directory of binary files — no database, no server, no cluster required.

**When to run indexing:**
- Before first search or serve
- After new data arrives (index is immutable — rebuild to update)
- After changing column type overrides

**What it produces:**
- Trigram posting lists for wildcard search
- Document chunks for result retrieval
- Columnar doc_values for aggregations and range queries

---

## Basic Indexing

### Build from CSV

```bash
flatseek build ./data.csv -o ./data
```

Flatseek reads the file, infers column types, tokenizes values, writes the index. By default, output goes to `./data/` (current directory = index name if file is in current dir).

### Build from directory

```bash
flatseek build ./csv_folder/ -o ./data
```

Recursively finds all CSV/JSON/JSONL files in the directory and indexes them as one dataset.

### What happens

1. File is sampled (first 200 rows per column) for type detection
2. Column names matched against semantic patterns (`status` → KEYWORD, `amount` → INT, etc.)
3. Tokenization + trigram generation
4. CRC32 hash bucketing → posting lists written to `index/`
5. Documents compressed into `docs/` chunks
6. Doc_values written to `dv/` for fast aggregation

---

## Indexing Different File Types

### CSV

```bash
flatseek build ./data.csv -o ./data
```

Delimiter detection is automatic. Use `-s` to override:

```bash
flatseek build ./data.tsv -o ./data -s $'\t'
```

### JSON / JSONL

```bash
flatseek build ./data.json -o ./data
```

FlatSeek handles:
- Single JSON array `[{"col": "val"}, ...]`
- JSON Lines / NDJSON (`{"col": "val"}\n{"col": "val"}\n`)
- JSON files with nested objects → stored as OBJECT type

### XLSX

```bash
flatseek build ./data.xlsx -o ./data
```

Reads the first sheet. Each column becomes a field. Type detection works the same as CSV.

---

## Output Structure

After a build, the index directory looks like this:

```
./data/
  index/
    {aa}/{bb}/
      idx.bin          # Trigram posting lists (compressed)
      terms.set        # Fast-reject term set (optional)
  docs/
    {aa}/{bb}/
      docs_0000000000.zlib  # Document chunks (100K docs each)
  dv/
    {field}/
      numeric.bin      # Doc_values for numeric/date fields
      terms.bin        # Term counts for KEYWORD aggregations
  stats.json           # Total docs, column types, index size
  column_map.json      # Column type overrides (if any)
  manifest.json        # Next doc ID, build metadata
```

**The index directory IS the index.** Point `flatseek search`, `flatseek serve`, or the Python client at it:

```bash
flatseek serve -d ./data
flatseek search ./data "program:raydium"
```

---

## Using Parallel Build

### When to use `-w`

| Dataset size | Workers | Notes |
|-------------|---------|-------|
| < 100K rows | `-w 1` | Parallel overhead not worth it |
| 100K – 10M rows | `-w 4` | Good speedup |
| > 10M rows | `-w 8` | Maximize CPU utilization |

### How it works

`-w N` auto-generates `build_plan.json`, splits the input by byte range, spawns N subprocess workers. Workers write independent shards (`idx_w{N}.bin`). On completion, shards are merged into the final `idx.bin`.

```bash
flatseek build ./large_file.csv -o ./data -w 8
```

### Resume support

If a parallel build is interrupted, re-running with the same `-w N` resumes from the last completed worker. `stats_w{N}.json` tracks completion per worker. Stale plans (compressed file + byte-range split failure → `estimated_rows=0` for all workers) are detected and regenerated automatically.

### Rule of thumb

More workers only helps if:
1. File is large enough (parallel overhead > benefit on small files)
2. Disk can handle concurrent reads (SSD preferred)
3. CPU has headroom (don't exceed physical cores)

---

## Estimating Before Build

```bash
flatseek build ./data.csv -o ./data --estimate
```

Samples 5,000 rows and prints:
- Estimated total rows
- Estimated build time
- Estimated output size

Use `--estimate` before committing to a large build, especially when tuning `-w` or choosing compression level.

---

## Schema Detection (Auto)

FlatSeek auto-detects column types from the first 200 rows:

| Detected type | Example values | Indexed as |
|--------------|---------------|------------|
| `INT` | `100`, `4200`, `-7` | Exact token + numeric doc_values |
| `FLOAT` | `3.14`, `-6.2088` | Exact token + numeric doc_values |
| `DATE` | `2026-04-19`, `19/04/2026` | Stored as `YYYYMMDD`, range via binary search |
| `BOOL` | `true`, `false`, `yes`, `1` | Exact token |
| `KEYWORD` | short strings, IDs, status codes | Exact token, fast aggregations |
| `TEXT` | long strings, descriptions | Trigrams, wildcard search |
| `ARRAY` | `["a","b"]` | Per-element exact token |
| `OBJECT` | `{"city":"Jakarta"}` | Dot-path expansion |

**Semantic override comes first.** Column name matching (`status`, `amount`, `created_at`) runs before value-based detection. So a column named `status` is always KEYWORD even if the first 200 rows happen to look like TEXT.

---

## Overriding Schema

### When to override

- Auto-detection misclassifies a column (numeric string with leading zeros → INT instead of KEYWORD)
- You want TEXT indexing on a column that auto-detected as KEYWORD
- You need exact semantics for an ID column

### Step 1: classify

```bash
flatseek classify ./data.csv -o ./data/column_map.json
```

Writes `column_map.json` with detected types per column.

### Step 2: edit

Edit `column_map.json` to change types:

```json
{
  "data.csv": {
    "tx_id":   "KEYWORD",
    "signer":  "KEYWORD",
    "amount":  "INT",
    "programs":"ARRAY"
  }
}
```

### Step 3: build with override

```bash
flatseek build ./data.csv -o ./data -m ./data/column_map.json
```

Or place `column_map.json` in the output directory — it auto-detects on next build.

---

## Large Dataset Tips

**Use parallel build from the start.** Don't wait until single-worker build is too slow.

```bash
flatseek build ./data.csv -o ./data -w 8 --estimate
```

**SSD matters.** Indexing does heavy random I/O on the bucket directory structure (65,536 sub-directories). Spinning disk + many workers = bottleneck.

**Split very large files.** If one CSV exceeds 50M rows, splitting into multiple files and using directory input can improve shard distribution.

**Pre-classify before parallel build.** Run `flatseek classify` first — otherwise all workers try to write `column_map.json` concurrently and race.

```bash
flatseek classify ./data.csv -o ./data/column_map.json
flatseek build ./data.csv -o ./data -w 8
```

**Use `--daemon` for max speed with ample RAM.** All index data stays in memory; lazy writer drains buffers every 10s. Finalize writes everything at the end.

```bash
flatseek build ./data.csv -o ./data --daemon
```

**Compress after build** (optional, before encrypt):

```bash
flatseek compress ./data -l 9
```

Compression saves disk space but prevents incremental appends. Only run this when the index is final.

---

## Common Errors

### File not detected

**Symptom:** `flatseek build` runs but produces empty or tiny index.

**Causes:**
- Wrong file extension (`.csv` vs `.txt`) — rename or use `--columns`
- Malformed CSV (inconsistent column counts)
- JSON not in supported format (XML, CSV with embedded JSON in cells)

**Fix:**
```bash
# Check file is readable
head -3 data.csv

# Force column names if no header
flatseek build ./data.csv -o ./data --columns id,name,status,amount
```

### Slow build

**Symptom:** Single worker taking hours for a multi-GB file.

**Fix:**
```bash
# Use parallel workers
flatseek build ./data.csv -o ./data -w 8

# Estimate first to see expected duration
flatseek build ./data.csv -o ./data --estimate
```

### Wrong query results

**Symptom:** Wildcard `*term*` returns no results on a column that should match.

**Cause:** Column auto-detected as KEYWORD (exact match only) instead of TEXT (trigram-indexed).

**Fix:**
```bash
# Classify and check the detected type
flatseek classify ./data.csv -o ./data/column_map.json

# Edit column_map.json to force TEXT
# Then rebuild
flatseek build ./data.csv -o ./data -m ./data/column_map.json
```

### Parallel build fails on compressed file

**Symptom:** `estimated_rows=0` for all workers, build stalls.

**Cause:** Byte-range splitting doesn't work on `.gz`/`.bz2` files (no seek).

**Fix:** Decompress the file first, or use a single worker:

```bash
gunzip data.csv.gz
flatseek build ./data.csv -o ./data -w 1
```

---

## Rebuilding Index

FlatSeek indexes are immutable — there is no update or append operation. To refresh an index with new or changed data, rebuild:

```bash
flatseek build ./data.csv -o ./data
```

If the output directory already exists, rebuild overwrites it. New doc IDs start from where the previous build left off (tracked in `manifest.json`).

**To replace entirely from scratch:**

```bash
rm -rf ./data
flatseek build ./data.csv -o ./data
```

Or use `flatseek delete` for fast parallel removal:

```bash
flatseek delete ./data -y
flatseek build ./data.csv -o ./data
```

---

## Best Practices

- **Classify before parallel builds** — avoids concurrent writes to `column_map.json`
- **Use `--estimate` before large builds** — catches issues before waiting hours
- **KEYWORD for filter/aggregation columns** — exact match, precomputed terms.bin
- **TEXT for searchable content** — trigram indexing enables `*substring*`
- **DATE as YYYYMMDD or ISO 8601** — enables binary-search range queries
- **Pre-compress before encrypt** — encryption preserves byte size, not compressibility
- **Avoid leading zeros in numeric columns** — `0812...` becomes KEYWORD, not INT
- **Keep arrays small** — large arrays increase posting list size per doc
- **Use `-w 8` for datasets > 1M rows** — parallel overhead is worth it