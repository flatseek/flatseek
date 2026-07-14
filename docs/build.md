# Build

Index CSV, JSON, JSONL, or XLSX files into a disk-based search index.

---

## Index types

| Type | How to use |
|------|-------------|
| **Directory index** | `flatseek build ./data.csv -o ./data` |
| **FSK file** | `flatseek build ./data.csv -o ./data.fsk --single-file` |
| **Remote** | Not supported — build locally, upload `.fsk` |

---

## CLI

### Basic build

```bash
# CSV
flatseek build ./data.csv -o ./data

# Directory of files (recursive)
flatseek build ./csv_folder/ -o ./data

# JSON or JSONL
flatseek build ./data.json -o ./data

# XLSX
flatseek build ./data.xlsx -o ./data
```

### Output as single .fsk file

```bash
flatseek build ./data.csv --single-file -o ./data.fsk
```

Creates a portable `.fsk` archive — one file containing the complete index.

### Build from Parquet

```bash
flatseek build ./parquet_bucket/ -o myindex.fsk
```

Parquet files are auto-detected by the scanner. Uses PyArrow for schema detection and chunked reading — streaming, OOM-safe.

### Parallel build

```bash
flatseek build ./data.csv -o ./data -w 8
```

Uses 8 parallel workers. Faster for large files. Default is 1 (sequential).

### Column types

#### Manual override

```bash
flatseek build ./data.csv -o ./data --map ./column_map.json
```

Or use `--columns` to specify column names for headerless files:

```bash
flatseek build ./data.csv -o ./data --columns "id,name,amount,timestamp"
```

#### Type detection

FlatSeek auto-detects column types from sample rows:

| Pattern | Type |
|---------|------|
| `id`, `*_id`, `user_id` | KEYWORD |
| `amount`, `price`, `fee`, `balance` | INT or FLOAT |
| `timestamp`, `created_at`, `date` | TIMESTAMP (Unix) |
| `lat`, `lon`, `latitude`, `longitude` | FLOAT |
| `status`, `type`, `category`, `level` | KEYWORD |
| Other | Inferred from values |

### Delimiter

```bash
# TSV
flatseek build ./data.tsv -o ./data -s $'\t'

# Pipe-delimited
flatseek build ./data.psv -o ./data -s '|'
```

### Dataset label

```bash
flatseek build ./data.csv -o ./data --dataset "transactions"
```

Sets the `_dataset` field on every document.

### Deduplication

```bash
flatseek build ./data.csv -o ./data --dedup
```

Skips duplicate rows (hashes all non-meta fields).

### Estimate before building

```bash
flatseek build ./data.csv -o ./data --estimate
```

Samples 5,000 rows first to report speed, ETA, and storage size.

---

## Python library

```python
from flatseek.core.builder import build

build(
    input_path="./data.csv",
    output_path="./data",
    columns=None,          # list of column names for headerless files
    column_map=None,       # path to column_map.json
    dataset=None,          # dataset label
    sep=",",               # delimiter
    workers=1,             # parallel workers
    dedup=False,           # skip duplicates
    estimate=False,        # sample before full build
)
```

```python
# With column map
build("./data.csv", "./data", column_map="./column_map.json")

# Parallel
build("./data.csv", "./data", workers=8)

# Single-file FSK output
from flatseek.flatseek_file import FlatseekPacker
build("./data.csv", "./tmp_data")
FlatseekPacker("./tmp_data", "./data.fsk").pack()
```

---

## Build plan (distributed / parallel)

For very large datasets, generate a build plan and run multiple workers:

```bash
# Generate plan
flatseek build ./data.csv -o ./data --plan ./build_plan.json

# Run workers
flatseek build ./data.csv --plan ./build_plan.json --worker-id 0
flatseek build ./data.csv --plan ./build_plan.json --worker-id 1
```

Each worker processes a shard of the input in parallel. Results are merged automatically.

---

## Output

```
./data/
  manifest.json      # metadata
  column_map.json    # column type overrides
  index/             # posting lists (trigrams)
  docs/              # document chunks
  dv/                # doc_values (for aggregations)
  wal/               # WAL (append log for incremental updates)
```

---

## Rebuilding

FlatSeek indexes are **immutable** — rebuild to refresh with new data:

```bash
flatseek build ./data.csv -o ./data
# If output exists, rebuilds in-place (new doc IDs continue from where previous left off)
```

To start fresh:

```bash
rm -rf ./data
flatseek build ./data.csv -o ./data
```
