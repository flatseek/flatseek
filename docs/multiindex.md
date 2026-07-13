# Multi-index

Flatseek supports two modes of multi-index: **directory subdirs** and **FSK buckets**.

---

## Directory subdirs

A root directory containing subdirectories, each with its own `index/` subdirectory:

```
./data/
  adsb/
    index/
      _docs/
  txs/
    index/
      _docs/
  logs/
    index/
      _docs/
```

When you point the CLI or API at `./data`, Flatseek auto-discovers all subdirectories with `index/` folders and queries all of them.

```bash
# Search all sub-indexes
flatseek search ./data "ERROR"

# Scope to one sub-index via query
flatseek search ./data "_index:adsb AND ERROR"

# Via --index flag
flatseek search ./data "ERROR" --index adsb
```

### How it works

`QueryEngine` at root directory calls `_discover_index_dirs()`, which walks the tree:

- Root has `index/` → single index mode (no fan-out)
- Root has subdirs each with `index/` → multi-index mode: creates one `QueryEngine` per subdir

Results from all sub-indexes are **merged and paginated**. Each result document gets an `_index` field:

```json
{"_id": 0, "_index": "adsb", "program": "raydium", "amount": "1500000"}
{"_id": 0, "_index": "txs", "signer": "RAYDIUM4", "amount": "750000"}
```

---

## FSK bucket

A directory containing multiple `.fsk` files:

```
./fsk_bucket/
  logs_2025-01.fsk
  logs_2025-02.fsk
  logs_2025-03.fsk
  events.fsk
```

```bash
# Search all FSKs in bucket
flatseek search ./fsk_bucket "ERROR"

# Scope via --index
flatseek search ./fsk_bucket "ERROR" --index logs_2025-*

# Specific FSK
flatseek search ./fsk_bucket "ERROR" --index logs_2025-01
```

Each `.fsk` file is opened as an independent `FlatseekFileStorageAdapter` + `QueryEngine` pair. Results are merged with `_index` set to the FSK stem name.

---

## `_index:pattern` routing

The `_index` field supports **fnmatch-style patterns**:

| Pattern | Matches |
|---------|---------|
| `logs_2025-*` | `logs_2025-01`, `logs_2025-02`, ... |
| `ads?` | `ads1`, `ads2`, `adss` |
| `logs_2025-0[1-3]` | `logs_2025-01`, `logs_2025-02`, `logs_2025-03` |
| `a,b,c` | `a` OR `b` OR `c` (comma-separated) |

```bash
flatseek search ./fsk_bucket "_index:logs_2025-0*" "ERROR"
flatseek search ./fsk_bucket "_index:ads?,txs" "ERROR"
```

### In query string vs `--index` flag

Both are equivalent:

```bash
flatseek search ./fsk_bucket "ERROR" --index "logs_2025-01,logs_2025-02"
flatseek search ./fsk_bucket "_index:logs_2025-01,logs_2025-02 AND ERROR"
```

The `--index` flag injects `_index:PATTERN` into the query string internally.

---

## Commands supporting multi-index

### `search`

```bash
# Directory subdirs
flatseek search ./data "ERROR" --index adsb

# FSK bucket
flatseek search ./fsk_bucket "ERROR" --index "logs_2025-*"
flatseek search ./fsk_bucket "_index:ads* OR _index:txs" "ERROR"
```

### `export`

```bash
flatseek export ./fsk_bucket -q "*" --index "logs_2025-*" -o out.jsonl
```

### `slice`

```bash
# Requires {index} placeholder when slicing multiple FSKs
flatseek slice ./fsk_bucket "ERROR" -o "sliced_{index}.fsk" --index "logs_2025-*"
```

---

## Library

```python
from flatseek import Flatseek

# Python library: Flatseek.search_multi()
r = Flatseek.search_multi(
    "./data_dir",                 # parent directory
    "logs_*,events_*",           # glob pattern
    "ERROR",                      # query (default "*")
    size=10,
    page=0,
    start="20250101",             # optional YYYYMMDD prune
    end="20250131",               # optional YYYYMMDD prune
)
# Returns: {"_index": "logs_*,events_*", "hits": {"total": N, "hits": [...]}, "took": ms}
# Each hit: {"_id": ..., "_index": "logs_2025", "_score": 1.0, "_source": {...}}
```

Pattern rules:
- `fnmatch` glob: `logs_*`, `events_2025?`, `a,b,c` (comma-separated)
- Date pruning: indices with `YYYYMMDD`-like segments filtered lexicographically
- Up to 8 parallel queries
- Works with local filesystem directories only (not remote storage)

## REST API

```bash
# Multi-FSK search via _index in body
curl -X POST http://localhost:8000/any/_search \
  -H "Content-Type: application/json" \
  -d '{
    "_index": "logs_2025-*",
    "q": "ERROR",
    "from": 0,
    "size": 20
  }'
```

### `expand_index_pattern` (Elasticsearch-compatible)

```bash
curl "http://localhost:8000/_index_pattern/logs_2025-*"
# → ["logs_2025-01", "logs_2025-02", "logs_2025-03"]

curl "http://localhost:8000/_index_pattern/ads?"
# → ["adsb", "adss"]
```

### Time pruning

```bash
curl -X POST http://localhost:8000/any/_search \
  -H "Content-Type: application/json" \
  -d '{
    "_index": "logs_2025-*",
    "start": "20250101",
    "end": "20251231",
    "q": "*"
  }'
```

Indexes whose names have a date segment are filtered by the date range. Indexes without dates are included if they have no date segment to prune against.

---

## Cross-Lookup (join two indexes on shared key)

Cross-lookup enriches results from one index with data from another, joined on a shared key field. Different from multi-index search (which runs the same query across many indexes) — cross-lookup runs two *different* queries on two *different* indexes and joins the results.

**Example:** Users index + Transactions index, joined on `user_id`:

```bash
flatseek cross-lookup ./data/users ./data/txs \
  "status:active" \
  --on user_id \
  --left-fields "email,name" \
  --right-fields "tstamp,amount"
```

```python
from flatseek import Flatseek

r = Flatseek.cross_lookup(
    source_dir="./data/users",
    target_dir="./data/txs",
    query="status:active",
    on="user_id",
    left_fields=["email", "name"],
    right_fields=["tstamp", "amount"],
)
```

**REST API:**

```bash
curl -X POST http://localhost:8000/users/_cross_lookup \
  -H "Content-Type: application/json" \
  -d '{
    "target": "txs",
    "query": "status:active",
    "on": "user_id",
    "left_fields": ["email", "name"],
    "right_fields": ["tstamp", "amount"]
  }'
```

| | Multi-index search | Cross-lookup |
|---|---|---|
| Indexes | Many | Two (source + target) |
| Query | Same query all indexes | Different query on each |
| Join key | None | Yes — `on=field` |
| Use case | "Search ERROR in all logs" | "Get user info + their transactions"
