# Python Library

FlatSeek provides a dual-mode Python API via the `Flatseek` class:

```python
from flatseek import Flatseek

# Direct mode (local filesystem)
fs = Flatseek("./data")              # single index
fs = Flatseek("./data", index="txs")  # named sub-index

# API mode (HTTP)
client = Flatseek("http://localhost:8000")
```

All methods are identical in both modes — Flatseek proxies unhandled calls to the underlying `_DirectEngine` or `_HTTPClient`.

---

## Quick Reference

```python
from flatseek import Flatseek

fs = Flatseek("./data")

# Search
fs.search(q="program:raydium AND amount:>1000000", size=10)
fs.search(q="ERROR", page=1, size=20)

# Count
fs.count()           # total docs
fs.count(q="level:ERROR")  # matching docs

# Aggregate
fs.aggregate(q="*", aggs={
    "by_level": {"terms": {"field": "level", "size": 10}},
    "fee_stats": {"stats":  {"field": "fee"}},
})

# Stats & columns
fs.stats()
fs.columns()

# Write operations (requires --id-field at build time)
fs.insert({"name": "Alice", "email": "alice@example.com"})       # auto ULID _id
fs.index("tx_001", {"id": "tx_001", "amount": 500})               # upsert by id
fs.update("tx_001", {"status": "confirmed"})                       # partial update
fs.delete("tx_001")                                                # soft-delete
fs.delete_query("status:spam")                                    # delete by query
fs.bulk([                                                        # ES-compatible bulk
    {"create": {"_id": "tx_002"}},
    {"id": "tx_002", "amount": 600},
])

# Multi-index search
Flatseek.search_multi("./data_dir", "logs_*,events_*", "ERROR", size=10)

# Cross-lookup (join 2 indexes on shared key)
Flatseek.cross_lookup(
    source_dir="./data/users",
    target_dir="./data/txs",
    query="status:active",
    on="user_id",
    left_fields=["email", "name"],
    right_fields=["tstamp", "amount"],
)
```

---

## Setup

```python
from flatseek import Flatseek          # dual-mode client
from flatseek.core.builder import build  # building indexes
```

**Low-level** (direct QueryEngine access):

```python
from flatseek.core.query_engine import QueryEngine

qe = QueryEngine("./data")            # single index
qe = QueryEngine("./data")            # multi-index (subdirs auto-discovered)
qe = QueryEngine("./data.fsk")        # single .fsk archive
```

---

## Build

```python
from flatseek.core.builder import build

build("./data.csv", "./data")
```

**Options:**

```python
build(
    input_path="./data.csv",
    output_path="./data",
    columns=["id", "name", "amount"],       # column names for headerless files
    column_map="./column_map.json",         # type overrides
    dataset="transactions",                  # adds _dataset field to every doc
    sep=",",                                  # delimiter
    id_field="id",                            # enables write operations
    workers=8,                                # parallel workers
    dedup=True,                              # skip duplicate rows
)
```

**Build to single .fsk file:**

```python
build(input_path="./data.csv", output_path="./tmp_data")
from flatseek.flatseek_file import FlatseekPacker
FlatseekPacker("./tmp_data", "./data.fsk").pack()
```

---

## Search

### Basic

```python
fs = Flatseek("./data")

result = fs.search(q="program:raydium AND amount:>1000000", size=20)
print(result.hits.total)          # total matches
for doc in result.docs:           # list of _source dicts
    print(doc)
```

`result` is a `Response` wrapper with `.hits`, `.total`, `.docs` properties.

### Pagination

```python
result = fs.search(q="ERROR", page=0, size=20)   # page 0
result = fs.search(q="ERROR", page=1, size=20)   # page 1
```

### With sort

```python
result = fs.search(q="ERROR", sort="timestamp:desc", size=20)
result = fs.search(q="ERROR", sort="amount:desc,timestamp:asc", size=20)
```

### Wildcards

```python
fs.search(q="GARUDA*")       # prefix
fs.search(q="*raydium*")     # infix
fs.search(q="RAYDIUM%%")     # suffix (trigram)
```

### Boolean

```python
fs.search(q="program:raydium NOT status:failed")
fs.search(q="ERROR OR WARN")
fs.search(q="level:INFO AND service:api-gateway")
```

### Numeric range

```python
fs.search(q="amount:>1000000")
fs.search(q="amount:[1000 TO 5000]")
```

### Phrase search

```python
fs.search(q='"exact phrase"')
```

### Empty query (match all)

```python
fs.search(q="*")
```

---

## Count

```python
fs.count()                     # total docs in index
fs.count(q="level:ERROR")     # count matching docs
```

Returns a `CountResponse` wrapper with `.count` property.

---

## Aggregate

```python
result = fs.aggregate(
    q="status:success",
    aggs={
        "by_level":   {"terms":        {"field": "level",   "size": 10}},
        "fee_stats":  {"stats":        {"field": "fee"}},
        "by_month":   {"date_histogram":{"field": "timestamp", "interval": "month"}},
        "unique":     {"cardinality":  {"field": "signer"}},
    },
)

print(result.aggs["by_level"]["buckets"])
print(result.aggs["fee_stats"]["avg"])
```

Returns an `AggsResponse` wrapper with `.total` and `.aggs` properties.

---

## Write Operations

**Requires `--id-field` at build time** so Flatseek knows the canonical ID field.

### UpsertQueue / Batch Upsert

For high-throughput ingestion, use `UpsertQueue` to batch documents and flush them in the background:

```python
from flatseek.core.upsert_queue import UpsertQueue

queue = UpsertQueue(
    data_dir="./data",
    index_name="txs",
    id_field="id",
    batch_size=1000,
    flush_interval=5.0,   # seconds
)

# Queue documents (from any thread)
queue.put("tx_001", {"id": "tx_001", "amount": 500})
queue.put("tx_002", {"id": "tx_002", "amount": 600})

# Start background flusher and wait for drain
queue.start()
# ... more puts from other threads ...
queue.close()   # waits for flush to complete

# Or use the helper:
# from flatseek.core.upsert_queue import close_upsert_queue
# close_upsert_queue("./data", "txs")
```

**How it works:**
- Each thread gets its own thread-local `Flatseek` writer (avoids lock contention)
- Documents are added to a shared `queue.Queue`
- A background flusher thread drains the queue in batches of `batch_size`
- Flush triggers when the queue reaches `batch_size` or after `flush_interval` seconds of inactivity
- `close()` waits for the queue to drain before returning

---

## Build from Parquet

Parquet files are auto-detected by the scanner — no special API needed:

```python
from flatseek.core.builder import build

# All .parquet files under ./parquet_bucket/ are detected and processed
build("./parquet_bucket/", output_path="./myindex")
```

Uses PyArrow for schema detection and chunked reading (streaming, OOM-safe).

```python
fs = Flatseek("./data", index="txs")   # must be built with --id-field
```

### Insert (auto ID)

```python
r = fs.insert({"name": "Alice", "city": "Jakarta"})
# r = {"result": "created", "doc_id": 42, "_id": "01AR5MK2Q0..."}  # ULID
```

Generates a ULID (26-char globally unique, time-sortable) as `_id`.

### Index / Upsert (by natural key)

```python
r = fs.index("tx_001", {"id": "tx_001", "amount": 500, "status": "pending"})
# First time: result = "created"
# Subsequent: result = "updated" (old doc soft-deleted, new one indexed)
```

Uses optimistic locking (retries on concurrent modification).

### Update (partial merge)

```python
r = fs.update("tx_001", {"status": "confirmed", "processed_by": "admin"})
# Only merges the given fields; all other fields untouched.
# r = {"result": "updated"} or {"result": "not_found"}
```

### Delete by ID

```python
r = fs.delete("tx_001")
# r = {"result": "deleted"} or {"result": "not_found"}
```

Soft-delete (tombstoned, not removed from disk until compaction).

### Delete by query

```python
r = fs.delete_query("status:spam AND level:ERROR")
# r = {"deleted": N}
```

### Bulk operations (ES-compatible)

```python
r = fs.bulk([
    {"index":  {"_id": "tx_001"}},
    {"id": "tx_001", "amount": 500},
    {"create": {"_id": "tx_002"}},
    {"id": "tx_002", "amount": 600},
    {"delete": {"_id": "tx_003"}},
])
# r = {"indexed": 2, "deleted": 1, "errors": [...]}
```

ES NDJSON format: **action dict and body dict as separate list items**.

Operations: `index` (upsert), `create` (insert only, error on conflict), `update`, `delete`.

---

## Multi-Index Search

Search across multiple index directories with a glob pattern:

```python
from flatseek import Flatseek

r = Flatseek.search_multi(
    "./data_dir",                    # parent directory
    "logs_*,events_*",               # glob pattern (comma-separated OK)
    "ERROR",                          # query (default "*")
    size=10,
    page=0,
    start="20250101",                 # optional YYYYMMDD prune
    end="20250131",                   # optional YYYYMMDD prune
)
# Returns: {"_index": "logs_*,events_*", "hits": {"total": N, "hits": [...]}, "took": ms}
# Each hit: {"_id": ..., "_index": "logs_2025", "_score": 1.0, "_source": {...}}
```

Pattern rules:
- `fnmatch` glob: `logs_*`, `events_2025?`, `a,b,c`
- Date pruning: indices with `YYYYMMDD`-like segments compared lexicographically
- Up to 8 parallel queries
- Works with local filesystem directories only

---

## Cross-Lookup (join 2 indexes on shared key)

Enrich results from a source index by looking up matching documents in a target index, joined on a shared field value:

```python
from flatseek import Flatseek

r = Flatseek.cross_lookup(
    source_dir="./data/users",    # left index directory
    target_dir="./data/txs",      # right index directory
    query="status:active",        # query on source index
    on="user_id",                # join key (must exist in both)
    left_fields=["email", "name"],   # keep only these from source (optional)
    right_fields=["tstamp", "amount"],  # keep only these from target (optional)
    top_n=10,
    page=0,
    page_size=20,
)
# r = {
#   "total": N,
#   "results": [
#     {
#       "_left":  {"email": "alice@x.com", "name": "Alice"},
#       "_right": [{"tstamp": "2025-01-01", "amount": 500}, ...],
#       "_match_count": 2,
#     },
#     ...
#   ]
# }
```

Each row in results has:
- `_left`: source document (filtered to `left_fields`)
- `_right`: list of matching target documents (filtered to `right_fields`)
- `_match_count`: how many target docs matched

---

## Stats & Columns

```python
fs.stats()      # {"total_docs": N, "index_bytes": ..., "total_bytes": ...}
fs.columns()    # {"id": "KEYWORD", "name": "TEXT", "amount": "FLOAT", ...}
```

---

## Export

```python
import json

qe = Flatseek("./data")
for doc in qe.query("*", page=0, page_size=0)["results"]:
    print(json.dumps(doc))
```

---

## Pack (FSK)

```python
from flatseek.flatseek_file import FlatseekPacker

FlatseekPacker("./data", "./data.fsk").pack()
FlatseekPacker("./data", "./protected.fsk", passphrase="mypass").pack()
FlatseekPacker("./data", "./enclosed.fsk", passphrase="mypass", enclosed=True).pack()
```

---

## Compact

Flush WAL changes and reclaim disk space:

```python
from flatseek import Flatseek
fs = Flatseek("./data")
# ... make updates/deletes ...
fs.stats()   # WAL still pending
```

---

## API Mode

```python
client = Flatseek("http://localhost:8000")

# All methods work identically in API mode
client.search(q="program:raydium", size=10)
client.insert({"id": "tx_001", "amount": 500})
client.bulk([...])
```

Storage backends via URL:

```python
client = Flatseek("http://localhost:8000")
# All search/insert/aggregate calls use ?bucket= URL routing internally
```
