# Upsert — Update & Delete

FlatSeek supports **mutable indexes** via a Write-Ahead Log (WAL). Changes (updates, deletes) are written to `wal/` and applied lazily on the next search, or can be flushed via `compact`.

---

## Index types

| Type | How to use |
|------|-------------|
| **Directory index** | `flatseek update-doc`, `flatseek delete-doc` |
| **FSK file** | Not supported — unpack FSK first, then update |

---

## Update documents

### By query

```bash
flatseek update-doc ./data -q "status:pending" --set "status=verified"
```

Updates all documents matching `status:pending` to have `status=verified`.

### By ID

```bash
flatseek update-doc ./data --id "TXN-123" --set "status=verified,amount=1500"
```

Requires `--id-field` was set during build. Updates the document with natural key `TXN-123`.

### Partial vs full update

`--set` is a **partial update** — only the named fields are changed, others are preserved.

For a full replace, omit `--set` and use a query that matches exactly one document.

### API

```bash
curl -X POST http://localhost:8000/my_index/_update_by_query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "status:pending",
    "set": {"status": "verified"}
  }'
```

```bash
curl -X POST http://localhost:8000/my_index/_update_by_query \
  -H "Content-Type: application/json" \
  -d '{
    "id": "TXN-123",
    "set": {"status": "verified", "amount": "1500"}
  }'
```

### Python library

```python
from flatseek.core.query_engine import QueryEngine

qe = QueryEngine("./data")

# By query
qe.update_by_query(
    query="status:pending",
    fields={"status": "verified"}
)

# By ID
qe.update_by_id(
    doc_id="TXN-123",
    fields={"status": "verified", "amount": "1500"}
)
```

---

## Delete documents

### By query

```bash
flatseek delete-doc ./data -q "status:spam"
```

Deletes all documents matching `status:spam`.

### By ID

```bash
flatseek delete-doc ./data --id "TXN-123"
```

Requires `--id-field` was set during build.

### API

```bash
curl -X POST http://localhost:8000/my_index/_delete_by_query \
  -H "Content-Type: application/json" \
  -d '{"query": "status:spam"}'
```

```bash
curl -X POST http://localhost:8000/my_index/_delete_by_query \
  -H "Content-Type: application/json" \
  -d '{"id": "TXN-123"}'
```

### Python library

```python
qe = QueryEngine("./data")

# By query
qe.delete_by_query("status:spam")

# By ID
qe.delete_by_id("TXN-123")
```

---

## How it works

Changes are written to `wal/` (Write-Ahead Log):

```
./data/wal/
  000000000.jsonl   # WAL entries
```

Each entry is a JSON object:

```jsonl
{"op": "update", "id": 42, "fields": {"status": "verified"}}
{"op": "delete", "id": 99}
```

On search, the WAL is replayed lazily:
- Deleted IDs are filtered from results (tombstones)
- Updates are merged into result documents

---

## Flushing changes (compact)

WAL changes are not reflected in aggregation stats until flushed. Run `compact` to permanently apply WAL changes to the index:

```bash
flatseek compact ./data
```

This rewrites the index with WAL changes applied, then clears the WAL.

**When to compact:**
- After a batch of updates/deletes before running aggregations
- Before `export` if you deleted sensitive data
- Periodically to reclaim WAL disk space

---

## Concurrency

Updates and deletes are **append-only** to the WAL. Multiple processes can update the same index concurrently — entries are merged by ID on replay, with the latest entry taking precedence.

---

## Batch Upsert Queue

High-throughput batch upsert via `UpsertQueue` — designed for parallel crawlers that need to minimize lock contention.

### Python library

```python
from flatseek.core.upsert_queue import UpsertQueue, get_upsert_queue, close_upsert_queue

# Create and start
queue = UpsertQueue(
    data_dir="./data",
    index_name="myindex",
    id_field="email",
    batch_size=500,       # flush when this many docs accumulate (per thread)
    flush_interval=5.0,   # flush after this many seconds idle
)
queue.start()

# In worker threads — non-blocking, returns immediately
queue.put("alice@example.com", {"email": "alice@example.com", "status": "active"})
queue.put("bob@example.com",   {"email": "bob@example.com",   "status": "active"})

# Trigger a synchronous flush (optional — background thread also flushes automatically)
queue.flush()

# Check queue depth and flush progress
status = queue.status()
# {"queue_depth": N, "pending_docs": M, "flushing": bool,
#  "total_docs_flushed": X, "total_flushes": Y, "last_flush_age_s": Z}

# Stop the background flusher
queue.stop()
```

Shared singleton per `(data_dir, index_name)`:

```python
queue = get_upsert_queue("./data", "myindex", id_field="email")
```

Close a shared queue:

```python
close_upsert_queue("./data", "myindex")
```

### HTTP API

```bash
# Queue a batch of documents (non-blocking)
curl -X POST http://localhost:8000/myindex/_bulk_upsert \
  -H "Content-Type: application/json" \
  -d '{
    "id_field": "email",
    "docs": [
      {"id": "alice@example.com", "doc": {"email": "alice@example.com", "status": "active"}},
      {"id": "bob@example.com",   "doc": {"email": "bob@example.com",   "status": "active"}}
    ]
  }'

# Synchronously flush all pending docs
curl -X POST http://localhost:8000/myindex/_bulk_upsert/flush

# Check queue status
curl -X GET http://localhost:8000/myindex/_bulk_upsert/status
```

### How it works

```
Worker threads (thread-local IndexBuilder per thread)
         ↓  (batch_size docs OR flush_interval seconds)
Shared queue.Queue of doc batches
         ↓
Background flusher thread
         ↓  (atomic finalize)
New chunks written to disk → engine cache invalidated
```

- **Thread-local builder**: Each worker thread has its own `IndexBuilder` instance to avoid lock contention on `add_row()` calls.
- **Non-blocking `put()`**: Returns immediately after queuing — workers never wait on I/O.
- **Automatic flush triggers**: When `batch_size` docs accumulate in a thread's builder, or after `flush_interval` seconds with no new documents.
- **Atomic finalize**: The background flusher groups pending docs into a single `IndexBuilder.finalize()` call per flush.
- **Shared queue**: A `threading.Queue` delivers flush requests from workers to the flusher thread.
- **Engine cache invalidation**: After each flush, the index engine cache is cleared so subsequent searches pick up the new chunks.

---

## Delete index directory

To delete an entire index:

```bash
flatseek delete ./data -y
```

This is a fast parallel `rm -rf`. Cannot be undone.

---

## Dedup

Remove duplicate documents after a parallel build:

```bash
flatseek dedup ./data --fields "email,phone"
```

Uses `--fields` as the fingerprint key. Duplicates (same fingerprint) keep the last-occurring document.
