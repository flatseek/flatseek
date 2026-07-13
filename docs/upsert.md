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
