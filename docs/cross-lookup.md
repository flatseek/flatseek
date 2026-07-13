# Cross-Lookup

Enrich results from one index with matching documents from another, joined on a shared key field.

Unlike [multi-index search](multiindex.md) (which runs the **same** query across many indexes), cross-lookup runs **two different** queries on two **different** indexes and joins the results via a shared key.

---

## When to use

| Scenario | Use |
|---|---|
| Search ERROR across all log indexes | [Multi-index search](multiindex.md) |
| Get user profile + their transaction history | **Cross-lookup** |
| Enrich events with user account details | **Cross-lookup** |

---

## Python library

```python
from flatseek import Flatseek

r = Flatseek.cross_lookup(
    source_dir="./data/users",
    target_dir="./data/txs",
    query="status:active",           # query on source (users)
    on="user_id",                   # join key (must exist in both)
    left_fields=["email", "name"],  # keep only these from users (optional)
    right_fields=["tstamp", "amount"],  # keep only these from txs (optional)
    top_n=10,                       # max source docs to process (default 10)
    page=0,
    page_size=20,
)
```

**Response:**

```python
{
    "total": 2,
    "page": 0,
    "page_size": 20,
    "results": [
        {
            "_left":  {"user_id": "1", "email": "alice@x.com", "name": "Alice"},
            "_right": [
                {"user_id": "1", "tstamp": "2025-01-01", "amount": "500"},
                {"user_id": "1", "tstamp": "2025-01-02", "amount": "300"},
            ],
            "_match_count": 2,
        },
        ...
    ]
}
```

Each result row has:
- `_left`: source document (filtered to `left_fields`)
- `_right`: list of matching target documents (filtered to `right_fields`)
- `_match_count`: how many target docs matched

---

## CLI

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

## REST API

```bash
curl -X POST http://localhost:8000/users/_cross_lookup \
  -H "Content-Type: application/json" \
  -d '{
    "target": "txs",
    "query": "status:active",
    "on": "user_id",
    "left_fields": ["email", "name"],
    "right_fields": ["tstamp", "amount"],
    "top_n": 10,
    "page": 0,
    "page_size": 20
  }'
```

**Request body fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `target` | string | Yes | Target index name (directory under data_dir) |
| `query` | string | No | Lucene query for source index (default `"*"`) |
| `on` | string | Yes | Shared field to join on |
| `left_fields` | array | No | Fields to keep from source (default: all) |
| `right_fields` | array | No | Fields to keep from target (default: all) |
| `top_n` | int | No | Max source docs to process (default: 10, max: 1000) |
| `page` | int | No | Page number (default: 0) |
| `page_size` | int | No | Results per page (default: 20, max: 100) |

---

## How it works

1. **Query source index** with the given Lucene query (up to `top_n` results)
2. **Collect unique join key values** from source results
3. **Look up each key** in the target index via a query like `user_id:123`
4. **Join** — pair each source doc with its matching target docs
5. **Filter** to `left_fields` / `right_fields` if specified
6. **Paginate** the joined results

Results only include rows where at least one target document matched (no cartesian product of all sources × all targets).
