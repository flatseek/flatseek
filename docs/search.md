# Search

Flatseek supports one query language across all index types and access methods.

---

## Index types

| Type | How to use |
|------|-------------|
| **Directory index** | `flatseek search ./data "query"` |
| **FSK file** | `flatseek search data.fsk "query"` |
| **Remote URL** | `flatseek search "https://example.com/data.fsk" "query"` |
| **Multi-index (directory)** | `flatseek search ./data "query"` — auto-discovers subdirs with `index/` |
| **Multi-FSK bucket** | `flatseek search ./fsk_bucket "query" --index "logs_2025-*"` |

---

## CLI

### Basic search

```bash
flatseek search ./data "program:raydium"
```

### Wildcards

```bash
flatseek search ./data "GARUDA*"           # prefix wildcard
flatseek search ./data "*raydium*"         # infix wildcard
flatseek search ./data "RAYDIUM%%"        # suffix wildcard
```

### Boolean

```bash
flatseek search ./data "program:raydium AND amount:>1000000"
flatseek search ./data "program:raydium NOT status:failed"
flatseek search ./data "ERROR OR WARN"
```

### Range

```bash
flatseek search ./data "amount:[1000000 TO *]"
flatseek search ./data "timestamp:[2025-01-01 TO 2025-12-31]"
```

### Sort

```bash
flatseek search ./data "ERROR" --sort timestamp:desc
flatseek search ./data "ERROR" --sort "amount:desc,timestamp:asc"
```

### Pagination

```bash
flatseek search ./data "ERROR" -p 0 -n 20    # page 0, 20 results
flatseek search ./data "ERROR" -p 1 -n 20    # page 1
```

### Multi-FSK scoped search

```bash
# All FSKs matching pattern
flatseek search ./fsk_bucket "ERROR" --index "logs_2025-*"

# Specific FSK
flatseek search ./fsk_bucket "ERROR" --index logs_2025-01

# Multiple patterns
flatseek search ./fsk_bucket "ERROR" --index "logs_2025-01,logs_2025-02"
```

### Remote search

```bash
flatseek search "https://huggingface.co/datasets/user/dataset/resolve/main/data.fsk" "query"
```

---

## REST API

### `GET /{index}/_search`

```bash
curl "http://localhost:8000/my_index/_search?q=program:raydium&from=0&size=20"
```

### `POST /{index}/_search`

```bash
curl -X POST http://localhost:8000/my_index/_search \
  -H "Content-Type: application/json" \
  -d '{
    "q": "program:raydium AND amount:>1000000",
    "from": 0,
    "size": 20,
    "sort": [{"timestamp": "desc"}, {"amount": "asc"}]
  }'
```

### Multi-index via `_index` in body

```bash
curl -X POST http://localhost:8000/any_index/_search \
  -H "Content-Type: application/json" \
  -d '{
    "_index": "logs_2025-*",
    "q": "ERROR",
    "from": 0,
    "size": 20
  }'
```

### Response format

```json
{
  "hits": {
    "total": 1523,
    "hits": [
      {
        "_index": "adsb",
        "_id": 42,
        "_source": {
          "program": "raydium",
          "amount": "1500000",
          "timestamp": "2025-01-15T10:30:00Z"
        }
      }
    ]
  },
  "took": 12
}
```

### Sort in API

```bash
curl -X POST http://localhost:8000/my_index/_search \
  -d '{"q": "*", "sort": [{"amount": "desc"}]}'
```

Sort field supports: `asc` (ascending) or `desc` (descending). Multiple fields can be comma-separated.

---

## Python library

```python
from flatseek.core.query_engine import QueryEngine

qe = QueryEngine("./data")

# Simple query
result = qe.query("program:raydium", page=0, page_size=20)

# With sort
result = qe.query(
    "ERROR",
    page=0,
    page_size=20,
    sort=[("timestamp", "desc"), ("amount", "asc")]
)

print(result["total"])
for doc in result["results"]:
    print(doc["_source"])
```

### QueryEngine API

```python
qe.query(query_str, page=0, page_size=20, sort=None)
qe.search(term, column=None, page=0, page_size=20, sort=None)
```

`sort` is a list of `(field, direction)` tuples, e.g. `[("amount", "desc")]`.

---

## Query syntax

See [query-language.md](query-language.md) for full syntax reference.
