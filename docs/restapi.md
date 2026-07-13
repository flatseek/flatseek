# REST API

FlatSeek's API is Elasticsearch-compatible. Start the server:

```bash
flatseek serve ./data
# → API: http://localhost:8000
# → Dashboard: http://localhost:8000/dashboard
```

---

## Cluster

### `GET /_health`

```bash
curl http://localhost:8000/_health
```

### `GET /_cluster/health`

```bash
curl http://localhost:8000/_cluster/health
```

### `GET /_indices`

List all discovered indices:

```bash
curl http://localhost:8000/_indices
```

---

## Search

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
    "sort": [{"timestamp": "desc"}]
  }'
```

**Request body options:**

| Field | Type | Description |
|-------|------|-------------|
| `q` | string | Lucene query string |
| `from` | int | Offset for pagination (default 0) |
| `size` | int | Results per page (default 20, max 10000) |
| `sort` | string | Sort spec, e.g. `"timestamp:desc"` |
| `_index` | string | Glob pattern for multi-index search |

**Response:**

```json
{
  "_index": "my_index",
  "hits": {
    "total": 1523,
    "hits": [
      {
        "_index": "my_index",
        "_id": 42,
        "_score": 1.0,
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

### Multi-index search

```bash
curl -X POST http://localhost:8000/any_index/_search \
  -H "Content-Type: application/json" \
  -d '{
    "_index": "logs_2025-*,events_*",
    "q": "ERROR",
    "from": 0,
    "size": 20,
    "start": "20250101",
    "end": "20251231"
  }'
```

`_index` accepts fnmatch glob patterns. `start`/`end` prune indices by date segment in their names.

### Remote index via `?bucket=`

```bash
curl "http://localhost:8000/my_index/_search?q=raydium&bucket=https://huggingface.co/datasets/user/dataset"
```

Supports HuggingFace datasets, GitHub releases, S3, Vercel Blob, and raw URLs.

### `POST /{index}/_validate`

Validate a query without executing it:

```bash
curl -X POST http://localhost:8000/my_index/_validate \
  -H "Content-Type: application/json" \
  -d '{"q": "program:raydium AND amount:>foo"}'
```

### `GET /_debug/chunks/{index}`

Debug endpoint: list all chunks in an index:

```bash
curl "http://localhost:8000/_debug/chunks/my_index"
```

---

## Count

### `GET /{index}/_count`

```bash
curl "http://localhost:8000/my_index/_count?q=level:ERROR"
```

### `POST /{index}/_count`

```bash
curl -X POST http://localhost:8000/my_index/_count \
  -H "Content-Type: application/json" \
  -d '{"q": "status:pending"}'
```

**Response:**

```json
{"count": 42}
```

---

## Aggregate

### `POST /{index}/_aggregate`

```bash
curl -X POST http://localhost:8000/my_index/_aggregate \
  -H "Content-Type: application/json" \
  -d '{
    "query": "status:success",
    "aggs": {
      "by_program":  {"terms":          {"field": "program",   "size": 10}},
      "fee_stats":   {"stats":          {"field": "fee"}},
      "by_month":    {"date_histogram": {"field": "timestamp", "interval": "month"}},
      "unique":      {"cardinality":    {"field": "signer"}}
    }
  }'
```

**Response:**

```json
{
  "total": 1523,
  "aggregations": {
    "program": {
      "buckets": [
        {"key": "raydium", "doc_count": 312},
        {"key": "jupiter", "doc_count": 208}
      ]
    },
    "fee": {
      "count": 1200,
      "min": 1000,
      "max": 5000000,
      "avg": 125000.5,
      "sum": 150000000
    }
  }
}
```

### Multi-index aggregation

```bash
curl -X POST http://localhost:8000/any_index/_aggregate \
  -H "Content-Type: application/json" \
  -d '{
    "_index": "logs_2025-*",
    "query": "*",
    "aggs": {
      "by_service": {"terms": {"field": "service", "size": 20}}
    }
  }'
```

### `GET /{index}/_aggregate`

```bash
curl "http://localhost:8000/my_index/_aggregate?q=status:success&agg=by_level:terms,level,10"
```

---

## Cross-Lookup

### `POST /{index}/_cross_lookup`

Enrich source index results by looking up matching documents in a target index on a shared key field:

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

**Request body:**

| Field | Type | Description |
|-------|------|-------------|
| `target` | string | Target index name (directory under data_dir) |
| `query` | string | Lucene query for source index (default "*") |
| `on` | string | Shared field to join on (required) |
| `left_fields` | array | Fields to keep from source (default: all) |
| `right_fields` | array | Fields to keep from target (default: all) |
| `top_n` | int | Max source docs to process (default 10) |
| `page` | int | Page number (default 0) |
| `page_size` | int | Results per page (default 20) |

**Response:**

```json
{
  "total": 2,
  "page": 0,
  "page_size": 20,
  "results": [
    {
      "_left": {"email": "alice@x.com", "name": "Alice"},
      "_right": [{"tstamp": "2025-01-01", "amount": 500}],
      "_match_count": 1
    }
  ]
}
```

---

## Document Operations

### `POST /{index}/_doc` — Index a document

```bash
curl -X POST http://localhost:8000/my_index/_doc \
  -H "Content-Type: application/json" \
  -d '{
    "signature": "5xJ3kAhD...7X",
    "program": "raydium",
    "signer": "7xMg3...X",
    "amount": 1000000,
    "fee": 5000,
    "status": "success"
  }'
```

Returns the document ID assigned.

> Note: To upsert by a known ID, use `PUT /{index}/_doc/{id}` instead.

### `PUT /{index}/_doc/{id}` — Upsert by ID

Insert or replace a document by its natural key:

```bash
curl -X PUT http://localhost:8000/my_index/_doc/txn_001 \
  -H "Content-Type: application/json" \
  -d '{
    "id": "txn_001",
    "amount": 500,
    "status": "confirmed"
  }'
```

- If the document already exists: replaces it (soft-delete old, index new)
- Requires the index was built with `--id-field`
- The ID in the URL must match the `id_field` value in the document

**Response:**

```json
{"result": "created", "_id": "txn_001"}
{"result": "updated", "_id": "txn_001"}
```

### `DELETE /{index}/_doc/{id}` — Delete by ID

```bash
curl -X DELETE http://localhost:8000/my_index/_doc/txn_001
```

**Response:**

```json
{"result": "deleted"}
```

### `POST /{index}/_delete_by_query` — Delete by query

```bash
curl -X POST http://localhost:8000/my_index/_delete_by_query \
  -H "Content-Type: application/json" \
  -d '{"query": "status:spam"}'
```

**Response:**

```json
{"deleted": 12}
```

### `POST /{index}/_update_by_query` — Update by query

```bash
curl -X POST http://localhost:8000/my_index/_update_by_query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "status:pending",
    "set": {"status": "verified", "processed_at": "2025-07-13"}
  }'
```

Partial update: only the fields in `set` are merged into matching documents.

**Response:**

```json
{"updated": 7}
```

---

## Bulk Operations

### `POST /{index}/_bulk_documents` — ES-compatible bulk (NDJSON)

Accepts NDJSON (newline-delimited JSON) — one JSON object per line.

**Action types:**
- `index` — upsert (insert or replace) by `_id`
- `create` — insert only; fails if `_id` already exists
- `update` — partial update by `_id`; merges fields into existing doc
- `delete` — delete by `_id`

**Request format:** action line, then body line (for index/create/update), repeat.

```bash
curl -X POST http://localhost:8000/my_index/_bulk_documents \
  -H "Content-Type: application/x-ndjson" \
  -d '{"index": {"_id": "tx_001"}}
{"id": "tx_001", "amount": 500, "status": "pending"}
{"create": {"_id": "tx_002"}}
{"id": "tx_002", "amount": 600, "status": "confirmed"}
{"update": {"_id": "tx_001"}}
{"doc": {"status": "confirmed"}}
{"delete": {"_id": "tx_003"}}
'
```

**Response (NDJSON):**

```json
{"result": "created", "_id": "tx_001", "doc_id": 42}
{"result": "created", "_id": "tx_002", "doc_id": 43}
{"result": "updated", "_id": "tx_001"}
{"result": "deleted", "_id": "tx_003"}
```

On error:

```json
{"error": {"type": "conflict", "message": "Document already exists"}}
```

---

## Index Management

### `POST /{index}/_doc` — Create index (implicit)

Creating the first document against an index name auto-creates the index:

```bash
curl -X POST http://localhost:8000/new_index/_doc \
  -H "Content-Type: application/json" \
  -d '{"field": "value"}'
```

### `GET /{index}/_mapping`

```bash
curl http://localhost:8000/my_index/_mapping
```

### `GET /{index}/_stats`

```bash
curl http://localhost:8000/my_index/_stats
```

### `POST /{index}/_flush`

```bash
curl -X POST http://localhost:8000/my_index/_flush
```

### `POST /{index}/_compact`

Flush WAL changes and reclaim disk space:

```bash
curl -X POST http://localhost:8000/my_index/_compact
```

**Response:**

```json
{"compacted": true, "docs_merged": 12}
```

### `GET /{index}/_logs`

```bash
curl http://localhost:8000/my_index/_logs
```

### `POST /{index}/_encrypt`

```bash
curl -X POST http://localhost:8000/my_index/_encrypt \
  -H "Content-Type: application/json" \
  -H "x-index-password: mypass"
```

### `POST /{index}/_decrypt`

```bash
curl -X POST http://localhost:8000/my_index/_decrypt \
  -H "Content-Type: application/json" \
  -H "x-index-password: mypass"
```

### `POST /{index}/_authenticate`

```bash
curl -X POST http://localhost:8000/my_index/_authenticate \
  -H "Content-Type: application/json" \
  -d '{"passphrase": "mypass"}'
```

### `GET /{index}/_is_encrypted`

```bash
curl http://localhost:8000/my_index/_is_encrypted
```

### `POST /{index}/_logout`

```bash
curl -X POST http://localhost:8000/my_index/_logout
```

---

## Index Patterns

### `GET /_index_pattern/{pattern}`

Expand a glob pattern to matching index names:

```bash
curl "http://localhost:8000/_index_pattern/logs_2025-*"
# → ["logs_2025-01", "logs_2025-02", "logs_2025-03"]

curl "http://localhost:8000/_index_pattern/ads?"
# → ["adsb", "adss"]
```

---

## Multi-Search

### `POST /{index}/_msearch`

Execute multiple queries in one request:

```bash
curl -X POST http://localhost:8000/any_index/_msearch \
  -H "Content-Type: application/json" \
  -d '{"index": "logs_*,events_*"}
{"q": "ERROR", "size": 10}
{"q": "WARN", "size": 10}
'
```

---

## Upload (Remote)

### `GET /{index}/_preview`

Preview a remote URL before indexing:

```bash
curl "http://localhost:8000/my_index/_preview?url=https://example.com/data.csv"
```

### `POST /{index}/_upload`

Upload and index from a remote URL:

```bash
curl -X POST http://localhost:8000/my_index/_upload \
  -H "Content-Type: application/json" \
  -d '{"url": "https://example.com/data.csv"}'
```
