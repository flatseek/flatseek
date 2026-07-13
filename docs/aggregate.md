# Aggregation

Run aggregations against the on-disk index without loading docs into RAM.

---

## Index types

| Type | How to use |
|------|-------------|
| **Directory index** | `POST /my_index/_aggregate` |
| **FSK file** | `POST /my_index/_aggregate` (FSK = index name) |
| **Remote URL** | Via API with URL storage backend |
| **Multi-index** | Set `_index` in request body, e.g. `"_index": "logs_2025-*"` |

---

## REST API

```bash
curl -X POST http://localhost:8000/my_index/_aggregate \
  -H "Content-Type: application/json" \
  -d '{
    "query": "*",
    "aggs": {
      "by_level":    {"terms":          {"field": "level",   "size": 10}},
      "fee_stats":   {"stats":          {"field": "fee"}},
      "by_month":    {"date_histogram": {"field": "timestamp", "interval": "month"}}
    }
  }'
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

Results from all matching indexes are merged (bucket counts summed).

### Time pruning

```bash
curl -X POST http://localhost:8000/any_index/_aggregate \
  -H "Content-Type: application/json" \
  -d '{
    "_index": "logs_2025-*",
    "query": "*",
    "start": "20250101",
    "end": "20251231",
    "aggs": {
      "by_day": {"date_histogram": {"field": "timestamp", "interval": "day"}}
    }
  }'
```

`start` and `end` are in `YYYYMMDD` format. Pruning removes indexes whose names have no date segment in the range.

### Response format

```json
{
  "total": 1523,
  "aggregations": {
    "level": {
      "buckets": [
        {"key": "ERROR", "doc_count": 312},
        {"key": "INFO", "doc_count": 1043},
        {"key": "WARN", "doc_count": 168}
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

---

## Python library

```python
from flatseek.core.query_engine import QueryEngine

qe = QueryEngine("./data")

result = qe.aggregate(
    query="status:success",
    aggs={
        "by_program":     {"terms":       {"field": "program", "size": 10}},
        "fee_stats":      {"stats":       {"field": "fee"}},
        "unique_signers": {"cardinality": {"field": "signer"}},
        "by_month":      {"date_histogram": {"field": "timestamp", "interval": "month"}},
    },
)
print(result.aggs["by_program"]["buckets"])
print(result.aggs["fee_stats"]["avg"])
```

### With multi-index

```python
# Directory-based multi-index (subdirs with index/)
# Flatseek auto-discovers subdirs when QueryEngine points to root
qe = QueryEngine("./data")  # ./data has subdirs adsb/, txs/
result = qe.aggregate(
    query="_index:adsb* AND status:ERROR",
    aggs={"by_level": {"terms": {"field": "level", "size": 10}}}
)
```

---

## Aggregation types

| Type | Output | Notes |
|------|--------|-------|
| `terms` | `buckets[{key, doc_count}]` | Frequency count per keyword value |
| `stats` | `{count, min, max, sum, avg}` | Single-pass numeric stats |
| `avg`, `min`, `max`, `sum` | `{value}` | Single metric, faster than stats |
| `cardinality` | `{value}` | HyperLogLog; ~1% relative error |
| `date_histogram` | `buckets[{key, doc_count}]` | Time buckets: hour, day, week, month, year |

### date_histogram intervals

- `hour`, `day`, `week`, `month`, `year`
- Calendar-aware: `"interval": "month"` groups by calendar month

### Multi-field aggregations

```bash
curl -X POST http://localhost:8000/my_index/_aggregate \
  -d '{
    "aggs": {
      "by_level":  {"terms": {"field": "level",   "size": 10}},
      "by_service": {"terms": {"field": "service", "size": 20}}
    }
  }'
```

Each aggregation key is the field name. Multiple aggregations are computed independently in one pass over the index.
