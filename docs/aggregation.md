# Aggregation

Run aggregations against the on-disk index without loading docs into RAM.

## REST API

```bash
curl -X POST http://localhost:8000/my_index/_aggregate \
  -H "Content-Type: application/json" \
  -d '{
    "query": "*",
    "aggs": {
      "by_program":  {"terms":          {"field": "program",   "size": 10}},
      "fee_stats":   {"stats":          {"field": "fee"}},
      "by_month":    {"date_histogram": {"field": "timestamp", "interval": "month"}}
    }
  }'
```

## Python

```python
result = client.aggregate(
    index="my_index",
    body={
        "query": "status:success",
        "aggs": {
            "by_program":     {"terms":       {"field": "program", "size": 10}},
            "fee_stats":      {"stats":       {"field": "fee"}},
            "unique_signers": {"cardinality": {"field": "signer"}},
        },
    },
)
print(result.aggs["by_program"]["buckets"])
```

## Aggregation types

| Type | Output | Notes |
|---|---|---|
| `terms` | `buckets[{key, doc_count}]` | Works on KEYWORD and ARRAY fields |
| `stats` | `{count, min, max, sum, avg}` | Single pass over numeric field |
| `avg` / `min` / `max` / `sum` | `{value}` | Faster than stats when you need one metric |
| `cardinality` | `{value}` | HyperLogLog; ~1% relative error |
| `date_histogram` | `buckets[{key, doc_count}]` | Intervals: hour, day, week, month |