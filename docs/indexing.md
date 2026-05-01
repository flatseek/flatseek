# Indexing

## Supported formats

CSV, JSON, JSONL, XLS, XLSX. Up to 500 MB per file via dashboard.

## Build index

```bash
flatseek build ./data.csv -o ./data
```

The output lands at `./data/my_index/` — that directory **is** the index. Multiple indices live as siblings under `./data/`.

## Parallel build

```bash
flatseek build ./data.csv -o ./data -w 8
```

Auto-splits by byte range. More workers = faster build.

## Inspect build plan

```bash
flatseek plan ./data.csv -o ./data
```

Writes build plan without executing. Useful to preview how data will be split across workers.

## Column type detection

```bash
flatseek classify ./data.csv
```

Detects column types and writes `column_map.json`. Override via the dashboard or by editing the file.

## Estimate build time + storage

```bash
flatseek build ./data.csv -o ./data --estimate
```

Samples 5K rows for ETA and storage projection.

## Column types

| Type | Indexed as | Query style | Use for |
|---|---|---|---|
| `TEXT` | Trigrams + tokenization | Wildcard, contains | Free text, descriptions |
| `KEYWORD` | Exact value | Equals, terms aggregation | Tags, status, IDs |
| `DATE` | ISO date (YYYYMMDD) | Range, date_histogram | Timestamps |
| `FLOAT` / `INT` | Numeric value | Range, stats aggregation | Price, lat/lng, counts |
| `BOOL` | Boolean | Equals | Flags |
| `ARRAY` | JSON array, element-wise | Contains | tags, categories |
| `OBJECT` | Dot-path expansion | `address.city:Jakarta` | Nested records |

## Encrypt at rest

```bash
flatseek encrypt ./data/my_index --passphrase mypassword
```

Query with:

```bash
curl -H "X-Index-Password: mypassword" "http://localhost:8000/my_index/_search?q=..."
```

## Compress index

```bash
flatseek compress ./data/my_index -l 9
```

zlib compression in place. Higher level = smaller but slower.