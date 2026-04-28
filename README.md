<div align="center">

<img src="logo.svg" alt="Flatseek" width="64" height="64">

# Flatseek

**Full text search over the flat files.** An Elasticsearch alternatives. Search billions of rows from CSV / JSON / XLSX without 24/7 machine.

[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)
[![Tests](https://github.com/flatseek/flatseek/workflows/Test/badge.svg)](https://github.com/flatseek/flatseek/actions)
[![PyPI version](https://img.shields.io/pypi/v/flatseek.svg)](https://pypi.org/project/flatseek/)

**Demo:** [flatlens.demo.flatseek.io](https://flatlens.demo.flatseek.io)
&nbsp;&middot;&nbsp;
**Docs:** [flatseek.io/docs](https://flatseek.io/docs)
</div>

---

## Why Flatseek?

Most teams don't actually need a 24/7 search cluster. They have CSVs of customer data, log archives, or scraped catalogs that need occasional querying and full text searching — and roughly nothing the rest of the time.

| Use case | Example query |
|---|---|
| Blockchain / DeFi | `program:raydium AND signer:*7xMg* AND amount:>1000000` |
| DevOps / SRE | `level:ERROR AND service:api-gateway AND region:us-east1` |
| Social / CRM | `platform:twitter AND lang:id AND sentiment:negative AND retweets:>100` |
| Aviation / ADS-B | `callsign:GARUDA* AND origin:WIII AND altitude:>30000` |
| AdTech / DSP | `campaign:*promo* AND country:ID AND bid:>50 AND status:active` |

**No JVM. No heap tuning. No cluster setup.** Disk-first, single Python process.

---


## Quick Start

```bash
# 1. Install (CLI + Flatlens dashboard)
curl -fsSL flatseek.io/install.sh | sh

# 2. Build an index from any CSV
flatseek build ./data/solana_txs.csv -o ./data

# 3. Serve API + dashboard
flatseek serve -d ./data
# → API:        http://localhost:8000
# → Dashboard:  http://localhost:8000/dashboard

# 4. Query
flatseek search ./data/solana_txs "program:raydium AND amount:>1000000"
```

That's it. No config files, no cluster.

---

## Install

### One-line script — recommended

```bash
curl -fsSL flatseek.io/install.sh | sh
```

Installs both the `flatseek` CLI and the Flatlens dashboard to `~/.local/share/flatlens`.

### From PyPI (CLI + Python client only)

```bash
pip install flatseek
```

The dashboard must be installed separately if you want it:

```bash
git clone https://github.com/flatseek/flatlens.git ~/.local/share/flatlens
```

### From source

```bash
git clone https://github.com/flatseek/flatseek.git
cd flatseek
pip install -e .
```

### Verify

```bash
flatseek --help
```

**Requirements:** Python ≥ 3.10. macOS, Linux, WSL.

---

## Step 1 — Index your data

Flatseek indexes any of these formats (up to 500 MB per file via dashboard): **CSV, JSON, JSONL, XLS, XLSX**.

### Option A — Dashboard (newbie-friendly)

```bash
flatseek serve -d ./data
```

Open `http://localhost:8000/dashboard`, click **+ Upload**, and walk through the four-step wizard:

1. **Drop** — drag a file or paste a URL.
2. **Preview** — Flatseek auto-detects column types and shows the first 5 rows.
3. **Configure** — name the index, optionally encrypt it, pick a batch size.
4. **Ingest** — streamed progress with elapsed time and ETA.

### Option B — CLI (advanced)

```bash
# Single-worker build (small files)
flatseek build ./data/solana_txs.csv -o ./data

# Parallel build with 8 workers (auto-splits by byte range)
flatseek build ./data/solana_txs.csv -o ./data -w 8

# Inspect the build plan without running it
flatseek plan ./data/solana_txs.csv -o ./data

# Detect column types only (writes column_map.json, no index)
flatseek classify ./data/solana_txs.csv

# Pre-flight estimate — sample 5K rows for ETA + storage projection
flatseek build ./data/large.csv -o ./data --estimate
```

The result lands at `./data/solana_txs/` — that subdirectory **is** the index. Multiple indices live as siblings under `./data/`.

### Column types

Auto-detected at index time. Override via `column_map.json` or the dashboard's mapping step.

| Type | Indexed as | Query style | Use for |
|---|---|---|---|
| `TEXT` | Trigrams + tokenization | Wildcard, contains | Free text, descriptions |
| `KEYWORD` | Exact value | Equals, terms aggregation | Tags, status, IDs |
| `DATE` | ISO date (YYYYMMDD) | Range, date_histogram | Timestamps |
| `FLOAT` / `INT` | Numeric value | Range, stats aggregation | Price, lat/lng, counts |
| `BOOL` | Boolean | Equals | Flags |
| `ARRAY` | JSON array, element-wise | Contains | tags, categories |
| `OBJECT` | Dot-path expansion | `address.city:Jakarta` | Nested records |

### Sample datasets — with arrays and nested objects

Two reference schemas that exercise every column type. CSVs matching these
shapes work end-to-end with the queries in the rest of this README.

#### Blockchain — `programs` is `ARRAY`, `swap` is `OBJECT`

```csv
signature, slot,      timestamp,            fee,  status,  signer,    programs,                       swap
5xJ3k...,  250000000, 2026-01-01T00:00:00Z, 5000, success, 7xMg3...,  ["raydium","jupiter"],          {"in":"USDC","out":"SOL","amount":2500000}
3Ab2k...,  250000001, 2026-01-01T00:01:00Z, 5000, success, 9kL2n...,  ["raydium"],                    {"in":"USDT","out":"BONK","amount":1500000}
1CdefG..., 250000002, 2026-01-01T00:02:00Z, 5000, failed,  9kL2n...,  ["jupiter","pump.fun","meteora"],   {"in":"SOL","out":"USDC","amount":3200000}
```

Queries that exploit the structure:

```text
programs:jupiter                                    # array element match
programs:raydium AND swap.amount:>1000000           # array + nested numeric
swap.in:USDC OR swap.out:USDC                       # nested OR
status:success AND programs:pump.fun                    # element-wise AND
```

#### Locations — `tags` is `ARRAY`, `address` is `OBJECT`

```csv
id, name,    country, lat,     lng,      tags,                                 address
1,  Depok,   ID,      -6.4413, 106.8183, ["urban","metro","west-java"],        {"province":"West Java","district":"Beji","postal":"16412"}
2,  Bandung, ID,      -6.9175, 107.6191, ["highland","tourism","west-java"],   {"province":"West Java","district":"Coblong","postal":"40132"}
3,  Bali,    ID,      -8.4095, 115.1889, ["island","tourism","beach"],         {"province":"Bali","district":"Denpasar","postal":"80111"}
```

Queries:

```text
tags:tourism                                        # any doc tagged "tourism"
tags:metro AND address.province:"West Java"        # array + nested string
address.postal:[10000 TO 50000]                    # nested numeric range
* AND NOT tags:beach                               # exclusions on array
```

---

## Step 2 — Query

Same query language across CLI, REST API, Python client, and dashboard.

### CLI

```bash
flatseek search ./data/solana_txs "program:raydium AND signer:*7xMg* AND amount:>1000000"
flatseek search ./data/solana_txs "fee:[4000 TO 6000]" -n 50
flatseek search ./data/solana_txs "*" --and status:success --and program:jupiter
```

### REST API

```bash
flatseek api -d ./data        # API only, no dashboard
# → http://localhost:8000

# GET — query string
curl "http://localhost:8000/solana_txs/_search?q=program:raydium AND amount:>1000000&size=10"

# POST — JSON body
curl -X POST http://localhost:8000/solana_txs/_search \
  -H "Content-Type: application/json" \
  -d '{"query": "program:raydium AND amount:>1000000", "size": 20, "from": 0}'
```

Interactive docs: [`/docs`](http://localhost:8000/docs) (Swagger UI), [`/redoc`](http://localhost:8000/redoc) (ReDoc).

### Python — API mode

```python
from flatseek import Flatseek

client = Flatseek("http://localhost:8000")

result = client.search(
    index="solana_txs",
    q="program:raydium AND signer:*7xMg* AND amount:>1000000",
    size=20,
)
print(f"Found {result.total} matches")
for doc in result.docs:
    print(doc["signature"], doc["status"], doc["fee"])
```

### Python — direct mode (no server)

```python
from flatseek import Flatseek

# Open an index directory directly — same API as remote mode
qe = Flatseek("./data/solana_txs")

result = qe.search(q="program:raydium AND amount:>1000000", size=10)
print(result.total, "matches")
```

---

## Step 3 — Aggregate

Run aggregations against the same on-disk index used for search. Results don't load documents into RAM.

### REST API

```bash
# Top programs by transaction count + fee stats + monthly histogram
curl -X POST http://localhost:8000/solana_txs/_aggregate \
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

### Python

```python
result = client.aggregate(
    index="solana_txs",
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

### Aggregation types

| Type | Output | Notes |
|---|---|---|
| `terms` | `buckets[{key, doc_count}]` | Works on `KEYWORD` and `ARRAY` fields |
| `stats` | `{count, min, max, sum, avg}` | Single pass over a numeric field |
| `avg` / `min` / `max` / `sum` | `{value}` | Faster than `stats` when you need just one |
| `cardinality` | `{value}` | HyperLogLog; ~1% relative error |
| `date_histogram` | `buckets[{key, doc_count}]` | Intervals: `hour`, `day`, `week`, `month` |

---

## Query syntax reference

| Pattern | Example | Description |
|---|---|---|
| `field:value` | `city:Jakarta` | Exact on `KEYWORD`, free-text on `TEXT` |
| `field:*term*` | `name:*john*` | Wildcard prefix / suffix / substring (trigram-backed) |
| `field:[A TO B]` | `amount:[1000000 TO 5000000]` | Inclusive range on numeric or date fields |
| `field:>N`, `field:>=N` | `score:>=88`, `level:<=10` | Open-ended numeric comparators |
| `field:true` / `false` | `enabled:true` | Booleans (also accepts `yes/no`, `1/0`) |
| `tags:value` | `tags:graphql` | Array fields — matches if **any** element equals |
| `obj.path:value` | `address.city:Jakarta` | Auto-expanded object dot-path (any depth) |
| `obj.arr[i]:val` | `info.tags[0]:alpha` | Index into a nested array |
| `AND` / `OR` / `NOT` | `level:ERROR AND NOT region:eu-west1` | Boolean combinators with parens |
| `*` | `* AND NOT enabled:true` | Match-all, useful for negation |
| `\<char\>` | `john\+doe` | Escape: ``+ - && \|\| ! ( ) { } [ ] ^ " ~ * ? : \ /`` |

Numeric and date queries walk a sorted block index — no document scan.
Wildcard queries route through trigram postings — same shape as a literal lookup.

---

## CLI reference

```bash
flatseek <command> --help    # detailed options
```

| Command | Description |
|---|---|
| `flatseek build <file\|dir> [-o out] [-w N]` | Build trigram index. `-w` for parallel workers, `--daemon` for RAM-mode builds. |
| `flatseek classify <file>` | Detect column semantic types → `column_map.json`. |
| `flatseek plan <file> [-o out] [-n N]` | Generate a parallel build plan without executing. |
| `flatseek serve [-d data] [-p 8000]` | Start API + Flatlens dashboard. |
| `flatseek api [-d data] [-p 8000]` | Start API only (no dashboard). |
| `flatseek search <dir> <query>` | CLI search. `-c col` to scope, `-n` page size, `-p` page. |
| `flatseek stats <dir>` | Doc count, index size, columns, types. |
| `flatseek dedup <dir> [--fields col1,col2]` | Cross-shard dedup after parallel build. |
| `flatseek compress <dir> [-l 1-9]` | zlib-compress index files in place. |
| `flatseek encrypt <dir> [--passphrase P]` | ChaCha20-Poly1305 encryption at rest. |
| `flatseek decrypt <dir> [--passphrase P]` | Reverse of encrypt. |
| `flatseek delete <dir> [--yes]` | Fast parallel-unlink delete. |
| `flatseek join <dir> "<qA>" "<qB>" --on field` | Cross-dataset join on a shared field. |
| `flatseek chat <dir> [--model NAME]` | Natural-language query via Ollama. |

---

## REST API endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/_indices` | List all indices |
| `GET` | `/_cluster/health` | Health check |
| `GET` | `/{index}/_search?q=&size=&from=` | Search (query string) |
| `POST` | `/{index}/_search` | Search (JSON body) |
| `GET` | `/{index}/_count?q=` | Match count without docs |
| `POST` | `/{index}/_aggregate` | Run aggregations |
| `POST` | `/{index}/_bulk` | Bulk index documents |
| `GET` | `/{index}/_stats` | Doc count, index size |
| `GET` | `/{index}/_mapping` | Column types |
| `POST` | `/{index}/_encrypt` | Encrypt at rest |
| `POST` | `/{index}/_decrypt` | Decrypt |
| `DELETE` | `/{index}` | Delete index |

**Encrypted indexes:** pass the passphrase via header `X-Index-Password: <pass>`.

---

## Architecture

Flatseek stores a **trigram inverted index** on disk using memory-mapped files.

```
data/
  solana_txs/
    index/           # Trigram postings (mmap'd, block-compressed)
    docs/            # Column-oriented document store
    stats.json       # Doc count, index size
    mapping.json     # Type per column
```

**Why trigram?** To match `*john*`, Flatseek extracts trigrams (`joh`, `ohn`) from the query, then intersects their posting lists. Postings are sorted, compressed, and skip-pointer enabled — wildcard queries cost the same shape as a literal lookup, and resident memory stays low regardless of index size.

---

## Deployment

### Docker

```bash
docker build -t flatseek .
docker run -v /path/to/data:/data -p 8000:8000 flatseek serve -d /data
```

### Vercel (dashboard front-end only)

```bash
cd flatlens && vercel --prod
```

The Flatseek API still runs on your own host; Vercel hosts the static dashboard pointed at it.

---

## Performance Benchmarks

Full benchmark suite: [`flatbench/`](https://github.com/flatseek/flatbench) — compares flatseek vs Elasticsearch on 100K-row article schema with verified correctness.

### Highlights (100K rows, article schema)

| Operation | flatseek | Elasticsearch | Winner |
|-----------|----------|---------------|--------|
| **Build index** | 155,965 ms / 641 rows/s | 17,100 ms / 5,848 rows/s | ES (9x faster) |
| **Search p50** | 2.31 ms | 7.60 ms | flatseek (3x faster) |
| **Range query** | 42.9 ms | 45.3 ms | flatseek |
| **Aggregation** | 300–700 ms | 5–50 ms | ES |

See [`flatbench/README.md`](https://github.com/flatseek/flatbench/blob/main/README.md) for full results including wildcard search, per-query breakdown, and correctness verification.

---

## Contributing

Issues and PRs welcome at [github.com/flatseek/flatseek](https://github.com/flatseek/flatseek).

Run the test suites:

```bash
# Search accuracy — ~110 cases covering wildcards, ranges, booleans, arrays, nested objects, aggs
pytest src/flatseek/test/test_search.py -v

# API smoke tests
pytest src/flatseek/test/test_api.py

# CLI integration tests
pytest src/flatseek/test/test_cli.py

# Legacy interactive query script
python test.py
```

---

## TODO

- [ ] **sort** — result ranking / scoring
- [ ] **geo near search** — distance-based queries
- [ ] **type: geo shape** — polygon search for geographic regions
- [ ] **opsi tmpfs** — RAM-backed index mode for in-memory search
- [ ] **paralel query** — concurrent query execution
- [ ] **vector search** — dense/sparse embedding similarity
- [ ] **grafana connector** — metrics export to Grafana

- [ ] **s3 upload / search** — cloud object storage integration
- [ ] **report, predefined dashboard** — custom query + metric dashboards
- [ ] **shareable link** — public dashboard sharing
- [ ] **auth, user leveling** — per-user index/report permissions

---

## License

Apache License 2.0. See [LICENSE](LICENSE).
