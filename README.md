<div align="center">

<img src="logo.svg" alt="Flatseek" width="64" height="64">

# Flatseek

**Full-text search over flat files and remote datasets.** No JVM. No cluster. No operational overhead.

<p align="center">
  <img src="docs/assets/dashboard.png" alt="Flatlens Dashboard" width="100%" />
</p>

<p align="center">
  <em>
    Explore, filter, and aggregate CSV/JSON like a spreadsheet with search-grade power — run locally or query directly from Public/HuggingFace datasets.
  </em>
</p>

[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)
[![Tests](https://github.com/flatseek/flatseek/workflows/Test/badge.svg)](https://github.com/flatseek/flatseek/actions)
[![PyPI version](https://img.shields.io/pypi/v/flatseek.svg)](https://pypi.org/project/flatseek/)

**Demo:** [flatlens.demo.flatseek.io](https://flatlens.demo.flatseek.io)
&nbsp;&middot;&nbsp;
**Docs:** [flatseek.io/docs](https://flatseek.io/docs)
&nbsp;&middot;&nbsp;
**Dashboard:** [flatlens](https://github.com/flatseek/flatlens/)
&nbsp;&middot;&nbsp;
**Benchmark:** [flatbench](https://github.com/flatseek/flatbench/)
&nbsp;&middot;&nbsp;
**Hosted Datasets:** [HuggingFace](https://huggingface.co/flatseek)

</div>

---

## Why Flatseek

- **Exact-by-design engine.** Query results are deterministic with full-count accuracy on flat files.
- **No infra required.** Run locally as a single binary or deploy as a lightweight API sidecar.
- **Millisecond search performance.** Built for interactive exploration over large datasets.
- **Zero-ops distribution.** Store and query indexes directly from HuggingFace, S3, or Vercel Blob.
- **Instant dataset access.** Paste a dataset URL in Flatlens and start searching immediately — no ingestion pipeline needed.
- **Built-in analytics dashboard.** Spreadsheet-like experience with Kibana-style filtering, aggregation, and exploration — closer to Microsoft Excel for structured search workflows.
- **Developer-first embedding.** Use as a Python library or full API without running a cluster or managing nodes.
---
## Search engine for public datasets instantly

Flatseek can query indexes hosted remotely — no need to deploy or pay for expensive storage infrastructure. Build your Flatseek index locally, upload it to any free-cheap publicly accessible storage service, and instantly search and aggregate data from anywhere.

Supported providers:

- HuggingFace Datasets / Buckets
- S3-compatible storage
- Vercel Blob
- Static HTTP hosting


#### Samples

| Dataset | Rows |
|---|---|
| Local Sample | [500 searchable rows](https://flatlens.demo.flatseek.io) |
| [https://huggingface.co/buckets/flatseek/flatdata](https://huggingface.co/buckets/flatseek/flatdata) | [10K searchable rows](https://flatlens.demo.flatseek.io/?bucket=https://huggingface.co/buckets/flatseek/flatdata&index=adsb) |
| [https://huggingface.co/datasets/flatseek/sample-articles](https://huggingface.co/datasets/flatseek/sample-articles) | [100K articles](https://flatlens.demo.flatseek.io/?bucket=https://huggingface.co/datasets/flatseek/sample-articles) |
| [https://huggingface.co/datasets/flatseek/sample-adsb](https://huggingface.co/datasets/flatseek/sample-adsb) | [100K ADS-B records](https://flatlens.demo.flatseek.io/?bucket=https://huggingface.co/datasets/flatseek/sample-adsb) |
| [https://huggingface.co/datasets/flatseek/sample-encrypted](https://huggingface.co/datasets/flatseek/sample-encrypted) | [100K encrypted rows](https://flatlens.demo.flatseek.io/?bucket=https://huggingface.co/datasets/flatseek/sample-encrypted) Password: `flatseek`|


Encrypted datasets are supported with per-request passphrases.

---

## Performance

500K rows, article schema, SSD. Flatseek is 2× faster on search than Elasticsearch with exact counts where others silently miss.

| Metric | Flatseek | Elasticsearch |
|--------|----------|---------------|
| Search p50 | **7.9ms** | 16.1ms |
| Range query hits | 501,011 (exact) | 505,044 |
| Build 500K rows | 216s | 113s |

Full comparison (tantivy, typesense, whoosh, zincsearch): [docs/benchmark.md](/docs/benchmark.md) or
[bench.flatseek.io](https://bench.flatseek.io)

---

## Quick Start

```bash
# 1. Install
curl -fsSL flatseek.io/install.sh | sh

# 2. Build index
flatseek build ./data.csv -o ./data

# 3. Serve API + dashboard
flatseek serve -d ./data

# Pack index into single portable file
flatseek pack ./data -o ./data.fsk

# Serve API + dashboard from single portable index file
flatseek serve data.fsk

# → API:
# http://localhost:8000

# → Dashboard:
# http://localhost:8000/dashboard

# 4. Query
flatseek search ./data "program:raydium AND amount:>1000000"

# Query from single portable file
flatseek search data.fsk "program:raydium AND amount:>1000000"

# Serve a .fsk directly from HuggingFace — no full download needed
flatseek serve https://huggingface.co/buckets/flatseek/flatdata/resolve/wikipedia.fsk

# Export filtered docs to JSONL (streaming, OOM-safe)
flatseek export ./data --query 'status:active' -o active.jsonl

# Slice out a subset matching a query into a new index
flatseek slice ./data --query 'region:APAC' -o apac_data

```
---
## Core Capabilities

- **Full text search** — tokenized, trigram-backed wildcard (`*kube*`)
- **Range queries** — exact counts on numeric, date, keyword fields
- **Aggregations** — terms, stats, min/max, cardinality, date histogram
- **Array fields** — matches any element (`tags:graphql`)
- **Nested objects** — dot-path queries (`address.city:Jakarta`)
- **Boolean operators** — AND, OR, NOT with grouping
- **Encryption at rest** — ChaCha20-Poly1305
- **Parallel indexing** — multi-worker builds
- **Remote querying** — search HuggingFace datasets without downloading
- **REST API** — Elasticsearch-compatible endpoints
- **Single-file archive (`.fsk`)** — portable, cryptographically signed index file
- **Export to JSONL/CSV** — streaming, with query filter and resume
- **Slice** — extract a query-matched subset as a new index
- **Serve from HTTP URL** — open `.fsk` from HuggingFace/S3 without full download

See [docs/](docs/) for full details.

---

## Docs

| Guide | Description |
|-------|-------------|
| [Quick Start](docs/quickstart.md) | Install, index, query — in 5 minutes |
| [Indexing](docs/indexing.md) | Formats, column types, parallel builds, encrypt |
| [Query Language](docs/query-language.md) | Full syntax reference |
| [CLI Reference](docs/cli.md) | All CLI commands |
| [REST API](docs/api.md) | API endpoints |
| [Remote Storage](docs/storage.md) | HuggingFace, S3, Vercel Blob |
| [Schemas](docs/schemas.md) | Supported Column Types |
| [Architecture](docs/architecture.md) | Structural and behavioral map |
| [Internals](docs/internals.md) | Deep technical breakdown |

---

## Install

### Recommended — one-liner

```bash
curl -fsSL flatseek.io/install.sh | sh
```

Includes API server + Flatlens dashboard (`http://localhost:8000/dashboard`).

### PyPI

```bash
pip install flatseek
```

CLI only. For [Flatlens dashboard](https://github.com/flatseek/flatlens):

```bash
git clone https://github.com/flatseek/flatlens
```

### From source

```bash
git clone https://github.com/flatseek/flatseek.git

cd flatseek
pip install -e .
```

**Requirements:** Python ≥ 3.10, macOS / Linux / WSL.

---

## Contributing

PRs welcome. Run tests:

```bash
pytest src/flatseek/test/test_search.py -v   # accuracy tests
pytest src/flatseek/test/test_api.py         # API smoke tests
pytest src/flatseek/test/test_cli.py         # CLI integration
```

---

## License

Apache 2.0. See [LICENSE](LICENSE).