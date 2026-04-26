<div align="center">

![Logo](logo.svg)

# Flatseek

**The Elasticsearch alternative for flat files.** Search blockchain transactions, logs, CSVs — without provisioning a cluster.

[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)
[![Tests](https://github.com/flatseek/flatseek/workflows/Test/badge.svg)](https://github.com/flatseek/flatseek/actions)
[![PyPI version](https://img.shields.io/pypi/v/flatseek.svg)](https://pypi.org/project/flatseek/)

**Demo:** [https://flatlens.demo.flatseek.io](https://flatlens.demo.flatseek.io)
&nbsp;&middot;&nbsp;
**Docs:** [https://flatseek.io](https://flatseek.io)
&nbsp;&middot;&nbsp;
**Author:** judotens@flatseek.io

</div>

---

## Why Flatseek?

| Use case | Example query |
|---|---|
| Blockchain / DeFi | `program:raydium AND signer:*7xMg AND slot:>150000000` |
| DevOps / SRE | `service:api-gateway AND level:ERROR AND timestamp:[NOW-1h TO NOW]` |
| Social media / CRM | `platform:twitter AND lang:id AND sentiment:negative AND retweets:>100` |
| Aviation / ADS-B | `callsign:*GARUDA* AND origin:WIII AND altitude:>30000` |
| AdTech / DSP | `campaign:*promo* AND country:ID AND bid:>50 AND status:active` |

**No JVM. No heap tuning. No cluster setup.** Just `pip install flatseek`.

---

## Features

- **Trigram index on disk** — Memory-mapped I/O. Resident memory stays low regardless of index size.
- **Sub-second queries** — Trigram postings skip lists narrow the search space fast, even on spinning disks.
- **Lucene-style query syntax** — Wildcards (`name:*john*`), AND/OR/NOT, phrase match, field filters, ranges.
- **On-disk aggregations** — Terms, stats, range, cardinality. No document loaded into heap.
- **Parallel multi-worker builds** — Auto-planning, resume on interrupt, ETA display, `--workers N`.
- **ChaCha20-Poly1305 encryption** — Passphrase-protected indices with PBKDF2 key derivation.
- **REST API** — FastAPI backend with auto-generated docs at `/docs` and `/redoc`.
- **Flatlens dashboard** — Upload CSV/JSON/JSONL/XLS/XLSX up to 500 MB directly from browser.
- **Dual-mode Python client** — API mode (HTTP) or direct mode (local files, no server needed).

---

## Quick Start

```bash
pip install flatseek
```

### Build an index

```bash
flatseek build ./data/solana_txs.csv -o ./data
```

Supported formats: **CSV, JSON, JSONL, XLS, XLSX** (up to 500 MB per file).

### Serve API + dashboard

```bash
flatseek serve -d ./data
# API:        http://localhost:8000
# Dashboard:  http://localhost:8000/dashboard
```

### Search from CLI

```bash
flatseek search ./data "program:raydium AND signer:*7xMg AND amount:>1000000"
```

### Search from Python

```python
from flatseek import Flatseek

client = Flatseek("http://localhost:8000")
result = client.search(
    index="solana_txs",
    q="program:raydium AND signer:*7xMg AND amount:>1000000",
    size=20
)
print(result.total, "matching transactions")
```

---

## Installation

### Via install.sh (recommended — includes Flatlens dashboard)

```bash
curl -fsSL flatseek.io/install.sh | sh
```

### From PyPI

```bash
pip install flatseek
```

### From source

```bash
git clone https://github.com/flatseek/flatseek.git
cd flatseek
pip install -e .

# Clone Flatlens for the dashboard (required for flatseek serve)
git clone https://github.com/flatseek/flatlens.git
```

**Requirements:** Python >= 3.10

---

## Running

```bash
# Serve API + dashboard
flatseek serve -d ./data
# API:        http://localhost:8000
# Dashboard:  http://localhost:8000/dashboard

# Or API only (no dashboard)
flatseek api -d ./data
```

---

## CLI Reference

| Command | Description |
|---|---|
| `flatseek build <file> [-o output]` | Build index from CSV/JSON/JSONL/XLS/XLSX |
| `flatseek serve [-d data] [-p port]` | Start API server + Flatlens dashboard |
| `flatseek api [-d data] [-p port]` | Start API server only (no dashboard) |
| `flatseek search <path> <query>` | Search from CLI |
| `flatseek stats <path>` | Show index statistics |
| `flatseek classify <path>` | Detect column semantic types |
| `flatseek plan <path> [-n workers]` | Generate parallel build plan |
| `flatseek encrypt <path> [--passphrase P]` | Encrypt index with ChaCha20-Poly1305 |
| `flatseek decrypt <path> [--passphrase P]` | Decrypt encrypted index |
| `flatseek delete <path> [--yes]` | Delete index directory |

Use `flatseek <command> --help` for detailed options.

---

## API Reference

**Base URL:** `http://localhost:8000`

### Core endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/_cluster/health` | Cluster health |
| `GET` | `/_indices` | List all indices |
| `GET` | `/{index}/_stats` | Index statistics |
| `GET` | `/{index}/_mapping` | Column type mappings |
| `POST` | `/{index}/_search` | Search (`{query, size, from}`) |
| `GET` | `/{index}/_search?q=&size=&from=` | Search via query params |
| `POST` | `/{index}/_aggregate` | Run aggregations |
| `DELETE` | `/{index}` | Delete an index |

### Interactive docs

- **Swagger UI:** `GET /docs`
- **ReDoc:** `GET /redoc`

---

## Query Syntax

```
# Wildcard (contains)
campaign_name:*promo*
callsign:GARUDA*

# Field filter
level:ERROR
status:active

# Range
timestamp:[20260101 TO 20261231]
amount:>1000000

# Boolean
level:ERROR AND service:api-gateway

# Bare term (searches all columns)
jakarta garuda error
```

---

## Architecture

Flatseek stores a **trigram inverted index** on disk using memory-mapped files.

```
data/
  solana_txs/
    index/           # Trigram postings (mmap-ed)
    docs/            # Column-oriented document store
    stats.json       # Doc count, index size
    mapping.json     # Semantic type per column
    manifest.json    # File manifest
```

**Why trigram?** To match `*john*`, Flatseek extracts trigrams (`joh`, `ohn`) from the query, then intersects their posting lists. Posting lists are sorted, compressed, and support skip-pointers.

---

## Deployment

### Vercel (recommended)

```bash
cd flatseek && vercel --prod
```

### Docker

```bash
docker build -t flatseek .
docker run -v /path/to/data:/data -p 8000:8000 flatseek serve -d /data
```

---

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

---

## License

Apache License 2.0. See [LICENSE](LICENSE).
