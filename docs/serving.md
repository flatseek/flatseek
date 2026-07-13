# Serve & Dashboard

Start the Flatseek API server and Flatlens dashboard.

---

## Start server

```bash
# Current directory as data root
flatseek serve

# Explicit data directory
flatseek serve ./data

# Custom port
flatseek serve ./data -p 9000

# Serve FSK file directly
flatseek serve data.fsk

# Serve FSK bucket (all .fsk files in directory)
flatseek serve ./fsk_bucket
```

Dashboard opens automatically at `http://localhost:8000/dashboard`.

---

## Index modes

### Single directory index

```
flatseek serve ./data
# → serves ./data as index "data"
```

### Directory subdirs (multi-index)

```
./data/
  adsb/
  txs/
flatseek serve ./data
# → auto-discovers subdirs, serves all as separate indexes
# → list at GET /_index_pattern/*
```

### Single FSK file

```
flatseek serve data.fsk
# → serves as index "data"
```

### FSK bucket (multi-index)

```
./fsk_bucket/
  logs_2025-01.fsk
  logs_2025-02.fsk
flatseek serve ./fsk_bucket
# → auto-discovers all .fsk files
# → GET /_index_pattern/* lists them
```

### Remote URL

```bash
flatseek serve "https://huggingface.co/datasets/user/dataset/resolve/main/data.fsk"
```

---

## Storage backends

### Local (default)

```bash
flatseek serve ./data
```

### S3

```bash
flatseek serve \
  --storage-backend s3 \
  --storage-bucket my-bucket \
  --storage-region us-east-1 \
  --storage-base-path "prefix/"
```

### Vercel Blob

```bash
flatseek serve \
  --storage-backend vercel-blob \
  --storage-bucket my-blob \
  --storage-region ot1 \
  --storage-base-path "flatseek/"
```

### URL storage (GitHub releases, HuggingFace)

```bash
flatseek serve \
  --storage-backend url \
  --storage-url "https://github.com/user/repo/releases/download/v1.0/"
```

---

## Environment variables

| Variable | Effect |
|---------|--------|
| `FLATSEEK_DATA_DIR` | Default data directory |
| `FLATSEEK_STORAGE_BACKEND` | Default storage backend |
| `FLATSEEK_BUCKET` | Default S3/Vercel bucket |
| `FLATSEEK_S3_REGION` | Default AWS region |
| `FLATSEEK_PASSPHRASE` | Encryption passphrase |
| `FLATSEEK_FSK_KEYS` | JSON map: `{"index_name": "base64_key", ...}` |

---

## API endpoints

| Endpoint | Method | Description |
|---------|--------|-------------|
| `/_index_pattern/{pattern}` | GET | List indexes matching fnmatch pattern |
| `/{index}/_search` | GET, POST | Search |
| `/{index}/_aggregate` | POST | Aggregations |
| `/{index}/_stats` | GET | Index statistics |
| `/{index}/_delete_by_query` | POST | Delete by query |
| `/{index}/_update_by_query` | POST | Update by query |
| `/_health` | GET | Health check |
| `/dashboard` | GET | Flatlens UI |

---

## Encrypted indexes

```bash
# With passphrase
flatseek serve ./data --passphrase mypass

# Or via environment
FLATSEEK_PASSPHRASE=mypass flatseek serve ./data

# For multiple encrypted FSKs with different keys
FLATSEEK_FSK_KEYS='{"logs_2025-01": "base64key1", "logs_2025-02": "base64key2"}' \
  flatseek serve ./fsk_bucket
```

---

## Flatlens dashboard

Flatlens is a visual query interface. It starts automatically when you run `flatseek serve`:

```
http://localhost:8000/dashboard
```

Features:
- Visual query builder
- Live aggregation charts (terms, date_histogram)
- Browse index schema
- Query history

Use `--flatlens-dir` if Flatlens is installed elsewhere:

```bash
flatseek serve ./data --flatlens-dir /path/to/flatlens
```
