# Remote Storage (S3 & Vercel Blob)

Flatseek supports storing indexes remotely using S3-compatible storage (AWS S3, Vercel Blob, MinIO, etc.) via a pluggable storage adapter layer.

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FLATSEEK_STORAGE_BACKEND` | Storage type: `local`, `s3`, or `vercel-blob` | `local` |
| `FLATSEEK_BUCKET` | Bucket name | (none) |
| `FLATSEEK_S3_REGION` | AWS region (e.g., `us-east-1`) | (none) |
| `FLATSEEK_S3_ENDPOINT` | Custom S3 endpoint URL (for MinIO, etc.) | (none) |
| `FLATSEEK_BASE_PATH` | Prefix path within bucket | (none) |
| `BLOB_READ_WRITE_TOKEN` | Vercel Blob token (for `vercel-blob` backend) | (none) |

## Backends

### Local (Default)

No configuration needed. Index stored on local filesystem.

```bash
FLATSEEK_STORAGE_BACKEND=local
```

### S3

```bash
FLATSEEK_STORAGE_BACKEND=s3
FLATSEEK_BUCKET=my-index-bucket
FLATSEEK_S3_REGION=us-east-1
# Optional: for S3-compatible stores like MinIO
FLATSEEK_S3_ENDPOINT=https://my-minio.example.com
FLATSEEK_BASE_PATH=indexes/production
```

IAM permissions required:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-index-bucket/*",
        "arn:aws:s3:::my-index-bucket"
      ]
    }
  ]
}
```

### Vercel Blob

```bash
FLATSEEK_STORAGE_BACKEND=vercel-blob
FLATSEEK_BUCKET=my-blob-token  # From BLOB_READ_WRITE_TOKEN
FLATSEEK_BASE_PATH=my-index
```

The `BLOB_READ_WRITE_TOKEN` environment variable is automatically used for authentication.

### URL (GitHub Releases, HuggingFace Datasets, HuggingFace Buckets, Generic URLs)

Query an index hosted on GitHub Releases, HuggingFace Datasets, HuggingFace Buckets, or any generic HTTP URL:

```bash
# GitHub release
flatseek search ./data "machine learning" \
  --storage-backend url \
  --storage-url "https://github.com/user/repo/releases/download/v1.0" \
  --storage-base-path "article_1k"

# HuggingFace dataset (multi-index repo)
flatseek search ./data "machine learning" \
  --storage-backend url \
  --storage-url "https://huggingface.co/datasets/user/my-indexes" \
  --storage-base-path "article_1k"

# HuggingFace bucket (new!)
# Buckets use /resolve/{index_name} pattern (index IS the revision)
# URL: https://huggingface.co/buckets/{owner}/{repo}
flatseek search ./data "machine learning" \
  --storage-backend url \
  --storage-url "https://huggingface.co/buckets/flatseek/flatdata" \
  --storage-base-path "adsb"
```

**HuggingFace Bucket Repos** (`/buckets/` URL path):
- Buckets store multiple named indexes (e.g., `adsb`, `article`, `sosmed`, `standard`)
- Each index uses `/resolve/{index_name}` as the file revision path (not `/resolve/main`)
- Index listing uses HF web UI scraping to discover available index names
- Example: `https://huggingface.co/buckets/flatseek/flatdata` → indexes: `adsb`, `article`, `sosmed`, `standard`

**HuggingFace Dataset Repos** (`/datasets/` or `/models/` URL path):
- Multi-index: root folder contains multiple index subdirs → listdir("") returns index names
- Direct index: repo named after the index → use repo name as index, files at root level

The URL adapter is **read-only** — you can query public indexes hosted on GitHub Releases, HuggingFace, or any HTTP server that serves raw files.

### Encrypted Indexes

When querying an encrypted index, include the `x-index-password` header:

```bash
curl -H "x-index-password: your-passphrase" \
  "http://localhost:8000/my-index/_search?q=*"
```

For encrypted HuggingFace bucket indexes, authenticate first:
```bash
# Authenticate to get session-scoped decryption
curl -X POST "http://localhost:8000/my-index/_authenticate" \
  -H "Content-Type: application/json" \
  -d '{"passphrase": "your-passphrase"}'

# Then include the password in subsequent requests
curl -H "x-index-password: your-passphrase" \
  "http://localhost:8000/my-index/_search?q=*&bucket=https://huggingface.co/buckets/..."
```

**Security**: Password is never stored server-side. Every encrypted request must include the `x-index-password` header.

## Programmatic Usage

### Python API

```python
from flatseek.core.storage import create_storage_adapter, StorageConfig

# Create from environment
storage = create_storage_adapter()

# Or create explicitly
config = StorageConfig(
    backend="s3",
    bucket="my-bucket",
    region="us-east-1",
    base_path="indexes/prod"
)
storage = create_storage_adapter(config)

# Use with QueryEngine
from flatseek.core.query_engine import QueryEngine
engine = QueryEngine("s3://my-bucket/indexes/prod/myindex", storage=storage)

# Use with IndexBuilder
from flatseek.core.builder import IndexBuilder
builder = IndexBuilder("s3://my-bucket/indexes/prod/myindex", storage=storage)
```

### Remote URL Format

When using remote storage, the `data_dir` / `output_dir` parameter accepts a path relative to the bucket root:

```python
# Index stored at: s3://my-bucket/indexes/prod/myindex/
engine = QueryEngine("/indexes/prod/myindex", storage=storage)
```

## How It Works

The `StorageAdapter` abstract layer provides:

- `read_bytes(rel_path)` - Read entire file
- `open_read(rel_path)` - Stream reading
- `write_bytes(rel_path, data)` - Write entire file
- `open_write(rel_path)` - Stream writing
- `exists(rel_path)` - Check if file exists
- `listdir(rel_path)` - List directory contents
- `mkdir(rel_path)` - Create directory (recursive)
- `delete(rel_path)` - Delete file

The same interface works for local filesystem, S3, and Vercel Blob.

## Search & Aggregate Operations

All search and aggregate operations use the storage adapter:

- Posting list reads (`_read_posting`)
- Doc values columnar reads (`_load_doc_values`)
- Document chunk reads (`_load_chunk`, `_iter_chunks`)
- Term set cache reads (`_load_bucket_term_set`)

## Build/Index Operations

All build operations use the storage adapter:

- Writing posting list segments
- Writing document chunks
- Writing doc values
- Writing stats.json

## Performance Notes

### Local Storage
- Uses memory-mapped files (mmap) for hot posting lists
- OS page cache provides automatic caching

### Remote Storage (S3/Blob)
- Segment-append strategy: each checkpoint writes immutable segment files
- No read-modify-write cycles (S3-friendly)
- Final merge at `finalize()` creates single compressed file per prefix

### Caching
- QueryEngine uses LRU caches for postings, doc chunks, and doc values
- For production S3 deployments, consider adding CloudFront CDN in front