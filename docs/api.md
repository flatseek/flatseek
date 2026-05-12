# REST API

Start the API:

```bash
flatseek api -d ./data        # no dashboard
flatseek serve -d ./data     # with dashboard at /dashboard
```

## Endpoints

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

## Examples

Search:

```bash
curl "http://localhost:8000/my_index/_search?q=program:raydium AND amount:>1000000&size=10"
```

Search with JSON body:

```bash
curl -X POST http://localhost:8000/my_index/_search \
  -H "Content-Type: application/json" \
  -d '{"query": "program:raydium AND amount:>1000000", "size": 20, "from": 0}'
```

Encrypted index — pass passphrase via header:

```bash
curl -H "x-index-password: mypassword" "http://localhost:8000/my_index/_search?q=..."
```

**Encrypted HuggingFace bucket indexes** — authenticate first, then query:

```bash
# Step 1: Authenticate to verify password
curl -X POST "http://localhost:8000/my-index/_authenticate?bucket=https://huggingface.co/buckets/owner/repo" \
  -H "Content-Type: application/json" \
  -d '{"passphrase": "my-password"}'
# Response: {"authenticated": true, "index": "my-index"}

# Step 2: Query with password header
curl -H "x-index-password: my-password" \
  "http://localhost:8000/my-index/_search?q=*&bucket=https://huggingface.co/buckets/owner/repo"
```

### Bucket URL Parameter

For HuggingFace and other remote indexes, append `?bucket=<url>` to any endpoint:

```bash
# List indices in a HuggingFace bucket
curl "http://localhost:8000/_indices?bucket=https://huggingface.co/buckets/owner/repo"

# Search an index within a bucket
curl "http://localhost:8000/adsb/_search?q=*&bucket=https://huggingface.co/buckets/owner/repo"
```

Interactive docs at `/_docs` (Swagger UI) and `/_redoc` (ReDoc).