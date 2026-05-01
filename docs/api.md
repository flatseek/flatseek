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
curl -H "X-Index-Password: mypassword" "http://localhost:8000/my_index/_search?q=..."
```

Interactive docs at `/_docs` (Swagger UI) and `/_redoc` (ReDoc).