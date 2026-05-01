# CLI Reference

```bash
flatseek <command> --help    # detailed options per command
```

## Indexing

| Command | Description |
|---|---|
| `flatseek build <file> -o <dir> [-w N]` | Build trigram index. `-w` for parallel workers. |
| `flatseek classify <file>` | Detect column types → `column_map.json`. |
| `flatseek plan <file> -o <dir>` | Generate parallel build plan without executing. |
| `flatseek compress <dir> [-l 1-9]` | zlib-compress index files in place. |
| `flatseek encrypt <dir> --passphrase P` | ChaCha20-Poly1305 encryption at rest. |
| `flatseek decrypt <dir> --passphrase P` | Decrypt. |
| `flatseek delete <dir> [--yes]` | Fast parallel-unlink delete. |

## Serve

| Command | Description |
|---|---|
| `flatseek serve [-d <dir>] [-p 8000]` | API + Flatlens dashboard. |
| `flatseek api [-d <dir>] [-p 8000]` | API only (no dashboard). |

## Query

| Command | Description |
|---|---|
| `flatseek search <dir> <query>` | CLI search. `-n` page size, `-p` page. |
| `flatseek stats <dir>` | Doc count, index size, columns, types. |
| `flatseek dedup <dir> --fields col1,col2` | Cross-shard dedup after parallel build. |

## Utilities

| Command | Description |
|---|---|
| `flatseek join <dir> "<qA>" "<qB>" --on field` | Cross-dataset join on shared field. |
| `flatseek chat <dir> [--model NAME]` | Natural-language query via Ollama. |