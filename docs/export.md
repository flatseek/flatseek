# Export

Stream matching documents out of an index as JSONL or CSV. Exports are **OOM-safe** — designed for datasets of any size because chunks are read and emitted one at a time.

---

## Index types

| Type | How to use |
|------|-------------|
| **Directory index** | `flatseek export ./data -o out.jsonl` |
| **FSK file** | `flatseek export data.fsk -o out.jsonl` |
| **Remote URL** | `flatseek export "https://..." -o out.jsonl` |
| **Multi-FSK bucket** | `flatseek export ./fsk_bucket -o out.jsonl --index "logs_2025-*"` |

---

## CLI

### Basic export

```bash
# JSONL (default)
flatseek export ./data -o out.jsonl

# CSV
flatseek export ./data -f csv -o out.csv
```

### Query filter

```bash
flatseek export ./data -q "level:ERROR" -o errors.jsonl
flatseek export ./data -q "amount:>1000000" -o large.jsonl
```

### Column selection

```bash
flatseek export ./data -c "id,timestamp,amount" -o subset.csv
```

### Limit rows

```bash
flatseek export ./data -n 1000 -o first1k.jsonl
```

### All index types

```bash
# FSK file
flatseek export data.fsk -o out.jsonl

# Remote URL
flatseek export "https://example.com/data.fsk" -o out.jsonl

# Directory index
flatseek export ./data -o out.jsonl
```

### Multi-FSK export

```bash
# All matching FSKs
flatseek export ./fsk_bucket -q "*" --index "logs_2025-*" -o out.jsonl

# Single FSK from bucket
flatseek export ./fsk_bucket -q "*" --index logs_2025-01 -o out.jsonl

# Multiple patterns
flatseek export ./fsk_bucket -q "*" --index "logs_2025-01,logs_2025-02" -o out.jsonl
```

### Streaming to stdout

```bash
flatseek export ./data -q "ERROR" | jq '.amount'
```

### Resume interrupted export

Exports support checkpointing. If interrupted, re-run with the same arguments to resume:

```bash
# First run — interrupted at row 50,000
flatseek export ./data -q "*" -o out.jsonl
^C  # Checkpoint saved

# Resume from row 50,000
flatseek export ./data -q "*" -o out.jsonl
# → [EXPORT] Resuming from checkpoint: 50000 rows already exported

# Force restart
flatseek export ./data -q "*" -o out.jsonl --no-resume
```

Resume works by tracking the last exported `_id` in a sidecar file (`out.jsonl.flatseek-export-checkpoint.json`). Re-running with the same output path and query resumes automatically.

### Quiet mode

```bash
flatseek export ./data -q "*" -o out.jsonl --quiet
```

---

## Output formats

### JSONL (default)

One JSON object per line:

```
{"_id": 0, "program": "raydium", "amount": "1500000"}
{"_id": 1, "program": "jupiter", "amount": "750000"}
```

### CSV

First row is the header:

```csv
_id,program,amount,timestamp
0,raydium,1500000,2025-01-15T10:30:00Z
1,jupiter,750000,2025-01-15T11:00:00Z
```

---

## Architecture

Exports stream chunk-by-chunk without loading the full result set into memory. The flow:

1. Resolve matching document IDs via posting lists
2. Group IDs by `chunk_start` (top 32 bits of doc ID)
3. For each chunk, load only that chunk, yield matching docs, discard it
4. Checkpoint every 1,000 rows — safe to interrupt

This means an export of 10M rows uses the same ~10 MB regardless of total result set size.
