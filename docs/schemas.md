# Schemas & Data Modeling

---

## Overview

Flatseek ingests flat files (CSV, JSON, JSONL) and infers a schema automatically. Each column becomes a typed field that determines how it's indexed and queried.

- **Auto-inference** — first 200 rows sampled per column to detect type
- **Semantic override** — column names matched against known patterns (`status`, `amount`, `email`, etc.) before type detection runs
- **Manual override** — `column_map.json` or API `PUT /{index}/_mapping` to force a specific type

Column type controls query behavior: `KEYWORD` enables fast aggregations, `TEXT` enables wildcard search, `DATE` enables range queries via binary search.

---

## Supported Column Types

| Type | Description | Indexing | Query Support | Example |
|------|-------------|----------|---------------|---------|
| `TEXT` | Free-text, long values | Trigrams on ≤15-char tokens | wildcard (`*term*`), phrase | `name:*john*` |
| `KEYWORD` | Exact-value identifiers | Exact token only | equality, aggregation | `status:active` |
| `INT` | Integer numbers | Exact token + doc_values | range, stats aggregation | `amount:>1000` |
| `FLOAT` | Decimal numbers | Exact token + doc_values | range, stats aggregation | `score:>=88.5` |
| `DATE` | Date strings | Stored as `YYYYMMDD`, doc_values | range via binary search | `created_at:[20260101 TO 20261231]` |
| `BOOL` | True/false flags | Exact token | equality only | `enabled:true` |
| `ARRAY` | Lists (any element type) | Per-element exact token | equality on any element | `tags:graphql` |
| `OBJECT` | Nested dict | Dot-path expansion (`a.b.c`) | field access via dot-path | `address.city:Jakarta` |

**Type-specific notes**:

- `INT`/`FLOAT` with doc_values (`dv/{field}/numeric.bin`) get O(log N) range queries. Without it, falls back to full chunk scan.
- `DATE` values stored as `YYYYMMDD` integer strings. Range queries on date fields require this format.
- `BOOL` accepts `yes/no/1/0` as truthy variants.
- Leading zeros preserved — `08123...` phone numbers are stored as `KEYWORD`, not `INT`.

---

## Example Schema: Blockchain Transactions

```csv
tx_id,signer,programs,swap,amount,fee,block_time
abc123,Wallet7xMg,[raydium,orca],{"pool":"RAYDIUM","amount":1500000},1500000,4200,20260419
```

| Column | Type | Why |
|--------|------|-----|
| `tx_id` | KEYWORD | Exact identifier, no trigram needed |
| `signer` | KEYWORD | Wallet address, substring match via wildcard |
| `programs` | ARRAY | List of program names |
| `swap` | OBJECT | Nested dict with pool + amount |
| `amount` | INT | Numeric range queries |
| `fee` | INT | Stats aggregation |
| `block_time` | DATE | Stored as `YYYYMMDD` for range queries |

### Example Queries

```bash
# Array match — any doc where programs contains "raydium"
programs:raydium

# Nested field — swap.amount > 1M
swap.amount:>1000000

# Numeric range on array element value
swap.amount:[1000000 TO 5000000] AND programs:orca

# Date range
block_time:>=20260401 AND block_time:<=20260430

# Boolean-style filter combined with array
programs:raydium AND NOT swap.pool:ORCA
```

---

## Example Schema: Locations

```csv
id,city,tags,address,lat,lng,population,active
1,Jakarta,[gdp,se-asia,capital],{"district":"Central","province":"DKI"},-6.2088,106.8456,10700000,true
2,Surabaya,[industrial,east-java],{"district":"North","province":"Jawa Timur"},-7.2575,112.7521,2800000,true
3,Bandung,[tech,west-java],{"district":"Setiabudi","province":"Jawa Barat"],-6.9175,107.6191,2500000,false
```

| Column | Type | Why |
|--------|------|-----|
| `id` | KEYWORD | Exact ID, no numeric interpretation |
| `tags` | ARRAY | List of string tags |
| `address` | OBJECT | Nested dict |
| `lat`/`lng` | FLOAT | Coordinate ranges |
| `population` | INT | Stats + range |
| `active` | BOOL | Equality filter |

### Example Queries

```bash
# Array filtering — docs tagged with both tags
tags:tech AND tags:se-asia

# Nested object — address.district is "Central"
address.district:Central

# Numeric range — population > 1M
population:>1000000

# Multi-condition with exclusion
active:true AND NOT address.province:"DKI"

# Float range (latitude bounds)
lat:>=-6.5 AND lat:<=-6.0
```

---

## How ARRAY Works

ARRAY columns store each element as a separate exact token. Matching is **any-element** — querying `tags:graphql` finds docs where any array element equals `graphql`.

```
tags:graphql
tags:finance AND tags:web3     # both elements must exist
```

No special syntax required. Flatseek expands `tags` to `tags[0]`, `tags[1]`, etc. internally — the query surface is identical to a scalar field.

**Limitations**:
- No order-sensitive queries (no "tags[0] = x AND tags[1] = y")
- No array-length filters
- Aggregation on ARRAY uses the streaming path (no pre-computed terms.bin)

---

## How OBJECT Works

OBJECT columns are flattened on ingest: each dot-path segment becomes a separate searchable column.

```json
{"address": {"district": "Central", "province": "DKI"}}
```

Becomes queryable as:

```
address.district:Central
address.province:DKI
```

Deep nesting works at any depth: `info.metadata.code:ABC123`. Empty path segments (`..`) are skipped.

**Query behavior**:
- Equality: `address.district:Central`
- Wildcard: `address.district:*Cent*`
- Range: `swap.amount:>1000000`
- Dot-path is literal — no regex over the path itself

---

## Type Inference

Inference runs in two passes:

**Pass 1 — Value sampling** (`scanner.detect_type`):
```
ARRAY    ≥70% of samples start with '[' and end with ']'
OBJECT   ≥70% of samples start with '{' and end with '}'
DATE     ≥80% of samples match date regex (YYYY-MM-DD, DD/MM/YYYY, etc.)
BOOL     ≥80% of samples in {true, false, yes, no, 1, 0}
INT      all samples parse as integer, no leading 0 / +
FLOAT    all samples parse as float
TEXT     avg tokens/row ≥ 2
KEYWORD  avg tokens/row < 2
```

**Pass 2 — Semantic classification** (`classify.classify_column`):
Column names matched against precomputed alias dictionaries (`_ALIAS_EXACT`, `_ALIAS_PARTIAL`, `_PATTERNS`) before value-based detection runs. This means `status` is KEYWORD even if the first 200 rows happen to be short strings that could be TEXT.

---

## Overriding the Inferred Schema

**Via `column_map.json`** (build-time):

```json
{
  "tx_id":   "KEYWORD",
  "signer":  "KEYWORD",
  "programs":"ARRAY",
  "amount":  "INT",
  "block_time": "DATE"
}
```

Place `column_map.json` in the output directory before building, or pass via CLI:

```bash
flatseek build ./data.csv -o ./data --column-map ./column_map.json
```

**Via API** (runtime):

```bash
curl -X PUT http://localhost:8000/my_index/_mapping \
  -H "Content-Type: application/json" \
  -d '{"programs":"ARRAY","swap":"OBJECT","amount":"INT"}'
```

Override is useful when:
- Column name is ambiguous (`id` could be KEYWORD or INT — force KEYWORD for address-like IDs)
- First rows are unrepresentative (nulls early, then valid data)
- You want `TEXT` indexing on a column that auto-detected as `KEYWORD` (e.g., long description field with low token count in sample)

---

## Best Practices

- **KEYWORD for filters + aggregations** — exact match, precomputed terms.bin for fast facet counts
- **TEXT for searchable content** — trigram indexing enables `*substring*` wildcards
- **DATE as YYYYMMDD or ISO 8601** — `20260419` enables binary-search ranges; free-form dates fall back to string matching
- **Keep arrays small** — ARRAY with high-cardinality elements (thousands per doc) increases posting list size
- **Prefer flat over deeply nested** — OBJECT works but each dot-segment adds a separate posting list entry
- **Force KEYWORD for ID columns** — auto-detection may misclassify numeric strings as INT
- **Avoid leading zeros in numeric columns** — `0812...` preserved as KEYWORD, not INT

---

## Quick Reference

| Need | Use type | Query example |
|------|----------|---------------|
| Exact filter | KEYWORD | `status:active` |
| Substring match | TEXT | `name:*john*` |
| Numeric range | INT/FLOAT | `amount:>1000` |
| Date range | DATE | `created_at:[20260101 TO 20261231]` |
| Boolean filter | BOOL | `enabled:true` |
| Match any array element | ARRAY | `tags:graphql` |
| Access nested field | OBJECT | `address.city:Jakarta` |
| Aggregation (terms) | KEYWORD | terms.bin precomputed |
| Aggregation (stats) | INT/FLOAT | numeric.bin binary search |
| Multi-condition | combined | `status:active AND amount:>1000 AND NOT region:eu` |