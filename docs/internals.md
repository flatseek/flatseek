# Flatseek Internals

**Purpose**: Deep technical teardown — how the engine actually works under the hood. Storage formats, algorithms, data structures, performance characteristics.
**Companion**: [`architecture.md`](architecture.html) — project structure and module map.

**Version**: 0.1.2 | **Last updated**: 2026-05-01

---

## Table of Contents

- [Overview](#overview)
- [Indexing Pipeline](#indexing-pipeline)
  - [File Ingestion](#file-ingestion)
  - [Column Type Detection](#column-type-detection)
  - [Internal Type Representation](#internal-type-representation)
  - [Tokenization](#tokenization)
  - [Trigram Generation](#trigram-generation)
  - [Hash Bucketing](#hash-bucketing)
  - [Posting List Construction](#posting-list-construction)
  - [Compression](#compression)
  - [Parallel Indexing](#parallel-indexing)
  - [Checkpoint Strategy](#checkpoint-strategy)
  - [Output Layout](#output-layout)
- [Storage Format](#storage-format)
  - [Trigram Index Format](#trigram-index-format)
  - [Columnar Doc Store](#columnar-doc-store)
  - [Doc Values (Columnar Storage)](#doc-values-columnar-storage)
  - [Memory Mapping](#memory-mapping)
  - [Compression Boundaries](#compression-boundaries)
- [Query Parsing](#query-parsing)
  - [Grammar](#grammar)
  - [Tokenizer](#tokenizer)
  - [AST Nodes](#ast-nodes)
  - [AST Execution](#ast-execution)
- [Execution Engine](#execution-engine)
  - [Term Lookup Resolution](#term-lookup-resolution)
  - [Posting List Retrieval](#posting-list-retrieval)
  - [Intersection Algorithms](#intersection-algorithms)
  - [Range Query Execution](#range-query-execution)
  - [Wildcard Execution](#wildcard-execution)
  - [NOT Query Execution](#not-query-execution)
  - [Result Set Construction](#result-set-construction)
- [Aggregations](#aggregations)
  - [Terms Aggregation](#terms-aggregation)
  - [Stats / Avg / Min / Max / Sum](#stats--avg--min--max--sum)
  - [Cardinality (HyperLogLog)](#cardinality-hyperloglog)
  - [Date Histogram](#date-histogram)
- [Performance Characteristics](#performance-characteristics)
  - [Why p50 Is Fast](#why-p50-is-fast)
  - [Why p95 Spikes](#why-p95-spikes)
  - [Disk vs CPU Tradeoffs](#disk-vs-cpu-tradeoffs)
  - [Memory Usage Patterns](#memory-usage-patterns)
  - [Cache Behavior](#cache-behavior)
- [Design Tradeoffs](#design-tradeoffs)
  - [Why Trigram Index](#why-trigram-index)
  - [Why Columnar Doc Store](#why-columnar-doc-store)
  - [Why mmap Instead of In-Memory Structures](#why-mmap-instead-of-in-memory-structures)
  - [Known Limitations](#known-limitations)
- [Comparison with Other Engines](#comparison-with-other-engines)
  - [vs. Elasticsearch](#vs-elasticsearch)
  - [vs. Tantivy](#vs-tantivy)
  - [vs. Typesense](#vs-typesense)
- [Implementation Reference](#implementation-reference)

---

## Overview

Flatseek is an embeddable full-text search engine optimized for analytical workloads on structured record data (CSV, JSON). It is designed to run on a single machine — no distributed cluster, no JVM, no separate service required for moderate-scale data (up to ~50M rows). At its core is a **trigram inverted index** backed by memory-mapped file I/O, with a columnar document store for fast aggregations.

The engine is structured in two phases:

**Build phase** (`IndexBuilder`): reads raw records, tokenizes, generates trigrams, writes hash-bucketed posting lists and compressed document chunks.

**Query phase** (`QueryEngine`): resolves Lucene-style query strings against the trigram index, fetches document chunks, executes filter/post-filter operations.

```
CSV/JSON → scanner (type detection) → IndexBuilder
    → index/{aa}/{bb}/idx.bin     (trigram posting lists, zlib-compressed)
    → docs/{aa}/{bb}/docs_{N}.zlib (document store, zlib/zstd-compressed)
    → dv/{field}/numeric.bin      (columnar doc_values)
    → dv/{field}/terms.bin        (columnar term counts)
    → stats.json                  (column schema + index stats)

QueryEngine
    → term_hash(term) → bucket dir → mmap read idx.bin
    → decode delta-varints → sorted doc_id set
    → _verify_wildcard (if trigram candidate)
    → load doc chunks → return results
```

---

## Indexing Pipeline

### File Ingestion

The build pipeline handles CSV, JSON, JSONL, and NDJSON, with transparent gzip/bzip2 decompression wrappers (`_open_source`). The reading strategy adapts based on file size and available libraries:

- **pandas** (if installed): used for CSV parsing — 3–5× faster than the stdlib `csv.DictReader`. Read in 10,000-row chunks via `pd.read_csv(..., chunksize=10_000)`.
- **csv.DictReader**: fallback, processes row-by-row.
- **JSON streams** (`_iter_json_file`): iterated directly without loading the full file.

Compressed files cannot be seeked, so byte-range parallel splits are disabled for `.gz`/`.bz2` sources. For parallel builds on large CSV files, the orchestrator uses `_ByteRangeReader` — a custom `io.RawIOBase` that injects the CSV header line at the start of each worker's byte slice, enabling O(1) seek to a worker's partition without reading and discarding rows.

### Column Type Detection

Type detection runs in two passes:

**Pass 1 — Value-based (`scanner.detect_type`)**: samples up to 200 rows per column, applies a priority cascade:

```
ARRAY   (≥70% of samples start with '[' and end with ']')
OBJECT  (≥70% of samples start with '{' and end with '}')
DATE    (≥80% of samples match a date regex)
BOOL    (≥80% of samples in known-true/false set)
INT     (all samples parse as integer, no leading 0/+)
FLOAT   (all samples parse as float)
TEXT    (avg tokens/row ≥ 2)
KEYWORD (avg tokens/row < 2)
```

Leading zeros are explicitly preserved as non-numeric — `0812...` phone numbers and E.164-formatted numbers (`+628...`) are stored as keywords, not integers.

**Pass 2 — Semantic classification (`classify.classify_column`)**: column names are matched against precomputed alias dictionaries:

- **KEYWORD**: `status`, `city`, `id_number`, `phone`, `gender`, etc.
- **TEXT**: `name`, `address`, `description`, `notes`, etc.
- **DATE**: `birthday`, `created_at`, `join_date`, etc.
- **EMAIL/PHONE**: regex-matched values, stored as KEYWORD
- **ARRAY/OBJECT**: detected from structure

Aliases use a bounded precomputed lookup (`_ALIAS_EXACT` dict, `_ALIAS_PARTIAL` list, `_PATTERNS` list) built once at module load — zero per-row regex compilation.

The result is a **semantic type** (KEYWORD, TEXT, DATE, FLOAT, etc.) that drives how the value is indexed.

### Internal Type Representation

| Storage type | Trigrams | Exact token | Range query | Doc values |
|---|---|---|---|---|
| TEXT | yes (≤15 char tokens) | yes | fallback scan | — |
| KEYWORD | no | yes | fallback scan | terms.bin |
| DATE | no | yes (YYYYMMDD) | binary search | numeric.bin |
| INT/FLOAT | no | yes | binary search | numeric.bin |
| BOOL | no | yes | exact only | — |
| ARRAY | no | yes (per element) | element scan | terms.bin |
| OBJECT | no | yes (dot-path keys) | dot-path scan | — |

### Tokenization

`tokenize(value)` extracts searchable tokens from a cell:

```python
def tokenize(value):
    tokens = set()
    tokens.add(value.strip().lower())           # full value
    tokens.update(_SPLIT_RE.split(lower))        # split on whitespace, _, -, ,, /, ., \, |
    if '@' in value:
        local, domain = value.split('@', 1)
        tokens.add(local); tokens.add(domain)    # email parts
    if has_digit(value):
        digits = _DIGITS_RE.sub("", value)        # strip non-digits
        tokens.add(digits[-4:])                  # last 4 digits (phone tail)
        tokens.add(digits[:4])                   # first 4 digits (phone head)
```

Tokens shorter than 2 characters are dropped from the token set but the full lowercased value is always indexed as an exact token (ensuring that single-character codes like `gender:L` work).

### Trigram Generation

For TEXT-type columns, every token ≤ 15 characters gets trigrams:

```python
def make_trigrams(token):
    for i in range(len(token) - 2):
        yield token[i:i+3]
```

The 15-character limit prevents long values (full email addresses, URLs, long addresses) from generating an excessive number of low-value trigrams. Short tokens already produce enough trigrams for practical wildcard matching; long tokens are covered by their constituent subtokens.

Each trigram `t` for column `col` is stored as the term `col:~t` in the posting list.

**Global trigrams** (unscoped `~t`) are NOT stored — wildcard queries across all columns expand to per-column trigram lookups and union the results.

### Hash Bucketing

Terms are assigned to 65,536 buckets via CRC32:

```python
def term_hash(term: str) -> str:
    h = zlib.crc32(term.encode()) & 0xFFFFFFFF
    return f"{(h >> 24) & 0xFF:02x}/{(h >> 16) & 0xFF:02x}"
```

This produces a 2-level hex directory tree: `index/aa/bb/idx.bin` where `aa = high byte`, `bb = next byte` of the CRC32. Each bucket covers 65536 bucket IDs. The bucket prefix string is cached by 16-bit bucket index (at most 65536 entries) to avoid repeated f-string allocation.

### Posting List Construction

The index entry binary format:

```
[2 bytes: term_len LE] [term_len bytes] [4 bytes: doclist_len LE] [doclist_len bytes]
```

Doc IDs are appended monotonically, then delta-encoded as varints at checkpoint flush:

```python
def encode_doclist(doc_ids):
    # doc_ids must be sorted and unique
    out = bytearray()
    prev = 0
    for did in doc_ids:
        d = did - prev          # delta is always non-negative
        while d >= 0x80:
            out.append((d & 0x7F) | 0x80)
            d >>= 7
        out.append(d)
        prev = did
    return bytes(out)
```

Varint encoding uses 7 bits per byte with a continuation bit (0x80). Decoding:

```python
def decode_doclist(data):
    ids, i, prev = [], 0, 0
    while i < len(data):
        val = shift = 0
        while data[i] & 0x80:
            val |= (data[i] & 0x7F) << shift
            shift += 7; i += 1
        val |= data[i] << shift; i += 1
        ids.append(prev + val)
    return ids
```

In-memory doc ID storage uses `array('I')` (C uint32) instead of Python `list` of `int` objects — a 7× memory reduction for large posting lists (20M entries: ~80 MB vs ~560 MB).

### Compression

**Index postings**: zlib level 9 (maximum), applied once at segment merge in `finalize()`. During checkpoints, data is accumulated in plain bytearrays. After `_SEGMENT_SWITCH_SIZE` (5 MB) accumulated in a single prefix, segment-append mode kicks in — new checkpoints write immutable `idx_seg0001.bin`, `idx_seg0002.bin`, ... files instead of appending to the growing idx.bin (which would require read+decompress+recompress at every checkpoint = O(N²) total).

At `finalize()`, all segment files are concatenated and compressed once into `idx.bin`, then segments are deleted.

**Document chunks**: zlib level 6 (default), or zstd if available (7× faster decompress, 7× compression ratio vs 6× for zlib). Each 100,000-document chunk is independently compressed and stored at `docs/{aa}/{bb}/docs_{N:010d}.zlib`. Magic bytes are used to detect format on read: zstd = `0x28 0xb5 0x2f 0xfd`, zlib = `0x78 0x9c` / `0x78 0x01` / etc.

**Encryption**: ChaCha20-Poly1305 with a 9-byte magic header (`FLATSEEK\x01`) prepended before ciphertext. Key derivation: PBKDF2-HMAC-SHA256, 600,000 iterations. Encrypted files are always encrypted AFTER compression.

### Parallel Indexing

The build orchestrator (`build()`) implements parallel workers via byte-range splitting:

1. `build_plan.json` is computed upfront: total rows estimated, then divided by doc ID ranges per worker.
2. Each worker receives `(byte_start, byte_end, doc_id_start, doc_id_end)` for their slice.
3. Workers write to `idx_w{worker_id}.bin` (sharded idx files) — no file lock conflicts.
4. `_discover_index_dirs()` in `QueryEngine` reads ALL `*.bin` files in a bucket directory and merges their posting lists.

At `finalize()` of the orchestrator, all shard stats are merged, and all segment files across shards are collected and merged per bucket.

### Checkpoint Strategy

```
CHECKPOINT_ROWS = 50,000     # flush every 50K rows
BUFFER_FLUSH_BYTES = 512 KB # flush a prefix buffer when it exceeds 512 KB
CHECKPOINT_WRITE_BYTES = 8 KB # minimum buffer size before writing to disk
```

**Problem it solves**: 65,536 prefix directories × 8 workers × 50K-row checkpoints = 524K file-open/write/close ops per cycle on macOS APFS. Each costs 50–100 µs, causing massive I/O wait.

**Solution**: small prefix buffers accumulate in RAM across checkpoints. Only buffers ≥ `CHECKPOINT_WRITE_BYTES` are written per checkpoint. This reduces file ops from 524K to ~11K per cycle (47× fewer).

**Pressure WAL**: when total in-memory buffer bytes exceed 50 MB (memory pressure), ALL 65K buffers are written as ONE sequential WAL file (one `f.write()` call), then buffers are cleared. A `finalize()` WAL merge step scatters the WAL data back to regular prefix files.

**Segment-append**: once a prefix file exceeds 5 MB, subsequent checkpoints write immutable segment files (`idx_seg*.bin`) instead of reading and recompressing the entire accumulated file. This eliminates O(N²) recompression on S3-compatible storage.

### Output Layout

```
{output_dir}/
  index/
    {aa}/{bb}/           # 2-level CRC32 hash directory (65536 combinations)
      idx.bin            # compressed postings (or)
      idx_w0.bin         # worker shard (parallel builds)
      idx_seg0001.bin    # immutable segments (large prefixes, S3-friendly)
      terms.set          # optional: pickled frozenset of terms in this bucket
  docs/
    {aa}/{bb}/           # 2-level hex directory (matches chunk numbering)
      docs_0000000000.zlib  # 100K docs each
      docs_0000100000.zlib
  dv/
    {field}/
      numeric.bin        # (doc_id: uint64, value: float64) pairs, sorted by value
      terms.bin          # (term_len: uint16, term: UTF-8, count: uint32), sorted by count
  stats.json            # total_docs, total_entries, index_size_mb, columns schema
  encryption.json       # (if encrypted) salt + key_id
  _wal/                 # pressure WAL files (temporary, merged at finalize)
```

---

## Storage Format

### Trigram Index Format

Each `idx.bin` (or `idx_seg*.bin`) is a concatenated sequence of binary index entries, zlib-compressed:

```
[2-byte term_lenLE][term_len bytes][4-byte doclist_lenLE][doclist_len bytes]
```

The entire file is compressed as one zlib stream. To read posting list for term `T`:

1. Decompress the entire file into memory.
2. Scan sequentially — no index on top of the postings.
3. For each entry: read term_len, compare term, if match → decode doclist, if not → skip.

**Bucket term set** (`terms.set`): a pickle of `frozenset` containing all terms indexed in that bucket. Used for **fast rejection** — before decompressing any posting file, if the sought term is not in the term set, the file is skipped entirely. This gives 2–4× speedup for non-existent terms.

**Parallel build shards**: each worker writes `idx_w{N}.bin`. `QueryEngine._read_posting()` reads ALL `*.bin` files (not just `idx.bin`) in the bucket directory, decodes each, merges and deduplicates doc ID lists.

### Columnar Doc Store

Document chunks at `docs/{aa}/{bb}/docs_{N:010d}.zlib`:

- Format: JSON (orjson, 3–5× faster than stdlib json), zlib/zstd compressed.
- Filename encodes the starting doc_id: `docs_0000000000.zlib` starts at doc_id 0.
- Each chunk: up to 100,000 documents, keyed by `doc_id` as string (JSON object where keys are doc ID integers).
- Two-level hex path: `n = chunk_start // 100000`, `aa = (n >> 8) & 0xFF`, `bb = n & 0xFF`.

Chunk loading (`_load_chunk`):
```python
def _chunk_start(doc_id):
    return (doc_id // 100_000) * 100_000

def _doc_path(chunk_start):
    n = chunk_start // 100_000
    aa = f"{(n >> 8) & 0xFF:02x}"
    bb = f"{n & 0xFF:02x}"
    return f"docs/{aa}/{bb}/docs_{chunk_start:010d}.zlib"
```

### Doc Values (Columnar Storage)

Written at `finalize()` for INT/FLOAT/KEYWORD/ARRAY fields:

**`dv/{field}/numeric.bin`**: `(doc_id: uint64_le, value: float64_le)` pairs, sorted by value ascending. Used for O(log N) range queries via binary search and O(N) stats aggregation.

**`dv/{field}/terms.bin`**: `(term_len: uint16_le, term_bytes: UTF-8, count: uint32_le)` entries, sorted by count descending. Used for O(1) terms aggregation (no document scan needed).

### Memory Mapping

`QueryEngine._mmap_read()` maps posting index files via `mmap.mmap(f.fileno(), size, access=mmap.ACCESS_READ)`. The OS page cache keeps hot index files in memory across queries. Mmap handles are cached in `_mmap_cache` and reused — the OS manages page faults transparently.

Doc chunks are NOT mmap'd — they are read into a Python bytes object and held in `_doc_cache`. This is because doc chunks are larger and accessed more selectively; mmap'd write-once files work well for posting lists.

### Compression Boundaries

- **Index postings**: compressed once at `finalize()` as a whole — individual posting entries are NOT independently compressed. Scanning requires decompressing the entire bucket file.
- **Document chunks**: compressed independently — each 100K-doc chunk is its own compressed stream. Corruption in one chunk does not affect others.
- **Encryption**: applied after compression. The 9-byte magic header allows the decryptor to detect encrypted vs plain files in the same read path.

---

## Query Parsing

### Grammar

Flatseek uses a hand-written recursive descent parser over a token stream from a custom lexer. There is no external parser generator.

```
query  ::= or_expr
or_expr ::= and_expr (OR and_expr)*
and_expr ::= not_expr (AND? not_expr)*     # AND is optional between terms
not_expr ::= NOT? primary
primary ::= TERM | RANGE | LP or_expr RP

TERM ::= field? ':'? value   # field:value or bare value
RANGE ::= field ( ('>=' | '>' | '<=' | '<' | 'between') value )+
```

Query precedence (high → low): `NOT` > `AND` > `OR`.

### Tokenizer

The `tokenize(text)` function (query_parser.py, separate from the builder's `tokenize`) scans the query string character by character, handling:

- **Quoted phrases**: `instruction:"close account"` — spaces inside quotes are preserved as part of the term value.
- **Field colons**: only break on `:` if it is NOT preceded by alphanumeric (to allow dotted field names like `info.metadata.a.tags[0]`).
- **Bracket ranges**: `altitude:[30000 TO 40000]` — the colon before `[` signals a range operator, not a field separator.
- **Array index notation**: `tags[0]:alpha` — detected and combined into a single `TERM("tags[0]", "alpha")` token.
- **Minus prefix**: `-field:value` is equivalent to `NOT field:value`.

Tokens are emitted as 2–4 tuples: `('AND',)`, `('TERM', field, value, quoted)`, `('RANGE', field, op, val)`, `('RANGE', field, 'between', lo, hi)`.

### AST Nodes

Five node types (plain tuples):

```python
('term',   field, value, quoted)   # field may be None (cross-column search)
('range',  field, op, val)        # op in ('>', '>=', '<', '<=', 'between')
('and',    left, right)
('andnot', left, right)            # left AND NOT right
('or',     left, right)
('not',    child)                  # standalone NOT, converted to andnot by parent
```

The parser promotes `not` children upward: if the right operand of an implicit AND is a `not` node, the parent constructs an `andnot`. This handles `-field:val` as `AND NOT`.

### AST Execution

`execute(node, qe)` walks the AST, dispatching each node to the appropriate `QueryEngine` method:

```python
def execute(node, qe):
    if node[0] == 'term':
        return set(qe._resolve(value, field, exact=exact))
    if node[0] == 'range':
        return qe._resolve_range(field, *rest)
    if node[0] == 'and':
        left_ids = execute(left, qe)
        return left_ids & execute(right, qe) if left_ids else set()
    if node[0] == 'andnot':
        return execute(left, qe) - execute(right, qe)
    if node[0] == 'or':
        return execute(left, qe) | execute(right, qe)
```

Short-circuit evaluation: AND returns empty set as soon as any operand returns empty. OR does not short-circuit (needs both sides to compute the union).

---

## Execution Engine

### Term Lookup Resolution (`_resolve`)

The resolution algorithm branches based on the term structure:

**Exact match** (no wildcard):
1. Read the posting list for `col:value`.
2. If empty and a column is specified: try expanded array/object variants (e.g., `program:` → also try `program[0]`, `address.city`).
3. If still empty: fall back to global exact token (no column scope).
4. If bare multi-word phrase: fan out across all columns.
5. If nothing found and `exact=False`: fall back to infix wildcard (`term` → `*term*`).

**Wildcard match** (`*` or `%` in term):
1. Split term on `*`/`%` to extract literal segments.
2. For each segment ≥ 3 characters, generate trigrams.
3. If no trigrams and internal wildcards exist: return empty. If only trailing/leading wildcards: fall back to exact lookup on the literal.
4. Read posting lists for each trigram (`col:~tg`).
5. Sort by list size ascending (rarest first).
6. Intersect smallest → largest.
7. Safety cap: if candidates > 500,000 → `_wildcard_fallback`.
8. `_verify_wildcard`: post-filter candidates against actual document values using regex.

**`_wildcard_fallback`**: when trigram intersection produces too many candidates, scan index terms directly. Build a regex from the wildcard pattern, scan all terms in all bucket directories, return only those matching the pattern. Limited to 100,000 terms checked.

### Posting List Retrieval

```python
def _read_posting(term):
    prefix = term_hash(term)                    # "aa/bb"
    bucket_dir = index_dir / prefix

    # Fast reject via terms.set
    term_set = _load_bucket_term_set(prefix)
    if term_set is not None and term not in term_set:
        return []

    # Read all *.bin files in the bucket
    bin_files = sorted(os.listdir(bucket_dir))  # handles idx_w*.bin shards
    all_ids = []
    for bin_file in bin_files:
        data = _mmap_read(bin_path)
        data = _decrypt_if_needed(data)
        if zlib_magic(data): data = zlib.decompress(data)

        # Scan entries sequentially
        offset = 0
        while offset + 2 <= len(data):
            term_len = struct.unpack_from("<H", data, offset)[0]; offset += 2
            stored = data[offset:offset + term_len].decode(); offset += term_len
            pl_len = struct.unpack_from("<I", data, offset)[0]; offset += 4
            if stored == term:
                all_ids.extend(decode_doclist(data[offset:offset + pl_len]))
            offset += pl_len

    return sorted(set(all_ids))
```

**Caching hierarchy**:
1. `_hot_postings`: never evicted. Terms with common column names (`status`, `type`, `program`) or small posting lists (< 50K docs).
2. `_posting_cache`: bounded LRU (8,192 terms). When full, evict half.
3. `_mmap_cache`: OS page cache for file contents. Reused across queries.

**Fast reject**: if `terms.set` exists and the term is not in it, all posting file reads are skipped — 2–4× speedup for non-existent terms.

### Intersection Algorithms

**Set intersection** (default): converts posting lists to Python `set`, uses `&`. For small-to-medium lists this is faster than merge-based approaches.

**Sorted merge** (used in `search_and`): sorts conditions by posting list size, then intersects smallest first. This is the standard Tantivy conjunction optimization — processing the rarest term first minimizes the number of comparisons.

### Range Query Execution

`_resolve_range(field, op, val)`:

1. **With doc_values available** (`dv/{field}/numeric.bin`): load the binary `(value, doc_id)` pairs, use `bisect` to find lower/upper bounds in O(log N). Returns a set of doc_ids.
2. **Without doc_values**: full chunk scan — iterate all chunks, all doc IDs in range, evaluate `_get_nested_value(doc, field)` against the comparison.

Numeric values are parsed by `_parse_numeric()`: strips commas, attempts `float()`. Pure 8-digit integers that match YYYYMMDD are rejected as numeric (treated as dates instead) via regex.

Date ranges (`[20260101 TO 20261231]`): extract year tokens (`\d{4}`), convert to posting list lookups for each year term (`field:2026`, `field:2027`). This is year-precision only — no month/day resolution at query time.

Age fields (`umur`, `age`, `usia`): special-case inversion converting age → birth year range (current year minus age).

### Wildcard Execution

```
pattern: "*garuda*"
  → literal segments: ["garuda"]
  → trigrams: ["gar", "aru", "rud", "uda"]
  → read posting lists for col:~gar, col:~aru, col:~rud, col:~uda
  → intersect smallest → largest
  → verify: regex ".*garuda.*" against actual document values
```

**Why verification is necessary**: trigram intersection guarantees all trigrams are present but NOT that they appear consecutively. "RAYDIUMV2" contains trigrams "ray" and "diu" but not the substring "garuda".

**False positive example**: "RAYDIUM" contains 'ray' + 'diu' + 'ium'; a query for `*garuda*` would find 'gar' + 'aru' + 'rud' + 'uda' — none of which appear in "RAYDIUM", so it gets correctly eliminated by the verifier.

### NOT Query Execution

```python
if node[0] == 'andnot':
    return execute(left, qe) - execute(right, qe)
```

Implemented as a Python set difference. This is efficient when the negated posting list is small (e.g., `NOT status:active` where `status:active` is large, the set difference still requires iterating the left operand's entire set). However, there is no special optimization for NOT when the right operand is large — the full left set must be materialized.

### Result Set Construction

Doc IDs are sorted integers. The `_paginate(doc_ids, page, page_size)` method:
1. Slices `doc_ids[start:start + page_size]`.
2. Groups remaining doc IDs by chunk (`_chunk_start`).
3. Loads each chunk once via `_load_chunk`.
4. Fetches individual docs via dict lookup.
5. `_collapse_expanded_fields` reconstructs nested arrays/objects from their flat dot-path keys (`tags[0]`, `info.metadata.a.value`) for API responses.

---

## Aggregations

### Terms Aggregation

Two execution paths depending on available doc_values:

**Fast path** (doc_values `terms.bin` available, no query filter):
```python
# Load term→count pairs directly from dv/{field}/terms.bin
terms = [(term, count) for term, count in dv]  # already sorted by count
buckets = terms[:size]
```

**Streaming path** (no doc_values, or with query filter):
```python
terms_counter = Counter()
for chunk_start, chunk in _iter_chunks():
    for doc_id, doc in chunk.items():
        if matching_ids and doc_id not in matching_ids: continue
        val = doc.get(field)
        if isinstance(val, (list, tuple)):
            for item in val: terms_counter[item] += 1
        else:
            terms_counter[val] += 1
top = terms_counter.most_common(size)
```

Memory bounded at 1M unique terms — after cap, overflow counter tracks that truncation occurred. `sum_other_doc_count` reflects the gap.

### Stats / Avg / Min / Max / Sum

Streaming single-pass computation:
```python
running_count = 0; running_sum = 0.0; running_min = None; running_max = None
for chunk in _iter_chunks():
    for doc_id, doc in chunk.items():
        num = _to_number(doc.get(field))
        if num is None: continue
        running_count += 1; running_sum += num
        running_min = min(running_min, num) if running_min else num
        running_max = max(running_max, num) if running_max else num
```

No second pass needed. All five metrics (count, min, max, avg, sum) are computed in a single iteration.

### Cardinality (HyperLogLog)

A fixed ~48KB sparse bitmap sketch (`_CardinalitySketch`):

```python
class _CardinalitySketch:
    def __init__(self, m=65536):
        self.m = m
        self._bitmap = [0] * m          # 65536 6-bit registers = 48 KB
        self._zero_count = m
        self._max_reg = 0

    def add(self, value):
        h = hashlib.sha256(str(value).encode()).digest()[:8]
        reg_idx = int.from_bytes(h[:4], "little") % self.m
        bits = h[4] & 0x3F
        # Update 6-bit register at reg_idx

    def count(self):
        if self._zero_count > 0:
            return int(self.m * math.log(self.m / self._zero_count))
        alpha = 0.6735  # for m=65536
        raw = alpha * self.m * self.m / sum(1.0 / (1 << max(1, reg_bits))
                                            for reg_bits in self._bitmap)
        return int(min(raw, 2 ** (self._max_reg + 1)))
```

Expected error: ~1.6% for large cardinalities. Uses SHA-256 (not MurmurHash) for deterministic cross-platform hashing.

### Date Histogram

Streaming `Counter` keyed by normalized date strings:
```python
date_counter = Counter()
for chunk in _iter_chunks():
    for doc_id, doc in chunk.items():
        val_str = _parse_date(val)  # "2026-04-19" → "20260419"
        date_counter[val_str] += 1

# Post-process: bucket by interval (hour/month/year)
processed = Counter()
for k, c in date_counter.items():
    if interval == "hour":   key = k[:8] + "0000"
    elif interval == "month": key = k[:6]
    elif interval == "year": key = k[:4]
    else: key = k[:8]        # day (default)
    processed[key] += c
```

---

## Performance Characteristics

### Why p50 Is Fast

p50 queries hit the **hot posting cache** (`_hot_postings`) — common filter terms like `status:active`, `type:tx`, `program:raydium` are promoted to the never-evicted cache on first use. Subsequent reads are pure Python list lookups (O(1)).

Even for uncached terms, a small posting list read is a single mmap + decompress of a small bucket file, followed by a sequential scan to find the term. With zlib compression at level 9, a small bucket file fits in L3 cache after the first decompress.

### Why p95 Spikes

**Wildcard**: `_verify_wildcard` must load document chunks for every candidate doc ID. If the trigram intersection produces 50,000 candidates, that's 50,000 doc chunk lookups. Each lookup may trigger a chunk load (if not in `_doc_cache`). The worst case is a very rare trigram combination that generates many candidates before the safety cap kicks in.

**NOT queries**: `andnot` must materialize the entire left operand's doc ID set to subtract the right operand. For large indices without a query filter that limits the left set, this means iterating all matching doc IDs twice.

**Range queries without doc_values**: full chunk scan — every document in the index is examined.

### Disk vs CPU Tradeoffs

- **Index reading is CPU-bound**: decompressing `idx.bin` (zlib) is the dominant cost. With `zstd` available, decompress is 7× faster but the index is still compressed.
- **Doc chunk reading is I/O-bound**: 100K docs per chunk, each compressed independently. Random-access doc fetching (for a result page) loads entire chunks.
- **Doc values make aggregations fast**: binary search on numeric.bin is O(log N) CPU, no I/O beyond a single sequential file read of the doc_values file.

### Memory Usage Patterns

- **Build time**: `array('I')` for pending doc IDs (7× smaller than Python list), bytearray buffers for encoded postings (no Python object overhead), compressed doc chunks in `_pending_docs`.
- **Query time**: `_doc_cache` holds decompressed doc chunks (100K docs each, ~10–50 MB per chunk). `_posting_cache` holds decoded posting lists (bounded to 8,192 terms).
- **Aggregation**: streaming — only one chunk in memory at a time. Memory check every 50 chunks triggers `gc.collect()`.

### Cache Behavior

- **OS page cache**: index `idx.bin` files stay mmap'd. Repeated queries to the same bucket benefit from page cache hits.
- **`_mmap_cache`**: reused mmap handles across queries for the same file path.
- **`_hot_postings`**: never evicted, populated on first use of common terms.
- **`_posting_cache`**: LRU with eviction at 8,192 entries, keeping the most recently used posting lists.
- **`_doc_cache`**: holds decompressed doc chunks, no explicit eviction (relies on Python GC).

---

## Design Tradeoffs

### Why Trigram Index

Flatseek chose trigrams over n-gram segments (bigrams, 4-grams) and over standard inverted indexing (which would require tokenizing on whitespace):

**vs. bigrams**: bigrams have lower selectivity — more documents match any given bigram, leading to larger intermediate candidate sets. 'th' appears in most English words, while 'thr' is much more selective.

**vs. 4-grams**: 4-grams have higher selectivity but fewer occurrences per token. A 4-gram query like `*jup*` generates only 'jupi' as a candidate, which may not exist in any token. Trigrams balance selectivity with coverage.

**vs. tokenized index**: analytical datasets have long values (descriptions, addresses, names) where whitespace tokenization misses substring matches. A search for `*north jakarta*` as a phrase would fail if the address is stored as `jl. north jakarta no. 5`. Trigrams enable substring matching without tokenization.

The cost: trigram intersection generates candidates that must be post-filtered. This is the fundamental performance tradeoff — faster candidate generation at the cost of verification I/O.

### Why Columnar Doc Store

Analytical queries typically access a subset of columns, not entire records. Storing each 100K-doc chunk as a single JSON blob means fetching even one field requires decompressing and parsing the entire chunk.

Flatseek's approach: document chunks store the full denormalized record (so documents can be returned in one piece). Doc values (`dv/{field}/numeric.bin`, `dv/{field}/terms.bin`) provide columnar projection for the aggregation path — stats aggregations read only the field's doc_values file, not the document chunks.

This is a **late materialization** design: doc chunks are only fetched when full document content is needed (search results), not for aggregation-only queries.

### Why mmap Instead of In-Memory Structures

The alternative (load entire index into Python dicts) would require ~1.5–2× the index size in RAM for a large dataset. mmap provides:

- **Zero-copy reads**: the OS maps the file directly into the process address space. No `read()` syscall copying into a userspace buffer.
- **OS-managed caching**: the page cache handles hot index data without application-level cache eviction logic.
- **Memory capped at actual usage**: only the pages actually touched fault in. A 100 GB index does not consume 100 GB RAM if most buckets are never queried.

The tradeoff: sequential scans over mmap'd data are slightly slower than sequential scans over an in-memory `bytes` object (page faults on cold pages). But the memory efficiency wins for indices that don't fit in RAM.

### Known Limitations

1. **Wildcard latency scales with candidate set size**: `_verify_wildcard` loads one chunk per candidate. Queries generating many candidates (common short trigrams like 'the', 'and') are slow. Safety caps (500K candidates, 10K verified results) prevent worst-case O(N) verification, but even 500K candidate chunk loads can be expensive.

2. **Aggregation on non-keyword fields without doc_values**: full chunk scan (O(N)). For large datasets with frequent aggregation queries, build doc_values by running a "no-op" aggregation after build to populate `dv/` — or request this feature be added as a build-time option.

3. **Parallel build coordination**: the orchestrator must read all worker stats shards and merge them. For very large parallel builds (16+ workers on large files), the coordinator's merge step can become I/O-bound.

4. **No native server-mode clustering**: single-machine only. For distributed search, Flatseek indexes must be served via the API server (`flatseek serve`) which is a stateless FastAPI process. Horizontal scaling requires an external load balancer.

5. **Year-precision date range queries**: `created_at:[20260101 TO 20261231]` resolves by enumerating year tokens. This works for date column values that are stored as `YYYYMMDD` strings, but month/day resolution is lost — a query for "April 2026" cannot be answered precisely without additional implementation.

---

## Comparison with Other Engines

### vs. Elasticsearch

| Dimension | Elasticsearch | Flatseek |
|---|---|---|
| Architecture | Distributed cluster (JVM) | Single-process embeddable (Python) |
| Indexing | REST API + background tasks | Direct CSV/JSON → filesystem |
| Index format | Lucene segment files + translog | Custom trigram posting lists + doc chunks |
| Trigrams | Per-token n-grams (edge n-grams for autocomplete) | Position-independent 3-grams on full token |
| Query language | Lucene query DSL (full) | Lucene-style subset (AND/OR/NOT/RANGE/wildcard) |
| Aggregations | Facets, metric aggs, pipeline aggs | terms/stats/date_histogram/cardinality/histogram |
| Storage | Translog + segment files | zlib/zstd-compressed doc chunks + columnar doc_values |
| Memory | Off-heap (direct buffers) | mmap + OS page cache |
| Scaling | Horizontal (shards + replicas) | Single machine (index size limited by disk) |

**Correctness implications**: Elasticsearch uses Lucene'sposting list format with position data for phrase queries. Flatseek's trigram approach does not support phrase proximity (e.g., `"close account"` as an ordered sequence). Quoted phrases in Flatseek are indexed as the full lowercased value — matching is exact, not positional.

**Performance**: Elasticsearch's segment-based architecture allows fast segment merges and predicate pushdown. Flatseek's sequential bucket scan means every non-cached term lookup is O(N_bucket). For terms with large posting lists (common values), this is dominated by the decode cost.

### vs. Tantivy

| Dimension | Tantivy | Flatseek |
|---|---|---|
| Language | Rust (compiled) | Python (core) |
| Index | Inverted index (Lucene-style) | Trigram inverted index |
| Wildcard | Regex automaton (deterministic finite automaton) | Trigram candidate generation + verification |
| Query parser | Full Lucene query parser | Subset (no phrase proximity, no fuzzy) |
| Storage | Column-oriented segment store | zlib/zstd doc chunks + columnar doc_values |
| API | Rust library / REST | Python library / REST |
| Memory | Managed by process | mmap + OS page cache |

**Architectural difference**: Tantivy's wildcard implementation uses a DFA/NFA over the character sequence — it can evaluate `*jup*` against the token dictionary without generating candidates. Flatseek's trigram approach generates candidates first, then verifies. For dense wildcard patterns (many matches), Tantivy's approach is more efficient. For sparse patterns, trigram intersection can be faster because it leverages the posting list infrastructure.

**Build time**: Tantivy compiles the index in Rust, which is significantly faster than Python. Flatseek's Python-based indexing is the main bottleneck for very large datasets.

### vs. Typesense

| Dimension | Typesense | Flatseek |
|---|---|---|
| Architecture | Distributed server (C++) | Embeddable library (Python) |
| Indexing | REST API | Direct CSV → filesystem |
| Tokenization | Token-prefix index (field-wise) | Trigram index (character-wise) |
| Wildcard | Token-prefix search only (`prefix:*`) | Full infix wildcard (`*term*`) |
| Query language | Simpler (no AND/OR/NOT combinators) | Lucene-style (AND/OR/NOT/RANGE) |
| Aggregations | Facet counts + top-k | Full aggregations (terms, stats, cardinality, date_histogram) |
| Memory | In-memory index (HNSW for vectors) | mmap (full index on disk) |

**Key correctness difference**: Typesense does not support infix (substring) wildcard matching — `*term*` is not a valid Typesense query. Flatseek's trigram index explicitly enables this use case, at the cost of candidate verification overhead.

**Memory implications**: Typesense loads the full index into RAM for fast queries. Flatseek uses mmap, which means memory usage scales with actual query load, not index size. For large indices on machines with limited RAM, Flatseek uses less memory at rest.

---

## Implementation Reference

### Key File Locations

| Component | File |
|---|---|
| Index builder | `src/flatseek/core/builder.py` |
| Query engine | `src/flatseek/core/query_engine.py` |
| Query parser | `src/flatseek/core/query_parser.py` |
| Type detection | `src/flatseek/core/scanner.py` |
| Semantic classifier | `src/flatseek/core/classify.py` |
| Client library | `src/flatseek/client.py` |
| API server | `src/flatseek/api/main.py` |

### Critical Constants

```python
DOC_CHUNK_SIZE = 100_000        # docs per doc chunk
CHECKPOINT_ROWS = 50_000        # flush frequency
BUFFER_FLUSH_BYTES = 512_000   # per-prefix buffer flush threshold
CHECKPOINT_WRITE_BYTES = 8_024 # minimum buffer to write per checkpoint
SEGMENT_SWITCH_SIZE = 5_242_880 # 5 MB — switch to segment-append
MAX_TRIGRAM_TOKEN_LEN = 15     # tokens longer than 15 chars get no trigrams
_MAX_WILDCARD_CANDIDATES = 500_000
_MAX_VERIFIED_RESULTS = 10_000
ZLIB_LEVEL = 6                  # doc compression
ZLIB_LEVEL_IDX = 9             # index compression
```

### Term Hash Reproducibility

The same term must hash identically in builder and query engine:

```python
def term_hash(term: str) -> str:
    h = zlib.crc32(term.encode()) & 0xFFFFFFFF
    return f"{(h >> 24) & 0xFF:02x}/{(h >> 16) & 0xFF:02x}"
```

This CRC32-based bucket assignment is the single point of contact between the build and query phases. Any change to this function breaks existing indexes.
