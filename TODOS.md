# TODOs

Backlog of features / fixes that are NOT in the current release line.
Each item has: status, brief description, why, and how to approach.

---

## [ ] `.fsk` over HTTP Range — load .fsk directly from HF/S3 without full download

**Status**: Deferred (post v0.1.8)

**Why**: HuggingFace datasets have a hard cap of ~100k files per repo. A
flatseek index for millions of docs typically has 100k+ bin files alone
(hex-sharded `index/aa/bb/idx_w*.bin`), 10k+ doc chunks, docvalues, etc. —
well over the cap. The `.fsk` single-file format solves this: one packed
file, can be hosted on HF as a single release asset or in a dataset
repo's `resolve/main/foo.fsk` path.

**Problem**: Current remote storage adapters can't serve `.fsk` directly
because `FlatseekFileStorageAdapter` needs random-access `seek()` (via
`self._fh.seek(self._start + self._pos)` at `flatseek_file.py:862`), but
`S3StorageAdapter.open_read` (`storage.py:229`), `URLStorageAdapter.open_read`
(`storage.py:687`), and `VercelBlobStorageAdapter.open_read` (`storage.py:433`)
all return `io.BytesIO(self.read_bytes(rel_path))` — full file into RAM.
Defeats `.fsk`'s purpose for super-large indexes.

Also: `create_storage_adapter(path=...)` only auto-detects `.fsk` for
local paths (`storage.py:898-903` — needs `p.is_file()`).

**Approach**: Use HTTP `Range: bytes=<start>-<end>` requests natively
supported by HF (Cloudflare-backed) and S3:

1. Add a `RemoteFlatseekStorageAdapter` (or generalize the existing
   `FlatseekFileStorageAdapter` to take any seekable file-like).
2. On `_open()`, fetch `Range: bytes=0-4095` to read header + offset
   table.
3. Each per-file read becomes
   `GET /file.fsk Range: bytes=<offset>-<offset+size-1>` with the file's
   ETag for conditional requests.
4. Encrypt-on-the-fly per blob (manifest decrypts via the same Range path).

**Effort**: ~200 lines.

**Files to touch**:
- `src/flatseek/core/storage.py` — new class next to `URLStorageAdapter`
- `src/flatseek/flatseek_file.py` — generalize the seek path so it
  works against both a real file handle and a Range-backed file-like
- `src/flatseek/cli.py` — wire through `?storage-backend=url&storage-url=...fsk`
  detection in `create_storage_adapter`

**Trigger phrases** (for future search): "fsk url", "hf fsk", "single
file from remote", "stream fsk from huggingface", "remote flatseek",
"flatseek range request".

---

## [ ] Serve multiple `.fsk` files as multiple indices

**Status**: Deferred (post v0.1.8, mentioned during `.fsk`-over-HTTP discussion)

**Why**: Today the API server can serve **at most one `.fsk`** (via
`FLATSEEK_SINGLE_FILE` env var / `flatseek serve file.fsk`). Each
`IndexManager` instance pre-loads a single `_single_file_engine` and all
API routes resolve to that one engine regardless of the URL prefix.
Realistic use cases need multiple `.fsk` files served simultaneously:

- **Per-tenant archive**: each customer gets their own `.fsk` file
  (`acme.fsk`, `globex.fsk`, `initech.fsk`) served under their own
  index name on one server.
- **Time-sharded**: `events_2025q1.fsk`, `events_2025q2.fsk`, …
- **Per-region**: `apac.fsk`, `emea.fsk`, `us.fsk` — each `.fsk`
  becomes its own index.

**Problem with current architecture (`api/deps.py`)**:

1. Single-file mode is stored as a single slot, not a registry:
   ```
   deps.py:37-41:
     self._single_file_path = os.environ.get("FLATSEEK_SINGLE_FILE")
     self._single_file_engine = QueryEngine | None
     self._single_file_name = ...
   ```
   Only one engine for the whole process.

2. Routing is fixed: `get_engine` (`deps.py:174-176`) ignores the
   `index` arg and returns `_single_file_engine` for ANY name.

3. `list_indices` (`deps.py:273-275`) returns `[self._single_file_name]`
   if any — single-element list.

4. The CLI's `cmd_serve` (`cli.py:3510-3542`) sets the env var once
   and either derives a key for a single .fsk or for the data dir —
   no iteration over multiple files.

**Approach (analysis)**:

Three registration patterns are reasonable. Pattern A is recommended as
the default; B and C as opt-in for advanced configs.

### Pattern A: directory of `.fsk` (recommended)

```
data/fsk_bucket/
├── logs_2025_q1.fsk
├── logs_2025_q2.fsk
├── users.fsk
```

`flatseek serve --fsk-dir data/fsk_bucket/`
(or env: `FLATSEEK_FSK_DIR=data/fsk_bucket`)

On boot:
- Glob `**/*.fsk` (recursive), one engine per file.
- Index name = `pathlib.Path(fsk).relative_to(fsk_dir).with_suffix('')` —
  uses path-stem-without-extension. So `subdir/foo.fsk` → index `subdir/foo`,
  `foo.fsk` → `foo`. Deterministic, no name collisions within the directory.

Pros:
- Mirrors `FLATSEEK_DATA_DIR` mental model (one root, many children).
- Easy mental model — drop files in dir, restart server.
- Works with hot-reload via SIGHUP if needed later.

Cons:
- Requires restart to pick up new files (acceptable for backend).
- Path collision (two files with same name in different subdirs) is
  resolved by full relative path; users probably won't expect that.

### Pattern B: glob pattern

`FLATSEEK_FSK_GLOB='/var/lib/flatseek/archives/*.fsk'`

Same as A but uses `glob.glob` instead of directory walk. Useful when
files are scattered across multiple parent dirs.

### Pattern C: explicit registry JSON

`FLATSEEK_FSK_REGISTRY=/etc/flatseek/indexes.json`

```json
{
  "logs_q1":     "/abs/path/logs_2025_q1.fsk",
  "logs_q2":     "/abs/path/logs_2025_q2.fsk",
  "users_prod":  "/var/data/users.fsk",
  "users_test":  "/tmp/users_v2.fsk"
}
```

Pros:
- Explicit index naming (decoupled from filename).
- One file mapped to multiple index aliases.
- Different encryption keys per index possible (with extended registry).

Cons:
- Extra config file to maintain.

### Internal changes (all patterns share most code)

1. **Replace single slot with a registry** in `IndexManager.__init__`:
   ```python
   # Old: self._single_file_engine: QueryEngine | None
   # New:
   self._fsk_engines: dict[str, QueryEngine] = {}  # index_name → engine
   self._fsk_index_map: dict[str, Path] = {}      # index_name → .fsk path
   self._fsk_keys: dict[str, bytes] = {}          # index_name → enc_key (if known)
   ```

2. **Discovery**: new `_discover_fsk_dir(path)` method that walks the
   directory, registers each `.fsk` by relative stem, and (lazily) opens
   a `FlatseekFileStorageAdapter` for each. **Lazy engine open** — don't
   materialize `QueryEngine` until first `get_engine(index)` call. Saves
   memory when only a few indices are queried.

3. **`get_engine(index)` routing** (deps.py:165):
   ```python
   if self._fsk_engines:
       if index not in self._fsk_index_map:
           raise HTTPException(404, f"Index not found: {index}")
       eng = self._fsk_engines.get(index)
       if eng is None:
           fsk_path = self._fsk_index_map[index]
           key = self._fsk_keys.get(index)
           storage = FlatseekFileStorageAdapter(fsk_path, enc_key=key)
           eng = QueryEngine(".", storage=storage)
           if key: eng._enc_key = key
           self._fsk_engines[index] = eng
       return eng
   ```
   Single-file mode becomes a degenerate case (registry with one entry).

4. **`list_indices()`** returns sorted list of registered index names.
   No need to open any engine to enumerate — discovery already happened.

5. **Encryption handling**:
   - If `--passphrase` is given at startup AND only one `.fsk` is in
     the registry → assume it unlocks that one (back-compat behavior).
   - Otherwise → on first request to an encrypted index, prompt via the
     existing `_is_encrypted` and `x-index-password` header flow
     (already works in current code — just need to map to per-index keys).
   - For Pattern C registry, support per-index pre-registered keys.

6. **CLI wiring** (`cmd_serve` in `cli.py`):
   ```python
   if getattr(args, "fsk_dir", None) or os.environ.get("FLATSEEK_FSK_DIR"):
       fsk_dir = Path(args.fsk_dir or os.environ["FLATSEEK_FSK_DIR"])
       os.environ["FLATSEEK_FSK_DIR"] = str(fsk_dir.resolve())
       # IndexManager picks this up on import
   ```
   Back-compat: when both `--fsk-dir` and `--data` are set, prefer
   `--fsk-dir`; when only `--data` is set, fall back to existing
   directory-mode behavior.

7. **Memory profile**:
   - `IndexManager` keeps `dict[str, Path]` and `dict[str, QueryEngine]` —
     ~hundreds of bytes per index regardless of size.
   - Each `QueryEngine` opens a file handle on first use → ~ few KB.
   - For N=100 indices: ~10-50 KB total in `IndexManager` overhead.
   - **Bottleneck** is `QueryEngine`'s per-file caches when actually
     serving traffic. Those are bounded; nothing to do here.

**Effort**: ~250 lines:
- ~50 lines discovery + registry in `IndexManager`
- ~50 lines routing change in `get_engine`
- ~30 lines CLI wiring in `cmd_serve`
- ~30 lines `list_indices` update
- ~50 lines tests (server starts with N .fsk, each index resolves)
- ~40 lines docs/CLI help

**Files to touch**:
- `src/flatseek/api/deps.py` — registry + routing
- `src/flatseek/cli.py` — `cmd_serve` flag (`--fsk-dir`) + env var
- `src/flatseek/api/main.py` — possibly extract a list helper
- `tests/test_api_multi_fsk.py` — new test file
- `docs/cli.md` — `--fsk-dir` section

**Trigger phrases** (for future search): "multi fsk", "multiple flatseek",
"per-tenant fsk", "fsk directory", "FLATSEEK_FSK_DIR", "serve multiple",
"index registry".

---

## [ ] Time-sharded `.fsk` — long date-range query across many daily `.fsk` files

**Status**: Deferred (post v0.1.8)

**Why**: Logs/trailing data is often split across many files by time —
e.g., `logs/2025-01-15.fsk`, `logs/2025-01-16.fsk`, ..., one per day.
All files share the same schema, only the `created_at` column differs.
Operations needs to query long date ranges (e.g., "errors between
2025-11-01 and 2025-11-30") and have results merged across all relevant
files. The current architecture has no support for this:

- Each `.fsk` is a separate `QueryEngine` (one logical index per file).
- `search()` opens a single engine and queries one shard.
- No "federated" / "sharded" concept — no metadata about which `.fsk`
  covers which date range, so a query can't prune irrelevant shards.
- No result-merge coordinator across shards with stable pagination.

Real use case from user: "trailing log disebar jadi beberapa file fsk
dengan struktur sama, hanya beda di created_at. Butuh long date range
query, filenya per hari. Folder backup manageable, tetap bisa
disearch lintas file."

**Existing infra to reuse**:

- `QueryEngine._sub_engines` mechanism (line 466 of `query_engine.py`)
  already implements multi-index fan-out. Each sub-engine is queried
  independently; `_multi_paginate` (line 1839) merges results and tags
  each doc with its source `_index`.
- `_discover_index_dirs` (line 242) walks a directory for sub-indexes
  with `index/` subdir. Needs extension for `.fsk` files.
- `query_parser` already supports range queries on numeric/datetime
  fields via `execute(node, qe)` (used in `cmd_export` etc).

→ The plumbing is 70% there. What's missing is **shard metadata** (so
we can prune by date), **`.fsk` as a sub-engine source** (currently
only directory subdirs work), and **stable cross-shard pagination**.

**Approach (analysis)**:

### Step 1: extend `.fsk` manifest with shard metadata

Add an optional `_shard_meta` field to the manifest section written by
`FlatseekPacker._build_manifest_bytes` (`flatseek_file.py:223`):

```json
{
  "stats": {...},
  "columns": {...},
  "_shard_meta": {
    "name":       "logs-2025-11-15",     // human-readable shard id
    "time_field": "created_at",          // which column is the date
    "min_time":   "2025-11-15T00:00:00Z",
    "max_time":   "2025-11-15T23:59:59Z"
  },
  "_encryption_b64": "..."
}
```

`FlatseekPacker.__init__` already takes an `enc_key`; extend to also
accept `shard_meta: dict | None = None`. `cmd_pack` in `cli.py:3593` gets
new flags `--shard-name`, `--time-field`, `--min-time`, `--max-time`.

The `_parse_manifest` path in `FlatseekFileStorageAdapter._open`
(`flatseek_file.py:1091`) is extended to expose this on the adapter:
`adapter._shard_meta` (or via `getattr` for back-compat when missing).

### Step 2: extend `_discover_index_dirs` to find `.fsk` files

Modify `_discover_index_dirs` to also walk for `.fsk` files
(alongside `*/index` subdirs). Or add a new
`_discover_fsk_shards(root)` that returns `list[Path]` of all `.fsk`
files in a directory tree. The new `TimeShardedIndex` (step 3) uses
this; back-compat for directory-mode shards is preserved.

### Step 3: a new `TimeShardedIndex` coordinator

New class `flatseek.core.sharded_index.TimeShardedIndex` (or extend
`QueryEngine.__init__` to detect a sharded dir). Constructor:

```python
class ShardedSubEngine:
    engine:    QueryEngine       # already-initialized QE for this .fsk
    path:      Path              # the .fsk file path
    name:      str               # shard name (from _shard_meta.name or stem)
    time_field: str | None
    min_time:  datetime | None
    max_time:  datetime | None

class TimeShardedIndex:
    def __init__(self, fsk_dir: Path):
        # walk dir, build one ShardedSubEngine per .fsk (lazy engine open)
        self._shards: list[ShardedSubEngine] = []

    def _shards_overlapping(self, t_from, t_to) -> list[ShardedSubEngine]:
        """Prune shards whose [min_time, max_time] doesn't overlap [t_from, t_to].
        Shards with no time metadata are kept (don't know → must check)."""
        return [s for s in self._shards
                if s.min_time is None or s.max_time is None
                or not (s.max_time < t_from or s.min_time > t_to)]

    def search(self, q, column=None, t_from=None, t_to=None,
               page=0, page_size=20):
        shards = self._shards_overlapping(t_from, t_to)
        # fan out to each shard (parallel via ThreadPoolExecutor)
        # merge + dedupe by (shard_name, doc_id)
        # stable pagination across the union
```

The `t_from`/`t_to` filter is enforced **before** opening engines for
irrelevant shards, so a "Nov 1-30" query only opens 30 of 365 daily
shards, not all of them.

### Step 4: handle doc_id collisions across shards

Two viable models. Pick (a) for simplicity; (b) is more flexible.

#### (a) Per-shard doc_id range (recommended)

At pack time, accept `--shard-id N` and rebase all doc_ids to
`[N * 1B, N * 1B + 1M)`. Result: no collisions across shards. Merging
is `concat + global sort + slice for page`. The user's existing
`doc_id asli` preference (kept from `flatseek slice`) is preserved
*within* a shard but documented to be shard-local. Shard N is
identifiable from doc_id: `N = doc_id // 1B`.

Pros: trivial merge, no coordination needed
Cons: per-shard doc_id range cap (~1B rows/shard)

#### (b) Composite (shard_id, local_doc_id) id

Each shard keeps its local doc_ids starting at 0. Internally, store
results as `(shard_idx, local_doc_id)`. Expose `shard_id` and
`local_doc_id` as separate response fields. Original local doc_id is
preserved (matches `flatseek slice` exactly).

Pros: no doc_id range cap, shard-agnostic
Cons: more bookkeeping, response schema change

Recommendation: **(a)** — most production shards won't exceed 1B docs,
and the simplicity benefit is large.

### Step 5: query API surface

Existing API at `/{index}/_search` already accepts `q`, `from`, `to`,
`page`, `page_size`. For a sharded index, `from`/`to` are interpreted
as **time range** (in addition to their existing pagination meaning —
this needs disambiguation, possibly rename to `time_from`/`time_to`).

API:
```
POST /logs/_search
{
  "q":       "level:ERROR",
  "time_from": "2025-11-01T00:00:00Z",
  "time_to":   "2025-11-30T23:59:59Z",
  "page":      0,
  "page_size": 20
}
```

`time_from` / `time_to` trigger the shard-prune step. If omitted, all
shards are queried (full scan — could be slow for high-cardinality
deployments; emit a warning when >100 shards are scanned without a
time filter).

### Step 6: cross-shard result merge

Two strategies:

#### Round-robin (simple)

For each shard (sorted by name), take the first K results. Concatenate.
Pros: simple, stable order across calls.
Cons: pagination can be uneven if shard sizes differ a lot.

#### Global rank (correct)

Per-shard `total = N_shard`. Compute global `total = sum(N_shard)`.
For page P of size S: collect first `S * (P+1)` results from each
shard (proportional to N_shard / total), sort, slice, dedupe.

Pros: even distribution
Cons: re-queries per page, more complex

Recommendation: start with **round-robin** for v1; revisit if uneven
shard sizes cause UX issues. Add `_index` field to each result so
clients can see which shard a result came from (already done by
`_multi_paginate`).

### Memory + performance profile

- **Per-shard engine**: opens `.fsk` file handle on first query (~few KB)
- **N shards, fan-out**: ThreadPoolExecutor with N workers; bounded by
  page_size, not total result count (results stream as they arrive)
- **Time pruning**: zero cost — just filter on `min_time`/`max_time`
  in-memory before opening engines
- **Hot path**: typical query touches 1-30 shards; full-scan (no time
  filter) on 365 daily shards = 365 engines open concurrently. Need
  to cap concurrency (suggest 8-16 worker pool) to avoid file-descriptor
  exhaustion

### Effort estimate

~400-500 lines:
- ~30 lines manifest extension (`_build_manifest_bytes` + `_parse_manifest`)
- ~30 lines `cmd_pack` flags + `FlatseekPacker.__init__` parameter
- ~80 lines `_discover_fsk_shards` + `ShardedSubEngine` class
- ~150 lines `TimeShardedIndex.search`/`search_and`/`_multi_paginate` +
  shard pruning + thread pool
- ~80 lines API wiring (`routes/search.py` `time_from`/`time_to` params)
- ~50 lines `flatseek serve` flag (`--fsk-dir` for the sharded case)
- ~80 lines tests + docs

### Files to touch

- `src/flatseek/flatseek_file.py` — manifest field + parser
- `src/flatseek/core/query_engine.py` — small extension to existing
  `_sub_engines` machinery (or new `TimeShardedIndex` class that
  composes `QueryEngine` instances)
- `src/flatseek/core/sharded_index.py` — **new** coordinator class
- `src/flatseek/cli.py` — `cmd_pack` shard flags, `cmd_serve` dir flag
- `src/flatseek/api/routes/search.py` — `time_from`/`time_to` plumbing
- `tests/test_sharded_index.py` — new test file
- `docs/cli.md` + `docs/api.md` — examples

### Trigger phrases (for future search)

"time shard", "daily fsk", "log archive", "date range query", "long
range", "shard pruning", "TimeShardedIndex", "sharded index",
"_shard_meta", "min_time max_time", "logs per day", "trailing data",
"cross-shard pagination".

---

