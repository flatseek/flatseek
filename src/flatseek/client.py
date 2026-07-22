"""
Flatseek — dual-mode client for Flatseek trigram index.

Usage (API mode):
    from flatseek import Flatseek

    client = Flatseek("http://localhost:8000")
    result = client.search(index="solana_txs", q="program:raydium AND amount:>1000000", size=10)

Usage (direct mode):
    from flatseek import Flatseek

    qe = Flatseek("./data")                          # single index
    qe = Flatseek("./data", index="solana_txs")      # named sub-index
    result = qe.search(q="program:raydium AND signer:*7xMg*", size=10)
"""

import os
import json
import os
import time
import hashlib
import math
import fnmatch
from typing import Any
from collections import Counter

import httpx


# ─── Response wrappers ────────────────────────────────────────────────────────

class Response:
    """Elasticsearch-like response wrapper."""

    def __init__(self, data: dict[str, Any]):
        self._data = data

    @property
    def hits(self):
        return self._data.get("hits", {})

    @property
    def total(self):
        return self.hits.get("total", 0)

    @property
    def docs(self):
        return [hit["_source"] for hit in self.hits.get("hits", [])]

    def __getitem__(self, key):
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __repr__(self):
        return f"<Response total={self.total}>"


class CountResponse:
    """Count response wrapper."""

    def __init__(self, data: dict[str, Any]):
        self._data = data

    @property
    def count(self):
        return self._data.get("count", 0)

    def __repr__(self):
        return f"<CountResponse count={self.count}>"


class AggsResponse:
    """Aggregations response wrapper."""

    def __init__(self, data: dict[str, Any]):
        self._data = data

    @property
    def total(self):
        return self._data.get("hits", {}).get("total", 0)

    @property
    def aggs(self):
        return self._data.get("aggregations", {})

    def __getitem__(self, key):
        return self._data[key]

    def __repr__(self):
        return f"<AggsResponse total={self.total}>"


# ─── Bounded cardinality sketch ───────────────────────────────────────────────

class _CardinalitySketch:
    """Fixed-memory cardinality estimator using a sparse bitmap.

    Uses m=65536 6-bit registers → 48KB bitmap.
    Expected error: ~1.6% for large cardinalities.
    """

    __slots__ = ("m", "_bitmap", "_zero_count", "_max_reg")

    def __init__(self, m: int = 65536):
        self.m = m
        self._bitmap = [0] * m
        self._zero_count = m
        self._max_reg = 0

    def add(self, value: Any) -> None:
        if value is None:
            return
        h = hashlib.sha256(str(value).encode()).digest()[:8]
        hash_u32 = int.from_bytes(h[:4], "little")
        reg_idx = hash_u32 % self.m
        bits = h[4] & 0x3F

        old = (self._bitmap[reg_idx >> 4] >> ((reg_idx & 0xF) * 4)) & 0xF
        if old == 0:
            self._zero_count -= 1
        if bits > self._max_reg:
            self._max_reg = bits
        shift = (reg_idx & 0xF) * 4
        self._bitmap[reg_idx >> 4] = (self._bitmap[reg_idx >> 4] & ~(0xF << shift)) | (bits << shift)

    def count(self) -> int:
        if self._zero_count > 0:
            return int(self.m * math.log(self.m / max(1, self._zero_count)))
        alpha = 0.6735 if self.m == 65536 else (0.7071 if self.m == 32768 else 0.7213)
        raw_estimate = alpha * self.m * self.m / sum(
            1.0 / (1 << max(1, ((self._bitmap[i >> 4] >> ((i & 0xF) * 4)) & 0xF)))
            for i in range(self.m)
        )
        return int(min(raw_estimate, 2 ** (self._max_reg + 1)))


# ─── Helpers ───────────────────────────────────────────────────────────────────

def _parse_date(value: str) -> str | None:
    if not value:
        return None
    value = str(value).replace("-", "").replace("/", "")
    if len(value) == 8 and value.isdigit():
        return value
    elif len(value) == 10:
        return value[:4] + value[5:7] + value[8:10]
    return value


def _to_number(val) -> int | float | None:
    try:
        s = str(val)
        if len(s) >= 4:
            return int(s[:4])
        return float(s)
    except Exception:
        return None


# ─── Direct mode (QueryEngine wrapper) ────────────────────────────────────────

class _DirectEngine:
    """Direct index access via QueryEngine."""

    # Memory limits for aggregation
    _MAX_TERMS_UNIQUE = 1_000_000
    _MEM_CHECK_INTERVAL = 50

    def __init__(self, data_dir: str, index: str | None = None):
        from flatseek.core.query_engine import QueryEngine

        self._root_dir = data_dir  # preserved for _reload_engine
        self._index = index
        if index:
            self._engine = QueryEngine(os.path.join(data_dir, index))
        else:
            self._engine = QueryEngine(data_dir)

    # ─── Query ────────────────────────────────────────────────────────────
    def query(self, q: str, page: int = 0, page_size: int = 20) -> dict:
        result = self._engine.query(q, page=page, page_size=page_size)
        return result

    def search(self, q: str, size: int = 10, page: int = 0) -> Response:
        result = self.query(q, page=page, page_size=size)
        return Response({
            "hits": {
                "total": result["total"],
                "hits": [{"_id": i, "_source": doc, "_score": 1.0}
                         for i, doc in enumerate(result["results"])],
            }
        })

    def count(self, q: str | None = None) -> CountResponse:
        if q:
            result = self.query(q, page=0, page_size=0)
            return CountResponse({"count": result["total"]})
        stats = self._engine.stats
        return CountResponse({"count": stats.get("total_docs", 0)})

    def stats(self) -> dict:
        return self._engine.stats

    def columns(self) -> dict:
        return self._engine.columns()

    # ─── Aggregation (delegates to core) ────────────────────────────────────
    def aggregate(self, q: str | None = None, aggs: dict | None = None,
                  size: int = 10) -> AggsResponse:
        """Execute aggregations with optional query filter.

        Delegates to QueryEngine.aggregate() in core for centralized logic.
        """
        result = self._engine.aggregate(q=q, aggs=aggs, size=size)
        return AggsResponse(result)

    # ─── ID generation ───────────────────────────────────────────────────────

    def _generate_ulid(self) -> str:
        """Generate a ULID string (26 chars, Crockford Base32).

        Time-sortable, globally unique without coordination.
        Compatible with Elasticsearch _id format.
        """
        import time, random
        _ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
        ts_ms = int(time.time() * 1000)
        rand_bits = random.getrandbits(80)
        value = (ts_ms << 80) | rand_bits
        chars = []
        for i in range(26):
            idx = (value >> (125 - i * 5)) & 31
            chars.append(_ALPHABET[idx])
        return "".join(chars)

    # ─── Write operations (require --id-field index) ─────────────────────────

    def _id_field(self) -> str | None:
        """Return the id_field for this index, or None if not set."""
        return self._engine.stats.get("_id_field")

    def _require_id_field(self):
        if not self._id_field():
            raise ValueError(
                "Index was built without --id-field. "
                "Write operations require an id-field to be set at build time."
            )

    def _next_doc_id(self) -> int:
        """Atomically read and increment .next_doc_id counter."""
        import fcntl
        index_dir = self._engine.data_dir
        counter_path = os.path.join(index_dir, ".next_doc_id")
        lock_path = os.path.join(index_dir, ".doc_id.lock")
        os.makedirs(os.path.dirname(lock_path), exist_ok=True)
        with open(lock_path, "a") as lockf:
            fcntl.flock(lockf.fileno(), fcntl.LOCK_EX)
            try:
                if os.path.exists(counter_path):
                    try:
                        total_docs = int(open(counter_path).read().strip())
                    except (ValueError, IOError):
                        total_docs = 0
                else:
                    stats = self._engine.stats
                    total_docs = stats.get("total_docs", 0)
                with open(counter_path, "w") as cf:
                    cf.write(str(total_docs + 1))
                return total_docs
            finally:
                fcntl.flock(lockf.fileno(), fcntl.LOCK_UN)

    def _reload_engine(self):
        """Reload the QueryEngine to pick up on-disk changes (new segments, tombstones)."""
        from flatseek.core.query_engine import QueryEngine
        if self._index:
            self._engine = QueryEngine(os.path.join(self._root_dir, self._index))
        else:
            self._engine = QueryEngine(self._root_dir)

    def _normalize_doc(self, doc: dict) -> tuple[dict[str, str], list[str]]:
        """Normalize doc values to strings (as IndexBuilder expects)."""
        normalized = {}
        for k, v in doc.items():
            if isinstance(v, (dict, list)):
                normalized[k] = json.dumps(v)
            else:
                normalized[k] = str(v)
        return normalized, list(doc.keys())

    def index(self, id: str, doc: dict) -> dict:
        """Insert or replace a document by its natural key (id_field).

        Requires the index was built with --id-field.
        Uses optimistic locking: retries on concurrent modification.
        The provided ``id`` becomes the document's ``_id`` field.

        Returns:
            {"result": "created"|"updated", "doc_id": id, "_id": str, "new_doc_id": int}
        """
        self._require_id_field()
        id_field = self._id_field()

        engine = self._engine
        tombstones = engine._tombstones
        if tombstones is None:
            raise ValueError("Index does not support update operations.")

        # Optimistic locking: up to 3 retries
        is_update = False
        existing_doc_id = None
        for _attempt in range(3):
            existing_doc_id = tombstones.get_doc_id_for_field(id_field, id)
            is_update = existing_doc_id is not None
            existing_ver = tombstones.get_version(existing_doc_id) if existing_doc_id else 0

            if is_update:
                count, conflicts = tombstones.mark_deleted(
                    [existing_doc_id], expected_versions={existing_doc_id: existing_ver}
                )
                if conflicts:
                    self._reload_engine()
                    continue
            break

        new_doc_id = self._next_doc_id()
        # Copy doc and inject _id field (like Elasticsearch)
        doc = dict(doc)
        doc["_id"] = id
        self._add_doc(new_doc_id, doc)

        # Update id mapping
        self._reload_engine()
        self._engine._tombstones.set_doc_id_for_field(id_field, id, new_doc_id)

        return {"result": "updated" if is_update else "created",
                "doc_id": id, "_id": id, "new_doc_id": new_doc_id}

    def insert(self, doc: dict) -> dict:
        """Insert a document (auto-generates a ULID _id).

        The document receives the next sequential internal doc_id and a
        globally unique _id (ULID string, like Elasticsearch).

        Returns:
            {"result": "created", "doc_id": int, "_id": str}
        """
        new_doc_id = self._next_doc_id()
        ulid = self._generate_ulid()
        doc = dict(doc)  # shallow copy so we don't mutate caller's dict
        doc["_id"] = ulid
        self._add_doc(new_doc_id, doc)

        # Register natural key in tombstone mapping so index/update/delete can find this doc
        id_field = self._id_field()
        if id_field and id_field in doc:
            self._engine._tombstones.set_doc_id_for_field(
                id_field, str(doc[id_field]), new_doc_id
            )

        self._reload_engine()
        return {"result": "created", "doc_id": new_doc_id, "_id": ulid}

    def update(self, id: str, fields: dict) -> dict:
        """Partially update a document by its natural key.

        Merges ``fields`` into the existing document (existing fields preserved unless overwritten).
        Requires --id-field at build time.

        Returns:
            {"result": "updated"|"not_found", "doc_id": id, "new_doc_id": int}
        """
        self._require_id_field()
        id_field = self._id_field()

        engine = self._engine
        tombstones = engine._tombstones
        if tombstones is None:
            raise ValueError("Index does not support update operations.")

        existing_doc_id = tombstones.get_doc_id_for_field(id_field, id)
        if existing_doc_id is None:
            return {"result": "not_found", "doc_id": id, "new_doc_id": None}

        # Fetch existing doc BEFORE tombstoning — fetch uses alive_ids filter which
        # excludes newly-tombstoned docs, so we must read first.
        existing = None
        for doc in engine._fetch_docs([existing_doc_id]):
            existing = doc
            break

        # Tombstone old doc
        tombstones.mark_deleted([existing_doc_id])

        merged = {k: v for k, v in (existing or {}).items() if not k.startswith("_")}
        merged = {k: v for k, v in merged.items() if k != "_id"}
        for k, v in fields.items():
            if isinstance(v, (dict, list)):
                merged[k] = json.dumps(v)
            else:
                merged[k] = str(v)

        new_doc_id = self._next_doc_id()
        self._add_doc(new_doc_id, merged)

        # Update id mapping
        self._reload_engine()
        self._engine._tombstones.set_doc_id_for_field(id_field, id, new_doc_id)

        return {"result": "updated", "doc_id": id, "new_doc_id": new_doc_id}

    def delete(self, id: str) -> dict:
        """Delete a document by its natural key value.

        Requires --id-field at build time.

        Returns:
            {"result": "deleted"|"not_found", "doc_id": id}
        """
        self._require_id_field()
        id_field = self._id_field()

        engine = self._engine
        tombstones = engine._tombstones
        if tombstones is None:
            raise ValueError("Index does not support delete operations.")

        existing_doc_id = tombstones.get_doc_id_for_field(id_field, id)
        if existing_doc_id is None:
            return {"result": "not_found", "doc_id": id}

        tombstones.mark_deleted([existing_doc_id])
        tombstones.remove_doc_id_for_field(id_field, id)
        return {"result": "deleted", "doc_id": id}

    def delete_query(self, q: str) -> dict:
        """Delete all documents matching a Lucene query.

        Requires --id-field at build time.

        Returns:
            {"deleted": int, "doc_ids": [int]}
        """
        self._require_id_field()
        id_field = self._id_field()

        result = self._engine.query(q, page=0, page_size=100_000)
        docs = result.get("results", [])
        if not docs:
            return {"deleted": 0, "doc_ids": []}

        tombstones = self._engine._tombstones

        # For each doc, figure out the tombstone doc_id:
        # - Built docs: _id field is integer doc_id (was stored as str(total_docs) at build time)
        # - Inserted docs: _id field is ULID string; tombstone has id_field → doc_id mapping
        # - Built docs have id_field set; inserted docs have _id but id_field might be unset
        int_doc_ids = []
        for doc in docs:
            doc_id_val = doc.get("_id")
            if isinstance(doc_id_val, int):
                int_doc_ids.append(doc_id_val)
            elif isinstance(doc_id_val, str) and doc_id_val.isdigit():
                int_doc_ids.append(int(doc_id_val))
            else:
                # ULID or other string — look up the natural key value first,
                # then use that to find the tombstone mapping (the mapping is keyed
                # by id_field name + natural key value, not by ULID).
                if id_field:
                    natural_val = doc.get(id_field)
                    if natural_val:
                        mapped = tombstones.get_doc_id_for_field(id_field, str(natural_val))
                        if mapped is not None:
                            int_doc_ids.append(mapped)

        if int_doc_ids:
            tombstones.mark_deleted(int_doc_ids)

        # Remove id mappings
        for doc in docs:
            natural_id = doc.get(id_field)
            if natural_id:
                tombstones.remove_doc_id_for_field(id_field, str(natural_id))

        return {"deleted": len(int_doc_ids), "doc_ids": int_doc_ids}

    def bulk(self, operations: list[dict]) -> dict:
        """Execute a list of bulk operations (ES-compatible format).

        Each operation is a dict with one key (the action) and a "_id" or "doc".
        Supported actions:
          - ``{"index": {"_id": "x"}, "doc": {...}}``  → upsert (insert or replace)
          - ``{"create": {"_id": "x"}, "doc": {...}}``  → insert only, fails if exists
          - ``{"update": {"_id": "x"}, "doc": {...}}``  → partial update (merge)
          - ``{"delete": {"_id": "x"}}``                → delete by id

        Returns:
            {"indexed": int, "updated": int, "deleted": int, "errors": list}
        """
        self._require_id_field()
        id_field = self._id_field()
        indexed = updated = deleted = 0
        errors = []

        i = 0
        while i < len(operations):
            op = operations[i]
            if not op:
                i += 1
                continue

            action = None
            meta = None
            for key in ("index", "create", "update", "delete"):
                if key in op:
                    action = key
                    meta = op[key]
                    break

            if action is None:
                errors.append({"type": "action_error", "op": op})
                i += 1
                continue

            if action in ("index", "create", "update"):
                if i + 1 >= len(operations):
                    errors.append({"type": "missing_body", "op": op})
                    i += 1
                    continue
                body = operations[i + 1]
                i += 2  # consume op + body
            elif action == "delete":
                body = None
                i += 1
            else:
                # no-body action
                body = None
                i += 1

            doc_id = meta.get("_id") if meta else None

            try:
                if action == "index":
                    if not doc_id:
                        self.insert(body or {})
                        indexed += 1
                    else:
                        r = self.index(doc_id, body or {})
                        if r["result"] == "created":
                            indexed += 1
                        else:
                            updated += 1
                elif action == "create":
                    if not doc_id:
                        errors.append({"type": "missing_id", "op": op})
                        continue  # body already consumed above
                    existing = self._engine._tombstones.get_doc_id_for_field(id_field, doc_id)
                    if existing is not None:
                        errors.append({"type": "conflict", "doc_id": doc_id})
                        i += 2  # skip duplicate op + its body
                        continue
                    new_doc_id = self._next_doc_id()
                    doc_with_id = dict(body or {})
                    doc_with_id["_id"] = doc_id
                    self._add_doc(new_doc_id, doc_with_id)
                    self._reload_engine()
                    self._engine._tombstones.set_doc_id_for_field(id_field, doc_id, new_doc_id)
                    indexed += 1
                elif action == "update":
                    r = self.update(doc_id, body or {})
                    if r["result"] == "not_found":
                        errors.append({"type": "not_found", "doc_id": doc_id})
                    else:
                        updated += 1
                elif action == "delete":
                    r = self.delete(doc_id)
                    if r["result"] == "deleted":
                        deleted += 1
                    else:
                        errors.append({"type": "not_found", "doc_id": doc_id})
            except Exception as e:
                errors.append({"type": "error", "doc_id": doc_id, "message": str(e)})

        return {"indexed": indexed, "updated": updated, "deleted": deleted, "errors": errors}

    def _add_doc(self, doc_id: int, doc: dict):
        """Low-level: add a single document via IndexBuilder and finalize."""
        from flatseek.core.builder import IndexBuilder
        index_dir = self._engine.data_dir
        normalized, fields = self._normalize_doc(doc)
        builder = IndexBuilder(
            index_dir,
            column_map={},
            start_doc_id=doc_id,
            dataset=self._index or os.path.basename(index_dir),
            checkpoint_cb=None,
            delimiter=",",
            columns=None,
            worker_id=None,
            dedup_fields=None,
            doc_id_end=None,
            daemon=False,
            id_field=self._id_field(),
        )
        builder.add_row(normalized, fields, file_rel=None, col_schema=None)
        builder.finalize()


# ─── API mode (HTTP client) ────────────────────────────────────────────────────

class _HTTPClient:
    """HTTP API client (existing behavior)."""

    def __init__(self, hosts: str | list[str], **kwargs):
        if isinstance(hosts, str):
            hosts = [hosts]
        self._hosts = hosts
        self._kwargs = kwargs
        self._client = httpx.Client(base_url=hosts[0], **kwargs)

    def _request(self, method: str, path: str, **kwargs) -> dict[str, Any]:
        url = path.lstrip("/")
        if method.upper() == "GET":
            response = self._client.get(url, **kwargs)
        elif method.upper() == "POST":
            response = self._client.post(url, **kwargs)
        elif method.upper() == "PUT":
            response = self._client.put(url, **kwargs)
        elif method.upper() == "DELETE":
            response = self._client.delete(url, **kwargs)
        else:
            raise ValueError(f"Unsupported method: {method}")
        response.raise_for_status()
        return response.json()

    def search(self, index: str | None = None, body: dict | None = None,
               q: str | None = None, size: int = 10, from_: int = 0, **kw) -> Response:
        if body is None:
            body = {}
        if "query" in body:
            q = body.pop("query")
        if "size" in body:
            size = body.pop("size")
        if "from" in body:
            from_ = body.pop("from")
        params = {"size": size, "from": from_}
        if q:
            params["q"] = q
        for k, v in body.items():
            if k not in params:
                params[k] = v
        path = f"/{index}/_search" if index else "/_search"
        if q or body:
            return Response(self._request("GET", path, params=params))
        else:
            return Response(self._request("POST", path, json=params))

    def count(self, index: str | None = None, body: dict | None = None,
              q: str | None = None) -> CountResponse:
        params = {}
        if q:
            params["q"] = q
        path = f"/{index}/_count" if index else "/_count"
        if q or body:
            data = self._request("GET", path, params=params)
        else:
            data = self._request("POST", path, json=body or {})
        return CountResponse(data)

    def aggregate(self, index: str, body: dict, q: str | None = None) -> dict:
        if q:
            body = {"query": q, **body}
        return self._request("POST", f"/{index}/_aggregate", json=body)

    def get(self, index: str, id: int | str) -> dict:
        return self._request("GET", f"/{index}/_doc/{id}").get("_source", {})

    def index(self, index: str, body: dict, id: str | None = None) -> dict:
        if id:
            return self._request("PUT", f"/{index}/_doc/{id}", json=body)
        return self._request("POST", f"/{index}/_doc", json=body)

    def bulk(self, operations: list[dict]) -> dict:
        if operations:
            return self._request("POST", f"/{operations[0].get('_index', '')}/_bulk", json=operations)
        return {"indexed": 0, "errors": None}

    def bulk_insert(self, index: str, docs: list[dict]) -> dict:
        return self._request("POST", f"/{index}/_bulk", json=docs)

    def bulk_search(self, index: str, queries: list[str], size: int = 10) -> list[dict]:
        request = [{"q": q, "size": size} for q in queries]
        return self._request("POST", f"/{index}/_msearch", json=request).get("responses", [])

    def flush(self, index: str) -> dict:
        return self._request("POST", f"/{index}/_flush")

    def delete_index(self, index: str) -> dict:
        return self._request("DELETE", f"/{index}")

    def indices(self) -> list[str]:
        return self._request("GET", "/_indices").get("indices", [])

    def stats(self, index: str | None = None) -> dict:
        path = f"/{index}/_stats" if index else "/_stats"
        return self._request("GET", path)

    def mapping(self, index: str) -> dict:
        return self._request("GET", f"/{index}/_mapping")

    def delete(self, index: str) -> dict:
        return self._request("DELETE", f"/{index}")

    def cluster(self) -> dict:
        return self._request("GET", "/_cluster/health")

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# ─── Flatseek dual-mode class ─────────────────────────────────────────────────

class Flatseek:
    """Dual-mode Flatseek client.

    Detects mode from first argument:
      - URL (http:// or https://) → API mode
      - Local path → direct mode

    API mode:
        client = Flatseek("http://localhost:8000")
        result = client.search(index="solana_txs", q="program:raydium AND amount:>1000000")

    Direct mode:
        qe = Flatseek("./data")
        result = qe.search(q="program:raydium AND signer:*7xMg*", size=10)

        # Or with explicit index name
        qe = Flatseek("./data", index="solana_txs")
        result = qe.search(q="amount:>500000")

        # Aggregation (direct mode)
        result = qe.aggregate(q="status:active AND country:ID", aggs={
            "by_campaign": {"terms": {"field": "campaign", "size": 10}},
            "bid_stats": {"stats": {"field": "bid"}}
        })
    """

    def __init__(
        self,
        host_or_path: str,
        index: str | None = None,
        **kwargs,
    ):
        """Initialize Flatseek client.

        Args:
            host_or_path: URL string or local path to index directory.
            index: Sub-index name (direct mode only). For single-index mode,
                   omit or pass None.
            **kwargs: Additional options (httpx options in API mode).
        """
        self._mode: str | None = None
        self._engine: _DirectEngine | None = None
        self._client: _HTTPClient | None = None
        self._index = index

        # Detect mode
        if host_or_path.startswith(("http://", "https://")):
            self._mode = "api"
            self._client = _HTTPClient(host_or_path, **kwargs)
        elif os.path.isdir(host_or_path):
            self._mode = "direct"
            self._engine = _DirectEngine(host_or_path, index)
        else:
            raise ValueError(
                f"Invalid argument: {host_or_path!r}. "
                "Expected URL (http://...) or local path to index directory."
            )

    # ─── Proxy unhandled methods to underlying engine/client ─────────────
    def __getattr__(self, name: str):
        if self._mode == "direct":
            if hasattr(self._engine, name):
                return getattr(self._engine, name)
        elif self._mode == "api":
            if hasattr(self._client, name):
                return getattr(self._client, name)
        raise AttributeError(
            f"'{type(self).__name__}' has no attribute '{name}' in {self._mode} mode"
        )

    def close(self):
        """Close the client (API mode only)."""
        if self._client:
            self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __repr__(self):
        if self._mode == "direct":
            path = self._engine._engine.data_dir
            idx = self._index or "(single)"
            return f"<Flatseek direct path={path!r} index={idx}>"
        return f"<Flatseek api host={self._hosts!r}>"

    @property
    def mode(self) -> str:
        """Current mode: 'api' or 'direct'."""
        return self._mode

    @staticmethod
    def search_multi(
        root_dir: str,
        pattern: str,
        query: str = "*",
        size: int = 10,
        page: int = 0,
        start: str | None = None,
        end: str | None = None,
        sort: str | None = None,
    ) -> dict:
        """Search across multiple index directories matching a glob pattern.

        Mirrors the REST API multi-index search behaviour for local filesystem indexes.

        Args:
            root_dir: Parent directory containing index subdirectories.
            pattern: Glob pattern, e.g. "logs_*" or "logs_*,events_*" (comma-separated).
            query: Lucene query string (default "*").
            size: Number of results per page (default 10, max 1000).
            page: Zero-based page number (default 0).
            start: Optional YYYYMMDD date to prune indices before this date.
            end: Optional YYYYMMDD date to prune indices after this date.
            sort: Optional sort spec "field:asc" or "field:desc" (not yet implemented).

        Returns:
            Dict with _index, hits{total,hits}, took (ms).
            Each hit has _index, _score, _source.
        """
        import concurrent.futures
        import re as _re
        from flatseek.core.query_engine import QueryEngine
        from flatseek.core.query_parser import parse, execute

        t0 = time.perf_counter()

        # ── 1. Discover and filter indices ──────────────────────────────────
        if not os.path.isdir(root_dir):
            return {"_index": pattern, "hits": {"total": 0, "hits": []}, "took": 0}

        all_names = sorted(
            d for d in os.listdir(root_dir)
            if os.path.isdir(os.path.join(root_dir, d))
        )

        seen: set[str] = set()
        for part in pattern.split(","):
            seen.update(fnmatch.filter(all_names, part.strip()))

        DATE_RE = _re.compile(r"(\d{4}[-_]?\d{2}[-_]?\d{0,2})")

        def _idx_date(name: str) -> str | None:
            m = DATE_RE.search(name)
            if not m:
                return None
            ds = m.group(1).replace("-", "").replace("_", "")
            return (ds + "01" * (8 - len(ds)))[:8]

        start_n = start.replace("-", "").replace("_", "") if start else None
        end_n = end.replace("-", "").replace("_", "") if end else None

        matched: list[str] = []
        for name in sorted(seen):
            d = _idx_date(name)
            if d is None:
                if start_n or end_n:
                    continue
            else:
                if start_n and d < start_n:
                    continue
                if end_n and d > end_n:
                    continue
            matched.append(name)

        if not matched:
            return {"_index": pattern, "hits": {"total": 0, "hits": []}, "took": 0}

        # ── 2. Open engines and run queries in parallel ────────────────────
        def _query_one(name: str):
            try:
                eng = QueryEngine(os.path.join(root_dir, name))
                if query == "*" or query == "":
                    ids = eng._resolve("", None)
                    ids = list(ids) if ids else []
                else:
                    ast = parse(query)
                    ids = sorted(execute(ast, eng)) if ast else []
                ids = eng._alive_ids(ids) if hasattr(eng, "_alive_ids") else ids
                return eng, ids, name
            except Exception:
                return None, [], name

        per_eng: list[tuple[QueryEngine, list[int], str]] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(matched), 8)) as pool:
            futures = {pool.submit(_query_one, n): n for n in matched}
            for future in concurrent.futures.as_completed(futures):
                eng, ids, name = future.result()
                if eng is not None:
                    per_eng.append((eng, ids, name))

        per_eng.sort(key=lambda x: x[2])

        # ── 3. Paginate across engines ──────────────────────────────────────
        total = sum(len(ids) for _, ids, _ in per_eng)
        req_size = min(size, 1000)
        page_start = page * req_size
        page_end = page_start + req_size
        results: list[dict] = []
        offset = 0

        for eng, ids, name in per_eng:
            n = len(ids)
            lo = max(0, page_start - offset)
            hi = min(n, page_end - offset)
            if lo < hi:
                docs = eng._fetch_docs(ids[lo:hi])
                for doc in docs:
                    doc["_index"] = name
                results.extend(docs)
            offset += n
            if offset >= page_end:
                break

        return {
            "_index": pattern,
            "hits": {
                "total": total,
                "hits": [
                    {
                        "_id": page_start + i,
                        "_score": 1.0,
                        "_index": r["_index"],
                        "_source": r,
                    }
                    for i, r in enumerate(results)
                ],
            },
            "took": int((time.perf_counter() - t0) * 1000),
        }

    @staticmethod
    def cross_lookup(
        source_dir: str,
        target_dir: str,
        query: str = "*",
        on: str | None = None,
        left_fields: list[str] | None = None,
        right_fields: list[str] | None = None,
        top_n: int = 10,
        page: int = 0,
        page_size: int = 20,
    ) -> dict:
        """Enrich source index results by looking up a target index on a shared key.

        Mirrors ``qe.cross_lookup()`` but accepts directory paths instead of
        pre-instantiated engines.

        Args:
            source_dir: Directory path of the source (left) index.
            target_dir: Directory path of the target (right) index.
            query: Lucene query for the source index (default "*").
            on: The shared field name to join on (e.g. "user_id", "signer").
                Required.
            left_fields: Fields to keep from the source index (default: all).
            right_fields: Fields to keep from the target index (default: all).
            top_n: Max results from source index to process (default 10).
            page: Zero-based page (default 0).
            page_size: Results per page (default 20).

        Returns:
            {
                "total": N,
                "page": N,
                "page_size": N,
                "results": [
                    {
                        "_left":  {filtered source doc},
                        "_right": [matching target docs],
                        "_match_count": N,
                    },
                    ...
                ]
            }
        """
        from flatseek.core.query_engine import QueryEngine
        qe = QueryEngine(source_dir)
        return qe.cross_lookup(
            query=query,
            target=target_dir,
            link_field=on,
            left_fields=left_fields,
            right_fields=right_fields,
            top_n=top_n,
            page=page,
            page_size=page_size,
        )


# ─── elasticsearch-py compatibility ──────────────────────────────────────────

def Elasticsearch(hosts=None, **kwargs) -> Flatseek:
    """Create Flatseek client (elasticsearch-py compatible factory)."""
    return Flatseek(hosts=hosts, **kwargs)


# ─── Package exports ─────────────────────────────────────────────────────────

__all__ = ["Flatseek", "Response", "CountResponse", "AggsResponse",
           "Elasticsearch", "__version__"]
__version__ = "0.1.11"
