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
import time
import hashlib
import math
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


# ─── elasticsearch-py compatibility ──────────────────────────────────────────

def Elasticsearch(hosts=None, **kwargs) -> Flatseek:
    """Create Flatseek client (elasticsearch-py compatible factory)."""
    return Flatseek(hosts=hosts, **kwargs)


# ─── Package exports ─────────────────────────────────────────────────────────

__all__ = ["Flatseek", "Response", "CountResponse", "AggsResponse",
           "Elasticsearch", "__version__"]
__version__ = "0.1.0"
