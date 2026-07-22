"""Aggregation endpoints (Elasticsearch-inspired).

All business logic is delegated to QueryEngine.aggregate() in core.
"""

import asyncio
import json
import logging
import os
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

from flatseek.api.deps import get_index_manager, IndexManager

router = APIRouter(tags=["aggregations"])
logger = logging.getLogger(__name__)


# ─── OpenAPI Schemas ──────────────────────────────────────────────────────────

class AggBucket(BaseModel):
    key: str | int = Field(..., example="raydium")
    doc_count: int = Field(..., example=892147)


class TermsAgg(BaseModel):
    buckets: list[AggBucket]


class StatsAgg(BaseModel):
    count: int = Field(..., example=1284)
    min: float = Field(..., example=5000.0)
    max: float = Field(..., example=10000.0)
    avg: float = Field(..., example=5500.0)
    sum: float = Field(..., example=7064000000.0)


class Aggregations(BaseModel):
    by_program: TermsAgg | None = None
    fee_stats: StatsAgg | None = None

    class Config:
        extra = "allow"


class AggregateHits(BaseModel):
    total: int = Field(..., example=1284)


class AggregateResponse(BaseModel):
    hits: AggregateHits
    aggregations: dict


@router.post("/{index}/_aggregate", response_model=AggregateResponse)
async def aggregate(
    index: str,
    body: dict[str, Any],
    request: Request = None,
    bucket: str | None = Query(None, description="URL storage bucket for remote indexes"),
    manager: IndexManager = Depends(get_index_manager),
):
    """Run aggregations (facets, statistics, buckets) over matching documents.

    Supports `terms` (top-N by count), `stats` (count/min/max/avg/sum), and
    `cardinality` (HyperLogLog unique count) aggregations. Combine multiple
    aggregations in one request for rich analytics.

    Example — program breakdown with fee stats:
    ```json
    {
      "query": "status:success",
      "aggs": {
        "by_program": {"terms": {"field": "program", "size": 10}},
        "fee_stats": {"stats": {"field": "fee"}}
      }
    }
    ```
    """
    encrypted = manager.is_encrypted(index, bucket)
    if encrypted:
        stored_pass = request.headers.get("x-index-password") if request else None
        if not stored_pass:
            raise HTTPException(
                403,
                f"Index '{index}' is encrypted. Passphrase required via X-Index-Password header."
            )

    _AGG_TIMEOUT = int(os.environ.get("FLATSEEK_TIMEOUT_AGG", 300))
    q_val = body.get("query", None)
    # Handle ES-style dict queries (e.g. {"match_all": {}}) vs Lucene string
    if isinstance(q_val, str):
        query = q_val
    elif isinstance(q_val, dict):
        if "match_all" in q_val:
            query = None  # None = all docs in aggregate
        elif "term" in q_val:
            for field, val in q_val["term"].items():
                query = f"{field}:{val}"
                break
        elif "match" in q_val:
            for field, val in q_val["match"].items():
                query = f"{field}:{val}"
                break
        else:
            query = None
    else:
        query = q_val
    size = body.get("size", 10)
    aggs = body.get("aggs", {})
    index_pattern = body.get("_index") if body and isinstance(body, dict) else None

    # ── Multi-index aggregation (check BEFORE engine resolution) ─────────────────
    if index_pattern:
        # Date-pruning: start/end params in YYYYMMDD format
        prune_start = body.get("start") if body and isinstance(body, dict) else None
        prune_end = body.get("end") if body and isinstance(body, dict) else None
        matched = manager.expand_index_pattern(index_pattern, start=prune_start, end=prune_end)
        if not matched:
            return {"aggregations": {}, "total": 0, "hits": {"total": 0}}

        import concurrent.futures
        from collections import Counter

        def _agg_one(name_eng):
            name, eng = name_eng
            try:
                eng.reload_stats()
                return name, eng.aggregate(q=query, aggs=aggs, size=size)
            except Exception:
                return name, {"aggregations": {}, "total": 0}

        try:
            engs = manager.get_engines_for_pattern(
                index_pattern, bucket_url=bucket, start=prune_start, end=prune_end
            )
        except Exception as e:
            raise HTTPException(500, f"Failed to open indices: {e}")

        per_result = []
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(engs), 8)) as pool:
                futures = {pool.submit(_agg_one, ne): ne for ne in engs}
                for future in concurrent.futures.as_completed(futures):
                    try:
                        name, result = future.result()
                    except Exception:
                        ne = futures[future]
                        name, result = ne[0], {"aggregations": {}, "total": 0}
                    per_result.append((name, result))
        except Exception as e:
            raise HTTPException(500, f"Multi-index aggregation failed: {e}")

        total = sum(r.get("total", 0) for _, r in per_result)
        merged_aggs = {}

        all_agg_keys = set()
        for _, r in per_result:
            all_agg_keys.update(r.get("aggregations", {}).keys())

        for agg_key in all_agg_keys:
            agg_config = None
            for _, r in per_result:
                if agg_key in r.get("aggregations", {}):
                    agg_config = r["aggregations"][agg_key]
                    break
            if agg_config is None:
                continue

            if "buckets" in agg_config:
                bucket_counter = Counter()
                bucket_key_map = {}
                for _, r in per_result:
                    for b in r.get("aggregations", {}).get(agg_key, {}).get("buckets", []):
                        bucket_counter[b["key"]] += b["doc_count"]
                        if b["key"] not in bucket_key_map:
                            bucket_key_map[b["key"]] = b
                merged_buckets = [
                    {**bucket_key_map[k], "doc_count": c}
                    for k, c in sorted(bucket_counter.items())
                ]
                merged_aggs[agg_key] = {"buckets": merged_buckets}
            elif "count" in agg_config:
                all_stats = [r.get("aggregations", {}).get(agg_key, {}) for _, r in per_result]
                cnt = sum(s.get("count", 0) for s in all_stats)
                s_sum = sum(s.get("sum", 0) for s in all_stats)
                mn_vals = [s["min"] for s in all_stats if s.get("count", 0) > 0]
                mx_vals = [s["max"] for s in all_stats if s.get("count", 0) > 0]
                mn = min(mn_vals) if mn_vals else 0
                mx = max(mx_vals) if mx_vals else 0
                merged_aggs[agg_key] = {
                    "count": cnt,
                    "min": mn if cnt > 0 else 0,
                    "max": mx if cnt > 0 else 0,
                    "avg": s_sum / cnt if cnt > 0 else 0,
                    "sum": s_sum,
                }
            elif "value" in agg_config:
                all_vals = [r.get("aggregations", {}).get(agg_key, {}) for _, r in per_result]
                merged_aggs[agg_key] = {
                    "value": sum(s.get("value", 0) for s in all_vals),
                    "count": sum(s.get("count", 0) for s in all_vals),
                }

        return {"aggregations": merged_aggs, "total": total, "hits": {"total": total}}

    # ── Single-index aggregation ────────────────────────────────────────────
    try:
        engine = manager.get_engine(index, bucket_url=bucket)
    except Exception as e:
        raise HTTPException(404, f"Index not found: {e}") from e

    if encrypted:
        try:
            from flatseek.core.query_engine import load_encryption_key
            from flatseek.flatseek_file import FlatseekFileStorageAdapter
            if bucket:
                storage = manager._get_bucket_storage(bucket)
                enc_data = storage.read_bytes("encryption.json")
                meta = json.loads(enc_data)
                key = load_encryption_key(None, stored_pass, meta)
                engine.set_key(key)
            elif index in manager._fsk_index_map:
                fsk_path = manager._fsk_index_map[index]
                import struct as _struct
                _ENC_MAGIC = b"FLATSEEK\x01"
                HEADER_SIZE = 1024
                SECT_DESC_SIZE = 50
                SID_MANIFEST = 0x01
                enc_meta = None
                try:
                    with fsk_path.open("rb") as f:
                        header = f.read(HEADER_SIZE)
                        for i in range(16):
                            off = 64 + i * SECT_DESC_SIZE
                            if off + SECT_DESC_SIZE > len(header):
                                break
                            sid = header[off]
                            if sid != SID_MANIFEST:
                                continue
                            offset = _struct.unpack_from("<Q", header, off + 2)[0]
                            size = _struct.unpack_from("<Q", header, off + 10)[0]
                            f.seek(offset)
                            manifest_bytes = f.read(size)
                            if manifest_bytes.startswith(_ENC_MAGIC):
                                break
                            parsed = json.loads(manifest_bytes.decode("utf-8"))
                            enc_b64 = parsed.get("_encryption_b64")
                            if enc_b64:
                                import base64 as _b64
                                enc_meta = json.loads(_b64.b64decode(enc_b64).decode("utf-8"))
                            break
                except Exception:
                    pass
                if enc_meta is None:
                    raise HTTPException(401, "Invalid passphrase for encrypted index")
                key = load_encryption_key(None, stored_pass, meta=enc_meta)
                engine.set_key(key)
            else:
                index_dir = os.path.join(manager.data_dir, index)
                if not os.path.isdir(os.path.join(index_dir, "index")):
                    index_dir = manager.data_dir
                key = load_encryption_key(index_dir, stored_pass)
                engine.set_key(key)
            await asyncio.to_thread(engine.reload_stats)
        except Exception:
            raise HTTPException(401, "Invalid passphrase for encrypted index")

    try:
        async def _run():
            await asyncio.to_thread(engine.reload_stats)
            return await asyncio.to_thread(
                engine.aggregate, q=query, aggs=aggs, size=size
            )
        result = await asyncio.wait_for(_run(), timeout=_AGG_TIMEOUT)
        return result
    except asyncio.TimeoutError:
        raise HTTPException(504, f"Aggregation timed out after {_AGG_TIMEOUT}s. Try a narrower query or fewer aggregations.")
    except MemoryError as e:
        raise HTTPException(503, f"Memory limit exceeded: {e}. Try a narrower query or fewer aggregations.")
    except Exception as e:
        try:
            from cryptography.fernet import InvalidToken as _IT
        except ImportError:
            _IT = None
        if _IT is not None and isinstance(e, _IT):
            raise HTTPException(401, "Invalid passphrase for encrypted index")
        if isinstance(e, HTTPException):
            raise e
        raise
        raise   # re-raise other errors as 500


@router.get("/{index}/_aggregate", response_model=AggregateResponse)
async def aggregate_get(
    index: str,
    q: str = Query("*", description="Query string"),
    aggs: str = Query(None, description="Aggregations as JSON"),
    bucket: str | None = Query(None, description="URL storage bucket for remote indexes"),
    request: Request = None,
    manager: IndexManager = Depends(get_index_manager),
):
    """Run aggregations via GET (query params)."""
    body = {"query": q}
    if aggs:
        try:
            body["aggs"] = json.loads(aggs)
        except Exception:
            raise HTTPException(400, "Invalid aggs JSON")
    return await aggregate(index, body, request, bucket=bucket, manager=manager)