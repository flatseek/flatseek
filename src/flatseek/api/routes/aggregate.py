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

    try:
        engine = manager.get_engine(index, bucket_url=bucket)
    except Exception as e:
        raise HTTPException(404, f"Index not found: {index}") from e

    if encrypted:
        try:
            from flatseek.core.query_engine import load_encryption_key
            if bucket:
                storage = manager._get_bucket_storage(bucket)
                enc_data = storage.read_bytes("encryption.json")
                meta = json.loads(enc_data)
                key = load_encryption_key(None, stored_pass, meta)
            else:
                index_dir = os.path.join(manager.data_dir, index)
                key = load_encryption_key(index_dir, stored_pass)
            engine.set_key(key)
            # Reload stats after set_key — encrypted stats.json needs the key to decrypt
            await asyncio.to_thread(engine.reload_stats)
        except Exception:
            raise HTTPException(401, "Invalid passphrase for encrypted index")


    _AGG_TIMEOUT = int(os.environ.get("FLATSEEK_TIMEOUT_AGG", 300))
    query = body.get("query", None)
    size = body.get("size", 10)
    aggs = body.get("aggs", {})

    try:
        # reload_stats + aggregate both decrypt encrypted data — wrong key fails here
        async with asyncio.timeout(_AGG_TIMEOUT):
            await asyncio.to_thread(engine.reload_stats)
            result = await asyncio.to_thread(
                engine.aggregate, q=query, aggs=aggs, size=size
            )
        return result
    except asyncio.TimeoutError:
        raise HTTPException(504, f"Aggregation timed out after {_AGG_TIMEOUT}s. Try a narrower query or fewer aggregations.")
    except MemoryError as e:
        raise HTTPException(
            503,
            f"Memory limit exceeded: {e}. Try a narrower query or fewer aggregations."
        ) from e
    except Exception as e:
        # Wrong key / corrupted encrypted data → 401 (not 500).
        try:
            from cryptography.fernet import InvalidToken as _IT
        except ImportError:
            _IT = None
        if _IT is not None and isinstance(e, _IT):
            raise HTTPException(401, "Invalid passphrase for encrypted index")
        if isinstance(e, HTTPException):
            raise e
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
    return await aggregate(index, body, request, bucket=None, manager=manager)