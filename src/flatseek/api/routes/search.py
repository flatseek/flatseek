"""Search endpoints (Elasticsearch-inspired)."""

import asyncio
import glob as _glob
import logging
import os
import re
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

from flatseek.api.deps import get_index_manager, IndexManager

logger = logging.getLogger(__name__)

# ─── OpenAPI Response Schemas ───────────────────────────────────────────────────
# Flexible schemas — no hardcoded field names so any dataset type works.

class HitSource(BaseModel):
    """Flexible source: extra fields are allowed, no required columns."""

class Hit(BaseModel):
    id: int = Field(..., alias="_id", example=0)
    score: float = Field(..., alias="_score", example=1.0)
    source: Any = Field(..., alias="_source")

    class Config:
        populate_by_name = True


class SearchHits(BaseModel):
    total: int = Field(..., example=1284)
    hits: list[Hit]


class SearchResponse(BaseModel):
    index: str = Field(..., alias="_index", example="my_index")
    hits: SearchHits
    took: int = Field(..., example=12)

    class Config:
        populate_by_name = True


class DocResponse(BaseModel):
    """Flexible single-doc response — extra fields allowed."""

    index: str = Field(..., alias="_index", example="my_index")
    id: int = Field(..., alias="_id", example=0)
    source: Any = Field(..., alias="_source")

    class Config:
        populate_by_name = True


class CountResponse(BaseModel):
    index: str = Field(..., alias="_index", example="my_index")
    count: int = Field(..., example=1284)

    class Config:
        populate_by_name = True


class MSearchResponse(BaseModel):
    responses: list[dict]


class ValidateResponse(BaseModel):
    valid: bool = Field(..., example=True)
    query: str = Field(..., example="*")


router = APIRouter(
    tags=["search"],
    responses={
        404: {"description": "Index not found"},
        400: {"description": "Invalid query"},
    },
)


@router.get("/_debug/chunks/{index}")
async def debug_chunks(index: str, manager: IndexManager = Depends(get_index_manager)):
    """Debug: list first 5 chunk files found by _iter_chunks."""
    import glob as _glob
    import os
    import re

    try:
        engine = manager.get_engine(index)
    except Exception as e:
        raise HTTPException(404, f"Index not found: {index}") from e

    docs_dir = engine.docs_dir
    pattern = os.path.join(docs_dir, "**", "*.zlib")
    files = _glob.glob(pattern, recursive=True)
    if not files:
        pattern2 = os.path.join(docs_dir, "chunks_*.zlib")
        files = _glob.glob(pattern2)

    return {
        "docs_dir": docs_dir,
        "pattern": pattern,
        "globbed_count": len(files),
        "first_files": sorted(files, key=lambda p: re.search(r"docs_(\d+)", p) or re.search(r"chunks_(\d+)", p) or "")[:5],
        "sub_engines": engine._sub_engines is not None,
        "stats_total_docs": engine.stats.get("total_docs"),
    }


@router.post(
    "/{index}/_search",
    response_model=SearchResponse,
    summary="Search documents",
    description="Full-text search using Lucene query syntax. Supports field filters (`program:raydium`), wildcards (`signer:*7xMg*`), range queries (`bid:[50 TO 200]`), boolean operators (AND/OR/NOT), and grouping.\n\n"
    "Query examples:\n"
    "- `program:raydium` — exact match on program field\n"
    "- `signer:*7xMg*` — wildcard contains\n"
    "- `amount:>1000000` — numeric range\n"
    "- `bid:[50 TO 200]` — closed numeric range\n"
    "- `program:raydium AND status:success` — boolean AND\n"
    "- `-(level:INFO)` — exclusion",
)
async def search(
    index: str,
    body: dict | None = None,
    q: str | None = Query(None, description="Query string (Lucene syntax)"),
    from_: int = Query(0, ge=0, alias="from"),
    size: int = Query(20, ge=0, le=10000),
    request: Request = None,
    manager: IndexManager = Depends(get_index_manager),
):
    """Search with request body or query params."""
    # Check if index is encrypted and requires password
    if manager.is_encrypted(index):
        stored_pass = manager.get_password(index)
        if not stored_pass and request:
            stored_pass = request.headers.get("x-index-password")
        if not stored_pass:
            raise HTTPException(
                403,
                f"Index '{index}' is encrypted. Submit password via POST /{index}/_authenticate"
            )
        try:
            from flatseek.core.query_engine import load_encryption_key
            index_dir = os.path.join(manager.data_dir, index)
            key = load_encryption_key(index_dir, stored_pass)
            manager.get_engine(index).set_key(key)
            manager.set_password(index, stored_pass)
        except Exception:
            raise HTTPException(401, "Invalid passphrase for encrypted index")

    try:
        engine = manager.get_engine(index)
    except Exception as e:
        raise HTTPException(404, f"Index not found: {index}") from e

    try:
        start = time.perf_counter()
        query = body.get('query', '*') if body else "*"
        if q:
            query = q

        if not query:
            query = "*"

        # Use body params if provided, else query params
        # Enforce maximum size to prevent overload
        MAX_SIZE = 1000

        req_size = body.get('size') if body else None
        req_size = min(req_size if req_size is not None else size, MAX_SIZE)
        req_from = body.get('from', body.get('from_')) if body else None
        req_from = req_from if req_from is not None else from_

        # Route queries to engine.query() or engine.search():
        # - engine.query() handles Lucene DSL (field:term, AND/OR/NOT, wildcards)
        # - engine.search() handles cross-column simple wildcards (*term* without field:)
        # If query contains ':' it is field-specific → use engine.query()
        # If query contains AND/OR/NOT → use engine.query() (Lucene DSL)
        # If query is just "*" → use engine.search() for fetch-all
        # Otherwise for simple wildcards (*term* without field:) → use engine.search()
        if query == "*" or query == "":
            result = engine.search("", page=req_from // max(req_size, 1), page_size=req_size)
            hits = result.get("results", [])
            total_hits = result.get("total", 0)
        elif ":" in query or any(op in query.upper() for op in [" AND ", " OR ", " NOT "]):
            # Field-specific or DSL query → parse with query engine
            result = engine.query(query, page=req_from // max(req_size, 1), page_size=req_size)
            hits = result.get("results", [])
            total_hits = result.get("total", 0)
        elif "*" in query or "%" in query:
            # Cross-column simple wildcard (no field:) → use search
            result = engine.search(query, page=req_from // max(req_size, 1), page_size=req_size)
            hits = result.get("results", [])
            total_hits = result.get("total", 0)
        else:
            result = engine.query(query, page=req_from // max(req_size, 1), page_size=req_size)
            hits = result.get("results", [])
            total_hits = result.get("total", 0)

        return {
            "_index": index,
            "hits": {
                "total": total_hits,
                "hits": [
                    {"_id": req_from + i, "_score": 1.0, "_source": doc}
                    for i, doc in enumerate(hits)
                ],
            },
            "took": int((time.perf_counter() - start) * 1000),
        }
    except asyncio.CancelledError:
        logger.debug("Search request cancelled by client")
        raise
    except Exception as e:
        logger.error(f"Search error: {e}")
        raise HTTPException(500, str(e))


@router.get(
    "/{index}/_search",
    response_model=SearchResponse,
    summary="Search documents (GET)",
    description="Search with GET query parameters. Same query syntax as POST but via URL params.",
)
async def search_get(
    index: str,
    q: str | None = Query(None),
    from_: int = Query(0, ge=0, alias="from"),
    size: int = Query(20, ge=0, le=10000),
    request: Request = None,
    manager: IndexManager = Depends(get_index_manager),
):
    """Search with GET (query params only)."""
    return await search(index, None, q, from_, size, request, manager)


@router.get(
    "/{index}/_doc/{doc_id}",
    response_model=DocResponse,
    summary="Get document by ID",
    description="Retrieve a single document by its numeric ID.",
)
async def get_document(
    index: str,
    doc_id: int,
    manager: IndexManager = Depends(get_index_manager),
):
    """Get a document by ID."""
    try:
        engine = manager.get_engine(index)
    except Exception as e:
        raise HTTPException(404, f"Index not found: {index}") from e
    
    docs = engine._fetch_docs([doc_id])
    
    if not docs:
        raise HTTPException(404, f"Document not found: {doc_id}")
    
    doc = docs[0]
    return {"_index": index, "_id": doc.get("_id", doc_id), "_source": doc}


@router.post(
    "/{index}/_msearch",
    response_model=MSearchResponse,
    summary="Multi-search",
    description="Execute up to 10 searches in a single request. Useful for dashboards comparing multiple queries.",
)
async def multi_search(
    index: str,
    request: list[dict[str, Any]],
    manager: IndexManager = Depends(get_index_manager),
):
    """Execute multiple searches."""
    try:
        engine = manager.get_engine(index)
    except Exception as e:
        raise HTTPException(404, f"Index not found: {index}") from e
    
    responses = []
    
    for req in request:
        query = req.get("q") or req.get("query", "*")
        size = req.get("size", 10)
        from_ = req.get("from", 0)

        result = engine.search(query, page=from_ // max(size, 1), page_size=size)
        
        responses.append({"hits": {"total": result.get("total", 0), "hits": result.get("results", [])}})
    
    return {"responses": responses}


@router.get(
    "/{index}/_count",
    response_model=CountResponse,
    summary="Count matching documents",
    description="Returns the number of documents in an index matching the given query.",
)
async def count(
    index: str,
    q: str | None = Query(None),
    manager: IndexManager = Depends(get_index_manager),
):
    """Count documents matching a query."""
    try:
        engine = manager.get_engine(index)
    except Exception as e:
        raise HTTPException(404, f"Index not found: {index}") from e
    
    query = q or "*"
    result = engine.search(query, page_size=0)
    
    return {"_index": index, "count": result.get("total", 0)}


@router.post(
    "/{index}/_validate",
    response_model=ValidateResponse,
    summary="Validate a query",
    description="Parse and validate a Lucene query string without executing it. Returns whether the syntax is valid.",
)
async def validate_query(
    index: str,
    body: dict[str, Any],
    manager: IndexManager = Depends(get_index_manager),
):
    """Validate a query."""
    query = body.get("query", "*")
    
    try:
        engine = manager.get_engine(index)
    except Exception as e:
        raise HTTPException(404, f"Index not found: {index}") from e
    
    from flatseek.core.query_parser import parse
    
    ast = parse(query)
    valid = ast is not None
    
    return {"valid": valid, "query": query}