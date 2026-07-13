"""Search endpoints (Elasticsearch-inspired)."""

import asyncio
import glob as _glob
import itertools
import json
import logging
import os
import re
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

from flatseek.api.deps import get_index_manager, IndexManager

logger = logging.getLogger(__name__)


def _parse_sort(raw):
    """Parse sort parameter into list of (field, direction) tuples.

    Accepts:
      - "field1:desc,field2:asc" (string shorthand)
      - [{"field": "amount", "order": "desc"}, ...] (Elasticsearch-style)
      - [{"amount": "desc"}, ...] (simple dict style)
    """
    if not raw:
        return None

    if isinstance(raw, str):
        # String format: "field1:desc,field2:asc"
        result = []
        for part in raw.split(","):
            part = part.strip()
            if ":" in part:
                field, direction = part.split(":", 1)
                result.append((field.strip(), direction.strip()))
        return result if result else None

    if isinstance(raw, list):
        result = []
        for item in raw:
            if isinstance(item, dict):
                # Elasticsearch-style: {"field": "amount", "order": "desc"}
                if "field" in item and "order" in item:
                    result.append((item["field"], item["order"]))
                # Simple dict style: {"amount": "desc"}
                else:
                    for field, direction in item.items():
                        result.append((field, direction))
        return result if result else None

    return None


def _multi_index_paginate(
    per_eng: list[tuple[Any, list[int]]],
    page: int,
    page_size: int,
) -> dict[str, Any]:
    """Paginate across (engine, doc_ids) pairs, tagging each doc with _index.

    Replicates the logic of QE._multi_paginate but standalone so it can be
    called from the API layer without needing a QE instance.
    """
    import os
    # Sort engines by name for stable ordering
    per_eng_sorted = sorted(per_eng, key=lambda x: getattr(x[0], "data_dir", str(x[0])))

    total = sum(len(ids) for _, ids in per_eng_sorted)
    start = page * page_size
    end = start + page_size
    results: list[dict[str, Any]] = []
    offset = 0

    for eng, ids in per_eng_sorted:
        n = len(ids)
        lo = max(0, start - offset)
        hi = min(n, end - offset)
        if lo < hi:
            docs = eng._fetch_docs(ids[lo:hi])
            # For FSK-based engines (FlatseekFileStorageAdapter), storage.path
            # has the actual .fsk path; for directory-based engines, data_dir
            # has the directory path. Use whichever is meaningful.
            eng_path = getattr(eng, "data_dir", str(eng))
            storage = getattr(eng, "storage", None)
            if storage is not None:
                storage_path = getattr(storage, "path", None)
                if storage_path:
                    eng_path = str(storage_path)
            idx_name = os.path.basename(eng_path)
            if idx_name.endswith(".fsk") or idx_name.endswith(".flatseek") or idx_name.endswith(".flat"):
                idx_name = idx_name.rsplit(".", 1)[0]
            for doc in docs:
                doc["_index"] = idx_name
            results.extend(docs)
        offset += n
        if offset >= end:
            break

    return {"total": total, "page": page, "page_size": page_size, "results": results}


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


# ─── Configurable timeouts (seconds) ────────────────────────────────────────────
# Override via environment variables before starting serve.
# FLATSEEK_TIMEOUT_SEARCH   = 300  (default, 5 min)
# FLATSEEK_TIMEOUT_COUNT     = 120  (default, 2 min)
# FLATSEEK_TIMEOUT_MSEARCH   = 300  (default, 5 min)
_SEARCH_TIMEOUT  = int(os.environ.get("FLATSEEK_TIMEOUT_SEARCH",  300))
_COUNT_TIMEOUT   = int(os.environ.get("FLATSEEK_TIMEOUT_COUNT",   120))
_MSEARCH_TIMEOUT = int(os.environ.get("FLATSEEK_TIMEOUT_MSEARCH", 300))


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
async def debug_chunks(index: str, bucket: str | None = Query(None), manager: IndexManager = Depends(get_index_manager)):
    """Debug: list first 5 chunk files found by _iter_chunks."""
    import glob as _glob
    import os
    import re

    try:
        engine = manager.get_engine(index, bucket_url=bucket)
    except Exception as e:
        raise HTTPException(404, f"Index not found: {index}") from e

    docs_dir = engine.docs_dir
    from flatseek.core.storage import URLStorageAdapter

    if isinstance(engine.storage, URLStorageAdapter):
        try:
            remote_files = engine._list_docs_recursive()
            first_files = sorted(remote_files, key=lambda p: re.search(r"docs_(\d+)", p) or re.search(r"chunks_(\d+)", p) or "")[:5]
            globbed_count = len(remote_files)
        except Exception as e:
            first_files = [f"error: {e}"]
            globbed_count = 0
    else:
        pattern = os.path.join(docs_dir, "**", "*.zlib")
        files = _glob.glob(pattern, recursive=True)
        if not files:
            pattern2 = os.path.join(docs_dir, "chunks_*.zlib")
            files = _glob.glob(pattern2)
        globbed_count = len(files)
        first_files = sorted(files, key=lambda p: re.search(r"docs_(\d+)", p) or re.search(r"chunks_(\d+)", p) or "")[:5]

    try:
        await asyncio.to_thread(engine.reload_stats)
    except Exception as e:
        try:
            from cryptography.fernet import InvalidToken
            if isinstance(e, InvalidToken):
                raise HTTPException(401, "Invalid passphrase for encrypted index")
        except ImportError:
            pass
        raise HTTPException(500, str(e))

    return {
        "docs_dir": docs_dir,
        "storage_url": engine.storage.base_url if hasattr(engine.storage, 'base_url') else str(engine.storage),
        "globbed_count": globbed_count,
        "first_files": first_files,
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
    sort: str | None = Query(None, description="Sort: field:asc or field:desc (comma-separated for multi-field)"),
    bucket: str | None = Query(None, description="URL storage bucket (HuggingFace/GitHub URL) for remote indexes"),
    request: Request = None,
    manager: IndexManager = Depends(get_index_manager),
):
    """Search with request body or query params."""

    # ── Multi-index search: _index pattern — checked BEFORE engine resolution ──
    # Parse params first so we can detect _index before calling get_engine
    query = None
    if body and isinstance(body, dict):
        query = body.get("query") or body.get("q")
    if query is None:
        query = q
    if not query:
        query = "*"

    req_size = None
    if body and isinstance(body, dict):
        req_size = body.get("size")
    req_size = min(req_size if req_size is not None else size, 1000)

    req_from = None
    if body and isinstance(body, dict):
        req_from = body.get("from") or body.get("from_")
    req_from = req_from if req_from is not None else from_

    sort_spec = None
    if body and isinstance(body, dict):
        raw_sort = body.get("sort")
        if raw_sort:
            sort_spec = _parse_sort(raw_sort)
    if sort_spec is None and sort:
        sort_spec = _parse_sort(sort)

    index_pattern = body.get("_index") if body and isinstance(body, dict) else None
    if index_pattern:
        # Multi-index: no single engine to resolve. Handle in multi-index branch.
        time_start = time.perf_counter()
        # Date-pruning: start/end params in YYYYMMDD format
        prune_start = body.get("start") if body and isinstance(body, dict) else None
        prune_end = body.get("end") if body and isinstance(body, dict) else None
        matched_indices = manager.expand_index_pattern(index_pattern, start=prune_start, end=prune_end)
        if not matched_indices:
            return {
                "_index": index_pattern,
                "hits": {"total": 0, "hits": []},
                "took": int((time.perf_counter() - time_start) * 1000),
            }

        # Parse the query once (same AST for all engines)
        from flatseek.core.query_parser import parse, execute
        try:
            ast = parse(query) if query != "*" and query != "" else None
        except Exception:
            raise HTTPException(400, f"Invalid query: {query}")

        # Get open engines for all matching indices
        try:
            engs_for_pattern = manager.get_engines_for_pattern(
                index_pattern, bucket_url=bucket, start=prune_start, end=prune_end
            )
        except Exception as e:
            raise HTTPException(500, f"Failed to open indices: {e}")

        import concurrent.futures

        def _query_one_index(name_eng: tuple[str, Any]) -> tuple[Any, list[int]]:
            name, eng = name_eng
            try:
                if query == "*" or query == "":
                    ids = eng._resolve("", None)
                    ids = list(ids) if ids else []
                else:
                    ids = sorted(execute(ast, eng))
                ids = eng._alive_ids(ids) if hasattr(eng, "_alive_ids") else ids
                return eng, ids
            except Exception:
                return eng, []

        per_eng: list[tuple[Any, list[int]]] = []
        try:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=min(len(engs_for_pattern), 8)
            ) as pool:
                futures = {pool.submit(_query_one_index, ne): ne for ne in engs_for_pattern}
                for future in concurrent.futures.as_completed(futures):
                    try:
                        eng, ids = future.result()
                    except Exception:
                        ne = futures[future]
                        eng = ne[1]
                        ids = []
                    per_eng.append((eng, ids))
        except Exception as e:
            raise HTTPException(500, f"Multi-index search failed: {e}")

        page = req_from // max(req_size, 1)
        result = _multi_index_paginate(per_eng, page, req_size)
        result["took"] = int((time.perf_counter() - time_start) * 1000)
        return {
            "_index": index_pattern,
            "hits": {
                "total": result["total"],
                "hits": [
                    {"_id": req_from + i, "_score": 1.0, "_source": doc}
                    for i, doc in enumerate(result["results"])
                ],
            },
            "took": result["took"],
        }

    # ── Single-index path (existing logic) ───────────────────────────────────
    encrypted = manager.is_encrypted(index, bucket)
    if encrypted:
        stored_pass = request.headers.get("x-index-password") if request else None
        already_authed = manager._enc_keys.get(index) is not None
        if not stored_pass and not already_authed:
            raise HTTPException(
                401,
                f"Index '{index}' is encrypted. Submit password via POST /{index}/_authenticate or pass X-Index-Password header."
            )

    try:
        engine = manager.get_engine(index, bucket_url=bucket)
    except PermissionError as e:
        raise HTTPException(401, str(e)) from e
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
                if already_authed and not stored_pass:
                    key = manager._enc_keys[index]
                elif enc_meta is None:
                    raise HTTPException(401, "Invalid passphrase for encrypted index")
                else:
                    key = load_encryption_key(None, stored_pass, meta=enc_meta)
                engine.set_key(key)
            else:
                index_dir = os.path.join(manager.data_dir, index)
                if not os.path.isdir(os.path.join(index_dir, "index")):
                    index_dir = manager.data_dir
                key = load_encryption_key(index_dir, stored_pass)
                engine.set_key(key)
            await asyncio.to_thread(engine.reload_stats)
        except Exception as exc:
            raise HTTPException(401, "Invalid passphrase for encrypted index")

    try:
        start = time.perf_counter()

        # Bail early if client already disconnected
        if request and await request.is_disconnected():
            raise HTTPException(499, "Client disconnected")

        # Use body params if provided, else query params
        MAX_SIZE = 1000

        req_size = body.get('size') if body else None
        req_size = min(req_size if req_size is not None else size, MAX_SIZE)
        req_from = body.get('from', body.get('from_')) if body else None
        req_from = req_from if req_from is not None else from_

        # ── Single-index search ───────────────────────────────────────────────

        # Route queries to engine.query() or engine.search():
        # - engine.query() handles Lucene DSL (field:term, AND/OR/NOT, wildcards)
        # - engine.search() handles cross-column simple wildcards (*term* without field:)
        # If query contains ':' it is field-specific → use engine.query()
        # If query contains AND/OR/NOT → use engine.query() (Lucene DSL)
        # If query is just "*" → use engine.search() for fetch-all
        # Otherwise for simple wildcards (*term* without field:) → use engine.search()

        # Helper: run blocking query in thread so cancellation can propagate
        def _do_query():
            if query == "*" or query == "":
                return engine.search("", page=req_from // max(req_size, 1), page_size=req_size, sort=sort_spec)
            elif ":" in query or any(op in query.upper() for op in [" AND ", " OR ", " NOT "]):
                return engine.query(query, page=req_from // max(req_size, 1), page_size=req_size, sort=sort_spec)
            elif "*" in query or "%" in query:
                return engine.search(query, page=req_from // max(req_size, 1), page_size=req_size, sort=sort_spec)
            else:
                return engine.query(query, page=req_from // max(req_size, 1), page_size=req_size, sort=sort_spec)

        # Run with configurable timeout — prevents runaway queries from holding resources
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(_do_query), timeout=_SEARCH_TIMEOUT
            )
        except asyncio.TimeoutError:
            raise HTTPException(504, f"Query timed out after {_SEARCH_TIMEOUT}s. Try a narrower query.")

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
    except HTTPException:
        raise
    except Exception as e:
        # Wrong key (InvalidToken from decrypt_bytes) → 401, not 500
        try:
            from cryptography.fernet import InvalidToken
            if isinstance(e, InvalidToken):
                raise HTTPException(401, "Invalid passphrase for encrypted index")
        except ImportError:
            pass
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
    sort: str | None = Query(None, description="Sort: field:asc or field:desc (comma-separated for multi-field)"),
    bucket: str | None = Query(None, description="URL storage bucket (HuggingFace/GitHub URL) for remote indexes"),
    request: Request = None,
    manager: IndexManager = Depends(get_index_manager),
):
    """Search with GET (query params only)."""
    return await search(index, None, q, from_, size, sort, bucket, request, manager)


@router.get(
    "/{index}/_doc/{doc_id}",
    response_model=DocResponse,
    summary="Get document by ID",
    description="Retrieve a single document by its numeric ID.",
)
async def get_document(
    index: str,
    doc_id: int,
    bucket: str | None = Query(None, description="URL storage bucket for remote indexes"),
    request: Request = None,
    manager: IndexManager = Depends(get_index_manager),
):
    """Get a document by ID."""
    encrypted = manager.is_encrypted(index, bucket)
    if encrypted:
        stored_pass = request.headers.get("x-index-password") if request else None
        if not stored_pass:
            raise HTTPException(
                403,
                f"Index '{index}' is encrypted. Submit password via POST /{index}/_authenticate"
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
                if not os.path.isdir(os.path.join(index_dir, "index")):
                    index_dir = manager.data_dir  # data_dir IS the index (unpacked .fsk)
                key = load_encryption_key(index_dir, stored_pass)
            engine.set_key(key)
            await asyncio.to_thread(engine.reload_stats)
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(401, "Invalid passphrase for encrypted index")

    try:
        docs = await asyncio.to_thread(engine._fetch_docs, [doc_id])
    except Exception as e:
        try:
            from cryptography.fernet import InvalidToken
            if isinstance(e, InvalidToken):
                raise HTTPException(401, "Invalid passphrase for encrypted index")
        except ImportError:
            pass
        raise HTTPException(500, str(e))

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
    body: list[dict[str, Any]],
    bucket: str | None = Query(None, description="URL storage bucket for remote indexes"),
    request: Request = None,
    manager: IndexManager = Depends(get_index_manager),
):
    """Execute multiple searches."""
    encrypted = manager.is_encrypted(index, bucket)
    if encrypted:
        stored_pass = request.headers.get("x-index-password") if request else None
        if not stored_pass:
            raise HTTPException(
                403,
                f"Index '{index}' is encrypted. Submit password via POST /{index}/_authenticate"
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
                if not os.path.isdir(os.path.join(index_dir, "index")):
                    index_dir = manager.data_dir  # data_dir IS the index (unpacked .fsk)
                key = load_encryption_key(index_dir, stored_pass)
            engine.set_key(key)
            await asyncio.to_thread(engine.reload_stats)
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(401, "Invalid passphrase for encrypted index")

    async def _run_one(req: dict):
        query = req.get("q") or req.get("query", "*")
        size = req.get("size", 10)
        from_ = req.get("from", 0)
        result = await asyncio.to_thread(
            engine.search, query, page=from_ // max(size, 1), page_size=size
        )
        return {"hits": {"total": result.get("total", 0), "hits": result.get("results", [])}}

    # Run all searches in parallel via gather, but individually wrapped in to_thread
    # so cancellation of any one cancels that thread
    # Overall timeout prevents runaway multi-search from holding resources
    try:
        responses = await asyncio.wait_for(
            asyncio.gather(*(_run_one(req) for req in itertools.islice(body, 10))),
            timeout=_MSEARCH_TIMEOUT
        )
        responses = list(responses)
    except Exception as e:
        try:
            from cryptography.fernet import InvalidToken
            if isinstance(e, InvalidToken):
                raise HTTPException(401, "Invalid passphrase for encrypted index")
        except ImportError:
            pass
        raise HTTPException(500, str(e))
    except asyncio.TimeoutError:
        raise HTTPException(504, f"Multi-search timed out after {_MSEARCH_TIMEOUT}s.")
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
    bucket: str | None = Query(None, description="URL storage bucket for remote indexes"),
    request: Request = None,
    manager: IndexManager = Depends(get_index_manager),
):
    """Count documents matching a query."""
    encrypted = manager.is_encrypted(index, bucket)
    if encrypted:
        stored_pass = request.headers.get("x-index-password") if request else None
        if not stored_pass:
            raise HTTPException(
                403,
                f"Index '{index}' is encrypted. Submit password via POST /{index}/_authenticate"
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
                if not os.path.isdir(os.path.join(index_dir, "index")):
                    index_dir = manager.data_dir  # data_dir IS the index (unpacked .fsk)
                key = load_encryption_key(index_dir, stored_pass)
            engine.set_key(key)
            # Reload stats after set_key — encrypted stats.json needs the key to decrypt
            await asyncio.to_thread(engine.reload_stats)
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(401, "Invalid passphrase for encrypted index")

    query = q or "*"
    try:
        result = await asyncio.wait_for(
            asyncio.to_thread(engine.search, query, page_size=0), timeout=_COUNT_TIMEOUT
        )
    except asyncio.TimeoutError:
        raise HTTPException(504, f"Count timed out after {_COUNT_TIMEOUT}s. Try a narrower query.")
    except Exception as e:
        try:
            from cryptography.fernet import InvalidToken
            if isinstance(e, InvalidToken):
                raise HTTPException(401, "Invalid passphrase for encrypted index")
        except ImportError:
            pass
        raise HTTPException(500, str(e))

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


# ─── Cross-lookup ───────────────────────────────────────────────────────────────

class CrossLookupResponse(BaseModel):
    total: int
    page: int
    page_size: int
    results: list[dict[str, Any]]


@router.post(
    "/{index}/_cross_lookup",
    response_model=CrossLookupResponse,
    summary="Cross-lookup: enrich source index with target index",
    description="Search the source index, then look up matching documents in a target index using a shared key field.",
)
async def cross_lookup(
    index: str,
    body: dict[str, Any],
    manager: IndexManager = Depends(get_index_manager),
):
    """Cross-lookup: enrich source results with target index lookups."""
    target_index = body.get("target")
    if not target_index:
        raise HTTPException(400, "target index name is required")

    query = body.get("query", "*")
    link_field = body.get("on")
    if not link_field:
        raise HTTPException(400, "'on' (link field) is required")
    target_field = body.get("target_field") or link_field
    left_fields = body.get("left_fields")
    right_fields = body.get("right_fields")
    top_n = min(body.get("top_n", 10), 1000)
    page = max(body.get("page", 0), 0)
    page_size = min(body.get("page_size", 20), 100)

    # Open source engine
    try:
        source_eng = manager.get_engine(index)
    except Exception as e:
        raise HTTPException(404, f"Source index not found: {index}") from e

    # Resolve target path
    from flatseek.api.deps import StorageConfig
    target_dir = os.path.join(manager.data_dir, target_index)
    if not os.path.isdir(target_dir):
        raise HTTPException(404, f"Target index not found: {target_index}")

    result = source_eng.cross_lookup(
        query=query,
        target=target_dir,
        link_field=link_field,
        target_field=target_field,
        left_fields=left_fields,
        right_fields=right_fields,
        top_n=top_n,
        page=page,
        page_size=page_size,
    )
    return result