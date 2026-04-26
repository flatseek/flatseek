"""Indexing endpoints (Elasticsearch-inspired)."""

import asyncio
import csv
import io
import json
import os
import urllib.request
import shutil
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from flatseek.api.deps import IndexManager, get_index_manager
from flatseek.core.builder import IndexBuilder
from flatseek.core.classify import infer_type

router = APIRouter(tags=["indexing"])


# ─── OpenAPI Schemas ──────────────────────────────────────────────────────────

class IndexedDoc(BaseModel):
    index: str = Field(..., alias="_index", example="solana_txs")
    id: int = Field(..., alias="_id", example=0)
    status: str = Field(..., example="created")

    class Config:
        populate_by_name = True


class BulkIndexResponse(BaseModel):
    indexed: int = Field(..., example=2)
    errors: list | None = Field(None, example=[])
    resume: dict = Field(
        default_factory=dict,
        example={"doc_offset": 2, "batch_count": 1},
    )


class UploadProgress(BaseModel):
    doc_offset: int = Field(..., example=15234)
    batch_count: int = Field(..., example=3)
    last_update: float | None = Field(None, example=1745500000.0)
    in_progress: bool = Field(..., example=True)
    bytes_done: int = Field(0, example=0)
    bytes_total: int = Field(0, example=0)
    file_name: str = Field("", example="solana_txs.csv")
    done_files: int = Field(0, example=0)
    total_files: int = Field(0, example=1)
    docs_per_sec: float = Field(0.0, example=0.0)


class IndexStats(BaseModel):
    index: str = Field(..., alias="_index", example="solana_txs")
    docs: dict = Field(
        default_factory=dict,
        example={"count": 1000000, "deleted": 0},
    )
    store: dict = Field(
        default_factory=dict,
        example={"size_bytes": 341200000, "index_size_mb": 180.5, "docs_size_mb": 160.7},
    )
    encrypted: bool = Field(..., example=False)
    columns: dict
    in_upload: bool = Field(..., example=False)

    class Config:
        populate_by_name = True


class IndexMapping(BaseModel):
    index: str = Field(..., alias="_index", example="solana_txs")
    mappings: dict

    class Config:
        populate_by_name = True


class CreateIndexResponse(BaseModel):
    status: str = Field(..., example="created")
    index: str = Field(..., alias="_index", example="adsb")
    message: str | None = Field(None, example=None)

    class Config:
        populate_by_name = True


class FlushResponse(BaseModel):
    index: str = Field(..., alias="_index", example="solana_txs")
    status: str = Field(..., example="flushed")
    docs: int = Field(..., example=100000)

    class Config:
        populate_by_name = True


class DeleteResponse(BaseModel):
    acknowledged: bool = Field(..., example=True)
    status: str = Field(..., example="deleting")


class IndexLogs(BaseModel):
    events: list[dict]
    count: int = Field(..., example=5)


class RenameResponse(BaseModel):
    acknowledged: bool = Field(..., example=True)
    old_name: str = Field(..., example="solana_txs")
    new_name: str = Field(..., example="solana_txs_v2")

# In-memory index builder (for API-based indexing)
# In production, you'd use a proper task queue
_index_builders: dict[str, IndexBuilder] = {}

# Thread pool for offloading blocking bulk indexing — keeps API event loop responsive
_bulk_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="flatseek_bulk")

# Background encrypt/decrypt job state
# { job_id: { index, op, passphrase, done, total, status, error } }
# status: "running" | "done" | "error"
_encrypt_jobs: dict[str, dict] = {}

# Index event logs (in-memory, persisted to {data_dir}/{index}/events.json on write)
# { index_normalized: [ {ts, type, message}, ... ] }
_index_logs: dict[str, list] = {}

# Upload resume tracking
# { index: { doc_offset, batch_count, last_update, bytes_processed, total_bytes_est } }
_upload_progress: dict[str, dict] = {}


def _add_index_log(index: str, event_type: str, message: str):
    """Append a timestamped event to the in-memory log and persist to events.json."""
    import json as _json
    ts = time.time()
    event = {"ts": ts, "type": event_type, "message": message}
    if index not in _index_logs:
        _index_logs[index] = []
    _index_logs[index].append(event)
    if len(_index_logs[index]) > 200:
        _index_logs[index] = _index_logs[index][-200:]
    try:
        events_path = os.path.join(
            os.environ.get("FLATSEEK_DATA_DIR", "data"), index, "events.json"
        )
        with open(events_path, "w") as f:
            _json.dump(_index_logs[index], f)
    except Exception:
        pass


def _normalize_doc(doc: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """Normalize a document and extract field names.

    Handles ES dump format: {"_source": {...}} — only _source fields are inserted.
    """
    # Unwrap ES dump format
    if "_source" in doc and isinstance(doc["_source"], dict):
        doc = doc["_source"]

    normalized = {}
    fields = []

    for key, value in doc.items():
        if value is None or value == "":
            continue

        # Normalize key: lowercase, replace spaces/dashes with underscore
        norm_key = key.lower().strip().replace(" ", "_").replace("-", "_")

        # Normalize value
        if isinstance(value, str):
            value = value.strip()

        normalized[norm_key] = value
        fields.append(norm_key)

    return normalized, fields

def _infer_semantic_type(col: str, value: str) -> str:
    """Infer storage type from column name and value.

    Delegates to classify.infer_type() — centralized in core/classify.py.
    Kept as alias here for backwards compatibility with existing call sites.
    """
    return infer_type(col, value)


def _build_column_schema(doc: dict[str, Any]) -> dict[str, tuple[str, str]]:
    """Build column schema from document.

    Returns {col_name: (semantic_type, canonical_key)}
    """
    schema = {}
    seen = set()

    for key, value in doc.items():
        if key in seen:
            continue
        seen.add(key)

        # Get string value for type inference
        str_value = ""
        if isinstance(value, str):
            str_value = value
        elif value is not None:
            str_value = str(value)

        # Check actual type first (array/object values) — expand_record converts
        # nested structures to flat keys like tags[0], address.city, so check the
        # value type before string inference.
        if isinstance(value, list):
            sem_type = "ARRAY"
        elif isinstance(value, dict):
            sem_type = "OBJECT"
        else:
            sem_type = _infer_semantic_type(key, str_value)
        schema[key] = (sem_type, key)

    return schema


@router.post("/{index}/_doc", response_model=IndexedDoc)
async def index_document(
    index: str,
    request: dict[str, Any],
    manager: IndexManager = Depends(get_index_manager),
):
    """Index a single document.

    POST /solana_txs/_doc
    {
        "signature": "5xJ3kAhD...7X",
        "program": "raydium",
        "signer": "7xMg3...X",
        "amount": 1000000,
        "fee": 5000,
        "status": "success"
    }
    """
    # Validate index name
    index = index.strip().replace(" ", "_")
    if not index.replace("_", "").replace("-", "").isalnum():
        raise HTTPException(400, f"Invalid index name: {index}")
    
    # Normalize the document
    doc, fields = _normalize_doc(request)
    if not doc:
        raise HTTPException(400, "Empty document")
    
    # Check if index exists for stats
    data_dir = manager.data_dir
    index_dir = os.path.join(data_dir, index)
    
    if os.path.isdir(os.path.join(index_dir, "index")):
        # Index exists - need to append (requires re-indexing)
        # For now, return a hint about the index existing
        raise HTTPException(
            400,
            f"Index '{index}' already exists. Use _bulk for adding documents "
            "or rebuild the index with new data."
        )
    
    # Create or get builder
    builder = _index_builders.get(index)
    if builder is None:
        # Infer column schema from first doc
        col_schema = _build_column_schema(doc)
        
        os.makedirs(index_dir, exist_ok=True)
        builder = IndexBuilder(
            index_dir,
            column_map={},
            start_doc_id=0,
            dataset=index,
            checkpoint_cb=None,
            delimiter=",",
            columns=None,
            worker_id=None,
            dedup_fields=None,
            doc_id_end=None,
            daemon=True,  # Keep in memory until explicitly flushed
        )
        _index_builders[index] = builder
    
    # Add a dummy row (our builder expects CSV-like rows)
    row = {k: str(v) for k, v in doc.items()}
    builder.add_row(row, list(doc.keys()), file_rel=None, col_schema=None)
    
    return {
        "_index": index,
        "_id": builder.doc_counter - 1,
        "status": "created",
    }


# Upload resume tracking
# { index: { doc_offset, batch_count, last_update, bytes_processed, total_bytes_est } }
_upload_progress: dict[str, dict] = {}


def _index_batch(builder: IndexBuilder, docs: list[dict[str, Any]], already_normalized: bool = False) -> int:
    """Run in thread pool — blocks the thread, not the API event loop.

    Args:
        builder: The IndexBuilder instance.
        docs: List of documents to index.
        already_normalized: If True, docs are already {col: str_value} dicts ready for add_row.
                            If False, docs are raw dicts that need _normalize_doc processing.
    """
    indexed = 0
    headers = None
    for doc in docs:
        try:
            if not doc:
                continue
            if already_normalized:
                # Docs are already {col: str_value} — just add directly
                norm_doc = doc
            else:
                norm_doc, _ = _normalize_doc(doc)
                if not norm_doc:
                    continue
                norm_doc = {k: str(v) for k, v in norm_doc.items()}
            if headers is None:
                headers = list(norm_doc.keys())
            builder.add_row(norm_doc, headers, file_rel=None, col_schema=None)
            indexed += 1
        except Exception:
            pass
    return indexed


def _fetch_and_parse_url(url: str, fmt: str, source_field: str | None = None) -> tuple[list[dict[str, Any]], list[str] | None]:
    """Run in thread pool — fetch remote URL and parse into normalized dicts.

    Args:
        url: Remote URL to fetch.
        fmt: Format hint — 'auto', 'csv', 'json', 'jsonl'.
        source_field: If set (e.g., '_source'), extract this field from each JSON object
                      rather than using the root object as the document.
                      Supports dot-notation for nested paths, e.g. 'data.results'.

    Returns (records, fieldnames) where records are plain dicts ready for _index_batch.
    """
    import csv as _csv
    import io as _io
    import json as _json

    # Fetch content
    try:
        with urllib.request.urlopen(url, timeout=120) as resp:
            content = resp.read().decode("utf-8")
    except Exception as e:
        raise RuntimeError(f"Failed to fetch URL: {e}")

    records: list[dict[str, Any]] = []
    fieldnames = None

    # Detect format from URL extension if auto
    lower_url = url.lower()
    if fmt == "auto":
        if lower_url.endswith(".jsonl") or lower_url.endswith(".ndjson"):
            fmt = "jsonl"
        elif lower_url.endswith(".json"):
            fmt = "json"
        else:
            fmt = "csv"

    def _extract_source(record: dict[str, Any]) -> dict[str, Any] | None:
        """Extract nested source field from a record using dot-notation path.

        Falls back to the raw record if the path doesn't resolve to a dict.
        If the resolved value is a list (e.g. data.results), each item in the
        list is yielded as a separate record.
        """
        if not source_field:
            return record
        parts = source_field.split(".")
        current = record
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
                if current is None:
                    return None
            elif isinstance(current, list):
                # Row source: each item in the list is a record dict
                return current  # caller will iterate over items
            else:
                return None
        # If extracted value is not a dict and not a list, return None
        if isinstance(current, list):
            return current
        return current if isinstance(current, dict) else None

    if fmt == "csv":
        reader = _csv.DictReader(_io.StringIO(content))
        fieldnames = reader.fieldnames
        for row in reader:
            doc = {k.lower().strip().replace(" ", "_").replace("-", "_"): v
                   for k, v in row.items() if v.strip()}
            if doc:
                # CSV doesn't have nested _source — apply source_field as a top-level
                # column rename (e.g. user passed source_field="data" to flatten one level)
                if source_field:
                    extracted = _extract_source(doc)
                    if extracted is not None:
                        doc = extracted
                records.append(doc)

    elif fmt == "json":
        try:
            data = _json.loads(content)
            if not isinstance(data, list):
                data = [data]
        except _json.JSONDecodeError:
            raise RuntimeError("Invalid JSON format")
        first_keys = None
        for record in data:
            if not isinstance(record, dict):
                continue
            extracted = _extract_source(record)
            # Row source: extracted may be a list of records
            if isinstance(extracted, list):
                for item in extracted:
                    if not isinstance(item, dict):
                        continue
                    if first_keys is None:
                        first_keys = list(item.keys())
                        fieldnames = first_keys
                    records.append(item)
            elif extracted is not None:
                if first_keys is None:
                    first_keys = list(extracted.keys())
                    fieldnames = first_keys
                records.append(extracted)
            else:
                # source_field didn't resolve — use raw record as fallback
                if first_keys is None:
                    first_keys = list(record.keys())
                    fieldnames = first_keys
                records.append(record)

    elif fmt == "jsonl":
        first_keys = None
        for line in content.strip().split("\n"):
            line = line.strip()
            if not line:
                continue
            try:
                record = _json.loads(line)
                if not isinstance(record, dict):
                    continue
                extracted = _extract_source(record)
                # Row source: extracted may be a list of records
                if isinstance(extracted, list):
                    for item in extracted:
                        if not isinstance(item, dict):
                            continue
                        if first_keys is None:
                            first_keys = list(item.keys())
                            fieldnames = first_keys
                        records.append(item)
                elif extracted is not None:
                    if first_keys is None:
                        first_keys = list(extracted.keys())
                        fieldnames = first_keys
                    records.append(extracted)
                else:
                    # source_field didn't resolve — use raw record as fallback
                    if first_keys is None:
                        first_keys = list(record.keys())
                        fieldnames = first_keys
                    records.append(record)
            except _json.JSONDecodeError:
                continue

    # If all records were skipped or fieldnames never set, use raw keys from first record
    if fieldnames is None and records:
        fieldnames = list(records[0].keys())

    return records, fieldnames


@router.post("/{index}/_bulk", response_model=BulkIndexResponse)
async def bulk_index(
    index: str,
    request: list[dict[str, Any]],
    response: Response,
    http_request: Request = None,
    manager: IndexManager = Depends(get_index_manager),
):
    """Bulk index multiple documents — runs in thread pool so API stays responsive.

    POST /solana_txs/_bulk
    [
        {"signature": "5xJ3k...", "program": "raydium", "amount": 1000000, "fee": 5000, "status": "success"},
        {"signature": "3Ab2k...", "program": "jupiter", "amount": 500000, "fee": 5000, "status": "failed"}
    ]

    Returns a resume token (`doc_offset`, `batch_count`) so clients can resume
    a partial upload without re-sending already-indexed documents.
    """
    index = index.strip().replace(" ", "_")

    if not request or not isinstance(request, list):
        raise HTTPException(400, "Request body must be a list of documents")

    import time as _time

    # Init or update upload progress
    if index not in _upload_progress:
        _upload_progress[index] = {
            "doc_offset": 0, "batch_count": 0, "last_update": _time.time(),
            "bytes_processed": 0, "total_bytes_est": 0,
            "total_estimate": 0, "done_files": 0, "total_files": 0,
        }

    data_dir = manager.data_dir
    index_dir = os.path.join(data_dir, index)

    # Get or create builder (stateful — stays alive across batches)
    builder = _index_builders.get(index)
    if builder is None:
        col_schema = _build_column_schema(request[0])
        os.makedirs(index_dir, exist_ok=True)
        builder = IndexBuilder(
            index_dir,
            column_map={},
            start_doc_id=0,
            dataset=index,
            checkpoint_cb=None,
            delimiter=",",
            columns=None,
            worker_id=None,
            dedup_fields=None,
            doc_id_end=None,
            daemon=True,
        )
        _index_builders[index] = builder
        _add_index_log(index, "indexing_started", f"Indexing started — first batch of up to {len(request)} docs")

    # Offload to thread pool — API event loop is NOT blocked
    loop = asyncio.get_running_loop()
    indexed = await loop.run_in_executor(_bulk_executor, _index_batch, builder, request)

    _upload_progress[index]["doc_offset"] += indexed
    _upload_progress[index]["batch_count"] += 1
    _upload_progress[index]["last_update"] = _time.time()

    # Read real-time stats from request headers
    req_headers = {}
    if http_request and hasattr(http_request, "headers"):
        req_headers = dict(http_request.headers)

    def _h(key, default=None):
        val = req_headers.get(key) or req_headers.get(key.lower())
        return val if val is not None else default

    if _h("x-total-bytes"):
        _upload_progress[index]["bytes_total"] = int(_h("x-total-bytes", 0))
    if _h("x-file-name"):
        _upload_progress[index]["file_name"] = _h("x-file-name", "")
    if _h("x-done-files"):
        _upload_progress[index]["done_files"] = int(_h("x-done-files", 0))
    if _h("x-total-files"):
        _upload_progress[index]["total_files"] = int(_h("x-total-files", 0))
    if _h("x-bytes-processed"):
        _upload_progress[index]["bytes_processed"] = int(_h("x-bytes-processed", 0))
    if _h("x-total-estimate"):
        _upload_progress[index]["total_estimate"] = int(_h("x-total-estimate", 0))
    if _h("x-elapsed"):
        _upload_progress[index]["elapsed"] = int(_h("x-elapsed", 0))
    if _h("x-eta"):
        _upload_progress[index]["eta"] = int(_h("x-eta", 0))
    if _h("x-docs-per-sec"):
        _upload_progress[index]["docs_per_sec"] = float(_h("x-docs-per-sec", 0))

    # Log milestone every 10k docs
    total = _upload_progress[index]["doc_offset"]
    if total > 0 and total % 10000 < indexed:
        _add_index_log(index, "indexing_progress", f"Indexed ~{total:,} docs")

    return {
        "indexed": indexed,
        "errors": None,
        "resume": {
            "doc_offset": _upload_progress[index]["doc_offset"],
            "batch_count": _upload_progress[index]["batch_count"],
        },
    }


@router.get("/_preview_from_url")
async def preview_from_url(
    url: str,
    index: str,
    format: str = "auto",
    sample_size: int = 100,
    source_field: str | None = None,
    manager: IndexManager = Depends(get_index_manager),
):
    """Fetch and preview a remote CSV/JSON/JSONL file — returns headers + sample rows.

    Use this to preview a remote URL before committing to bulk indexing.
    After preview, the user sets column mapping in step 2, then bulk indexes
    via POST /{index}/_bulk.

    For JSON/JSONL, use source_field to extract a nested field (e.g. '_source' for
    Elasticsearch dumps, 'results' for array-of-records paths, 'data.results' for
    nested array paths).

    GET /_preview_from_url?url=https://example.com/data.json&index=my_data&source_field=results
    """
    import time as _time

    # Init progress (without creating builder)
    if index not in _upload_progress:
        _upload_progress[index] = {
            "doc_offset": 0, "batch_count": 0, "last_update": _time.time(),
            "bytes_processed": 0, "total_bytes_est": 0,
            "total_estimate": 0, "done_files": 0, "total_files": 0,
            "file_name": url.split("/")[-1],
        }

    # Run fetch+parse in thread pool (blocking I/O) — non-blocking for the event loop
    loop = asyncio.get_running_loop()
    try:
        records, fieldnames = await loop.run_in_executor(
            _bulk_executor, _fetch_and_parse_url, url, format, source_field,
        )
    except RuntimeError as e:
        raise HTTPException(502, str(e))

    total_rows = len(records)
    sample = records[:sample_size]

    return {
        "index": index,
        "format": format,
        "source_url": url,
        "source_field": source_field,
        "file_name": url.split("/")[-1],
        "total_rows": total_rows,
        "sample_size": len(sample),
        "headers": fieldnames or (list(sample[0].keys()) if sample else []),
        "sample": sample,
    }


@router.get("/_fetch_from_url")
async def fetch_from_url(
    url: str,
    index: str,
    format: str = "auto",
    source_field: str | None = None,
    manager: IndexManager = Depends(get_index_manager),
):
    """Fetch all records from a remote URL (not just a preview).

    Used after preview确认 — returns the full dataset so the frontend can
    stream it via POST /{index}/_bulk in batches.

    For JSON/JSONL, use source_field to extract a nested field (e.g. '_source' for
    Elasticsearch dump format, 'data.results' for dot-notation paths).

    GET /_fetch_from_url?url=https://example.com/data.json&index=my_data&source_field=_source
    """
    loop = asyncio.get_running_loop()
    try:
        records, fieldnames = await loop.run_in_executor(
            _bulk_executor, _fetch_and_parse_url, url, format, source_field,
        )
    except RuntimeError as e:
        raise HTTPException(502, str(e))

    return {
        "index": index,
        "format": format,
        "source_url": url,
        "source_field": source_field,
        "total_rows": len(records),
        "headers": fieldnames or (list(records[0].keys()) if records else []),
        "records": records,
    }


@router.post("/_upload_from_url")
async def upload_from_url(
    url: str,
    index: str,
    format: str = "auto",
    batch_size: int = 5000,
    source_field: str | None = None,
    wait: bool = True,
    manager: IndexManager = Depends(get_index_manager),
):
    """Fetch a remote CSV/JSON/JSONL file and stream it into an index (async by default).

    This is a single-shot endpoint: fetches, parses, and indexes all in one call.
    For large files or when you want to preview before committing, use
    GET /_preview_from_url + GET /_fetch_from_url + POST /{index}/_bulk instead.

    For JSON/JSONL, use source_field to extract a nested field (e.g. '_source' for
    Elasticsearch dump format).

    POST /_upload_from_url?url=https://example.com/data.json&index=my_data&source_field=_source
    POST /_upload_from_url?wait=false — starts in background, returns immediately
    """
    import time as _time

    index = index.strip().replace(" ", "_")

    # Init progress
    if index not in _upload_progress:
        _upload_progress[index] = {
            "doc_offset": 0, "batch_count": 0, "last_update": _time.time(),
            "bytes_processed": 0, "total_bytes_est": 0,
            "total_estimate": 0, "done_files": 0, "total_files": 0,
            "file_name": url.split("/")[-1],
        }

    data_dir = manager.data_dir
    index_dir = os.path.join(data_dir, index)

    # Get or create builder (stateful — stays alive across batches)
    builder = _index_builders.get(index)
    if builder is None:
        os.makedirs(index_dir, exist_ok=True)
        builder = IndexBuilder(
            index_dir,
            column_map={},
            start_doc_id=0,
            dataset=index,
            checkpoint_cb=None,
            delimiter=",",
            columns=None,
            worker_id=None,
            dedup_fields=None,
            doc_id_end=None,
            daemon=True,
        )
        _index_builders[index] = builder
        _add_index_log(index, "indexing_started", f"Indexing started from URL: {url}")

    # Blocking: fetch + normalize + index all in one call
    if wait:
        loop = asyncio.get_running_loop()
        try:
            records, _ = await loop.run_in_executor(
                _bulk_executor, _fetch_and_parse_url, url, format, source_field,
            )
        except RuntimeError as e:
            raise HTTPException(502, str(e))

        normalized: list[dict[str, Any]] = []
        for doc in records:
            norm_doc, _ = _normalize_doc(doc)
            if norm_doc:
                normalized.append({k: str(v) for k, v in norm_doc.items()})

        total_indexed = 0
        for i in range(0, len(normalized), batch_size):
            batch = normalized[i : i + batch_size]
            indexed = await loop.run_in_executor(
                _bulk_executor, _index_batch, builder, batch, True,
            )
            total_indexed += indexed
            _upload_progress[index]["doc_offset"] = total_indexed
            _upload_progress[index]["batch_count"] += 1
            _upload_progress[index]["last_update"] = _time.time()

        _add_index_log(index, "indexing_completed", f"URL indexing done — {total_indexed:,} docs")
        return {
            "index": index,
            "indexed": total_indexed,
            "format": format,
            "source_url": url,
            "source_field": source_field,
            "batch_count": _upload_progress[index]["batch_count"],
        }

    # Non-blocking: run everything in background thread
    _idx = index
    _url = url
    _format = format
    _batch_size = batch_size
    _source_field = source_field
    _mgr = manager

    def _bg_upload():
        import time as _t
        try:
            loop2 = asyncio.new_event_loop()
            asyncio.set_event_loop(loop2)
            records, _ = _fetch_and_parse_url(_url, _format, _source_field)
            normalized = []
            for doc in records:
                norm_doc, _ = _normalize_doc(doc)
                if norm_doc:
                    normalized.append({k: str(v) for k, v in norm_doc.items()})
            bldr = _index_builders.get(_idx)
            total_indexed = 0
            for i in range(0, len(normalized), _batch_size):
                batch = normalized[i : i + _batch_size]
                indexed = _index_batch(bldr, batch, True)
                total_indexed += indexed
                _upload_progress[_idx]["doc_offset"] = total_indexed
                _upload_progress[_idx]["batch_count"] += 1
                _upload_progress[_idx]["last_update"] = _t.time()
            _add_index_log(_idx, "indexing_completed", f"URL indexing done — {total_indexed:,} docs")
        except Exception as e:
            _add_index_log(_idx, "indexing_failed", f"URL indexing failed: {e}")
            print(f"URL indexing failed for '{_idx}': {e}", file=sys.stderr)
        finally:
            try:
                loop2.close()
            except Exception:
                pass

    threading.Thread(target=_bg_upload, daemon=True).start()
    return {"index": index, "status": "started", "url": url}


@router.post("/{index}/_flush", response_model=FlushResponse)
async def flush_index(
    index: str,
    wait: bool = True,
    manager: IndexManager = Depends(get_index_manager),
):
    """Flush in-memory buffers to disk and finalize the index.

    POST /my-index/_flush           — blocks until flush completes
    POST /my-index/_flush?wait=false — starts flush in background, returns immediately

    Poll GET /{index}/_upload_progress for status during background flush.
    """
    index = index.strip().replace(" ", "_")
    builder = _index_builders.get(index)
    if builder is None:
        raise HTTPException(404, f"No pending data for index: {index}")

    if not wait:
        # Non-blocking: run finalize in background thread, return immediately.
        # Keep _upload_progress alive and refresh its timestamp so in_upload stays
        # true during flush — prevents premature "upload done" signals from stats.
        import threading
        import time as _time

        _upload_progress[index]["last_update"] = _time.time()
        _upload_progress[index]["status"] = "flushing"
        _upload_progress[index]["flush_done"] = 0
        _upload_progress[index]["flush_total"] = 0
        _upload_progress[index]["flush_message"] = "Starting flush..."
        _add_index_log(index, "flush_started", "Flush started — writing index buffers in background")

        # Capture manager reference for the bg thread so we can reload engine stats
        _manager = manager

        def _bg_finalize():
            import sys
            import time as _time
            t0 = _time.time()
            progress = {"done": 0, "total": 0}

            def progress_callback(done, total, message):
                progress["done"] = done
                progress["total"] = total
                _upload_progress[index]["flush_done"] = done
                _upload_progress[index]["flush_total"] = total
                _upload_progress[index]["flush_message"] = message
                _upload_progress[index]["last_update"] = _time.time()

            try:
                builder.finalize(progress_callback=progress_callback)
                elapsed = _time.time() - t0
                print(f"Background flush completed for '{index}': {elapsed:.1f}s", file=sys.stderr)
                _add_index_log(index, "flush_completed", f"Flush completed in {elapsed:.1f}s")
                # Reload engine stats from disk so GET /_stats returns updated values
                try:
                    eng = _manager.get_engine(index)
                    eng.reload_stats()
                except Exception as eng_err:
                    print(f"Warning: could not reload engine stats: {eng_err}", file=sys.stderr)
            except Exception as e:
                print(f"Background flush failed for index '{index}': {e}", file=sys.stderr)
                _add_index_log(index, "flush_failed", f"Flush failed: {e}")
            finally:
                _index_builders.pop(index, None)
                _upload_progress.pop(index, None)

        t = threading.Thread(target=_bg_finalize, daemon=True)
        t.start()
        # Give the thread a maximum flush time; if exceeded, assume it succeeded
        # and force-clear so the UI isn't stuck waiting forever.
        def _join_with_timeout():
            t.join(timeout=300)
            if t.is_alive():
                # Flush taking > 5 min — force-clear so in_upload goes false
                _index_builders.pop(index, None)
                _upload_progress.pop(index, None)
                _add_index_log(index, "flush_timeout", "Flush timed out after 300s — forcing clear")
                import sys
                print(f"Flush timeout for index '{index}' after 300s — forcing clear.", file=sys.stderr)
        threading.Thread(target=_join_with_timeout, daemon=True).start()
        return {"_index": index, "status": "flush_started", "docs": 0, "polling": f"/{index}/_upload_progress"}

    # Blocking: wait for finalize to complete (legacy behaviour)
    _add_index_log(index, "flush_started", "Flush started (sync)")
    try:
        stats = builder.finalize()
        _add_index_log(index, "flush_completed", f"Flush completed — {stats.get('total_docs', 0)} docs indexed")
    except Exception as e:
        _add_index_log(index, "flush_failed", f"Flush failed: {e}")
        raise
    finally:
        del _index_builders[index]
        _upload_progress.pop(index, None)

    return {
        "_index": index,
        "status": "flushed",
        "docs": stats.get("total_docs", 0),
    }


@router.get("/{index}/_upload_progress", response_model=UploadProgress)
async def get_upload_progress(
    index: str,
    manager: IndexManager = Depends(get_index_manager),
):
    """Get current upload resume position for an index.

    Returns `{doc_offset, batch_count, last_update, in_progress}`.
    Call this before starting an upload to know where to resume.
    """
    index = index.strip().replace(" ", "_")
    info = _upload_progress.get(index)
    if not info:
        return {"doc_offset": 0, "batch_count": 0, "in_progress": False}
    import time
    age = int(time.time() - info.get("last_update", 0))
    # If last update > 30 min, assume stale — return 0 to start fresh
    if age > 1800:
        _upload_progress.pop(index, None)
        return {"doc_offset": 0, "batch_count": 0, "in_progress": False}
    return {
        "doc_offset": info.get("doc_offset", 0),
        "batch_count": info.get("batch_count", 0),
        "last_update": info.get("last_update"),
        "in_progress": True,
        "bytes_done": info.get("bytes_processed", 0),
        "bytes_total": info.get("bytes_total", 0),
        "file_name": info.get("file_name", ""),
        "done_files": info.get("done_files", 0),
        "total_files": info.get("total_files", 0),
        "total_estimate": info.get("total_estimate", 0),
        "elapsed": info.get("elapsed", 0),
        "eta": info.get("eta", 0),
        "docs_per_sec": info.get("docs_per_sec", 0),
    }


@router.patch("/{index}/_upload_progress")
async def update_upload_progress(
    index: str,
    body: dict,
    manager: IndexManager = Depends(get_index_manager),
):
    """Update upload progress stats for an index.

    PATCH /my-index/_upload_progress
    Used by the client to push real-time progress (bytes, speed, eta) during upload.
    """
    import time as _time
    index = index.strip().replace(" ", "_")
    info = _upload_progress.get(index)
    if not info:
        raise HTTPException(404, f"No upload in progress for index: {index}")
    # Update provided fields
    for key in ["doc_offset", "bytes_processed", "bytes_total", "file_name",
                "done_files", "total_files", "total_estimate", "elapsed", "eta", "docs_per_sec"]:
        if key in body:
            info[key] = body[key]
    info["last_update"] = _time.time()
    return {"status": "ok", "last_update": info["last_update"]}


@router.delete("/{index}", response_model=DeleteResponse)
async def delete_index(
    index: str,
    manager: IndexManager = Depends(get_index_manager),
):
    """Delete an index and all its data. Runs asynchronously."""
    data_dir = manager.data_dir

    index = index.strip().replace(" ", "_")
    index_dir = os.path.join(data_dir, index)
    if not os.path.isdir(index_dir):
        raise HTTPException(404, f"Index not found: {index}")

    # Immediately clear manager cache so subsequent list calls don't return this index
    try:
        manager._engines.pop(index, None)
    except Exception:
        pass

    def _do_delete():
        import shutil
        import time as _time
        try:
            shutil.rmtree(index_dir)
            _add_index_log(index, "index_deleted", "Index deleted")
        except Exception as e:
            print(f"Delete index '{index}' failed: {e}", file=sys.stderr)
        finally:
            # Clear in-memory state
            _index_builders.pop(index, None)
            _upload_progress.pop(index, None)
            _index_logs.pop(index, None)
            # Clear encrypt jobs for this index
            for job_id, job_state in list(_encrypt_jobs.items()):
                if job_state.get("index") == index:
                    _encrypt_jobs.pop(job_id, None)

    threading.Thread(target=_do_delete, daemon=True).start()
    return {"acknowledged": True, "status": "deleting"}


@router.get("/{index}/_logs", response_model=IndexLogs)
async def get_index_logs(
    index: str,
    manager: IndexManager = Depends(get_index_manager),
):
    """Get event logs for an index (indexing_started, flush_completed, errors, etc.)."""
    import json as _json
    index = index.strip().replace(" ", "_")

    # Load from disk if not in memory
    if index not in _index_logs:
        events_path = os.path.join(manager.data_dir, index, "events.json")
        if os.path.isfile(events_path):
            try:
                with open(events_path) as f:
                    _index_logs[index] = _json.load(f)
            except Exception:
                _index_logs[index] = []

    events = _index_logs.get(index, [])
    return {"events": events, "count": len(events)}


@router.post("/{index}/_rename")
async def rename_index(
    index: str,
    body: dict,
    manager: IndexManager = Depends(get_index_manager),
):
    """Rename an index by renaming its folder.

    POST /my-index/_rename  { "new_name": "new-index-name" }
    """
    import shutil
    data_dir = manager.data_dir
    index = index.strip().replace(" ", "_")
    new_name = (body.get("new_name") or "").strip().replace(" ", "_")
    if not new_name:
        raise HTTPException(400, "new_name is required")
    if not new_name.replace("_", "").replace("-", "").isalnum():
        raise HTTPException(400, f"Invalid index name: {new_name}")

    old_dir = os.path.join(data_dir, index)
    new_dir = os.path.join(data_dir, new_name)
    if not os.path.isdir(old_dir):
        raise HTTPException(404, f"Index not found: {index}")
    if os.path.isdir(new_dir):
        raise HTTPException(409, f"Index already exists: {new_name}")

    shutil.move(old_dir, new_dir)

    # Update in-memory references
    if index in _index_builders:
        _index_builders[new_name] = _index_builders[index]
        del _index_builders[index]
    if index in _upload_progress:
        _upload_progress[new_name] = _upload_progress[index]
        _upload_progress.pop(index)
    if index in _index_logs:
        _index_logs[new_name] = _index_logs[index]
        _index_logs.pop(index)
    # Notify manager to refresh
    try:
        manager._on_index_renamed(index, new_name)
    except Exception:
        pass  # manager may not have this hook

    return {"acknowledged": True, "old_name": index, "new_name": new_name}


@router.put("/{index}", response_model=CreateIndexResponse)
async def create_index(
    index: str,
    manager: IndexManager = Depends(get_index_manager),
):
    """Create an empty index with no documents.

    PUT /my-index
    """
    index = index.strip().replace(" ", "_")
    if not index.replace("_", "").replace("-", "").isalnum():
        raise HTTPException(400, f"Invalid index name: {index}")

    data_dir = manager.data_dir
    index_dir = os.path.join(data_dir, index)

    if os.path.isdir(os.path.join(index_dir, "index")):
        return {"status": "exists", "message": f"Index '{index}' already exists", "_index": index}

    os.makedirs(os.path.join(index_dir, "index"), exist_ok=True)

    return {"status": "created", "_index": index}


@router.post("/{index}/_encrypt")
async def encrypt_index(
    index: str,
    body: dict[str, Any] | None = None,
    manager: IndexManager = Depends(get_index_manager),
):
    """Encrypt an index in-place with ChaCha20-Poly1305 (async, progress-tracked).

    POST /my-index/_encrypt
    { "passphrase": "secret" }

    Returns immediately with a job_id. Poll GET /{index}/_encrypt_progress?job_id=xxx
    for progress updates.
    """
    from flatseek.core.query_engine import derive_key, encrypt_bytes, is_encrypted
    import secrets
    from concurrent.futures import ThreadPoolExecutor
    from flatseek.core.builder import FLUSH_THREADS

    data_dir = manager.data_dir
    index_dir = os.path.join(data_dir, index)

    if not os.path.isdir(os.path.join(index_dir, "index")):
        raise HTTPException(404, f"Index not found: {index}")

    enc_path = os.path.join(index_dir, "encryption.json")
    if os.path.isfile(enc_path):
        with open(enc_path) as f:
            meta = json.load(f)
        salt = bytes.fromhex(meta["salt"])
    else:
        salt = secrets.token_bytes(32)

    passphrase = (body or {}).get("passphrase") if body else None
    if not passphrase:
        raise HTTPException(400, "passphrase is required")

    key = derive_key(passphrase, salt)

    # Generate a verification token to prove the password is correct
    verify_token = secrets.token_bytes(16)
    verify_ciphertext = encrypt_bytes(b"SEEKIO_VERIFY_" + verify_token, key)

    with open(enc_path, "w") as f:
        json.dump({
            "salt": salt.hex(),
            "algorithm": "ChaCha20-Poly1305",
            "kdf": "PBKDF2-HMAC-SHA256",
            "iterations": 600_000,
            "index": index,
            "verify_token": verify_ciphertext.hex(),
        }, f, indent=2)

    targets = []
    index_subdir = os.path.join(index_dir, "index")
    docs_dir = os.path.join(index_dir, "docs")
    for root, _, files in os.walk(index_subdir):
        for fn in files:
            if fn.endswith(".bin"):
                targets.append(os.path.join(root, fn))
    for root, _, files in os.walk(docs_dir):
        for fn in files:
            if fn.endswith(".zlib"):
                targets.append(os.path.join(root, fn))

    job_id = str(uuid.uuid4())
    total = len(targets)
    state = _encrypt_jobs[job_id] = {
        "index": index,
        "op": "encrypt",
        "done": 0,
        "total": total,
        "status": "running",
        "error": None,
        "created_at": time.time(),
    }
    lock = threading.Lock()

    def _encrypt_file_safe(path):
        try:
            with open(path, "rb") as f:
                data = f.read()
            if not data or is_encrypted(data):
                with lock:
                    state["done"] += 1
                return
            enc = encrypt_bytes(data, key)
            tmp = path + f".{os.getpid()}.etmp"
            with open(tmp, "wb") as f:
                f.write(enc)
            os.replace(tmp, path)
            with lock:
                state["done"] += 1
        except Exception:
            with lock:
                state["done"] += 1

    def _run():
        try:
            with ThreadPoolExecutor(max_workers=FLUSH_THREADS) as ex:
                list(ex.map(_encrypt_file_safe, targets))
            manager._engines.pop(index, None)
            with lock:
                state["status"] = "done"
        except Exception as e:
            with lock:
                state["status"] = "error"
                state["error"] = str(e)

    threading.Thread(target=_run, daemon=True).start()

    return {"job_id": job_id, "index": index, "status": "started", "total_files": total}


@router.get("/{index}/_encrypt_progress")
async def encrypt_progress(
    index: str,
    job_id: str = None,
    manager: IndexManager = Depends(get_index_manager),
):
    """Poll encrypt/decrypt job progress.

    GET /my-index/_encrypt_progress?job_id=xxx
    Returns { done, total, status, error }.
    """
    if not job_id:
        raise HTTPException(400, "job_id is required")

    # Cleanup old jobs (> 5 min old) to prevent unbounded growth
    cutoff = time.time() - 300
    for jid in list(_encrypt_jobs.keys()):
        s = _encrypt_jobs.get(jid)
        if s and s.get("created_at", 0) < cutoff:
            _encrypt_jobs.pop(jid, None)

    state = _encrypt_jobs.get(job_id)
    if not state:
        raise HTTPException(404, "Job not found or already cleaned up")
    if state.get("index") != index:
        raise HTTPException(404, "Job not found for this index")
    return {
        "done": state["done"],
        "total": state["total"],
        "status": state["status"],
        "error": state.get("error"),
    }


@router.post("/{index}/_decrypt")
async def decrypt_index(
    index: str,
    body: dict[str, Any] | None = None,
    manager: IndexManager = Depends(get_index_manager),
):
    """Decrypt an encrypted index in-place.

    POST /my-index/_decrypt
    { "passphrase": "secret" }
    """
    from flatseek.core.query_engine import decrypt_bytes, is_encrypted, load_encryption_key
    from concurrent.futures import ThreadPoolExecutor
    from flatseek.core.builder import FLUSH_THREADS

    data_dir = manager.data_dir
    index_dir = os.path.join(data_dir, index)

    if not os.path.isdir(os.path.join(index_dir, "index")):
        raise HTTPException(404, f"Index not found: {index}")

    enc_path = os.path.join(index_dir, "encryption.json")
    if not os.path.isfile(enc_path):
        raise HTTPException(400, "Index is not encrypted — no encryption.json found")

    passphrase = (body or {}).get("passphrase") if body else None
    if not passphrase:
        raise HTTPException(400, "passphrase is required")

    try:
        key = load_encryption_key(index_dir, passphrase)
    except FileNotFoundError:
        raise HTTPException(400, "Index is not encrypted")
    except Exception:
        raise HTTPException(401, "Invalid passphrase — decryption failed. Check your password.")

    targets = []
    index_subdir = os.path.join(index_dir, "index")
    docs_dir = os.path.join(index_dir, "docs")
    for root, _, files in os.walk(index_subdir):
        for fn in files:
            if fn.endswith(".bin"):
                targets.append(os.path.join(root, fn))
    for root, _, files in os.walk(docs_dir):
        for fn in files:
            if fn.endswith(".zlib"):
                targets.append(os.path.join(root, fn))

    job_id = str(uuid.uuid4())
    total = len(targets)
    state = _encrypt_jobs[job_id] = {
        "index": index,
        "op": "decrypt",
        "done": 0,
        "total": total,
        "status": "running",
        "error": None,
        "created_at": time.time(),
    }
    lock = threading.Lock()

    def _decrypt_file_safe(path):
        try:
            with open(path, "rb") as f:
                data = f.read()
            if not data or not is_encrypted(data):
                with lock:
                    state["done"] += 1
                return
            dec = decrypt_bytes(data, key)
            tmp = path + f".{os.getpid()}.dtmp"
            with open(tmp, "wb") as f:
                f.write(dec)
            os.replace(tmp, path)
            with lock:
                state["done"] += 1
        except Exception:
            with lock:
                state["done"] += 1

    def _run():
        try:
            with ThreadPoolExecutor(max_workers=FLUSH_THREADS) as ex:
                list(ex.map(_decrypt_file_safe, targets))
            # Remove encryption.json
            try:
                os.remove(enc_path)
            except OSError:
                pass
            manager._engines.pop(index, None)
            with lock:
                state["status"] = "done"
        except Exception as e:
            with lock:
                state["status"] = "error"
                state["error"] = str(e)

    threading.Thread(target=_run, daemon=True).start()

    return {"job_id": job_id, "index": index, "status": "started", "total_files": total}


@router.get("/{index}/_is_encrypted")
async def is_index_encrypted(
    index: str,
    manager: IndexManager = Depends(get_index_manager),
):
    """Check if an index is encrypted."""
    data_dir = manager.data_dir
    index_dir = os.path.join(data_dir, index)

    if not os.path.isdir(os.path.join(index_dir, "index")):
        raise HTTPException(404, f"Index not found: {index}")

    return {"index": index, "encrypted": manager.is_encrypted(index)}


@router.post("/{index}/_authenticate")
async def authenticate_index(
    index: str,
    body: dict[str, Any] | None = None,
    manager: IndexManager = Depends(get_index_manager),
):
    """Submit password for an encrypted index.

    POST /my-index/_authenticate
    { "passphrase": "secret" }

    On success, the password is stored in the session and subsequent
    search/aggregate/map requests will use it automatically.
    """
    from flatseek.core.query_engine import load_encryption_key, decrypt_bytes

    data_dir = manager.data_dir
    index_dir = os.path.join(data_dir, index)

    if not os.path.isdir(os.path.join(index_dir, "index")):
        raise HTTPException(404, f"Index not found: {index}")

    enc_path = os.path.join(index_dir, "encryption.json")
    if not os.path.isfile(enc_path):
        raise HTTPException(400, "Index is not encrypted — no encryption.json found in index folder")

    passphrase = (body or {}).get("passphrase") if body else None
    if not passphrase:
        raise HTTPException(400, "passphrase is required")

    try:
        key = load_encryption_key(index_dir, passphrase)
    except FileNotFoundError:
        raise HTTPException(400, "Index is not encrypted")

    # Verify passphrase by decrypting the stored verification token
    try:
        with open(enc_path) as f:
            meta = json.load(f)
        verify_ct = bytes.fromhex(meta["verify_token"])
        decrypted = decrypt_bytes(verify_ct, key)
        if not decrypted.startswith(b"SEEKIO_VERIFY_"):
            return {"authenticated": False, "index": index}
    except Exception:
        return {"authenticated": False, "index": index}

    manager.set_password(index, passphrase)

    # Pre-load the key into the engine so the next request doesn't need to re-derive
    try:
        engine = manager.get_engine(index)
        engine.set_key(key)
    except Exception:
        pass

    return {"authenticated": True, "index": index}


@router.delete("/{index}/_authenticate")
async def logout_index(
    index: str,
    manager: IndexManager = Depends(get_index_manager),
):
    """Clear stored password for an encrypted index.

    DELETE /my-index/_authenticate
    """
    manager.clear_password(index)
    return {"cleared": True, "index": index}


@router.put("/{index}/_mapping")
async def put_mapping(
    index: str,
    body: dict[str, Any],
    manager: IndexManager = Depends(get_index_manager),
):
    """Create or update index mapping.

    PUT /my-index/_mapping
    {
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"}
        }
    }
    """
    index = index.strip().replace(" ", "_")
    if not index.replace("_", "").replace("-", "").isalnum():
        raise HTTPException(400, f"Invalid index name: {index}")

    data_dir = manager.data_dir
    index_dir = os.path.join(data_dir, index)
    os.makedirs(os.path.join(index_dir, "index"), exist_ok=True)

    mapping_path = os.path.join(index_dir, "mapping.json")
    with open(mapping_path, 'w') as f:
        json.dump(body, f)

    return {"acknowledged": True, "index": index, "mapping": body}


@router.get("/{index}/_mapping", response_model=IndexMapping)
async def get_mapping(
    index: str,
    request: Request = None,
    manager: IndexManager = Depends(get_index_manager),
):
    """Get the column type mapping for an index."""
    index_dir = os.path.join(manager.data_dir, index)
    # Try to authenticate if encrypted — but don't block mapping if no password
    if manager.is_encrypted(index):
        stored_pass = manager.get_password(index)
        if not stored_pass and request:
            stored_pass = request.headers.get("x-index-password")
        if stored_pass:
            try:
                from flatseek.core.query_engine import load_encryption_key
                key = load_encryption_key(index_dir, stored_pass)
                manager.get_engine(index).set_key(key)
                manager.set_password(index, stored_pass)
            except Exception:
                pass  # Password invalid — continue without decrypting

    try:
        engine = manager.get_engine(index)
    except Exception as e:
        raise HTTPException(404, f"Index not found or cannot load: {e}")

    try:
        columns = engine.columns()
    except Exception as e:
        # Encrypted index without key — return minimal mapping
        return {
            "_index": index,
            "mappings": {"properties": {}},
            "_mapping_error": str(e),
        }

    return {
        "_index": index,
        "mappings": {
            "properties": {
                col: {"type": sem_type}
                for col, sem_type in columns.items()
            }
        }
    }


@router.get("/{index}/_stats", response_model=IndexStats)
async def get_stats(
    index: str,
    request: Request = None,
    manager: IndexManager = Depends(get_index_manager),
):
    """Get index statistics: document count, size, columns, encryption status."""
    index_dir = os.path.join(manager.data_dir, index)
    # Try to authenticate if encrypted — but don't block stats if no password
    if manager.is_encrypted(index):
        stored_pass = manager.get_password(index)
        if not stored_pass and request:
            stored_pass = request.headers.get("x-index-password")
        if stored_pass:
            try:
                from flatseek.core.query_engine import load_encryption_key
                key = load_encryption_key(index_dir, stored_pass)
                manager.get_engine(index).set_key(key)
                manager.set_password(index, stored_pass)
            except Exception:
                pass  # Password invalid — continue without decrypting

    try:
        engine = manager.get_engine(index)
    except Exception as e:
        raise HTTPException(404, f"Index not found or cannot load: {e}")

    try:
        stats = engine.stats
    except Exception as e:
        # Encrypted index without key — return minimal stats
        return {
            "_index": index,
            "docs": {"count": 0, "deleted": 0},
            "store": {"size_bytes": 0, "index_size_mb": 0, "docs_size_mb": 0},
            "encrypted": manager.is_encrypted(index),
            "columns": {},
            "in_upload": False,
            "upload": None,
            "encrypt_job": None,
            "_stats_error": str(e),
        }

    # Use total_size_mb (index + docs combined) for store size
    total_mb = stats.get("total_size_mb", stats.get("docs_size_mb", 0))
    # Normalize index name to match bulk_index's normalization
    index_normalized = index.strip().replace(" ", "_")
    # Check if index has an in-progress upload or encrypt job
    upload_info = _upload_progress.get(index_normalized)
    has_builder = index_normalized in _index_builders
    upload_stale_time = time.time() - upload_info.get("last_update", 0) if upload_info else 999
    # Upload is "interrupted" if _upload_progress has data but no active builder (builder died)
    upload_interrupted = upload_info is not None and not has_builder and upload_stale_time < 600
    # Active upload: has builder AND (normal batch upload OR flush-in-progress with builder still present)
    flush_status = upload_info.get("status") if upload_info else None
    in_upload = (
        upload_info is not None
        and (
            # Normal batch upload — builder is live
            (has_builder and upload_stale_time < 600)
            # Flush-in-progress — builder might still be in memory momentarily after stats is called
            # during the race window. Only count as "in_upload" if builder is still present.
            or flush_status == "flushing"
        )
    ) and upload_stale_time < 600
    encrypt_info = None
    for job in _encrypt_jobs.values():
        if job.get("index") == index_normalized and job.get("status") == "running":
            encrypt_info = {"op": job.get("op"), "done": job.get("done"), "total": job.get("total"), "status": job.get("status")}
            break
    return {
        "_index": index,
        "docs": {
            "count": stats.get("total_docs", 0),
            "deleted": 0,
        },
        "store": {
            "size_bytes": int(total_mb * 1024 * 1024),
            "index_size_mb": stats.get("index_size_mb", 0),
            "docs_size_mb": stats.get("docs_size_mb", 0),
        },
        "encrypted": manager.is_encrypted(index),
        "columns": stats.get("columns", {}),
        "in_upload": in_upload,
        "upload_interrupted": upload_interrupted,
        "flush_progress": {
            "done": upload_info.get("flush_done", 0) if upload_info else 0,
            "total": upload_info.get("flush_total", 0) if upload_info else 0,
            "message": upload_info.get("flush_message", "") if upload_info else "",
        } if flush_status == "flushing" and upload_info else None,
        "upload": {
            "doc_offset": upload_info.get("doc_offset", 0),
            "batch_count": upload_info.get("batch_count", 0),
            "bytes_done": upload_info.get("bytes_processed", 0),
            "bytes_total": upload_info.get("bytes_total", 0),
            "file_name": upload_info.get("file_name", ""),
            "done_files": upload_info.get("done_files", 0),
            "total_files": upload_info.get("total_files", 0),
            "total_estimate": upload_info.get("total_estimate", 0),
            "elapsed": upload_info.get("elapsed", 0),
            "eta": upload_info.get("eta", 0),
            "docs_per_sec": upload_info.get("docs_per_sec", 0),
        } if upload_info else None,
        "encrypt_job": encrypt_info,
    }