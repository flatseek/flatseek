"""Document-level delete and update routes for flatseek API."""

from __future__ import annotations

import json
import os
import time
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

from flatseek.api.deps import get_index_manager, IndexManager
from flatseek.core.builder import IndexBuilder


router = APIRouter(tags=["documents"])


# ─── Request/Response Schemas ──────────────────────────────────────────────


class DeleteByIdResponse(BaseModel):
    result: str  # "deleted" | "not_found"
    doc_id: str | int


class DeleteByQueryResponse(BaseModel):
    result: str = "deleted"
    query: str
    deleted: int


class UpsertResponse(BaseModel):
    result: str  # "created" | "updated"
    doc_id: str | int
    new_doc_id: int | None = None


class UpdateByQueryResponse(BaseModel):
    result: str = "updated"
    query: str
    updated: int
    new_doc_ids: list[int]


# ─── Helpers ──────────────────────────────────────────────────────────────


def _check_not_fsk(manager: IndexManager, index: str) -> None:
    """Raise 409 if index is a packed .fsk file (read-only)."""
    manager._discover_local_fsk()
    if index in manager._fsk_index_map:
        raise HTTPException(
            409,
            f"Cannot modify packed .fsk index '{index}'. "
            "Unpack first: flatseek unpack {index}.fsk"
        )


def _get_id_field(manager: IndexManager, index: str) -> str | None:
    """Get _id_field from stats.json, or None if not set."""
    data_dir = manager.data_dir
    stats_path = os.path.join(data_dir, index, "stats.json")
    if not os.path.exists(stats_path):
        return None
    try:
        stats = json.loads(open(stats_path).read())
        return stats.get("_id_field")
    except Exception:
        return None


def _normalize_doc(doc: dict[str, Any]) -> tuple[dict[str, str], list[str]]:
    """Normalize document values to strings (as builder expects)."""
    normalized = {}
    fields = []
    for k, v in doc.items():
        if isinstance(v, (dict, list)):
            normalized[k] = json.dumps(v)
        else:
            normalized[k] = str(v)
        fields.append(k)
    return normalized, fields


# ─── DELETE /{index}/_doc/{id} ────────────────────────────────────────────


@router.delete("/{index}/_doc/{id}", response_model=DeleteByIdResponse)
async def delete_by_id(
    index: str,
    id: str,
    manager: IndexManager = Depends(get_index_manager),
):
    """Delete a single document by its natural key value.

    DELETE /myindex/_doc/user@example.com
    → Deletes the doc where _id_field = "user@example.com"
    """
    _check_not_fsk(manager, index)

    id_field = _get_id_field(manager, index)
    if not id_field:
        raise HTTPException(400, "Index was not built with --id-field. Cannot delete by ID.")

    engine = manager.get_engine(index)
    if engine._tombstones is None:
        raise HTTPException(400, "Index does not support delete operations.")

    existing_doc_id = engine._tombstones.get_doc_id_for_field(id_field, id)
    if existing_doc_id is None:
        return DeleteByIdResponse(result="not_found", doc_id=id)

    existing_ver = engine._tombstones.get_version(existing_doc_id)
    count, conflicts = engine._tombstones.mark_deleted(
        [existing_doc_id], expected_versions={existing_doc_id: existing_ver}
    )
    if conflicts:
        raise HTTPException(409, f"Concurrent modification detected for doc_id={existing_doc_id}. Retry.")
    engine._tombstones.remove_doc_id_for_field(id_field, id)

    return DeleteByIdResponse(result="deleted", doc_id=id)


@router.delete("/{index}/_doc", response_model=DeleteByQueryResponse)
async def delete_by_query(
    index: str,
    q: str = Query(..., description="Lucene query to select documents to delete"),
    manager: IndexManager = Depends(get_index_manager),
):
    """Delete all documents matching a Lucene query.

    DELETE /myindex/_doc?q=email:*@gmail.com
    """
    _check_not_fsk(manager, index)

    engine = manager.get_engine(index)
    if engine._tombstones is None:
        raise HTTPException(400, "Index does not support delete operations.")

    result = engine.query(q, page=0, page_size=100_000)
    doc_ids = [doc["_id"] for doc in result.get("results", [])]

    if not doc_ids:
        return DeleteByQueryResponse(query=q, deleted=0)

    deleted_count, conflicts = engine._tombstones.mark_deleted(doc_ids)
    if conflicts:
        return DeleteByQueryResponse(query=q, deleted=deleted_count)

    return DeleteByQueryResponse(query=q, deleted=deleted_count)


# ─── PUT /{index}/_doc/{id} ───────────────────────────────────────────────


@router.put("/{index}/_doc/{id}", response_model=UpsertResponse)
async def upsert_by_id(
    index: str,
    id: str,
    request: dict[str, Any],
    manager: IndexManager = Depends(get_index_manager),
):
    """Insert or replace a document by its natural key.

    PUT /myindex/_doc/user@example.com
    {"email": "user@example.com", "status": "active", "name": "Alice"}

    If the document already exists (same _id_field value), it is replaced.
    Uses optimistic locking: if concurrent modification detected, retries up to 3 times.
    Returns the new doc_id assigned.
    """
    _check_not_fsk(manager, index)

    id_field = _get_id_field(manager, index)
    if not id_field:
        raise HTTPException(400, "Index was not built with --id-field. Cannot upsert by ID.")

    is_update = False
    existing_doc_id = None

    for attempt in range(3):
        engine = manager.get_engine(index)
        if engine._tombstones is None:
            raise HTTPException(400, "Index does not support update operations.")

        existing_doc_id = engine._tombstones.get_doc_id_for_field(id_field, id)
        is_update = existing_doc_id is not None
        existing_ver = engine._tombstones.get_version(existing_doc_id) if existing_doc_id else 0

        if is_update:
            count, conflicts = engine._tombstones.mark_deleted(
                [existing_doc_id], expected_versions={existing_doc_id: existing_ver}
            )
            if conflicts:
                manager._engines.pop(index, None)
                continue

        break

    data_dir = manager.data_dir
    index_dir = os.path.join(data_dir, index)
    os.makedirs(index_dir, exist_ok=True)

    import fcntl
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
                stats_path = os.path.join(index_dir, "stats.json")
                if os.path.exists(stats_path):
                    try:
                        stats = json.loads(open(stats_path).read())
                        total_docs = stats.get("total_docs", 0)
                    except Exception:
                        total_docs = 0
                else:
                    total_docs = 0
            new_doc_id = total_docs
            with open(counter_path, "w") as cf:
                cf.write(str(total_docs + 1))

            builder = IndexBuilder(
                index_dir,
                column_map={},
                start_doc_id=total_docs,
                dataset=index,
                checkpoint_cb=None,
                delimiter=",",
                columns=None,
                worker_id=None,
                dedup_fields=None,
                doc_id_end=None,
                daemon=False,
                id_field=id_field,
            )

            normalized, fields = _normalize_doc(request)
            builder.add_row(normalized, fields, file_rel=None, col_schema=None)
            builder.finalize()

            manager._engines.pop(index, None)
            engine = manager.get_engine(index)
            engine._tombstones.set_doc_id_for_field(id_field, id, new_doc_id)
        finally:
            fcntl.flock(lockf.fileno(), fcntl.LOCK_UN)

    return UpsertResponse(
        result="updated" if is_update else "created",
        doc_id=id,
        new_doc_id=new_doc_id,
    )


@router.put("/{index}/_doc", response_model=UpdateByQueryResponse)
async def update_by_query(
    index: str,
    q: str = Query(..., description="Lucene query to select documents to update"),
    request: dict[str, Any] = Body(...),
    manager: IndexManager = Depends(get_index_manager),
):
    """Update all documents matching a Lucene query.

    PUT /myindex/_doc?q=email:*@gmail.com
    {"status": "verified"}

    Partial update: merges new fields with existing documents.
    Full replacement: replaces entire document (include all fields).
    """
    _check_not_fsk(manager, index)

    id_field = _get_id_field(manager, index)

    for attempt in range(3):
        engine = manager.get_engine(index)
        if engine._tombstones is None:
            raise HTTPException(400, "Index does not support update operations.")

        result = engine.query(q, page=0, page_size=100_000)
        docs = result.get("results", [])

        if not docs:
            return UpdateByQueryResponse(query=q, updated=0, new_doc_ids=[])

        expected_versions = {}
        for doc in docs:
            doc_id = doc["_id"]
            expected_versions[doc_id] = engine._tombstones.get_version(doc_id)

        deleted_count, conflicts = engine._tombstones.mark_deleted(
            [doc["_id"] for doc in docs],
            expected_versions=expected_versions
        )

        if conflicts:
            manager._engines.pop(index, None)
            continue

        break

    data_dir = manager.data_dir
    index_dir = os.path.join(data_dir, index)

    stats_path = os.path.join(index_dir, "stats.json")
    total_docs = 0
    if os.path.exists(stats_path):
        try:
            stats = json.loads(open(stats_path).read())
            total_docs = stats.get("total_docs", 0)
        except Exception:
            pass

    new_doc_ids = []
    old_doc_ids = []

    for doc in docs:
        old_doc_id = doc["_id"]
        old_doc_ids.append(old_doc_id)

        merged = {k: v for k, v in doc.items() if not k.startswith("_")}
        merged = {k: v for k, v in merged.items() if k != "_id"}

        for k, v in request.items():
            if isinstance(v, (dict, list)):
                merged[k] = json.dumps(v)
            else:
                merged[k] = str(v)

        builder = IndexBuilder(
            index_dir,
            column_map={},
            start_doc_id=total_docs,
            dataset=index,
            checkpoint_cb=None,
            delimiter=",",
            columns=None,
            worker_id=None,
            dedup_fields=None,
            doc_id_end=None,
            daemon=True,
        )

        fields = list(merged.keys())
        normalized = {k: (json.dumps(v) if isinstance(v, (dict, list)) else str(v)) for k, v in merged.items()}
        builder.add_row(normalized, fields, file_rel=None, col_schema=None)
        builder.finalize()

        new_doc_ids.append(total_docs)

        if id_field and id_field in merged:
            manager._engines.pop(index, None)
            engine = manager.get_engine(index)
            engine._tombstones.set_doc_id_for_field(id_field, str(merged[id_field]), total_docs)

        total_docs += 1

    return UpdateByQueryResponse(query=q, updated=len(old_doc_ids), new_doc_ids=new_doc_ids)


# ─── POST /{index}/_bulk ──────────────────────────────────────────────────
# Elasticsearch-compatible bulk API.
#
# Request format (NDJSON — one JSON object per line):
#   {"index": {"_id": "1"}}
#   {"email": "alice@example.com", "name": "Alice"}
#   {"create": {"_id": "2"}}
#   {"email": "bob@example.com", "name": "Bob"}
#   {"update": {"_id": "1"}}
#   {"doc": {"status": "inactive"}}
#   {"delete": {"_id": "3"}}
#
# Action lines: exactly one of "index", "create", "update", "delete" must be set.
# For index/create: next line is the full document body.
# For update: next line is {"doc": {...}} (partial merge) or full doc for replacement.
# For delete: no next line needed.
#
# Response is also NDJSON — one result object per action line, in order.
# Lines alternate with action lines: result, result, result, ...
# For index/create: status 200/201 on success, 409 if doc already exists (create only).
# For update: status 200 on success, 404 if doc not found.
# For delete: status 200 on success, 404 if doc not found.


class BulkResultItem(BaseModel):
    """Result for one bulk operation — mirrors ES bulk response item format."""

    operation: str  # "index" | "create" | "update" | "delete"
    doc_id: str
    status: int  # 200/201 = ok, 404 = not found, 409 = conflict
    result: str | None = None  # "created" | "updated" | "deleted" | "not_found" | "conflict"
    new_doc_id: int | None = None
    error: str | None = None


class BulkResponse(BaseModel):
    took: int  # milliseconds
    errors: bool  # true if any item had status >= 400
    results: list[BulkResultItem]
    total_created: int
    total_updated: int
    total_deleted: int
    total_conflicts: int


@router.post("/{index}/_bulk_documents")
async def bulk(
    index: str,
    request: Request,
    manager: IndexManager = Depends(get_index_manager),
):
    """Elasticsearch-compatible bulk API.

    Accepts NDJSON (newline-delimited JSON) — one JSON object per line.

    Action line types (exactly one must be set):
      - index  → upsert (insert or replace) by _id
      - create → insert only; fails if _id already exists
      - update → partial update by _id; merges fields into existing doc
      - delete → delete by _id

    For index/create/update: the next line is the document body.

    Example NDJSON request:
      {"index": {"_id": "1"}}
      {"email": "alice@example.com", "name": "Alice"}
      {"create": {"_id": "2"}}
      {"email": "bob@example.com", "name": "Bob"}
      {"update": {"_id": "1"}}
      {"doc": {"status": "inactive"}}
      {"delete": {"_id": "3"}}

    Response is also NDJSON — one result object per action line.
    Compatible with the official elasticsearch-py client.
    """
    _check_not_fsk(manager, index)

    id_field = _get_id_field(manager, index)
    if not id_field:
        raise HTTPException(400, "Index was not built with --id-field. Cannot use bulk upsert/update/delete by ID.")

    body = await request.body()
    if not body.strip():
        return BulkResponse(
            took=0, errors=False, results=[],
            total_created=0, total_updated=0, total_deleted=0, total_conflicts=0
        )

    # Parse NDJSON: alternate action lines and doc lines
    lines = body.decode("utf-8").splitlines()
    actions: list[dict[str, Any]] = []
    docs: list[dict[str, Any] | None] = []

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line:
            i += 1
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            raise HTTPException(400, f"Invalid JSON on line {i}: {line[:100]}")
        # Determine which action type this is
        action_type = None
        action_meta: dict[str, Any] = {}
        for key in ("index", "create", "update", "delete"):
            if key in obj:
                action_type = key
                action_meta = obj[key] if isinstance(obj[key], dict) else {}
                break
        if action_type is None:
            raise HTTPException(400, f"Unknown bulk action on line {i}: {line[:100]}. Expected one of: index, create, update, delete.")

        actions.append({"type": action_type, "meta": action_meta})

        if action_type in ("index", "create", "update"):
            # Next line is the document
            i += 1
            if i >= len(lines):
                raise HTTPException(400, f"Missing document body after action line {i-1}")
            doc_line = lines[i].strip()
            if not doc_line:
                docs.append({})
            else:
                try:
                    docs.append(json.loads(doc_line))
                except json.JSONDecodeError:
                    raise HTTPException(400, f"Invalid JSON document on line {i}: {doc_line[:100]}")
        else:
            # delete has no document
            docs.append(None)
        i += 1

    # Execute all actions
    data_dir = manager.data_dir
    index_dir = os.path.join(data_dir, index)
    os.makedirs(index_dir, exist_ok=True)

    import fcntl
    counter_path = os.path.join(index_dir, ".next_doc_id")
    lock_path = os.path.join(index_dir, ".doc_id.lock")
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)

    start_time = time.time()
    results: list[BulkResultItem] = []
    total_created = 0
    total_updated = 0
    total_deleted = 0
    total_conflicts = 0
    errors = False

    with open(lock_path, "a") as lockf:
        fcntl.flock(lockf.fileno(), fcntl.LOCK_EX)
        try:
            # Read counter
            if os.path.exists(counter_path):
                try:
                    doc_id_counter = int(open(counter_path).read().strip())
                except (ValueError, IOError):
                    doc_id_counter = 0
            else:
                stats_path = os.path.join(index_dir, "stats.json")
                if os.path.exists(stats_path):
                    try:
                        stats = json.loads(open(stats_path).read())
                        doc_id_counter = stats.get("total_docs", 0)
                    except Exception:
                        doc_id_counter = 0
                else:
                    doc_id_counter = 0

            engine = manager.get_engine(index)
            if engine._tombstones is None:
                raise HTTPException(400, "Index does not support bulk operations.")

            # Process index/create actions: build plan first (before any writes)
            # For create: need to check existence before writing
            write_plan: list[tuple[int, str, dict[str, Any] | None, str]] = []
            # (action_idx, doc_id_str, doc, action_type)

            for idx, action in enumerate(actions):
                action_type = action["type"]
                doc_id_str = action["meta"].get("_id")
                doc = docs[idx]

                if action_type == "delete":
                    # Resolve doc_id for delete
                    resolved_id = None
                    if doc_id_str and id_field:
                        resolved_id = engine._tombstones.get_doc_id_for_field(id_field, doc_id_str)
                    if resolved_id is None:
                        results.append(BulkResultItem(
                            operation=action_type,
                            doc_id=doc_id_str or "",
                            status=404,
                            result="not_found",
                            error="Document not found",
                        ))
                        total_conflicts += 1
                        errors = True
                    else:
                        ver = engine._tombstones.get_version(resolved_id)
                        count, conflicts = engine._tombstones.mark_deleted(
                            [resolved_id], expected_versions={resolved_id: ver}
                        )
                        if conflicts:
                            results.append(BulkResultItem(
                                operation=action_type,
                                doc_id=doc_id_str or "",
                                status=409,
                                result="conflict",
                                error="Concurrent modification",
                            ))
                            total_conflicts += 1
                            errors = True
                        else:
                            if id_field and doc_id_str:
                                engine._tombstones.remove_doc_id_for_field(id_field, doc_id_str)
                            results.append(BulkResultItem(
                                operation=action_type,
                                doc_id=doc_id_str or "",
                                status=200,
                                result="deleted",
                            ))
                            total_deleted += 1

                elif action_type in ("index", "create"):
                    # Check existence for create
                    existing_doc_id = None
                    if id_field and doc_id_str:
                        existing_doc_id = engine._tombstones.get_doc_id_for_field(id_field, doc_id_str)

                    if action_type == "create" and existing_doc_id is not None:
                        results.append(BulkResultItem(
                            operation=action_type,
                            doc_id=doc_id_str or "",
                            status=409,
                            result="conflict",
                            error="Document already exists",
                        ))
                        total_conflicts += 1
                        errors = True
                        continue

                    # Plan write — is_update = existing doc was found (for index action)
                    is_update = existing_doc_id is not None
                    write_plan.append((idx, doc_id_str or "", doc, action_type, is_update))

                elif action_type == "update":
                    # Resolve doc_id for update
                    resolved_id = None
                    if doc_id_str and id_field:
                        resolved_id = engine._tombstones.get_doc_id_for_field(id_field, doc_id_str)

                    if resolved_id is None:
                        results.append(BulkResultItem(
                            operation=action_type,
                            doc_id=doc_id_str or "",
                            status=404,
                            result="not_found",
                            error="Document not found",
                        ))
                        total_conflicts += 1
                        errors = True
                        continue

                    # Tombstone old version
                    ver = engine._tombstones.get_version(resolved_id)
                    count, conflicts = engine._tombstones.mark_deleted(
                        [resolved_id], expected_versions={resolved_id: ver}
                    )
                    if conflicts:
                        results.append(BulkResultItem(
                            operation=action_type,
                            doc_id=doc_id_str or "",
                            status=409,
                            result="conflict",
                            error="Concurrent modification",
                        ))
                        total_conflicts += 1
                        errors = True
                        continue

                    # Plan write (update is a full-replacement of merged doc)
                    if doc is None:
                        doc = {}
                    update_fields = doc.get("doc", doc)
                    # Fetch existing doc for merge
                    existing_docs = engine.query(f"_id:{resolved_id}", page=0, page_size=1)
                    existing_doc = {}
                    for d in existing_docs.get("results", []):
                        if d["_id"] == resolved_id:
                            existing_doc = {k: v for k, v in d.items() if not k.startswith("_") and k != "_id"}
                            break
                    merged = dict(existing_doc)
                    merged.update(update_fields)
                    write_plan.append((idx, doc_id_str or "", merged, action_type, True))  # is_update=True for update action

            # Execute all writes in one builder (the key efficiency gain)
            if write_plan:
                builder = IndexBuilder(
                    index_dir,
                    column_map={},
                    start_doc_id=doc_id_counter,
                    dataset=index,
                    checkpoint_cb=None,
                    delimiter=",",
                    columns=None,
                    worker_id=None,
                    dedup_fields=None,
                    doc_id_end=None,
                    daemon=False,
                )

                id_to_new_doc_id: dict[int, int] = {}
                for action_idx, doc_id_str, doc, action_type, _is_update in write_plan:
                    if doc is None:
                        continue
                    normalized, fields = _normalize_doc(doc)
                    builder.add_row(normalized, fields, file_rel=None, col_schema=None)
                    id_to_new_doc_id[action_idx] = doc_id_counter
                    doc_id_counter += 1

                builder.finalize()

                # Update counter
                with open(counter_path, "w") as cf:
                    cf.write(str(doc_id_counter))

                # Record ID mappings and build results for writes
                engine = manager.get_engine(index)
                for action_idx, doc_id_str, doc, action_type, is_update in write_plan:
                    new_id = id_to_new_doc_id[action_idx]
                    if id_field and doc_id_str:
                        engine._tombstones.set_doc_id_for_field(id_field, doc_id_str, new_id)
                    # ES: index returns 200 (not 201), only create returns 201
                    status = 201 if (action_type == "create" and not is_update) else 200
                    results.append(BulkResultItem(
                        operation=action_type,
                        doc_id=doc_id_str,
                        status=status,
                        result="updated" if is_update else "created",
                        new_doc_id=new_id,
                    ))
                    if is_update:
                        total_updated += 1
                    else:
                        total_created += 1

            # Sort results back to original action order
            results.sort(key=lambda r: next(
                (i for i, a in enumerate(actions) if a["type"] == r.operation and a["meta"].get("_id") == r.doc_id), i
            ))

        finally:
            fcntl.flock(lockf.fileno(), fcntl.LOCK_UN)

    took_ms = int((time.time() - start_time) * 1000)
    return BulkResponse(
        took=took_ms,
        errors=errors,
        results=results,
        total_created=total_created,
        total_updated=total_updated,
        total_deleted=total_deleted,
        total_conflicts=total_conflicts,
    )


# ─── POST /{index}/_compact ───────────────────────────────────────────────


class CompactResponse(BaseModel):
    result: str  # "ok"
    deleted_removed: int
    alive_rewritten: int
    old_size_mb: float
    new_size_mb: float


@router.post("/{index}/_compact", response_model=CompactResponse)
async def compact_index(
    index: str,
    manager: IndexManager = Depends(get_index_manager),
):
    """Reclaim disk space by rewriting the index without deleted documents.

    POST /myindex/_compact
    → {result: "ok", deleted_removed: N, alive_rewritten: M, old_size_mb: X, new_size_mb: Y}
    """
    _check_not_fsk(manager, index)

    engine = manager.get_engine(index)
    if engine._tombstones is None:
        raise HTTPException(400, "Index does not support compaction.")

    from flatseek.core.compactor import IndexCompactor
    compactor = IndexCompactor(manager.data_dir, engine._tombstones)

    try:
        result = compactor.compact()
    except ValueError as e:
        raise HTTPException(400, str(e))

    manager._engines.pop(index, None)

    return CompactResponse(
        result=result.result,
        deleted_removed=result.deleted_removed,
        alive_rewritten=result.alive_rewritten,
        old_size_mb=result.old_size_mb,
        new_size_mb=result.new_size_mb,
    )
