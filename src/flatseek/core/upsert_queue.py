"""
Batch upsert queue for high-throughput parallel crawlers.

Architecture:
  ThreadLocal builder per worker (avoids lock contention on builder state)
      ↓
  Shared queue: Queue of doc batches [(id_value, doc_dict), ...]
      ↓
  Background flusher thread: every N docs OR every T seconds, flush batch
      ↓
  Atomic finalize to make new docs searchable

Usage:
  queue = UpsertQueue(data_dir, index_name, id_field="email", batch_size=500, flush_interval=5.0)
  queue.start()

  # In worker threads:
  queue.put("email@example.com", {"email": "email@example.com", "status": "active"})

  # In background (or on shutdown):
  queue.flush()

  # Status:
  status = queue.status()  # {"queue_depth": N, "pending_docs": M, "flushing": bool}

  # Shutdown:
  queue.stop()
"""

from __future__ import annotations

import os
import json
import time
import queue
import threading
import fcntl
from typing import Any


# ─── Per-worker thread-local builder ─────────────────────────────────────────


class _WorkerBuilder:
    """Thread-local IndexBuilder and pending docs for one worker.

    Each worker thread has its own builder instance, so add_row() calls
    never contend on a shared lock. Documents are accumulated in a thread-local
    list and passed through to the flush queue when batch size is reached.
    """

    __slots__ = ("builder", "pending", "last_active")

    def __init__(self, builder):
        from flatseek.core.builder import IndexBuilder

        self.builder = builder  # IndexBuilder instance
        self.pending: list[tuple[str | None, dict[str, Any]]] = []  # (id_value, doc)
        self.last_active = time.monotonic()


class _BuilderRegistry:
    """Thread-local registry of per-thread IndexBuilder instances."""

    def __init__(
        self,
        index_dir: str,
        id_field: str | None,
        dataset: str,
    ):
        self._index_dir = index_dir
        self._id_field = id_field
        self._dataset = dataset
        self._local = threading.local()

    def get(self) -> _WorkerBuilder:
        """Get or create the caller's thread-local builder."""
        wb = getattr(self._local, "_wb", None)
        if wb is not None:
            wb.last_active = time.monotonic()
            return wb

        from flatseek.core.builder import IndexBuilder

        builder = IndexBuilder(
            self._index_dir,
            column_map={},
            start_doc_id=0,
            dataset=self._dataset,
            checkpoint_cb=None,
            delimiter=",",
            columns=None,
            worker_id=None,
            dedup_fields=None,
            doc_id_end=None,
            daemon=False,
            id_field=self._id_field,
        )
        wb = _WorkerBuilder(builder)
        self._local._wb = wb
        return wb

    def clear_pending(self) -> list[tuple[str | None, dict[str, Any]]]:
        """Clear and return the caller's pending docs. Thread-safe via current thread."""
        wb = getattr(self._local, "_wb", None)
        if wb is None:
            return []
        docs = list(wb.pending)
        wb.pending.clear()
        return docs


# ─── Flush request ────────────────────────────────────────────────────────────


class _FlushRequest:
    """A request to flush accumulated documents to disk."""

    def __init__(
        self,
        docs: list[tuple[str | None, dict[str, Any]]],
        # (id_value, doc) — id_value may be None for auto-ID
        doc_id_start: int,
    ):
        self.docs = docs
        self.doc_id_start = doc_id_start
        self.event = threading.Event()
        self.error: BaseException | None = None


# ─── UpsertQueue ──────────────────────────────────────────────────────────────


class UpsertQueue:
    """High-throughput batch upsert queue with background flusher.

    Accumulates documents in per-thread builders and flushes them in batches
    to minimize disk I/O from concurrent crawlers.

    Args:
        data_dir: Root data directory containing index subdirectories.
        index_name: Name of the index to upsert into.
        id_field: Natural key field name (required for upsert by ID).
        batch_size: Max docs per flush (default 500).
        flush_interval: Max seconds between flushes (default 5.0).
        queue_maxsize: Max items in the flush queue (default 0 = unlimited).
    """

    def __init__(
        self,
        data_dir: str,
        index_name: str,
        id_field: str | None = None,
        batch_size: int = 500,
        flush_interval: float = 5.0,
        queue_maxsize: int = 0,
    ):
        self.data_dir = data_dir
        self.index_name = index_name
        self.id_field = id_field
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._queue: queue.Queue[_FlushRequest | None] = queue.Queue(maxsize=queue_maxsize)
        self._registry = _BuilderRegistry(
            os.path.join(data_dir, index_name),
            id_field=id_field,
            dataset=index_name,
        )
        self._lock = threading.Lock()
        self._started = False
        self._stopping = False
        self._flush_thread: threading.Thread | None = None
        self._total_docs_flushed = 0
        self._total_flushes = 0
        self._last_flush_time = 0.0
        self._flush_in_progress = False
        self._pending_count = 0  # total docs waiting to be flushed
        self._doc_counter = 0  # global doc_id counter (protected by _lock)
        self._thread_counts: dict[int, int] = {}  # thread_id -> count in that thread's builder

    # ── Public API ────────────────────────────────────────────────────────────

    def start(self) -> None:
        """Start the background flusher thread. Idempotent."""
        with self._lock:
            if self._started:
                return
            self._started = True
            self._flush_thread = threading.Thread(target=self._flusher_loop, daemon=True)
            self._flush_thread.start()

    def put(self, id_value: str | None, doc: dict[str, Any]) -> None:
        """Queue a document for batch upsert.

        Args:
            id_value: The natural key value (value of id_field), or None for auto-ID.
            doc: The document fields.
        """
        self._check_started()
        wb = self._registry.get()
        builder = wb.builder

        # Inject _id so normalize handles it correctly
        doc_to_add = dict(doc)
        if id_value is not None:
            doc_to_add["_id"] = id_value

        # Normalize and add row
        normalized, fields = self._normalize_doc(doc_to_add)
        builder.add_row(normalized, fields, file_rel=None, col_schema=None)

        # Track the pending doc
        wb.pending.append((id_value, doc_to_add))

        # Track per-thread count
        tid = threading.current_thread().ident
        with self._lock:
            self._pending_count += 1
            self._thread_counts[tid] = self._thread_counts.get(tid, 0) + 1
            local_count = self._thread_counts.get(tid, 0)

        # Check if we should trigger a flush for this thread's batch
        if local_count >= self.batch_size:
            self._flush_caller_thread()

    def flush(self) -> None:
        """Synchronously flush all pending documents from the current thread. Blocks until flush completes."""
        self._check_started()
        self._flush_caller_thread()

    def status(self) -> dict[str, Any]:
        """Return current queue and flush status.

        Returns:
            dict with keys:
              - queue_depth: number of flush requests pending in the queue
              - pending_docs: number of docs accumulated but not yet flushed
              - flushing: whether a flush is currently in progress
              - total_docs_flushed: cumulative docs flushed since start
              - total_flushes: cumulative flush operations
              - last_flush_age_s: seconds since last flush completed
        """
        with self._lock:
            pending = self._pending_count
            flushing = self._flush_in_progress
            queue_depth = self._queue.qsize()
            last_flush_age = time.time() - self._last_flush_time if self._last_flush_time else None

        return {
            "queue_depth": queue_depth,
            "pending_docs": pending,
            "flushing": flushing,
            "total_docs_flushed": self._total_docs_flushed,
            "total_flushes": self._total_flushes,
            "last_flush_age_s": last_flush_age,
        }

    def stop(self, timeout: float = 30.0) -> None:
        """Stop the flusher thread. Drains all pending docs first."""
        with self._lock:
            if not self._started:
                return
            self._stopping = True

        # Enqueue a sentinel to wake the flusher
        self._queue.put(None)

        if self._flush_thread is not None:
            self._flush_thread.join(timeout=timeout)
            self._flush_thread = None

        with self._lock:
            self._started = False
            self._stopping = False

    # ── Internal ──────────────────────────────────────────────────────────────

    def _check_started(self) -> None:
        if not self._started:
            raise RuntimeError("UpsertQueue.start() must be called before put()")

    def _normalize_doc(self, doc: dict[str, Any]) -> tuple[dict[str, str], list[str]]:
        """Normalize document values to strings (as IndexBuilder expects)."""
        normalized = {}
        fields = []
        for k, v in doc.items():
            if isinstance(v, (dict, list)):
                normalized[k] = json.dumps(v)
            else:
                normalized[k] = str(v)
            fields.append(k)
        return normalized, fields

    def _flush_caller_thread(self) -> None:
        """Flush the current thread's builder and enqueue a flush request."""
        tid = threading.current_thread().ident

        # Get current thread's pending docs from registry
        docs_to_flush = self._registry.clear_pending()
        if not docs_to_flush:
            return

        with self._lock:
            count = len(docs_to_flush)
            self._pending_count -= count
            self._thread_counts.pop(tid, None)
            doc_id_start = self._doc_counter
            self._doc_counter += count

        req = _FlushRequest(docs_to_flush, doc_id_start)
        self._queue.put(req)

    def _flusher_loop(self) -> None:
        """Background thread that drains the flush queue and writes batches."""
        while True:
            try:
                req = self._queue.get(timeout=self.flush_interval)
            except queue.Empty:
                # Timeout — no flush request received, loop continues
                # Pending docs in worker threads will be flushed on next put() or stop()
                continue

            if req is None:
                # Sentinel: stop requested
                break

            # Process the flush request
            try:
                self._do_flush_request(req)
            except BaseException as e:
                req.error = e
            finally:
                req.event.set()

    def _do_flush_request(self, req: _FlushRequest) -> None:
        """Execute a single flush request."""
        with self._lock:
            self._flush_in_progress = True

        try:
            if not req.docs:
                return

            index_dir = os.path.join(self.data_dir, self.index_name)
            os.makedirs(index_dir, exist_ok=True)

            # Use the doc_id_start from the request (captured when flush was triggered)
            # This ensures each flush request uses a unique, non-overlapping doc_id range
            doc_id_counter = req.doc_id_start

            # Acquire lock to update .next_doc_id atomically
            counter_path = os.path.join(index_dir, ".next_doc_id")
            lock_path = os.path.join(index_dir, ".doc_id.lock")
            os.makedirs(os.path.dirname(lock_path), exist_ok=True)

            new_doc_ids: list[int] = []

            with open(lock_path, "a") as lockf:
                fcntl.flock(lockf.fileno(), fcntl.LOCK_EX)
                try:
                    # Update .next_doc_id to reflect the next available doc_id
                    doc_id_end = doc_id_counter + len(req.docs)
                    with open(counter_path, "w") as cf:
                        cf.write(str(doc_id_end))

                    from flatseek.core.builder import IndexBuilder

                    builder = IndexBuilder(
                        index_dir,
                        column_map={},
                        start_doc_id=doc_id_counter,
                        dataset=self.index_name,
                        checkpoint_cb=None,
                        delimiter=",",
                        columns=None,
                        worker_id=None,
                        dedup_fields=None,
                        doc_id_end=None,
                        daemon=False,
                        id_field=self.id_field,
                    )

                    # Add all docs to the builder
                    for idx, (id_value, doc) in enumerate(req.docs):
                        normalized, fields = self._normalize_doc(doc)
                        builder.add_row(normalized, fields, file_rel=None, col_schema=None)
                        new_doc_ids.append(doc_id_counter + idx)

                    builder.finalize()

                    # Update counter
                    doc_id_end = doc_id_counter + len(req.docs)
                    with open(counter_path, "w") as cf:
                        cf.write(str(doc_id_end))

                    # Update tombstone id mappings (if index is registered with manager)
                    try:
                        from flatseek.api.deps import get_index_manager
                        manager = get_index_manager()
                        engine = manager.get_engine(self.index_name)
                        if self.id_field:
                            for idx, (id_value, doc) in enumerate(req.docs):
                                if id_value:
                                    engine._tombstones.set_doc_id_for_field(
                                        self.id_field, id_value, new_doc_ids[idx]
                                    )
                        # Invalidate engine cache so subsequent reads pick up new chunks
                        manager._engines.pop(self.index_name, None)
                    except Exception:
                        # Index not registered with manager, or no tombstone support
                        # The docs are still flushed to disk, just not registered in tombstones
                        pass

                finally:
                    fcntl.flock(lockf.fileno(), fcntl.LOCK_UN)

            with self._lock:
                self._total_docs_flushed += len(req.docs)
                self._total_flushes += 1
                self._last_flush_time = time.time()

        finally:
            with self._lock:
                self._flush_in_progress = False


# ─── Global registry ─────────────────────────────────────────────────────────


# Global registry of active UpsertQueue instances per index
# Keyed by (data_dir, index_name)
_queues: dict[tuple[str, str], UpsertQueue] = {}
_queues_lock = threading.Lock()


def get_upsert_queue(
    data_dir: str,
    index_name: str,
    id_field: str | None = None,
    batch_size: int = 500,
    flush_interval: float = 5.0,
) -> UpsertQueue:
    """Get or create a shared UpsertQueue for an index.

    Thread-safe singleton per (data_dir, index_name) pair.
    """
    key = (data_dir, index_name)
    with _queues_lock:
        if key not in _queues:
            q = UpsertQueue(
                data_dir=data_dir,
                index_name=index_name,
                id_field=id_field,
                batch_size=batch_size,
                flush_interval=flush_interval,
            )
            q.start()
            _queues[key] = q
        return _queues[key]


def close_upsert_queue(data_dir: str, index_name: str) -> None:
    """Stop and remove a shared UpsertQueue."""
    key = (data_dir, index_name)
    with _queues_lock:
        if key in _queues:
            _queues[key].stop()
            del _queues[key]
