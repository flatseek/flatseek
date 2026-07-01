"""Regression tests for .fsk over HTTP Range requests.

Verifies that ``FlatseekFileStorageAdapter`` can serve a .fsk archive
hosted on a remote HTTP server without downloading the whole file
first. Uses an in-process HTTP server so tests are fully self-contained.
"""
from __future__ import annotations

import http.server
import io
import shutil
import socketserver
import tempfile
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from flatseek.cli import cmd_encrypt
from flatseek.core.builder import build
from flatseek.core.query_engine import QueryEngine, load_encryption_key
from flatseek.flatseek_file import FlatseekFileStorageAdapter, FlatseekPacker
from flatseek.flatseek_file import HEADER_SIZE, FlatseekHeader


# ── In-process HTTP server that supports Range requests ───────────────────

class _RangeHTTPHandler(http.server.BaseHTTPRequestHandler):
    """HTTP handler that serves a single .fsk file with proper Range support.

    The .fsk path is set as a class attribute (set by the fixture).
    """

    fsk_path: Path | None = None
    fsk_size: int = 0
    request_log: list | None = None  # for counting HTTP requests in tests

    def do_GET(self):  # noqa: N802
        if self.fsk_path is None:
            self.send_response(500)
            self.end_headers()
            return
        rng = self.headers.get("Range")
        if self.request_log is not None:
            self.request_log.append(rng)
        with open(self.fsk_path, "rb") as f:
            if rng and rng.startswith("bytes="):
                spec = rng[len("bytes="):]
                if "-" in spec:
                    start_s, end_s = spec.split("-", 1)
                    start = int(start_s) if start_s else 0
                    end = int(end_s) if end_s else self.fsk_size - 1
                    f.seek(start)
                    data = f.read(end - start + 1)
                    self.send_response(206)
                    self.send_header(
                        "Content-Range",
                        f"bytes {start}-{start + len(data) - 1}/{self.fsk_size}",
                    )
                    self.send_header("Content-Length", str(len(data)))
                    self.send_header("Accept-Ranges", "bytes")
                    self.end_headers()
                    self.wfile.write(data)
                    return
            # Full file (server ignores Range, or no Range header)
            f.seek(0)
            data = f.read()
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            self.wfile.write(data)

    def log_message(self, *args):  # silence
        pass


class _NoRangeHTTPHandler(_RangeHTTPHandler):
    """HTTP handler that IGNORES Range requests and always returns 200
    with the full file. Used to test the fallback path for servers
    that don't support Range."""

    def do_GET(self):  # noqa: N802
        if self.fsk_path is None:
            self.send_response(500)
            self.end_headers()
            return
        if self.request_log is not None:
            self.request_log.append(self.headers.get("Range"))
        with open(self.fsk_path, "rb") as f:
            data = f.read()
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Accept-Ranges", "bytes")  # claims support but ignores
        self.end_headers()
        self.wfile.write(data)


class _HTTPServer:
    """Context-manager that runs an in-process HTTP server for tests."""

    def __init__(self, handler_class):
        self.handler_class = handler_class
        self.httpd = None
        self.thread = None
        self.port = None
        self.request_log: list = []

    def __enter__(self):
        # Use a fresh handler class with our state — handlers are per-request
        # so we need to make a subclass with the state pre-bound
        from functools import partial
        handler = type(
            "BoundHandler",
            (self.handler_class,),
            {"fsk_path": None, "fsk_size": 0, "request_log": self.request_log},
        )
        self.handler_class_bound = handler
        # Find a free port
        with socketserver.TCPServer(("127.0.0.1", 0), handler) as httpd:
            self.port = httpd.server_address[1]
        # Now bind for real (with the chosen port)
        self.httpd = socketserver.TCPServer(("127.0.0.1", self.port), handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *args):
        if self.httpd is not None:
            self.httpd.shutdown()
            self.httpd.server_close()

    def set_fsk(self, fsk_path: Path):
        self.handler_class_bound.fsk_path = fsk_path
        self.handler_class_bound.fsk_size = fsk_path.stat().st_size


# ── Helpers ───────────────────────────────────────────────────────────────

def _build_and_pack(tmp: Path) -> Path:
    """Build a tiny index, encrypt it, pack as .fsk. Returns the .fsk path."""
    data_dir = tmp / "data"
    csv = tmp / "tiny.csv"
    csv.write_text("name,city\nalice,Jakarta\nbob,Bandung\n")
    build(str(csv), str(data_dir))
    cmd_encrypt(SimpleNamespace(
        data_dir=str(data_dir), passphrase="pp",
        resume=True, verify=False, verify_sample=100,
    ))
    fsk = tmp / "enc.fsk"
    key = load_encryption_key(str(data_dir), "pp")
    FlatseekPacker(data_dir, fsk, enc_key=key).pack()
    return fsk


# ── Tests ────────────────────────────────────────────────────────────────

class TestFskOverHTTPRange:
    """The full path: build a .fsk, serve it over HTTP, open via RangeFile,
    run queries — no full download."""

    def setup_method(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="fsk_http_range_"))

    def teardown_method(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_open_via_url_with_correct_key(self):
        fsk = _build_and_pack(self.tmp)
        with _HTTPServer(_RangeHTTPHandler) as srv:
            srv.set_fsk(fsk)
            url = f"http://127.0.0.1:{srv.port}/enc.fsk"
            key = load_encryption_key(str(self.tmp / "data"), "pp")
            adp = FlatseekFileStorageAdapter(url, enc_key=key)
            assert not adp.is_manifest_locked()
            qe = QueryEngine("placeholder", storage=adp)
            assert qe.stats["total_docs"] == 2
            assert qe.query("city:Jakarta")["total"] == 1
            assert qe.query("city:Bandung")["total"] == 1

    def test_open_via_url_with_wrong_key_marks_locked(self):
        fsk = _build_and_pack(self.tmp)
        with _HTTPServer(_RangeHTTPHandler) as srv:
            srv.set_fsk(fsk)
            url = f"http://127.0.0.1:{srv.port}/enc.fsk"
            wrong_key = load_encryption_key(str(self.tmp / "data"), "wrong")
            adp = FlatseekFileStorageAdapter(url, enc_key=wrong_key)
            assert adp.is_manifest_locked()
            assert "stats" in adp.locked_fields()

    def test_open_via_url_with_no_key_marks_locked(self):
        fsk = _build_and_pack(self.tmp)
        with _HTTPServer(_RangeHTTPHandler) as srv:
            srv.set_fsk(fsk)
            url = f"http://127.0.0.1:{srv.port}/enc.fsk"
            adp = FlatseekFileStorageAdapter(url, enc_key=None)
            assert adp.is_manifest_locked()
            assert "stats" in adp.locked_fields()

    def test_range_file_uses_range_requests(self):
        """Range-supporting server: confirm Range headers are actually used
        (so we don't download the whole .fsk just to read a few bytes)."""
        fsk = _build_and_pack(self.tmp)
        with _HTTPServer(_RangeHTTPHandler) as srv:
            srv.set_fsk(fsk)
            url = f"http://127.0.0.1:{srv.port}/enc.fsk"
            from flatseek.core.storage import RangeFile
            rf = RangeFile(url)
            # Probe size via Range request
            size = rf.size()
            assert size == fsk.stat().st_size
            # Should have made at least one Range request
            assert any(rng and rng.startswith("bytes=")
                       for rng in srv.request_log), \
                f"expected Range request, got log: {srv.request_log}"
            # Read a small chunk and verify content
            rf.seek(0)
            data = rf.read(HEADER_SIZE)
            assert data == fsk.read_bytes()[:HEADER_SIZE]

    def test_fallback_when_server_ignores_range(self):
        """Some servers don't honor Range (e.g. plain file://, GitHub
        LFS, etc). RangeFile must still work — it just caches the
        whole file in the first request and serves from memory."""
        fsk = _build_and_pack(self.tmp)
        with _HTTPServer(_NoRangeHTTPHandler) as srv:
            srv.set_fsk(fsk)
            url = f"http://127.0.0.1:{srv.port}/enc.fsk"
            from flatseek.core.storage import RangeFile
            rf = RangeFile(url)
            size = rf.size()
            assert size == fsk.stat().st_size
            rf.seek(0)
            data = rf.read(HEADER_SIZE)
            assert data == fsk.read_bytes()[:HEADER_SIZE]
            # Re-read — should be served from _full_data cache, no extra HTTP
            n_before = len(srv.request_log)
            rf.seek(100)
            rf.read(50)
            assert len(srv.request_log) == n_before, \
                "expected no additional HTTP request for cached data"

    def test_local_path_still_works(self):
        """Regression: passing a local Path (not URL) still works."""
        fsk = _build_and_pack(self.tmp)
        key = load_encryption_key(str(self.tmp / "data"), "pp")
        adp = FlatseekFileStorageAdapter(fsk, enc_key=key)
        assert not adp.is_manifest_locked()
        qe = QueryEngine("placeholder", storage=adp)
        assert qe.query("city:Jakarta")["total"] == 1

    def test_create_storage_adapter_auto_detects_fsk_url(self):
        """Regression: when a user passes an http://...foo.fsk URL via
        ``create_storage_adapter(config)`` (e.g. the API path), the
        factory must return FlatseekFileStorageAdapter with RangeFile
        — NOT URLStorageAdapter (directory mode).

        Without this, the URL adapter joins base_url + '/' + rel_path
        and requests non-existent sub-paths of the .fsk file (404s).
        """
        from flatseek.core.storage import create_storage_adapter
        from flatseek.flatseek_file import FlatseekFileStorageAdapter
        from flatseek.core.storage import URLStorageAdapter

        fsk = _build_and_pack(self.tmp)
        with _HTTPServer(_RangeHTTPHandler) as srv:
            srv.set_fsk(fsk)
            url = f"http://127.0.0.1:{srv.port}/enc.fsk"
            adp = create_storage_adapter(
                path=url,
                config=None,  # let it construct from env
            )
            # Must be the Range-based adapter, NOT directory-mode URL
            assert isinstance(adp, FlatseekFileStorageAdapter), (
                f"expected FlatseekFileStorageAdapter for .fsk URL, got "
                f"{type(adp).__name__}"
            )
            assert not isinstance(adp, URLStorageAdapter)
            # And the engine should work end-to-end
            key = load_encryption_key(str(self.tmp / "data"), "pp")
            adp.set_key(key)
            qe = QueryEngine("placeholder", storage=adp)
            assert qe.query("city:Jakarta")["total"] == 1

    def test_url_backend_url_with_fsk_uses_range(self):
        """When ``--storage-backend=url --storage-url=.../foo.fsk`` is
        passed, the factory should still detect the .fsk extension
        and use FlatseekFileStorageAdapter — not the directory-mode
        URLStorageAdapter. This is the bug the user reported (404s on
        sub-paths of the .fsk URL)."""
        from flatseek.core.storage import create_storage_adapter, StorageConfig
        from flatseek.flatseek_file import FlatseekFileStorageAdapter

        fsk = _build_and_pack(self.tmp)
        with _HTTPServer(_RangeHTTPHandler) as srv:
            srv.set_fsk(fsk)
            url = f"http://127.0.0.1:{srv.port}/enc.fsk"
            config = StorageConfig(backend="url", url=url)
            adp = create_storage_adapter(config=config)
            assert isinstance(adp, FlatseekFileStorageAdapter), (
                f"backend=url + .fsk URL should still detect FlatseekFile, "
                f"got {type(adp).__name__}"
            )

    def test_list_url_indices_handles_fsk_storage(self):
        """Regression: ``_list_url_indices`` in api/deps.py used
        ``storage._original_url`` (URLStorageAdapter-only attr) and
        crashed with AttributeError when the storage was a
        FlatseekFileStorageAdapter (which doesn't have it).

        This used to break the API path: ``GET /_indices?bucket=.../x.fsk``
        raised AttributeError before reaching the 200 response. Now the
        helper detects the .fsk URL and returns a single-element list
        with the file stem as the index name.
        """
        from flatseek.core.storage import create_storage_adapter
        from flatseek.flatseek_file import FlatseekFileStorageAdapter

        fsk = _build_and_pack(self.tmp)
        with _HTTPServer(_RangeHTTPHandler) as srv:
            srv.set_fsk(fsk)
            url = f"http://127.0.0.1:{srv.port}/wiki.fsk"
            adp = create_storage_adapter(path=url)
            assert isinstance(adp, FlatseekFileStorageAdapter)

            # Simulate the API path that called list_indices. We just
            # need to make sure _list_url_indices doesn't crash on the
            # .fsk-URL case.
            from flatseek.api.deps import IndexManager
            mgr = IndexManager()
            mgr._storage = adp
            # Should NOT raise AttributeError for missing _original_url
            result = mgr._list_url_indices(adp)
            assert result == ["wiki"], (
                f"expected single index 'wiki' for wiki.fsk, got {result}"
            )

    def test_get_engine_works_for_fsk_url(self):
        """Regression: IndexManager.get_engine(index, bucket_url=<.fsk>)
        used to 404 because the isinstance check only matched
        URLStorageAdapter. After my fix it also matches
        FlatseekFileStorageAdapter, so the API's search route can
        actually create a QueryEngine against a .fsk URL."""
        from flatseek.api.deps import IndexManager
        from flatseek.flatseek_file import FlatseekFileStorageAdapter

        fsk = _build_and_pack(self.tmp)
        with _HTTPServer(_RangeHTTPHandler) as srv:
            srv.set_fsk(fsk)
            url = f"http://127.0.0.1:{srv.port}/wiki.fsk"

            mgr = IndexManager()
            adp = mgr._get_bucket_storage(url)
            # Should be a FlatseekFileStorageAdapter (not URLStorageAdapter)
            assert isinstance(adp, FlatseekFileStorageAdapter)

            # Now ask the API path to get an engine for "wiki" via this URL.
            # Manifest is locked (no key provided) — engine.stats will be
            # the placeholder. We need to set the key AFTER get_engine
            # to match the API auth flow (POST /_authenticate sets the
            # key on the engine, then subsequent search uses it).
            engine = mgr.get_engine("wiki", bucket_url=url)
            assert engine is not None
            assert engine.stats.get("_locked") is True
            # Set the key (simulating the POST /_authenticate round-trip)
            key = load_encryption_key(str(self.tmp / "data"), "pp")
            engine.storage.set_key(key)
            engine._enc_key = key
            # Now reload stats to get the real values
            engine.reload_stats()
            assert engine.stats.get("total_docs") == 2
            # The query should also work
            r = engine.query("city:Jakarta")
            assert r["total"] == 1