"""FastAPI application for Flatseek (Elasticsearch-like API)."""

import os
import sys
import logging
import re
import tempfile
import shutil
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

# ── Version from pyproject.toml ──────────────────────────────────────────────
_PYPROJECT_TOML = os.path.join(os.path.dirname(__file__), "..", "..", "..", "pyproject.toml")
try:
    import tomllib
    with open(_PYPROJECT_TOML, "rb") as _f:
        API_VERSION = tomllib.load(_f)["project"]["version"]
except Exception:
    API_VERSION = "0.1.3"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def create_app():
    """Create and return the FastAPI app."""
    try:
        from fastapi import FastAPI
        from fastapi.middleware.cors import CORSMiddleware

        app = FastAPI(
            title="Flatseek API",
            description="Trigram inverted index API — search, aggregate, and index your data. "
                        "Supports Solana blockchain txs, aviation ADS-B, AdTech campaigns, DevOps logs, and more.",
            version=API_VERSION,
            terms_of_service="https://flatseek.io/terms",
            contact={"name": "Flatseek", "url": "https://flatseek.io"},
        )

        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        return app
    except ImportError:
        return _MockFastAPI()


# ─── OpenAPI Schemas ──────────────────────────────────────────────────────────

class RootInfo(BaseModel):
    name: str = Field(..., example="Flatseek API")
    version: str = Field(..., example="0.10.0")
    description: str = Field(..., example="Trigram inverted index API")


class ClusterHealth(BaseModel):
    status: str = Field(..., example="green")
    number_of_indices: int = Field(..., example=3)
    number_of_nodes: int = Field(..., example=1)
    indices: list[str]


class IndicesList(BaseModel):
    indices: list[str]
    count: int


class _MockFastAPI:
    """Minimal FastAPI mock for testing without FastAPI installed."""

    def __init__(self):
        self._routes = []
        self._middlewares = []

    def add_middleware(self, *args, **kwargs):
        self._middlewares.append((args, kwargs))

    def include_router(self, router):
        self._routes.append(router)

    def get(self, path):
        def decorator(func):
            self._routes.append((path, "GET", func))
            return func
        return decorator

    def post(self, path):
        def decorator(func):
            self._routes.append((path, "POST", func))
            return func
        return decorator

    def delete(self, path):
        def decorator(func):
            self._routes.append((path, "DELETE", func))
            return func
        return decorator


# ─── Dashboard (flatlens) mounting ────────────────────────────────────

_dashboard_attached = False
_dashboard_temp_dir = None  # holds patched flatlens dir if we rewrite API_BASE


def _find_flatlens():
    """Return path to flatlens directory or None."""
    candidates = [
        os.environ.get("FLATSEEK_FLATLENS_DIR", ""),
        os.environ.get("FLATLENS_DIR", ""),
        os.path.join(os.path.expanduser("~"), ".local", "share", "flatlens"),
        "/opt/flatlens",
    ]
    # Dev: sibling repo at ../../flatlens relative to flatseek repo root
    # flatseek_pkg = flatseek/flatseek/src/flatseek
    # flatseek_repo = flatseek/flatseek/src  → flatlens is at flatseek/flatlens (two levels up)
    _flatseek_pkg = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    _flatseek_repo = os.path.dirname(_flatseek_pkg)
    candidates.insert(0, os.path.normpath(os.path.join(_flatseek_repo, "..", "..", "flatlens")))

    for p in candidates:
        if p and os.path.isdir(p) and os.path.isfile(os.path.join(p, "index.html")):
            return p
    return None


def _copy_and_patch(flatlens_dir, api_base):
    """Copy flatlens to temp dir with API_BASE rewrite + logo href patch.

    Always copies to a temp dir so we never modify the original flatlens source.
    """
    global _dashboard_temp_dir
    if _dashboard_temp_dir:
        shutil.rmtree(_dashboard_temp_dir, ignore_errors=True)

    dest = tempfile.mkdtemp(prefix="flatlens_patched_")
    shutil.copytree(flatlens_dir, dest, dirs_exist_ok=True)

    api_js = os.path.join(dest, "js", "api.js")
    if os.path.exists(api_js):
        with open(api_js, "r", encoding="utf-8") as f:
            content = f.read()
        # The api.js uses an IIFE pattern where API_BASE resolves via:
        #   window.API_BASE > ?api= param > '__FLATLENS_API_URL__' fallback
        # Replace the placeholder string directly so the fallback uses the right port.
        content = content.replace("__FLATLENS_API_URL__", api_base)
        with open(api_js, "w", encoding="utf-8") as f:
            f.write(content)

    # Fix logo href in index.html — "/" would conflict with API docs routes
    index_html = os.path.join(dest, "index.html")
    if os.path.exists(index_html):
        with open(index_html, "r", encoding="utf-8") as f:
            content = f.read()
        content = content.replace(
            'href="/" class="dashboard-title"',
            'href="/dashboard" class="dashboard-title"',
        )
        with open(index_html, "w", encoding="utf-8") as f:
            f.write(content)

    _dashboard_temp_dir = dest
    return dest


def attach_dashboard(app, api_base):
    """Mount flatlens dashboard at /dashboard with correct API_BASE."""
    global _dashboard_attached
    if _dashboard_attached:
        return

    flatlens_dir = _find_flatlens()
    if not flatlens_dir:
        logger.warning("flatlens dashboard not found — skipping /dashboard mount")
        logger.warning("  Set FLATLENS_DIR or install flatlens to ~/.local/share/flatlens")
        return

    # Always copy to temp and patch — never touch the original
    patched_dir = _copy_and_patch(flatlens_dir, api_base)

    app.mount("/dashboard", StaticFiles(directory=patched_dir, html=True), name="flatlens")
    _dashboard_attached = True
    logger.info(f"Flatlens dashboard mounted at /dashboard (API_BASE={api_base})")


def _lazy_attach_dashboard(app, api_base):
    """Attach dashboard on first access to /dashboard, not at import time.

    copytree of flatlens (~7MB, 15 subdirs) during module import causes
    startup to hang for several seconds.  Defer to first request instead.
    """
    from starlette.middleware.base import BaseHTTPMiddleware
    from starlette.requests import Request

    _attached = False

    class _DashboardAttachMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request: Request, call_next):
            nonlocal _attached
            if request.url.path.startswith("/dashboard") and not _attached:
                _attached = True
                attach_dashboard(app, api_base)
            return await call_next(request)

    app.add_middleware(_DashboardAttachMiddleware)


# ─── App setup ──────────────────────────────────────────────────────────

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flatseek.api.deps import get_index_manager, IndexManager
from fastapi import Depends

app = create_app()

# Mount dashboard BEFORE routers so StaticFiles takes priority over /{index} routes.
_api_base = os.environ.get("FLATSEEK_API_BASE", "")
if not _api_base:
    _api_base = f"http://localhost:{os.environ.get('FLATSEEK_PORT', '8000')}"

# Attach lazily on first /dashboard request — avoids copytree hang at import
_lazy_attach_dashboard(app, _api_base)

# Also add a redirect from /dashboard → /dashboard/ (without trailing slash)
from starlette.responses import RedirectResponse
@app.get("/dashboard", include_in_schema=False)
async def redirect_dashboard():
    return RedirectResponse(url="/dashboard/", status_code=302)

from flatseek.api.routes.index import router as index_router
from flatseek.api.routes.search import router as search_router
from flatseek.api.routes.aggregate import router as aggregate_router
app.include_router(index_router)
app.include_router(search_router)
app.include_router(aggregate_router)


# ─── Root ───────────────────────────────────────────────────────────────

@app.get("/", response_model=RootInfo)
async def root():
    """Root endpoint — API name and version."""
    return {
        "name": "Flatseek API",
        "version": API_VERSION,
        "description": "Trigram inverted index API",
    }


@app.get("/_cluster/health", response_model=ClusterHealth)
async def cluster_health(manager: IndexManager = Depends(get_index_manager)):
    """Cluster health (single node). Returns all indices and their count."""
    indices = manager.list_indices()
    return {
        "status": "green",
        "number_of_indices": len(indices),
        "number_of_nodes": 1,
        "indices": indices,
    }


@app.get("/_indices", response_model=IndicesList)
async def list_indices(manager: IndexManager = Depends(get_index_manager)):
    """List all available indices in the cluster."""
    indices = manager.list_indices()
    return {
        "indices": indices,
        "count": len(indices),
    }


# ─── Run ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("FLATSEEK_PORT", "8000"))
    host = os.environ.get("FLATSEEK_HOST", "0.0.0.0")

    uvicorn.run(
        "api.main:app",
        host=host,
        port=port,
        reload=os.environ.get("FLATSEEK_RELOAD", "0") == "1",
    )