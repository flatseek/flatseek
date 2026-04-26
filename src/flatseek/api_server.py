"""Flatseek API server entry point.

Usage:
    flatseek api                      # Start API server on port 8000
    flatseek api --port 9000          # Custom port
    python -m flatseek.api_server    # Alternative
"""
from __future__ import annotations

import os
import sys

def run():
    """Run the API server."""
    from flatseek.api.main import app
    import uvicorn

    port = int(os.environ.get("FLATSEEK_PORT", "8000"))
    host = os.environ.get("FLATSEEK_HOST", "0.0.0.0")

    uvicorn.run(
        "flatseek.api.main:app",
        host=host,
        port=port,
        reload=os.environ.get("FLATSEEK_RELOAD", "0") == "1",
    )

if __name__ == "__main__":
    run()
