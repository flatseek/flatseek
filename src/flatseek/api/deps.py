"""Dependency injection for Flatseek API."""

import os
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, AsyncGenerator, Any

from fastapi import Depends, HTTPException

# Default data directory
DEFAULT_DATA_DIR = os.environ.get("FLATSEEK_DATA_DIR", "data")

# Import QueryEngine lazily to avoid circular imports
if TYPE_CHECKING:
    from flatseek.core.query_engine import QueryEngine


class IndexManager:
    """Manages QueryEngine instances per index (lazy loading)."""

    def __init__(self, data_dir: str = DEFAULT_DATA_DIR):
        self.data_dir = data_dir
        self._engines: dict[str, Any] = {}
        # Per-index passwords for encrypted indexes
        self._index_passwords: dict[str, str] = {}

    def set_password(self, index: str, password: str) -> None:
        """Store password for an encrypted index."""
        self._index_passwords[index] = password

    def get_password(self, index: str) -> str | None:
        """Get stored password for an index, or None."""
        return self._index_passwords.get(index)

    def clear_password(self, index: str) -> None:
        """Remove stored password for an index."""
        self._index_passwords.pop(index, None)

    def is_encrypted(self, index: str) -> bool:
        """Check if a specific index is encrypted.

        Each index stores its own encryption.json inside its folder.
        If that file exists, the index is encrypted.
        """
        possible_paths = [
            os.path.join(self.data_dir, index),
            os.path.join(self.data_dir, index, "..", index),
        ]
        for index_path in possible_paths:
            enc_path = os.path.join(index_path, "encryption.json")
            if os.path.isfile(enc_path):
                return True
        return False

    def get_engine(self, index: str) -> "QueryEngine":
        """Get or create QueryEngine for an index."""
        if not index.replace("_", "").replace("-", "").replace("/", "").isalnum():
            raise HTTPException(400, f"Invalid index name: {index}")

        engine = self._engines.get(index)
        if engine is None:
            possible_paths = [
                os.path.join(self.data_dir, index),
                os.path.join(self.data_dir, index, "..", index),
            ]
            from flatseek.core.query_engine import QueryEngine
            for path in possible_paths:
                if os.path.isdir(os.path.join(path, "index")):
                    engine = QueryEngine(path)
                    self._engines[index] = engine
                    break

        if engine is None:
            raise HTTPException(404, f"Index not found: {index}")

        return engine
    
    def list_indices(self) -> list[str]:
        """List all available indices."""
        if not os.path.isdir(self.data_dir):
            return []
        
        indices = []
        for name in os.listdir(self.data_dir):
            path = os.path.join(self.data_dir, name)
            if os.path.isdir(os.path.join(path, "index")):
                indices.append(name)
            elif os.path.isdir(path):
                # Check for sub-indexes
                for sub in os.listdir(path):
                    sub_path = os.path.join(path, sub)
                    if os.path.isdir(os.path.join(sub_path, "index")):
                        indices.append(f"{name}/{sub}")
        return sorted(indices)


# Global index manager
_index_manager: IndexManager | None = None


def get_index_manager() -> IndexManager:
    """Get the global IndexManager instance, re-reading FLATSEEK_DATA_DIR each call."""
    global _index_manager
    if _index_manager is None:
        _index_manager = IndexManager()
    else:
        # Pick up changed FLATSEEK_DATA_DIR for tests
        _index_manager.data_dir = os.environ.get("FLATSEEK_DATA_DIR", _index_manager.data_dir)
    return _index_manager


async def get_query_engine(
    index: str,
    manager: IndexManager = Depends(get_index_manager),
) -> "QueryEngine":
    """Dependency to get QueryEngine for an index."""
    return manager.get_engine(index)


def require_index(
    index: str,
    manager: IndexManager = Depends(get_index_manager),
) -> str:
    """Dependency that validates index exists."""
    try:
        manager.get_engine(index)
    except Exception as e:
        raise HTTPException(404, str(e)) from e
    return index