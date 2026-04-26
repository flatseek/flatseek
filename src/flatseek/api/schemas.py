"""Pydantic schemas for Flatseek API (Elasticsearch-inspired)."""

# Simple schemas - using dicts directly instead of Pydantic models
# to avoid version compatibility issues

# ─── Common ────────────────────────────────────────────────────────────────────
def create_index_stats(**kwargs):
    """Index statistics."""
    return {
        "docs_count": kwargs.get("docs_count", 0),
        "size_bytes": kwargs.get("size_bytes", 0),
        "columns": kwargs.get("columns", {}),
    }


def create_mapping(columns: dict):
    """Index mapping (column definitions)."""
    return {"columns": columns}


def create_hit(doc_id: int, source: dict, score: float = 1.0):
    """Single search hit."""
    return {
        "_id": doc_id,
        "_score": score,
        "_source": source,
    }


def create_search_response(index: str, hits: list, total: int, took: int = 0):
    """Search response."""
    return {
        "_index": index,
        "hits": {
            "total": total,
            "hits": hits,
        },
        "took": took,
    }