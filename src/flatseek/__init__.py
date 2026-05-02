"""Flatseek — dual-mode client for Flatseek trigram index.

Usage (API mode):
    from flatseek import Flatseek

    client = Flatseek("http://localhost:8000")
    result = client.search(index="solana_txs", q="program:raydium AND amount:>1000000")

Usage (direct mode):
    from flatseek import Flatseek

    qe = Flatseek("./data")                    # single index
    qe = Flatseek("./data", index="solana_txs")    # named sub-index
    result = qe.search(q="program:raydium AND signer:*7xMg*", size=10)

    # Aggregation (direct mode, with query filter)
    result = qe.aggregate(q="status:active AND country:ID", aggs={
        "by_campaign": {"terms": {"field": "campaign", "size": 10}},
        "bid_stats": {"stats": {"field": "bid"}}
    })
    print(result.aggs["by_campaign"]["buckets"])
"""

__version__ = "0.1.3"

from flatseek.client import (
    Flatseek,
    Response,
    CountResponse,
    AggsResponse,
    Elasticsearch,
)

__all__ = [
    "Flatseek",
    "Response",
    "CountResponse",
    "AggsResponse",
    "Elasticsearch",
    "__version__",
]
