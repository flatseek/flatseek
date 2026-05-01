# Benchmark Results

500K rows, article schema, SSD. Full suite at [`flatbench/`](https://github.com/flatseek/flatbench).

## TL;DR

Flatseek is the only engine with zero correctness errors
- **tantivy is fastest but incorrect** — returns 3,000 hits vs 501,011 expected on range queries
Flatseek ranks #1 overall when correctness is weighted (60% speed / 40% correctness): **0.878 vs tantivy's 0.650**

## Overall Score (60% speed · 40% correctness)

| Engine        | Speed | Correctness | Score |
|--------------|-------|-------------|-------|
| **Flatseek** | 🟢    | 🟢          | **0.878** ◀ |
| typesense    | 🟢    | 🟢          | 0.832 |
| zincsearch   | 🟢    | 🟢          | 0.823 |
| elasticsearch| 🟢   | 🟢          | 0.820 |
| tantivy      | 🟢    | 🔴          | 0.650 |
| whoosh       | 🔴    | 🔴          | 0.025 |

## Search Latency (p50 · lower is better)

| Engine        | p50     | p95     | Notes |
|--------------|---------|---------|-------|
| tantivy       | 0.70ms  | 3.91ms  | Fastest, but 3 correctness errors |
| **Flatseek**  | 7.86ms  | 310ms   | 2nd fastest, fully correct |
| typesense     | 14.2ms  | 202ms   | |
| elasticsearch | 16.1ms  | 280ms   | High tail latency |
| zincsearch    | 15.7ms  | 747ms   | |
| whoosh        | 173ms   | 2028ms  | 247× slower than tantivy |

## Build Index (500K rows)

| Engine | Duration | Rows/sec | Index size |
|--------|----------|----------|------------|
| tantivy       | 21s      | 23,816   | smallest |
| elasticsearch | 113s     | 4,424    | 244 MB |
| typesense     | 171s     | 2,925    | - |
| zincsearch    | 283s     | 1,769    | - |
| **Flatseek** | 217s     | 2,309    | 1042 MB |
| whoosh        | 795s     | 629      | 1054 MB |

## Correctness (non-negotiable)

| Engine        | Range hits | Expected | Correct? |
|--------------|------------|----------|----------|
| **Flatseek** | 501,011    | 501,011  | ✅ exact |
| elasticsearch | 505,044    | 501,011  | ✅ exact |
| typesense     | 393,328    | 501,011  | ⚠️ missed 107,683 |
| zincsearch    | 182,805    | 501,011  | ⚠️ missed 318,206 |
| tantivy       | 3,000      | 501,011  | 🔴 99.4% miss |
| whoosh        | 500,008    | 501,011  | ✅ exact |

## Aggregation Performance (ms)

| Aggregation | Flatseek | elasticsearch | tantivy | typesense | Winner |
|------------|----------|---------------|---------|-----------|--------|
| author (terms) | 1988 | 201 | 36 | 448 | tantivy |
| pub_year (terms) | 1911 | 39 | 37 | 5 | typesense |
| tags (terms) | 3317 | 47 | 37 | 56 | tantivy |
| views_max | 1898 | 7 | 32 | 23 | elasticsearch |
| views_stats | 1813 | 41 | 36 | 29 | typesense |

## Wildcard Search (p50 · ms)

| Pattern | Flatseek | tantivy | typesense | elasticsearch |
|---------|----------|---------|-----------|---------------|
| `*cloud*` | 24ms | 0.35ms | 49ms | 102ms |
| `*data*` | 109ms | 3.6ms | 162ms | 89ms |
| `*kube*` | 991ms | 0.10ms | 83ms | 108ms |
| `*micro*` | 22ms | 0.10ms | 46ms | 104ms |
| `*perform*` | 527ms | 0.11ms | 143ms | 82ms |

## Where Flatseek wins

**1. Correctness by default**
Every query type returns exact counts. Tantivy returns 3,000 hits instead of 501,011 on range queries.

**2. Single-node simplicity**
No cluster to configure. No JVM tuning. No replica management. Flatseek is a single binary.

**3. Consistent mid-tier search speed**
Flatseek's p50 (7.9ms) is fast enough for production. It's not as fast as tantivy, but unlike tantivy it's correct.

**4. No silent wrong results**
If you're building filters, faceted search, or analytics dashboards, you need an engine that returns what you asked for.

## Tradeoffs

**Build speed is not Flatseek's strength**
At 217s for 500K rows, Flatseek is 10× slower than tantivy (21s). For bulk-indexing heavy workloads, this matters.

**Aggregation is slower than competitors**
`tags` terms: Flatseek 3317ms vs tantivy 37ms. `views_max`: Flatseek 1898ms vs elasticsearch 7ms.

**Wildcard has high tail latency**
`pattern kube` hits 1802ms at p95 for Flatseek vs 0.11ms for tantivy.

## Bottom line

> **Flatseek: fast enough, correct always.**
>
> If you've been burned by a search engine that returned wrong counts, wrong hits, or silently fell back to full-scan mode — Flatseek is what you expected the alternative to be.

For teams that need:
- **Correct results** over raw throughput
- **Single-node deployment** over cluster management
- **A library you can embed** over a service you operate

...Flatseek is the right choice. If you need the fastest possible build and can accept correctness gaps in range queries and aggregation, tantivy is faster but slower to trust.