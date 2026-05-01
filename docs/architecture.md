# Architecture

Flatseek stores a **trigram inverted index** on disk using memory-mapped files.

## Storage layout

```
data/
  my_index/
    index/           # Trigram postings (mmap'd, block-compressed)
    docs/             # Column-oriented document store
    stats.json         # Doc count, index size
    mapping.json       # Type per column
```

## How trigram search works

To match `*john*`, Flatseek:
1. Extracts trigrams (`joh`, `ohn`) from the query
2. Intersects their posting lists
3. Returns matching doc IDs

Postings are sorted, compressed, and skip-pointer enabled. Wildcard queries cost the same algorithmic complexity as a literal lookup. Resident memory stays low regardless of index size.

## Why memory-mapped?

Memory-mapping means the OS handles caching — hot data stays in page cache automatically, cold data doesn't consume RAM. Seeking into a 50GB index behaves like a much smaller one if working set fits in memory.