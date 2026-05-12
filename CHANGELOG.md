# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.4] - 2026-05-08

### Hosting & UX
- **HuggingPost** — billion-row indexes hosted on HuggingFace (free)
  - Public datasets: `https://huggingface.co/buckets/flatseek/flatdata` (adsb, article, sosmed, standard — 10M rows each)
- **Flatlens** — visual search dashboard at [flatlens.flatseek.io](https://flatlens.flatseek.io)
  - Direct links with bucket+index pre-filled: `flatlens.flatseek.io/?bucket=...&index=...`

### Security Fixes
- **Critical**: Server-global password storage removed (`IndexManager._index_passwords`)
  - Password MUST now come from `x-index-password` HTTP header every request
  - Prevents cross-session data leakage where Browser A's auth bleeds to Browser B
- `_stats` and `_mapping` endpoints now return 403 for encrypted indexes without password
- Engines for encrypted bucket URLs are never cached (fresh engine per request)
- `_authenticate` now verifies password by decrypting a known file before marking as authenticated

### HuggingFace Bucket URL Support
- **New URL pattern**: `https://huggingface.co/buckets/{owner}/{repo}` is now supported
  - Buckets use `/resolve/{index_name}` (index IS the revision), not `/resolve/main`
- **Index listing** for buckets: HTML scraping of HF web UI to extract index names
- `_list_url_indices` handles three patterns:
  - Multi-index repos (root folder with index subdirs)
  - Direct index repos (single index named after repo)
  - **NEW** Bucket repos (uses web UI parsing for index discovery)

### API Improvements
- Bucket URL parameter (`?bucket=<url>`) support across all search/aggregate endpoints
- Debug endpoint `GET /_debug/chunks/{index}` shows chunk file paths for URL storage
- `_aggregate` endpoint supports both POST body and GET query params

### CLI Improvements
- `flatseek build --bucket <url>` for building indexes from HuggingFace bucket URLs

## [0.1.3] - 2026-05-07
### Changed
- Lazy dashboard mounting (not loaded until first access)
- Version bump

## [0.1.2] - 2026-05-06
### Added
- Initial release documentation