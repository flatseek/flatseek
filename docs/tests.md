# Flatseek Test Coverage Matrix

> Generated: 2026-07-14 | Updated: 2026-07-14 | Tests added: sort, pagination, pack, unpack, export, verify, slice, delete_query fix, parquet_build, bulk_upsert

**TEST** column = test file name if test exists, `—` if not yet covered.

---

## Index Management

| Feature | CLI | Library | REST API |
|---------|-----|---------|----------|
| Build index from CSV | test_cli.py | — | — |
| Build index (parallel) | — | — | — |
| Build index from JSONL | — | — | — |
| Build index from Parquet | test_parquet_build.py | test_parquet_build.py | — |
| Classify CSV columns | test_cli.py | — | — |
| Generate build plan | test_cli.py | — | — |
| Generate dummy data | — | — | — |
| Create empty index | — | — | test_api.py |
| Delete index | test_cli.py | — | — |
| Rename index | — | — | test_api.py |
| Index stats | test_cli.py | — | test_api.py |
| Index mapping | — | — | test_api.py |
| WAL merge | — | — | — |
| WAL info | — | — | — |
| Pack to `.fsk` | test_cli.py | — | — |
| Unpack `.fsk` | test_cli.py | — | — |
| Compress index | test_cli.py | — | — |
| Compact (reclaim space) | — | — | — |
| Verify integrity | test_cli.py | — | — |
| Export to JSONL/CSV | test_cli.py | — | — |
| Slice index by query | test_cli.py | — | — |

---

## Encryption & Security

| Feature | CLI | Library | REST API |
|---------|-----|---------|----------|
| Encrypt index | test_cli.py | — | test_api.py |
| Decrypt index | test_cli.py | — | — |
| Authenticate (passphrase) | — | — | test_api.py |
| Check if encrypted | — | — | test_api.py |
| Generate passphrase token | — | — | — |
| Generate license key | — | — | — |
| Enclosed encryption | — | — | — |

---

## Search

| Feature | CLI | Library | REST API |
|---------|-----|---------|----------|
| Full-text search | test_cli.py | — | test_search.py |
| Search (GET) | — | — | test_api.py |
| Multi-index search | — | ✅ `cases/crawl.py` (functional) | — |
| Cross-lookup (join 2 indexes) | — | ✅ `cases/crawl.py` (functional) | — |
| Wildcard trigram | test_search.py | — | — |
| Field filters | test_search.py | — | — |
| Numeric range | test_search.py | — | — |
| Boolean AND | test_search.py | — | — |
| Boolean OR | test_search.py | — | — |
| Boolean NOT | test_search.py | — | — |
| Phrase search | test_search.py | — | — |
| Sort | test_cli.py | test_search.py | — |
| Pagination | — | test_search.py | — |
| Multi-search (batch) | — | — | — |
| Validate query | — | — | test_api.py |
| Debug chunk listing | — | — | — |
| Cross-dataset join | test_cli.py | — | — |
| Upload from URL | — | — | — |
| Preview from URL | — | — | — |

---

## Aggregation

| Feature | CLI | Library | REST API |
|---------|-----|---------|----------|
| Terms aggregation | — | test_search.py | — |
| Stats aggregation | — | test_search.py | — |
| Date histogram | — | test_search.py | — |
| Numeric histogram | — | test_search.py | — |
| Cardinality (HLL) | — | — | — |
| Min / Max / Sum / Avg | — | test_search.py | — |
| Aggregation + query filter | — | test_search.py | — |

---

## Document Operations

| Feature | CLI | Library | REST API |
|---------|-----|---------|----------|
| Upsert by natural key | — | test_update.py | test_api.py |
| Insert (no ID) | — | test_write_ops.py | — |
| Partial update by ID | — | test_update.py | test_api.py |
| Update by query | — | — | test_api.py |
| Delete by natural key | — | test_write_ops.py | test_api.py |
| Delete by Lucene query | — | test_write_ops.py | test_api.py |
| Get by numeric doc_id | — | — | — |
| Bulk — ES NDJSON | — | test_write_ops.py | test_api.py |
| Bulk upsert queue | test_bulk_upsert.py | test_bulk_upsert.py | — |
| Bulk — JSON array | — | — | test_api.py |
| Count | test_cli.py | test_search.py | test_api.py |
| Export to JSONL | — | — | — |
| Export to CSV | — | — | — |
| Deduplicate docs | test_cli.py | — | — |
| Slice index by query | — | — | — |

---

## Storage Backends

| Feature | CLI | Library | REST API |
|---------|-----|---------|----------|
| Local filesystem | — | — | — |
| Amazon S3 | — | — | — |
| Vercel Blob | — | — | — |
| Remote URL (HTTP Range) | — | — | — |
| `.fsk` over HTTP Range | — | — | — |
| HuggingFace datasets via `?bucket=` | — | — | test_search.py |

---

## API Server

| Feature | CLI | Library | REST API |
|---------|-----|---------|----------|
| Start API + Dashboard | test_cli.py | — | — |
| Start API only | — | — | — |
| Dashboard only | — | — | — |
| Health check | — | — | test_api.py |
| List indices | — | — | test_api.py |
| Flush index | — | — | test_api.py |
| Index logs | — | — | test_api.py |
| Upload progress | — | — | test_api.py |
| Reset dashboard URL | — | — | — |

---

## Utilities

| Feature | CLI | Library | REST API |
|---------|-----|---------|----------|
| Natural language chat | — | — | — |
| Count docs | test_cli.py | test_search.py | test_api.py |
| Classify text | — | — | — |
| Scan with regex | — | — | — |

---

## Test Files

| File | Location | Coverage |
|------|----------|----------|
| [`test_cli.py`](src/flatseek/test/test_cli.py) | line 1 | classify(56), build(96), search(148), stats(184), compress(195), encrypt/decrypt(210), delete(230), plan(246), serve/api(265), dedup(291), join(307) |
| [`test_api.py`](src/flatseek/test/test_api.py) | line 1 | root(16), health(27), indices(39), index CRUD(53), mapping(126), search(138), aggregate(158), count(178), bulk(190), flush(241), logs(253), stats(266), rename(278), encrypt(290), auth(318), validate(347), delete_by_query(359) |
| [`test_search.py`](src/flatseek/test/test_search.py) | line 1 | expansion(298), keyword(318), text(336), wildcard(352), array(367), object/nested(387), numeric(455), range(474), date(490), boolean(503), operators(515), match-all(557), empty(565), programmatic(578), aggregations(596), HuggingFace URL(663) |
| [`fixtures.py`](src/flatseek/test/fixtures.py) | line 1 | `generate_sample_events_csv`, `IndexContext`, `cleanup_index` |
| [`test_parquet_build.py`](src/flatseek/test/test_parquet_build.py) | line 1 | basic build+search, multiple files, mixed Parquet+CSV, numeric types, boolean, nested JSON, chunked reading, scanner integration (8 tests) |
| [`test_bulk_upsert.py`](src/flatseek/test/test_bulk_upsert.py) | line 1 | concurrent load test for UpsertQueue |

---

## Test Detail Reference

### test_cli.py — `src/flatseek/test/test_cli.py`

| Class | Line | Tests |
|-------|------|-------|
| `TestClassify` | 56 | `test_classify_detects_columns`, `test_classify_with_custom_separator` |
| `TestBuild` | 96 | `test_build_creates_index_directory`, `test_build_with_dedup` |
| `TestSearch` | 148 | `test_search_index_exists`, `test_search_with_term` |
| `TestStats` | 184 | `test_stats_shows_index_info` |
| `TestCompress` | 195 | `test_compress_index` |
| `TestEncryptDecrypt` | 210 | `test_encrypt_decrypt_roundtrip` |
| `TestDelete` | 230 | `test_delete_index` |
| `TestPlan` | 246 | `test_plan_generates_build_plan` |
| `TestServeApi` | 265 | `test_serve_command_accepts_port` |
| `TestDedup` | 404 | `test_dedup_dry_run` |
| `TestJoin` | 420 | `test_join_command_args` |
| `TestPack` | 445 | `test_pack_creates_fsk_file`, `test_pack_with_passphrase` |
| `TestUnpack` | 496 | `test_unpack_restores_index_directory`, `test_unpack_with_passphrase` |
| `TestExport` | 569 | `test_export_all_jsonl`, `test_export_with_query_filter`, `test_export_csv_format` |
| `TestVerify` | 648 | `test_verify_healthy_index` |
| `TestSlice` | 673 | `test_slice_creates_new_index` |

---

### test_api.py — `src/flatseek/test/test_api.py`

| Class | Line | Tests |
|-------|------|-------|
| `TestRoot` | 16 | `test_root_returns_name_and_version` |
| `TestClusterHealth` | 27 | `test_cluster_health_returns_indices_count` |
| `TestIndicesList` | 39 | `test_indices_returns_empty_when_no_data` |
| `TestIndexCreate` | 53 | `test_create_and_delete_index`, `test_create_twice_returns_exists`, `test_delete_nonexistent_returns_404` |
| `TestMapping` | 126 | `test_get_mapping_returns_404_for_unknown_index` |
| `TestSearch` | 138 | `test_get_search_returns_404_for_unknown_index`, `test_post_search_returns_404_for_unknown_index` |
| `TestAggregate` | 158 | `test_post_aggregate_returns_404_for_unknown_index`, `test_get_aggregate_returns_404_for_unknown_index` |
| `TestCount` | 178 | `test_get_count_returns_404_for_unknown_index` |
| `TestBulkIndex` | 190 | `test_bulk_index_nonexistent_index_creates_it`, `test_bulk_index_response_has_indexed_field` |
| `TestUploadProgress` | 227 | `test_get_upload_progress_returns_for_unknown_index` |
| `TestFlush` | 241 | `test_flush_nonexistent_index_returns_404` |
| `TestLogs` | 253 | `test_get_logs_returns_for_unknown_index` |
| `TestStats` | 266 | `test_stats_returns_404_for_unknown_index` |
| `TestRename` | 278 | `test_rename_unknown_index_returns_404` |
| `TestEncrypt` | 290 | `test_encrypt_nonexistent_index_returns_404`, `test_decrypt_nonexistent_index_returns_404`, `test_encrypt_progress_nonexistent_returns_404` |
| `TestAuth` | 318 | `test_is_encrypted_nonexistent_returns_404`, `test_authenticate_nonexistent_index_returns_404`, `test_logout_unknown_index_returns_200` |
| `TestValidate` | 347 | `test_validate_nonexistent_index_returns_404` |
| `TestDeleteByQuery` | 359 | `test_delete_by_query_returns_404_for_nonexistent` |

---

### test_search.py — `src/flatseek/test/test_search.py`

| Class | Line | Tests |
|-------|------|-------|
| `TestExpansion` | 298 | `test_array_expanded_to_indexed_keys`, `test_object_expanded_to_dot_paths` |
| `TestKeywordExact` | 318 | `test_keyword_match` (parametrized) |
| `TestTextField` | 336 | `test_text_partial_match` (parametrized) |
| `TestWildcards` | 352 | `test_wildcard_patterns` (parametrized) |
| `TestArrayField` | 367 | `test_array_element_match` (parametrized) |
| `TestObjectDotPath` | 387 | `test_first_level_dot_path`, `test_deeply_nested_string_match`, `test_deeply_nested_numeric_range`, `test_deeply_nested_wildcard` (parametrized) |
| `TestNestedArrayIndex` | 437 | `test_indexed_array_access` (parametrized) |
| `TestNumericComparators` | 455 | `test_open_range` (parametrized) |
| `TestRangeQuery` | 474 | `test_inclusive_range` (parametrized) |
| `TestDateRange` | 490 | `test_date_range` (parametrized) |
| `TestBoolean` | 503 | `test_boolean_match` (parametrized) |
| `TestBooleanOperators` | 515 | `test_and`, `test_or`, `test_not`, `test_combined` (parametrized) |
| `TestMatchAll` | 557 | `test_star_returns_everything` |
| `TestEmptyResults` | 565 | `test_no_match_returns_zero` (parametrized) |
| `TestProgrammaticAPI` | 578 | `test_search_and`, `test_search_simple_term` (parametrized) |
| `TestAggregations` | 596 | `test_terms_on_keyword`, `test_terms_on_array_field`, `test_terms_on_status`, `test_date_histogram`, `test_avg`, `test_stats_bundle`, `test_min_max_sum`, `test_aggregation_with_query_filter` |
| `TestSort` | 869 | `test_sort_numeric_asc`, `test_sort_numeric_desc`, `test_sort_two_fields`, `test_sort_page_0_returns_correct_slice`, `test_sort_page_1_continues_from_page_0` |
| `TestPagination` | 906 | `test_page_0_returns_first_results`, `test_page_1_returns_next_results`, `test_page_beyond_results_is_empty`, `test_total_reflects_all_matching_docs`, `test_page_size_larger_than_corpus_returns_all` |
| `TestHuggingFaceBucketURL` | 941 | `test_bucket_url_resolve_path`, `test_dataset_url_resolve_path`, `test_list_indices_from_dataset` (@network), `test_search_via_api_with_bucket_param` (@network), `test_search_encrypted_dataset_with_correct_passphrase` (@network), `test_bucket_indices_endpoint` (@network) |

---

## Features Without Tests

### High Priority (widely used features)
- (All widely-used features now have tests — sort, pagination, pack, unpack, write ops)

### Medium Priority
- `POST /{index}/_compact` — REST API compaction
- WAL merge/info — CLI commands
- S3 / Vercel Blob storage backends
- `flatseek indices` — CLI list indices
- `flatseek flush` — CLI flush

### Low Priority
- `flatseek verify`
- Natural language chat (`flatseek chat`)
- `classify_text()` / `scanner.scan()`
- Percentiles aggregation (if it exists)
