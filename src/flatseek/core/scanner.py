"""Auto-discover CSV files, detect schema, link common columns."""

import csv
import os
import json
import re
from pathlib import Path
from collections import defaultdict
import difflib


# Storage types for Fastseek type system.
# These map to how data is indexed and stored, separate from semantic classifiers.
STORAGE_TYPES = (
    "BOOL",      # true/false, 1/0, yes/no
    "INT",       # whole numbers
    "FLOAT",     # decimal numbers
    "DATE",      # date / datetime strings
    "KEYWORD",   # short exact-match values (codes, statuses, IDs)
    "TEXT",      # free text / long strings (trigrams, wildcard search)
    "ARRAY",     # JSON array: ["a", "b", "c"]
    "OBJECT",    # JSON object: {"key": "value"}
)

# Date patterns — matched as regex
_DATE_PATTERNS = (
    (r"^\d{4}-\d{2}-\d{2}$", "YYYY-MM-DD"),           # 2026-04-19
    (r"^\d{4}/\d{2}/\d{2}$", "YYYY/MM/DD"),
    (r"^\d{2}-\d{2}-\d{4}$", "DD-MM-YYYY"),
    (r"^\d{2}/\d{2}/\d{4}$", "DD/MM/YYYY"),
    (r"^\d{8}$",              "YYYYMMDD"),
    (r"^\d{12}$",             "YYYYMMDDHHmm"),
    (r"^\d{14}$",             "YYYYMMDDHHmmss"),
    (r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", "ISO8601"),
    (r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", "YYYY-MM-DD HH:mm:ss"),
    (r"^\d{2}[-/]\d{2}[-/]\d{4}$", "DD-MM-YYYY or DD/MM/YYYY"),
)

_COMPILED_DATE = [(re.compile(p), fmt) for p, fmt in _DATE_PATTERNS]

# Boolean values
_BOOL_TRUE = {"true", "yes", "1", "y", "t", "aktif", "ada", "exists", "enabled"}
_BOOL_FALSE = {"false", "no", "0", "n", "f", "nonaktif", "tidak", "none", "disabled", "inactive"}


def detect_type(samples):
    """Detect column storage type from samples.

    Returns one of: BOOL, INT, FLOAT, DATE, KEYWORD, TEXT, ARRAY, OBJECT

    Priority order:
    1. ARRAY  — if any value looks like a JSON array
    2. OBJECT — if any value looks like a JSON object
    3. DATE   — if all values match a date pattern
    4. BOOL   — if all values are boolean-like
    5. INT    — if all values parse as integers
    6. FLOAT  — if all values parse as floats
    7. KEYWORD — if avg token count <= _TEXT_MIN_WORDS (short exact values)
    8. TEXT   — free text / long strings
    """
    non_empty = [s.strip() for s in samples if s and s.strip()]
    if not non_empty:
        return "KEYWORD"   # empty columns are treated as keyword (minimal indexing)

    n = min(len(non_empty), 200)

    # 1. ARRAY check
    arr_count = sum(1 for s in non_empty[:n]
                    if isinstance(s, str) and s.startswith("[") and s.endswith("]"))
    if arr_count >= n * 0.7:
        return "ARRAY"

    # 2. OBJECT check
    obj_count = sum(1 for s in non_empty[:n]
                    if isinstance(s, str) and s.startswith("{") and s.endswith("}"))
    if obj_count >= n * 0.7:
        return "OBJECT"

    # 3. DATE check
    date_count = 0
    for s in non_empty[:n]:
        for compiled_pat, _ in _COMPILED_DATE:
            if compiled_pat.match(s):
                date_count += 1
                break
    if date_count >= n * 0.8:
        return "DATE"

    # 4. BOOL check
    bool_count = sum(1 for s in non_empty[:n]
                     if s.lower() in _BOOL_TRUE or s.lower() in _BOOL_FALSE)
    if bool_count >= n * 0.8:
        return "BOOL"

    # 5. Numeric checks
    # IMPORTANT: strings starting with 0 or + are NOT numeric —
    # leading zeros (phone numbers, IDs) must be preserved as strings.
    int_count = 0
    float_count = 0
    for s in non_empty[:n]:
        stripped = s.replace(",", "").strip()
        # Leading 0 or + means it's not a native integer — preserve as string.
        # This handles phone numbers (0812...), IDs (001234...), E.164 (+628...).
        if stripped.startswith(("0", "+")):
            continue  # not numeric — will fall to KEYWORD/TEXT
        try:
            int(stripped)
            int_count += 1
            continue
        except ValueError:
            pass
        try:
            float(stripped)
            float_count += 1
            continue
        except ValueError:
            pass

    if int_count == n:
        return "INT"
    if int_count + float_count == n:
        return "FLOAT"

    # 6. KEYWORD vs TEXT based on token density
    # avg tokens >= 2 → TEXT (multi-word phrases need trigrams for wildcard)
    # avg tokens < 2 → KEYWORD (single token = exact match)
    total_tokens = sum(len(s.split()) for s in non_empty[:n])
    avg_tokens = total_tokens / n
    if avg_tokens >= 2:
        return "TEXT"

    return "KEYWORD"




SKIP_DIRS = {"data", "testdata", "tmp", ".git", "__pycache__"}

_DATA_EXTS = (
    ".csv", ".json", ".jsonl", ".ndjson",
    ".csv.gz", ".csv.bz2",
    ".json.gz", ".json.bz2",
    ".jsonl.gz", ".jsonl.bz2",
    ".ndjson.gz", ".ndjson.bz2",
)

def discover_csvs(directory):
    """Find all CSV and JSON files in directory, including .gz and .bz2 variants."""
    result = []
    for root, dirs, files in os.walk(directory):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for f in sorted(files):
            if any(f.endswith(e) for e in _DATA_EXTS):
                result.append(os.path.join(root, f))
    return result


def scan_file(path, sample_rows=200):
    """Read header + sample rows to detect schema."""
    rows = []
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        reader = csv.DictReader(fh)
        headers = reader.fieldnames or []
        for i, row in enumerate(reader):
            rows.append(row)
            if i >= sample_rows:
                break

    columns = {}
    for col in headers:
        samples = [row.get(col, "") for row in rows]
        columns[col] = {
            "type": detect_type(samples),
            "sample_count": len(samples),
            "non_null": sum(1 for s in samples if s.strip()),
        }

    return {
        "file": os.path.abspath(path),
        "headers": headers,
        "columns": columns,
        "row_estimate": sum(1 for _ in open(path, "r", encoding="utf-8", errors="replace")) - 1,
    }


def similarity_score(a, b):
    """Fuzzy similarity between two column names."""
    a_norm = a.lower().strip().replace("_", " ").replace("-", " ")
    b_norm = b.lower().strip().replace("_", " ").replace("-", " ")
    if a_norm == b_norm:
        return 1.0
    if a_norm in b_norm or b_norm in a_norm:
        return 0.85
    return difflib.SequenceMatcher(None, a_norm, b_norm).ratio()


def link_columns(file_schemas, threshold=0.85):
    """Link columns across files that have same/similar names."""
    # Collect all column names across files
    col_map = defaultdict(list)  # unified_name -> [(file_idx, original_col)]

    # First pass: exact matches
    seen = {}
    for fi, schema in enumerate(file_schemas):
        for col in schema["headers"]:
            key = col.lower().strip().replace(" ", "_").replace("-", "_")
            col_map[key].append((fi, col))

    # Second pass: fuzzy match columns that don't have exact matches
    all_known = set(col_map.keys())
    for fi, schema in enumerate(file_schemas):
        for col in schema["headers"]:
            col_norm = col.lower().strip().replace(" ", "_").replace("-", "_")
            if col_norm in all_known:
                continue
            # Find closest match
            best_match = None
            best_score = 0
            for known in all_known:
                score = similarity_score(col, known.replace("_", " "))
                if score > best_score and score >= threshold:
                    best_score = score
                    best_match = known
            if best_match:
                col_map[best_match].append((fi, col))
            else:
                all_known.add(col_norm)
                col_map[col_norm].append((fi, col))

    return dict(col_map)


def scan(directory):
    """Full scan + schema linking."""
    csvs = discover_csvs(directory)
    if not csvs:
        print(f"No CSV files found in {directory}")
        return None

    file_schemas = []
    total_rows = 0
    for path in csvs:
        schema = scan_file(path)
        file_schemas.append(schema)
        total_rows += schema["row_estimate"]
        print(f"  Found: {path} ({schema['row_estimate']:,} rows, {len(schema['headers'])} cols)")

    links = link_columns(file_schemas)

    unified = {
        "files": [s["file"] for s in file_schemas],
        "file_schemas": file_schemas,
        "total_files": len(file_schemas),
        "total_rows": total_rows,
        "unified_columns": {
            k: {
                "files": [(fi, col) for fi, col in v],
                "type": next(
                    file_schemas[fi]["columns"][col]["type"]
                    for fi, col in v
                    if col in file_schemas[fi]["columns"]
                ),
            }
            for k, v in links.items()
        },
    }

    print(f"\nScanned {len(csvs)} files, {total_rows:,} total rows")
    print(f"Unified {len(unified['unified_columns'])} columns:")
    for col, info in unified["unified_columns"].items():
        n_files = len(set(fi for fi, _ in info["files"]))
        print(f"  {col} ({info['type']}, in {n_files} file(s))")

    return unified
