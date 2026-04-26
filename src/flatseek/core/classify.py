"""Semantic column classifier: detect KEYWORD, TEXT, DATE, FLOAT, INT, etc.

Usage:
    python classify.py <csv_dir> [-o data/column_map.json]

Output column_map.json maps each file's columns to canonical types.
Review and edit before running build.
"""

import re
import csv
import os
import sys
import json
import threading
import itertools
import time


class _BoundedTextReader:
    """Wraps a text stream; returns '' (EOF) after max_bytes decompressed bytes.

    Used to hard-limit how much data is decompressed when sampling compressed
    files.  Prevents OOM when a bad file or slow decompressor reads far more
    than the N sample rows we need.
    """
    def __init__(self, f, max_bytes):
        self._f         = f
        self._remaining = max_bytes

    def readline(self):
        if self._remaining <= 0:
            return ""
        line = self._f.readline()
        self._remaining -= len(line.encode("utf-8", errors="replace"))
        return line

    def __iter__(self):
        while True:
            line = self.readline()
            if not line:
                break
            yield line

    # csv.DictReader accesses .fieldnames which calls readline() internally —
    # no extra interface needed beyond readline() and __iter__.


# ─── Semantic type classifier ─────────────────────────────────────────────────
#
# Each entry maps a semantic type → storage type + optional normalization hints.
# The semantic type IS the storage type returned to the caller.
# Normalization (phone, date) is determined by column NAME patterns, not type name.

TYPES = {
    "KEYWORD": {
        # Short exact-match values — IDs, codes, statuses, categories.
        # No trigrams. Leading zeros preserved.
        "aliases": [
            # Status / enum
            "status", "state", "kondisi", "keadaan", "flag", "aktif",
            "active", "enabled", "tipe", "type", "jenis", "category",
            # IDs / codes (leading zeros preserved as KEYWORD)
            "id_number", "password", "pass", "pwd", "hash", "passwd", "kata_sandi",
            "username", "uname", "login", "handle", "nickname", "nick", "user_name",
            # Short categorical
            "gender", "sex", "jenis_kelamin", "kelamin",
            "province", "provinsi", "prov", "state", "region", "wilayah", "propinsi",
            "country", "negara", "nationality", "negara_asal",
            "city", "kota", "town",
            # Phone — column name hint for normalize_value
            "phone", "hp", "handphone", "telp", "telephone", "mobile",
            "no_hp", "nomor_hp", "nomor_telepon", "nohp", "telepon",
            "cell", "cellphone", "no_telp", "phone_number",
        ],
        "values": {
            # Status values
            "active", "inactive", "aktif", "nonaktif", "enabled", "disabled",
            "pending", "approved", "rejected", "open", "closed",
            "y", "n", "yes", "no", "true", "false", "1", "0",
            # Gender values
            "m", "f", "male", "female", "laki", "perempuan", "l", "p", "pria", "wanita",
        },
        "storage": "KEYWORD",
    },
    "TEXT": {
        # Free-text columns — trigrams enabled, supports wildcard search.
        # Names, addresses, descriptions, notes.
        "aliases": [
            "name", "full_name", "fullname", "nama_lengkap",
            "customer_name", "firstname", "lastname", "first_name",
            "last_name", "nama_depan", "nama_belakang", "nama_ibu",
            "nama_ayah", "nama",
            "address", "alamat", "addr", "street", "jalan",
            "domisili", "alamat_lengkap", "alamat_rumah",
            "text", "teks", "deskripsi", "description", "keterangan",
            "notes", "catatan", "caption", "message",
            # Place-of-birth is a location, free-text
            "tempat_lahir",
            # Lat/lon as text
            "lat_lon", "location", "koordinat", "coordinates",
        ],
        "storage": "TEXT",
    },
    "DATE": {
        # Date / datetime values — normalized to YYYYMMDD or YYYYMMDDHHmm.
        # Birthday and generic date both use DATE storage + column-name hint
        # for appropriate normalization.
        "aliases": [
            "birthday", "dob", "birth_date", "birthdate", "date_of_birth",
            "tgl_lahir", "tanggal_lahir", "lahir", "birth", "born",
            "tgl_bergabung", "joined", "join_date", "created_at", "updated_at",
            "tgl_daftar", "register_date", "tgl_registrasi", "tanggal_daftar",
            "tgl_buat", "tgl_update", "date", "tanggal",
        ],
        "pattern": r"^\d{4}[-/|.]\d{2}[-/|.]\d{2}$|^\d{8}$|^\d{2}[-/|.]\d{2}[-/|.]\d{4}$",
        "storage": "DATE",
    },
    "FLOAT": {
        # Numeric columns — indexed as-is for range queries.
        "aliases": [
            "jumlah", "total", "amount", "count", "qty", "quantity",
            "nilai", "angka", "skor", "score",
            "no", "nomor", "nomer",
            "price", "harga", "discount", "diskon",
            "latitude", "lat", "longitude", "lon", "lng",
            "temperature", "temp", "humidity", "kelembaban",
        ],
        "pattern": r"^\d+(\.\d+)?$",
        "storage": "FLOAT",
    },
    "BOOL": {
        "aliases": [],
        "values": {"true", "false", "yes", "no", "y", "n", "1", "0", "aktif", "nonaktif"},
        "storage": "BOOL",
    },
    "ARRAY": {
        "aliases": ["tags", "kategori", "categories", "labels", "array"],
        "storage": "ARRAY",
    },
    "OBJECT": {
        "aliases": [],
        "storage": "OBJECT",
    },
    "EMAIL": {
        "aliases": ["email", "e_mail", "email_address", "mail", "surel"],
        "pattern": r"^[\w.+\-]+@[\w\-]+\.[a-z]{2,}$",
        "storage": "KEYWORD",   # email stored as KEYWORD (exact match)
    },
    "PHONE": {
        "aliases": ["phone", "hp", "handphone", "telp", "telephone", "mobile",
                    "no_hp", "nomor_hp", "nomor_telepon", "nohp", "telepon",
                    "cell", "cellphone", "no_telp", "phone_number"],
        "pattern": r"^[0+(][\d\s()\-]{6,15}$",
        "storage": "KEYWORD",   # phone stored as KEYWORD (leading zeros)
    },
}


def _norm(s):
    return s.lower().strip().replace(" ", "_").replace("-", "_")


# ─── Pre-computed lookup structures (built once at module load) ───────────────
#
# Avoids re-normalising aliases and re-compiling regexes on every classify call.

# Exact alias → semantic_type  (O(1) dict lookup)
_ALIAS_EXACT: dict = {}
for _st, _info in TYPES.items():
    for _a in _info.get("aliases", []):
        _ALIAS_EXACT[_norm(_a)] = _st

# Partial alias list for substring matching — pre-normalised
_ALIAS_PARTIAL: list = []   # [(normalised_alias, sem_type), ...]
for _st, _info in TYPES.items():
    for _a in _info.get("aliases", []):
        _ALIAS_PARTIAL.append((_norm(_a), _st))

# Pre-compiled patterns with their types — avoids re.compile on every call
_PATTERNS: list = []   # [(compiled_re, sem_type), ...]
for _st, _info in TYPES.items():
    if "pattern" in _info:
        _PATTERNS.append((re.compile(_info["pattern"], re.IGNORECASE), _st))

# Value sets with their types — for fast categorical matching
_VALUE_SETS: list = []  # [(frozenset, sem_type), ...]
for _st, _info in TYPES.items():
    if "values" in _info:
        _VALUE_SETS.append((frozenset(_info["values"]), _st))


def _get_storage(sem_type):
    """Return the storage type for a semantic type, falling back to sem_type itself."""
    info = TYPES.get(sem_type, {})
    return info.get("storage", sem_type)


from flatseek.core.scanner import detect_type


def infer_type(col_name: str, value: str) -> str:
    """Infer storage type from column name and a single sample value.

    Convenience wrapper around classify_column() for single-value inference.
    Used by the API layer — for bulk classification use classify_column() instead.
    Returns one of: TEXT, KEYWORD, DATE, FLOAT, BOOL, ARRAY, OBJECT.
    """
    # Empty / null → KEYWORD
    if not value or (isinstance(value, str) and not value.strip()):
        return "KEYWORD"
    # classify_column wants a list of samples; pass single value
    sem_type, _ = classify_column(col_name, [value])
    return sem_type


def classify_column(col_name, samples):
    """Return (semantic_type, confidence) for a column.

    Returns storage type directly — TEXT, KEYWORD, DATE, FLOAT, BOOL, ARRAY, OBJECT.
    Falls back to detect_type() when no semantic alias matches.
    """
    col_key = _norm(col_name)
    non_empty = [s.strip() for s in samples if s and s.strip()]

    # 1. Exact alias match — O(1) dict lookup
    if col_key in _ALIAS_EXACT:
        sem = _ALIAS_EXACT[col_key]
        return _get_storage(sem), 1.0

    # 2. Partial alias match — aliases pre-normalised, no re-compute per call
    for alias, sem_type in _ALIAS_PARTIAL:
        if alias in col_key or col_key in alias:
            return _get_storage(sem_type), 0.8

    if not non_empty:
        return "KEYWORD", 0.0

    # 3. Fixed value set (e.g. gender, status) — value sets pre-extracted
    for values, sem_type in _VALUE_SETS:
        n = min(50, len(non_empty))
        ratio = sum(1 for s in non_empty[:n] if s.lower() in values) / n
        if ratio >= 0.8:
            return _get_storage(sem_type), round(ratio, 2)

    # 4. Regex pattern match — patterns pre-compiled (email, phone, dates)
    for pat, sem_type in _PATTERNS:
        n = min(100, len(non_empty))
        matches = sum(1 for s in non_empty[:n] if pat.match(s))
        ratio = matches / n
        if ratio >= 0.7:
            return _get_storage(sem_type), round(ratio, 2)

    # 5. No semantic match — use detect_type (value-based inference)
    return detect_type(non_empty), 0.5


def _sample_file(path, sample_rows=300, delimiter=",", columns=None):
    """Return (headers, list_of_row_dicts) from a CSV or JSON file.

    Transparently handles .gz and .bz2 compressed files.

    columns: explicit list of column names. When provided the first row of the
             file is treated as data (not a header), which is necessary for
             headerless files.
    """
    from flatseek.core.builder import _open_source, _inner_ext, _iter_json_file, _compress_ext
    ext  = _inner_ext(path)
    cext = _compress_ext(path)

    if ext == ".csv":
        rows, headers = [], []
        # newline="" is required by csv module docs — prevents TextIOWrapper from
        # consuming \r that csv should handle itself.
        with _open_source(path, newline="") as f:
            # For compressed files, wrap in a byte-limit guard so we never
            # decompress more than ~5 MB regardless of what the stream does.
            # Without this, a bug (or bad file) could read gigabytes for sampling.
            if cext:
                f = _BoundedTextReader(f, max_bytes=5 * 1024 * 1024)
            reader = csv.DictReader(f, fieldnames=columns or None, delimiter=delimiter)
            headers = list(reader.fieldnames or [])
            for i, row in enumerate(reader):
                rows.append(dict(row))
                if i >= sample_rows:
                    break
        return headers, rows

    elif ext in (".json", ".jsonl", ".ndjson"):
        rows, headers = [], None
        for hdrs, row in _iter_json_file(path):
            if headers is None:
                headers = hdrs
            rows.append(row)
            if len(rows) >= sample_rows:
                break
        return (headers or []), rows

    return [], []


def classify_file(path, sample_rows=300, delimiter=",", columns=None, type_overrides=None):
    """Classify all columns in a CSV or JSON file.

    columns:        explicit list of column names for headerless files.
    type_overrides: {col_name: sem_type} — user-confirmed types that bypass auto-detection.

    Returns:
        {original_col_name: {semantic_type, canonical_key, confidence},
         "_headerless": True,          # only present when columns were provided
         "_columns": ["col1", ...],    # stored so builder can re-use on resume
         "_delimiter": "#"}            # stored for the same reason
    """
    headers, rows = _sample_file(path, sample_rows, delimiter=delimiter, columns=columns)
    if not headers:
        return {}

    overrides = type_overrides or {}
    result = {}
    for col in headers:
        if col in overrides:
            sem_type   = overrides[col]
            confidence = 1.0   # user explicitly specified
        else:
            samples    = [r.get(col, "") for r in rows]
            sem_type, confidence = classify_column(col, samples)
        canonical = _norm(col)
        result[col] = {
            "semantic_type": sem_type,
            "confidence": round(confidence, 2),
            "canonical_key": canonical,
        }

    # Persist headerless metadata so the builder can reconstruct the correct
    # DictReader on resume / incremental builds without requiring --columns again.
    if columns:
        result["_headerless"] = True
        result["_columns"]    = list(columns)
        result["_delimiter"]  = delimiter

    return result


def build_column_map(csv_dir, output_path=None, delimiter=",", columns=None, type_overrides=None):
    """Classify all CSVs in a directory and write column_map.json.

    columns:        explicit list of column names for headerless files.
    type_overrides: {col_name: sem_type} — user-confirmed types.

    Returns:
        {relative_filename: {col_name: {semantic_type, canonical_key, confidence}}}
    """
    from flatseek.core.scanner import discover_csvs

    csvs = discover_csvs(csv_dir)
    if not csvs:
        print(f"No data files found in {csv_dir}")
        return {}

    from flatseek.core.builder import _compress_ext, _inner_ext

    column_map = {}
    for csv_path in csvs:
        rel        = os.path.relpath(csv_path, csv_dir)
        size_bytes = os.path.getsize(csv_path)
        size_mb    = size_bytes / 1024 ** 2
        cext       = _compress_ext(csv_path)

        if cext:
            cname = cext.lstrip(".")
            note  = f"{cname}, {size_mb:.1f} MB on disk — streaming, samples first 300 rows only"
        else:
            note  = f"{size_mb:.1f} MB"

        print(f"\n  {rel}  ({note})")
        sys.stdout.flush()

        t0 = time.time()

        if cext:
            # Show a live spinner on stderr while decompression/sampling runs.
            # Spinner is on stderr so it doesn't mix with the column output on stdout.
            _done   = threading.Event()
            _frames = itertools.cycle(r"|/-\\")

            def _spin(cname=cname):
                while not _done.is_set():
                    elapsed = time.time() - t0
                    sys.stderr.write(f"\r  Decompressing {cname}... {elapsed:.1f}s")
                    sys.stderr.flush()
                    _done.wait(0.1)
                sys.stderr.write("\r" + " " * 36 + "\r")
                sys.stderr.flush()

            _t = threading.Thread(target=_spin, daemon=True)
            _t.start()
            try:
                classifications = classify_file(csv_path, delimiter=delimiter,
                                                columns=columns, type_overrides=type_overrides)
            finally:
                _done.set()
                _t.join()
        else:
            classifications = classify_file(csv_path, delimiter=delimiter,
                                            columns=columns, type_overrides=type_overrides)

        elapsed = time.time() - t0

        for col, info in classifications.items():
            if col.startswith("_"):
                continue
            flag = " ✓" if info["confidence"] >= 0.8 else " ?"
            print(f"    {col!r:35s} → {info['semantic_type']:12s} ({info['confidence']:.0%}){flag}")

        if cext:
            print(f"    (sampled in {elapsed:.1f}s)")

        column_map[rel] = classifications

    if output_path:
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(column_map, f, indent=2, ensure_ascii=False)
        print(f"\nSaved: {output_path}")
        print("Review and edit before running build.")

    return column_map


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("csv_dir")
    p.add_argument("-o", "--output", default="./data/column_map.json")
    args = p.parse_args()
    build_column_map(args.csv_dir, args.output)
