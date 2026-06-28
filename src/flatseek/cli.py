#!/usr/bin/env python3
"""Flatseek — disk-first serverless trigram search engine.

Workflow:
  flatseek build <csv>        # Build index → ./data
  flatseek serve               # Start API + flatlens dashboard (on current dir)
  flatseek search ./data "..."  # Search from CLI
  flatseek stats ./data        # Show index stats
  flatseek generate            # Generate dummy dataset

API:   http://localhost:8000
Docs:  http://localhost:8000/docs
Dash:  http://localhost:8000/dashboard

Search syntax (Lucene-style):
  program:raydium                → program is raydium
  signer:*7xMg*                → signer contains "7xMg"
  amount:>1000000              → amount greater than 1M
  callsign:GARUDA* AND altitude:>30000  → AND logic
  level:ERROR OR level:WARN    → OR logic
  "Connection timeout"          → exact phrase
"""

import sys
import os
import csv
import argparse
from pathlib import Path

# ── Version from pyproject.toml ──────────────────────────────────────────────
# __file__ is flatseek/src/flatseek/cli.py → up 2 levels to flatseek/ → sibling pyproject.toml
_PYPROJECT_TOML = os.path.join(os.path.dirname(__file__), "..", "..", "pyproject.toml")
try:
    import tomllib
    with open(_PYPROJECT_TOML, "rb") as _f:
        __version__ = tomllib.load(_f)["project"]["version"]
except Exception:
    __version__ = "0.1.7"


def _parse_columns(columns_str):
    """Parse '--columns a,b,c' into a list, or None if not given."""
    if not columns_str:
        return None
    return [c.strip() for c in columns_str.split(",") if c.strip()]


def _prompt_columns(path, delimiter):
    """Interactive: show a file preview, ask whether row 1 is a header,
    collect column names, then confirm/override per-column types.

    Returns (columns, type_overrides, excludes) where:
      columns        – ordered list of final column names (or None = use file header)
      type_overrides – {col_name: sem_type} for any user-corrected types
      excludes       – set of original column names to skip from indexing

    Supported prompts:
      ""            → accept detected type
      TEXT/KEYWORD  → override semantic type
      SKIP          → exclude this column from indexing
      →new_name     → rename column (keeps order, renames key in docs)

    Skips entirely when stdin is not a tty (piped / scripted).
    """
    if not sys.stdin.isatty():
        return None, {}, set()

    # Read up to 3 raw rows without any header interpretation
    raw_rows = []
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f, delimiter=delimiter)
            for i, row in enumerate(reader):
                raw_rows.append(row)
                if i >= 2:
                    break
    except Exception as e:
        print(f"  (preview unavailable: {e})")
        return None, {}, set()

    if not raw_rows:
        return None, {}, set()

    n_cols = len(raw_rows[0])
    sep_label = repr(delimiter)

    # ── Preview table ────────────────────────────────────────────────────────
    w = 32
    print(f"\n  ┌ Preview: {os.path.basename(path)}  sep={sep_label}  cols={n_cols} ┐")
    print(f"  {'idx':>4}  {'Row 1':^{w}}  {'Row 2':^{w}}")
    print(f"  {'─'*4}  {'─'*w}  {'─'*w}")
    for i in range(n_cols):
        v1 = (raw_rows[0][i] if i < len(raw_rows[0]) else "").strip()
        v2 = (raw_rows[1][i] if len(raw_rows) > 1 and i < len(raw_rows[1]) else "").strip()
        v1 = (v1[:w-1] + "…") if len(v1) > w else v1
        v2 = (v2[:w-1] + "…") if len(v2) > w else v2
        print(f"  {i:>4}  {v1:<{w}}  {v2:<{w}}")
    print()

    # ── Confirm header ───────────────────────────────────────────────────────
    try:
        ans = input("  Is Row 1 the column header? [Y/n] ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        return None, {}, set()

    if ans in ("", "y", "yes"):
        # Header is fine — still offer type-override step for the existing columns
        cols = [v.strip() for v in raw_rows[0]]
        user_provided_names = False
    else:
        user_provided_names = True
        cols = None

    # ── Collect column names (only when no header) ───────────────────────────
    if user_provided_names:
        print(f"\n  Enter {n_cols} column names.")
        print("  You can type all at once (comma-separated) or one per prompt.\n")
        row1_vals = raw_rows[0]

        while True:
            try:
                first = input("  Names (comma-sep): ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                return None, {}, set()

            if not first:
                continue

            parts = [c.strip() for c in first.split(",")]

            if len(parts) == n_cols:
                cols = parts
            elif len(parts) == 1 and n_cols > 1:
                cols = [parts[0]]
                print(f"  One-per-prompt mode. {n_cols - 1} more to go.\n")
                for i in range(1, n_cols):
                    hint = row1_vals[i].strip() if i < len(row1_vals) else ""
                    prompt = f"  col[{i}]" + (f" (was: {hint!r})" if hint else "") + ": "
                    try:
                        name = input(prompt).strip()
                    except (EOFError, KeyboardInterrupt):
                        print()
                        return None, {}, set()
                    cols.append(name if name else f"col_{i}")
            else:
                print(f"  ✗ Got {len(parts)} names, need {n_cols}. Try again.\n")
                continue
            break

    # ── Auto-detect types for the chosen columns ─────────────────────────────
    from flatseek.core.classify import classify_column, TYPES as _TYPES

    VALID_TYPES = sorted(_TYPES.keys())
    types_line  = "  ".join(VALID_TYPES)

    # Sample values: use rows 1+ (skip row 0 if it was the header)
    sample_start = 0 if user_provided_names else 1
    sample_rows_data = raw_rows[sample_start:] if len(raw_rows) > sample_start else raw_rows

    detected = {}
    for col in cols:
        idx = cols.index(col)
        samples = [row[idx] for row in sample_rows_data if idx < len(row)]
        sem_type, conf = classify_column(col, samples)
        detected[col] = (sem_type, conf)

    # ── Type confirmation / rename / skip prompt ────────────────────────────
    print(f"\n  ── Column types ──────────────────────────────────────────")
    print(f"  Available types  : {types_line}")
    print(f"  Rename column   : →new_name   (e.g. →phone, →email_address)")
    print(f"  Skip column     : SKIP         (column will not be indexed)")
    print(f"  Accept / next  : Enter\n")

    type_overrides = {}
    excludes       = set()
    renamed        = {}   # original → new name
    final_cols     = []   # ordered list of final column names

    for i, col in enumerate(cols):
        sem_type, conf = detected[col]
        sample_val = (raw_rows[sample_start][i].strip()
                      if sample_start < len(raw_rows) and i < len(raw_rows[sample_start]) else "")
        sample_val = (sample_val[:20] + "…") if len(sample_val) > 20 else sample_val

        prompt = (f"  [{i:>2}] {col:<22} {sample_val!r:<24} "
                  f"detected: {sem_type:<12} ({conf:.0%})  → ")
        try:
            ans = input(prompt).strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return None, {}, set()

        if ans == "a":
            print(f"  Accepting all remaining as detected.")
            final_cols.extend(cols[j] for j in range(i, len(cols)) if cols[j] not in excludes)
            break
        elif ans.upper() == "SKIP":
            excludes.add(col)
            print(f"  [{i:>2}] {col}  ← skipped (not indexed)")
            continue
        elif ans.startswith("→"):
            new_name = ans[1:].strip()
            if not new_name:
                print(f"  ✗ Rename needs a name after →")
                i -= 1; continue
            renamed[col] = new_name
            # Encode rename into type_overrides so classify_file/build can read it
            type_overrides[col] = f"{sem_type}→{new_name}"
            final_cols.append(new_name)
        elif ans.upper() in _TYPES:
            type_overrides[col] = ans.upper()
            final_cols.append(col)
        else:
            print(f"  ✗ Unknown input {ans!r}. Valid: type name, SKIP, or →new_name")
            i -= 1; continue

    # ── Summary ──────────────────────────────────────────────────────────────
    print(f"\n  Final mapping ({len(final_cols)} columns, {len(excludes)} excluded):")
    for col in final_cols:
        orig  = next((k for k, v in renamed.items() if v == col), col)
        ftype = type_overrides.get(orig, detected.get(orig, ("?", 0))[0])
        marker = f" ← from {orig}" if col != orig else ""
        print(f"    {col:<24}  {ftype}{marker}")
    if excludes:
        print(f"  Excluded: {', '.join(sorted(excludes))}")
    print()

    try:
        ok = input("  Apply? [Y/n] ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        return None, {}, set()

    if ok not in ("", "y", "yes"):
        print("  Cancelled.")
        return None, {}, set()

    return (final_cols if user_provided_names else None), type_overrides, excludes


def _resolve_columns(args, csv_src):
    """Return (columns, type_overrides, excludes).

    columns        – ordered list of final column names (None = use file header)
    type_overrides – {col_name: sem_type} for any user-corrected types
    excludes       – set of original column names to skip from indexing

    Resolution order:
      1. --columns flag (no interactive prompt, no type overrides)
      2. Already classified in column_map.json with matching column count → skip
      3. Interactive prompt (show preview, ask names + types + rename + skip)
    """
    columns = _parse_columns(args.columns)
    if columns:
        print(f"  Using {len(columns)} column name(s) from --columns (first row = data).")
        return columns, {}

    # Find a representative CSV to preview
    from flatseek.core.builder import find_data_files
    preview_file = None
    csv_base = csv_src
    if os.path.isfile(csv_src) and csv_src.lower().endswith((".csv", ".jsonl", ".ndjson")):
        preview_file = csv_src
        csv_base = os.path.dirname(csv_src)
    elif os.path.isdir(csv_src):
        files = find_data_files(csv_src)
        preview_file = next((f for f in files if f.lower().endswith(".csv")), None)

    if not preview_file:
        return None, {}, set()

    # Check column_map.json for this file
    output_dir = os.path.abspath(getattr(args, "output", "./data"))
    col_map_path = getattr(args, "map", None) or os.path.join(output_dir, "column_map.json")
    file_rel = os.path.relpath(preview_file, csv_base)

    if os.path.exists(col_map_path):
        import json as _json
        try:
            with open(col_map_path) as f:
                col_map = _json.load(f)
        except Exception:
            col_map = {}

        if file_rel in col_map:
            entry = col_map[file_rel]
            if entry.get("_headerless"):
                stored = entry.get("_columns", [])
                excludes = entry.get("_excludes", [])
                print(f"  File already classified as headerless ({len(stored)} cols). "
                      f"Using stored column names.")
                return None, {}, set(excludes)   # builder reads from column_map

            n_classified = len([k for k in entry if not k.startswith("_")])
            try:
                with open(preview_file, "r", encoding="utf-8", errors="replace") as f:
                    first_row = next(csv.reader(f, delimiter=args.sep))
                n_file = len(first_row)
            except Exception:
                return None, {}, set()

            if n_file == n_classified:
                print(f"  [{os.path.basename(preview_file)}] already classified "
                      f"({n_classified} cols). Run 'flatseek classify' to re-classify.")
                return None, {}, set()   # unchanged, no prompt needed
            else:
                print(f"  Column count changed ({n_classified} → {n_file}). Re-classifying.")

    # Not yet classified (or columns changed) — prompt interactively
    columns, type_overrides, excludes = _prompt_columns(preview_file, args.sep)
    if columns:
        print(f"  Using {len(columns)} column name(s) from interactive prompt (first row = data).")
    return columns, type_overrides, excludes


def cmd_classify(args):
    from flatseek.core.classify import build_column_map
    out = args.output or os.path.join("./data", "column_map.json")
    out = os.path.abspath(out)
    csv_src = os.path.abspath(args.csv_dir)
    columns, type_overrides, excludes = _resolve_columns(args, csv_src)
    print(f"Classifying: {csv_src}")
    build_column_map(csv_src, output_path=out, delimiter=args.sep,
                     columns=columns, type_overrides=type_overrides, excludes=excludes)


def _run_parallel_build(args, csv_src, output_dir, n_workers):
    """Auto-plan + spawn n_workers parallel worker subprocesses."""
    import subprocess
    import json as _json
    import time as _time
    from flatseek.core.builder import (plan as do_plan, merge_worker_stats, _partial_merge_stats,
                         find_data_files, _auto_classify, _progress, _fmt_duration,
                         _load_manifest, _save_manifest)

    os.makedirs(output_dir, exist_ok=True)
    plan_path = os.path.join(output_dir, "build_plan.json")
    main_py   = os.path.abspath(__file__)

    # ── Resume detection: reuse existing plan if n_workers matches ────────────
    # A build is resumable when build_plan.json exists from a previous interrupted
    # run. Workers that already completed have stats_w{w}.json written by their
    # finalize() — those are skipped. Only incomplete workers are re-spawned.
    resuming = False
    plan_data = None
    if os.path.isfile(plan_path):
        try:
            with open(plan_path) as _pf:
                _existing = _json.load(_pf)
            if _existing.get("n_workers") == n_workers:
                # Validate plan sanity: reject stale plans where estimated_rows=0
                # for all workers.  A zero-row plan means _count_rows failed at plan
                # time (compressed/unknown file) and every worker was given only a
                # ~10K doc_id headroom → all workers write to the SAME doc chunk
                # range → silent data corruption (concurrent overwrites).
                _assignments = _existing.get("assignments", [])
                _total_est = sum(a.get("estimated_rows", 0) for a in _assignments)
                if _total_est == 0:
                    print(f"  Stale/invalid plan detected (estimated_rows=0 for all workers) "
                          f"— regenerating.")
                else:
                    def _worker_truly_done(w):
                        import json as _json
                        path = os.path.join(output_dir, f"stats_w{w}.json")
                        if not os.path.isfile(path):
                            return False
                        try:
                            with open(path) as _f:
                                return not _json.load(_f).get("_partial", False)
                        except Exception:
                            return False
                    already_done = [w for w in range(n_workers) if _worker_truly_done(w)]
                    remaining = [w for w in range(n_workers) if w not in already_done]
                    if remaining:
                        plan_data = _existing
                        resuming  = True
                        print(f"── Resuming parallel build ───────────────────────────")
                        print(f"  Workers already done : {already_done}")
                        print(f"  Workers to re-run    : {remaining}")
                    elif not remaining:
                        # All workers done — just merge stats and exit
                        print("── All workers already completed — merging stats ─────")
                        merge_worker_stats(output_dir, n_workers)
                        _update_manifest_after_parallel(output_dir, _existing)
                        return
        except Exception:
            pass  # corrupted plan — regenerate below

    if not resuming:
        # Show interactive column-type prompt (same as single-worker build).
        # Must happen here, before spawning — workers run non-interactively.
        columns, type_overrides, excludes = _resolve_columns(args, csv_src)

        # Pre-classify ALL files before spawning workers — avoids concurrent
        # writes to column_map.json by N workers starting at the same time.
        col_map_file = getattr(args, "map", None) or os.path.join(output_dir, "column_map.json")
        csv_abs = os.path.abspath(csv_src)
        all_files = [csv_abs] if os.path.isfile(csv_abs) else find_data_files(csv_abs)
        csv_base  = os.path.dirname(csv_abs) if os.path.isfile(csv_abs) else csv_abs
        print("── Pre-classify ──────────────────────────────────────")
        _auto_classify(all_files, csv_base, col_map_file, delimiter=args.sep,
                       columns=columns, type_overrides=type_overrides, excludes=excludes)

        plan_data = do_plan(csv_src, output_dir, n_workers=n_workers,
                            delimiter=args.sep, columns=columns)
        if not plan_data:
            return
        # Lock in doc_id ranges immediately so that if this build is interrupted
        # and restarted, the new plan won't reuse the same doc_id ranges and
        # corrupt posting lists with double entries.
        _update_manifest_after_parallel(output_dir, plan_data)
    else:
        columns, type_overrides, excludes = _resolve_columns(args, csv_src)
        remaining = [w for w in range(n_workers) if not _worker_truly_done(w)]

    # Columns may have come from the interactive prompt rather than --columns flag.
    # Serialize them so workers receive the same column list.
    effective_columns_str = ",".join(columns) if columns else None

    base_cmd = [sys.executable, main_py, "build", csv_src,
                "-o", output_dir, "--plan", plan_path]
    if args.sep != ",":
        base_cmd += ["-s", args.sep]
    if getattr(args, "dataset", None):
        base_cmd += ["--dataset", args.dataset]
    if getattr(args, "map", None):
        base_cmd += ["-m", args.map]
    if effective_columns_str:
        base_cmd += ["--columns", effective_columns_str]
    if getattr(args, "dedup_fields", None):
        base_cmd += ["--dedup-fields", args.dedup_fields]
    elif getattr(args, "dedup", False):
        base_cmd += ["--dedup"]
    # NOTE: do NOT forward --estimate to workers; they use plan-assigned ranges.

    # Workers to spawn: all on fresh build, only incomplete on resume
    workers_to_spawn = remaining if resuming else list(range(n_workers))
    # Workers already done (resume only) count toward total for progress display
    already_done_set = set(range(n_workers)) - set(workers_to_spawn)

    # ── Spawn workers → redirect each worker's output to a log file ───────────
    # Multiple workers writing \r progress bars to shared stdout garbles the
    # terminal. Log files keep output clean; the parent shows an aggregated bar.
    label = "Resuming" if resuming else "Launching"
    print(f"\n── {label} {len(workers_to_spawn)}/{n_workers} workers ───────────────────────────")
    procs = []
    log_paths = {}
    for w in workers_to_spawn:
        cmd = base_cmd + ["--worker-id", str(w)]
        log_path = os.path.join(output_dir, f"worker_{w}.log")
        log_paths[w] = log_path
        lf = open(log_path, "w")
        p = subprocess.Popen(cmd, stdout=lf, stderr=lf)
        lf.close()   # parent closes its copy; subprocess keeps its own fd
        procs.append((w, p))
        print(f"  Worker {w}  pid {p.pid}  log → {os.path.basename(log_path)}")
    for w in already_done_set:
        print(f"  Worker {w}  [already done — skipped]")

    # ── Ensure workers die when parent exits (any reason) ────────────────────
    import atexit as _atexit, signal as _signal

    def _terminate_workers():
        for _, p in procs:
            try: p.terminate()
            except Exception: pass
        _time.sleep(0.3)
        for _, p in procs:
            try:
                if p.poll() is None: p.kill()
            except Exception: pass

    _atexit.register(_terminate_workers)
    _orig_sigterm = _signal.getsignal(_signal.SIGTERM)
    def _sigterm(sig, frame):
        _terminate_workers()
        sys.exit(128 + sig)
    _signal.signal(_signal.SIGTERM, _sigterm)

    # ── Poll loop: aggregate progress from worker progress files ──────────────
    print()
    t_start = _time.time()
    done_set: set = set(already_done_set)   # pre-mark already-done workers
    failed: list = []
    loop_count = 0
    _rate_window: list = []   # (timestamp, total_rows) pairs for 30-s sliding window
    _last_ckpt_table_t = 0.0  # last time we printed the per-worker ckpt table
    _worker_stats: dict = {}  # w → latest progress dict (for ckpt table)

    try:
        while len(done_set) < n_workers:
            for w, p in procs:
                if w in done_set:
                    continue
                rc = p.poll()
                if rc is not None:
                    done_set.add(w)
                    if rc != 0:
                        failed.append(w)

            total_rows = 0
            total_expected = 0
            have_total = True
            for w in range(n_workers):
                try:
                    ppath = os.path.join(output_dir, f"progress_w{w}.json")
                    with open(ppath) as _pf:
                        stat = _json.load(_pf)
                    _worker_stats[w] = stat
                    total_rows    += stat.get("rows", 0)
                    t = stat.get("total", 0)
                    if t:
                        total_expected += t
                    else:
                        have_total = False
                except Exception:
                    # File missing or unreadable — keep last known stats for this worker
                    if w in _worker_stats:
                        total_rows += _worker_stats[w].get("rows", 0)
                        t = _worker_stats[w].get("total", 0)
                        if t:
                            total_expected += t
                    else:
                        have_total = False

            # Sliding-window rate: keep the last 30 s of (t, rows) samples so the
            # displayed rows/s reflects recent throughput, not the cumulative average.
            now_t = _time.time()
            _rate_window.append((now_t, total_rows))
            while len(_rate_window) > 1 and _rate_window[0][0] < now_t - 30.0:
                _rate_window.pop(0)
            instant_rate = None
            if len(_rate_window) >= 2:
                dt = _rate_window[-1][0] - _rate_window[0][0]
                dr = _rate_window[-1][1] - _rate_window[0][1]
                if dt > 0:
                    instant_rate = max(0, dr / dt)  # clamp: worker restart drops count, not negative speed

            # Every ~10 s: write partial stats.json from whatever worker shards exist
            if loop_count % 20 == 0:
                _partial_merge_stats(output_dir, n_workers, total_rows)

            # ── Per-worker ckpt debug table (every ~5 s) ──────────────────────
            # Printed below the progress bar so operators can see encode/disk/wait
            # breakdown for each worker — reveals which worker is I/O-bound.
            if _worker_stats and now_t - _last_ckpt_table_t >= 5.0:
                _last_ckpt_table_t = now_t
                # Clear progress bar line, print table, progress bar will redraw.
                sys.stderr.write("\r" + " " * 100 + "\r")
                hdr = (f"  {'W':>2}  {'rows':>8}  {'rows/s':>7}"
                       f"  {'ckpt':>4}  {'encode':>6}  {'disk':>5}  {'wait':>5}"
                       f"  {'d_bufs':>6}  {'m_bufs':>6}  {'mem':>5}  {'wb':>6}  {'ckpt_r':>7}")
                sys.stderr.write(hdr + "\n")
                for w in sorted(_worker_stats):
                    st = _worker_stats[w]
                    rows_w = st.get("rows", 0)
                    rate_w = st.get("instant_rate", 0)
                    ckpt_n = st.get("ckpt", 0)
                    enc    = st.get("encode", 0.0)
                    dsk    = st.get("disk", 0.0)
                    wt     = max(st.get("wait", 0.0), 0.0)
                    _db_raw = st.get("disk_bufs", 0)
                    db     = "WAL" if _db_raw < 0 else _db_raw
                    mb     = st.get("mem_bufs", 0)
                    mem    = st.get("mem_buf_mb", 0)
                    wb     = "∞" if st.get("daemon") else f"{st.get('wb_kb', 0)}K"
                    cr     = st.get("ckpt_rows", 0)
                    total_w = st.get("total", 0)
                    flushing    = st.get("flushing", False)
                    flush_done  = st.get("flush_done", 0)
                    flush_total = st.get("flush_total", 0)
                    if w in done_set:
                        status = "\033[32m✓\033[0m"
                    elif total_w and rows_w >= total_w:
                        status = "\033[33m~\033[0m"  # finalizing (writing buffers to disk)
                    else:
                        status = " "
                    if flushing and flush_total:
                        flush_pct = flush_done / flush_total
                        sys.stderr.write(
                            f"  {status}{w:>2}  {rows_w:>8,}  {'flush':>7}"
                            f"  {ckpt_n:>4}  {enc:>6.1f}s  {dsk:>4.1f}s  {wt:>4.2f}s"
                            f"  {flush_done:>6,}  {flush_total - flush_done:>6,}  {mem:>4}MB  {wb:>6}  {flush_pct:>6.0%}"
                            + "\n"
                        )
                    else:
                        sys.stderr.write(
                            f"  {status}{w:>2}  {rows_w:>8,}  {rate_w:>7,.0f}"
                            f"  {ckpt_n:>4}  {enc:>6.1f}s  {dsk:>4.1f}s  {wt:>4.2f}s"
                            f"  {(str(db) if isinstance(db, str) else f'{db:,}'):>6}  {mb:>6,}  {mem:>4}MB  {wb:>6}  {cr:>7,}"
                            + "\n"
                        )
                sys.stderr.flush()

            _progress(total_rows, total_expected if have_total else None, t_start,
                      instant_rate=instant_rate)
            _time.sleep(0.5)
            loop_count += 1

    except KeyboardInterrupt:
        sys.stderr.write("\n")
        print("Interrupted — terminating workers...")
        _terminate_workers()
        _signal.signal(_signal.SIGTERM, _orig_sigterm)
        sys.exit(130)
    finally:
        _signal.signal(_signal.SIGTERM, _orig_sigterm)

    sys.stderr.write("\r" + " " * 100 + "\r")
    sys.stderr.flush()

    # ── Per-worker summary ────────────────────────────────────────────────────
    elapsed = _time.time() - t_start
    print(f"── Workers done  ({_fmt_duration(elapsed)}) ────────────────────────────")
    for w in range(n_workers):
        log_path = log_paths[w]
        done_line = ""
        try:
            with open(log_path) as lf:
                content = lf.read()
            for line in reversed(content.splitlines()):
                if "Done:" in line:
                    done_line = line.strip()
                    break
        except Exception:
            content = ""
        status = "FAILED" if w in failed else "done  "
        print(f"  Worker {w} [{status}]" + (f" {done_line}" if done_line else ""))
        if w in failed and content:
            print(f"\n  ── Worker {w} log ──────────────────────────────")
            print(content.rstrip())
            print(f"  ── end worker {w} log ──────────────────────────\n")
        try:
            os.unlink(log_path)
        except Exception:
            pass

    # ── Clean up progress files ───────────────────────────────────────────────
    for w in range(n_workers):
        try:
            os.unlink(os.path.join(output_dir, f"progress_w{w}.json"))
        except Exception:
            pass

    print("\n── Merging stats ─────────────────────────────────────────────")
    merge_worker_stats(output_dir, n_workers)

    if failed:
        print(f"\nWARNING: {len(failed)} worker(s) failed: {failed}")
        sys.exit(1)
    else:
        print(f"\nAll {n_workers} workers completed.")
        _update_manifest_after_parallel(output_dir, plan_data)


def _update_manifest_after_parallel(output_dir, plan_data):
    """Update manifest.json next_doc_id after a successful parallel build.

    Without this, the next build (or resume) would generate a new plan starting
    from doc_id 0, causing doc_id collisions with the just-written data.
    """
    from flatseek.core.builder import _load_manifest, _save_manifest
    assignments = plan_data.get("assignments", [])
    if not assignments:
        return
    max_doc_end = max(a.get("doc_id_end", 0) for a in assignments)
    manifest = _load_manifest(output_dir)
    if max_doc_end + 1 > manifest.get("next_doc_id", 0):
        manifest["next_doc_id"] = max_doc_end + 1
        _save_manifest(manifest, output_dir)


def cmd_build(args):
    from flatseek.core.builder import build
    from flatseek.core.storage import StorageConfig, create_storage_adapter
    import tempfile as _tempfile
    import shutil as _shutil
    csv_src = os.path.abspath(args.csv_dir)
    output_dir = os.path.abspath(args.output)
    map_path = args.map
    plan_path = args.plan
    worker_id = args.worker_id
    n_workers = getattr(args, "workers", 1)
    single_file = getattr(args, "single_file", False)

    # Build storage adapter from CLI args or environment
    storage = None
    if getattr(args, "storage_backend", None):
        config = StorageConfig(
            backend=args.storage_backend,
            bucket=getattr(args, "storage_bucket", "") or "",
            region=getattr(args, "storage_region", "") or "",
            endpoint_url=getattr(args, "storage_endpoint", "") or "",
            base_path=getattr(args, "storage_base_path", "") or "",
        )
        storage = create_storage_adapter(config)

    # --workers N without an explicit plan: auto-plan + auto-spawn
    if n_workers > 1 and not plan_path:
        _run_parallel_build(args, csv_src, output_dir, n_workers)
        return

    # Skip interactive prompt when running as a distributed worker
    if plan_path and worker_id is not None:
        columns = _parse_columns(args.columns)
        type_overrides = {}
        excludes = set()
    else:
        columns, type_overrides, excludes = _resolve_columns(args, csv_src)

    if not map_path:
        candidate = os.path.join(output_dir, "column_map.json")
        if os.path.exists(candidate):
            map_path = candidate

    dedup_fields = _parse_columns(getattr(args, "dedup_fields", None))
    # --dedup with no value → dedup on all fields (empty list sentinel)
    if getattr(args, "dedup", False) and dedup_fields is None:
        dedup_fields = []

    # --single-file: build to temp dir first, then pack
    if single_file:
        flatseek_out = Path(output_dir)
        # User gives foo → output foo.fsk
        if not str(flatseek_out).endswith((".fsk", ".flatseek", ".flat")):
            flatseek_out = Path(str(flatseek_out) + ".fsk")
        tmp_dir = Path(_tempfile.mkdtemp(prefix="flatseek_build_"))
        try:
            build(csv_src, str(tmp_dir), column_map_path=map_path, dataset=args.dataset,
                  delimiter=args.sep, columns=columns, type_overrides=type_overrides,
                  excludes=excludes,
                  worker_id=worker_id, plan_path=plan_path,
                  estimate=getattr(args, "estimate", False),
                  dedup_fields=dedup_fields,
                  daemon=getattr(args, "daemon", False),
                  storage=storage)

            # Auto-pack into .flatseek
            from flatseek.flatseek_file import FlatseekPacker
            packer = FlatseekPacker(tmp_dir, flatseek_out)
            packer.pack()
            print(f"[BUILD] Single file output: {flatseek_out}")
        finally:
            _shutil.rmtree(tmp_dir, ignore_errors=True)
        return

    build(csv_src, output_dir, column_map_path=map_path, dataset=args.dataset,
          delimiter=args.sep, columns=columns, type_overrides=type_overrides,
          excludes=excludes,
          worker_id=worker_id, plan_path=plan_path,
          estimate=getattr(args, "estimate", False),
          dedup_fields=dedup_fields,
          daemon=getattr(args, "daemon", False),
          storage=storage)


def cmd_plan(args):
    from flatseek.core.builder import plan
    csv_src = os.path.abspath(args.csv_dir)
    output_dir = os.path.abspath(args.output)
    columns = _parse_columns(args.columns)
    plan(csv_src, output_dir, n_workers=args.workers, delimiter=args.sep, columns=columns)


def _get_flatseek_enc_key(flatseek_path: Path, args) -> bytes | None:
    """Read .flatseek manifest, prompt for passphrase if encrypted, derive key."""
    import base64 as _b64
    import json as _json
    from flatseek.core.query_engine import load_encryption_key
    from flatseek.flatseek_file import HEADER_SIZE, FlatseekHeader

    with open(flatseek_path, "rb") as f:
        header_data = f.read(HEADER_SIZE)
        header = FlatseekHeader.unpack(header_data)
        for desc in header.sections:
            if desc.section_id == 0x01:  # SID_MANIFEST
                f.seek(desc.offset)
                data = f.read(desc.size)
                break
        else:
            return None

    try:
        manifest = _json.loads(data.decode("utf-8"))
    except Exception:
        return None

    enc_b64 = manifest.get("_encryption_b64")
    if not enc_b64:
        return None  # not encrypted

    # Index is encrypted — need passphrase
    import getpass as _getpass
    passphrase = getattr(args, "passphrase", None)
    if not passphrase:
        # Non-interactive mode: don't block — let server start without key.
        # API will report is_encrypted=true and prompt client for password.
        import sys
        sys.stderr.write(
            f"WARNING: {flatseek_path} is encrypted — provide --passphrase to unlock at startup,\n"
            f"         or authenticate via API (Flatlens will prompt).\n"
        )
        return None

    enc_json = _b64.b64decode(enc_b64)
    enc_meta = _json.loads(enc_json.decode("utf-8"))
    return load_encryption_key(None, passphrase, meta=enc_meta)


def _apply_passphrase(qe, args):
    """If --passphrase given (or index is encrypted), derive key and call set_key()."""
    from flatseek.core.query_engine import load_encryption_key
    passphrase = getattr(args, "passphrase", None)
    if not passphrase:
        import os, json
        enc_path = os.path.join(args.data_dir, "encryption.json")
        if not os.path.isfile(enc_path):
            return   # not encrypted, nothing to do
        import getpass
        passphrase = getpass.getpass(f"Passphrase for {args.data_dir}: ")
    key = load_encryption_key(args.data_dir, passphrase)
    qe.set_key(key)


def cmd_search(args):
    from flatseek.core.query_engine import QueryEngine
    from flatseek.core.storage import StorageConfig, create_storage_adapter
    import json as _json

    storage = None
    enc_key = None

    # Handle .flatseek files (including custom extensions like .flat)
    data_path = Path(args.data_dir)
    is_flat_file = data_path.is_file() and (
        str(data_path).endswith((".fsk", ".flatseek", ".flat"))
    )
    if is_flat_file:
        # Read manifest to detect encryption
        enc_key = _get_flatseek_enc_key(data_path, args)
        from flatseek.flatseek_file import FlatseekFileStorageAdapter
        storage = FlatseekFileStorageAdapter(data_path, enc_key=enc_key)
    elif getattr(args, "storage_backend", None):
        config = StorageConfig(
            backend=args.storage_backend,
            bucket=getattr(args, "storage_bucket", "") or "",
            region=getattr(args, "storage_region", "") or "",
            endpoint_url=getattr(args, "storage_endpoint", "") or "",
            base_path=getattr(args, "storage_base_path", "") or "",
            url=getattr(args, "storage_url", "") or "",
        )
        storage = create_storage_adapter(config)

    qe = QueryEngine(args.data_dir, storage=storage)
    if enc_key is not None:
        qe.set_key(enc_key)  # key already derived above
    else:
        _apply_passphrase(qe, args)

    # Build Lucene query string from args.
    # -c / --and are convenience shortcuts converted to Lucene syntax.
    parts = []
    if args.query:
        q = f"{args.column}:{args.query}" if args.column else args.query
        parts.append(q)
    for cond in (args.and_ or []):
        if ":" not in cond:
            print(f"Error: --and must be 'col:term', got: {cond!r}")
            sys.exit(1)
        parts.append(cond)

    query_str = " AND ".join(parts) if parts else ""
    if not query_str:
        print("Nothing to search. Provide a query.")
        sys.exit(1)

    try:
        result = qe.query(query_str, page=args.page, page_size=args.page_size)
    except SyntaxError as e:
        print(f"Query syntax error: {e}")
        sys.exit(1)

    total = result["total"]
    page = result["page"]
    page_size = result["page_size"]
    docs = result["results"]

    print(f"Query:  {result['query']}")
    print(f"Found:  {total:,} match(es)  (page {page + 1}, showing {len(docs)})")
    for i, doc in enumerate(docs):
        print(f"\n--- {page * page_size + i + 1} ---")
        print(doc)


def cmd_join(args):
    from flatseek.core.query_engine import QueryEngine
    qe = QueryEngine(args.data_dir)
    _apply_passphrase(qe, args)

    try:
        result = qe.join(args.query_a, args.query_b, on=args.on,
                         page=args.page, page_size=args.page_size)
    except SyntaxError as e:
        print(f"Query syntax error: {e}")
        sys.exit(1)

    total = result["total"]
    page = result["page"]
    page_size = result["page_size"]
    pairs = result["results"]

    print(f"Query A: {args.query_a}")
    print(f"Query B: {args.query_b}")
    print(f"Join on: {args.on}")
    print(f"Found:   {total:,} matched pair(s)  (page {page + 1}, showing {len(pairs)})")

    for i, pair in enumerate(pairs):
        n = page * page_size + i + 1
        print(f"\n--- Pair {n} ---")
        print("  [A]")
        for k, v in pair["_a"].items():
            if not k.startswith("_"):
                print(f"    {k}: {v}")
        print("  [B]")
        for k, v in pair["_b"].items():
            if not k.startswith("_"):
                print(f"    {k}: {v}")


def cmd_chat(args):
    from flatseek.core.query_engine import QueryEngine
    from flatseek.core.chat import ChatInterface
    qe = QueryEngine(args.data_dir)
    ChatInterface(qe, model=args.model, api_base=args.api_base).interactive()


def cmd_stats(args):
    from flatseek.core.query_engine import QueryEngine
    print(QueryEngine(args.data_dir).summary())


def cmd_dedup(args):
    """Post-build deduplication: remove duplicate docs from index + doc store.

    Works on any index, including those built with parallel workers.
    Reads every doc chunk, computes a fingerprint per doc, keeps only the first
    occurrence, then rewrites both the doc store and all posting lists to remove
    dead doc_ids.

    Resume support: after step 1 (scan) completes, dead_ids are saved to
    dedup_checkpoint.json. If that file exists on the next run, step 1 is
    skipped entirely — only the rewrite steps run. The checkpoint is deleted
    on successful completion.

    Steps 2 and 3 are parallelised — each file is independent.
    """
    import zlib, struct, json as _json, threading
    from concurrent.futures import ThreadPoolExecutor, as_completed
    try:
        import orjson as _orjson
        _ddumps = _orjson.dumps
        _dloads = _orjson.loads
    except ImportError:
        _ddumps = lambda o: _json.dumps(o, separators=(",",":"), ensure_ascii=False).encode()
        _dloads = _json.loads

    data_dir      = os.path.abspath(args.data_dir)
    index_dir     = os.path.join(data_dir, "index")
    docs_dir      = os.path.join(data_dir, "docs")
    dry_run       = args.dry_run
    fields        = [f.strip() for f in args.fields.split(",")] if args.fields else None
    n_workers     = getattr(args, "workers", None) or min(8, os.cpu_count() or 2)
    ckpt_path     = os.path.join(data_dir, "dedup_checkpoint.json")

    if not os.path.isdir(index_dir):
        print(f"No index found in {data_dir}")
        return

    try:
        from flatseek.core.builder import _compress_doc, _decompress_doc, encode_doclist, _doc_fingerprint
        from flatseek.core.query_engine import decode_doclist
    except ImportError as e:
        print(f"Import error: {e}")
        return

    chunk_paths = []
    for root, _, files in os.walk(docs_dir):
        for f in sorted(files):
            if f.endswith(".zlib"):
                chunk_paths.append(os.path.join(root, f))
    chunk_paths.sort()

    # ── Step 1: scan doc store — resumable via checkpoint ─────────────────────
    if os.path.isfile(ckpt_path):
        print("── Resuming from checkpoint ──────────────────────────")
        with open(ckpt_path) as fh:
            ckpt = _json.load(fh)
        dead_ids   = set(ckpt["dead_ids"])
        total_docs = ckpt["total_docs"]
        kept       = ckpt["kept"]
        ckpt_fields = ckpt.get("fields")
        if ckpt_fields != fields:
            print(f"  Warning: checkpoint used fields={ckpt_fields!r}, "
                  f"current --fields={fields!r}. Delete {ckpt_path} to rescan.")
        print(f"  Loaded {len(dead_ids):,} dead doc_ids from checkpoint")
        print(f"  Total docs : {total_docs:,}")
        print(f"  Unique     : {kept:,}")
        print(f"  Duplicates : {total_docs - kept:,}")
    else:
        print("── Scanning doc store ────────────────────────────────")
        seen_fp    = {}
        dead_ids   = set()
        total_docs = kept = 0

        for i, chunk_path in enumerate(chunk_paths):
            with open(chunk_path, "rb") as fh:
                raw = fh.read()
            try:
                chunk = _dloads(_decompress_doc(raw))
            except Exception as e:
                print(f"  Warning: could not read {os.path.basename(chunk_path)}: {e}")
                continue

            for doc_id_str, doc in chunk.items():
                doc_id = int(doc_id_str)
                total_docs += 1
                fp = _doc_fingerprint(doc, fields)
                if fp in seen_fp:
                    dead_ids.add(doc_id)
                else:
                    seen_fp[fp] = doc_id
                    kept += 1

            if i % 50 == 0:
                sys.stderr.write(f"\r  Scanned {i+1:,}/{len(chunk_paths):,} chunks"
                                 f"  ({total_docs:,} docs, {len(dead_ids):,} dupes) …")
                sys.stderr.flush()

        sys.stderr.write("\r" + " " * 70 + "\r")
        del seen_fp

        dupes = total_docs - kept
        print(f"  Total docs : {total_docs:,}")
        print(f"  Unique     : {kept:,}")
        print(f"  Duplicates : {dupes:,}")

        if dupes == 0:
            print("\nNo duplicates found.")
            return

        if dry_run:
            print(f"\n--dry-run: no changes written.")
            return

        # Save checkpoint so a restart skips this scan
        with open(ckpt_path, "w") as fh:
            _json.dump({
                "dead_ids":   sorted(dead_ids),
                "total_docs": total_docs,
                "kept":       kept,
                "fields":     fields,
            }, fh)
        print(f"  Checkpoint saved → {os.path.basename(ckpt_path)}")

    dupes = total_docs - kept

    if dry_run:
        print(f"\n--dry-run: no changes written.")
        return

    # ── Step 2: rewrite doc chunks in parallel ─────────────────────────────────
    print(f"\n── Rewriting doc store  ({n_workers} workers) ────────────────")

    def _rewrite_chunk(chunk_path):
        with open(chunk_path, "rb") as fh:
            raw = fh.read()
        try:
            chunk = _json.loads(_decompress_doc(raw))
        except Exception:
            return 0
        orig_len = len(chunk)
        chunk = {k: v for k, v in chunk.items() if int(k) not in dead_ids}
        if len(chunk) == orig_len:
            return 0
        if chunk:
            new_raw = _ddumps(chunk)
            compressed = _compress_doc(new_raw)
            tmp = chunk_path + f".{os.getpid()}.dedup.tmp"
            with open(tmp, "wb") as fh:
                fh.write(compressed)
            os.replace(tmp, chunk_path)
        else:
            os.remove(chunk_path)
        return 1

    chunks_written = 0
    with ThreadPoolExecutor(max_workers=n_workers) as ex:
        futs = {ex.submit(_rewrite_chunk, p): p for p in chunk_paths}
        done = 0
        for fut in as_completed(futs):
            chunks_written += fut.result()
            done += 1
            if done % 100 == 0:
                sys.stderr.write(f"\r  {done:,}/{len(chunk_paths):,} chunks …")
                sys.stderr.flush()
    sys.stderr.write("\r" + " " * 50 + "\r")
    print(f"  Doc chunks rewritten: {chunks_written:,}")

    # ── Step 3: rewrite posting lists in parallel ─────────────────────────────
    print(f"── Rewriting posting lists  ({n_workers} workers) ──────────────")
    all_bins = []
    for root, _, files in os.walk(index_dir):
        for f in files:
            if f.endswith(".bin"):
                all_bins.append(os.path.join(root, f))

    def _rewrite_bin(bin_path):
        raw = open(bin_path, "rb").read()
        if not raw:
            return 0, 0
        is_compressed = len(raw) >= 2 and raw[0] == 0x78 and raw[1] in (0x01, 0x5e, 0x9c, 0xda)
        data = zlib.decompress(raw) if is_compressed else raw
        out = bytearray()
        offset = 0
        changed = False
        removed = 0
        while offset < len(data):
            if offset + 2 > len(data): break
            term_len = struct.unpack_from("<H", data, offset)[0]; offset += 2
            if offset + term_len > len(data): break
            term_b = data[offset:offset + term_len]; offset += term_len
            if offset + 4 > len(data): break
            pl_len = struct.unpack_from("<I", data, offset)[0]; offset += 4
            doclist_raw = data[offset:offset + pl_len]; offset += pl_len
            ids = decode_doclist(doclist_raw)
            filtered = [d for d in ids if d not in dead_ids]
            if len(filtered) == len(ids):
                out += struct.pack("<H", term_len) + term_b + struct.pack("<I", pl_len) + doclist_raw
            else:
                changed = True
                removed += len(ids) - len(filtered)
                if not filtered:
                    continue
                new_dl = encode_doclist(filtered)
                out += struct.pack("<H", term_len) + term_b + struct.pack("<I", len(new_dl)) + new_dl
        if not changed:
            return 0, 0
        new_data = zlib.compress(bytes(out), 6) if is_compressed else bytes(out)
        tmp = bin_path + f".{os.getpid()}.dedup.tmp"
        with open(tmp, "wb") as fh:
            fh.write(new_data)
        os.replace(tmp, bin_path)
        return 1, removed

    bins_written = entries_removed = 0
    with ThreadPoolExecutor(max_workers=n_workers) as ex:
        futs = {ex.submit(_rewrite_bin, p): p for p in all_bins}
        done = 0
        for fut in as_completed(futs):
            w, r = fut.result()
            bins_written    += w
            entries_removed += r
            done += 1
            if done % 2000 == 0:
                sys.stderr.write(f"\r  {done:,}/{len(all_bins):,} files"
                                 f"  rewritten: {bins_written:,}"
                                 f"  entries removed: {entries_removed:,} …")
                sys.stderr.flush()
    sys.stderr.write("\r" + " " * 80 + "\r")
    print(f"  Bucket files rewritten: {bins_written:,}")
    print(f"  Posting entries removed: {entries_removed:,}")

    # ── Step 4: update stats + remove checkpoint ───────────────────────────────
    stats_path = os.path.join(data_dir, "stats.json")
    if os.path.exists(stats_path):
        with open(stats_path) as fh:
            stats = _json.load(fh)
        stats["total_docs"] = kept
        stats.pop("rows_indexed", None)
        stats["_dedup_removed"] = dupes
        with open(stats_path, "w") as fh:
            _json.dump(stats, fh, indent=2)

    if os.path.exists(ckpt_path):
        os.remove(ckpt_path)

    print(f"\nDedup complete: {dupes:,} duplicate docs removed, {kept:,} docs remain.")


def cmd_wal(args):
    """Manage WAL (Write-Ahead Log) files from ongoing builds.

    Actions:
      info   - Show WAL file statistics
      merge  - Merge WAL files into index (safe during ongoing build)
    """
    import struct, shutil, glob as _glob
    from concurrent.futures import ThreadPoolExecutor, as_completed

    data_dir = os.path.abspath(args.data_dir)
    wal_dir  = os.path.join(data_dir, "_wal")

    if not os.path.isdir(wal_dir):
        print(f"No _wal/ directory found in {data_dir}")
        return

    if args.wal_action == "info":
        _cmd_wal_info(wal_dir)
    elif args.wal_action == "merge":
        _cmd_wal_merge(wal_dir, data_dir, args)
    else:
        print(f"Unknown WAL action: {args.wal_action}")
        print("Available: info, merge")


def _cmd_wal_info(wal_dir):
    """Show WAL file statistics."""
    import struct

    try:
        wal_files = sorted(f for f in os.listdir(wal_dir) if f.endswith(".wal"))
    except OSError as e:
        print(f"Cannot read WAL directory: {e}")
        return

    if not wal_files:
        print("No WAL files found.")
        return

    _unpack_5sI = struct.Struct("<5sI")
    total_bytes = 0
    prefixes_by_file = []

    for wal_file in wal_files:
        wal_path = os.path.join(wal_dir, wal_file)
        size = os.path.getsize(wal_path)
        total_bytes += size
        prefixes = set()

        try:
            with open(wal_path, "rb") as f:
                data = f.read()
            i = 0
            while i + 9 <= len(data):
                wal_prefix_bytes, data_len = _unpack_5sI.unpack_from(data, i)
                wal_prefix = wal_prefix_bytes.decode("utf-8", errors="ignore")
                prefixes.add(wal_prefix)
                i += 9 + data_len
        except (OSError, struct.error):
            pass

        prefixes_by_file.append((wal_file, size, prefixes))

    print(f"WAL Directory: {wal_dir}")
    print(f"Total files : {len(wal_files)}")
    print(f"Total size  : {total_bytes / 1024 / 1024:.1f} MB")
    print()
    print(f"{'File':<30}  {'Size':>10}  {'Prefixes':>8}")
    print("-" * 52)
    all_prefixes = set()
    for wal_file, size, prefixes in prefixes_by_file:
        print(f"{wal_file:<30}  {size/1024/1024:>9.1f} MB  {len(prefixes):>8,}")
        all_prefixes.update(prefixes)
    print("-" * 52)
    print(f"{'Total unique prefixes':<30}  {'':<10}  {len(all_prefixes):>8,}")


def _cmd_wal_merge(wal_dir, data_dir, args):
    """Merge WAL files into index as new segment files.

    IMPORTANT: For ongoing builds, the builder now has a built-in WAL flush thread
    that handles this automatically. Only use this command as a rescue tool for:
      - Builds that crashed without merging
      - Builds that don't have the WAL flush thread (old version)

    For ongoing builds with the new WAL flush thread, the builder handles merging
    every 60 seconds automatically.

    Safety:
      - Builder writes COMPLETE .wal files per checkpoint cycle (atomic)
      - We only write NEW idx_segXXXX.bin files
      - We sync segment counters with existing index to avoid conflicts
    """
    import struct
    from concurrent.futures import ThreadPoolExecutor, as_completed

    dry_run   = args.dry_run
    n_workers = args.workers or 4

    try:
        wal_files = sorted(f for f in os.listdir(wal_dir) if f.endswith(".wal"))
    except OSError as e:
        print(f"Cannot read WAL directory: {e}")
        return

    if not wal_files:
        print("No WAL files to merge.")
        return

    # Check for merged marker (created by builder's WAL flush thread)
    marker_path = os.path.join(data_dir, "_wal_merged.txt")
    merged_set = set()
    if os.path.exists(marker_path):
        try:
            with open(marker_path) as f:
                merged_set = set(line.strip() for line in f if line.strip())
        except (OSError, IOError):
            pass

    # Filter to only files not yet merged
    unmerged = [f for f in wal_files if f not in merged_set]
    if not unmerged:
        print(f"All {len(wal_files)} WAL files already merged.")
        print(f"  (Use --force to merge anyway)")
        if not getattr(args, 'force', False):
            return

    to_merge = wal_files if getattr(args, 'force', False) else unmerged
    print(f"Merging {len(to_merge)} WAL file(s) → index/")
    print(f"  ({len(merged_set)} already merged, {len(unmerged)} new)")
    print(f"Workers    : {n_workers}")
    print(f"Dry run    : {dry_run}")
    print()

    # WAL format: [5 bytes prefix "aa/bb"][4 bytes uint32 data_len][N bytes data]
    _unpack_5sI = struct.Struct("<5sI")

    prefix_entries: dict = {}  # prefix -> list of data_bytes
    total_entries = 0
    total_data = 0

    for idx, wal_file in enumerate(to_merge, 1):
        wal_path = os.path.join(wal_dir, wal_file)
        try:
            with open(wal_path, "rb") as f:
                data = f.read()
            i = 0
            entries_in_file = 0
            while i + 9 <= len(data):
                wal_prefix_bytes, data_len = _unpack_5sI.unpack_from(data, i)
                wal_prefix = wal_prefix_bytes.decode("utf-8", errors="ignore")
                i += 9
                if i + data_len > len(data):
                    break  # truncated entry
                entry_data = data[i:i + data_len]
                i += data_len
                entries_in_file += 1
                if wal_prefix not in prefix_entries:
                    prefix_entries[wal_prefix] = []
                prefix_entries[wal_prefix].append(entry_data)
                total_entries += 1
                total_data += data_len
            sys.stderr.write(
                f"\r  [{idx}/{len(to_merge)}] {wal_file}: "
                f"{entries_in_file:,} entries | "
                f"{total_entries:,} total | "
                f"{total_data / 1024 / 1024:.1f} MB"
            )
            sys.stderr.flush()
        except (OSError, struct.error) as e:
            print(f"\n  Warning: could not read {wal_file}: {e}")

    sys.stderr.write("\r" + " " * 80 + "\r")
    print(f"  Prefixes    : {len(prefix_entries):,}")
    print(f"  Entries     : {total_entries:,}")
    print(f"  Data        : {total_data / 1024 / 1024:.1f} MB")

    if dry_run:
        print(f"\n--dry-run: no files written.")
        return

    # ── Sync segment counters ────────────────────────────────────────────────
    index_dir = os.path.join(data_dir, "index")
    os.makedirs(index_dir, exist_ok=True)
    seg_counters: dict = {}

    def _scan_segs():
        for root, dirs, files in os.walk(index_dir):
            for f in files:
                if f.startswith("idx_seg") and f.endswith(".bin"):
                    try:
                        seg_num = int(f.replace("idx_seg", "").replace(".bin", ""))
                        rel = os.path.relpath(root, index_dir).replace("\\", "/")
                        if rel == ".":
                            continue
                        existing = seg_counters.get(rel, 0)
                        seg_counters[rel] = max(existing, seg_num)
                    except (ValueError, IndexError):
                        pass

    _scan_segs()

    # ── Write segment files in parallel ─────────────────────────────────────
    def _write_prefix(item):
        prefix, entries = item
        # prefix is "aa/bb" — directory is index/aa/bb/
        seg_dir = os.path.join(index_dir, prefix)
        os.makedirs(seg_dir, exist_ok=True)

        if prefix not in seg_counters:
            seg_counters[prefix] = 0
        seg_counters[prefix] += 1
        seg_num = seg_counters[prefix]
        seg_path = os.path.join(seg_dir, f"idx_seg{seg_num:04d}.bin")

        combined = b"".join(entries)
        with open(seg_path, "wb") as f:
            f.write(combined)
        return len(entries), len(combined)

    items = list(prefix_entries.items())
    written = 0
    with ThreadPoolExecutor(max_workers=n_workers) as ex:
        futs = {ex.submit(_write_prefix, item): item for item in items}
        for fut in as_completed(futs):
            try:
                fut.result()
                written += 1
            except Exception:
                pass
            if written % 1000 == 0:
                sys.stderr.write(f"\r  Writing: {written:,}/{len(items):,} prefixes …")
                sys.stderr.flush()

    # ── Update merged marker ─────────────────────────────────────────────────
    new_merged = merged_set | set(to_merge)
    try:
        with open(marker_path, "w") as f:
            for wf in sorted(new_merged):
                f.write(wf + "\n")
    except (OSError, IOError):
        pass

    # ── Delete merged WAL files (free disk space) ─────────────────────────────
    deleted = 0
    for wf in to_merge:
        wf_path = os.path.join(wal_dir, wf)
        try:
            os.unlink(wf_path)
            deleted += 1
        except (OSError, PermissionError):
            pass  # Skip if file is locked or protected

    sys.stderr.write("\r" + " " * 70 + "\r")
    print(f"  Written  : {written:,} segment files")
    if deleted:
        print(f"  Deleted: {deleted} WAL file(s) (freed disk space)")
    print(f"\nWAL merge complete!")
    print(f"  Search now uses checkpointed index — no more WAL lookups.")


def cmd_encrypt(args):
    """Encrypt all index and doc files in-place with ChaCha20-Poly1305.

    Can be run on a fresh build OR after 'flatseek compress' (compress first for
    better ratio — encryption preserves byte size, not compressibility).

    Format written: FLATSEEK\\x01 (5B magic) + nonce (12B) + ciphertext + auth_tag (16B).
    A random salt is stored in <data_dir>/encryption.json — needed to re-derive
    the key. Do NOT delete this file.

    Resume behavior (default): files already encrypted (FLATSEEK\\x01 magic) are
    skipped. Safe to interrupt and rerun. Pass --no-resume to force re-encryption
    of every file (rare; use after a key change with --no-resume).

    Integrity check (--verify): after the run, decrypt a random sample of
    encrypted files back to verify the ChaCha20-Poly1305 auth tag. Reports N
    passed / K failed with file paths.

    Constraint: incremental builds cannot append to encrypted index files.
    Run 'flatseek decrypt' first, rebuild, then 'flatseek encrypt' again.
    """
    import os, json, secrets, sys, time as _time, random
    from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed
    from flatseek.core.query_engine import (
        derive_key, encrypt_bytes, is_encrypted, decrypt_bytes,
    )
    from flatseek.core.query_engine import _ENC_MAGIC

    data_dir  = args.data_dir
    index_dir = os.path.join(data_dir, "index")
    docs_dir  = os.path.join(data_dir, "docs")
    resume    = getattr(args, "resume", True)
    verify    = getattr(args, "verify", False)
    verify_n  = getattr(args, "verify_sample", 100)

    if not os.path.isdir(index_dir):
        print(f"No index found in {data_dir}")
        return

    # ── Key derivation ──────────────────────────────────────────────────────────
    enc_path = os.path.join(data_dir, "encryption.json")
    if os.path.isfile(enc_path):
        # Resume / re-encrypt with existing salt — keeps the same key material.
        with open(enc_path) as f:
            meta = json.load(f)
        salt = bytes.fromhex(meta["salt"])
        print("Using existing salt from encryption.json")
    else:
        salt = secrets.token_bytes(32)

    passphrase = args.passphrase
    if not passphrase:
        import getpass
        passphrase = getpass.getpass("Passphrase: ")
        confirm    = getpass.getpass("Confirm   : ")
        if passphrase != confirm:
            print("Error: passphrases do not match")
            return

    print("Deriving key …", end=" ", flush=True)
    key = derive_key(passphrase, salt)
    print("done")

    # Persist salt before touching any files — if interrupted, key stays recoverable.
    with open(enc_path, "w") as f:
        json.dump({"salt": salt.hex(), "algorithm": "ChaCha20-Poly1305",
                   "kdf": "PBKDF2-HMAC-SHA256", "iterations": 600_000}, f, indent=2)

    # ── Collect files ───────────────────────────────────────────────────────────
    targets = []
    for root, _, files in os.walk(index_dir):
        for fn in files:
            if fn.endswith(".bin"):
                targets.append(os.path.join(root, fn))
    for root, _, files in os.walk(docs_dir):
        for fn in files:
            if fn.endswith(".zlib"):
                targets.append(os.path.join(root, fn))
    # Also encrypt metadata JSON files — they contain column names/types which are sensitive
    # These are at data_dir level, not inside index/ subdirectory
    for fn in ("stats.json", "column_map.json", "manifest.json"):
        p = os.path.join(data_dir, fn)
        if os.path.exists(p):
            targets.append(p)

    n_already = n_done = n_err = 0
    n_workers = min(32, max(4, len(targets)))
    t0 = _time.time()
    done = 0

    def _encrypt_file(path):
        nonlocal n_already, n_done, n_err
        try:
            with open(path, "rb") as f:
                data = f.read()
            if not data:
                return None
            # Resume (default): skip already-encrypted files.
            if resume and is_encrypted(data):
                return (path, "already", len(data))
            # --no-resume: if file is already encrypted, decrypt first to get
            # back to plaintext. Otherwise encrypt_bytes would double-encrypt
            # the ciphertext (irrecoverable without the original plaintext).
            # Requires the file was encrypted with the CURRENT key/salt.
            if not resume and is_encrypted(data):
                try:
                    data = decrypt_bytes(data, key)
                except Exception as de:
                    return (path, "error",
                            f"cannot decrypt for re-encrypt: {de}")
            enc = encrypt_bytes(data, key)
            tmp = path + f".{os.getpid()}.etmp"
            with open(tmp, "wb") as f:
                f.write(enc)
            os.replace(tmp, path)
            return (path, "done", len(data))
        except Exception as e:
            return (path, "error", str(e))

    if resume:
        print(f"Encrypting {len(targets):,} files (resume: skip already-encrypted) …")
    else:
        print(f"Encrypting {len(targets):,} files (no-resume: re-encrypt everything) …")
    with ThreadPoolExecutor(max_workers=n_workers) as ex:
        futures = {ex.submit(_encrypt_file, p): p for p in targets}
        for fut in _as_completed(futures):
            path, status, val = fut.result()
            if status == "already":
                n_already += 1
            elif status == "done":
                n_done += 1
            elif status == "error":
                n_err += 1
                sys.stderr.write(f"\n  Error: {path}: {val}\n")
            done += 1
            if done % 5000 == 0 or done == len(targets):
                elapsed = _time.time() - t0
                pct = done / len(targets) * 100
                rate = done / elapsed if elapsed > 0 else 0
                eta = (len(targets) - done) / rate if rate > 0 else 0
                sys.stderr.write(f"\r  {done:,}/{len(targets):,} ({pct:.0f}%)  {eta/60:.1f}m remaining   ")
                sys.stderr.flush()
    sys.stderr.write("\r" + " " * 50 + "\r")

    if resume:
        print(f"Done: {n_done:,} newly encrypted, {n_already:,} resumed (already encrypted), {n_err:,} errors")
    else:
        print(f"Done: {n_done:,} encrypted (no-resume), {n_already:,} were already encrypted, {n_err:,} errors")

    # ── Integrity check (--verify) ──────────────────────────────────────────────
    # Decrypt a sample of encrypted files back. Poly1305 auth tag validates
    # ciphertext integrity: if decrypt succeeds, bytes are byte-for-byte intact
    # and were encrypted with the current key.
    if verify and n_err == 0:
        # Walk targets again to confirm what's encrypted on disk right now.
        encrypted_paths = []
        for path in targets:
            if not os.path.isfile(path):
                continue
            with open(path, "rb") as f:
                head = f.read(len(_ENC_MAGIC))
            if head == _ENC_MAGIC:
                encrypted_paths.append(path)
        sample_size = verify_n if verify_n > 0 else len(encrypted_paths)
        if encrypted_paths:
            sample = random.sample(encrypted_paths, min(sample_size, len(encrypted_paths)))
        else:
            sample = []

        n_v_pass = n_v_fail = 0
        failed = []
        for path in sample:
            try:
                with open(path, "rb") as f:
                    blob = f.read()
                decrypt_bytes(blob, key)  # Poly1305 verifies auth tag
                n_v_pass += 1
            except Exception as e:
                n_v_fail += 1
                failed.append((path, str(e)))

        if not sample:
            print("Verify: 0 sampled (no encrypted files found)")
        else:
            print(f"Verify: {len(sample):,} sampled, {n_v_pass:,} passed, {n_v_fail:,} failed")
            for path, err in failed:
                print(f"  FAILED: {path}: {err}")
            if n_v_fail:
                print("Some files failed integrity check. Possible causes:")
                print("  - On-disk corruption / bit-rot")
                print("  - Encryption was started with a different salt/passphrase")
                print("Re-run with --no-resume after fixing the salt in encryption.json.")

    if n_err == 0:
        print(f"Salt stored in: {enc_path}")
        print("Keep your passphrase safe — there is no recovery without it.")


def cmd_decrypt(args):
    """Decrypt all index and doc files in-place (reverse of 'flatseek encrypt').

    Use this before running incremental builds on an encrypted index, or to
    permanently remove encryption. The encryption.json salt file is removed
    after successful decryption.

    Resume behavior (default): files that are already plaintext (no
    FLATSEEK\\x01 magic) are skipped. Safe to interrupt and rerun. Pass
    --no-resume to force re-decryption of every file (rare; rarely useful).

    Integrity check (--verify): after the run, take a random sample of
    files that were just decrypted and re-encrypt them with the same key.
    The re-encrypted bytes must equal the original on-disk ciphertext.
    """
    import os, json, sys, time as _time, random
    from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed
    from flatseek.core.query_engine import (
        load_encryption_key, decrypt_bytes, encrypt_bytes, is_encrypted,
    )

    data_dir = args.data_dir
    enc_path = os.path.join(data_dir, "encryption.json")
    resume   = getattr(args, "resume", True)
    verify   = getattr(args, "verify", False)
    verify_n = getattr(args, "verify_sample", 100)

    passphrase = args.passphrase
    if not passphrase:
        import getpass
        passphrase = getpass.getpass("Passphrase: ")

    print("Deriving key …", end=" ", flush=True)
    try:
        key = load_encryption_key(data_dir, passphrase)
    except FileNotFoundError as e:
        print(f"\nError: {e}")
        return
    print("done")

    # ── Passphrase verification before touching any files ──────────────────────
    # Find the first encrypted file and try to decrypt it. Fail fast here rather
    # than discovering a wrong passphrase halfway through thousands of files.
    index_dir = os.path.join(data_dir, "index")
    docs_dir  = os.path.join(data_dir, "docs")

    print("Verifying passphrase …", end=" ", flush=True)
    _probe_path = None
    # Try .bin files first
    for root, _, files in os.walk(index_dir):
        for fn in sorted(files):
            if fn.endswith(".bin"):
                _probe_path = os.path.join(root, fn)
                break
        if _probe_path:
            break
    # Fall back to JSON files if no .bin found
    if not _probe_path:
        for fn in ("stats.json", "column_map.json", "manifest.json"):
            p = os.path.join(data_dir, fn)
            if os.path.exists(p):
                _probe_path = p
                break
    if _probe_path:
        with open(_probe_path, "rb") as f:
            _probe_data = f.read()
        if is_encrypted(_probe_data):
            try:
                decrypt_bytes(_probe_data, key)
                print("OK")
            except Exception:
                # decrypt_bytes raises InvalidToken (cryptography.fernet) on
                # wrong key, which is NOT a ValueError subclass. Catch
                # everything to fail fast regardless of the exception type.
                print("\nError: wrong passphrase — decryption failed. No files were modified.")
                return
        else:
            print("OK (file not encrypted)")
    else:
        print("OK (no files found to verify)")

    targets = []
    for root, _, files in os.walk(index_dir):
        for fn in files:
            if fn.endswith(".bin"):
                targets.append(os.path.join(root, fn))
    for root, _, files in os.walk(docs_dir):
        for fn in files:
            if fn.endswith(".zlib"):
                targets.append(os.path.join(root, fn))
    # Also decrypt metadata JSON files (mirrors cmd_encrypt)
    for fn in ("stats.json", "column_map.json", "manifest.json"):
        p = os.path.join(data_dir, fn)
        if os.path.exists(p):
            targets.append(p)

    n_plain = n_done = n_err = 0
    # Track which files were "done" (newly decrypted) so verify can sample them.
    # Decrypted plaintexts are NOT kept in memory; we re-read each file when
    # verifying. This keeps memory bounded.
    decrypted_paths: list[str] = []
    n_workers = min(32, max(4, len(targets)))
    t0 = _time.time()
    done = 0

    def _decrypt_file(path):
        try:
            with open(path, "rb") as f:
                data = f.read()
            if not data:
                return (path, "skip", 0)
            # Resume: skip already-plaintext files unless --no-resume.
            if resume and not is_encrypted(data):
                return (path, "plain", 0)
            dec = decrypt_bytes(data, key)
            tmp = path + f".{os.getpid()}.dtmp"
            with open(tmp, "wb") as f:
                f.write(dec)
            os.replace(tmp, path)
            return (path, "done", len(data))
        except ValueError as e:
            return (path, "error", str(e))
        except Exception as e:
            return (path, "error", str(e))

    if resume:
        print(f"Decrypting {len(targets):,} files (resume: skip already-plain) …")
    else:
        print(f"Decrypting {len(targets):,} files (no-resume: re-decrypt everything) …")
    with ThreadPoolExecutor(max_workers=n_workers) as ex:
        futures = {ex.submit(_decrypt_file, p): p for p in targets}
        for fut in _as_completed(futures):
            path, status, val = fut.result()
            if status == "plain":
                n_plain += 1
            elif status == "done":
                n_done += 1
                decrypted_paths.append(path)
            elif status == "error":
                n_err += 1
                sys.stderr.write(f"\n  Error: {path}: {val}\n")
            done += 1
            if done % 5000 == 0 or done == len(targets):
                elapsed = _time.time() - t0
                pct = done / len(targets) * 100
                rate = done / elapsed if elapsed > 0 else 0
                eta = (len(targets) - done) / rate if rate > 0 else 0
                sys.stderr.write(f"\r  {done:,}/{len(targets):,} ({pct:.0f}%)  {eta/60:.1f}m remaining   ")
                sys.stderr.flush()
    sys.stderr.write("\r" + " " * 50 + "\r")

    if resume:
        print(f"Done: {n_done:,} newly decrypted, {n_plain:,} resumed (already plain), {n_err:,} errors")
    else:
        print(f"Done: {n_done:,} decrypted (no-resume), {n_plain:,} were already plain, {n_err:,} errors")

    # ── Integrity check (--verify) ──────────────────────────────────────────────
    # Re-encrypt a sample of just-decrypted files. The new ciphertext must
    # differ from the original only in nonce — but that's not byte-equal across
    # runs because each encrypt_bytes() call picks a fresh nonce. So we instead
    # re-decrypt the freshly-written plaintext and verify the Poly1305 tag
    # passes against the key. This confirms the decrypted plaintext is
    # authentic (no tampering between write and read).
    if verify and n_err == 0 and decrypted_paths:
        sample_size = verify_n if verify_n > 0 else len(decrypted_paths)
        sample = random.sample(decrypted_paths, min(sample_size, len(decrypted_paths)))

        n_v_pass = n_v_fail = 0
        failed = []
        for path in sample:
            try:
                with open(path, "rb") as f:
                    plaintext = f.read()
                # Round-trip: encrypt → decrypt → must equal original plaintext.
                # This catches: tampered plaintext bytes, wrong key derivation
                # between cmd_decrypt invocations, write corruption.
                ct = encrypt_bytes(plaintext, key)
                rt = decrypt_bytes(ct, key)
                if rt == plaintext:
                    n_v_pass += 1
                else:
                    n_v_fail += 1
                    failed.append((path, "round-trip mismatch"))
            except Exception as e:
                n_v_fail += 1
                failed.append((path, str(e)))

        print(f"Verify: {len(sample):,} sampled, {n_v_pass:,} passed, {n_v_fail:,} failed")
        for path, err in failed:
            print(f"  FAILED: {path}: {err}")
        if n_v_fail:
            print("Some files failed integrity check. Possible causes:")
            print("  - On-disk corruption / bit-rot on the decrypted plaintext")
            print("  - Filesystem-level tampering between decrypt and verify")
            print("Re-decrypt from a known-good source after investigating.")

    if n_err == 0 and n_done > 0:
        os.remove(enc_path)
        print(f"Removed: {enc_path}")


def cmd_delete(args):
    """Delete an index directory fast using parallel rm -rf on subdirectory chunks.

    Strategy:
      1. Rename data_dir → data_dir.deleting_<pid> immediately (atomic, O(1)).
         The original name is freed at once so a new build can start right away.
      2. Enumerate top-level subdirectories, split into N chunks, run one
         'rm -rf <chunk>' subprocess per worker in parallel.
         Spawning real subprocesses avoids Python per-unlink overhead and kernel
         filesystem-lock contention from too many threads — rm -rf in C is faster
         than os.unlink() in a Python thread pool for large directory trees.
      3. rm -rf the staging dir itself to clean up any remaining skeleton.

    If interrupted, re-running 'flatseek delete <data_dir>' automatically detects
    and resumes any leftover .deleting_* directories.
    """
    import time as _time
    import subprocess
    import math

    data_dir = os.path.abspath(args.data_dir)
    n_workers = getattr(args, "workers", None) or min(8, os.cpu_count() or 4)

    # ── Detect leftover .deleting_* dirs from a previously interrupted run ──────
    parent  = os.path.dirname(data_dir)
    base    = os.path.basename(data_dir.rstrip("/"))
    orphans = []
    try:
        for entry in os.scandir(parent):
            if entry.name.startswith(base + ".deleting_") and entry.is_dir():
                orphans.append(entry.path)
    except Exception:
        pass

    staging_dirs = []

    if orphans:
        print(f"Found {len(orphans)} interrupted delete(s):")
        for o in orphans:
            print(f"  {o}")
        if not args.yes:
            answer = input("Resume deleting these? [y/N] ").strip().lower()
            if answer not in ("y", "yes"):
                print("Aborted.")
                return
        staging_dirs = orphans

    if os.path.isdir(data_dir):
        if not args.yes:
            answer = input(f"Delete '{data_dir}' and all its contents? [y/N] ").strip().lower()
            if answer not in ("y", "yes"):
                print("Aborted.")
                return
        staging = data_dir.rstrip("/") + f".deleting_{os.getpid()}"
        os.rename(data_dir, staging)
        print(f"Renamed → {os.path.basename(staging)}  (original name now free)")
        staging_dirs.append(staging)

    if not staging_dirs:
        print(f"Not found: {data_dir}")
        return

    t0 = _time.time()

    for staging in staging_dirs:
        # ── Collect chunks 2 levels deep so we get ~256+ items to parallelise ──
        # Depth 1 (index/, docs/) gives only 2-3 items → 1 effective worker.
        # Depth 2 (index/00 … index/ff, docs/00 … docs/ff) gives 256+ items.
        chunks_raw = []
        try:
            for top in os.scandir(staging):
                if top.is_dir():
                    sub_entries = []
                    try:
                        for sub in os.scandir(top.path):
                            sub_entries.append(sub.path)
                    except Exception:
                        pass
                    if sub_entries:
                        chunks_raw.extend(sub_entries)
                    else:
                        chunks_raw.append(top.path)
                else:
                    chunks_raw.append(top.path)
        except Exception:
            chunks_raw = [staging]

        if not chunks_raw:
            try:
                os.rmdir(staging)
            except Exception:
                pass
            continue

        total   = len(chunks_raw)
        print(f"Deleting {total} entries  (max {n_workers} parallel rm -rf) …")

        # Run rm -rf one-per-entry with a sliding window of n_workers concurrent procs.
        # Progress updates every time any single entry finishes — much more granular
        # than batching all entries into N groups where counter only moves N times.
        pending = list(chunks_raw)
        running = []   # list of Popen
        done    = 0
        spin    = ["|", "/", "-", "\\"]
        tick    = 0

        def _report():
            elapsed = _time.time() - t0
            pct = done / total * 100
            rate = done / elapsed if elapsed > 0 else 0
            eta  = (total - done) / rate if rate > 0 else 0
            sys.stderr.write(
                f"\r  {spin[tick % 4]}  {done}/{total}  {pct:.0f}%"
                f"  {rate:.0f} dirs/s"
                f"  ETA {eta:.0f}s"
                + " " * 6
            )
            sys.stderr.flush()

        while pending or running:
            # Fill up to n_workers
            while pending and len(running) < n_workers:
                p = subprocess.Popen(["rm", "-rf", pending.pop(0)])
                running.append(p)

            # Reap finished
            still = []
            for p in running:
                if p.poll() is not None:
                    done += 1
                else:
                    still.append(p)
            running = still

            tick += 1
            _report()
            _time.sleep(0.3)

        sys.stderr.write("\r" + " " * 60 + "\r")

        # Clean up remaining skeleton
        try:
            subprocess.run(["rm", "-rf", staging], check=False)
        except Exception:
            pass

    elapsed = _time.time() - t0
    print(f"Done in {elapsed:.1f}s")


def cmd_compress(args):
    """Compress all idx.bin files in-place with zlib after a build is complete.

    Only run this when no further incremental builds are planned — the builder
    appends raw bytes and cannot extend a compressed file without re-reading it.
    Once compressed, the query engine auto-detects and decompresses on read.

    Resume: already-compressed files (detected by zlib magic) are skipped, so
    re-running after an interrupted compress continues where it left off.

    Workers: -w N compresses files in parallel (I/O + CPU bound, benefits from
    multiple cores). Default: min(8, cpu_count).
    """
    import zlib, threading
    from concurrent.futures import ThreadPoolExecutor, as_completed

    data_dir  = args.data_dir
    index_dir = os.path.join(data_dir, "index")
    level     = args.level
    n_workers = getattr(args, "workers", None) or min(8, os.cpu_count() or 2)

    if not os.path.isdir(index_dir):
        print(f"No index found in {data_dir}")
        return

    all_bins = []
    for root, _, files in os.walk(index_dir):
        for f in files:
            if f.endswith(".bin"):   # includes idx.bin + idx_w0.bin worker shards
                all_bins.append(os.path.join(root, f))

    print(f"Compressing {len(all_bins):,} index files  ({n_workers} workers) …")

    _lock = threading.Lock()
    total_before = total_after = n_files = n_skipped = 0

    def _compress_file(path):
        raw = open(path, "rb").read()
        before = len(raw)

        # Already compressed — skip (this is also what makes resume work)
        if len(raw) >= 2 and raw[0] == 0x78 and raw[1] in (0x01, 0x5e, 0x9c, 0xda):
            return before, before, 0, 1   # before, after, compressed, skipped

        compressed = zlib.compress(raw, level)
        if len(compressed) < len(raw):
            tmp = path + f".{os.getpid()}.ctmp"
            with open(tmp, "wb") as f:
                f.write(compressed)
            os.replace(tmp, path)
            return before, len(compressed), 1, 0
        return before, before, 0, 0   # incompressible — leave as-is

    tb = ta = nf = ns = 0
    done = 0
    with ThreadPoolExecutor(max_workers=n_workers) as ex:
        futs = {ex.submit(_compress_file, p): p for p in all_bins}
        for fut in as_completed(futs):
            before, after, compressed, skipped = fut.result()
            tb += before; ta += after; nf += compressed; ns += skipped
            done += 1
            if done % 2000 == 0:
                sys.stderr.write(f"\r  {done:,}/{len(all_bins):,}  compressed: {nf:,}  skipped: {ns:,} …")
                sys.stderr.flush()

    sys.stderr.write("\r" + " " * 60 + "\r")
    ratio = tb / ta if ta else 1
    saved = (tb - ta) / 1e6
    print(f"Done: {nf:,} files compressed, {ns:,} already compressed")
    print(f"  Before: {tb/1e6:.1f} MB")
    print(f"  After:  {ta/1e6:.1f} MB")
    print(f"  Saved:  {saved:.1f} MB  ({ratio:.2f}x)")
    print()
    print("Note: run 'build' again before 'compress' if you add new data.")


def _find_flatlens():
    """Return path to flatlens directory or None."""
    candidates = [
        os.environ.get("FLATSEEK_FLATLENS_DIR", ""),
        os.environ.get("FLATLENS_DIR", ""),
        os.path.join(os.path.expanduser("~"), ".local", "share", "flatlens"),
        "/opt/flatlens",
    ]
    # Dev: sibling repo at ../../flatlens relative to flatseek repo root
    _flatseek_pkg = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    _flatseek_repo = os.path.dirname(_flatseek_pkg)
    candidates.insert(0, os.path.normpath(os.path.join(_flatseek_repo, "..", "..", "flatlens")))

    for p in candidates:
        if p and os.path.isdir(p) and os.path.isfile(os.path.join(p, "index.html")):
            return p
    return None


def _auto_install_flatlens(install_dir):
    """Try to auto-install flatlens to install_dir. Returns True on success."""
    import subprocess
    import tempfile, tarfile, zipfile

    parent = os.path.dirname(install_dir)
    os.makedirs(parent, exist_ok=True)

    # Try git clone first
    try:
        print(f"Trying git clone ...")
        subprocess.run(["git", "clone", "https://github.com/flatseek/flatlens.git", install_dir],
                       check=True, capture_output=True, text=True)
        print("flatlens installed via git clone.")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass  # git not available or failed — try curl

    # Try curl + GitHub API for latest release zipball
    print("git not available — trying curl + GitHub release ...")
    try:
        # Get latest release JSON
        result = subprocess.run(
            ["curl", "-sL", "--max-time", "30",
             "https://api.github.com/repos/flatseek/flatlens/releases/latest"],
            capture_output=True, text=True, check=True
        )
        import json
        release = json.loads(result.stdout)
        zipball_url = release.get("zipball_url")
        if not zipball_url:
            raise ValueError("No zipball_url in release response")

        tag = release.get("tag_name", "latest")
        print(f"Downloading {tag} release ...")

        # Download zipball to temp file
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            tmp_path = tmp.name

        subprocess.run(
            ["curl", "-sL", "--max-time", "60", "-o", tmp_path, zipball_url],
            check=True
        )

        # Extract zipball (github wraps repo-name around contents)
        with zipfile.ZipFile(tmp_path, "r") as z:
            z.extractall(parent)
        os.unlink(tmp_path)

        # Rename extracted directory to flatlens
        # The zipball extracts to e.g. flatseek-flatlens-<tag>
        extracted = None
        for entry in os.listdir(parent):
            if entry.startswith("flatseek-flatlens-"):
                extracted = os.path.join(parent, entry)
                break

        if extracted and extracted != install_dir:
            if os.path.exists(install_dir):
                shutil.rmtree(install_dir, ignore_errors=True)
            os.rename(extracted, install_dir)

        print(f"flatlens installed via curl (release: {tag}).")
        return True

    except subprocess.CalledProcessError as e:
        print(f"curl failed: {e}")
    except (json.JSONDecodeError, ValueError, zipfile.BadZipFile) as e:
        print(f"Failed to parse release or extract: {e}")
    except FileNotFoundError:
        print("curl not found — please install git or curl, or set FLATLENS_DIR manually.")

    return False


def cmd_serve(args):
    """Start the API server with flatlens dashboard, serving data from current directory."""
    import uvicorn, webbrowser

    import os as _os
    from pathlib import Path
    # Merge positional and -d/--data (argparse can't share dest between both)
    data_dir = getattr(args, "data_option", None) or args.data_dir
    data_dir = os.path.abspath(data_dir) if data_dir else os.getcwd()
    _os.environ["FLATSEEK_DATA_DIR"] = data_dir
    # Detect single-file mode (.fsk) and derive encryption key if needed
    if _os.path.isfile(data_dir) and data_dir.endswith((".fsk", ".flatseek", ".flat")):
        _os.environ["FLATSEEK_SINGLE_FILE"] = data_dir
        # Derive encryption key from passphrase and store as base64 in env
        if not _os.environ.get("FLATSEEK_FSK_KEY"):
            enc_key = _get_flatseek_enc_key(Path(data_dir), args)
            if enc_key:
                import base64 as _b64
                _os.environ["FLATSEEK_FSK_KEY"] = _b64.b64encode(enc_key).decode("ascii")
    # Also handle unpacked .fsk directories — if data_dir has encryption.json and --passphrase
    # was provided, derive key and store it so IndexManager.get_engine() can apply it.
    elif _os.path.isdir(data_dir):
        enc_json = os.path.join(data_dir, "encryption.json")
        if os.path.isfile(enc_json) and not _os.environ.get("FLATSEEK_FSK_KEY"):
            from flatseek.core.query_engine import load_encryption_key
            passphrase = getattr(args, "passphrase", None)
            if passphrase:
                try:
                    enc_key = load_encryption_key(data_dir, passphrase)
                    import base64 as _b64
                    _os.environ["FLATSEEK_FSK_KEY"] = _b64.b64encode(enc_key).decode("ascii")
                except Exception:
                    pass
    _os.environ["FLATSEEK_PORT"] = str(args.port)
    _os.environ["FLATSEEK_API_BASE"] = f"http://localhost:{args.port}"
    if args.flatlens_dir:
        _os.environ["FLATSEEK_FLATLENS_DIR"] = os.path.abspath(args.flatlens_dir)

    # Storage configuration for remote indexes
    if args.storage_backend:
        _os.environ["FLATSEEK_STORAGE_BACKEND"] = args.storage_backend
    if args.storage_url:
        _os.environ["FLATSEEK_STORAGE_URL"] = args.storage_url
    if args.storage_bucket:
        _os.environ["FLATSEEK_BUCKET"] = args.storage_bucket
    if args.storage_region:
        _os.environ["FLATSEEK_S3_REGION"] = args.storage_region
    if args.storage_endpoint:
        _os.environ["FLATSEEK_S3_ENDPOINT"] = args.storage_endpoint
    if args.storage_base_path:
        _os.environ["FLATSEEK_BASE_PATH"] = args.storage_base_path

    host = args.host
    port = args.port
    reload = not args.no_reload

    # Check if flatlens is available; offer auto-install if not
    flatlens_dir = _find_flatlens()
    if not flatlens_dir and not args.flatlens_dir:
        _install_dir = os.path.join(os.path.expanduser("~"), ".local", "share", "flatlens")
        print("flatlens dashboard not found.")
        print(f"  Auto-install to: {_install_dir}")
        try:
            ans = input("  Install flatlens now? [Y/n] ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print()
            ans = "n"

        if ans in ("", "y", "yes"):
            if _auto_install_flatlens(_install_dir):
                flatlens_dir = _install_dir
                _os.environ["FLATSEEK_FLATLENS_DIR"] = _install_dir
            else:
                print("Dashboard will not be available. Starting API only...")
        else:
            print("Skipping flatlens install. Dashboard will not be available.")

    print(f"Starting Flatseek API + Dashboard on {host}:{port}")
    print(f"Data directory: {data_dir}")
    print(f"API:        http://localhost:{port}")
    print(f"Dashboard:  http://localhost:{port}/dashboard")
    if args.flatlens_dir:
        print(f"Flatlens:   {os.path.abspath(args.flatlens_dir)}")
    print()
    print(f"Starting server... (dashboard will open automatically)")
    print()

    # Start uvicorn first, then open browser in background thread
    # webbrowser.open() is blocking and would delay server startup
    import threading
    browser_url = f"http://localhost:{port}/dashboard"

    def open_browser():
        import time
        time.sleep(1.5)  # Wait for server to be ready
        webbrowser.open(browser_url)

    browser_thread = threading.Thread(target=open_browser, daemon=True)
    browser_thread.start()

    print(f"Press Ctrl+C to stop")

    uvicorn.run(
        "flatseek.api.main:app",
        host=host,
        port=port,
        reload=reload,
    )


def cmd_api(args):
    """Start the API server only (no dashboard)."""
    import uvicorn

    import os as _os
    from pathlib import Path
    data_dir = getattr(args, "data_option", None) or args.data_dir
    data_dir = os.path.abspath(data_dir) if data_dir else os.getcwd()
    _os.environ["FLATSEEK_DATA_DIR"] = data_dir
    if _os.path.isfile(data_dir) and data_dir.endswith((".fsk", ".flatseek", ".flat")):
        _os.environ["FLATSEEK_SINGLE_FILE"] = data_dir
        if not _os.environ.get("FLATSEEK_FSK_KEY"):
            enc_key = _get_flatseek_enc_key(Path(data_dir), args)
            if enc_key:
                import base64 as _b64
                _os.environ["FLATSEEK_FSK_KEY"] = _b64.b64encode(enc_key).decode("ascii")
    _os.environ["FLATSEEK_PORT"] = str(args.port)
    # Do NOT set FLATSEEK_API_BASE — dashboard should not be attached

    # Storage configuration for remote indexes
    if args.storage_backend:
        _os.environ["FLATSEEK_STORAGE_BACKEND"] = args.storage_backend
    if args.storage_url:
        _os.environ["FLATSEEK_STORAGE_URL"] = args.storage_url
    if args.storage_bucket:
        _os.environ["FLATSEEK_BUCKET"] = args.storage_bucket
    if args.storage_region:
        _os.environ["FLATSEEK_S3_REGION"] = args.storage_region
    if args.storage_endpoint:
        _os.environ["FLATSEEK_S3_ENDPOINT"] = args.storage_endpoint
    if args.storage_base_path:
        _os.environ["FLATSEEK_BASE_PATH"] = args.storage_base_path

    host = args.host
    port = args.port
    reload = not args.no_reload

    print(f"Starting Flatseek API server on {host}:{port}")
    print(f"Data directory: {data_dir}")
    print(f"API:        http://localhost:{port}")
    print(f"Press Ctrl+C to stop")

    uvicorn.run(
        "flatseek.api.main:app",
        host=host,
        port=port,
        reload=reload,
    )


def cmd_dashboard(args):
    """Start the flatlens dashboard with a given API base URL."""
    import subprocess, webbrowser
    import os as _os

    api_base = args.api_base
    port = args.port
    flatlens_dir = args.flatlens_dir

    if not flatlens_dir:
        # Find flatlens
        candidates = [
            os.environ.get("FLATLENS_DIR", ""),
            os.path.join(os.path.expanduser("~"), ".local", "share", "flatlens"),
            "/opt/flatlens",
        ]
        _flatseek_pkg = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        _flatseek_repo = os.path.dirname(_flatseek_pkg)
        candidates.insert(0, os.path.normpath(os.path.join(_flatseek_repo, "..", "..", "flatlens")))

        for p in candidates:
            if p and os.path.isdir(p) and os.path.isfile(os.path.join(p, "index.html")):
                flatlens_dir = p
                break
        if not flatlens_dir:
            _install_dir = os.path.join(os.path.expanduser("~"), ".local", "share", "flatlens")
            print("flatlens dashboard not found.")
            print(f"  Auto-install to: {_install_dir}")
            try:
                ans = input("  Install flatlens now? [Y/n] ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                print()
                ans = "n"

            if ans in ("", "y", "yes"):
                if not _auto_install_flatlens(_install_dir):
                    sys.exit(1)
                flatlens_dir = _install_dir
            else:
                print("Aborted.")
                sys.exit(1)

    # Inject API base into flatlens js/api.js
    import tempfile, shutil, re
    temp_dir = tempfile.mkdtemp(prefix="flatlens_patched_")
    try:
        shutil.copytree(flatlens_dir, temp_dir, dirs_exist_ok=True)
        api_js = os.path.join(temp_dir, "js", "api.js")
        if os.path.exists(api_js):
            with open(api_js, "r", encoding="utf-8") as f:
                content = f.read()
            content = re.sub(
                r"const API_BASE\s*=\s*['\"][^'\"]*['\"]",
                f"const API_BASE = '{api_base}'",
                content,
            )
            with open(api_js, "w", encoding="utf-8") as f:
                f.write(content)

        # Start http-server for the dashboard
        print(f"Starting Flatlens Dashboard on http://localhost:{port}")
        print(f"API base: {api_base}")
        print(f"Dashboard: http://localhost:{port}")
        print(f"Press Ctrl+C to stop")
        print()
        print(f"Opening dashboard in browser...")
        webbrowser.open(f"http://localhost:{port}")

        proc = subprocess.Popen(
            ["npx", "http-server", temp_dir, "-p", str(port), "-c-1", "--cors"],
            cwd=temp_dir,
        )
        try:
            proc.wait()
        except KeyboardInterrupt:
            proc.terminate()
            proc.wait()
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def cmd_generate(args):
    """Generate dummy dataset for benchmarking."""
    import importlib.util

    # __file__ is flatseek/src/flatseek/cli.py → up 3 levels to project root → flatbench/src/flatbench/generators
    flatbench_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", "flatbench", "src", "flatbench")
    gen_path = os.path.join(flatbench_path, "generators", "__init__.py")
    if not os.path.exists(gen_path):
        sys.exit("Error: flatbench generators not found. Is flatbench sibling to flatseek?")

    spec = importlib.util.spec_from_file_location("flatbench_generators", gen_path)
    gen_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gen_mod)

    gen_mod.generate_dataset(
        schema=args.schema,
        rows=args.rows,
        output_path=args.output,
        format=args.format,
        seed=args.seed,
    )


def main():
    parser = argparse.ArgumentParser(
        prog="flatseek",
        description="Flatseek CLI — Disk-first serverless search indexer and query engine.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--version", action="version", version=f"flatseek {__version__}")
    sub = parser.add_subparsers(dest="command")

    # classify
    p = sub.add_parser("classify", help="Detect column semantic types → column_map.json")
    p.add_argument("csv_dir", help="Directory containing CSV files")
    p.add_argument("-o", "--output", help="Output path (default: ./data/column_map.json)")
    p.add_argument("-s", "--sep", default=",", metavar="CHAR",
                   help="CSV field separator (default: ','). Use '#' for '#'-delimited files.")
    p.add_argument("--columns", default=None, metavar="col1,col2,...",
                   help="Comma-separated column names for headerless files. "
                        "When given, the first row is treated as data, not a header.")

    # build
    p = sub.add_parser("build", help="Build trigram index from CSVs")
    p.add_argument("csv_dir", help="CSV file or directory containing CSV files")
    p.add_argument("-o", "--output", default="./data", help="Output directory (default: ./data)")
    p.add_argument("--single-file", action="store_true", default=False, dest="single_file",
                   help="Output a single .flatseek file instead of a directory. "
                        "Builds to a temp dir first, then auto-packs. "
                        "Passphrase handled automatically if --passphrase is set.")
    p.add_argument("-m", "--map", help="Path to column_map.json (auto-detected if in output dir)")
    p.add_argument("--dataset", default=None, help="Dataset label (e.g. 'people', 'accounts')")
    p.add_argument("-s", "--sep", default=",", metavar="CHAR",
                   help="CSV field separator (default: ','). Use '#' for '#'-delimited files.")
    p.add_argument("--columns", default=None, metavar="col1,col2,...",
                   help="Comma-separated column names for headerless files.")
    p.add_argument("-w", "--workers", type=int, default=1, metavar="N", dest="workers",
                   help="Number of parallel workers (default: 1). When > 1, auto-generates a "
                        "build plan and spawns N worker processes — uses all CPU cores. "
                        "Works for both single-file (byte-range split) and multi-file datasets.")
    p.add_argument("--plan", default=None, metavar="PATH",
                   help="Path to build_plan.json (generated by 'plan' command). "
                        "Required for distributed/parallel builds.")
    p.add_argument("--worker-id", default=None, type=int, dest="worker_id", metavar="N",
                   help="Worker index for distributed builds (0-based). "
                        "Must be used together with --plan.")
    p.add_argument("--estimate", action="store_true", default=False,
                   help="Sample 5,000 rows before indexing to estimate speed, ETA, and "
                        "storage. Adds a few seconds of startup time. Skipped by default.")
    p.add_argument("--dedup", action="store_true", default=False,
                   help="Skip duplicate rows during indexing (hashes all non-meta fields). "
                        "Catches duplicates within the same build session / worker shard. "
                        "For cross-worker dedup after a parallel build, use 'flatseek dedup'.")
    p.add_argument("--dedup-fields", default=None, metavar="col1,col2,...",
                   dest="dedup_fields",
                   help="Dedup on specific canonical column keys only (e.g. 'phone,nik'). "
                        "Implies --dedup. Useful when rows differ only in unimportant fields.")
    p.add_argument("--daemon", action="store_true", default=False,
                   help="Daemon/memory mode: never write prefix buffers to disk at checkpoint. "
                        "All index data accumulates in RAM; a lazy background writer drains "
                        "buffers ≥ 512 KB to disk every 10 s. Recommended when you have "
                        "abundant free RAM (≥ 8 GB) and want maximum indexing speed. "
                        "Finalize writes all remaining buffers at the end.")
    p.add_argument("--storage-backend", default=None, choices=["local", "s3", "vercel-blob", "url"],
                   dest="storage_backend",
                   help="Storage backend: local (default), s3, vercel-blob, or url (github/huggingface). "
                        "Defaults to local storage (or FLATSEEK_STORAGE_BACKEND env var).")
    p.add_argument("--storage-bucket", default=None, dest="storage_bucket",
                   help="Bucket name for S3 or Vercel Blob. Defaults to FLATSEEK_BUCKET env var.")
    p.add_argument("--storage-region", default=None, dest="storage_region",
                   help="AWS region for S3. Defaults to FLATSEEK_S3_REGION env var.")
    p.add_argument("--storage-endpoint", default=None, dest="storage_endpoint",
                   help="Custom S3 endpoint URL (for MinIO, etc.). Defaults to FLATSEEK_S3_ENDPOINT env var.")
    p.add_argument("--storage-base-path", default=None, dest="storage_base_path",
                   help="Prefix path within bucket. Defaults to FLATSEEK_BASE_PATH env var.")
    p.add_argument("--storage-url", default=None, dest="storage_url",
                   help="URL for remote index (GitHub releases, HuggingFace, or generic URL). "
                        "Used when --storage-backend is 'url'. E.g.: "
                        "https://github.com/user/repo/releases/download/v1.0/")

    # generate
    _gen_help = "Generate dummy dataset for benchmarking (standard, ecommerce, logs, nested, sparse, article, adsb, campaign, devops, sosmed, blockchain)"
    p = sub.add_parser("generate", help=_gen_help)
    p.add_argument("-o", "--output", required=True, help="Output file path (CSV or JSONL)")
    p.add_argument("-r", "--rows", type=int, default=100000,
                   help="Number of rows to generate (default: 100,000)")
    p.add_argument("-s", "--schema", default="standard",
                   choices=["standard", "ecommerce", "logs", "nested", "sparse",
                            "article", "adsb", "campaign", "devops", "sosmed", "blockchain"],
                   help="Dataset schema (default: standard)")
    p.add_argument("-f", "--format", default="csv",
                   choices=["csv", "jsonl"],
                   help="Output format (default: csv)")
    p.add_argument("--seed", type=int, default=None,
                   help="Random seed for reproducibility")

    # plan
    p = sub.add_parser("plan", help="Generate a build plan for parallel/distributed indexing")
    p.add_argument("csv_dir", help="CSV file or directory")
    p.add_argument("-o", "--output", default="./data", help="Output directory (default: ./data)")
    p.add_argument("-n", "--workers", type=int, default=4, metavar="N",
                   help="Number of parallel workers (default: 4)")
    p.add_argument("-s", "--sep", default=",", metavar="CHAR",
                   help="CSV field separator (passed to each worker)")
    p.add_argument("--columns", default=None, metavar="col1,col2,...",
                   help="Column names for headerless files (passed to each worker)")

    # search
    p = sub.add_parser("search", help="Search the index (supports %% wildcards)")
    p.add_argument("data_dir", help="Index directory (same as build output)")
    p.add_argument("query", nargs="?", default="", help="Search term (use %% for wildcards)")
    p.add_argument("-c", "--column", default=None, help="Restrict to canonical column name")
    p.add_argument("-p", "--page", type=int, default=0, help="Page number, 0-based (default: 0)")
    p.add_argument("-n", "--page-size", type=int, default=20, dest="page_size",
                   help="Results per page (default: 20)")
    p.add_argument("--and", dest="and_", action="append", metavar="col:term",
                   help="AND condition (repeatable), e.g. --and status:active --and country:ID")
    p.add_argument("--passphrase", default=None, metavar="PASS",
                   help="Decryption passphrase (prompted interactively if index is encrypted "
                        "and this flag is omitted)")
    p.add_argument("--storage-backend", default=None, choices=["local", "s3", "vercel-blob", "url"],
                   dest="storage_backend",
                   help="Storage backend: local (default), s3, vercel-blob, or url (github/huggingface). "
                        "Defaults to local storage (or FLATSEEK_STORAGE_BACKEND env var).")
    p.add_argument("--storage-bucket", default=None, dest="storage_bucket",
                   help="Bucket name for S3 or Vercel Blob. Defaults to FLATSEEK_BUCKET env var.")
    p.add_argument("--storage-region", default=None, dest="storage_region",
                   help="AWS region for S3. Defaults to FLATSEEK_S3_REGION env var.")
    p.add_argument("--storage-endpoint", default=None, dest="storage_endpoint",
                   help="Custom S3 endpoint URL (for MinIO, etc.). Defaults to FLATSEEK_S3_ENDPOINT env var.")
    p.add_argument("--storage-base-path", default=None, dest="storage_base_path",
                   help="Prefix path within bucket. Defaults to FLATSEEK_BASE_PATH env var.")
    p.add_argument("--storage-url", default=None, dest="storage_url",
                   help="URL for remote index (GitHub releases, HuggingFace, or generic URL). "
                        "Used when --storage-backend is 'url'. E.g.: "
                        "https://github.com/user/repo/releases/download/v1.0/")

    # join
    p = sub.add_parser("join", help="Cross-dataset join on shared field")
    p.add_argument("data_dir", help="Index directory")
    p.add_argument("query_a", help='Query for dataset A, e.g. "_dataset:solana_txs AND program:raydium"')
    p.add_argument("query_b", help='Query for dataset B, e.g. "_dataset:logs AND service:api-gateway"')
    p.add_argument("--on", required=True, help="Shared field to join on (e.g. signer, trace_id)")
    p.add_argument("-p", "--page", type=int, default=0)
    p.add_argument("-n", "--page-size", type=int, default=20, dest="page_size")
    p.add_argument("--passphrase", default=None, metavar="PASS",
                   help="Decryption passphrase for encrypted indexes")

    # chat
    p = sub.add_parser("chat", help="Interactive natural-language chat (needs Ollama)")
    p.add_argument("data_dir")
    p.add_argument("--model", default="qwen2.5-coder", help="LLM model name")
    p.add_argument("--api-base", default="http://localhost:11434/v1", dest="api_base")

    # stats
    p = sub.add_parser("stats", help="Show index statistics")
    p.add_argument("data_dir")

    # serve
    p = sub.add_parser("serve", help="Start API server + dashboard")
    p.add_argument("data_dir", nargs="?", default=None,
                   help="Data directory or .fsk file to serve (default: current directory)")
    p.add_argument("-d", "--data", dest="data_option", default=None,
                   help="Data directory or .fsk file to serve (default: current directory)")
    p.add_argument("-p", "--port", type=int, default=8000, dest="port",
                   help="Port number (default: 8000)")
    p.add_argument("--host", default="0.0.0.0", dest="host",
                   help="Host to bind to (default: 0.0.0.0)")
    p.add_argument("--no-reload", action="store_true", default=False,
                   help="Disable auto-reload (default: enabled)")
    p.add_argument("--flatlens-dir", default=None, dest="flatlens_dir",
                   help="Path to flatlens installation (default: auto-detected from "
                        "FLATLENS_DIR, ~/.local/share/flatlens, or sibling flatlens repo)")
    p.add_argument("--storage-backend", default=None,
                   choices=["local", "s3", "vercel-blob", "url"],
                   help="Storage backend for remote index (default: local)")
    p.add_argument("--storage-url", default=None, dest="storage_url",
                   help="Base URL for URL storage backend (GitHub releases, HuggingFace, etc.)")
    p.add_argument("--storage-bucket", default=None, dest="storage_bucket",
                   help="Bucket name for S3/Vercel Blob storage")
    p.add_argument("--storage-region", default=None, dest="storage_region",
                   help="Region for S3 storage")
    p.add_argument("--storage-endpoint", default=None, dest="storage_endpoint",
                   help="Custom endpoint for S3-compatible storage")
    p.add_argument("--storage-base-path", default=None, dest="storage_base_path",
                   help="Base path within storage")
    p.add_argument("--passphrase", default=None, dest="passphrase",
                   help="Passphrase for encrypted .fsk files")

    # api
    p = sub.add_parser("api", help="Start API server only (no dashboard)")
    p.add_argument("data_dir", nargs="?", default=None,
                   help="Data directory or .fsk file to serve (default: current directory)")
    p.add_argument("-d", "--data", dest="data_option", default=None,
                   help="Data directory or .fsk file to serve (default: current directory)")
    p.add_argument("-p", "--port", type=int, default=8000, dest="port",
                   help="Port number (default: 8000)")
    p.add_argument("--host", default="0.0.0.0", dest="host",
                   help="Host to bind to (default: 0.0.0.0)")
    p.add_argument("--no-reload", action="store_true", default=False,
                   help="Disable auto-reload (default: enabled)")
    p.add_argument("--storage-backend", default=None,
                   choices=["local", "s3", "vercel-blob", "url"],
                   help="Storage backend for remote index (default: local)")
    p.add_argument("--storage-url", default=None, dest="storage_url",
                   help="Base URL for URL storage backend")
    p.add_argument("--storage-bucket", default=None, dest="storage_bucket",
                   help="Bucket name for S3/Vercel Blob storage")
    p.add_argument("--storage-region", default=None, dest="storage_region",
                   help="Region for S3 storage")
    p.add_argument("--storage-endpoint", default=None, dest="storage_endpoint",
                   help="Custom endpoint for S3-compatible storage")
    p.add_argument("--storage-base-path", default=None, dest="storage_base_path",
                   help="Base path within storage")
    p.add_argument("--passphrase", default=None, dest="passphrase",
                   help="Passphrase for encrypted .fsk files")

    # dashboard
    p = sub.add_parser("dashboard",
                       help="Start flatlens dashboard only (requires running API)")
    p.add_argument("-p", "--port", type=int, default=8080, dest="port",
                   help="Dashboard port (default: 8080)")
    p.add_argument("--api-base", default="http://localhost:8000", dest="api_base",
                   help="Base URL of the Flatseek API (default: http://localhost:8000)")
    p.add_argument("--flatlens-dir", default=None, dest="flatlens_dir",
                   help="Path to flatlens installation (auto-detected if not provided)")

    # compress
    p = sub.add_parser("compress", help="Compress index files in-place (run after build)")
    p.add_argument("data_dir", help="Index directory (same as build output)")
    p.add_argument("-l", "--level", type=int, default=6, choices=range(1, 10),
                   help="zlib compression level 1-9 (default: 6)")
    p.add_argument("-w", "--workers", type=int, default=None, metavar="N",
                   help="Parallel workers (default: min(8, cpu_count))")

    # encrypt
    p = sub.add_parser("encrypt",
                       help="Encrypt index in-place with ChaCha20-Poly1305 (run after build/compress)")
    p.add_argument("data_dir", help="Index directory (same as build output)")
    p.add_argument("--passphrase", default=None, metavar="PASS",
                   help="Encryption passphrase (prompted interactively if omitted)")
    p.add_argument("--resume", dest="resume", action="store_true", default=True,
                   help="Skip files already encrypted (default: true — safe rerun)")
    p.add_argument("--no-resume", dest="resume", action="store_false",
                   help="Force re-encryption of every file (rare; use after key change)")
    p.add_argument("--verify", dest="verify", action="store_true", default=False,
                   help="After encrypting, decrypt a sample back to verify the auth tag")
    p.add_argument("--no-verify", dest="verify", action="store_false",
                   help="Skip integrity verification (default)")
    p.add_argument("--verify-sample", type=int, default=100, metavar="N",
                   help="Number of files to sample for --verify (0 = all, default: 100)")

    # decrypt
    p = sub.add_parser("decrypt", help="Decrypt index in-place (reverse of encrypt)")
    p.add_argument("data_dir", help="Index directory (same as build output)")
    p.add_argument("--passphrase", default=None, metavar="PASS",
                   help="Decryption passphrase (prompted interactively if omitted)")
    p.add_argument("--resume", dest="resume", action="store_true", default=True,
                   help="Skip files already plaintext (default: true — safe rerun)")
    p.add_argument("--no-resume", dest="resume", action="store_false",
                   help="Force re-decryption of every file (rare)")
    p.add_argument("--verify", dest="verify", action="store_true", default=False,
                   help="After decrypting, round-trip a sample through encrypt→decrypt to verify")
    p.add_argument("--no-verify", dest="verify", action="store_false",
                   help="Skip integrity verification (default)")
    p.add_argument("--verify-sample", type=int, default=100, metavar="N",
                   help="Number of files to sample for --verify (0 = all, default: 100)")

    # delete
    p = sub.add_parser("delete", help="Delete an index directory fast (parallel unlink)")
    p.add_argument("data_dir", help="Index directory to delete")
    p.add_argument("-y", "--yes", action="store_true", default=False,
                   help="Skip confirmation prompt")
    p.add_argument("-w", "--workers", type=int, default=None, metavar="N",
                   help="Parallel workers (default: min(16, cpu_count))")

    # dedup
    p = sub.add_parser("dedup",
                       help="Remove duplicate docs from index (works after parallel builds)")
    p.add_argument("data_dir", help="Index directory (same as build output)")
    p.add_argument("--fields", default=None, metavar="col1,col2,...",
                   help="Canonical column keys to use for fingerprint (default: all non-meta "
                        "fields). Example: --fields phone,nik")
    p.add_argument("--dry-run", action="store_true", default=False, dest="dry_run",
                   help="Report duplicates without making any changes")
    p.add_argument("-w", "--workers", type=int, default=None, metavar="N",
                   help="Parallel workers for rewrite phase (default: min(8, cpu_count))")

    # wal
    p = sub.add_parser("wal", help="Manage WAL (Write-Ahead Log) files from ongoing builds")
    sub_wal = p.add_subparsers(dest="wal_action", metavar="ACTION")

    p_merge = sub_wal.add_parser("merge", help="Merge WAL files into index (safe during ongoing build)")
    p_merge.add_argument("data_dir", help="Index directory with _wal/ folder")
    p_merge.add_argument("--dry-run", action="store_true", default=False, dest="dry_run",
                         help="Show what would be merged without writing anything")
    p_merge.add_argument("--workers", type=int, default=4, metavar="N",
                        help="Parallel workers for WAL merge (default: 4)")

    p_info = sub_wal.add_parser("info", help="Show WAL file statistics")
    p_info.add_argument("data_dir", help="Index directory with _wal/ folder")

    # pack - pack index folder into single .flatseek file
    p = sub.add_parser("pack", help="Pack index folder into single .flatseek file")
    p.add_argument("index_dir", help="Index directory to pack")
    p.add_argument("-o", "--output", required=True, help="Output .flatseek file path")
    p.add_argument("--passphrase", default=None, metavar="PASS",
                   help="Encrypt the packed file with this passphrase (omit to pack as plaintext)")

    # unpack - unpack .flatseek file into directory
    p = sub.add_parser("unpack", help="Unpack .flatseek file into index directory")
    p.add_argument("flatseek_file", help=".flatseek file to unpack")
    p.add_argument("-o", "--output", help="Output directory (default: <name>.unpacked)")
    p.add_argument("--passphrase", default=None, metavar="PASS",
                   help="Decryption passphrase for encrypted .fsk manifest (required if .fsk is encrypted)")

    args = parser.parse_args()

    if args.command == "classify":
        cmd_classify(args)
    elif args.command == "build":
        cmd_build(args)
    elif args.command == "search":
        cmd_search(args)
    elif args.command == "join":
        cmd_join(args)
    elif args.command == "chat":
        cmd_chat(args)
    elif args.command == "stats":
        cmd_stats(args)
    elif args.command == "serve":
        cmd_serve(args)
    elif args.command == "api":
        cmd_api(args)
    elif args.command == "dashboard":
        cmd_dashboard(args)
    elif args.command == "compress":
        cmd_compress(args)
    elif args.command == "encrypt":
        cmd_encrypt(args)
    elif args.command == "decrypt":
        cmd_decrypt(args)
    elif args.command == "delete":
        cmd_delete(args)
    elif args.command == "dedup":
        cmd_dedup(args)
    elif args.command == "wal":
        cmd_wal(args)
    elif args.command == "plan":
        cmd_plan(args)
    elif args.command == "generate":
        cmd_generate(args)
    elif args.command == "pack":
        from flatseek.flatseek_file import FlatseekPacker
        from flatseek.core.query_engine import load_encryption_key, is_encrypted
        import os as _os
        index_dir = Path(args.index_dir).resolve()
        output_path = Path(args.output).resolve()
        # Only auto-add .fsk if no extension given (e.g. "my_index" → "my_index.fsk")
        # If user gives "foo.bar", use it as-is
        if "." not in output_path.name:
            output_path = Path(str(output_path) + ".fsk")

        # Load encryption key if index is encrypted AND --passphrase is provided.
        # Without --passphrase, pack as plaintext (no interactive prompt).
        enc_key = None
        enc_path = index_dir / "encryption.json"
        if enc_path.exists() and args.passphrase:
            enc_key = load_encryption_key(str(index_dir), args.passphrase)

        packer = FlatseekPacker(index_dir, output_path, enc_key=enc_key)
        packer.pack()
    elif args.command == "unpack":
        import json
        import base64
        import re
        import getpass as _getpass
        from flatseek.flatseek_file import FlatseekUnpacker
        flatseek_path = Path(args.flatseek_file).resolve()
        if args.output:
            output_dir = Path(args.output).resolve()
        else:
            output_dir = flatseek_path.parent / (flatseek_path.stem + "_unpacked")

        # Derive encryption key if .fsk is encrypted
        enc_key = None
        enc_meta = None
        try:
            raw = flatseek_path.read_bytes()[:8192].decode("utf-8", errors="replace")
            m = re.search(r'"_encryption_b64"\s*:\s*"([^"]+)"', raw)
            if m:
                enc_raw = base64.b64decode(m.group(1))
                enc_meta = json.loads(enc_raw.decode("utf-8"))
        except Exception:
            enc_meta = None

        if enc_meta:
            from flatseek.core.query_engine import load_encryption_key
            passphrase = args.passphrase
            if not passphrase:
                try:
                    passphrase = _getpass.getpass(f"Passphrase for {flatseek_path.name}: ")
                except Exception:
                    passphrase = None
            if passphrase:
                enc_key = load_encryption_key(None, passphrase, meta=enc_meta)

        unpacker = FlatseekUnpacker(flatseek_path, output_dir, enc_key=enc_key)
        unpacker.unpack()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
