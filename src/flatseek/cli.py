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

# ── Version from pyproject.toml ──────────────────────────────────────────────
# __file__ is flatseek/src/flatseek/cli.py → up 2 levels to flatseek/ → sibling pyproject.toml
_PYPROJECT_TOML = os.path.join(os.path.dirname(__file__), "..", "..", "pyproject.toml")
try:
    import tomllib
    with open(_PYPROJECT_TOML, "rb") as _f:
        __version__ = tomllib.load(_f)["project"]["version"]
except Exception:
    __version__ = "0.1.3"


def _parse_columns(columns_str):
    """Parse '--columns a,b,c' into a list, or None if not given."""
    if not columns_str:
        return None
    return [c.strip() for c in columns_str.split(",") if c.strip()]


def _prompt_columns(path, delimiter):
    """Interactive: show a file preview, ask whether row 1 is a header,
    collect column names, then confirm/override per-column types.

    Returns (columns, type_overrides) where:
      columns       – list of names, or None (use file's own header row)
      type_overrides – {col_name: sem_type} for any user-corrected types

    Skips entirely when stdin is not a tty (piped / scripted).
    """
    if not sys.stdin.isatty():
        return None, {}

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
        return None, {}

    if not raw_rows:
        return None, {}

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
        return None, {}

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
                return None, {}

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
                        return None, {}
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
        samples = [row[cols.index(col)] for row in sample_rows_data
                   if cols.index(col) < len(row)]
        sem_type, conf = classify_column(col, samples)
        detected[col] = (sem_type, conf)

    # ── Type confirmation prompt ──────────────────────────────────────────────
    print(f"\n  ── Column types ──────────────────────────────────────────")
    print(f"  Available: {types_line}")
    print(f"  Press Enter to accept detected type, or type a replacement.\n")

    type_overrides = {}
    i = 0
    while i < len(cols):
        col = cols[i]
        sem_type, conf = detected[col]
        sample_val = (raw_rows[sample_start][cols.index(col)].strip()
                      if sample_start < len(raw_rows)
                      and cols.index(col) < len(raw_rows[sample_start]) else "")
        sample_val = (sample_val[:20] + "…") if len(sample_val) > 20 else sample_val

        prompt = (f"  [{i:>2}] {col:<20} {sample_val!r:<24} "
                  f"detected: {sem_type:<12} ({conf:.0%})  → ")
        try:
            ans = input(prompt).strip().lower()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if ans == "a":
            print(f"  Accepting all remaining as detected.")
            break
        elif ans == "" or ans == sem_type:
            pass   # accept detected
        elif ans in _TYPES:
            type_overrides[col] = ans
        else:
            print(f"  ✗ Unknown type {ans!r}. Valid: {types_line}")
            continue   # re-prompt same column

        i += 1

    # ── Summary ──────────────────────────────────────────────────────────────
    print(f"\n  Final mapping ({len(cols)} columns):")
    for i, col in enumerate(cols):
        final_type = type_overrides.get(col, detected[col][0])
        override_marker = " ← overridden" if col in type_overrides else ""
        print(f"    [{i:>2}]  {col:<20}  {final_type}{override_marker}")
    print()

    try:
        ok = input("  Apply? [Y/n] ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        return None, {}

    if ok not in ("", "y", "yes"):
        print("  Cancelled.")
        return None, {}

    return (cols if user_provided_names else None), type_overrides


def _resolve_columns(args, csv_src):
    """Return (columns, type_overrides).

    columns       – list of names (None = use file's own header row)
    type_overrides – {col_name: sem_type} for any corrected types

    Resolution order:
      1. --columns flag (no interactive prompt, no type overrides)
      2. Already classified in column_map.json with matching column count → skip
      3. Interactive prompt (show preview, ask names + types)
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
        return None, {}

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
                print(f"  File already classified as headerless ({len(stored)} cols). "
                      f"Using stored column names.")
                return None, {}   # builder reads from column_map

            n_classified = len([k for k in entry if not k.startswith("_")])
            try:
                with open(preview_file, "r", encoding="utf-8", errors="replace") as f:
                    first_row = next(csv.reader(f, delimiter=args.sep))
                n_file = len(first_row)
            except Exception:
                return None, {}

            if n_file == n_classified:
                print(f"  [{os.path.basename(preview_file)}] already classified "
                      f"({n_classified} cols). Run 'flatseek classify' to re-classify.")
                return None, {}   # unchanged, no prompt needed
            else:
                print(f"  Column count changed ({n_classified} → {n_file}). Re-classifying.")

    # Not yet classified (or columns changed) — prompt interactively
    columns, type_overrides = _prompt_columns(preview_file, args.sep)
    if columns:
        print(f"  Using {len(columns)} column name(s) from interactive prompt (first row = data).")
    return columns, type_overrides


def cmd_classify(args):
    from flatseek.core.classify import build_column_map
    out = args.output or os.path.join("./data", "column_map.json")
    out = os.path.abspath(out)
    csv_src = os.path.abspath(args.csv_dir)
    columns, type_overrides = _resolve_columns(args, csv_src)
    print(f"Classifying: {csv_src}")
    build_column_map(csv_src, output_path=out, delimiter=args.sep,
                     columns=columns, type_overrides=type_overrides)


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
        columns, type_overrides = _resolve_columns(args, csv_src)

        # Pre-classify ALL files before spawning workers — avoids concurrent
        # writes to column_map.json by N workers starting at the same time.
        col_map_file = getattr(args, "map", None) or os.path.join(output_dir, "column_map.json")
        csv_abs = os.path.abspath(csv_src)
        all_files = [csv_abs] if os.path.isfile(csv_abs) else find_data_files(csv_abs)
        csv_base  = os.path.dirname(csv_abs) if os.path.isfile(csv_abs) else csv_abs
        print("── Pre-classify ──────────────────────────────────────")
        _auto_classify(all_files, csv_base, col_map_file, delimiter=args.sep,
                       columns=columns, type_overrides=type_overrides)

        plan_data = do_plan(csv_src, output_dir, n_workers=n_workers,
                            delimiter=args.sep, columns=columns)
        if not plan_data:
            return
        # Lock in doc_id ranges immediately so that if this build is interrupted
        # and restarted, the new plan won't reuse the same doc_id ranges and
        # corrupt posting lists with double entries.
        _update_manifest_after_parallel(output_dir, plan_data)
    else:
        columns, type_overrides = _resolve_columns(args, csv_src)
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
    csv_src = os.path.abspath(args.csv_dir)
    output_dir = os.path.abspath(args.output)
    map_path = args.map
    plan_path = args.plan
    worker_id = args.worker_id
    n_workers = getattr(args, "workers", 1)

    # --workers N without an explicit plan: auto-plan + auto-spawn
    if n_workers > 1 and not plan_path:
        _run_parallel_build(args, csv_src, output_dir, n_workers)
        return

    # Skip interactive prompt when running as a distributed worker
    if plan_path and worker_id is not None:
        columns = _parse_columns(args.columns)
        type_overrides = {}
    else:
        columns, type_overrides = _resolve_columns(args, csv_src)

    if not map_path:
        candidate = os.path.join(output_dir, "column_map.json")
        if os.path.exists(candidate):
            map_path = candidate

    dedup_fields = _parse_columns(getattr(args, "dedup_fields", None))
    # --dedup with no value → dedup on all fields (empty list sentinel)
    if getattr(args, "dedup", False) and dedup_fields is None:
        dedup_fields = []

    build(csv_src, output_dir, column_map_path=map_path, dataset=args.dataset,
          delimiter=args.sep, columns=columns, type_overrides=type_overrides,
          worker_id=worker_id, plan_path=plan_path,
          estimate=getattr(args, "estimate", False),
          dedup_fields=dedup_fields,
          daemon=getattr(args, "daemon", False))


def cmd_plan(args):
    from flatseek.core.builder import plan
    csv_src = os.path.abspath(args.csv_dir)
    output_dir = os.path.abspath(args.output)
    columns = _parse_columns(args.columns)
    plan(csv_src, output_dir, n_workers=args.workers, delimiter=args.sep, columns=columns)


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
    qe = QueryEngine(args.data_dir)
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


def cmd_encrypt(args):
    """Encrypt all index and doc files in-place with ChaCha20-Poly1305.

    Can be run on a fresh build OR after 'flatseek compress' (compress first for
    better ratio — encryption preserves byte size, not compressibility).

    Format written: FLATSEEK\\x01 (5B magic) + nonce (12B) + ciphertext + auth_tag (16B).
    A random salt is stored in <data_dir>/encryption.json — needed to re-derive
    the key. Do NOT delete this file.

    Constraint: incremental builds cannot append to encrypted index files.
    Run 'flatseek decrypt' first, rebuild, then 'flatseek encrypt' again.
    """
    import os, json, secrets, sys
    from concurrent.futures import ThreadPoolExecutor
    from flatseek.core.query_engine import derive_key, encrypt_bytes, is_encrypted
    from flatseek.core.builder import FLUSH_THREADS

    data_dir  = args.data_dir
    index_dir = os.path.join(data_dir, "index")
    docs_dir  = os.path.join(data_dir, "docs")

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

    n_already = n_done = n_err = 0
    lock = __import__("threading").Lock()

    def _encrypt_file(path):
        nonlocal n_already, n_done, n_err
        try:
            with open(path, "rb") as f:
                data = f.read()
            if not data:
                return
            if is_encrypted(data):
                with lock:
                    n_already += 1
                return
            enc = encrypt_bytes(data, key)
            tmp = path + f".{os.getpid()}.etmp"
            with open(tmp, "wb") as f:
                f.write(enc)
            os.replace(tmp, path)
            with lock:
                n_done += 1
        except Exception as e:
            with lock:
                n_err += 1
            print(f"\n  Error encrypting {path}: {e}")

    print(f"Encrypting {len(targets):,} files …")
    with ThreadPoolExecutor(max_workers=FLUSH_THREADS) as ex:
        for i, _ in enumerate(ex.map(_encrypt_file, targets)):
            if i % 2000 == 0:
                sys.stderr.write(f"\r  {i:,}/{len(targets):,} …")
                sys.stderr.flush()
    sys.stderr.write("\r" + " " * 30 + "\r")

    print(f"Done: {n_done:,} encrypted, {n_already:,} already encrypted, {n_err:,} errors")
    if n_err == 0:
        print(f"Salt stored in: {enc_path}")
        print("Keep your passphrase safe — there is no recovery without it.")


def cmd_decrypt(args):
    """Decrypt all index and doc files in-place (reverse of 'flatseek encrypt').

    Use this before running incremental builds on an encrypted index, or to
    permanently remove encryption. The encryption.json salt file is removed
    after successful decryption.
    """
    import os, json, sys
    from concurrent.futures import ThreadPoolExecutor
    from flatseek.core.query_engine import load_encryption_key, decrypt_bytes, is_encrypted
    from flatseek.core.builder import FLUSH_THREADS

    data_dir = args.data_dir
    enc_path = os.path.join(data_dir, "encryption.json")

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
    for root, _, files in os.walk(index_dir):
        for fn in sorted(files):
            if fn.endswith(".bin"):
                _probe_path = os.path.join(root, fn)
                break
        if _probe_path:
            break
    if _probe_path:
        with open(_probe_path, "rb") as f:
            _probe_data = f.read()
        if is_encrypted(_probe_data):
            try:
                decrypt_bytes(_probe_data, key)
                print("OK")
            except ValueError:
                print("\nError: wrong passphrase — decryption failed. No files were modified.")
                return
        else:
            print("OK (index not encrypted)")
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

    n_plain = n_done = n_err = 0
    lock = __import__("threading").Lock()

    def _decrypt_file(path):
        nonlocal n_plain, n_done, n_err
        try:
            with open(path, "rb") as f:
                data = f.read()
            if not data:
                return
            if not is_encrypted(data):
                with lock:
                    n_plain += 1
                return
            dec = decrypt_bytes(data, key)
            tmp = path + f".{os.getpid()}.dtmp"
            with open(tmp, "wb") as f:
                f.write(dec)
            os.replace(tmp, path)
            with lock:
                n_done += 1
        except ValueError as e:
            with lock:
                n_err += 1
            print(f"\n  Error decrypting {path}: {e}")
        except Exception as e:
            with lock:
                n_err += 1
            print(f"\n  Error decrypting {path}: {e}")

    print(f"Decrypting {len(targets):,} files …")
    with ThreadPoolExecutor(max_workers=FLUSH_THREADS) as ex:
        for i, _ in enumerate(ex.map(_decrypt_file, targets)):
            if i % 2000 == 0:
                sys.stderr.write(f"\r  {i:,}/{len(targets):,} …")
                sys.stderr.flush()
    sys.stderr.write("\r" + " " * 30 + "\r")

    print(f"Done: {n_done:,} decrypted, {n_plain:,} already plain, {n_err:,} errors")
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
    data_dir = os.path.abspath(args.data_dir) if args.data_dir else os.getcwd()
    _os.environ["FLATSEEK_DATA_DIR"] = data_dir
    _os.environ["FLATSEEK_PORT"] = str(args.port)
    _os.environ["FLATSEEK_API_BASE"] = f"http://localhost:{args.port}"
    if args.flatlens_dir:
        _os.environ["FLATSEEK_FLATLENS_DIR"] = os.path.abspath(args.flatlens_dir)

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
    print(f"Opening dashboard in browser...")
    webbrowser.open(f"http://localhost:{port}/dashboard")
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
    data_dir = os.path.abspath(args.data_dir) if args.data_dir else os.getcwd()
    _os.environ["FLATSEEK_DATA_DIR"] = data_dir
    _os.environ["FLATSEEK_PORT"] = str(args.port)
    # Do NOT set FLATSEEK_API_BASE — dashboard should not be attached

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
    p.add_argument("-d", "--data", dest="data_dir", default=None,
                   help="Data directory to serve (default: current directory)")
    p.add_argument("-p", "--port", type=int, default=8000, dest="port",
                   help="Port number (default: 8000)")
    p.add_argument("--host", default="0.0.0.0", dest="host",
                   help="Host to bind to (default: 0.0.0.0)")
    p.add_argument("--no-reload", action="store_true", default=False,
                   help="Disable auto-reload (default: enabled)")
    p.add_argument("--flatlens-dir", default=None, dest="flatlens_dir",
                   help="Path to flatlens installation (default: auto-detected from "
                        "FLATLENS_DIR, ~/.local/share/flatlens, or sibling flatlens repo)")

    # api
    p = sub.add_parser("api", help="Start API server only (no dashboard)")
    p.add_argument("-d", "--data", dest="data_dir", default=None,
                   help="Data directory to serve (default: current directory)")
    p.add_argument("-p", "--port", type=int, default=8000, dest="port",
                   help="Port number (default: 8000)")
    p.add_argument("--host", default="0.0.0.0", dest="host",
                   help="Host to bind to (default: 0.0.0.0)")
    p.add_argument("--no-reload", action="store_true", default=False,
                   help="Disable auto-reload (default: enabled)")

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

    # decrypt
    p = sub.add_parser("decrypt", help="Decrypt index in-place (reverse of encrypt)")
    p.add_argument("data_dir", help="Index directory (same as build output)")
    p.add_argument("--passphrase", default=None, metavar="PASS",
                   help="Decryption passphrase (prompted interactively if omitted)")

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
    elif args.command == "plan":
        cmd_plan(args)
    elif args.command == "generate":
        cmd_generate(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
