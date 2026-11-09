#!/usr/bin/env python3
"""
Ingestion harness for the large alpha_vectors / regime_state update.

Design (confirmed 2026-11-04):
  - Absolute upsert: INSERT ... ON CONFLICT(pk) DO UPDATE SET ... (latest-snapshot per key)
  - WAL + busy_timeout=5000 + synchronous=NORMAL
  - Batches of MAX_BATCH=100 rows per explicit transaction (lock-hold hygiene
    vs. 10-min cron readers)
  - Field whitelist from live PRAGMA table_info — unknown payload fields REJECT
    the batch (fail loud, never silently drop or wedge)

Usage:
  python3 ingest_quant_update.py <payload.json> [--db PATH] [--dry-run]

Payload shapes accepted:
  A) {"alpha_vectors": [ {...}, ... ], "regime_state": [ {...}, ... ]}
  B) [ {...}, ... ]            -> routed by key presence: 'ticker' => alpha_vectors,
                                   'pillar' => regime_state
  C) {"rows": [ ... ]}         -> same key routing as B
"""

import argparse
import json
import sqlite3
import sys
import time

DB_PATH = "/home/jaren/.hermes/cron/universe_cache.db"
MAX_BATCH = 100

ALLOWED_KEYS = {"alpha_vectors", "regime_state"}
PKS = {"alpha_vectors": "ticker", "regime_state": "pillar"}


def table_columns(conn, table):
    # name -> (declared_type, is_notnull, has_default)
    return {r[1]: (r[2], bool(r[3]), r[4] is not None) for r in conn.execute(f"PRAGMA table_info({table})")}


def build_upsert(table, cols, pk):
    names = [c for c in cols]  # preserve table order
    cols_sql = ", ".join(names)
    ph = ", ".join("?" for _ in names)
    set_sql = ", ".join(
        f"{c} = excluded.{c}" for c in names if c != pk
    )
    return (
        f"INSERT INTO {table} ({cols_sql}) VALUES ({ph}) "
        f"ON CONFLICT({pk}) DO UPDATE SET {set_sql}"
    )


def validate_rows(rows, cols):
    """cols: {name: (type, is_notnull, has_default)}. Returns (ok_rows, errors)."""
    ok, errors = [], []
    for i, row in enumerate(rows):
        bad = []
        for name, (_typ, is_notnull, has_default) in cols.items():
            if name in row and row[name] is not None:
                continue
            if is_notnull and not has_default:
                bad.append(f"missing required field '{name}'")
        for name in row:
            if name not in cols:
                bad.append(f"unknown field '{name}'")
        if bad:
            errors.append(f"row {i}: {'; '.join(bad)}")
        else:
            ok.append(row)
    return ok, errors


def route(rows):
    alpha, regime, unknown = [], [], 0
    for r in rows:
        if "ticker" in r:
            alpha.append(r)
        elif "pillar" in r:
            regime.append(r)
        else:
            unknown += 1
    return alpha, regime, unknown


def ingest_one(conn, table, rows):
    cols = table_columns(conn, table)
    ok, errors = validate_rows(rows, cols)
    if errors:
        # Fail loud: reject the whole table's batch set — no partial writes
        raise ValueError(f"{table}: validation failed:\n" + "\n".join(errors[:20]))
    sql = build_upsert(table, cols, PKS[table])
    col_names = list(cols.keys())
    n = 0
    for start in range(0, len(ok), MAX_BATCH):
        batch = ok[start : start + MAX_BATCH]
        params = [tuple(r.get(c) for c in col_names) for r in batch]
        conn.execute("BEGIN IMMEDIATE")
        conn.executemany(sql, params)
        conn.commit()  # explicit commit per <=100-row transaction
        n += len(batch)
    return n, errors


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("payload")
    ap.add_argument("--db", default=DB_PATH)
    ap.add_argument("--dry-run", action="store_true",
                    help="validate + route only; write nothing")
    args = ap.parse_args()

    with open(args.payload) as f:
        payload = json.load(f)

    if isinstance(payload, dict):
        alpha, regime = payload.get("alpha_vectors", []), payload.get("regime_state", [])
        for k in payload:
            if k not in ALLOWED_KEYS and k != "rows":
                sys.exit(f"unexpected top-level key: {k}")
        if "rows" in payload:
            a2, r2, _ = route(payload["rows"])
            alpha, regime = alpha + a2, regime + r2
    else:
        alpha, regime, _ = route(payload)

    total = len(alpha) + len(regime)
    print(f"payload: {len(alpha)} alpha_vectors rows, {len(regime)} regime_state rows ({total} total, "
          f"{(total + MAX_BATCH - 1) // MAX_BATCH} transactions)")

    if args.dry_run:
        conn = sqlite3.connect(args.db)
        total_invalid = 0
        for t, rows in (("alpha_vectors", alpha), ("regime_state", regime)):
            cols = table_columns(conn, t)
            ok, errors = validate_rows(rows, cols)
            total_invalid += len(errors)
            print(f"[dry-run] {t}: {len(ok)} valid, {len(errors)} invalid")
            for e in errors[:10]:
                print(f"  ERROR {e}")
        sys.exit(1 if total_invalid else 0)

    t0 = time.time()
    with sqlite3.connect(args.db, timeout=10.0) as conn:
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA busy_timeout = 5000;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        before = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in ("alpha_vectors", "regime_state")}
        na, _ = ingest_one(conn, "alpha_vectors", alpha) if alpha else (0, [])
        nr, _ = ingest_one(conn, "regime_state", regime) if regime else (0, [])
        after = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in ("alpha_vectors", "regime_state")}
        # spot-check freshness
        fresh = conn.execute("SELECT COUNT(*) FROM alpha_vectors WHERE last_updated >= ?", (int(time.time()) - 120,)).fetchone()[0]
    print(f"committed: alpha_vectors +{na} upserts ({before['alpha_vectors']} -> {after['alpha_vectors']} rows), "
          f"regime_state +{nr} upserts ({before['regime_state']} -> {after['regime_state']} rows)")
    print(f"fresh alpha_vectors rows in last 2 min: {fresh}")
    print(f"done in {time.time() - t0:.2f}s")


if __name__ == "__main__":
    main()
