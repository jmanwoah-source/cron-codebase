#!/usr/bin/env python3
"""dd_delta_cache.py — Phase 2 delta-hash gate for the Deep-Dive job (2026-11-07).

Cuts the 2h53m Qwen3.8 scoring loop: before the LLM re-scores a ticker,
hash its identity tuple  sha256(ticker|cik|latest_filing_id|latest_brief_id):
  * latest_filing_id = latest 8-K accession for the ticker
                        (~/.hermes/cron/edgar_8k_hits.jsonl)
  * latest_brief_id  = latest brief date_created touching the ticker
                        (~/.hermes/cron/signal_briefs.json)
If the hash is UNCHANGED from the stored record AND the record is fresher than
the window (default 24h) AND the ticker has a cached score row with a
non-null best_track_score in ticker_scores.json, the LLM leg SKIPS re-scoring
that ticker (price/options math still refreshes every cycle). Any change to a
filing or a new brief -> the ticker JUDGES fresh.

Contract: writes ~/.hermes/cron/dd_delta_cache.json
  {as_of, window_h, judged:[...], skipped:[...], forced:[...],
   stats:{n_total, n_judged, n_skipped, skipped_pct, est_llm_tokens_saved}}
The Deep-Dive job prompt reads `skipped` and reuses cached scores for those
tickers instead of spending LLM tokens.

Usage:
  python3 dd_delta_cache.py [--window 24] [--force T1,T2]
Idempotent: re-running within the same minute yields the same split.
"""
import argparse
import datetime
import hashlib
import json
import os
import sys

HERE = os.path.expanduser("~/.hermes/cron")
STATE = os.path.join(HERE, "dd_delta_cache_state.json")
OUT = os.path.join(HERE, "dd_delta_cache.json")
SCORES = os.path.join(HERE, "ticker_scores.json")
HITS = os.path.join(HERE, "edgar_8k_hits.jsonl")
BRIEFS = os.path.join(HERE, "signal_briefs.json")
# conservative per-ticker LLM scoring cost (tokens), for the savings estimate
TOKENS_PER_TICKER = 4500


def read_json(path, default):
    try:
        with open(path) as fh:
            return json.load(fh)
    except Exception:
        return default


def atomic_write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(obj, fh)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def latest_filings():
    """ticker -> latest accession (most recent ts wins)."""
    out = {}
    try:
        for line in open(HITS):
            line = line.strip()
            if not line:
                continue
            try:
                h = json.loads(line)
            except Exception:
                continue
            t = (h.get("ticker") or "").upper()
            acc = h.get("accession")
            if not t or not acc:
                continue
            ts = h.get("ts") or ""
            if t not in out or ts >= (out[t][1] or ""):
                out[t] = (acc, ts)
        return {t: v[0] for t, v in out.items()}
    except OSError:
        return {}


def latest_briefs():
    """ticker -> latest brief date_created touching it."""
    out = {}
    briefs = read_json(BRIEFS, [])
    if isinstance(briefs, dict):
        briefs = briefs.get("briefs", [])
    for b in briefs:
        if not isinstance(b, dict):
            continue
        dc = b.get("date_created") or b.get("ts") or ""
        for k in ("tickers_benefit", "tickers_affected"):
            for t in (b.get(k) or []):
                t = str(t).upper()
                if t not in out or dc > out[t]:
                    out[t] = dc
    return out


def score_rows():
    d = read_json(SCORES, {})
    rows = d.get("rankings", []) if isinstance(d, dict) else []
    return {r.get("ticker"): r for r in rows if isinstance(r, dict) and r.get("ticker")}


def universe(tickers_from_scores):
    """Universe = ranked tickers (the set the LLM leg would score)."""
    v = read_json(os.path.join(HERE, "vetted_tickers.json"), {})
    vt = {t.get("ticker") for t in v.get("all_tickers", []) if t.get("ticker")}
    univ = vt | set(tickers_from_scores)
    return sorted(univ)


def compute_hashes(universe, cik_map, filings, briefs):
    out = {}
    for t in universe:
        h = hashlib.sha256(
            f"{t}|{cik_map.get(t, '')}|{filings.get(t, '')}|{briefs.get(t, '')}"
            .encode()).hexdigest()
        out[t] = h
    return out


def main():
    import time
    t0 = time.time()
    ap = argparse.ArgumentParser()
    ap.add_argument("--window", type=float, default=24.0,
                    help="max age (hours) for a hash to allow a skip")
    ap.add_argument("--force", default="",
                    help="comma-separated tickers that must JUDGE regardless")
    a = ap.parse_args()
    forced = {x.strip().upper() for x in a.force.split(",") if x.strip()}

    rows = score_rows()
    cik_map = {t: (r.get("cik") or "") for t, r in rows.items()}
    filings = latest_filings()
    briefs = latest_briefs()
    univ = universe(rows)
    t1 = time.time()

    state = read_json(STATE, {})
    recs = state.get("records", {})
    now = datetime.datetime.now(datetime.timezone.utc)
    window_s = a.window * 3600.0

    judged, skipped, used_forced = [], [], []
    new_recs = {}
    for t in univ:
        h = compute_hashes([t], cik_map, filings, briefs)[t]
        r = recs.get(t)
        fresh = False
        if r and r.get("hash") == h:
            try:
                age = (now - datetime.datetime.fromisoformat(r["ts"])).total_seconds()
                fresh = age <= window_s
            except Exception:
                fresh = False
        has_score = isinstance(rows.get(t), dict) and rows[t].get("best_track_score") is not None
        if t in forced:
            judged.append(t)
            used_forced.append(t)
        elif fresh and has_score:
            skipped.append(t)
            new_recs[t] = {"hash": h, "ts": now.isoformat(),
                           "last_skip": now.isoformat(),
                           "cik": cik_map.get(t, ""),
                           "filing_id": filings.get(t, ""),
                           "brief_id": briefs.get(t, "")}
        else:
            judged.append(t)
            new_recs[t] = {"hash": h, "ts": now.isoformat(),
                           "last_skip": (r or {}).get("last_skip"),
                           "cik": cik_map.get(t, ""),
                           "filing_id": filings.get(t, ""),
                           "brief_id": briefs.get(t, "")}
    t2 = time.time()

    n = len(univ)
    payload = {
        "as_of": now.isoformat(),
        "window_h": a.window,
        "judged": sorted(judged),
        "skipped": sorted(skipped),
        "forced": sorted(used_forced),
        "stats": {
            "n_total": n,
            "n_judged": len(judged),
            "n_skipped": len(skipped),
            "skipped_pct": round(100.0 * len(skipped) / max(n, 1), 1),
            "est_llm_tokens_saved": len(skipped) * TOKENS_PER_TICKER,
        },
    }
    atomic_write_json(OUT, payload)
    atomic_write_json(STATE, {"updated": now.isoformat(), "records": new_recs})

    print(f"dd_delta_cache: {n} tickers | judged {len(judged)} | "
          f"skipped {len(skipped)} ({payload['stats']['skipped_pct']}%) | "
          f"forced {len(used_forced)} | ~{payload['stats']['est_llm_tokens_saved']:,} "
          f"LLM tokens saved this cycle")
    print(f"  timing: identity-load {round(t1-t0,2)}s | decision pass "
          f"{round(t2-t1,2)}s | total {round(t2-t0,2)}s")
    for t in sorted(univ)[:20]:
        tag = "SKIP" if t in skipped else ("FORCE" if t in forced else "JUDGE")
        r = new_recs[t]
        print(f"  {t:<6} {tag:<5} filing={r['filing_id'] or '-'} "
              f"brief={r['brief_id'] or '-'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
