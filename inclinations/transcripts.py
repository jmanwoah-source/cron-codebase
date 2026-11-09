#!/usr/bin/env python3
"""Earnings-transcript "onto something" scanner + R&D/revenue trend.

Pulls (via the shared SSE-MCP client in fmp.py — REST is dead):
  1. earningsTranscript / transcripts-dates-by-symbol  -> latest quarter/year
  2. earningsTranscript / search-transcripts           -> transcript text
  3. statements / income-statement (quarter, limit=4)  -> R&D & revenue

Extracts forward-looking statements (regex), flags design-win language,
computes R&D/revenue % per quarter and a 3-quarter rising trend.

Budget: 3 FMP calls per ticker, 0.4s sleep between calls (<=12 total).
Output: /home/jaren/.hermes/cron/inclinations/transcripts_out.json
"""
import glob
import json
import os
import re
import sys
import time

sys.path.insert(0, "/home/jaren/.hermes/cron/inclinations")
from fmp import fmp_session, fmp_call  # noqa: E402

OUT_PATH = "/home/jaren/.hermes/cron/inclinations/transcripts_out.json"
DEFAULT_TICKERS = ["SMTC", "CMST", "LSCC", "MCHP"]   # fallback only (wiki missing)
WIKI_GAPS = "/home/jaren/wiki/entities/tech_gaps"
STAGE_ORDER = {"shipping": 3, "first_customer": 2, "pilot": 1, "academic": 0}

FWD_RE = re.compile(
    r"(design win|design wins|we expect to|roadmap|next-generation|next generation"
    r"|secured|pilot|commercializ\w*|we anticipate)",
    re.IGNORECASE,
)
DESIGN_WIN_RE = re.compile(r"design win", re.IGNORECASE)

MAX_STATEMENTS = 12
# Budget: 16 gap-filler R&D sweeps (1 call each) + 12 design-win transcript
# pulls (2 calls each) = 40. The most-advanced-stage tickers get transcripts;
# the trailing pilot/academic tickers keep their R&D (the 1-call floor). 0.4s
# spacing keeps the whole run well under the 300s timeout. 8h-gated (~3x/day).
CALL_BUDGET = 44


def _sentences(text):
    """Split transcript text into sentence-ish chunks (max ~600 chars)."""
    parts = re.split(r"(?<=[.!?])\s+", text)
    out = []
    for p in parts:
        p = p.strip()
        if len(p) > 600:
            p = p[:600]
        if len(p) >= 15:
            out.append(p)
    return out


def extract_forward_statements(text, max_n=MAX_STATEMENTS):
    """Return list of {text, design_win} for forward-looking sentences."""
    hits, seen = [], set()
    for s in _sentences(text):
        if FWD_RE.search(s):
            key = s.lower()[:80]
            if key in seen:
                continue
            seen.add(key)
            hits.append({"text": s, "design_win": bool(DESIGN_WIN_RE.search(s))})
        if len(hits) >= max_n:
            break
    return hits


def _latest_transcript_date(sid, ticker, call_id, budget):
    r = fmp_call(sid, "earningsTranscript",
                 {"endpoint": "transcripts-dates-by-symbol", "symbol": ticker, "limit": 4},
                 call_id)
    budget["n"] += 1
    time.sleep(0.4)
    if isinstance(r, list) and r:
        return r[0]  # [{quarter, fiscalYear, date}, ...] newest first
    if isinstance(r, dict) and "dates" in r and isinstance(r["dates"], list) and r["dates"]:
        return r["dates"][0]
    return None


def _pull_transcript_text(sid, ticker, q, y, call_id, budget):
    r = fmp_call(sid, "earningsTranscript",
                 {"endpoint": "search-transcripts", "symbol": ticker,
                  "quarter": q, "year": y, "limit": 1},
                 call_id)
    budget["n"] += 1
    time.sleep(0.4)
    items = r if isinstance(r, list) else (
        r.get("results") or r.get("data") if isinstance(r, dict) else None)
    if not isinstance(items, list) or not items:
        return None, None
    entry = items[0]
    if not isinstance(entry, dict):
        return None, None
    text = None
    for k in ("transcript", "transcript_text", "text", "content", "fullTranscript"):
        v = entry.get(k)
        if isinstance(v, str) and len(v) > 200:
            text = v
            break
    if text is None:  # fall back: longest string field
        for v in entry.values():
            if isinstance(v, str) and len(v) > (len(text) if text else 200):
                text = v
    date = entry.get("date") or entry.get("fiscalDate")
    return text, date


RD_FIELDS = ("researchAndDevelopmentExpenses", "researchAndDevelopmentExpense",
             "researchAndDevelopment")


def _rd_revenue(sid, ticker, call_id, budget):
    """R&D/revenue % per quarter. Returns (pct, dates, err).

    CONFIRMED FMP field name (2026-10-17): 'researchAndDevelopmentExpenses' on
    statements/income-statement. An error-dict response must NOT be swallowed
    silently — it used to come back as ([], None) indistinguishable from no data.
    """
    r = fmp_call(sid, "statements",
                 {"endpoint": "income-statement", "symbol": ticker,
                  "period": "quarter", "limit": 4},
                 call_id)
    budget["n"] += 1
    time.sleep(0.4)
    if isinstance(r, dict):
        if r.get("_error"):
            return [], None, r["_error"]
        if r.get("status") == "not_found" or r.get("_raw_text"):
            return [], None, "no income-statement data: %s" % str(r.get("message") or r.get("_raw_text"))[:160]
        rows = r.get("incomeStatements") or r.get("data")
    elif isinstance(r, list):
        rows = r
    else:
        rows = None
    if not isinstance(rows, list):
        return [], None, "unexpected response shape: %r" % str(r)[:160]
    pct, dates = [], []
    for row in rows:
        if not isinstance(row, dict):
            continue
        rev = row.get("revenue")
        rd = None
        for f in RD_FIELDS:
            v = row.get(f)
            if isinstance(v, (int, float)):
                rd = v
                break
        if isinstance(rev, (int, float)) and rev > 0 and isinstance(rd, (int, float)):
            dates.append(row.get("date", "?"))
            pct.append(round(100.0 * rd / rev, 2))
    pct.reverse()
    dates.reverse()
    return pct, dates, None


def rd_trending_up(pct):
    """True if last 3 quarters strictly rise (or non-fall then rise)."""
    if len(pct) < 3:
        return False
    a, b, c = pct[-3], pct[-2], pct[-1]
    return c > b and b > a


SYM_OK = re.compile(r"^[A-Z][A-Z0-9]{0,6}$")


def gap_filler_universe():
    """Return {ticker: best_stage} for every ticker in the tech-gap wiki.

    This is the ACTUAL set of tickers the scanner should cover (not the 4
    legacy analog default): these are the 'onto something' companies the
    pipeline exists to flag early. Sorted later by stage (most-advanced first)
    so the limited design-win budget lands where it matters most.
    """
    universe = {}
    try:
        files = glob.glob(os.path.join(WIKI_GAPS, "*.md"))
    except Exception:
        return {}
    for path in files:
        base = os.path.basename(path)
        if base in ("SCHEMA.md", "CASCADE.md", "INDEX.md"):
            continue
        try:
            text = open(path).read()
        except Exception:
            continue
        for row in re.findall(r"^\|\s*([A-Z][A-Z0-9.\-]{1,9})\s*\|[^|]*\|\s*([a-z_]+)\s*\|",
                              text, re.M):
            tick, stage = row
            if stage not in STAGE_ORDER:
                continue
            if not SYM_OK.match(tick) or tick in ("PRIVATE", "N/A"):
                continue
            cur = universe.get(tick)
            if cur is None or STAGE_ORDER[stage] > STAGE_ORDER[cur]:
                universe[tick] = stage
    return universe


def analyze(sid, ticker, call_id, budget, want_transcript=True):
    rec = {"transcript_date": None, "forward_statements": [], "design_win": False,
           "rd_revenue_pct_by_qtr": [], "rd_trending_up": False, "error": None}
    if budget["n"] >= CALL_BUDGET:
        rec["error"] = "call budget exhausted"
        return rec

    # R&D is the 1-call signal every gap filler MUST get — do it first so the
    # budget never starves it for expensive transcript pulls.
    pct, dates, rd_err = _rd_revenue(sid, ticker, call_id, budget)
    rec["rd_revenue_pct_by_qtr"] = pct
    rec["rd_trend_dates"] = dates
    rec["rd_trending_up"] = rd_trending_up(pct)
    if rd_err:
        rec["rd_error"] = rd_err
        rec["error"] = "rd: " + rd_err

    # Design-win / forward-looking transcript is the premium 2-call signal —
    # only spend it when asked AND budget remains.
    if not want_transcript:
        rec["skip_transcript"] = True
        return rec
    if budget["n"] >= CALL_BUDGET:
        rec["error"] = (rec["error"] + "; " if rec["error"] else "") + "transcript: budget exhausted"
        return rec
    cid2 = budget["n"] + 300   # independent call ids per phase
    td = _latest_transcript_date(sid, ticker, cid2, budget)
    if td and budget["n"] < CALL_BUDGET:
        q, y = td.get("quarter"), td.get("fiscalYear")
        text, tdate = _pull_transcript_text(sid, ticker, q, y, cid2 + 1, budget)
        rec["transcript_date"] = td.get("date") or tdate
        if text:
            stmts = extract_forward_statements(text)
            rec["forward_statements"] = stmts
            rec["design_win"] = any(s["design_win"] for s in stmts)
        else:
            rec["error"] = (rec["error"] + "; " if rec["error"] else "") + "no transcript text found"
    elif td is None:
        rec["error"] = (rec["error"] + "; " if rec["error"] else "") + "no transcript dates returned"
    return rec


def main(tickers=None):
    """Two-pass scan over the tech-gap filler universe.

    Pass 1 (R&D sweep): one income-statement call per gap filler -> the
    R&D-trending signal (15 pts) lands on EVERY gap filler, not 4 legacy tickers.
    Pass 2 (design-win): with leftover budget, spend the 2-call transcript pull
    on the most-advanced-stage gap fillers (the ones likeliest to clear 50).
    An explicit `tickers` arg (CLI) overrides the wiki universe (for testing).
    """
    if tickers:
        ordered = {t: "unknown" for t in tickers}
    else:
        ordered = gap_filler_universe()
        if not ordered:                       # wiki missing -> legacy fallback
            ordered = {t: "unknown" for t in DEFAULT_TICKERS}
        # most-advanced stage first (best chance of a clear-50 design win)
        ordered = dict(sorted(ordered.items(), key=lambda kv: -STAGE_ORDER.get(kv[1], 0)))

    sid = fmp_session()
    if not sid:
        raise SystemExit("could not open FMP MCP session")
    budget = {"n": 0}
    out = {}
    tickers_list = list(ordered.keys())

    # ---- Pass 1: R&D for every gap filler (1 call each) ----
    for i, t in enumerate(tickers_list):
        rec = analyze(sid, t, 10 + i, budget, want_transcript=False)
        out[t] = rec
    # ---- Pass 2: design-win for top-priority tickers while budget allows ----
    # 2 transcript calls each, most-advanced stage first. Ticker keeps its
    # pass-1 R&D either way; if budget runs out we simply stop (no error tag).
    for i, t in enumerate(tickers_list):
        if budget["n"] >= CALL_BUDGET - 2:
            break
        rec = out[t]
        base = 200 + i
        td = _latest_transcript_date(sid, t, base, budget)
        if td and budget["n"] < CALL_BUDGET - 1:
            q, y = td.get("quarter"), td.get("fiscalYear")
            text, tdate = _pull_transcript_text(sid, t, q, y, base + 1, budget)
            rec["transcript_date"] = td.get("date") or tdate
            rec.pop("skip_transcript", None)
            if text:
                stmts = extract_forward_statements(text)
                rec["forward_statements"] = stmts
                rec["design_win"] = any(s["design_win"] for s in stmts)

    with open(OUT_PATH, "w") as f:
        json.dump(out, f, indent=2)
    print(f"saved {OUT_PATH}  fmp_calls={budget['n']}  tickers={len(out)}")
    for t, r in out.items():
        print(f"{t}: rd%={r['rd_revenue_pct_by_qtr']} up={r['rd_trending_up']} "
              f"design_win={r['design_win']} stmts={len(r['forward_statements'])} "
              f"date={r['transcript_date']} err={r['error']}")
    return out


if __name__ == "__main__":
    main(sys.argv[1:] or None)
