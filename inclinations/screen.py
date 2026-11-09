#!/usr/bin/env python3
"""Deterministic screen for NEW early-advantage US ticker candidates.

Fix (2026-10-17): the old screen returned the LARGEST small-caps in range
(Hasbro, Penske) because it paged one unfiltered market-cap query and took
whatever came first. Now it screens the TARGET SECTORS only (persona pillars),
hard 12-call budget, dedup across buckets AND pages, max 8 candidates/bucket.

FMP screener quirks CONFIRMED live (2026-10-17):
  * industry="Semiconductors", "Computer Hardware", "Regulated Electric",
    "Independent Power Producers" filters WORK server-side.
  * industry="Aerospace & Defense" / "Electrical Equipment & Parts" SILENTLY
    RETURN 0 ROWS (ampersand strings break the filter) -> those are filtered
    CLIENT-SIDE by paging the parent sector and keeping allow-listed rows.
  * rows REPEAT across pages (same symbol, page+1) -> seen-set dedup mandatory.
  * `price` can be str or float -> coerce in _norm_rec.

Candidate universe (US, actively trading, no ETFs/funds, $50M-$15B cap):
  1. Technology / Semiconductors            (pure-play chip designers)
  2. Technology / Computer Hardware         (optical / thermal / AI infra HW)
  3. Industrials -> {Aerospace & Defense, Electrical Equipment & Parts}
  4. Utilities / Regulated Electric         (grid)
  5. Utilities / Independent Power Producers (power gen, incl. OKLO-class)

Output: screen_out.json {'candidates': [...], 'fmp_calls_made': N, 'buckets': [...]}
"""
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fmp import fmp_session, fmp_call  # noqa: E402

OUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "screen_out.json")
MIN_CAP, MAX_CAP = 50_000_000, 15_000_000_000
PAGE_SIZE = 25
MAX_FMP_CALLS = 12          # session + 10 screener calls, hard cap
MAX_PER_BUCKET = 8
MAX_TOTAL = 40
PAGES_PER_BUCKET = 2       # 5 buckets x 2 = exactly 10 screener calls
SYM_RE = re.compile(r"^[A-Z][A-Z0-9]{0,6}$")          # plain US common shares only
JUNK_NAME_RE = re.compile(
    r"\b(warrants?|preferred|rights|units|depositary|exchangeable|depository)\b", re.I)
NAME_BLACKLIST = re.compile(
    r"\b(gaming|game\b|toy|toys|3d printing|3d printer|craft|cricut|hobby|entertainment)\b", re.I)


def _f(x, d=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return d


def _norm_rec(r):
    """Flatten one screener row; None if out of range / foreign listing / bad."""
    try:
        sym = r.get("symbol")
        name = r.get("companyName") or r.get("name")
        cap = _f(r.get("marketCap"))
        if not sym or not name or cap <= 0:
            return None
        if not SYM_RE.match(sym):   # kills .DE/.L foreigns, MOG-A preferreds, XRXDW
            return None
        if JUNK_NAME_RE.search(name) or NAME_BLACKLIST.search(name):
            return None
        if not (MIN_CAP <= cap <= MAX_CAP):
            return None
        return {
            "ticker": sym,
            "name": name,
            "sector": r.get("sector") or "",
            "industry": r.get("industry") or "",
            "market_cap": cap,
            "price": _f(r.get("price")),
        }
    except Exception:
        return None


def _screener_page(sid, args, page, budget):
    a = dict(args)
    a.update({"marketCapMoreThan": MIN_CAP, "marketCapLowerThan": MAX_CAP,
              "isEtf": False, "isFund": False, "isActivelyTrading": True,
              "country": "US", "limit": PAGE_SIZE, "page": page})
    res = fmp_call(sid, "search", {"endpoint": "search-company-screener", **a},
                   call_id=100 + budget["n"])
    budget["n"] += 1
    if isinstance(res, dict):
        err = res.get("_error") or (res.get("message") if res.get("status") == "not_found" else None)
        if err:
            return None, "error: %s" % str(err)[:160]
        rows = res.get("data")
    else:
        rows = res
    if not isinstance(rows, list):
        return None, "unexpected shape: %r" % str(res)[:160]
    return rows, None


def run():
    """Bucket plan (label, server-side args, client-side industry allow-list, pages).
    allow=None -> keep every row (server industry filter already applied).
    """
    # Fixed allocation: 5 buckets x 2 pages = 10 calls (session + 10 = 12).
    # Every bucket is guaranteed its slot; no dynamic reservation math.
    BUCKETS = [
        ("Semiconductors",
         {"sector": "Technology", "industry": "Semiconductors"}, None),
        ("AI/Data-Center Hardware",
         {"sector": "Technology", "industry": "Computer Hardware"}, None),
        ("Defense/Electrical Hardware",
         {"sector": "Industrials"},
         {"Aerospace & Defense", "Electrical Equipment & Parts"}),
        ("Grid (Regulated Electric)",
         {"sector": "Utilities", "industry": "Regulated Electric"}, None),
        ("Power Generation (IPP)",
         {"sector": "Utilities", "industry": "Independent Power Producers"}, None),
    ]

    sid = fmp_session()
    budget = {"n": 1}            # session call
    seen = set()
    candidates = []
    log = []
    for label, args, allow in BUCKETS:
        if budget["n"] >= MAX_FMP_CALLS or len(candidates) >= MAX_TOTAL:
            log.append("%s: skipped (budget/cap)" % label)
            continue
        for pg in range(PAGES_PER_BUCKET):
            if budget["n"] >= MAX_FMP_CALLS:
                break
            if sum(1 for x in candidates if x["bucket"] == label) >= MAX_PER_BUCKET:
                break
            rows, err = _screener_page(sid, args, pg, budget)
            if err:
                log.append("%s page %d: %s" % (label, pg, err))
                break
            new_on_page = 0
            for r in rows:
                if allow and r.get("industry") not in allow:
                    continue
                if sum(1 for x in candidates if x["bucket"] == label) >= MAX_PER_BUCKET:
                    break
                c = _norm_rec(r)
                if c and c["ticker"] not in seen:
                    seen.add(c["ticker"])
                    c["bucket"] = label
                    c["why_flagged"] = (
                        "target-sector small-cap (${:,.0f}M mktcap) in {} — "
                        "below news-coverage radar".format(c["market_cap"] / 1e6, label))
                    candidates.append(c)
                    new_on_page += 1
            if len(rows) < PAGE_SIZE:
                break            # exhausted or empty page
            if new_on_page == 0:
                break            # page produced nothing new (duplicate rows)
        log.append("%s: %d candidates" % (label, sum(1 for x in candidates if x["bucket"] == label)))

    candidates.sort(key=lambda c: c["market_cap"])
    return candidates[:MAX_TOTAL], budget["n"], log


def main():
    candidates, calls, log = run()
    for line in log:
        print("bucket: %s" % line)
    print("\nTOP (smallest caps first):")
    for i, c in enumerate(candidates[:12]):
        print("  #%02d %s (%s) | %s | $%.2f | $%.1fM" % (
            i + 1, c["ticker"], c["name"], c["bucket"], c["price"], c["market_cap"] / 1e6))
    payload = {"candidates": candidates, "fmp_calls_made": calls, "buckets": log}
    with open(OUT_PATH, "w") as f:
        json.dump(payload, f, indent=2)
    print("\nTOTAL: %d candidates, %d FMP calls" % (len(candidates), calls))
    print("saved -> %s" % OUT_PATH)
    print("CANDIDATES_JSON:" + json.dumps(candidates))


if __name__ == "__main__":
    main()
