#!/usr/bin/env python3
"""weekend_tech_seed.py — ONE-TIME seed of the Market Data rolling cache.

weekend_tech.py accumulates one bar per symbol per day from the Market Data
last-bar endpoints, but the cache starts empty — RSI/RVOL/SMA20 need 20-30
bars of history before they can compute. This seeder backfills the cache
ONCE from FMP MCP historical-price-eod-full (real bars, real dates — no
synthetic data) for the tracked universe, then weekend_tech.py takes over
from today (it appends its Market Data bars after the seed, same schema).

Usage: python3 weekend_tech_seed.py [N]   (N = bars, default 40)
Idempotency rule: if a symbol already has >= N/2 cached bars, it is SKIPPED
(the accumulator is working; seeding would truncate its tail).
"""
import datetime
import json
import os
import sys

HERE = os.path.expanduser("~/.hermes/cron")
sys.path.insert(0, os.path.expanduser("~/.hermes/scripts"))
sys.path.insert(0, HERE)  # cron dir for fmp_client
CACHE = os.path.join(HERE, "marketdata_candles_cache.json")
SCORES = os.path.join(HERE, "ticker_scores.json")
BARS_KEEP = 60
FMP_SCRIPT = os.path.expanduser("~/.hermes/scripts/fmp_client.py")


def read_json(path, default):
    try:
        with open(path) as fh:
            return json.load(fh)
    except Exception:
        return default


def fmp_eod(symbol, limit):
    """Fetch daily EOD bars via the FMP MCP script client (stdlib).
    Returns oldest->newest list of {date, close, volume}."""
    import subprocess
    from datetime import date, timedelta
    to = date.today().isoformat()
    frm = (date.today() - timedelta(days=int(limit * 2.2) + 10)).isoformat()
    code = (
        "import sys, json\n"
        f"sys.path.insert(0, {os.path.dirname(FMP_SCRIPT)!r})\n"
        "from fmp_client import FMP\n"
        "f = FMP()\n"
        "with f:\n"
        "    r = f.parse('chart', endpoint='historical-price-eod-full', "
        "symbol=__import__('sys').argv[1], from_date=__import__('sys').argv[2], "
        "to_date=__import__('sys').argv[3])\n"
        "json.dump(r, sys.stdout)\n"
    )
    p = subprocess.run(["python3", "-c", code, symbol, frm, to],
                       capture_output=True, text=True, timeout=120)
    try:
        rows = json.loads(p.stdout)
    except Exception:
        return None
    if isinstance(rows, dict):
        rows = rows.get("data") or rows.get("result") or []
    if not isinstance(rows, list):
        return None
    out = []
    for r in rows:
        if not isinstance(r, dict) or r.get("close") in (None, 0):
            continue
        d = str(r.get("date") or "")[:10]
        if not d:
            continue
        out.append({"date": d, "close": float(r["close"]),
                    "volume": r.get("volume")})
    out.sort(key=lambda x: x["date"])  # oldest -> newest (FMP is newest-first)
    return out[-limit:]


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 40
    d = read_json(SCORES, {})
    rows = d.get("rankings", []) if isinstance(d, dict) else []
    tickers = sorted({r["ticker"] for r in rows if r.get("ticker")})
    cache = read_json(CACHE, {})
    seeded, skipped, failed = 0, 0, []
    for i, t in enumerate(tickers):
        cur = (cache.get(t) or {}).get("bars", [])
        if len(cur) >= n // 2:
            skipped += 1
            continue
        bars = fmp_eod(t, n)
        if not bars or len(bars) < 10:
            failed.append(t)
            continue
        # merge: seed bars first, then whatever Market Data already appended
        have = {b["date"] for b in bars}
        merged = bars + [b for b in cur if b.get("date") not in have]
        merged.sort(key=lambda b: b.get("date") or "")
        cache[t] = {"symbol": t, "bars": merged[-BARS_KEEP:]}
        seeded += 1
        if (i + 1) % 5 == 0:
            print(f"  {i+1}/{len(tickers)} seeded so far", file=sys.stderr)
    tmp = CACHE + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(cache, fh)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, CACHE)
    print(f"seed: {seeded} seeded, {skipped} already-accumulated (skipped), "
          f"{len(failed)} failed {failed[:10] if failed else ''}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
