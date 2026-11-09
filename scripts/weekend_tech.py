#!/usr/bin/env python3
"""weekend_tech.py — Phase 2: kill the 90% weekend-NULL bug (2026-11-07).

The deep-dive engine computes RSI/SMA/RVOL from FMP technicalIndicators
(_technical_signals in pipeline_engine.py). On weekends that feed returns
Friday-only or null rows and technical_100/valuation_100 come back ~90% NULL.

Fix: build the same {bars, rsi14, sma20, rvol, pct_14d, pct_5d, last_close,
as_of} block from Market Data:
  * /v1/stocks/bulkcandles/D?symbols=...  (last daily bar, fresh each run)
  * /v1/stocks/quotes?symbols=...         (last price/volume, weekend-safe)
  * a LOCAL rolling daily cache
    (~/.hermes/cron/marketdata_candles_cache.json) that appends one bar per
    symbol per calendar day, so the series GROWS across runs and 30+ bars of
    history accumulate even though the upstream only ever returns the last bar.
Formulas are byte-for-byte the pipeline's own (Wilder RSI-14, trailing-30 RVOL,
pct windows) so the block is drop-in equivalent for the LLM rubric.

Output: ~/.hermes/cron/weekend_technicals.json
  {as_of, generated, source: "marketdata", tickers: {SYM: block}}
Idempotent: re-running the same day never duplicates bars.
"""
import datetime
import json
import os
import sys

sys.path.insert(0, os.path.expanduser("~/.hermes/scripts"))
import marketdata_client as md

HERE = os.path.expanduser("~/.hermes/cron")
CACHE = os.path.join(HERE, "marketdata_candles_cache.json")
OUT = os.path.join(HERE, "weekend_technicals.json")
VETTED = os.path.join(HERE, "vetted_tickers.json")
SCORES = os.path.join(HERE, "ticker_scores.json")
BARS_KEEP = 60          # rolling window kept per symbol in cache
MIN_BARS_RSI = 15       # Wilder needs period+1


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


def ticker_universe():
    v = read_json(VETTED, {})
    ts = [t.get("ticker") for t in v.get("all_tickers", []) if t.get("ticker")]
    if len(ts) >= 5:
        return sorted(set(ts))
    d = read_json(SCORES, {})
    rows = d.get("rankings", []) if isinstance(d, dict) else []
    return sorted({r["ticker"] for r in rows if r.get("ticker")})


def update_cache(cache, symbols, today):
    """Fetch last bar (bulk) + last quote per symbol; append one bar per symbol
    for today if today's bar is not already cached. Returns (n_updated, source)."""
    token = md.load_token()
    updated = 0
    bulk = md.bulk_candles_daily(symbols, token=token) or {}
    quotes = md.quotes(symbols, token=token) or {}
    for s in symbols:
        b = cache.get(s) or {"symbol": s, "bars": []}
        bars = b["bars"]
        have_today = bool(bars) and bars[-1].get("date") == today
        # prefer the bulk candle (full OHLCV); fall back to quote last/volume
        rec = None
        if s in bulk and bulk[s].get("close") is not None:
            rec = {"date": bulk[s].get("date") or today,
                   "close": float(bulk[s]["close"]),
                   "volume": bulk[s].get("volume")}
            src = "bulk"
        elif s in quotes and quotes[s].get("last") is not None:
            rec = {"date": today, "close": float(quotes[s]["last"]),
                   "volume": quotes[s].get("volume")}
            src = "quote"
        else:
            continue
        if not have_today and (not bars or rec["date"] > bars[-1]["date"]):
            bars.append(rec)
            updated += 1
        elif have_today and rec.get("volume") is not None and bars[-1].get("volume") is None:
            bars[-1]["volume"] = rec["volume"]  # fill missing volume only
        b["bars"] = bars[-BARS_KEEP:]
        cache[s] = b
    return updated


def wilder_rsi(closes, period=14):
    """Mirror of pipeline_engine._wilder_rsi (newest-last series)."""
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)


def tech_block(bars):
    """Same semantics as pipeline_engine._technical_signals + sma20.
    bars: newest-LAST list of {date, close, volume}."""
    rows = [b for b in bars if b.get("close") is not None and float(b["close"]) > 0]
    if len(rows) < 5:
        return {"bars": len(rows), "note": "insufficient_bars"}
    closes = [float(b["close"]) for b in rows]
    vols = [float(b.get("volume") or 0) for b in rows]
    rsi14 = wilder_rsi(closes)
    rvol = None
    if vols[-1] > 0 and len(vols) >= 20:
        avg30 = sum(vols[-31:-1]) / max(1, min(30, len(vols) - 1))
        if avg30 > 0:
            rvol = round(vols[-1] / avg30, 2)
    sma20 = round(sum(closes[-20:]) / min(20, len(closes)), 2) if len(closes) >= 20 else None
    px14 = round((closes[-1] / closes[-15] - 1) * 100, 1) if len(closes) >= 15 else None
    px5 = round((closes[-1] / closes[-6] - 1) * 100, 1) if len(closes) >= 6 else None
    return {"bars": len(rows),
            "rsi14": round(rsi14, 1) if rsi14 is not None else None,
            "sma20": sma20,
            "rvol": rvol,
            "pct_14d": px14, "pct_5d": px5,
            "last_close": closes[-1],
            "as_of": str(rows[-1].get("date", ""))[:10],
            "source": "marketdata",
            "note": "weekend_fill" if len(rows) < 30 else "rolling_cache"}


def main():
    today = datetime.date.today().isoformat()
    tickers = ticker_universe()
    if not tickers:
        print("weekend_tech: no tickers found in vetted_tickers.json or "
              "ticker_scores.json — abort")
        return 1
    cache = read_json(CACHE, {})
    updated = update_cache(cache, tickers, today)
    atomic_write_json(CACHE, cache)

    prev = read_json(OUT, {})
    prev_rates = None
    if prev.get("tickers"):
        pv = list(prev["tickers"].values())
        n = len(pv)
        prev_rates = {"n": n,
                      "rsi14_null_pct": round(100 * sum(1 for b in pv
                                                        if b.get("rsi14") is None) / n, 1),
                      "rvol_null_pct": round(100 * sum(1 for b in pv
                                                        if b.get("rvol") is None) / n, 1)}

    out = {}
    for t in tickers:
        out[t] = tech_block((cache.get(t) or {}).get("bars", []))
    n = len(out)
    rsi_null = sum(1 for b in out.values() if b.get("rsi14") is None)
    rvol_null = sum(1 for b in out.values() if b.get("rvol") is None)
    sma_null = sum(1 for b in out.values() if b.get("sma20") is None)
    atomic_write_json(OUT, {"as_of": today,
                            "generated": datetime.datetime.utcnow().isoformat(),
                            "source": "marketdata",
                            "tickers": out})
    print(f"weekend_tech: {n} tickers, cache bars updated={updated} (as_of {today})")
    print(f"  rsi14 NULL: {rsi_null}/{n} = {100*rsi_null/max(n,1):.1f}% | "
          f"rvol NULL: {rvol_null}/{n} = {100*rvol_null/max(n,1):.1f}% | "
          f"sma20 NULL: {sma_null}/{n} = {100*sma_null/max(n,1):.1f}%")
    if prev_rates:
        print(f"  previous run: {prev_rates}")
    deep = {t: out[t] for t in list(out)[:3]}
    print("  sample:", json.dumps(deep))
    return 0


if __name__ == "__main__":
    sys.exit(main())
