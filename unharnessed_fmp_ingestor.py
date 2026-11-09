#!/usr/bin/env python3
"""unharnessed_fmp_ingestor.py — Agent 1 (Harvester-Prime) + Agent 2 (Quant-Gatekeeper).

Per ~/.hermes/cron/jane_street_braid_spec.md (V3 FINAL), Section "IMPLEMENTATION
DELIVERABLES" item 1. Runs OUTSIDE the Hermes harness (tmux/overnight) so it is
not bounded by the 10-min cron timeout or the 1-MCP-call-per-turn cap.

Stage 1 — Open-Loop Faucets:
  - Pull tickers from universe_cache.db `universe` (pillar-assigned names),
    resumable via `ORDER BY last_updated ASC NULLS FIRST LIMIT 100`-style
    chunking over `alpha_vectors.last_updated`.
  - 15 FMP endpoints per ticker via ~/.hermes/scripts/fmp_client.py
    (Streamable-HTTP MCP; /stable/ REST fallback is NOT used — fmp_client is
    the script-reachable transport, memory 2026-10-26).

Stage 2 — 29-Column Vector Enrichment:
  - Upsert `alpha_vectors` (INSERT OR REPLACE, 100-row chunked transactions).
  - Upsert `regime_state` per pillar (benchmarks, COT crowding, macro pulse).
  - Append primary wire / insider / transcript items to feed_raw.json
    (atomic, via fmp_client.atomic_write_json).

Stage 3 — Quant-Gatekeeper deterministic pre-LLM scoring:
  - 4 pillars (financial/valuation/technical/catalyst, 0-100 each) from the
    spec math contract, written to `unharnessed_scores.json` via
    fmp_client.atomic_write_json (the ingestor's handoff bus for the
    recalibrator; the CANONICAL ticker_scores.json is written by the
    recalibrator AFTER red-team kill overrides).

Data-gap discipline (skill fmp-data-exhaustion): missing deep endpoints stay
NULL in alpha_vectors — "data unavailable" is never scored as zero value.
data_quality_score is penalized per missing core fundamental.

Usage:
  python3 unharnessed_fmp_ingestor.py                 # 100-ticker chunk
  python3 unharnessed_frowing.py --limit 5            # smoke test
  python3 unharnessed_fmp_ingestor.py --tickers CEG,APH,CEG --limit 5
  python3 unharnessed_fmp_ingestor.py --no-score      # harvest only

Exit codes: 0 = chunk complete (full universe may remain), 2 = fatal.
"""
import argparse
import asyncio
import datetime
import json
import os
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, wait as _fut_wait
from pathlib import Path

CRON = os.path.expanduser("~/.hermes/cron")
SCRIPTS = os.path.expanduser("~/.hermes/scripts")
# --- Mandatory path invariants (clean-slate standardization 2026-11-05) ---
WIKI_ROOT = Path("/home/jaren/wiki").resolve(strict=True)
assert not (WIKI_ROOT / "wiki").exists(), "FATAL: Nested wiki directory detected."
COMPANIES_DIR = WIKI_ROOT / "companies"
assert COMPANIES_DIR.is_dir() and not COMPANIES_DIR.is_symlink(), \
    "FATAL: companies/ missing or symlink."
# --- end invariants ---
sys.path.insert(0, SCRIPTS)
import fmp_client  # noqa: E402  (FMP, atomic_write_json, bus_append_entries)

DB = os.path.join(CRON, "universe_cache.db")
SCORES_BUS = os.path.join(CRON, "unharnessed_scores.json")
BRIEFS = os.path.join(CRON, "signal_briefs.json")
FEED = os.path.join(CRON, "feed_raw.json")

MCAP_FLOOR = 500_000_000        # AGENTS.md rule 6 / alpha_vectors DDL
MCAP_CEILING = 300_000_000_000
BAND_FLAG = 200_000_000_000     # 200-300B: allowed + flagged near_ceiling

# Pillar -> benchmark ETF/index (EP-14 relative strength + regime_state)
PILLAR_BENCHMARK = {
    "SEMI_SUBSYSTEMS": "SMH",
    "ENERGY_GRID_SMR": "URA",
    "PHYSICAL_AI_ROBOTICS": "BOTZ",
    "AI_INFRA_THERMAL": "SMH",
    "DEFENSE_GOV": "SPY",
    "QUANTUM": "SPY",
}
DEFAULT_BENCH = "SPY"

# ETF -> sector pillar (matches the 4-sector ETF rotation, 2026-11-05)
ETF_PILLAR = {
    "SMH": "SEMI_SUBSYSTEMS", "XSD": "SEMI_SUBSYSTEMS",
    "URA": "ENERGY_GRID_SMR", "URNJ": "ENERGY_GRID_SMR",
    "BOTZ": "PHYSICAL_AI_ROBOTICS", "ROBO": "PHYSICAL_AI_ROBOTICS",
    "PPA": "AI_INFRA_THERMAL", "SHLD": "AI_INFRA_THERMAL",
}


def pull_etf_holdings(db, f, etf_list):
    """--etf incremental target: for each fund, pull holdings via FMP
    etfAndMutualFunds, upsert plain-equity tickers into universe (idempotent —
    existing rows keep their pillar; only gaps get filled), and return the
    (ticker, name, pillar) harvest set for this run.

    Band gate stays downstream in main (mcap verified at E1/E2), so no ETF
    row is dropped here on data grounds — only structurally (ETF/SPAC/fund
    name filter + symbol shape).
    """
    sel = []
    seen = set()
    for etf in etf_list:
        pillar = ETF_PILLAR.get(etf.upper())
        if pillar is None:
            log(f"  --etf {etf}: no pillar map — skipped (add to ETF_PILLAR)")
            continue
        try:
            rows = f.parse_with_rest_fallback("etfAndMutualFunds",
                                              endpoint="holdings", symbol=etf.upper())
        except Exception as e:
            log(f"  --etf {etf} holdings ERR {e}")
            continue
        if not isinstance(rows, list):
            log(f"  --etf {etf}: holdings payload not a list ({type(rows).__name__})")
            continue
        added = 0
        for r in rows:
            if not isinstance(r, dict):
                continue
            # FMP holdings shape: `symbol` = the FUND's own ticker on every row,
            # `asset` = the actual holding (verified 2026-11-05: PPA/XSD).
            sym = str(r.get("asset") or r.get("symbol") or "").strip().upper()
            name = r.get("name") or ""
            if sym == etf.upper():
                continue  # row 0 of FMP holdings = the fund itself
            if not (1 <= len(sym) <= 5) or any(c in sym for c in " -./"):
                continue  # drop weird tickers (wrappers, delisted markers)
            if any(k in name.upper() for k in ("ETF", "ETP", "FUND", "ACQUISITION CORP", "SPAC")):
                continue
            if sym in seen:
                continue
            seen.add(sym)
            db.execute(
                "INSERT INTO universe (ticker, name, pillar) VALUES (?,?,?) "
                "ON CONFLICT(ticker) DO NOTHING", (sym, name or None, pillar))
            added += 1
            sel.append((sym, name or None, pillar))
        db.commit()
        log(f"  --etf {etf}: {added} holdings -> universe ({pillar})")
    if not sel:
        log("  --etf: no harvest set (all funds empty/err) — nothing to do")
    return sel

CHUNK = 100                     # spec: 100-row chunked transactions
NOW_EPOCH = int(time.time())
NOW_ISO = datetime.datetime.now(datetime.timezone.utc).isoformat()

PILLAR_COLS = [
    "ticker", "cik", "company_name", "sector_pillar", "mcap", "price",
    "float_shares", "free_float_pct", "float_turnover_5d",
    "altman_z", "piotroski_f",
    "dcf_standard", "dcf_levered", "pt_consensus", "analyst_count",
    "blended_upside_pct",
    "rev_est_next_yr", "eps_revision_delta_30d",
    "trojan_segment_flag", "trojan_segment_name", "trojan_segment_rev_pct",
    "trojan_segment_growth_yoy",
    "adx_14", "atr_pct", "mfi_14", "macd_hist_slope",
    "rs_20d_vs_pillar",
    "data_quality_score", "last_updated",
]


def log(msg):
    print(f"[ingestor {datetime.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def num(v):
    try:
        if v is None:
            return None
        f = float(v)
        if f != f or f in (float("inf"), float("-inf")):
            return None
        return f
    except (TypeError, ValueError):
        return None


def first_row(payload):
    """FMP per-symbol endpoints return list[1]; error-dict status -> None."""
    if isinstance(payload, list):
        return payload[0] if payload else None
    if isinstance(payload, dict):
        if str(payload.get("status", "")).startswith("not_found"):
            return None
        return payload
    return None


# ---------------------------------------------------------------------------
# Native technicals (no pandas_ta — spec rule). FMP has NO MFI/MACD/ATR
# endpoints (probed live 2026-11-04: all error out), so compute from the free
# RSI endpoint series (25+ daily OHLCV bars) + historical-price-eod-full.
# ---------------------------------------------------------------------------
def compute_technicals(bars):
    """bars: ascending [{date,open,high,low,close,volume},...] (>= ~40 needed
    for full MFI/ADX quality; degrades gracefully). Returns dict or {} on error."""
    try:
        bars = [b for b in bars if num(b.get("close")) and num(b.get("close")) > 0]
        if len(bars) < 20:
            return {}
        closes = [num(b["close"]) for b in bars]
        highs = [num(b["high"]) for b in bars]
        lows = [num(b["low"]) for b in bars]
        vols = [num(b.get("volume")) or 0.0 for b in bars]
        price = closes[-1]
        out = {}

        # --- MFI(14): typical-price money flow, standard 14-period ratio ---
        if len(closes) >= 15:
            tp = [(highs[i] + lows[i] + closes[i]) / 3.0 for i in range(len(closes))]
            mf = [tp[i] * vols[i] for i in range(len(closes))]
            w = 14
            pos = neg = 0.0
            for i in range(len(tp) - w, len(tp)):
                if tp[i] > tp[i - 1]:
                    pos += mf[i]
                elif tp[i] < tp[i - 1]:
                    neg += mf[i]
            if neg > 0:
                out["mfi_14"] = round(100.0 - 100.0 / (1.0 + pos / neg), 4)
            elif pos > 0:
                out["mfi_14"] = 100.0

        # --- ATR(14) % of price ---
        if len(closes) >= 15:
            trs = []
            for i in range(len(closes) - 14, len(closes)):
                trs.append(max(highs[i] - lows[i],
                               abs(highs[i] - closes[i - 1]),
                               abs(lows[i] - closes[i - 1])))
            out["atr_pct"] = round(sum(trs) / 14.0 / price * 100.0, 4)

        # --- MACD(12,26,9) histogram slope (last 5 hist points) ---
        def ema(vals, period):
            k = 2.0 / (period + 1)
            e = vals[0]
            out = [e]
            for v in vals[1:]:
                e = v * k + e * (1 - k)
                out.append(e)
            return out
        if len(closes) >= 35:
            e12 = ema(closes, 12)
            e26 = ema(closes, 26)
            macd = [a - b for a, b in zip(e12, e26)]
            sig = ema(macd, 9)
            hist = [a - b for a, b in zip(macd, sig)]
            out["macd_hist_slope"] = round(hist[-1] - hist[-5], 6)

        # --- ADX(14): Wilder smoothing on +DI/-DI ---
        if len(closes) >= 30:
            pdm = mdm = trs_all = []
            for i in range(1, len(closes)):
                up = highs[i] - highs[i - 1]
                dn = lows[i - 1] - lows[i]
                pdm.append(up if (up > dn and up > 0) else 0.0)
                mdm.append(dn if (dn > up and dn > 0) else 0.0)
                trs_all.append(max(highs[i] - lows[i],
                                   abs(highs[i] - closes[i - 1]),
                                   abs(lows[i] - closes[i - 1])))
            n = 14
            trs_s = sum(trs_all[-n:])
            ps = sum(pdm[-n:])
            ms = sum(mdm[-n:])
            adx_vals = []
            for i in range(len(trs_all) - n, len(trs_all)):
                if i > n:
                    trs_s = trs_s - trs_s / n + trs_all[i]
                    ps = ps - ps / n + pdm[i]
                    ms = ms - ms / n + mdm[i]
                if trs_s <= 0:
                    continue
                pdi = 100.0 * ps / trs_s
                mdi = 100.0 * ms / trs_s
                if pdi + mdi <= 0:
                    continue
                adx_vals.append(100.0 * abs(pdi - mdi) / (pdi + mdi))
            if adx_vals:
                out["adx_14"] = round(sum(adx_vals[-14:]) / min(14, len(adx_vals)), 4)
        return out
    except Exception:
        return {}


def pct_ret(closes_ascending, n=20):
    if len(closes_ascending) < n + 1:
        return None
    a, b = closes_ascending[-(n + 1)], closes_ascending[-1]
    if not a:
        return None
    return (b / a - 1.0) * 100.0


# ---------------------------------------------------------------------------
# FMP fetch plan (15 endpoints per ticker, one session for the whole chunk)
# ---------------------------------------------------------------------------
def fetch_vector(f, tk, name, pillar):
    v = {c: None for c in PILLAR_COLS}
    v["ticker"] = tk
    v["company_name"] = name
    v["sector_pillar"] = pillar
    dq = 100
    got = set()

    # E1 profile-symbol (authoritative: cik, name, mcap) — fmp_client camelCase
    try:
        r = first_row(f.parse_with_rest_fallback("company", endpoint="profile-symbol", symbol=tk))
        if r:
            v["cik"] = r.get("cik")
            if r.get("companyName"):
                v["company_name"] = r["companyName"]
            v["mcap"] = num(r.get("marketCap"))
            got.add("profile")
    except Exception as e:
        log(f"  {tk} profile-symbol ERR {e}")

    # E2 quote (price, volume, mcap fallback)
    try:
        q = first_row(f.parse_with_rest_fallback("quote", endpoint="quote", symbol=tk))
        if q:
            v["price"] = num(q.get("price"))
            if v["mcap"] is None:
                v["mcap"] = num(q.get("marketCap"))
            got.add("quote")
    except Exception as e:
        log(f"  {tk} quote ERR {e}")

    # E3 shares-float (EP-5)
    try:
        r = first_row(f.parse_with_rest_fallback("company", endpoint="shares-float", symbol=tk))
        if r:
            v["float_shares"] = num(r.get("floatShares"))
            v["free_float_pct"] = num(r.get("freeFloat"))
            got.add("float")
    except Exception as e:
        log(f"  {tk} shares-float ERR {e}")

    # E4 financial-scores (EP-6: altmanZScore + piotroskiScore in one call)
    try:
        r = first_row(f.parse_with_rest_fallback("statements", endpoint="financial-scores", symbol=tk))
        if r:
            v["altman_z"] = num(r.get("altmanZScore"))
            v["piotroski_f"] = int(r["piotroskiScore"]) if r.get("piotroskiScore") is not None else None
            got.add("scores")
    except Exception as e:
        log(f"  {tk} financial-scores ERR {e}")

    # E5/E6 DCF standard + levered (EP-7)
    try:
        r = first_row(f.parse_with_rest_fallback("discountedCashFlow", endpoint="dcf-advanced", symbol=tk))
        if r:
            v["dcf_standard"] = num(r.get("dcf"))
            got.add("dcf")
    except Exception as e:
        log(f"  {tk} dcf-advanced ERR {e}")
    try:
        r = first_row(f.parse_with_rest_fallback("discountedCashFlow", endpoint="dcf-levered", symbol=tk))
        if r:
            v["dcf_levered"] = num(r.get("dcf"))
    except Exception as e:
        log(f"  {tk} dcf-levered ERR {e}")

    # E7 price-target-consensus (EP-8)
    try:
        r = first_row(f.parse_with_rest_fallback("analyst", endpoint="price-target-consensus", symbol=tk))
        if r:
            v["pt_consensus"] = num(r.get("targetConsensus"))
    except Exception as e:
        log(f"  {tk} pt-consensus ERR {e}")

    # E8 grades-summary (analyst_count = sum of grade buckets)
    try:
        r = first_row(f.parse_with_rest_fallback("analyst", endpoint="grades-summary", symbol=tk))
        if r:
            v["analyst_count"] = int(sum(num(r.get(k)) or 0 for k in
                                         ("strongBuy", "buy", "hold", "sell", "strongSell")))
            got.add("grades")
    except Exception as e:
        log(f"  {tk} grades-summary ERR {e}")

    # E9 financial-estimates annual (EP-4: rev est next year + 30d EPS revision)
    try:
        rows = f.parse_with_rest_fallback("analyst", endpoint="financial-estimates", symbol=tk, period="annual")
        if isinstance(rows, list) and rows:
            rows = sorted(rows, key=lambda x: str(x.get("date", "")))
            if len(rows) >= 2 and rows[1].get("date", "").startswith("20"):
                v["rev_est_next_yr"] = num(rows[1].get("revenueAvg"))
            eps_now = num(rows[-2].get("epsAvg")) if len(rows) >= 2 else None
            eps_prev = num(rows[-3].get("epsAvg")) if len(rows) >= 3 else None
            if eps_now and eps_prev:
                v["eps_revision_delta_30d"] = round(eps_now - eps_prev, 4)
            got.add("estimates")
    except Exception as e:
        log(f"  {tk} financial-estimates ERR {e}")

    # E10 revenue-product-segmentation (EP-10 Trojan Horse)
    try:
        rows = f.parse_with_rest_fallback("statements", endpoint="revenue-product-segmentation", symbol=tk)
        if isinstance(rows, list) and rows:
            segs, meta = fmp_client.latest_seg_map(rows)
            if segs:
                total = sum(segs.values())
                if total > 0 and len(segs) >= 2:
                    top = max(segs.items(), key=lambda kv: kv[1])
                    v["trojan_segment_flag"] = 1 if top[1] / total >= 0.25 else 0
                    v["trojan_segment_name"] = top[0]
                    v["trojan_segment_rev_pct"] = round(top[1] / total, 4)
                    # growth: compare same segment across periods where available
                    seg_dates = sorted(((str(r2.get("date", "")), r2) for r2 in rows
                                        if isinstance(r2.get("data"), dict)), reverse=True)
                    growth = None
                    for _, r2 in seg_dates:
                        d2 = r2["data"]
                        if len(d2) >= 2:
                            t2 = sum(d2.values())
                            if t2 > 0 and t2 != total:
                                growth = round(top[1] / d2.get(top[0], 0) - 1.0, 4) \
                                    if d2.get(top[0]) else None
                                break
                    v["trojan_segment_growth_yoy"] = growth
                got.add("segment")
    except Exception as e:
        log(f"  {tk} segmentation ERR {e}")

    # E11 ADX(14) technicalIndicators (free OHLCV + adx series)
    adx_row = None
    ohlcv = None
    try:
        rows = f.parse_with_rest_fallback("technicalIndicators", endpoint="average-directional-index",
                       symbol=tk, periodLength=14, timeframe="1day")
        if isinstance(rows, list) and rows:
            rows = sorted(rows, key=lambda x: str(x.get("date", "")))
            adx_row = rows[-1]
            ohlcv = rows  # ascending OHLCV, used for MFI/ATR/MACD
    except Exception as e:
        log(f"  {tk} ADX ERR {e}")
    if adx_row is not None:
        v["adx_14"] = num(adx_row.get("adx"))
        got.add("adx")

    # E12 historical-price-eod-full (rs_20d + float turnover; OHLCV fallback)
    eod = None
    try:
        rows = f.parse_with_rest_fallback("chart", endpoint="historical-price-eod-full", symbol=tk)
        if isinstance(rows, list) and rows:
            rows = sorted(rows, key=lambda x: str(x.get("date", "")))
            eod = rows
            closes = [num(r.get("close")) for r in rows]
            closes = [c for c in closes if c]
            r20 = pct_ret(closes, 20)
            if r20 is not None:
                # pillar benchmark fetched in regime pass; relative leg applied
                # later via v['_rs_own_20d'] stash
                v["_rs_own_20d"] = r20
            if v["float_shares"]:
                v5 = sum(num(r.get("volume")) or 0.0 for r in rows[-5:])
                v["float_turnover_5d"] = round(v5 / v["float_shares"], 6)
            got.add("eod")
            if ohlcv is None:
                ohlcv = rows
    except Exception as e:
        log(f"  {tk} eod-full ERR {e}")

    # Native MFI / ATR / MACD-slope from whatever OHLCV we have
    tech = compute_technicals(ohlcv or [])
    v["mfi_14"] = tech.get("mfi_14")
    v["atr_pct"] = tech.get("atr_pct")
    v["macd_hist_slope"] = tech.get("macd_hist_slope")
    if v["mfi_14"] is not None:
        got.add("mfi")

    # data quality: penalize missing CORE fundamentals (spec: 0 for missing
    # must not masquerade as zero-value; quality score is the guardrail)
    core = {"profile": 20, "scores": 20, "float": 15, "grades": 15, "estimates": 15, "eod": 15}
    dq = 100 - sum(pen for k, pen in core.items() if k not in got)
    v["data_quality_score"] = max(0, min(100, dq))
    v["last_updated"] = NOW_EPOCH

    # blended upside (EP-7/8) — only when a valuation anchor actually exists
    anchors = [a for a in (v["dcf_levered"], v["pt_consensus"]) if a and v["price"]]
    if anchors and v["price"]:
        blended = sum((a - v["price"]) / v["price"] * 100.0 for a in anchors) / (2.0 * len(anchors)) \
            if len(anchors) == 2 else (anchors[0] - v["price"]) / v["price"] * 100.0
        v["blended_upside_pct"] = round(blended, 4)
    return v


def fetch_feed_items(f, tk):
    """Wire / insider / transcript primary items for feed_raw.json."""
    items = []
    try:
        rows = f.parse_with_rest_fallback("news", endpoint="search-stock-news", symbols=tk, limit=3)
        for r in (rows if isinstance(rows, list) else [])[:3]:
            items.append({
                "source": "FMP_PRIMARY_WIRE", "ticker": tk,
                "headline": r.get("title"), "url": r.get("url") or r.get("articleLink"),
                "date": r.get("publishedDate"),
            })
    except Exception:
        pass
    try:
        rows = f.parse_with_rest_fallback("insiderTrades", endpoint="search-insider-trades",
                       symbol=tk, transaction_type="Purchase")
        for r in (rows if isinstance(rows, list) else [])[:3]:
            items.append({
                "source": "FMP_INSIDER_SENATE", "ticker": tk,
                "headline": f"insider purchase {r.get('transactionDate')} "
                             f"{r.get('transactionShares')} sh by {r.get('reportingCik')}",
                "transaction_date": r.get("transactionDate"),
            })
    except Exception:
        pass
    try:
        rows = f.parse_with_rest_fallback("earningsTranscript", endpoint="transcripts-dates-by-symbol", symbol=tk)
        if isinstance(rows, list) and rows:
            newest = max(rows, key=lambda x: str(x.get("date", "")))
            items.append({
                "source": "FMP_TRANSCRIPT_BREADCRUMB", "ticker": tk,
                "headline": f"latest transcript {newest.get('fiscalYear')}Q{newest.get('quarter')} "
                             f"{newest.get('date')}",
            })
    except Exception:
        pass
    return [i for i in items if i.get("headline")]


# ---------------------------------------------------------------------------
# Regime state (per pillar, benchmark + COT + macro)
# ---------------------------------------------------------------------------
_cot_cache = {}
_macro_cache = {}


def fetch_regime(db, f, pillars):
    """Upsert regime_state for the chunk's pillars. Fail-soft per pillar."""
    rows = []
    macro = None
    if "macro" not in _macro_cache:
        y10 = None
        try:
            tr = f.parse_with_rest_fallback("economics", endpoint="treasury-rates")
            if isinstance(tr, list) and tr:
                y10 = num(tr[0].get("year10"))
        except Exception as e:
            log(f"  treasury-rates ERR {e}")
        ism = None
        try:
            # NOTE: f.call()/f.parse_with_rest_fallback() take `name` as the tool-name POSITIONAL
            # arg, so `name=` in kwargs raises "multiple values". Bypass with
            # the raw JSON-RPC layer and parse the MCP content block ourselves.
            # 2026-11-05: economics-indicators returns "Invalid name" for every
            # candidate string probed (ISM variants incl. the calendar's exact
            # label). The window is now DYNAMIC (was hardcoded to 2026-11-04,
            # which froze the value even if a name ever worked). All-fail =
            # ism stays None (data-gap, never scored as zero) + a single log.
            _d1 = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
            _d0 = (datetime.datetime.now(datetime.timezone.utc).date()
                   - datetime.timedelta(days=180)).isoformat()
            _names = ("ISM Manufacturing New Orders Index", "ISM Manufacturing New Orders")
            _errs = []
            for _nm in _names:
                r = f._rpc("tools/call", {
                    "name": "economics",
                    "arguments": {"endpoint": "economics-indicators",
                                   "name": _nm, "from_date": _d0, "to_date": _d1},
                    "id": 99999})
                texts = [c.get("text", "") for c in
                         r.get("result", {}).get("content", []) if c.get("type") == "text"]
                if texts:
                    d = json.loads(texts[0])
                    if isinstance(d, list) and d:
                        ism = num(d[0].get("value"))
                        break
                    _errs.append(texts[0][:60])
            if ism is None:
                log(f"  economics-indicators: ISM leg NULL ({len(_names)} names failed: "
                    f"{_errs[:2]}) — verdict falls back to us10y/SPY only")
        except Exception as e:
            log(f"  economics-indicators ERR {e} (ISM orders leg skipped)")
        macro = {"us10y_yield": y10, "ism_new_orders": ism}
        _macro_cache["macro"] = macro
    us10y, ism = macro["us10y_yield"], macro["ism_new_orders"]
    if us10y is not None and ism is not None:
        verdict = "EXPANSION" if (ism >= 50 and us10y < 4.5) else \
                  "CONTRACTION" if (ism < 47 or us10y > 5.0) else "NEUTRAL"
    else:
        verdict = "NEUTRAL"

    for pillar in pillars:
        bench = PILLAR_BENCHMARK.get(pillar, DEFAULT_BENCH)
        bench_price, bench_rs, cot_z, crowding = None, None, None, 0
        try:
            rows2 = f.parse_with_rest_fallback("indexes", endpoint="index-historical-price-eod-full", symbol=bench)
            if isinstance(rows2, list) and rows2:
                # FMP returns NEWEST-FIRST with no limit honored (verified 2026-11-05:
                # 1254 rows back to 2021 for SMH). Sort oldest-first so index -1
                # is the LATEST session; previously the unsorted [-1] read a stale
                # mid-history row and the sorted [-1] read the OLDEST (2021) close.
                rows2 = sorted(rows2, key=lambda x: str(x.get("date", "")))
                closes = [num(r.get("close")) for r in rows2]
                closes = [c for c in closes if c]
                bench_price = closes[-1]
                bench_rs = round(pct_ret(closes, 20), 4)
        except Exception as e:
            log(f"  regime {pillar} bench {bench} ERR {e}")
        # COT leg: commodity pillars only (URA); equities have no COT report
        if bench in ("URA",) or pillar == "ENERGY_GRID_SMR":
            cot_sym = "GOLD" if bench == "URA" else bench
            try:
                rep = f.parse_with_rest_fallback("commitmentOfTraders", endpoint="COT-report", symbol=cot_sym)
                if isinstance(rep, list) and rep:
                    nz = num(rep[0].get("netNonCommercialZScore") or rep[0].get("zScore"))
                    if nz is not None:
                        cot_z, crowding = nz, 1 if nz > 2.0 else 0
            except Exception as e:
                log(f"  regime {pillar} COT ERR {e} (crowding flag stays 0)")
        rows.append((pillar, bench, bench_price, bench_rs, cot_z, crowding,
                     us10y, ism, None, verdict, NOW_EPOCH))

    cur = db.cursor()
    cur.executemany(
        "INSERT OR REPLACE INTO regime_state (pillar, benchmark_ticker, benchmark_price, "
        "benchmark_rs_20d, cot_net_spec_zscore, cot_extreme_crowding_flag, us10y_yield, "
        "ism_new_orders, fed_defense_spending_yoy, macro_regime_verdict, last_updated) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
    db.commit()
    log(f"regime_state: upserted {len(rows)} pillar rows")
    return rows


# ---------------------------------------------------------------------------
# Stage 3 — Quant-Gatekeeper deterministic scoring (Agent 2 math contract)
# ---------------------------------------------------------------------------
def valuation_score(upside):
    """Spec: <=0% -> 20, +15% -> 60, >=+35% -> 95 (linear between)."""
    if upside is None:
        return None  # no valuation anchor: unscored, not zero
    if upside <= 0:
        return 20.0
    if upside >= 35:
        return 95.0
    if upside <= 15:
        return 20.0 + (upside / 15.0) * 40.0
    return 60.0 + ((upside - 15.0) / 20.0) * 35.0


def load_brief_map(limit=None):
    """signal_briefs.json -> {ticker: [brief,...]} + 14d rolling counts."""
    try:
        d = json.load(open(BRIEFS))
        briefs = d.get("signal_briefs", [])
    except Exception:
        return {}, {}
    out, recent = {}, {}
    cutoff = NOW_EPOCH - 14 * 86400
    for b in briefs:
        if limit and b.get("brief_id") not in limit:
            continue
        for k in ("tickers_affected", "tickers_benefit", "tickers_mentioned"):
            for t in b.get(k) or []:
                out.setdefault(t, []).append(b)
        dc = b.get("date_created", "")
        ts = None
        try:
            ts = int(datetime.datetime.fromisoformat(dc.replace("Z", "+00:00")).timestamp())
        except Exception:
            pass
        if ts and ts >= cutoff:
            for t in (b.get("tickers_affected") or []) + (b.get("tickers_benefit") or []):
                recent[t] = recent.get(t, 0) + (1 if b.get("impact_direction") == "positive"
                                                 else -0.5 if b.get("impact_direction") == "negative" else 0)
    return out, recent


def gatekeeper_score(v, briefs, recent_briefs):
    """4 pillars 0-100 per spec math contract. None = leg unscoreable (data gap).

    Guard #1 (null-coalescing): every numeric field is passed through num()
    before ANY arithmetic — dcf_levered / pt_consensus / rev_est_next_yr /
    the whole technicals block can arrive as None from the DB or the bus and
    the gatekeeper must return None legs, never TypeError.
    """
    # ---- null-coalesced inputs (num() also coerces str/bad types safely) ----
    price = num(v.get("price"))
    upside = num(v.get("blended_upside_pct"))
    pio = v.get("piotroski_f")
    pio = int(pio) if isinstance(pio, (int, float)) and pio is not None else None
    altman = num(v.get("altman_z"))
    mfi = num(v.get("mfi_14"))
    adx = num(v.get("adx_14"))
    macd_slope = num(v.get("macd_hist_slope"))
    rs_own = num(v.get("_rs_own_20d"))
    rs_rel = num(v.get("rs_20d_vs_pillar"))
    flt = num(v.get("float_shares"))
    eps_d = num(v.get("eps_revision_delta_30d"))
    # valuation anchors fed by the ingestor (None-safe: blended_upside_pct was
    # only computed when a real anchor + price existed)
    dcf_l = num(v.get("dcf_levered"))      # noqa: F841 — anchor presence check
    pt_c = num(v.get("pt_consensus"))     # noqa: F841
    rev_n = num(v.get("rev_est_next_yr"))  # noqa: F841 — context only
    trojan = 1 if (num(v.get("trojan_segment_flag")) or 0) == 1 else 0

    # financial: min(72, piotroski*8) + altman>3 -> +15 + trojan -> +15
    pf = []
    if pio is not None:
        pf.append(min(72, max(0, pio) * 8))
    if altman is not None and altman > 3.0:
        pf.append(15)
    if trojan == 1:
        pf.append(15)
    financial = min(100.0, float(sum(pf))) if pf else None

    valuation = valuation_score(upside)

    # technical: MFI>55 / ADX>20+MACD slope / RS>0 / float<50M
    tf = []
    any_leg = mfi is not None or adx is not None or \
        rs_own is not None or rs_rel is not None or flt is not None
    if any_leg:
        if mfi is not None and mfi > 55:
            tf.append(25)
        if adx is not None and adx > 20 and macd_slope is not None and macd_slope > 0:
            tf.append(25)
        rs_best = rs_own if rs_own is not None else rs_rel
        if rs_best is not None and rs_best > 0:
            tf.append(25)
        if flt is not None and flt < 50e6:
            tf.append(25)
        technical = min(100.0, float(sum(tf)))
    else:
        technical = None

    # catalyst: rolling 14d brief score + eps revision (+20) + 13F/insider cluster (+25)
    base = max(0.0, min(55.0, float(recent_briefs.get(v["ticker"], 0) or 0) * 11.0))
    cat = 0.0
    if base:
        cat = base
    if eps_d is not None and eps_d > 0:
        cat += 20
    if briefs:
        has_cluster = any(b.get("source_category") in ("FMP_INSIDER_SENATE", "FMP_13F")
                          for b in briefs)
        if has_cluster:
            cat += 25
    if not briefs and eps_d is None and not base:
        catalyst = None
    else:
        catalyst = min(100.0, cat)

    return {
        "financial": financial, "valuation": valuation,
        "technical": technical, "catalyst": catalyst,
    }


def write_scores_chunk(vectors, brief_map, recent, bench_rs_map):
    """Write deterministic pre-LLM scores to the handoff bus (atomic)."""
    # apply RS relative leg (own 20d - benchmark 20d) before scoring
    for v in vectors:
        own = v.pop("_rs_own_20d", None)
        if own is not None:
            b = bench_rs_map.get(v["sector_pillar"])
            v["rs_20d_vs_pillar"] = round(own - b, 4) if b is not None else None
    rows = []
    for v in vectors:
        tk = v["ticker"]
        briefs = brief_map.get(tk, [])
        sc = gatekeeper_score(v, briefs, recent)
        row = {c: v[c] for c in PILLAR_COLS}
        row["pillar_scores_100"] = sc
        rows.append(row)
    payload = {
        "generated": NOW_ISO,
        "agent": "Harvester-Prime+Quant-Gatekeeper (unharnessed ingestor)",
        "tickers_scored": len(rows),
        "rows": rows,
    }
    fmp_client.atomic_write_json(SCORES_BUS, payload)
    log(f"unharnessed_scores.json: {len(rows)} rows (atomic write OK)")


# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="Unharnessed FMP Ingestor")
    parser.add_argument("--limit", type=int, default=None, help="Limit tickers")
    parser.add_argument("--tickers", type=str, default=None, help="Comma-separated tickers")
    parser.add_argument("--no-score", action="store_true", help="Skip gatekeeper scoring")
    parser.add_argument("--no-feed", action="store_true", help="Skip feed_raw appends")
    parser.add_argument("--etf", type=str, default=None,
                        help="Comma-separated ETFs (e.g. SMH,XSD): pull each fund's "
                             "holdings via FMP etfAndMutualFunds, seed them into the "
                             "universe (pillar-assigned, idempotent) and harvest that "
                             "incremental set this run. For the 6h cron rotation.")
    args = parser.parse_args()

    db = sqlite3.connect(DB)
    if args.tickers:
        want = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
        placeholders = ",".join("?" * len(want))
        sel = db.execute(f"SELECT ticker, name, pillar FROM universe WHERE ticker IN ({placeholders})", tuple(want)).fetchall()
        have = {r[0] for r in sel}
        for w in want:
            if w not in have:
                sel.append((w, None, "UNCLASSIFIED"))
    elif args.limit:
        sel = db.execute(
            "SELECT u.ticker, u.name, COALESCE(u.pillar, 'SMH') as pillar "
            "FROM universe u "
            "LEFT JOIN alpha_vectors a ON u.ticker = a.ticker "
            "WHERE (u.pillar IS NOT NULL AND u.pillar != '') "
            "ORDER BY (a.last_updated IS NOT NULL), a.last_updated ASC LIMIT ?",
            (args.limit,)
        ).fetchall()
    else:
        sel = db.execute(
            "SELECT u.ticker, u.name, COALESCE(u.pillar, 'SMH') as pillar "
            "FROM universe u "
            "LEFT JOIN alpha_vectors a ON u.ticker = a.ticker "
            "WHERE (u.pillar IS NOT NULL AND u.pillar != '') "
            "ORDER BY (a.last_updated IS NOT NULL), a.last_updated ASC"
        ).fetchall()

    # --- mcap band gate (AGENTS.md rule 6): skip out-of-band names ---
    def in_band(m):
        return m is not None and MCAP_FLOOR <= m <= MCAP_CEILING

    # --- Stage 1+2: harvest + vectorize (one FMP session for the chunk) ---
    # Request-Level Throttling (2026-11-05): TICKER_SEM caps concurrent ticker
    # tasks at 10; the global HTTP ceiling (fmp_client.HTTP_SEM = 20) caps
    # in-flight requests across ALL workers/clients. fmp_client's transport is
    # strictly sequential today (single MCP session), so the real win is the
    # 8-worker pool overlapping DB/compute work with I/O — and full parallelism
    # the moment the transport allows it.
    TICKER_SEM = asyncio.Semaphore(10)
    _ticker_loop = asyncio.new_event_loop()
    _ticker_thr = threading.Thread(target=_ticker_loop.run_forever,
                                   name="ingestor-ticker-sem", daemon=True)
    _ticker_thr.start()

    def ticker_acquire():
        fut = asyncio.run_coroutine_threadsafe(TICKER_SEM.acquire(), _ticker_loop)
        _fut_wait([fut], timeout=600)
        return fut.result()

    def ticker_release():
        try:
            fut = asyncio.run_coroutine_threadsafe(TICKER_SEM.release(), _ticker_loop)
            _fut_wait([fut], timeout=60)
            fut.result()
        except Exception:
            pass

    with fmp_client.FMP(timeout=45, retries=3, backoff=1.5) as f:
        log(f"FMP session up: {f.server}")
        # --- --etf incremental target: seed universe + define this run's set ---
        if args.etf:
            etfs = [t.strip().upper() for t in args.etf.split(",") if t.strip()]
            log(f"--etf incremental target: {','.join(etfs)}")
            etf_sel = pull_etf_holdings(db, f, etfs)
            if etf_sel:
                sel = etf_sel

        vectors = []
        feed_items = []
        _vec_lock = threading.Lock()
        _feed_lock = threading.Lock()

        def work(tk, name, pillar):
            ticker_acquire()
            try:
                v = fetch_vector(f, tk, name, pillar or "UNCLASSIFIED")
                if v["mcap"] is not None and not in_band(v["mcap"]):
                    if v["mcap"] > MCAP_CEILING:
                        log(f"  {tk}: mcap ${v['mcap']/1e9:,.1f}B >= 300B ceiling — DROPPED (not upserted)")
                        return None
                    # below floor: allowed only as WATCH-tier data; keep (floor is soft per DDL comment)
                items = []
                if not args.no_feed:
                    items = fetch_feed_items(f, tk)
                return v, items
            finally:
                ticker_release()

        max_workers = min(8, max(1, len(sel)))
        with ThreadPoolExecutor(max_workers=max_workers,
                                thread_name_prefix="ticker") as ex:
            futs = {ex.submit(work, tk, name, pillar): tk for tk, name, pillar in sel}
            for fut in as_completed(futs):
                tk = futs[fut]
                try:
                    res = fut.result()
                except Exception as e:
                    log(f"  {tk}: harvest FAILED {e} (row stays NULL — data-gap discipline)")
                    continue
                if res is None:
                    continue
                v, items = res
                with _vec_lock:
                    vectors.append(v)
                if items:
                    with _feed_lock:
                        feed_items.extend(items)

        pillars = sorted({v["sector_pillar"] for v in vectors})
        regime_rows = fetch_regime(db, f, pillars)
        bench_rs_map = {r[0]: r[3] for r in regime_rows}

        # 100-row chunked upsert (INSERT OR REPLACE)
        ph = ",".join("?" * len(PILLAR_COLS))
        for i in range(0, len(vectors), CHUNK):
            chunk = vectors[i:i + CHUNK]
            db.executemany(
                "INSERT OR REPLACE INTO alpha_vectors (%s) VALUES (%s)" %
                (",".join(PILLAR_COLS), ph),
                [[v[c] for c in PILLAR_COLS] for v in chunk])
            db.commit()
            log(f"alpha_vectors: upserted chunk {i//CHUNK + 1} ({len(chunk)} rows)")

    if feed_items and not args.no_feed:
        fmp_client.bus_append_entries(FEED, feed_items)
        log(f"feed_raw.json: appended {len(feed_items)} primary items (atomic)")

    if not args.no_score:
        brief_map, recent = load_brief_map()
        write_scores_chunk(vectors, brief_map, recent, bench_rs_map)
    n = db.execute("SELECT COUNT(*) FROM alpha_vectors").fetchone()[0]
    log(f"DONE: alpha_vectors now {n} rows. Full-universe backfill continues on next run "
        f"(resumable). Smoke-test-safe exit.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
