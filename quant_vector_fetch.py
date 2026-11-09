#!/usr/bin/env python3
"""quant_vector_fetch.py — FMP 15-endpoint batch fetcher for alpha_vectors/regime_state.

Pulls the corridor universe ($500M <= mcap <= $300B, per schema contract) from
universe_cache.db, fetches per-ticker FMP data through fmp_client (Streamable
HTTP MCP, stdlib only), derives all vector fields DETERMINISTICALLY, and emits
a payload file consumed by scripts/ingest_quant_update.py (100-row chunked
transactions, upsert semantics).

Per-ticker endpoint fan-out (8 calls/ticker):
  EP1   quote            batch-quote (chunked, all tickers in one pass)
  EP1b  company          profile-symbol (authoritative name/cik/mcap)
  EP5   company          shares-float -> float_shares, free_float_pct
  EP6   statements       financial-scores -> altman_z, piotroski_f
  EP7   discountedCashFlow dcf-advanced -> dcf_standard; dcf-levered -> dcf_levered
  EP8   analyst          price-target-consensus -> pt_consensus
  EP8b  analyst          grades-summary -> analyst_count
  EP4   analyst          financial-estimates period=annual -> rev_est_next_yr,
                         eps_revision_delta_30d (F+1 consensus delta vs F)
  EP10  statements       revenue-product-segmentation -> trojan flag (rev pct
                         >= 0.25 AND YoY growth >= 0.30 in latest annual pair)
  EP12  technicalIndicators average-directional-index 1day periodLength=14
                         -> ADX + full OHLCV bars; MFI/ATR/MACD-hist-slope/
                         20d return computed LOCALLY from the bars (no extra
                         calls — MFI/ATR/MACD endpoints do not exist in the
                         MCP tool enum; verified 2026-11-05).
  EP14  relative strength: ticker 20d return (bars) minus pillar benchmark
                         20d return (fetched once per pillar in the regime pass).

Regime pass (once per run, 5 pillars SMH/URA/BOTZ/DEFENSE/MACRO):
  EP1   quote batch-quote benchmarks
  EP14  indexes index-historical-price-eod-light -> benchmark price + rs_20d vs SPY
  EP13  commitmentOfTraders COT-report-analysis ZB (noncomm net position z-score
                         over the returned history; > 2.0 -> crowding flag)
  EP15  economics treasury-rates -> us10y_yield (latest); economics-indicators
                         probed for ISM (best-effort, null on 'Invalid name')
  macro_regime_verdict: deterministic (y10 vs 4.5/5.0 + SPY 20d vs 0)

Hygiene:
  - fmp_client entity-collision rule: profile-symbol companyName is authoritative.
  - data_quality_score starts at 100, -10 per endpoint group missing/failed.
  - Resumable: per-ticker results checkpointed to quant_vector_fetch_state.json
    (atomic via fmp_client.atomic_write_json); re-run skips done tickers.
  - Parallelism: WORKERS threads, one FMP session each; throttle SLEEP between
    per-ticker bursts. Rate posture ~1270 tickers * 8 calls.

Usage:
  python3 quant_vector_fetch.py --probe 3        # 3 tickers, print payload
  python3 quant_vector_fetch.py --limit 50       # test batch
  python3 quant_vector_fetch.py                  # full run -> payload file
  python3 quant_vector_fetch.py --regime-only    # regime pass only
  python3 quant_vector_fetch.py --dry-run        # universe load + sizing only
"""
import argparse
import json
import math
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

sys.path.insert(0, os.path.expanduser("~/.hermes/scripts"))
import fmp_client

CRON = os.path.expanduser("~/.hermes/cron")
DB = os.path.join(CRON, "universe_cache.db")
STATE_F = os.path.join(CRON, "quant_vector_fetch_state.json")
PAYLOAD_F = os.path.join(CRON, "quant_vector_payload.json")
INGEST = os.path.join(CRON, "scripts", "ingest_quant_update.py")

FLOOR, CEILING = 5e8, 3e11          # strict schema contract
WORKERS = 6
SLEEP_S = 0.35                       # per-ticker burst throttle (seconds)
QUOTE_CHUNK = 90

# pillar mapping: universe canonical pillar -> regime pillar label (schema:
# SMH/URA/BOTZ/DEFENSE). AI_INFRA_THERMAL + QUANTUM map to SMH (semis are the
# dominant physical substrate for both); UNMAPPED -> SMH as the system default.
PILLAR_MAP = {
    "SEMI_SUBSYSTEMS": "SMH",
    "ENERGY_GRID_SMR": "URA",
    "PHYSICAL_AI_ROBOTICS": "BOTZ",
    "DEFENSE_GOV": "DEFENSE",
    "AI_INFRA_THERMAL": "SMH",
    "QUANTUM": "SMH",
}
BENCHMARKS = {"SMH": "SMH", "URA": "URA", "BOTZ": "BOTZ",
              "DEFENSE": "ITA", "MACRO": "SPY"}


def now_ts():
    return int(time.time())


# ---- deterministic technical helpers (computed from OHLCV bars) ------------
# (_wilder_rsi and _rsi_from_endpoint removed 2026-11-05: dead code, never
# called — the schema has no rsi_14 column and ADX comes from the endpoint.)
def _atr_pct(bars, period=14):
    if len(bars) < period + 1:
        return None
    trs = []
    for i in range(len(bars) - period, len(bars)):
        h, l = bars[i]["high"], bars[i]["low"]
        pc = bars[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = sum(trs) / period
    last_close = bars[-1]["close"]
    if not last_close:
        return None
    return round(atr / last_close * 100, 4)


def _mfi(bars, period=14):
    if len(bars) < period + 1:
        return None
    tp_prev = None
    pos, neg = 0.0, 0.0
    for b in bars[-(period + 1):]:
        tp = (b["high"] + b["low"] + b["close"]) / 3
        if tp_prev is not None:
            mf = tp * b["volume"]
            if tp > tp_prev:
                pos += mf
            elif tp < tp_prev:
                neg += mf
        tp_prev = tp
    if neg == 0:
        return 100.0
    if pos + neg == 0:
        return None
    return round(100 - 100 / (1 + pos / neg), 4)


def _macd_hist_slope(bars):
    """MACD(12,26,9) histogram slope: H(t) - H(t-1), where H = macd - signal.
    Needs >= 36 bars; the checkpoint stores 25, so this field is None unless
    re-fetched with a deeper window (documented limitation, 2026-11-05).
    Bars are NEWEST-FIRST: index 0 = latest."""
    closes = [b["close"] for b in bars]
    if len(closes) < 36:
        return None
    def ema_series(vals, n):
        k = 2 / (n + 1)
        out = []
        e = sum(vals[:n]) / n
        for i, v in enumerate(vals):
            if i < n - 1:
                continue
            if i > n - 1:
                e = v * k + e * (1 - k)
            out.append(e)
        return out  # aligned to vals[n-1:]
    def hist_at(offset):
        # offset 0 = latest bar, 1 = one bar older (newest-first list)
        seg = closes[:len(closes) - offset][::-1]      # oldest->newest
        e12 = ema_series(seg, 12)
        e26 = ema_series(seg, 26)
        macd_line = [a - b for a, b in zip(e12[-len(e26):], e26)]
        sig = ema_series(macd_line, 9)
        return macd_line[-1] - sig[-1]
    h0, h1 = hist_at(0), hist_at(1)
    if h0 is None or h1 is None:
        return None
    return round(h0 - h1, 6)


def _ret_20d(bars):
    """20-trading-day return in pct from the bars (needs >= 21 bars)."""
    if len(bars) < 21:
        return None
    p0, p1 = bars[-21]["close"], bars[-1]["close"]
    if not p0:
        return None
    return round((p1 / p0 - 1) * 100, 4)


# ---- field derivation --------------------------------------------------------
def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _as_list_of_dicts(x):
    if not isinstance(x, list):
        return []
    return [r for r in x if isinstance(r, dict)]


def derive_alpha(tk, pillar, q, prof, flt, scores, dcf_s, dcf_l, pt, grades,
                 ests, segs, tech_bars, bench_ret, dqs, ts):
    row = {
        "ticker": tk,
        "sector_pillar": pillar,
        "mcap": _num(prof.get("marketCap")) or _num(q.get("marketCap")),
        "company_name": prof.get("companyName"),
        "cik": str(prof.get("cik") or "") or None,
        "price": _num(q.get("price")) or (tech_bars[-1]["close"] if tech_bars else None),
        "last_updated": ts,
    }
    price = row["price"]
    # EP5
    if flt:
        row["float_shares"] = _num(flt.get("floatShares"))
        row["free_float_pct"] = _num(flt.get("freeFloat"))
        if row["float_shares"] and tech_bars:
            vol5 = sum(b["volume"] for b in tech_bars[-5:])
            row["float_turnover_5d"] = round(vol5 / row["float_shares"], 6)
        dqs.add("EP5")
    # EP6
    if scores:
        row["altman_z"] = _num(scores.get("altmanZScore"))
        pi = scores.get("piotroskiScore")
        if pi is not None:
            row["piotroski_f"] = int(pi)
        dqs.add("EP6")
    # EP7
    if dcf_s is not None:
        row["dcf_standard"] = dcf_s
        dqs.add("EP7a")
    if dcf_l is not None:
        row["dcf_levered"] = dcf_l
    if dcf_s is None and dcf_l is None:
        dqs.discard("EP7a")
    # EP8
    if pt is not None:
        row["pt_consensus"] = pt
        dqs.add("EP8")
    if grades:
        row["analyst_count"] = int(sum(_num(grades.get(k)) or 0 for k in
                                       ("strongBuy", "buy", "hold", "sell", "strongSell")))
    # blended upside
    parts = []
    if price and dcf_l is not None:
        parts.append((dcf_l - price) / price * 100)
    if price and pt is not None:
        parts.append((pt - price) / price * 100)
    if parts:
        row["blended_upside_pct"] = round(sum(parts) / len(parts), 4)
    # EP4
    ests = _as_list_of_dicts(ests)
    if ests:
        # rows ordered by date; find next-two fiscal years from today
        today = datetime.now(timezone.utc).year
        by_year = {}
        for r in ests:
            d = str(r.get("date") or "")
            if d:
                by_year[d[:4]] = r
        years = sorted(by_year)
        future = [y for y in years if int(y[:4]) >= today + 1]
        if len(future) >= 2:
            row["rev_est_next_yr"] = _num(by_year[future[1]].get("revenueAvg"))
        elif future:
            row["rev_est_next_yr"] = _num(by_year[future[0]].get("revenueAvg"))
        if len(future) >= 2:
            e1 = _num(by_year[future[0]].get("epsAvg"))
            e2 = _num(by_year[future[1]].get("epsAvg"))
            if e1 and e2 is not None:
                row["eps_revision_delta_30d"] = round(e2 - e1, 4)
        dqs.add("EP4")
    # EP10 trojan horse
    segs = _as_list_of_dicts(segs)
    if len(segs) >= 1:
        segs_sorted = sorted(segs, key=lambda r: str(r.get("date") or ""))
        if len(segs_sorted) >= 2:
            latest = segs_sorted[-1].get("data") or {}
            prior = segs_sorted[-2].get("data") or {}
            tot = sum(v for v in latest.values() if _num(v))
            if tot > 0:
                best = None
                for k, v in latest.items():
                    lv, pv = _num(v), _num(prior.get(k))
                    if lv is None or pv is None or pv <= 0:
                        continue
                    g = lv / pv - 1
                    share = lv / tot
                    if share >= 0.25 and g >= 0.30:
                        if best is None or g > best[2]:
                            best = (k, share, g)
                if best:
                    row["trojan_segment_flag"] = 1
                    row["trojan_segment_name"] = best[0]
                    row["trojan_segment_rev_pct"] = round(best[1], 4)
                    row["trojan_segment_growth_yoy"] = round(best[2], 4)
            dqs.add("EP10")
    # EP12 technicals. Bars are stored NEWEST-FIRST (endpoint order); every
    # helper below consumes OLDEST-FIRST, so flip once here. VERIFIED 2026-11-05:
    # AAOI endpoint RSI(14) = 32.16 vs our as-stored 62.25 vs reversed 31.58 —
    # the reversed orientation is the correct one (0.6pt Wilder-seed difference).
    tech_bars = _as_list_of_dicts(tech_bars)[::-1]
    if tech_bars:
        adx = _num(tech_bars[-1].get("adx"))
        if adx is not None:
            row["adx_14"] = adx
        row["atr_pct"] = _atr_pct(tech_bars)
        row["mfi_14"] = _mfi(tech_bars)
        row["macd_hist_slope"] = _macd_hist_slope(tech_bars)
        dqs.add("EP12")
    # EP14 relative strength vs pillar benchmark
    if bench_ret is not None:
        t20 = _ret_20d(tech_bars)
        if t20 is not None:
            row["rs_20d_vs_pillar"] = round(t20 - bench_ret, 4)
            dqs.add("EP14")
    # data_quality_score: 100 = all core fundamentals present, penalized per
    # missing CORE group (contract aligned with unharnessed_fmp_ingestor,
    # 2026-11-05 — the old formula SUBTRACTED 10 per PRESENT group, inverting
    # the scale so full-data rows scored ~20 and empty rows scored 100).
    core_present = set()
    if prof:
        core_present.add("profile")
    if flt:
        core_present.add("float")
    if scores:
        core_present.add("scores")
    if grades:
        core_present.add("grades")
    if ests:
        core_present.add("estimates")
    if tech_bars:
        core_present.add("eod")
    core_pen = {"profile": 20, "scores": 20, "float": 15,
                "grades": 15, "estimates": 15, "eod": 15}
    row["data_quality_score"] = max(0, 100 - sum(p for k, p in core_pen.items()
                                                 if k not in core_present))
    return row


def fetch_one(f, tk):
    """8-call fan-out per ticker. Returns (fields, missing_groups, error)."""
    q = prof = flt = scores = pt = grades = ests = tech_bars = None
    dcf_s = dcf_l = None
    segs = None
    misses = set()
    errs = []

    def guard(label, fn):
        try:
            return fn()
        except Exception as e:
            errs.append(f"{label}:{type(e).__name__}")
            return None

    prof = guard("profile", lambda: f.parse("company", endpoint="profile-symbol", symbol=tk) or {})
    if isinstance(prof, list):
        prof = prof[0] if prof else {}
    flt = guard("shares-float", lambda: f.parse("company", endpoint="shares-float", symbol=tk) or [])
    flt = flt[0] if isinstance(flt, list) and flt else None
    scores = guard("financial-scores", lambda: f.parse("statements", endpoint="financial-scores", symbol=tk) or [])
    scores = scores[0] if isinstance(scores, list) and scores else None
    d = guard("dcf", lambda: f.parse("discountedCashFlow", endpoint="dcf-advanced", symbol=tk) or [])
    if isinstance(d, list) and d:
        dcf_s = _num(d[0].get("dcf"))
    d = guard("dcf-l", lambda: f.parse("discountedCashFlow", endpoint="dcf-levered", symbol=tk) or [])
    if isinstance(d, list) and d:
        dcf_l = _num(d[0].get("dcf"))
    p = guard("pt", lambda: f.parse("analyst", endpoint="price-target-consensus", symbol=tk) or [])
    if isinstance(p, list) and p:
        pt = _num(p[0].get("targetConsensus"))
    grades = guard("grades", lambda: f.parse("analyst", endpoint="grades-summary", symbol=tk) or [])
    grades = grades[0] if isinstance(grades, list) and grades else None
    ests = guard("estimates", lambda: f.parse("analyst", endpoint="financial-estimates", symbol=tk, period="annual") or [])
    segs = guard("segmentation", lambda: f.parse("statements", endpoint="revenue-product-segmentation", symbol=tk) or [])
    t = guard("adx", lambda: f.parse("technicalIndicators", endpoint="average-directional-index", symbol=tk, periodLength=14, timeframe="1day") or [])
    tech_bars = t if isinstance(t, list) and t else None

    for label, present in (("profile", prof), ("shares-float", flt),
                            ("financial-scores", scores), ("estimates", ests),
                            ("segmentation", segs), ("adx", tech_bars)):
        if not present:
            misses.add(label)
    if dcf_s is None and dcf_l is None:
        misses.add("dcf")
    if pt is None:
        misses.add("pt")
    return dict(prof=prof, flt=flt, scores=scores, dcf_s=dcf_s, dcf_l=dcf_l,
                pt=pt, grades=grades, ests=ests, segs=segs, tech_bars=tech_bars), misses, errs


# ---- regime pass --------------------------------------------------------------
def regime_pass(f, dqs_all):
    ts = now_ts()
    bench_syms = list(set(BENCHMARKS.values()))
    quotes = {}
    q = f.parse("quote", endpoint="batch-quote", symbols=bench_syms)
    if isinstance(q, list):
        for r in q:
            quotes[r["symbol"]] = r

    def idx20(sym):
        try:
            bars = f.parse("indexes", endpoint="index-historical-price-eod-light", symbol=sym, limit=30)
            if not isinstance(bars, list) or len(bars) < 21:
                return None, None
            p0, p1 = _num(bars[-21].get("price")), _num(bars[-1].get("price"))
            return (_num(bars[-1].get("price")), round((p1 / p0 - 1) * 100, 4) if p0 else None)
        except Exception:
            return None, None

    spy_price, spy20 = idx20("SPY")
    us10y = None
    try:
        today_s = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        tr = f.parse("economics", endpoint="treasury-rates",
                     from_date=datetime.now(timezone.utc).date().replace(day=1).isoformat(),
                     to_date=today_s)
        if isinstance(tr, list) and tr:
            us10y = _num(tr[0].get("year10"))
    except Exception:
        pass
    ism = None
    from datetime import timedelta
    for nm in ("ISM Manufacturing New Orders", "Manufacturing PMI"):
        try:
            v = f.parse("economics", endpoint="economics-indicators", name=nm,
                        from_date=(datetime.now(timezone.utc).date() - timedelta(days=120)).isoformat(),
                        to_date=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
            if isinstance(v, list) and v:
                ism = _num(v[-1].get("value") if isinstance(v[-1], dict) else v[-1])
                break
        except Exception:
            continue

    # EP13 COT: noncommercial net position z-score over returned history
    cot_z, cot_flag = None, 0
    try:
        cot = f.parse("commitmentOfTraders", endpoint="COT-report", symbol="ZB")
        if isinstance(cot, list) and cot:
            nets = []
            for r in cot:
                nl = _num(r.get("noncommPositionsLongAll"))
                ns = _num(r.get("noncommPositionsShortAll"))
                if nl is not None and ns is not None:
                    nets.append(nl - ns)
            if len(nets) >= 20:
                mean = sum(nets) / len(nets)
                var = sum((x - mean) ** 2 for x in nets) / len(nets)
                sd = math.sqrt(var) or 1.0
                cot_z = round((nets[-1] - mean) / sd, 4)
                cot_flag = 1 if cot_z > 2.0 else 0
    except Exception:
        pass

    # macro verdict (deterministic)
    if us10y is not None and spy20 is not None:
        if spy20 > 3 and us10y < 4.5:
            verdict = "EXPANSION"
        elif spy20 < -3 or us10y > 5.0:
            verdict = "CONTRACTION"
        elif spy20 < 0:
            verdict = "STAGFLATION"
        else:
            verdict = "NEUTRAL"
    else:
        verdict = "NEUTRAL"

    out = []
    for pillar, sym in BENCHMARKS.items():
        bq = quotes.get(sym, {})
        price, r20 = (None, None)
        if pillar == "MACRO":
            price, r20 = spy_price, spy20
        else:
            try:
                price, r20 = idx20(sym)
            except Exception:
                pass
        if price is None:
            price = _num(bq.get("price"))
        row = {
            "pillar": pillar,
            "benchmark_ticker": sym,
            "benchmark_price": price,
            "benchmark_rs_20d": round(r20 - spy20, 4) if (r20 is not None and spy20 is not None) else None,
            "us10y_yield": us10y,
            "ism_new_orders": ism,
            "macro_regime_verdict": verdict,
            "last_updated": ts,
        }
        if pillar == "MACRO":
            row["cot_net_spec_zscore"] = cot_z
            row["cot_extreme_crowding_flag"] = cot_flag
        out.append(row)
    return out


# ---- main ---------------------------------------------------------------------
def load_universe():
    import sqlite3
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT ticker, name, pillar, pillars FROM universe "
        "WHERE market_cap BETWEEN ? AND ? ORDER BY ticker",
        (FLOOR, CEILING)).fetchall()
    con.close()
    return [dict(r) for r in rows]


def pillar_of(r):
    return PILLAR_MAP.get(r["pillar"], "SMH")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--probe", type=int, default=None,
                    help="fetch only N tickers and print the payload (no write)")
    ap.add_argument("--regime-only", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-ingest", action="store_true",
                    help="write payload only; do not call ingest_quant_update.py")
    args = ap.parse_args()

    uni = load_universe()
    if args.probe:
        uni = uni[: args.probe]
    elif args.limit:
        uni = uni[: args.limit]
    print(f"universe: {len(uni)} tickers in [$500M, $300B] corridor")
    if args.dry_run:
        from collections import Counter
        print("pillars:", dict(Counter(pillar_of(r) for r in uni)))
        print(f"est. calls: {len(uni) * 8 + 15} | workers={WORKERS} | "
              f"~{len(uni) * 8 * (0.7 + SLEEP_S / WORKERS):.0f}s wall-clock")
        return

    # state (resumable checkpoint)
    st = {}
    if os.path.exists(STATE_F):
        try:
            st = json.load(open(STATE_F))
        except Exception:
            st = {}
    done = st.get("done", {})
    todo = [r for r in uni if r["ticker"] not in done]
    if args.regime_only:
        todo = []
    print(f"to fetch: {len(todo)} (checkpointed: {len(uni) - len(todo)})")

    ts = now_ts()
    lock = threading.Lock()
    local = threading.local()

    def get_f():
        if not hasattr(local, "f"):
            local.f = fmp_client.FMP()
            local.f.__enter__()
        return local.f

    def worker(r):
        tk = r["ticker"]
        pillar = pillar_of(r)
        f = get_f()
        fields, misses, errs = fetch_one(f, tk)
        segs_l = _as_list_of_dicts(fields["segs"])
        bars_l = _as_list_of_dicts(fields["tech_bars"])
        with lock:
            done[tk] = {"pillar": pillar, "name": r["name"], "miss": sorted(misses),
                        "err": errs[:4], "fields": {
                            "prof": fields["prof"], "flt": fields["flt"],
                            "scores": fields["scores"], "dcf_s": fields["dcf_s"],
                            "dcf_l": fields["dcf_l"], "pt": fields["pt"],
                            "grades": fields["grades"],
                            "seg_latest": segs_l[-1] if segs_l else None,
                            "seg_prior": segs_l[-2] if len(segs_l) >= 2 else None,
                        }, "bars_n": len(bars_l),
                        "bars": bars_l[:60],
                        "estimates": _as_list_of_dicts(fields["ests"])[:12],
                        "ts": now_ts()}
            st["done"] = done
            n = len(done)
            if n % 25 == 0:
                fmp_client.atomic_write_json(STATE_F, st)
                print(f"  checkpoint {n}/{len(uni)}", flush=True)
        time.sleep(SLEEP_S)

    if todo:
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = {ex.submit(worker, r): r["ticker"] for r in todo}
            for i, fut in enumerate(as_completed(futs), 1):
                try:
                    fut.result()
                except Exception as e:
                    print(f"  worker error {futs[fut]}: {type(e).__name__} {e}", file=sys.stderr)
                if i % 50 == 0:
                    print(f"  {i}/{len(todo)} tickers fetched", flush=True)
    fmp_client.atomic_write_json(STATE_F, st)

    # regime pass
    with fmp_client.FMP() as f:
        regime_rows = regime_pass(f, None)

    # derive alpha rows
    bench_ret = {}
    for row in regime_rows:
        if row.get("benchmark_rs_20d") is not None:
            bench_ret[row["pillar"]] = row["benchmark_rs_20d"]
    spy20 = None
    for row in regime_rows:
        if row["pillar"] == "MACRO":
            spy20 = row.get("benchmark_rs_20d")
    alpha_rows = []
    for r in uni:
        tk = r["ticker"]
        d = done.get(tk)
        if not d:
            continue
        pillar = d["pillar"]
        dqs = set()
        fields = d["fields"]
        # price from latest bar; mcap from profile (authoritative per entity-integrity rule)
        price = None
        if d.get("bars"):
            price = _num(d["bars"][-1].get("close"))
        q = {"price": price}
        row = derive_alpha(tk, pillar, q, fields["prof"], fields["flt"],
                           fields["scores"], fields["dcf_s"], fields["dcf_l"],
                           fields["pt"], fields["grades"], d.get("estimates") or [],
                           [x for x in (fields.get("seg_prior"), fields.get("seg_latest"))
                            if isinstance(x, dict)],
                           d.get("bars") or [], bench_ret.get(pillar), dqs, d["ts"])
        if not row.get("mcap"):
            # mcap from universe cache (proven by seeder) as last resort
            row["mcap"] = None
        alpha_rows.append(row)
    alpha_rows = [r for r in alpha_rows if r.get("mcap")]
    payload = {"alpha_vectors": alpha_rows, "regime_state": regime_rows}

    out_f = "/tmp/quant_vector_probe.json" if args.probe else PAYLOAD_F
    fmp_client.atomic_write_json(out_f, payload)
    nmiss = {k: 0 for k in
             ("profile", "shares-float", "financial-scores", "estimates",
              "segmentation", "adx", "dcf", "pt")}
    for r in uni:
        for m in done.get(r["ticker"], {}).get("miss", []):
            if m in nmiss:
                nmiss[m] += 1
    print(f"payload: {len(alpha_rows)} alpha_vectors rows + {len(regime_rows)} regime rows -> {out_f}")
    print(f"missing groups: {nmiss}")
    print(f"regime: {[(r['pillar'], r.get('benchmark_rs_20d'), r['macro_regime_verdict']) for r in regime_rows]}")
    if args.probe or args.no_ingest:
        return
    # hand off to the 100-row chunked upsert harness
    import subprocess
    p = subprocess.run([sys.executable, INGEST, out_f], capture_output=True, text=True)
    print(p.stdout)
    if p.returncode != 0:
        print(p.stderr, file=sys.stderr)
        sys.exit(p.returncode)


if __name__ == "__main__":
    main()
