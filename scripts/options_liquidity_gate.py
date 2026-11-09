#!/usr/bin/env python3
"""options_liquidity_gate.py — Phase 2 options-queue graduation gate (2026-11-07).

Before ANY ticker graduates to the options delta queue
(~/.hermes/cron/options_delta_queue.db), its 45-DTE .35-.55-delta CALLS must
pass ALL THREE bars:
  1. spread <= 12%   ((ask-bid)/mid*100, mid fallback bid)
  2. open_interest >= 100
  3. iv < 150%       (feed iv is a FRACTION: 0.85 = 85%; guard >1.0-as-percent)

API contract (live-verified 2026-11-07):
  * the DEFAULT chain query returns ONLY the nearest expiry;
  * a specific tenor requires ?expiration=YYYY-MM-DD;
  * the date= parameter is silently IGNORED (returns the nearest expiry) —
    that is why the roadmap says never pass date= outside market hours; we
    never do. We pass expiration= only for the 35-60 DTE candidate tenors.
Strategy: expirations(sym) -> candidates in the 35-60 DTE window (else the
closest available, degraded=True) -> fetch each candidate chain (max 4 reqs)
-> evaluate .35-.55-delta CALLS; best candidate = min spread, tie-break OI.
Unquoted contracts (bid<=0 or ask<=0) = hard fail.

Usage: python3 options_liquidity_gate.py SYM1 SYM2 ... [--json] [-o OUTPATH]
Exit 0 always (cron-safe).
"""
import argparse
import datetime
import json
import os
import sys

sys.path.insert(0, os.path.expanduser("~/.hermes/scripts"))
import marketdata_client as md

SPREAD_MAX_PCT = 12.0
OI_MIN = 100
IV_MAX = 1.50          # 150% as a fraction
DTE_TARGET = 45
DTE_WINDOW = (35, 60)
DELTA_BAND = (0.35, 0.55)


def _num(v):
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _iv_fraction(iv):
    """Feed iv arrives as a fraction (0.85 = 85%). If a value > 3.0 arrives it
    is already-percent (142.0 = 142%); normalize to fraction."""
    f = _num(iv)
    if f is None:
        return None
    if f > 3.0:
        f = f / 100.0
    return f


def spread_pct(bid, ask, mid):
    bid, ask, mid = _num(bid), _num(ask), _num(mid)
    if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
        return None
    base = mid if (mid is not None and mid > 0) else bid
    return round((ask - bid) / base * 100.0, 2)


def _eval_band(sym, expiry, dte, degraded, band):
    """Score the delta-band candidates; pick min spread, tie-break higher OI."""
    cands = []
    for r in band:
        sp = spread_pct(r.get("bid"), r.get("ask"), r.get("mid"))
        oi = _num(r.get("openInterest"))
        cands.append((sp if sp is not None else 1e9, -(oi or 0), r, sp, oi))
    quoted = [c for c in cands if c[3] is not None]
    if not quoted:
        best = min(cands)[2]
        return {"symbol": sym, "pass": False, "degraded": degraded,
                "reason": "no_quoted_contract_in_delta_band",
                "expiry": expiry, "dte": dte,
                "strike": best.get("strike"), "delta": best.get("delta")}
    _, _, best, sp, oi = min(quoted, key=lambda c: (c[0], c[1]))
    iv = _iv_fraction(best.get("iv"))
    fails = []
    if sp > SPREAD_MAX_PCT:
        fails.append(f"spread {sp}% > {SPREAD_MAX_PCT}%")
    if oi is None or oi < OI_MIN:
        fails.append(f"OI {oi if oi is not None else 'n/a'} < {OI_MIN}")
    if iv is None:
        fails.append("iv missing")
    elif iv >= IV_MAX:
        fails.append(f"IV {iv*100:.0f}% >= 150%")
    under = _num(best.get("underlyingPrice"))
    return {
        "symbol": sym, "pass": not fails, "degraded": degraded,
        "reason": "; ".join(fails) if fails else "pass",
        "expiry": expiry, "dte": dte, "strike": best.get("strike"),
        "delta": best.get("delta"),
        "iv": round(iv, 4) if iv is not None else None,
        "bid": best.get("bid"), "ask": best.get("ask"), "mid": best.get("mid"),
        "spread_pct": sp, "open_interest": oi,
        "best_of": len(quoted), "underlying_price": under,
    }


def gate_symbol(sym, today=None, token=None, side="call"):
    """45-DTE liquidity gate for one side (call or put).
    CALLS: delta +0.35..+0.55. PUTS: delta -0.55..-0.35 (same distance from
    ATM, mirrored)."""
    today = today or datetime.date.today()
    side_lo, side_hi = DELTA_BAND if side == "call" else (-DELTA_BAND[1], -DELTA_BAND[0])
    exps = md.expirations(sym, token)
    if not exps:
        return {"symbol": sym, "pass": False, "degraded": False,
                "reason": "no_expirations_available"}

    def dte_of_date(s):
        try:
            return (datetime.date.fromisoformat(s[:10]) - today).days
        except Exception:
            return None

    dated = [(d, e) for e in exps if (d := dte_of_date(e)) is not None and d >= 7]
    if not dated:
        return {"symbol": sym, "pass": False, "degraded": False,
                "reason": "no_usable_expirations"}
    in_window = [(d, e) for d, e in dated if DTE_WINDOW[0] <= d <= DTE_WINDOW[1]]
    if in_window:
        candidates = sorted(in_window, key=lambda x: abs(x[0] - DTE_TARGET))[:4]
        degraded = False
    else:
        candidates = sorted(dated, key=lambda x: abs(x[0] - DTE_TARGET))[:2]
        degraded = True

    best_result = None
    for d, exp in candidates:
        rows = md.option_chain(sym, token, date=exp)
        if not rows:
            continue
        side_key = side[0].upper()
        calls = [r for r in rows
                 if (r.get("side") or "").upper().startswith(side_key)
                 and _num(r.get("strike")) is not None
                 and _num(r.get("delta")) is not None]
        if not calls:
            continue
        dte_here = int(_num(calls[0].get("dte")) or d)
        band = [r for r in calls
                if side_lo <= _num(r["delta"]) <= side_hi]
        if not band:
            band_lo = side_lo if side == "call" else side_hi
            band_hi = side_hi if side == "call" else side_lo
            best_result = {"symbol": sym, "pass": False, "degraded": degraded,
                           "reason": (f"no_{side}s_in_{band_lo}-{band_hi}"
                                      f"_delta_band_at_{dte_here}DTE"),
                           "expiry": exp, "dte": dte_here}
            continue
        result = _eval_band(sym, exp, dte_here, degraded, band)
        if best_result is None or result["pass"]:
            best_result = result
        if best_result and best_result["pass"]:
            break
    if best_result is None:
        return {"symbol": sym, "pass": False, "degraded": degraded,
                "reason": "all_candidate_tenors_empty"}
    return best_result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("symbols", nargs="+")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("-o", dest="out", default=None)
    ap.add_argument("--side", choices=["call", "put"], default="call",
                    help="delta band: call +0.35..+0.55 (default), put -0.55..-0.35")
    a = ap.parse_args()
    token = md.load_token()
    results = [gate_symbol(s, token=token, side=a.side) for s in a.symbols]
    if a.json:
        print(json.dumps(results, indent=1))
    else:
        for r in results:
            iv = r.get("iv")
            iv_s = "n/a" if iv is None else f"{iv*100:.0f}%"
            line = (f"{r['symbol']:<6} {'PASS' if r['pass'] else 'FAIL'}"
                    f"{' (degraded)' if r.get('degraded') else ''}"
                    f"  {r.get('dte')}DTE K{r.get('strike')} d={r.get('delta')}"
                    f" IV={iv_s} spread={r.get('spread_pct')}%"
                    f" OI={r.get('open_interest')}  :: {r.get('reason')}")
            print(line)
    if a.out:
        tmp = a.out + ".tmp"
        with open(tmp, "w") as fh:
            json.dump({"generated": datetime.datetime.utcnow().isoformat(),
                       "results": results}, fh, indent=1)
        os.replace(tmp, a.out)
        print(f"wrote {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
