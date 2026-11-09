#!/usr/bin/env python3
"""Inclination aggregator + notifier — unifies all signal sources into one
0-100 'early-advantage' score per ticker and writes the notification set.

Inputs (all optional; each source contributes if present):
  - signal_briefs.json / signal_briefs_history.json  (news corroboration)
  - ticker_scores.json                                (financial + conviction)
  - watchlist_state.json                              (existing status)
  - inclinations/screen.py output                     (NEW candidates: rvol, insider, FCF)
  - inclinations/transcripts.py output                (R&D trend, design-win language)
  - inclinations/ip_signal.py output                  (patent filings, 8-K IP flags)
  - wiki/entities/tech_gaps/*.md                      (gap stage + doomed incumbent)
  - inclinations/contract_signals.json                (contract/market-cap ratios)

Output:
  - inclinations/inclination_scores.json  (all tickers, scored)
  - inclinations/inclination_alerts.md    (>= threshold, with DOOMED contrast, dated)
  - (optional) prints a compact alert block for cron to deliver

Run:  python3 pipeline_engine.py inclination
"""
from __future__ import annotations
import glob
import json
import os
import re
import sys
from datetime import datetime, timezone

HERE = os.path.expanduser("~/.hermes/cron")
WIKI = os.path.expanduser("~/wiki")          # canonical wiki (NOT ~/.hermes/wiki)
INC = os.path.join(HERE, "inclinations")

sys.path.insert(0, INC)
from inclination_score import inclination_score, THRESHOLD  # noqa: E402


def _now():
    return datetime.now(timezone.utc).isoformat()


def _read(path, default):
    try:
        return json.load(open(path))
    except Exception:
        return default


def _valid_ticker(t):
    """Briefs sometimes carry non-tickers ('PRIVATE', 'N/A') — drop them."""
    return bool(re.match(r"^[A-Z][A-Z0-9.\-]{0,8}$", t)) and t not in (
        "PRIVATE", "N/A", "TBD", "TICKER", "SYMBOL")


# ---------------------------------------------------------------- sources
def load_news_facts():
    """brief_count + categories per ticker from briefs history (engine-owned)."""
    hist = _read(os.path.join(HERE, "signal_briefs_history.json"), {})
    briefs = hist.get("briefs", []) if isinstance(hist, dict) else hist
    if not isinstance(briefs, list):
        briefs = []
    facts = {}
    for b in briefs:
        if not isinstance(b, dict):
            continue
        cat = b.get("category", "")
        direct = set(b.get("tickers_benefit", [])) | set(b.get("tickers_affected", []))
        for t in direct:
            if not _valid_ticker(t):
                continue
            f = facts.setdefault(t, {"brief_count": 0, "sources": {}})
            f["brief_count"] += 1
            if cat:
                f["sources"][cat] = True
    return facts


def load_ticker_meta():
    """market_cap + sector + conviction from the latest deepdive briefing / scores."""
    meta = {}
    dd = _read("/tmp/dd_briefing.json", {})
    if isinstance(dd, dict):
        for t, v in dd.get("tickers", {}).items():
            if isinstance(v, dict):
                meta[t] = {"market_cap": v.get("market_cap"), "sector": v.get("sector"),
                           "name": v.get("name"), "financial_100": (v.get("financial_100") or {}).get("financial_100")}
    scores = _read(os.path.join(HERE, "ticker_scores.json"), {})
    if isinstance(scores, dict):
        # CONFIRMED shape (2026-10-17): {"scores": [ {ticker, sector, market_cap, ...} ]}
        # — the old code only looked for "ranked"/"rankings" and silently read nothing.
        entries = (scores.get("scores") or scores.get("ranked") or scores.get("rankings") or [])
        for entry in entries:
            if isinstance(entry, dict) and entry.get("ticker"):
                t = entry["ticker"].upper()
                m = meta.setdefault(t, {})
                m.setdefault("market_cap", entry.get("market_cap"))
                m.setdefault("sector", entry.get("sector"))
                if entry.get("name"):
                    m.setdefault("name", entry.get("name"))
                if "conviction" in entry:
                    m["conviction"] = entry.get("conviction")
    return meta


def load_screen_facts():
    """NEW candidates from the deterministic screen (target-sector small-caps)."""
    out = _read(os.path.join(INC, "screen_out.json"), {})
    facts = {}
    items = out.get("candidates", out if isinstance(out, list) else [])
    if isinstance(items, list):
        for c in items:
            if not isinstance(c, dict):
                continue
            t = (c.get("ticker") or "").upper()
            if not t:
                continue
            f = facts.setdefault(t, {"early": {}, "sources": {}})
            f["sources"]["screen"] = True
            if c.get("rvol") and c["rvol"] >= 2:
                f["early"]["rvol_spike"] = True
            if c.get("insider_buys_60d"):
                f["early"]["insider_buy"] = True
            if c.get("fcf_positive"):
                f["early"]["fcf_positive"] = True
            f.setdefault("market_cap", c.get("market_cap"))
            f.setdefault("sector", c.get("sector"))
            if c.get("name"):
                f.setdefault("name", c.get("name"))
    return facts


def load_transcript_facts():
    out = _read(os.path.join(INC, "transcripts_out.json"), {})
    facts = {}
    for t, v in (out.items() if isinstance(out, dict) else []):
        f = facts.setdefault(t.upper(), {"early": {}, "sources": {}})
        f["sources"]["earnings"] = True
        if v.get("rd_trending_up"):
            f["early"]["rd_trending_up"] = True
        if v.get("design_win"):
            f["early"]["design_win"] = True
    return facts


def load_ip_facts():
    out = _read(os.path.join(INC, "ip_out.json"), {})
    facts = {}
    # patent filings keyed by company -> map to ticker when we can
    for pf in out.get("patent_filings", []):
        comp = (pf.get("company") or "")
        f = facts.setdefault(comp.upper(), {"early": {}, "sources": {}})
        f["sources"]["patent"] = True
        f["early"]["patent_filings"] = f.get("early", {}).get("patent_filings", 0) + 1
        f["early"]["patent_area"] = pf.get("tech_area")
    for flag in out.get("eight_k_ip_flags", []):
        t = (flag.get("ticker") or "").upper()
        if not t:
            continue
        f = facts.setdefault(t, {"early": {}, "sources": {}})
        f["sources"]["sec"] = True
        f["early"]["eight_k_ip"] = flag.get("matched_term")
    return facts


def parse_tech_gaps():
    """Parse tech-gap wiki files into per-ticker gap facts.

    Captures, per gap: fillers (ticker->stage), the ADVANTAGED ticker (the one 'onto
    something'), and the DOOMED incumbent description (the contrast the notification must carry).
    """
    gap_map = {}
    stage_order = {"academic": 0, "pilot": 1, "first_customer": 2, "shipping": 3}
    for path in glob.glob(os.path.join(WIKI, "entities", "tech_gaps", "*.md")):
        if os.path.basename(path) in ("SCHEMA.md", "CASCADE.md", "INDEX.md"):
            continue
        try:
            text = open(path).read()
        except Exception:
            continue
        title_m = re.search(r"^#\s+(.+)$", text, re.M)
        gap = title_m.group(1).strip() if title_m else os.path.basename(path)
        # fillers: | TICKER | Company | stage | ... |   (stage is a known keyword)
        fillers = []
        for row in re.findall(r"^\|\s*([A-Z][A-Z0-9.\-]{1,9})\s*\|[^|]*\|\s*([a-z_]+)\s*\|", text, re.M):
            tick, stage = row[0], row[1]
            if stage in stage_order:
                fillers.append((tick, stage))
        if not fillers:
            continue
        # advantaged ticker (the lead "onto something")
        adv_m = re.search(r"Advantaged:?\**\s*[:\-]?\s*\*{0,2}([A-Z][A-Z0-9.\-]{1,9})", text)
        advantaged = adv_m.group(1).upper() if adv_m else None
        # doomed description = the full bullet under 'DOOMED incumbent' (may wrap lines)
        doomed_note = ""
        doom_m = re.search(r"DOOMED incumbent:?\**\s*[:\-]?\s*(.+?)(?=\n\s*\n|\n\s*- |\n#|$)", text, re.S)
        if doom_m:
            doomed_note = re.sub(r"\s+", " ", doom_m.group(1)).strip()[:400]
        for tick, stage in fillers:
            g = gap_map.setdefault(tick.upper(), {})
            g["gap"] = gap
            # keep the most-advanced stage seen for this ticker across gaps
            if stage_order.get(stage, 0) > stage_order.get(g.get("gap_stage", "academic"), 0):
                g["gap_stage"] = stage
            g["gap_advantaged"] = advantaged
            g["gap_doomed_note"] = doomed_note
    return gap_map


def load_contract_facts():
    """Pre-computed contract/market-cap ratios (written by the contract parser)."""
    out = _read(os.path.join(INC, "contract_signals.json"), {})
    facts = {}
    for t, ratio in (out.items() if isinstance(out, dict) else []):
        f = facts.setdefault(t.upper(), {"early": {}, "sources": {}})
        f["sources"]["contract"] = True
        f["early"]["contract_to_market_cap"] = ratio
    return facts


# ---------------------------------------------------------------- name backfill
NAME_CACHE = os.path.join(INC, "name_cache.json")

# FMP profile-symbol sometimes resolves an ambiguous symbol to a NON-EQUITY
# (ETF / mutual fund / bond series). Caching that name poisons every later
# output for the ticker (e.g. "NXP" -> "Nuveen Select Tax-Free Income
# Portfolio"). Any companyName matching this is rejected and never cached.
ETF_FUND_RE = re.compile(
    r"\b(etf|etn|etf trust|index fund|mutual fund|exchange[- ]traded fund"
    r"|income portfolio|the new .*? fund|series due"
    r"|junior subordinated|subordinated notes|depositary share|depository share)\b", re.I)


def backfill_names(meta, priority_order=None):
    """Fix null `name` AND null `market_cap` in alerts via profile-symbol lookups.

    Names are cached to inclinations/name_cache.json so repeat lookups are
    0-FMP. `priority_order` (list of tickers) is honored first so the alert set
    (gap fillers, news-briefed) gets filled before low-score stragglers consume
    the budget. A ticker is fetched if its name is missing, OR if it has a name
    but no market_cap yet (the $150B scope guard can't run without a cap).
    Tickers whose market_cap we already tried-and-confirmed-missing, or that
    resolved to a poison ETF/fund, are NOT re-fetched every cycle.
    """
    cache = _read(NAME_CACHE, {})

    def _needs(t):
        m = meta.get(t)
        c = cache.get(t, {})
        if c.get("poisoned"):
            return False                 # known ETF/fund — not a real equity, stop
        if not isinstance(m, dict):
            return False
        if not (m.get("name") or c.get("name")):
            return True                  # no name at all
        if not (m.get("market_cap") or c.get("market_cap")):
            return not c.get("mcap_attempted")   # name known, cap unknown & untried
        return False

    need = [t for t in meta if _needs(t)]
    if not need:
        return
    # alert-set tickers (priority_order) get uncapped backfill (they are few:
    # gap fillers + news-briefed); the remainder is capped at 10 calls/run.
    prio_set = set(priority_order or [])
    alert_need = [t for t in need if t in prio_set]
    other_need = [t for t in need if t not in prio_set]
    plan = alert_need + other_need[:10]
    from fmp import fmp_session, fmp_call
    if not plan:
        return
    sid = fmp_session()
    fetched = 0
    for i, t in enumerate(plan):
        r = fmp_call(sid, "company", {"endpoint": "profile-symbol", "symbol": t.upper()}, 200 + i)
        fetched += 1
        rows = r if isinstance(r, list) else (r.get("data") if isinstance(r, dict) else None)
        if isinstance(rows, list) and rows and isinstance(rows[0], dict):
            p = rows[0]
            nm = p.get("companyName")
            if nm and ETF_FUND_RE.search(nm):
                # poison response — never cache the name; mark so we stop retrying
                cache[t] = {"name": None, "poisoned": nm, "mcap_attempted": _now(),
                            "ts": _now()}
                continue
            if nm:
                # profile-symbol DOES carry marketCap + sector (+ cik) —
                # persist all so the next run is 0-FMP for this ticker
                cache[t] = {"name": nm, "sector": p.get("sector"), "cik": p.get("cik"),
                            "market_cap": p.get("marketCap"), "ts": _now()}
                if not isinstance(p.get("marketCap"), (int, float)):
                    cache[t]["mcap_attempted"] = _now()   # confirmed missing — don't retry
                m = meta.setdefault(t, {})
                m["name"] = nm
                if not m.get("market_cap") and isinstance(p.get("marketCap"), (int, float)):
                    m["market_cap"] = float(p["marketCap"])
                if not m.get("sector") and p.get("sector"):
                    m["sector"] = p["sector"]
    if fetched:
        try:
            json.dump(cache, open(NAME_CACHE, "w"), indent=1)
        except Exception:
            pass
    # apply any remaining cached names (+ cached mcap/sector)
    for t in meta:
        c = cache.get(t) or {}
        if not meta[t].get("name") and c.get("name"):
            meta[t]["name"] = c["name"]
        if not meta[t].get("market_cap") and isinstance(c.get("market_cap"), (int, float)):
            meta[t]["market_cap"] = float(c["market_cap"])
        if not meta[t].get("sector") and c.get("sector"):
            meta[t]["sector"] = c["sector"]


# ---------------------------------------------------------------- aggregate
def aggregate():
    news = load_news_facts()
    meta = load_ticker_meta()
    screen = load_screen_facts()
    transcripts = load_transcript_facts()
    ip = load_ip_facts()
    gaps = parse_tech_gaps()
    contracts = load_contract_facts()

    universe = set()
    for src in (news, meta, screen, transcripts, ip, gaps, contracts):
        if isinstance(src, dict):
            universe |= set(src.keys())
    universe = {t for t in universe if _valid_ticker(t)}
    # meta must cover the whole universe so backfill_names sees every ticker
    for t in universe:
        meta.setdefault(t, {})
    # fill alert-set names first: gap fillers, then news-briefed, then rest
    prio = [t for t in sorted(universe) if gaps.get(t)] + \
           [t for t in sorted(universe) if news.get(t)] + sorted(universe)
    backfill_names(meta, priority_order=prio)

    results = []
    for t in sorted(universe):
        facts = {"brief_count": 0, "sources": {}, "early": {}}
        # merge facts from each source (dict-union, sources flags OR)
        for src in (news, screen, transcripts, ip, contracts, gaps):
            v = src.get(t) if isinstance(src, dict) else None
            if not isinstance(v, dict):
                continue
            for k, val in v.items():
                if k == "sources":
                    facts["sources"].update(val)
                elif k == "early":
                    facts["early"].update(val)
                else:
                    facts.setdefault(k, val)
        m = meta.get(t, {})
        facts["market_cap"] = facts.get("market_cap", m.get("market_cap"))
        facts["sector"] = m.get("sector")
        facts["name"] = m.get("name")
        # gap facts (from tech-gap wiki): advantaged ticker + doomed note
        gf = gaps.get(t)
        if gf:
            facts["gap"] = gf.get("gap")
            facts.setdefault("gap_stage", gf.get("gap_stage", "unknown"))
            facts["gap_advantaged"] = gf.get("gap_advantaged")
            facts["gap_doomed_note"] = gf.get("gap_doomed_note", "")
            # if this ticker is a filler but not the lead, still score its stage
            facts["is_gap_filler"] = True
            facts["is_advantaged"] = (gf.get("gap_advantaged") == t)
        sc = inclination_score(t, facts)
        sc["market_cap"] = facts.get("market_cap")
        sc["sector"] = facts.get("sector")
        sc["name"] = facts.get("name")
        results.append(sc)

    results.sort(key=lambda x: -x["inclination_score"])
    return results


# ---------------------------------------------------------------- notify
def write_outputs(results):
    os.makedirs(INC, exist_ok=True)
    alerts = [r for r in results if r["notify"]]
    alerts.sort(key=lambda x: -x["inclination_score"])

    scores_path = os.path.join(INC, "inclination_scores.json")
    with open(scores_path, "w") as f:
        json.dump({"generated": _now(), "threshold": THRESHOLD,
                   "tickers": results, "alerts": alerts}, f, indent=1)

    alert_path = os.path.join(INC, "inclination_alerts.md")
    lines = [f"# Inclination Alerts — {_now()[:16]}",
             f"Threshold {THRESHOLD} (50% conviction). {len(alerts)} alert(s).", ""]
    if not alerts:
        lines.append("_No tickers cleared the threshold this cycle._")
    for r in alerts:
        lines.append(f"## {r['ticker']} — inclination {r['inclination_score']}/100")
        lines.append(f"- Company: {r.get('name') or '?'} · {r.get('sector') or '?'} · mcap {_mcap(r.get('market_cap'))}")
        lines.append(f"- Why: {', '.join(r['why'])}")
        comp = r["components"]
        lines.append(f"- Components: early-adv {comp['early_advantage']} · news {comp['news_corroboration']} · "
                     f"diversity {comp['source_diversity']} · commercial {comp['commercialization']} · "
                     f"displacement {comp['displacement_edge']}")
        if r.get("gap"):
            lines.append(f"- Onto (gap): **{r['gap']}** @ stage `{r.get('gap_stage','?')}`")
        lines.append(f"- **DOOMED vs advantaged:** {doomed_line(r)}")
        lines.append("")
    with open(alert_path, "w") as f:
        f.write("\n".join(lines))

    return alerts


def _mcap(mc):
    if not mc:
        return "?"
    return f"${mc/1e9:.2f}B" if mc >= 1e9 else f"${mc/1e6:.0f}M"


def doomed_line(r):
    """Render the DOOMED-vs-advantaged contrast (the mandated notification note)."""
    note = (r.get("gap_doomed_note") or "").strip()
    gap = r.get("gap") or "the gap"
    adv_tick = (r.get("gap_advantaged") or r["ticker"]).upper()
    adv_name = r.get("name") or r["ticker"]
    if r.get("is_advantaged"):
        lead = f"**{adv_name}** ({adv_tick}) is the advantaged play in {gap}."
    else:
        lead = f"{r['ticker']} is a gap-filler in {gap}; the lead advantaged play is {adv_tick}."
    if note:
        return f"{lead} DOOMED: {note}"
    return f"{lead} (no incumbent identified — additive, not displacing)."


def main():
    results = aggregate()
    alerts = write_outputs(results)
    print(json.dumps({
        "generated": _now(), "threshold": THRESHOLD,
        "tickers_scored": len(results), "alerts": len(alerts),
        "top": [(r["ticker"], r["inclination_score"]) for r in results[:10]],
        "alerting": [(r["ticker"], r["inclination_score"], r.get("gap")) for r in alerts],
        "alerts_file": os.path.join(INC, "inclination_alerts.md"),
    }, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
