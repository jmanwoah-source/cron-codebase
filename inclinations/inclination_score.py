#!/usr/bin/env python3
"""Inclination scoring — the single 0-100 'early-advantage' score per ticker.

Deterministic, 0-LLM. Combines every signal source into one comparable number so the
user can start early research at a fixed threshold (default 50 = '50% conviction').

Design goal (Karpathy style): catch a company that is ONTO SOMETHING (a technology,
a process, or a hole it fills) BEFORE the mainstream news catches up. A single strong
early-advantage edge should clear 50; generic noise (one bland news brief) should not.

Components (sum to 100, all tunable in pipeline_config.json -> "inclination"):
  1. news_corroboration   0-25  independent news briefs (direct mentions)
  2. early_advantage      0-30  the 'onto something' edge (strongest single signal):
                                contract/market-cap, R&D trending, design-win transcript,
                                patent filing, or tech-gap filler stage
  3. source_diversity     0-20  distinct signal categories touching the ticker
  4. commercialization    0-15  proximity to revenue (shipping > first_customer > pilot > academic)
  5. displacement_edge    0-10  a tech gap names this ticker advantaged AND names a doomed incumbent

Threshold (default 50) -> notification. The notification always carries the DOOMED
contrast: the incumbent the advantaged ticker is displacing (or 'none identified').
"""
from __future__ import annotations
import json
import os

HERE = os.path.expanduser("~/.hermes/cron")


def _load_config():
    try:
        cfg = json.load(open(os.path.join(HERE, "pipeline_config.json")))
        return cfg.get("inclination", {})
    except Exception:
        return {}


CFG = _load_config()
THRESHOLD = CFG.get("notify_threshold", 50)
MAX_CAP = CFG.get("max_market_cap", 150e9)          # $150B ceiling (persona scope)
MIN_CAP = CFG.get("min_market_cap", 5e7)            # >= $50M
STAGE_POINTS = CFG.get("stage_points", {
    "shipping": 15, "first_customer": 13, "pilot": 9, "academic": 4, "unknown": 0
})


def _band(value, bands, default=0):
    """bands = [(floor, points), ...] descending by floor; return first matching."""
    if value is None:
        return default
    for floor, pts in bands:
        if value >= floor:
            return pts
    return default


def score_news(brief_count):
    return _band(brief_count, [(5, 25), (3, 18), (2, 12), (1, 6)], 0)


def score_early_advantage(sig, is_advantaged=False, gap_stage=None):
    """Strongest single 'onto something' edge. sig = early-advantage facts for a ticker."""
    best = 0
    # (a) contract-to-market-cap ratio (revenue disconnect) — highest-signal ratio
    cm = sig.get("contract_to_market_cap")
    if cm is not None:
        best = max(best, _band(cm, [(0.5, 30), (0.3, 24), (0.15, 16), (0.05, 8)]))
    # (b) R&D/revenue trending up 3 consecutive quarters
    if sig.get("rd_trending_up"):
        best = max(best, 15)
    # (c) design-win / commercialize language in a recent transcript
    if sig.get("design_win"):
        best = max(best, 24)
    # (d) recent patent filings in a target gap (assignee = this ticker)
    pf = sig.get("patent_filings") or 0
    if pf >= 2:
        best = max(best, 26)
    elif pf == 1:
        best = max(best, 18)
    # (e) the IDENTIFIED advantaged play in a tech gap (the named 'onto something' ticker)
    if is_advantaged and gap_stage in STAGE_POINTS and gap_stage != "unknown":
        # lead play with a real stage = a strong early-advantage edge even before news catches up
        best = max(best, 24)
    # (f) generic tech-gap filler stage (not the lead)
    stage = (sig.get("gap_stage") or "unknown").lower()
    if stage in STAGE_POINTS and stage != "unknown":
        best = max(best, min(22, STAGE_POINTS[stage] + 9))
    return best


def score_diversity(categories):
    n = len(set(categories))
    return _band(n, [(5, 20), (4, 16), (3, 12), (2, 7), (1, 3)], 0)


def score_commercialization(stage):
    return STAGE_POINTS.get((stage or "unknown").lower(), 0)


def score_displacement(gap, doomed, doomed_note):
    # displacement confirmed when a gap names this ticker AND a doomed incumbent (list or note)
    if gap and (doomed or (doomed_note and doomed_note.strip())):
        return 10
    if gap:
        return 4
    return 0


def inclination_score(ticker, facts):
    """facts = {
       'brief_count': int,
       'market_cap': float|None,
       'sector': str,
       'sources': {category: bool},     # which signal categories touch this ticker
       'early': {contract_to_market_cap, rd_trending_up, design_win, patent_filings, gap_stage},
       'gap_stage': str,
       'gap': str|None, 'doomed': [ {ticker, company} ],
     }
    Returns dict with score + component breakdown + why.
    """
    early = facts.get("early", {})
    gap_stage = facts.get("gap_stage", early.get("gap_stage", "unknown"))
    comp = {
        "news_corroboration": score_news(facts.get("brief_count", 0)),
        "early_advantage": score_early_advantage(
            early, is_advantaged=facts.get("is_advantaged", False), gap_stage=gap_stage),
        "source_diversity": score_diversity([c for c, on in facts.get("sources", {}).items() if on]),
        "commercialization": score_commercialization(gap_stage),
        "displacement_edge": score_displacement(facts.get("gap"), facts.get("doomed"), facts.get("gap_doomed_note")),
    }
    total = sum(comp.values())
    # hard scope guard: out-of-scope market cap is flagged, not scored as advantaged
    mc = facts.get("market_cap")
    scope = "in"
    if mc is not None and (mc > MAX_CAP or mc < MIN_CAP):
        scope = "out_of_scope"
    why = []
    if comp["early_advantage"] >= 16:
        why.append("strong early-advantage edge")
    if comp["news_corroboration"] >= 18:
        why.append("multi-source news corroboration")
    if comp["displacement_edge"] >= 10:
        why.append(f"displaces incumbent in {facts.get('gap') or 'its gap'}")
    if comp["source_diversity"] >= 16:
        why.append("wide source diversity")
    if facts.get("is_advantaged"):
        why.append(f"identified advantaged play in {facts.get('gap') or 'a tech gap'}")
    if not why:
        why.append("single-signal; watchlist only")
    return {
        "ticker": ticker,
        "inclination_score": total,
        "notify": total >= THRESHOLD and scope == "in",
        "scope": scope,
        "components": comp,
        "why": why,
        "gap": facts.get("gap"),
        "gap_stage": facts.get("gap_stage", early.get("gap_stage", "unknown")),
        "gap_advantaged": facts.get("gap_advantaged"),
        "gap_doomed_note": facts.get("gap_doomed_note", ""),
        "is_advantaged": facts.get("is_advantaged", False),
        "is_gap_filler": facts.get("is_gap_filler", False),
        "doomed": facts.get("doomed", []),
    }


def doomed_names(facts):
    d = facts.get("doomed") or []
    return ", ".join((x.get("company") or x.get("ticker")) for x in d) or "none identified"
