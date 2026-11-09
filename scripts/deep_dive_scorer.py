#!/usr/bin/env python3
"""Agent 9 Deep-Dive Scorer. Scores all briefing tickers with bifurcated 0-100 track scoring."""
import json, os, sys
from pathlib import Path

BRIEFING = "/tmp/dd_briefing.json"
WL_STATE = "/home/jaren/.hermes/cron/watchlist_state.json"
OUT_SCORES = "/home/jaren/.hermes/cron/ticker_scores.json"
OUT_EVAL = "/home/jaren/.hermes/cron/evaluation_report.json"
DELTA_CACHE = "/home/jaren/.hermes/cron/dd_delta_cache.json"

def load(path, default=None):
    try:
        with open(path) as f:
            return json.load(f)
    except:
        return default

def save(path, obj):
    with open(path, 'w') as f:
        json.dump(obj, f, indent=2)

# Load data
briefing = load(BRIEFING, {})
delta = load(DELTA_CACHE, {})
wl_state = load(WL_STATE, {"version":"2.0","updated":"","tickers":{}})

judged_set = set(delta.get("judged", []))
promotion_flags = {}
for tid, info in wl_state.get("tickers", {}).items():
    if info.get("promote_to_deepdive"):
        promotion_flags[tid] = info

# All OK tickers from briefing (exclude ENTITY_MISMATCH and FETCH_ERROR)
tickers_dict = briefing.get("tickers", {})
entity_mismatches = []
invalid_rows = []
ok_rows = []
fetch_errors = []
near_ceiling_rows = []

for tid, row in tickers_dict.items():
    status = row.get("status")
    if status == "ENTITY_MISMATCH":
        entity_mismatches.append({"ticker": tid, "detail": row.get("detail", "")})
    elif status == "FETCH_ERROR":
        fetch_errors.append(tid)
    elif status == "INVALID_TICKER":
        invalid_rows.append(tid)
    elif status == "OK":
        ok_rows.append(tid)
        mcap = row.get("market_cap", 0)
        if mcap >= 200000000000:
            near_ceiling_rows.append(tid)

# Also check for mcap >= $300B that should be excluded
mcap_excluded = [tid for tid in ok_rows if tickers_dict[tid].get("market_cap", 0) >= 300000000000]

# Check entity drift (name vs quote_name)
entity_review = []
for tid in ok_rows:
    row = tickers_dict[tid]
    name = row.get("name", "")
    qname = row.get("quote_name", "")
    if qname and name and qname != name and not qname.endswith(name) and not name.endswith(qname):
        # Check if clearly different company
        if qname != name:
            entity_review.append({"ticker": tid, "name": name, "quote_name": qname})

# Scoring functions
def score_catalyst(context):
    """Score catalyst_100 from context."""
    if not context or len(context) == 0:
        return 0
    scores = []
    for item in context:
        direction = item.get("direction", "")
        materiality = item.get("materiality", "")
        confidence = item.get("confidence", "")
        headline = item.get("headline", "").lower()
        
        # Strong: direct materiality, high/med confidence, positive
        if direction == "positive" and materiality == "direct" and confidence in ("high", "medium"):
            scores.append(80)
        elif direction == "positive" and materiality == "direct" and confidence == "low":
            scores.append(55)
        elif direction == "positive" and materiality == "indirect":
            scores.append(35)
        elif direction == "negative" and materiality == "direct":
            scores.append(10)
        elif direction == "negative":
            scores.append(5)
        else:
            scores.append(25)
    
    # If multiple, take the strongest
    return max(scores) if scores else 0

def score_valuation(price, mcap, sector, industry):
    """Score valuation_100 from price/mcap perspective."""
    if not mcap or mcap <= 0:
        return 50
    if price is None or price <= 0:
        return 50
    
    # P/S approx for early-stage, P/E for mature
    mcap_b = mcap / 1e9
    price_t = price
    
    if mcap_b < 0.5:
        # Micro/nano: valuation is speculative, score from growth potential
        return 60
    elif mcap_b < 2:
        return 65
    elif mcap_b < 5:
        return 70
    elif mcap_b < 20:
        return 68
    elif mcap_b < 100:
        return 62
    else:
        return 55

def score_technical(row):
    """Score technical_100 from measured technicals."""
    tech = row.get("technical")
    if not tech:
        return None
    
    rsi14 = tech.get("rsi14")
    rvol = tech.get("rvol")
    pct_14d = tech.get("pct_14d")
    pct_5d = tech.get("pct_5d")
    
    if rsi14 is None and rvol is None:
        return None
    
    score = 50
    if rsi14 is not None:
        if rsi14 < 30:
            score += 25  # Oversold
        elif rsi14 < 40:
            score += 15
        elif rsi14 > 70:
            score -= 15  # Stretched
        elif rsi14 > 60:
            score -= 5
        else:
            score += 5  # Neutral
    
    if rvol is not None and rvol >= 3:
        score += 10  # Volume squeeze positive for long thesis
    elif rvol is not None and rvol >= 2:
        score += 5
    
    if pct_14d is not None:
        if pct_14d < -0.15:
            score += 5  # Significant pullback
        elif pct_14d > 0.20:
            score -= 5  # Already ran up
    
    return max(0, min(100, score))

def score_insider_conv(row):
    """Get insider conviction from insider block."""
    insider = row.get("insider")
    if not insider:
        return 0
    
    flow = insider.get("net_insider_flow", "")
    buys = insider.get("purchases", 0)
    sells = insider.get("sales", 0)
    
    if flow == "buy":
        return 15
    elif flow == "sell":
        return -10
    elif buys > 0 and sells == 0:
        return 10
    return 0

def score_track_a(row, catalyst_score):
    """Track A: Golden Tier Fortress. 30/20/20/30, total max 100."""
    bif = row.get("bifurcation", {})
    a1 = bif.get("track_a_p1", {})
    p1_engine = a1.get("p1_engine_30", 0)
    waiver = a1.get("p1_waiver_pts", 0)
    p1 = p1_engine + waiver  # Cap at 27
    
    # P2 Bottleneck & Moat (max 20)
    context = row.get("catalyst_context", [])
    has_moat = False
    has_competitors = False
    has_defensible = False
    
    for item in context:
        cat = item.get("category", "")
        headline = item.get("headline", "").lower()
        if "monopoly" in headline or "exclusive" in headline or "patent" in headline:
            has_defensible = True
        if "competition" in headline or "competitor" in headline or "market share" in headline:
            has_competitors = True
    
    if has_defensible:
        p2 = 18
    elif has_competitors:
        p2 = 8
    else:
        p2 = 12  # Default moderate
    
    # P3 Unrecognized Edge (max 20)
    has_ip = False
    has_new_product = False
    has_hidden = False
    
    for item in context:
        headline = item.get("headline", "").lower()
        if "ip" in headline or "patent" in headline or "proprietary" in headline:
            has_ip = True
        if "new product" in headline or "next-gen" in headline or "platform" in headline:
            has_new_product = True
        if "hidden" in headline or "underappreciated" in headline:
            has_hidden = True
    
    p3_base = 12
    if has_ip and has_new_product:
        p3 = 18
    elif has_ip or has_new_product:
        p3 = 14
    elif has_hidden:
        p3 = 10
    else:
        p3 = 6
    
    # P3 squeeze modifier
    tech = row.get("technical", {})
    rvol = tech.get("rvol", 0) or 0
    if rvol >= 3:
        p3 = min(20, p3 + 3)
    else:
        p3 = min(20, p3)
    
    # P4 Leading Inflection (max 30)
    pre_print = row.get("pre_print")
    
    # Check for 8K actuals
    _8k = bif.get("_8k", {})
    has_8k_actuals = bool(_8k.get("item_202"))
    
    if pre_print and not has_8k_actuals:
        # Pre-print gate: cap at 18
        p4_base = min(18, 22)  # At least 22-30 but capped
        # From context, estimate
        positive_briefs = sum(1 for c in context if c.get("direction") == "positive")
        if positive_briefs >= 3:
            p4 = 18
        elif positive_briefs >= 1:
            p4 = 14
        else:
            p4 = 8
    elif has_8k_actuals:
        guidance_raised = _8k.get("guidance_raised", False)
        margin_expansion = _8k.get("margin_expansion", False)
        if guidance_raised and margin_expansion:
            p4 = 28
        elif guidance_raised or margin_expansion:
            p4 = 24
        else:
            p4 = 20
    else:
        positive_briefs = sum(1 for c in context if c.get("direction") == "positive")
        if positive_briefs >= 4:
            p4 = 26
        elif positive_briefs >= 2:
            p4 = 22
        elif positive_briefs >= 1:
            p4 = 18
        else:
            p4 = 12
    
    # Pre-print cap enforcement
    if pre_print and not has_8k_actuals:
        p4 = min(18, p4)
    
    total = p1 + p2 + p3 + p4
    total = max(0, min(100, total))
    
    return {
        "score": total,
        "p1": p1, "p2": p2, "p3": p3, "p4": p4,
        "pillars": {"p1": p1, "p2": p2, "p3": p3, "p4": p4},
        "pass": total >= 75
    }

def score_track_b(row, catalyst_score):
    """Track B: Emerging Bottleneck. 20/25/30/25 + dilution kill."""
    bif = row.get("bifurcation", {})
    signals = bif.get("signals", {})
    b1_eng = bif.get("track_b_engine", {})
    runway = b1_eng.get("runway_gate_20", 10)
    
    # P1 Survival Runway (max 20)
    p1 = runway
    # Add up to 6 for grants/awards
    context = row.get("catalyst_context", [])
    grant_words = ["grant", "award", "nre", "prepay", "doe", "chips", "contract"]
    grant_count = 0
    for item in context:
        headline = item.get("headline", "").lower()
        if any(w in headline for w in grant_words):
            grant_count += 1
    
    if grant_count > 0:
        p1 = min(20, p1 + min(6, grant_count * 2))
    
    # Check for toxic convertible
    toxic = False
    for item in context:
        headline = item.get("headline", "").lower()
        if "toxic" in headline or "death spiral" in headline or "convertible" in headline:
            toxic = True
            break
    if toxic:
        p1 = 0
    
    # P2 Supply-Chain Chokepoint (max 25)
    has_allocation = False
    has_lead_time = False
    has_hyperscaler = False
    has_qualification = False
    
    for item in context:
        headline = item.get("headline", "").lower()
        if "allocation" in headline or "sold out" in headline or "shortage" in headline:
            has_allocation = True
        if "lead time" in headline or "wait list" in headline or "backlog" in headline:
            has_lead_time = True
        if "hyperscaler" in headline or "tier-1" in headline or "nvidia" in headline or "amd" in headline or "intel" in headline:
            has_hyperscaler = True
        if "qualification" in headline or "spec'd" in headline or "design win" in headline:
            has_qualification = True
    
    p2_count = sum([has_allocation, has_lead_time, has_hyperscaler, has_qualification])
    if p2_count >= 4:
        p2 = 22
    elif p2_count >= 2:
        p2 = 15
    elif p2_count >= 1:
        p2 = 8
    else:
        p2 = 4
    
    # P3 Unrealized Edge (max 30)
    has_design_win = False
    has_trojan = False
    has_monopoly = False
    has_hidden_ip = False
    
    for item in context:
        headline = item.get("headline", "").lower()
        if "design win" in headline:
            has_design_win = True
        if "trojan" in headline or "legacy" in headline and "new" in headline:
            has_trojan = True
        if "monopoly" in headline or "geopolitical" in headline:
            has_monopoly = True
        if "hidden ip" in headline or "proprietary" in headline:
            has_hidden_ip = True
    
    if has_design_win and has_hidden_ip:
        p3 = 26
    elif has_design_win or has_hidden_ip:
        p3 = 20
    elif has_trojan:
        p3 = 18
    elif has_monopoly:
        p3 = 22
    else:
        p3 = 8
    
    # P4 Pre-Revenue Inflection (max 25)
    drev = signals.get("deferred_revenue_delta_pct_qoq", 0)
    gm_bps = signals.get("gross_margin_qoq_bps", 0)
    _8k = bif.get("_8k", {})
    
    p4 = 10  # Base
    if drev is not None and drev > 0:
        if drev > 10:
            p4 += 8
        else:
            p4 += 4
    if gm_bps is not None and gm_bps > 200:
        p4 += 4
    if _8k.get("backlog"):
        p4 += 3
    
    # Check for tape-out/sample language
    for item in context:
        headline = item.get("headline", "").lower()
        if "tape-out" in headline or "sample" in headline or "engineering" in headline:
            p4 += 3
    
    p4 = min(25, max(0, p4))
    
    # Kill switch
    dilution_kill = signals.get("dilution_kill_8pct", False)
    psfv = signals.get("psfv", 1.0)
    psfv_carveout = signals.get("psfv_carveout", False)
    
    track_b_total = p1 + p2 + p3 + p4
    
    kill_penalty = 0
    if dilution_kill:
        if not psfv_carveout:
            kill_penalty = 30
        else:
            kill_penalty = 5  # Minor friction
    elif dilution_kill and psfv_carveout:
        kill_penalty = -5  # Don't deduct for accretive
    
    track_b_total = max(0, track_b_total - kill_penalty)
    # Cap at 20 if kill switch fires
    if kill_penalty >= 30:
        track_b_total = min(track_b_total, 20)
    
    return {
        "score": track_b_total,
        "p1": p1, "p2": p2, "p3": p3, "p4": p4,
        "pillars": {"p1": p1, "p2": p2, "p3": p3, "p4": p4},
        "kill_penalty": kill_penalty,
        "pass": track_b_total >= 75
    }

def score_track_c(row):
    """Track C: Mirage. 100 - severity - hype_penalty."""
    bif = row.get("bifurcation", {})
    c100 = bif.get("track_c_engine", {}).get("track_c_100", 50)
    
    # Hype penalty
    context = row.get("catalyst_context", [])
    positive_briefs = [c for c in context if c.get("direction") == "positive"]
    
    # Check if positive news pileup with zero substance
    has_milestones = False
    for item in positive_briefs:
        headline = item.get("headline", "").lower()
        if "milestone" in headline or "design win" in headline or "contract" in headline:
            has_milestones = True
    
    fundamentals_declining = True  # Default assumption
    if context:
        for item in context:
            if item.get("direction") == "positive" and item.get("materiality") == "direct":
                fundamentals_declining = False
    
    if not has_milestones and positive_briefs and len(positive_briefs) > 2:
        hype = 25
    elif positive_briefs and len(positive_briefs) > 0:
        hype = 10
    else:
        hype = 0
    
    score = max(0, 100 - c100 - hype)
    return {
        "score": score,
        "c100": c100,
        "hype_penalty": hype,
        "is_mirage": score <= 35
    }

# Process each ticker
rankings = []
entity_mismatch_list = []
invalid_list = []
mcap_excluded_list = []
entity_review_list = []

for tid in ok_rows:
    row = tickers_dict[tid]
    
    # Check mcap ceiling
    mcap = row.get("market_cap", 0)
    if mcap >= 300000000000:
        mcap_excluded_list.append({"ticker": tid, "market_cap": mcap, "reason": "exited_mcap_ceiling"})
        continue
    
    # Check entity drift
    name = row.get("name", "")
    qname = row.get("quote_name", "")
    if qname and name and qname != name:
        entity_review_list.append({"ticker": tid, "name": name, "quote_name": qname, "status": "entity_review"})
        continue
    
    financial_100 = row.get("financial_100", {})
    fin_total = financial_100.get("adjusted", financial_100.get("financial_100"))
    fin_coverage = financial_100.get("data_coverage", 1.0)
    
    catalyst_100 = score_catalyst(row.get("catalyst_context", []))
    
    # Valuation and technical only for promotion queue and HIGH tier
    is_high_tier = tid in promotion_flags or (catalyst_100 >= 40 and fin_total is not None and fin_total >= 65)
    
    valuation_100 = None
    technical_100 = None
    
    if is_high_tier:
        valuation_100 = score_valuation(
            row.get("price"), 
            row.get("market_cap"),
            row.get("sector", ""),
            row.get("industry", "")
        )
        technical_100 = score_technical(row)
    
    # Bifurcated scoring
    track_a = score_track_a(row, catalyst_100)
    track_b = score_track_b(row, catalyst_100)
    track_c = score_track_c(row)
    
    best_track = max(track_a["score"], track_b["score"])
    
    # Write watchlist note
    note_parts = []
    if row.get("technical"):
        t = row["technical"]
        if t.get("rsi14"):
            note_parts.append(f"RSI {t['rsi14']}")
        if t.get("rvol"):
            note_parts.append(f"RVOL {t['rvol']}")
    if track_a["score"] >= 75:
        note_parts.append(f"A={track_a['score']}")
    if track_b["score"] >= 75:
        note_parts.append(f"B={track_b['score']}")
    note_parts.append(f"fin={fin_total} cat={catalyst_100}")
    watchlist_note = " | ".join(note_parts)
    
    entry = {
        "ticker": tid,
        "name": row.get("name", ""),
        "sector": row.get("sector", ""),
        "industry": row.get("industry", ""),
        "market_cap": row.get("market_cap", 0),
        "price": row.get("price", 0),
        "price_change_pct": row.get("price_change_pct", 0),
        "entity_check": row.get("entity_check", "ok"),
        "data_coverage": fin_coverage,
        "pillar_scores": {
            "financial_100": fin_total,
            "catalyst_100": catalyst_100,
            "valuation_100": valuation_100,
            "technical_100": technical_100
        },
        "financial_100": fin_total,
        "financial_data_coverage": fin_coverage,
        "bifurcation": {
            "track_a_score": track_a["score"],
            "track_a_pillars": track_a["pillars"],
            "track_a_pass": track_a["pass"],
            "track_b_score": track_b["score"],
            "track_b_pillars": track_b["pillars"],
            "track_b_kill_penalty": track_b.get("kill_penalty", 0),
            "track_b_pass": track_b["pass"],
            "track_c_score": track_c["score"],
            "track_c_is_mirage": track_c["is_mirage"],
            "track_c_severity": track_c["c100"],
            "track_c_hype_penalty": track_c["hype_penalty"]
        },
        "track_a_score": track_a["score"],
        "track_b_score": track_b["score"],
        "track_c_score": track_c["score"],
        "best_track_score": best_track,
        "track_a_pillars": track_a["pillars"],
        "track_b_pillars": track_b["pillars"],
        "track_a_pillars_note": f"p1={track_a['p1']} p2={track_a['p2']} p3={track_a['p3']} p4={track_a['p4']}",
        "track_b_pillars_note": f"p1={track_b['p1']} p2={track_b['p2']} p3={track_b['p3']} p4={track_b['p4']}",
        "track_c_reasons": f"c100={track_c['c100']} hype={track_c['hype_penalty']}",
        "catalyst_100": catalyst_100,
        "valuation_100": valuation_100,
        "technical_100": technical_100,
        "watchlist_note": watchlist_note,
        "tier": "HIGH" if is_high_tier else ("MODERATE" if best_track >= 55 else "BELOW_55"),
        "categories": list(set([c.get("category", "") for c in row.get("catalyst_context", []) if c.get("category")]))
    }
    rankings.append(entry)

# Sort by best_track_score descending
rankings.sort(key=lambda x: x["best_track_score"] if x["best_track_score"] is not None else 0, reverse=True)

# Save ticker_scores.json
save(OUT_SCORES, {
    "generated": "2026-11-09T07:17:51+00:00",
    "engine_session": "7c273d73-5f3d-46",
    "total_ranked": len(rankings),
    "entity_mismatches": entity_mismatch_list,
    "fetch_errors": fetch_errors,
    "mcap_policy_excluded": mcap_excluded_list,
    "entity_review": entity_review_list,
    "near_ceiling": near_ceiling_rows,
    "rankings": rankings
})

print(f"Scored {len(rankings)} tickers -> {OUT_SCORES}")
print(f"Entity mismatches: {[e['ticker'] for e in entity_mismatch_list]}")
print(f"Mcap excluded: {[e['ticker'] for e in mcap_excluded_list]}")
print(f"Entity review: {[e['ticker'] for e in entity_review_list]}")
print(f"Top 10 by best_track_score: {[(r['ticker'], r['best_track_score'], r['track_a_score'], r['track_b_score']) for r in rankings[:10]]}")

# Count A/B pass
a_pass = sum(1 for r in rankings if r["track_a_score"] >= 75)
b_pass = sum(1 for r in rankings if r["track_b_score"] >= 75)
both_pass = sum(1 for r in rankings if r["track_a_score"] >= 75 and r["track_b_score"] >= 75)
c_mirage = sum(1 for r in rankings if r["track_c_score"] <= 35 and r["track_a_score"] < 75 and r["track_b_score"] < 75)
print(f"Track A pass: {a_pass}, Track B pass: {b_pass}, Both: {both_pass}, Mirage: {c_mirage}")
