#!/usr/bin/env python3
"""Tier list writers — read ticker_scores.json, write golden_tier_list.md, emerging_edge_list.md, mirage_short_list.md."""
import json, os, shutil
from datetime import datetime, timezone

SCORES_FILE = "/home/jaren/.hermes/cron/ticker_scores.json"
GOLDEN_TIER = "/home/jaren/wiki/golden_tier_list.md"
EMERGING_EDGE = "/home/jaren/wiki/emerging_edge_list.md"
MIRAGE_LIST = "/home/jaren/wiki/mirage_short_list.md"
ARCHIVED_DIR = "/home/jaren/wiki/archived/"

TIER_PASS = 75  # Track A or Track B >= 75 = pass

with open(SCORES_FILE) as f:
    data = json.load(f)

results = data.get("ticker_scores", [])

# Classify
high_a = [r for r in results if r.get("track_a_score", 0) >= TIER_PASS]
high_b = [r for r in results if r.get("track_b_score", 0) >= TIER_PASS]
mirage = [r for r in results if r.get("is_mirage", False) and r.get("track_a_score", 0) < TIER_PASS and r.get("track_b_score", 0) < TIER_PASS]

# Double-passers (both A and B >= 75)
double_pass = set(r["ticker"] for r in high_a) & set(r["ticker"] for r in high_b)

def write_tier_list(path, title, entries, subtitle=""):
    """Write a markdown tier list."""
    lines = [f"# {title}", f"", f"_Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC_", ""]
    if subtitle:
        lines.append(f"**{subtitle}**")
        lines.append("")
    
    if not entries:
        lines.append("_No tickers currently qualify._")
        lines.append("")
    else:
        lines.append(f"## Top Candidates ({len(entries)} total)")
        lines.append("")
        lines.append("| Rank | Ticker | Name | Fin100 | Best | A | B | C | Category |")
        lines.append("|------|--------|------|--------|------|---|---|---|----------|")
        
        for i, r in enumerate(entries, 1):
            ticker = r["ticker"]
            ticker_name = r.get("name", ticker)
            fin100 = r.get("financial_100", "")
            best = r.get("best_track_score", "")
            a = r.get("track_a_score", "")
            b = r.get("track_b_score", "")
            c = r.get("track_c_score", "")
            cat = r.get("industry", "")
            double = " ★" if ticker in double_pass else ""
            
            lines.append(f"| {i}{double} | {ticker} | {ticker_name} | {fin100} | {best} | {a} | {b} | {c} | {cat} |")
        
        lines.append("")
        
        # Detailed entries
        lines.append(f"## Detail")
        lines.append("")
        for i, r in enumerate(entries, 1):
            ticker = r["ticker"]
            name = r.get("name", "")
            a_score = r.get("track_a_score", 0)
            b_score = r.get("track_b_score", 0)
            c_score = r.get("track_c_score", 0)
            a_pillars = r.get("track_a_pillars_note", "")
            b_pillars = r.get("track_b_pillars_note", "")
            c_reasons = r.get("track_c_reasons", "")
            mc = r.get("market_cap", "")
            fi = r.get("financial_100", "")
            double = " ★(double-pass)" if ticker in double_pass else ""
            
            lines.append(f"### {i}. {ticker} — {name}{double}")
            lines.append(f"- **Financial 100**: {fi}")
            lines.append(f"- **Market Cap**: ${mc:,.0f}" if isinstance(mc, (int, float)) else f"- **Market Cap**: N/A")
            lines.append(f"- **Track A**: {a_score} ({a_pillars})")
            lines.append(f"- **Track B**: {b_score} ({b_pillars})")
            lines.append(f"- **Track C**: {c_score} ({c_reasons})")
            lines.append("")
    
    with open(path, "w") as f:
        f.write("\n".join(lines))

# Write tier lists
write_tier_list(GOLDEN_TIER, "Golden Tier List — Track A ≥ 75", 
                high_a, 
                "Rubber Band Man: high-safety oversold naked-CALL targets — Fortress financials + moat + inflection")

write_tier_list(EMERGING_EDGE, "Emerging Edge List — Track B ≥ 75", 
                high_b, 
                "Rubber Band Man: high-beta explosive-delta oversold naked-CALL targets — Emerging bottleneck / tech edge")

write_tier_list(MIRAGE_LIST, "Mirage Short List — Track C ≤ 35 (non-passing only)", 
                mirage, 
                "Rubber Band Man: high-conviction overbought naked-PUT targets — Structural breakdown, story without substance")

# Also write tier_list_state.json for signals_bridge.py to read
TIER_LIST_STATE = "/home/jaren/.hermes/cron/tier_list_state.json"
tier_state = {
    "golden": [r for r in high_a],
    "emerging": [r for r in high_b],
    "mirage": [r for r in mirage],
    "updated": datetime.now(timezone.utc).isoformat()
}
with open(TIER_LIST_STATE, "w") as f:
    json.dump(tier_state, f, indent=2)

# Print summary
print(f"Tier lists written:")
print(f"  Golden Tier: {len(high_a)} tickers (double-pass: {len(double_pass)})")
print(f"  Emerging Edge: {len(high_b)} tickers")
print(f"  Mirage Short: {len(mirage)} tickers")
print(f"  State: {TIER_LIST_STATE}")
print(f"  Files: {GOLDEN_TIER}, {EMERGING_EDGE}, {MIRAGE_LIST}")
