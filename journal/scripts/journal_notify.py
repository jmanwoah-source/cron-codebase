#!/usr/bin/env python3
"""
journal_notify.py — Proposition C: The Feedback Engine
Discord notification generator for the scoring journal.
Produces a concise, structured markdown message suitable for Discord delivery.

Runs after journal_analyze.py completes.
"""

import json
import os
import sys
import sqlite3
from datetime import datetime, timezone

JOURNAL_DB = "/home/jaren/.hermes/cron/journal/scoring_journal.db"
REPORT_DIR = "/home/jaren/.hermes/cron/journal/reports"

DISCORD_DELIVERY = "discord"  # Home channel


def load_recommendations():
    """Load the latest tuning recommendations."""
    recs_path = os.path.join(REPORT_DIR, "tuning_recommendations.json")
    if not os.path.exists(recs_path):
        return None
    with open(recs_path, "r") as f:
        return json.load(f)


def generate_notification():
    """Generate the Discord notification message."""
    recs = load_recommendations()
    if not recs:
        return "No recommendations available yet — insufficient data."
    
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    disc_score = recs.get("discrimination_score", 0)
    tier_stats = recs.get("tier_stats", {})
    pillar_corr = recs.get("pillar_correlations", {})
    rec_list = recs.get("recommendations", [])
    
    lines = []
    lines.append(f"**Scoring Journal - Weekly Digest** ({now})")
    lines.append("")
    
    # Overall status
    total = sum(ts.get("n", 0) for ts in tier_stats.values())
    lines.append(f"**Ticker coverage:** {total} total, {len(tier_stats)} tier(s)")
    lines.append(f"**Tier discrimination score:** {disc_score:.4f}")
    
    # Discrimination score interpretation
    if disc_score >= 1.0:
        lines.append("→ *Strong: tiers separate well by returns*")
    elif disc_score >= 0.3:
        lines.append("→ *Moderate: some separation, room for improvement*")
    elif disc_score >= 0.1:
        lines.append("→ *Weak: tiers barely separate — recalibration needed*")
    else:
        lines.append("→ *Very weak: no meaningful discrimination*")
    
    # Tier breakdown
    lines.append("")
    lines.append("**By Tier:**")
    for tier, stats in tier_stats.items():
        n = stats.get("n", 0)
        ret_7d = stats.get("ret_7d", {})
        ret_30d = stats.get("ret_30d", {})
        ret_90d = stats.get("ret_90d", {})
        
        avg_7d = ret_7d.get("avg", "N/A")
        avg_30d = ret_30d.get("avg", "N/A")
        avg_90d = ret_90d.get("avg", "N/A")
        pos_30d = ret_30d.get("positive_pct", "N/A")
        
        lines.append(f"- **{tier}** (n={n}): 7d avg={avg_7d}%, 30d avg={avg_30d}%, 30d positive rate={pos_30d}%")
    
    # Pillar correlations
    lines.append("")
    lines.append("**Pillar → Return Correlation (Spearman):**")
    for pillar in ["financial_100", "catalyst_100", "valuation_100", "technical_100"]:
        windows = pillar_corr.get(pillar, {})
        w30 = windows.get(30, {})
        corr = w30.get("correlation")
        interp = w30.get("interpretation", "?")
        n = w30.get("n", 0)
        
        if corr is not None:
            corr_str = f"{corr:+.3f}"
        else:
            corr_str = "N/A"
        
        lines.append(f"- **{pillar}**: r={corr_str} (n={n}, {interp})")
    
    # Recommendations
    lines.append("")
    if rec_list:
        lines.append("**Recommendations:**")
        for i, rec in enumerate(rec_list, 1):
            action = rec.get("action", "?")
            pillar = rec.get("pillar") or rec.get("rule", "N/A")
            rationale = rec.get("rationale", "")
            confidence = rec.get("confidence", "?")
            n = rec.get("n", 0)
            
            lines.append(f"")
            lines.append(f"**{i}. {action} - {pillar}**")
            lines.append(f"   Rationale: {rationale}")
            lines.append(f"   Confidence: {confidence} (n={n})")
            
            if rec.get("risk"):
                lines.append(f"   Risk: {rec['risk']}")
            if rec.get("from_pct"):
                lines.append(f"   Change: {rec['from_pct']} → {rec.get('to_pct', '?')}")
            
            if not rec.get("requires_data", True):
                lines.append(f"   ⚠ Requires more data before implementing")
    else:
        lines.append("**Recommendations:** None yet (insufficient data)")
    
    # Cold start notice
    if total < 10:
        lines.append("")
        lines.append(f"⚠ *Cold start: {total} tickers tracked. Need 15+ for statistical significance.*")
        lines.append(f"   Est. time to significance: {(15 - total) * 3} more days of tracking")
    
    # Data quality notes
    lines.append("")
    lines.append(f"*Data as of {now}. Next digest in 7 days.*")
    
    return "\n".join(lines)


def main():
    """Main notification generator."""
    message = generate_notification()
    
    # Write to file for cron delivery
    output_path = os.path.join(REPORT_DIR, "notification_latest.md")
    with open(output_path, "w") as f:
        f.write(message)
    
    # Also print for cron output capture
    print(message)
    
    return message


if __name__ == "__main__":
    main()
