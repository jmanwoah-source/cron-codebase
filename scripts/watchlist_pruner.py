#!/usr/bin/env python3
"""Watchlist pruner — bi-weekly gated recheck for ACTIVE/WATCH tickers."""
import json, os, shutil
from datetime import datetime, timezone

WATCHLIST_FILE = "/home/jaren/.hermes/cron/watchlist_state.json"
SCORES_FILE = "/home/jaren/.hermes/cron/ticker_scores.json"
CHANGELOG_FILE = "/home/jaren/wiki/watchlist_changelog.md"

with open(WATCHLIST_FILE) as f:
    state = json.load(f)

tickers_state = state.get("tickers", {})

with open(SCORES_FILE) as f:
    data = json.load(f)

scores = data.get("ticker_scores", [])
scores_map = {s["ticker"]: s for s in scores}

# Also check entity mismatches and excluded
entity_mismatches = {e["ticker"]: e for e in data.get("entity_mismatches", [])}

summary = {
    "ACTIVE": 0, "WATCH": 0, "DORMANT": 0, "ARCHIVED": 0,
    "rechecked_this_run": 0, "promotions": 0, "demotions": 0, "archived_this_run": 0,
    "next_due_in_days": 14
}

updates = {}
changelog_entries = []

for ticker, info in tickers_state.items():
    status = info.get("status", "WATCH")
    last_recheck = info.get("last_recheck", "")
    
    # Check if due for recheck (14-day window)
    if last_recheck:
        try:
            last_dt = datetime.fromisoformat(last_recheck.replace("Z", "+00:00"))
            days_since = (datetime.now(timezone.utc) - last_dt).days
            is_due = days_since >= 14
        except:
            is_due = True
    else:
        is_due = True
    
    # Check if there's a score update available
    if ticker in scores_map:
        s = scores_map[ticker]
        new_score = s.get("best_track_score", info.get("best_track_score"))
        new_a = s.get("track_a_score", info.get("track_a_score"))
        new_b = s.get("track_b_score", info.get("track_b_score"))
        new_c = s.get("track_c_score", info.get("track_c_score"))
        fin100 = s.get("financial_100", info.get("financial_100"))
        
        if new_score is not None:
            # Update with new scores
            info["best_track_score"] = new_score
            info["track_a_score"] = new_a
            info["track_b_score"] = new_b
            info["track_c_score"] = new_c
            info["financial_100"] = fin100
            info["last_recheck"] = datetime.now(timezone.utc).isoformat()
            summary["rechecked_this_run"] += 1
            
            # Check for demotion due to score drop
            old_score = info.get("last_scored_best", new_score)
            if new_score < 75 and status == "ACTIVE":
                info["last_signal"] = datetime.now(timezone.utc).isoformat()
                info["empty_streak"] = info.get("empty_streak", 0) + 1
                info["status"] = "WATCH"
                summary["demotions"] += 1
                changelog_entries.append(f"- {ticker}: ACTIVE -> WATCH (score dropped to {new_score})")
                summary["status"] = "WATCH"
            
            # Check for promotion
            if new_score >= 75 and status in ("WATCH", "DORMANT"):
                info["status"] = "ACTIVE"
                summary["promotions"] += 1
                changelog_entries.append(f"- {ticker}: WATCH -> ACTIVE (score {new_score})")
    
    # Update counts
    summary[status] = summary.get(status, 0) + 1 if status in summary else 0

# Recount statuses properly
for ticker, info in tickers_state.items():
    st = info.get("status", "WATCH")
    if st in summary:
        summary[st] = summary.get(st, 0) + 1

state["tickers"] = tickers_state
state["summary"] = summary
state["last_run"] = {
    "ts": datetime.now(timezone.utc).isoformat(),
    "rechecked": summary["rechecked_this_run"],
    "promotions": [],
    "demotions": changelog_entries,
    "archived": []
}

with open(WATCHLIST_FILE, "w") as f:
    json.dump(state, f, indent=2)

print(f"Watchlist pruned: {summary['rechecked_this_run']} rechecked, {summary['promotions']} promos, {summary['demotions']} demos")
print(f"Status counts: ACTIVE={summary['ACTIVE']} WATCH={summary['WATCH']} DORMANT={summary['DORMANT']} ARCHIVED={summary['ARCHIVED']}")
