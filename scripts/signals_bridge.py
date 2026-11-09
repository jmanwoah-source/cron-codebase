#!/usr/bin/env python3
"""Signals bridge — converts tier_list_state.json to intel_bus/signals_bridge.json."""
import json
import os
from datetime import datetime, timezone

TIER_STATE_FILE = "/home/jaren/.hermes/cron/tier_list_state.json"
OUTPUT_DIR = "/home/jaren/.hermes/cron/intel_bus"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "signals_bridge.json")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Read tier list state
try:
    with open(TIER_STATE_FILE) as f:
        tier_state = json.load(f)
except FileNotFoundError:
    tier_state = {"golden": [], "emerging": [], "mirage": []}

gold = tier_state.get("golden", [])
emerging = tier_state.get("emerging", [])
mirage = tier_state.get("mirage", [])

# Build signals
signals = []

for entry in gold:
    ticker = entry.get("ticker", entry.get("symbol", ""))
    if not ticker:
        continue
    signals.append({
        "symbol": ticker,
        "track": "A",
        "wiki_score": entry.get("track_a_score", entry.get("score", 0)),
        "primary_benchmark": "SMH" if any(k in entry.get("sector", "").lower() + entry.get("industry", "").lower() for k in ["semiconductor", "chip", "semi"]) else "XLU" if any(k in entry.get("sector", "").lower() for k in ["utility", "energy"]) else "QQQ",
        "parent_whale": "VOO" if entry.get("market_cap", 0) and entry.get("market_cap", 0) > 10_000_000_000 else "IWM",
        "dilution_risk": False,
        "thesis_hook": entry.get("thesis_hook", entry.get("catalyst", entry.get("name", ticker) + " golden tier candidate"))[-80:],
        "last_updated": datetime.now(timezone.utc).isoformat()
    })

for entry in emerging:
    ticker = entry.get("ticker", entry.get("symbol", ""))
    if not ticker:
        continue
    signals.append({
        "symbol": ticker,
        "track": "B",
        "wiki_score": entry.get("track_b_score", entry.get("score", 0)),
        "primary_benchmark": "SMH" if any(k in entry.get("sector", "").lower() + entry.get("industry", "").lower() for k in ["semiconductor", "chip", "semi"]) else "XLU" if any(k in entry.get("sector", "").lower() for k in ["utility", "energy"]) else "QQQ",
        "parent_whale": "VOO" if entry.get("market_cap", 0) and entry.get("market_cap", 0) > 10_000_000_000 else "IWM",
        "dilution_risk": entry.get("dilution_risk", entry.get("dilution_kill", False)),
        "thesis_hook": entry.get("thesis_hook", entry.get("catalyst", entry.get("name", ticker) + " emerging edge candidate"))[-80:],
        "last_updated": datetime.now(timezone.utc).isoformat()
    })

for entry in mirage:
    ticker = entry.get("ticker", entry.get("symbol", ""))
    if not ticker:
        continue
    signals.append({
        "symbol": ticker,
        "track": "C",
        "wiki_score": entry.get("track_c_score", entry.get("score", 0)),
        "primary_benchmark": "SPY" if entry.get("market_cap", 0) and entry.get("market_cap", 0) > 1_000_000_000 else "IWM",
        "parent_whale": "SPY",
        "dilution_risk": True,
        "thesis_hook": entry.get("thesis_hook", entry.get("reason", entry.get("name", ticker) + " mirage target"))[-80:],
        "last_updated": datetime.now(timezone.utc).isoformat()
    })

# Deduplicate by symbol
seen = set()
deduped = []
for s in signals:
    if s["symbol"] not in seen:
        seen.add(s["symbol"])
        deduped.append(s)

output = {
    "generated": datetime.now(timezone.utc).isoformat(),
    "signals": deduped
}

with open(OUTPUT_FILE, "w") as f:
    json.dump(output, f, indent=2)

print(f"Bridge dispatch: {len(deduped)} signals → {OUTPUT_FILE}")
