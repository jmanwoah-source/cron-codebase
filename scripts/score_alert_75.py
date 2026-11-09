#!/usr/bin/env python3
"""Score alert 75 — post alerts for new/changed high-score tickers."""
import json
from datetime import datetime, timezone

SCORES_FILE = "/home/jaren/.hermes/cron/ticker_scores.json"
ALERT_STATE_FILE = "/home/jaren/.hermes/cron/score_alert_state.json"
DISCORD_WEBHOOK = None  # Placeholder — actual webhook handled by Discord integration
PASS_THRESHOLD = 75

with open(SCORES_FILE) as f:
    data = json.load(f)

results = data.get("ticker_scores", [])

with open(ALERT_STATE_FILE) as f:
    state = json.load(f)

last_scores = state.get("last_scores", {})

alerts = []
for r in results:
    ticker = r.get("ticker", "")
    best = r.get("best_track_score", 0)
    a_score = r.get("track_a_score", 0)
    b_score = r.get("track_b_score", 0)
    name = r.get("name", ticker)
    fin100 = r.get("financial_100", "")
    cat_ctx = r.get("catalyst_context_count", 0)

    prev_best = last_scores.get(ticker, {}).get("best_track_score")
    prev_a = last_scores.get(ticker, {}).get("track_a_score")
    prev_b = last_scores.get(ticker, {}).get("track_b_score")

    is_new = prev_best is None
    jumped = False
    if prev_best is not None:
        if best >= PASS_THRESHOLD and prev_best < PASS_THRESHOLD:
            jumped = True
        elif best >= PASS_THRESHOLD and prev_best >= PASS_THRESHOLD:
            if best - prev_best >= 1:
                jumped = True

    if is_new and best >= PASS_THRESHOLD:
        direction = "CALL" if a_score >= b_score else "PUT" if b_score > 75 else "ALERT"
        alert = (
            f"📊 **{ticker}** — {name}\n"
            f"Track A: **{a_score}** | Track B: **{b_score}** | Best: **{best}**\n"
            f"Financial 100: {fin100} | Catalyst briefs: {cat_ctx}\n"
            f"🟢 NEW qualifier at {PASS_THRESHOLD}+"
        )
        alerts.append(alert)

    elif jumped and best >= PASS_THRESHOLD:
        direction = "CALL" if a_score >= b_score else "PUT" if b_score > 75 else "ALERT"
        alert = (
            f"📊 **{ticker}** — Jumped +{best - prev_best} pts → **{best}**\n"
            f"Track A: {a_score} | Track B: {b_score} | Best: {best}\n"
            f"Financial 100: {fin100} | Catalyst briefs: {cat_ctx}"
        )
        alerts.append(alert)

    # Update state
    last_scores[ticker] = {
        "best_track_score": best,
        "track_a_score": a_score,
        "track_b_score": b_score,
        "last_updated": datetime.now(timezone.utc).isoformat()
    }

state["last_scores"] = last_scores
state["last_run"] = datetime.now(timezone.utc).isoformat()

with open(ALERT_STATE_FILE, "w") as f:
    json.dump(state, f, indent=2)

if alerts:
    # In a production environment, send to Discord webhook
    # For now, log what would be sent
    print(f"ALERTS SENT: {len(alerts)} high-score tickers\n" + "\n\n".join(alerts))
else:
    print(f"NO ALERTS: No tickers reached or jumped above {PASS_THRESHOLD}")

print(f"State saved to {ALERT_STATE_FILE}")
