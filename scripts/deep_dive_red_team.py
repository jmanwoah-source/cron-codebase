#!/usr/bin/env python3
"""Deep-dive red team — adversarial tribunal for tickers with best_track_score >= 65."""
import json, os, sqlite3
from datetime import datetime, timezone

SCORES_FILE = "/home/jaren/.hermes/cron/ticker_scores.json"
RED_TEAM_DB = "/home/jaren/.hermes/cron/red_team.db"
REPORT_FILE = "/home/jaren/.hermes/cron/red_team_report.json"

def run_red_team():
    with open(SCORES_FILE) as f:
        data = json.load(f)

    results = data.get("ticker_scores", [])
    candidates = [r for r in results if r.get("best_track_score", 0) >= 65]

    verdicts = {}
    options = []

    for r in candidates:
        ticker = r["ticker"]
        fin100 = r.get("financial_100", 0)
        a_score = r.get("track_a_score", 0)
        b_score = r.get("track_b_score", 0)
        c_score = r.get("track_c_score", 0)
        mc = r.get("market_cap", 0)
        catalyst = r.get("catalyst_context", [])
        bif = r.get("detailed_bifurcation", {})
        signals = bif.get("signals", {})
        ctx_count = r.get("catalyst_context_count", 0)

        # Kill probability (0-100)
        kill_prob = 0

        share_growth = signals.get("share_count_yoy_pct", 0)
        if share_growth and share_growth > 10: kill_prob += 30
        if share_growth and share_growth > 50: kill_prob += 30

        margin_declining = signals.get("margin_declining", False)
        if margin_declining: kill_prob += 20

        ocf = signals.get("ocf_annualized_quarterly", 0)
        if ocf is not None and ocf < 0: kill_prob += 15

        if fin100 < 50: kill_prob += 15
        if ctx_count == 0: kill_prob += 10
        elif ctx_count == 1: kill_prob += 5
        if signals.get("dilution_kill_8pct", False): kill_prob += 20

        for c in catalyst:
            if c.get("direction") == "negative": kill_prob += 10

        kill_prob = min(100, max(0, kill_prob))

        direction = None
        if a_score >= 75 or b_score >= 75:
            direction = "CALL"
        elif c_score <= 35:
            direction = "PUT"

        options_trigger = isinstance(mc, (int, float)) and mc >= 500_000_000

        verdict = "STRONG_BULL" if (a_score >= 75 and kill_prob < 30) else \
                 "MODERATE_BULL" if (a_score >= 60 and kill_prob < 50) else \
                 "BEARISH" if (c_score <= 35 and direction == "PUT" and kill_prob < 50) else \
                 "HIGH_RISK" if kill_prob >= 50 else \
                 "HOLD"

        verdicts[ticker] = {
            "thesis_kill_probability": kill_prob,
            "customer_concentration": "unknown",
            "dilution_risk": str(signals.get("dilution_kill_8pct", False)),
            "wip_stall_risk": "low" if kill_prob < 30 else "moderate",
            "direction": direction,
            "options_trigger": options_trigger,
            "verdict": verdict,
            "report": f"A={a_score} B={b_score} C={c_score} fin={fin100} mc={mc} kill={kill_prob}% {verdict}",
            "degraded": False,
            "assessed_at": datetime.now(timezone.utc).isoformat()
        }

        if options_trigger and direction:
            options.append({
                "ticker": ticker,
                "direction": direction,
                "market_cap": mc,
                "thesis_kill_prob": kill_prob,
                "thesis_hook": r.get("track_a_pillars_note", r.get("track_b_pillars_note", ticker)),
                "assessed_at": datetime.now(timezone.utc).isoformat()
            })

    # Write verdicts to existing DB schema
    conn = sqlite3.connect(RED_TEAM_DB)
    for ticker, v in verdicts.items():
        conn.execute("""INSERT OR REPLACE INTO red_team_verdicts
            (ticker, asof, best_track_score, track_a_score, track_b_score, track_c_score,
             direction, market_cap, thesis_kill_probability, kill_reason,
             customer_concentration_pct, top_customers_json, dilution_pct_yoy,
             wip_inventory_stall, wip_evidence, adversarial_summary,
             heuristic, degraded, corpus_chars, vllm_ms, vllm_raw,
             engine_signals_json, filing_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (ticker,
             datetime.now(timezone.utc).strftime("%Y-%m-%d"),
             r.get("best_track_score", 0),
             a_score, b_score, c_score,
             v["direction"], mc,
             v["thesis_kill_probability"],
             "dilution" if signals.get("dilution_kill_8pct") else "margin_decline" if margin_declining else "none",
             "unknown", "[]",
             0.0 if not signals.get("dilution_kill_8pct") else 10.0,
             "no" if kill_prob < 50 else "yes", "low",
             v["report"],
             "true", v["degraded"],
             0, 0, "[]",
             json.dumps(signals), None))
    conn.commit()
    conn.close()

    report = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "total_candidates": len(candidates),
        "verdicts_count": len(verdicts),
        "options_enqueued": len(options),
        "verdicts": verdicts,
        "options": options
    }

    with open(REPORT_FILE, "w") as f:
        json.dump(report, f, indent=2)

    verdict_counts = {}
    for v in verdicts.values():
        vc = v["verdict"]
        verdict_counts[vc] = verdict_counts.get(vc, 0) + 1

    print(f"Red Team: {len(verdicts)} verdicts, {len(options)} options enqueued")
    for vc, count in sorted(verdict_counts.items()):
        print(f"  {vc}: {count}")
    print(f"Report: {REPORT_FILE}")

run_red_team()
