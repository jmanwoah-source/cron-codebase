#!/usr/bin/env python3
"""Discord delivery for inclination alerts — with dedup so it only notifies on
NEW alerts or a significant score jump. Sends via `hermes send` (no LLM, no agent
loop). Records notified state so repeat cycles stay silent.

Run:  python3 deliver_inclinations.py [--all]
  default: send only if there is a new alert or a >=10-pt score jump on a notifying ticker
  --all:   send the full current alert set (for testing)
"""
import json
import os
import subprocess
import time

INC = os.path.expanduser("~/.hermes/cron/inclinations")
TARGET = "discord:1561210297966923787:1561210297966923787"   # Early-advantage notifications thread in #lous-channel (VERIFIED working 2026-10-18; the 1561209635296383077 pasted in chat 404s)
STATE = os.path.join(INC, "notified_state.json")
JUMP = 10                                  # pts that force a re-notification


def _load_state():
    try:
        return json.load(open(STATE))
    except Exception:
        return {"notified": {}}


def _mcap(mc):
    if not mc:
        return ""
    return f"${mc/1e9:.2f}B" if mc >= 1e9 else f"${mc/1e6:.0f}M"


def build_message(alerts):
    lines = ["**📈 Early-Advantage Inclination Alerts** (50% conviction trigger)", ""]
    for r in alerts:
        mc = _mcap(r.get("market_cap"))
        head = f"**{r['ticker']}** — {r['inclination_score']}/100"
        extras = " · ".join(x for x in [r.get("name"), mc] if x)
        if extras:
            head += f" · {extras}"
        lines.append(head)
        lines.append(f"> onto: **{r.get('gap') or 'n/a'}** @ `{r.get('gap_stage','?')}`")
        lines.append(f"> {r.get('doomed_line','')}")
        lines.append("")
    return "\n".join(lines).strip()


def main(force_all=False):
    scores = json.load(open(os.path.join(INC, "inclination_scores.json")))
    alerts = [a for a in scores.get("alerts", [])]
    if not alerts:
        print(json.dumps({"sent": False, "reason": "no alerts"}))
        return 0
    # render the doomed line into each alert for the message
    from aggregate import doomed_line  # reuse the renderer
    for a in alerts:
        a["doomed_line"] = doomed_line(a)

    state = _load_state()
    notified = state.setdefault("notified", {})
    new = []
    for a in alerts:
        t = a["ticker"]
        prev = notified.get(t, {})
        prev_score = prev.get("score", -1)
        if t not in notified:
            new.append(a)          # never notified before
        elif a["inclination_score"] - prev_score >= JUMP:
            new.append(a)          # significant jump
    if force_all:
        new = alerts
    if not new:
        print(json.dumps({"sent": False, "reason": "no new alerts",
                          "current": [(a["ticker"], a["inclination_score"]) for a in alerts]}))
        return 0

    msg = build_message(new)
    try:
        r = subprocess.run(["hermes", "send", "-t", TARGET, msg],
                           capture_output=True, text=True, timeout=60)
        sent = (r.returncode == 0)
    except Exception as e:
        sent = False
        r = None
    # record notified (only tickers that are currently notifying)
    for a in alerts:
        notified[a["ticker"]] = {"score": a["inclination_score"], "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                                 "gap": a.get("gap")}
    # prune old entries (keep last 30 days)
    cutoff = time.time() - 30 * 86400
    for t in list(notified):
        try:
            from datetime import datetime
            ts = datetime.strptime(notified[t]["ts"], "%Y-%m-%dT%H:%M:%SZ").timestamp()
            if ts < cutoff:
                del notified[t]
        except Exception:
            pass
    state["notified"] = notified
    state["last_run"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    json.dump(state, open(STATE, "w"), indent=1)
    out = {"sent": sent, "new_alerts": [a["ticker"] for a in new],
           "details": (r.stdout[:200] if r else "") if not sent else ""}
    print(json.dumps(out, indent=1))
    return 0


if __name__ == "__main__":
    import sys
    force = "--all" in sys.argv
    sys.exit(main(force_all=force))
