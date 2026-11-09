#!/usr/bin/env python3
"""Inclination orchestrator — runs every signal source (self-gated), then aggregates.

One command:  python3 run_inclinations.py
Design: FMP-heavy sources (screen/transcripts/ip) are self-gated on file age so most
120-min cycles are cheap (they re-run only when the source is missing or stale). The
aggregate always runs (it is 0-FMP and reads the tech-gap wiki + briefs + scores).

Each source module is isolated in try/except — one failing source never blocks the others.
"""
import json
import os
import subprocess
import sys
import time

HERE = os.path.expanduser("~/.hermes/cron")
INC = os.path.join(HERE, "inclinations")
# FMP-heavy sources refresh at most once per 8h (3 cycles at 120m); wiki/briefs are live.
GATE_S = 8 * 3600


def _fresh(path, max_age=GATE_S):
    try:
        return (time.time() - os.path.getmtime(path)) < max_age
    except OSError:
        return False


def _run(mod, out_file):
    """Run a signal-source module; return (status, detail)."""
    if _fresh(out_file):
        return "fresh", f"{os.path.basename(out_file)} < 8h old, skipped"
    py = os.path.join(INC, mod)
    if not os.path.exists(py):
        return "missing", f"{mod} not present"
    t0 = time.time()
    try:
        r = subprocess.run([sys.executable, py], capture_output=True, text=True, timeout=300)
        dt = time.time() - t0
        if r.returncode == 0:
            return "ok", f"{mod} {dt:.0f}s rc=0"
        return "error", f"{mod} rc={r.returncode} {r.stderr.strip()[:200]}"
    except subprocess.TimeoutExpired:
        return "timeout", f"{mod} >300s"
    except Exception as e:
        return "error", f"{mod} {type(e).__name__} {str(e)[:160]}"


def main():
    report = {"generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    for mod, out in [("screen.py", "screen_out.json"),
                     ("transcripts.py", "transcripts_out.json"),
                     ("ip_signal.py", "ip_out.json")]:
        st, det = _run(mod, os.path.join(INC, out))
        report.setdefault("sources", {})[out] = {"status": st, "detail": det}
        print(f"[source] {st}: {det}", file=sys.stderr)
    # aggregate always runs (0 FMP)
    try:
        r = subprocess.run([sys.executable, os.path.join(INC, "aggregate.py")],
                           capture_output=True, text=True, timeout=60)
        print(r.stdout, end="")
        report["aggregate_rc"] = r.returncode
        if r.stderr.strip():
            report["aggregate_err"] = r.stderr.strip()[:300]
    except Exception as e:
        report["aggregate_rc"] = -1
        report["aggregate_err"] = f"{type(e).__name__} {str(e)[:200]}"
    # deliver new alerts to Discord (dedup'd; only new or >=10-pt-jump alerts send)
    try:
        d = subprocess.run([sys.executable, os.path.join(INC, "deliver_inclinations.py")],
                           capture_output=True, text=True, timeout=90)
        report["delivery"] = d.stdout.strip()[:300]
        print("[deliver]", d.stdout.strip()[:200], file=sys.stderr)
    except Exception as e:
        report["delivery"] = f"error {type(e).__name__} {str(e)[:160]}"
    # write a machine-readable status for the cron LLM to report
    with open(os.path.join(INC, "inclination_status.json"), "w") as f:
        json.dump(report, f, indent=1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
