#!/usr/bin/env python3
"""deadman_audit.py — Pipeline Dead-Man's Switch Telemetry (E#1.3)

Catches SEMANTIC DEGRADATION and EMPTY CYCLES that pass exit-code checks.
A stage can report 'completed' (exit 0) while scoring zero tickers, feeding
null metrics through the pillars, or silently degrading vLLM — the runner's
exit code would never know. This audit inspects what the cycle actually
PRODUCED.

Post-Cycle Health Audit (semantic checks over cycle artifacts):
  1. tickers_evaluated   — rows in ticker_scores.json 'rankings' (fallback:
                           evaluation_report.json 'rows_scored'), scoped to
                           artifacts fresh within the cycle window.
  2. null_metric_rate    — fraction of NULL core financial fields across all
                           scored rows; per-pillar null rates across the four
                           active sector pillars (financial/catalyst/
                           valuation/technical).
  3. vllm_schema_failures — INVALID + FETCH_ERROR + entity_mismatch counts
                           parsed from evaluation_report.json 'engine' line +
                           live vLLM endpoint probe (HTTP 5xx / timeout).

Hard Operational Assertions (trip conditions — any one fires an alarm):
  T1  tickers_evaluated == 0 during active trading hours (09:30-16:00 ET,
      Mon-Fri) — an empty cycle inside the session is never healthy.
  T2  null_metric_rate > 0.25 on any ACTIVE sector pillar (a pillar is
      active when it appears in >= 25% of rows; a null-dominated pillar
      means the metric source upstream went silent).
  T3  vLLM >= 2 CONSECUTIVE HTTP 500 or timeout errors (consecutive =
      previous audit's error count + this probe, persisted in state).

On trip:
  * append a FATAL line to ~/.hermes/logs/ALERTS.log
  * issue a notification via `hermes send` to discord:#lous-channel
  * return {"tripped": true, ...} so the runner can escalate its exit code
    to 3 (semantic degradation) even when all stages exited 0.

Standalone: python3 deadman_audit.py [--dry-run] [--window-min N]
  --dry-run   compute + log, but SKIP the Discord notification
  --window-min  cycle-window in minutes (default 180 = one 2h cycle)

Exit codes: 0 = healthy, 3 = trip condition fired, 1 = audit self-error.
"""

import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

CRON = os.path.expanduser("~/.hermes/cron")
TICKER_SCORES = os.path.join(CRON, "ticker_scores.json")
EVAL_REPORT = os.path.join(CRON, "evaluation_report.json")
STATE_F = os.path.join(CRON, "deadman_state.json")
LOGS_DIR = os.path.expanduser("~/.hermes/logs")
ALERTS_LOG = os.path.join(LOGS_DIR, "ALERTS.log")
VLLM_HEALTH = "http://localhost:8000/health"
NOTIFY_TARGET = "discord:#lous-channel"

DEFAULT_WINDOW_MIN = 180          # one 2h pipeline cycle
NULL_TRIP_THRESHOLD = 0.25        # T2 per-pillar null rate
VLLM_CONSECUTIVE_TRIP = 2         # T3 consecutive 500/timeout count
HERMES_BIN = "hermes"
HERMES_CMD_TIMEOUT = 90

# Core financial fields checked at row level for nulls
CORE_FIELDS = ("market_cap", "price", "best_track_score")
# The four active sector pillars (pillar_scores block per row)
PILLARS = ("financial_100", "catalyst_100", "valuation_100", "technical_100")


def log(msg):
    ts = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"[{ts}] {msg}", flush=True)


def load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def file_age_min(path):
    try:
        return (time.time() - os.path.getmtime(path)) / 60.0
    except OSError:
        return None


def in_trading_hours(now_et=None):
    """Mon-Fri 09:30-16:00 America/New_York (regular session)."""
    now_et = now_et or datetime.now(ZoneInfo("America/New_York"))
    if now_et.weekday() >= 5:
        return False
    hhmm = now_et.hour * 60 + now_et.minute
    return 9 * 60 + 30 <= hhmm < 16 * 60


def vllm_probe(timeout=8.0):
    """GET vLLM /health. Returns (ok, error_detail)."""
    try:
        req = urllib.request.Request(VLLM_HEALTH, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as r:
            if r.status >= 500:
                return False, f"HTTP {r.status}"
            return r.status < 400, ""
    except urllib.error.HTTPError as e:
        return e.code < 500, f"HTTP {e.code}"
    except Exception as e:
        return False, f"timeout/unreachable ({type(e).__name__})"


def parse_engine_failures(eval_report):
    """Parse 'engine': "... (118 OK / 0 invalid / 1 entity_mismatch / 1 fetch_error ...)'."""
    out = {"invalid": 0, "entity_mismatch": 0, "fetch_error": 0,
           "vllm_degraded": bool(eval_report.get("vllm_degraded"))}
    eng = eval_report.get("engine") or ""
    for key, pat in (("invalid", r"(\d+)\s+invalid"),
                     ("entity_mismatch", r"(\d+)\s+entity_mismatch"),
                     ("fetch_error", r"(\d+)\s+fetch_error")):
        m = re.search(pat, eng)
        if m:
            out[key] = int(m.group(1))
    return out


def audit_metrics(window_min=DEFAULT_WINDOW_MIN):
    """Compute the three semantic metrics over cycle-fresh artifacts."""
    m = {
        "tickers_evaluated": 0,
        "tickers_source": "none",
        "null_metric_rate": None,
        "pillar_null_rates": {},
        "vllm_schema_failures": 0,
        "vllm_engine_detail": None,
        "vllm_probe_ok": None,
        "vllm_probe_error": None,
    }

    # ---- 1 + 2. ticker_scores.json (primary) --------------------------
    age = file_age_min(TICKER_SCORES)
    scores = load_json(TICKER_SCORES)
    rows = (scores or {}).get("rankings") if isinstance(scores, dict) else None
    if rows and age is not None and age <= window_min:
        m["tickers_evaluated"] = len(rows)
        m["tickers_source"] = f"ticker_scores.json (age {int(age)}m)"
        total_pairs = 0
        null_pairs = 0
        pillar_nulls = {p: 0 for p in PILLARS}
        pillar_present = {p: 0 for p in PILLARS}
        pillar_assessed = {p: 0 for p in PILLARS}
        for r in rows:
            if not isinstance(r, dict):
                continue
            for f in CORE_FIELDS:
                total_pairs += 1
                if r.get(f) is None:
                    null_pairs += 1
            ps = r.get("pillar_scores") or {}
            for p in PILLARS:
                present = p in ps
                pillar_present[p] += 1 if present else 0
                if present and ps.get(p) is not None:
                    pillar_assessed[p] += 1
                total_pairs += 1
                if not present or ps.get(p) is None:
                    pillar_nulls[p] += 1
        if rows:
            m["null_metric_rate"] = round(null_pairs / total_pairs, 4) if total_pairs else 0.0
            m["pillar_null_rates"] = {
                p: round(pillar_nulls[p] / len(rows), 4) for p in PILLARS
            }
            m["_pillar_present_frac"] = {
                p: round(pillar_present[p] / len(rows), 4) for p in PILLARS
            }
            # ACTIVITY GATE FIX (2026-11-08): active = pillar ASSSESSED in
            # >=25% of rows, not merely present. Deep-Dive deliberately scores
            # valuation_100/technical_100 for HIGH-tier rows only and writes
            # null ("unassessed") everywhere else per the engine's deep-dive
            # instructions — a tier-shaped partial assessment is NOT a
            # degradation. Presence-only gating false-tripped T2 on 2026-11-07
            # 13:53Z (134 rows, key present in all 131, 117 nulls).
            m["_pillar_assessed_frac"] = {
                p: round(pillar_assessed[p] / len(rows), 4) for p in PILLARS
            }
    else:
        # stale/missing primary artifact -> try the eval report, else zero
        eage = file_age_min(EVAL_REPORT)
        ev = load_json(EVAL_REPORT)
        if ev and eage is not None and eage <= window_min:
            m["tickers_evaluated"] = int(ev.get("rows_scored") or 0)
            m["tickers_source"] = f"evaluation_report.json rows_scored (age {int(eage)}m)"
        else:
            m["tickers_source"] = "none (no cycle-fresh scoring artifact)"

    # ---- 3. vLLM schema validation failures ---------------------------
    ev = load_json(EVAL_REPORT) or {}
    fails = parse_engine_failures(ev)
    m["vllm_schema_failures"] = fails["invalid"] + fails["fetch_error"]
    if fails["entity_mismatch"]:
        m["vllm_schema_failures"] += fails["entity_mismatch"]  # quarantined rows: schema/entity broken
    m["vllm_engine_detail"] = {
        "invalid": fails["invalid"], "entity_mismatch": fails["entity_mismatch"],
        "fetch_error": fails["fetch_error"], "vllm_degraded": fails["vllm_degraded"],
    }
    ok, err = vllm_probe()
    m["vllm_probe_ok"] = ok
    m["vllm_probe_error"] = err or None
    return m


def load_state():
    try:
        with open(STATE_F) as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(st):
    os.makedirs(os.path.dirname(STATE_F), exist_ok=True)
    tmp = STATE_F + ".tmp"
    with open(tmp, "w") as f:
        json.dump(st, f, indent=1)
    os.replace(tmp, STATE_F)


def append_alerts_log(severity, condition, metrics, detail):
    os.makedirs(LOGS_DIR, exist_ok=True)
    ts = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    line = (f"{ts} | {severity} | DEADMAN_TRIP | condition={condition} | "
            f"detail={detail} | metrics={json.dumps(metrics, default=str)}\n")
    with open(ALERTS_LOG, "a") as f:
        f.write(line)
    return line


def notify(title, body, dry_run=False):
    """Issue a Discord notification via hermes send (no LLM, no gateway needed)."""
    if dry_run:
        log(f"  [dry-run] would notify: {title}")
        return True
    try:
        p = subprocess.run(
            [HERMES_BIN, "--accept-hooks", "send", "--to", NOTIFY_TARGET,
             "-s", title, "-f", "-"],
            input=body, capture_output=True, text=True, timeout=HERMES_CMD_TIMEOUT)
        out = ((p.stdout or "") + (p.stderr or "")).strip()[:200]
        if p.returncode == 0:
            log(f"  notified discord:{NOTIFY_TARGET} :: {out}")
            return True
        log(f"  NOTIFY FAILED rc={p.returncode} :: {out}")
        return False
    except Exception as e:
        log(f"  NOTIFY FAILED: {e}")
        return False


def run_audit(cycle_start_ts=None, window_min=DEFAULT_WINDOW_MIN,
              dry_run=False, quiet=False):
    """Full dead-man's-switch pass. Returns the audit report dict."""
    t0 = time.time()
    if not quiet:
        log("=== DEAD-MAN'S SWITCH: post-cycle semantic audit ===")
    metrics = audit_metrics(window_min=window_min)

    # ---- T3 state: consecutive vLLM 500/timeout ------------------------
    st = load_state()
    prev_errors = int(st.get("vllm_consecutive_errors", 0))
    probe_error = (not metrics["vllm_probe_ok"]) if metrics["vllm_probe_ok"] is not None else None
    if metrics["vllm_probe_ok"] is None:
        consec = prev_errors
    else:
        consec = prev_errors + 1 if probe_error else 0
    # also treat engine-degraded (vllm_degraded flag) as an error tick
    if metrics["vllm_probe_ok"] and metrics["vllm_engine_detail"].get("vllm_degraded"):
        consec = prev_errors + 1
    st["vllm_consecutive_errors"] = consec
    st["last_probe_ts"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    st["last_probe_error"] = metrics["vllm_probe_error"]
    st["last_run"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    save_state(st)
    metrics["vllm_consecutive_errors"] = consec

    # ---- Hard Operational Assertions ------------------------------------
    trips = []
    now_et = datetime.now(ZoneInfo("America/New_York"))

    if metrics["tickers_evaluated"] == 0 and in_trading_hours(now_et):
        trips.append(("T1", "EMPTY_CYCLE_TRADING_HOURS",
                      f"tickers_evaluated==0 at {now_et.strftime('%a %H:%M')} ET "
                      f"(09:30-16:00 session) — cycle produced no scored rows"))

    pnr = metrics.get("pillar_null_rates") or {}
    assessed = metrics.get("_pillar_assessed_frac") or {}
    bad_pillars = []
    for p in PILLARS:
        rate = pnr.get(p)
        active = (assessed.get(p) or 0) >= 0.25
        if rate is not None and active and rate > NULL_TRIP_THRESHOLD:
            bad_pillars.append((p, rate))
    if bad_pillars:
        trips.append(("T2", "NULL_METRIC_RATE",
                      "pillar null rate > 0.25 on active pillar(s): "
                      + ", ".join(f"{p}={r:.0%}" for p, r in bad_pillars)))

    if consec >= VLLM_CONSECUTIVE_TRIP:
        trips.append(("T3", "VLLM_CONSECUTIVE_ERRORS",
                      f"vLLM >= {VLLM_CONSECUTIVE_TRIP} consecutive HTTP 500/timeout "
                      f"(consec={consec}, last={metrics.get('vllm_probe_error')})"))

    report = {
        "ran_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "window_min": window_min,
        "cycle_start": (datetime.fromtimestamp(cycle_start_ts).isoformat(timespec="seconds")
                        if cycle_start_ts else None),
        "trading_hours": in_trading_hours(now_et),
        "metrics": metrics,
        "tripped": bool(trips),
        "trips": [{"id": i, "condition": c, "detail": d} for i, c, d in trips],
        "notify_sent": None,
        "audit_ms": None,
    }

    if trips:
        if not quiet:
            for i, c, d in trips:
                log(f"  TRIP {i}: {c} — {d}")
        for i, c, d in trips:
            try:
                append_alerts_log("FATAL", c, metrics, d)
            except Exception as e:
                log(f"  ALERTS.log append failed: {e}")
        title = f"[DEADMAN] FATAL — {' + '.join(t['id'] for t in report['trips'])} pipeline semantic degradation"
        body_lines = ["🚨 LOU PIPELINE DEAD-MAN'S SWITCH — FATAL TRIP", ""]
        for t in report["trips"]:
            body_lines.append(f"• {t['id']} {t['condition']}: {t['detail']}")
        m = metrics
        body_lines += [
            "",
            f"tickers_evaluated: {m['tickers_evaluated']} (source: {m['tickers_source']})",
            f"null_metric_rate: {m['null_metric_rate']}  pillar-null: {json.dumps(m['pillar_null_rates'])}",
            f"vllm_schema_failures: {m['vllm_schema_failures']} ({json.dumps(m['vllm_engine_detail'])})",
            f"vllm_probe: {'OK' if m['vllm_probe_ok'] else 'FAIL'} consec_errors={m['vllm_consecutive_errors']} "
            f"({m['vllm_probe_error'] or 'n/a'})",
            "",
            f"Full report: {ALERTS_LOG} (last entries) — state: {STATE_F}",
        ]
        report["notify_sent"] = notify(title, "\n".join(body_lines), dry_run=dry_run)
        log(f"  ALERTS.log: {ALERTS_LOG}")
    else:
        if not quiet:
            log(f"  HEALTHY — {metrics['tickers_evaluated']} tickers, "
                f"null_rate={metrics['null_metric_rate']}, "
                f"vllm={metrics['vllm_schema_failures']} schema failures "
                f"(consec_errors={consec})")

    report["audit_ms"] = int((time.time() - t0) * 1000)
    if not quiet:
        log(f"=== DEAD-MAN'S SWITCH: {'TRIPPED' if trips else 'HEALTHY'} "
            f"({report['audit_ms']}ms) ===")
    return report


def main():
    args = sys.argv[1:]
    dry = "--dry-run" in args
    window = DEFAULT_WINDOW_MIN
    if "--window-min" in args:
        try:
            window = int(args[args.index("--window-min") + 1])
        except (IndexError, ValueError):
            log("bad --window-min; using default")
    rpt = run_audit(window_min=window, dry_run=dry)
    if rpt["tripped"]:
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
