#!/usr/bin/env python3
"""
run_pipeline_cycle.py — Single Locked Pipeline Runner (E#1.1)
============================================================
Collapses the staggered :00 / :15 / :30 cron entries into ONE sequential,
lock-protected cycle:

    stage_sources_ingest()     -> job ba24fd4ddad6  (Sources: Research Agents 1-6)
    stage_processing_sieve()   -> job cfdab394b3bd  (Meat Parser + Scribe + Orchestrator + Conviction)
    stage_deep_dive_tribunal() -> job 7267b662cf52  (Deep-Dive: FMP Vet + Bifurcated Scoring)

Concurrency guard:
    Non-blocking fcntl.flock on /tmp/hermes_pipeline.lock. If a prior cycle is
    still running, log a warning and exit 0 cleanly — never run concurrently.

Per-stage protocol (verified live 2026-11-07 against Hermes gateway 11/7):
    1. `hermes cron resume <id>`  — a PAUSED job's `cron run` trigger is
       SILENTLY DROPPED by the scheduler; resume is mandatory first.
    2. `hermes cron run <id>`     — dispatch; output "Ran now: ..." = executed
       synchronously, else the claim rides the next gateway tick (~60s).
    3. Poll ~/.hermes/cron/executions.db until the execution row reaches a
       terminal status (3h hard wall per stage).
    4. `hermes cron pause <id>`   — RE-LOCK the stage so its original
       :00/:15/:30 schedule can never fire it again. The three stage jobs
       live paused in jobs.json; only this runner wakes them.
    A stage that does not reach 'completed' ABORTS the chain — downstream
    stages must not process incomplete/stale data (cascade error anti-pattern).

Exit codes: 0 = all stages completed (or cycle skipped via lock),
            1 = a stage failed/timed out, 2 = dispatch/infrastructure error,
            3 = DEGRADED: stages completed but the dead-man's switch post-cycle
                semantic audit tripped (empty cycle / null-metric collapse /
                vLLM consecutive 500s — E#1.3).
"""

import fcntl
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
CRON_DIR = os.path.expanduser("~/.hermes/cron")
EXEC_DB = os.path.join(CRON_DIR, "executions.db")
JOBS_JSON = os.path.join(CRON_DIR, "jobs.json")
LOCK_FILE = "/tmp/hermes_pipeline.lock"
CYCLE_STATE = os.path.join(CRON_DIR, "pipeline_cycle_state.json")
HERMES_BIN = "hermes"

# Execution chain (order matters — data flows sources -> processing -> deep-dive)
STAGES = [
    ("stage_sources_ingest", "ba24fd4ddad6"),
    ("stage_processing_sieve", "cfdab394b3bd"),
    ("stage_deep_dive_tribunal", "7267b662cf52"),
]

POLL_INTERVAL_S = 30          # executions.db poll cadence
STAGE_TIMEOUT_S = 180 * 60    # 3h hard wall per stage (agent jobs normally end < 30m)
DISPATCH_GRACE_S = 180        # window to see the execution row appear after async dispatch
HERMES_CMD_TIMEOUT = 1800     # `cron run` of agent jobs executes SYNCHRONOUSLY and
                              # blocks until the stage finishes — wall must cover a
                              # full stage (rc=124 on timeout = still running, not failure)

TERMINAL_STATUSES = {"completed", "failed", "error", "unknown", "canceled", "cancelled", "timeout", "superseded"}


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"[{ts}] {msg}", flush=True)


def run_hermes(args, timeout=HERMES_CMD_TIMEOUT):
    """Run a hermes CLI subcommand; return (exit_code, stdout+stderr)."""
    cmd = [HERMES_BIN, "--accept-hooks"] + args
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return p.returncode, (p.stdout or "") + (p.stderr or "")
    except subprocess.TimeoutExpired:
        return 124, "hermes CLI timed out"
    except FileNotFoundError:
        return 127, "hermes binary not found"


def job_state(job_id: str):
    try:
        jobs = json.load(open(JOBS_JSON))
        jl = jobs if isinstance(jobs, list) else jobs.get("jobs", [])
        return next((x for x in jl if x.get("id") == job_id), None)
    except Exception:
        return None


def resume_job(job_id: str) -> bool:
    """Resume a paused stage job. Benign no-op if already scheduled+enabled."""
    rc, out = run_hermes(["cron", "resume", job_id])
    tail = out.strip()[:200]
    if rc == 0:
        log(f"  resume {job_id}: rc=0 :: {tail}")
        return True
    # already-enabled jobs refuse resume with rc!=0 — verify actual state
    j = job_state(job_id)
    if j and j.get("state") == "scheduled" and j.get("enabled"):
        log(f"  resume {job_id}: refused (rc={rc}) but already enabled — OK")
        return True
    log(f"  resume {job_id}: FAILED rc={rc} :: {tail}")
    return False


def pause_job(job_id: str) -> bool:
    """Re-lock a stage job so its old schedule cannot fire it."""
    rc, out = run_hermes(["cron", "pause", job_id])
    log(f"  pause {job_id}: rc={rc} :: {out.strip()[:120]}")
    if rc != 0:
        j = job_state(job_id)
        if j and not j.get("enabled", True):
            return True
        return False
    return True


def wait_for_dispatch(job_id: str, since_ts: float):
    """Fresh-dispatch wait: pin to the FIRST row claimed near/after `since_ts`
    and follow THAT row to terminal (a later row — e.g. a retry nudge's second
    fire — can never steal the verdict). 'no_claim' if no row appears within
    the grace window."""
    deadline = time.time() + STAGE_TIMEOUT_S
    grace_deadline = time.time() + DISPATCH_GRACE_S
    pinned = None
    while time.time() < deadline:
        row = execution_row(job_id, pinned)
        if pinned is None and row and row[2]:
            try:
                # .timestamp() honors the ISO offset (time.mktime would read
                # the fields as local wall clock).
                claimed_ts = datetime.fromisoformat(row[2]).timestamp()
            except ValueError:
                claimed_ts = time.time()
            if claimed_ts >= since_ts - 10:
                pinned = row[0]
        if pinned and row and row[1] in TERMINAL_STATUSES:
            return row[1], f"exec={row[0][:12]} claimed={row[2]} finished={row[4]}", (row[5] or "")
        if not pinned and time.time() > grace_deadline:
            return "no_claim", "execution row never appeared after dispatch (trigger dropped?)", ""
        if pinned:
            log(f"  poll {job_id}: exec={row[0][:12] if row else '?'} status={row[1] if row else '?'}")
        time.sleep(POLL_INTERVAL_S)
    return "timeout", f"no terminal status after {STAGE_TIMEOUT_S // 60}m", ""


def execution_row(job_id: str, row_id: str = None):
    """Execution row by job_id (+ optional row id). Read-only; gateway may be mid-write."""
    try:
        db = sqlite3.connect(f"file:{EXEC_DB}?mode=ro", uri=True, timeout=5)
        if row_id:
            row = db.execute(
                "SELECT id, status, claimed_at, started_at, finished_at, error FROM executions "
                "WHERE job_id = ? AND id = ?", (job_id, row_id)).fetchone()
        else:
            row = db.execute(
                "SELECT id, status, claimed_at, started_at, finished_at, error FROM executions "
                "WHERE job_id = ? ORDER BY rowid DESC LIMIT 1", (job_id,)).fetchone()
        db.close()
        return row
    except sqlite3.OperationalError:
        return None


def latest_execution(job_id: str):
    """Latest execution row for the job (read-only; gateway may be mid-write)."""
    return execution_row(job_id)


def wait_for_stage(job_id: str, since_ts: float, track_row_id: str = None):
    """Poll executions.db until the post-dispatch execution row is terminal.

    track_row_id: if given (adopt path), wait on THAT row regardless of when
    it was claimed — the row is already ours. Otherwise (fresh dispatch) the
    row must be claimed near/after `since_ts` to count as ours; if none
    appears within DISPATCH_GRACE_S the dispatch was dropped ('no_claim').
    Returns (status, detail, error).
    """
    deadline = time.time() + STAGE_TIMEOUT_S
    grace_deadline = time.time() + DISPATCH_GRACE_S
    seen = False
    while time.time() < deadline:
        row = execution_row(job_id, track_row_id)
        if row and not seen:
            if track_row_id:
                # Adopt path: our row exists by construction — wait on it.
                if row[0] == track_row_id:
                    seen = True
            elif row[2]:
                try:
                    # .timestamp() honors the ISO offset (time.mktime would
                    # read the fields as local wall clock).
                    claimed_ts = datetime.fromisoformat(row[2]).timestamp()
                except ValueError:
                    claimed_ts = time.time()
                if claimed_ts >= since_ts - 10:
                    seen = True
        if seen and row and row[1] in TERMINAL_STATUSES:
            # row = (id, status, claimed_at, started_at, finished_at, error)
            return row[1], f"exec={row[0][:12]} claimed={row[2]} finished={row[4]}", (row[5] or "")
        if not seen and time.time() > grace_deadline and not track_row_id:
            return "no_claim", "execution row never appeared after dispatch (trigger dropped?)", ""
        if seen:
            log(f"  poll {job_id}: exec={row[0][:12] if row else '?'} status={row[1] if row else '?'}")
        time.sleep(POLL_INTERVAL_S)
    return "timeout", f"no terminal status after {STAGE_TIMEOUT_S // 60}m", ""


def running_execution(job_id: str):
    """An open 'running'/'pending' execution row for the job, or None."""
    try:
        db = sqlite3.connect(f"file:{EXEC_DB}?mode=ro", uri=True, timeout=5)
        row = db.execute(
            "SELECT id, status, claimed_at, started_at FROM executions "
            "WHERE job_id = ? AND status IN ('running','pending') ORDER BY rowid DESC LIMIT 1",
            (job_id,)).fetchone()
        db.close()
        return row
    except sqlite3.OperationalError:
        return None


JOBS_LOCK = os.path.join(CRON_DIR, ".jobs.lock")


def _jobs_lock_ctx():
    """flock around jobs.json mutations (gateway writes this file too;
    .jobs.lock is the gateway's own cron/jobs.py lock path)."""
    import contextlib

    @contextlib.contextmanager
    def _ctx():
        fd = os.open(JOBS_LOCK, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    return _ctx


def nudge_job(job_id: str) -> bool:
    """Nudge a job so the GATEWAY fires it NOW as source=builtin (durable).

    One atomic jobs.json mutation (under the gateway's .jobs.lock) that
    replicates cron/jobs.py `trigger_job` EXACTLY — the same field set, so
    the due-scan's `manual_run = (manual_run_at == next_run_at)` gate is
    true and ALL lattice/grace/occurrence-dedup guards are bypassed:
      enabled=true, state=scheduled, paused_at=None, paused_reason=None,
      next_run_at=T, manual_run_at=T (same ISO string), manual_run_prompt=None

    Why manual_run_at is REQUIRED (verified live 2026-11-07): a kind=cron
    job with a plain next_run_at=now sits OFF the cron lattice (e.g.
    04:26:11 on a 0 */2 * * * grid), and _reanchor_stale_cron re-anchors it
    to the next lattice slot WITHOUT firing (STALE_CRON_EXPR_EDIT) — the
    stage is silently skipped and next_run_at jumps 2h ahead. mark_job_run
    clears manual_run_at after the fire, and the legacy schedule is then
    safe because the job is re-paused before the next tick.

    Do NOT combine this with a concurrent `hermes cron resume` CLI call:
    the CLI re-arms next_run_at and saves jobs.json — if its save lands
    after this nudge it clobbers the fields and the tick sees the job as
    not-due (race lost live at 2026-11-07 03:38).
    """
    _ctx = _jobs_lock_ctx()
    now = datetime.now(timezone.utc).astimezone().isoformat()
    with _ctx():
        jobs = json.load(open(JOBS_JSON))
        jl = jobs if isinstance(jobs, list) else jobs.get("jobs", [])
        hit = None
        for j in jl:
            if j.get("id") == job_id:
                j["next_run_at"] = now
                j["manual_run_at"] = now      # trigger_job: same isoformat string
                j["manual_run_prompt"] = None
                j["state"] = "scheduled"
                j["enabled"] = True
                j["paused_at"] = None
                j["paused_reason"] = None
                hit = j
                break
        if hit is None:
            return False
        tmp = JOBS_JSON + ".tmp"
        with open(tmp, "w") as f:
            json.dump(jobs, f, indent=2)
        os.replace(tmp, JOBS_JSON)
    log(f"  nudge {job_id}: trigger_job-style (manual_run_at={now}), pause markers cleared "
        f"(gateway will fire as builtin)")
    return True


def dispatch_and_wait(stage_name: str, job_id: str) -> bool:
    """nudge (gateway fires builtin) -> wait for terminal -> re-pause.

    The nudge alone re-enables the job (clears every pause marker
    atomically) — a separate `hermes cron resume` CLI call is a WRITE RACE
    (its re-arm save can clobber next_run_at=now after the nudge).
    True iff the stage's execution row reaches 'completed'.
    """
    log(f"=== {stage_name} -> {job_id} ===")
    since_ts = time.time()

    # If a prior (crashed-runner) cycle already left this stage in flight,
    # do NOT re-trigger it — just adopt and wait on the running row.
    existing = running_execution(job_id)
    if existing:
        log(f"  adopt: execution {existing[0][:12]} already {existing[1]} "
            f"(claimed {existing[2]}) — waiting on it instead of re-dispatching")
        status, detail, err = wait_for_stage(job_id, since_ts, track_row_id=existing[0])
        ok = status == "completed"
        log(f"  {stage_name} result: {status} ({detail}) {err[:200] if err else ''}")
        pause_job(job_id)
        return ok

    # Dispatch by nudging jobs.json so the GATEWAY owns the execution
    # (source=builtin, durable). A `cron run` CLI dispatch is source=direct:
    # the CLI process is the execution's owner and exits on return, so a
    # long-running agent stage can reconcile to 'unknown' and lose its
    # side effects.
    if not nudge_job(job_id):
        log("  FATAL: nudge failed (job not found in jobs.json) — aborting chain")
        return False

    status, detail, err = wait_for_dispatch(job_id, since_ts)
    if status == "no_claim":
        # Nudge may not have been picked up (gateway tick lag). Retry once.
        # NOTE: if the first nudge's row arrives late AND the retry also fires,
        # two runs exist — the verdict follows the FIRST row (pinned); the
        # duplicate run is a residual risk accepted over a dropped stage.
        log("  dispatch: no execution row after grace window — retrying nudge once")
        if not nudge_job(job_id):
            log("  FATAL: retry nudge failed — aborting chain")
            return False
        status, detail, err = wait_for_dispatch(job_id, since_ts)

    ok = status == "completed"
    log(f"  {stage_name} result: {status} ({detail}) {err[:200] if err else ''}")

    # Always re-lock the stage job, regardless of outcome, so the legacy
    # :00/:15/:30 schedule can never fire it outside this runner.
    if not pause_job(job_id):
        log(f"  WARNING: failed to re-pause {job_id} — legacy schedule may fire it; needs manual lock")
    return ok


def atomic_write_state(state: dict) -> None:
    tmp = CYCLE_STATE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=1)
    os.replace(tmp, CYCLE_STATE)


def preflight() -> bool:
    log("preflight checks")
    ok = True
    if not os.path.exists(EXEC_DB):
        log("  FATAL: executions.db missing"); ok = False
    for stage_name, job_id in STAGES:
        j = job_state(job_id)
        if not j:
            log(f"  FATAL: job {job_id} ({stage_name}) not found in jobs.json"); ok = False
    if ok:
        log("  stage jobs: " + ", ".join(
            f"{(job_state(i) or {}).get('name', i).split(':')[0].strip()}[{(job_state(i) or {}).get('state', '?')}]"
            for _, i in STAGES))
    rc, _ = run_hermes(["cron", "status"])
    if rc != 0:
        log("  FATAL: hermes gateway not responding"); ok = False
    return ok


def main() -> int:
    log("=== PIPELINE CYCLE START ===")

    # ---- Non-blocking lock: never run two cycles concurrently ----
    lock_fd = os.open(LOCK_FILE, os.O_RDWR | os.O_CREAT, 0o644)
    lock_fh = os.fdopen(lock_fd, "r+")
    try:
        fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log("WARNING: prior 2h cycle still holds /tmp/hermes_pipeline.lock — skipping this cycle (clean exit)")
        try:
            holder = lock_fh.read().strip()
            if holder:
                log(f"  lock holder: {holder}")
        except Exception:
            pass
        return 0
    lock_fh.seek(0)
    lock_fh.truncate()
    lock_fh.write(f"pid={os.getpid()} acquired={datetime.now(timezone.utc).isoformat()}\n")
    lock_fh.flush()

    report = {"cycle_started": datetime.now(timezone.utc).isoformat(),
              "stages": [], "outcome": None, "exit_code": 2}
    try:
        if not preflight():
            report["outcome"] = "ERROR"
            return 2
        all_ok = True
        for stage_name, job_id in STAGES:
            ok = dispatch_and_wait(stage_name, job_id)
            report["stages"].append({"stage": stage_name, "job_id": job_id,
                                     "ok": ok,
                                     "finished_at": datetime.now(timezone.utc).isoformat()})
            if not ok:
                all_ok = False
                log(f"ABORT: {stage_name} did not complete — skipping downstream stages (stale-data guard)")
                break
        report["outcome"] = "OK" if all_ok else "ABORTED"
        report["exit_code"] = 0 if all_ok else 1
    except Exception as e:
        log(f"FATAL: unhandled exception: {e}")
        report["outcome"] = "ERROR"
        report["exit_code"] = 2

    # ---- Dead-Man's Switch: post-cycle semantic audit (E#1.3) ----------
    # Catches empty cycles / null-metric collapse / vLLM degradation that
    # pass exit-code checks. Only escalates the exit code to 3 when the
    # stages themselves completed (a stage failure already owns exit 1/2).
    if report["exit_code"] in (0, 1):
        try:
            # deadman_audit lives in CRON_DIR, not on sys.path when the runner
            # is launched from the scripts/ mirror — put it on the path first.
            sys.path.insert(0, CRON_DIR)
            import deadman_audit
            dm = deadman_audit.run_audit(cycle_start_ts=time.time(), quiet=False)
            report["deadman"] = dm
            if dm["tripped"] and report["exit_code"] == 0:
                report["exit_code"] = 3
                report["outcome"] = "DEGRADED"
                log("DEAD-MAN TRIP: stages exited 0 but semantic audit failed — exit 3")
        except Exception as e:
            # The audit must never break the cycle chain — degrade the audit,
            # not the pipeline. Log loudly; the watchdog's executions.db
            # checks still catch upstream failures.
            log(f"WARNING: deadman audit self-error (cycle outcome preserved): {e}")
            report["deadman"] = {"error": str(e)}

    report["cycle_finished"] = datetime.now(timezone.utc).isoformat()
    atomic_write_state(report)
    log(f"=== PIPELINE CYCLE END: {report['outcome']} (exit {report['exit_code']}) ===")
    return report["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
