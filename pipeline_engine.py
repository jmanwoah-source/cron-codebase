#!/usr/bin/env python3
"""Pipeline engine — deterministic phases that take load off the LLM.

Phases:
  health           : gate checks + pipeline_health.md report (zero-LLM)
  deepdive         : FMP fetch + financial scoring -> /tmp/dd_briefing.json
  sources          : SearXNG fan-out (queries from pipeline_config.json),
                     dedup vs feed_raw.json, junk-URL filter, ticker hints ->
                     /tmp/sources_candidates.json (LLM quality-bar judges)
  sources apply    : read /tmp/sources_keep.json, fetch article text, append to
                     feed_raw.json, write query_health.json ledger
  process          : dedup feed items, diff vs existing briefs ->
                     /tmp/process_items.json (LLM So-What judgment only)
  process apply    : read /tmp/process_briefs_final.json, write signal_briefs.json
                     (rolling 14d window), deterministic conviction scores ->
                     vetted_tickers.json, wiki ticker notes, research_interest.md,
                     orchestrator_output.json
  tuner            : self-gated (24h) query scoring on last 12 ledger windows ->
                     /tmp/tuner_report.json (flags) + monitor_report.md block
  tuner apply      : A/B test LLM candidate queries (/tmp/tuner_candidates.json)
                     vs originals on SearXNG, commit winners to
                     pipeline_config.json, append query_changes.jsonl
  deepdive health  etc.:  run as:  pipeline_engine.py <phase> [apply] [force]

Run:  python3 pipeline_engine.py <phase> [apply] [force]
Config: ~/.hermes/cron/pipeline_config.json
"""
import concurrent.futures
import difflib
import hashlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser

HERE = os.path.expanduser("~/.hermes/cron")
CONFIG = json.load(open(os.path.join(HERE, "pipeline_config.json")))
CRON = os.path.expanduser(CONFIG["cron_dir"])
WIKI = os.path.expanduser(CONFIG["wiki_dir"])
# --- Mandatory path invariants (clean-slate standardization 2026-11-05) ---
from pathlib import Path as _Path
WIKI_ROOT = _Path("/home/jaren/wiki").resolve(strict=True)
assert not (WIKI_ROOT / "wiki").exists(), "FATAL: Nested wiki directory detected."
COMPANIES_DIR = WIKI_ROOT / "companies"
assert COMPANIES_DIR.is_dir() and not COMPANIES_DIR.is_symlink(), \
    "FATAL: companies/ missing or symlink."
# --- end invariants ---
TH = CONFIG["thresholds"]
S_CFG = CONFIG["sources"]
P_CFG = CONFIG["process"]
T_CFG = CONFIG["tuner"]
DD_CFG = CONFIG.get("deepdive", {})

UA = {"User-Agent": "Mozilla/5.0 (compatible; LouPipeline/1.0)"}


# ---------------------------------------------------------------- utilities
def now_iso():
    return datetime.now(timezone.utc).isoformat()


def http_get(url, timeout=15):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except Exception as e:
        return None, str(e)


def http_ok(url, timeout=8):
    st, _ = http_get(url, timeout=timeout)
    return st is not None and st < 400


def age_min(path):
    try:
        return round((time.time() - os.path.getmtime(path)) / 60.0, 1)
    except OSError:
        return None


def read_json(path, default):
    try:
        return json.load(open(path))
    except Exception:
        return default


def write_json(path, obj):
    # Atomic write (2026-10-26): shared fmp_client implementation (orjson +
    # same-dir temp + fsync + os.replace). A kill mid-write can no longer
    # leave a truncated load-bearing artifact (dd_briefing.json,
    # watchlist_state.json, jobs.json, cache files) — readers see old OR new,
    # never partial.
    sys.path.insert(0, os.path.expanduser("~/.hermes/scripts"))
    import fmp_client
    return fmp_client.atomic_write_json(path, obj)


def norm_headline(s):
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", (s or "").lower())).strip()


def ts_parse(s):
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


_DOLLAR_RE = re.compile(r"\$\s?(\d[\d,]*\.?\d*)\s*(billion|trillion|million|thousand|bn|b|t|m|k)(?![a-z])", re.IGNORECASE)
_DOLLAR_PLAIN_RE = re.compile(r"\$\s?(\d[\d,]*\.?\d*)(?!\d)")
_DOLLAR_MULT = {"billion": 1e9, "bn": 1e9, "b": 1e9, "trillion": 1e12,
                "t": 1e12, "million": 1e6, "m": 1e6, "thousand": 1e3, "k": 1e3}


def extract_dollar_amount(text):
    """First concrete dollar figure in the text, normalized to integer USD.

    Feeds the Layer-3 dedup entity-number guard: (ticker, dollar_amount) is a
    stable catalyst identity that survives headline rewrites. Returns the
    normalized int or None. "$20B" and "$20,000,000,000" normalize identically.
    Ignores bare numbers under $1M (noise) and percentages/odds like "$4/1"."""
    if not text:
        return None
    best = None
    for rx, has_suffix in ((_DOLLAR_RE, True), (_DOLLAR_PLAIN_RE, False)):
        for m in rx.finditer(text):
            try:
                raw = float(m.group(1).replace(",", ""))
            except ValueError:
                continue
            suf = (m.group(2) or "").lower() if has_suffix else None
            amt = raw * (_DOLLAR_MULT[suf] if suf else 1)
            if amt >= 1_000_000 and (best is None or amt > best):
                best = amt
    return int(round(best)) if best else None


# ---------------------------------------------------------------- health
def phase_health():
    lines = []
    fail = []

    def emit(key, status, is_fail=False):
        lines.append(f"- {key}: {status}")
        if is_fail:
            fail.append(key)

    st, _ = http_get(CONFIG["vllm_health_url"])
    emit("vllm_endpoint", "UP" if st == 200 else f"DOWN({st})", st != 200)

    st, _ = http_get(CONFIG["searxng_url"] + "/healthz")
    emit("searxng_api", "UP" if st == 200 else f"DOWN({st})", st != 200)

    try:
        import subprocess
        r = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", "searxng"],
                           capture_output=True, text=True, timeout=8)
        running = r.stdout.strip() == "true"
        emit("searxng_container", "RUNNING" if running else "DOWN", not running)
    except Exception as e:
        emit("searxng_container", f"UNKNOWN({type(e).__name__})")

    for name in ("feed_raw.json", "signal_briefs.json", "vetted_tickers.json"):
        a = age_min(os.path.join(CRON, name))
        if a is None:
            emit(f"file_{name}", "MISSING", True)
        elif a > TH["stale_file_min"]:
            emit(f"file_{name}", f"STALE_{int(a // 60)}h")
        else:
            emit(f"file_{name}", "FRESH")

    stt = os.statvfs("/")
    free_pct = round(stt.f_bavail / stt.f_blocks * 100, 1)
    emit("disk_free", f"{'LOW' if free_pct < TH['disk_low_pct'] else 'OK'}_{free_pct}pct",
         free_pct < TH["disk_low_pct"])

    drift = os.path.isdir(os.path.join(CRON, "wiki", "entities", "tickers")) or os.path.isdir(os.path.expanduser("~/wiki/entities"))
    emit("wiki_drift", "SPLIT" if drift else "OK", drift)

    import sqlite3
    con = sqlite3.connect(os.path.join(CRON, "executions.db"))
    rows = con.execute(
        "SELECT job_id, status, started_at, finished_at, substr(COALESCE(error,'-'),1,120) "
        "FROM executions ORDER BY started_at DESC LIMIT 12").fetchall()
    names = {"ba24fd4ddad6": "Sources", "cfdab394b3bd": "Processing", "7267b662cf52": "Deep-Dive"}
    seen = {}
    for jid, status, s, f, err in rows:
        if jid in seen:
            continue
        try:
            sdt = datetime.fromisoformat(s).replace(tzinfo=None)
            fdt = datetime.fromisoformat(f).replace(tzinfo=None)
            dur = round((fdt - sdt).total_seconds() / 60, 1)
        except Exception:
            dur = None
        seen[jid] = (status, dur, err)
    exec_lines = []
    for jid in ("ba24fd4ddad6", "cfdab394b3bd", "7267b662cf52"):
        if jid in seen:
            status, dur, err = seen[jid]
            exec_lines.append(f"- {names[jid]}: {status} · {dur if dur is not None else '?'}min · {err if err != '-' else '-'}")

    feed = read_json(os.path.join(CRON, "feed_raw.json"), None)
    feed_n = len(feed) if isinstance(feed, list) else (len(feed.get("items", [])) if isinstance(feed, dict) else 0)
    briefs_doc = read_json(os.path.join(CRON, "signal_briefs.json"), {})
    briefs_n = briefs_doc.get("signal_briefs_generated") if isinstance(briefs_doc, dict) else None
    vetted = read_json(os.path.join(CRON, "vetted_tickers.json"), {})
    vetted_n = len(vetted.get("all_tickers", [])) if isinstance(vetted, dict) else 0
    scores = read_json(os.path.join(CRON, "ticker_scores.json"), {})
    scores_ts = scores.get("run_timestamp", "?") if isinstance(scores, dict) else "?"

    ledger = read_json(os.path.join(CRON, "query_health.json"), {})
    hist = ledger.get("history", []) if isinstance(ledger, dict) else []
    newest = hist[-1]["ts"] if hist else "?"

    usage = []
    try:
        with open(os.path.join(CRON, "usage_audit.jsonl")) as f:
            usage = [json.loads(l) for l in f.readlines()[-3:]]
    except Exception:
        pass
    usage_lines = []
    for u in usage:
        usage_lines.append(f"- {names.get(u['job_id'], u['job_id'][:8])}: {u.get('total_tokens', 0)/1e6:.2f}M tokens @ {u.get('ts', '?')[:16]}")

    verdict = "FAIL: " + ", ".join(fail) if fail else "PASS"
    report = f"""# Pipeline Health — {now_iso()}
- Overall: {verdict}
{chr(10).join(exec_lines)}
- Data: feed={feed_n} items · briefs={briefs_n} · vetted={vetted_n} · scores_run={scores_ts}
- Query ledger: {len(hist)} rows, newest {newest} (Tuner due: {'YES' if len(hist) >= 12 and age_min(os.path.join(CRON, 'query_health.json')) > 1440 else 'NO'})
{chr(10).join(usage_lines)}
{chr(10).join(lines)}
"""
    path = os.path.join(CRON, "pipeline_health.md")
    old = open(path).read() if os.path.exists(path) else ""
    # keep only well-formed blocks (legacy LLM-assembled blocks may lack the '# ' prefix)
    blocks = [b for b in old.split("# Pipeline Health —") if b.strip() and "Overall:" in b]
    blocks = ["# Pipeline Health —" + b for b in blocks]
    blocks.append(report)
    with open(path, "w") as f:
        f.write("\n\n".join(blocks[-10:]))
    print(report)
    print(f"[engine:health] verdict={verdict} report={path}")
    return 0 if not fail else 1


# ---------------------------------------------------------------- deepdive
FMP_URL = CONFIG["fmp_mcp_url"]

# --- MCP session state + auto re-initialize (directive 2026-11-07) ---
# Shared Mcp-Session-Id with lock-serialized re-init. Any tool call that
# hits a dead-session signature (HTTP 400/404 on the streamable session,
# session-lost/disconnect body, JSON-RPC transport error) re-issues the
# /mcp initialize handshake, takes a 1s backoff, and retries ONCE — a
# ticker is never failed on a dead session. fmp_raw_send returns raw
# (status, body, exception-string-or-None); fmp_call keeps its original
# {dict} contract so all call sites stay untouched.
_FMP_SESSION = {"sid": None}
_FMP_SESS_LOCK = threading.Lock()
_FMP_BACKOFF_S = 1.0


def fmp_session_init():
    payload = json.dumps({"jsonrpc": "2.0", "method": "initialize",
                          "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                                     "clientInfo": {"name": "pipeline-engine", "version": "1.0"}},
                          "id": 1}).encode()
    st, body, headers, exc = _fmp_http_post(payload)
    if exc is not None:
        return None
    # FMP issues the session id in the RESPONSE HEADER (verified live
    # 2026-11-07); the body's result.sessionId is only a fallback.
    sid = headers.get("mcp-session-id") or headers.get("Mcp-Session-Id")
    if not sid:
        try:
            d = json.loads(body)
            sid = d.get("result", {}).get("sessionId")
        except Exception:
            pass
    return sid


def fmp_session():
    # Lock-serialized so parallel workers can't fork N fresh handshakes;
    # callers reuse the shared id when one is already live.
    with _FMP_SESS_LOCK:
        if _FMP_SESSION["sid"]:
            return _FMP_SESSION["sid"]
        sid = fmp_session_init()
        if not sid:
            # A dead initialize must degrade to per-ticker _error, not kill
            # the whole phase (2026-10-26 hardening: Phase 0 raised
            # calls-per-ticker, so a shared-session failure has a bigger
            # blast radius).
            print("[engine:fmp] session init failed", file=sys.stderr)
            return None
        _FMP_SESSION["sid"] = sid
        return sid


def _fmp_http_post(payload):
    """Raw MCP POST -> (status, body, headers, exc_str). Never raises."""
    req = urllib.request.Request(FMP_URL, data=payload, method="POST",
                                 headers={"Content-Type": "application/json",
                                          "Accept": "application/json, text/event-stream"})
    try:
        with urllib.request.urlopen(req, timeout=45) as r:
            return r.status, r.read().decode("utf-8", "replace"), dict(r.headers), None
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", "replace")
        except Exception:
            body = ""
        return e.code, body, dict(e.headers or {}), str(e)
    except Exception as e:
        return None, "", {}, str(e)


def fmp_raw_send(sid, payload):
    """Raw tools/call POST with session header -> (status, body, exc_str)."""
    req = urllib.request.Request(FMP_URL, data=payload, method="POST",
                                 headers={"Content-Type": "application/json",
                                          "Accept": "application/json, text/event-stream",
                                          "Mcp-Session-Id": sid or ""})
    try:
        with urllib.request.urlopen(req, timeout=45) as r:
            return r.status, r.read().decode("utf-8", "replace"), None
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", "replace")
        except Exception:
            body = ""
        return e.code, body, str(e)
    except Exception as e:
        return None, "", str(e)


def _is_session_failure(st, body, exc):
    """Dead-session signature: 400/404 on the streamable session, a
    session-lost/disconnect body, or a JSON-RPC transport error code."""
    if st in (400, 404):
        return True
    if not body:
        # Network/timeout exception with no body: can't distinguish —
        # do NOT re-init (would fork sessions on a transient blip);
        # the caller's single retry on the same sid is the recovery.
        return False
    low = body.lower()
    if "session" in low and ("expired" in low or "invalid" in low or "not found"
                             in low or "lost" in low or "missing" in low
                             or "disconnect" in low):
        return True
    try:
        d = json.loads(body)
        err = d.get("error")
        if isinstance(err, dict):
            code = err.get("code")
            if code in (-32700, -32000, -32001, -32002):
                return True
            msg = str(err.get("message") or "").lower()
            if "session" in msg or "transport" in msg:
                return True
    except Exception:
        pass
    return False


def fmp_reinit():
    """Fresh /mcp initialize handshake (1s backoff), lock-serialized.
    Returns the fresh Mcp-Session-Id or None."""
    time.sleep(_FMP_BACKOFF_S)
    with _FMP_SESS_LOCK:
        sid = fmp_session_init()
        if sid:
            _FMP_SESSION["sid"] = sid
            print("[engine:fmp] session re-initialized (auto-recover)", file=sys.stderr)
            return sid
        print("[engine:fmp] session re-init failed", file=sys.stderr)
        return None


def _fmp_recover(sid):
    """Recovery choice for a dead session: if `sid` IS the shared session,
    re-init and rotate the shared state (fmp_reinit); if `sid` is a
    detached local session, issue a fresh handshake WITHOUT touching the
    shared id (never clobber a healthy session another worker owns)."""
    if sid == _FMP_SESSION["sid"]:
        return fmp_reinit()
    return fmp_session_init()


def _fmp_parse(body):
    """SSE/JSON decode of an MCP tools/call body."""
    if body.startswith("event:") or "\ndata:" in body or body.lstrip().startswith("data:"):
        for line in body.splitlines():
            if line.startswith("data:"):
                body = line[5:].strip()
                break
    d = json.loads(body)
    content = d.get("result", {}).get("content", [])
    if content and isinstance(content, list) and content[0].get("type") == "text":
        try:
            return json.loads(content[0]["text"])
        except Exception:
            return {"_raw_text": content[0]["text"][:400]}
    return d.get("result")


def fmp_call(sid, name, arguments, call_id):
    payload = json.dumps({"jsonrpc": "2.0", "method": "tools/call",
                          "params": {"name": name, "arguments": arguments},
                          "id": call_id}).encode()
    for attempt in (1, 2):
        st, body, exc = fmp_raw_send(sid, payload)
        if exc is not None and st is None:
            # Network-level failure: retry ONCE on the same sid (no
            # re-init — a blip shouldn't fork a new session).
            if attempt == 2:
                return {"_error": str(exc)}
            time.sleep(_FMP_BACKOFF_S)
            continue
        if exc is not None or _is_session_failure(st, body, None):
            # HTTP 400/404 / session-lost / JSON-RPC transport error:
            # fresh handshake + 1s backoff + retry once.
            nsid = _fmp_recover(sid)
            if not nsid:
                return {"_error": f"mcp-session-lost (re-init failed): {str(exc)[:100] or body[:100]}"}
            sid = nsid
            if attempt == 2:
                continue
            continue
        try:
            return _fmp_parse(body)
        except Exception:
            low = body.lower()
            # Last resort: a session-related text error the JSON parse
            # couldn't see (HTML/502 page) — still recover once.
            if attempt == 2 or ("session" not in low and "transport" not in low):
                return {"_error": "unparseable", "_body": body[:300]}
            nsid = _fmp_recover(sid)
            if not nsid:
                return {"_error": "unparseable", "_body": body[:300]}
            sid = nsid
    return {"_error": "mcp-retry-exhausted"}


def financial_scoring(km, inc, incg, bal):
    """Deterministic 0-100 financial score from FMP data (5 x 20pt bands)."""
    bands = CONFIG["financial_scoring_bands"]

    def band_score(value, band_key):
        if value is None:
            return 8
        for floor, pts in bands[band_key]:
            if value >= floor:
                return pts
        return 0

    def g(d, key):
        try:
            return float(d.get(key)) if d and isinstance(d, dict) else None
        except (TypeError, ValueError):
            return None

    rg = g(incg, "growthRevenue") * 100 if g(incg, "growthRevenue") is not None else None
    rg_pts = band_score(rg, "revenue_growth")
    rev = g(inc, "revenue"); opinc = g(inc, "operatingIncome")
    om = (opinc / rev * 100) if (rev and opinc is not None) else None
    om_pts = band_score(om, "operating_margin")
    ni = g(inc, "netIncome")
    nm = (ni / rev * 100) if (rev and ni is not None) else None
    nm_pts = band_score(nm, "net_margin")
    total_debt = g(bal, "totalDebt"); equity = g(bal, "totalStockholdersEquity") or g(bal, "totalEquity")
    de = (total_debt / equity) if (total_debt is not None and equity and equity > 0) else None
    cr = g(km, "currentRatio")
    if de is None and cr is None:
        bs_pts = 8
    elif de is not None and cr is not None:
        if de < 1 and cr >= 1.5: bs_pts = 20
        elif de < 2 and cr >= 1: bs_pts = 16
        elif de < 3 and cr >= 0.8: bs_pts = 12
        elif de < 5: bs_pts = 8
        else: bs_pts = 4
    elif cr is not None:
        bs_pts = 16 if cr >= 1.5 else (12 if cr >= 1 else (8 if cr >= 0.7 else 4))
    else:
        bs_pts = 16 if de < 1 else (12 if de < 2 else (8 if de < 4 else 4))
    fcfy = g(km, "freeCashFlowYield")
    fcf_pct = fcfy * 100 if fcfy is not None else None
    if fcf_pct is None:
        cash_pts = 8
    elif fcf_pct >= 5: cash_pts = 20
    elif fcf_pct >= 0: cash_pts = 14
    elif fcf_pct >= -5: cash_pts = 8
    else: cash_pts = 4
    roe = g(km, "returnOnEquity")
    roe_pct = roe * 100 if roe is not None else None
    if roe_pct is None:
        prof_pts = nm_pts
    elif roe_pct >= 25: prof_pts = 20
    elif roe_pct >= 15: prof_pts = 16
    elif roe_pct >= 8: prof_pts = 12
    elif roe_pct >= 0: prof_pts = 8
    else: prof_pts = 0

    # F1 FIX (2026-11-04): data-coverage confidence. A missing band is still
    # scored at the neutral 8 for raw continuity, but a score is only as
    # trustworthy as the data behind it. data_coverage = fraction of the 5 bands
    # backed by REAL FMP data (not the neutral-8 default); adjusted = raw scaled
    # by (0.5 + 0.5*coverage) so a data-poor name ranks below a data-rich name
    # with the same raw, and a 0-data name caps at 50% (never nukes a name,
    # never rewards a gap). gap_bands names exactly what is missing so the LLM
    # never reads a gap as a real zero.
    rg_present = g(incg, "growthRevenue") is not None
    om_present = bool(rev) and opinc is not None
    bs_present = (de is not None) or (cr is not None)
    cash_present = fcfy is not None
    prof_present = (roe_pct is not None) or (nm is not None)
    _present = {"revenue_growth": rg_present, "operating_margin": om_present,
                "balance_sheet": bs_present, "cash_liquidity": cash_present,
                "profitability": prof_present}
    gap_bands = [k for k, v in _present.items() if not v]
    data_coverage = round((5 - len(gap_bands)) / 5.0, 3)

    total = rg_pts + om_pts + bs_pts + cash_pts + prof_pts

    # --- PIT DATE EXTRACTION (Phase 3): Extract bitemporal dates from FMP payloads ---
    _fin_dates = {}
    # period_end_date from income statement (inc param)
    if isinstance(inc, dict) and inc:
        _fin_dates['period_end_date'] = inc.get('periodEndDate') or inc.get('reportDate') or inc.get('asOfDate')
    # sec_accepted_at from income statement growth (incg param) or income statement
    if isinstance(incg, dict) and incg:
        _fin_dates['sec_accepted_at'] = incg.get('acceptedDate') or incg.get('filingDate') or incg.get('asOfDate')
    # asof_date from key metrics (km param)
    if isinstance(km, dict) and km:
        _fin_dates['asof_date'] = km.get('asOfDate') or km.get('calendarDate') or _fin_dates.get('asof_date')
    # Also try balance sheet and cash flow
    if isinstance(bal, dict) and bal:
        if not _fin_dates.get('period_end_date'):
            _fin_dates['period_end_date'] = bal.get('periodEndDate') or bal.get('reportDate') or bal.get('asOfDate')
        if not _fin_dates.get('sec_accepted_at'):
            _fin_dates['sec_accepted_at'] = bal.get('acceptedDate') or bal.get('filingDate') or bal.get('asOfDate')
        if not _fin_dates.get('asof_date'):
            _fin_dates['asof_date'] = bal.get('asOfDate') or bal.get('calendarDate')
    # fin_source tracks which endpoint provided the dates
    sources = []
    if isinstance(inc, dict) and inc and inc.get('periodEndDate'):
        sources.append('income-statement')
    if isinstance(incg, dict) and incg and incg.get('filingDate'):
        sources.append('income-statement-growth')
    if isinstance(km, dict) and km and km.get('asOfDate'):
        sources.append('key-metrics')
    if sources:
        _fin_dates['fin_source'] = ';'.join(sources)
    _fin_dates['fin_data_coverage'] = round(len(sources) / 3.0, 3)
    # Filter: reject rows where sec_accepted_at is in the future relative to current eval time
    # (This is a soft guard — the hard PIT rule is enforced in queries that use eval_ts)
    if _fin_dates.get('sec_accepted_at'):
        try:
            from datetime import datetime
            _accepted = datetime.fromisoformat(_fin_dates['sec_accepted_at'].replace('Z', '+00:00'))
            _now = datetime.now(_accepted.tzinfo) if _accepted.tzinfo else datetime.now()
            if _accepted > _now:
                # Future-dated filing — mark as potentially stale/unverified
                _fin_dates['sec_accepted_at'] = None
                _fin_dates['warning'] = 'sec_accepted_at_in_future'
        except (ValueError, TypeError):
            _fin_dates['warning'] = 'sec_accepted_at_parse_error'

    # Store dates in the return dict under a new 'pitt' key
    _return_fin = {"period_end_date": _fin_dates.get('period_end_date'),
                   "sec_accepted_at": _fin_dates.get('sec_accepted_at'),
                   "asof_date": _fin_dates.get('asof_date'),
                   "fin_source": _fin_dates.get('fin_source'),
                   "fin_data_coverage": _fin_dates.get('fin_data_coverage')}

    return {
        "financial_100": total,
        "adjusted": int(round(total * (0.5 + 0.5 * data_coverage))),
        "data_coverage": data_coverage,
        "gap_bands": gap_bands,
        "sub_scores": {
            "revenue_growth_20": rg_pts,
            "margin_profile_20": om_pts,
            "balance_sheet_20": bs_pts,
            "cash_liquidity_20": cash_pts,
            "profitability_20": prof_pts,
        },
        "raw": {"revenue_growth_pct": round(rg, 2) if rg is not None else None,
                "operating_margin_pct": round(om, 2) if om is not None else None,
                "net_margin_pct": round(nm, 2) if nm is not None else None,
                "debt_to_equity": round(de, 2) if de is not None else None,
                "current_ratio": round(cr, 2) if cr is not None else None,
                "fcf_yield_pct": round(fcf_pct, 2) if fcf_pct is not None else None,
                "roe_pct": round(roe_pct, 2) if roe_pct is not None else None},
    }


def bifurcation_scoring(bal_a, bal_q, inc_q, cf_q, market_cap, kdata=None, bif_8k=None):
    """Two-track deterministic sub-scores (user spec 2026-10-18, Deep Dive Research 1.2).

    FMP gaps (documented, never faked): no retained-earnings split (X2=0 when
    absent), no raw-materials/WIP inventory split, no RPO field, no S-3/ATM
    flag (dilution is measured via share count YoY). Those checklist items
    score from brief text at the LLM layer.

    2026-11-03 directive: bif_8k (optional) carries 8-K fast-pass extracts
    {backlog_usd, book_to_bill, fy_guide_usd, ...} for the PSFV carve-out and
    P1/P4 gates. Absent = engine-side signals only (backlog term of the
    carve-out simply cannot fire).
    """
    def fnum(d, key):
        try:
            v = d.get(key) if d else None
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    def rnd(v, n=2):
        return round(v, n) if v is not None else None

    flags, notes = [], []

    # ---- Altman Z (Track A / Track C anchor) ----
    total_assets = fnum(bal_a, "totalAssets")
    inc0 = inc_q[0] if isinstance(inc_q, list) and inc_q and isinstance(inc_q[0], dict) else {}
    ebit = fnum(inc0, "ebit") or fnum(bal_a, "ebit")
    revenue = fnum(inc0, "revenue")
    equity = fnum(bal_a, "totalStockholdersEquity") or fnum(bal_a, "totalEquity")
    ret_earn = fnum(bal_a, "retainedEarnings")
    wc = (fnum(bal_a, "totalCurrentAssets") or 0) - (fnum(bal_a, "totalCurrentLiabilities") or 0)
    if total_assets is None or total_assets <= 0:
        altman_z = None
        flags.append("altman_z_unavailable")
    else:
        if ret_earn is None:
            ret_earn = 0.0
            flags.append("retained_earnings_missing_x2_zeroed")
        x1 = wc / total_assets
        x2 = ret_earn / total_assets
        x3 = (ebit / total_assets) if ebit is not None else 0.0
        x4 = (market_cap / equity) if (market_cap and equity and equity > 0) else (
            (market_cap / total_assets) if (market_cap and total_assets > 0) else 0.0)
        x5 = (revenue / total_assets) if revenue is not None else 0.0
        altman_z = 1.2 * x1 + 1.4 * x2 + 3.3 * x3 + 0.6 * x4 + 1.0 * x5

    def z_pts(z, fcf_pct, de):
        # spec 2026-10-18: 40 = "Altman Z > 3.0, high FCF, low debt"
        if z is None:
            return 0
        if z > 3.0: base = 40
        elif z >= 2.6: base = 34
        elif z >= 2.2: base = 28
        elif z >= 1.8: base = 20
        elif z >= 1.2: base = 10
        else: base = 0
        if fcf_pct is not None and fcf_pct < 0: base -= 10  # FCF not high
        if de is not None and de > 2: base -= 10  # debt not low
        return max(0, min(40, base))

    fcfy = (kdata or {}).get("freeCashFlowYield")
    fcf_pct = fcfy * 100 if fcfy is not None else None
    de_ratio = None
    eq_chk = fnum(bal_a, "totalStockholdersEquity") or fnum(bal_a, "totalEquity")
    td_chk = fnum(bal_a, "totalDebt")
    if td_chk is not None and eq_chk and eq_chk > 0:
        de_ratio = td_chk / eq_chk

    # ---- Cash runway (Track B pillar 1 / Track C gate 1) ----
    cash_sti = fnum(bal_a, "cashAndShortTermInvestments") or fnum(bal_a, "cashAndCashEquivalents")
    ocf = None
    if isinstance(cf_q, list) and cf_q:
        for row in cf_q:
            if isinstance(row, dict) and row.get("operatingCashFlow") is not None:
                ocf = float(row["operatingCashFlow"])
                break
    if cash_sti is None or cash_sti <= 0 or ocf is None:
        runway_q = None
        if ocf is not None and ocf >= 0:
            runway_q = 99.0  # OCF already positive -> full runway by spec
        if runway_q is None:
            flags.append("runway_unavailable")
    elif ocf >= 0:
        runway_q = 99.0
    else:
        runway_q = cash_sti / abs(ocf)

    def runway_pts(r):
        if r is None: return 0
        if r >= 12: return 20
        if r >= 9: return 17
        if r >= 6: return 14
        if r >= 4: return 9
        if r >= 2: return 4
        return 0

    # ---- Dilution kill-switch (Track C gate 2) ----
    shs = None
    if isinstance(inc_q, list) and len(inc_q) >= 5:
        s0, s1 = fnum(inc_q[0], "weightedAverageShsOut"), fnum(inc_q[4], "weightedAverageShsOut")
        if s0 and s1:
            shs = s0 / s1 - 1.0
    dilution_kill = bool(shs is not None and shs > 0.08)

    # ---- 2026-11-03 directive additions (AEHR/AAOI fast-pass inputs) ----
    # Cash/Debt ratio (P1 waiver gate 1; None when no debt data)
    cash_debt = None
    td_v = fnum(bal_a, "totalDebt")
    if cash_sti is not None and td_v is not None:
        cash_debt = (cash_sti - td_v) / td_v if td_v > 0 else (99.0 if cash_sti >= td_v else None)
    # Revenue growth signals from the quarterly income rows (newest first)
    rev_qoq_pct = rev_yoy_pct = rev_latest_q = rev_ttm = None
    revs = []
    if isinstance(inc_q, list):
        for row in inc_q:
            if isinstance(row, dict):
                rv = fnum(row, "revenue")
                if rv is not None:
                    revs.append(rv)
    if len(revs) >= 2 and revs[1]:
        rev_qoq_pct = (revs[0] / revs[1] - 1.0) * 100.0
    if len(revs) >= 5 and revs[4]:
        rev_yoy_pct = (revs[0] / revs[4] - 1.0) * 100.0
    if revs:
        rev_latest_q = revs[0]
        rev_ttm = sum(revs[:4])
    # PSFV (Per-Share Fundamental Velocity — directive #3: deterministic,
    # computed HERE in Python and injected as a read-only float. The LLM
    # never calculates PSFV in text. Share growth from weightedAverageShsOutDil
    # (diluted, preferred) falling back to basic. Backlog/RPO growth come from
    # the 8-K fast-pass layer (not in FMP) — None here unless injected.
    shs_dil = None
    if isinstance(inc_q, list) and len(inc_q) >= 5:
        d0, d1 = fnum(inc_q[0], "weightedAverageShsOutDil"), fnum(inc_q[4], "weightedAverageShsOutDil")
        if d0 and d1:
            shs_dil = d0 / d1 - 1.0
    shs_growth = shs_dil if shs_dil is not None else shs
    psfv_val = None
    if shs_growth is not None and rev_yoy_pct is not None:
        psfv_val = round((1.0 + max(rev_yoy_pct / 100.0, 0.0)) / (1.0 + shs_growth) - 1.0, 4)

    # ---- Gross margin trend QoQ (Track C gate 3) ----
    gm_pct = []
    if isinstance(inc_q, list):
        for row in inc_q:
            if not isinstance(row, dict):
                continue
            rv, gp = fnum(row, "revenue"), fnum(row, "grossProfit")
            gm_pct.append((gp / rv * 100.0) if (rv and gp is not None) else None)
    gm_valid = [g for g in gm_pct if g is not None]
    gm_qoq_bps = None
    if len(gm_valid) >= 2:
        gm_qoq_bps = (gm_valid[0] - gm_valid[1]) * 100
    margin_declining = bool(gm_qoq_bps is not None and gm_qoq_bps < -50)  # >50bps shrink QoQ

    # ---- Deferred revenue delta (Track B pillar 4 / Category D #1) ----
    dr = None
    if isinstance(bal_q, list) and len(bal_q) >= 2:
        d0, d1 = fnum(bal_q[0], "deferredRevenue"), fnum(bal_q[1], "deferredRevenue")
        if d0 is not None and d1 is not None and d1:
            dr = (d0 - d1) / abs(d1)

    # ---- 2026-11-04 pillar lock: P1 Financial Fortress scaled to /30 ----
    # The 40-point z_pts stays as the raw engine reference (financial_100
    # legacy); the Track A P1 score uses the 30-scale: z_pts * 3/4 so the
    # band anchors map cleanly (40->30, 34->25.5, 28->21, 20->15, 10->7.5).
    # Working-Capital Ramp Waiver (2026-11-03 directive #1, GAAP/Non-GAAP
    # split #5, 2026-11-04 re-scale): full +4 ONLY on GAAP evidence
    # (GAAP GM_Delta_QoQ >= +200bps OR GAAP OpMargin_Delta_QoQ >= +250bps);
    # partial +2 when Non-GAAP GM delta >= +200bps (8-K) AND GAAP GM delta
    # >= -50bps. WAIVER DENIED (0) whenever GAAP GM delta < -50bps. Cap
    # 27/30. Gate 1: Altman Z > 4.5 AND Cash/Debt >= 3.0 (or net-cash +
    # >= 8q runway). Gate 3 (deterministic backlog proxy only): B2B >= 1.2
    # OR Backlog/QRev >= 1.2 (8-K fast-pass) OR DeferredRev QoQ >= +15% OR
    # Rev QoQ >= +25%. totalCurrentLiabilities is BANNED as a backlog proxy.
    # The engine emits the deterministic inputs; the LLM applies the split
    # on 8-K Non-GAAP deltas where the engine cannot see them.
    op_margin_pct = []
    if isinstance(inc_q, list):
        for row in inc_q:
            if not isinstance(row, dict):
                continue
            rv, ob = fnum(row, "revenue"), fnum(row, "ebit")
            op_margin_pct.append((ob / rv * 100.0) if (rv and ob is not None) else None)
    om_valid = [g for g in op_margin_pct if g is not None]
    op_margin_qoq_bps = (om_valid[0] - om_valid[1]) * 100 if len(om_valid) >= 2 else None
    p1_40 = z_pts(altman_z, fcf_pct, de_ratio)
    p1_30_base = min(30, max(0, int(round(p1_40 * 3.0 / 4.0))))
    gaap_gm_qoq = gm_qoq_bps  # FMP income statements are GAAP by construction
    gaap_floor_ok = (gaap_gm_qoq is not None and gaap_gm_qoq >= -50)
    waiver_gate1 = bool(altman_z is not None and altman_z > 4.5 and
                        ((cash_debt is not None and cash_debt >= 3.0) or
                         (cash_debt is not None and cash_debt < 0 and
                          runway_q is not None and runway_q >= 8)))
    blq_8k = None
    if isinstance(bif_8k, dict) and bif_8k.get("backlog_usd") and rev_latest_q:
        blq_8k = bif_8k["backlog_usd"] / rev_latest_q
    waiver_gate3 = (
        ((bif_8k or {}).get("book_to_bill") is not None and (bif_8k or {}).get("book_to_bill") >= 1.2) or
        (blq_8k is not None and blq_8k >= 1.2) or
        ((dr is not None and dr * 100.0 >= 15.0) or
         (rev_qoq_pct is not None and rev_qoq_pct >= 25.0)))
    p1_waiver_gate = {
        "gate1_z45_cash3": waiver_gate1,
        "gate3_backlog": bool(waiver_gate3),
        "gaap_gm_qoq_bps": rnd(gaap_gm_qoq),
        "gaap_opmargin_qoq_bps": rnd(op_margin_qoq_bps),
        "gaap_floor_minus50bps": bool(gaap_floor_ok),
        "backlog_to_qrev_8k": rnd(blq_8k, 2),
        "nongaap_gm_qoq_bps_8k": ((bif_8k or {}).get("nongaap_gm_qoq_bps")
                                  if isinstance(bif_8k, dict) else None),
    }
    # deterministic waiver decision (LLM may only confirm it, not upgrade it):
    if waiver_gate1 and waiver_gate3 and (
            (gaap_gm_qoq is not None and gaap_gm_qoq >= 200) or
            (op_margin_qoq_bps is not None and op_margin_qoq_bps >= 250)):
        p1_waiver_pts = 4  # full (GAAP-verified)
    elif waiver_gate1 and waiver_gate3 and gaap_floor_ok:
        # partial +2 REQUIRES 8-K non-GAAP GM delta >= +200bps — computed
        # from the fast-pass extract when both margins are present, else the
        # LLM confirms from brief text and the validator checks the cap.
        p1_waiver_pts = 2
    else:
        p1_waiver_pts = 0
    if (gaap_gm_qoq is not None and gaap_gm_qoq < -50):
        p1_waiver_pts = 0  # HARD DENY: GAAP margin collapsing
    # 2026-11-04: the waiver is a RECOVERY of the FCF/working-capital penalty
    # (directive #1: restore points to a name that LOST them), capped at 27/30.
    # It must never LOWER a name already at full base (a perfect fortress that
    # needs no recovery stays at its base). max(base, ...) enforces the floor.
    if p1_waiver_pts:
        p1_30 = max(p1_30_base, min(27, p1_30_base + p1_waiver_pts))
    else:
        p1_30 = p1_30_base


    # PSFV dilution carve-out (directive #2, 2026-11-03): share growth >8%
    # no longer UNCONDITIONALLY caps Track B at 20. The cap fires only when
    # PSFV < 0.15 OR (OCF < 0 AND book-to-bill < 1.0). An accretive-dilution
    # carve-out (PSFV >= 0.35 AND [backlog >= 1.0x TTM rev OR deferred rev
    # QoQ >= +10% OR rev QoQ >= +25%]) waives the cap entirely (minor -5
    # friction instead of -30). Backlog from the 8-K fast-pass layer
    # (bif_8k); engine-side signals are the fallback evidence.
    track_b_cap_20 = False
    psfv_carveout = False
    if shs_growth is not None and shs_growth > 0.08 and psfv_val is not None:
        bk_8k = (bif_8k or {}).get("backlog_usd")
        b2b = (bif_8k or {}).get("book_to_bill")
        cap_triggers = (psfv_val < 0.15) or \
            ((ocf is not None and ocf < 0) and (b2b is not None and b2b < 1.0))
        accretive = psfv_val >= 0.35 and (
            ((bk_8k is not None and rev_ttm is not None and bk_8k >= rev_ttm) or
             ((dr is not None and dr * 100.0 >= 10.0) or
              (rev_qoq_pct is not None and rev_qoq_pct >= 25.0))))
        track_b_cap_20 = bool(cap_triggers and not accretive)
        psfv_carveout = bool(accretive)
        dilution_kill = dilution_kill and cap_triggers  # Track C severity aligns
    elif shs_growth is not None and shs_growth > 0.08 and psfv_val is None:
        # no PSFV computable (missing share data): fail SAFE = legacy kill
        track_b_cap_20 = True

    # ---- Track C composite (0-100; <=35 = Mirage). Runway <4q = primary tell,
    #      then ATM dilution, then LLM-assessed hype-without-substance. ----
    c_pts = 0
    if runway_q is not None:
        if runway_q < 2: c_pts += 50
        elif runway_q < 4: c_pts += 20
    elif runway_q is None and "runway_unavailable" in flags:
        c_pts += 10
    if dilution_kill: c_pts += 20
    if margin_declining: c_pts += 20
    c_pts = min(100, c_pts)

    return {
        "track_a_engine": {"financial_fortress_40": z_pts(altman_z, fcf_pct, de_ratio),
                           "raw_altman_z": rnd(altman_z, 3),
                           "raw_debt_to_equity": rnd(de_ratio, 2),
                           "raw_fcf_yield_pct": rnd(fcf_pct, 2)},
        "track_a_p1": {"p1_base_30": p1_30_base,
                       "p1_waiver_pts": p1_waiver_pts,
                       "p1_engine_30": p1_30,
                       "waiver_gate": p1_waiver_gate},
        "track_b_engine": {"runway_gate_20": runway_pts(runway_q),
                           "raw_runway_quarters": rnd(runway_q, 1)},
        "track_c_engine": {"track_c_100": c_pts},
        "signals": {
            "deferred_revenue_delta_pct_qoq": rnd((dr * 100) if dr is not None else None),
            "gross_margin_pct_latest": rnd(gm_valid[0] if gm_valid else None),
            "gross_margin_qoq_bps": rnd(gm_qoq_bps),
            "share_count_yoy_pct": rnd((shs * 100) if shs is not None else None),
            "dilution_kill_8pct": track_b_cap_20,
            "psfv_carveout": psfv_carveout,
            "psfv": psfv_val,
            "cash_to_debt_ratio": rnd(cash_debt, 3),
            "revenue_qoq_pct": rnd(rev_qoq_pct, 1),
            "revenue_yoy_pct": rnd(rev_yoy_pct, 1),
            "revenue_latest_q": int(rev_latest_q) if rev_latest_q is not None else None,
            "revenue_ttm": int(rev_ttm) if rev_ttm is not None else None,
            "margin_declining": margin_declining,
            "ocf_annualized_quarterly": int(ocf) if ocf is not None else None,
            "cash_short_term_investments": int(cash_sti) if cash_sti is not None else None,
        },
        "flags": flags,
        **({"_8k": bif_8k} if bif_8k else {}),
    }


def _wilder_rsi(closes, period=14):
    """Wilder RSI from a closes series (newest last). Pure helper."""
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0)); losses.append(max(-d, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)


def _technical_signals(rsi_rows):
    """Compact engine technical block from technicalIndicators RSI 1day rows
    (each row: date/open/high/low/close/volume/rsi, newest first). Pure.
    2026-10-26 hardening (adversarial review R2): fail-soft — any malformed bar
    feed returns an error marker, never raises; string/zero closes are coerced
    or dropped (FMP has returned str numerics and 0 closes on suspended days)."""
    try:
        rows = [r for r in (rsi_rows or []) if isinstance(r, dict)]
        rows = [r for r in rows
                if r.get("close") is not None and float(r["close"]) > 0]
        if len(rows) < 5:
            return {"bars": len(rows), "note": "insufficient_bars"}
        rows = sorted(rows, key=lambda r: str(r.get("date", "")))  # oldest -> newest
        closes = [float(r["close"]) for r in rows]
        vols = [float(r.get("volume") or 0) for r in rows]
        rsi_series = [r.get("rsi") for r in rows]
        rsi14 = None
        if any(isinstance(x, (int, float)) for x in rsi_series):
            # prefer the endpoint's own RSI on the latest bar
            last = rsi_series[-1]
            rsi14 = float(last) if isinstance(last, (int, float)) else None
        if rsi14 is None:
            rsi14 = _wilder_rsi(closes)
        rvol = None
        if vols[-1] > 0 and len(vols) >= 20:
            avg30 = sum(vols[-31:-1]) / max(1, min(30, len(vols) - 1))
            if avg30 > 0:
                rvol = round(vols[-1] / avg30, 2)
        px14 = round((closes[-1] / closes[-15] - 1) * 100, 1) if len(closes) >= 15 else None
        px5 = round((closes[-1] / closes[-6] - 1) * 100, 1) if len(closes) >= 6 else None
        return {"bars": len(rows), "rsi14": round(rsi14, 1) if rsi14 is not None else None,
                "rvol": rvol,
                "pct_14d": px14, "pct_5d": px5,
                "last_close": closes[-1], "as_of": str(rows[-1].get("date", ""))[:10]}
    except Exception as e:
        # a broken bar feed must not kill the phase: the LLM contract reads a
        # missing/invalid technical block as "not fetched", never as a wrong number.
        return {"error": str(e)[:80]}


def _weekend_tech_map():
    """Phase 2 (2026-11-07): weekend-NULL fix seam.
    weekend_tech.py (Market Data feed + rolling cache) writes
    ~/.hermes/cron/weekend_technicals.json {as_of, tickers:{T: block}}.
    Returned as {T: block} when fresh (same-day), else {} — a stale weekend
    file must never feed a Monday morning with Friday numbers labeled as
    today. Fail-soft: any read error -> {}."""
    import datetime as _dt
    p = os.path.join(CRON, "weekend_technicals.json")
    try:
        d = read_json(p, {})
        if not d.get("tickers"):
            return {}
        as_of = str(d.get("as_of", ""))[:10]
        today = _dt.date.today().isoformat()
        if as_of != today:
            return {}
        return d["tickers"]
    except Exception:
        return {}


def _insider_signals(stat_rows):
    """Quarterly insider buy/sell aggregation from insider-trade-statistics
    (newest quarter first). Pure. Returns compact signal block or None.
    2026-10-26 hardening (adversarial review R2): type-coerced sort keys
    (year/quarter may arrive as str), numeric coercion on all aggregates,
    fail-soft wrapper."""
    try:
        rows = [r for r in (stat_rows or []) if isinstance(r, dict) and r.get("year") is not None]
        if not rows:
            return None

        def _key(r):
            try:
                return (int(r.get("year", 0)), int(r.get("quarter", 0)))
            except (TypeError, ValueError):
                return (0, 0)

        rows.sort(key=_key, reverse=True)
        q0, q1 = rows[0], (rows[1] if len(rows) > 1 else {})

        def _num(x):
            return x if isinstance(x, (int, float)) else 0

        a0, d0 = q0.get("totalAcquired"), q0.get("totalDisposed")
        net = None
        if (isinstance(a0, (int, float)) or isinstance(d0, (int, float))) and (_num(a0) or _num(d0)):
            net = "buy" if _num(a0) > _num(d0) else ("sell" if _num(d0) > _num(a0) else "flat")
        return {"latest_quarter": f"{q0.get('year', '?')}Q{q0.get('quarter', '?')}",
                "acquired_shares": a0, "disposed_shares": d0,
                "purchases": _num(q0.get("totalPurchases")), "sales": _num(q0.get("totalSales")),
                "net_insider_flow": net,
                "vs_prior": {"purchases": _num(q1.get("totalPurchases")),
                             "sales": _num(q1.get("totalSales"))}}
    except Exception as e:
        return {"error": str(e)[:80]}


def _pre_inflection_coil(bif, pre_dt):
    """2026-11-03 directive (#3.3): PRE_INFLECTION_COIL flag for pre-print
    watchlist names. Gate (deterministic): Cash_Runway >= 8 quarters AND
    (Backlog_YoY >= +40% OR Deferred_Rev_QoQ >= +20%).
    2026-11-03 hard rule (#4): totalCurrentLiabilities is BANNED as a backlog
    proxy (AP + short-term debt spikes on liquidity distress too). Backlog
    YoY comes ONLY from the 8-K fast-pass extract (bif["_8k"]); without a
    fresh 8-K the backlog term cannot fire and only the deferred-rev leg
    can trigger the flag."""
    try:
        sig = bif.get("signals") or {}
        runway = (bif.get("track_b_engine") or {}).get("raw_runway_quarters")
        if runway is None or runway < 8:
            return False
        dr = sig.get("deferred_revenue_delta_pct_qoq") or 0.0
        backlog_yoy = (bif.get("_8k") or {}).get("backlog_yoy_pct")
        return dr >= 20.0 or (backlog_yoy is not None and backlog_yoy >= 40.0)
    except Exception:
        return False


def _fastpass_8k_map():
    """2026-11-03 directive: latest 8-K fast-pass extract per ticker from
    ~/.hermes/cron/fastpass_hits.jsonl (append-only, one row per filing).
    Returns {TICKER: extract-dict-or-None}. Only the NEWEST hit per ticker
    is used (a stale earnings 8-K must not re-inflate a later run)."""
    path = os.path.join(CRON, "fastpass_hits.jsonl")
    out = {}
    try:
        with open(path) as f:
            for line in f:
                try:
                    h = json.loads(line)
                except Exception:
                    continue
                t = str(h.get("ticker") or "").upper()
                if t:
                    out[t] = h.get("extract")  # newest wins (append order)
    except FileNotFoundError:
        pass
    return {t: e for t, e in out.items() if e}


def _short_interest_map():
    """2026-11-04 item 6: short-interest snapshot per ticker from
    ~/.hermes/cron/short_interest_snapshot.json (written by
    scripts/short_interest_batch.py, biweekly FINRA cycle data via
    stockanalysis.com). Returns {TICKER: short-data-dict}. Fail-soft: {} on
    any error — the short block is additive context, never a gate. Stale
    rows (source blip on a per-ticker fetch) are tagged stale=True by the
    writer; they still surface so the LLM can note 'stale carry' rather
    than fabricate."""
    path = os.path.join(CRON, "short_interest_snapshot.json")
    try:
        snap = json.load(open(path))
        return {t: r for t, r in (snap.get("data") or {}).items() if isinstance(r, dict)}
    except FileNotFoundError:
        return {}
    except Exception:
        return {}


def _earnings_preprint_window():
    """2026-11-04 directive (#6): next earnings date per universe ticker via
    FMP earnings-calendar. Returns {TICKER: {date, window, days}} for
    universe+watchlist prints within 14 days. T-3 (<=7d) = T3_PRE_PRINT
    (P4 hard cap 18/30); T-14 (<=14d) = T14_EARNINGS_LANED (staging only).
    FMP quirks (verified 2026-11-03/04): the endpoint IGNORES the symbol arg,
    and with no from_date/to_date it streams oldest-first — the range query
    is mandatory. Fail-soft: {} on any error — the pre-print P4 cap is a
    refinement, not a gate."""
    try:
        con = sqlite3.connect(os.path.join(CRON, "universe_cache.db"))
        uni = {t for (t,) in con.execute("SELECT ticker FROM universe") if t and len(t) <= 6}
        con.close()
        try:
            w = json.load(open(os.path.join(CRON, "watchlist_state.json")))
            uni |= {t.upper() for t in w.get("tickers", {})}
        except Exception:
            pass
        if not uni:
            return {}
        sid = fmp_session()
        if not sid:
            return {}
        # 2026-11-04 fix: the endpoint IGNORES the symbol arg AND, with no
        # date range, streams oldest-first (3x1000 rows only reached early
        # foreign prints — the US T-14 window was structurally unreachable).
        # from_date/to_date targets the window directly; page through the
        # range (cap 4 pages x 1000).
        from datetime import date as _date, timedelta as _td
        from_date = _date.today().isoformat()
        to_date = (_date.today() + _td(days=14)).isoformat()
        rows = []
        for page in range(1, 5):
            payload = json.dumps({"jsonrpc": "2.0", "method": "tools/call",
                                  "params": {"name": "calendar", "arguments": {
                                      "endpoint": "earnings-calendar",
                                      "from_date": from_date, "to_date": to_date,
                                      "limit": 1000, "page": page}},
                                  "id": 9100 + page}).encode()
            # Shared rail (2026-11-07): fmp_raw_send + dead-session recovery
            # so a session drop mid-pager re-inits instead of truncating.
            st, body, exc = fmp_raw_send(sid, payload)
            if exc is not None:
                if _is_session_failure(st, body, exc):
                    nsid = _fmp_recover(sid)
                    if not nsid:
                        break
                    st, body, exc = fmp_raw_send(nsid, payload)
                    sid = nsid
                if exc is not None:
                    break
            try:
                d = json.loads(body)
            except Exception:
                break
            content = d.get("result", {}).get("content", [])
            if not content:
                break
            try:
                chunk = json.loads(content[0]["text"])
            except Exception:
                break
            if not isinstance(chunk, list) or not chunk:
                break
            rows.extend(chunk)
            if len(chunk) < 50:
                break
        # T-14 / T-3 dual window (2026-11-04 directive #6): T-3 (<=7d) is the
        # PRE_PRINT P4-cap window; T-14 (<=14d) is the earlier EARNINGS_LANED
        # staging window (pre-print hurdle brief gets prepared, P4 NOT yet
        # capped). Universe bound: universe_cache.db + watchlist (the manual
        # list is retired).
        now = time.time()
        floor = now - 1 * 86400  # ignore already-released >1d-old prints
        out = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            t = str(row.get("symbol") or "").upper().split(".")[0]
            dt = row.get("date") or ""
            if t in uni and dt:
                try:
                    ts = datetime.fromisoformat(dt).replace(tzinfo=None).timestamp()
                except Exception:
                    continue
                if not (floor <= ts <= now + 14 * 86400):
                    continue
                window = "T3_PRE_PRINT" if ts <= now + 7 * 86400 else "T14_EARNINGS_LANED"
                prev = out.get(t)
                if prev is None or (dt[:10] < prev["date"]):
                    out[t] = {"date": dt[:10], "window": window,
                              "days": round((ts - now) / 86400.0, 1)}
        return out
    except Exception as e:
        print(f"[engine:deepdive] earnings calendar unavailable ({e}) — pre-print window skipped",
              file=sys.stderr)
        return {}


def phase_deepdive():
    vetted = read_json(os.path.join(CRON, "vetted_tickers.json"), {})
    watch = read_json(os.path.join(CRON, "watchlist_state.json"), {"tickers": {}})
    # catalyst context uses the engine-owned history union (briefs the LLM
    # trimmed out of signal_briefs.json still count as recent signal)
    _, briefs = load_brief_history(30)

    tickers = sorted(set(
        [t.get("ticker") for t in vetted.get("all_tickers", []) if t.get("ticker")]
        + [t for t, v in watch.get("tickers", {}).items() if v.get("promote_to_deepdive")]
    ))
    invalid_prev = read_json(os.path.join(CRON, "ticker_scores.json"), {})
    invalid_raw = invalid_prev.get("invalid_tickers_flagged", []) if isinstance(invalid_prev, dict) else []
    invalid = set()
    for x in invalid_raw:
        if isinstance(x, str):
            invalid.add(x)
        elif isinstance(x, dict) and x.get("ticker"):
            invalid.add(x["ticker"])
    tickers = [t for t in tickers if t not in invalid]

    # Phase 0 deep-track set: measured technicals/insider blocks only for
    # ACTIVE or promoted tickers (the FULL-depth tier per the Deep-Dive job
    # spec). Everyone else gets the 9-call base fetch, unchanged.
    # 2026-10-26 (adversarial review F6): CAP the set. RSI returns FULL price
    # history (the endpoint ignores from_date), so an unbounded ACTIVE list
    # grows the cycle cost linearly with zero marginal value past ~20 names
    # the LLM deep-scores anyway. Promoted tickers are NEVER capped (they are
    # the only path back into scoring). Rank the cap by conviction (watchlist
    # best_track_score, fallback 0) — NOT alphabetically.
    _wtk = watch.get("tickers", {})
    _promoted = {t for t in tickers if _wtk.get(t, {}).get("promote_to_deepdive")}
    _active_ranked = sorted(
        (t for t in tickers if _wtk.get(t, {}).get("status") == "ACTIVE"),
        key=lambda t: (float(_wtk.get(t, {}).get("best_track_score")
                             or _wtk.get(t, {}).get("conviction") or 0), t),
        reverse=True)
    deep_set = _promoted | set(_active_ranked[:20])

    # ---- LLM-load control: exclude already-decided tickers (config-gated) ----
    # The watchlist pruner already demoted ARCHIVED tickers (mostly
    # exited_100b_mcap_ceiling) and their wiki files were moved to archived/.
    # Re-scoring them every 2h with full 3-track depth burns ~38% of the
    # Deep-Dive LLM completion tokens on names the pruner already decided on.
    # Config-gated + visible: excluded tickers are reported in the summary,
    # never silently dropped. A ticker with promote_to_deepdive=true is
    # NEVER excluded here (that flag is the only path back into scoring).
    watch_tk = watch.get("tickers", {})
    excluded = []
    if DD_CFG.get("include_archived", False):
        pass  # full universe, no exclusion (legacy behavior)
    else:
        for t in tickers:
            st = watch_tk.get(t, {}).get("status")
            promoted = bool(watch_tk.get(t, {}).get("promote_to_deepdive"))
            if st == "ARCHIVED" and not promoted:
                excluded.append(t)
        if excluded:
            tickers = [t for t in tickers if t not in excluded]

    cutoff = time.time() - TH["recheck_days"] * 86400
    # 2026-11-03 directive: 8-K fast-pass extracts + pre-print earnings window
    fastpass_8k = _fastpass_8k_map()
    preprint_window = _earnings_preprint_window()
    short_interest = _short_interest_map()  # 2026-11-04 item 6: biweekly FINRA short data
    weekend_tech = _weekend_tech_map()  # Phase 2: weekend-NULL fix (Market Data fallback)
    if fastpass_8k:
        print(f"[engine:deepdive] 8-K fast-pass extracts injected: {', '.join(sorted(fastpass_8k))}",
              file=sys.stderr)
    if preprint_window:
        print(f"[engine:deepdive] pre-print earnings window (<=7d): {', '.join(sorted(preprint_window))}",
              file=sys.stderr)
    briefs_by_ticker = {}
    for b in briefs:
        try:
            created = datetime.fromisoformat(b.get("date_created", "")).replace(tzinfo=None).timestamp()
        except Exception:
            created = time.time()
        if created < cutoff:
            continue
        for k in ("tickers_affected", "tickers_benefit"):
            for t in b.get(k, []):
                briefs_by_ticker.setdefault(t, []).append({
                    "headline": b.get("headline", "")[:200],
                    "direction": b.get("impact_direction"),
                    "materiality": b.get("materiality"),
                    "confidence": b.get("confidence"),
                    "category": b.get("category"),
                })

    sid = fmp_session()
    print(f"[engine:deepdive] FMP session: {str(sid)[:12]}… fetching {len(tickers)} tickers (parallel)", file=sys.stderr)
    briefing = {}
    errors = 0

    def _fetch_one(idx, t):
        """One ticker's 9 FMP calls (thread-safe: single sid shared, distinct
        call_ids, fmp_call/financial_scoring/bifurcation_scoring are pure).
        Returns (ticker, briefing_row, is_error). Mirrors the old sequential
        per-ticker logic exactly so briefing data is identical, only faster.
        call_ids use the ticker's stable index (idx) so they are unique,
        deterministic, and identical to the old sequential 100+i / 200+i ...
        """
        base = idx
        prof = fmp_call(sid, "company", {"endpoint": "profile-symbol", "symbol": t}, 100 + base)
        quote = fmp_call(sid, "quote", {"endpoint": "quote", "symbol": t}, 200 + base)
        kmetrics = fmp_call(sid, "statements", {"endpoint": "key-metrics", "symbol": t, "period": "quarter", "limit": 4}, 300 + base)
        income = fmp_call(sid, "statements", {"endpoint": "income-statement", "symbol": t, "period": "annual", "limit": 2}, 400 + base)
        incg = fmp_call(sid, "statements", {"endpoint": "income-statement-growth", "symbol": t, "period": "annual", "limit": 3}, 500 + base)
        balance = fmp_call(sid, "statements", {"endpoint": "balance-sheet-statement", "symbol": t, "period": "annual", "limit": 2}, 600 + base)
        bal_q = fmp_call(sid, "statements", {"endpoint": "balance-sheet-statement", "symbol": t, "period": "quarter", "limit": 4}, 650 + base)
        inc_q = fmp_call(sid, "statements", {"endpoint": "income-statement", "symbol": t, "period": "quarter", "limit": 5}, 700 + base)
        cf_q = fmp_call(sid, "statements", {"endpoint": "cashflow-statement", "symbol": t, "period": "quarter", "limit": 4}, 750 + base)
        if isinstance(prof, dict) and prof.get("_error"):
            return t, {"status": "FETCH_ERROR", "error": prof.get("_error", "")[:120]}, True
        if not isinstance(prof, list) or not prof or prof[0].get("symbol") is None:
            return t, {"status": "INVALID_TICKER", "detail": "not_found on FMP profile"}, True
        p = prof[0]
        kdata = kmetrics if isinstance(kmetrics, list) and kmetrics else []
        kdata = kdata[0] if kdata else {}
        q = quote[0] if isinstance(quote, list) and quote else {}
        incdata = income[0] if isinstance(income, list) and income else {}
        incgdata = incg[0] if isinstance(incg, list) and incg else {}
        baldata = balance[0] if isinstance(balance, list) and balance else {}
        # ENTITY INTEGRITY (2026-10-26): profile-symbol is camelCase
        # (companyName/marketCap) — the old snake_case reads returned None for
        # name on EVERY ticker. The deep statement endpoints carry `cik`;
        # any CIK that disagrees with the profile's CIK means a reused/legacy
        # ticker resolved to a DIFFERENT entity — quarantine the whole row.
        # CIK normalization (review F3): coerce to int FIRST so a float CIK
        # (str(123.0).zfill -> "123.0") can never false-quarantine a healthy row.
        def _cik10(v):
            try:
                return str(int(float(v))).zfill(10)
            except (TypeError, ValueError):
                return ""
        prof_cik = _cik10(p.get("cik"))
        mismatches = []
        for label, ds in (("income", incdata), ("balance", baldata)):
            d_cik = _cik10(ds.get("cik"))
            # CIK = 0 means the statement row simply lacks the field — NOT a
            # mismatch (PALX false positive 2026-10-26). Only a NONZERO
            # disagreeing CIK proves a different legal entity.
            if d_cik and d_cik != "0000000000" and prof_cik and d_cik != prof_cik:
                mismatches.append(f"{label}_cik:{d_cik}!=profile:{prof_cik}")
        if mismatches:
            return t, {"status": "ENTITY_MISMATCH", "detail": "; ".join(mismatches)[:150],
                       "profile_cik": prof_cik or None,
                       "profile_name": p.get("companyName") or p.get("name")}, True
        fin = financial_scoring(kdata, incdata, incgdata, baldata)
        mcap = p.get("marketCap") or p.get("market_cap") or q.get("marketCap") or kdata.get("marketCap")
        # 2026-11-03 directive: inject the latest 8-K fast-pass extract (if any)
        # so the PSFV carve-out backlog term and P1/P4 gates can use REAL
        # backlog/book-to-bill/guide numbers instead of brief prose.
        hit = fastpass_8k.get(t)
        bif = bifurcation_scoring(baldata, bal_q, inc_q, cf_q, mcap, kdata, bif_8k=hit)
        # PRE_PRINT flag (directive #2): ticker's next earnings date within
        # 7 days -> P4 capped at 18/30 in the LLM rubric (no 90s on unreported
        # guidance). A post-print 8-K hit in fastpass_8k clears the flag
        # automatically (actuals are in now).
        pre_win = preprint_window.get(t)
        row = {
            "status": "OK",
            "name": p.get("companyName") or p.get("name"),
            "cik": prof_cik or None,
            "sector": p.get("sector"),
            "industry": p.get("industry"),
            "market_cap": mcap if isinstance(mcap, (int, float)) else None,
            "price": q.get("price"),
            "quote_name": q.get("name"),  # live-quote entity label for LLM cross-check
            "entity_check": "ok",
            "financial_100": fin,
            "bifurcation": bif,
            "catalyst_context": briefs_by_ticker.get(t, []),
            "watchlist": watch.get("tickers", {}).get(t, {}),
        }
        # PRE_PRINT flag (2026-11-04 final): T-3 window (<=7d to earnings)
        # -> P4 hard-capped at 18/30 in the LLM rubric (no Inflection
        # Trifecta band on unreported guidance). T-14 window -> the
        # PRE_PRINT_HURDLE_BRIEF is staged but P4 is NOT capped. A fresh
        # post-print 8-K hit in fastpass_8k clears the flag automatically
        # (actuals are in now).
        if pre_win and pre_win.get("window") == "T3_PRE_PRINT":
            row["pre_print"] = pre_win
            row["pre_inflection_coil"] = _pre_inflection_coil(bif, pre_win.get("date"))
        elif pre_win:
            row["earnings_laned"] = pre_win
        # 2026-11-04 item 6: short-interest block (biweekly FINRA cycle, via
        # stockanalysis.com — source of record FINRA api.finra.org is
        # decommissioned). Pure additive context: Track A P3 short-squeeze
        # modifier evidence (short % of float >= 20 + days-to-cover >= 2 or
        # cycle delta >= +10%) and Track C fuel (squeezable puts). Absent =
        # not fetched (never a zero, never a negative).
        if t in short_interest:
            row["short_interest"] = short_interest[t]
        # Phase 0 (2026-10-26): measured technicals + insider aggregation for
        # DEEP-TRACK tickers only (ACTIVE / promoted). Pure additive blocks —
        # `technical` / `insider` are None for the rest, so the LLM treats
        # their absence as "not fetched" (never as zero).
        if t in deep_set:
            rsi_rows = fmp_call(sid, "technicalIndicators",
                                {"endpoint": "relative-strength-index", "symbol": t,
                                 "periodLength": 14, "timeframe": "1day"}, 800 + base)
            ins_rows = fmp_call(sid, "insiderTrades",
                                {"endpoint": "insider-trade-statistics", "symbol": t}, 850 + base)
            row["technical"] = _technical_signals(rsi_rows)
            row["insider"] = _insider_signals(ins_rows)
            # Phase 2 (2026-11-07): weekend-NULL fix. FMP's technical feed
            # returns thin/zero rows outside market hours and the block comes
            # back error/insufficient_bars -> technical_100 90% NULL on
            # weekends. When the FMP block has no rsi14, fall back to the
            # Market Data rolling cache (weekend_tech.py, same-day freshness
            # enforced in _weekend_tech_map) so the LLM rubric still gets
            # measured RSI/RVOL/pct blocks. The marketdata block carries
            # source='marketdata' + note='weekend_fill' so the provenance is
            # visible in the briefing. If neither has rsi14, keep the FMP
            # error marker (the contract reads it as 'not fetched').
            tb = row["technical"]
            if not (isinstance(tb, dict) and tb.get("rsi14") is not None):
                wb = weekend_tech.get(t)
                if isinstance(wb, dict) and wb.get("rsi14") is not None:
                    wb = dict(wb)
                    wb.setdefault("note", "weekend_fill")
                    row["technical"] = wb
        return t, row, False

    # Parallel fetch: one worker per ticker, 5 concurrent (matches the
    # sources fan-out worker count the parallel session established). FMP
    # tolerates this; if 429s spike the per-call _error is captured and the
    # ticker shows as FETCH_ERROR exactly as the sequential path did.
    workers = min(5, max(1, len(tickers)))
    done_n = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_fetch_one, i, t): t for i, t in enumerate(tickers)}
        for fut in concurrent.futures.as_completed(futs):
            t_orig = futs[fut]
            try:
                t, row, is_err = fut.result()
            except Exception as e:
                # Worker-exception isolation (review F1, 2026-10-26): before
                # this, ANY uncaught exception in _fetch_one (a ZeroDivision on
                # a 0-close RSI bar, a type surprise in an FMP payload) raised
                # through fut.result() and KILLED THE WHOLE PHASE — no briefing,
                # forcing the LLM into its manual-curl fallback. Now the fault
                # isolates to ONE ticker as FETCH_ERROR, exactly like a
                # per-call _error did in the pre-Phase-0 design.
                t, row, is_err = t_orig, {"status": "FETCH_ERROR",
                                          "error": f"worker_exception: {str(e)[:100]}"}, True
            briefing[t] = row
            if is_err:
                errors += 1
            done_n += 1
            if done_n % 20 == 0 or done_n == len(tickers):
                print(f"[engine:deepdive] {done_n}/{len(tickers)} fetched, errors={errors}", file=sys.stderr)
    # deterministic briefing order = original ticker order (dict, insertion)
    briefing = {t: briefing[t] for t in tickers if t in briefing}

    out = {
        "generated": now_iso(),
        "session_id": str(sid)[:16],
        "tickers_fetched": len(tickers),
        "fetch_errors": errors,
        "instructions": (
            "You are Agent 9 (Deep-Dive). The FMP fetch + financial scoring is DONE below. "
            "YOUR tasks only: (1) assign catalyst_100 (0-100) per ticker from catalyst_context "
            "(empty context = 0; strong recent catalysts with high confidence/direct materiality = 70-100; "
            "moderate = 40-69; weak/negative = 0-39) and, where conviction justifies it, "
            "valuation_100 and technical_100 (use price + market_cap + briefs); "
            "(2) write one-line watchlist note per ticker; "
            "(3) compute best_track_score = max(track_a_score, track_b_score) — the single ranking key "
            "(0-100 scale; the 400-point canonical sum was removed 2026-10-18, do not emit it); "
            "(4) update /home/jaren/.hermes/cron/watchlist_state.json per STEP 7 rules "
            "(ACTIVE bar: best_track_score>=75 AND >=2 briefs; promotion flags cleared for tickers you fully scored); "
            "(5) write ticker_scores.json (rankings sorted by best_track_score, include pillar_scores with null for unassessed pillars "
            "(raw reference signals only — never summed or ranked on), "
            "invalid_tickers_flagged = all FETCH_ERROR/INVALID_TICKER/ENTITY_MISMATCH tickers); "
            "(6) MEASURED TECHNICALS/INSIDER (Phase 0, 2026-10-26): some briefing rows carry "
            "engine-computed 'technical' {rsi14, rvol, pct_14d, pct_5d, as_of} and 'insider' "
            "{net_insider_flow, purchases, sales, latest_quarter} blocks (ACTIVE/promoted tickers only). "
            "When present, technical_100 MUST be grounded in these measured values (RSI <30 = oversold "
            "favorable for Track A/B long theses; RSI >70 = stretched; rvol >= 3 = volume squeeze; "
            "use pct_14d/pct_5d for momentum) — the LLM judgment is the tiebreaker, not the primary source. "
            "Insider block: net_insider_flow 'buy' with rising purchases corroborates conviction (Track B P1 "
            "add-up context); 'sell' heavy = discount the thesis, never fabricate a buy signal from it. "
            "When 'technical'/'insider' are ABSENT (other tiers or null), that is 'not fetched' — "
            "score from price/market_cap as before; NEVER treat absence as zero or negative. "
            "(7) BIFURCATED SCORING: each 'bifurcation' block in this briefing carries engine-computed "
            "track_a_engine.financial_fortress_40 (from raw_altman_z), track_b_engine.runway_gate_20 "
            "(from raw_runway_quarters, 99 = OCF already positive), track_c_engine.track_c_100, and "
            "signals (deferred_revenue_delta_pct_qoq, gross_margin_qoq_bps, share_count_yoy_pct, "
            "dilution_kill_8pct). Use those numbers as FIXED points for those pillars. YOU assign the "
            "remaining qualitative pillar points (0-100 scale each track, see job prompt) from "
            "catalyst_context and write track_a_score, track_b_score, track_c_score (0-100 ints) plus "
            "track_a_pillars / track_b_pillars / track_c_reasons (short evidence strings) into every "
            "ranking in ticker_scores.json. Keep a ticker if track_a_score >= 75 OR track_b_score >= 75 "
            "(write BOTH when both pass); track_c_score <= 35 AND not passing A/B marks it as mirage. "
            "Do NOT recompute the engine sub-scores. "
            "Do NOT re-fetch FMP. Do NOT recompute financial sub-scores."
        ),
        "tickers": briefing,
    }
    write_json("/tmp/dd_briefing.json", out)
    summary = {
        "tickers_ok": sum(1 for v in briefing.values() if v.get("status") == "OK"),
        "tickers_invalid": sum(1 for v in briefing.values() if v.get("status") == "INVALID_TICKER"),
        "entity_mismatches": sum(1 for v in briefing.values() if v.get("status") == "ENTITY_MISMATCH"),
        "fetch_errors": errors,
        "deep_tracked": sorted(deep_set),
        "deep_tracked_count": len(deep_set),
        "briefing_file": "/tmp/dd_briefing.json",
        "archived_excluded": excluded,
        "archived_excluded_count": len(excluded),
        "top_financial": sorted(
            [(t, v["financial_100"]["financial_100"]) for t, v in briefing.items() if v.get("status") == "OK"],
            key=lambda x: -x[1])[:10],
        "tickers_with_catalyst_context": sum(1 for v in briefing.values() if v.get("catalyst_context")),
    }
    print(json.dumps(summary, indent=1))
    print(f"[engine:deepdive] done: {summary['tickers_ok']} OK / {summary['tickers_invalid']} invalid / "
          f"{summary['entity_mismatches']} entity-mismatch / "
          f"{errors} errors / {len(excluded)} archived-excluded -> /tmp/dd_briefing.json")
    if excluded:
        print(f"[engine:deepdive] archived-excluded (not scored this cycle): {', '.join(sorted(excluded))}")
    return 0


# ---------------------------------------------------------------- sources
def entity_gate():
    """Pre-warmed $500M-$300B US corridor gate (entity_universe_seeder.py,
    weekly). Returns (tickers_map, name_index, gate_ts). The tickers map is
    the authoritative match set for ticker hints: it closes the loop where
    a first-mover item about a mid-cap was discarded because the wiki only
    knew its own existing entries. Missing file -> empty (fail-open; the
    FMP gate at apply time is still the band authority)."""
    d = read_json(os.path.join(CRON, "entity_universe_gate.json"), {})
    if not isinstance(d, dict):
        d = {}
    ts = d.get("ts") or ""
    return (d.get("tickers") or {}), (d.get("name_index") or {}), ts


def ticker_universe():
    """Symbol set for ticker hints: wiki stems (length>=2, non-blocklisted)
    + watchlist + pre-warmed entity gate (chunk 1 of the Entity
    Acquisition Upgrade). Name aliases: dd_briefing cache + gate names."""
    u = set()
    names = {}
    blocked = {w.upper() for w in S_CFG.get("common_word_blocklist", [])}
    d = os.path.join(WIKI, "companies")  # clean-slate layout 2026-11-05 (was wiki/wiki/companies)

    def _symbol_ok(stem):
        # wiki dir also holds non-ticker containers (e.g. legacy_20261013/);
        # only 1-6 char US-style stems are ticker symbols
        return bool(re.fullmatch(r"[A-Z0-9.]{1,6}", stem))

    try:
        for f in os.listdir(d):
            if f.endswith(".md"):
                s = f[:-3].upper()
                if len(s) >= 2 and s not in blocked and _symbol_ok(s):
                    u.add(s)
            elif os.path.isdir(os.path.join(d, f)) and _symbol_ok(f.upper()):
                u.add(f.upper())
    except OSError:
        pass
    watch = read_json(os.path.join(CRON, "watchlist_state.json"), {})
    u |= {t.upper() for t in watch.get("tickers", {}) if len(t) >= 2}
    # company-name aliases from any cached deepdive briefing
    dd = read_json("/tmp/dd_briefing.json", {})
    if isinstance(dd, dict):
        for t, v in dd.get("tickers", {}).items():
            if isinstance(v, dict) and v.get("name"):
                names[t.upper()] = v["name"]
    # pre-warmed corridor (chunk 1): every $500M-$300B US name the seeder
    # harvested. This is what makes first-mover mid-caps matchable.
    gate_tickers, _n, _ts = entity_gate()
    for sym, v in gate_tickers.items():
        if not isinstance(v, dict):
            continue
        su = str(sym).upper()
        if len(su) >= 2 and su not in blocked and _symbol_ok(su):
            u.add(su)
        if v.get("name"):
            names.setdefault(su, v["name"])
    return u, names


def suggest_tickers(headline, universe, names, aliases, etf, common_block,
                    lead=None):
    """Hint tickers from a title (plus optional lead body text, chunk 3
    step 1: extract from headline + first words of the body, not just the
    headline). Case-sensitive: only capitalized, non-sentence-initial
    tokens are matched against the universe (kills 'The/And/In'-style false
    positives). Alias + company-name matching stays case-insensitive.

    Entity-resolution pass (chunk 3): multi-word company names from the
    pre-warmed name index are matched with word boundaries against the
    normalized lead text — commercial-name mentions ('Oklo announced',
    'Aehr Test Systems won') resolve to symbols even when the headline
    only carries the operating name. Only index names with >=2 tokens are
    matched this way: single-token index entries collide with ordinary
    English words (the ATMS/GFS-class false positives the upgrade exists
    to kill)."""
    raw_tokens = re.findall(r"[A-Za-z0-9.]+", headline or "")
    low_tokens = set(t.lower() for t in raw_tokens)
    found = set()
    etf_block = {t.upper() for t in etf}
    for pos, tok in enumerate(raw_tokens):
        if pos == 0:
            continue  # sentence-initial capital is not evidence
        if not tok or not tok[0].isupper():
            continue
        tok_u = tok.upper()
        if tok_u in etf_block or tok_u in common_block:
            continue
        if tok_u in universe:
            found.add(tok_u)
    blocked_w = {w.lower() for w in common_block}
    for alias, tick in aliases.items():
        if alias.lower() in blocked_w:
            continue
        if alias.lower() in low_tokens:
            found.add(tick.upper())
    # company-name aliases (exact single-word match), single-word names length >= 4
    for tick, name in names.items():
        for word in re.findall(r"[a-z]{4,}", (name or "").lower()):
            if word in blocked_w:
                continue
            if word in low_tokens:
                found.add(tick)
                break
    # --- entity-resolution pass (chunk 3): multi-word names, word-boundary
    # Cost control: the name index has ~2k multi-word entries; a plain
    # regex-per-name scan per candidate is ~0.5-1s of pure CPU. Two gates:
    #  (1) first-token prefilter (the name's anchor token must appear in the
    #      lead text at all — substring test, sub-microsecond),
    #  (2) cached compiled patterns (compiled once per index entry).
    try:
        _gt, name_index, _ts = entity_gate()
    except Exception:
        name_index = {}
    if name_index:
        lead_low = re.sub(r"[^a-z0-9 ]+", " ", (headline or "") + " " + (lead or "")).lower()
        lead_low = " ".join(lead_low.split())
        if lead_low:
            # module-level cache keyed on the index generation (gate ts)
            cache_key = _ts + str(len(name_index))
            global _NAME_PAT_CACHE
            if _NAME_PAT_CACHE[0] != cache_key:
                pats = {}
                for nm, sym in name_index.items():
                    if " " not in nm:
                        continue
                    pat = r"(?<![a-z0-9])" + re.escape(nm).replace(r"\ ", r"[\s\-]+") + r"(?![a-z0-9])"
                    try:
                        pats[nm] = (re.compile(pat), nm.split()[0], sym)
                    except re.error:
                        continue
                _NAME_PAT_CACHE = (cache_key, pats)
            for nm, (pat, anchor, sym) in _NAME_PAT_CACHE[1].items():
                if anchor not in lead_low:
                    continue
                if pat.search(lead_low):
                    su = str(sym).upper()
                    if su not in etf_block and su not in common_block:
                        found.add(su)
    return sorted(found)


def searxng_search(query, time_range, max_results=10):
    url = (CONFIG["searxng_url"] + "/search?q=" + urllib.parse.quote(query)
           + "&format=json&time_range=" + time_range)
    st, body = http_get(url, timeout=25)
    if st != 200:
        raise RuntimeError(f"searxng http {st}: {body[:120]}")
    d = json.loads(body)
    out = []
    for r in d.get("results", [])[:max_results]:
        out.append({"title": r.get("title", ""), "url": r.get("url", ""),
                    "content": r.get("content", ""), "publishedDate": r.get("publishedDate"),
                    "engine": r.get("engine", "")})
    return out


def gnews_search(query, time_range, max_results=5):
    """Google News RSS fallback (only when SearXNG is down)."""
    import xml.etree.ElementTree as ET  # parses only Google's own RSS over HTTPS; trusted source
    from email.utils import parsedate_to_datetime
    when = "7d" if time_range == "week" else "1d"
    url = "https://news.google.com/rss/search?q=" + urllib.parse.quote(query) + "&when=" + when
    _gnews_throttle(0.6)  # rate-limit: max ~1.7 req/s across parallel workers
    st, body = http_get(url, timeout=20)
    if st != 200:
        raise RuntimeError(f"gnews http {st}: {body[:120]}")
    root = ET.fromstring(body)
    out = []
    for item in root.iter("item"):
        pub = item.findtext("pubDate") or ""
        try:
            pub = parsedate_to_datetime(pub).isoformat()
        except Exception:
            pass
        out.append({"title": (item.findtext("title") or "").strip(),
                    "url": (item.findtext("link") or "").strip(),
                    "content": re.sub(r"<[^>]+>", "", (item.findtext("description") or ""))[:300],
                    "publishedDate": pub, "engine": "gnews_rss"})
        if len(out) >= max_results:
            break
    return out


class _TextExtract(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []
        self.skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "noscript", "svg", "header", "footer", "nav", "aside", "form"):
            self.skip += 1

    def handle_endtag(self, tag):
        if tag in ("script", "style", "noscript", "svg", "header", "footer", "nav", "aside", "form") and self.skip > 0:
            self.skip -= 1

    def handle_data(self, d):
        if not self.skip and d.strip():
            self.parts.append(d)


_GNEWS_LOCK = threading.Lock()
_GNEWS_LAST = [0.0]


def _gnews_throttle(interval=0.6):
    """Serialize Google News RSS requests: space them >= interval apart."""
    with _GNEWS_LOCK:
        wait = _GNEWS_LAST[0] + interval - time.time()
        if wait > 0:
            time.sleep(wait)
        _GNEWS_LAST[0] = time.time()


_BROWSER_LOCK = threading.Lock()
_BROWSER_OK = [None]  # None = unknown, True/False = probed


def _browser_fallback(url, max_chars, budget=32):
    """Render a JS-heavy/bot-walled page with the agent-browser CLI.

    Uses one dedicated browser session for all workers (single-Chromium,
    serialized under _BROWSER_LOCK):
      agent-browser open <url> --session pipengine   (launch + navigate)
      agent-browser eval 'document.body.innerText' --session pipengine
      agent-browser close --session pipengine        (best effort)
    Each step is bounded (25s/15s/5s subprocess timeout) so the whole
    browser path plus the 8s urllib attempt stays well under ~40s worst
    case. Any exception/timeout returns None and the caller keeps the
    existing snippet fallback. CLI absent -> probe once, then skip.
    """
    if _BROWSER_OK[0] is False:
        return None
    if _BROWSER_OK[0] is None:
        _BROWSER_OK[0] = shutil.which("agent-browser") is not None
        if not _BROWSER_OK[0]:
            return None
    with _BROWSER_LOCK:
        sess = "pipengine"
        try:
            subprocess.run(["agent-browser", "open", url, "--session", sess],
                           capture_output=True, text=True, timeout=25)
            r = subprocess.run(["agent-browser", "eval", "document.body.innerText",
                                "--session", sess],
                               capture_output=True, text=True, timeout=15)
        finally:
            try:
                subprocess.run(["agent-browser", "close", "--session", sess],
                               capture_output=True, text=True, timeout=5)
            except Exception:
                pass
    if r.returncode != 0 or not r.stdout:
        return None
    try:
        raw = json.loads(r.stdout)  # eval prints a JSON-encoded string
    except Exception:
        raw = r.stdout
    text = re.sub(r"\s+", " ", raw if isinstance(raw, str) else "").strip()
    if len(text) < 200:
        return None
    return text[:max_chars]


def fetch_article_text(url, max_chars):
    st, body = http_get(url, timeout=8)
    if st == 200:
        p = _TextExtract()
        try:
            p.feed(body)
        except Exception:
            p = None
        text = re.sub(r"\s+", " ", " ".join(p.parts)).strip() if p else ""
        if len(text) >= 200:
            return text[:max_chars]
    # urllib failed or page text < 200 chars (JS-rendered / bot-walled):
    # try the agent-browser CLI render fallback; never raises.
    try:
        return _browser_fallback(url, max_chars)
    except Exception:
        return None


def phase_sources():
    feed = read_json(os.path.join(CRON, "feed_raw.json"), [])
    if not isinstance(feed, list):
        feed = []
    feed_headlines = {norm_headline(i.get("headline", "")) for i in feed if isinstance(i, dict)}
    feed_urls = {i.get("url") for i in feed if isinstance(i, dict)}
    junk = [d.lower() for d in S_CFG.get("junk_domains", [])]
    etf = [t.upper() for t in S_CFG.get("etf_blocklist", [])]
    common_block = {w.upper() for w in S_CFG.get("common_word_blocklist", [])}
    aliases = S_CFG.get("ticker_aliases", {})
    universe, names = ticker_universe()
    queries = S_CFG["queries"]

    searx_ok = http_ok(CONFIG["searxng_url"] + "/healthz")
    fallback = not searx_ok
    if fallback:
        print("[engine:sources] SearXNG DOWN -> Google News RSS fallback", file=sys.stderr)

    # ---- parallel query fan-out ----------------------------------------
    # 4 workers: SearXNG is a local docker container, so no inter-query sleep
    # is needed there; the gnews RSS fallback self-throttles via
    # _gnews_throttle. Circuit breaker: >=3 query errors flip the
    # not-yet-dispatched queries to RSS (error count is race-safe under
    # err_lock; the original "consecutive" semantics become "cumulative"
    # because failures are no longer strictly ordered).
    workers = min(4, max(1, len(queries)))
    gnews_flag = [fallback]  # shared: True when SearXNG down or >=3 errors
    err_lock = threading.Lock()
    err_count = [0]

    def _run_query(q):
        tag, tr = q["tag"], q["time_range"]
        t0 = time.time()
        results, err = [], None
        try:
            if gnews_flag[0]:
                results = gnews_search(q["query"], tr)
            else:
                results = searxng_search(q["query"], tr, S_CFG["max_results_per_query"])
        except Exception as e:
            err = e
            with err_lock:
                err_count[0] += 1
                if err_count[0] >= 3 and not gnews_flag[0]:
                    gnews_flag[0] = True
                    print("[engine:sources] 3+ query errors -> RSS fallback", file=sys.stderr)
        now = time.time()
        fresh = 0
        new_urls = 0
        for r in results:
            pd = ts_parse(r.get("publishedDate") or "")
            if pd and now - pd < 7 * 86400:
                fresh += 1
            if r.get("url") not in feed_urls:
                new_urls += 1
        stat = {"query_tag": tag, "query": q["query"], "time_range": tr,
                "results_total": len(results), "results_fresh": fresh,
                "results_new": new_urls,
                "engines": sorted({r.get("engine", "") for r in results if r.get("engine")}),
                "elapsed_s": round(time.time() - t0, 1)}
        if err is not None:
            stat["results_total"] = 0
            stat["results_fresh"] = 0
            stat["results_new"] = 0
            stat["engines"] = []
            stat["error"] = str(err)[:150]
        return results, stat

    cands_by_agent = {}
    stats = []
    seen_urls = set()
    raw_total = junk_dropped = dup_dropped = 0
    got = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_run_query, q): i for i, q in enumerate(queries)}
        for fut in concurrent.futures.as_completed(futs):
            got[futs[fut]] = fut.result()

    # Candidate assembly stays sequential on the main thread in query order
    # so first-seen dedup (seen_urls) is deterministic.
    for i, q in enumerate(queries):
        results, stat = got[i]
        stats.append(stat)
        if "error" in stat:
            continue
        raw_total += len(results)
        tag = q["tag"]
        for r in results:
            title = (r.get("title") or "").strip()
            url = (r.get("url") or "").strip()
            if not url or not title:
                continue
            host = urllib.parse.urlparse(url).netloc.lower()
            if any(host == d or host.endswith("." + d) for d in junk):
                junk_dropped += 1
                continue
            if norm_headline(title) in feed_headlines or url in feed_urls or url in seen_urls:
                dup_dropped += 1
                continue
            seen_urls.add(url)
            cands_by_agent.setdefault(q["agent"], []).append({
                "agent": q["agent"], "tag": tag, "query": q["query"],
                "source_name": q["source_name"], "source_type": q["source_type"],
                "title": title[:300], "url": url,
                "publishedDate": r.get("publishedDate"),
                "content": (r.get("content") or "")[:S_CFG["content_chars"]],
                "engine": r.get("engine", ""),
                "suggested_category": q.get("category", ""),
                "suggested_tickers": suggest_tickers(title, universe, names, aliases, etf, common_block,
                                                    lead=(r.get("content") or "")[:400]),
            })

    fallback = fallback or gnews_flag[0]

    # rank + cap per agent: fresh (newest first), then content length
    per_agent = S_CFG["candidates_per_agent"]
    candidates = []
    for agent in sorted(cands_by_agent):
        items = cands_by_agent[agent]

        def rank(x):
            pd = ts_parse(x.get("publishedDate") or "")
            return (-(pd or 0), -len(x.get("content") or ""))
        items.sort(key=rank)
        candidates.extend(items[:per_agent])

    write_json("/tmp/sources_candidates.json", {
        "generated": now_iso(), "fallback": fallback,
        "instructions": ("Quality bar: keep ONLY items naming a specific company/contract/grant/event with a "
                         "concrete fact ($, %, named party); drop trend/landscape/thinkpiece/roundups. "
                         "Fix suggested_tickers (real US-listed tickers only; drop ETFs/foreign ADRs/fakes; "
                         "add tickers the headline clearly names) — if none remain, drop the item. "
                         "Write /tmp/sources_keep.json = {\"items\":[{url, headline, source_agent (agent key), "
                         "category, tickers_mentioned, tickers_benefit, tickers_affected}]} "
                         "(negative_intelligence items need both affected and benefit), then run "
                         "'python3 pipeline_engine.py sources apply'."),
        "candidates": candidates,
        "totals": {"raw_results": raw_total, "junk_dropped": junk_dropped,
                   "dup_dropped": dup_dropped, "kept_candidates": len(candidates)},
    })
    write_json("/tmp/sources_fetch_stats.json", {"generated": now_iso(), "fallback": fallback, "stats": stats})
    summary = {"fallback": fallback, "raw_results": raw_total, "junk_dropped": junk_dropped,
               "dup_dropped": dup_dropped, "candidates": len(candidates),
               "per_agent": {a: min(len(v), per_agent) for a, v in sorted(cands_by_agent.items())},
               "candidates_file": "/tmp/sources_candidates.json",
               "queries_error": [s["query_tag"] for s in stats if s.get("error")]}
    print(json.dumps(summary, indent=1))
    if not candidates:
        print("NO_WORK: 0 new candidates after dedup/junk filter — reply with exactly the line "
              "'Sources: no new candidates' and STOP. Do not judge, do not write sources_keep.json, "
              "do not run 'sources apply'.")
    else:
        print(f"[engine:sources] {len(candidates)} candidates -> /tmp/sources_candidates.json (LLM quality-bar next)")
    return 0


def phase_sources_apply():
    keep = read_json("/tmp/sources_keep.json", {"items": []})
    cands = read_json("/tmp/sources_candidates.json", {})
    stats_doc = read_json("/tmp/sources_fetch_stats.json", {})
    items = keep.get("items", [])
    cands_by_url = {c["url"]: c for c in cands.get("candidates", [])}

    feed = read_json(os.path.join(CRON, "feed_raw.json"), [])
    if not isinstance(feed, list):
        feed = []
    feed_headlines = {norm_headline(i.get("headline", "")) for i in feed if isinstance(i, dict)}
    feed_urls = {i.get("url") for i in feed if isinstance(i, dict)}

    agent_meta = {q["agent"]: q for q in S_CFG["queries"]}
    appended, skipped = [], 0
    per_agent_applied = {}

    # Pass 1 — deterministic in-scope selection, identical rules to the
    # original sequential loop (order of `items` is the tie-breaker).
    in_scope, seen_in_run = [], set()
    for it in items:
        url = (it.get("url") or "").strip()
        headline = (it.get("headline") or "").strip()
        agent = str(it.get("source_agent", ""))
        if not url or not headline or agent not in agent_meta:
            skipped += 1
            continue
        if per_agent_applied.get(agent, 0) >= S_CFG["max_items_per_agent_apply"] or len(in_scope) >= S_CFG["max_total_items"]:
            skipped += 1
            continue
        if norm_headline(headline) in feed_headlines or url in feed_urls or url in seen_in_run:
            skipped += 1
            continue
        seen_in_run.add(url)
        in_scope.append(it)
        per_agent_applied[agent] = per_agent_applied.get(agent, 0) + 1

    # Pass 2 — fetch article text in parallel (4-6 workers). Each fetch is
    # independent; the browser fallback serializes itself under its own lock.
    texts = {}
    if in_scope:
        n_workers = min(6, max(4, len(in_scope)))
        with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as ex:
            fut_url = {ex.submit(fetch_article_text, (it.get("url") or "").strip(),
                                 S_CFG["max_text_chars"]): (it.get("url") or "").strip()
                       for it in in_scope}
            for fut in concurrent.futures.as_completed(fut_url):
                texts[fut_url[fut]] = fut.result()

    # Pass 3 — assemble entries deterministically in original order.
    for it in in_scope:
        url = (it.get("url") or "").strip()
        headline = (it.get("headline") or "").strip()
        agent = str(it.get("source_agent", ""))
        cand = cands_by_url.get(url, {})
        text = texts.get(url)
        text_source = "fetch" if text else "snippet"
        text = text or (cand.get("content") or (it.get("text") or ""))[:S_CFG["max_text_chars"]]
        meta = agent_meta[agent]
        benefit = [t for t in (it.get("tickers_benefit") or []) if re.fullmatch(r"[A-Z][A-Z0-9.\-]{1,9}", t)]
        affected = [t for t in (it.get("tickers_affected") or []) if re.fullmatch(r"[A-Z][A-Z0-9.\-]{1,9}", t)]
        mentioned = [t for t in (it.get("tickers_mentioned") or benefit + affected)
                     if re.fullmatch(r"[A-Z][A-Z0-9.\-]{1,9}", t)]
        if meta["source_type"] == "negative_intelligence" and not affected:
            affected = list(benefit)
        entry = {
            "source_name": meta["source_name"], "source_type": meta["source_type"],
            "headline": headline, "url": url, "date_discovered": now_iso(),
            "tickers_mentioned": mentioned, "text": text,
            "category": it.get("category") or cand.get("suggested_category", ""),
            "tickers_benefit": benefit, "tickers_affected": affected,
            "text_source": text_source,
        }
        feed.append(entry)
        feed_headlines.add(norm_headline(headline))
        feed_urls.add(url)
        appended.append(entry)

    write_json(os.path.join(CRON, "feed_raw.json"), feed)

    # ledger rows (one per executed query; results_new = fetch-time raw new count)
    ledger = read_json(os.path.join(CRON, "query_health.json"), {"history": []})
    hist = ledger.get("history", []) if isinstance(ledger, dict) else []
    applied_by_tag = {}
    for e in appended:
        c = cands_by_url.get(e["url"])
        if c:
            applied_by_tag[c["tag"]] = applied_by_tag.get(c["tag"], 0) + 1
    for s in stats_doc.get("stats", []):
        row = {"ts": now_iso(), "query_tag": s["query_tag"], "query": s["query"],
               "time_range": s["time_range"], "results_total": s["results_total"],
               "results_fresh": s["results_fresh"], "results_new": s["results_new"],
               "engines": s.get("engines", [])}
        if s.get("error"):
            row["error"] = s["error"][:120]
        row["results_applied"] = applied_by_tag.get(s["query_tag"], 0)
        row["fallback"] = stats_doc.get("fallback", False)
        hist.append(row)
    ledger["history"] = hist[-336:]
    write_json(os.path.join(CRON, "query_health.json"), ledger)

    summary = {"appended": len(appended), "skipped": skipped,
               "per_agent": per_agent_applied, "feed_total": len(feed),
               "ledger_rows": len(ledger["history"])}
    print(json.dumps(summary, indent=1))
    print(f"[engine:sources:apply] {len(appended)} items appended to feed_raw.json, ledger updated")
    return 0


# ---------------------------------------------------------------- process
MAT_RANK = {"direct": 3, "indirect": 2, "speculative": 1}
CONF_RANK = {"high": 3, "medium": 2, "low": 1}


def brief_source(b):
    """Source identity for a brief: new briefs carry source_name, legacy ones _src_type."""
    return b.get("source_name") or b.get("_src_type") or ""

# entity-resolution pass pattern cache (suggest_tickers): (key, {nm: (pat, anchor, sym)})
_NAME_PAT_CACHE = (None, {})


def load_brief_history(max_days=30):
    """Union of current signal_briefs + the engine-owned history file.
    The LLM may rewrite signal_briefs.json with a smaller set each cycle; the
    history file makes previously-briefed items un-losable (no re-judging,
    stable conviction math, deepdive catalyst context)."""
    briefs_doc = read_json(os.path.join(CRON, "signal_briefs.json"), {})
    existing = briefs_doc.get("signal_briefs", []) if isinstance(briefs_doc, dict) else briefs_doc
    if not isinstance(existing, list):
        existing = []
    hist_doc = read_json(os.path.join(CRON, "signal_briefs_history.json"), {})
    hist = hist_doc.get("briefs", []) if isinstance(hist_doc, dict) else hist_doc
    if not isinstance(hist, list):
        hist = []
    by_id = {}
    for b in hist + existing:
        if isinstance(b, dict) and b.get("brief_id"):
            by_id[b["brief_id"]] = b
    return existing, list(by_id.values())


def trim_briefs(briefs, max_days):
    cutoff = time.time() - max_days * 86400
    out = []
    for b in briefs:
        if not isinstance(b, dict):
            continue
        created = ts_parse(b.get("date_created", "")) or time.time()
        if created >= cutoff:
            out.append(b)
    return out


def claim_ah_promotions():
    """Wave-A Step 6 (2026-10-31): atomic claim of the after-hours
    promotion bridge. The bridge (ah_promote_bridge.py, 90m lane) writes
    high-magnitude hits to ah_promotions.json via a single atomic write;
    ah_research (90m) and Processing (15 */2) overlap every 6h, so this
    consumer must NOT read-then-truncate the file in place. Instead:
      1. os.replace(ah_promotions.json, ah_promotions.claiming.json) —
         the single-writer claim. A concurrent second claimant gets
         FileNotFoundError -> clean no-op.
      2. ingest + dedup from the CLAIMING file against
         ah_bridge_state.json (same key scheme as the bridge:
         ticker:dollar:headline-60, 14d) + the feed's headline/url sets.
      3. append feed entries to feed_raw.json (the engine owns that
         file; this runs inside phase_process under the engine's own
         lock discipline — no cross-writer overlap).
      4. unlink the claiming file ONLY after the feed + state writes
         land (a crash mid-ingest keeps the claiming file on disk; the
         next run re-claims it and the 14d bridge dedup state makes the
         re-ingest idempotent — tickets are never lost).
    FileNotFoundError when no promotions exist is the normal steady
    state (most collector runs find nothing) and is a no-op, not an
    error. Returns (n_new, n_dup, claimed_bool)."""
    src = os.path.expanduser(os.path.join(CRON, "ah_promotions.json"))
    claiming = src + ".claiming.json"
    if os.path.exists(claiming):
        # a crashed claimant left a claiming file: re-claim it rather
        # than let it sit and starve future claims.
        data = read_json(claiming, None)
    elif os.path.exists(src):
        try:
            os.replace(src, claiming)
        except FileNotFoundError:
            return (0, 0, False)  # a concurrent claimant won the rename
        data = read_json(claiming, None)
    else:
        return (0, 0, False)  # no promotions this run — the normal case
    if not isinstance(data, dict):
        if os.path.exists(claiming):
            os.remove(claiming)
        return (0, 0, False)
    tickets = data.get("tickets") or []
    state_f = os.path.expanduser(os.path.join(CRON, "ah_bridge_state.json"))
    st = read_json(state_f, {"seen": {}})
    st.setdefault("seen", {})
    cutoff = time.time() - 14 * 86400
    st["seen"] = {k: v for k, v in st["seen"].items() if v >= cutoff}
    feed = read_json(os.path.join(CRON, "feed_raw.json"), [])
    if not isinstance(feed, list):
        feed = []
    seen_u = {i.get("url") for i in feed if isinstance(i, dict)}
    seen_h = {norm_headline(i.get("headline", "")) for i in feed
              if isinstance(i, dict)}
    n_new = n_dup = 0
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    for e in tickets:
        if not isinstance(e, dict):
            continue
        tk = str(e.get("ticker") or "").upper()
        amt = e.get("amount_usd") or 0
        h = e.get("headline") or ""
        key = f"{tk}:{int(amt) if amt else 0}:" + \
            re.sub(r"\W+", " ", (h or "").lower()).strip()[:60]
        if key in st["seen"] or norm_headline(h) in seen_h \
                or (e.get("url") and e["url"] in seen_u):
            n_dup += 1
            continue
        st["seen"][key] = time.time()
        kind = e.get("kind") or "ah_catalyst"
        amount = e.get("amount_usd")
        amt_s = f"${amount/1e6:,.0f}M" if amount else ""
        mc = e.get("mcap")
        mc_s = (f" | mcap ${mc/1e9:.1f}B" if isinstance(mc, (int, float))
                and mc >= 1e9
                else (f" | mcap ${mc/1e6:.0f}M"
                      if isinstance(mc, (int, float)) and mc else ""))
        entry = {
            "source_name": "ah_bridge",
            "source_type": "catalyst",
            "category": e.get("category") or "edge_robotics",
            "headline": re.sub(r"\s+", " ",
                               f"After-hours {kind}: {tk} {amt_s} "
                               f"{h}")[:300],
            "url": e.get("url") or "",
            "date_discovered": now,
            "source_date": (e.get("pubdate") or "")[:10] or now[:10],
            "tickers_mentioned": [tk] if tk else [],
            "text": (f"ah_promote_bridge {kind} | {e.get('ticker')} | "
                     f"{amt_s or 'no $ extracted'} | kind {kind} | "
                     f"query {e.get('query') or 'n/a'} | "
                     f"pubdate {e.get('pubdate') or 'n/a'}"
                     f"{mc_s} | headline: {h[:200]}"),
            "severity": "S1" if (isinstance(amount, (int, float))
                                 and amount >= 100_000_000
                                 and isinstance(mc, (int, float))
                                 and mc < 5_000_000_000) else "S2",
            "text_source": "ah_bridge_claim",
        }
        feed.append(entry)
        seen_h.add(norm_headline(entry["headline"]))
        if entry["url"]:
            seen_u.add(entry["url"])
        n_new += 1
    # land BOTH writes before the claim is closed (atomic per-file; the
    # claiming file is the crash-recovery net, see the docstring)
    write_json(os.path.join(CRON, "feed_raw.json"), feed)
    write_json(state_f, st)
    os.remove(claiming)
    print(f"[engine:ah-claim] claimed {len(tickets)} ah_promotions: "
          f"{n_new} new feed entries, {n_dup} deduped "
          f"(source: {data.get('generated', '?')})")
    return (n_new, n_dup, True)


def phase_process():
    # Wave-A Step 6: claim the after-hours promotion bridge BEFORE any
    # other processing (the claimed entries join this run's feed view).
    # Must never kill the phase: a bridge file glitch degrades to a no-op.
    try:
        claim_ah_promotions()
    except Exception as ex:
        print(f"[engine:ah-claim] WARNING: claim failed ({ex}) — "
              f"processing continues without it")
    feed = read_json(os.path.join(CRON, "feed_raw.json"), [])
    if not isinstance(feed, list):
        feed = []
    existing, all_briefed = load_brief_history(30)

    briefed_h = set()
    briefed_u = set()
    # Layer-3 guard (2026-10-30, "entity-number guard"): a syndicated rewrite
    # that dodges the 0.9 SequenceMatcher threshold still re-announces the SAME
    # catalyst. If (ticker, extracted dollar amount) matches an already-briefed
    # item within the 7-day window, the item is a corroboration, NOT a net-new
    # catalyst: it is skipped from LLM judgment AND never increments conviction.
    # Purely deterministic; the 0.9 similarity path above is untouched.
    corrob_window_days = P_CFG.get("corroboration_window_days", 7)
    corrob_cutoff = time.time() - corrob_window_days * 86400
    corrob_keys = set()
    for b in all_briefed:
        if not isinstance(b, dict):
            continue
        created = ts_parse(b.get("date_created", "")) or time.time()
        if created < corrob_cutoff:
            continue
        amt = b.get("dollar_amount")
        if not amt:
            continue
        for t in (set(b.get("tickers_benefit", [])) | set(b.get("tickers_affected", []))
                  | set(b.get("tickers_mentioned", []))):
            corrob_keys.add((str(t).upper(), amt))
    corrob_hits = 0
    for b in all_briefed:
        if not isinstance(b, dict):
            continue
        briefed_h.add(norm_headline(b.get("headline", "")))
        briefed_u.update(b.get("source_urls", []) or [b.get("url", "")])

    # dedup feed: exact normalized headline, then near-duplicate similarity
    sim = P_CFG.get("headline_similarity", 0.9)
    kept, kept_norms = [], []
    for it in feed:
        if not isinstance(it, dict):
            continue
        h = it.get("headline", "")
        n = norm_headline(h)
        if not n:
            continue
        dup = any(difflib.SequenceMatcher(None, n, k).ratio() >= sim for k in kept_norms)
        if dup:
            continue
        kept.append(it)
        kept_norms.append(n)

    new_items = []
    for idx, it in enumerate(kept):
        h = norm_headline(it.get("headline", ""))
        if h in briefed_h or it.get("url") in briefed_u:
            continue
        # Layer-3 corroboration guard: same (ticker, $ amount) already briefed
        # within the window -> corroborates, not a net-new catalyst.
        it_amt = extract_dollar_amount((it.get("headline", "") or "") + " " + (it.get("text", "") or ""))
        it_tickers = set(it.get("tickers_benefit", [])) | set(it.get("tickers_affected", [])) | set(it.get("tickers_mentioned", []))
        if it_amt and any((str(t).upper(), it_amt) in corrob_keys for t in it_tickers):
            corrob_hits += 1
            continue
        new_items.append({
            "i": idx,
            "headline": (it.get("headline") or "")[:300],
            "url": it.get("url", ""),
            "source_name": it.get("source_name", ""),
            "source_type": it.get("source_type", ""),
            "category": it.get("category", ""),
            "tickers_mentioned": it.get("tickers_mentioned", []),
            "tickers_benefit": it.get("tickers_benefit", []),
            "tickers_affected": it.get("tickers_affected", []),
            "dollar_amount": it_amt,
            "text": (it.get("text") or "")[:P_CFG.get("item_text_chars", 600)],
        })

    write_json("/tmp/process_items.json", {
        "generated": now_iso(),
        "feed_total": len(feed), "after_dedup": len(kept),
        "already_briefed_skipped": len(kept) - len(new_items) - corrob_hits,
        "corroboration_skipped": corrob_hits,
        "new_items": len(new_items),
        "instructions": ("So-What judgment: for EACH item write a keep decision + brief fields. "
                         "keep=false for noise (no company-specific fact, pure price chatter, opinion/forecast). "
                         "Write /tmp/process_briefs_final.json = {\"briefs\":[{i, keep, impact_direction "
                         "(positive|negative|neutral|ambiguous), impact_level (high|medium|low), materiality "
                         "(direct|indirect|speculative), confidence (high|medium|low), summary (3-5 sentence "
                         "So-What analysis, not a restatement), next_steps}]}. Then run "
                         "'python3 pipeline_engine.py process apply'."),
        "items": new_items,
    })
    summary = {"feed_total": len(feed), "after_dedup": len(kept),
               "already_briefed_skipped": len(kept) - len(new_items),
               "new_items_to_judge": len(new_items),
               "items_file": "/tmp/process_items.json"}
    print(json.dumps(summary, indent=1))
    if not new_items:
        print("NO_WORK: 0 new items to judge — skip STEP 2/3 entirely (no So-What, no 'process apply'), "
              "proceed straight to STEP 4 (QUERY TUNER) and report 'Processing: nothing new' in one line.")
    else:
        print(f"[engine:process] {len(new_items)} new items -> /tmp/process_items.json (LLM So-What judgment next)")
    return 0


def conviction_scores(briefs):
    """Deterministic tiered conviction (0-100) per unique ticker.
    Direct mention = ticker in tickers_benefit/affected (counts for all dims).
    Generic mention = tickers_mentioned only (boosts cross-source only).
    """
    td = {}

    def entry(t):
        return td.setdefault(t, {"direct": [], "sources": set(), "domains": set(),
                                 "categories": set(), "generic": 0})

    for b in briefs:
        if not isinstance(b, dict):
            continue
        direct = set(b.get("tickers_benefit", [])) | set(b.get("tickers_affected", []))
        all_t = set(b.get("tickers_mentioned", []) or direct)
        generic = all_t - direct
        src = brief_source(b)
        urls = b.get("source_urls", []) or []
        domains = set()
        for u in urls:
            try:
                domains.add(urllib.parse.urlparse(u).netloc)
            except Exception:
                pass
        for t in direct:
            e = entry(t)
            e["direct"].append(b)
            if src:
                e["sources"].add(src)
            e["domains"] |= domains
            e["categories"].add(b.get("category", ""))
        for t in generic:
            e = entry(t)
            e["generic"] += 1
            if src:
                e["sources"].add(src)
            e["domains"] |= domains

    out = []
    for t, e in td.items():
        ds = e["direct"]
        n = len(ds)
        # brief count (direct only)
        pts_brief = 0 if n < 2 else (5 if n <= 4 else (10 if n <= 7 else (15 if n <= 19 else 20)))
        # best direct brief by (materiality rank, confidence rank, recency)
        def brank(b):
            return (MAT_RANK.get(b.get("materiality"), 1), CONF_RANK.get(b.get("confidence"), 1),
                    ts_parse(b.get("date_created", "")) or 0)
        best = max(ds, key=brank) if ds else None
        if best is None:
            pts_dir, pts_mat, pts_conf = 0, 0, 0
            direction = "ambiguous"
        else:
            d = best.get("impact_direction", "ambiguous")
            m = best.get("materiality", "")
            if d == "positive":
                pts_dir = 20 if m == "direct" else 15
            elif d in ("ambiguous", "neutral"):
                pts_dir = 10
            else:
                pts_dir = 0
            pts_mat = 15 if m == "direct" else (10 if m == "indirect" else 0)
            pts_conf = CONF_RANK.get(best.get("confidence", ""), 1) * 5
            direction = d
        cross = len(e["sources"] | e["domains"])
        pts_cross = 0 if cross <= 1 else (5 if cross == 2 else (10 if cross == 3 else (15 if cross == 4 else 20 if cross == 5 else 30)))
        score = pts_brief + pts_dir + pts_mat + pts_conf + pts_cross
        heads = sorted(ds, key=lambda b: brank(b), reverse=True)[:3]
        out.append({
            "ticker": t, "conviction_score": score, "brief_count": n,
            "impact_direction": direction,
            "materiality": best.get("materiality", "") if best else "",
            "confidence": best.get("confidence", "") if best else "",
            "cross_source_count": cross,
            "brief_categories": sorted(c for c in e["categories"] if c),
            "top_headlines": [(b.get("headline", ""))[:120] for b in heads],
        })
    out.sort(key=lambda x: (-x["conviction_score"], x["ticker"]))
    return out


def wiki_note_path(ticker):
    root = os.path.join(WIKI, "companies")  # clean-slate layout 2026-11-05
    p = os.path.join(root, f"{ticker}.md")
    if os.path.isfile(p):
        return p
    d = os.path.join(root, ticker)
    if os.path.isdir(d):
        inner = os.path.join(d, f"{ticker}.md")
        if os.path.isfile(inner):
            return inner
        mds = [f for f in os.listdir(d) if f.endswith(".md")]
        if mds:
            return os.path.join(d, mds[0])
    elif os.path.isfile(os.path.join(d, f"{ticker}.md")):
        # dir may not exist as a directory but the inner dossier is present
        # (created directly at the nested path); treat as canonical.
        return os.path.join(d, f"{ticker}.md")
    return p


# ---------------------------------------------------------------- mcap vetting gate
# Wiki policy (2026-10-16): tickers must be real FMP-listed equities under the
# wiki_max_market_cap threshold to enter the wiki. Fetched once per ticker via
# FMP batch-market-cap, cached to CRON/wiki_mcap_cache.json (TTL from config).
MCAP_CACHE = os.path.join(CRON, "wiki_mcap_cache.json")


def _mcap_cache_load():
    d = read_json(MCAP_CACHE, {})
    return (d.get("values") if isinstance(d, dict) else {}) or {}


def _mcap_cache_save(values):
    write_json(MCAP_CACHE, {"updated": now_iso(), "values": values})


def fmp_batch_market_cap(tickers):
    """Direct FMP batch-market-cap lookup. Returns {TICKER: market_cap_or_0};
    empty dict on transport error (caller falls back to cache).
    Shared-rail (2026-11-07): reuses the shared Mcp-Session-Id via
    fmp_session() and recovers dead sessions via _fmp_recover() — no more
    per-call handshake fork, and a mid-chunk session drop retries on a
    fresh session instead of returning {}."""
    if not tickers:
        return {}
    sid = fmp_session()
    if not sid:
        return {}
    out = {}
    for chunk_start in range(0, len(tickers), 99):
        chunk = tickers[chunk_start:chunk_start + 99]
        payload = json.dumps({"jsonrpc": "2.0", "method": "tools/call",
            "params": {"name": "company", "arguments": {
                "endpoint": "batch-market-cap", "symbols": chunk}}, "id": 900}).encode()
        st, body, exc = fmp_raw_send(sid, payload)
        if exc is not None:
            if _is_session_failure(st, body, exc):
                nsid = _fmp_recover(sid)
                if not nsid:
                    return {}
                st, body, exc = fmp_raw_send(nsid, payload)
                sid = nsid
            if exc is not None:
                return {}
        try:
            if body.lstrip().startswith("data:"):
                body = [l[5:].strip() for l in body.splitlines() if l.startswith("data:")][0]
            d = json.loads(body)
            res = d.get("result") or {}
            if res.get("sessionId"):
                sid = res["sessionId"]
            content = res.get("content")
            if isinstance(content, list) and content and isinstance(content[0], dict):
                data = json.loads(content[0].get("text") or "null")
            else:
                data = res
            if isinstance(data, dict) and data.get("_error"):
                return {}
            for row in (data if isinstance(data, list) else []):
                if isinstance(row, dict) and row.get("symbol"):
                    mc = row.get("marketCap")
                    out[row["symbol"].upper()] = (mc if isinstance(mc, (int, float)) else 0)
        except Exception:
            return {}
        time.sleep(0.3)
    return out


def _fmp_profile_symbol(sid, symbol, call_id):
    """Authoritative per-symbol check (2026-10-17: batch-market-cap silently
    drops rows when busy and is NOT used for gate decisions anymore).
    Returns dict: found(bool), cik, name, mcap, price, error."""
    out = fmp_call(sid, "company", {"endpoint": "profile-symbol", "symbol": symbol}, call_id)
    if isinstance(out, dict) and out.get("_error"):
        return {"found": False, "error": str(out.get("_error"))[:150]}
    if not (isinstance(out, list) and out and out[0].get("symbol") is not None):
        return {"found": False, "error": "not_found_on_profile"}
    p = out[0]
    mc = p.get("market_cap") or p.get("marketCap")
    q = fmp_call(sid, "quote", {"endpoint": "quote", "symbol": symbol}, call_id + 1)
    price = None
    if isinstance(q, list) and q and isinstance(q[0].get("marketCap"), (int, float)):
        price = q[0].get("price")
        if not isinstance(mc, (int, float)) or mc == 0:
            mc = q[0]["marketCap"]
    return {"found": True, "cik": p.get("cik"), "name": p.get("name") or p.get("companyName"),
            "mcap": mc if isinstance(mc, (int, float)) else None, "price": price}


def vet_tickers(tickers, cache, vet_sid=None):
    """Apply the wiki market-cap gate. Returns (allowed_list, dropped_dict).
    2026-10-17: per-symbol profile-symbol is the AUTHORITATIVE lookup
    (batch-market-cap silently drops rows when busy — a dropped row must never
    be read as 'not a real ticker'). Soft rule: hard-exclude >=
    wiki_mcap_band_ceiling (default $110B); the band [floor, ceiling)
    ($90B-$110B default) is allowed with a near_ceiling flag; funds/ETFs
    (null CIK) are excluded. Unknowns are dropped fail-closed; transport
    errors keep the ticker in a HOLD list (never silently dropped).
    vet_sid: pre-opened FMP session id (deepdive reuses its own to avoid
    re-initializing); when None a session is opened for this call only."""
    limit = TH.get("wiki_max_market_cap", 300_000_000_000)          # hard ceiling ($300B)
    near_floor = 500_000_000              # $500M hard floor — the only floor
    near_miss_floor = 200_000_000_000      # $200B near-miss band (reporting only, never blocks)
    ttl = TH.get("mcap_cache_ttl_days", 7) * 86400
    sid = vet_sid or fmp_session()
    cid = 9500
    allowed, dropped, flagged, held = [], {}, {}, []
    changed = False
    for t in tickers:
        c = cache.get(t)
        fresh = c and (time.time() - c.get("ts", 0)) <= ttl
        if not fresh:
            prof = _fmp_profile_symbol(sid, t, cid); cid += 2
            time.sleep(0.1)
            if prof.get("error") and prof["error"].startswith(("HTTP", "urlopen", "timed out")):
                if c and c.get("cap"):
                    allowed.append(t)  # stale-but-proven cache: allow through
                    continue
                held.append(t)          # no proof at all: hold, do NOT drop
                continue
            if not prof.get("found"):
                if c and c.get("cap"):
                    pass               # recheck failed to find but cache says real: keep cached verdict
                else:
                    dropped[t] = "not_a_real_ticker"
                    continue
            if not prof.get("cik"):
                dropped[t] = f"etf_or_fund:{(prof.get('name') or '')[:30]}"
                continue
            c = {"cap": prof.get("mcap"), "ts": time.time(), "src": "per_symbol_gate_20261017"}
            cache[t] = c
            changed = True
        cap = c.get("cap") if isinstance(c.get("cap"), (int, float)) else 0
        if cap >= limit:
            dropped[t] = f"over_cap:{cap}"
        elif cap < near_floor:
            dropped[t] = f"below_floor:{cap}"
        elif cap >= near_miss_floor:
            allowed.append(t)
            flagged[t] = f"near_ceiling:{cap}"
        else:
            allowed.append(t)
    if changed:
        _mcap_cache_save(cache)
    if held:
        print(f"[engine:vet] HOLD (no proof, not dropped) {len(held)}: {','.join(sorted(held)[:30])}",
              file=sys.stderr)
    return allowed, dropped, flagged


def ensure_wiki_file(path):
    """Create a missing wiki dossier. CANONICAL FORM (2026-11-04): a dossier
    created at the companies/ root is forced into <T>/<T>.md directory form —
    flat root files are the historical split-brain source; dir form is
    authoritative. (Pre-existing root files are read fine by wiki_note_path.)"""
    if os.path.isfile(path):
        return
    parent = os.path.dirname(path)
    if os.path.basename(parent).endswith("companies"):
        t = os.path.basename(path)[:-3]
        path = os.path.join(parent, t, os.path.basename(path))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    t = os.path.basename(path)[:-3]
    with open(path, "w") as f:
        f.write(f"# {t}\n\n## Catalyst Log\n")


def append_wiki_note(ticker, brief):
    """Append a dated note line to the ticker wiki file under '## Catalyst Log',
    creating the section at end-of-file if absent. Idempotent on brief_id.
    NOTE: only ever appends — LLM-authored detailed blocks below the heading are
    preserved verbatim; the engine never truncates wiki files."""
    path = wiki_note_path(ticker)
    ensure_wiki_file(path)
    # ensure_wiki_file relocates missing root dossiers to <T>/<T>.md; the
    # pre-ensure path can be stale — re-resolve so opens below hit the real file
    # (root file now orphaned: remove it if the note file lives elsewhere).
    live = wiki_note_path(ticker)
    if live != path and os.path.isfile(live) and os.path.isfile(path):
        os.remove(path)
    path = live
    bid8 = (brief.get("brief_id") or "")[:8].upper()
    if bid8 and bid8.lower() in open(path).read().lower():
        return False
    date = (brief.get("date_created") or "")[:10]
    note = (f"- [{date}] {brief.get('headline', '')[:120]} | {brief.get('category', '')} "
            f"({brief.get('impact_direction', '')} {brief.get('impact_level', '')}) [`{bid8}`]")
    content = open(path).read()
    # newest note at the TOP of the Catalyst Log section (first occurrence);
    # fall back to end-of-file if the section is missing
    m = re.search(r"^## Catalyst Log\s*$", content, re.M)
    if m:
        insert_at = m.end()
        rest = content[insert_at:]
        linebreak = rest.find("\n")
        linebreak = linebreak if linebreak != -1 else len(rest)
        # skip an existing blank line
        gap = rest[:linebreak].strip()
        insert_at += linebreak + (1 if gap else 0)
        content = content[:insert_at] + note + "\n" + content[insert_at:]
    else:
        content = content.rstrip() + "\n\n## Catalyst Log\n" + note + "\n"
    with open(path, "w") as f:
        f.write(content)
    return True


# ---------------------------------------------------------------- staging
# Chunk 4 of the Entity Acquisition Upgrade (2026-11-04): a SQLite buffer in
# front of append_wiki_note. Single-source, uncorroborated catalysts stage
# here instead of writing wiki files immediately; they promote when a
# second DISTINCT source corroborates (or on a single high-conviction
# event), and purge after 14 days without follow-up. This blocks single-
# source mirages from polluting the Karpathy wiki while keeping catalyst
# velocity (the 30-day brief half-life is unchanged).
STAGING_TABLE = """
CREATE TABLE IF NOT EXISTS wiki_staging_queue (
    ticker TEXT PRIMARY KEY,
    company_name TEXT,
    first_detected TEXT,
    last_detected TEXT,
    hit_count INTEGER DEFAULT 1,
    highest_conviction REAL,
    primary_catalyst TEXT,
    sources TEXT,
    promoted INTEGER DEFAULT 0
);
"""
STAGE_PURGE_DAYS = 14


def staging_db():
    con = sqlite3.connect(os.path.join(CRON, "universe_cache.db"),
                          timeout=15.0, isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=15000")
    con.execute(STAGING_TABLE)
    return con


def staging_purge(con):
    """Drop un-promoted rows with no follow-up inside the half-life window.
    Called every process-apply run (cheap single DELETE). isolation_level=None
    autocommits the single statement — no explicit COMMIT (there is no open
    transaction to commit, and a bare COMMIT raises OperationalError)."""
    cutoff = (datetime.now(timezone.utc)
              - timedelta(days=STAGE_PURGE_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
    cur = con.execute(
        "DELETE FROM wiki_staging_queue WHERE promoted = 0 AND last_detected < ?",
        (cutoff,))
    return cur.rowcount or 0


def staging_touch(con, t, brief, score_map):
    """Record a catalyst hit for ticker t in the staging table.
    Returns the updated row as a dict (or None when the DB is unavailable)."""
    sources_key = _staging_sources_key(brief)
    ts = now_iso()
    conv = score_map.get(t, {}).get("conviction_score", 0)
    head = (brief.get("headline") or "")[:160]
    con.execute("BEGIN IMMEDIATE")
    row = con.execute("SELECT hit_count, sources, highest_conviction, first_detected, promoted "
                      "FROM wiki_staging_queue WHERE ticker = ?", (t,)).fetchone()
    if row is None:
        con.execute(
            """INSERT INTO wiki_staging_queue
               (ticker, company_name, first_detected, last_detected, hit_count,
                highest_conviction, primary_catalyst, sources, promoted)
               VALUES (?,?,?,?,1,?,?,?,0)""",
            (t, "", ts, ts, float(conv), head, sources_key))
    else:
        hc = max(float(row[2] or 0), float(conv))
        srcs = row[1] or ""
        if sources_key and sources_key not in srcs.split(","):
            srcs = (srcs + "," + sources_key)[:600]
        con.execute(
            """UPDATE wiki_staging_queue SET last_detected = ?, hit_count = hit_count + 1,
               highest_conviction = MAX(highest_conviction, ?), sources = ? WHERE ticker = ?""",
            (ts, float(conv), srcs, t))
    con.execute("COMMIT")
    r = con.execute("SELECT hit_count, sources, highest_conviction, promoted "
                    "FROM wiki_staging_queue WHERE ticker = ?", (t,)).fetchone()
    return {"hit_count": r[0], "sources": [s for s in (r[1] or "").split(",") if s],
            "highest_conviction": r[2], "promoted": r[3]}


def staging_promote(con, t):
    # single statement: autocommits under isolation_level=None (a bare
    # COMMIT would raise — there is no open transaction)
    con.execute("UPDATE wiki_staging_queue SET promoted = 1 WHERE ticker = ?", (t,))


def _staging_sources_key(brief):
    src = brief_source(brief) or "unknown"
    url = (brief.get("source_urls") or [None])[0] or ""
    dom = urllib.parse.urlparse(url).netloc if url else ""
    return (src + "/" + dom) if dom else src


def staging_admit(t, brief, score_map):
    """Admission decision (chunk 4 rules), called AFTER staging_touch:
      A. Single high-conviction event: conviction >= 75 -> promote now.
      B. Corroborated: >=2 recorded hits AND a DIFFERENT source than the
         current one in the staged history AND best conviction >= 50
         -> promote now.
      C. Otherwise: stays staged (wiki write skipped).
    Fail-open: any DB error returns True (wiki write proceeds) — a staging
    outage must never silence a real catalyst."""
    conv = score_map.get(t, {}).get("conviction_score", 0)
    if conv >= 75:
        return True
    try:
        con = staging_db()
    except Exception:
        return True
    try:
        row = con.execute("SELECT hit_count, sources, highest_conviction "
                          "FROM wiki_staging_queue WHERE ticker = ?", (t,)).fetchone()
        if not row:
            return True  # touch failed to land: don't silently drop
        cur_src = _staging_sources_key(brief)
        priors = [s for s in (row[1] or "").split(",") if s and s != cur_src]
        if row[0] >= 2 and priors and max(float(row[2] or 0), conv) >= 50:
            return True
        return False
    except Exception:
        return True
    finally:
        con.close()


def regenerate_research_interest(briefs, new_count):
    """Rebuild research_interest.md deterministically. Cumulative counters live in
    process_state.json (the regenerated file must never be the source of truth —
    this keeps the counters correct across rewrites)."""
    path = os.path.join(WIKI, "research_interest.md")
    state = read_json(os.path.join(CRON, "process_state.json"), {})
    cum_briefs = state.get("cumulative_briefs_processed", 0) + new_count
    state["cumulative_briefs_processed"] = cum_briefs
    state["last_run"] = now_iso()
    write_json(os.path.join(CRON, "process_state.json"), state)

    cat_counts = {}
    src_counts = {}
    per_ticker = {}
    rows = []
    for b in briefs:
        cat = b.get("category", "")
        cat_counts[cat] = cat_counts.get(cat, 0) + 1
        sn = brief_source(b)
        if sn:
            src_counts[sn] = src_counts.get(sn, 0) + 1
        tickers = sorted(set(b.get("tickers_benefit", [])) | set(b.get("tickers_affected", [])))
        for t in tickers:
            e = per_ticker.setdefault(t, {"briefs": 0, "cats": set(), "imp": set()})
            e["briefs"] += 1
            if cat:
                e["cats"].add(cat)
            e["imp"].add(f"{b.get('impact_direction', '')} {b.get('impact_level', '')}".strip())
        rows.append("| {d} | {h} | {t} | {i} | {m} | {c} | {cf} | `{b}` |".format(
            d=(b.get("date_created") or "")[:10],
            h=re.sub(r"\|", "/", (b.get("headline") or "")[:70]),
            t=",".join(tickers[:3]),
            i=f"{b.get('impact_direction', '')} {b.get('impact_level', '')}".strip(),
            m=b.get("materiality", ""), c=b.get("category", ""),
            cf=b.get("confidence", ""), b=(b.get("brief_id") or "")[:8].upper()))

    all_tickers = set(per_ticker)
    table = ["| Ticker | Briefs | Category | Impact |", "|--------|--------|----------|--------|"]
    for t, e in sorted(per_ticker.items(), key=lambda kv: (-kv[1]["briefs"], kv[0])):
        table.append(f"| {t} | {e['briefs']} | {','.join(sorted(e['cats']))} | {','.join(sorted(e['imp']))} |")
    src_rows = ["| Source Type | Count |", "|-------------|-------|"]
    for k, v in sorted(src_counts.items(), key=lambda kv: -kv[1]):
        src_rows.append(f"| {k} | {v} |")
    out = f"""# Research of Interest

## Signal Hit Rate Dashboard
- **Briefs Processed**: {cum_briefs}
- **Unique Tickers**: {len(all_tickers)}
- **Categories**: {cat_counts}

## Source Type Distribution
{chr(10).join(src_rows)}

## Active Research List

{chr(10).join(table)}

## Research of Interest

| Date | Headline | Tickers | Impact | Materiality | Category | Confidence | Brief ID |
|------|----------|---------|--------|-------------|----------|------------|----------|
{chr(10).join(rows)}
"""
    with open(path, "w") as f:
        f.write(out)
    return cum_briefs, len(all_tickers)


def phase_process_apply():
    final = read_json("/tmp/process_briefs_final.json", {"briefs": []})
    items_doc = read_json("/tmp/process_items.json", {})
    items = {it["i"]: it for it in items_doc.get("items", [])}
    feed = read_json(os.path.join(CRON, "feed_raw.json"), [])
    if not isinstance(feed, list):
        feed = []
    feed_by_headline = {}
    for it in feed:
        if isinstance(it, dict):
            feed_by_headline.setdefault(norm_headline(it.get("headline", "")), it)

    briefs_doc = read_json(os.path.join(CRON, "signal_briefs.json"), {})
    existing, all_briefed = load_brief_history(30)

    new_briefs = []
    errors = []
    mcap_cache = _mcap_cache_load()
    mcap_dropped = {}
    mcap_near_ceiling = {}
    for row in final.get("briefs", []):
        it = items.get(row.get("i"))
        if not it:
            errors.append(f"unknown item index {row.get('i')}")
            continue
        if not row.get("keep", True):
            continue
        url = it.get("url", "")
        headline = it.get("headline", "")
        bid = uuid.uuid5(uuid.NAMESPACE_URL, url + "|" + norm_headline(headline)).hex
        if any(b.get("brief_id") == bid for b in all_briefed + new_briefs):
            continue
        feed_item = feed_by_headline.get(norm_headline(headline), {})
        benefit = [t for t in (row.get("tickers_benefit") or it.get("tickers_benefit") or [])
                   if re.fullmatch(r"[A-Z][A-Z0-9.\-]{1,9}", t)]
        affected = [t for t in (row.get("tickers_affected") or it.get("tickers_affected") or [])
                    if re.fullmatch(r"[A-Z][A-Z0-9.\-]{1,9}", t)]
        mentioned = [t for t in (it.get("tickers_mentioned") or benefit + affected)
                     if re.fullmatch(r"[A-Z][A-Z0-9.\-]{1,9}", t)]
        for bad in set(mentioned + benefit + affected) - set(mentioned):
            errors.append(f"invalid ticker {bad!r} in {headline[:60]!r}")
        if it.get("source_type") == "negative_intelligence" and not affected:
            affected = list(benefit)
        # wiki market-cap gate: drop not-a-real-ticker, funds/ETFs and >= $110B;
        # $90B-$110B band allowed with near_ceiling flag (soft rule)
        vetted_benefit, dropped, near_ceiling = vet_tickers(list(benefit), mcap_cache)
        vetted_affected, _da, _ = vet_tickers(list(affected), mcap_cache)
        vetted_mentioned, _dm, _ = vet_tickers(list(mentioned), mcap_cache)
        if dropped:
            mcap_dropped.update(dropped)
        if near_ceiling:
            mcap_near_ceiling.update(near_ceiling)
        benefit, affected, mentioned = vetted_benefit, vetted_affected, vetted_mentioned
        nb = {
            "brief_id": bid, "date_created": now_iso(), "headline": headline,
            "source_urls": [url],
            "dollar_amount": it.get("dollar_amount") or extract_dollar_amount(
                headline + " " + (feed_item.get("text") or "")),
            "tickers_affected": affected, "tickers_benefit": benefit,
            "tickers_mentioned": mentioned,
            "impact_direction": row.get("impact_direction", "ambiguous"),
            "impact_level": row.get("impact_level", "medium"),
            "materiality": row.get("materiality", "indirect"),
            "confidence": row.get("confidence", "medium"),
            "category": it.get("category", ""),
            "summary": (row.get("summary") or "").strip(),
            "raw_text": (feed_item.get("text") or it.get("text") or "")[:5000],
            "next_steps": (row.get("next_steps") or "").strip(),
            "source_name": it.get("source_name", ""),
        }
        if not nb["summary"]:
            errors.append(f"missing summary for item {row.get('i')}")
        new_briefs.append(nb)

    merged = trim_briefs(existing, P_CFG.get("brief_retention_days", 14)) + new_briefs
    # engine-owned history: union current + previous history, 30-day trim
    hist_doc = read_json(os.path.join(CRON, "signal_briefs_history.json"), {})
    hist_prev = hist_doc.get("briefs", []) if isinstance(hist_doc, dict) else hist_doc
    if not isinstance(hist_prev, list):
        hist_prev = []
    by_id = {}
    for b in hist_prev + merged:
        if isinstance(b, dict) and b.get("brief_id"):
            by_id[b["brief_id"]] = b
    hist_merged = trim_briefs(list(by_id.values()), P_CFG.get("history_retention_days", 30))
    write_json(os.path.join(CRON, "signal_briefs_history.json"),
               {"updated": now_iso(), "count": len(hist_merged), "briefs": hist_merged})
    write_json(os.path.join(CRON, "signal_briefs.json"), {
        "version": "1.0", "agent": "Agent6_MeatParser", "run_timestamp": now_iso(),
        "raw_items_processed": len(feed), "items_after_dedup": len(feed),
        "signal_briefs_generated": len(merged), "signal_briefs": merged,
    })

    # conviction -> vetted_tickers.json
    scored = conviction_scores(merged)
    tiers = {
        "high_tickers": [s for s in scored if s["conviction_score"] >= 75],
        "moderate_tickers": [s for s in scored if 65 <= s["conviction_score"] <= 74],
        "low_tickers": [s for s in scored if 55 <= s["conviction_score"] <= 64],
        "all_tickers": scored,
    }
    write_json(os.path.join(CRON, "vetted_tickers.json"), {
        "version": "3.0", "agent": "Agent8_Orchestrator", "run_timestamp": now_iso(),
        "briefs_processed": len(merged), "unique_tickers": len(scored),
        "tier_summary": {
            "high_conviction_75_plus": len(tiers["high_tickers"]),
            "moderate_conviction_65_to_74": len(tiers["moderate_tickers"]),
            "low_conviction_55_to_64": len(tiers["low_tickers"]),
            "below_threshold_below_55": len([s for s in scored if s["conviction_score"] < 55]),
        },
        **tiers,
    })

    # wiki: append notes for NEW briefs only (idempotent on brief_id).
    # Chunk 4 staging gate (Entity Acquisition Upgrade 2026-11-04):
    # single-source uncorroborated catalysts stage in
    # wiki_staging_queue instead of writing wiki files; they promote on a
    # distinct second source (conviction >= 50) or a single high-conviction
    # event (>= 75), and purge after 14 quiet days.
    wiki_notes = 0
    staged = 0
    score_map = {s["ticker"]: s for s in scored}
    try:
        sdb = staging_db()
    except Exception:
        sdb = None
    if sdb:
        try:
            staging_purge(sdb)
        except Exception:
            pass
    for b in new_briefs:
        brief_tickers = set(b["tickers_affected"]) | set(b["tickers_benefit"])
        for t in brief_tickers:
            if sdb:
                try:
                    staging_touch(sdb, t, b, score_map)
                    admit = staging_admit(t, b, score_map)
                    if admit:
                        staging_promote(sdb, t)
                except Exception:
                    admit = True  # fail-open: never silence a real catalyst
            else:
                admit = True
            if not admit:
                staged += 1
                continue
            if append_wiki_note(t, b):
                wiki_notes += 1
    if sdb:
        try:
            sdb.close()
        except Exception:
            pass

    cum_briefs, uniq_tickers = regenerate_research_interest(merged, len(new_briefs))

    write_json(os.path.join(CRON, "orchestrator_output.json"), {
        "timestamp": now_iso(),
        "feed_raw_valid": True,
        "signal_briefs_valid": not errors,
        "briefs_total": len(merged), "briefs_new": len(new_briefs),
        "briefs_dropped_out_of_window": len(existing) - len(trim_briefs(existing, P_CFG.get("brief_retention_days", 14))),
        "history_total": len(hist_merged),
        "unique_tickers": len(scored),
        "validation_errors": errors,
    })

    summary = {"briefs_total": len(merged), "briefs_new": len(new_briefs),
               "unique_tickers": len(scored),
               "tier_summary": read_json(os.path.join(CRON, "vetted_tickers.json"), {}).get("tier_summary"),
               "wiki_notes_appended": wiki_notes, "wiki_staged": staged,
               "cumulative_briefs": cum_briefs,
               "mcap_gate_dropped": mcap_dropped,
               "mcap_near_ceiling": mcap_near_ceiling,
               "validation_errors": errors[:10]}
    print(json.dumps(summary, indent=1))
    print(f"[engine:process:apply] signal_briefs.json + vetted_tickers.json + wiki written ({len(new_briefs)} new briefs)")
    return 0


# ---------------------------------------------------------------- tuner
def load_tuner_windows():
    ledger = read_json(os.path.join(CRON, "query_health.json"), {})
    hist = ledger.get("history", []) if isinstance(ledger, dict) else []
    by_tag = {}
    for row in hist:
        by_tag.setdefault(row.get("query_tag"), []).append(row)
    for tag in by_tag:
        by_tag[tag] = by_tag[tag][-T_CFG["windows"]:]
    return hist, by_tag


def score_query(rows):
    import statistics
    windows = len(rows)
    fresh_shares = [ (r.get("results_fresh", 0) / r["results_total"]) if r.get("results_total") else 0.0 for r in rows ]
    med_fresh = statistics.median(fresh_shares) * 100 if windows else 0.0
    freshness = 50 if med_fresh >= 80 else (30 if med_fresh >= 50 else 10)
    med_new = statistics.median([r.get("results_new", 0) for r in rows]) if windows else 0
    novelty = 30 if med_new >= 2 else (15 if med_new == 1 else 0)
    zero_windows = sum(1 for r in rows if not r.get("results_total"))
    stability = 20 if zero_windows == 0 else (10 if zero_windows == 1 else 0)
    score = freshness + novelty + stability
    # trailing streaks
    trailing_zero = 0
    for r in reversed(rows):
        if r.get("results_total"):
            break
        trailing_zero += 1
    return score, freshness, novelty, stability, trailing_zero


def phase_tuner(force=False):
    hist, by_tag = load_tuner_windows()
    if not hist:
        print("tuner: skipped, ledger empty")
        write_json("/tmp/tuner_report.json", {"skipped": True, "reason": "ledger empty"})
        return 0
    newest = max((ts_parse(r.get("ts", "")) or 0) for r in hist)
    age_h = (time.time() - newest) / 3600
    if not force and age_h < T_CFG["gate_hours"]:
        print(f"tuner: skipped, ledger {age_h:.1f}h old")
        write_json("/tmp/tuner_report.json", {"skipped": True, "reason": f"ledger {age_h:.1f}h old"})
        return 0

    # per-query scores; group tags by query text (tags share the query they were run with)
    queries = {}
    for tag, rows in sorted(by_tag.items()):
        q = rows[-1].get("query", tag)
        e = queries.setdefault(q, {"query": q, "time_range": rows[-1].get("time_range", "week"),
                                   "tags": [], "rows": []})
        e["tags"].append(tag)
        e["rows"].extend(rows)

    report_queries = []
    flagged = []
    for q, e in queries.items():
        # score on per-query row streams (use the most recent window's rows if tag rows overlap)
        score, fresh, nov, stab, trail_zero = score_query(sorted(e["rows"], key=lambda r: r.get("ts", "")))
        if trail_zero >= 2 and score < 40:
            decision = "REWRITE"
        elif score < 40:
            decision = "ROTATION"
        elif score < 70:
            decision = "WATCH"
        else:
            decision = "OK"
        if decision in ("REWRITE", "ROTATION"):
            flagged.append(e["tags"][-1])
        report_queries.append({"query": q, "time_range": e["time_range"],
                               "tags": e["tags"], "score": score,
                               "freshness": fresh, "novelty": nov, "stability": stab,
                               "decision": decision, "trailing_zero_windows": trail_zero,
                               "windows": len(e["rows"])})
    report_queries.sort(key=lambda x: x["score"])
    report = {"skipped": False, "generated": now_iso(), "queries": report_queries,
              "flagged": flagged,
              "instructions": ("If flagged is non-empty: write /tmp/tuner_candidates.json = "
                               "{\"candidates\":{\"<tag>\":[\"candidate query 1\",\"candidate query 2\"]}} — 2 "
                               "candidates per flagged tag (same signal, different phrasing or site: target), "
                               "then run 'python3 pipeline_engine.py tuner apply'. If flagged is empty, do nothing else.")}
    write_json("/tmp/tuner_report.json", report)
    write_monitor_block(report)
    print(json.dumps({"queries_scored": len(report_queries), "flagged": flagged,
                      "weakest": report_queries[0] if report_queries else None,
                      "strongest": report_queries[-1] if report_queries else None,
                      "report_file": "/tmp/tuner_report.json"}, indent=1))
    return 0


def write_monitor_block(report, changes_lines=None):
    qs = report.get("queries", [])
    healthy = sum(1 for q in qs if q["decision"] == "OK")
    watch = sum(1 for q in qs if q["decision"] == "WATCH")
    changed = sum(1 for q in qs if q["decision"] in ("REWRITE", "ROTATION"))
    changes = "\n".join(f"- {c}" for c in changes_lines) if changes_lines else "- none"
    weakest = qs[0] if qs else None
    strongest = qs[-1] if qs else None
    block = (f"## Query Tuner — {now_iso()[:10]}\n"
             f"- Healthy: {healthy} | Watch: {watch} | Changed: {changed}\n"
             f"- Changes: {changes}\n"
             f"- Weakest query: {weakest['query'] if weakest else '-'} (score {weakest['score'] if weakest else '-'}) — {weakest['decision'] if weakest else '-'}\n"
             f"- Strongest query: {strongest['query'] if strongest else '-'} (score {strongest['score'] if strongest else '-'})\n")
    path = os.path.join(CRON, "monitor_report.md")
    old = open(path).read() if os.path.exists(path) else ""
    if report.get("skipped"):
        # no scoring this cycle: leave existing blocks untouched
        return
    blocks = old.split("## Query Tuner —")
    prior = [b for b in blocks[1:] if b.strip()]
    # if today's block exists already, replace it
    prior = [b for b in prior if not b.startswith(f" {now_iso()[:10]}")]
    with open(path, "w") as f:
        f.write(blocks[0] + "".join(f"## Query Tuner —{b}" for b in (prior + ["\n" + block.rstrip() + "\n"])[-4:]))


def phase_tuner_apply():
    report = read_json("/tmp/tuner_report.json", {})
    cand_doc = read_json("/tmp/tuner_candidates.json", {})
    flagged = report.get("flagged", [])
    cands = cand_doc.get("candidates", {})
    if not flagged:
        print("tuner apply: nothing flagged; nothing to do")
        return 0
    feed = read_json(os.path.join(CRON, "feed_raw.json"), [])
    feed_urls = {i.get("url") for i in feed if isinstance(feed, list) and isinstance(i, dict)}
    q_by_tag = {q["tag"]: q for q in S_CFG["queries"]}
    changes_lines = []
    applied = []
    now = time.time()

    def ab_score(query, tr):
        try:
            results = searxng_search(query, tr, 10)
        except Exception as e:
            print(f"[engine:tuner:apply] A/B error for {query!r}: {e}", file=sys.stderr)
            return -1
        fresh = 0
        newc = 0
        for r in results:
            pd = ts_parse(r.get("publishedDate") or "")
            if pd and now - pd < 7 * 86400:
                fresh += 1
            if r.get("url") not in feed_urls:
                newc += 1
        return fresh + newc, None

    for tag in flagged:
        if tag not in q_by_tag:
            print(f"[engine:tuner:apply] unknown tag {tag}", file=sys.stderr)
            continue
        q = q_by_tag[tag]
        options = list(cands.get(tag, []))[:2]
        if not options:
            changes_lines.append(f"{tag}: flagged {report['queries'][0]['decision'] if report.get('queries') else '?'} but no candidates provided — unchanged")
            continue
        scores = {q["query"]: ab_score(q["query"], q["time_range"])}
        old_query = q["query"]
        time.sleep(S_CFG.get("sleep_s", 2.5))
        for c in options:
            scores[c] = ab_score(c, q["time_range"])
            time.sleep(S_CFG.get("sleep_s", 2.5))
        best = max(scores.items(), key=lambda kv: kv[1][0])
        winner = best[0]
        if winner == old_query:
            changes_lines.append(f"{tag}: A/B — original wins ({best[1][0]} pts) — unchanged")
            continue
        # commit winner to config
        q["query"] = winner
        write_json(os.path.join(HERE, "pipeline_config.json"), CONFIG)
        decision = next((x["decision"] for x in report.get("queries", []) if tag in x.get("tags", [])), "ROTATION")
        row = {"ts": now_iso(), "query_old": old_query, "query_new": winner,
               "reason": "rewrite" if decision == "REWRITE" else "rotation",
               "score_old": next((x["score"] for x in report.get("queries", []) if tag in x.get("tags", [])), None),
               "ab_winner": "new", "windows_analyzed": T_CFG["windows"],
               "ab_scores": {k: v[0] for k, v in scores.items()}}
        with open(os.path.join(CRON, "query_changes.jsonl"), "a") as f:
            f.write(json.dumps(row) + "\n")
        changes_lines.append(f"{tag}: {old_query[:50]!r} -> {winner[:60]!r}")
        applied.append(winner)

    # rewrite today's monitor block with the change lines
    report["changes"] = changes_lines
    write_monitor_block(report, changes_lines=changes_lines)
    print(json.dumps({"applied": applied, "changes": changes_lines}, indent=1))
    print("[engine:tuner:apply] done")
    return 0


# ---------------------------------------------------------------- main
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: pipeline_engine.py <health|deepdive|sources|process|tuner> [apply] [force]")
        sys.exit(2)
    phase = sys.argv[1]
    sub = sys.argv[2] if len(sys.argv) > 2 else None
    force = (len(sys.argv) > 3 and sys.argv[3] == "force") or (sub == "force")
    t0 = time.time()
    if phase == "health":
        rc = phase_health()
    elif phase == "deepdive":
        rc = phase_deepdive()
    elif phase == "sources":
        rc = phase_sources_apply() if sub == "apply" else phase_sources()
    elif phase == "process":
        rc = phase_process_apply() if sub == "apply" else phase_process()
    elif phase == "tuner":
        rc = phase_tuner_apply() if sub == "apply" else phase_tuner(force=force)
    elif phase == "inclination":
        import subprocess
        rc = subprocess.run([sys.executable, os.path.join(HERE, "inclinations", "aggregate.py")]).returncode
    else:
        print(f"unknown phase: {phase}")
        rc = 2
    print(f"[engine:{phase}{':' + sub if sub else ''}] elapsed {time.time() - t0:.1f}s rc={rc}")
    sys.exit(rc)
