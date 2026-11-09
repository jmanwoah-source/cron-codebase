#!/usr/bin/env python3
"""unharnessed_swarm_recalibrator.py — Agent 3 (Thesis-Forge-Alpha) + Agent 4
(Red-Team-Inquisitor), per ~/.hermes/cron/jane_street_braid_spec.md V3 FINAL
item 2. Runs standalone in tmux (asyncio + local vLLM), outside the harness.

Pipeline (deterministic-first, LLM as classifier/auditor only):
  1. Read `alpha_vectors` from universe_cache.db (everything the ingestor
     upserted this run) + `unharnessed_scores.json` (Agent 2 pre-scores) +
     `signal_briefs.json` (catalyst context) + `feed_raw.json` (corroboration
     + contract-value detection for the EP-4 ratio).
  2. EP-4 Contract-to-Consensus: contract_value / rev_est_next_yr >= 0.05 OR
     analyst_count <= 2 -> materiality="high", confidence="high" (15/15 each),
     written into the canonical vetted-style block of ticker_scores.json.
     Cross-Source Cap: web-news-only briefs capped 15/30 unless corroborated
     by FMP_PRIMARY_WIRE / 8-K / 13F / FMP_INSIDER_SENATE / FMP_TRANSCRIPT_BREADCRUMB.
  3. Agent 3 (vLLM): per-ticker Track A/B/C classification (~350 input tokens:
     pre-computed row + top-5 catalyst briefs). Fallback: deterministic
     classifier when vLLM is down (never blocks the overnight wave).
  4. Agent 4 (vLLM + deterministic kill overrides): audit tickers scoring
     >= 75. Deterministic overrides ALWAYS win:
       - altman_z < 1.5 AND piotroski_f <= 3 -> Track C, kill_prob >= 0.75
       - cot_extreme_crowding_flag == 1 AND mfi_14 > 80 -> kill_prob += 0.20
  5. Writes (all via fmp_client.atomic_write_json):
       - ~/.hermes/cron/ticker_scores.json     (canonical; validator-shaped)
       - ~/.hermes/cron/tier_list_state.json   (lifecycle: enter/update/demote)
       - /home/jaren/wiki/companies/<T>/<T>.md (catalyst line appends, deduped)
     then triggers scripts/tier_lists.py to render the root .md tier lists.

Usage:
  python3 unharnessed_swarm_recalibrator.py --limit 5       # smoke test
  python3 unharnessed_swarm_recalibrator.py --no-vllm       # deterministic-only
  python3 unharnessed_swarm_recalibrator.py --no-tiers      # skip tier_lists.py
  python3 unharnessed_swarm_recalibrator.py --force         # rescore even if
                                                             scored < 6h ago
Exit: 0 = complete, 2 = fatal (inputs unreadable).

Crash-resumability (Guard #2): ticker_scores.json is the incremental
checkpoint — every FLUSH_EVERY (10) FRESH rows are flushed via
fmp_client.atomic_write_json mid-run, and any ticker already scored within
RESCORE_WINDOW_S (6h) is skipped (pass through from the prior canonical doc),
so a killed run resumes cleanly: fresh rows resume, prior rows are preserved.
"""
import argparse
import asyncio
import datetime
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
import urllib.request
from pathlib import Path

CRON = os.path.expanduser("~/.hermes/cron")
SCRIPTS = os.path.expanduser("~/.hermes/scripts")
# --- Mandatory path invariants (clean-slate standardization 2026-11-05) ---
WIKI_ROOT = Path("/home/jaren/wiki").resolve(strict=True)
assert not (WIKI_ROOT / "wiki").exists(), "FATAL: Nested wiki directory detected."
COMPANIES_DIR = WIKI_ROOT / "companies"
assert COMPANIES_DIR.is_dir() and not COMPANIES_DIR.is_symlink(), \
    "FATAL: companies/ missing or symlink."
# --- end invariants ---
WIKI = os.path.expanduser("~/wiki")
sys.path.insert(0, SCRIPTS)
import fmp_client  # noqa: E402
import vllm_guided  # noqa: E402  (E1.2 schema-enforced vLLM transport + ForgeClass/RedTeamAudit)

DB = os.path.join(CRON, "universe_cache.db")
SCORES_BUS = os.path.join(CRON, "unharnessed_scores.json")
BRIEFS = os.path.join(CRON, "signal_briefs.json")
FEED = os.path.join(CRON, "feed_raw.json")
CANON_SCORES = os.path.join(CRON, "ticker_scores.json")
TIER_STATE = os.path.join(CRON, "tier_list_state.json")
TIER_RENDER = os.path.join(SCRIPTS, "tier_lists.py")

VLLM_BASE = os.environ.get("VLLM_BASE", "http://localhost:8000/v1")
VLLM_MODEL = os.environ.get("VLLM_MODEL", "/model")
VLLM_TIMEOUT_S = 120
CONCURRENCY = 3

AUDIT_GATE = 75.0
GATE_UP = 75.0
GATE_DOWN = 35.0
FAIL_LIMIT = 2
HISTORY_KEEP = 20

NOW_ISO = datetime.datetime.now(datetime.timezone.utc).isoformat()
NOW_TAG = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

# Guard #2 — incremental checkpointing knobs
FLUSH_EVERY = 10                  # flush ticker_scores.json every N fresh rows
RESCORE_WINDOW_S = 6 * 3600       # skip tickers scored within the last 6 hours

CORROBORATORS = {"FMP_PRIMARY_WIRE", "FMP_8K", "FMP_13F",
                 "FMP_INSIDER_SENATE", "FMP_TRANSCRIPT_BREADCRUMB"}
CONTRACT_RE = re.compile(r"\$\s*(\d+(?:\.\d+)?)\s*(billion|million|b|mm|bn|k)?",
                         re.IGNORECASE)

# Step D (2026-11-05): Two-Stage Sieve knobs
LLM_SEM_SLOTS = 8           # concurrent vLLM dispatch ceiling (forge path)
SIEVE_MIN_HASH = 16         # state_hash length (sha256[:16])


def _safe_float(v, default=0.0):
    """Never-raise float coercion for hash payload legs (Step D safe keys).
    None / absent / non-numeric -> default. Data-gap discipline: a missing
    metric quantizes to the default bucket, never a crash."""
    f = num(v)
    return default if f is None else f


def _canonical_read(path):
    """Read a canonical bus doc with orjson (fast path) under an advisory
    exclusive flock (filelock when installed; stdlib fcntl fallback — the
    dependency is absent on this host, 2026-11-05). Corrupt/missing -> {}.
    Readers serialize here so a mid-replace read can never see a torn file
    beyond what atomic_write_json already guarantees; the lock is the belt."""
    try:
        import orjson as _oj
        _loader = _oj.loads
    except ImportError:
        _loader = lambda b: json.loads(b)
    try:
        try:
            from filelock import SoftFileLock
            with SoftFileLock(str(path) + ".lock"):
                with open(path, "rb") as fh:
                    return _loader(fh.read())
        except ImportError:
            import fcntl
            with open(path, "rb") as fh:
                fcntl.flock(fh, fcntl.LOCK_EX)
                try:
                    return _loader(fh.read())
                finally:
                    fcntl.flock(fh, fcntl.LOCK_UN)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}


def log(msg):
    print(f"[recalibrator {datetime.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def num(v):
    try:
        if v is None:
            return None
        f = float(v)
        return None if (f != f or f in (float("inf"), float("-inf"))) else f
    except (TypeError, ValueError):
        return None


def clamp(v, lo=0.0, hi=100.0):
    return max(lo, min(hi, v))


# --- Robust pillar scoring (MAD Z, 50/50 macro-pillar / ETF-cohort blend) ---
# ETF cohort -> macro pillar. DB `alpha_vectors.sector_pillar` values (verified
# 2026-11-05): SMH 1023 / SEMI_SUBSYSTEMS 100 / DEFENSE_GOV 62 /
# PHYSICAL_AI_ROBOTICS 59 / AI_INFRA_THERMAL 17 / ENERGY_GRID_SMR 11 /
# QUANTUM 7 / UNCLASSIFIED 2. The macro roll-up pools the thin cohorts
# (QUANTUM, UNCLASSIFIED) into their pillar family so the macro-leg MAD Z has
# a real sample; SMH is its own cohort AND macro pillar (both legs agree).
ETF_TO_MACRO_PILLAR = {
    "SMH": "SEMI",
    "SEMI_SUBSYSTEMS": "SEMI",
    "DEFENSE_GOV": "PHYSICAL_AI",
    "PHYSICAL_AI_ROBOTICS": "PHYSICAL_AI",
    "AI_INFRA_THERMAL": "AI_INFRA",
    "ENERGY_GRID_SMR": "ENERGY",
    "QUANTUM": "AI_INFRA",          # thin cohort (n=7) -> pool with AI infra
    "UNCLASSIFIED": "UNCLASSIFIED",
}

CORE_METRICS = ["altman_z", "piotroski_f", "blended_upside_pct",
                "dcf_upside_pct", "inst_ownership_pct"]
# NOTE: inst_ownership_pct / dcf_upside_pct are NOT columns of the current
# alpha_vectors schema — the present_cols filter below degrades gracefully
# (coverage is computed over the 3 present metrics; z_inst defaults to 0.0).


def _score_rows_stdlib(conn: sqlite3.Connection) -> list:
    """Pure-stdlib scorer: raw alpha_vectors rows + cohort legs + imputation
    + 50/50 MAD-Z blend + quant_composite_score. Same math as the pandas
    path (which recomputes imputation/z from these imputed values); this
    version exists so scoring works under any interpreter (system python3,
    cron agent venv, bare CPython) with zero package dependencies.
    composite = within-macro-pillar percentile rank * 100 * coverage damp."""
    import statistics
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute("SELECT * FROM alpha_vectors")]
    if not rows:
        return rows
    present_cols = [c for c in CORE_METRICS if c in rows[0]]
    for row in rows:
        for col in present_cols:
            row[col] = num(row.get(col))
        raw = (str(row.get("sector_pillar") or "").upper().strip()
               or "UNCLASSIFIED")
        row["raw_cohort"] = raw
        row["macro_pillar"] = ETF_TO_MACRO_PILLAR.get(raw, raw)
        n_present = sum(1 for c in present_cols if row.get(c) is not None)
        row["data_coverage"] = (n_present / len(present_cols)) if present_cols else 1.0

    def group_med(col, key):
        out = {}
        buckets = {}
        for row in rows:
            buckets.setdefault(row[key], []).append(row.get(col))
        for k, vals in buckets.items():
            vals = [v for v in vals if v is not None]
            out[k] = (statistics.median(vals) if vals else 0.0)
        return out

    # Impute: ETF cohort median -> macro pillar median -> 0.0
    for col in present_cols:
        coh_med = group_med(col, "raw_cohort")
        mac_med = group_med(col, "macro_pillar")
        for row in rows:
            v = row.get(col)
            if v is None:
                v = coh_med.get(row["raw_cohort"])
            if v is None:
                v = mac_med.get(row["macro_pillar"])
            row[col] = 0.0 if v is None else v

    # group (median, MAD) precompute per (key, col)
    def group_med_mad(col, key):
        buckets = {}
        for row in rows:
            buckets.setdefault(row[key], []).append(row.get(col))
        out = {}
        for k, vals in buckets.items():
            vals = [v for v in vals if v is not None]
            if len(vals) < 3:
                out[k] = (0.0, 0.0)
            else:
                med = statistics.median(vals)
                mad = statistics.median([abs(v - med) for v in vals])
                out[k] = (0.0, 0.0) if mad < 1e-6 else (med, mad)
        return out

    z_legs = [("blended_upside_pct", "z_upside"), ("altman_z", "z_altman"),
              ("piotroski_f", "z_piotroski"), ("inst_ownership_pct", "z_inst")]
    for metric, z_col in z_legs:
        if metric not in rows[0]:
            for row in rows:
                row[z_col] = 0.0
            continue
        m_coh = group_med_mad(metric, "raw_cohort")
        m_mac = group_med_mad(metric, "macro_pillar")
        for row in rows:
            v = row[metric]
            def z_of(v, m, mad):
                return 0.0 if mad <= 0 else max(-3.0, min(3.0, 0.6745 * (v - m) / mad))
            z_coh = z_of(v, *m_coh.get(row["raw_cohort"], (0.0, 0.0)))
            z_mac = z_of(v, *m_mac.get(row["macro_pillar"], (0.0, 0.0)))
            row[z_col] = 0.50 * z_mac + 0.50 * z_coh

    # composite: within-macro-pillar percentile rank * 100 * coverage damping
    for row in rows:
        row["_raw_composite"] = (0.35 * row["z_upside"] + 0.25 * row["z_piotroski"]
                                 + 0.20 * row["z_altman"] + 0.20 * row["z_inst"])
    for mp in {row["macro_pillar"] for row in rows}:
        members = sorted((row for row in rows if row["macro_pillar"] == mp),
                         key=lambda r: r["_raw_composite"])
        n = len(members)
        for i, row in enumerate(members):
            rank = (i + 0.5) / n if n else 0.5   # mid-rank percentile
            row["quant_composite_score"] = round(
                rank * 100.0 * (0.85 + 0.15 * row["data_coverage"]), 2)
    for row in rows:
        row.pop("_raw_composite", None)
    return rows


def compute_robust_pillar_scores(conn: sqlite3.Connection):
    """Loads the full alpha_vectors table.
    Imputes missing values with cohort median.
    Computes 50/50 Macro Pillar and ETF Cohort MAD Robust Z-Scores.

    ALWAYS returns a list of row dicts (full schema + computed legs:
    raw_cohort, macro_pillar, data_coverage, z_upside, z_altman,
    z_piotroski, z_inst, quant_composite_score). pandas is used when
    importable (DataFrame path); otherwise a pure-stdlib fallback
    reproduces the same math so the scoring never depends on the
    interpreter's package set.
    """
    rows = _score_rows_stdlib(conn)
    try:
        import pandas as pd_local
    except ImportError:
        return rows
    pd = pd_local
    df = pd.DataFrame(rows)

    # schema fix: column is sector_pillar in alpha_vectors (no `pillar` col)
    df["raw_cohort"] = df["sector_pillar"].fillna("UNCLASSIFIED").astype(str).str.upper().str.strip()
    df["macro_pillar"] = df["raw_cohort"].map(lambda x: ETF_TO_MACRO_PILLAR.get(x, x))

    core_metrics = CORE_METRICS
    present_cols = [c for c in core_metrics if c in df.columns]
    for col in present_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["data_coverage"] = df[present_cols].notna().mean(axis=1) if present_cols else 1.0

    # Impute missing values: ETF cohort median -> macro pillar median -> 0.0
    for col in present_cols:
        df[col] = df.groupby("raw_cohort")[col].transform(lambda s: s.fillna(s.median()))
        df[col] = df.groupby("macro_pillar")[col].transform(lambda s: s.fillna(s.median())).fillna(0.0)

    # Median Absolute Deviation (MAD) Robust Z-Score clipped to [-3.0, +3.0]
    def robust_z(series: pd.Series) -> pd.Series:
        if len(series) < 3:
            return pd.Series(0.0, index=series.index)
        med = series.median()
        mad = (series - med).abs().median()
        if mad < 1e-6:
            return pd.Series(0.0, index=series.index)
        return (0.6745 * (series - med) / mad).clip(-3.0, 3.0)

    for metric, z_col in [
        ("blended_upside_pct", "z_upside"),
        ("altman_z", "z_altman"),
        ("piotroski_f", "z_piotroski"),
        ("inst_ownership_pct", "z_inst")
    ]:
        if metric in df.columns:
            z_macro = df.groupby("macro_pillar")[metric].transform(robust_z)
            z_cohort = df.groupby("raw_cohort")[metric].transform(robust_z)
            df[z_col] = 0.50 * z_macro + 0.50 * z_cohort
        else:
            df[z_col] = 0.0

    raw_composite = (
        0.35 * df["z_upside"] +
        0.25 * df["z_piotroski"] +
        0.20 * df["z_altman"] +
        0.20 * df["z_inst"]
    )
    df["quant_composite_score"] = (
        raw_composite.groupby(df["macro_pillar"]).rank(pct=True) * 100.0 * (0.85 + 0.15 * df["data_coverage"])
    ).round(2)

    return df


def load_json(path, default):
    try:
        with open(path) as fh:
            return json.load(fh)
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------
def load_inputs(limit):
    """Returns (rows, extras, prior_doc). prior_doc is the existing canonical
    ticker_scores.json ({rankings: [...], generated: ...}) or {} — used for
    the 6h skip gate and for preserving skipped rows in checkpoint flushes."""
    vec_db = load_json(SCORES_BUS, {})
    rows = vec_db.get("rows", []) if isinstance(vec_db, dict) else []
    # Step D: canonical ticker_scores.json read via orjson + filelock
    # (flock-fallback reader; torn-read immune)
    prior_doc = _canonical_read(CANON_SCORES)
    if not rows:
        # fallback: straight from the DB for anything the ingestor wrote —
        # through the robust MAD-Z scorer (replaces the raw
        # `SELECT * FROM alpha_vectors ORDER BY last_updated DESC` query):
        # cohort-median imputation + 50/50 macro-pillar / ETF-cohort
        # robust Z-blend + quant_composite_score (NaN-last ordering preserved)
        conn = sqlite3.connect(DB, timeout=5)
        scored = compute_robust_pillar_scores(conn)
        conn.close()
        if hasattr(scored, "to_dict"):
            scored = scored.sort_values("last_updated", ascending=False,
                                        na_position="last")
            rows = scored.to_dict("records")
        else:
            rows = list(scored)  # no-pandas degradation: raw row dicts
    else:
        # Step D: the bus rows predate the MAD-Z scorer — re-score the full
        # table and merge the computed legs onto the bus rows by ticker.
        # (Bus rows are already in last_updated order, so this does not
        # change ordering; it only adds the scoring columns.)
        conn = sqlite3.connect(DB, timeout=5)
        scored = compute_robust_pillar_scores(conn)
        conn.close()
        if hasattr(scored, "to_dict"):
            by_tk = {r["ticker"]: r for r in scored.to_dict("records")}
            for row in rows:
                s = by_tk.get(row.get("ticker"))
                if s:
                    row.update({k: v for k, v in s.items() if v is not None})
        else:
            by_tk = {r["ticker"]: r for r in scored}
            for row in rows:
                s = by_tk.get(row.get("ticker"))
                if s:
                    row.update(s)
    if not rows:
        return None, None, prior_doc
    rows = rows[:limit]
    briefs = (load_json(BRIEFS, {}) or {}).get("signal_briefs", [])
    brief_map = {}
    for b in briefs:
        for k in ("tickers_affected", "tickers_benefit", "tickers_mentioned"):
            for t in b.get(k) or []:
                brief_map.setdefault(t, []).append(b)
    feed = (load_json(FEED, {}) or {}).get("entries", [])
    return rows, (brief_map, feed), prior_doc


def scored_recent_map(prior_doc, now=None):
    """{ticker: (age_s, entry)} for rows in a prior canonical doc, newest first.
    A missing/empty generated timestamp is treated as infinitely old (never
    skip a doc we can't date)."""
    now = now or time.time()
    ts = None
    try:
        ts = datetime.datetime.fromisoformat(str(prior_doc.get("generated", ""))
                                             .replace("Z", "+00:00")).timestamp()
    except Exception:
        ts = 0.0
    out = {}
    for r in (prior_doc.get("rankings") or []):
        if not isinstance(r, dict) or not r.get("ticker"):
            continue
        out[r["ticker"]] = (max(0.0, now - ts), r)
    return out


def contract_value_for(tk, feed_entries, cutoff=14 * 86400):
    """Largest contract/award amount in recent wire+insider feed items for tk.
    Returns None when no contract-shaped item exists (no guessing)."""
    import datetime as _dt
    newest = time.time() - cutoff
    best = None
    for e in feed_entries:
        if not isinstance(e, dict) or e.get("ticker") != tk:
            continue
        ts = None
        try:
            ts = _dt.datetime.fromisoformat(str(e.get("ts") or e.get("date") or NOW_ISO)
                                            .replace("Z", "+00:00")).timestamp()
        except Exception:
            ts = time.time()
        if ts < newest:
            continue
        text = f"{e.get('headline', '')}"
        if not re.search(r"contract|award|order|grant|allocation|selected|win", text, re.I):
            continue
        for m in CONTRACT_RE.finditer(text):
            amt = num(m.group(1))
            if amt is None:
                continue
            mult = {"billion": 1e9, "b": 1e9, "bn": 1e9, "million": 1e6,
                    "mm": 1e6, "m": 1e6, "k": 1e3}.get(
                (m.group(2) or "").lower().rstrip("."), 1.0)
            val = amt * mult
            if best is None or val > best:
                best = val
    return best


# ---------------------------------------------------------------------------
# EP-4 Contract-to-Consensus + cross-source cap
# ---------------------------------------------------------------------------
def ep4_row(row, briefs, contract_val):
    rev = num(row.get("rev_est_next_yr"))
    ac = row.get("analyst_count")
    ratio = (contract_val / rev) if (contract_val and rev) else None
    forced = (ratio is not None and ratio >= 0.05) or (ac is not None and ac <= 2)
    corrob = any((b.get("source_category") or b.get("source")) in CORROBORATORS
                 or "8k" in str(b.get("source_category", "")).lower()
                 for b in briefs)
    web_only = len(briefs) > 0 and not corrob
    return {
        "materiality": "high" if forced else ("direct" if any(b.get("materiality") == "direct" for b in briefs) else "sector"),
        "confidence": "high" if (forced or corrob) else ("medium" if briefs else "low"),
        "materiality_points": 15 if (forced or corrob) else (10 if briefs else 0),
        "confidence_points": 15 if (forced or corrob) else (10 if briefs else 0),
        "contract_value": contract_val,
        "contract_to_consensus_ratio": round(ratio, 4) if ratio is not None else None,
        "ep4_forced_high": bool(forced),
        "web_news_capped": bool(web_only),   # cross-source cap 15/30 active
        "corroborated": bool(corrob),
    }


# ---------------------------------------------------------------------------
# Track math (deterministic; LLM classifies, math binds)
# ---------------------------------------------------------------------------
def pillar_scores(row):
    p = row.get("pillar_scores_100") or {}
    return {k: num(p.get(k)) for k in ("financial", "valuation", "technical", "catalyst")}


def track_scores(ps):
    """Track A = Fortress blend (financial+valuation), Track B = Edge blend
    (technical+catalyst), Track C = 100 - max(A,B) when nothing passes."""
    fin, val, tech, cat = ps["financial"], ps["valuation"], ps["technical"], ps["catalyst"]
    a = clamp((fin + val) / 2.0) if (fin is not None and val is not None) else \
        (fin if fin is not None else None)
    b = clamp((tech + cat) / 2.0) if (tech is not None and cat is not None) else \
        (tech if tech is not None else None)
    if a is None and b is None:
        return None, None, None
    best = max(x for x in (a, b) if x is not None)
    c = clamp(100.0 - best)
    return (a if a is not None else 0.0), (b if b is not None else 0.0), c


def compute_state_hash(row: dict, regime_id: str) -> str:
    """Computes a quantized SHA-256 hash to detect real fundamental shifts.
    Guard #1: every field uses safe dict.get with an empty default —
    alpha_vectors has NO latest_transcript_date / latest_13f_date columns
    (verified 2026-11-05); missing keys hash as '' and never raise."""
    payload = (
        f"{row.get('ticker', '')}|{regime_id}|"
        f"{round(_safe_float(row.get('quant_composite_score'), 0.0), 0)}|"
        f"{round(_safe_float(row.get('blended_upside_pct'), 0.0) / 2.0) * 2}|"
        f"{round(_safe_float(row.get('piotroski_f'), 0.0), 0)}|"
        f"{row.get('latest_transcript_date', '')}|"
        f"{row.get('latest_13f_date', '')}"
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def should_dispatch_to_vllm(
    quant_score: float,
    new_hash: str,
    prev_record: dict | None,
    force_recalibrate: bool = False
) -> bool:
    """Two-Stage Sieve: Only calls LLM if ticker is active, in top/bottom tier, or score moved >= 4.0."""
    if force_recalibrate or not prev_record:
        return True
    prev_tier = prev_record.get("tier", "NEUTRAL")
    if prev_tier in ("GOLDEN", "EMERGING_EDGE", "MIRAGE_SHORT"):
        return new_hash != prev_record.get("state_hash")
    if quant_score >= 80.0 or quant_score <= 15.0:
        return True
    prev_quant = float(prev_record.get("quant_composite_score", 50.0))
    if new_hash != prev_record.get("state_hash") and abs(quant_score - prev_quant) >= 4.0:
        return True
    return False


def apply_hysteresis(new_score: float, prev_tier: str, is_short_candidate: bool = False) -> str:
    """5-point buffer zone to prevent tier flip-flopping across 6-hour runs."""
    if prev_tier == "GOLDEN" and new_score >= 75.0:
        return "GOLDEN"
    if new_score >= 80.0:
        return "GOLDEN"

    if prev_tier == "EMERGING_EDGE" and 62.0 <= new_score < 80.0:
        return "EMERGING_EDGE"
    if 67.0 <= new_score < 80.0:
        return "EMERGING_EDGE"

    if is_short_candidate or new_score <= 20.0 or (prev_tier == "MIRAGE_SHORT" and new_score <= 25.0):
        return "MIRAGE_SHORT"

    return "NEUTRAL"


# ---------------------------------------------------------------------------
# vLLM client (E1.2): the schema-enforced transport now lives in
# scripts/vllm_guided.py (vllm_guided_chat / vllm_guided_chat_async). The old
# free-form async vllm_chat() that callers regex-scanned for a JSON blob has been
# REMOVED — forge/audit now get grammar-constrained output validated to
# vllm_guided.ForgeClass / RedTeamAudit. Only the up/down health probe remains
# local (it gates the deterministic fallback, not generation).
# ---------------------------------------------------------------------------
async def vllm_health():
    loop = asyncio.get_running_loop()
    def do():
        with urllib.request.urlopen(VLLM_BASE.replace("/v1", "") + "/health", timeout=5) as r:
            return r.status == 200
    try:
        return await loop.run_in_executor(None, do)
    except Exception:
        return False


def format_row_ctx(row, briefs):
    """~350-token input per spec: pre-computed row + top-5 catalyst briefs."""
    ps = pillar_scores(row)
    lines = [
        f"{row.get('ticker')} | {row.get('company_name')} | pillar {row.get('sector_pillar')}",
        f"mcap ${num(row.get('mcap'))/1e9 if num(row.get('mcap')) else 0:.1f}B price {row.get('price')}",
        f"altman_z {row.get('altman_z')} piotroski {row.get('piotroski_f')} "
        f"mfi {row.get('mfi_14')} adx {row.get('adx_14')} float {row.get('float_shares')}",
        f"upside {row.get('blended_upside_pct')}% analyst_count {row.get('analyst_count')}",
        f"pillar scores: fin {ps['financial']} val {ps['valuation']} tech {ps['technical']} cat {ps['catalyst']}",
    ]
    top = briefs[:5]
    if top:
        lines.append("top briefs:")
        for b in top:
            lines.append(f"- [{(b.get('date_created') or '')[:10]}] "
                         f"{(b.get('headline') or '')[:90]} ({b.get('impact_direction')} "
                         f"{b.get('impact_level')})")
    return "\n".join(lines)


FORGE_PROMPT = """You are Thesis-Forge-Alpha, a quant thesis classifier.
Given the pre-computed data row and catalyst briefs, classify the ticker into
exactly ONE track:
A = Fundamental Fortress (strong balance sheet + valuation support),
B = Catalyst Edge (catalyst/technical momentum outweighs fundamentals),
C = Mirage/Short (weak fundamentals, hype, or structural breakdown).
Respond with ONLY JSON: {"track":"A|B|C","rationale":"<=25 words"}"""

AUDIT_PROMPT = """You are Red-Team-Inquisitor, an adversarial thesis auditor.
This ticker scores >= 75. Find the strongest reason the thesis is a MIRAGE
(dilution, customer concentration, hype without cash flow, regime crowding).
Respond with ONLY JSON: {"kill_add":<number 0 to 0.3>,
"reason":"<=20 words"} — kill_add is the extra kill probability you assert
beyond the deterministic floor."""


# Step D — static system prompt prefix (Jaren's 4 Persona Pillars embedded).
# Static ON PURPOSE: the prefix is byte-identical on every dispatch so vLLM's
# Radix prefix cache hits — only the per-ticker user message varies.
PIGGY_PROMPT = """You are Thesis-Forge-Alpha, running on the Lou swarm for Jaren.
You score tickers against Jaren's 4 Persona Pillars (the deterministic math
behind each pillar is already computed for you — judge the THESIS, don't recompute):
PILLAR 1 FINANCIAL: Piotroski F-Score strength (min(72, F*8)), Altman Z-Score
survivability (>3.0 fortress; <1.5 with F<=3 is a balance-sheet kill), and
Trojan-segment concentration flags (hidden revenue dependency).
PILLAR 2 VALUATION: blended upside = 0.5*levered-DCF + 0.5*consensus price
target vs last close (<=0% weak, +15% adequate, >=+35% strong).
PILLAR 3 TECHNICAL: money-flow regime (MFI-14 > 55 = accumulation, >80 =
exhaustion), ADX-14 trend with MACD histogram slope, 20d relative strength vs
the pillar's benchmark ETF, low-float squeeze capacity (float < 50M shares).
PILLAR 4 CATALYST: 14-day corroborated brief flow (web news alone is capped
at 15/30), 30d EPS revision momentum, and 13F/insider/Senate accumulation
clusters.
Given the pre-computed data row and catalyst briefs, classify the ticker into
exactly ONE track:
A = Fundamental Fortress (Pillars 1+2 dominate),
B = Catalyst Edge (Pillars 3+4 outweigh fundamentals),
C = Mirage/Short (fundamental breakdown, hype, or structural dilution risk).
Respond with ONLY JSON: {"track":"A|B|C","rationale":"<=25 words"}"""


async def forge_classify(sem, row, briefs, vllm_ok, system_prompt=FORGE_PROMPT):
    if not vllm_ok:
        # deterministic fallback: never blocks the wave
        ps = pillar_scores(row)
        a, b, _ = track_scores(ps)
        if a is not None and b is not None:
            t = "A" if a >= b else "B"
        elif a is not None:
            t = "A"
        else:
            t = "B" if (b or 0) >= 50 else "C"
        return t, "deterministic-fallback (vLLM down)"
    async with sem:
        try:
            # E1.2: schema-enforced forge classification (ForgeClass). The grammar
            # constrains track to A|B|C and rationale to a string — no regex scan.
            out = await vllm_guided.vllm_guided_chat_async(
                [{"role": "system", "content": system_prompt},
                 {"role": "user", "content": format_row_ctx(row, briefs)}],
                schema=vllm_guided.ForgeClass, max_tokens=300)
            return out.track, str(out.rationale)[:200]
        except Exception as e:
            log(f"  forge {row.get('ticker')} vLLM err {type(e).__name__}: {e}")
            ps = pillar_scores(row)
            a, b, _ = track_scores(ps)
            t = ("A" if a >= b else "B") if (a is not None and b is not None) else \
                ("A" if a is not None else "B")
            return t, f"fallback ({type(e).__name__})"


async def audit_ticker(sem, row, kill_prob, reasons, vllm_ok):
    if not vllm_ok:
        return kill_prob, reasons
    async with sem:
        try:
            # E1.2: schema-enforced red-team audit (RedTeamAudit): kill_add in
            # [0,0.3], reason a string. Out-of-range kill_add is clamped at decode
            # time — the old re.search + manual clamp is gone.
            out = await vllm_guided.vllm_guided_chat_async(
                [{"role": "system", "content": AUDIT_PROMPT},
                 {"role": "user", "content": format_row_ctx(row, []) +
                                      f"\nkill_prob_floor {kill_prob:.2f}"}],
                schema=vllm_guided.RedTeamAudit, max_tokens=160)
            add = clamp(num(out.kill_add) or 0.0, 0.0, 0.3)
            if add > 0:
                kill_prob = min(0.99, kill_prob + add)
                reasons.append(f"redteam +{add:.2f}: {str(out.reason)[:120]}")
        except Exception as e:
            log(f"  audit {row.get('ticker')} vLLM err {type(e).__name__}")
    return kill_prob, reasons


# ---------------------------------------------------------------------------
# Kill overrides (deterministic — spec Agent 4)
# ---------------------------------------------------------------------------
def kill_overrides(row, crowding):
    """Deterministic kill checks. Guard #1: every input is None-safe —
    None/absent crowding and string-typed fields must never raise; they can
    only ever ADD a kill, a missing field can never false-trigger one."""
    az = num(row.get("altman_z"))
    pf = row.get("piotroski_f")
    pf = int(pf) if isinstance(pf, (int, float)) and not isinstance(pf, bool) else None
    mfi = num(row.get("mfi_14"))
    crowding = int(crowding) if isinstance(crowding, (int, float)) and not isinstance(crowding, bool) else None
    kill, reasons, forced_c = 0.0, [], False
    if az is not None and az < 1.5 and pf is not None and pf <= 3:
        kill, forced_c = max(kill, 0.75), True
        reasons.append("Balance sheet distress / dilution risk")
    if crowding == 1 and mfi is not None and mfi > 80:
        kill = min(0.99, kill + 0.20)
        reasons.append("COT extreme crowding + exhaustion")
    return kill, reasons, forced_c


# ---------------------------------------------------------------------------
# Wiki catalyst line append (AGENTS.md rule 4: dedupe on normalized line,
# 140-char prefix; never reformat the block)
# ---------------------------------------------------------------------------
def wiki_append(ticker, briefs):
    path = os.path.join(WIKI, "companies", ticker, f"{ticker}.md")
    if not os.path.isfile(path):
        return False, "no dossier"
    with open(path) as fh:
        text = fh.read()
    added = 0
    for b in briefs[:10]:
        bid = str(b.get("brief_id") or "")[:8].upper()
        if not bid:
            continue
        date = (b.get("date_created") or "")[:10]
        headline = re.sub(r"\s+", " ", str(b.get("headline") or "")).strip()[:140]
        line = f"- [{date}] {bid} {headline} | {b.get('category', 'uncategorized')} " \
               f"({b.get('impact_direction', 'neutral')} {b.get('impact_level', 'low')})"
        norm = re.sub(r"\[\d{4}-\d\d-\d\d\]\s*", "[date] ", line)[:140]
        if line in text or bid in text:
            continue
        if "## Catalyst Log" in text:
            text = text.replace("## Catalyst Log", "## Catalyst Log\n" + line, 1)
        else:
            text += f"\n## Catalyst Log\n{line}\n"
        added += 1
    if added:
        # dossier is markdown — same atomic pattern (tmp + fsync + os.replace)
        import tempfile
        d = os.path.dirname(path)
        payload = text.encode("utf-8")
        tf = tempfile.NamedTemporaryFile(dir=d, prefix=".wiki.", suffix=".tmp", delete=False)
        try:
            with tf:
                tf.write(payload)
                tf.flush()
                os.fsync(tf.fileno())
            os.replace(tf.name, path)
        except BaseException:
            try:
                os.unlink(tf.name)
            except OSError:
                pass
            raise
    return True, f"+{added} catalyst line(s)"


# ---------------------------------------------------------------------------
# Tier list state (lifecycle semantics mirror tier_lists.py)
# ---------------------------------------------------------------------------
def apply_tier_state(state, tk, name, mcap, track, score, evidence):
    if track is None or score is None:
        return  # did not qualify for any list this run
    lists = state.setdefault("lists", {"A": {}, "B": {}, "C": {}})
    for lk in ("A", "B", "C"):
        lists.setdefault(lk, {})
    target = lists[track]
    ok = (score >= GATE_UP) if track in ("A", "B") else (score <= GATE_DOWN)
    if track == "C" and not ok:
        for lk in ("A", "B", "C"):
            e = lists[lk].pop(tk, None)  # recovered from mirage: leave all
        return
    if ok and tk not in target:
        target[tk] = {
            "name": name, "market_cap": mcap, "score": score,
            "engine": {}, "evidence": evidence[:6], "entered": NOW_TAG,
            "updated": NOW_TAG, "fail_streak": 0, "history": [],
        }
        return
    if tk not in target:
        return
    e = target[tk]
    e["history"] = (e.get("history") or []) + [f"{NOW_TAG} score={score}"]
    e["history"] = e["history"][-HISTORY_KEEP:]
    e["updated"] = NOW_TAG
    if evidence:
        e["evidence"] = evidence[:6]
    if mcap is not None:
        e["market_cap"] = mcap
    if ok:
        e["fail_streak"] = 0
        e["score"] = score
    else:
        e["fail_streak"] = int(e.get("fail_streak", 0)) + 1
        if e["fail_streak"] >= FAIL_LIMIT:
            del target[tk]
            return
        e["score"] = score


# ---------------------------------------------------------------------------
async def main_async(args):
    rows, extras, prior_doc = load_inputs(args.limit)
    if rows is None:
        log("FATAL: no rows in unharnessed_scores.json / alpha_vectors — ingestor has not run")
        return 2
    brief_map, feed = extras
    conn = sqlite3.connect(DB, timeout=5)
    conn.row_factory = sqlite3.Row
    crowd = {r["pillar"]: r["cot_extreme_crowding_flag"]
             for r in conn.execute("SELECT pillar, cot_extreme_crowding_flag FROM regime_state")}
    conn.close()

    vllm_ok = not args.no_vllm and await vllm_health()

    # ---- Guard #2: 6h skip gate + incremental checkpoint state ----
    recent = scored_recent_map(prior_doc)
    skipped_entries = []            # prior entries passed through (skipped)
    fresh_rows = []
    skipped_tks = []
    for row in rows:
        tk = row["ticker"]
        if not args.force and tk in recent:
            age_s, prior_entry = recent[tk]
            if age_s < RESCORE_WINDOW_S:
                e = dict(prior_entry)
                e["checkpoint"] = "skip-recent"     # audit tag: carried from prior run
                e["skipped_age_h"] = round(age_s / 3600.0, 2)
                skipped_entries.append(e)
                skipped_tks.append(tk)
                continue
        fresh_rows.append(row)
    if skipped_tks:
        log(f"Guard #2: skipping {len(skipped_tks)} ticker(s) scored < 6h ago "
            f"{'(override: --force)' if args.force else ''}: {skipped_tks[:12]}")
    log(f"vLLM {'UP' if vllm_ok else 'DOWN — deterministic fallback mode'}; "
        f"{len(fresh_rows)} fresh rows ({len(skipped_entries)} carried from checkpoint)")
    sem = asyncio.Semaphore(CONCURRENCY)

    # ---- Step D: Two-Stage Sieve state (per-ticker cache from prior doc) ----
    sieve = {}
    for r in prior_doc.get("rankings") or []:
        if isinstance(r, dict) and r.get("ticker") and r.get("state_hash"):
            sieve[r["ticker"]] = r

    # checkpoint doc shared by every flush — skipped rows stay in it so a
    # killed run resumes with prior scores intact
    checkpoint_doc = {
        "generated": NOW_ISO,
        "engine_session": f"unharnessed-{int(time.time())}",
        "agent": "Thesis-Forge-Alpha + Red-Team-Inquisitor (unharnessed swarm recalibrator)",
        "briefing_tickers_fetched": len(rows),
        "vllm_live": vllm_ok,
        "dry_run": bool(getattr(args, "dry_run", False)),
        "incremental_checkpoint": True,
        "sieve": {
            "version": "1.0",
            "cache_hits": 0,
            "dispatches": 0,
            "llm_sem_slots": LLM_SEM_SLOTS,
        },
        "rankings": skipped_entries,
        "invalid_tickers_flagged": [],
    }
    _ckpt = {"n_flushes": 0}

    def flush_checkpoint(rankings, final=False):
        """Guard #2 flush: atomic write of the running canonical doc. Skipped
        (recent) entries are always preserved; fresh entries appended as they
        complete. tier_list_state.json is flushed on the SAME cadence
        (non-dry-run) so tier assignments also survive a kill. --dry-run
        writes to the probe target, never canonical/tier files.
        Step D: canonical writes serialize via orjson (fmp_client writer is
        the stdlib fallback) and stamp sieve cache-hit/dispatch counters.
        Step D merge semantics: prior-run rankings stay in the canonical doc
        (the sieve cache); this run's fresh/skipped entries UPDATE their
        tickers in place — a --limit 20 run never drops the other 98 rows."""
        doc = dict(checkpoint_doc)
        merged = {r["ticker"]: r for r in (prior_doc.get("rankings") or [])
                  if isinstance(r, dict) and r.get("ticker")}
        for r in skipped_entries + rankings:
            if r.get("ticker"):
                merged[r["ticker"]] = r
        doc["rankings"] = sorted(merged.values(),
                                 key=lambda r: -(r.get("best_track_score") or 0))
        doc["invalid_tickers_flagged"] = [r["ticker"] for r in doc["rankings"]
                                          if (r.get("data_quality_score") or 0) < 40]
        doc["checkpoint_complete"] = bool(final)
        target = (os.path.join(CRON, ".smoke_ticker_scores_probe.json") if args.dry_run
                  else CANON_SCORES)
        try:
            import orjson as _oj
            payload = _oj.dumps(doc)
        except ImportError:
            payload = None
        if payload is None:
            fmp_client.atomic_write_json(target, doc)
        else:
            import tempfile
            d = os.path.dirname(target)
            tf = tempfile.NamedTemporaryFile(dir=d, prefix=".bus.", suffix=".tmp",
                                             delete=False)
            try:
                with tf:
                    tf.write(payload)
                    tf.flush()
                    os.fsync(tf.fileno())
                os.replace(tf.name, target)
            except BaseException:
                try:
                    os.unlink(tf.name)
                except OSError:
                    pass
                raise
        if not args.dry_run:
            TIER_STATE_HOLDER["state"]["updated"] = NOW_TAG
            fmp_client.atomic_write_json(TIER_STATE, TIER_STATE_HOLDER["state"])
        _ckpt["n_flushes"] += 1
        tag = "FINAL" if final else f"flush #{_ckpt['n_flushes']} (every {FLUSH_EVERY} fresh)"
        log(f"checkpoint {tag}: {len(doc['rankings'])} rows -> {os.path.basename(target)} (atomic OK)")

    # ---- Agent 3: classify (parallel) — Step D sieve-gated dispatch ----
    llm_sem = asyncio.Semaphore(LLM_SEM_SLOTS)   # vLLM dispatch ceiling (8)
    dispatched = []
    sieve_hits = []

    async def do_forge(row):
        tk = row["ticker"]
        quant = _safe_float(row.get("quant_composite_score"), 0.0)
        # regime key: prefer the ticker's ETF cohort, else the MACRO row,
        # else UNCLASSIFIED (regime_state rows: SMH/URA/BOTZ/DEFENSE/MACRO/
        # the 6 canonical pillar names)
        regime_id = row.get("sector_pillar") or "UNCLASSIFIED"
        state_hash = compute_state_hash(row, regime_id)
        prev = sieve.get(tk)
        if should_dispatch_to_vllm(quant, state_hash, prev, args.force):
            dispatched.append(tk)
            t, why = await forge_classify(llm_sem, row, brief_map.get(tk, []), vllm_ok,
                                          system_prompt=PIGGY_PROMPT)
            row["_checkpoint"] = "fresh"
        elif prev is not None:
            # Sieve cache hit: carry forward the cached score/thesis verbatim
            t = prev.get("assigned_track", "C")
            why = f"sieve-cache: hash {state_hash[:8]} unchanged since " \
                  f"{str(prev.get('state_hash'))[:8]} (carried {prev.get('best_track_score')})"
            row["_checkpoint"] = "sieve-cache"
            sieve_hits.append(tk)
        else:
            dispatched.append(tk)
            t, why = await forge_classify(llm_sem, row, brief_map.get(tk, []), vllm_ok,
                                          system_prompt=PIGGY_PROMPT)
            row["_checkpoint"] = "fresh"
        row["_track"], row["_why"] = t, why
        row["_quant"], row["_state_hash"] = quant, state_hash
        row["_sieve_prev_tier"] = (prev or {}).get("tier", "NEUTRAL")
    await asyncio.gather(*[do_forge(r) for r in fresh_rows])
    if fresh_rows:
        log(f"Agent 3 (Thesis-Forge-Alpha): classifications "
            f"{ {t: sum(1 for r in fresh_rows if r['_track'] == t) for t in 'ABC'} } | "
            f"LLM dispatch {len(dispatched)} / sieve cache hit {len(sieve_hits)} "
            f"(LLM_SEM={LLM_SEM_SLOTS})")
    checkpoint_doc["sieve"]["cache_hits"] = len(sieve_hits)
    checkpoint_doc["sieve"]["dispatches"] = len(dispatched)

    # ---- EP-4 + scoring + Agent 4 ----
    rankings = []
    for i, row in enumerate(fresh_rows, 1):
        tk = row["ticker"]
        ps = pillar_scores(row)
        a, b, c = track_scores(ps)
        track, why = row["_track"], row["_why"]
        crowding = crowd.get(row.get("sector_pillar"), 0)
        kill, kreasons, forced_c = kill_overrides(row, crowding)

        if forced_c:
            # Deterministic kill override is unconditional (spec Agent 4): it
            # forces Track C regardless of the >=75 audit gate.
            track, kill = "C", max(kill, 0.75)
            why = "DETERMINISTIC KILL: " + why

        best = max(x for x in (a, b) if x is not None) if any(x is not None for x in (a, b)) else 0.0
        # a/b None (no scoreable pillars — e.g. PALX-shaped all-NULL row) =>
        # best 0.0: data-gap rows sink to the bottom and can never cross the
        # 75 audit gate; track stays whatever Agent 3 classified (C fallback).
        audit_kill, audit_reasons = 0.0, []
        if best >= AUDIT_GATE:
            audit_kill, audit_reasons = await audit_ticker(
                sem, row, kill, list(kreasons), vllm_ok)

        contract_val = contract_value_for(tk, feed)
        ep4 = ep4_row(row, brief_map.get(tk, []), contract_val)
        briefs = brief_map.get(tk, [])
        evidence = [why[:120]]
        if kreasons:
            evidence.extend(kreasons[:3])
        for bl in briefs[:3]:
            evidence.append(f"[{(bl.get('date_created') or '')[:10]}] "
                            f"{str(bl.get('headline', ''))[:110]}")
        # cross-source cap: web-only briefs contribute at most 15/30 of catalyst
        if ep4["web_news_capped"] and ps["catalyst"] is not None:
            ps["catalyst"] = min(ps["catalyst"], 50.0)  # 15/30 on the catalyst pillar's web leg

        # Step D: final score -> hysteresis tier (5-pt buffer vs prior tier).
        # The final score is the robust quant composite (0-100); the A/B track
        # score stays in best_track_score as the legacy track metric.
        quant_final = _safe_float(row.get("_quant"), 0.0)
        short_candidate = (track == "C") and (kill + audit_kill) >= 0.5
        prev_tier = row.get("_sieve_prev_tier") or "NEUTRAL"
        tier = apply_hysteresis(quant_final, prev_tier, short_candidate)

        entry = {
            "ticker": tk,
            "name": row.get("company_name"),
            "cik": row.get("cik"),
            "market_cap": num(row.get("mcap")),
            "price": num(row.get("price")),
            "pillar_scores": ps,
            "track_a_score": round(a, 1) if a is not None else None,
            "track_b_score": round(b, 1) if b is not None else None,
            "track_c_score": round(c, 1) if c is not None else None,
            "best_track_score": round(best, 1),
            "quant_composite_score": round(quant_final, 2),
            "tier": tier,
            "state_hash": row.get("_state_hash"),
            "assigned_track": track,
            "thesis_kill_probability": round(min(0.99, kill + audit_kill), 2),
            "kill_reasons": kreasons + audit_reasons,
            "ep4": ep4,
            "data_quality_score": row.get("data_quality_score"),
            "engine": {
                "raw_altman_z": num(row.get("altman_z")),
                "raw_piotroski_f": row.get("piotroski_f"),
                "raw_mfi_14": num(row.get("mfi_14")),
                "raw_float_shares": num(row.get("float_shares")),
                "raw_blended_upside_pct": num(row.get("blended_upside_pct")),
            },
            "checkpoint": row.get("_checkpoint", "fresh"),
        }
        rankings.append(entry)
        # tier lifecycle state
        if not args.dry_run:
            apply_tier_state(TIER_STATE_HOLDER["state"], tk, row.get("company_name"),
                             num(row.get("mcap")),
                             track if (track in ("A", "B") and best >= GATE_UP)
                             else ("C" if best <= GATE_DOWN else None),
                             best if track != "C" else c, evidence)
        # Guard #2: incremental checkpoint — flush every FLUSH_EVERY fresh rows
        if i % FLUSH_EVERY == 0 and i < len(fresh_rows):
            flush_checkpoint(rankings, final=False)

    rankings.sort(key=lambda r: -(r["best_track_score"] or 0))
    n75 = sum(1 for r in rankings if (r["best_track_score"] or 0) >= AUDIT_GATE)
    log(f"Agent 4 (Red-Team-Inquisitor): {n75} fresh tickers audited >= {AUDIT_GATE}; "
        f"forced Track C: {sum(1 for r in rankings if 'DETERMINISTIC KILL' in r['assigned_track'] or r['assigned_track'] == 'C')}")

    # ---- canonical ticker_scores.json: FINAL checkpoint (validator-shaped) ----
    flush_checkpoint(rankings, final=True)

    if args.dry_run:
        # production files untouched; verify the atomic probe round-trips
        probe = os.path.join(CRON, ".smoke_ticker_scores_probe.json")
        back = load_json(probe, {})
        assert back.get("rankings") is not None and len(back["rankings"]) >= \
            len(rankings) + len(skipped_entries), "atomic write probe mismatch"
        os.unlink(probe)
        log(f"DRY-RUN: ticker_scores.json verified via atomic probe "
            f"({len(rankings)} fresh + {len(skipped_entries)} carried); production files untouched")
        return 0
    log(f"ticker_scores.json: FINAL checkpoint "
        f"({len(rankings)} fresh + {len(skipped_entries)} carried, "
        f"{_ckpt['n_flushes']} total flushes) (atomic write OK)")

    # ---- tier_list_state.json ----
    TIER_STATE_HOLDER["state"]["updated"] = NOW_TAG
    fmp_client.atomic_write_json(TIER_STATE, TIER_STATE_HOLDER["state"])
    ts = TIER_STATE_HOLDER["state"]["lists"]
    log(f"tier_list_state.json: A={len(ts['A'])} B={len(ts['B'])} C={len(ts['C'])} (atomic write OK)")

    # ---- wiki catalyst lines (skipped in dry-run) ----
    if args.dry_run:
        return 0
    for r in rankings:
        try:
            ok2, note = wiki_append(r["ticker"], brief_map.get(r["ticker"], []))
            if ok2:
                log(f"wiki {r['ticker']}: {note}")
        except Exception as e:
            log(f"wiki {r['ticker']} ERR {type(e).__name__}: {e}")

    # ---- trigger tier_lists.py renderer ----
    if not args.no_tiers and not args.dry_run:
        import subprocess
        p = subprocess.run([sys.executable, TIER_RENDER], capture_output=True,
                           text=True, timeout=300, cwd=SCRIPTS)
        for line in (p.stdout or "").strip().splitlines()[:6]:
            log(f"tier_lists: {line}")
        if p.returncode != 0:
            log(f"tier_lists.py exit {p.returncode}: {(p.stderr or '')[:300]}")
    return 0


TIER_STATE_HOLDER = {"state": None}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--no-vllm", action="store_true")
    ap.add_argument("--no-tiers", action="store_true")
    ap.add_argument("--dry-run", action="store_true",
                    help="verify pipeline + atomic write path without touching "
                         "ticker_scores.json / tier_list_state.json / wiki files")
    ap.add_argument("--force", action="store_true",
                    help="rescore tickers even if scored < 6h ago (bypass Guard #2)")
    args = ap.parse_args()
    TIER_STATE_HOLDER["state"] = load_json(TIER_STATE, {"version": "1.0",
                                                         "lists": {"A": {}, "B": {}, "C": {}}})
    TIER_STATE_HOLDER["state"].setdefault("lists", {"A": {}, "B": {}, "C": {}})
    try:
        return asyncio.run(main_async(args))
    except KeyboardInterrupt:
        log("interrupted — state preserved (atomic writes only complete files)")
        return 130


if __name__ == "__main__":
    sys.exit(main())
