"""
Portfolio & Cohort Concentration Gate
Enforce cluster cap on options_delta_signals before insertion.

Usage:
    from concentration_gate import check_concentration_gate
    result = check_concentration_gate(
        ticker="NEW_TK",
        best_track_score=85,
        direction="CALL",
        generated=datetime.now().isoformat()
    )
    if result["status"] == "CLUSTER_CAPPED":
        reject_or_swap(result)
    else:
        insert_signal(result)
"""

from datetime import datetime, timedelta
from collections import defaultdict

# ── ETF pillar mapping ──────────────────────────────────────────────────────

SECTOR_TO_ETF = {
    # From alpha_vectors sector_pillar column
    "SMH": "SMH/SOXX",
    "SEMI_SUBSYSTEMS": "SMH/SOXX",
    # Robotics / AI
    "BOTZ": "BOTZ/ROBO",
    "PHYSICAL_AI_ROBOTICS": "BOTZ/ROBO",
    # Defense
    "DEFENSE": "ITA/PPA",
    "DEFENSE_GOV": "ITA/PPA",
}

STANDALONE_PILLARS = {
    "AI_INFRA_THERMAL": "AI_INFRA_THERMAL",
    "QUANTUM": "QUANTUM",
    "ENERGY_GRID_SMR": "ENERGY_GRID_SMR",
}

# Cluster cap threshold
MAX_PER_PILLAR = 2

# Swap threshold: candidate must beat weakest by this many points
SWAP_DELTA_MIN = 10

# ── Pillar resolution ───────────────────────────────────────────────────────

def resolve_ETF_pillar(ticker, conn, univ_cache=None, alpha_pillars=None):
    """
    Resolve ticker → primary ETF pillar group.

    Priority:
      1. alpha_vectors.sector_pillar → SECTOR_TO_ETF
      2. universe_cache.pillar (single column)
      3. universe_cache.pillars (multi-pillar, take first match)
    """
    if alpha_pillars is not None and ticker in alpha_pillars:
        sp = alpha_pillars[ticker]
        if sp in SECTOR_TO_ETF:
            return SECTOR_TO_ETF[sp]
        if sp in STANDALONE_PILLARS:
            return STANDALONE_PILLARS[sp]

    if univ_cache is not None:
        entry = univ_cache.get(ticker)
        if entry:
            pillar = entry.get("pillar")
            if pillar and pillar in SECTOR_TO_ETF:
                return SECTOR_TO_ETF[pillar]
            if pillar and pillar in STANDALONE_PILLARS:
                return STANDALONE_PILLARS[pillar]
            # Multi-pillar fallback
            multi = entry.get("pillars")
            if multi:
                for p in multi.split(","):
                    p = p.strip()
                    if p in SECTOR_TO_ETF:
                        return SECTOR_TO_ETF[p]
                    if p in STANDALONE_PILLARS:
                        return STANDALONE_PILLARS[p]

    return "UNKNOWN"


# ── Gate enforcement ────────────────────────────────────────────────────────

def check_concentration_gate(ticker, best_track_score, direction, generated, conn=None,
                             univ_cache=None, alpha_pillars=None):
    """
    Enforce portfolio/cohort concentration cap before signal insertion.

    Parameters
    ----------
    ticker : str
        Ticker of the new signal candidate.
    best_track_score : int
        Candidate's best_track_score (0–100 scale).
    direction : str
        "CALL" or "PUT".
    generated : str
        ISO-8601 generated timestamp of the candidate.
    conn : sqlite3.Connection or callable
        If a Connection, used directly.  If None, a fresh connection is opened
        to options_delta_queue.db.  Callables receive no args and must return a
        connection (for dependency injection in tests).
    univ_cache : dict or None
        Pre-loaded universe_cache rows: {ticker: {"pillar": ..., "pillars": ...,
        "status": ...}}.  Populated automatically if None.
    alpha_pillars : dict or None
        Pre-loaded alpha_vectors rows: {ticker: sector_pillar_str}.  Populated
        automatically if None.

    Returns
    -------
    dict with keys:
        status     : "APPROVED" | "CLUSTER_CAPPED"
        reason     : human-readable explanation
        pillar     : resolved ETF pillar group
        pillar_count : current active signals in pillar (excl. self if already present)
        weakest_score : lowest score in same pillar (or None)
        delta      : best_track_score − weakest_score (or None)
    """

    # ── resolve pillar ────────────────────────────────────────────────────
    pillar = resolve_ETF_pillar(ticker, conn, univ_cache, alpha_pillars)

    # ── fetch active signals (last 10 trading days ≈ 14 calendar days) ────
    active_signals, latest_gen = _fetch_active_signals(ticker, conn)

    # ── filter to same pillar ─────────────────────────────────────────────
    pillar_signals = [
        s for s in active_signals
        if s["ETF_pillar"] == pillar and s["ticker"] != ticker
    ]
    pillar_count = len(pillar_signals)

    # ── apply cap ─────────────────────────────────────────────────────────
    if pillar_count < MAX_PER_PILLAR:
        return {
            "status": "APPROVED",
            "reason": f"Pillar count ({pillar_count}) below cap of {MAX_PER_PILLAR}",
            "pillar": pillar,
            "pillar_count": pillar_count,
            "weakest_score": None,
            "delta": None,
        }

    weakest_score = min(s["best_track_score"] for s in pillar_signals)
    delta = best_track_score - weakest_score

    if delta >= SWAP_DELTA_MIN:
        return {
            "status": "APPROVED",
            "reason": (
                f"Swap-eligible: score {best_track_score} beats pillar weakest "
                f"{weakest_score} by {delta}pts (threshold {SWAP_DELTA_MIN})"
            ),
            "pillar": pillar,
            "pillar_count": pillar_count,
            "weakest_score": weakest_score,
            "delta": delta,
        }

    return {
        "status": "CLUSTER_CAPPED",
        "reason": (
            f"Pillar cap reached ({pillar_count}/{MAX_PER_PILLAR} active). "
            f"Score {best_track_score} beats weakest {weakest_score} by only "
            f"{delta}pts (need ≥{SWAP_DELTA_MIN})"
        ),
        "pillar": pillar,
        "pillar_count": pillar_count,
        "weakest_score": weakest_score,
        "delta": delta,
    }


def _fetch_active_signals(ticker, conn_factory):
    """
    Query active signals from the last 10 trading days (14 calendar days
    relative to the most recent signal timestamp).

    Returns (signals_list, latest_generated_str).
    """
    if callable(conn_factory) and not hasattr(conn_factory, "execute"):
        conn = conn_factory()
    else:
        conn = conn_factory

    latest_gen = conn.execute(
        "SELECT MAX(generated) FROM options_delta_signals"
    ).fetchone()[0]

    latest_dt = datetime.strptime(latest_gen, "%Y-%m-%dT%H:%M:%S.%f+00:00")
    cutoff_dt = latest_dt - timedelta(days=14)
    cutoff_str = cutoff_dt.strftime("%Y-%m-%dT%H:%M:%S")

    rows = conn.execute(
        "SELECT ticker, best_track_score, generated, direction "
        "FROM options_delta_signals "
        "WHERE generated >= ?",
        (cutoff_str,),
    ).fetchall()

    signals = [
        {
            "ticker": r[0],
            "best_track_score": r[1],
            "generated": r[2],
            "direction": r[3],
            "ETF_pillar": None,  # resolved by caller / resolver
        }
        for r in rows
    ]

    return signals, latest_gen
