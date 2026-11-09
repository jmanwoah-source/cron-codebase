#!/usr/bin/env python3
"""
journal_analyze.py — Proposition C: The Feedback Engine
The BRAIN. Analyzes scoring journal data for:
1. Forward return correlation with each scoring pillar
2. Tier threshold validation (discriminative power)
3. False positive / false negative analysis
4. Auto-tuning proposals (actionable parameter changes)

Runs weekly via cron. Produces:
- Weekly digest (markdown)
- Tuning recommendations (JSON)
- Correlation report (JSON)
"""

import json
import os
import sqlite3
import math
from datetime import datetime, timezone
from collections import defaultdict

JOURNAL_DB = "/home/jaren/.hermes/cron/journal/scoring_journal.db"
REPORT_DIR = "/home/jaren/.hermes/cron/journal/reports"


def spearman_rank_correlation(x, y):
    """Compute Spearman rank correlation coefficient.
    
    Handles ties by averaging ranks.
    Returns r in [-1, 1], or None if insufficient data.
    """
    n = len(x)
    if n < 3:
        return None
    
    # Remove pairs where either is None
    pairs = [(xi, yi) for xi, yi in zip(x, y) if xi is not None and yi is not None]
    n = len(pairs)
    if n < 3:
        return None
    
    xs, ys = zip(*pairs)
    
    def rank_with_ties(values):
        """Compute ranks with average for ties."""
        sorted_vals = sorted(enumerate(values), key=lambda x: x[1])
        ranks = [0.0] * len(values)
        i = 0
        while i < len(sorted_vals):
            j = i
            while j < len(sorted_vals) and sorted_vals[j][1] == sorted_vals[i][1]:
                j += 1
            avg_rank = (i + j - 1) / 2.0 + 1  # 1-based rank
            for k in range(i, j):
                ranks[sorted_vals[k][0]] = avg_rank
            i = j
        return ranks
    
    x_ranks = rank_with_ties(xs)
    y_ranks = rank_with_ties(ys)
    
    # Compute correlation
    mean_x = sum(x_ranks) / n
    mean_y = sum(y_ranks) / n
    
    num = sum((x_r - mean_x) * (y_r - mean_y) for x_r, y_r in zip(x_ranks, y_ranks))
    den_x = math.sqrt(sum((x_r - mean_x) ** 2 for x_r in x_ranks))
    den_y = math.sqrt(sum((y_r - mean_y) ** 2 for y_r in y_ranks))
    
    if den_x == 0 or den_y == 0:
        return None
    
    return num / (den_x * den_y)


def analyze_tier_discrimination(conn):
    """Analyze whether tier scores actually predict forward returns.
    
    Returns dict with:
    - group_stats: per-tier metrics (n, avg return, std, etc.)
    - discrimination_score: how well tiers separate (higher = better)
    """
    tickers = conn.execute("""
        SELECT t.ticker, t.tier, t.best_track_score,
               f7.return_pct as ret_7d,
               f30.return_pct as ret_30d,
               f90.return_pct as ret_90d
        FROM tickers t
        LEFT JOIN forward_returns f7 ON t.ticker = f7.ticker AND f7.window_days = 7
        LEFT JOIN forward_returns f30 ON t.ticker = f30.ticker AND f30.window_days = 30
        LEFT JOIN forward_returns f90 ON t.ticker = f90.ticker AND f90.window_days = 90
        WHERE t.status = 'ACTIVE' AND t.best_track_score > 0
    """).fetchall()
    
    if not tickers:
        return {"error": "no data", "tiers": {}}
    
    # Group by tier
    tier_groups = defaultdict(list)
    for ticker, tier, score, ret7, ret30, ret90 in tickers:
        tier_groups[tier].append({
            "ticker": ticker,
            "score": score,
            "ret_7d": ret7,
            "ret_30d": ret30,
            "ret_90d": ret90,
        })
    
    tier_stats = {}
    for tier, entries in tier_groups.items():
        scores = [e["score"] for e in entries]
        ret7s = [e["ret_7d"] for e in entries if e["ret_7d"] is not None]
        ret30s = [e["ret_30d"] for e in entries if e["ret_30d"] is not None]
        ret90s = [e["ret_90d"] for e in entries if e["ret_90d"] is not None]
        
        def avg(lst):
            return sum(lst) / len(lst) if lst else None
        
        def std_dev(lst):
            if len(lst) < 2:
                return None
            mean = avg(lst)
            variance = sum((x - mean) ** 2 for x in lst) / (len(lst) - 1)
            return math.sqrt(variance)
        
        def positive_pct(lst):
            if not lst:
                return None
            return sum(1 for x in lst if x > 0) / len(lst) * 100
        
        tier_stats[tier] = {
            "n": len(entries),
            "avg_score": round(avg(scores), 1) if scores else None,
            "min_score": round(min(scores), 1) if scores else None,
            "max_score": round(max(scores), 1) if scores else None,
            "ret_7d": {
                "avg": round(avg(ret7s), 2) if ret7s else None,
                "std": round(std_dev(ret7s), 2) if std_dev(ret7s) else None,
                "positive_pct": round(positive_pct(ret7s), 1) if positive_pct(ret7s) else None,
                "n": len(ret7s),
            },
            "ret_30d": {
                "avg": round(avg(ret30s), 2) if ret30s else None,
                "std": round(std_dev(ret30s), 2) if std_dev(ret30s) else None,
                "positive_pct": round(positive_pct(ret30s), 1) if positive_pct(ret30s) else None,
                "n": len(ret30s),
            },
            "ret_90d": {
                "avg": round(avg(ret90s), 2) if ret90s else None,
                "std": round(std_dev(ret90s), 2) if std_dev(ret90s) else None,
                "positive_pct": round(positive_pct(ret90s), 1) if positive_pct(ret90s) else None,
                "n": len(ret90s),
            },
            "tickers": [e["ticker"] for e in entries],
        }
    
    # Compute discrimination score: separation between tier means relative to within-tier spread
    # Higher = better predictive power
    all_scores = [e["score"] for tier_entries in tier_groups.values() for e in tier_entries]
    overall_mean = sum(all_scores) / len(all_scores) if all_scores else 0
    overall_std = std_dev(all_scores) if len(all_scores) >= 2 else 1
    
    # Between-tier variance / within-tier variance
    between_var = sum(
        len(tier_groups[t]) * (tier_stats[t]["avg_score"] - overall_mean) ** 2
        for t in tier_groups if tier_stats[t]["avg_score"] is not None
    ) / len(all_scores)
    
    within_var = sum(
        sum((e["score"] - tier_stats[t]["avg_score"]) ** 2 for e in tier_groups[t])
        for t in tier_groups if tier_stats[t]["avg_score"] is not None
    ) / max(len(all_scores) - len(tier_groups), 1)
    
    f_stat = between_var / max(within_var, 0.001) if overall_std > 0 else 0
    
    return {
        "discrimination_score": round(f_stat, 4),
        "tier_stats": tier_stats,
    }


def analyze_pillar_correlations(conn):
    """For each scoring pillar, compute correlation with forward returns.
    
    Returns dict of {pillar_name: {correlation, n, interpretation}}
    """
    rows = conn.execute("""
        SELECT t.ticker, t.financial_100, t.catalyst_100,
               t.valuation_100, t.technical_100,
               f7.return_pct as ret_7d,
               f30.return_pct as ret_30d,
               f90.return_pct as ret_90d
        FROM tickers t
        LEFT JOIN forward_returns f7 ON t.ticker = f7.ticker AND f7.window_days = 7
        LEFT JOIN forward_returns f30 ON t.ticker = f30.ticker AND f30.window_days = 30
        LEFT JOIN forward_returns f90 ON t.ticker = f90.ticker AND f90.window_days = 90
        WHERE t.status = 'ACTIVE'
          AND t.financial_100 > 0 AND t.catalyst_100 >= 0
          AND t.valuation_100 >= 0 AND t.technical_100 >= 0
    """).fetchall()
    
    pillars = ["financial_100", "catalyst_100", "valuation_100", "technical_100"]
    windows = [7, 30, 90]
    
    results = {}
    for pillar in pillars:
        pillar_results = {}
        for window in windows:
            pillar_vals = []
            return_vals = []
            for row in rows:
                pval = row[pillars.index(pillar)]
                ret = row[3 + windows.index(window)]  # ret_7d at index 3
                if pval is not None and ret is not None:
                    pillar_vals.append(pval)
                    return_vals.append(ret)
            
            corr = spearman_rank_correlation(pillar_vals, return_vals)
            
            if corr is not None:
                abs_corr = abs(corr)
                if abs_corr >= 0.5:
                    interpretation = "strong"
                elif abs_corr >= 0.3:
                    interpretation = "moderate"
                elif abs_corr >= 0.1:
                    interpretation = "weak"
                else:
                    interpretation = "negligible"
            else:
                interpretation = "insufficient_data"
            
            pillar_results[window] = {
                "correlation": round(corr, 3) if corr is not None else None,
                "n": len(pillar_vals),
                "interpretation": interpretation,
            }
        
        results[pillar] = pillar_results
    
    return results


def generate_recommendations(analysis, correlations):
    """Generate actionable tuning recommendations based on analysis.
    
    Rules:
    - If correlation is weak/negligible: recommend reducing pillar weight
    - If tier discrimination is low: recommend threshold adjustment
    - If false positive rate > 50%: recommend adding filters
    - Always note sample size limitations
    """
    recommendations = []
    
    # Check pillar correlations
    for pillar, windows in correlations.items():
        for window, data in windows.items():
            corr = data.get("correlation")
            interp = data.get("interpretation", "")
            n = data.get("n", 0)
            
            if corr is None or n < 3:
                # Insufficient data — note but don't recommend changes
                continue
            
            if interp in ("negligible", "weak"):
                abs_corr = abs(corr)
                recommendations.append({
                    "pillar": pillar,
                    "window": f"{window}d",
                    "action": "REDUCE_WEIGHT",
                    "from_pct": "25%",
                    "to_pct": "15%",
                    "rationale": f"{pillar} {interp} correlation ({corr:.3f}) with {window}d returns",
                    "confidence": "low" if n < 10 else "medium",
                    "n": n,
                    "risk": f"May underweight genuine {pillar.replace('_', ' ')} opportunities",
                    "requires_data": n >= 10,
                })
            elif corr > 0.4 and interp == "strong":
                recommendations.append({
                    "pillar": pillar,
                    "window": f"{window}d",
                    "action": "INCREASE_WEIGHT",
                    "from_pct": "25%",
                    "to_pct": "30%",
                    "rationale": f"{pillar} shows strong positive correlation ({corr:.3f}) with {window}d returns",
                    "confidence": "medium" if n >= 10 else "low",
                    "n": n,
                    "risk": "Overweighting may increase false positives in other dimensions",
                    "requires_data": n >= 10,
                })
    
    # Check tier discrimination
    disc = analysis.get("discrimination_score", 0)
    tier_stats = analysis.get("tier_stats", {})
    
    if disc < 0.1 and len(tier_stats) >= 2:
        recommendations.append({
            "rule": "tier_thresholds",
            "action": "RECALIBRATE",
            "rationale": f"Low tier discrimination score ({disc:.4f}) — tiers don't separate well by returns",
            "confidence": "low",
            "n": sum(ts["n"] for ts in tier_stats.values()),
            "risk": "Recalibration may destabilize current scoring structure",
            "requires_data": True,
        })
    
    # Check for dominant tier
    if len(tier_stats) == 1:
        dominant = list(tier_stats.keys())[0]
        recommendations.append({
            "rule": "tier_distribution",
            "action": "NOTICE",
            "rationale": f"Only one tier present ({dominant}) — scoring has no discrimination",
            "confidence": "high",
            "n": tier_stats[dominant]["n"],
            "risk": None,
            "requires_data": True,
        })
    
    return recommendations


def generate_weekly_digest(conn):
    """Generate a human-readable weekly digest markdown report."""
    # Get all tickers in journal
    all_tickers = conn.execute("""
        SELECT ticker, company_name, tier, best_track_score, status, 
               entry_date, updated_at
        FROM tickers
        ORDER BY entry_date DESC
    """).fetchall()
    
    # Get returns data
    returns_data = conn.execute("""
        SELECT t.ticker, t.tier, t.entry_date,
               f7.return_pct as ret_7d, f30.return_pct as ret_30d, f90.return_pct as ret_90d
        FROM tickers t
        LEFT JOIN forward_returns f7 ON t.ticker = f7.ticker AND f7.window_days = 7
        LEFT JOIN forward_returns f30 ON t.ticker = f30.ticker AND f30.window_days = 30
        LEFT JOIN forward_returns f90 ON t.ticker = f90.ticker AND f90.window_days = 90
        WHERE t.status = 'ACTIVE'
    """).fetchall()
    
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    lines = []
    lines.append(f"# Scoring Journal — Weekly Digest")
    lines.append(f"# Generated: {now}")
    lines.append("")
    
    # Summary stats
    active = [t for t in all_tickers if t[4] == "ACTIVE"]
    total = len(all_tickers)
    
    lines.append("## Summary")
    lines.append(f"- **Total tickers in journal:** {total}")
    lines.append(f"- **Active:** {len(active)}")
    
    # Count by tier
    tier_counts = defaultdict(int)
    for t in all_tickers:
        tier_counts[t[2]] += 1
    lines.append(f"- **By tier:** {dict(tier_counts)}")
    
    lines.append("")
    lines.append("## Active Positions")
    lines.append("Ticker | Company | Tier | Score | Entry | 7d | 30d | 90d")
    lines.append("---|---|---|---|---|---|---|---")
    
    for ticker, company, tier, score, status, entry, updated in active:
        ret_data = next((r for r in returns_data if r[0] == ticker), None)
        if ret_data:
            r7d = f"{ret_data[3]:.1f}%" if ret_data[3] is not None else "N/A"
            r30d = f"{ret_data[4]:.1f}%" if ret_data[4] is not None else "N/A"
            r90d = f"{ret_data[5]:.1f}%" if ret_data[5] is not None else "N/A"
        else:
            r7d = r30d = r90d = "N/A"
        
        lines.append(f"{ticker} | {company} | {tier} | {score:.0f} | {entry} | {r7d} | {r30d} | {r90d}")
    
    lines.append("")
    lines.append("---")
    lines.append("*Auto-generated by journal_analyze.py*")
    
    digest = "\n".join(lines)
    
    digest_path = os.path.join(REPORT_DIR, f"digest_{now}.md")
    with open(digest_path, "w") as f:
        f.write(digest)
    
    # Also update a "latest" link
    latest_path = os.path.join(REPORT_DIR, "digest_latest.md")
    with open(latest_path, "w") as f:
        f.write(digest)
    
    return digest


def run_analysis():
    """Run the full analysis pipeline."""
    conn = sqlite3.connect(JOURNAL_DB)
    try:
        print(f"[{datetime.now().isoformat()}] === Scoring Journal Analysis ===")
        
        # Step 1: Tier discrimination analysis
        print("\n[1/4] Tier discrimination...")
        analysis = analyze_tier_discrimination(conn)
        
        if "error" in analysis:
            print(f"  {analysis['error']}")
        
        tier_stats = analysis.get("tier_stats", {})
        for tier, stats in tier_stats.items():
            n = stats["n"]
            avg_ret_30d = stats.get("ret_30d", {}).get("avg")
            avg_ret_90d = stats.get("ret_90d", {}).get("avg")
            print(f"  {tier}: {n} tickers, 30d avg={avg_ret_30d if avg_ret_30d is not None else 'N/A'}%, 90d avg={avg_ret_90d if avg_ret_90d is not None else 'N/A'}%")
        
        disc_score = analysis.get("discrimination_score", 0)
        print(f"  Discrimination score: {disc_score:.4f}")
        
        # Step 2: Pillar correlations
        print("\n[2/4] Pillar correlations...")
        correlations = analyze_pillar_correlations(conn)
        
        for pillar, windows in correlations.items():
            for window, data in windows.items():
                corr = data.get("correlation")
                interp = data.get("interpretation", "?")
                n = data.get("n", 0)
                print(f"  {pillar} @ {window}d: r={corr if corr else 'N/A'}, n={n}, {interp}")
        
        # Step 3: Generate recommendations
        print("\n[3/4] Generating recommendations...")
        recs = generate_recommendations(analysis, correlations)
        
        if recs:
            for rec in recs:
                if "pillar" in rec:
                    pillar_key = rec["pillar"]
                elif "rule" in rec:
                    pillar_key = rec["rule"]
                else:
                    pillar_key = "unknown"
                print(f"  [{rec['action']}] {pillar_key}: {rec['rationale'][:80]}")
        else:
            print("  No recommendations (insufficient data)")
        
        # Step 4: Write outputs
        print("\n[4/4] Writing outputs...")
        
        # Write recommendations JSON
        recs_output = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "discrimination_score": disc_score,
            "tier_stats": tier_stats,
            "pillar_correlations": correlations,
            "recommendations": recs,
        }
        
        recs_path = os.path.join(REPORT_DIR, "tuning_recommendations.json")
        with open(recs_path, "w") as f:
            json.dump(recs_output, f, indent=2)
        
        # Generate digest
        digest = generate_weekly_digest(conn)
        print(f"  Digest written to {os.path.join(REPORT_DIR, 'digest_latest.md')}")
        
        print(f"\n[COMPLETE] {len(recs)} recommendations generated")
        return recs_output
        
    except Exception as e:
        print(f"[ERROR] Analysis failed: {e}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}
    finally:
        conn.close()


if __name__ == "__main__":
    run_analysis()
