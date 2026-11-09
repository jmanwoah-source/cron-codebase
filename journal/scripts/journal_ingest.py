#!/usr/bin/env python3
"""
journal_ingest.py — Proposition A: The Ledger
Ingests scoring data from Deep-Dive output into the SQLite journal.
Runs after each Deep-Dive scoring cycle.

Reads: ~/.hermes/cron/ticker_scores.json
Writes: ~/.hermes/cron/journal/scoring_journal.db
"""

import json
import os
import sys
import sqlite3
from datetime import datetime, timezone

JOURNAL_DB = "/home/jaren/.hermes/cron/journal/scoring_journal.db"
SCORE_FILE = "/home/jaren/.hermes/cron/ticker_scores.json"
EVENTS_FILE = "/home/jaren/.hermes/cron/journal/reports/ingestion.log"

def init_db():
    """Initialize journal database with schema."""
    schema_path = "/home/jaren/.hermes/cron/journal/schema.sql"
    conn = sqlite3.connect(JOURNAL_DB)
    with open(schema_path, "r") as f:
        conn.executescript(f.read())
    conn.commit()
    return conn

def log_event(conn, event_type, ticker, cycle, details):
    """Record a journal event."""
    conn.execute(
        "INSERT INTO journal_events (event_type, ticker, cycle, details) VALUES (?, ?, ?, ?)",
        (event_type, ticker, cycle, details)
    )

def parse_score_file(filepath):
    """Parse the Deep-Dive scoring output file.
    
    Expected structure:
    {
        "generated": "2026-11-09T...",
        "cycle": "2026-11-09",
        "scored_count": 11,
        "excluded_mcap_policy": {...},
        "tickers": {
            "ADT": {
                "ticker": "ADT",
                "name": "Adtalem Global Education",
                "sector": "Consumer Discretionary",
                "industry": "Education Services",
                "market_cap": 6663535900,
                "price": 9.86,
                "data_coverage": 1.0,
                "financial_100": {"total": 62, "adjusted": 62, ...},
                "catalyst_100": 0,
                "valuation_100": 30,
                "technical_100": 20,
                "bifurcation": {
                    "track_a_pillars": {"p1": 0, "p2": 7, ...},
                    "track_a_score": 10,
                    "track_b_pillars": {"p1": 20, ...},
                    "track_b_score": 50,
                    "track_c_score": 70
                }
            }
        }
    }
    """
    with open(filepath, "r") as f:
        data = json.load(f)
    
    cycle = data.get("cycle", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    score_cycle = data.get("generated", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"))
    tickers_dict = data.get("tickers", {})
    excluded = data.get("excluded_mcap_policy", {})
    
    results = {
        "cycle": cycle,
        "generated": score_cycle,
        "scanned_count": len(tickers_dict),
        "excluded_count": len(excluded),
        "tickers": {}
    }
    
    for ticker, tdata in tickers_dict.items():
        bif = tdata.get("bifurcation", {})
        track_a = bif.get("track_a_score", 0)
        track_b = bif.get("track_b_score", 0)
        track_c_raw = bif.get("track_c_score", 0)
        
        # Determine tier from best track score
        best = max(track_a, track_b)
        if best >= 65:
            tier = "HIGH"
        elif best >= 55:
            tier = "MODERATE"
        else:
            tier = "LOW"
        
        fin = tdata.get("financial_100", {})
        fin_total = fin.get("adjusted", fin.get("total", 0))
        
        results["tickers"][ticker] = {
            "ticker": ticker,
            "company_name": tdata.get("name", tdata.get("company_name", "")),
            "sector": tdata.get("sector", ""),
            "industry": tdata.get("industry", ""),
            "market_cap": tdata.get("market_cap", 0),
            "price_at_entry": tdata.get("price", 0),
            "entry_date": cycle,
            "best_track_score": round(best, 1),
            "track_a_score": track_a,
            "track_b_score": track_b,
            "track_c_score": track_c_raw,
            "tier": tier,
            "financial_100": fin_total,
            "catalyst_100": tdata.get("catalyst_100", 0),
            "valuation_100": tdata.get("valuation_100", 0),
            "technical_100": tdata.get("technical_100", 0),
            "bifurcation_note": bif.get("track_a_pillars_note", ""),
            "data_coverage": tdata.get("data_coverage", 0),
        }
    
    return results

def upsert_ticker(conn, ticker_data):
    """Insert or update a ticker in the journal."""
    ticker = ticker_data["ticker"]
    
    # Check if exists
    row = conn.execute("SELECT ticker FROM tickers WHERE ticker = ?", (ticker,))
    exists = row.fetchone() is not None
    
    if exists:
        conn.execute("""
            UPDATE tickers SET
                company_name = ?,
                sector = ?,
                industry = ?,
                market_cap = ?,
                price_at_entry = ?,
                entry_date = ?,
                best_track_score = ?,
                tier = ?,
                track_a_score = ?,
                track_b_score = ?,
                track_c_score = ?,
                financial_100 = ?,
                catalyst_100 = ?,
                valuation_100 = ?,
                technical_100 = ?,
                updated_at = datetime('now')
            WHERE ticker = ?
        """, (
            ticker_data["company_name"],
            ticker_data["sector"],
            ticker_data["industry"],
            ticker_data["market_cap"],
            ticker_data["price_at_entry"],
            ticker_data["entry_date"],
            ticker_data["best_track_score"],
            ticker_data["tier"],
            ticker_data["track_a_score"],
            ticker_data["track_b_score"],
            ticker_data["track_c_score"],
            ticker_data["financial_100"],
            ticker_data["catalyst_100"],
            ticker_data["valuation_100"],
            ticker_data["technical_100"],
            ticker,
        ))
        log_event(conn, "RESCORED", ticker, ticker_data["entry_date"],
                  f"tier={ticker_data['tier']} best={ticker_data['best_track_score']}")
    else:
        conn.execute("""
            INSERT INTO tickers (
                ticker, company_name, sector, industry, market_cap,
                price_at_entry, entry_date, best_track_score, tier,
                track_a_score, track_b_score, track_c_score,
                financial_100, catalyst_100, valuation_100, technical_100,
                status, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?)
        """, (
            ticker,
            ticker_data["company_name"],
            ticker_data["sector"],
            ticker_data["industry"],
            ticker_data["market_cap"],
            ticker_data["price_at_entry"],
            ticker_data["entry_date"],
            ticker_data["best_track_score"],
            ticker_data["tier"],
            ticker_data["track_a_score"],
            ticker_data["track_b_score"],
            ticker_data["track_c_score"],
            ticker_data["financial_100"],
            ticker_data["catalyst_100"],
            ticker_data["valuation_100"],
            ticker_data["technical_100"],
            ticker_data.get("bifurcation_note", ""),
        ))
        log_event(conn, "SCORED", ticker, ticker_data["entry_date"],
                  f"tier={ticker_data['tier']} best={ticker_data['best_track_score']}")

def run_ingestion():
    """Main ingestion pipeline."""
    if not os.path.exists(SCORE_FILE):
        print(f"ERROR: Score file not found: {SCORE_FILE}")
        return False
    
    conn = init_db()
    
    try:
        results = parse_score_file(SCORE_FILE)
        cycle = results["cycle"]
        
        # Upsert all scored tickers
        for ticker, tdata in results["tickers"].items():
            upsert_ticker(conn, tdata)
        
        # Handle excluded tickers
        excluded = results.get("excluded_mcap_policy", {})
        for ticker, reason in excluded.items():
            log_event(conn, "EXCLUDED", ticker, cycle, f"mcap_exclusion: {reason}")
            # Check if already in journal as ACTIVE, demote it
            row = conn.execute("SELECT status FROM tickers WHERE ticker = ?", (ticker,))
            status_row = row.fetchone()
            if status_row and status_row[0] == "ACTIVE":
                conn.execute("UPDATE tickers SET status = 'EXCLUDED', demotion_reason = ?, updated_at = datetime('now') WHERE ticker = ?",
                           (reason, ticker))
        
        conn.commit()
        
        # Generate summary
        ticker_count = len(results["tickers"])
        tier_counts = {}
        for t in results["tickers"].values():
            tier = t["tier"]
            tier_counts[tier] = tier_counts.get(tier, 0) + 1
        
        summary = {
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "cycle": cycle,
            "scored_count": ticker_count,
            "excluded_count": results.get("excluded_count", 0),
            "tier_distribution": tier_counts,
        }
        
        # Write summary
        summary_path = "/home/jaren/.hermes/cron/journal/reports/ingestion_summary.json"
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2)
        
        # Print summary for cron output
        print(f"Ingestion complete — cycle {cycle}")
        print(f"  Scored: {ticker_count} tickers")
        for tier, count in sorted(tier_counts.items()):
            print(f"    {tier}: {count}")
        print(f"  Excluded (mcap): {results.get('excluded_count', 0)}")
        
        return True
        
    except Exception as e:
        print(f"ERROR: Ingestion failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    success = run_ingestion()
    sys.exit(0 if success else 1)
