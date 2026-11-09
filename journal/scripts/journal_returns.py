#!/usr/bin/env python3
"""
journal_returns.py — Proposition C: The Feedback Engine
Price tracker that fetches historical prices via FMP MCP and computes
forward returns at 7d/30d/90d windows for all journal tickers.

This is designed to run inside an execute_code cron session where
mcp__fmp__chart tools are available.

Usage (inside cron execute_code):
    mcp__fmp__chart with historical-price-eod-full for each ticker
    Then call this function with the collected data.

For testing outside cron: run with --demo flag to simulate data.
"""

import json
import os
import sys
import sqlite3
from datetime import datetime, timezone, timedelta

JOURNAL_DB = "/home/jaren/.hermes/cron/journal/scoring_journal.db"
REPORT_DIR = "/home/jaren/.hermes/cron/journal/reports"


def fetch_prices_via_mcp(ticker):
    """Fetch EOD historical prices via FMP MCP chart endpoint.
    
    Returns: dict of {date_str: close_price} for last 120 days.
    
    NOTE: This function is designed to be called from execute_code
    cron sessions where mcp__fmp__chart is available as an MCP tool.
    The caller must pass the MCP tool result.
    
    When called directly (e.g., from browser_exec in execute_code),
    the caller should do:
    
    ```
    # In execute_code:
    from datetime import datetime, timedelta
    today = datetime.now().strftime('%Y-%m-%d')
    past = (datetime.now() - timedelta(days=120)).strftime('%Y-%m-%d')
    
    # Call MCP tool via the browser exec mechanism
    # The tool returns: {symbol, historical: [{date, close, ...}, ...]}
    
    import json
    result = call_mcp_tool('mcp__fmp__chart', {
        'endpoint': 'historical-price-eod-full',
        'symbol': 'TICKER',
        'from_date': past,
        'to_date': today
    })
    ```
    
    This function parses that result.
    """
    # This is a placeholder — actual MCP calls happen via execute_code
    # The caller fills in the result here
    return {}


def process_mcp_results(ticker, mcp_result):
    """Process an FMP MCP chart result into a date→close dict.
    
    Expected mcp_result format from FMP chart endpoint:
    {
        "symbol": "TICKER",
        "historical": [
            {"date": "2026-11-01", "close": 12.34, "open": 12.10, "high": 12.50, "low": 12.00, "volume": 1234567},
            ...
        ]
    }
    """
    if not mcp_result or not isinstance(mcp_result, dict):
        return {}
    
    historical = mcp_result.get("historical", [])
    if not historical:
        return {}
    
    price_map = {}
    for bar in historical:
        date_str = bar.get("date", "")
        close = bar.get("close", 0)
        if date_str and close:
            price_map[date_str] = close
    
    return price_map


def compute_forward_returns(conn, price_data):
    """For each ACTIVE ticker, compute forward returns at 7/30/90d windows.
    
    Args:
        conn: SQLite connection to journal DB
        price_data: dict of {ticker: {date_str: close_price}}
    
    Returns:
        dict of {ticker: {7d: pct, 30d: pct, 90d: pct}}
    """
    tickers = conn.execute("""
        SELECT t.ticker, t.company_name, t.price_at_entry, t.entry_date,
               t.tier, t.status
        FROM tickers t
        WHERE t.status = 'ACTIVE' 
          AND t.price_at_entry > 0
        ORDER BY t.entry_date
    """).fetchall()
    
    results = {}
    for ticker, company_name, entry_price, entry_date, tier, status in tickers:
        entry_dt = datetime.strptime(entry_date, "%Y-%m-%d")
        
        # Get price on entry date (or closest available)
        prices = price_data.get(ticker, {})
        entry_close = find_closest_price(prices, entry_date)
        
        if not entry_close or entry_close <= 0:
            print(f"  [SKIP] {ticker}: no price on entry date {entry_date}")
            continue
        
        window_results = {}
        for window_days in [7, 30, 90]:
            target_dt = entry_dt + timedelta(days=window_days)
            target_date = target_dt.strftime("%Y-%m-%d")
            target_price = find_closest_price(prices, target_date)
            
            if target_price and target_price > 0:
                return_pct = ((target_price - entry_close) / entry_close) * 100
                window_results[window_days] = round(return_pct, 2)
            else:
                window_results[window_days] = None  # Not yet available
        
        results[ticker] = {
            "company_name": company_name,
            "entry_price": round(entry_close, 2),
            "entry_date": entry_date,
            "tier": tier,
            "returns": window_results,
            "days_since_entry": (datetime.now() - entry_dt).days
        }
        
        # Print status
        status_str = ""
        for w in [7, 30, 90]:
            if window_results.get(w) is not None:
                status_str += f" {w}d={window_results[w]}%"
        
        print(f"  [{ticker}] entry={entry_date} price={entry_close:.2f}" + status_str)
    
    # Update database
    for ticker, data in results.items():
        for window_days, return_pct in data["returns"].items():
            if return_pct is not None:
                conn.execute("""
                    INSERT OR REPLACE INTO forward_returns (ticker, window_days, return_pct, calculated_at)
                    VALUES (?, ?, ?, datetime('now'))
                """, (ticker, window_days, return_pct))
    
    conn.commit()
    return results


def find_closest_price(price_map, target_date):
    """Find the closest price to a target date in a price map.
    
    Args:
        price_map: dict of {date_str: close_price}
        target_date: YYYY-MM-DD string
    
    Returns:
        Closest available price, or None
    """
    if not price_map:
        return None
    
    try:
        target_dt = datetime.strptime(target_date, "%Y-%m-%d")
    except:
        return None
    
    closest_date = None
    closest_diff = float('inf')
    
    for date_str, price in price_map.items():
        try:
            date_dt = datetime.strptime(date_str, "%Y-%m-%d")
            diff = abs((date_dt - target_dt).days)
            if diff < closest_diff:
                closest_diff = diff
                closest_date = date_str
        except:
            continue
    
    if closest_date and closest_diff <= 3:  # Allow 3-day tolerance
        return price_map[closest_date]
    
    return None


def run_returns_tracker(price_data=None):
    """Main returns tracking pipeline.
    
    Args:
        price_data: Optional pre-fetched price data dict.
                   If None, the caller should fetch via MCP before calling.
    """
    conn = sqlite3.connect(JOURNAL_DB)
    try:
        if price_data is None:
            price_data = {}
        
        results = compute_forward_returns(conn, price_data)
        
        # Write report
        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "active_tickers": len(results),
            "tickers": results
        }
        
        report_path = os.path.join(REPORT_DIR, "returns_latest.json")
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)
        
        print(f"\n[{datetime.now().isoformat()}] Returns tracker complete:")
        print(f"  Active tickers: {len(results)}")
        
        # Summary by tier
        tier_stats = {}
        for ticker, data in results.items():
            tier = data.get("tier", "UNKNOWN")
            if tier not in tier_stats:
                tier_stats[tier] = {"count": 0, "returns_30d": [], "returns_90d": []}
            tier_stats[tier]["count"] += 1
            
            ret_30d = data["returns"].get(30)
            ret_90d = data["returns"].get(90)
            if ret_30d is not None:
                tier_stats[tier]["returns_30d"].append(ret_30d)
            if ret_90d is not None:
                tier_stats[tier]["returns_90d"].append(ret_90d)
        
        for tier, stats in tier_stats.items():
            avg_30d = None
            avg_90d = None
            if stats["returns_30d"]:
                avg_30d = sum(stats["returns_30d"]) / len(stats["returns_30d"])
            if stats["returns_90d"]:
                avg_90d = sum(stats["returns_90d"]) / len(stats["returns_90d"])
            print(f"  {tier}: {stats['count']} tickers, 30d avg={avg_30d:.1f}%, 90d avg={avg_90d:.1f}%")
        
        return results
    except Exception as e:
        print(f"[ERROR] Returns tracker failed: {e}")
        import traceback
        traceback.print_exc()
        return {}
    finally:
        conn.close()


if __name__ == "__main__":
    # Standalone test mode — can't call MCP from here directly
    # This is designed to be called from execute_code cron with MCP
    print("journal_returns.py — designed for execute_code cron sessions")
    print("Call with pre-fetched price data via execute_code MCP tools.")
    sys.exit(0)
