# Scoring Journal — Accountability & Feedback System

## Architecture Overview

Built for the Pip pipeline to close the feedback loop: track which tickers made it past the 70+ conviction gate, then measure whether the scoring system actually predicted forward returns.

**Proposition A (Ledger):** SQLite journal recording every scored ticker with entry price, score, tier, and status.
**Proposition C (Feedback Engine):** Statistical analysis of correlations between scoring pillars and actual forward returns, producing actionable tuning recommendations.

## File Structure

```
~/.hermes/cron/journal/
├── schema.sql                          ← SQLite schema (tickers, price_snapshots, forward_returns, journal_events)
├── scoring_journal.db                  ← SQLite database (created on first run)
├── reports/
│   ├── digest_latest.md                ← Human-readable weekly digest
│   ├── digest_YYYY-MM-DD.md            ← Archived digests
│   ├── tuning_recommendations.json     ← Machine-readable tuning suggestions
│   ├── returns_latest.json             ← Price tracking results
│   ├── ingestion.log                   ← Ingestion history
│   └── returns.log                     ← Returns tracking log
└── scripts/
    ├── journal_ingest.py              ← Reads ticker_scores.json → updates SQLite
    ├── journal_returns.py             ← Computes forward returns (7/30/90d)
    ├── journal_analyze.py             ← Spearman correlations, tier discrimination, recommendations
    ├── journal_notify.py              ← Formats digest for Discord
    └── journal_orchestrator.py        ← Chains all four scripts into one pipeline
```

## Cron Jobs

### 1. Scoring Journal — Ingest (every 4 hours)
**Job ID:** `d13172aff66f`  
Reads `~/.hermes/cron/ticker_scores.json` and writes/upserts to SQLite. Runs after each Deep-Dive scoring cycle.

### 2. Scoring Journal — Returns Tracker (every 6 hours)  
**Job ID:** `6639772a82fb`  
Tracks historical prices for all ACTIVE tickers. Computes forward returns at 7d/30d/90d windows.

### 3. Scoring Journal — Analyze + Notify (every 7 days, PAUSED until Nov 23)
**Job ID:** `a593bbb49d44`  
Full pipeline: ingest → returns → analyze → notify. Currently paused with deliver="local". On Nov 23, the warmup timer (job `922ef6890a34`) will resume it and change deliver to "origin" (Discord).

### 4. Journal Warmup Timer (one-shot, Nov 23)
**Job ID:** `922ef6890a34`  
Fires once on November 23 to unpause the analyze+notify job and switch deliver to Discord origin.

## How It Works

### Ingestion Pipeline
1. Reads `ticker_scores.json` (Deep-Dive output from `~/.hermes/cron/ticker_scores.json`)
2. Extracts each ticker's: company_name, sector, industry, market_cap, price, best_track_score, tier (HIGH/MODERATE/LOW), pillar scores (financial_100, catalyst_100, valuation_100, technical_100)
3. Upserts into SQLite tickers table
4. Records excluded tickers (mcap policy violations)
5. Writes summary to `reports/ingestion_summary.json`

### Returns Tracking
1. For each ACTIVE ticker in journal, fetches historical price data via FMP MCP
2. Computes forward return at 7d, 30d, 90d windows from entry price
3. Stores in `forward_returns` table
4. Results are consumed by the analysis engine

### Analysis Engine
1. **Tier Discrimination:** Measures whether HIGH/MODERATE/LOW tiers actually separate by forward returns. Uses F-statistic (between-group variance / within-group variance).
2. **Pillar Correlations:** For each pillar (financial_100, catalyst_100, valuation_100, technical_100), computes Spearman rank correlation with 7d/30d/90d forward returns.
3. **Recommendations:** Based on correlation strength, generates actionable tuning suggestions:
   - `REDUCE_WEIGHT`: Pillar has weak/negligible correlation with returns
   - `INCREASE_WEIGHT`: Pillar shows strong positive correlation with returns
   - `RECALIBRATE`: Tier thresholds not separating well
   - `NOTICE`: Only one tier present (no discrimination)
4. Outputs: tuning_recommendations.json, digest_YYYY-MM-DD.md, digest_latest.md

### Notification Output
Formats digest for Discord with:
- Summary stats (total tickers, active, tier distribution)
- By-tier performance (avg returns, positive rate)
- Pillar correlations with interpretation
- Numbered recommendations with rationale, confidence, n, and risk
- Cold start notice (n < 15 means not enough data for statistical significance)

## Cold Start Behavior

The system is designed for progressive improvement:
- **n < 5:** Insufficient data, all correlations will be "insufficient_data"
- **n < 10:** Weak correlations may appear but not statistically meaningful
- **n ≥ 15:** Reasonable baseline for tier threshold validation
- **n ≥ 30:** Meaningful pillar correlation analysis

The warmup timer (Nov 23) is calibrated so that by the time Discord notifications start, there should be ~3-4 weeks of data from the Deep-Dive pipeline.

## Key Design Decisions

1. **SQLite, not JSON files:** Single-file DB means ACID transactions, efficient queries, and atomic writes. All scripts connect to the same DB.
2. **Upsert, never delete:** Tickers accumulate over time. Demotion/archival is a status change, not removal.
3. **Correlation-driven, not opinion-driven:** The analysis engine produces numeric recommendations. The user decides whether to implement them.
4. **Separation of concerns:** ingest → returns → analyze → notify. Each script can run independently if needed.
5. **Discord deliver deferred:** The system produces all analysis outputs to local files immediately. Discord notifications only start after the warmup period.

## Testing

```bash
# Test ingest
python3 -c "
import sys; sys.path.insert(0, '/home/jaren/.hermes/cron/journal/scripts')
import journal_ingest
print(journal_ingest.run_ingestion())
"

# Test analysis
python3 -c "
import sys; sys.path.insert(0, '/home/jaren/.hermes/cron/journal/scripts')
import journal_analyze
print(journal_analyze.run_analysis())
"

# Test notification
python3 -c "
import sys; sys.path.insert(0, '/home/jaren/.hermes/cron/journal/scripts')
import journal_notify
print(journal_notify.main())
"
```

## Limitations

1. **FMP REST API dead:** All price data must come through MCP tools. The `journal_returns.py` script is designed for execute_code cron sessions where MCP tools are available. Direct HTTP calls to FMP will fail (403).
2. **Single-tier dataset:** Currently all 11 tickers are LOW tier. No tier discrimination data exists yet. The system will only produce meaningful tier analysis once scoring produces spread across HIGH/MODERATE/LOW.
3. **No forward price data yet:** All forward_returns entries are NULL because we only just started tracking. Correlation analysis needs at least some 30d and 90d returns to be meaningful.
4. **Sample size:** At n=11, any correlation finding is preliminary. The system should be treated as a research tool, not an authoritative scoring validator, until n ≥ 30.

## File Descriptions

### schema.sql
Defines four tables: tickers, price_snapshots, forward_returns, and journal_events. Each ticker is a primary key with upsert support.

### journal_ingest.py
Reads the Deep-Dive scoring output (ticker_scores.json), extracts all fields, and upserts into SQLite. Handles excluded tickers via mcap policy.

### journal_returns.py
Computes forward returns by comparing entry price against later prices. Designed to run daily, accumulating data over time.

### journal_analyze.py
The brain. Computes Spearman rank correlations, tier discrimination via F-statistic, and generates actionable recommendations. Produces both human-readable markdown and machine-readable JSON.

### journal_notify.py
Formats the analysis output for Discord delivery. Strips emoji (not Discord-native), uses bold/italic formatting, and includes cold start notices when sample size is insufficient.

### journal_orchestrator.py
Chains all four scripts into one pipeline execution. Designed for execute_code cron sessions.
