# Bitemporal Point-in-Time (PIT) Schema Diff Report

**Date:** 2026-11-08  
**Scope:** `universe_cache.db` (table: `universe`) + `pipeline_engine.py` financial ingestion  
**Risk Rating:** 🔴 **HIGH** — Zero bitemporal stamps, zero PIT enforcement

---

## 1. Current State: Zero Bitemporal Stamps

The `universe` table in `universe_cache.db` contains **no date-stamping columns whatsoever** for financial records.

### Existing columns (17 total):

| Column | Type | PK | Purpose |
|---|---|---|---|
| `ticker` | TEXT | YES | Primary key |
| `name` | TEXT | | Company name |
| `cik` | TEXT | | SEC CIK identifier |
| `market_cap` | REAL | | Latest market cap |
| `mcap_band` | TEXT | | MEGA/MID/SMALL band |
| `mcap_asof` | TEXT | | ISO timestamp of last mcap fetch |
| `pillar` | TEXT | | Primary pillar |
| `pillars` | TEXT | | Comma-joined pillar set |
| `fmp_industry` | TEXT | | FMP industry classification |
| `status` | TEXT | | ACTIVE/WATCH/VETTED/WIKI |
| `conviction` | INTEGER | | Conviction score |
| `best_track_score` | REAL | | Best track score |
| `track_tier` | TEXT | | A/B/C tier |
| `promote_to_deepdive` | INTEGER | | Promotion flag |
| `sources` | TEXT | | Comma-joined sources |
| `updated` | TEXT | | Row update timestamp |
| `mcap_attempts` | INTEGER | | Market cap fetch attempts |

### Absent fields (all zero occurrences):

| Field | Expected Source | Status |
|---|---|---|
| `sec_accepted_at` | FMP `acceptedDate` | ❌ NOT FOUND |
| `period_end_date` | FMP `periodEndDate` / `calendarDate` | ❌ NOT FOUND |
| `acceptedDate` | FMP `acceptedDate` | ❌ NOT FOUND |
| `fillingDate` | FMP `filingDate` | ❌ NOT FOUND |
| `asOfDate` | FMP `asOfDate` | ❌ NOT FOUND |
| `reportDate` | FMP `reportDate` | ❌ NOT FOUND |
| `fiscalDateEnding` | FMP `fiscalDateEnding` | ❌ NOT FOUND |

**The `updated` column on the `universe` table is a row-level pipeline timestamp, NOT a financial record timestamp.** It reflects when the row was last modified by the pipeline, not when any financial data was accepted by the SEC.

---

## 2. Ingestion Gap Analysis

### 2.1 `financial_scoring()` (line 462–560)

**Function signature:** `financial_scoring(km, inc, incg, bal)`

| Input | Source | Date fields extracted? |
|---|---|---|
| `km` (key-metrics) | FMP `key-metrics` endpoint (quarterly, limit 4) | ❌ NO |
| `inc` (income-statement) | FMP `income-statement` endpoint (annual, limit 2) | ❌ NO |
| `incg` (income-statement-growth) | FMP `income-statement-growth` endpoint (annual, limit 3) | ❌ NO |
| `bal` (balance-sheet) | FMP `balance-sheet-statement` endpoint (annual, limit 2) | ❌ NO |

**Return value:** Dict with `financial_100`, `adjusted`, `data_coverage`, `gap_bands`, `sub_scores`, `raw` — **zero date fields.**

**Gap:** The function receives raw FMP dicts that contain date fields (`asOfDate`, `periodEndDate`, `calendarDate`, `filingDate`) in the first element of each list, but discards them entirely. The returned financial score is an unlabeled point-in-time value with no temporal anchoring.

### 2.2 `phase_deepdive()` → `_fetch_one()` (line 1246–1375)

**FMP calls per ticker (9 calls, parallel):**

| Call | FMP Endpoint | FMP Endpoint Type | Date fields in response? | Stored? |
|---|---|---|---|---|
| `profile` | `company/profile-symbol` | Profile | `sector`, `industry`, `marketCap`, `price`, `cik` — **no financial dates** | ✅ Stored (name, cik, sector, industry, mcap, price) |
| `quote` | `quote` | Quote | **no period/acceptance dates** | ✅ Stored (price) |
| `key-metrics` | `statements/key-metrics` | Periodic metrics | `asOfDate`, `reportDate`, `calendarDate`, `filingDate` per row | ❌ **NOT stored** — only metrics extracted via `financial_scoring()` |
| `income-statement` | `statements/income-statement` | Periodic statement | `periodEndDate`, `calendarDate`, `filingDate`, `reportDate` per row | ❌ **NOT stored** — stored as raw dict but dates not extracted |
| `income-statement-growth` | `statements/income-statement-growth` | Periodic statement | `periodEndDate`, `reportDate`, `calendarDate`, `filingDate` per row | ❌ **NOT stored** |
| `balance-sheet-statement` (annual) | `statements/balance-sheet-statement` | Periodic statement | `periodEndDate`, `calendarDate`, `filingDate`, `reportDate` per row | ❌ **NOT stored** |
| `balance-sheet-statement` (quarterly) | `statements/balance-sheet-statement` | Periodic statement | Same as above | ❌ **NOT stored** |
| `income-statement` (quarterly) | `statements/income-statement` | Periodic statement | Same as above | ❌ **NOT stored** |
| `cashflow-statement` (quarterly) | `statements/cashflow-statement` | Periodic statement | Same as above | ❌ **NOT stored** |

**Briefing row structure (line 1312–1327):**

```python
row = {
    "status": "OK",
    "name": ..., "cik": ..., "sector": ..., "industry": ...,
    "market_cap": ..., "price": ..., "quote_name": ..., "entity_check": "ok",
    "financial_100": fin,           # financial_scoring() output — zero dates
    "bifurcation": bif,            # bifurcation_scoring() output — zero dates
    "catalyst_context": ...,       # catalyst context — zero dates
    "watchlist": ...,              # watchlist state — zero dates
    "technical": ...,              # technical signals — only `as_of` string
    "insider": ...,                # insider signals — `latest_quarter` string only
    "short_interest": ...,         # short interest — zero dates
    "pre_print": ...,              # earnings calendar window — has date but not PIT
}
```

**Critical gap:** The entire `briefing` dict stored to `/tmp/dd_briefing.json` and passed to the LLM carries **zero period-end dates or SEC acceptance dates** on any financial record. The raw FMP data is present as nested dicts, but no date extraction is performed.

### 2.3 `bifurcation_scoring()` (line 563–869)

**Function signature:** `bifurcation_scoring(bal_a, bal_q, inc_q, cf_q, market_cap, kdata, bif_8k)`

**Return value:** Dict with `track_a_engine`, `track_a_p1`, `track_b_engine`, `track_c_engine`, `signals`, `flags`, optionally `_8k` — **zero date fields.**

The function receives quarterly and annual statement rows that each contain date fields (`periodEndDate`, `reportDate`, `calendarDate`, `filingDate`), but:
- Uses `inc_q[0]` as "latest quarter" with no verification that the data is current
- Uses `inc_q[4]` for YoY comparison with no verification of temporal validity
- Computes QoQ and YoY deltas assuming row order == temporal order but has no timestamp to validate this
- **No guard against stale data** — if an FMP row is 3 quarters old, the engine treats it as current

### 2.4 `_technical_signals()` (line 891–928)

**Only date-related field:** `as_of: str(rows[-1].get("date", ""))[:10]` — the last price bar's date. This is a **technical data** date, not a financial statement date. It's the closest thing to a temporal stamp in the entire codebase.

### 2.5 `_earnings_preprint_window()` (line 1052–1148)

Extracts `date` from the earnings calendar and converts to timestamp. This is a **forward-looking earnings date**, not a historical financial statement date. It does not anchor financial data to a point-in-time.

---

## 3. Query Audit Results

### 3.1 Direct date-field queries in `pipeline_engine.py`

| Search Pattern | Matches | Notes |
|---|---|---|
| `sec_accepted_at` | 0 | Never referenced |
| `period_end_date` | 0 | Never referenced |
| `acceptedDate` | 0 | Never referenced |
| `fillingDate` | 0 | Never referenced |
| `asOfDate` | 0 | Never referenced |
| `periodEndDate` | 0 | Never referenced |
| `bitemporal` | 0 | Never referenced |
| `point.*in.*time` | 0 | Never referenced |

### 3.2 Indirect date references (non-financial)

| Pattern | Location | Context |
|---|---|---|
| `updated` | Line 2023, 2414, 2656 | Row pipeline timestamps (not financial) |
| `mcap_asof` | Line 2023 | Market cap fetch timestamp |
| `as_of` | Line 924, 934, 944 | Technical signals + weekend tech map freshness |
| `date_created` | Line 1226, 2066, 2213 | Brief/catalyst creation timestamps |
| `last_updated` | Alpha vectors, regime state | Schema-level timestamps |
| `asof` | equity_verdicts, ticker_resolution | Verdict timestamps (epoch float) |
| `timestamp` | Line 118, 224, 1134 | Time parsing utilities |

**None of these date references are on financial statement data.**

### 3.3 PIT Rule Verification

**Result: NO PIT rule exists.**

- No code checks `sec_accepted_at <= T` (where T = "current time of analysis")
- No code validates that financial data was filed by a specific date
- No code guards against data revisions (e.g., a restatement that makes a company look better retroactively)
- No code prevents using forward-looking data that wouldn't have been available at a historical point-in-time
- The `financial_100` score is computed from raw FMP data with no temporal anchoring — it is effectively a **latest-available-data** score, not a **point-in-time** score

---

## 4. Schema Diff for PIT Compliance

### 4.1 Proposed ALTER TABLE statements

```sql
-- On the `universe` table, add a JSON column for bitemporal financial metadata:

ALTER TABLE universe ADD COLUMN fin_asof TEXT;
-- ISO timestamp of the most recent financial data snapshot (pipeline-level).
-- Equivalent to current `updated` but scoped to financial scoring only.

ALTER TABLE universe ADD COLUMN fin_source TEXT;
-- 'manual' | 'fmp_deepdive' | 'fmp_batch' — data provenance for audit trail.

ALTER TABLE universe ADD COLUMN fin_data_coverage REAL;
-- Fraction of required financial fields present (0.0–1.0).
-- Carries over from the existing `data_coverage` in financial_scoring()'s output.
```

### 4.2 Proposed `financial_records` table (new)

```sql
CREATE TABLE financial_records (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT    NOT NULL REFERENCES universe(ticker),
    endpoint        TEXT    NOT NULL,         -- 'income-statement' | 'balance-sheet' | 'cashflow' | 'key-metrics'
    period          TEXT    NOT NULL,         -- 'annual' | 'quarterly'
    period_end_date TEXT    NOT NULL,         -- FMP 'periodEndDate' / 'reportDate'
    filing_date     TEXT,                      -- FMP 'filingDate' / 'acceptedDate'
    asof_date       TEXT,                      -- FMP 'asOfDate' / 'calendarDate'
    sec_accepted_at TEXT    NOT NULL,          -- SEC acceptance timestamp (primary PIT key)
    data            TEXT    NOT NULL,          -- Raw FMP JSON array for this endpoint/period
    score_100       INTEGER,                   -- financial_100 score at time of ingestion
    score_adjusted  INTEGER,                   -- adjusted score
    data_coverage   REAL,                      -- 0.0–1.0 data completeness
    ingest_time     TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE(ticker, endpoint, period_end_date, sec_accepted_at)
);

-- Indexes for PIT queries:
CREATE INDEX idx_fin_records_ticker_sec ON financial_records(ticker, sec_accepted_at);
CREATE INDEX idx_fin_records_ticker_period ON financial_records(ticker, period_end_date);
CREATE INDEX idx_fin_records_ticker_scoring ON financial_records(ticker, score_100);
```

### 4.3 Ingestion code changes required

**`financial_scoring()`** — must accept and return date fields:

```python
# Before: def financial_scoring(km, inc, incg, bal):
# After:  def financial_scoring(km, inc, incg, bal, timestamps=None):

# Add to return dict:
#   "period_end_dates": {
#       "income_statement": "2026-09-30",
#       "balance_sheet": "2026-09-30",
#       "key_metrics": "2026-09-30",
#       "income_statement_growth": None,  # growth data has no period end
#   },
#   "sec_accepted_at": {
#       "income_statement": "2026-11-01",
#       "balance_sheet": "2026-11-01",
#       ...
#   },
#   "pit_valid": True/False,  # All sec_accepted_at <= T where T = current analysis time
```

**`phase_deepdive()` → `_fetch_one()`** — must extract and validate dates:

```python
# For each FMP call, extract:
#   row["period_end_date"] = item.get("periodEndDate") or item.get("reportDate")
#   row["sec_accepted_at"] = item.get("acceptedDate") or item.get("filingDate")
#   row["asof_date"] = item.get("asOfDate") or item.get("calendarDate")

# Then: row["financial_100"] now carries date metadata
#       row["bifurcation"] now carries date metadata
#       Brief stores these on disk and in the DB
```

**`bifurcation_scoring()`** — must accept and validate:

```python
# Add parameter: asof_time (datetime)
# Validate: all period_end_dates <= asof_time
# Validate: all sec_accepted_at <= asof_time
# Return: "pit_valid": True/False, "stale_data": [...periods that violate PIT]
```

### 4.4 Query audit additions required

After schema changes, the following queries must be added to the pipeline:

```sql
-- PIT compliance check: is all financial data filed before analysis time?
SELECT ticker, period_end_date, sec_accepted_at, endpoint
FROM financial_records
WHERE sec_accepted_at > :analysis_time;  -- PIT VIOLATION

-- Data staleness check: is the latest data > N quarters old?
SELECT ticker, MAX(period_end_date) as latest_period
FROM financial_records
WHERE period = 'quarterly' AND endpoint = 'income-statement'
GROUP BY ticker
HAVING latest_period < :cutoff;  -- STALE

-- Revision detection: has data for a ticker been updated since last analysis?
SELECT ticker, period_end_date, sec_accepted_at, ingest_time
FROM financial_records
WHERE ticker = ? AND period_end_date IN (
    SELECT period_end_date FROM financial_records WHERE ticker = ?
)
AND ingest_time > :last_analysis_time;  -- REVISION DETECTED
```

---

## 5. Risk Assessment

| Risk Area | Severity | Description |
|---|---|---|
| **No PIT enforcement** | 🔴 HIGH | `financial_100` scores are computed on latest-available data with no temporal validation. A company that was poor 4 quarters ago but improved last quarter receives the same score as a company that has been improving for 8 quarters. |
| **No data staleness guard** | 🟡 MEDIUM | No check whether FMP data is current. If FMP returns stale data (e.g., Q1 data in Q4), scores are computed on outdated financials with no warning. |
| **No revision detection** | 🟡 MEDIUM | SEC filings are routinely restated (10-K/10-Q amendments, 8-K material changes). Without `filingDate` tracking, the system cannot detect when a previously-good company has had its financials revised downward. |
| **No forward-leakage protection** | 🟠 HIGH | No guard against using data that wouldn't have been available at historical points-in-time. This is critical backtesting integrity — scores computed on data that didn't exist yet at time T are invalid for historical analysis. |
| **No audit trail** | 🟡 MEDIUM | The `/tmp/dd_briefing.json` file is ephemeral (overwritten each run). Without persistent financial record storage, there's no way to reconstruct what data was used for any past score. |
| **No data provenance** | 🟢 LOW | `updated` timestamp exists but is a pipeline-level timestamp, not a data-level timestamp. No record of which FMP data source was used for which score. |

### Overall Risk Rating: 🔴 HIGH

**Justification:** The system computes and persists financial quality scores (`financial_100`) on financial data that has **zero temporal anchoring**. Every score is effectively an opaque point with no way to answer: "Is this score based on Q3 2026 data or Q4 2026 data?" or "Was this data available at the time of the investment decision?" or "Has the underlying data been revised since we last scored?"

This is a fundamental data integrity gap that affects every downstream decision (promotions, wiki admissions, conviction scoring, wiki notes).

---

## 6. Summary

| Metric | Value |
|---|---|
| Total universe table columns | 17 |
| Columns with financial timestamps | 0 |
| Columns with any date stamp | 0 (only row-level `updated`) |
| Ingestion functions with date extraction | 0 of 3 |
| PIT rule enforcement code | 0 |
| Proposals for PIT columns | 3 (`fin_asof`, `fin_source`, `fin_data_coverage`) |
| Proposed new table | 1 (`financial_records`) |
| Required function signatures changed | 3 (`financial_scoring`, `phase_deepdive`, `bifurcation_scoring`) |

**Bottom line:** The financial ingestion pipeline treats all FMP data as "current" with no temporal validation. Implementing bitemporal tracking would add ~30 lines of date extraction code and one new SQLite table, but the investment prevents scoring on stale, revised, or forward-leaking data — a prerequisite for any credible systematic equity research system.
