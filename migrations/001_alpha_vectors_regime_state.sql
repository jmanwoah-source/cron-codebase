-- Migration 001: alpha_vectors + regime_state (canonical schema, user-provided)
-- Target: /home/jaren/.hermes/cron/universe_cache.db
-- Idempotent: safe to re-run.
-- Enforce write-ahead log & 5-second busy timeout for multi-cron concurrency
PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 5000;
PRAGMA synchronous = NORMAL;

-- ============================================================================
-- 1. ALPHA VECTORS: Per-Ticker Quantitative & Fundamental Enrichment
-- Sources: FMP endpoints 4, 5, 6, 7, 8, 10, 11, 12, 14
-- ============================================================================
CREATE TABLE IF NOT EXISTS alpha_vectors (
    ticker                      TEXT PRIMARY KEY,
    cik                         TEXT,
    company_name                TEXT,
    sector_pillar               TEXT NOT NULL,            -- 'SMH', 'URA', 'BOTZ', 'DEFENSE'
    mcap                        REAL NOT NULL,            -- Filtered strictly: $500M <= mcap <= $300B
    price                       REAL,

    -- [EP-5] Float & Kinetic Setup (company/shares-float)
    float_shares                REAL,
    free_float_pct              REAL,
    float_turnover_5d           REAL,                     -- 5d rolling volume / float_shares

    -- [EP-6] Fortress Health (statements/financial-scores)
    altman_z                    REAL,                     -- <1.8 = distress, >3.0 = safe
    piotroski_f                 INTEGER,                  -- 0-9; >=7 triggers Track A qualification

    -- [EP-7 & EP-8] Valuation Anchor (discountedCashFlow + analyst/price-target-consensus)
    dcf_standard                REAL,
    dcf_levered                 REAL,
    pt_consensus                REAL,
    analyst_count               INTEGER DEFAULT 0,
    blended_upside_pct          REAL,                     -- 0.5 * ((dcf_levered - price)/price) + 0.5 * ((pt_consensus - price)/price)

    -- [EP-4] Growth & Revision Signals (analyst/financial-estimates)
    rev_est_next_yr             REAL,                     -- Denominator for Contract-to-Consensus ratio
    eps_revision_delta_30d      REAL,                     -- Upward inflection signal

    -- [EP-10] Trojan Horse Flag (statements/revenue-product-segmentation)
    trojan_segment_flag         INTEGER DEFAULT 0,        -- 1 if high-growth pure-play segment in legacy shell
    trojan_segment_name         TEXT,
    trojan_segment_rev_pct      REAL,                     -- Segment revenue / Total revenue (>= 0.25 threshold)
    trojan_segment_growth_yoy   REAL,                     -- Segment YoY growth (>= 0.30 threshold)

    -- [EP-12] Kinetic & Technical Setup (technicalIndicators)
    adx_14                      REAL,                     -- Trend strength
    atr_pct                     REAL,                     -- ATR(14) / price
    mfi_14                      REAL,                     -- Money Flow Index (accumulation indicator)
    macd_hist_slope             REAL,                     -- Momentum inflection direction

    -- [EP-14] Sector Relative Strength (indexes / marketPerformance)
    rs_20d_vs_pillar            REAL,                     -- Ticker 20d return minus Pillar index 20d return

    -- State & Hygiene
    data_quality_score          INTEGER DEFAULT 100,      -- Penalized if missing core fundamentals
    last_updated                INTEGER NOT NULL          -- Epoch timestamp (UTC)
);

CREATE INDEX IF NOT EXISTS idx_alpha_pillar_mcap 
    ON alpha_vectors(sector_pillar, mcap);

CREATE INDEX IF NOT EXISTS idx_alpha_kinetic 
    ON alpha_vectors(mfi_14, adx_14, free_float_pct);

CREATE INDEX IF NOT EXISTS idx_alpha_trojan 
    ON alpha_vectors(trojan_segment_flag) 
    WHERE trojan_segment_flag = 1;

-- ============================================================================
-- 2. REGIME STATE: Macro & Sector-Level Context
-- Sources: FMP endpoints 13, 14, 15
-- ============================================================================
CREATE TABLE IF NOT EXISTS regime_state (
    pillar                      TEXT PRIMARY KEY,         -- 'SMH', 'URA', 'BOTZ', 'DEFENSE', 'MACRO'
    benchmark_ticker            TEXT,                     -- e.g., 'SMH', 'URA', 'BOTZ', 'ITA', 'SPY'
    benchmark_price             REAL,
    benchmark_rs_20d            REAL,                     -- Benchmark vs SPY 20d performance

    -- [EP-13] Positioning & Crowding (commitmentOfTraders)
    cot_net_spec_zscore         REAL,                     -- Non-commercial net positioning standard deviations
    cot_extreme_crowding_flag   INTEGER DEFAULT 0,        -- 1 if z-score > 2.0 (exhaustion/red-team penalty)

    -- [EP-15] Macro Liquidity & Capex Pulse (economics)
    us10y_yield                 REAL,
    ism_new_orders              REAL,
    fed_defense_spending_yoy    REAL,
    macro_regime_verdict        TEXT,                     -- 'EXPANSION', 'CONTRACTION', 'STAGFLATION', 'NEUTRAL'

    last_updated                INTEGER NOT NULL          -- Epoch timestamp (UTC)
);
