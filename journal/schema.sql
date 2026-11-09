CREATE TABLE IF NOT EXISTS tickers (
    ticker TEXT PRIMARY KEY,
    company_name TEXT,
    sector TEXT,
    industry TEXT,
    market_cap REAL,
    price_at_entry REAL,
    entry_date TEXT NOT NULL,
    best_track_score REAL,
    tier TEXT,
    track_a_score REAL,
    track_b_score REAL,
    track_c_score REAL,
    financial_100 REAL,
    catalyst_100 REAL,
    valuation_100 REAL,
    technical_100 REAL,
    status TEXT DEFAULT 'ACTIVE',
    demotion_reason TEXT,
    notes TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS price_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    snapshot_date TEXT NOT NULL,
    close_price REAL,
    open_price REAL,
    high REAL,
    low REAL,
    volume INTEGER,
    FOREIGN KEY (ticker) REFERENCES tickers(ticker)
);

CREATE TABLE IF NOT EXISTS forward_returns (
    ticker TEXT NOT NULL,
    window_days INTEGER NOT NULL,
    return_pct REAL,
    calculated_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (ticker, window_days),
    FOREIGN KEY (ticker) REFERENCES tickers(ticker)
);

CREATE TABLE IF NOT EXISTS journal_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    ticker TEXT,
    cycle TEXT,
    details TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_price_snapshots_date ON price_snapshots(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_price_snapshots_ticker_date ON price_snapshots(ticker, snapshot_date);
CREATE INDEX IF NOT EXISTS idx_forward_returns_ticker ON forward_returns(ticker);
CREATE INDEX IF NOT EXISTS idx_tickers_status ON tickers(status);
