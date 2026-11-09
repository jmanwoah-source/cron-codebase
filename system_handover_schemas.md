# LOU SYSTEM — HANDOVER: SCHEMAS, FILE MAP & WORKING EXAMPLES
_Generated 2026-11-04 from live inspection. Companion to `system_topology.md` (architecture + recursion). This file is the data contract: every schema below was dumped from a real run, not inferred._

## 0. Handover inventory (what to copy to hand the system over)
1. `~/.hermes/cron/system_topology.md` — architecture, recursion, hard rules.
2. This file — schemas, file map, examples.
3. `~/.hermes/cron/jobs.json` — the 10 live cron jobs (prompts included inside).
4. `~/.hermes/cron/pipeline_config.json` — engine config (wiki_dir, universe, thresholds).
5. `~/.hermes/cron/feeds.json` — deterministic primary-feed query config (hash-stamped).
6. `~/.hermes/skills/` — ~120 skills; the ones cron prompts load by name are listed in §8 of system_topology.
7. `~/wiki/` — git repo: 869 dossiers + tier lists + SCHEMA.md + AGENTS.md (operational rules) + log.md ledger.
8. `~/.hermes/cron/*.json` state files (§1–§8 below) — these ARE the system's memory; copy or the loops restart cold.
9. `~/.hermes/cron/scripts/` + `~/.hermes/scripts/` (fmp_client.py, primary_feeds.py, tier_lists.py, pipeline_engine.py, pipeline_health_gate.py, generate_golden_tier.py, rebuild_index.py, wiki_processor.py).
10. `universe_cache.db` + z-score/drift sqlite DBs; `fmp_key.local` (600).

## 1. Message bus (JSON files in ~/.hermes/cron/)
Pipeline order: feeds.json → feed_raw.json → signal_briefs.json → vetted_tickers.json → ticker_scores.json → tier_list_state.json → wiki .md files.

### feed_raw.json — raw list, no wrapper (604 items last run)
```json
{
  "source_name": "quantum_research",        // or patent_supply_chain, macro_government, primary_feed:<name>, etc.
  "source_type": "quantum",                 // 6 categories: semiconductors|materials|ai_infra|energy|quantum|edge_robotics
  "headline": "One step closer to quantum...",
  "url": "https://...",
  "date_discovered": "2026-10-12T00:00:00-04:00",
  "tickers_mentioned": ["IBM"],
  "text": "full article/snippet text"
  // primary-feed layer ADDS: text_source, severity, source_date
}
```

### signal_briefs.json — wrapper (276 briefs, 14-day rolling; history twin: signal_briefs_history.json)
```json
{
  "agent": "...", "version": "...",
  "run_timestamp": "ISO8601",
  "raw_items_processed": 604, "items_after_dedup": 272,
  "signal_briefs_generated": 4,
  "signal_briefs": [
    {
      "brief_id": "9e7d7e431bb9529e804db0ca281a1220",   // md5(normalized headline) — deterministic, idempotent writes
      "date_created": "ISO8601",
      "headline": "...", "summary": "...", "raw_text": "...",
      "category": "semiconductors",
      "tickers_benefit": ["SEM"],      // DIRECT mention — counts for all scoring dimensions
      "tickers_affected": ["TSEM"],
      "tickers_mentioned": ["TSEM","SEM"],  // generic mentions — boost cross-source ONLY, never brief_count
      "impact_direction": "positive|neutral|negative",
      "impact_level": "low|medium|high",
      "materiality": "direct|indirect|speculative",
      "confidence": "low|medium|high",   // ALWAYS string, title-case
      "source_name": "...", "source_urls": ["https://..."],
      "next_steps": "..."
    }
  ]
}
```

### vetted_tickers.json — conviction-filter output (processing → deep-dive handoff)
Tickers with conviction_score ≥ 75. Conviction dimensions (0–100): brief_count_direct 0-20 (counts BENEFIT/AFFECTED only), impact_direction 0-20, materiality 0-15, confidence 0-15, cross_source 0-30 (distinct source_name ∪ URL domains).

### ticker_scores.json — deep-dive scoring engine (105 ranked last run)
```json
{
  "generated": "ISO8601",
  "engine_session": "...", "red_team_asof": "ISO8601",
  "entity_mismatches": {}, "entity_review": [],
  "invalid_tickers_flagged": [], "mcap_policy_excluded": [],
  "mirage_list": [], "fetch_errors": [], "notes": [],
  "rankings": [
    {
      "ticker": "AEHR", "name": "Aehr Test Systems", "cik": "0001040470",
      "market_cap": 1973940180, "price": 60.44,
      "pillar_scores": {"financial_100": 36, "catalyst_100": 90, "valuation_100": 55, "technical_100": 55},
      "track_a_score": 80, "track_b_score": 79, "track_c_score": 100,
      "best_track_score": 80,                    // tier: HIGH >= 75
      "track_a_pillars": {"p1": 26, "p2": 15, "p3": 13, "p4": 26},
      "track_a_pillars_note": "P1=engine-fixed 26",
      "track_b_pillars_note": "carveout friction -5",
      "track_c_reasons": "severity=0; hype=0",    // Track C = mirage/short list (negative-signal track)
      "passes_track_a": true, "passes_track_b": true, "mirage": false,
      "watchlist_note": "2026-11-04: re-scored best=80 A=80 B=79 C=100 | 8-K: $33M rev +200% YoY ... waiver DENIED (GAAP gm QoQ -107bps < -50 floor)",
      "pre_print": null, "earnings_laned": null, "mcap_flag": null,
      "red_team": {"asof": "...", "thesis_kill_probability": 0.65, "kill_reason": "customer concentration ..."}
    }
  ]
}
```
Bifurcated scoring: Track A (fundamental fortress), Track B (catalyst/edge with friction penalties), Track C (mirage/short). Best-of-tracks determines tier.

### tier_list_state.json — curation state; rendered to root .md lists by tier_lists.py ONLY
```json
{"version": "1.0", "updated": "YYYY-MM-DD HH:MM",
 "lists": {"A": {...}, "B": {...}, "C": {...}}   // A=golden, B=emerging edge, C=mirage
}
```
Entry (A, 5 tickers / B, 2 / C, 14 last run):
```json
"INFY": {"name": "Infosys Limited", "market_cap": 50931414000, "score": 62.0,
  "engine": {"financial_fortress_40": 40, "raw_altman_z": 5.196, "raw_debt_to_equity": 0.1, "raw_fcf_yield_pct": 2.17},
  "evidence": ["40ff+10+6+6 off-pillar"],
  "entered": "...", "updated": "...", "fail_streak": 1,
  "history": ["2026-10-31 07:13 score=76.0", "..."]}
```

### Orchestrator / health reports
```json
// orchestrator_output.json
{"timestamp": "...", "feed_raw_valid": true, "signal_briefs_valid": true,
 "briefs_total": 276, "briefs_new": 4, "briefs_dropped_out_of_window": 1,
 "history_total": 443, "unique_tickers": 137, "validation_errors": []}

// engine_run_report.json (drift detector)
{"run": "2026-11-04", "results": [{"drift_id": "DRIFT-001", "composite": 432, "fetch_errors": []}]}

// cascade_state.json — per-drift auto-scaling state machine
"DRIFT-001": {"last_status": "scaling|emerging", "zero_streak": 0, "saturated": false, "last_composite": 432, "last_run": "..."}

// process_state.json
{"cumulative_briefs_processed": 464, "last_run": "..."}
```

## 2. Deterministic state (dedup / cursor / seen-tracking)
| File | Schema |
|---|---|
| `anomaly_feed_state.json` | `{"seen": {"insider:RAPP:HEALY JAMES:2026-10-30": epoch, "volume:XMAX:2026-10-31": epoch, "revenue:IONQ:2025": epoch}}` — key = `lane:ticker:[:name]:[:date]` |
| `ah_bridge_state.json` | `{"seen": {"TICKER:$value:normalized_headline": epoch}}` — intel-bus dedup |
| `13f_radar_state.json` | `{"runs": {"2026Q3": {"ts": "...", "status": "early_quit|complete", "filers": N, "failed": [names], "last_early_deliver": ...}}}` |
| `earnings_fastpass_state.json`, `anomaly_feed_state.json`, `universe_cache_report.json` | per-lane cursors / run reports |
| `processed_ids.json` | MD5(headline+url) seen log across runs |
| `query_health.json` (102KB) | per-query hit/miss history; drives phrase staging (promote after 2 days ≥3 hits) |
| `feed_state.json` (17KB) | per-feed cursor + error_streak + demoted (auto-demotion after 7 consecutive real failures; 429/500 = cooldown, NOT error_streak) |

## 3. feeds.json — primary-source feed layer (config, LLM never rewrites)
```json
{"comment": "...", "feed_cadence_hours": 6,
 "feeds": {"osti": {"enabled": true, "endpoint": "https://www.osti.gov/api/v1/records",
   "window_days": 1, "coldstart_window_days": 14, "phrases": ["small modular reactor", "uranium fuel cycle", ...]}}}
```
Feeds: OSTI (DOE), arXiv, EDGAR, Federal Register, EIA. Two record classes: EVENT feeds dedup into the delta stream; METRIC feeds (EIA) alert only on derivatives (MoM %), never raw values. `query_version` config hash stamped per run.

## 4. Wiki schemas (~/wiki)

### Dossier (wiki/companies/<T>/<T>.md — directory form authoritative)
```markdown
# AAPL
## Recent Catalysts
- [2026-10-15] [News] GF Eyes Two New York Fabs... | semiconductors (positive medium) [`9D4F31B9`]
## Full History
Auto-created by Scribe 2026-10-15T13:35:00Z.
- [2026-10-15] [News] ... (ambiguous low) [475E23EA]

### Dated Catalyst Notes:
- [2026-10-02] Sam Zeloof and Jim Keller start a new semiconductor fab (patent_supply_chain: positive)
```
Note line format: `- [YYYY-MM-DD] [8-char brief_id] headline | category (direction level)` — dedup key = case-insensitive brief_id; append idempotent. Brief ids are lowercase internally; old uppercase legacy exists.

### Tier list files (root, generated — never hand-edit)
`golden_tier_list.md` (Track A), `emerging_edge_list.md` (Track B), `mirage_short_list.md` (Track C), `watchlist_changelog.md` (dated deep-dive blocks).

### Governance
`AGENTS.md` (10 hard rules: one root, raw/ read-only, archive-never-delete, tier writes via tier_lists.py only, append-only log.md, git commits), `SCHEMA.md` (taxonomy), `index.md`, `log.md`.
Mcap policy: soft ~$300B (note: AGENTS.md says ~$100B — the live pipeline engine uses the 200-300B band; treat pipeline_engine.py as authoritative).

## 5. SQLite DBs
- `universe_cache.db` — ticker universe + FMP market cap (deterministic 6h fill; profile-symbol authoritative, batch-market-cap silently drops rows).
- z-score DB, drift DB — scores per run, keyed by ticker.
- sqlite3 is the standard (no duckdb on host).

## 6. Key file map (paths an external agent needs)
```
~/.hermes/cron/
├── jobs.json                  # 10 live jobs + prompts (source of truth for schedules)
├── pipeline_config.json       # engine: wiki_dir, universe, thresholds
├── feeds.json                 # primary feed queries (hash-stamped)
├── pipeline_engine.py         # ticker_universe(), wiki_note_path(), brief appends
├── pipeline_health_gate.py    # verdict + intel-bus sentinel + drift sentinels
├── tier_lists.py              # tier_list_state.json → root .md lists
├── generate_golden_tier.py    # 100-pt composite + cap multipliers
├── scripts/rebuild_index.py   # index rebuild (run after any schema change)
├── scripts/wiki_processor.py  # filtered_results + golden_tier → dossier .md
├── pillar_scan_data/          # per-sector scan JSONs (5 heterogeneous formats)
└── output/                    # local-deliver cron output
~/.hermes/scripts/
├── fmp_client.py              # stdlib Streamable-HTTP FMP client (REST is dead)
├── fmp_key.local (600)
└── primary_feeds.py           # deterministic fetcher for feeds.json
~/wiki/                        # see §4
```

## 7. Live job roster (verified 2026-11-04, all scheduled)
Sources 0m/2h → Processing 15m/2h → Deep-Dive 30m/2h (Discord deliver); 13F Radar 1h (local); After-Hours 90m (local); 8-K Fast-Pass 10m (local); FMP Anomaly Feeder 6h; Universe Cache 6h; Short-Interest 6h/biweekly (local); Drift Detector 6h.

## 8. Open items an external reviewer should know (known-weak spots)
1. 13F Radar Q3 burst: 1 of N filers succeeded, `status: early_quit` on 2026-11-04 run — all-filer failure alert skill exists; Q3 filings land ~Nov 14.
2. AGENTS.md mcap ceiling ($100B) vs live engine (~$300B) — doc drift; engine is authoritative.
3. FMP REST dead; entire FMP surface rides on one SSE MCP transport + one stdlib client. Single point of failure.
4. batch-market-cap silent row-drops — per-symbol profile-symbol is the only trustworthy mcap path (slow).
5. Dual-form dossier quirk (root .md + dir) survives; merge logic is migration-only, not cron-routed.
6. `companies/index.json` metadata-only, no live consumer.
7. AGENTS.md writes are approval-gated (times out headless).
8. 13F `early_quit` + anomaly `seen` maps grow unbounded (no TTL visible in these dumps — verify).
