# LOU SYSTEM TOPOLOGY — agent-to-agent handoff document
_Last verified live: 2026-11-04. Single node: ASUS GX10, Hermes Agent (Nous Research), one persona (Lou). All state is local to /home/jaren._

## 1. Runtime
- Hermes Agent, isolated cron sessions (10-min wall clock per job, no shared context between jobs).
- Scheduler: `~/.hermes/cron/jobs.json`. CLI: `hermes cron run <job_id>` / `hermes cron jobs`. NO `cronjob(action='run')` tool exists — handoff is file-based only.
- Delivery: Discord (Home channel 1511514381555601421) or `local` (files under `~/.hermes/cron/output/`).
- Shared state bus: `~/.hermes/cron/intel_bus/signals_bridge.json` (always written; `/shared` mirror absent on host — best-effort only). `pipeline_health_gate.py` carries a bus sentinel (bus down => verdict FAIL).

## 2. Knowledge base (the "Karpathy wiki")
- Root: `/home/jaren/wiki` (git repo). Dead path: `~/.hermes/cron/wiki/` — writes there FAIL the drift sentinel.
```
/home/jaren/wiki/
├── index.md / SCHEMA.md / log.md / research_interest.md   (root-level, NOT under wiki/)
├── golden_tier_list.md / emerging_edge_list.md / mirage_short_list.md / watchlist_changelog.md
├── raw/{articles,transcripts,metrics}/        # READ-ONLY ingestion
└── wiki/
    ├── companies/          # ~869 atomic markdown dossiers
    ├── sectors/ concepts/ comparisons/ research/ signal_briefs/
    └── archived/ archived_archive/
```
- Dossiers are dual-form: `companies/<T>.md` AND `companies/<T>/<T>.md`; the DIRECTORY form is authoritative. Never write root .md when a same-name dir exists.
- Notes append idempotently via deterministic brief_id = md5(normalized headline) — never uuid4. Strip today's notes before re-appending.
- `companies/index.json` is metadata-only (no live consumer); root tier lists are the live curation outputs.
- Market cap ceiling: soft ~$300B (200–300B allowed + flagged, ≥300B drop, null-CIK = fund = drop).
- Details + migration rules: skill `wiki-directory-structure`.

## 3. Databases
- **Markdown wiki** = primary persistent store (ticker history, catalysts, tiers).
- **sqlite3** = local DB standard (no duckdb on host): `universe_cache.db` (FMP market-cap cache, deterministic 6h refresh), z-score DB, drift DB.
- **JSON message files** = inter-job bus (no shared memory, no DB between LLM stages):
  `feed_raw.json` → `signal_briefs.json` → `vetted_tickers.json` → deep-dive outputs; `pipeline_state.json` (stage status), `error_log.txt`, `confluence_scores.json`, `processed_ids.json` (dedup ingestion log), `market_cap_watchlist.json`.
- FMP market cap: `batch-market-cap` SILENTLY DROPS ROWS — `profile-symbol` per ticker is authoritative.

## 4. Data sources / tools
- **FMP (Ultimate plan)**: REST API (v3/v4/stable) is 100% DEAD (403, legacy shutdown Aug 2025). Only working transport: SSE Streamable-HTTP MCP endpoint (key as `apikey=` in URL) → tools `mcp__fmp__*`. Also script-reachable: stdlib client `~/.hermes/scripts/fmp_client.py` + `fmp_key.local` (600). Sub-ops via `endpoint=` param.
- **Exa MCP** (`mcp__exa_search__web_search_exa`) — news/structural catalyst search. Needs HTTP/1.1 (skill `exa-mcp-protocol`).
- **SearXNG MCP** (`mcp__searxng__web_search`) — self-hosted, zero-cost fallback.
- **data-explorer MCP** (`mcp__data_explorer__*`) — local file read/write/edit; restricted to whitelisted dirs (non-whitelisted paths silently "Access denied").
- **EDGAR** (Atom feeds, fast lane) — filings/8-K; **vLLM** — thesis verdict generation (skill `thesis-forge`).
- Search query rule: NO `site:` queries in pipeline config (purged 2026-10-30).

## 5. Cron jobs (all live, verified 2026-11-04)
| Job | Cadence | Role |
|---|---|---|
| Sources: Agents 1-6 | 0 */2 * * * | Multi-source feed ingest → feed_raw.json |
| Processing: Meat Parser + Scribe + Orchestrator + Conviction Filter | 15 */2 * * * | signal_briefs.json, wiki updates, vetted_tickers.json |
| Deep-Dive: FMP Vet + 100-pt Bifurcated Scoring | 30 */2 * * * | Per-ticker deep score + self-evaluation |
| 13F Delta Radar | 1h | Elite-fund new positions (deterministic; Q3 burst ~Nov 14) |
| After-Hours Deep Research | 90m | Tracked-universe catalyst sweep (open web) |
| Earnings 8-K Fast-Pass | 10m | T+2m pre-call window, watchlist-scoped |
| FMP Bottom-Up Anomaly Feeder | 6h | Insider/volume/revenue lanes |
| Universe Cache Refresh | 6h | universe_cache.db mcap fill (deterministic) |
| Short-Interest Batch | 6h (biweekly cycle) | FINRA, watchlist-scoped |
| Drift Detector | 6h | Substitution scan + win/lose judgment + file events |

Pipeline chain (sector scans): pillar scans → Second Filter (data-quality gate) → Z-Score/Conviction → Golden Tier (`generate_golden_tier.py`) → wiki population. Scoring: Golden Tier Confluence 100-point system (skill `ticker-golden-tier-scoring`).

## 6. Skills used during cron (load by name in prompts)
- `fmp-data-exhaustion` — FMP transport quirks, endpoint dead-ness, mcap ceiling.
- `wiki-directory-structure` — canonical tree, dual-form quirk, write/merge strategy.
- `cron-pipeline-architecture` — handoff rules, prompt-length rules, state machine, primary-feed layer.
- `pillar-pipeline-architecture` — sector scan stages, MCP naming/call rules, pitfalls.
- `ticker-golden-tier-scoring` — 100-pt confluence scoring.
- `13f-radar`, `fmp-anomaly-feeder`, `entity-gate`, `feed-ticker-validation`, `stale-handoff-watchdog`, `cascade-invariant-check`, `loop-circuit-breaker`, `thesis-forge`.
- Reference skills (human-facing context): `multi-agent-research-architecture`, `duckdb-analyst`, `research-pipeline`.

## 7. Hard rules for any agent touching this system
1. Cron prompts < 3,500 chars; long logic goes in a Python script run via terminal/execute_code, never inline in the prompt.
2. MCP calls ONE AT A TIME in cron sessions (batched individual calls silently dropped; native batch endpoints are fine).
3. MCP tool names: double underscore `mcp__fmp__quote` — single-underscore names are invalid.
4. Handoff = write file, exit, let the schedule fire. Never wait for the downstream job.
5. Cron jobs need `enabled_toolsets: ["mcp","terminal","file","code_execution"]` or MCP tools silently absent.
6. Dedup everything (stateless dedup per run + processed_ids.json across runs); stale handoff files must be age-checked before consumption.
7. Atomic writes (.tmp then rename); schema-validate every stage; record to pipeline_state.json.
8. Wiki structural changes: full tar backup → dry-run engine → apply → patch consumers (pipeline_engine.py, health gate, jobs.json prompts, tier_lists.py) → verify counts → git commit.
9. FMP: REST dead, MCP/Streamable-HTTP only; profile-symbol authoritative for mcap; NUS = Nu Skin (NOT NuScale).
10. 8-K/EDGAR, 13F, and short-interest lanes are deterministic scripts — LLM never rewrites queries; queries live in config files with a hash stamped per run for audit.

## 8. Recursion — how the system feeds on itself
The pipeline is a closed loop, not a feed: every output is an input to the next cycle.

**Loop 1 — Discovery → Memory → Discovery (core loop).**
Sources → briefs → wiki dossiers → next cycle's scan. `pipeline_engine.ticker_universe()` reads `wiki/companies/`, so what gets scanned is defined by what was discovered. History is never purged; each pass runs conditioned on all prior passes. The wiki is both the output sink and the input prior.

**Loop 2 — Selection pressure (watchlist feedback).**
Scoring writes root tier lists + `market_cap_watchlist.json` → the watchlist-scoped lanes (8-K Fast-Pass every 10 min, Short-Interest, After-Hours Research) rescan ONLY tracked tickers. High-ranked names get more frequent, multi-source rescans; conviction is temporal — confluence scoring requires 3+ independent sources within 72h, so a ticker only reaches CONVICTION by re-surfacing across runs. Rankers are scanned harder; losers fade to archive (demote, never delete).

**Loop 3 — Temporal self-reference.**
Every run is evaluated against the system's own past outputs: processed_ids.json (seen-before dedup), adversarial validator (90% similarity vs last 3 runs), 14-day rolling signal_briefs.json, Drift Detector win/lose judgments on prior tiering. Present state is always delta-checked against recorded past state.

**Loop 4 — Self-monitoring (the system grades itself).**
pipeline_health_gate.py emits a verdict every run; intel-bus sentinel (bus down → FAIL); per-feed error_streak auto-demotes a feed after 7 consecutive real failures (fault vs. dead-feed distinguished via cooldown); ghost-job audit (`last_run_at: null`); error_log.txt + pipeline_state.json let the orchestrator consume failures from every stage. Degradation is provable, never assumed.

**Loop 5 — Memory recursion (skills as self-modifying instructions).**
Every failure mode learned gets written into a skill (`fmp-data-exhaustion`, `entity-gate`, `stale-handoff-watchdog`…) that cron prompts load by name — the system's record of its own failures reshapes the behavior of all future runs. Memory notes are injected into every session. The pipeline literally edits its own operating manual between runs.

**Loop 6 — State recursion (caches feed themselves).**
universe_cache.db refreshes from FMP and is read by scans; universe defines scans; scans extend the wiki; wiki defines the universe.

**Fixpoint properties (why the loops don't diverge).**
- Brief ids are deterministic (md5 of normalized headline) → re-scans are idempotent; a re-run cannot double-write a note.
- Dedup logs (processed_ids, stateless in-run dedup) bound growth of the feed.
- Feed auto-demotion + cooldown timers bound the source layer.
- Archive-not-delete + drift sentinels bound the wiki layer.
Net effect: cycles are safe to repeat indefinitely — the system converges on the same truth state no matter how often it is re-run.
