# LOU CRON SYSTEM — FULL TOPOLOGY & HONEST ANALYSIS
**Built: 2026-11-06 (16:45 EST) by live host inspection.** Every number below was pulled from
`jobs.json`, `executions.db`, `usage_audit.jsonl`, the engine source, and live service probes on this
exact host — not from memory or from the older `system_topology.md` (2026-11-04, now stale in 5 places).
Companion file (schemas + examples): `~/.hermes/cron/system_handover_schemas.md`.

---

## 0. HONEST EXECUTIVE SUMMARY (read this first)

1. **It is 12 cron jobs, not a swarm of LLM agents.** 5 run now (3 LLM + 1 no_agent + 1 one-shot fired).
   7 are paused or done. The "multi-agent" identity of the system is **inside a 3,020-line stdlib-only
   Python engine** (`pipeline_engine.py`) that does all deterministic work with threads + a direct
   HTTP/SSE JSON-RPC client. LLM sessions are deliberately thinned out to a handful of judgment calls.
2. **The LLM is NOT the pipeline. The engine is.** The LLM's jobs are: quality-bar judgment (Sources),
   So-What brief summaries (Processing), qualitative track-scoring (Deep-Dive), drift judgment,
   after-hours materiality. Everything else — fetching, dedup, conviction math, financial scores, tier
   lists, validation, bridge writes, alert dispatch — is deterministic Python.
3. **The only real "agent fan-out" is thread pools inside the engine**: 4 concurrent search workers,
   5 concurrent FMP-fetch workers, 4–6 article-fetch workers. There is **no `delegate_task`
   subagent-fan-out anywhere in cron** (verified: zero references in every prompt and in the engine).
   The "6 agents 1–6" in the Sources job name are **query-tag labels inside config**, not 6 processes.
4. **One local LLM model powers the heavy reasoning**: Qwen3.8-27B (NVFP4) via vLLM on :8000,
   262k context, prefix-caching ON. It is used by the Thesis Forge, the 13F equity gate, and the
   Deep-Dive adversarial red-team tribunal. It is not the model running the cron sessions.
5. **Token burn is real and measured**: ~547M input / ~13M output tokens across 943 recorded runs
   (`usage_audit.jsonl`). The Deep-Dive job alone accounts for ~308M input. Average run lengths:
   Sources ~15.7m, Processing ~13.0m, Deep-Dive ~96m (30-day window — it is the long pole by design).
6. **Known open items (honest):** (a) one one-shot quant-upgrade job **aborted cleanly** because
   `~/wiki/QUANT_UPGRADE_SPEC.md` never landed — the upgrade is NOT applied; (b) 7 of 12 jobs are
   paused by design (overnight/manual lanes); (c) 8 historical Deep-Dive rows carry
   "Scheduler restarted before durable terminal state" — side-effect status unknown on those 8;
   (d) `batch-market-cap` is known-unreliable and the system treats per-symbol `profile-symbol`
   as the only authoritative mcap source.

---

## 1. RUNTIME & SCHEDULER

- **Host:** ASUS GX10 (aarch64, NVIDIA), user `jaren`. Hermes Agent, one persona (Lou).
- **Scheduler store:** `~/.hermes/cron/jobs.json` (12 jobs, `updated_at` 2026-11-06T16:39 local).
  Each job: schedule (cron expr or interval), prompt (the full agent instruction), `deliver` target,
  `no_agent` flag, `enabled_toolsets`, `monitor_script`, fire-claim/lock files (`.fire-*.lock`),
  failure-deliver hooks, dispatch lateness tracking (`last_dispatch` with `lateness_seconds`).
- **Wall clock:** agent jobs get a 10-minute harness window. **Exception pattern:** long deterministic
  work runs *outside* the harness in tmux (the "unharnessed" overnight wave, spec:
  `jane_street_braid_spec.md` V3 FINAL) to escape the 10-min cap and per-turn MCP call caps.
- **No `cronjob(action='run')` tool exists.** Inter-job handoff is **file-based only** (write a JSON
  bus file; the next scheduled job consumes it). CLI force-run is `hermes cron run <job_id>` (documented,
  but the standing rule is: hand off the file and let the schedule fire).
- **Telemetry:** `~/.hermes/cron/executions.db` (SQLite: `executions` 1,002 rows, `cron_incidents` 4)
  + `usage_audit.jsonl` (943 rows: prompt/completion tokens, duration, silence flag, deliver target).
- **Delivery targets:** Discord thread `discord:1511514381555601421:1561241901426942014`
  (High-Score/research thread) for the 3 live LLM jobs + 3 paused LLM jobs; `local`
  (`~/.hermes/cron/output/<job>/<run>.md`) for deterministic jobs. `[SILENT]` token suppresses
  delivery when nothing new; hash-stable monitor output enables "unchanged → suppressed".

## 2. JOB REGISTER (all 12, live state as of 2026-11-06)

| # | Job ID | Name | Schedule | Mode | State | Last run | Runs (30d) | Avg dur |
|---|--------|------|----------|------|-------|----------|-----------|---------|
| 1 | `ba24fd4ddad6` | Sources: All Research Agents 1–6 | `0 */2 * * *` | LLM (6.2KB… 5,650-ch prompt) | **ENABLED** | 16:00 today | 222 | 942s |
| 2 | `cfdab394b3bd` | Processing: Meat Parser + Scribe + Orchestrator + Conviction | `15 */2 * * *` | LLM (6,271-ch) | **ENABLED** | 16:33 today | 228 | 776s |
| 3 | `7267b662cf52` | Deep-Dive: FMP Vet + 100-Point Bifurcated Scoring + Self-Eval | `30 */2 * * *` | LLM (26,146-ch!) | **ENABLED** | 16:10 today (running) | 144 | 5,767s |
| 4 | `e9711f5abb15` | Drift Detector (substitution scan + win/lose judgment) | every 360m | LLM (12,615-ch) | PAUSED (11-04) | 11-04 14:21 | 33 | 1,582s |
| 5 | `bf59f02d39ba` | 13F Delta Radar (deterministic) | every 60m | LLM-thin (1,973-ch) | PAUSED (11-04) | 11-04 18:33 | 188 | 122s |
| 6 | `fd3a24115408` | After-Hours Deep Research (tracked universe, open web) | every 90m | LLM (3,288-ch) | PAUSED (11-04) | 11-04 18:46 | 55 | 503s |
| 7 | `3af8db9a6c2b` | FMP Bottom-Up Anomaly Feeder (insider/volume/revenue) | every 360m | **no_agent** → `fmp_anomaly_feed.py` | PAUSED (11-04) | 11-04 16:12 | 13 | 21s |
| 8 | `95617415066d` | Universe Cache Refresh (mcap fill) | every 360m | **no_agent** → `universe_refresh_cron.py` | PAUSED (11-04) | 11-04 16:12 | 17 | 7s |
| 9 | `bfd0b0750c17` | Earnings 8-K Fast-Pass (watchlist-scoped) | every 10m | **no_agent** → `earnings_fastpass.py` | PAUSED (11-04) | 11-04 19:13 | 85 | 45s |
| 10 | `64aa26f970d5` | Short-Interest Batch (biweekly FINRA cycle) | every 360m | **no_agent** → `short_interest_batch.py` | PAUSED (11-04) | 11-04 19:04 | 2 | 143s |
| 11 | `79d430b5f565` | Entity Universe Seed ($500M–$300B corridor harvest) | every 2160m | **no_agent** → `entity_universe_seed_cron.py` | **ENABLED** | 07:38 today | 1 | <1s |
| 12 | `297cbad05c47` | quant-upgrade-surgical-execution | one-shot (once, done) | LLM | COMPLETED (aborted by design) | 11-06 00:39 | 1 | 110s |

**Schedule interlock (the live trio):** Sources fires at :00, Processing at :15, Deep-Dive at :30
every 2h — a strict time-ordered chain with file handoffs, staggered to avoid API/scheduler collision.
Jobs 4–10 were paused together on 2026-11-04 (the "pillar lock" cutover day) and remain dark; the
anomaly feeder, fast-pass, short-interest, drift, 13F, and after-hours lanes are currently **manual/
overnight** rather than cron. Job 5 (13F) is paused pending the ~Nov 14 Q3 filing burst.

### 2.1 The live trio — exactly what each does

**Job 1 `ba24fd4ddad6` — Sources (5,650-char prompt, `enabled_toolsets: null` = terminal/file only):**
- STEP 0: run `pipeline_health_gate.py` (see §6), print verdict.
- STEP 1: `python3 pipeline_engine.py sources` — the engine runs **all 12 tuned queries**
  (`pipeline_config.json → sources.queries`, tags 1b,1c,2b,3a,3b,3c,4a,4c,5a,5b,6a,6b) against
  SearXNG (:8888) with a **4-worker ThreadPoolExecutor**; circuit breaker: ≥3 query errors flips the
  whole fan-out to Google News RSS (self-throttled ~1.7 req/s). Junk-domain drop (19 domains),
  ETF blocklist (47 tickers), common-word blocklist (~90 tokens), dedup vs `feed_raw.json` headlines+URLs,
  7-day freshness window, max 8 candidates/agent → `/tmp/sources_candidates.json`.
- STEP 1B: `python3 primary_feeds.py run` — **9 deterministic primary-source feeds**
  (config: `feeds.json`, hash-stamped `query_version`): osti (DOE), arxiv, sec_edgar full-text,
  edgar_atom (fast lane), federalreg, eia (metric feed — alerts on MoM derivatives only), gnews wires,
  fmp_pr (press-release firehose scoped to the $500M–$300B corridor), fmp_8k (same-day 8-K firehose).
  Cursor windows, per-feed health with auto-demotion after 7 consecutive failures, flock single-flight
  lock, hard cap 25 records/run appended to `feed_raw.json` (additive fields only).
- STEP 2 (the ONLY LLM work): keep/drop quality-bar judgment per candidate + suggested_ticker fixes
  (corridor rules: <$300B, $200–300B flagged near-ceiling; drop ETFs/foreign/common words).
- STEP 3: `pipeline_engine.py sources apply` — engine merges kept candidates into `feed_raw.json`
  (max 5 items/agent, 30 total), fetches article text in a **4–6 worker parallel pass**
  (`fetch_article_text` with browser fallback under its own lock), appends.

**Job 2 `cfdab394b3bd` — Processing (6,271-char prompt):**
- STEP 0: health gate.
- STEP 1: `pipeline_engine.py process` — dedup feed, diff vs existing briefs →
  `/tmp/process_items.json` (new items only). Zero new → skip to STEP 4.
- STEP 2 (the ONLY LLM work): So-What judgment per item — keep/drop + impact_direction,
  impact_level, materiality, confidence, 3–5 sentence summary, next_steps →
  `/tmp/process_briefs_final.json`.
- STEP 3: `pipeline_engine.py process apply` — engine merges into `signal_briefs.json`
  (rolling 14-day retention; 30-day history), computes **deterministic conviction scores**
  (brief count, impact, materiality, confidence, cross-source corroboration — direct-mention vs
  generic-mention separation), writes `vetted_tickers.json` tiered, appends idempotent wiki catalyst
  notes, rebuilds `~/wiki/research_interest.md`, writes `orchestrator_output.json`.
  **Does NOT trigger Deep-Dive** — the :30 schedule picks up `vetted_tickers.json`.
- STEP 4: `pipeline_engine.py tuner` — **Query Tuner**, self-gated to ≤1 pass/24h; scores the last
  12 ledger windows per query; LLM writes 2 candidate rewrites for flagged tags;
  `tuner apply` A/B tests candidates vs the original on SearXNG and commits only the winner to
  `pipeline_config.json` + `query_changes.jsonl`. The query list lives in config, never in prompts.
- STEP 5: `inclinations/run_inclinations.py` — the early-advantage ("inclination") detector:
  screen.py (rvol/insider/FCF), transcripts.py (R&D trend/design-win language), ip_signal.py
  (patents/8-K IP flags) — FMP-heavy sources self-gated to ≤1/8h — then aggregate.py unifies all
  signal sources (briefs, scores, watchlist, tech-gap wiki) into a 0–100 early-advantage score;
  ≥50 triggers Discord alerts (`inclination_alerts.md`).

**Job 3 `7267b662cf52` — Deep-Dive (26,146-char prompt; `enabled_toolsets: [mcp, terminal, file, code_execution]`):**
- STEP 0: health gate.
- STEP 1: `pipeline_engine.py deepdive` — the engine's heaviest phase. Opens ONE FMP MCP session,
  fetches **9–10 endpoints per ticker** (profile-symbol, quote, key-metrics q, income-statement a,
  income-statement-growth a, balance-sheet a, balance-sheet q, income-statement q, cashflow q,
  RSI-14 technical, insider-trade-statistics) for the full watchlist universe in a
  **5-worker ThreadPoolExecutor** (shared session, distinct call_ids, per-worker exception isolation).
  Computes deterministic `financial_100` (5×20pt bands from config, with `adjusted` = raw×(0.5+0.5·coverage)
  + `data_coverage` + `gap_bands`), the **bifurcation engine** sub-scores (Track A P1 = Altman Z /30 +
  deterministic Working-Capital waiver; Track B runway gate; Track C breakdown severity), plus
  pre-print / earnings-laned windows, 8-K fast-pass extract map, short-interest map, entity checks
  (CIK agreement between statements and profile → `ENTITY_MISMATCH` quarantine). Output:
  `/tmp/dd_briefing.json` (~1 min for 40–60 tickers).
- STEP 2 (LLM): `catalyst_100` for every ticker; `valuation_100`/`technical_100` only for promotion
  queue + HIGH tier (fetch depth is tier-controlled); Track A/B/C qualitative pillars under the
  2026-11-04 **pillar lock** (hard caps: A=30/20/20/30, B=20/25/30/25, dilution kill-switch −30 &
  cap-20; pre-print P4 cap 18/30). Writes `ticker_scores.json` + `evaluation_report.json`
  (self-evaluation: distribution, compression, over/under-scored flags, duplicate check).
- STEP 6a: `score_alert_75.py` — deterministic ≥75 alert dispatch to the high-score Discord thread
  (dedup state `score_alert_state.json`).
- STEP 6b: `tier_lists.py` — writes the three cumulative tier lists
  (`golden_tier_list.md` A≥75, `emerging_edge_list.md` B≥75, `mirage_short_list.md` C≤35) +
  runs `score_validation.py` which cross-checks LLM track scores against the engine's fixed
  sub-scores and REJECTS rows violating the caps (flagged rows excluded from lists).
  Lifecycle: demotion only after 2 consecutive sub-gate runs.
- STEP 6c: `signals_bridge.py` — machine-to-machine bridge for the RubberBandMan options scanner:
  deterministic schema (symbol, track, wiki_score, primary_benchmark, parent_whale, dilution_risk,
  thesis_hook, last_updated). Canonical bus: `~/.hermes/cron/intel_bus/signals_bridge.json` (always);
  `/shared/` mirror best-effort and absent on this host. LLM may curate `bridge_overrides.json` only.
- STEP 6d: `deep_dive_red_team.py` — the adversarial tribunal: every candidate with
  best_track ≥65 gets its sliced 10-K (sec_slicer corpus) + engine signals sent to **local vLLM** as a
  hostile short-seller committee → thesis_kill_probability, customer concentration, dilution, WIP stall.
  Candidates that also clear the options trigger (mcap ≥$500M) + direction are written to
  `options_delta_queue.db` (SQLite WAL) for the options scanner. ~9 min of vLLM for a full set;
  degrades to heuristic verdicts if vLLM down (never aborts).
- STEP 7: **watchlist pruner** (LLM, bi-weekly self-gated: most cycles skip entirely). Status machine
  ACTIVE/WATCH/DORMANT/ARCHIVED, 2-of-3 demotion rule, promote_to_deepdive hook (the ONLY path from
  WATCH to the ACTIVE bar of best_track ≥75), batched FMP quote calls (max 30/call),
  `watchlist_changelog.md` append.
- STEP 6 (final): `pipeline_engine.py health` — post-cycle pipeline health report →
  `pipeline_health.md` (last 10 kept). Verdict FAIL already fires the failure-deliver Discord alert.

### 2.2 Paused lanes — what they are (for the handoff reviewer)

- **Drift Detector (`e9711f5abb15`, every 360m):** deterministic substitution-language scan
  (`drift_scan.py`, 13 sector + 6 small-cap-winner-pattern queries from `pipeline_config.json → drift`,
  SearXNG, overlap lock, upstream-suspension guard) + `drift_ingest.py` (news-break merge from
  `drift_inbox.json`) → LLM judges + files `~/wiki/drifts/` with win/lose verdicts and a Trigger-v2
  **cascade** (eligible drifts get next-layer value-chain queries authored and scanned — "Leg B").
  Filing cap 1–2 base + 1 shadow (A2) per cycle.
- **13F Delta Radar (`bf59f02d39ba`, every 60m):** `radar_13f.py` (1,084 lines, zero LLM). Fixed cohort
  of **12 elite funds with pinned CIKs** (Renaissance 1037389, Millennium 1273087, Coatue 1135730,
  D1 1747057, Tiger 1167483, Verition 1454027, Pershing 1336528, Dymon Asia 1672142, Balyasny 1218710,
  Marshall Wace 1318757, PointState 1509842, Summit 1638555). Pulls last-quarter + prior-quarter 13Fs
  via the FMP MCP client, computes NEW/INCREASED/CLOSED deltas in Python, confluence-scores
  (≥7 → frontier), writes `frontier_candidates.json` + raw immutable copies
  (`raw_13f/<YYYY>Q<n>/<cik>.json`), routes frontier names into `watchlist_state.json` with
  `promote_to_deepdive=true`, wiki workpaper + log + own Discord summary. Self-gating:
  **early-quit if <10/12 filed** (a missing filing would flag every position CLOSED); no-op if
  quarter already processed. Q3 burst expected ~Nov 14 → re-enable then.
- **After-Hours (`fd3a24115408`, every 90m):** fills 16:00–10:00 ET. `market_hours.py` gates DEEP vs
  LIGHT mode; `ah_research_list.py` regenerates the tracked tier list (watchlist ACTIVE/WATCH + vetted
  tiers, per-symbol FMP mcap); `ah_research.py` pulls Google News RSS per tier (A: 6 queries/ticker,
  B: 2, C: 1; 14-day seen-ledger); LLM does materiality So-What (dilution-watch hard rule: any
  issuance/offering = high+negative regardless of spin); HIGH-impact findings file to wiki dossiers
  only (read-only toward scores); `ah_promote_bridge.py` single-writer atomic claim pattern
  (`os.replace` claim → no lost-update races with the 2h trio).
- **No-agent deterministic lanes (7, 8, 9, 10):** the scripts ARE the jobs; the cron prompt is just
  "run it bare, report stdout verbatim, do not debug per-ticker noise."
  - `fmp_anomaly_feed.py` (670 lines): 3 lanes — insider cluster detection, volume/RVOL anomalies,
    revenue disconnects (contract value ≫ mcap), via fmp_client.
  - `universe_refresh_cron.py` → `build_universe.py --refresh` (768 lines): rebuilds
    `universe_cache.db` from local state (watchlist/vetted/tiers/after-hours/wiki) + capped 150-call
    FMP profile-symbol mcap fill, priority conviction DESC / track_tier ASC, 3-attempt quarantine
    (mcap_attempts column, −1 sentinel).
  - `earnings_fastpass.py` (546 lines): polls EDGAR submissions for NEW earnings 8-K (Item 2.02)
    in the lookback window, resolves the exhibit via index.json, regex-extracts the numbers
    (revenue, GM, EPS, bookings, book-to-bill, backlog/RPO, guidance flags), scores under the
    pillar lock (P1 /30 with waiver cap 27; P4 /30 with pre-print cap 18; P4 floor ≥18 for a band
    alert; PSFV dilution carve-out), appends `fastpass_hits.jsonl` (dedup by accession), and **sends
    its own Discord alert** when a band clears 80. Deep-Dive engine consumes it as ground truth.
  - `short_interest_batch.py` (9,047 bytes): stockanalysis.com statistics pages (keyless; FINRA
    api.finra.org datasets decommissioned — frozen 2018–2020 snapshots), self-gates on <4h-old
    snapshot, computes cycle deltas, `squeeze watch` line at short%-of-float ≥20 AND
    (days-to-cover ≥2 OR delta ≥+10%) → `short_interest_snapshot.json` → Deep-Dive P3 modifier.
- **Entity Universe Seed (`79d430b5f565`, weekly, ENABLED):** `entity_universe_seeder.py` (21.8KB) —
  FMP MCP screener sweep (Technology/Industials/Basic Materials/Energy, $500M–$300B,
  NASDAQ/NYSE/AMEX, ~32 calls), INSERT OR IGNORE `DISCOVERED` rows into `universe_cache.db`,
  name backfill (capped 200 profile-symbol calls), writes `entity_universe_gate.json`
  (resolver match set + name/anchor index — 340KB) + report. Consumers: engine `suggest_tickers`
  (entity gate), `entity_resolver.py` two-pass resolution, `primary_feeds.py` fmp_pr/fmp_8k scoping.
  This is what makes "mid-cap named in a headline" a valid ticker hint instead of a false positive.
- **Quant upgrade one-shot (`297cbad05c47`):** fired 11-06 00:39; **aborted by its own precondition**
  — `~/wiki/QUANT_UPGRADE_SPEC.md` was absent, and the prompt explicitly forbids fabricating the
  surgical code from memory. Verified target scripts exist; no edits applied. **The spec still does
  not exist — this upgrade is pending.** (Robust-pillar MAD Z-scores, state-hash vLLM dispatch
  gating, hysteresis tiering — per its own report.)

## 3. THE ENGINE: `pipeline_engine.py` (3,020 lines, 142KB, stdlib-only)

The core of the system. Phases dispatched from `__main__`:
`health | deepdive | sources [apply] | process [apply] | tuner [apply] [force] | inclination`.
59 top-level functions. Highlights:

- **FMP transport:** `fmp_session()` + `fmp_call()` — hand-rolled JSON-RPC 2.0 over
  **Streamable-HTTP MCP** (POST to `https://financialmodelingprep.com/mcp?apikey=…`,
  `mcp-session-id` header, SSE `data:` line parsing, 45s per-call timeout, `_error` capture
  per call instead of crashing the phase). The legacy REST API is dead (403); this direct
  script-reachable transport replaced MCP-toolserver calls in 2026-10.
- **Parallelism (the "batch agents"):** `concurrent.futures.ThreadPoolExecutor` in three places —
  sources query fan-out (4 workers + error circuit breaker), deepdive per-ticker fetch (5 workers,
  shared session, distinct call_ids, worker-exception isolation), article-text fetch (4–6 workers,
  browser fallback serialized under its own lock).
- **Scoring math (all deterministic, in Python):** `financial_scoring` (5×20 bands from config),
  `bifurcation_scoring` (Tracks A/B/C engine sub-scores + dilution_kill_8pct/PSFV signals),
  `_wilder_rsi` (Wilder RSI-14), `_technical_signals` (RVOL, %14d, %5d), `_insider_signals`,
  `_pre_inflection_coil`, `_fastpass_8k_map`, `_short_interest_map`, `_earnings_preprint_window`.
- **Entity integrity:** `entity_gate` (mcap corridor per-symbol via profile-symbol; batch-market-cap
  explicitly untrusted), `suggest_tickers` (pre-warmed corridor names), `vet_tickers`
  (profile+quote double-check, ENTITY_MISMATCH quarantine).
- **Search layer:** `searxng_search` (local :8888), `gnews_search` (Google News RSS fallback with
  global throttle), `fetch_article_text` (HTML→text stdlib parse + `_browser_fallback`).
- **Conviction + briefs:** `conviction_scores` (direct vs generic mention separation),
  `trim_briefs` (14d rolling), `load_brief_history` (30d).
- **Wiki layer:** `ensure_wiki_file`, `append_wiki_note` (idempotent, never overwrite),
  `regenerate_research_interest` (dashboard rebuild), `staging_db`/`staging_admit`/
  `staging_promote` (SQLite staging queue: candidates admitted after repeated detection before
  becoming wiki dossiers).
- **Query Tuner:** `score_query` (12-window ledger from `query_health.json`), A/B commit to config.
- **Health:** `phase_health` → service gate + executions.db + data files + query ledger +
  `usage_audit.jsonl` → `pipeline_health.md` (10 kept).
- **mcap cache:** `wiki_mcap_cache.json` (7-day TTL) + `fmp_batch_market_cap` with per-symbol
  fallback (batch silently drops rows — known, treated as untrusted).

**Config:** `pipeline_config.json` (10.7KB) — FMP MCP URL + key, SearXNG/vLLM URLs, thresholds
(conviction bars, wiki mcap band 200–300B, retention windows), financial scoring bands, the 12
source queries + blocklists, drift scan queries, tuner gates, inclination thresholds, deep-dive
LLM-load control (`include_archived: false` — pruned tickers excluded from scoring universe).

## 4. DETERMINISTIC SCRIPT FLEET (`~/.hermes/scripts/`)

| Script | Lines | Role |
|--------|-------|------|
| `pipeline_health_gate.py` | 148 | Pre-run service gate (see §6) |
| `primary_feeds.py` | 1,500+ | 9 primary-source feeds, cursors, health, flock, atomic writes |
| `radar_13f.py` | 1,084 | 13F delta radar (12-fund cohort, zero LLM) |
| `fmp_client.py` | 421 | **Shared transport**: Streamable-HTTP MCP client (asyncio loop, HTTP semaphore, session lock), REST fallback (`parse_with_rest_fallback`), `atomic_write_json` (orjson + temp + fsync + os.replace — the shared bus-write impl used by 18+ files), `bus_append_entries` |
| `fmp_anomaly_feed.py` | 670 | Insider/volume/revenue anomaly lanes |
| `build_universe.py` | 768 | `universe_cache.db` builder (Gate 1.1) |
| `entity_universe_seeder.py` / `_cron.py` | 21.8KB/1.2KB | Corridor harvest + thin cron wrapper |
| `entity_resolver.py` | 346 | Two-pass entity resolution (local cache → FMP vet gate, 30d negative cache) |
| `earnings_fastpass.py` | 546 | 8-K fast-pass scanner + own Discord alert |
| `short_interest_batch.py` | — | stockanalysis.com short-interest snapshot |
| `tier_lists.py` | 281 | 3 cumulative tier lists + lifecycle |
| `score_validation.py` | 283 | LLM-vs-engine contract validator (caps, identity p1+…=total) |
| `score_alert_75.py` | — | ≥75 Discord alert dispatch + dedup state |
| `signals_bridge.py` | 181 | RubberBandMan machine bus writer (canonical local path) |
| `deep_dive_red_team.py` | 39KB | vLLM adversarial tribunal + options_delta_queue |
| `sec_slicer.py` | 21KB | 10-K slice (Items 1/1A/7/8) corpus for the tribunal |
| `drift_scan/ingest/scoring/lock/backtest/denoise/track_d` | ~93KB | The full drift detector suite |
| `ah_research.py` / `ah_research_list.py` / `ah_promote_bridge.py` / `market_hours.py` | ~30KB | After-hours lane |
| `thesis_fetch.py` / `thesis_llm.py` | 12KB/6.5KB | Thesis Forge: SEC+news corpus → vLLM thesis (static Radix prefix, ticker/corpus at prompt END for prefix-cache hits; verdict STRONG/MODERATE/WEAK/NO_EVIDENCE) |
| `_shared.py` | 1.2KB | atomic_write_json, state_load, state_backup (shared helpers) |
| `radar_fund_validator.py`, `market_hours.py`, `heartbeat.py`, `quant_vector_fetch.py` (cron root) | — | Support |

**Older-generation scripts (superseded, still on disk, not referenced by live jobs):**
`pipeline/agent6_meat_parser.py`, `agent7_scribe.py`, `agent8_orchestrator.py` (the original
3-agent pipeline, replaced by engine phases), `conviction_scoring.py`, `brain.py`,
`alpha_tracker.py`, `cross_temporal_audit.py`, `polymarket_scanner.py`, `option_check.py`,
`ticker_monitor.py`, `restore_jobs.py`, `trim_jobs.py`, `update_prompts*.py`.
These are dead weight — archive candidates (the audit kanban tracks this class of sweep).

## 5. SERVICES ON THE BOX (probed live 2026-11-06)

- **vLLM** (Docker container `qwen38-research-apc`, up 3 weeks):
  `vllm/vllm-openai:nightly-aarch64`, model `unsloth/Qwen3.8-27B-NVFP4`, served as `/model`,
  `--max-model-len 262144`, `--gpu-memory-utilization 0.88`, `--max-num-seqs 8`,
  `--enable-prefix-caching`, `--kv-cache-dtype fp8`, `--reasoning-parser qwen3`,
  `--enable-auto-tool-choice --tool-call-parser qwen3_coder`. Port :8000, `/health` = 200.
  This is the local "big model" — Thesis Forge, 13F equity gate, red-team tribunal,
  and the quant recalibrator all call `http://localhost:8000/v1/chat/completions`.
- **SearXNG** (Docker container `searxng`, up 3 weeks): port :8888, `/healthz` OK.
  Local meta-search engine; the pipeline's search backbone with Google News RSS as fallback.
- **FMP MCP endpoint** (remote): Streamable-HTTP JSON-RPC, key embedded in URL in
  `pipeline_config.json` (and mirrored in `fmp_key.local`, mode 600).
- **EDGAR, OSTI, arXiv, Federal Register, EIA, Google News RSS, stockanalysis.com** — keyless
  public APIs called directly by `primary_feeds.py` / `earnings_fastpass.py` / `short_interest_batch.py`.
- No other custom services (DGX dashboard/telemetry are vendor defaults; port :11000 = Hermes gateway).

## 6. THE HEALTH GATE (runs before every live LLM job)

`pipeline_health_gate.py` (148 lines, stdlib, localhost-only checks, **hash-stable output** so the
scheduler can hash-suppress unchanged results):
1. `vllm_endpoint` — `http://localhost:8000/health` → UP/DOWN (fail)
2. `searxng_api` — `:8888/healthz` → UP/DOWN (fail)
3. `searxng_container` — `docker inspect` running state (fail on DOWN)
4. `file_feed_raw.json` / `file_signal_briefs.json` / `file_vetted_tickers.json` — age >150min = STALE
   (informational only; STALE never aborts)
5. `disk_free` — <5% free = LOW (fail)
6. `wiki_drift` — sentinel that a second wiki tree must not reappear under cron (SPLIT = fail)
7. `intel_bus` — canonical bus dir exists + writable (DOWN = fail; the bus is the RubberBandMan lifeline)
8. `primary_feeds` — freshness of `primary_feeds_report.json` (DEGRADED = informational)
9. `entity_universe` — age of `entity_universe_gate.json` (STALE = informational)

Verdict: `ALL_GOOD` or `FAIL:<flags>`. Job prompts bind this to behavior (SearXNG down → engine
auto-falls back to RSS; vLLM down → finish fast; STALE → note and continue). The same gate is
re-run as the Deep-Dive's final step and its `pipeline_engine.py health` phase writes
`pipeline_health.md`.

## 7. STATE & DATA BUS (file map, all under `~/.hermes/cron/` unless noted)

**Ingestion → briefs → scores → lists (the main chain):**
`feed_raw.json` (35KB, engine-owned) → `signal_briefs.json` (598KB, 14d rolling) +
`signal_briefs_history.json` (1MB, 30d) → `vetted_tickers.json` (47KB) →
`/tmp/dd_briefing.json` (engine, per-run) → `ticker_scores.json` (87KB) →
`tier_list_state.json` (13.5KB + .bak) → wiki tier lists (markdown) →
`watchlist_state.json` (215KB) → `intel_bus/signals_bridge.json` (8.2KB, machine bus).

**Deterministic caches/gates:**
- `universe_cache.db` (856KB, SQLite WAL): `universe` 1,537 rows, `alpha_vectors` 1,281 rows
  (29-col vector, 8 pillars: SMH/URA/BOTZ/DEFENSE/MACRO + others), `regime_state` 12 rows
  (benchmark prices, 20d RS, COT crowding z-scores, macro pulse), `pillar_map`, `equity_verdicts`
  142 rows (13F radar public-equity gate memo), `wiki_staging_queue` 4 rows, `meta`.
- `entity_universe_gate.json` (340KB) + `entity_universe_tracked_names.json` (67KB) — pre-warmed
  corridor name set (weekly seeder).
- `feed_state.json` (18KB) — per-feed health/cursors for the 9 primary feeds.
- `query_health.json` (82KB) — tuner ledger; `query_changes.jsonl` — tuner commit log.
- `wiki_mcap_cache.json` (24.5KB, 7d TTL).
- `watchlist_state.json` — the curation spine (ACTIVE/WATCH/DORMANT/ARCHIVED).
- `fastpass_hits.jsonl` (6.5KB) — earnings 8-K extracts (dedup by accession).
- `short_interest_snapshot.json` (26.5KB) — biweekly short-interest.
- `inclinations/` — `inclination_scores.json` (138KB), `screen_out.json`, `transcripts_out.json`,
  `ip_out.json`, `name_cache.json`, `inclination_alerts.md`.
- `red_team.db` (1.3MB) + `options_delta_queue.db` (29KB) + `red_team_report.json` — tribunal.
- `frontier_candidates.json` (410KB) + `raw_13f/` + `13f_radar_state.json` — 13F lane.
- `after_hours_research.json` (37KB) + `ah_research_raw/judged.json` + `ah_bridge_state.json` — AH lane.
- `drift_candidates.json` + `cascade_state/queue.json` + `drift_query_health.jsonl` (115KB) — drift lane.
- `unharnessed_scores.json` (218KB) + `quant_vector_payload.json` (805KB) — overnight wave outputs.

**Telemetry:** `executions.db` (737KB: 1,002 executions + 4 incidents), `usage_audit.jsonl`
(302KB, 943 rows), `deliveries.db` (434KB), `engine_run_report.json`, `pipeline_health.md`.

**Wiki (the durable output):** `~/wiki` (git repo) — 869+ dossiers `companies/<T>/<T>.md`,
`research_interest.md` dashboard, 3 tier lists, `watchlist_changelog.md`, `drifts/INDEX.md`,
`corpus/<T>/` (thesis inputs), `archived/` (demotions, never deleted), `log.md`, `SCHEMA.md`,
`AGENTS.md`.

## 8. FAULT-TOLERANCE MECHANISMS (the "fine details" the reviewer should check)

- **Atomic bus writes everywhere it matters:** `fmp_client.atomic_write_json`
  (orjson + temp + fsync + os.replace) — a mid-write kill can never truncate `feed_raw.json` or
  tier state. Used by 18+ files.
- **Single-flight locks:** flock on `primary_feeds.py` (`kernel-released on death — no stale lock`),
  `drift_scan.lock` with overlap-skip semantics, `.tick.lock`, `.fmp_anomaly.lock`.
- **Single-writer claim pattern** (`ah_promote_bridge.py`): `os.replace(src, claiming)` — a second
  claimant gets FileNotFoundError → clean no-op. Eliminates lost-update races on shared files.
- **Circuit breakers:** ≥3 SearXNG query errors → whole fan-out flips to RSS; 2 consecutive same-error
  FMP lookups → halt + flag remainder "unverifiable"; vLLM down → deterministic classifier fallback
  (red team degrades to heuristics, never aborts).
- **Self-gating (token savers):** watchlist recheck bi-weekly; tuner ≤1/24h; inclination FMP sources
  ≤1/8h; short-interest <4h no-op; universe seed 6–7d effective cadence; deep-dive excludes
  archived tickers from the scoring universe.
- **Idempotency keys:** 13F keyed on (year, quarter); fastpass on EDGAR accession; briefs on
  normalized headline + URL; staging on repeated-detection counts.
- **Crash-resumable overnight wave:** incremental atomic checkpoint every 10 items + final flush;
  skip-gate on `generated` timestamp (idempotent re-runs); `--force` bypass; transport lock held
  for the whole MCP request round-trip (parallel in-flight requests interleave session state
  and drop the transport — the exact bug that would silently corrupt the overnight wave).
- **Incident tracking:** `cron_incidents` table dedupes error signatures; 4 historical incidents,
  all resolved (2× connection errors, 1× 13F FMP-handshake timeout, 1× unknown).
- **Delivery hardening:** hash-suppression of unchanged output, `[SILENT]` token, failure-deliver
  hook to local, dead-channel detection documented.

## 9. MEASURED BURN (usage_audit.jsonl, 943 runs, all-time recorded)

| Job | Runs | Input tokens | Output tokens |
|-----|------|--------------|---------------|
| 7267b662cf52 (Deep-Dive) | 172 | 308,217,831 | 8,469,876 |
| cfdab394b3bd (Processing) | 249 | 121,667,128 | 1,989,402 |
| ba24fd4ddad6 (Sources) | 244 | 67,838,011 | 1,782,576 |
| e9711f5abb15 (Drift, paused) | 33 | 23,922,941 | 431,413 |
| bf59f02d39ba (13F, paused) | 189 | 13,429,871 | 146,410 |
| fd3a24115408 (After-Hours, paused) | 55 | 12,339,038 | 153,919 |
| 297cbad05c47 (one-shot) | 1 | 52,121 | 909 |
| **Total** | **943** | **547,466,941** | **12,974,505** |

The input-to-output ratio (~42:1) is the signature of the architecture: the LLM mostly *reads*
big briefings and writes compact verdicts. Deep-Dive's 308M input comes from the 26KB prompt
× 172 runs + the briefing context. **Honest note:** the Deep-Dive prompt is 26,146 chars — far
over the 3,500-char "keep it lean" rule in the cron-pipeline-architecture skill. It is
intentionally a spec document (the full pillar-lock rubric) rather than a task list, and it
works (avg 96 min, 144/145 completed in 30d, 1 failure). The alternative (rubric in a skill
reference + lean prompt) is the documented pattern and would cut per-run input by ~20–30K tokens;
trade-off: the rubric must then load perfectly every run.

## 10. HONEST ANALYSIS — STRENGTHS, GAPS, RISKS

**Strengths (verified, not vibes):**
1. **Deterministic-first architecture with LLM-as-judge** is the right shape for this problem:
   every number that enters a score is either Python-computed from FMP/SEC data or bounded-qualitative
   with hard caps enforced by a validator (`score_validation.py` actually rejects LLM rows).
2. **Entity integrity is genuinely hard** — per-symbol profile CIK checks, ENTITY_MISMATCH quarantine,
   two-pass resolver with 30-day negative cache, the $500M–$300B corridor gate applied at fetch time.
   This is where naive pipelines die and this one is built to survive.
3. **Degradation is designed, not fatal:** every external dependency has a fallback (SearXNG→RSS,
   vLLM→heuristic, FMP-REST→MCP→REST-cache, dead-feed→auto-demotion after 7, mcap batch→per-symbol).
4. **The machine bus is contract-clean:** exact schema, honesty rules (never fabricate
   parent_whale/dilution_risk — None when unknown), atomic writes, health-gate sentinel.
5. **Self-tuning is real and auditable:** the query tuner A/B tests and commits winners to config
   with a change log; the drift lane cascade-authored queries; keyword staging is evidence-gated.
6. **Everything is inspectable:** executions.db, usage_audit.jsonl, query ledgers, run reports,
   the health gate's hash-stable output. A reviewer (large model or human) can replay any run.

**Gaps / honest risks:**
1. **The paused majority means the "24/7 pipeline" is actually a 12-hour-a-day trio + manual
   overnight.** Seven lanes (drift, 13F, after-hours, anomaly feeder, universe refresh, fast-pass,
   short-interest) went dark on 2026-11-04. Their inputs age out; re-enabling is a one-line
   `enabled` flip per job but the 13F one is intentionally gated on the Nov 14 filing burst.
2. **One scheduler, one box, one gateway process.** The 8 Deep-Dive rows with
   "Scheduler restarted before durable terminal state" (11-04 window) are the proof: when the
   gateway restarts mid-run, side-effect status is *unknown*. No durable journaling exists for
   what the run already did. (Mitigation today: idempotency keys everywhere, which is why it
   didn't corrupt anything — but it's luck-adjacent.)
3. **The Deep-Dive prompt is a 26KB spec, not a task list.** It works, but it is the single biggest
   token cost in the system and a single rubric edit is a 26KB diff. The skill-documented pattern
   (rubric in reference file) is the cheaper path and is unimplemented.
4. **The quant-upgrade one-shot ABORTED** — the spec file (`~/wiki/QUANT_UPGRADE_SPEC.md`) never
   landed, so the robust-pillar MAD Z-score / hysteresis upgrade to
   `unharnessed_swarm_recalibrator.py` + `build_universe.py` is **not applied**. The overnight wave
   is running pre-upgrade code.
5. **FMP is a single remote dependency with a single key** (in URL + 2 local files). No key
   rotation mechanism, no second provider. The REST fallback exists but hits the same account.
   A 403/revocation = pipeline-blind (documented in skills; prevention pre-flight exists in the
   health gate's FMP check path).
6. **Dead weight on disk:** superseded agent6/7/8 scripts, June-era `brain.py`/`alpha_tracker.py`,
   805KB `quant_vector_payload.json` from a superseded pipeline, multiple `.bak_*` state files,
   `.bak_stepd` files from the 11-05 step-D work. Archive sweeps are tracked in AUDIT_KANBAN.md
   but not completed. (The earlier sweep reclaimed ~165MB.)
7. **Two wiki tier-list files show uncommitted git modifications** (flagged in the one-shot run
   report) — the wiki git tree is not being committed on the automation path; the "durable output"
   is durable only in the working tree.
8. **Subagent self-report risk is structural:** cron sessions and the overnight wave self-report
   "done"; the only true verification is re-running the validators (score_validation.py,
   the health gate, the 13F verify line). This handover doc deliberately lists what a checker
   should re-verify (§11).
9. **Known FMP data gaps** (documented, accepted): no RPO field, no raw-materials/WIP inventory
   split, no S-3/ATM flag; `batch-market-cap` silently drops rows; `secFilings 8k-latest` global
   feed stale; FINRA short-interest decommissioned. Scoring bands are coverage-scaled
   (`adjusted` = raw×(0.5+0.5·data_coverage)) so data-poor names can't outscore data-rich ones —
   but a reviewer should confirm any new threshold uses `adjusted`, not `raw`.

## 11. WHAT A REVIEWER SHOULD RE-VERIFY (15-minute checklist)

1. `hermes cron jobs` — confirm the 5-enabled / 7-paused state matches §2.
2. `python3 pipeline_health_gate.py` — expect `verdict: ALL_GOOD` (vllm UP, searxng UP+RUNNING,
   feeds FRESH, disk OK, wiki OK, bus OK).
3. `python3 pipeline_engine.py health` — read `pipeline_health.md` last block.
4. `sqlite3 executions.db "SELECT job_id,status,COUNT(*) FROM executions WHERE started_at>'2026-11-04' GROUP BY 1,2"` —
   live trio should be 3 completions/cycle since the 11-04 cutover.
5. `jq '.quarter,.filers,(.frontier|length)' frontier_candidates.json` — 13F state (expect early-quit
   until ~Nov 14 Q3 burst; then flip job `bf59f02d39ba` enabled=true).
6. `jq '.summary' watchlist_state.json` — ACTIVE/WATCH/DORMANT/ARCHIVED counts.
7. `head -1 inclinations/inclination_status.json` — last inclination run status.
8. `sqlite3 universe_cache.db "SELECT COUNT(*), SUM(market_cap IS NOT NULL) FROM universe"` —
   1,537 rows, mcap fill ratio.
9. `jq '.generated,.per_feed' primary_feeds_report.json` — 9 feeds, any `error:`?
10. `jq '.status_line' orchestrator_output.json` — last Processing cycle.
11. Check `~/wiki/QUANT_UPGRADE_SPEC.md` — still absent = quant upgrade still pending (§2.2).
12. `jq '.verdicts|length' red_team_report.json` + `sqlite3 options_delta_queue.db "SELECT COUNT(*) FROM options_delta_signals"` —
    tribunal output freshness.
13. Grep any threshold logic you add for `raw` vs `adjusted` (coverage-scaled) financial_100 use.
14. Confirm no `cronjob(action='run')` strings appear in any job prompt (anti-pattern; file handoff only).
15. Confirm tier-list writers (`tier_lists.py`, `signals_bridge.py`) are the ONLY writers of their
    outputs — any LLM hand-write of `signals_bridge.json` or `golden_tier_list.md` is a contract violation.

---
*Generated from live host inspection 2026-11-06. All file paths, line counts, DB row counts,
token totals, and schedule states were read from the host at generation time, not from memory.*
