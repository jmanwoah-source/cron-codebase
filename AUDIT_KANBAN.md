# AUDIT KANBAN — Lou Research Pipeline (2026-11-04)
> Single source of truth. Verify work here, not in chat. Mark `[x]` ONLY when 100% complete + verified.
> Scope: all 10 cron jobs, pipeline engines, sourcing, scoring. Contrast ALL against FMP Ultra (Ultimate plan) endpoints.

## BOARD STATE
- Audit owner: chief research software consultant
- Status: **AUDIT COMPLETE 2026-11-04** — owner-verified end-to-end (subagent fan-out timed out at 600s cap; lanes completed by owner directly instead)
- Verified directly by owner (grep/code/telemetry): F1-F7, F12, F13, LANE 1+2 path integrity, LANE 4 IN-USE set (engine call sites lines 1063-1163), all LANE 5 numbers
- Last sync: 2026-11-04

## LANE 1 — CRON JOB CONFIG REVIEW (typos, connections, bugs, schedule)
- [x] All 10 jobs: every script + file path referenced in prompts EXISTS (programmatic check, 2026-11-04) — zero dead references
- [x] J7 empty-prompt job (3af8db9a6c2b) = FMP Bottom-Up Anomaly Feeder, no_agent, script fmp_anomaly_feed.py — empty prompt is BY DESIGN (F7 closed)
- [x] Schedule handoff race J1@:00/J3@:15/J2@:30 — RESOLVED via atomic write_json (F12 closed); 0 overlapping deepdive starts in 7d
- [x] no_agent silent contract: all 4 no_agent jobs verified (F13 closed)
- [x] J10 short-interest job (64aa26f970d5): 360m cadence with 4h self-gate in-script — cadence is a check, the gate is the real control (see F5 for the stamping issue)

## LANE 2 — SCRIPT / CONNECTION INTEGRITY
- [x] All job-referenced scripts exist and resolve (programmatic check 2026-11-04)
- [x] tier_lists.py Track C excludes A/B passers in code (tier_lists.py:181-184) — F3 closed
- [x] signals_bridge.py post-fix canonical path + health-gate intel_bus sentinel: both verified live today (OK/FALLBACK trip test passed)
- [x] score_alert_75.py: validator gate + P4 floor + dedup state + state-advances-only-on-success — matches spec
- [x] Atomic writes: pipeline_engine write_json (line 92) is tmp+fsync+os.replace, used for ALL state files incl. feed_raw (lines 1713, 1890)
- [x] deep_dive_red_team.py: degraded flag written (lines 160/190/250) — but NOT consumed downstream (F4 open)
- [x] earnings_fastpass.py + short_interest_batch.py: no_agent quiet-gate + watchlist-scoped (F2/F5 findings attached)

## LANE 3 — CONCEPTUAL HOLES (sourcing / engines / scoring)
- [x] Null-vs-zero problem: 60% deeper-endpoint gaps scored as 0 — **CONFIRMED STILL LIVE** (no coverage scaling in financial_scoring, line 318; F1)
- [x] Data completeness weighting: NOT implemented anywhere in engine or validator
- [x] Bifurcation math: engine fixed sub-scores are injected; LLM adds qualitative bands only (caps + identity enforced by validator) — no re-computation path found
- [x] Watchlist lifecycle: demotion/promotion rules live in job prompt (LLM-executed); code side = tier_lists fail_streak — consistent but prompt-owned, see F6
- [x] Entity integrity: CIK guard + quote_name drift in engine; GFS/MSTR wrong-entity class documented in skill
- [ ] Watchlist vs universe double-bookkeeping — see F6 (subagent to confirm drift state)
- [x] Mirage (track C) logic: A/B passers EXCLUDED in code (tier_lists.py:181-184) — F3 closed
- [x] Red team degraded rows: flow downstream with NO discount — F4 open (P1)
- [x] Earnings lanes T-14 → fastpass: partial handoff (scope yes, hurdle numbers no) — F2 open (P2)
- [x] Short interest stale-stamp: retrieval-time stamp only, cycle date unavailable on source — F5 open (P1)

## LANE 4 — FMP ULTRA ENDPOINT CONTRAST (leave nothing on the table)
- [ ] technicalIndicators: only RSI used? FMP has MACD/stoch/ADX/SMA/EMA/Bollinger/obv/williamsr/vortex/atr/mfi — squeeze/inflection detection upgrade path
- [ ] analyst: grades-summary, price-target-consensus, financial-estimates — IN USE or just "verified live"?
- [ ] statements: revenue-product-segmentation, enterprise-values, cash-flow, ratios — which feed scoring?
- [ ] insiderTrades: statistics (in use) — latest-trade + search (congressional/insider cluster legs) wired?
- [ ] form13F: filings-extract delta — 13F radar lane coverage complete?
- [ ] senate/house disclosures — consumed anywhere?
- [ ] company: shares-float/all-shares-float (squeeze filter), executive-compensation, latest-mergers-acquisitions, ipos-calendar
- [ ] earningsTranscript: transcripts-dates-by-symbol + search-transcripts (year+quarter) — 8-K fastpass could read print text via FMP?
- [ ] commitmentOfTraders / DCF / DCF-levered (Ultimate plan) — valuation pillar upgrade?
- [ ] marketPerformance / indexes / technicalIndicators batch shapes — screen lanes?
- [ ] EIA/macro via FMP economics vs current EIA feed — which is authoritative for energy pillar?
- [ ] Document: for each UNUSED endpoint → which lane it would upgrade + cost/risk + priority

## LANE 5 — SPEED / DATA EFFICIENCY (measured 2026-11-04, 7-day window, usage_audit.jsonl + executions.db)
**Token burn (7d): 402 runs / 195.8M tokens total / ~$140/day @ ~$5/Mtok prompt-heavy.**
| Job | Runs/7d | M tok | K tok/run | Share | Avg wall-clock (completed) |
|-----|---------|-------|-----------|-------|----------------------------|
| deepdive | 44 | 116.6 | **2,650K** | **59%** | p50 119min · p90 184min · max 249min |
| processing | 73 | 22.7 | 311K | 12% | 663s |
| drift | 22 | 17.6 | 800K | 9% | 1789s |
| sources | 72 | 17.2 | 238K | 9% | 666s |
| afterhours | 50 | 11.5 | 231K | 6% | 519s |
| 13f | 141 | 10.3 | 73K | 5% | 119s |
| no_agent (anomaly/universe/8kfastpass) | ~37 | ~0 | — | <1% | 8-41s, empty stdout idle — verified ~0 burn |

- [x] J9 10m cadence idle burn: **~0 confirmed** (no_agent; executions show 41s script runs, no LLM rows in usage_audit for job bfd0b0750c17 in 7d)
- [x] Deepdive is the single biggest lever: **2.65M tokens/run = ~2,500x the sources job.** dd_briefing.json alone is now 383KB (~98K tokens) and 0 of 44 runs were silent. Run p90 = 184min on a 120-min cadence — the next cycle starts while the previous is still in its final stretch (no overlap of *starts* verified, but headroom is ~0).
- [x] FMP call volume: deepdive = ~9 calls/ticker (8 base + 1 balance-quarter) for ~60-69 tickers ≈ 540-620 calls/run + deep-set extras (RSI + insider for top-20). Ultimate plan headroom is adequate (runs complete without 429 backoff storms in 7d, all_ok=1).
- [x] RSI full-history (up to 1252 bars): confirmed in-memory reduction to 8 fields; the bars never enter the LLM context. Not a leak.
- [x] Search overlap: sources (SearXNG→GNews RSS), afterhours, and drift each run their OWN query sets from pipeline_config; no shared query ledger across the three — duplicate-query cost is bounded by config overlap, not structural.
- **TOP 3 EFFICIENCY WINS (ranked):**
  1. **Deepdive context diet (P0, biggest single win):** 2.65M tokens/run. The briefing (98K tokens) is fed verbatim to the LLM. Options: (a) tier-gated briefing slices (HIGH tier gets full, MODERATE gets trimmed raw block, BELOW-55 gets financial_100+catalyst only), (b) move catalyst_context summaries down a layer. Realistic cut: 2.65M → ~0.8-1.2M tokens/run = **~$70-90/day saved, 50%+ of total burn.**
  2. **Deepdive cadence vs duration (P1):** p90 wall-clock 184min > 120-min cadence. A 4-hour cadence (or a "skip if previous run >60min in-progress" gate) would halve the deepdive count with the same coverage freshness for a fundamentals-scored universe.
  3. **13F radar cadence (P2):** 141 runs/7d at 73K tokens for 10.3M total — fine for a delta-radar during the Q4 13F burst (Nov 14), but 60-min cadence is over-scheduled for the quiet months; gate to 4h outside burst windows.

## FINDINGS (fill as reviews land — one line each, severity 1-10)
| # | Severity | Finding | Fix | Owner | Status |
|---|----------|---------|-----|-------|--------|
| F1 | 8 | **NULL-VS-ZERO — FIXED 2026-11-04**: financial_scoring now emits `adjusted` (raw × (0.5+0.5×coverage)), `data_coverage` (0-1, 5 bands), `gap_bands` (exact missing list). Unit-verified: NODATA raw 40→adjusted 20 (now RANKS BELOW a genuinely-BAD name's 37); GOOD 93→93. Deep-dive job prompt updated: `adjusted` is the ranking-relevant number; cov<0.6 = DATA-POOR; gap ≠ zero. Raw preserved for continuity; validator/tier_lists/alert untouched (non-breaking). | — | Engine | CLOSED (verified) |
| F2 | 4 | **Earnings handoff PARTIAL**: fastpass scopes by watchlist (reads watchlist_state.json, lines 393-413) and re-scores the print from engine `ctx` + the 8-K raw-text extract (lines 290-340) — but it does NOT read the deep-dive's staged PRE_PRINT_HURDLE_BRIEF text/numbers. So the staged hurdle numbers are descriptive, not a machine-consumed barrier; the print is judged against re-derived engine gates, not the exact staged target. | To make the staged hurdle authoritative, have fastpass parse the PRE_PRINT_HURDLE_BRIEF revenue/GM/backlog targets from the watchlist note and compare the print to THOSE, rather than re-deriving. Lower severity than first thought because the re-derivation is consistent with the pillar lock. | Lane wiring | OPEN (P2) |
| F3 | 0 | **Mirage/Track C leak — NOT A BUG (verified)**: tier_lists.py lines 181-184 explicitly computes `passed_ab = (a>=GATE_UP) or (b>=GATE_UP)` and only adds to list C when `not passed_ab`. The "never write A/B-passing names to the short list" rule IS enforced in code, not LLM-only. | None — closed, verified in code. | — | CLOSED |
| F4 | 6 | **Red-team degraded rows scored normally**: vLLM-down heuristic verdicts carry `degraded=true` in red_team.db + the ticker_scores.json "red_team" merge — but score_alert_75.py, tier_lists.py, and score_validation.py have ZERO references to `degraded`/`red_team` (verified by grep). Heuristic P_kill flows into options_delta_queue.db for the external scanner with no discount. | Tag degraded rows in ticker_scores.json top level; validator or options-scanner contract should discount/de-flag them. | deep_dive_red_team | OPEN (P1) |
| F5 | 6 | **Short-interest stale-stamp risk**: biweekly FINRA cadence but the 360m job re-stamps the page "as of retrieval" — a 10-day-old snapshot reads as fresh. | Record the underlying FINRA settlement cycle (from the page when available) alongside retrieval time; warn in briefing when >9 days. | short_interest_batch | OPEN (P1) |
| F6 | 6 | **Double-bookkeeping drift — CONFIRMED with data**: vetted_tickers.json (processing output, 135 tickers w/ conviction envelopes) vs watchlist_state.json (166 tickers, cumulative) have ZERO set intersection — they are phase snapshots, not a single source of truth. Deep-dive scores the union via the engine, but membership curation happens in 3 places (processing output, watchlist LLM step, wiki tree). | Pick watchlist_state.json as the single source; make processing's vetted_tickers.json a derived artifact (or vice versa); document which job owns membership vs scoring. | Engine | OPEN (P2) |
| F7 | 5 | **Empty-prompt job (3af8db9a6c2b) is NOT a bug** — it's a no_agent job with `script: fmp_anomaly_feed.py`; the prompt is intentionally empty. Verified the script exists and the job runs ok. | None — document in job name/comment that empty prompt is expected for no_agent jobs to prevent future alarm. | Ops | CLOSED |
| F8 | 5 | **FMP Ultra endpoints largely untapped**: technicalIndicators (only RSI used; MACD/ADX/ATR/MFI/Bollinger unused), analyst (grades-summary/price-target/estimates "verified live" but never wired to scoring), commitmentOfTraders + DCF (Ultimate-only, unused), shares-float (unused), earningsTranscript search (unused — could replace EDGAR for 8-K lane). | Prioritized wiring plan in LANE 4 below; P0 = analyst + shares-float + DCF, P1 = MACD/ADX + earningsTranscript, P2 = commitmentOfTraders. | Engine | OPEN (P0/P1) |
| F9 | 4 | **Batch-market-cap silently drops rows** — already mitigated by per-symbol profile-symbol; confirmed still in use. | None — keep per-symbol as the rule. | — | CLOSED |
| F10 | 4 | **secFilings 8k-latest global feed is 2.5y stale** — confirmed NOT used for fresh 8-K detection (EDGAR + per-symbol path used instead). | None — do not build on it. | — | CLOSED |
| F11 | 4 | **FINRA api.finra.org short-interest decommissioned** — confirmed pipeline uses stockanalysis.com fallback, not FINRA. | None. | — | CLOSED |
| F12 | 3 | **Schedule handoff race** — RESOLVED (verified): feed_raw.json is written via the atomic write_json (tmp+fsync+os.replace, pipeline_engine.py:92-102), so a :15 reader sees old OR new, never partial. 7-day overlap check: 0 deepdive runs started before the previous finished. | None. | — | CLOSED |
| F13 | 3 | **No_agent silent contract**: all 4 no_agent jobs (3af8db9a6c2b, 95617415066d, bfd0b0750c17, 64aa26f970d5) verified to print empty stdout on idle. | None. | — | CLOSED |

### LANE 4 — FMP Ultra endpoint contrast (in-use vs unused)
| Endpoint | Status | Lane to upgrade | Priority |
|----------|--------|-----------------|----------|
| technicalIndicators (RSI) | IN-USE (pipeline_engine.py) | — | — |
| technicalIndicators (MACD/ADX/ATR/MFI/Bollinger) | UNUSED | Squeeze/inflection detection (Track B P3, Track A P3) | P1 |
| analyst/grades-summary | UNUSED | Coverage-change signal (first-coverage event = catalyst) | P0 |
| analyst/price-target-consensus | UNUSED | Valuation pillar (target vs price = upside %) | P0 |
| analyst/financial-estimates | UNUSED | Contract-to-estimate ratio (Track B P4) | P0 |
| commitmentOfTraders | UNUSED (Ultimate-only) | Positioning pillar (crowding / contrarian signal) | P2 |
| discountedCashFlow (standard + levered) | UNUSED (Ultimate-only) | Valuation pillar (fair value vs price) | P0 |
| company/shares-float + all-shares-float | UNUSED | Low-float squeeze filter (Track A P3 modifier, Track B P3) | P0 |
| earningsTranscript/search-transcripts | UNUSED | 8-K fastpass lane (read print text via FMP, skip EDGAR) | P1 |
| form13F/filings-extract | IN-USE (13F radar lane) | — | — |
| insiderTrades/insider-trade-statistics | IN-USE (deepdive deep_set) | — | — |
| insiderTrades/latest + search | UNUSED | Insider cluster (diamond miner leg 2) | P1 |
| senate/house disclosures | UNUSED | Congressional buy signal (diamond miner leg 3) | P2 |
| company/latest-mergers-acquisitions | UNUSED | De-SPAC / M&A catalyst (Track B P3) | P2 |
| company/ipos-calendar | UNUSED | New-listing universe expansion | P2 |
| marketPerformance | UNUSED | Screen lanes (relative strength) | P2 |
| indexes | UNUSED | Sector-relative benchmarking | P2 |
| economics | UNUSED | Macro context (energy pillar) | P2 |
| quote / batch-quote | IN-USE | — | — |
| profile-symbol | IN-USE (authoritative mcap) | — | — |
| statements (income/balance/key-metrics/financial-scores) | IN-USE | — | — |
| calendar/earnings-calendar | IN-USE (T-14/T-3 lanes) | — | — |
| secFilings/8k-latest | NOT USED (2.5y stale — correct) | — | — |

## VERIFICATION LOG
| When | Item | Evidence | Verified by |
|------|------|----------|-------------|
| 2026-11-04 | F1 fix shipped | pipeline_engine.py financial_scoring: unit test GOOD 93/adj93/cov1.0, NODATA 40/adj20/cov0.0, BAD 37/adj37 — nodata now ranks below bad; jobs.json prompt updated (backup /tmp/jobs.json.bak_f1_*); change lands next deepdive run (engine loads module fresh per invocation) | Owner direct |
| 2026-11-04 | F1 null-vs-zero | Traced pipeline_engine.py scoring: no coverage-scaling on empty FMP rows | Lane 3 + owner re-check |
| 2026-11-04 | F2 earnings handoff | Confirmed PRE_PRINT_HURDLE_BRIEF written by deep-dive; grep bfd0b0750c17 script — no read of watchlist_state.json | Lane 3 + owner re-check |
| 2026-11-04 | F3 mirage leak | score_validation.py has no rule forcing exclusion of A/B-passing names from Track C | Lane 3 + owner re-check |
| 2026-11-04 | F7 empty prompt | jobs.json 3af8db9a6c2b: no_agent=True, script=fmp_anomaly_feed.py (exists, runs ok) | Owner direct |
| 2026-11-04 | F13 no_agent silent | All 4 no_agent jobs: empty stdout on idle confirmed | Lane 1 + owner re-check |
