# PHASE 1 — DIAMOND MINER: DESIGN SPEC (2026-10-26)

Status: SPEC ONLY — not built yet. For review before implementation.
Owner: Lou (cron pipeline). This file is the contract; the builder implements TO it.

## 1. Purpose

Daily sub-$2B "diamond in the rough" discovery job. Finds nano/micro/small caps
positioned for exponential inflection via eight signal families (elite funds,
insider clusters, congressional buys, mechanical RVOL squeezes, contract-to-mcap
disconnects, IPO/de-SPAC windows, first/zero coverage, balance-sheet inflections).

## 2. Integration principles (non-negotiable)

1. **FEEDER, NOT PARALLEL SYSTEM.** Diamond Miner never scores. It emits
   candidates; survivors append to `vetted_tickers.json` and flow through the
   EXISTING path: mcap vet gate (per-symbol profile-symbol, authoritative) ->
   deepdive engine (entity guard included) -> 100-pt bifurcated scoring ->
   tier lists / alerts. One schema, one funnel.
2. **Deterministic legs first, LLM last.** All fetching/diffing/regex is pure
   Python (pattern: drift_scan.py). The LLM runs ONE judgment pass over the
   deduped candidate set, capped at top-5 output.
3. **Entity integrity inherited.** Every candidate carries symbol + cik +
   company_name from profile-symbol at emit time. Reused/legacy tickers are
   caught at the SAME wall as the rest of the pipeline (CIK cross-check in
   deepdive; null-CIK = fund drop at the vet gate).
4. **Dedup against the wiki.** Candidates already in
   `wiki/companies/` or `watchlist_state.json` are NOT re-emitted as new —
   they get a `signal_refresh` note instead (feeds brief_count for the
   promotion hook).
5. **Config lives in pipeline_config.json** under a `"diamond_miner"` key —
   thresholds are tunable there, never in the job prompt.
6. **Delivery is gated.** Discord delivery fires ONLY when a candidate passes
   the existing 75-point bar in the downstream funnel (via the normal
   score_alert_75.py path) OR on a weekly summary of the candidate pipeline.
   No daily spam.

## 3. Cadence & schedule

- Daily at 05:30 UTC (before the 120-min Sources cycle, so finds enter the
  next feed window).
- Estimated cost: ~15-25 FMP calls for legs 1/3/4/6 (global feeds, page 1),
  +1 per deepdive-universe ticker for legs 2/5/7/8 ONLY when the quarterly
  snapshot changes (13F/insider-stats refresh quarterly; RVOL/contract/
  coverage/balance scan the existing vetted universe = reuses deepdive's
  profile/quote data already fetched — those legs are near-free).
- Quarterly trigger: after 13F season closes (~mid-May, mid-Aug, mid-Nov,
  mid-Feb) the elite-fund delta leg runs full; otherwise it polls
  `latest-filings` for NEW filings by the watched CIKs only.

## 4. The eight signal legs (deterministic)

Each leg appends candidate dicts to a shared list. Field contract per leg is
the COMMON SCHEMA (section 5) — `signal_type` + `evidence` differ.

### LEG 1 — Elite Fund 13F Delta
- Source: `form13F latest-filings` (poll watched CIKs) -> `filings-extract`
  (cik, year, quarter) for each NEW filing by a watched fund.
- Watchlist (~15 funds, in config `diamond_miner.elite_fund_ciks`):
  Scion Asset Management (0001365645 - VERIFY CIK), D1 Capital, Tiger Global,
  Point72, Situational Awareness, Appaloza, Coatue, Citadel (discretionary),
  + user's existing "elite funds endpoint" watchlist.
  **BUILD TASK: verify each fund's CIK via `form-13f-filings-dates` before
  first run — never hardcode from memory.**
- Signal: `NEW` position (present this quarter, absent last) with symbol
  market cap < $2B. Additions to existing positions = `13f_add` (secondary,
  only if position value doubled).
- Output signal_type: `elite_fund_new` / `elite_fund_add`.
- Evidence: fund name, position value, shares, quarter.
- Caveat (in evidence, always): 13F is 45 days late, long-only.

### LEG 2 — Insider Cluster
- Source: `insiderTrades latest-insider-trade` (rolling page scan) +
  `insider-trade-statistics` per symbol (cheapest pre-filter).
- Signal: >=3 DISTINCT insiders with `acquisitionOrDisposition == 'A'` and
  `transactionType` starting 'P' (open-market purchase, NOT awards/S) on the
  same symbol within 30 days; OR a CEO/President/P-CEO purchase at a price
  >=20% below the current quote.
- Universe filter: symbol mcap < $2B (profile check).
- Output signal_type: `insider_cluster` / `insider_conviction_buy`.
- Evidence: insider names/roles, dates, prices, shares, notional.

### LEG 3 — Congressional Disclosure Scan
- Source: `senate senate-latest` + `house house-latest` (page 1 per day;
  volume is low enough to full-judge).
- Signal: `type` contains "Purchase" AND `assetType == "Stock"` AND symbol
  resolves to a hard-tech (semis/defense/energy/robotics) name < $2B.
  SELLs are IGNORED (congressional sells are 30-day pre-committed = noise).
- Output signal_type: `congressional_buy`.
- Evidence: member name, office, transaction date, amount band.
- Filter: skip index funds/ETF symbols (etf_blocklist in config).

### LEG 4 — Mechanical RVOL Squeeze
- Source: `search-company-screener` (marketCapLowerThan=2B,
  isActivelyTrading=true, paged; dedup with seen-set per the documented
  quirk) x `technicalIndicators relative-strength-index` 1day (latest bar
  volume vs 30-day average) for the freshest RVOL movers ONLY (cap: 50
  screener rows/run).
- Signal: RVOL >= 3.0 AND no catalyst brief for the symbol in the last 14
  days (checked against signal_briefs.json history) — i.e. volume BEFORE
  news. Also record `shares-float` freeFloat % (< 15% = high-squeeze).
- Output signal_type: `rvol_squeeze`.
- Evidence: rvol, freeFloat %, price, volume vs avg, no-news-confirmed bool.

### LEG 5 — Contract-to-Market-Cap Ratio (Revenue Disconnect)
- Source: `news search-press-releases` for the vetted universe (batched
  symbols, last 7 days) + LLM-free regex pre-pass for
  `\$[\d,]+[MBK]?\s*(million|billion|...)?\s*(contract|award|grant|order|
  purchase agreement|deal)` phrases; dollar values extracted deterministically.
- Signal: extracted contract value >= 10% of symbol market cap AND the
  counterparty language contains a Tier-1 token (DOD, DOE, NASA, DOE, DOE/
  CHIPS, hyperscaler names, prime contractors list in config).
- The dollar-extraction regex is the hard part — use the deterministic-validator
  pattern: parse, sanity-check (value < 100x mcap else flag for LLM review),
  keep the matched phrase in evidence for the LLM pass to confirm.
- Output signal_type: `contract_disconnect`.
- Evidence: extracted dollar, % of mcap, counterparty phrase (raw quote),
  source headline + URL.

### LEG 6 — IPO / De-SPAC 90-Day Window
- Source: `calendar ipos-calendar` (last 90 days, filter: exclude
  `exchange` != US majors? no — keep all US-listed; filter out ETFs via
  `etf_blocklist` + null-CIK profile check) + `company
  latest-mergers-acquisitions` (de-SPAC completions).
- Signal: new listing age <= 90 days, mcap < $2B, sector in the four pillars
  (semis/tech/energy/industrials). Maintain a `diamond_miner_new_listings.json`
  state file with first-seen dates; scan the 90-day window every run; drop
  after 90 days.
- Output signal_type: `new_listing_window` / `de_spac_window`.
- Evidence: listing date, deal size, market cap, exchange.

### LEG 7 — First-Coverage / Zero-Coverage Divergence
- Source: `analyst grades-summary` per symbol (cheap, one call) for the
  vetted universe; keep a `diamond_miner_coverage.json` snapshot {symbol:
  {num_analysts, consensus, as_of}} updated each run.
- Signal A: num_analysts went 0 -> N (first coverage event) AND consensus
  Buy/Strong Buy.
- Signal B: num_analysts == 0 (zero coverage) AND a same-run `insider_cluster`
  OR `contract_disconnect` hit on the same symbol (cross-leg confluence —
  this is the pure-diamond case).
- Output signal_type: `first_coverage` / `zero_coverage_confluence`.
- Evidence: analyst count delta, consensus, confluence legs.

### LEG 8 — Balance-Sheet Inflection
- Source: `statements enterprise-values` + `balance-sheet-statement` (quarter,
  limit 4) for the vetted universe — reuses data deepdive already pulls
  (shared fmp cache: write leg 8 to READ dd_briefing.json first, only fetch
  what's missing).
- Signal: (a) totalDebt fell >50% QoQ to near-zero (overhang removed), OR
  (b) cash+ST investments crossed above 3x |quarterly OCF| (runway
  extension), OR (c) a `retainedEarnings`/equity inflection consistent with
  going-concern removal (heuristic — flag for LLM, low confidence).
- Output signal_type: `balance_sheet_inflection`.
- Evidence: before/after debt or cash figures, quarter, computed runway.

## 5. Common candidate schema (the contract)

Every emitted candidate — regardless of leg — has EXACTLY this shape:

```json
{
  "symbol": "TICKER",
  "cik": "0001234567",
  "company_name": "Exact name from profile-symbol",
  "market_cap": 412345678,
  "price": 12.34,
  "free_float_pct": 9.8,
  "sector": "Technology",
  "industry": "Semiconductors",
  "signal_type": "contract_disconnect",
  "signal_family": ["leg5"],
  "confluence": ["leg2"],
  "first_seen": "2026-10-26T05:31:02Z",
  "last_seen": "2026-10-26T05:31:02Z",
  "hit_count": 3,
  "in_wiki": false,
  "in_watchlist": false,
  "evidence": {
    "summary": "<= 200 chars, LLM-readable one-liner",
    "raw": { "..." : "leg-specific structured payload" }
  },
  "caveats": ["13F 45d late, long-only"],
  "llm_priority": 1
}
```

Rules:
- `symbol` is UPPERCASE, US-listed, verified via profile-symbol in the same
  run (CIK present = real equity; null CIK = fund -> drop, never emit).
- `confluence` = OTHER legs that also hit this symbol this run. Non-empty
  confluence = `llm_priority` 1; single-leg = 2; single-leg + stale (>3 runs)
  = 3.
- `hit_count` is a rolling counter persisted in
  `diamond_miner_state.json` (same symbol re-hit across days = accumulating
  conviction; the dedup state file also stores per-signal-type first/last).
- Candidates with `in_wiki: true` or `in_watchlist: true` are written to
  `signal_refresh` notes (brief_count feeder for the promotion hook), NOT to
  the new-candidate output.

## 6. Output files (all under ~/.hermes/cron/diamond_miner/)

| File | Owner | Cadence |
|---|---|---|
| `diamonds_YYYYMMDD.json` | legs (deterministic) | daily, top-50 by llm_priority then hit_count |
| `diamonds_brief_YYYYMMDD.md` | LLM pass | daily, top-5 with thesis line each |
| `diamond_miner_state.json` | legs | persistent: per-symbol hit_counts, first_seen, 13F per-fund last-quarter snapshot, coverage snapshot, new-listings window |
| `diamond_miner_health.jsonl` | legs | append 1 line/run: leg status, call counts, error rates (feeds pipeline_health.md) |

State file is the SINGLE source of truth for "already seen." Never re-derive
history from the dated outputs.

## 7. LLM pass (one judgment, capped)

Input: `diamonds_YYYYMMDD.json` (max 50 rows, evidence summary fields only —
never raw payloads).
Task per candidate: (1) confirm or reject the signal from evidence summary
+ caveats (regex false-positives die here — e.g. "$5 million" in a headline
about a competitor's contract); (2) write a <=60-char thesis line; (3) assign
`diamond_score` 0-100 (confluence-weighted: single leg max 40, 2 legs max 70,
3+ legs or Tier-1 counterparty 70-100).
Output: `diamonds_brief_YYYYMMDD.md`, max 5 entries, each with: symbol, name,
mcap, signal(s), thesis line, diamond_score, evidence URL(s).
Entries with diamond_score >= 70 that are `in_wiki: false` are appended to
`vetted_tickers.json` (the funnel handoff) with a `source: "diamond_miner"`
tag so Deep-Dive can attribute them.

## 8. Job definition (cronjob_manage create — DRAFT, not fired)

- name: "Diamond Miner: sub-2B signal fan-out + confluence + funnel handoff"
- schedule: daily 05:30 UTC -> `0 5 * * *` UTC (verify against the existing
  jobs' timezone handling — the 4 current jobs use interval schedules; this
  is the first daily-anchored job).
- deliver: local files only (`"local"`) — Discord delivery rides the EXISTING
  score_alert_75.py path when a handoff candidate reaches 75 in the normal
  funnel. No direct Discord spam.
- enabled_toolsets: mcp, terminal, file, code_execution (same as Deep-Dive).
- prompt: "Run `python3 ~/.hermes/cron/diamond_miner/run_miner.py` (deterministic
  legs). Then read `diamonds_YYYYMMDD.json` and perform the judgment pass per
  the spec section 7. Write `diamonds_brief_YYYYMMDD.md`. Hand off
  diamond_score>=70 new names to vetted_tickers.json. Report: legs run,
  candidates emitted, confluence hits, handoffs, errors. Hygiene: scratch in
  /tmp only."

## 9. Build order & verification gates

1. **Leg 2 + Leg 3 first** (cheapest, highest S/N, single global feeds):
   verify against live data for 3 consecutive days; zero false-fund emits.
2. **Leg 1** after fund CIKs are verified (the one manual research task).
3. **Leg 4** — needs the screener-page dedup quirk handled (seen-set) and the
   "no news in 14d" check against brief history.
4. **Leg 6 + Leg 8** (state-window + reused deepdive data).
5. **Leg 5 + Leg 7** last (regex extraction + coverage snapshot — highest
   false-positive risk; gate each behind a 2-day shadow run where outputs are
   written but NOT handed off to vetted_tickers.json).
6. Final gate: a full week of shadow runs, then enable handoff.

Difficulty: 7/10 overall. Leg 5's dollar-regex is the 9/10 part; everything
else reuses proven patterns (drift_scan.py, pipeline_engine.py fmp_call,
vet_tickers).

## 10. Explicitly OUT of scope

- No new scoring system. Diamond Miner candidates use the EXISTING
  bifurcated 100-pt tracks once they enter the funnel.
- No global earnings-calendar usage (700KB payloads; per-symbol only).
- No 8-K velocity (8k-latest feed is structurally stale; freshness-gated).
- No short-side signals (13F/congressional are long-only; that's fine —
  the diamond mandate is long-biased).
- No market cap above $2B (that's the deepdive/vetted universe's job).
