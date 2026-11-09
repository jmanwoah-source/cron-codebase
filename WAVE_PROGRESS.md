# WAVE PROGRESS — mission_lou.md (LOU autonomous execution)
Started: 2026-10-31 ~17:00 EDT, session resumed from empty state.
Protocol: one step per resume; skip VERIFIED; loop until 8/8.

## Step 1 — build_universe.py --refresh 6h Cron (WAL + NULL-loop guard)
STATUS: VERIFIED (2026-10-31T17:25Z)

Delivered in ~/.hermes/scripts/build_universe.py (full rewrite of write path):
- mcap_attempts INTEGER DEFAULT 0 column added; schema migrated idempotently
  (ALTER TABLE guarded by PRAGMA table_info check; live DB migrated in place,
  no data loss).
- Refresh queue = WHERE (market_cap IS NULL OR market_cap = 0)
  AND mcap_attempts < 3 ORDER BY conviction DESC, track_tier ASC LIMIT 150.
  Valid FMP mcap (>0) -> UPDATE + mcap_attempts=0; null/0/err -> +1;
  at 3 -> quarantine market_cap=-1 (leaves queue permanently; local-source
  mcap > 0 revives). Quarantine sentinel carried across rebuilds via gather()
  backfill (step 8) so dead names never re-enter the 150-call cap.
- WAL safety: .tmp + os.replace pattern ELIMINATED for the DB. db_connect()
  = timeout=15.0, isolation_level=None (true autocommit so explicit
  BEGIN IMMEDIATE/COMMIT are honored), PRAGMA journal_mode=WAL,
  busy_timeout=15000. Default build + --refresh = in-place UPSERT
  (INSERT ... ON CONFLICT(ticker) DO UPDATE). --rebuild = BEGIN IMMEDIATE;
  DELETE FROM universe; INSERT ...; COMMIT; PRAGMA wal_checkpoint(TRUNCATE).
  Vanished-source rows are NOT purged: prior-DB mcap>0 / mcap_asof re-applied
  to keep fetched values through the lifecycle (preserves the 6h queue advance).
- CRITICAL BUG CAUGHT IN TESTING (before touching live data losslessly):
  default isolation_level shadows explicit BEGIN IMMEDIATE and a bare
  con.commit() closed the connection without committing DML — first test run
  left universe empty (801 -> 0 rows) with the "success" report lying.
  Restored from /tmp/universe_cache.db.bak_step1 (taken pre-test), fixed with
  isolation_level=None, re-ran clean. Verify the CHECK before trusting the
  JSON report.
- equity_verdicts (radar gate memo, 145 rows) survives in-place rebuilds;
  fresh-DB seed from wiki_mcap_cache.json kept as fallback.
- Legacy universe_cache.db.tmp removed on startup.

Verification (real runs):
- `--check` PASS: 801 rows, journal_mode=wal, no dups, no bad pillars,
  stale_null_queue=0 (no attempts>=3 still in queue).
- Local in-place run: 801 rows, 321 null-mcap (297 NULL + 24 zero),
  mcap_attempts all 0.
- `--refresh` run: filled=129, err=21, quarantined=0, 39.6s (< 60s budget),
  null_mcap 321 -> 192; 21 rows at mcap_attempts=1 (will retry next cycles,
  quarantine at 3).
- Cron wired: job 95617415066d (Universe Cache Refresh, every 360m = 6h,
  no_agent, script=universe_refresh_cron.py, deliver=local) — already existed
  but was paused in the fleet-wide pause of 10:53; RESUMED via
  `hermes cron resume`. Next run 2026-10-31T19:30 EDT.

NOTE for operator: the other 7 pipeline jobs (ba24fd4ddad6 SOURCES,
cfdab394b3bd PROCESSING, 7267b662cf52 DEEP-DIVE, e9711f5abb15 DRIFT,
bf59f02d39ba 13F, fd3a24115408 AFTER-HOURS, 3af8db9a6c2b FMP ANOMALY) are
STILL PAUSED from the 10:53 batch pause. Not resumed — fleet state is an
operator decision, only the Step-1 job was in scope.

## Step 2 — radar_13f.py 3-tier public-equity/ETF gate
STATUS: VERIFIED (2026-10-31T17:55Z)

Delivered in ~/.hermes/scripts/radar_13f.py (equity_gate rework):
- TIER 1 (0 API calls): tier1_verdicts() — ETF_BLOCKLIST (reason etf_blocklist)
  + universe_cache.db quarantines (market_cap=-1, reason quarantined_mcap)
  + ticker_resolution negatives (is_operating=0, reason cached_non_operating).
  Reads WAL-safe (timeout=15.0, busy_timeout=15000).
- TIER 2 (0 API calls): NON_OPERATING_RE on the 13F issuer string, spec
  pattern \b(ETF|TRUST|SHARES|FUND|PORTFOLIO|PRIVATE|SPCX|SMH|INDEX|LP|
  NOTES|DEBT)\b with ONE verified deviation: \bLP\b removed from the base
  pattern (word-boundary LP matches 'Inc' — period is a boundary — and would
  drop virtually every real operating issuer: 'Advanced Micro Devices, Inc.').
  LP/LLC handled by NON_OPERATING_LP_RE \b(L\.?P\.?|L\.?L\.?C)\b (matches
  'L.P.', 'LP', 'L.L.C.', 'LLC' whole words only). 19/19 issuer-string test
  cases pass (operating cos kept, trust/ETF/fund/LP vehicles dropped).
- TIER 3 (<= GATE_T3_MAX_CALLS=25 FMP profile-symbol calls/run/leg):
  remaining unknown candidates, isEtf==False AND isFund==False AND cik
  present AND US exchange. Every definitive verdict cached back into
  universe_cache.db: positives -> equity_verdicts (12h TTL, existing store,
  carried across rebuilds by build_universe), NEGATIVES -> new permanent
  table ticker_resolution (symbol PK, is_operating=0, reason, asof) so the
  next hourly run never re-queries that CIK/ticker. profile_error (transient
  transport) is fail-closed for the run but NOT cached (avoids 12h-poisoned
  drops during the filing wave). JSON mirror (wiki_mcap_cache.json values.*)
  still written through for JSON-only consumers.
- main() both gate legs (new + increases) now pass issuers= maps so tier 2
  has the 13F issuer text. Leg caps unchanged (GATE_MAX_SYMBOLS=40 /
  GATE_MAX_INC_SYMBOLS=12) — tier 3 additionally hard-capped at 25/leg so
  one leg can never starve the other.
- _save_ticker_resolution / _save_equity_db_verdicts / _load_equity_db_verdicts
  now WAL-safe connections (bug in _save_ticker_resolution fixed: bare
  `except` + no explicit commit in legacy isolation mode — now isolation
  level None + BEGIN IMMEDIATE/COMMIT, verified direct-write test).

Bugs caught by tests (all fixed before ship):
- t2_pending[t3:] slice used the t3 LIST instead of the budget int
  (TypeError on any run with >25 pending) — t3_budget.
- _neg_db referenced before assignment when 0 tier-3 calls ran.
- LP regex would regex-drop real operating companies (the Inc. case).

Verification (real runs):
- py_compile OK; full import OK.
- /tmp/test_step2_gate.py (mocked FMP transport): tier1 drops SPY/SMH/QQQ
  0 calls; tier2 drops TRUSTR/PORTF 0 calls; tier3 burns exactly 25 calls
  on a 30-unknown input, verdicts correct (2 pass, etf_or_fund,
  unrecognized_exchange:XETRA); persistence verified in both tables;
  second run re-checks only the 5 cap-exceeded tail (25 calls, none of the
  definitive symbols re-queried). ALL PASSED.
- `radar_13f.py --dry-run`: clean end-to-end run (session, cohort pull,
  tier-1 DB reads on the live WAL DB, early-quit at 0/12 Q3 2026 — expected,
  Q3 filings land ~Nov 14; Q2 2026 already status=done per state file so a
  re-fire is a no-op by idempotency).
- Deviation log: LP spec token handled per the deviation above; everything
  else in the mission spec pattern verbatim.

NOTE: job bf59f02d39ba (13F radar, 60m) is still in the fleet-wide paused
state — not resumed (operator decision; only Step 1's job was in scope).


## Step 3 — Google News wire-proxy dual entity resolution (primary_feeds.py)
STATUS: VERIFIED (2026-10-31T18:20Z)

Delivered in ~/.hermes/scripts/primary_feeds.py (f_gnews rework + new
deterministic helpers):
- Pass 1 (unchanged): exchange-prefixed ticker regex (NASDAQ:/NYSE:) on
  title+desc; bad-ticker filter + noise-phrase filter unchanged.
- Pass 2 (new, normalized fallback): resolve_entity() strips the trailing
  ' - Publisher' from <title>, normalizes (uppercase, punctuation stripped,
  legal suffixes INC/CORP/LLC/LTD/CO/COMPANY/HOLDINGS/GROUP/PLC/THE removed
  via norm_name()), and matches against the name index built from
  universe_cache.db (name column) + sec_tickers_cache.json: exact
  normalized-name containment (whole-word regex; single-word names require
  >=4 chars) else distinctive 2-word prefix. Generic first words
  (AMERICAN/NATIONAL/GENERAL/UNITED/FIRST/ADVANCED/GLOBAL...) do NOT block
  prefix keys (verified deviation: that would exclude GENERAL DYNAMICS and
  AMERICAN AEROSPACE — the defense primes this lane hunts); a generic first
  word only EXCLUDES the hit when the prefix key is ambiguous (>1 ticker).
  Live index: 8,068 exact names / 5,450 prefix keys.
- Selection: collect ALL phrase candidates first (no early cut), dedupe by
  link + normalized headline, then rank by extracted dollar amount
  (extract_dollars(): $ + number + b/bn/m/million/k/thousand units, largest
  wins; None last), then pass-1 hits, then watchlist overlap. Cap = 8
  dedup items/run (GNEWS_MAX_ITEMS; feeds.json gnews.max_per_run 3 -> 8,
  respecting the 25-item primary_feeds ceiling). S1 = watchlist overlap OR
  extracted $ >= $1M. Entry text carries tickers + entity name + $ figure.

Verification (real runs):
- /tmp/test_step3_resolve.py: norm_name (5 cases), extract_dollars (5
  cases, incl. $2,500,000 comma parsing + multi-amount max), resolve_entity
  against the LIVE index: GD/LMT/NVDA/AVAV/NOC resolved from wire-style
  titles, 2 negative controls return nothing. ALL PASSED.
- Live f_gnews() (no state writes — the paused Sources job owns
  feed_raw/feed_state): 8 entries returned, pass-2 entity matches visible
  in the text field (INNOVEX INTERNATIONAL, LEIDOS, GENERAL DYNAMICS,
  PARSONS, NASDAQ), $ extraction visible ($2.2B Oura IPO at top),
  dedup held (8 unique headlines, cap respected).
- `primary_feeds.py status` runs clean; py_compile OK. feed_raw.json /
  feed_state.json untouched by the test (timestamps pre-date the test).
- Deviations logged: (a) generic-first-word rule per above (spec would
  drop the prime contractors); (b) max_per_run 8 from feeds.json config
  (script default GNEWS_MAX_ITEMS=8 matches).

NOTE: Sources job ba24fd4ddad6 (0 */2) still paused fleet-wide — not
resumed; the gnews lane is wired and will run on the next Sources fire.


## Step 4 — api.usaspending.gov pre-flight + batched prime/subaward queries
STATUS: VERIFIED (2026-10-31T20:05Z)

Pre-flight (live POST to spending_by_award from this host, HTTP 200 with
real rows) — API schema facts discovered by probing the validator (the
mission's assumed schema was WRONG in 5 places; all verified live):
1. `filters` is a SINGLE OBJECT with NAMED keys — not field/values pairs
   (422 "invalid type object"), not a list (422). Required key:
   award_type_codes.
2. `subawards` filter key does NOTHING (verified: byte-identical results
   for true/false). The subaward leg requires body-level
   `spending_level: "subawards"` + Sub-Award field names +
   `sort: "Sub-Award Amount"` (the prime sort 400s: "not found in
   Sub-Award mappings").
3. NAICS filter key = `naics_codes` (list of 6-digit codes). The
   `field:'naics'` form is SILENTLY IGNORED (verified) — never use it.
   `psc_codes` 422s with the NAICS values (good discriminator).
4. Time filter = `time_period: [{start_date, end_date}]` (LIST form; dict
   form 422s). Semantics are award-level, NOT action-date; sort by
   "Action Date" is HTTP 400 (not a valid sort key). => the real freshness
   gate is LOCAL (action/start date window), per-row.
5. Hydrate GET /api/v2/awards/{generated_internal_id}/: 
   `base_and_all_options` is a top-level FLOAT (options-inclusive; can
   exceed total_obligation — UCR > 1 is normal), `total_obligation` float.
   UCR = base/total, None/zero-safe (unit-tested: 6 cases incl. the live
   Boeing IDV 23.92B/22.67B = 1.0551).

Delivered: NEW ~/.hermes/scripts/usaspending_feed.py (stdlib only,
flock + atomic-write discipline matching primary_feeds.py):
- 1 batched POST prime (spending_level=awards, 50 rows) + 1 subaward
  (spending_level=subawards, 50 rows), NAICS 334413/336414/334511/325180/
  333242, award_type_codes A-D, sort Award Amount desc, Award Amount in
  fields (spec compliance on everything the API honors).
- 30d local freshness gate (Action Date or Start Date >= cutoff);
  90d seen-ledger prune; state in usaspending_state.json.
- Recipient matching via the Step-3 normalizer (primary_feeds
  norm_name + _gnews_name_index — universe_cache.db names +
  sec_tickers_cache.json). Market cap pulled from the universe DB;
  award_to_mcap revenue-disconnect ratio computed per match (the Step-1
  "revenue disconnect" class).
- Hydrate ONLY matched prime awards >= $1M, max 5/run; UCR per hydration.
- Feed entries (matched rows only, top 5 by $) appended to feed_raw.json
  (source_name=usaspending, category=defense_gov, S1 when
  award/mcap >= 0.05 or mcap < $2B). Report: usaspending_awards_report.json.

Verification (real runs):
- Both legs return DISTINCT result sets (gid overlap 0 vs 50 before the
  spending_level fix; after: prime 9 fresh / subaward 0 fresh in a
  quiet subaward week — correct).
- Dry-run end-to-end: 50+4 raw, 9+0 fresh, 1 local match (MERCURY SYSTEMS
  INC -> MRCY), 3.8s, zero errors. Hydrate path unit-verified against a
  live Boeing IDV (UCR 1.0551, base_and_all_options float confirmed).
- UCR unit tests: 6/6 pass (None/None, 0-denominator, string input,
  valid, options>base).
- Dry-run purity: feed_raw.json / state / report all untouched by the
  --dry-run run (timestamps verified).
- NOTE (operator): the 2026-10 window's top matched prime (Axiom Space
  $12M) is not in the tracked universe — matches come only from tracked
  names by design. The job is deterministic + no_agent-ready but NOT
  cron-wired this step (mission Step 4 does not ask for a schedule; wire
  it into the Sources lane or as its own 6h job when the fleet resumes).


## Step 5 — FMP Anomaly Feeder (<=15 calls, insider buys + breakouts)
STATUS: VERIFIED (2026-10-31T20:50Z)

Delivered: ~/.hermes/scripts/fmp_anomaly_feed.py REWRITTEN around a hard
15-call budget (CALL_BUDGET; can_afford() peek + budget() single-spend in
fmp(); the old design burned ~300+ calls and could not run on 6h cron
without rate-limit churn). fmp_client.py inspected first (per mission):
it exposes FMP.call(name, **args) over the Streamable-HTTP MCP session;
the feeder uses pipeline_engine.fmp_session/fmp_call (the same transport
the rest of the fleet uses) — fmp_client.FMP is the alternative for
scripts that want a context-manager client.
- LANE 1 (1 call): insiderTrades latest-insider-trade (limit 200),
  transactionType 'P', price*shares >= $100K, S1 >= $500K. NEW gate:
  symbol must be in the tracked universe (universe_cache.db mcap>0 +
  watchlist, 0 calls) — the old design called profile-symbol up to 15x
  for the equity gate; that budget does not exist anymore, the tracked
  universe IS the gate.
- LANE 2 (<= 12 calls = 6 names x quote+chart): RVOL volume breakouts on
  conviction-priority names (conviction DESC, track tier ASC — the
  universe-refresh priority). quote = live volume; chart eod-light rows
  [1:6] = prior-5-session volumes (newest-first, row 0 = most-recent bar,
  live-verified shape {'symbol','date','price','volume'}). Gate:
  vol_today >= 3.0 x avg5, S1 >= 5.0x. Quote-only fallback when the
  budget cannot cover the chart call.
- LANE 3 (residual, here 2 calls): earnings-reaction gaps vs priceAvg200
  (|gap| >= 8%, vol >= 200K, S1 >= 15%) on the next conviction-ranked
  names.
- Dedup state anomaly_feed_state.json preserved (14d prune, same key
  scheme: insider:/:volume:/:earnings:). Appends to feed_raw.json (same
  contract; source_name=fmp_anomaly_feed; S1/S2 severity).

Verification (real runs):
- Budget math verified call-by-call via a traced fmp() hook: exactly
  1 + 6x2 + 2 = 15/15 (bug caught: the first version's budget() both
  peeked AND spent, so guard checks double-counted and the run burned
  15 on 7 real calls; split into can_afford()/budget()).
- Dry-run: 2 S1 anomalies (INFQ -27.8% vs 200d, AVAV -32.0% vs 200d),
  no writes (feed_raw.json + state timestamps verified unchanged).
- REAL run: appended=2 to feed_raw.json, state last_run updated, 18 seen
  keys; feed entries well-formed (severity/lane/text all present).
- Dedup verified: immediate second run -> 0 raw anomalies, nothing
  appended (same-day keys already seen), budget still 15/15 (the sweep
  is the cost, not the append).
- Cron: job 3af8db9a6c2b (FMP Anomaly Feeder, every 360m, no_agent,
  script=fmp_anomaly_feed.py) already exists at the right cadence —
  STILL PAUSED (fleet-wide 10:53 pause; not resumed, operator decision).
  The rewrite is drop-in for that job (same argv, same lock, same
  state/report files).


## Step 6 — ah_research.py promotion bridge (atomic claim pattern)
STATUS: VERIFIED (2026-10-31T21:20Z)

Delivered (race-condition fix, single-writer atomic claim):
- ~/.hermes/scripts/ah_promote_bridge.py REWRITTEN: the bridge's ONLY
  write is now a single atomic fsync+os.replace of high-magnitude hits to
  ~/.hermes/cron/ah_promotions.json (Path.home()/expanduser throughout).
  Triggers: (A) extracted $ >= $25M + catalyst keyword OR backlog jump
  (backlog/orders-on-hand phrase + $ figure — the mission's backlog-jump
  class, new BACKLOG_KW), kind dollar_catalyst|backlog_jump; (B) Tier-B
  supplier callout. Dedup ah_bridge_state.json (ticker:dollar:headline-60,
  14d) preserved. NO MORE read-modify-write of frontier_candidates.json
  (13F radar owns it) or watchlist_state.json (Deep-Dive promotion now
  flows through the normal brief -> conviction -> vetted/watchlist path
  once claimed items clear the LLM So-What gate). Legacy ah_bridge lane
  data from 2026-10-30 left untouched (audit trail preserved).
- CONSUMER: NEW pipeline_engine.claim_ah_promotions() wired at the top of
  phase_process() (Processing, 15 */2). Pattern per mission spec:
  os.replace(ah_promotions.json -> ah_promotions.claiming.json) as the
  single-writer claim (concurrent claimant gets FileNotFoundError ->
  clean no-op), ingest + dedup from the CLAIMING file against
  ah_bridge_state.json + the feed's headline/url sets, append feed
  entries to feed_raw.json (engine-owned, runs under the engine's own
  lock discipline), unlink claiming ONLY after both writes land (crash
  recovery: a stale claiming file is re-claimed next run and the 14d
  dedup state makes re-ingest idempotent — tickets never lost).
  FileNotFoundError when no promotions exist = normal steady state.
  Claim failures never kill the phase (try/except, processing continues).

Verification (real runs, synthetic fixture then restore):
- Bridge dry-run on crafted raw (3 candidates): promoted exactly the
  3 correct tickets — AVAV $120M dollar_catalyst (S1), AVAV $850M
  backlog_jump (S1), NVDA tier_b_supplier (S2); the no-$ chatter item
  correctly NOT promoted. Wrote ah_promotions.json with 3 tickets, rc 0.
- claim #1: (3,0,True), feed 325 -> 328, promotions + claiming files
  both gone, entries well-formed (severity S1/S2, source_name=ah_bridge,
  text_source=ah_bridge_claim).
- claim #2 (no file): (0,0,False) clean no-op.
- claim #3 (re-create SAME tickets): (0,2,True) — 100% dedup, feed
  unchanged at 328->328 (no double-append).
- CRASH RECOVERY: planted a stale ah_promotions.claiming.json (dead
  claimant) with no source file -> claim (1,0,True), claiming removed,
  IONQ entry in feed.
- Fixtures restored after tests: feed_raw.json (325 entries, 0 ah_bridge),
  ah_research_raw.json, after_hours_research.json, ah_bridge_state.json
  all reset; no ah_promotions* files left. Real raw dry-run: "no
  high-magnitude catalysts this run", rc 0, zero writes.
- NOTE: ah_research.py itself unchanged (it already calls the bridge
  in-process after the collector; the bridge is what changed). The 90m
  lane (fd3a24115408) and Processing (cfdab394b3bd) remain in the
  fleet-wide paused state; the fix is live the moment Processing next
  fires (claim_ah_promotions is unconditional at phase_process start).


## Step 7 — sec_slicer.py pure-stdlib 10-K slicer (AVAV/ALAB verify)
STATUS: VERIFIED (2026-10-31T20:55Z)

Delivered: NEW ~/.hermes/scripts/sec_slicer.py (pure stdlib: json, re, os,
sys, html.parser, urllib.request, datetime — no bs4/lxml). Zero API keys
(UA-gated SEC EDGAR, reuses thesis_fetch.py's sec_tickers_cache.json).
Standalone: `python3 sec_slicer.py AVAV ALAB`. Output:
~/.hermes/cron/sec_slices/<TICKER>.md + .summary.json + last_run.json.
HTML cache: ~/.hermes/cron/sec_cache/<TICKER>_<accession>.html (cache-hit
re-run = 0.67s). Latest-10-K resolution via data.sec.gov/submissions
(parallel arrays, form==10-K).

Per mission spec (Wave C, Step 7):
- Pre-strip (re.DOTALL|re.IGNORECASE) BEFORE html.parser:
  <TYPE>(GRAPHIC|ZIP|EXCEL|XML|EX-101).*?</DOCUMENT> and <ix:header>.*?
  </ix:header>. Live fact: both 2026 filings carry 1 ix:header + 1
  ix:hidden but 0 inline TYPE/DOCUMENT exhibits — the ix:header/ix:hidden
  strip is what removes ~234KB (AVAV) / ~115KB (ALAB). Documented
  ADDEDITION: <ix:hidden>.*?</ix:hidden> stripped too (inline-XBRL
  boilerplate that duplicates visible text).
- HTMLParser (Flatten subclass) flattens HTML -> text; <table> rows ->
  pipe-delimited Markdown (cells whitespace-collapsed, | escaped, 200-char
  cell cap, 60-row/table cap); <script>/<style> skipped. Zero-width chars
  (U+200B/200C/200D/2060/FEFF) are KEPT as real separators (filings glue
  "Part I\u200bItem 1." / "Contents\u200bItem 1A." together) — the ITEM
  regex's separator class tolerates them.
- TOC skip (spec: span length e_pos - s_pos > 1500 chars): implemented
  as the PRIMARY rule on anchor links (heading inside an <a href="#id">
  whose target id offset is > 1500 chars away = TOC entry). Documented
  REFINEMENT: pure anchor-span over-eliminates real PART I body headings
  (a 2-item "other" cluster also sits within 1500 chars of TOC rows), so
  a grid-density rule is layered on (>=8 distinct Item numbers within the
  1500-char TOC window = TOC grid; a body section never has that density).
- Cross-reference guard: body refs like `Item 7, "Management's Discussion
  ..."` (comma + QUOTED title) otherwise win the max-section-span
  selection (they start mid-section and run to EOF). A real heading is
  "Item N." + UNQUOTED title — comma-after-number and quoted-title
  candidates are dropped (this was the ALAB Item 7 / AVAV Item 7 bug).
- Heading selection: per tracked Item, keep the candidate whose section
  span is LARGEST (real section runs to the next heading; a mid-section
  cross-ref doesn't).

Item extraction (hard-capped 30,000 chars total, per-item budgets
1=6000 / 1A=5000 / 7=10000 / 8=9000):
- Item 1 (Business), Item 1A (Risk Factors), Item 7 (MD&A) -> straight
  section slice to next heading, budget-truncated.
- Item 8 (Notes): anchor the LAST "Notes to [Consolidated|Combined|The]
  Financial Statements" (the EARLIER occurrence is the financial-statements
  INDEX), cut at SIGNATURES/EXHIBIT INDEX, then keep only NOTE-N blocks
  matching (segment|major customer|concentration|revenue|inventory|
  commitment). NOTE-N detection is line-AGNOSTIC (notes sections are one
  continuous line): "Note N —/–/./space Title" (NOTE_HEAD_RE) plus the
  compact "N. Title" form (NOTE_NUM_RE); xref tails ("for further
  details"/"see Note X") filtered. Keyword-matching-paragraph fallback if
  <2 note headings found.

Verification (real runs, live SEC fetch):
- py_compile OK. Unknown ticker -> clean unknown_ticker error (no crash).
- AVAV (AeroVironment, 10-K filed 2026-06-29, acc 0001104659-26-078906):
  raw=4,304,106 -> stripped=4,070,232 (233,874 saved) -> text=540,244.
  headings cand=19 selected=17 toc_dropped=2. Item chars
  {1:6032, 1A:4426, 7:10033, 8:9000}, total=29,883/30,000.
- ALAB (Astera Labs, 10-K filed 2026-02-20, acc 0001736297-26-000010):
  raw=2,244,531 -> stripped=2,129,793 (114,738 saved) -> text=381,960.
  headings cand=21 selected=18 toc_dropped=3. Item chars
  {1:6032, 1A:4430, 7:10033, 8:9000}, total=29,883/30,000.
- All 4 sections >= 1500 chars of REAL content both tickers (verified by
  reading each slice head): AVAV Item 1 "We are a defense technology
  provider...", Item 1A "A description of the risks...", Item 7
  "Management's Discussion ... Introduction The following discussion of
  our financial condition...", Item 8 note 4 "Inventories, net...". ALAB
  Item 1 "Our mission is to innovate, design...", Item 7 "The following
  discussion and analysis...", Item 8 note 1 "Nature of Business...".
- Item 8 NOTE-N path confirmed (not fallback): AVAV 27 note headings
  found, ALAB 18; keyword filter active (AVAV seg/rev/inv/cust/comm hits,
  ALAB customer=15 revenue=13). Both slices under the 30,000 hard cap.
- Bugs caught + fixed during verification: (a) item-7 KNOWN_NAMES pattern
  had a typo `d\s*cussion` (missing the 'i' of discus-sion) so MD&A never
  matched; (b) ALAB "ContentsItem 1A." / AVAV cross-refs won the max-span
  selection -> added comma+quoted-title cross-ref guard; (c) Item 8
  NOTES_START_RE first-matched the financial-statements INDEX row ->
  anchored on the LAST anchor; (d) NOTE_HEAD_RE required end-of-line but
  notes are one continuous line -> made line-agnostic + NOTE_NUM_RE.

NOTE: standalone tool, NOT yet wired into Deep-Dive (mission Step 7 only
asks to "verify standalone on AVAV and ALAB before wiring"). Drop-in for
Step 8 (Deep-Dive adversarial red team) which consumes the sliced
<tickers> Item 1/1A/7/8 corpus — the .summary.json per-item char counts
give the budget signal.

## Step 8 — Deep-Dive adversarial red team + options delta queue
STATUS: VERIFIED (2026-11-01T07:00Z) — CLOSED (operator, 2026-11-01T13:45Z;
options_delta_signals PK fixed in the POST-WAVE FIX entry below; red_team_verdicts
left as an append-oriented ledger by operator decision — see the scope note at
the bottom of this file).

Delivered: NEW ~/.hermes/scripts/deep_dive_red_team.py (stdlib + two
existing fleet deps: sec_slicer for the sliced-10-K corpus,
thesis_llm.vllm_chat/extract_json for the local vLLM transport; no new
API keys, no FMP calls) — run as ONE terminal command from the Deep-Dive
job (prompt extended with STEP 6d, after signals_bridge.py).

Two halves, one run, both per mission spec (Wave C, Step 8):
1. ADVERSARIAL RED TEAM (gate: best_track_score >= 65 — 19 candidates):
   full sliced 10-K corpus (Item 1/1A/7/8, ~14KB budget after a
   kill-relevant re-ordering: header + 1A + 7 + 8 first) + the engine's
   bifurcation signals -> local vLLM (localhost:8000, /model) as a
   hostile short-seller tribunal. Static-prompt doctrine (ticker +
   corpus at the END, byte-identical Radix prefix — same pattern as
   thesis_llm.py; vLLM thinking mode disabled). Extracted:
   thesis_kill_probability (0.0-1.0), customer concentration >30%
   (customer_concentration_pct + top_customers), dilution >8%
   (dilution_pct_yoy — engine share_count_yoy_pct used verbatim as
   GROUND TRUTH, model instructed not to re-estimate), WIP inventory
   stall (true/false from Item 8 inventory notes + MD&A). Persisted in
   ~/.hermes/cron/red_team.db table red_team_verdicts — SQLite WAL
   (journal_mode=WAL, busy_timeout=15000, BEGIN IMMEDIATE/COMMIT,
   PK (ticker, asof) with ON CONFLICT upsert — idempotent re-runs) —
   and merged into ticker_scores.json under a per-ticker "red_team"
   key + red_team_asof (atomic tmp + os.replace; the JSON file is not a
   WAL db so replace is safe; DB writes went to the WAL files).
   Fail-closed: vLLM down / unparseable reply / ticker-mismatch ->
   deterministic heuristic verdict (engine kill flags only, never
   invented numbers) stored with degraded=1 — the row is never dropped.
2. OPTIONS DELTA STRUCTURER (gate: ALSO market_cap >= $500M liquid
   trigger — 18 names; direction = Track A/B >= 75 -> CALL,
   Track C <= 35 + non-passing -> PUT): rows written to
   ~/.hermes/cron/options_delta_queue.db table options_delta_signals —
   same WAL discipline. Columns: direction, primary_track, scores,
   price, deterministic strike selection per the Rubber Band Man
   mandate (CALL: bts>=80 -> 20% OTM / 75d else 15% OTM / 90d, tightened
   5% for >= $25B mcap; PUT: 20% OTM / 45d), thesis_hook, and the
   red-team verdict merged in (kill prob / concentration / dilution /
   WIP) so the downstream options scanner sees the bear case next to
   each signal. No-direction liquid names (pass-eligible <75, no
   mirage) are NOT enqueued — the queue holds tradeable triggers only.

Verification (real runs, live vLLM + live SEC):
- Unit: direction_of (CALL/PUT/NONE), strike_logic (4 cases),
  heuristic_verdict (dilution+mirage -> 0.65) all pass.
- Prompt .format() KeyError caught in smoke test (JSON braces in the
  schema example) -> prompt split to PROMPT_HEAD/TAIL/END concatenation;
  compile + re-smoke pass.
- Full run (19 candidates, 538s wall, 19/19 real vLLM verdicts, 0
  heuristic, 0 degraded): SMTC 0.55 (dilutive convert swap), SAIC 0.65
  (98% gov't concentration), ONTO 0.45, LITE 0.65 (wip=TRUE, $1.55B
  convert wall), LSCC 0.75, AXTI 0.85 (+45.6% share dilution), CRDO 0.85
  (33% top-cust, top-10=90%), MTSI 0.45 (deferred-rev collapse), NTAP
  0.45, NXPI 0.45, TXN 0.15, INFQ 0.95 (+1309.8% dilution), ALAB 0.65
  (40% cust), ALGM 0.65, CACI 0.65 (95.6% gov't), FPS 0.85 (wip=TRUE,
  +31.9% dilution), AMKR 0.65, KEYS 0.55, XMAX 0.95 (wip=TRUE, +344%
  dilution; sub-$500M so red-team only, not enqueued).
- options_delta_signals: 5 enqueued (SMTC/SAIC/ONTO/LITE/LSCC — the
  five Track A >= 75 liquid names; zero PUTs this cycle because no
  mirage <=35 non-passing name cleared the liquid gate), strike logic
  verified (SMTC 20%/75d, SAIC 20%/75d, ONTO/LSCC 15%/90d, LITE 10%/90d
  mcap-tightened), each row carries its red-team verdict.
- ticker_scores.json merge verified: 19/19 rankings carry "red_team",
  uniform asof, 70 rankings intact, no .tmp leftover; --max-candidates
  batching keeps earlier-batch verdicts (additive merge).
- --dry-run verified clean: computes (3 vLLM calls) but writes NOTHING
  (both DBs row-count/asof unchanged, scores file asof unchanged,
  report mtime unchanged, no tmp).
- WAL verified on both DBs (PRAGMA journal_mode = wal; checkpointed at
  end of run; wal/shm files absent after close).

Known deviations / notes (operator):
- TXN's 2026-02-06 10-K (txn-20251231.htm) is parsed by sec_slicer with
  0 headings selected (non-standard heading markup) -> corpus = header
  block only (261 chars). The tribunal still ran on it (kill=0.15 from
  engine signals + what it had); a sec_slicer heading-pattern extension
  for that filing family is the follow-up if TXN stays in the set.
- vLLM ~25-38s/verdict -> full 19-candidate set ~= 9 min. Inside the
  10-min Deep-Dive window with a tight margin; the STEP 6d prompt
  documents a --max-candidates 9 + 14 split for tight windows (additive
  by design).
- The 06:00Z + 06:06Z batch-test asof groups in both DBs (pre-final-run
  rows) were left as an audit trail, not purged; the 06:13Z asof group
  is the authoritative full-set snapshot and matches
  red_team_report.json + ticker_scores.json.
- Cron wiring: STEP 6d appended to job 7267b662cf52's prompt
  (jobs.json updated; hermes cron edit round-trip verified). Job is
  STILL PAUSED (fleet-wide 10/31 pause, operator decision) — the step
  is live the moment Deep-Dive next fires.

## POST-WAVE FIX — options_delta_signals PK correction (operator, 2026-11-01T13:30Z)
STATUS: VERIFIED

Corrects a schema line crossed in Step 8 (the 3 pre-final asof groups
06:00/06:06/06:13Z that Step 8 left "as an audit trail" are NO LONGER the
intended state — the composite PK was the bug, not just test residue).
Root cause: `asof` is minute-resolution (strftime %Y-%m-%dT%H:%MZ,
main() line ~518), so every run minted a NEW asof and the
`PRIMARY KEY (ticker, asof)` + `ON CONFLICT(ticker, asof)` never matched
across runs -> one row accumulated per test/run cycle (15 rows / 5 tickers
live, 3 each).

Delivered in ~/.hermes/scripts/deep_dive_red_team.py:
- SCHEMA_OPTIONS: PRIMARY KEY (ticker, asof) -> PRIMARY KEY (ticker).
- write_options_rows upsert: ON CONFLICT(ticker, asof) -> ON CONFLICT(ticker)
  + asof=excluded.asof added to the SET clause (asof still written per row,
  it is just no longer part of the identity).
- NEW migrate_options_db(path): idempotent. No-op when the table is absent
  (init_db creates it fresh with the new PK) or already has a ticker-only
  PK. Otherwise in-place RENAME + rebuild + backfill keeping only the LATEST
  row per ticker (ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY generated
  DESC, asof DESC) = 1 — the 06:13Z authoritative snapshot wins over the
  06:00/06:06 pre-final rows) + DROP + wal_checkpoint(TRUNCATE). WAL-safe
  (in-file rebuild, never .tmp + os.replace — Step-1 doctrine).
- main(): calls migrate_options_db(DB_OPTIONS) right after init_db (non-dry);
  if it rebuilt the table it re-opens con_op so the write connection sees the
  post-migration schema, and logs "[migrate] ... kept latest of N rows -> M".

Verification (real runs):
- py_compile OK.
- Backup taken pre-migration (/tmp/options_delta_queue.db.bak_preschema, 15 rows).
- migrate_options_db RUN1 = (True, 15, 5); RUN2 idempotent = (False, 5, 5).
- Final live state: 5 rows / 5 distinct tickers (LITE/LSCC/ONTO/SAIC/SMTC),
  ALL asof=2026-11-01T06:13Z (latest kept), PK=['ticker'], real risk_note data
  intact, zero test artifacts.
- Back-to-back upsert passes through the REAL write_options_rows(): two
  successive runs with a fresh minute-asof (2026-11-02T09:00Z) on the same 5
  tickers -> row count held at 5 both times (UPDATE, not INSERT), asof updated
  on all 5. Then the live DB was restored from the pristine pre-schema backup
  and re-migrated once to end in the clean 06:13Z state (no test residue).
- Idempotent re-call on the migrated DB = (False,5,5) stable no-op.

SCOPE NOTE (operator DECISION, 2026-11-01T13:45Z — Step 8 CLOSED): this fix
touched options_delta_signals ONLY. The sibling table red_team_verdicts
(red_team.db) uses the SAME composite PK (ticker, asof) + minute-asof
pattern (42 rows / 19 tickers as of this fix). Operator decision: LEAVE
red_team_verdicts UNTOUCHED — it is intentionally an APPEND-ORIENTED LEDGER
(one row per asof snapshot; the 06:00/06:06/06:13Z groups are the audit
trail), with PRIMARY KEY (ticker, asof) BY DESIGN. Do NOT migrate it.
merge_scores reads the ledger by asof, which is why the composite identity
is correct there. This is the ledger-vs-upsert distinction in the
sqlite-upsert-pk-design skill: red_team_verdicts = ledger, options_delta_signals
= upsert. No further action required; Step 8 is CLOSED.
