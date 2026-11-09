# LOU — AUTONOMOUS STATE-RECOVERY & EXECUTION DIRECTIVE

## 0. CONTEXT & INSTANCE HANDOFF PROTOCOL (READ FIRST ON EVERY TURN/SPAWN)
To prevent context window exhaustion, execute **ONE step at a time**:
1. **Check State First:** Before reading or editing code, read ~/.hermes/cron/WAVE_PROGRESS.md (create it if missing).
2. **Resume Point:** Skip any Step marked STATUS: VERIFIED. Identify the first incomplete Step (1 through 8).
3. **Scoped Context:** Only load the Python file(s) required for that single active step. Do NOT read files for future steps yet.
4. **Commit & Handoff:** Once the active step passes its test, immediately append Step X — STATUS: VERIFIED to ~/.hermes/cron/WAVE_PROGRESS.md.
5. **Sub-Agent / Compaction Rule:** If you spawn a sub-instance or compact context for the next Wave, pass ONLY this instruction: "Read ~/.hermes/cron/mission_lou.md and ~/.hermes/cron/WAVE_PROGRESS.md, and execute the next unverified step."
6. **No Human Wait:** Do NOT stop to ask for confirmation between steps or waves. Loop until all 8 steps are VERIFIED.

---

### WAVE A: State Hygiene & Fast Gates (Steps 1, 2, and 6)

1. **Step 1 (build_universe.py --refresh 6h Cron) — 2 Safeguards:**
   - **Prevent Permanent-NULL Loops:** Ensure column mcap_attempts INTEGER DEFAULT 0 exists in universe_cache.db (handle ALTER TABLE idempotently). Query WHERE (mcap IS NULL OR mcap = 0) AND mcap_attempts < 3 ORDER BY conviction DESC, track_tier ASC LIMIT 150. On each FMP profile-symbol check, if mcap is valid (>0), update mcap and reset mcap_attempts = 0. If null/0, increment mcap_attempts = mcap_attempts + 1. When mcap_attempts >= 3 (delisted/shell/ADR), set mcap = -1 so those rows never clog the 150-call refresh cap on future runs.
   - **SQLite WAL Safety:** Never use .tmp + os.replace on an active WAL database if readers are attached, as unlinking the base .db inode while .db-wal / .db-shm exist causes lock/schema corruption. Always connect with timeout=15.0 and PRAGMA journal_mode=WAL; PRAGMA busy_timeout=15000;. Perform --refresh via in-place UPDATE transactions, and if --rebuild is used, execute BEGIN IMMEDIATE; DELETE FROM ...; INSERT ...; COMMIT; inside the existing DB file followed by PRAGMA wal_checkpoint(TRUNCATE);.
   - Wire the 6h cron via cronjob_manage.

2. **Step 2 (radar_13f.py Public-Equity & ETF Gate) — 3-Tier Cascade:**
   - Before burning FMP calls on 13F holdings, filter in 3 fast tiers:
     1. Check existing ETF_BLOCKLIST + universe_cache.db first (0 API calls).
     2. Regex-drop obvious non-operating names in the 13F issuer string: \b(ETF|TRUST|SHARES|FUND|PORTFOLIO|PRIVATE|SPCX|SMH|INDEX|LP|NOTES|DEBT)\b.
     3. For remaining unknown confluence candidates (cap at <=25 calls/run), resolve ticker/CIK via sec_tickers_cache.json / universe_cache.db first, then query FMP profile-symbol (isEtf == False, isFund == False, isActivelyTrading == True, US exchange: NASDAQ, NYSE, AMEX) and cache the result into universe_cache.db (storing negative hits as is_operating = 0 or mcap = -1) so we never re-query that CIK/ticker on the next hourly run.

3. **Step 6 (ah_research.py Promotion Bridge) — Single-Writer Atomic Claim Pattern:**
   - **Critical Race-Condition Fix:** ah_research.py (90m) and Processing (15 */2) overlap in wall-clock time every 6 hours. Do NOT let ah_research.py do a read-modify-write on frontier_candidates.json or feed_raw.json.
   - Have ah_research.py write high-magnitude hits (extracted $ >= $25M, backlog jumps, or Tier-B supplier callouts) to ~/.hermes/cron/ah_promotions.json via atomic write. Always resolve paths with Path.home() / os.path.expanduser.
   - **Atomic Claim in Processing:** At the start of Processing, do NOT read-then-truncate ah_promotions.json in place. Instead, atomically rename os.replace(expand("~/.hermes/cron/ah_promotions.json"), expand("~/.hermes/cron/ah_promotions.claiming.json")), ingest and dedup from .claiming.json, and unlink .claiming.json when finished. Handle FileNotFoundError gracefully if no promotions exist.

---

### WAVE B: Unblocked Discovery Feeders (Steps 3, 4, and 5)

4. **Step 3 (Google News Wire-Proxy in primary_feeds.py) — Dual Entity Resolution:**
   - **Fix:** After pulling Google News RSS queries (when:6h / when:12h), run two deterministic passes in Python:
     - Pass 1: Regex match (?:NASDAQ|NYSE|AMEX)\s*[:\-]\s*([A-Z]{1,5}) in title + description.
     - Pass 2 (Normalized Fallback): Strip trailing - Publisher from <title>. Normalize headline prefix and universe_cache.db (company_name) + sec_tickers_cache.json by uppercasing, stripping punctuation, and stripping legal suffixes \b(INC|CORP|CORPORATION|LLC|LTD|LIMITED|CO|COMPANY|HOLDINGS|GROUP|PLC|THE)\b. Match exact normalized name or distinctive 2-word prefix (excluding generic first words like AMERICAN, NATIONAL, GENERAL, UNITED, FIRST, ADVANCED, GLOBAL).
   - Cap wire-proxy output at max 8 deduplicated items/run (prioritized by extracted dollar amount) to respect the 25-item primary_feeds.py ceiling.

5. **Step 4 (api.usaspending.gov/api/v2/) — Pre-Flight & Batched Query:**
   - **Pre-Flight Check First:** Run test POST against https://api.usaspending.gov/api/v2/search/spending_by_award/ from terminal.
   - **API Schema Safeguard:** spending_by_award requires sort: Award Amount, order: desc, and Award Amount MUST be in fields array. Run 1 batched POST for prime contracts (subawards: false, award_type_codes: ["A","B","C","D"]) and 1 separate batched POST for subawards (subawards: true, award_type_codes: ["A","B","C","D"]) using NAICS codes 334413, 336414, 334511, 325180, 333242.
   - Match recipient names locally using Step 3 normalizer against universe_cache.db and sec_tickers_cache.json.
   - Hydrate GET /api/v2/awards/{generated_internal_id}/ only for matched prime awards >= $1M (max 5 hydrations/run) and compute UCR = base_and_all_options_value / total_obligation (safely handling None or zero division).

6. **Step 5 (FMP Anomaly Feeder):**
   - Inspect fmp_client.py first for exposed methods. Keep total FMP calls <= 15 per run, targeting open-market insider buys (P-Purchase >= $100k) and earnings/volume breakouts in universe_cache.db tickers.

---

### WAVE C: Stdlib SEC Slicer & Deep-Dive Tribunal (Steps 7 and 8)

7. **Step 7 (sec_slicer.py — Pure Stdlib html.parser + re):**
   - Regex-strip <TYPE>(GRAPHIC|ZIP|EXCEL|XML|EX-101).*?</DOCUMENT> and <ix:header>.*?</ix:header> (re.DOTALL | re.IGNORECASE) before feeding into HTMLParser.
   - Convert <table> rows into pipe-delimited Markdown, skip TOC matches via span length (e_pos - s_pos > 1500 chars), and extract Item 1, Item 1A, Item 7, and Item 8 Notes matching (segment|major customer|concentration|revenue|inventory|commitment) (hard-capped at 30,000 chars total).
   - Verify standalone on AVAV and ALAB before wiring into Deep-Dive.

8. **Step 8 (Adversarial Red Team & Options Delta Queue in Deep-Dive):**
   - Keep token budgets tight: run full sliced-10K Adversarial Red-Team (thesis_kill_probability, customer concentration >30%, dilution >8%, WIP inventory stall) and Options Delta Structurer (options_delta_signals table in SQLite WAL) ONLY on candidates with best_track_score >= 65 and require mcap >= 500000000 ($500M) for liquid options triggers.

---

### VERIFICATION & COMPLETION PROTOCOL
After each step, run a dry-run test from terminal, verify zero runtime errors, and append summary to ~/.hermes/cron/WAVE_PROGRESS.md. Loop until all 8 steps show STATUS: VERIFIED.
