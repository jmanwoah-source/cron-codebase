# SPEC: UNHARNESSED MULTI-AGENT BRAID & OVERNIGHT RECALIBRATION (V3 - FINAL)
**State Baseline (ALREADY COMPLETED - DO NOT RE-RUN):**
- [x] `AGENTS.md` ($500M–$300B), `13f-radar` (Nov 16 deadline), `fmp_client.atomic_write_json` (18 files).
- [x] SQLite DDL live in `~/.hermes/cron/universe_cache.db` (`alpha_vectors` 29 cols, `regime_state` 11 cols, WAL + 5s timeout).
- [x] Canonical wiki dossier layout (`wiki/companies/<T>/<T>.md`).
- [x] Rule: NO `import pandas_ta` (use native `pandas`/`numpy` or FMP `technicalIndicators`).

---

## ARCHITECTURE: OUTSIDE THE HERMES HARNESS
To bypass the 10-min cron timeout and 1-MCP-call-per-turn cap, the overnight wave runs as a standalone Python + vLLM Multi-Agent Swarm in `tmux`, writing directly to `~/.hermes/cron/universe_cache.db` and the canonical `.json` bus files via `fmp_client.atomic_write_json`.

### AGENT TEAM TOPOLOGY (4 SPECIALIZED WORKERS)
1. **Agent 1: `Harvester-Prime` (Pure Python / Async I/O — Zero LLM Tokens)**
   - **Role:** Executes Stage 1 (Open-Loop Faucets) & Stage 2 (29-Column Vector Enrichment).
   - **Inputs:** FMP Endpoints 1–15 via `fmp_client.py` (with `/stable/` REST fallback reading `~/.hermes/cron/fmp_key.local`).
   - **Output:** Upserts into `universe_cache.db` (`alpha_vectors` & `regime_state`) in **100-row chunked transactions** (`INSERT OR REPLACE`). Appends primary wire/insider/transcript items to `feed_raw.json`.

2. **Agent 2: `Quant-Gatekeeper` (Pure Python Deterministic Scorer)**
   - **Role:** Reads `alpha_vectors` + `signal_briefs.json` and computes the deterministic 0–100 sub-scores BEFORE any LLM runs.
   - **Math Contract:**
     - **Contract-to-Consensus [EP-4]:** If contract value `/ rev_est_next_yr >= 0.05` OR `analyst_count <= 2`, force `materiality="high"`, `confidence="high"` (strings), awarding `15/15` materiality + `15/15` confidence in `vetted_tickers.json`.
     - **Cross-Source Cap:** Cap `Exa/SearXNG` web news at `15/30` unless corroborated by `FMP_PRIMARY_WIRE`, `8-K`, `13F`, `FMP_INSIDER_SENATE`, or `FMP_TRANSCRIPT_BREADCRUMB`.
     - **4 Pillars (`0–100` each for `ticker_scores.json`):**
       - `financial`: `min(72, piotroski_f * 8) + (15 if altman_z > 3.0 else 0) + (15 if trojan_segment_flag == 1 else 0)`
       - `valuation`: Linear map of `blended_upside_pct = 0.5*((dcf_levered - price)/price) + 0.5*((pt_consensus - price)/price)` (`<=0% -> 20`, `+15% -> 60`, `>=+35% -> 95`).
       - `technical`: `(25 if mfi_14 > 55 else 0) + (25 if adx_14 > 20 and macd_hist_slope > 0 else 0) + (25 if rs_20d_vs_pillar > 0 else 0) + (25 if float_shares < 50e6 else 0)`.
       - `catalyst`: Rolling 14d brief score + `eps_revision_delta_30d > 0` (+20) + 13F/Insider/Senate cluster (+25).

3. **Agent 3: `Thesis-Forge-Alpha` (vLLM Worker — Bull / Track A & B Classifier)**
   - **Role:** Consumes pre-computed row from `alpha_vectors` + top 5 catalyst briefs (~350 input tokens total).
   - **Action:** Assigns `Track A` (Fundamental Fortress), `Track B` (Catalyst Edge), or `Track C` (Mirage/Short) and writes the concise markdown catalyst line `[date] [8-char-brief_id] headline | category (direction level)` to `wiki/companies/<T>/<T>.md`.

4. **Agent 4: `Red-Team-Inquisitor` (vLLM Worker — Adversarial Kill-Switch)**
   - **Role:** Audits every ticker scoring `>= 75` using `alpha_vectors` + `regime_state`.
   - **Deterministic Kill Overrides:**
     - If `altman_z < 1.5` AND `piotroski_f <= 3` -> force `Track C`, `thesis_kill_probability >= 0.75` (`"Balance sheet distress / dilution risk"`).
     - If `regime_state.cot_extreme_crowding_flag == 1` AND `mfi_14 > 80` -> add `+0.20` to `thesis_kill_probability` (`"COT extreme crowding + exhaustion"`).
   - **Output:** Writes final `ticker_scores.json` via `fmp_client.atomic_write_json` and triggers `tier_lists.py` to render root `.md` tier lists from `tier_list_state.json`.

---

## IMPLEMENTATION DELIVERABLES (2 STANDALONE SCRIPTS)
Create in `~/.hermes/cron/`:
1. `unharnessed_fmp_ingestor.py` (Implements Agent 1 + Agent 2; resumable via `ORDER BY last_updated ASC NULLS FIRST LIMIT 100`).
2. `unharnessed_swarm_recalibrator.py` (Implements Agent 3 + Agent 4 via `asyncio` against local vLLM endpoint, updating `ticker_scores.json`, `tier_list_state.json`, and `wiki/companies/<T>/<T>.md`).
