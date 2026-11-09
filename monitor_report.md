# Monitor Report - Signal Pipeline Orchestration

## Query Tuner — 2026-10-16
- Healthy: 0 | Watch: 16 | Changed: 0
- Changes: none (0-result streaks and low-freshness streaks are caused by the engine's results_fresh field being 0 in all 80 ledger rows and by SearXNG time_range=day/week returning 0 while default range returns results — environmental, not query quality; anti-churn rule holds)
- Weakest query: site:patents.google.com silicon carbide power semiconductor (score 10) — reason: freshness=0% (engine field broken), novelty median 0, 4/5 windows returned 0 (time_range quirk)
- Strongest query: GaN oxide semiconductor Ga2O3 foundry supply (score 60) — reason: novelty median 10, 0 zero-result windows
## Query Tuner —
## Query Tuner — 2026-11-02
- Healthy: 0 | Watch: 5 | Changed: 11
- Changes: - 3b: A/B — original wins (9 pts) — unchanged
- 3c: A/B — original wins (10 pts) — unchanged
- 4a: 'DOE grant "small modular reactor"' -> '"SMR" nuclear DOE grant recipient announced'
- 4c: A/B — original wins (10 pts) — unchanged
- 5a: 'quantum error correction "logical qubit"' -> '"logical qubit" "below threshold" quantum processor'
- 5b: 'quantum sensor defense "award" OR "contract"' -> '"quantum sensing" defense contract win announcement'
- 6a: 'counter-UAS drone detection "awarded" OR "wins con' -> '"counter-UAS" system contract win announcement'
- 6b: A/B — original wins (10 pts) — unchanged
- Weakest query: site:patents.google.com silicon carbide power semiconductor (score 10) — REWRITE
- Strongest query: advanced packaging chiplet supplier "sole source" (score 60)
