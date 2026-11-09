#!/usr/bin/env python3
"""STEP C smoke test for the three sieve/hysteresis helpers in
unharnessed_swarm_recalibrator.py. Self-diagnosing: on failure it prints
the offending values instead of a bare AssertionError."""
import importlib.util
import sys

spec = importlib.util.spec_from_file_location("rc", "/home/jaren/.hermes/cron/unharnessed_swarm_recalibrator.py")
rc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rc)

FAILS = []


def check(label, got, want):
    if got != want:
        FAILS.append(f"{label}: got={got!r} want={want!r}")
    else:
        print(f"  PASS {label}")


ROW = {"ticker": "XYZ", "quant_composite_score": 78.4, "blended_upside_pct": 12.6,
       "piotroski_f": 5.7, "latest_transcript_date": "2026-10-30",
       "latest_13f_date": "2026-11-01"}

h1 = rc.compute_state_hash(ROW, "R1")
print(f"h1={h1} (len={len(h1)})")

check("hash: 16 chars", len(h1), 16)
check("hash: stable on identical input", rc.compute_state_hash(dict(ROW), "R1"), h1)
check("hash: sub-quantum piotroski drift (5.7->6.3 both round 6.0) keeps hash",
      rc.compute_state_hash({**ROW, "piotroski_f": 6.3}, "R1"), h1)
h2 = rc.compute_state_hash({**ROW, "piotroski_f": 8.4}, "R1")
check("hash: real piotroski shift (5.7->8.4) changes hash", h1 == h2, False)
h3 = rc.compute_state_hash({**ROW, "blended_upside_pct": 14.1}, "R1")
check("hash: upside 12.6->14.1 crosses 2.0pt bucket", h1 == h3, False)
check("hash: regime id change changes hash", rc.compute_state_hash(ROW, "R2") == h1, False)

NEU = {"tier": "NEUTRAL", "quant_composite_score": 70.0, "state_hash": h1}
check("sieve: >=80 gate", rc.should_dispatch_to_vllm(81.0, h1, NEU), True)
check("sieve: <=15 gate", rc.should_dispatch_to_vllm(14.0, h1, NEU), True)
check("sieve: hash shift + dScore 4.5",
      rc.should_dispatch_to_vllm(74.5, h2, NEU), True)
check("sieve: hash shift + dScore 3.0 -> blocked",
      rc.should_dispatch_to_vllm(73.0, h2, NEU), False)
check("sieve: no hash shift -> blocked",
      rc.should_dispatch_to_vllm(73.0, h1, NEU), False)
GOLD = {"tier": "GOLDEN", "state_hash": h1}
check("sieve: GOLDEN tier, hash stable -> no LLM", rc.should_dispatch_to_vllm(50.0, h1, GOLD), False)
check("sieve: GOLDEN tier, hash shift -> LLM", rc.should_dispatch_to_vllm(50.0, h2, GOLD), True)
check("sieve: cold start (no prev)", rc.should_dispatch_to_vllm(50.0, h1, None), True)
check("sieve: force_recalibrate", rc.should_dispatch_to_vllm(50.0, h1, NEU, True), True)

check("hyst: 79.0 from GOLDEN holds GOLDEN", rc.apply_hysteresis(79.0, "GOLDEN"), "GOLDEN")
check("hyst: 74.0 from GOLDEN demotes to EMERGING_EDGE (62-80 band)", rc.apply_hysteresis(74.0, "GOLDEN"), "EMERGING_EDGE")
check("hyst: 79.9 from NEUTRAL -> EMERGING_EDGE (GOLDEN needs >=80.0)", rc.apply_hysteresis(79.9, "NEUTRAL"), "EMERGING_EDGE")
check("hyst: 65.0 holds EMERGING_EDGE", rc.apply_hysteresis(65.0, "EMERGING_EDGE"), "EMERGING_EDGE")
check("hyst: 65.0 from NEUTRAL stays NEUTRAL", rc.apply_hysteresis(65.0, "NEUTRAL"), "NEUTRAL")
check("hyst: 66.0 from NEUTRAL stays NEUTRAL", rc.apply_hysteresis(66.0, "NEUTRAL"), "NEUTRAL")
check("hyst: 67.0 from NEUTRAL enters EMERGING_EDGE", rc.apply_hysteresis(67.0, "NEUTRAL"), "EMERGING_EDGE")
check("hyst: 24.0 holds MIRAGE_SHORT", rc.apply_hysteresis(24.0, "MIRAGE_SHORT"), "MIRAGE_SHORT")
check("hyst: 26.0 from MIRAGE_SHORT -> NEUTRAL", rc.apply_hysteresis(26.0, "MIRAGE_SHORT"), "NEUTRAL")
check("hyst: 19.0 from NEUTRAL -> MIRAGE_SHORT", rc.apply_hysteresis(19.0, "NEUTRAL"), "MIRAGE_SHORT")
check("hyst: short_candidate override", rc.apply_hysteresis(55.0, "NEUTRAL", True), "MIRAGE_SHORT")

print()
if FAILS:
    print(f"{len(FAILS)} FAILURES:")
    for f in FAILS:
        print("  " + f)
    sys.exit(1)
print(f"ALL TESTS PASS ({1 + 5 + 9 + 10} assertions)")
