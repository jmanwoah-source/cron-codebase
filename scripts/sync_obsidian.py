#!/usr/bin/env python3
"""
Obsidian Vault Sync — Pushes Hermes research outputs into the local Obsidian vault.
Runs after each pipeline cycle via cron job.

Architecture:
  - Reads vetted_tickers.json, ticker_scores.json, after_hours_research.md
  - Generates Obsidian-compatible markdown (YAML frontmatter + body)
  - Writes to vault directory at ~/.hermes/sessions/obsidian_vault/
  - Tracks which files are written to avoid unnecessary updates (via obsidian_sync_state.json)

Usage:
  python3 sync_obsidian.py [--dry-run] [--force] [--tickers-only]
"""

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# ── Paths ──────────────────────────────────────────────────────────────────

BASE_DIR = Path.home() / ".hermes"
VAULT_DIR = BASE_DIR / "sessions" / "obsidian_vault"
STATE_FILE = BASE_DIR / "cron" / "obsidian_sync_state.json"
SEC_SLICES_DIR = BASE_DIR / "cron" / "sec_slices"

# Folder structure (matches existing vault layout)
FOLDERS = {
    "inbox": "00-Inbox",
    "sectors": "10-Sectors",
    "tickers": "20-Tickers",
    "scans": "30-Scans",
    "reference": "90-Reference",
}

# ── Helpers ────────────────────────────────────────────────────────────────

def _now() -> str:
    """ISO timestamp string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _sha256(text: str) -> str:
    """SHA256 hex digest of a string."""
    return hashlib.sha256(text.encode()).hexdigest()


def _safe_filename(ticker: str, suffix: str = "") -> str:
    """Ensure ticker name doesn't break Obsidian."""
    return f"{ticker.upper()}{suffix}.md"


def _load_json(path: Path) -> Optional[Any]:
    """Load JSON safely, return None if missing/corrupt."""
    if not path.exists():
        return None
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _load_manifest() -> Dict[str, str]:
    """Load sync manifest: {file_path: sha256_hash}."""
    if not STATE_FILE.exists():
        return {}
    data = _load_json(STATE_FILE)
    return data or {}


def _save_manifest(manifest: Dict[str, str]):
    """Persist sync manifest."""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(manifest, f, indent=2)


def _yaml_bool(val: str) -> str:
    """Convert string to YAML boolean."""
    v = val.lower()
    if v in ("true", "yes", "high", "direct", "positive", "full"):
        return "true"
    if v in ("false", "no", "low", "indirect", "negative", "partial"):
        return "false"
    return val  # leave as-is for things like "medium"


def _yaml_scalar(val: Any) -> str:
    """Format a YAML scalar value."""
    if val is None:
        return "null"
    if isinstance(val, bool):
        return "true" if val else "false"
    if isinstance(val, (int, float)):
        return str(val)
    return str(val)


def _build_frontmatter(metadata: Dict[str, Any]) -> str:
    """Generate YAML frontmatter block."""
    lines = ["---"]
    for k, v in metadata.items():
        formatted = _yaml_scalar(v)
        # Escape quotes and wrap in quotes if contains special chars
        if '"' in formatted or ":" in formatted or "\n" in formatted:
            formatted = formatted.replace('"', '""')
            lines.append(f'  {k}: "{formatted}"')
        else:
            lines.append(f"  {k}: {formatted}")
    lines.append("---")
    return "\n".join(lines) + "\n\n"


# ── Ticker note generator ─────────────────────────────────────────────────

def _generate_ticker_note(ticker: str, brief: Dict, score_entry: Optional[Dict]) -> str:
    """
    Generate an Obsidian note for a single ticker.

    brief comes from vetted_tickers.json tier entries:
      {ticker, conviction_score, brief_count, impact_direction, materiality,
       confidence, cross_source_count, brief_categories, top_headlines}

    score_entry comes from ticker_scores.json:
      {ticker, name, sector, industry, market_cap, price, pillar_scores: {
         financial_100, catalyst_100, valuation_100, technical_100}}
    """
    now = _now()

    # ── Frontmatter ──────────────────────────────────────────────────────

    meta = {
        "ticker": ticker.upper(),
        "conviction_score": str(brief.get("conviction_score", "")),
        "impact_direction": str(brief.get("impact_direction", "")),
        "materiality": str(brief.get("materiality", "")),
        "confidence": str(brief.get("confidence", "")),
        "cross_source_count": str(brief.get("cross_source_count", "")),
        "brief_categories": brief.get("brief_categories", []),
        "brief_count": str(brief.get("brief_count", "")),
        "synced_at": now,
    }

    # Add score data to frontmatter if available
    if score_entry:
        pillar = score_entry.get("pillar_scores", {})
        meta["financial_score"] = str(pillar.get("financial_100", ""))
        meta["catalyst_score"] = str(pillar.get("catalyst_100", ""))
        meta["valuation_score"] = str(pillar.get("valuation_100", ""))
        meta["technical_score"] = str(pillar.get("technical_100", ""))
        meta["sector"] = str(score_entry.get("sector", ""))
        meta["industry"] = str(score_entry.get("industry", ""))
        meta["name"] = str(score_entry.get("name", ""))
        mc = score_entry.get("market_cap")
        meta["market_cap"] = f"${float(mc) / 1e9:.2f}B" if mc and float(mc) > 0 else str(mc)
        price = score_entry.get("price")
        meta["price"] = str(price) if price else ""

    # ── Body ──────────────────────────────────────────────────────────────

    body_lines = []

    # Name line
    name = score_entry.get("name", "") if score_entry else ""
    body_lines.append(f"# {ticker.upper()} — {name or 'Unknown'}")
    body_lines.append("")

    # Quick stats
    body_lines.append("## Overview")
    body_lines.append("")

    if score_entry:
        mc = score_entry.get("market_cap")
        if mc and float(mc) > 0:
            body_lines.append(f"- **Market Cap:** ${float(mc) / 1e9:.2f}B")
        if score_entry.get("price"):
            body_lines.append(f"- **Price:** ${score_entry['price']}")
        if score_entry.get("sector"):
            body_lines.append(f"- **Sector:** {score_entry['sector']}")
        if score_entry.get("industry"):
            body_lines.append(f"- **Industry:** {score_entry['industry']}")
        data_cov = score_entry.get("data_coverage")
        if data_cov:
            body_lines.append(f"- **Data Coverage:** {data_cov:.0%}")

    body_lines.append(f"- **Conviction:** {brief.get('conviction_score', '—')}")
    body_lines.append(f"- **Brief Count:** {brief.get('brief_count', 0)}")
    body_lines.append(f"- **Impact:** {brief.get('impact_direction', '—')}")
    body_lines.append(f"- **Materiality:** {brief.get('materiality', '—')}")
    body_lines.append(f"- **Source Confidence:** {brief.get('confidence', '—')}")
    body_lines.append(f"- **Cross-Source Count:** {brief.get('cross_source_count', 0)}")
    body_lines.append("")

    # Scores
    if score_entry:
        pillar = score_entry.get("pillar_scores", {})
        body_lines.append("### Score Breakdown")
        body_lines.append("")
        body_lines.append("|||")
        body_lines.append("| Component | Score |")
        body_lines.append("|---|---|")
        body_lines.append(f"| Financial | {pillar.get('financial_100', '—')} |")
        body_lines.append(f"| Catalyst | {pillar.get('catalyst_100', '—')} |")
        body_lines.append(f"| Valuation | {pillar.get('valuation_100', '—')} |")
        body_lines.append(f"| Technical | {pillar.get('technical_100', '—')} |")
        body_lines.append("|||")
        body_lines.append("")

    # Brief categories (tags)
    cats = brief.get("brief_categories", [])
    if cats:
        body_lines.append("## Brief Categories")
        body_lines.append("")
        body_lines.append(", ".join(f"`{c}`" for c in cats))
        body_lines.append("")

    # Headlines
    headlines = brief.get("top_headlines", [])
    if headlines:
        body_lines.append("## Headlines")
        body_lines.append("")
        for i, hl in enumerate(headlines[:5], 1):
            body_lines.append(f"{i}. {hl}")
        if len(headlines) > 5:
            body_lines.append(f"... and {len(headlines) - 5} more")
        body_lines.append("")

    # Wiki link
    body_lines.append(f"See also: [[{ticker.upper()}-sec-slices]]")
    body_lines.append("")

    return _build_frontmatter(meta) + "\n".join(body_lines)


# ── SEC slices merger ─────────────────────────────────────────────────────

def _merge_sec_slices(ticker: str) -> Optional[str]:
    """Merge all SEC slice files for a ticker into one note."""
    meta_file = SEC_SLICES_DIR / f"{ticker}.summary.json"
    meta = _load_json(meta_file)
    if not meta:
        return None

    now = _now()
    meta_map = {
        "ticker": ticker.upper(),
        "filing_date": str(meta.get("filing_date", "")),
        "accession": str(meta.get("accession", "")),
        "doc": str(meta.get("doc", "")),
        "synced_at": now,
    }

    md_file = SEC_SLICES_DIR / f"{ticker}.md"
    if not md_file.exists():
        return None

    with open(md_file, "r") as f:
        content = f.read()

    body_lines = []
    body_lines.append("## SEC 10-K Filing Slices")
    body_lines.append("")
    body_lines.append("Source: SEC EDGAR — raw filing slices extracted by the pipeline.")
    body_lines.append("")
    # Truncate massive content to avoid bloating the vault
    max_len = 50000
    if len(content) > max_len:
        head = content[:max_len - 5000]
        tail = content[-5000:]
        body_lines.append(head)
        body_lines.append("\n<!-- ... content truncated to ~50k chars ... -->\n\n")
        body_lines.append(tail)
    else:
        body_lines.append(content)
    body_lines.append("")

    return _build_frontmatter(meta_map) + "\n".join(body_lines)


# ── Main sync logic ───────────────────────────────────────────────────────

def _read_after_hours_research() -> Optional[str]:
    """Read the after-hours research markdown."""
    path = BASE_DIR / "cron" / "after_hours_research.md"
    if path.exists():
        return path.read_text()
    return None


def _write_file_tracking(
    filepath: Path,
    content: str,
    manifest: Dict,
    actions: Dict,
    dry_run: bool,
    force: bool,
) -> None:
    """Write a file and track it in the manifest."""
    sha = _sha256("") if filepath.exists() else ""
    # Quick re-read for sha
    try:
        sha = _sha256(filepath.read_text())
    except OSError:
        sha = ""
    content_sha = _sha256(content)

    if sha != content_sha or force:
        if dry_run:
            print(f"  [DRY-RUN] {'CREATE' if not sha else 'UPDATE'}: {filepath}")
        else:
            filepath.parent.mkdir(parents=True, exist_ok=True)
            filepath.write_text(content)
        manifest[str(filepath)] = content_sha
        if sha:
            actions["updated"].append(str(filepath))
        else:
            actions["created"].append(str(filepath))
    else:
        actions["unchanged"].append(str(filepath))


def sync_tickers(
    vetted: Dict,
    scores: Dict,
    manifest: Dict,
    force: bool = False,
    dry_run: bool = False,
    tickers_only: bool = False,
) -> Dict[str, List[str]]:
    """
    Sync ticker notes from vetted_tickers.json.

    Data shapes:
      vetted["high_tickers"] = [{ticker, conviction_score, ...}, ...]
      scores["tickers"] = {ticker: {pillar_scores, ...}, ...}
    """
    actions: Dict[str, List[str]] = {"created": [], "updated": [], "unchanged": []}

    # Score lookup
    scored_map = scores.get("tickers", {}) if scores else {}

    for tier_name in ("high_tickers", "moderate_tickers", "low_tickers"):
        items = vetted.get(tier_name, [])
        if not items:
            continue

        for item in items:
            ticker = item.get("ticker", "")
            if not ticker:
                continue

            brief = item  # the item IS the brief
            score_entry = scored_map.get(ticker, {})

            note_content = _generate_ticker_note(ticker, brief, score_entry)
            filename = _safe_filename(ticker)
            filepath = VAULT_DIR / FOLDERS["tickers"] / filename

            _write_file_tracking(filepath, note_content, manifest, actions, dry_run, force)

    return actions


def sync_sec_slices(
    manifest: Dict,
    force: bool = False,
    dry_run: bool = False,
) -> Dict[str, List[str]]:
    """Sync SEC slice files per ticker."""
    actions: Dict[str, List[str]] = {"created": [], "updated": [], "unchanged": []}

    for md_file in sorted(SEC_SLICES_DIR.glob("*.md")):
        if md_file.name.endswith(".summary.json"):
            continue
        ticker = md_file.stem
        content = _merge_sec_slices(ticker)
        if not content:
            continue

        filename = _safe_filename(ticker, "-sec-slices")
        filepath = VAULT_DIR / FOLDERS["tickers"] / filename

        sha = ""
        try:
            sha = _sha256(filepath.read_text())
        except OSError:
            pass
        content_sha = _sha256(content)

        if sha != content_sha or force:
            if dry_run:
                print(f"  [DRY-RUN] {'CREATE' if not sha else 'UPDATE'}: {filepath}")
            else:
                filepath.parent.mkdir(parents=True, exist_ok=True)
                filepath.write_text(content)
            manifest[str(filepath)] = content_sha
            if sha:
                actions["updated"].append(str(filepath))
            else:
                actions["created"].append(str(filepath))

    return actions


def sync_inbox(
    manifest: Dict,
    force: bool = False,
    dry_run: bool = False,
) -> Dict[str, List[str]]:
    """Sync after-hours research to Inbox."""
    actions: Dict[str, List[str]] = {"created": [], "updated": [], "unchanged": []}
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    ah_content = _read_after_hours_research()
    if ah_content:
        filename = f"ah_research_{now}.md"
        filepath = VAULT_DIR / FOLDERS["inbox"] / filename

        sha = ""
        try:
            sha = _sha256(filepath.read_text())
        except OSError:
            pass
        content_sha = _sha256(ah_content)

        if sha != content_sha or force:
            if dry_run:
                print(f"  [DRY-RUN] CREATE: {filepath}")
            else:
                filepath.parent.mkdir(parents=True, exist_ok=True)
                filepath.write_text(ah_content)
            manifest[str(filepath)] = content_sha
            actions["created"].append(str(filepath))
        else:
            actions["unchanged"].append(str(filepath))

    return actions


def main():
    parser = argparse.ArgumentParser(description="Sync Hermes research → Obsidian vault")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be written")
    parser.add_argument("--force", action="store_true", help="Overwrite all files")
    parser.add_argument("--tickers-only", action="store_true", help="Only sync ticker notes")
    parser.add_argument("--verbose", "-v", action="store_true", help="Print file details")
    args = parser.parse_args()

    print(f"=== Obsidian Vault Sync — {_now()} ===")
    print(f"Vault: {VAULT_DIR}")

    # Load sources
    vetted = _load_json(BASE_DIR / "cron" / "vetted_tickers.json")
    scores = _load_json(BASE_DIR / "cron" / "ticker_scores.json")

    if not vetted:
        print("ERROR: vetted_tickers.json not found. Is the pipeline running?")
        return

    manifest = _load_manifest()
    all_actions: Dict[str, List[str]] = {"created": [], "updated": [], "unchanged": []}

    # Sync ticker notes
    print("\n--- Ticker Notes ---")
    actions = sync_tickers(vetted, scores, manifest, args.force, args.dry_run, args.tickers_only)
    for k, v in actions.items():
        all_actions[k].extend(v)
        if v:
            print(f"  {k}: {len(v)} files")
            if args.verbose:
                for f in v:
                    print(f"    - {f}")

    if not args.tickers_only:
        # Sync SEC slices
        print("\n--- SEC Slices ---")
        actions = sync_sec_slices(manifest, args.force, args.dry_run)
        for k, v in actions.items():
            all_actions[k].extend(v)
            if v:
                print(f"  {k}: {len(v)} files")

        # Sync Inbox
        print("\n--- Inbox ---")
        actions = sync_inbox(manifest, args.force, args.dry_run)
        for k, v in actions.items():
            all_actions[k].extend(v)
            if v:
                print(f"  {k}: {len(v)} files")

    # Save manifest
    if not args.dry_run:
        _save_manifest(manifest)

    # Summary
    total_created = len(all_actions["created"])
    total_updated = len(all_actions["updated"])
    total_unchanged = len(all_actions["unchanged"])
    print(f"\n=== Summary ===")
    print(f"  Created: {total_created}")
    print(f"  Updated: {total_updated}")
    print(f"  Unchanged: {total_unchanged}")
    if not args.dry_run:
        print(f"  Manifest saved: {STATE_FILE}")
    else:
        print("  (dry run — no files written)")


if __name__ == "__main__":
    main()
