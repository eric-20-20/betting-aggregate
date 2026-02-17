#!/usr/bin/env python3
"""
Repair truncated scores in Dimers backfill data.

Reads the backfill JSONL, visits each game URL to re-fetch scores with the
improved regex, and writes a repaired output file.

Usage:
    python scripts/repair_dimers_scores.py [--debug]
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))

try:
    from playwright.sync_api import sync_playwright, Page
except ImportError:
    sync_playwright = None

# Paths
INPUT_FILE = REPO_ROOT / "out" / "backfill_dimers_bets.jsonl"
OUTPUT_FILE = REPO_ROOT / "out" / "backfill_dimers_bets_repaired.jsonl"
STORAGE_STATE = REPO_ROOT / "dimers_storage_state.json"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def is_valid_score(score: Optional[str]) -> bool:
    """Check if a score looks valid (two 2-3 digit numbers)."""
    if not score:
        return False
    parts = score.split("-")
    if len(parts) != 2:
        return False
    try:
        s1, s2 = int(parts[0]), int(parts[1])
        # NBA scores are typically between 70 and 170
        return 70 <= s1 <= 170 and 70 <= s2 <= 170
    except ValueError:
        return False


def fetch_score_for_url(page: Page, game_url: str, debug: bool = False) -> Optional[str]:
    """Fetch the final score from a game URL using improved regex."""
    try:
        page.goto(game_url, wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(1000)

        score = page.evaluate("""
            () => {
                const text = document.body.innerText;

                // Pattern 1: "FINAL" on same line with scores
                let scoreMatch = text.match(/FINAL\\s+(\\d{2,3})\\s*[-:]\\s*(\\d{2,3})/i);

                // Pattern 2: Scores might be on next line after FINAL
                if (!scoreMatch) {
                    scoreMatch = text.match(/FINAL\\s*\\n\\s*(\\d{2,3})\\s*[-:]\\s*(\\d{2,3})/i);
                }

                // Pattern 3: Look for two 2-3 digit numbers separated by dash near "FINAL"
                if (!scoreMatch) {
                    const finalIdx = text.toUpperCase().indexOf('FINAL');
                    if (finalIdx !== -1) {
                        // Search within 100 chars after FINAL
                        const searchArea = text.substring(finalIdx, finalIdx + 100);
                        scoreMatch = searchArea.match(/(\\d{2,3})\\s*[-:]\\s*(\\d{2,3})/);
                    }
                }

                return scoreMatch ? `${scoreMatch[1]}-${scoreMatch[2]}` : null;
            }
        """)

        if debug and score:
            print(f"    Fetched score: {score}")

        return score

    except Exception as e:
        if debug:
            print(f"    Error fetching {game_url}: {e}")
        return None


def repair_scores(debug: bool = False) -> None:
    """Main repair function."""
    if sync_playwright is None:
        raise RuntimeError("playwright not installed")

    if not INPUT_FILE.exists():
        print(f"Input file not found: {INPUT_FILE}")
        return

    # Load all records
    records = []
    with open(INPUT_FILE) as f:
        for line in f:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    print(f"Loaded {len(records)} records from {INPUT_FILE}")

    # Find records with bad scores
    bad_score_urls = {}
    for rec in records:
        score = rec.get("final_score")
        if not is_valid_score(score):
            url = rec.get("game_url")
            if url:
                bad_score_urls[url] = score

    print(f"Found {len(bad_score_urls)} unique URLs with invalid scores")

    if not bad_score_urls:
        print("No scores to repair!")
        return

    # Fetch corrected scores
    storage_path = str(STORAGE_STATE) if STORAGE_STATE.exists() else None
    if storage_path:
        print(f"Using storage state from {storage_path}")

    url_to_score = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=USER_AGENT,
            storage_state=storage_path
        )
        page = context.new_page()

        for i, (url, old_score) in enumerate(bad_score_urls.items()):
            print(f"[{i+1}/{len(bad_score_urls)}] Fetching score for {url}")
            if debug:
                print(f"  Old score: {old_score}")

            new_score = fetch_score_for_url(page, url, debug=debug)
            url_to_score[url] = new_score

            if new_score and is_valid_score(new_score):
                print(f"  ✓ Fixed: {old_score} -> {new_score}")
            else:
                print(f"  ✗ Still bad or missing: {new_score}")

        browser.close()

    # Update records and write output
    fixed_count = 0
    with open(OUTPUT_FILE, "w") as f:
        for rec in records:
            url = rec.get("game_url")
            if url in url_to_score and url_to_score[url]:
                old_score = rec.get("final_score")
                new_score = url_to_score[url]
                if is_valid_score(new_score):
                    rec["final_score"] = new_score
                    rec["score_repaired_from"] = old_score
                    fixed_count += 1
            f.write(json.dumps(rec) + "\n")

    print(f"\n=== Summary ===")
    print(f"Total records: {len(records)}")
    print(f"URLs checked: {len(bad_score_urls)}")
    print(f"Scores fixed: {fixed_count}")
    print(f"Output written to: {OUTPUT_FILE}")


def main():
    parser = argparse.ArgumentParser(description="Repair truncated Dimers scores")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    args = parser.parse_args()

    repair_scores(debug=args.debug)


if __name__ == "__main__":
    main()
