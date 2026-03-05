#!/usr/bin/env python3
"""
Dimers.com NBA picks ingestion script.

Fetches best bets and player props from Dimers using Playwright.

Usage:
    python3 dimers_ingest.py [--debug] [--sport NBA] [--bets-only] [--props-only]

Output:
    - out/raw_dimers_nba.json
    - out/normalized_dimers_nba.json
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    sync_playwright = None

from src.dimers_extractors import (
    RawDimersRecord,
    extract_best_bets,
    extract_best_props,
    click_load_more_until_done,
    scroll_to_load_all,
)
from src.normalizer_dimers_nba import normalize_dimers_records
from store import NBA_SPORT, NCAAB_SPORT, write_json

OUT_DIR = Path(os.getenv("NBA_OUT_DIR", "out"))

# Default storage state file for Dimers Pro authentication
DEFAULT_STORAGE_STATE = "dimers_storage_state.json"

DIMERS_URLS = {
    NBA_SPORT: {
        "best_bets": "https://www.dimers.com/best-bets/nba",
        "best_props": "https://www.dimers.com/best-props/nba",
    },
    NCAAB_SPORT: {
        "best_bets": "https://www.dimers.com/best-bets/cbb",
        "best_props": "https://www.dimers.com/best-props/cbb",
    },
}

# Browser user agent
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def ensure_out_dir() -> None:
    """Create output directory if it doesn't exist."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def dedupe_records(records: List[RawDimersRecord]) -> List[RawDimersRecord]:
    """Remove duplicate records based on fingerprint."""
    seen = set()
    deduped = []
    for record in records:
        if record.raw_fingerprint not in seen:
            seen.add(record.raw_fingerprint)
            deduped.append(record)
    return deduped


def ingest_dimers(
    sport: str = NBA_SPORT,
    debug: bool = False,
    bets_only: bool = False,
    props_only: bool = False,
    storage_state: Optional[str] = None,
) -> None:
    """
    Main ingestion function for Dimers.

    Args:
        sport: Sport to ingest (currently only NBA supported)
        debug: Enable debug output
        bets_only: Only fetch best bets page
        props_only: Only fetch best props page
        storage_state: Path to storage state JSON for Dimers Pro authentication
    """
    if sync_playwright is None:
        raise RuntimeError(
            "playwright is not installed. "
            "Install with `pip install playwright` then `playwright install chromium`."
        )

    urls = DIMERS_URLS.get(sport, DIMERS_URLS[NBA_SPORT])
    observed_at = datetime.now(timezone.utc)
    all_raw: List[RawDimersRecord] = []

    print(f"[dimers] Starting ingestion for {sport}")
    print(f"[dimers] URLs: bets={urls['best_bets']}, props={urls['best_props']}")

    # Resolve storage state path
    storage_path = storage_state or DEFAULT_STORAGE_STATE
    use_storage = Path(storage_path).exists()

    if use_storage:
        print(f"[dimers] Using storage state from {storage_path}")
    else:
        print(f"[dimers] No storage state found at {storage_path}, running unauthenticated")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=USER_AGENT,
            storage_state=storage_path if use_storage else None
        )

        # Fetch best bets
        if not props_only:
            print(f"[dimers] Fetching best bets from {urls['best_bets']}")
            page = context.new_page()
            try:
                page.goto(urls["best_bets"], wait_until="networkidle", timeout=30000)

                # Try to load all content
                load_clicks = click_load_more_until_done(page, debug=debug)
                scroll_count = scroll_to_load_all(page, debug=debug)

                if debug:
                    print(f"[dimers] Best bets: {load_clicks} load-more clicks, {scroll_count} scrolls")

                # Wait for any final content to load
                page.wait_for_timeout(2000)

                # Extract bets
                bets = extract_best_bets(page, urls["best_bets"], debug=debug)
                all_raw.extend(bets)
                print(f"[dimers] Extracted {len(bets)} best bets")

            except Exception as e:
                print(f"[dimers] Error fetching best bets: {e}")
            finally:
                page.close()

        # Fetch best props
        if not bets_only:
            print(f"[dimers] Fetching best props from {urls['best_props']}")
            page = context.new_page()
            try:
                page.goto(urls["best_props"], wait_until="networkidle", timeout=30000)

                # Try to load all content
                load_clicks = click_load_more_until_done(page, debug=debug)
                scroll_count = scroll_to_load_all(page, debug=debug)

                if debug:
                    print(f"[dimers] Best props: {load_clicks} load-more clicks, {scroll_count} scrolls")

                # Wait for any final content to load
                page.wait_for_timeout(2000)

                # Extract props
                props = extract_best_props(page, urls["best_props"], debug=debug)
                all_raw.extend(props)
                print(f"[dimers] Extracted {len(props)} best props")

            except Exception as e:
                print(f"[dimers] Error fetching best props: {e}")
            finally:
                page.close()

        browser.close()

    # Deduplicate
    all_raw = dedupe_records(all_raw)
    print(f"[dimers] Total raw records after dedup: {len(all_raw)}")

    # Write raw output
    ensure_out_dir()
    raw_path = OUT_DIR / f"raw_dimers_{sport.lower()}.json"
    write_json(str(raw_path), [asdict(r) for r in all_raw])
    print(f"[dimers] Wrote {len(all_raw)} raw records to {raw_path}")

    # Normalize
    normalized = normalize_dimers_records(all_raw, sport=sport, debug=debug)
    norm_path = OUT_DIR / f"normalized_dimers_{sport.lower()}.json"
    write_json(str(norm_path), normalized)

    eligible_count = sum(1 for r in normalized if r.get("eligible_for_consensus"))
    print(
        f"[dimers] Wrote {len(normalized)} normalized records "
        f"({eligible_count} eligible) to {norm_path}"
    )

    # Print summary
    print("\n[dimers] Summary:")
    print(f"  Total raw records:      {len(all_raw)}")
    print(f"  Total normalized:       {len(normalized)}")
    print(f"  Eligible for consensus: {eligible_count}")

    # Breakdown by type
    bets_count = sum(1 for r in all_raw if r.source_surface == "dimers_best_bets")
    props_count = sum(1 for r in all_raw if r.source_surface == "dimers_best_props")
    print(f"  Best bets:              {bets_count}")
    print(f"  Best props:             {props_count}")

    # Sample ineligible reasons
    ineligible = [r for r in normalized if not r.get("eligible_for_consensus")]
    if ineligible:
        reasons = {}
        for r in ineligible:
            reason = r.get("ineligibility_reason", "unknown")
            reasons[reason] = reasons.get(reason, 0) + 1
        print(f"\n[dimers] Ineligible breakdown:")
        for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
            print(f"    {reason}: {count}")


def main():
    parser = argparse.ArgumentParser(description="Ingest Dimers picks")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    parser.add_argument(
        "--sport",
        choices=["NBA", "NCAAB"],
        default="NBA",
        help="Sport to ingest (default: NBA)",
    )
    parser.add_argument(
        "--bets-only",
        action="store_true",
        help="Only fetch best bets page",
    )
    parser.add_argument(
        "--props-only",
        action="store_true",
        help="Only fetch best props page",
    )
    parser.add_argument(
        "--storage-state",
        type=str,
        default=None,
        help=f"Path to storage state JSON for Dimers Pro auth (default: {DEFAULT_STORAGE_STATE})",
    )
    args = parser.parse_args()

    ingest_dimers(
        sport=args.sport,
        debug=args.debug,
        bets_only=args.bets_only,
        props_only=args.props_only,
        storage_state=args.storage_state,
    )


if __name__ == "__main__":
    main()
