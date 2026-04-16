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
from store import NBA_SPORT, NCAAB_SPORT, MLB_SPORT, write_json

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
    MLB_SPORT: {
        "best_bets": "https://www.dimers.com/best-bets/mlb",
        "best_props": "https://www.dimers.com/best-props/mlb",
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


def _apply_24h_filter(page, debug: bool = False) -> bool:
    """
    Click through the Dimers filter UI to select the 24 Hrs timeframe.

    Steps:
      1. Click the Filter button
      2. Wait for filter panel to appear
      3. Click the Timeframe section header to expand it
      4. Click the "24 Hrs" button
      5. Click "Show X Bets" to apply and close

    Returns True if all steps succeeded, False if any step failed.
    Logs warnings on failure but never raises.
    """
    try:
        # 1. Click Filter button
        filter_btn = page.locator("button:has-text('Filter')").first
        if not filter_btn.is_visible(timeout=3000):
            print("[dimers] WARNING: Filter button not visible — skipping 24h filter")
            return False
        filter_btn.click()
        page.wait_for_timeout(1500)

        # 2. Expand the Timeframe section (it's collapsed by default)
        expanded = page.evaluate("""() => {
            const tf = document.querySelector('.select-menu.time-frame');
            if (!tf) return false;
            const row = tf.querySelector('[class*="cursor-pointer"]');
            if (!row) return false;
            row.click();
            return true;
        }""")
        if not expanded:
            print("[dimers] WARNING: Could not find/expand Timeframe section")
            return False
        page.wait_for_timeout(800)

        # 3. Click "24 Hrs" inside the expanded Timeframe section
        clicked_24h = page.evaluate("""() => {
            const tf = document.querySelector('.select-menu.time-frame');
            if (!tf) return false;
            for (const btn of tf.querySelectorAll('button')) {
                if (btn.innerText?.trim() === '24 Hrs') {
                    btn.click();
                    return true;
                }
            }
            return false;
        }""")
        if not clicked_24h:
            print("[dimers] WARNING: '24 Hrs' button not found in Timeframe section")
            return False
        page.wait_for_timeout(800)

        # 4. Click "Show X Bets" to apply
        applied = page.evaluate("""() => {
            for (const btn of document.querySelectorAll('button')) {
                if (/Show.*Bet/i.test(btn.innerText?.trim())) {
                    btn.click();
                    return true;
                }
            }
            return false;
        }""")
        if not applied:
            print("[dimers] WARNING: 'Show X Bets' apply button not found")
            return False

        page.wait_for_timeout(1500)
        if debug:
            print("[dimers] 24h filter applied successfully")
        return True

    except Exception as e:
        print(f"[dimers] WARNING: 24h filter failed ({e}) — continuing without filter")
        return False


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

    filtered_to_24h_bets = False
    filtered_to_24h_props = False

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

                # Apply 24h timeframe filter via UI (requires Pro auth)
                filtered_to_24h_bets = _apply_24h_filter(page, debug=debug)

                # Wait for any final content to load
                page.wait_for_timeout(2000)

                # Extract bets
                bets = extract_best_bets(page, urls["best_bets"], debug=debug)
                all_raw.extend(bets)
                print(f"[dimers] Extracted {len(bets)} best bets (filtered_to_24h={filtered_to_24h_bets})")

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

                # Apply 24h timeframe filter via UI
                filtered_to_24h_props = _apply_24h_filter(page, debug=debug)

                # Wait for any final content to load
                page.wait_for_timeout(2000)

                # Extract props
                props = extract_best_props(page, urls["best_props"], debug=debug)
                all_raw.extend(props)
                print(f"[dimers] Extracted {len(props)} best props (filtered_to_24h={filtered_to_24h_props})")

            except Exception as e:
                print(f"[dimers] Error fetching best props: {e}")
            finally:
                page.close()

        browser.close()

    filtered_to_24h = filtered_to_24h_bets or filtered_to_24h_props
    print(f"[dimers] filtered_to_24h: bets={filtered_to_24h_bets} props={filtered_to_24h_props}")

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

    # Post-scrape date fallback: drop any records not assigned to today's ET date.
    # This is a safety net for when the UI filter partially works or misses cards.
    from zoneinfo import ZoneInfo
    _today_et = datetime.now(ZoneInfo("America/New_York")).strftime(f"{sport}:%Y:%m:%d")
    before_date_filter = len(normalized)
    normalized = [
        r for r in normalized
        if (r.get("event") or {}).get("day_key") == _today_et
    ]
    dropped_future = before_date_filter - len(normalized)
    if dropped_future:
        print(f"[dimers] Post-scrape date filter: dropped {dropped_future} non-today records (kept today={_today_et})")

    norm_path = OUT_DIR / f"normalized_dimers_{sport.lower()}.json"
    write_json(str(norm_path), normalized)

    eligible_count = sum(1 for r in normalized if r.get("eligible_for_consensus"))
    print(
        f"[dimers] Wrote {len(normalized)} normalized records "
        f"({eligible_count} eligible) to {norm_path}"
    )

    # Print summary
    print("\n[dimers] Summary:")
    print(f"  filtered_to_24h:        {filtered_to_24h}")
    print(f"  Total raw records:      {len(all_raw)}")
    print(f"  Before date filter:     {before_date_filter}")
    print(f"  After date filter:      {len(normalized)}")
    print(f"  Eligible for consensus: {eligible_count}")

    # Breakdown by type
    bets_count = sum(1 for r in all_raw if r.source_surface == "dimers_best_bets")
    props_count = sum(1 for r in all_raw if r.source_surface == "dimers_best_props")
    print(f"  Best bets (raw):        {bets_count}")
    print(f"  Best props (raw):       {props_count}")

    # day_key distribution (after filter)
    from collections import Counter
    day_dist = Counter((r.get("event") or {}).get("day_key", "UNKNOWN") for r in normalized)
    print(f"  Day distribution:       {dict(sorted(day_dist.items()))}")

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
        choices=["NBA", "NCAAB", "MLB"],
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
