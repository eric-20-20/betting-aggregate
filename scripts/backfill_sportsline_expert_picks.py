#!/usr/bin/env python3
"""
Backfill SportsLine expert picks with Win/Loss results.

Clicks "Load More" and intercepts GraphQL responses to collect picks.
Results are pre-graded (Win/Loss/Push) by SportsLine.

Usage:
    python3 scripts/backfill_sportsline_expert_picks.py --max-picks 200
    python3 scripts/backfill_sportsline_expert_picks.py --max-picks 500 --debug
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Set

from playwright.sync_api import sync_playwright

REPO_ROOT = Path(__file__).resolve().parents[1]
STORAGE_STATE_PATH = REPO_ROOT / "data" / "sportsline_storage_state.json"

# Sport-specific configuration
NBA_SPORT = "NBA"
NCAAB_SPORT = "NCAAB"

EXPERT_PICKS_URLS = {
    NBA_SPORT: "https://www.sportsline.com/nba/picks/experts/?sc=",
    NCAAB_SPORT: "https://www.sportsline.com/college-basketball/picks/experts/?sc=",
}

def get_output_path(sport: str) -> Path:
    """Get output path for a sport."""
    suffix = sport.lower()
    return REPO_ROOT / "out" / f"raw_sportsline_{suffix}_expert_picks.jsonl"

def get_state_path(sport: str) -> Path:
    """Get state path for a sport."""
    suffix = sport.lower()
    return REPO_ROOT / "out" / f"sportsline_{suffix}_expert_picks_state.json"


def fetch_picks_via_load_more(
    storage_state_path: str,
    max_picks: int = 200,
    max_clicks: int = 50,
    delay_ms: int = 2000,
    debug: bool = False,
    sport: str = NBA_SPORT,
) -> List[Dict]:
    """
    Fetch expert picks by clicking Load More and intercepting responses.
    """
    all_picks: List[Dict] = []
    seen_ids: Set[str] = set()
    expert_picks_url = EXPERT_PICKS_URLS.get(sport, EXPERT_PICKS_URLS[NBA_SPORT])

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(storage_state=storage_state_path)
        page = context.new_page()

        def handle_response(response):
            if "helios" in response.url:
                try:
                    body = response.json()
                    picks_data = body.get("data", {}).get("expertPicks", {})
                    if picks_data:
                        for edge in picks_data.get("edges", []):
                            node = edge.get("node", {})
                            pick_id = node.get("id")
                            if pick_id and pick_id not in seen_ids:
                                seen_ids.add(pick_id)
                                all_picks.append(node)
                except:
                    pass

        page.on("response", handle_response)

        if debug:
            print(f"[DEBUG] Navigating to {expert_picks_url}")

        page.goto(expert_picks_url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(5000)

        if debug:
            print(f"[DEBUG] After initial load: {len(all_picks)} picks")

        # Click Load More until we have enough picks or run out of pages
        for click_num in range(max_clicks):
            if len(all_picks) >= max_picks:
                if debug:
                    print(f"[DEBUG] Reached max picks ({max_picks})")
                break

            # Scroll to bottom to reveal button
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(500)

            # Find and click Load More
            load_more = page.locator("button:has-text('Load More')")
            if load_more.count() == 0:
                if debug:
                    print(f"[DEBUG] No more Load More button after {click_num} clicks")
                break

            load_more.first.click()
            page.wait_for_timeout(delay_ms)

            if debug and (click_num + 1) % 5 == 0:
                print(f"[DEBUG] Click {click_num + 1}: {len(all_picks)} total picks")

        browser.close()

    return all_picks[:max_picks]


def load_state(sport: str = NBA_SPORT) -> Dict[str, Any]:
    """Load backfill state."""
    state_path = get_state_path(sport)
    if state_path.exists():
        try:
            return json.loads(state_path.read_text())
        except:
            pass
    return {"processed_ids": [], "last_run": None}


def save_state(state: Dict[str, Any], sport: str = NBA_SPORT) -> None:
    """Save backfill state."""
    state_path = get_state_path(sport)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, indent=2))


def main():
    parser = argparse.ArgumentParser(description="Backfill SportsLine expert picks")
    parser.add_argument("--storage", default=str(STORAGE_STATE_PATH), help="Path to storage state JSON")
    parser.add_argument("--output", help="Output JSONL file (default: auto-generated based on sport)")
    parser.add_argument("--max-picks", type=int, default=200, help="Maximum picks to fetch")
    parser.add_argument("--max-clicks", type=int, default=50, help="Maximum Load More clicks")
    parser.add_argument("--delay", type=int, default=2000, help="Delay between clicks (ms)")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    parser.add_argument("--append", action="store_true", help="Append to existing output")
    parser.add_argument(
        "--sport",
        choices=["NBA", "NCAAB"],
        default="NBA",
        help="Sport to fetch (default: NBA)",
    )
    args = parser.parse_args()

    if not os.path.exists(args.storage):
        print(f"Storage state not found: {args.storage}")
        print("Run scripts/create_sportsline_storage.py first")
        return 1

    # Use sport-specific output path if not explicitly provided
    output_path = args.output or str(get_output_path(args.sport))

    print(f"[BACKFILL] Fetching up to {args.max_picks} SportsLine {args.sport} expert picks...")

    # Fetch picks
    picks = fetch_picks_via_load_more(
        storage_state_path=args.storage,
        max_picks=args.max_picks,
        max_clicks=args.max_clicks,
        delay_ms=args.delay,
        debug=args.debug,
        sport=args.sport,
    )

    print(f"[BACKFILL] Fetched {len(picks)} picks")

    # Summarize
    results: Dict[str, int] = {}
    markets: Dict[str, int] = {}
    experts: Dict[str, int] = {}
    dates: set = set()

    for pick in picks:
        result = pick.get("resultStatus", "Unknown")
        results[result] = results.get(result, 0) + 1

        selection = pick.get("selection", {})
        market = selection.get("marketType", "Unknown")
        markets[market] = markets.get(market, 0) + 1

        expert = pick.get("expert", {})
        name = f"{expert.get('firstName', '')} {expert.get('lastName', '')}".strip()
        experts[name] = experts.get(name, 0) + 1

        game = pick.get("game", {})
        dt = game.get("scheduledTime", "")[:10]
        if dt:
            dates.add(dt)

    print(f"[BACKFILL] Results: {results}")
    print(f"[BACKFILL] Markets: {markets}")
    print(f"[BACKFILL] Experts: {len(experts)} unique")
    if dates:
        print(f"[BACKFILL] Date range: {min(dates)} to {max(dates)}")

    # Write output
    output_path_obj = Path(output_path)
    output_path_obj.parent.mkdir(parents=True, exist_ok=True)

    mode = "a" if args.append else "w"
    with open(output_path_obj, mode) as f:
        for pick in picks:
            f.write(json.dumps(pick) + "\n")

    print(f"[BACKFILL] Wrote {len(picks)} {args.sport} picks to {output_path}")

    # Save state
    state = load_state(args.sport)
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    state["picks_count"] = len(picks)
    state["sport"] = args.sport
    state["date_range"] = {"min": min(dates) if dates else None, "max": max(dates) if dates else None}
    save_state(state, args.sport)

    return 0


if __name__ == "__main__":
    sys.exit(main())
