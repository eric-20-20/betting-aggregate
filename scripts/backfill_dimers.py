#!/usr/bin/env python3
"""
Dimers.com historical data backfill script.

Navigates through the Dimers schedule calendar to extract Best Bets data
from historical NBA or NCAAB games. Only extracts bets with edge >= 2.0%.

Usage:
    python scripts/backfill_dimers.py --start-date 2025-10-22 --end-date 2026-01-31 --debug
    python scripts/backfill_dimers.py --sport NCAAB --start-date 2024-11-01 --end-date 2026-03-10

Output:
    - out/backfill_dimers_nba_bets.jsonl   (NBA, appends)
    - out/backfill_dimers_ncaab_bets.jsonl (NCAAB, appends)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any

# Add repo root to path
REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))

try:
    from playwright.sync_api import sync_playwright, Page
except ImportError:
    sync_playwright = None

from src.dimers_extractors import sha256_digest

# Constants
STORAGE_STATE = REPO_ROOT / "dimers_storage_state.json"
OUT_DIR = REPO_ROOT / "out"

SCHEDULE_URLS = {
    "NBA":   "https://www.dimers.com/bet-hub/nba/schedule",
    "NCAAB": "https://www.dimers.com/bet-hub/cbb/schedule",
}
SCHEDULE_HREF_FILTERS = {
    "NBA":   "/bet-hub/nba/schedule/",
    "NCAAB": "/bet-hub/cbb/schedule/",
}
# Default output files per sport (used when --output not specified)
OUTPUT_FILES = {
    "NBA":   OUT_DIR / "backfill_dimers_nba_bets.jsonl",
    "NCAAB": OUT_DIR / "backfill_dimers_ncaab_bets.jsonl",
}

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Minimum edge percentage to include (2.0%)
MIN_EDGE_PCT = 2.0

# NBA teams for filtering
NBA_TEAMS = {
    'hawks', 'celtics', 'nets', 'hornets', 'bulls', 'cavaliers', 'mavericks',
    'nuggets', 'pistons', 'warriors', 'rockets', 'pacers', 'clippers', 'lakers',
    'grizzlies', 'heat', 'bucks', 'timberwolves', 'pelicans', 'knicks', 'thunder',
    'magic', 'sixers', '76ers', 'suns', 'blazers', 'kings', 'spurs', 'raptors',
    'jazz', 'wizards'
}


@dataclass
class DimersBetRecord:
    """Record for a Dimers backfill bet."""
    source_id: str
    source_surface: str
    sport: str
    game_date: str
    game_url: str
    matchup: str
    market_type: str  # spread, total, moneyline
    pick_text: str
    probability_pct: float
    edge_pct: float
    best_odds: Optional[str]
    observed_at_utc: str
    raw_fingerprint: str

    # Game result (if available)
    final_score: Optional[str] = None


def extract_bets_from_game_page(
    page: Page,
    game_url: str,
    game_date: str,
    sport: str = "NBA",
    debug: bool = False
) -> List[DimersBetRecord]:
    """
    Extract best bets from a Dimers game page.

    Only returns bets with edge >= MIN_EDGE_PCT.
    """
    records = []
    observed = datetime.now(timezone.utc).isoformat()

    try:
        # Navigate to game page
        page.goto(game_url, wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(1500)

        # Get matchup from title
        title = page.title()
        matchup_match = re.search(r"(\w+)\s+vs\.?\s+(\w+)", title)
        matchup = matchup_match.group(0) if matchup_match else "Unknown"

        # For NBA: verify the page is an NBA game via team name filter
        # For NCAAB: trust the schedule URL — all games are CBB
        if sport == "NBA":
            is_nba = any(team in title.lower() for team in NBA_TEAMS)
            if not is_nba:
                if debug:
                    print(f"  Skipping non-NBA game: {title}")
                return []

        # Click Best Bets tab
        best_bets_tab = page.locator('div.tab-option:has-text("Best Bets")')
        if best_bets_tab.count() == 0:
            if debug:
                print(f"  No Best Bets tab found for {game_url}")
            return []

        best_bets_tab.first.click()
        page.wait_for_timeout(1500)

        # Extract bets using JavaScript
        bets_data = page.evaluate("""
            () => {
                const bets = [];
                const text = document.body.innerText;
                const lines = text.split('\\n').map(l => l.trim()).filter(l => l);

                // State machine to parse bets
                let inBestBets = false;
                let currentBet = {};
                let lineBuffer = [];

                for (let i = 0; i < lines.length; i++) {
                    const line = lines[i];

                    // Detect start of Best Bets section
                    if (line.includes('BEST BETS FOR THIS GAME') || line.includes('FINAL BETS FOR THIS GAME')) {
                        inBestBets = true;
                        continue;
                    }

                    // Stop at next major section
                    if (inBestBets && (line.includes('We Compare') || line.includes('Best Sportsbook'))) {
                        break;
                    }

                    if (!inBestBets) continue;

                    // Skip "Last Updated" line
                    if (line.includes('Last Updated')) continue;

                    // Detect bet start - look for bet type patterns
                    const isBetLine = line.match(/^(Over|Under)\\s+[\\d.]+$/) ||
                                     line.match(/win$/i) ||
                                     line.match(/^\\w+\\s+[+-][\\d.]+$/);

                    if (isBetLine) {
                        // Save previous bet if complete
                        if (currentBet.pick && currentBet.probability !== undefined) {
                            bets.push({...currentBet});
                        }
                        currentBet = {pick: line};
                        continue;
                    }

                    // Parse probability
                    if (line === 'Probability') {
                        // Next line should be percentage
                        continue;
                    }

                    const probMatch = line.match(/^([\\d.]+)%$/);
                    if (probMatch && currentBet.pick && currentBet.probability === undefined) {
                        currentBet.probability = parseFloat(probMatch[1]);
                        continue;
                    }

                    // Parse edge
                    if (line === 'Edge') {
                        continue;
                    }

                    const edgeMatch = line.match(/^(-?[\\d.]+)%$/);
                    if (edgeMatch && currentBet.probability !== undefined && currentBet.edge === undefined) {
                        currentBet.edge = parseFloat(edgeMatch[1]);
                        continue;
                    }

                    // Parse odds
                    if (line === 'Best odds') {
                        continue;
                    }

                    const oddsMatch = line.match(/^([+-]\\d{3,4})$/);
                    if (oddsMatch && currentBet.edge !== undefined) {
                        currentBet.odds = oddsMatch[1];
                        // Bet is complete
                        bets.push({...currentBet});
                        currentBet = {};
                        continue;
                    }

                    // Skip badge lines
                    if (line === 'Sweet Spot' || line === 'High Edge' || line === 'Value Play' || line === 'Best Value') {
                        if (currentBet.pick) {
                            currentBet.badge = line;
                        }
                        continue;
                    }
                }

                // Don't forget last bet
                if (currentBet.pick && currentBet.probability !== undefined) {
                    bets.push(currentBet);
                }

                return bets;
            }
        """)

        if debug:
            print(f"  Found {len(bets_data)} bets on page")

        # Get final score if available
        final_score = page.evaluate("""
            () => {
                const text = document.body.innerText;
                // Match full basketball scores (2-3 digit numbers, typically 80-150 range)
                // Look for pattern like "FINAL" followed by two scores
                // Try multiple patterns in order of specificity

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

        # Process bets - only keep those with edge >= MIN_EDGE_PCT
        for bet in bets_data:
            pick = bet.get("pick", "")
            prob = bet.get("probability")
            edge = bet.get("edge")
            odds = bet.get("odds")

            if prob is None or edge is None:
                continue

            # Apply edge filter
            if edge < MIN_EDGE_PCT:
                if debug:
                    print(f"    Skipping {pick} - edge {edge}% < {MIN_EDGE_PCT}%")
                continue

            # Determine market type
            pick_lower = pick.lower()
            if "over" in pick_lower or "under" in pick_lower:
                market_type = "total"
            elif "win" in pick_lower:
                market_type = "moneyline"
            elif re.search(r"[+-]\d", pick):
                market_type = "spread"
            else:
                market_type = "unknown"

            fingerprint = sha256_digest(f"dimers|backfill|{game_url}|{pick}")

            record = DimersBetRecord(
                source_id="dimers",
                source_surface="dimers_schedule_backfill",
                sport=sport,
                game_date=game_date,
                game_url=game_url,
                matchup=matchup,
                market_type=market_type,
                pick_text=pick,
                probability_pct=prob,
                edge_pct=edge,
                best_odds=odds,
                observed_at_utc=observed,
                raw_fingerprint=fingerprint,
                final_score=final_score,
            )
            records.append(record)

            if debug:
                print(f"    ✓ {pick} | prob={prob}% | edge={edge}% | odds={odds}")

    except Exception as e:
        if debug:
            print(f"  Error extracting from {game_url}: {e}")

    return records


def get_game_urls_for_date(
    page: Page,
    target_date: datetime,
    sport: str = "NBA",
    debug: bool = False
) -> List[str]:
    """
    Navigate to a specific date and get all game URLs for the given sport.
    """
    game_urls = []
    schedule_url = SCHEDULE_URLS.get(sport, SCHEDULE_URLS["NBA"])
    href_filter = SCHEDULE_HREF_FILTERS.get(sport, SCHEDULE_HREF_FILTERS["NBA"])

    try:
        # Go to schedule page
        page.goto(schedule_url, wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(2000)

        # Open calendar by clicking date dropdown
        date_dropdown = page.locator('.date-dropdown')
        if date_dropdown.count() == 0:
            if debug:
                print("  Date dropdown not found")
            return []

        date_dropdown.first.click()
        page.wait_for_timeout(1000)

        # Navigate to correct month
        current_month = page.evaluate("""
            () => {
                const header = document.querySelector('.mat-calendar-period-button');
                return header ? header.textContent.trim() : '';
            }
        """)

        target_month_str = target_date.strftime("%b").upper()
        target_year = target_date.year

        # Navigate months until we reach the target
        max_nav = 24  # Max 2 years of navigation
        for _ in range(max_nav):
            current_header = page.evaluate("""
                () => {
                    const header = document.querySelector('.mat-calendar-period-button');
                    return header ? header.textContent.trim() : '';
                }
            """)

            if debug:
                print(f"    Calendar at: {current_header}")

            # Check if we're at the right month
            if (target_month_str in current_header.upper() and
                str(target_year) in current_header):
                break

            # Determine direction - compare dates
            # Parse current header (e.g., "FEB 2026")
            current_parts = current_header.replace("FEB", "Feb").replace("JAN", "Jan").split()
            if len(current_parts) >= 2:
                try:
                    current_month_dt = datetime.strptime(f"{current_parts[0]} 1 {current_parts[1]}", "%b %d %Y")
                    target_month_dt = datetime(target_year, target_date.month, 1)

                    if current_month_dt > target_month_dt:
                        # Need to go back
                        page.evaluate("() => document.querySelector('.mat-calendar-previous-button')?.click()")
                    else:
                        # Need to go forward
                        page.evaluate("() => document.querySelector('.mat-calendar-next-button')?.click()")
                    page.wait_for_timeout(500)
                except:
                    # Default to going back
                    page.evaluate("() => document.querySelector('.mat-calendar-previous-button')?.click()")
                    page.wait_for_timeout(500)
            else:
                page.evaluate("() => document.querySelector('.mat-calendar-previous-button')?.click()")
                page.wait_for_timeout(500)

        # Now click on the target date
        target_label = target_date.strftime("%B %-d, %Y")  # e.g., "January 1, 2026"

        # Try clicking the date
        clicked = page.evaluate(f"""
            () => {{
                const cells = document.querySelectorAll('.mat-calendar-body-cell');
                for (const cell of cells) {{
                    const label = cell.getAttribute('aria-label');
                    if (label === "{target_label}") {{
                        if (cell.getAttribute('aria-disabled') === 'true') {{
                            return 'disabled';
                        }}
                        cell.click();
                        return 'clicked';
                    }}
                }}
                return 'not_found';
            }}
        """)

        if clicked == 'disabled':
            if debug:
                print(f"  Date {target_label} is disabled (no games)")
            return []
        elif clicked == 'not_found':
            if debug:
                print(f"  Date {target_label} not found in calendar")
            return []

        page.wait_for_timeout(2000)

        # Extract game URLs for the target sport
        game_urls = page.evaluate(f"""
            () => {{
                const urls = [];
                document.querySelectorAll('a[href*="{href_filter}"]').forEach(a => {{
                    const href = a.href;
                    // Only include actual game URLs (have underscores indicating game ID)
                    if (href.includes('_') && !href.endsWith('/schedule')) {{
                        if (!urls.includes(href)) {{
                            urls.push(href);
                        }}
                    }}
                }});
                return urls;
            }}
        """)

        if debug:
            print(f"  Found {len(game_urls)} {sport} games for {target_date.strftime('%Y-%m-%d')}")

    except Exception as e:
        if debug:
            print(f"  Error getting games for {target_date}: {e}")

    return game_urls


def load_processed_urls(output_file: Path) -> set:
    """Load already processed game URLs from output file."""
    processed = set()
    if output_file.exists():
        with open(output_file) as f:
            for line in f:
                try:
                    record = json.loads(line)
                    processed.add(record.get("game_url", ""))
                except:
                    continue
    return processed


def append_records(output_file: Path, records: List[DimersBetRecord]) -> None:
    """Append records to output JSONL file."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "a") as f:
        for record in records:
            f.write(json.dumps(asdict(record)) + "\n")


def backfill_dimers(
    start_date: datetime,
    end_date: datetime,
    sport: str = "NBA",
    debug: bool = False,
    skip_processed: bool = True,
    output_file: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Main backfill function.

    Args:
        start_date: Start date for backfill
        end_date: End date for backfill
        sport: Sport to backfill ("NBA" or "NCAAB")
        debug: Enable debug output
        skip_processed: Skip games already in output file

    Returns:
        Statistics dict
    """
    if output_file is None:
        output_file = OUTPUT_FILES.get(sport, OUTPUT_FILES["NBA"])
    if sync_playwright is None:
        raise RuntimeError("playwright not installed")

    stats = {
        "dates_processed": 0,
        "games_processed": 0,
        "bets_extracted": 0,
        "bets_with_edge": 0,
        "errors": 0,
    }

    # Load processed URLs if skipping
    processed_urls = load_processed_urls(output_file) if skip_processed else set()
    if processed_urls and debug:
        print(f"Loaded {len(processed_urls)} already processed URLs")

    storage_path = str(STORAGE_STATE) if STORAGE_STATE.exists() else None
    if storage_path:
        print(f"Using storage state from {storage_path}")
    else:
        print("Warning: No storage state found, running unauthenticated")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=USER_AGENT,
            storage_state=storage_path
        )
        page = context.new_page()

        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            print(f"\n[{date_str}] Processing...")

            # Get game URLs for this date
            game_urls = get_game_urls_for_date(page, current_date, sport=sport, debug=debug)

            if not game_urls:
                print(f"  No games found")
                current_date += timedelta(days=1)
                continue

            stats["dates_processed"] += 1

            for game_url in game_urls:
                # Skip if already processed
                if game_url in processed_urls:
                    if debug:
                        print(f"  Skipping (already processed): {game_url}")
                    continue

                if debug:
                    print(f"  Processing: {game_url}")

                # Extract bets from game page
                records = extract_bets_from_game_page(
                    page, game_url, date_str, sport=sport, debug=debug
                )

                stats["games_processed"] += 1
                stats["bets_extracted"] += len(records)

                if records:
                    stats["bets_with_edge"] += len(records)
                    append_records(output_file, records)
                    processed_urls.add(game_url)

            current_date += timedelta(days=1)

        browser.close()

    return stats


def main():
    parser = argparse.ArgumentParser(description="Backfill Dimers historical bets (NBA or NCAAB)")
    parser.add_argument(
        "--sport",
        type=str,
        default="NBA",
        choices=["NBA", "NCAAB"],
        help="Sport to backfill (default: NBA)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        required=True,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )
    parser.add_argument(
        "--no-skip",
        action="store_true",
        help="Don't skip already processed games",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output file (default: out/backfill_dimers_{sport}_bets.jsonl)",
    )
    args = parser.parse_args()

    sport = args.sport.upper()

    # Parse dates
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")

    output_file = Path(args.output) if args.output else OUTPUT_FILES[sport]

    print(f"Dimers Backfill ({sport})")
    print(f"===============")
    print(f"Date range: {args.start_date} to {args.end_date}")
    print(f"Min edge: {MIN_EDGE_PCT}%")
    print(f"Output: {output_file}")
    print()

    stats = backfill_dimers(
        start_date=start_date,
        end_date=end_date,
        sport=sport,
        debug=args.debug,
        skip_processed=not args.no_skip,
        output_file=output_file,
    )

    print(f"\n=== Summary ===")
    print(f"Dates processed:    {stats['dates_processed']}")
    print(f"Games processed:    {stats['games_processed']}")
    print(f"Total bets found:   {stats['bets_extracted']}")
    print(f"Bets with edge≥{MIN_EDGE_PCT}%: {stats['bets_with_edge']}")
    print(f"Output written to:  {output_file}")


if __name__ == "__main__":
    main()
