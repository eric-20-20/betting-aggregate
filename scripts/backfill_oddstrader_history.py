#!/usr/bin/env python3
"""OddsTrader historical backfill.

Navigates OddsTrader's calendar date picker to scrape AI picks for a date
range, storing one JSONL file per date.

Usage:
    python3 scripts/backfill_oddstrader_history.py --start 2026-01-01 --end 2026-02-18
    python3 scripts/backfill_oddstrader_history.py --start 2026-02-01 --end 2026-02-18 --sport NCAAB
    python3 scripts/backfill_oddstrader_history.py --start 2026-02-10 --end 2026-02-12 --debug
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from playwright.sync_api import Page, sync_playwright

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from oddstrader_ingest import (
    EXTRACT_PICKS_JS,
    LEAGUE_IDS,
    MIN_STARS,
    URLS,
    _build_event_lookup,
    _build_uuid_to_team,
    _market_type_from_dom,
    _parse_cover_prob,
    _parse_ev,
    _parse_game_time,
    _parse_line,
    normalize_pick,
)
from store import NBA_SPORT, NCAAB_SPORT

logger = logging.getLogger("oddstrader.backfill")

OUT_ROOT = Path("data/history/oddstrader")

# Month name → abbreviation used in OddsTrader calendar month selector IDs
MONTH_ABBRS = [
    "", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]


def date_range(start: date, end: date) -> List[date]:
    """Generate inclusive date range, capped at today."""
    today = date.today()
    end = min(end, today)
    dates: List[date] = []
    cur = start
    while cur <= end:
        dates.append(cur)
        cur += timedelta(days=1)
    return dates


def _out_path(sport: str, target_date: date) -> Path:
    """Path for a single date's backfill file."""
    sport_dir = OUT_ROOT / sport.lower()
    return sport_dir / f"{target_date.isoformat()}.jsonl"


def _already_scraped(sport: str, target_date: date) -> bool:
    return _out_path(sport, target_date).exists()


def write_jsonl_atomic(path: Path, rows: List[Dict[str, Any]]) -> None:
    """Atomically write rows to JSONL."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
                f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        raise


def switch_date(page: Page, target: date, current_month: Optional[int] = None) -> str:
    """Navigate OddsTrader's calendar to a target date.

    OddsTrader's calendar has:
    - A calendar button: [data-cy="calendar-button"]
    - Month selector tabs with IDs: #Oct, #Nov, ..., #Jun
    - Day cells: td.day.isFromMonth

    Returns the date display text after navigation.
    """
    # Use JS clicks throughout to avoid sticky header interception issues.
    # The "ODDSTRADER PICKS" sticky banner overlaps calendar elements and
    # blocks Playwright's actionability checks, but JS .click() bypasses this.

    # Open calendar dropdown
    page.evaluate("""() => {
        const btn = document.querySelector('[data-cy="calendar-button"]');
        if (btn) btn.click();
    }""")
    page.wait_for_timeout(600)

    # Navigate to correct month if needed
    target_month_abbr = MONTH_ABBRS[target.month]
    if current_month is None or current_month != target.month:
        page.evaluate(f"""() => {{
            const el = document.getElementById('{target_month_abbr}');
            if (el) el.click();
        }}""")
        page.wait_for_timeout(800)

    # Click the target day
    page.evaluate(f"""() => {{
        const cells = document.querySelectorAll('td.day.isFromMonth');
        for (const c of cells) {{
            if (c.innerText.trim() === '{target.day}') {{
                c.click();
                return true;
            }}
        }}
        return false;
    }}""")

    # Wait for content to load after date change
    page.wait_for_timeout(5000)

    # Verify date changed
    try:
        date_text = page.evaluate(
            """() => {
            const el = document.querySelector('[class*="currentDate"]');
            return el ? el.innerText.trim() : '';
        }"""
        )
    except Exception:
        date_text = ""

    return date_text


def extract_picks_for_date(
    page: Page, sport: str, target_date: date, debug: bool = False
) -> List[Dict[str, Any]]:
    """Extract and normalize all picks visible on the current page.

    Assumes the page is already navigated to the target date.
    Returns a list of normalized pick records.
    """
    lid = LEAGUE_IDS.get(sport, "5")

    # Extract __INITIAL_STATE__ for metadata
    state_json = page.evaluate("() => JSON.stringify(window.__INITIAL_STATE__)")
    state = json.loads(state_json) if state_json else {}
    uuid_to_team = _build_uuid_to_team(state, lid)
    event_lookup = _build_event_lookup(state, lid)

    if debug:
        logger.debug(
            "  UUID→team: %d, Event lookup: %d games", len(uuid_to_team), len(event_lookup)
        )

    # Extract DOM picks
    dom_picks = page.evaluate(EXTRACT_PICKS_JS)
    if debug:
        logger.debug("  DOM picks: %d", len(dom_picks))

    # Convert DOM picks to raw records (same logic as scrape_picks in oddstrader_ingest.py)
    raw_picks = []
    for dp in dom_picks:
        market_type = _market_type_from_dom(dp.get("marketType", ""))
        if not market_type:
            continue

        home_team = dp.get("homeTeam", "")
        away_team = dp.get("awayTeam", "")

        evt_meta = (
            event_lookup.get(f"{away_team}@{home_team}")
            or event_lookup.get(f"{home_team}@{away_team}")
            or {}
        )
        if evt_meta:
            home_team = evt_meta.get("home_team", home_team)
            away_team = evt_meta.get("away_team", away_team)

        if market_type == "total":
            selection = "game_total"
            side = dp.get("totalDirection", "")
            if not side:
                bl = dp.get("bestLine", "")
                if bl.startswith("o"):
                    side = "over"
                elif bl.startswith("u"):
                    side = "under"
                else:
                    continue
        else:
            logo_uuid = dp.get("teamLogoUUID", "")
            picked_team = uuid_to_team.get(logo_uuid, "")
            if not picked_team:
                continue
            selection = picked_team
            side = picked_team

        line_val, odds_val = _parse_line(dp.get("bestLine", ""))
        ev = _parse_ev(dp.get("ev", ""))
        cover_prob = _parse_cover_prob(dp.get("coverProb", ""))
        stars = dp.get("stars", 0)

        event_time_utc = evt_meta.get("event_time_utc")
        if not event_time_utc:
            event_time_utc = _parse_game_time(dp.get("gameTime", ""))

        # Fix year in event_time_utc — __INITIAL_STATE__ timestamps may
        # have correct month/day/time but wrong year when browsing historical
        # dates via the calendar. Replace the year with target_date's year.
        if event_time_utc:
            try:
                dt_parsed = datetime.fromisoformat(event_time_utc.replace("Z", "+00:00"))
                # The timestamp month/day should match target_date (±1 day for UTC offset)
                # Use target_date's year context: Oct-Dec = target year, Jan-Jun = target year
                correct_year = target_date.year
                if dt_parsed.year != correct_year:
                    dt_fixed = dt_parsed.replace(year=correct_year)
                    event_time_utc = dt_fixed.isoformat()
            except (ValueError, OverflowError):
                pass

        event_desc = evt_meta.get("des", f"{away_team}@{home_team}")

        if market_type == "spread":
            raw_pick_text = f"{selection} {dp.get('bestLine', '')} | {event_desc}"
        elif market_type == "total":
            raw_pick_text = f"{dp.get('bestLine', '')} | {event_desc}"
        elif market_type == "moneyline":
            raw_pick_text = f"{selection} ML {dp.get('bestLine', '')} | {event_desc}"
        else:
            raw_pick_text = f"{selection} | {event_desc}"

        raw = {
            "source_id": "oddstrader",
            "source_surface": f"oddstrader_ai_{market_type}",
            "sport": sport,
            "event_id": evt_meta.get("eid"),
            "event_desc": event_desc,
            "event_slug": evt_meta.get("slg", ""),
            "event_time_utc": event_time_utc,
            "home_team": home_team or evt_meta.get("home_team", ""),
            "away_team": away_team or evt_meta.get("away_team", ""),
            "market_type": market_type,
            "side": side,
            "selection": selection,
            "line": line_val,
            "odds": odds_val,
            "rating_stars": stars,
            "ev_pct": ev,
            "cover_prob": cover_prob,
            "raw_pick_text": raw_pick_text,
            "expert_name": "OddsTrader AI",
            "backfill_date": target_date.isoformat(),
        }

        norm = normalize_pick(raw, sport, min_stars=MIN_STARS)
        if norm:
            raw_picks.append(norm)

    return raw_picks


def run_backfill(
    sport: str,
    start: date,
    end: date,
    force: bool = False,
    debug: bool = False,
    headless: bool = True,
    min_stars: int = MIN_STARS,
) -> Dict[str, Any]:
    """Run OddsTrader backfill for a date range.

    Returns summary statistics.
    """
    dates = date_range(start, end)
    if not force:
        skipped = [d for d in dates if _already_scraped(sport, d)]
        dates = [d for d in dates if not _already_scraped(sport, d)]
        if skipped:
            logger.info("Skipping %d already-scraped dates", len(skipped))

    if not dates:
        logger.info("No dates to scrape")
        return {"dates_total": 0, "dates_scraped": 0, "picks_total": 0}

    logger.info(
        "Backfilling %s: %d dates from %s to %s",
        sport, len(dates), dates[0], dates[-1],
    )

    url = URLS.get(sport)
    if not url:
        raise ValueError(f"No URL for sport {sport}")

    stats = {"dates_total": len(dates), "dates_scraped": 0, "picks_total": 0, "errors": []}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            viewport={"width": 1400, "height": 900},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )
        page = context.new_page()

        try:
            # Initial page load
            logger.info("Loading %s", url)
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(6000)

            current_month: Optional[int] = None

            for i, target_date in enumerate(dates):
                try:
                    date_str = target_date.isoformat()
                    logger.info(
                        "[%d/%d] %s %s",
                        i + 1, len(dates), sport, date_str,
                    )

                    # Navigate to date
                    date_display = switch_date(page, target_date, current_month)
                    current_month = target_date.month

                    if debug:
                        logger.debug("  Date display: %s", date_display)

                    # Extract picks
                    picks = extract_picks_for_date(page, sport, target_date, debug=debug)

                    # Write output
                    out = _out_path(sport, target_date)
                    write_jsonl_atomic(out, picks)

                    eligible = sum(1 for p in picks if p.get("eligible_for_consensus"))
                    logger.info(
                        "  → %d picks (%d eligible) saved to %s",
                        len(picks), eligible, out,
                    )

                    stats["dates_scraped"] += 1
                    stats["picks_total"] += len(picks)

                    # Brief pause between dates to be polite
                    if i < len(dates) - 1:
                        page.wait_for_timeout(1000)

                except Exception as exc:
                    logger.error("  Error on %s: %s", target_date, exc)
                    stats["errors"].append({"date": str(target_date), "error": str(exc)})
                    # Try to recover by reloading
                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=30000)
                        page.wait_for_timeout(6000)
                        current_month = None
                    except Exception:
                        logger.error("  Failed to recover, stopping")
                        break

        finally:
            page.close()
            context.close()
            browser.close()

    return stats


def main():
    parser = argparse.ArgumentParser(description="Backfill OddsTrader historical picks.")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument(
        "--sport", choices=["NBA", "NCAAB"], default="NBA",
        help="Sport (default: NBA)",
    )
    parser.add_argument("--force", action="store_true", help="Overwrite existing dates")
    parser.add_argument("--debug", action="store_true", help="Verbose logging")
    parser.add_argument("--no-headless", action="store_true", help="Show browser")

    args = parser.parse_args()

    level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()

    stats = run_backfill(
        sport=args.sport,
        start=start,
        end=end,
        force=args.force,
        debug=args.debug,
        headless=not args.no_headless,
    )

    logger.info("Done: %s", json.dumps(stats, indent=2, default=str))


if __name__ == "__main__":
    main()
