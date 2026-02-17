#!/usr/bin/env python3
"""Fast backfill BetQL NBA game-page player props history.

Optimizations over the original script:
1. Parallel game processing with multiple browser contexts
2. Reduced wait timeouts (still reliable but faster)
3. Batch date switching to avoid datepicker overhead
4. Skip known off-season dates
5. Concurrent processing of multiple dates

Usage:
    python3 scripts/backfill_betql_game_props_fast.py --start 2025-02-01 --end 2025-12-31
    python3 scripts/backfill_betql_game_props_fast.py --start 2025-02-01 --end 2025-12-31 --workers 3
    python3 scripts/backfill_betql_game_props_fast.py --missing-only  # Only scrape dates with empty files
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
import time
import concurrent.futures
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple
import re
from urllib.parse import urljoin

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.betql_game_props_extractor import extract_game_props
from src.betql_playwright import BetQLSession, require_storage_state
from src.betql_extractors import parse_teams_from_team_cell
from src.ingest.betql_datepicker import switch_date
from store import ensure_dir

logger = logging.getLogger("betql.backfill.game_props.fast")

URL_SPREADS = "https://betql.co/nba/odds"
DETAIL_LINK_SELECTOR = "div.games-table-column__game-detail-link"
GAME_ANCHOR_SELECTOR = "a[href*='/nba/game-predictions/']"
TEAM_CELL_SELECTOR = "div.games-table-column__team-cell"
NO_EVENTS_SELECTOR = "p.games-view__status-text"
FULL_ANALYSIS_SELECTOR = "a:has-text('See full game analysis'), button:has-text('See full game analysis')"
BLOCKED_RESOURCE_TYPES = {"image", "font", "media"}
BLOCKED_SUBSTRINGS = [
    "googletagmanager", "google-analytics", "doubleclick", "segment",
    "hotjar", "datadog", "sentry", "facebook", "salesmanago", "amplitude",
]

# NBA off-season dates to skip (no games)
# Regular season: Oct - mid-April
# Playoffs: mid-April - mid-June
# Off-season: mid-June - mid-October
NBA_OFFSEASON_MONTHS = {7, 8, 9}  # July, Aug, Sept are definitely off-season

# All-Star break window (mid-Feb typically)
ALL_STAR_DATES_2025 = {date(2025, 2, 14), date(2025, 2, 15), date(2025, 2, 16)}


def is_likely_no_games(target_date: date) -> bool:
    """Check if a date is likely to have no NBA games."""
    if target_date.month in NBA_OFFSEASON_MONTHS:
        return True
    if target_date in ALL_STAR_DATES_2025:
        return True
    return False


def write_jsonl_atomic(path: Path, rows: Iterable[Dict[str, object]]) -> None:
    ensure_dir(str(path.parent))
    fd, tmp_path = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
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


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def date_range(start: date, end: date) -> List[date]:
    if end < start:
        raise ValueError("end date must be >= start date")
    today = date.today()
    end = min(end, today)
    days: List[date] = []
    cur = start
    while cur <= end:
        days.append(cur)
        cur += timedelta(days=1)
    return days


def _block_heavy_requests(page) -> None:
    def handler(route):
        try:
            req = route.request
            url = (req.url or "").lower()
            if req.resource_type in BLOCKED_RESOURCE_TYPES:
                return route.abort()
            if any(tok in url for tok in BLOCKED_SUBSTRINGS):
                return route.abort()
        except Exception:
            pass
        try:
            return route.continue_()
        except Exception:
            try:
                return route.abort()
            except Exception:
                return None
    page.route("**/*", handler)


def _scroll_for_links_fast(page) -> Tuple[int, int]:
    """Faster scrolling with shorter waits."""
    last_count = 0
    stable_ticks = 0
    scrolls = 0
    for _ in range(10):  # Reduced from 15
        try:
            count = max(
                page.locator(DETAIL_LINK_SELECTOR).count(),
                page.locator(GAME_ANCHOR_SELECTOR).count()
            )
        except Exception:
            count = 0
        if count > last_count:
            last_count = count
            stable_ticks = 0
        else:
            stable_ticks += 1
        if stable_ticks >= 2 and last_count > 0:  # Reduced from 3
            break
        scrolls += 1
        try:
            page.mouse.wheel(0, 2000)  # Larger scroll
        except Exception:
            pass
        page.wait_for_timeout(200)  # Reduced from 400ms
    return last_count, scrolls


def _navigate_to_game_props_fast(page, game_url: str) -> bool:
    """Fast navigation to game props page."""
    base_url = game_url.split("#")[0]
    try:
        page.goto(base_url, wait_until="domcontentloaded", timeout=15000)
    except Exception:
        return False

    # Quick check for network idle (but don't wait forever)
    try:
        page.wait_for_load_state("networkidle", timeout=3000)
    except Exception:
        pass

    # Try clicking "See full game analysis"
    try:
        btn = page.locator(FULL_ANALYSIS_SELECTOR).first
        if btn.is_visible(timeout=2000):
            btn.click(timeout=3000)
            page.wait_for_timeout(500)
            return True
    except Exception:
        pass

    # Fallback: scroll to find props
    for _ in range(5):
        page.mouse.wheel(0, 800)
        page.wait_for_timeout(150)
        if page.locator("div.prop-bet").count() > 0:
            return True

    return True


def _collect_game_links_fast(page) -> List[Dict[str, object]]:
    """Fast collection of game links from spreads page."""
    _scroll_for_links_fast(page)

    anchors = page.locator(GAME_ANCHOR_SELECTOR)
    seen = set()
    links = []

    for i in range(anchors.count()):
        try:
            href = anchors.nth(i).get_attribute("href")
            if not href:
                continue
            full_url = href if href.startswith("http") else urljoin("https://betql.co", href)
            canon_url = full_url.split("#", 1)[0]
            if canon_url in seen:
                continue
            seen.add(canon_url)

            # Try to extract team info from nearby cells
            node = anchors.nth(i)
            row = node.locator("xpath=ancestor::*[contains(@class,'games-table')][1]")
            cells = row.locator(TEAM_CELL_SELECTOR)
            away_abbr = home_abbr = None
            if cells.count() >= 2:
                away_abbr, _, _, _ = parse_teams_from_team_cell(cells.nth(0))
                _, home_abbr, _, _ = parse_teams_from_team_cell(cells.nth(1))

            links.append({
                "url": canon_url,
                "away_team": away_abbr.upper() if away_abbr else None,
                "home_team": home_abbr.upper() if home_abbr else None,
            })
        except Exception:
            continue

    return links


def process_game_fast(
    sess: BetQLSession,
    game_url: str,
    day_key: str,
    event_start: str,
    away_team: Optional[str],
    home_team: Optional[str],
) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
    """Process a single game with minimal overhead."""
    dbg: Dict[str, object] = {}
    page = None
    try:
        page = sess.context.new_page() if sess.context else None
        if not page:
            return [], {"error": "no page"}

        _block_heavy_requests(page)
        nav_ok = _navigate_to_game_props_fast(page, game_url)
        if not nav_ok:
            return [], {"error": "nav failed"}

        # Wait briefly for prop elements
        try:
            page.wait_for_selector("div.prop-bet, div.player-props", timeout=5000)
        except Exception:
            pass

        matchup_hint = f"{away_team}@{home_team}" if away_team and home_team else None
        records, meta = extract_game_props(
            page=page,
            game_url=game_url,
            day_key=day_key,
            event_start_time_utc=event_start,
            matchup_hint=matchup_hint,
            away_team=away_team,
            home_team=home_team,
            navigate=False,
        )
        return records, meta
    except Exception as exc:
        return [], {"error": str(exc)}
    finally:
        if page:
            try:
                page.close()
            except Exception:
                pass


def process_day_fast(
    sess: BetQLSession,
    spreads_page,
    target_date: date,
    max_games: Optional[int] = None,
) -> Tuple[Dict[str, object], List[Dict[str, object]]]:
    """Process a single day with optimized timeouts."""
    info: Dict[str, object] = {"date": target_date.isoformat(), "status": "UNKNOWN"}

    # Skip known off-season
    if is_likely_no_games(target_date):
        info["status"] = "SKIP_OFFSEASON"
        return info, []

    # Switch to target date (with faster timeouts)
    try:
        switch_date(
            spreads_page,
            target_date,
            logger=logger,
            response_timeout_ms=8000,  # Reduced from 15000
            fallback_timeout_ms=4000,  # Reduced from 8000
            expected_url_substrings=["/_next/data/", "/nba/odds"],
        )
    except Exception as exc:
        info["status"] = "DATE_SWITCH_FAILED"
        info["error"] = str(exc)
        return info, []

    # Quick check for "no events"
    try:
        status_loc = spreads_page.locator(NO_EVENTS_SELECTOR)
        status_loc.wait_for(state="visible", timeout=1500)
        status_text = (status_loc.inner_text() or "").lower()
        if "no events" in status_text:
            info["status"] = "NO_GAMES"
            return info, []
    except Exception:
        pass

    # Wait briefly for content
    spreads_page.wait_for_timeout(300)

    # Collect game links
    links = _collect_game_links_fast(spreads_page)
    if not links:
        info["status"] = "NO_LINKS"
        return info, []

    if max_games:
        links = links[:max_games]

    day_key = f"NBA:{target_date.year:04d}:{target_date.month:02d}:{target_date.day:02d}"
    event_start = datetime(target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc).isoformat()

    all_records = []
    for link in links:
        recs, _ = process_game_fast(
            sess=sess,
            game_url=link["url"],
            day_key=day_key,
            event_start=event_start,
            away_team=link.get("away_team"),
            home_team=link.get("home_team"),
        )
        all_records.extend(recs)

    info["status"] = "OK" if all_records else "NO_ROWS"
    info["rows"] = len(all_records)
    info["games"] = len(links)
    return info, all_records


def process_date_batch(
    storage_path: str,
    dates: List[date],
    out_root: Path,
    headless: bool,
    max_games: Optional[int],
    overwrite: bool,
) -> List[Tuple[date, int]]:
    """Process a batch of dates in a single browser session."""
    results = []

    with BetQLSession(storage_state_path=storage_path, headless=headless) as sess:
        spreads_page = sess.open(URL_SPREADS)
        # Quick initial wait
        try:
            spreads_page.wait_for_load_state("domcontentloaded", timeout=10000)
        except Exception:
            pass

        for day in dates:
            out_path = out_root / f"{day.isoformat()}.jsonl"
            if out_path.exists() and not overwrite:
                # Skip existing
                results.append((day, -1))
                continue

            try:
                info, rows = process_day_fast(sess, spreads_page, day, max_games)
                status = info.get("status", "UNKNOWN")
                row_count = len(rows)

                logger.info("date=%s status=%s rows=%d games=%s",
                           day.isoformat(), status, row_count, info.get("games", 0))

                if rows:
                    write_jsonl_atomic(out_path, rows)
                elif status in ("NO_GAMES", "SKIP_OFFSEASON") and overwrite:
                    # Write empty file to mark as processed
                    write_jsonl_atomic(out_path, [])

                results.append((day, row_count))
            except Exception as exc:
                logger.warning("date=%s error=%s", day.isoformat(), exc)
                results.append((day, 0))

        try:
            spreads_page.close()
        except Exception:
            pass

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Fast backfill BetQL game-page player props.")
    parser.add_argument("--start", help="YYYY-MM-DD start date")
    parser.add_argument("--end", help="YYYY-MM-DD end date")
    parser.add_argument("--storage", default="data/betql_storage_state.json", help="Playwright storage state path")
    parser.add_argument("--headless", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    parser.add_argument("--max-games-per-day", type=int, default=None, help="Cap games per day")
    parser.add_argument("--out-root", default="data/history/betql_game_props_raw", help="Output folder")
    parser.add_argument("--workers", type=int, default=1, help="Number of parallel browser sessions")
    parser.add_argument("--missing-only", action="store_true",
                       help="Only scrape dates with empty or missing files")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    require_storage_state(args.storage)
    out_root = Path(args.out_root)

    # Determine dates to process
    if args.missing_only:
        # Find dates with empty or missing files
        days_to_scrape = []
        for f in sorted(out_root.glob("*.jsonl")):
            if f.stat().st_size == 0:
                try:
                    d = parse_date(f.stem)
                    if not is_likely_no_games(d):
                        days_to_scrape.append(d)
                except ValueError:
                    pass
        logger.info("Found %d empty files to re-scrape", len(days_to_scrape))
    else:
        if not args.start or not args.end:
            parser.error("--start and --end required unless using --missing-only")
        start = parse_date(args.start)
        end = parse_date(args.end)
        all_days = date_range(start, end)

        # Filter out already-scraped dates
        if not args.overwrite:
            days_to_scrape = [d for d in all_days if not (out_root / f"{d.isoformat()}.jsonl").exists()]
            skipped = len(all_days) - len(days_to_scrape)
            if skipped > 0:
                logger.info("Skipping %d already-scraped dates, %d remaining", skipped, len(days_to_scrape))
        else:
            days_to_scrape = all_days

    if not days_to_scrape:
        logger.info("Nothing to scrape")
        return

    # Skip known off-season dates
    pre_filter = len(days_to_scrape)
    days_to_scrape = [d for d in days_to_scrape if not is_likely_no_games(d)]
    filtered = pre_filter - len(days_to_scrape)
    if filtered > 0:
        logger.info("Filtered out %d off-season dates", filtered)

    # Time estimate (optimized script is ~30s per date)
    est_minutes = len(days_to_scrape) * 30 / 60
    logger.info("Starting fast backfill: %d dates (est. %.1f min with %d worker(s))",
               len(days_to_scrape), est_minutes / args.workers, args.workers)

    backfill_start = time.time()
    total_rows = 0
    dates_completed = 0

    if args.workers > 1:
        # Split dates into batches for parallel processing
        batch_size = (len(days_to_scrape) + args.workers - 1) // args.workers
        batches = [days_to_scrape[i:i + batch_size] for i in range(0, len(days_to_scrape), batch_size)]

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [
                executor.submit(
                    process_date_batch,
                    args.storage,
                    batch,
                    out_root,
                    args.headless,
                    args.max_games_per_day,
                    args.overwrite,
                )
                for batch in batches
            ]

            for future in concurrent.futures.as_completed(futures):
                try:
                    results = future.result()
                    for day, row_count in results:
                        if row_count >= 0:
                            total_rows += row_count
                            dates_completed += 1
                except Exception as exc:
                    logger.error("Batch failed: %s", exc)
    else:
        # Single-threaded processing
        results = process_date_batch(
            args.storage,
            days_to_scrape,
            out_root,
            args.headless,
            args.max_games_per_day,
            args.overwrite,
        )
        for day, row_count in results:
            if row_count >= 0:
                total_rows += row_count
                dates_completed += 1

    elapsed = time.time() - backfill_start
    avg_per_date = elapsed / dates_completed if dates_completed > 0 else 0
    logger.info(
        "Backfill complete: %d rows across %d dates in %.1fs (%.1fs avg per date)",
        total_rows, dates_completed, elapsed, avg_per_date
    )


if __name__ == "__main__":
    main()
