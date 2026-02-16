#!/usr/bin/env python3
"""Backfill BetQL NBA game-page player props history via spreads datepicker."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple
import re
from urllib.parse import urljoin

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.betql_game_props_extractor import extract_game_props, PROPS_ROOT_SELECTOR
from src.betql_playwright import BetQLSession, wait_for_ready, require_storage_state
from src.betql_extractors import parse_teams_from_team_cell
from src.ingest.betql_datepicker import switch_date
from store import ensure_dir

logger = logging.getLogger("betql.backfill.game_props")

URL_SPREADS = "https://betql.co/nba/odds"
DETAIL_LINK_SELECTOR = "div.games-table-column__game-detail-link"
GAME_ANCHOR_SELECTOR = "a[href*='/nba/game-predictions/']"
TEAM_CELL_SELECTOR = "div.games-table-column__team-cell"
NO_EVENTS_SELECTOR = "p.games-view__status-text"
# Selector for "See full game analysis" button on the game page
FULL_ANALYSIS_SELECTOR = "a:has-text('See full game analysis'), button:has-text('See full game analysis'), a:has-text('Full Analysis'), button:has-text('Full Analysis')"
# Fallback: look for props tab or section link
PROPS_TAB_SELECTOR = "a:has-text('Props'), button:has-text('Props'), [data-tab='props'], a[href*='#props']"
BLOCKED_RESOURCE_TYPES = {"image", "font", "media"}
BLOCKED_SUBSTRINGS = [
    "googletagmanager",
    "google-analytics",
    "doubleclick",
    "segment",
    "hotjar",
    "datadog",
    "sentry",
    "facebook",
    "salesmanago",
    "amplitude",
]


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


def _scroll_for_links(page) -> Tuple[int, int]:
    last_count = 0
    stable_ticks = 0
    scrolls = 0
    for _ in range(15):
        try:
            count_detail = page.locator(DETAIL_LINK_SELECTOR).count()
            count_anchor = page.locator(GAME_ANCHOR_SELECTOR).count()
            count = max(count_detail, count_anchor)
        except Exception:
            count = 0
        if count > last_count:
            last_count = count
            stable_ticks = 0
        else:
            stable_ticks += 1
        if stable_ticks >= 3 and last_count > 0:
            break
        scrolls += 1
        try:
            page.mouse.wheel(0, 1600)
        except Exception:
            try:
                page.keyboard.press("End")
            except Exception:
                pass
        page.wait_for_timeout(400)
    return last_count, scrolls


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


def _navigate_to_game_props(page, game_url: str, debug: bool = False) -> Dict[str, object]:
    """
    Navigate to a game page and click through to show the prop picks with ratings.

    The game page has "See full game analysis" which reveals the model props.
    Direct navigation to #props shows raw prop lines without model picks.
    """
    dbg: Dict[str, object] = {
        "strategy": None,
        "full_analysis_clicked": False,
        "props_tab_clicked": False,
        "scrolled_to_props": False,
    }

    # Navigate to game page (without #props hash)
    base_url = game_url.split("#")[0]
    try:
        page.goto(base_url, wait_until="domcontentloaded", timeout=20000)
    except Exception as exc:
        dbg["nav_error"] = str(exc)
        return dbg

    # Wait for page to load
    try:
        page.wait_for_load_state("networkidle", timeout=8000)
    except Exception:
        pass

    # Try to click "See full game analysis" button
    try:
        full_analysis = page.locator(FULL_ANALYSIS_SELECTOR).first
        if full_analysis.is_visible(timeout=3000):
            full_analysis.click(timeout=5000)
            dbg["full_analysis_clicked"] = True
            dbg["strategy"] = "full_analysis"
            page.wait_for_timeout(1000)
            if debug:
                logger.debug("[game-props] clicked 'See full game analysis'")
    except Exception as exc:
        if debug:
            logger.debug("[game-props] full analysis button not found: %s", exc)

    # If that didn't work, try clicking Props tab
    if not dbg["full_analysis_clicked"]:
        try:
            props_tab = page.locator(PROPS_TAB_SELECTOR).first
            if props_tab.is_visible(timeout=2000):
                props_tab.click(timeout=5000)
                dbg["props_tab_clicked"] = True
                dbg["strategy"] = "props_tab"
                page.wait_for_timeout(800)
                if debug:
                    logger.debug("[game-props] clicked Props tab")
        except Exception as exc:
            if debug:
                logger.debug("[game-props] props tab not found: %s", exc)

    # Scroll down to find props section
    try:
        # Look for player props section
        props_section = page.locator("div.player-props, div.team-player-props-head, div.prop-bet")
        for _ in range(8):
            if props_section.count() > 0:
                try:
                    props_section.first.scroll_into_view_if_needed(timeout=2000)
                    dbg["scrolled_to_props"] = True
                    break
                except Exception:
                    pass
            page.mouse.wheel(0, 600)
            page.wait_for_timeout(300)
    except Exception as exc:
        if debug:
            logger.debug("[game-props] scroll to props failed: %s", exc)

    if not dbg["strategy"]:
        dbg["strategy"] = "scroll_only"

    return dbg


def _extract_mmddyyyy_from_url(url: str) -> Optional[date]:
    """
    Parse MM-DD-YYYY date at end of URL path.
    """
    try:
        m = re.search(r"(\d{2})-(\d{2})-(\d{4})(?:/)?(?:[#?]|$)", url)
        if not m:
            return None
        month = int(m.group(1))
        day = int(m.group(2))
        year = int(m.group(3))
        return date(year, month, day)
    except Exception:
        return None


def wait_for_model_ready_light(page) -> None:
    """
    Lightweight readiness: short waits for rows/links/status; tolerates No Events and returns quickly.
    """
    signals = [
        "div.games-table-column__team-cell",
        DETAIL_LINK_SELECTOR,
        "p.games-view__status-text",
    ]

    def _check():
        for sel in signals:
            loc = page.locator(sel)
            try:
                loc.wait_for(state="visible", timeout=2000)
                if sel == "p.games-view__status-text":
                    txt = (loc.inner_text() or "").lower()
                    if any(tok in txt for tok in ("no events", "no games", "no picks")):
                        return True
                return True
            except Exception:
                continue
        return False

    if _check():
        return
    try:
        page.reload(wait_until="domcontentloaded")
    except Exception:
        return
    _check()


def fast_gate_spreads_ready(page) -> Tuple[str, Dict[str, object]]:
    """
    Fast gate for spreads page: detect No Events or early rows with limited waits.
    Returns (status, meta) where status in {"READY", "NO_GAMES", "SKIP_NOT_READY"}.
    """
    meta: Dict[str, object] = {}

    def _status_text() -> Optional[str]:
        try:
            loc = page.locator("p.games-view__status-text")
            loc.wait_for(state="visible", timeout=6000)
            return (loc.inner_text() or "").lower()
        except Exception:
            return None

    def _rows_present() -> bool:
        try:
            page.locator("div.games-table-column__team-cell").wait_for(state="attached", timeout=6000)
            return page.locator("div.games-table-column__team-cell").count() > 0
        except Exception:
            return False

    # Pass 1
    status_txt = _status_text()
    if status_txt and "no events" in status_txt:
        meta["status_text"] = status_txt
        return "NO_GAMES", meta
    if _rows_present():
        return "READY", meta

    # Scroll once and retry
    try:
        page.mouse.wheel(0, 2000)
    except Exception:
        try:
            page.keyboard.press("End")
        except Exception:
            pass
    page.wait_for_timeout(500)

    status_txt = _status_text()
    if status_txt and "no events" in status_txt:
        meta["status_text"] = status_txt
        return "NO_GAMES", meta
    if _rows_present():
        return "READY", meta

    # Still nothing; capture debug info
    try:
        meta["url"] = page.url
    except Exception:
        pass
    try:
        meta["title"] = page.title()
    except Exception:
        pass
    try:
        meta["body_snippet"] = (page.locator("body").inner_text() or "")[:300]
    except Exception:
        pass
    return "SKIP_NOT_READY", meta


def _collect_game_links(page, debug: bool = False) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
    """
    Collect single-game links from the spreads table.
    Returns (links, debug_meta) where links carry url/away/home/matchup_hint.
    """
    dbg: Dict[str, object] = {
        "href_via_anchor": 0,
        "href_via_click": 0,
        "sample_urls": [],
    }
    count, scrolls = _scroll_for_links(page)
    dbg["scrolls"] = scrolls

    anchors = page.locator(GAME_ANCHOR_SELECTOR)
    detail_links = page.locator(DETAIL_LINK_SELECTOR)
    dbg["anchor_count"] = anchors.count()
    dbg["detail_link_count"] = detail_links.count()

    seen = set()
    links: List[Dict[str, object]] = []

    def _row_for(node: Locator):
        row = node.locator("xpath=ancestor::div[.//div[contains(@class,'games-table-column__team-cell')]][1]")
        if row.count() == 0:
            row = node.locator("xpath=ancestor::*[contains(@class,'games-table-row') or contains(@class,'games-table')][1]")
        return row

    def _add_link(href: Optional[str], node: Locator):
        if not href:
            return
        full_url = href if href.startswith("http") else urljoin("https://betql.co", href)
        canon_url = full_url.split("#", 1)[0]
        if canon_url in seen:
            return
        seen.add(canon_url)
        row = _row_for(node)
        cells = row.locator(TEAM_CELL_SELECTOR)
        away_abbr = home_abbr = away_name = home_name = None
        if cells.count() >= 2:
            away_abbr, _, away_name, _ = parse_teams_from_team_cell(cells.nth(0))
            _, home_abbr, _, home_name = parse_teams_from_team_cell(cells.nth(1))
        away = away_abbr or away_name
        home = home_abbr or home_name
        matchup_hint = f"{away}@{home}" if away and home else None
        links.append(
            {
                "url": canon_url,
                "away_team": away.upper() if isinstance(away, str) else away,
                "home_team": home.upper() if isinstance(home, str) else home,
                "matchup_hint": matchup_hint,
            }
        )
        if debug and len(dbg["sample_urls"]) < 2:
            dbg["sample_urls"].append(canon_url)

    # Priority: explicit anchors
    for i in range(anchors.count()):
        node = anchors.nth(i)
        href = None
        try:
            href = node.get_attribute("href")
        except Exception:
            href = None
        _add_link(href, node)

    # Fallback: detail link nodes
    for i in range(detail_links.count()):
        node = detail_links.nth(i)
        href = None
        try:
            anchor = node.locator("xpath=ancestor::a[1]")
            if anchor.count() > 0:
                href = anchor.first.get_attribute("href")
                if href:
                    dbg["href_via_anchor"] += 1
        except Exception:
            href = None
        if not href:
            try:
                href = node.get_attribute("href")
            except Exception:
                href = None
        if not href:
            current_url = page.url
            try:
                with page.expect_navigation(wait_until="domcontentloaded", timeout=6000):
                    node.click(force=True)
            except Exception:
                try:
                    node.click(force=True)
                    page.wait_for_timeout(500)
                except Exception:
                    pass
            new_url = page.url
            if new_url != current_url and "/nba/game-predictions/" in new_url:
                href = new_url
                dbg["href_via_click"] += 1
            elif "/nba/game-predictions/" in new_url:
                href = new_url
                dbg["href_via_click"] += 1
            try:
                page.goto(current_url, wait_until="domcontentloaded")
            except Exception:
                try:
                    page.wait_for_timeout(500)
                except Exception:
                    pass
        _add_link(href, node)

    dbg["links"] = len(links)
    return links, dbg


def _day_meta(day: date) -> Tuple[str, str]:
    event_start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc).isoformat()
    day_key = f"NBA:{day.year:04d}:{day.month:02d}:{day.day:02d}"
    return day_key, event_start


def _process_game(
    sess: BetQLSession,
    game_page,
    game_url: str,
    day_key: str,
    event_start_time_utc: str,
    matchup_hint: Optional[str],
    away_team: Optional[str],
    home_team: Optional[str],
    retries: int,
    debug: bool,
    rolling_times: List[int],
    strict_ready: bool,
) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
    attempt = 0
    last_error: Optional[Exception] = None
    dbg: Dict[str, object] = {}
    while attempt < retries:
        attempt += 1
        total_start = time.time()
        nav_start = time.time()
        page = game_page
        created_page = False
        if page is None:
            try:
                page = sess.context.new_page() if sess.context else None
                if page:
                    _block_heavy_requests(page)
                    created_page = True
            except Exception:
                page = None
        # Use the new navigation flow that clicks "See full game analysis"
        # to get to the model props (not just raw prop lines)
        nav_dbg: Dict[str, object] = {}
        if page:
            nav_dbg = _navigate_to_game_props(page, game_url, debug=debug)
        nav_ms = int((time.time() - nav_start) * 1000)
        dbg["nav_strategy"] = nav_dbg.get("strategy")
        dbg["full_analysis_clicked"] = nav_dbg.get("full_analysis_clicked")

        try:
            ready_start = time.time()
            # Wait for prop bet elements (model picks use div.prop-bet)
            if page:
                try:
                    page.wait_for_selector("div.prop-bet, div.player-props", timeout=8000)
                except Exception:
                    # Fallback: scroll more to find props
                    for _ in range(5):
                        page.mouse.wheel(0, 800)
                        page.wait_for_timeout(300)
                        if page.locator("div.prop-bet").count() > 0:
                            break
            ready_ms = int((time.time() - ready_start) * 1000)
        except Exception:
            ready_ms = int((time.time() - ready_start) * 1000) if "ready_start" in locals() else nav_ms
        try:
            if page is None:
                raise RuntimeError("game page unavailable")
            records, meta = extract_game_props(
                page=page,
                game_url=game_url,
                day_key=day_key,
                event_start_time_utc=event_start_time_utc,
                matchup_hint=matchup_hint,
                away_team=away_team,
                home_team=home_team,
                debug=debug,
                navigate=False,
            )
            extract_ms = meta.get("elapsed_ms", 0)
            total_ms = int((time.time() - total_start) * 1000)
            dbg = {
                **meta,
                "nav_ms": nav_ms,
                "ready_ms": ready_ms,
                "extract_ms": extract_ms,
                "total_ms": total_ms,
            }
            rolling_times.append(total_ms)
            if len(rolling_times) > 20:
                rolling_times.pop(0)
            if debug:
                avg_ms = sum(rolling_times) / max(1, len(rolling_times))
                logger.debug(
                    "[game-props] game=%s rows=%s strategy=%s full_analysis=%s nav_ms=%s ready_ms=%s extract_ms=%s total_ms=%s avg_ms=%.1f",
                    game_url,
                    len(records),
                    dbg.get("nav_strategy"),
                    dbg.get("full_analysis_clicked"),
                    nav_ms,
                    ready_ms,
                    extract_ms,
                    total_ms,
                    avg_ms,
                )
            dbg = meta
            dbg["attempt"] = attempt
            dbg["nav_ms"] = nav_ms
            dbg["ready_ms"] = ready_ms
            dbg["extract_ms"] = extract_ms
            dbg["total_ms"] = total_ms
            return records, dbg
        except Exception as exc:
            last_error = exc
            dbg["error"] = str(exc)
            if debug:
                logger.exception("[game-props] attempt %s failed for %s", attempt, game_url)
        finally:
            if created_page and page:
                try:
                    page.close()
                except Exception:
                    pass
    if last_error and debug:
        logger.error("[game-props] giving up on %s after %s attempts: %s", game_url, retries, last_error)
    return [], dbg


def process_day(
    sess: BetQLSession,
    spreads_page,
    target_date: date,
    max_games_per_day: Optional[int],
    retries: int,
    debug: bool,
    strict_ready: bool,
    seen_game_urls: Optional[Set[str]] = None,
    filter_url_date: bool = False,
) -> Tuple[Dict[str, object], List[Dict[str, object]]]:
    try:
        info = switch_date(
            spreads_page,
            target_date,
            logger=logger,
            expected_url_substrings=["/_next/data/", "/nba/odds"],
        )
    except Exception as exc:
        return {"status": "TIMEOUT_NOT_READY", "rows": 0, "date": target_date.isoformat(), "error": str(exc)}, []

    # Pure DOM-based fast gate
    try:
        status_loc = spreads_page.locator(NO_EVENTS_SELECTOR)
        status_loc.wait_for(state="visible", timeout=1500)
        status_text = (status_loc.inner_text() or "").lower()
        if "no events" in status_text:
            info.update({"status": "NO_GAMES", "rows": 0, "links": {"detail_links_after_scroll": 0}, "dbg": {"status_text": status_text}})
            logger.info("[game-props] %s NO_GAMES (status)", target_date.isoformat())
            return info, []
    except Exception:
        status_text = None

    selector_counts: Dict[str, Optional[int]] = {}

    def _update_counts():
        for name, sel in [("team_cell_count", TEAM_CELL_SELECTOR), ("anchor_count", GAME_ANCHOR_SELECTOR), ("detail_link_count", DETAIL_LINK_SELECTOR)]:
            try:
                selector_counts[name] = spreads_page.locator(sel).count()
            except Exception:
                selector_counts[name] = None

    ready = False
    for pass_idx in range(2):
        try:
            spreads_page.wait_for_timeout(50)
            spreads_page.locator(f"{TEAM_CELL_SELECTOR}, {GAME_ANCHOR_SELECTOR}, {DETAIL_LINK_SELECTOR}").wait_for(state="attached", timeout=2500)
        except Exception:
            pass
        _update_counts()
        if ((selector_counts.get("team_cell_counts") or 0) > 0
            or (selector_counts.get("anchor_count") or 0) > 0
            or (selector_counts.get("detail_link_count") or 0) > 0):
            ready = True
            break
        try:
            spreads_page.mouse.wheel(0, 2000)
        except Exception:
            try:
                spreads_page.keyboard.press("End")
            except Exception:
                pass
        spreads_page.wait_for_timeout(300)

    if not ready:
        try:
            url = spreads_page.url
        except Exception:
            url = None
        try:
            title = spreads_page.title()
        except Exception:
            title = None
        try:
            body_snippet = (spreads_page.locator("body").inner_text() or "")[:300]
        except Exception:
            body_snippet = None
        meta = {
            "status": "SKIP_NOT_READY",
            "rows": 0,
            "links": selector_counts,
            "dbg": {"status_text": status_text, "url": url, "title": title, "body_snippet": body_snippet},
        }
        meta.update(info)
        logger.info(
            "[game-props] %s SKIP_NOT_READY (gate) team_cells=%s anchors=%s detail_links=%s",
            target_date.isoformat(),
            selector_counts.get("team_cell_count"),
            selector_counts.get("anchor_count"),
            selector_counts.get("detail_link_count"),
        )
        return meta, []

    spreads_page.wait_for_timeout(250)
    day_key, event_start = _day_meta(target_date)

    links, links_dbg = _collect_game_links(spreads_page, debug=debug)
    info["links"] = links_dbg

    skipped_seen = 0
    skipped_other_date = 0
    unique_links: List[Dict[str, object]] = []
    for link in links:
        canon_url = link["url"].split("#", 1)[0]
        url_day = _extract_mmddyyyy_from_url(canon_url) if filter_url_date else None
        if filter_url_date and url_day and url_day != target_date:
            skipped_other_date += 1
            continue
        if seen_game_urls is not None:
            if canon_url in seen_game_urls:
                skipped_seen += 1
                continue
            seen_game_urls.add(canon_url)
        link = dict(link)
        link["url"] = canon_url
        unique_links.append(link)
    links = unique_links
    info["links"]["skipped_seen"] = skipped_seen
    info["links"]["skipped_other_date"] = skipped_other_date
    info["links"]["unique_games"] = len(links)

    if max_games_per_day is not None:
        links = links[:max_games_per_day]
        info["links"]["capped"] = max_games_per_day

    if len(links) == 0:
        info.update({"status": "NO_GAMES", "rows": 0})
        return info, []

    all_records: List[Dict[str, object]] = []
    rolling_times: List[int] = []
    game_page = None
    try:
        game_page = sess.context.new_page() if sess.context else None
        if game_page:
            _block_heavy_requests(game_page)
    except Exception:
        game_page = None

    per_game: List[Dict[str, object]] = []
    for link in links:
        url = link["url"]
        recs, dbg = _process_game(
            sess=sess,
            game_page=game_page,
            game_url=url,
            day_key=day_key,
            event_start_time_utc=event_start,
            matchup_hint=link.get("matchup_hint"),
            away_team=link.get("away_team"),
            home_team=link.get("home_team"),
            retries=retries,
            debug=debug,
            rolling_times=rolling_times,
            strict_ready=strict_ready,
        )
        for rec in recs:
            rec["day_key"] = rec.get("day_key") or day_key
            rec["event_start_time_utc"] = rec.get("event_start_time_utc") or event_start
        all_records.extend(recs)
        per_game.append({"url": url, "rows": len(recs), "dbg": dbg})
    try:
        if game_page:
            game_page.close()
    except Exception:
        pass

    info.update(
        {
            "status": "OK" if len(all_records) > 0 else "NO_ROWS",
            "rows": len(all_records),
            "games": per_game,
        }
    )
    return info, all_records


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill BetQL game-page player props history.")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD start date")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD end date")
    parser.add_argument("--storage", default="data/betql_storage_state.json", help="Playwright storage state path")
    parser.add_argument("--headless", action=argparse.BooleanOptionalAction, default=True, help="Run headless browser")
    parser.add_argument("--dry-run", action="store_true", help="Do not write files")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing output files")
    parser.add_argument("--max-games-per-day", type=int, default=None, help="Optional cap on games per day")
    parser.add_argument("--retries", type=int, default=3, help="Per-game retries")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--out-root", default="data/history/betql_game_props_raw", help="Root folder for raw JSONL output")
    parser.add_argument("--strict-ready", action="store_true", help="Use full wait_for_ready for game pages")
    parser.add_argument(
        "--filter-url-date",
        action="store_true",
        help="Skip game URLs whose embedded date does not match the target day (avoids cross-day overlap).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    require_storage_state(args.storage)
    start = parse_date(args.start)
    end = parse_date(args.end)
    days = date_range(start, end)
    seen_game_urls: Set[str] = set()

    # Filter out already-scraped dates unless --overwrite
    out_root = Path(args.out_root)
    days_to_scrape = days
    if not args.overwrite:
        days_to_scrape = [d for d in days if not (out_root / f"{d.isoformat()}.jsonl").exists()]
        skipped = len(days) - len(days_to_scrape)
        if skipped > 0:
            logger.info("Skipping %s already-scraped dates, %s remaining", skipped, len(days_to_scrape))

    if not days_to_scrape:
        logger.info("All dates already scraped, nothing to do")
        return

    # Estimate time (props are slower - ~60s per date with multiple games)
    est_minutes = len(days_to_scrape) * 60 / 60
    logger.info(
        "Starting game props backfill: %s dates to scrape (est. %.1f min)",
        len(days_to_scrape), est_minutes
    )
    backfill_start = time.time()
    total_rows = 0
    dates_completed = 0

    with BetQLSession(storage_state_path=args.storage, headless=args.headless) as sess:
        spreads_page = sess.open(URL_SPREADS)
        wait_for_model_ready_light(spreads_page)
        for i, day in enumerate(days_to_scrape):
            out_path = out_root / f"{day.isoformat()}.jsonl"
            try:
                meta, rows = process_day(
                    sess=sess,
                    spreads_page=spreads_page,
                    target_date=day,
                    max_games_per_day=args.max_games_per_day,
                    retries=args.retries,
                    debug=args.debug,
                    strict_ready=args.strict_ready,
                    seen_game_urls=seen_game_urls,
                    filter_url_date=args.filter_url_date,
                )
            except Exception as exc:
                meta, rows = (
                    {"status": "TIMEOUT_NOT_READY", "rows": 0, "date": day.isoformat(), "error": str(exc)},
                    [],
                )
                logger.warning("[game-props] failed processing %s: %s", day.isoformat(), exc)
            progress = f"[{i+1}/{len(days_to_scrape)}]"
            logger.info(
                "%s date=%s status=%s rows=%s games=%s",
                progress,
                day.isoformat(),
                meta.get("status"),
                meta.get("rows"),
                len(meta.get("games", [])),
            )
            if args.dry_run:
                continue
            if len(rows) == 0 and not args.overwrite:
                logger.info("no rows for %s; skipping write (use --overwrite to force empty file)", day.isoformat())
                continue
            write_jsonl_atomic(out_path, rows)
            total_rows += len(rows)
            dates_completed += 1

        try:
            spreads_page.close()
        except Exception:
            pass

    elapsed = time.time() - backfill_start
    avg_per_date = elapsed / dates_completed if dates_completed > 0 else 0
    logger.info(
        "Backfill complete: %s rows across %s dates in %.1fs (%.1fs avg per date)",
        total_rows, dates_completed, elapsed, avg_per_date
    )


if __name__ == "__main__":
    main()
