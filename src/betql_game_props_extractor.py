from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlsplit, urlunsplit

from playwright.sync_api import Page

from src.betql_prop_picks import (
    BET_ROW_SELECTOR,
    BET_TEXT_SELECTOR,
    PLAYER_CARD_SELECTOR,
    PLAYER_NAME_SELECTOR,
    TEAM_ABBREV_SELECTOR,
    _count_gold_stars,
    _iter_team_sections,
    _parse_bet_text,
    _text_or_none,
)
from store import sha256_json

logger = logging.getLogger(__name__)

PROPS_ROOT_SELECTOR = "#props, div.player-props"
DEFAULT_LOCATOR_TIMEOUT_MS = 1500
DEFAULT_ATTR_TIMEOUT_MS = 800


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_url(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit(parts._replace(fragment=""))


def safe_text(locator, timeout_ms: int = DEFAULT_ATTR_TIMEOUT_MS) -> Optional[str]:
    try:
        return locator.first.text_content(timeout=timeout_ms).strip()
    except Exception:
        return None


def safe_attr(locator, name: str, timeout_ms: int = DEFAULT_ATTR_TIMEOUT_MS) -> Optional[str]:
    try:
        return locator.first.get_attribute(name, timeout=timeout_ms)
    except Exception:
        return None


def _detect_matchup(page: Page, away_hint: Optional[str], home_hint: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    away = away_hint.upper() if isinstance(away_hint, str) else away_hint
    home = home_hint.upper() if isinstance(home_hint, str) else home_hint
    try:
        teams = page.locator(TEAM_ABBREV_SELECTOR)
        if teams.count() >= 1 and not away:
            txt = _text_or_none(teams.nth(0))
            away = txt.upper() if txt else away
        if teams.count() >= 2 and not home:
            txt = _text_or_none(teams.nth(1))
            home = txt.upper() if txt else home
    except Exception:
        pass
    return away, home


def _scroll_props(page: Page, selector: str = BET_ROW_SELECTOR, max_scrolls: int = 15, poll_ms: int = 250, max_total_wait_s: float = 3.0) -> Tuple[int, int, Dict[str, int]]:
    """
    Scroll the page until the number of matching nodes stabilizes or max_scrolls reached.
    Uses short polling instead of long sleeps. Returns (final_count, scrolls_performed, dbg).
    """
    last_count = 0
    stable_ticks = 0
    scrolls = 0
    max_rows_seen = 0
    wait_ms_total = 0
    dbg: Dict[str, int] = {}

    def current_count() -> int:
        try:
            return page.locator(selector).count()
        except Exception:
            return 0

    for _ in range(max_scrolls):
        count = current_count()
        max_rows_seen = max(max_rows_seen, count)
        if count > last_count:
            last_count = count
            stable_ticks = 0
        else:
            stable_ticks += 1
        if stable_ticks >= 3 and last_count > 0:
            break
        scrolls += 1
        try:
            page.mouse.wheel(0, 1400)
        except Exception:
            try:
                page.keyboard.press("End")
            except Exception:
                pass
        # short poll loop to wait for growth, capped
        growth = False
        deadline = time.time() + 2.5  # seconds
        while time.time() < deadline and wait_ms_total < max_total_wait_s * 1000:
            page.wait_for_timeout(min(poll_ms, 500))
            wait_ms_total += min(poll_ms, 500)
            new_count = current_count()
            max_rows_seen = max(max_rows_seen, new_count)
            if new_count > last_count:
                last_count = new_count
                stable_ticks = 0
                growth = True
                break
        if not growth:
            stable_ticks += 1
            if wait_ms_total >= max_total_wait_s * 1000 and logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "[betql-game-props] scroll wait cap hit rows=%s max_rows=%s stable_ticks=%s",
                    last_count,
                    max_rows_seen,
                    stable_ticks,
                )
    dbg["scroll_steps"] = scrolls
    dbg["max_rows"] = max_rows_seen
    dbg["stable_ticks"] = stable_ticks
    dbg["wait_ms_total"] = wait_ms_total
    return last_count, scrolls, dbg


def _evaluate_rows(page: Page) -> List[Dict[str, Optional[str]]]:
    """
    Extract prop rows in one DOM evaluate call to avoid per-row locator timeouts.
    """
    return page.evaluate(
        """
        (rootSelector) => {
            const root = document.querySelector(rootSelector) || document;
            const rows = Array.from(root.querySelectorAll(arguments[0] || "div.games-table-column__player-props-container div.prop-row"));
            return rows.map(r => {
                const text = (el) => (el && el.textContent ? el.textContent.trim() : null);
                return {
                    player: text(r.querySelector("div.prop-row__player-name")),
                    bet_text: text(r.querySelector("div.prop-row__bet-text")) || text(r.querySelector("div.prop-row__bet")),
                    odds: text(r.querySelector("div.prop-row__odds")),
                    stars: text(r.querySelector("div.prop-stars")),
                };
            });
        }
        """,
        PROPS_ROOT_SELECTOR,
    )


def _goto_props(page: Page, game_url: str, debug: bool = False, navigate: bool = True) -> Dict[str, object]:
    dbg: Dict[str, object] = {"navigated_hash": False, "anchor_present": False, "scrolled_anchor": False, "nudges": 0}
    target_url = game_url if "#props" in str(game_url) else f"{game_url}#props"
    if navigate:
        try:
            page.goto(target_url, wait_until="domcontentloaded", timeout=20000)
            dbg["navigated_hash"] = True
        except Exception as exc:
            if debug:
                logger.debug("[betql-game-props] hash nav failed: %s", exc)
    try:
        anchor = page.locator("#props")
        dbg["anchor_present"] = anchor.count() > 0
        if dbg["anchor_present"]:
            anchor.first.scroll_into_view_if_needed(timeout=2000)
            dbg["scrolled_anchor"] = True
        else:
            for _ in range(3):
                try:
                    page.mouse.wheel(0, 800)
                    dbg["nudges"] += 1
                except Exception:
                    break
                page.wait_for_timeout(200)
    except Exception:
        pass
    return dbg


def _scrape_props_on_page(
    page: Page,
    canonical_url: str,
    observed_at: str,
    day_key: str,
    event_start_time_utc: str,
    matchup_hint: Optional[str],
    away_team: Optional[str],
    home_team: Optional[str],
    dedupe: Set[Tuple[Optional[str], Optional[str], str]],
    source_surface: str,
    debug_label: Optional[str] = None,
) -> List[Dict[str, object]]:
    records: List[Dict[str, object]] = []
    player_cards = page.locator(PLAYER_CARD_SELECTOR)
    team_sections = list(_iter_team_sections(page, away_team, home_team, debug=bool(debug_label)))
    for section_idx, (team_abbrev, team_side, team_name, card_indexes) in enumerate(team_sections):
        for card_idx in card_indexes:
            card = player_cards.nth(card_idx)
            player_name = safe_text(card.locator(PLAYER_NAME_SELECTOR))
            bet_rows = card.locator(BET_ROW_SELECTOR)
            for row_idx in range(bet_rows.count()):
                row = bet_rows.nth(row_idx)
                bet_text = safe_text(row.locator(BET_TEXT_SELECTOR))
                if not bet_text:
                    continue
                dedupe_key = (matchup_hint, player_name, bet_text)
                if dedupe_key in dedupe:
                    continue
                dedupe.add(dedupe_key)

                direction, line, stat, odds = _parse_bet_text(bet_text)
                stat_raw = stat.strip() if stat else None
                rating_stars = _count_gold_stars(row.locator("div.prop-stars"))
                raw_pick_text = f"{player_name or ''} {bet_text}".strip()
                raw_block = f"{matchup_hint or ''} {player_name or ''} {bet_text}".strip()

                rec: Dict[str, object] = {
                    "source_id": "betql",
                    "source_surface": source_surface,
                    "sport": "NBA",
                    "market_family": "player_prop",
                    "canonical_url": canonical_url,
                    "observed_at_utc": observed_at,
                    "event_start_time_utc": event_start_time_utc,
                    "day_key": day_key,
                    "matchup_hint": matchup_hint,
                    "away_team": away_team,
                    "home_team": home_team,
                    "team_name": team_name,
                    "team_panel_abbrev": team_abbrev,
                    "team_panel_side": team_side,
                    "player_team": team_abbrev,
                    "player_team_side": team_side,
                    "player_name": player_name,
                    "bet_text": bet_text,
                    "raw_pick_text": raw_pick_text,
                    "direction": direction,
                    "line": line,
                    "stat": stat,
                    "stat_raw": stat_raw,
                    "odds": odds,
                    "rating_stars": rating_stars,
                    "raw_block": raw_block,
                    "raw_fingerprint": None,
                }
                fingerprint_payload = {
                    "canonical_url": canonical_url,
                    "raw_pick_text": raw_pick_text,
                    "matchup_hint": matchup_hint,
                    "player_name": player_name,
                    "bet_text": bet_text,
                    "stat_raw": stat_raw,
                    "direction": direction,
                    "line": line,
                }
                rec["raw_fingerprint"] = sha256_json(fingerprint_payload)
                records.append(rec)
                if debug_label:
                    logger.debug(
                        "[betql-game-props] %s section=%s card=%s row=%s player=%s bet=%s",
                        debug_label,
                        section_idx,
                        card_idx,
                        row_idx,
                        player_name,
                        bet_text,
                    )
    return records


def extract_game_props(
    page: Page,
    game_url: str,
    day_key: str,
    event_start_time_utc: str,
    matchup_hint: Optional[str] = None,
    away_team: Optional[str] = None,
    home_team: Optional[str] = None,
    source_surface: str = "betql_game_prop",
    debug: bool = False,
    navigate: bool = True,
) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
    """
    Extract player props from a BetQL single-game page.
    Returns (records, debug_meta).
    """
    start_ts = time.time()
    observed = _now_iso()
    canonical_url = _canonical_url(game_url)
    dbg: Dict[str, object] = {"goto": None, "scrolls": 0, "bet_rows": 0}

    try:
        page.set_default_timeout(DEFAULT_LOCATOR_TIMEOUT_MS)
        page.set_default_navigation_timeout(5000)
    except Exception:
        pass

    nav_start = time.time()
    if navigate:
        try:
            page.goto(game_url if "#props" in str(game_url) else f"{game_url}#props", wait_until="domcontentloaded", timeout=20000)
        except Exception:
            pass
    nav_ms = int((time.time() - nav_start) * 1000)
    dbg["goto"] = _goto_props(page, game_url, debug=debug, navigate=navigate)
    dbg["nav_ms"] = nav_ms

    away_team, home_team = _detect_matchup(page, away_team, home_team)
    if not matchup_hint and away_team and home_team:
        matchup_hint = f"{away_team}@{home_team}"

    bet_rows, scrolls, scroll_dbg = _scroll_props(page)
    dbg["bet_rows"] = bet_rows
    dbg["scrolls"] = scrolls
    dbg.update({k: v for k, v in scroll_dbg.items() if k in {"scroll_steps", "max_rows", "stable_ticks", "wait_ms_total"}})

    dedupe: Set[Tuple[Optional[str], Optional[str], str]] = set()
    records = _scrape_props_on_page(
        page=page,
        canonical_url=canonical_url,
        observed_at=observed,
        day_key=day_key,
        event_start_time_utc=event_start_time_utc,
        matchup_hint=matchup_hint,
        away_team=away_team,
        home_team=home_team,
        dedupe=dedupe,
        source_surface=source_surface,
        debug_label="game" if debug else None,
    )
    dbg["records"] = len(records)
    dbg["elapsed_ms"] = int((time.time() - start_ts) * 1000)
    if debug:
        logger.debug(
            "[betql-game-props] extracted url=%s anchor=%s rows=%s elapsed_ms=%s",
            canonical_url,
            dbg.get("goto", {}).get("anchor_present"),
            len(records),
            dbg["elapsed_ms"],
        )
    return records, dbg
