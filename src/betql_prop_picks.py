from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

from playwright.sync_api import Page

from store import sha256_json

CANONICAL_URL = "https://betql.co/nba/prop-picks"
GAME_CARD_SELECTOR = "div.carousel-track div.carousel-pane > button"
ACTIVE_GAME_SELECTOR = "div.team-player-props-head .team-name"
PLAYER_CARD_SELECTOR = "div.player-props"
PLAYER_NAME_SELECTOR = ".player-name > div"
BET_ROW_SELECTOR = "div.prop-bet"
BET_TEXT_SELECTOR = "div.prop-bet-text"
TEAM_ABBREV_SELECTOR = "div.team-abbrev"
SCROLL_RIGHT_SELECTOR = "button[aria-label='Player Props Page Scroll Right']"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _text_or_none(locator) -> Optional[str]:
    try:
        txt = locator.inner_text()
        return txt.strip()
    except Exception:
        return None


def _count_gold_stars(container_locator) -> Optional[int]:
    """
    Count gold stars inside a container. Returns None if stars are absent.
    """
    try:
        fills: List[str] = container_locator.evaluate(
            """(node)=>{
                const paths = node.querySelectorAll('svg path');
                const fills = [];
                paths.forEach(p=>{
                    const fill = getComputedStyle(p).fill || '';
                    fills.push(fill);
                });
                return fills;
            }"""
        )
    except Exception:
        return None
    gold = sum(1 for f in fills if f and f.strip().lower() == "rgb(255, 204, 1)")
    gold = max(0, min(5, gold))
    return gold if fills else None


def _parse_bet_text(bet_text: str) -> Tuple[Optional[str], Optional[float], Optional[str], Optional[int]]:
    """
    Returns (direction, line, stat, odds) from a bet text like 'Over 12.5 Points (-120)'.
    """
    if not bet_text:
        return None, None, None, None
    txt = bet_text.replace("½", ".5")
    m = re.match(
        r"(?i)\s*(over|under)\s+([0-9]+(?:\.[0-9]+)?)\s+([^(]+?)\s*\(([-+]?\d+)\)",
        txt.strip(),
    )
    if not m:
        return None, None, None, None
    direction = m.group(1).upper()
    try:
        line = float(m.group(2))
    except ValueError:
        line = None
    stat = m.group(3).strip()
    try:
        odds = int(m.group(4))
    except ValueError:
        odds = None
    return direction, line, stat, odds


def _game_matchup_hint(card_locator) -> Optional[str]:
    abbrs: List[str] = []
    try:
        items = card_locator.locator(TEAM_ABBREV_SELECTOR)
        for i in range(items.count()):
            txt = _text_or_none(items.nth(i))
            if txt:
                abbrs.append(txt.upper())
    except Exception:
        return None
    if len(abbrs) >= 2:
        return f"{abbrs[0]}@{abbrs[1]}"
    return None


def _active_team_name(page: Page) -> Optional[str]:
    try:
        return _text_or_none(page.locator(ACTIVE_GAME_SELECTOR).first)
    except Exception:
        return None


def _scrape_visible_props(
    page: Page,
    canonical_url: str,
    observed_at: str,
    matchup_hint: Optional[str],
    team_name: Optional[str],
    dedupe: Set[Tuple[Optional[str], Optional[str], str]],
    debug_label: Optional[str] = None,
) -> List[Dict[str, object]]:
    records: List[Dict[str, object]] = []
    player_cards = page.locator(PLAYER_CARD_SELECTOR)
    for p_idx in range(player_cards.count()):
        card = player_cards.nth(p_idx)
        player_name = _text_or_none(card.locator(PLAYER_NAME_SELECTOR).first)
        bet_rows = card.locator(BET_ROW_SELECTOR)
        if debug_label is not None:
            print(f"[DEBUG] card={debug_label} player_idx={p_idx} bet_rows={bet_rows.count()}")
        for r_idx in range(bet_rows.count()):
            row = bet_rows.nth(r_idx)
            bet_text = _text_or_none(row.locator(BET_TEXT_SELECTOR).first)
            if not bet_text:
                continue
            dedupe_key = (matchup_hint, player_name, bet_text)
            if dedupe_key in dedupe:
                continue
            dedupe.add(dedupe_key)

            direction, line, stat, odds = _parse_bet_text(bet_text)
            rating_stars = _count_gold_stars(row.locator("div.prop-stars"))
            raw_pick_text = f"{player_name} {bet_text}".strip() if player_name else bet_text

            rec: Dict[str, object] = {
                "source_id": "betql",
                "source_surface": "betql_prop_picks",
                "sport": "NBA",
                "market_family": "player_prop",
                "canonical_url": canonical_url,
                "observed_at_utc": observed_at,
                "matchup_hint": matchup_hint,
                "team_name": team_name,
                "player_name": player_name,
                "bet_text": bet_text,
                "raw_pick_text": raw_pick_text,
                "direction": direction,
                "line": line,
                "stat": stat,
                "odds": odds,
                "rating_stars": rating_stars,
                "raw_block": f"{matchup_hint or ''} {player_name or ''} {bet_text}",
                "raw_fingerprint": None,
            }
            rec["raw_fingerprint"] = sha256_json({k: v for k, v in rec.items() if k != "raw_fingerprint"})
            records.append(rec)
    return records


def extract_prop_picks(page: Page, canonical_url: str = CANONICAL_URL, debug: bool = False) -> List[Dict[str, object]]:
    """
    Extract prop picks across all games on the prop-picks page using the carousel controls.
    Assumes the page is already loaded and authenticated.
    """
    observed = _now_iso()
    records: List[Dict[str, object]] = []
    seen_rows: Set[Tuple[Optional[str], Optional[str], str]] = set()
    visited_cards: Set[str] = set()
    max_scrolls = 30  # safety against infinite carousel loops

    # Ensure key elements exist before looping.
    page.wait_for_selector(GAME_CARD_SELECTOR, state="attached", timeout=20000)
    page.wait_for_selector(ACTIVE_GAME_SELECTOR, state="attached", timeout=20000)

    scroll_attempts = 0
    while True:
        cards = page.locator(GAME_CARD_SELECTOR)
        card_count = cards.count()
        for idx in range(card_count):
            card = cards.nth(idx)
            matchup_hint = _game_matchup_hint(card)
            card_key = matchup_hint or f"card-{idx}-{_text_or_none(card)}"
            if card_key in visited_cards:
                continue
            visited_cards.add(card_key)

            prev_active = _active_team_name(page) or ""
            card.click()
            page.wait_for_function(
                """(sel, prev)=>{const el=document.querySelector(sel); return el && el.textContent.trim() !== prev;}""",
                arg=(ACTIVE_GAME_SELECTOR, prev_active),
                timeout=15000,
            )
            page.wait_for_selector(PLAYER_CARD_SELECTOR, state="attached", timeout=15000)
            team_name = _active_team_name(page)
            recs = _scrape_visible_props(
                page,
                canonical_url=canonical_url,
                observed_at=observed,
                matchup_hint=matchup_hint,
                team_name=team_name,
                dedupe=seen_rows,
                debug_label=f"{idx}:{matchup_hint}" if debug else None,
            )
            records.extend(recs)
            if debug:
                print(f"[DEBUG] processed card idx={idx} matchup={matchup_hint} rows={len(recs)}")

        # attempt to scroll right for more games
        scroll_attempts += 1
        arrow = page.locator(SCROLL_RIGHT_SELECTOR)
        if scroll_attempts > max_scrolls or arrow.count() == 0:
            break
        disabled = False
        try:
            disabled = arrow.first.is_disabled()
        except Exception:
            disabled = False
        first_before = _text_or_none(cards.first) if card_count else None
        arrow.first.click()
        page.wait_for_timeout(400)
        cards = page.locator(GAME_CARD_SELECTOR)
        first_after = _text_or_none(cards.first) if cards.count() else None
        if disabled or first_after == first_before:
            break

    return records
