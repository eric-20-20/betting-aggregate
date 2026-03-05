from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

from playwright.sync_api import Page

from store import sha256_json
from utils import normalize_text

logger = logging.getLogger(__name__)

CANONICAL_URL = "https://betql.co/nba/prop-picks"
GAME_CARD_SELECTOR = "div.carousel-track div.carousel-pane > button"
ACTIVE_GAME_SELECTOR = "div.team-player-props-head .team-name"
PLAYER_CARD_SELECTOR = "div.player-props"
PLAYER_NAME_SELECTOR = ".player-name > div"
BET_ROW_SELECTOR = "div.prop-bet"
BET_TEXT_SELECTOR = "div.prop-bet-text"
TEAM_ABBREV_SELECTOR = "div.team-abbrev"
SCROLL_RIGHT_SELECTOR = "button[aria-label='Player Props Page Scroll Right']"
TEAM_PANEL_LOGO_SELECTOR = "div.team-player-props-head img[src*='/team/logo/NBA/']"

TEAM_NAME_MAP = {
    "hawks": "ATL",
    "celtics": "BOS",
    "nets": "BKN",
    "hornets": "CHA",
    "bulls": "CHI",
    "cavaliers": "CLE",
    "cavs": "CLE",
    "mavericks": "DAL",
    "nuggets": "DEN",
    "pistons": "DET",
    "warriors": "GSW",
    "rockets": "HOU",
    "pacers": "IND",
    "clippers": "LAC",
    "lakers": "LAL",
    "grizzlies": "MEM",
    "heat": "MIA",
    "bucks": "MIL",
    "wolves": "MIN",
    "timberwolves": "MIN",
    "pelicans": "NOP",
    "knicks": "NYK",
    "thunder": "OKC",
    "magic": "ORL",
    "76ers": "PHI",
    "sixers": "PHI",
    "suns": "PHX",
    "blazers": "POR",
    "kings": "SAC",
    "spurs": "SAS",
    "raptors": "TOR",
    "jazz": "UTA",
    "wizards": "WAS",
}


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

    Counts the number of SVG elements that contain at least one gold-filled
    path, rather than counting individual paths (which can over-count when
    a single star icon has multiple paths with the gold fill).
    """
    try:
        result: dict = container_locator.evaluate(
            """(node)=>{
                const svgs = node.querySelectorAll('svg');
                if (!svgs.length) return {total: 0, gold: 0};
                let gold = 0;
                svgs.forEach(svg=>{
                    const paths = svg.querySelectorAll('path');
                    let hasGold = false;
                    paths.forEach(p=>{
                        const fill = getComputedStyle(p).fill || '';
                        if (fill.trim().toLowerCase() === 'rgb(255, 204, 1)') hasGold = true;
                    });
                    if (hasGold) gold++;
                });
                return {total: svgs.length, gold: gold};
            }"""
        )
    except Exception:
        return None
    total = result.get("total", 0)
    gold = result.get("gold", 0)
    gold = max(0, min(5, gold))
    return gold if total > 0 else None


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


def _game_card_teams(card_locator) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    away = home = None
    try:
        items = card_locator.locator(TEAM_ABBREV_SELECTOR)
        if items.count() >= 1:
            txt = _text_or_none(items.nth(0))
            away = txt.upper() if txt else None
        if items.count() >= 2:
            txt = _text_or_none(items.nth(1))
            home = txt.upper() if txt else None
    except Exception:
        pass
    matchup = f"{away}@{home}" if away and home else None
    return away, home, matchup


def _active_team_name(page: Page) -> Optional[str]:
    try:
        return _text_or_none(page.locator(ACTIVE_GAME_SELECTOR).first)
    except Exception:
        return None


def _team_panel_info(page: Page, away_abbrev: Optional[str], home_abbrev: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    try:
        logo = page.locator(TEAM_PANEL_LOGO_SELECTOR).first
        src = logo.get_attribute("src") or ""
        m = re.search(r"/NBA/([A-Z]{2,3})\.(?:png|webp|svg)", src)
        if not m:
            return None, None
        abbrev = m.group(1)
        side = "away" if abbrev == away_abbrev else "home" if abbrev == home_abbrev else None
        return abbrev, side
    except Exception:
        return None, None


def _team_side(abbrev: Optional[str], away_abbrev: Optional[str], home_abbrev: Optional[str]) -> Optional[str]:
    if abbrev and away_abbrev and abbrev == away_abbrev:
        return "away"
    if abbrev and home_abbrev and abbrev == home_abbrev:
        return "home"
    return None


def _header_team_abbrev(
    head_locator, away_abbrev: Optional[str], home_abbrev: Optional[str], section_index: int, debug: bool = False
) -> Tuple[Optional[str], Optional[str]]:
    team_name_txt = _text_or_none(head_locator.locator(".team-name").first)

    def logo_sources() -> List[str]:
        try:
            imgs = head_locator.locator("img")
            return [
                (imgs.nth(i).get_attribute("src") or imgs.nth(i).get_attribute("data-src") or "").strip()
                for i in range(imgs.count())
            ]
        except Exception:
            return []

    # Prefer logo src/data-src
    for src in logo_sources():
        m = re.search(r"/NBA/([A-Z]{2,4})\.(?:png|webp|svg)", src)
        if m:
            abbrev = m.group(1)
            return abbrev, team_name_txt

    # Fallback: map from team name text
    norm = normalize_text(team_name_txt or "")
    for key, val in TEAM_NAME_MAP.items():
        if key in norm.split() or key in norm:
            return val, team_name_txt

    # Fallback: section position
    if section_index == 0 and away_abbrev:
        return away_abbrev, team_name_txt
    if section_index == 1 and home_abbrev:
        return home_abbrev, team_name_txt

    if debug:
        logger.debug("[betql-props] missing abbrev section=%s text=%s logos=%s", section_index, team_name_txt, logo_sources())
    return None, team_name_txt


def _iter_team_sections(page: Page, away_abbrev: Optional[str], home_abbrev: Optional[str], debug: bool = False):
    """
    Yield per-team sections on the page: header-derived team info and the player card indexes belonging to that section.
    """
    data = page.evaluate(
        """({ headSel, cardSel }) => {
            const heads = Array.from(document.querySelectorAll(headSel));
            const cards = Array.from(document.querySelectorAll(cardSel));
            const nodes = Array.from(document.querySelectorAll(`${headSel}, ${cardSel}`));

            const groups = [];
            let current = [];

            nodes.forEach(n => {
                if (n.matches(headSel)) {
                    if (current.length) {
                        groups.push(current);
                        current = [];
                    }
                } else if (n.matches(cardSel)) {
                    const idx = cards.indexOf(n);
                    if (idx >= 0) current.push(idx);
                }
            });

            if (current.length) groups.push(current);

            return { headCount: heads.length, groups, totalCards: cards.length };
        }""",
        {
            "headSel": "div.team-player-props-head",
            "cardSel": PLAYER_CARD_SELECTOR,
        },
    )

    head_count = data.get("headCount") or 0
    groups = data.get("groups") or []
    total_cards = data.get("totalCards") or 0
    headers = page.locator("div.team-player-props-head")

    if head_count == 0:
        # Fallback: treat all cards as one section with unknown team
        yield (None, None, None, list(range(total_cards)))
        return

    for i in range(head_count):
        head = headers.nth(i)
        card_idxs = [int(x) for x in (groups[i] if i < len(groups) else list(range(total_cards)))]
        team_abbrev, team_name_txt = _header_team_abbrev(head, away_abbrev, home_abbrev, i, debug=debug)
        side = _team_side(team_abbrev, away_abbrev, home_abbrev)
        yield (team_abbrev, side, team_name_txt, card_idxs)


def _props_snapshot(page: Page) -> Dict[str, str]:
    try:
        logo = page.locator(TEAM_PANEL_LOGO_SELECTOR).first
        logo_src = (logo.get_attribute("src") or "").strip()
    except Exception:
        logo_src = ""
    try:
        name = page.locator(PLAYER_NAME_SELECTOR).first.inner_text().strip()
    except Exception:
        name = ""
    try:
        bet = page.locator(BET_TEXT_SELECTOR).first.inner_text().strip()
    except Exception:
        bet = ""
    return {"logo": logo_src, "name": name, "bet": bet}


def _has_usable_props(page: Page) -> bool:
    try:
        cards = page.locator(PLAYER_CARD_SELECTOR).count()
        bets = page.locator(BET_ROW_SELECTOR).count()
        return cards > 0 and bets > 0
    except Exception:
        return False


def _click_card_and_wait(page: Page, card, prev_snapshot: Dict[str, str], debug: bool = False) -> None:
    attempts = 0
    last_snapshot = prev_snapshot
    while attempts < 3:
        attempts += 1
        try:
            card.scroll_into_view_if_needed()
        except Exception:
            pass
        try:
            card.click(timeout=5000, force=True)
        except Exception:
            try:
                page.evaluate("(el)=>el && el.click()", card)
            except Exception:
                pass
        try:
            page.wait_for_selector(PLAYER_CARD_SELECTOR, state="attached", timeout=15000)
        except Exception:
            pass
        page.wait_for_timeout(300 + 100 * attempts)
        snap = _props_snapshot(page)
        usable = _has_usable_props(page)
        changed = (snap != prev_snapshot)
        if debug:
            try:
                pc = page.locator(PLAYER_CARD_SELECTOR).count()
                br = page.locator(BET_ROW_SELECTOR).count()
            except Exception:
                pc = br = -1
            logger.debug(
                "[betql-props] card wait attempt=%s usable=%s changed=%s pc=%s br=%s snap=%s prev=%s",
                attempts,
                usable,
                changed,
                pc,
                br,
                snap,
                prev_snapshot,
            )
        if usable and (changed or attempts >= 1):
            return
        last_snapshot = snap
    pc = page.locator(PLAYER_CARD_SELECTOR).count() if page else -1
    br = page.locator(BET_ROW_SELECTOR).count() if page else -1
    raise TimeoutError(
        f"Failed to load props after click; usable={_has_usable_props(page)} prev={prev_snapshot} last={last_snapshot} pc={pc} br={br}"
    )

def _scrape_visible_props(
    page: Page,
    canonical_url: str,
    observed_at: str,
    matchup_hint: Optional[str],
    team_name: Optional[str],
    team_abbrev: Optional[str],
    team_side: Optional[str],
    away_team_abbrev: Optional[str],
    home_team_abbrev: Optional[str],
    player_card_indexes: List[int],
    dedupe: Set[Tuple[Optional[str], Optional[str], str]],
    debug_label: Optional[str] = None,
) -> List[Dict[str, object]]:
    records: List[Dict[str, object]] = []
    player_cards = page.locator(PLAYER_CARD_SELECTOR)
    for p_idx in player_card_indexes:
        card = player_cards.nth(p_idx)
        player_name = _text_or_none(card.locator(PLAYER_NAME_SELECTOR).first)
        bet_rows = card.locator(BET_ROW_SELECTOR)
        if debug_label is not None:
            logger.debug("[betql-props] card=%s player_idx=%s bet_rows=%s", debug_label, p_idx, bet_rows.count())
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
            stat_raw = stat.strip() if stat else None
            stat = stat_raw
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
                "away_team": away_team_abbrev,
                "home_team": home_team_abbrev,
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
    page.wait_for_selector(PLAYER_CARD_SELECTOR, state="attached", timeout=20000)

    scroll_attempts = 0
    while True:
        cards = page.locator(GAME_CARD_SELECTOR)
        card_count = cards.count()
        for idx in range(card_count):
            card = cards.nth(idx)
            away_abbrev, home_abbrev, matchup_hint = _game_card_teams(card)
            card_key = matchup_hint or f"card-{idx}-{away_abbrev}-{home_abbrev}-{_text_or_none(card)}"
            if card_key in visited_cards:
                continue
            visited_cards.add(card_key)

            snapshot = _props_snapshot(page)
            _click_card_and_wait(page, card, snapshot, debug=debug)
            for sec_abbrev, sec_side, sec_team_name, card_indexes in _iter_team_sections(
                page, away_abbrev, home_abbrev, debug=debug
            ):
                recs = _scrape_visible_props(
                    page,
                    canonical_url=canonical_url,
                    observed_at=observed,
                    matchup_hint=matchup_hint,
                    team_name=sec_team_name,
                    team_abbrev=sec_abbrev,
                    team_side=sec_side,
                    away_team_abbrev=away_abbrev,
                    home_team_abbrev=home_abbrev,
                    player_card_indexes=card_indexes,
                    dedupe=seen_rows,
                    debug_label=f"{idx}:{matchup_hint}:{sec_abbrev}" if debug else None,
                )
                records.extend(recs)
                if debug:
                    logger.debug(
                        "[betql-props] processed card idx=%s matchup=%s team=%s rows=%s",
                        idx,
                        matchup_hint,
                        sec_abbrev,
                        len(recs),
                    )

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
