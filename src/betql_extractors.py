from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from playwright.sync_api import Page

from store import sha256_json


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _common_fields(source_surface: str, canonical_url: str, observed: str) -> Dict[str, object]:
    return {
        "source_id": "betql",
        "source_surface": source_surface,
        "sport": "NBA",
        "observed_at_utc": observed,
        "canonical_url": canonical_url,
        "player_hint": None,
        "stat_hint": None,
        "line_hint": None,
        "odds_hint": None,
        "expert_handle": None,
        "expert_profile": None,
        "expert_slug": None,
        "tailing_handle": None,
    }


def _count_gold_stars(button_locator) -> Tuple[Optional[int], Optional[List[str]]]:
    try:
        fills: List[str] = button_locator.evaluate(
            """(btn)=>{
                const paths = btn.querySelectorAll('svg path');
                const fills = [];
                paths.forEach(p=>{
                    const fill = getComputedStyle(p).fill || p.getAttribute('fill') || '';
                    fills.push(fill);
                });
                return fills;
            }"""
        )
        gold = sum(1 for f in fills if f.strip().lower() == "rgb(255, 204, 1)")
        gold = max(0, min(5, gold))
        return gold if gold or fills else None, fills
    except Exception:
        return None, None


def _count_gold_stars_in_cell(rating_cell_locator) -> Optional[int]:
    """
    Read star rating from a games-table-column__value-rating-cell.
    Each cell has two rating-containers (away, home); only one has a button.
    Returns the star count from whichever container has a rating button, or None.
    """
    try:
        containers = rating_cell_locator.locator("div.games-table-column__rating-container")
        for idx in range(containers.count()):
            btn = containers.nth(idx).locator("button.games-table-column__rating-button")
            if btn.count() > 0:
                stars, _ = _count_gold_stars(btn.first)
                return stars
        return None
    except Exception:
        return None


def _recommended_row_index(rating_cell_locator, line_cell_locator) -> Tuple[Optional[int], Optional[int], Optional[float]]:
    """
    Determine which team row (0=away, 1=home) BetQL recommends for a spread game.

    Primary signal: whichever rating-container has a rating-button holds the stars.
    Fallback: whichever line-cell row contains a play/action button (▶).

    Returns (row_index, star_count, line_value).
    row_index is 0 (away) or 1 (home), or None if undetermined.
    star_count and line_value may be None independently.
    """
    star_idx: Optional[int] = None
    star_count: Optional[int] = None

    # Primary: star rating container index
    try:
        containers = rating_cell_locator.locator("div.games-table-column__rating-container")
        for idx in range(containers.count()):
            btn = containers.nth(idx).locator("button.games-table-column__rating-button")
            if btn.count() > 0:
                stars, _ = _count_gold_stars(btn.first)
                star_idx = idx
                star_count = stars
                break
    except Exception:
        pass

    # Fallback: play button presence in line cell rows
    play_idx: Optional[int] = None
    if star_idx is None:
        try:
            rows = line_cell_locator.locator("div.games-table-column__line-container")
            for idx in range(rows.count()):
                # Look for any button that looks like a play/action button
                btns = rows.nth(idx).locator("button")
                if btns.count() > 0:
                    play_idx = idx
                    break
        except Exception:
            pass

    row_idx = star_idx if star_idx is not None else play_idx

    # Read the line from the correct row
    line_val: Optional[float] = None
    if row_idx is not None:
        try:
            rows = line_cell_locator.locator("div.games-table-column__line-container")
            if rows.count() > row_idx:
                bold = rows.nth(row_idx).locator("p.games-table-column__line-text--bold").first
            else:
                # Fallback: all bolds in cell, pick by index
                bolds = line_cell_locator.locator("p.games-table-column__line-text--bold")
                bold = bolds.nth(row_idx) if bolds.count() > row_idx else bolds.first
            txt = bold.inner_text(timeout=500).strip().replace("½", ".5")
            m = re.search(r"([+-]?\d+(?:\.\d+)?)", txt)
            if m:
                line_val = float(m.group(1))
        except Exception:
            pass

    return row_idx, star_count, line_val


def _read_pro_pct_from_cell(pro_edge_cell_locator) -> Optional[int]:
    """
    Read Pro Betting % from a games-table-column__pro-edge-cell.
    The bold <p> inside the cell holds the percentage for the picked side.
    Returns the integer percentage (e.g. 7 for "7%"), or None if missing/unparseable.
    """
    try:
        bold = pro_edge_cell_locator.locator("p.games-table-column__line-text--bold").first
        if bold.count() == 0:
            return None
        txt = bold.inner_text(timeout=500).strip()
        if txt == "--" or not txt:
            return None
        m = re.search(r"(\d+)", txt)
        return int(m.group(1)) if m else None
    except Exception:
        return None


def _parse_date_from_href(href: Optional[str]) -> Optional[str]:
    """
    Extract ISO date string (YYYY-MM-DD) from a BetQL game-predictions href.
    Format: /nba/game-predictions/away-slug-vs-home-slug-MM-DD-YYYY
    Returns e.g. "2026-03-17" or None.
    """
    if not href:
        return None
    m = re.search(r"-(\d{2})-(\d{2})-(\d{4})$", href)
    if not m:
        return None
    month, day, year = m.group(1), m.group(2), m.group(3)
    return f"{year}-{month}-{day}"


def _event_start_from_href(href: Optional[str]) -> Optional[str]:
    """Return ISO datetime string (noon UTC on game date) from href, or None."""
    date_str = _parse_date_from_href(href)
    if not date_str:
        return None
    return f"{date_str}T12:00:00+00:00"


def _parse_spread_pick(txt: str) -> Tuple[Optional[str], Optional[float]]:
    txt = txt.replace("½", ".5")
    m = re.search(r"([+-]\d+(?:\.\d+)?)", txt)
    if not m:
        return None, None
    try:
        line = float(m.group(1))
    except ValueError:
        return None, None
    side = None
    if txt.strip().startswith("+") or txt.strip().startswith("-"):
        side = "AWAY_OR_HOME"
    return side, line


def _parse_total_pick(txt: str) -> Tuple[Optional[str], Optional[float]]:
    txt = txt.replace("½", ".5")
    m = re.search(r"(O|U)\s*([0-9]+(?:\.[0-9]+)?)", txt, re.IGNORECASE)
    if not m:
        return None, None
    side = "OVER" if m.group(1).upper() == "O" else "UNDER"
    try:
        line = float(m.group(2))
    except ValueError:
        return None, None
    return side, line


def _team_from_container(container) -> Tuple[Optional[str], Optional[str]]:
    abbr = None
    name = None
    try:
        img = container.locator("img[aria-label$='Team Logo']").first
        src = img.get_attribute("src") or img.get_attribute("data-src") or ""
        m = re.search(r"/NBA/([A-Z]{2,3})\\.(?:png|webp)", src)
        if m:
            abbr = m.group(1)
    except Exception:
        pass
    try:
        name = container.locator("p.games-table-column__team-name").first.inner_text().strip()
    except Exception:
        pass
    return abbr, name


def parse_teams_from_team_cell(team_cell_locator) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    away_abbr = home_abbr = away_name = home_name = None
    try:
        containers = team_cell_locator.locator("div.games-table-column__team-container")
        if containers.count() >= 1:
            away_abbr, away_name = _team_from_container(containers.nth(0))
        if containers.count() >= 2:
            home_abbr, home_name = _team_from_container(containers.nth(1))
    except Exception:
        pass
    return away_abbr, home_abbr, away_name, home_name


def extract_spread_columns(page: Page, canonical_url: str, debug: bool = False) -> Tuple[List[Dict[str, object]], Dict[str, int]]:
    observed = _now_iso()
    records: List[Dict[str, object]] = []

    team_cells = page.locator("div.games-table-column__team-cell")
    line_cells = page.locator("div.games-table-column__current-line-cell")
    pro_edge_cells = page.locator("div.games-table-column__pro-edge-cell")
    rating_cells = page.locator("div.games-table-column__value-rating-cell")
    matchup_links = page.locator("a.games-table-column__team-link")

    n = min(team_cells.count(), line_cells.count(), pro_edge_cells.count(), rating_cells.count(), matchup_links.count())
    dbg = {"rows": n, "team_cells": team_cells.count(), "line_cells": line_cells.count()}

    for i in range(n):
        try:
            # Skip 1st-half and moneyline rows
            matchup_text = matchup_links.nth(i).inner_text(timeout=500).strip()
            if "1st half" in matchup_text.lower() or "moneyline" in matchup_text.lower():
                continue

            # Teams (away=index 0, home=index 1)
            away_abbr, home_abbr, away_name, home_name = parse_teams_from_team_cell(team_cells.nth(i))
            away = away_abbr or away_name
            home = home_abbr or home_name
            if not (away and home):
                continue
            matchup_hint = f"{away}@{home}"

            # Determine recommended team by which row holds the star rating (primary)
            # or play button (fallback). row_idx: 0=away, 1=home.
            row_idx, rating_stars, line_hint = _recommended_row_index(
                rating_cells.nth(i), line_cells.nth(i)
            )

            # Validate we got a usable line
            if line_hint is None:
                # Last-resort: read first bold text from line cell
                bolds = line_cells.nth(i).locator("p.games-table-column__line-text--bold")
                if bolds.count() == 0:
                    continue
                pick_txt_fallback = bolds.first.inner_text(timeout=500).strip().replace("½", ".5")
                m_fb = re.search(r"[+-]?\d+(?:\.\d+)?", pick_txt_fallback)
                if not m_fb:
                    continue
                line_hint = float(m_fb.group())

            # Reject moneyline-sized lines (>= 100)
            if abs(line_hint) >= 100:
                continue

            # Selection from star/play-button row; None if row undetermined
            if row_idx == 0:
                selection = away
            elif row_idx == 1:
                selection = home
            else:
                selection = None

            # Reconstruct pick text for raw_block legibility
            sign = "+" if line_hint >= 0 else ""
            pick_txt = f"{sign}{line_hint:g}"

            # Pro Betting %
            pro_pct = _read_pro_pct_from_cell(pro_edge_cells.nth(i))

            # Game date from href
            href = matchup_links.nth(i).get_attribute("href")
            event_start = _event_start_from_href(href)

            base_block = f"rating_stars={rating_stars} pro_pct={pro_pct} row_idx={row_idx} {matchup_text} {pick_txt}"

            base = _common_fields("betql_model_spread", canonical_url, observed)
            rec = {
                **base,
                "market_family": "spread",
                "raw_pick_text": pick_txt,
                "raw_block": base_block,
                "raw_fingerprint": None,
                "source_updated_at_utc": None,
                "event_start_time_utc": event_start,
                "expert_name": "BetQL Model",
                "matchup_hint": matchup_hint,
                "rating_stars": rating_stars,
                "pro_pct": pro_pct,
                "line_hint": line_hint,
                "odds_hint": None,
                "away_team": away,
                "home_team": home,
                "selection_hint": selection,
            }
            rec["raw_fingerprint"] = sha256_json({k: v for k, v in rec.items() if k != "raw_fingerprint"})
            records.append(rec)
        except Exception as e:
            if debug:
                print(f"[betql_extractors] spread row {i} error: {e}")
            continue

    return records, dbg


def extract_totals_columns(page: Page, canonical_url: str, debug: bool = False) -> Tuple[List[Dict[str, object]], Dict[str, int]]:
    observed = _now_iso()
    records: List[Dict[str, object]] = []

    team_cells = page.locator("div.games-table-column__team-cell")
    line_cells = page.locator("div.games-table-column__current-line-cell")
    pro_edge_cells = page.locator("div.games-table-column__pro-edge-cell")
    rating_cells = page.locator("div.games-table-column__value-rating-cell")
    matchup_links = page.locator("a.games-table-column__team-link")

    n = min(team_cells.count(), line_cells.count(), pro_edge_cells.count(), rating_cells.count(), matchup_links.count())
    dbg = {"rows": n, "team_cells": team_cells.count(), "line_cells": line_cells.count()}

    for i in range(n):
        try:
            matchup_text = matchup_links.nth(i).inner_text(timeout=500).strip()
            if "1st half" in matchup_text.lower() or "moneyline" in matchup_text.lower():
                continue

            bolds = line_cells.nth(i).locator("p.games-table-column__line-text--bold")
            if bolds.count() == 0:
                continue
            pick_txt = bolds.first.inner_text(timeout=500).strip()
            if not pick_txt or pick_txt == "--":
                continue
            if "1st half" in pick_txt.lower():
                continue

            side_hint, line_hint = _parse_total_pick(pick_txt)
            if side_hint is None:
                continue

            away_abbr, home_abbr, away_name, home_name = parse_teams_from_team_cell(team_cells.nth(i))
            away = away_abbr or away_name
            home = home_abbr or home_name
            if not (away and home):
                continue
            matchup_hint = f"{away}@{home}"

            pro_pct = _read_pro_pct_from_cell(pro_edge_cells.nth(i))
            rating_stars = _count_gold_stars_in_cell(rating_cells.nth(i))

            href = matchup_links.nth(i).get_attribute("href")
            event_start = _event_start_from_href(href)

            base_block = f"rating_stars={rating_stars} pro_pct={pro_pct} {matchup_text} {pick_txt}"

            base = _common_fields("betql_model_total", canonical_url, observed)
            rec = {
                **base,
                "market_family": "total",
                "raw_pick_text": pick_txt,
                "raw_block": base_block,
                "raw_fingerprint": None,
                "source_updated_at_utc": None,
                "event_start_time_utc": event_start,
                "expert_name": "BetQL Model",
                "matchup_hint": matchup_hint,
                "rating_stars": rating_stars,
                "pro_pct": pro_pct,
                "line_hint": line_hint,
                "odds_hint": None,
                "away_team": away,
                "home_team": home,
                "side_hint": side_hint,
            }
            rec["raw_fingerprint"] = sha256_json({k: v for k, v in rec.items() if k != "raw_fingerprint"})
            records.append(rec)
        except Exception as e:
            if debug:
                print(f"[betql_extractors] total row {i} error: {e}")
            continue

    return records, dbg


def extract_sharp_spreads_totals(page: Page, canonical_url: str, debug: bool = False) -> Tuple[List[Dict[str, object]], Dict[str, int]]:
    """
    Sharp picks page (betql.co/nba/sharp-picks): reads Pro Betting % from
    games-table-column__pro-edge-cell (bold p inside). No star ratings on this page.
    Selection is determined by which row contains the green play button (▶), via
    _recommended_row_index() fallback path (star_idx will always be None here).
    Spreads: row 0 = away, row 1 = home.
    Totals:  row 0 = OVER, row 1 = UNDER.
    """
    observed = _now_iso()
    records: List[Dict[str, object]] = []

    team_cells = page.locator("div.games-table-column__team-cell")
    line_cells = page.locator("div.games-table-column__current-line-cell")
    pro_edge_cells = page.locator("div.games-table-column__pro-edge-cell")
    matchup_links = page.locator("a.games-table-column__team-link")

    n = min(team_cells.count(), line_cells.count(), pro_edge_cells.count(), matchup_links.count())
    dbg = {"rows": n, "team_cells": team_cells.count(), "pro_edge_cells": pro_edge_cells.count()}

    for i in range(n):
        try:
            matchup_text = matchup_links.nth(i).inner_text(timeout=500).strip()
            if "1st half" in matchup_text.lower() or "moneyline" in matchup_text.lower():
                continue

            # On the sharp page there are no buttons or star ratings — the recommended
            # side is indicated by whichever text-container row has the bold <p> class.
            # text-container 0 = away (spreads) / OVER (totals)
            # text-container 1 = home (spreads) / UNDER (totals)
            row_idx = None
            line_hint = None
            containers = line_cells.nth(i).locator("div.games-table-column__text-container")
            for j in range(containers.count()):
                bold_p = containers.nth(j).locator("p.games-table-column__line-text--bold")
                if bold_p.count() > 0:
                    raw = bold_p.first.inner_text(timeout=500).strip().replace("½", ".5")
                    if not raw or raw == "--":
                        break
                    m = re.search(r"([+-]?\d+(?:\.\d+)?)", raw)
                    if m:
                        line_hint = float(m.group(1))
                        row_idx = j
                    break

            if line_hint is None:
                continue

            # Determine market family from line value:
            # totals lines are always > 100 (e.g. 228.5); spreads are < 50
            if abs(line_hint) > 50:
                market_family = "total"
            else:
                market_family = "spread"
                # Reject moneyline bleed-through
                if abs(line_hint) >= 100:
                    continue

            away_abbr, home_abbr, away_name, home_name = parse_teams_from_team_cell(team_cells.nth(i))
            away = away_abbr or away_name
            home = home_abbr or home_name
            if not (away and home):
                continue
            matchup_hint = f"{away}@{home}"

            # Selection from play-button row index
            if market_family == "spread":
                if row_idx == 0:
                    selection = away
                elif row_idx == 1:
                    selection = home
                else:
                    selection = None
                side_hint = None
            else:
                # Totals: row 0 = OVER, row 1 = UNDER
                if row_idx == 0:
                    side_hint = "OVER"
                elif row_idx == 1:
                    side_hint = "UNDER"
                else:
                    side_hint = None
                selection = side_hint

            pro_pct = _read_pro_pct_from_cell(pro_edge_cells.nth(i))

            href = matchup_links.nth(i).get_attribute("href")
            event_start = _event_start_from_href(href)

            sign = "+" if line_hint >= 0 else ""
            pick_txt = f"{sign}{line_hint:g}"

            surface = "betql_sharp_spread" if market_family == "spread" else "betql_sharp_total"
            base = _common_fields(surface, canonical_url, observed)
            rec = {
                **base,
                "market_family": market_family,
                "raw_pick_text": pick_txt,
                "raw_block": f"pro_pct={pro_pct} row_idx={row_idx} {matchup_text} {pick_txt}",
                "raw_fingerprint": None,
                "source_updated_at_utc": None,
                "event_start_time_utc": event_start,
                "expert_name": "BetQL Pro Betting",
                "matchup_hint": matchup_hint,
                "rating_stars": None,
                "pro_pct": pro_pct,
                "line_hint": line_hint,
                "odds_hint": None,
                "away_team": away,
                "home_team": home,
                "selection_hint": selection,
                "side_hint": side_hint if market_family == "total" else None,
            }
            rec["raw_fingerprint"] = sha256_json({k: v for k, v in rec.items() if k != "raw_fingerprint"})
            records.append(rec)
        except Exception as e:
            if debug:
                print(f"[betql_extractors] sharp row {i} error: {e}")
            continue

    return records, dbg
