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
                    const fill = getComputedStyle(p).fill || '';
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
    rating_buttons = page.locator("button.games-table-column__rating-button")
    spread_cells = page.locator(".games-table-column__current-line-cell")
    matchups = page.locator("a.games-table-column__team-link")
    team_cells = page.locator("div.games-table-column__team-cell")
    pro_pcts = page.locator("p.games-table-column__line-text--bold")
    n = min(rating_buttons.count(), spread_cells.count(), matchups.count(), pro_pcts.count(), team_cells.count())
    debug = {"ratings": rating_buttons.count(), "rows": n}

    for i in range(n):
        matchup_text = matchups.nth(i).inner_text().strip()
        if "1st half" in matchup_text.lower() or "moneyline" in matchup_text.lower():
            continue

        rating_stars, fills = _count_gold_stars(rating_buttons.nth(i))
        bolds = spread_cells.nth(i).locator("p.games-table-column__line-text--bold")
        if bolds.count() == 0:
            continue
        pick_txt = bolds.nth(0).inner_text().strip()
        side_hint, line_hint = _parse_spread_pick(pick_txt)

        pct_txt = pro_pcts.nth(i).inner_text()
        m_pct = re.search(r"(\d+)", pct_txt)
        pro_pct = int(m_pct.group(1)) if m_pct else None
        if pro_pct is not None and pro_pct > 100:
            pro_pct = None
        if pro_pct is not None and pro_pct > 100:
            pro_pct = None

        away_abbr, home_abbr, away_name, home_name = parse_teams_from_team_cell(team_cells.nth(i))
        away = away_abbr or away_name
        home = home_abbr or home_name
        selection = away if pick_txt.startswith("+") else home if pick_txt.startswith("-") else None
        matchup_hint = f"{away}@{home}" if away and home else None

        base_block = f"rating_stars={rating_stars} pro_pct={pro_pct} {matchup_text} {pick_txt}"
        if debug and fills is not None:
            base_block += f" rating_rgb_fill={fills}"

        base = _common_fields("betql_model_spread", canonical_url, observed)
        model_rec = {
            **base,
            "market_family": "spread",
            "raw_pick_text": pick_txt,
            "raw_block": base_block,
            "raw_fingerprint": None,
            "source_updated_at_utc": None,
            "event_start_time_utc": None,
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
        model_rec["raw_fingerprint"] = sha256_json({k: v for k, v in model_rec.items() if k != "raw_fingerprint"})
        records.append(model_rec)

    return records, debug


def extract_totals_columns(page: Page, canonical_url: str, debug: bool = False) -> Tuple[List[Dict[str, object]], Dict[str, int]]:
    observed = _now_iso()
    records: List[Dict[str, object]] = []
    rating_buttons = page.locator("button.games-table-column__rating-button")
    total_cells = page.locator(".games-table-column__current-line-cell")
    matchups = page.locator("a.games-table-column__team-link")
    team_cells = page.locator("div.games-table-column__team-cell")
    pro_pcts = page.locator("p.games-table-column__line-text--bold")
    n = min(rating_buttons.count(), total_cells.count(), matchups.count(), pro_pcts.count(), team_cells.count())
    debug = {"ratings": rating_buttons.count(), "rows": n}

    for i in range(n):
        matchup_text = matchups.nth(i).inner_text().strip()
        if "1st half" in matchup_text.lower() or "moneyline" in matchup_text.lower():
            continue

        rating_stars, fills = _count_gold_stars(rating_buttons.nth(i))
        bolds = total_cells.nth(i).locator("p.games-table-column__line-text--bold")
        if bolds.count() == 0:
            continue
        pick_txt = bolds.nth(0).inner_text().strip()
        side_hint, line_hint = _parse_total_pick(pick_txt)
        if side_hint is None:
            continue

        pct_txt = pro_pcts.nth(i).inner_text()
        m_pct = re.search(r"(\d+)", pct_txt)
        pro_pct = int(m_pct.group(1)) if m_pct else None

        away_abbr, home_abbr, away_name, home_name = parse_teams_from_team_cell(team_cells.nth(i))
        away = away_abbr or away_name
        home = home_abbr or home_name
        matchup_hint = f"{away}@{home}" if away and home else None

        base_block = f"rating_stars={rating_stars} pro_pct={pro_pct} {matchup_text} {pick_txt}"
        if debug and fills is not None:
            base_block += f" rating_rgb_fill={fills}"

        base = _common_fields("betql_model_total", canonical_url, observed)
        model_rec = {
            **base,
            "market_family": "total",
            "raw_pick_text": pick_txt,
            "raw_block": base_block,
            "raw_fingerprint": None,
            "source_updated_at_utc": None,
            "event_start_time_utc": None,
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
        model_rec["raw_fingerprint"] = sha256_json({k: v for k, v in model_rec.items() if k != "raw_fingerprint"})
        records.append(model_rec)

    return records, debug


def extract_sharp_spreads_totals(page: Page, canonical_url: str, debug: bool = False) -> Tuple[List[Dict[str, object]], Dict[str, int]]:
    """
    Sharp picks page: bold pick text carries the pro bet selection. No ratings; we only emit probet bets.
    """
    observed = _now_iso()
    records: List[Dict[str, object]] = []

    line_cells = page.locator(".games-table-column__current-line-cell")
    matchups = page.locator("a.games-table-column__team-link")
    team_cells = page.locator("div.games-table-column__team-cell")
    pro_pcts = page.locator("p:has-text('%')")
    n = min(line_cells.count(), matchups.count(), team_cells.count(), pro_pcts.count())
    dbg = {"rows": n}

    for i in range(n):
        matchup_text = matchups.nth(i).inner_text().strip()
        bolds = line_cells.nth(i).locator("p.games-table-column__line-text--bold")
        if bolds.count() == 0:
            continue
        pick_txt = bolds.nth(0).inner_text().strip()
        side_hint, line_hint = _parse_total_pick(pick_txt)
        market_family = "total" if side_hint else "spread"
        if market_family == "spread":
            side_hint, line_hint = _parse_spread_pick(pick_txt)
        if market_family not in {"spread", "total"}:
            continue

        pct_txt = pro_pcts.nth(i).inner_text()
        m_pct = re.search(r"(\\d+)", pct_txt)
        pro_pct = int(m_pct.group(1)) if m_pct else None
        if pro_pct is not None and pro_pct > 100:
            pro_pct = None

        away_abbr, home_abbr, away_name, home_name = parse_teams_from_team_cell(team_cells.nth(i))
        away = away_abbr or away_name
        home = home_abbr or home_name
        if not (away and home):
            continue
        matchup_hint = f"{away}@{home}"

        selection = None
        if market_family == "spread":
            selection = away if pick_txt.startswith("+") else home if pick_txt.startswith("-") else None
        else:
            selection = side_hint

        base = _common_fields(
            "betql_sharp_spread" if market_family == "spread" else "betql_sharp_total",
            canonical_url,
            observed,
        )
        rec = {
            **base,
            "market_family": market_family,
            "raw_pick_text": pick_txt,
            "raw_block": f"pro_pct={pro_pct} {matchup_text} {pick_txt}",
            "raw_fingerprint": None,
            "source_updated_at_utc": None,
            "event_start_time_utc": None,
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

    return records, dbg
