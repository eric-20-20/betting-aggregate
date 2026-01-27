from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup

from store import sha256_json


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean(text: str) -> str:
    return " ".join(text.split()).strip()


def _common_fields(source_surface: str, canonical_url: str, observed: str) -> Dict[str, Any]:
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


def _parse_star_rating(container) -> Optional[int]:
    if not container:
        return None
    svgs = container.select("svg") if hasattr(container, "select") else []
    count = len(svgs)
    if count == 0 and hasattr(container, "select"):
        paths = container.select("svg path")
        colored = [p for p in paths if "rgb(255, 204, 1)" in (p.get("fill") or "")]
        count = len(colored)
    if count == 0:
        text = container.get_text(" ", strip=True) if hasattr(container, "get_text") else str(container)
        m = re.search(r"(\d+)\s*stars?", text, re.IGNORECASE)
        if m:
            try:
                count = int(m.group(1))
            except ValueError:
                count = 0
    if count == 0:
        return None
    count = max(0, min(5, count))
    return int(count)


def _parse_pro_pct(container) -> Optional[float]:
    text = container.get_text(" ", strip=True) if hasattr(container, "get_text") else str(container)
    m = re.search(r"pro\s*betting[^0-9]*([\d\.]+)\s*%", text, re.IGNORECASE)
    if not m:
        m = re.search(r"pro%\s*([\d\.]+)", text, re.IGNORECASE)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return None
    return None


def _parse_logo_team_codes(row) -> List[str]:
    codes: List[str] = []
    for img in row.select("img"):
        src = img.get("src") or ""
        m = re.search(r"/NBA/([A-Z]{2,3})", src)
        if m:
            codes.append(m.group(1))
    return codes


def _parse_line_from_cell(row) -> Optional[float]:
    cell = row.select_one("div.games-table-column__current-line-cell")
    if not cell:
        return None
    txt = cell.get_text(" ", strip=True)
    m = re.search(r"([+-]?\d+(?:\.\d+)?)", txt.replace("½", ".5"))
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return None
    return None


def _parse_odds(row) -> Optional[int]:
    txt = row.get_text(" ", strip=True)
    m = re.search(r"([+-]\d{2,4})", txt)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None


def extract_model_rows(html: str, canonical_url: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    observed = _now_iso()
    records: List[Dict[str, Any]] = []
    candidates_text: List[str] = []
    for row in soup.select("div.game-row, div.game-table-row, div.game-card"):
        row_text = _clean(row.get_text(" ", strip=True))
        candidates_text.append(row_text)
        rating_el = row.select_one("button.games-table-column__rating-button")
        rating_stars = _parse_star_rating(rating_el) if rating_el else None
        pro_pct = None
        # Scope pro % to column containing header text
        pro_col = None
        for col in row.select("div.games-table__column, div.games-table-column"):
            if "Pro Betting" in col.get_text(" ", strip=True):
                pro_col = col
                break
        if pro_col:
            for pct_el in pro_col.select("p.games-table-column__line-text--bold"):
                candidate = _parse_pro_pct(pct_el)
                if candidate is not None:
                    pro_pct = int(candidate)
                    break
        logos = _parse_logo_team_codes(row)
        away = logos[0] if len(logos) >= 1 else None
        home = logos[1] if len(logos) >= 2 else None
        line_val = _parse_line_from_cell(row)
        odds_val = _parse_odds(row)
        matchup_hint = None
        if away and home:
            matchup_hint = f"{away} vs {home}"

        base_block = f"rating_stars={rating_stars} pro_pct={pro_pct} {row_text}"

        base = _common_fields("nba_model_bets", canonical_url, observed)
        model_rec = {
            **base,
            "market_family": "spread",
            "raw_pick_text": row_text,
            "raw_block": base_block,
            "raw_fingerprint": None,
            "source_updated_at_utc": None,
            "event_start_time_utc": None,
            "expert_name": "BetQL Model",
            "matchup_hint": matchup_hint,
            "rating_stars": rating_stars,
            "pro_pct": pro_pct,
            "line_hint": line_val,
            "odds_hint": odds_val,
            "away_team": away,
            "home_team": home,
        }
        model_rec["raw_fingerprint"] = sha256_json({k: v for k, v in model_rec.items() if k != "raw_fingerprint"})
        records.append(model_rec)

        if pro_pct is not None:
            base2 = _common_fields("nba_model_pro_betting", canonical_url, observed)
            sharp_rec = {
                **base2,
                "market_family": "spread",
                "raw_pick_text": row_text,
                "raw_block": base_block,
                "raw_fingerprint": None,
                "source_updated_at_utc": None,
                "event_start_time_utc": None,
                "expert_name": "BetQL Sharps",
                "matchup_hint": matchup_hint,
                "rating_stars": rating_stars,
                "pro_pct": pro_pct,
                "line_hint": line_val,
                "odds_hint": odds_val,
                "away_team": away,
                "home_team": home,
            }
            sharp_rec["raw_fingerprint"] = sha256_json({k: v for k, v in sharp_rec.items() if k != "raw_fingerprint"})
            records.append(sharp_rec)
    return {"records": records, "candidates": candidates_text}


def extract_sharp_rows(html: str, canonical_url: str) -> Dict[str, Any]:
    # legacy; now covered by model rows pro_pct; keep for compatibility
    soup = BeautifulSoup(html, "html.parser")
    observed = _now_iso()
    records: List[Dict[str, Any]] = []
    candidates: List[str] = []
    for row in soup.select("div.game-row, div.game-table-row, div.game-card"):
        row_text = _clean(row.get_text(" ", strip=True))
        candidates.append(row_text)
        pct = _parse_pro_pct(row)
        if pct is None:
            continue
        matchup = row.select_one(".game, .matchup, .game-name")
        raw_pick = row_text
        base = _common_fields("nba_sharp_picks", canonical_url, observed)
        rec = {
            **base,
            "market_family": "spread",
            "raw_pick_text": raw_pick,
            "raw_block": row_text,
            "raw_fingerprint": None,
            "source_updated_at_utc": None,
            "event_start_time_utc": None,
            "expert_name": "BetQL Sharps",
            "matchup_hint": matchup.get_text(" ", strip=True) if matchup else None,
            "pro_pct": pct,
        }
        rec["raw_fingerprint"] = sha256_json({k: v for k, v in rec.items() if k != "raw_fingerprint"})
        records.append(rec)
    return {"records": records, "candidates": candidates}


def extract_prop_cards(html: str, canonical_url: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    observed = _now_iso()
    records: List[Dict[str, Any]] = []
    candidates: List[str] = []
    for card in soup.select("div.player-prop-card, div.best-bets-card"):
        title = _clean(card.get_text(" ", strip=True))
        candidates.append(title)
        rating_el = card.select_one("[class*='star'], .rating")
        rating = _parse_star_rating(rating_el) if rating_el else None
        base = _common_fields("nba_player_props", canonical_url, observed)
        rec = {
            **base,
            "market_family": "player_prop",
            "raw_pick_text": title,
            "raw_block": title,
            "raw_fingerprint": None,
            "source_updated_at_utc": None,
            "event_start_time_utc": None,
            "expert_name": "BetQL Props",
            "matchup_hint": None,
            "rating": rating,
        }
        rec["raw_fingerprint"] = sha256_json({k: v for k, v in rec.items() if k != "raw_fingerprint"})
        records.append(rec)
    return {"records": records, "candidates": candidates}
