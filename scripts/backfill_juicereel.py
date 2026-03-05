"""
JuiceReel expert picks backfill script.

Fetches full historical pick tables for:
  - sXeBets: https://app.juicereel.com/users/sXeBets
  - nukethebooks: https://app.juicereel.com/users/nukethebooks

For spread/total/moneyline picks (which lack lines in the history table),
uses The Odds API historical endpoint to fetch the closing line for each game.

Usage:
    python3 scripts/backfill_juicereel.py [--debug] [--storage PATH] [--dry-run]
    python3 scripts/backfill_juicereel.py --expert sxebets --debug
    python3 scripts/backfill_juicereel.py --since 2026-01-01

Output:
    - out/raw_juicereel_backfill_nba.json
    - out/normalized_juicereel_backfill_nba.json

After running, add normalized_juicereel_backfill_nba.json to consensus INPUT_FILES_BY_SPORT
(already done if you ran the pipeline wiring step).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    from playwright.sync_api import sync_playwright, Page
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
except ImportError:
    sync_playwright = None
    PlaywrightTimeoutError = Exception

from store import write_json
from juicereel_ingest import (
    RawPickRecord,
    sha256_digest,
    write_records,
    EXPERTS,
    USER_AGENT,
    _parse_odds,
    _parse_dollar_amount,
    _parse_unit_amount,
    _infer_market_from_pick,
    ensure_out_dir,
)

OUT_DIR = os.getenv("NBA_OUT_DIR", "out")
DEFAULT_STORAGE_STATE = "data/juicereel_storage_state.json"
LINES_CACHE_DIR = Path("data/cache/historical_lines")
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")

# The Odds API team name → standard 3-letter code
TEAM_MAP: Dict[str, str] = {
    "Atlanta Hawks": "ATL", "Boston Celtics": "BOS", "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA", "Chicago Bulls": "CHI", "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL", "Denver Nuggets": "DEN", "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW", "Houston Rockets": "HOU", "Indiana Pacers": "IND",
    "Los Angeles Clippers": "LAC", "LA Clippers": "LAC",
    "Los Angeles Lakers": "LAL", "LA Lakers": "LAL",
    "Memphis Grizzlies": "MEM", "Miami Heat": "MIA", "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN", "New Orleans Pelicans": "NOP",
    "New York Knicks": "NYK", "Oklahoma City Thunder": "OKC",
    "Orlando Magic": "ORL", "Philadelphia 76ers": "PHI", "Phoenix Suns": "PHX",
    "Portland Trail Blazers": "POR", "Sacramento Kings": "SAC",
    "San Antonio Spurs": "SAS", "Toronto Raptors": "TOR",
    "Utah Jazz": "UTA", "Washington Wizards": "WAS",
}


# ─── LGF score cache for result-based side inference ─────────────────────────

LGF_CACHE_DIR = Path("data/cache/results")
_lgf_score_cache: Dict[str, Any] = {}  # date_str → list of game dicts


def _load_lgf_for_date(game_date: str) -> List[Dict]:
    """Load LGF game scores for a given date (YYYY-MM-DD). Cached in memory."""
    if game_date in _lgf_score_cache:
        return _lgf_score_cache[game_date]
    path = LGF_CACHE_DIR / f"lgf_{game_date}.json"
    if not path.exists():
        _lgf_score_cache[game_date] = []
        return []
    try:
        with open(path) as f:
            games = json.load(f)
        _lgf_score_cache[game_date] = games if isinstance(games, list) else []
    except Exception:
        _lgf_score_cache[game_date] = []
    return _lgf_score_cache[game_date]


def _find_lgf_game(game_date: str, away_team: str, home_team: str) -> Optional[Dict]:
    """Find a specific game in LGF cache by date and teams."""
    games = _load_lgf_for_date(game_date)
    for g in games:
        if {g.get("away_team"), g.get("home_team")} == {away_team, home_team}:
            return g
    return None


def _infer_ml_selection_from_result(
    game: Dict, pregraded_result: str, away_team: str, home_team: str
) -> Optional[str]:
    """
    For a moneyline bet, infer which team was selected based on WIN/LOSS result
    and the actual game outcome from LGF scores.

    Returns team abbreviation (e.g. "LAL") or None if indeterminate.
    """
    if pregraded_result not in ("WIN", "LOSS"):
        return None
    away_score = game.get("away_score")
    home_score = game.get("home_score")
    if away_score is None or home_score is None:
        return None
    if away_score == home_score:
        return None  # tie / push, can't determine

    # Normalize orientation: game may have teams in either order
    g_away = game.get("away_team")
    g_home = game.get("home_team")
    if g_away == home_team and g_home == away_team:
        # LGF has teams flipped — swap scores to match our convention
        away_score, home_score = home_score, away_score

    winning_team = away_team if away_score > home_score else home_team
    losing_team = home_team if away_score > home_score else away_team

    if pregraded_result == "WIN":
        return winning_team
    else:  # LOSS
        return losing_team


# ─── Historical line fetching via The Odds API ───────────────────────────────

def _snap_spread(line: Optional[float]) -> Optional[float]:
    """Snap spread to nearest 0.5 (NBA spreads are always .0 or .5)."""
    if line is None:
        return None
    return round(line * 2) / 2


def _to_abbr(name: str) -> Optional[str]:
    if not name:
        return None
    if len(name) in {2, 3, 4} and name.isupper():
        return name
    return TEAM_MAP.get(name)


def fetch_historical_lines_for_date(game_date: str, debug: bool = False) -> Dict[str, Any]:
    """
    Fetch historical NBA lines for a given date from The Odds API.
    Caches results to avoid re-fetching.

    Returns dict keyed by (away_abbr, home_abbr) → {spread_away, spread_home, total}
    """
    LINES_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = LINES_CACHE_DIR / f"lines_{game_date}.json"

    if cache_path.exists():
        with open(cache_path) as f:
            cached = json.load(f)
        if debug:
            print(f"[lines] Loaded {len(cached)} games from cache for {game_date}")
        return cached

    if not ODDS_API_KEY:
        if debug:
            print(f"[lines] No ODDS_API_KEY — skipping line fetch for {game_date}")
        return {}

    # Historical odds endpoint — requires date in ISO format
    # The API uses UTC; NBA games on Eastern date YYYY-MM-DD start in UTC
    # Request the full day window
    date_iso = f"{game_date}T00:00:00Z"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": "us",
        "markets": "spreads,totals",
        "oddsFormat": "american",
        "date": date_iso,
    }
    url = "https://api.the-odds-api.com/v4/historical/sports/basketball_nba/odds/?" + urllib.parse.urlencode(params)

    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=20) as resp:
            remaining = resp.headers.get("x-requests-remaining", "?")
            used = resp.headers.get("x-requests-used", "?")
            if debug:
                print(f"[lines] API quota: {remaining} remaining, {used} used")
            raw = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode()
        except Exception:
            pass
        print(f"[lines] HTTP {e.code} fetching historical odds for {game_date}: {body}")
        return {}
    except Exception as e:
        print(f"[lines] Failed to fetch historical odds for {game_date}: {e}")
        return {}

    # Historical endpoint wraps data in {"timestamp": ..., "data": [...]}
    events = raw.get("data", raw) if isinstance(raw, dict) else raw
    if not isinstance(events, list):
        events = []

    result: Dict[str, Any] = {}
    for event in events:
        away_name = event.get("away_team", "")
        home_name = event.get("home_team", "")
        away_abbr = _to_abbr(away_name)
        home_abbr = _to_abbr(home_name)
        if not away_abbr or not home_abbr:
            continue

        bookmakers = event.get("bookmakers", [])
        # Collect all spread/total points across bookmakers
        spread_points: Dict[str, List[float]] = {}
        total_points: List[float] = []

        for bk in bookmakers:
            for mkt in bk.get("markets", []):
                key = mkt.get("key")
                if key == "spreads":
                    for outcome in mkt.get("outcomes", []):
                        tname = outcome.get("name", "")
                        pt = outcome.get("point")
                        abbr = _to_abbr(tname)
                        if abbr and pt is not None:
                            spread_points.setdefault(abbr, []).append(float(pt))
                elif key == "totals":
                    for outcome in mkt.get("outcomes", []):
                        pt = outcome.get("point")
                        if pt is not None:
                            total_points.append(float(pt))
                            break

        # Consensus: median across bookmakers
        def _median(pts: List[float]) -> Optional[float]:
            if not pts:
                return None
            pts.sort()
            n = len(pts)
            if n % 2 == 1:
                return pts[n // 2]
            return (pts[n // 2 - 1] + pts[n // 2]) / 2

        total = _snap_spread(_median(total_points))  # snap to 0.5

        game_info: Dict[str, Any] = {
            "away": away_abbr,
            "home": home_abbr,
            "total": total,
            "spreads": {},
        }
        for abbr, pts in spread_points.items():
            snapped = _snap_spread(_median(pts))
            game_info["spreads"][abbr] = snapped

        result_key = f"{away_abbr}@{home_abbr}"
        result[result_key] = game_info

    if debug:
        print(f"[lines] Fetched {len(result)} games for {game_date}")

    # Cache result
    with open(cache_path, "w") as f:
        json.dump(result, f, indent=2)

    # Rate-limit: Odds API allows ~10 req/sec on paid plans; be conservative
    time.sleep(0.3)

    return result


def get_line_for_game(
    game_date: str,
    away_abbr: Optional[str],
    home_abbr: Optional[str],
    market_type: str,
    selection: Optional[str],
    debug: bool = False,
) -> Optional[float]:
    """
    Look up historical line for a game.
    Returns the spread for the selected team, or the game total.
    """
    if not away_abbr or not home_abbr:
        return None

    lines = fetch_historical_lines_for_date(game_date, debug=debug)
    if not lines:
        return None

    # Try both orientations
    game = lines.get(f"{away_abbr}@{home_abbr}") or lines.get(f"{home_abbr}@{away_abbr}")
    if not game:
        # Fuzzy: search for any game containing both teams
        for k, v in lines.items():
            if (v.get("away") == away_abbr and v.get("home") == home_abbr) or \
               (v.get("away") == home_abbr and v.get("home") == away_abbr):
                game = v
                break

    if not game:
        if debug:
            print(f"[lines] No line found for {away_abbr}@{home_abbr} on {game_date}")
        return None

    if market_type == "total":
        return game.get("total")
    elif market_type == "spread" and selection:
        return game.get("spreads", {}).get(selection)

    return None


# ─── JuiceReel table extraction ──────────────────────────────────────────────

def _parse_date_from_row(date_text: str) -> Optional[str]:
    """
    Parse a date from a table row date field.
    JuiceReel likely shows dates like "3/2/2026" or "Mar 2, 2026" or "2026-03-02".
    Returns "YYYY-MM-DD" string.
    """
    if not date_text:
        return None
    text = date_text.strip()

    # ISO format
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", text)
    if m:
        return text

    # M/D/YYYY or MM/DD/YYYY
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", text)
    if m:
        month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f"{year:04d}-{month:02d}-{day:02d}"

    # "Mar 2, 2026" or "March 2, 2026"
    m = re.match(r"^(\w{3,9})\s+(\d{1,2}),?\s+(\d{4})$", text, re.IGNORECASE)
    if m:
        month_map = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
        }
        month_abbr = m.group(1)[:3].lower()
        month_num = month_map.get(month_abbr)
        if month_num:
            day, year = int(m.group(2)), int(m.group(3))
            return f"{year:04d}-{month_num:02d}-{day:02d}"

    return None


def _parse_result_text(result_text: str) -> Optional[str]:
    """Map result text to WIN/LOSS/PUSH."""
    if not result_text:
        return None
    t = result_text.strip().lower()
    if t in {"won", "win", "w"}:
        return "WIN"
    if t in {"lost", "loss", "l"}:
        return "LOSS"
    if t in {"push", "p", "tie"}:
        return "PUSH"
    return None


def scroll_table_to_load_all(page: "Page", max_scrolls: int = 80, debug: bool = False) -> int:
    """Scroll to load all rows — tracks page text length to detect when content stops growing."""
    scroll_count = 0
    prev_text_len = 0
    stable_rounds = 0
    for _ in range(max_scrolls):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(800)
        try:
            text_len = page.evaluate("() => document.body.innerText.length")
        except Exception:
            text_len = 0

        if text_len == prev_text_len:
            stable_rounds += 1
            if stable_rounds >= 3:
                break
        else:
            stable_rounds = 0
        prev_text_len = text_len
        scroll_count += 1

    if debug:
        print(f"[scroll] scrolled {scroll_count} times, ~{prev_text_len} chars visible")
    return scroll_count


def click_table_view(page: "Page", debug: bool = False) -> bool:
    """
    Click the 'table view' button to switch from calendar to table view.
    JuiceReel has a calendar button and a table button next to it.
    """
    table_selectors = [
        "[data-testid='table-view']",
        "[aria-label='table view']",
        "[aria-label='Table']",
        "button[title*='table' i]",
        "button[title*='Table' i]",
        # Common icon-based buttons — look for SVG table icon near calendar
        "[class*='table-view']",
        "[class*='TableView']",
        "[class*='tableButton']",
    ]

    for selector in table_selectors:
        try:
            btn = page.query_selector(selector)
            if btn:
                btn.click()
                page.wait_for_timeout(1500)
                if debug:
                    print(f"[table] Clicked table view button: {selector}")
                return True
        except Exception:
            continue

    # Fallback: look for buttons near "calendar" text
    try:
        # Try to find buttons that are siblings of or near a calendar icon
        buttons = page.query_selector_all("button")
        for btn in buttons:
            try:
                text = btn.inner_text().strip().lower()
                title = btn.get_attribute("title") or ""
                aria = btn.get_attribute("aria-label") or ""
                if "table" in text or "table" in title.lower() or "table" in aria.lower():
                    btn.click()
                    page.wait_for_timeout(1500)
                    if debug:
                        print(f"[table] Clicked button with table text: '{text}'")
                    return True
            except Exception:
                continue
    except Exception:
        pass

    if debug:
        print("[table] Could not find table view button — will try to extract table directly")
    return False


def _is_date_line(text: str) -> bool:
    """Return True if text looks like a table row date (M/D/YYYY)."""
    return bool(re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", text.strip()))


def _is_odds_line(text: str) -> bool:
    """Return True if text looks like American odds (-115, +700, etc.)."""
    return bool(re.match(r"^[+-]?\d{3,4}$", text.strip()))


def _is_risk_line(text: str) -> bool:
    """Return True if text looks like a risk amount ($350.00 or 0.9u)."""
    t = text.strip()
    return bool(re.match(r"^\$[\d,.]+$", t) or re.match(r"^\d+\.?\d*u$", t, re.IGNORECASE))


def _is_result_line(text: str) -> bool:
    """Return True if text looks like a bet result."""
    return text.strip().lower() in {"won", "lost", "push", "win", "loss"}


def _parse_risk_value(text: str, profit_type: str) -> Optional[float]:
    """Parse risk text into unit_size based on profit_type."""
    t = text.strip()
    # Dollar amount: $350.00
    m = re.match(r"^\$?([\d,]+\.?\d*)$", t)
    if m:
        try:
            val = float(m.group(1).replace(",", ""))
            if profit_type == "dollars":
                return round(val / 100.0, 3)
            else:
                return round(val / 100.0, 3)  # fallback
        except ValueError:
            pass
    # Unit amount: 0.9u or 1.8u
    m = re.match(r"^([\d.]+)u$", t, re.IGNORECASE)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass
    return None


def parse_table_from_text(
    page_text: str,
    expert: Dict[str, Any],
    observed_at: datetime,
    since_date: Optional[date],
    debug: bool = False,
) -> List[Dict[str, str]]:
    """
    Parse the settled bets table from the page's inner text.

    The table section starts after "Calendar\\nTable\\n" and has a header row:
    "Date\\nMatchup\\nPick\\nOdds\\nRisk\\nTo Win\\nResult\\nProfit\\n"

    Each data row follows the same 8-field pattern.

    Returns list of dicts with keys: date, matchup, pick, odds, risk, to_win, result, profit
    """
    # Find the start of the table section
    # The header appears as "Calendar\nTable\nDate\nMatchup\nPick\nOdds\nRisk\nTo Win\nResult\nProfit"
    table_header = "Date\nMatchup\nPick\nOdds\nRisk\nTo Win\nResult\nProfit"
    idx = page_text.find(table_header)
    if idx == -1:
        # Try without "To Win"
        table_header = "Date\nMatchup\nPick\nOdds\nRisk"
        idx = page_text.find(table_header)
    if idx == -1:
        if debug:
            print("[parse] Could not find table header in page text")
        return []

    table_text = page_text[idx + len(table_header):]
    lines = [l.strip() for l in table_text.split("\n") if l.strip()]

    if debug:
        print(f"[parse] Found table section, {len(lines)} lines after header")
        print(f"[parse] First 30 lines: {lines[:30]}")

    rows: List[Dict[str, str]] = []
    i = 0

    while i < len(lines):
        line = lines[i]

        # Each row starts with a date line (M/D/YYYY)
        if not _is_date_line(line):
            i += 1
            continue

        date_text = line
        i += 1

        # Collect lines until we hit the next date or run out
        # Gather up to ~10 lines for this row
        row_lines = []
        while i < len(lines) and len(row_lines) < 12:
            next_line = lines[i]
            if _is_date_line(next_line) and len(row_lines) >= 4:
                break
            row_lines.append(next_line)
            i += 1

        if len(row_lines) < 4:
            continue

        # The structure we observed:
        # For sXeBets (game bets): matchup, market_label, odds, risk, to_win, result, profit
        # For sXeBets (props): matchup, "• Player Over X Stat\nTotal", odds, risk, to_win, result, profit
        # For nukethebooks: long_matchup_description, "• Player Over X Stat\nTotal", odds, risk, to_win, result, profit
        #
        # Strategy: work backwards from known positions
        # - profit = last line (looks like "+$332.26" or "+0.9u" or "-$159.05")
        # - result = second to last ("Won" or "Lost" or "Push")
        # - odds = first line matching odds pattern
        # - risk = first line after odds matching risk pattern
        # - to_win = line after risk
        # Everything before odds is matchup + pick

        # Find result (Won/Lost/Push) — usually near end
        result_idx = None
        for j in range(len(row_lines) - 1, -1, -1):
            if _is_result_line(row_lines[j]):
                result_idx = j
                break

        if result_idx is None:
            # No result found — skip (pending/open bet)
            if debug:
                print(f"[parse] No result in row starting {date_text}: {row_lines}")
            continue

        profit_text = row_lines[result_idx + 1] if result_idx + 1 < len(row_lines) else ""
        result_text = row_lines[result_idx]

        # Everything before result is: matchup, pick, odds, risk, to_win
        pre_result = row_lines[:result_idx]

        # Find odds (backwards from result)
        odds_idx = None
        for j in range(len(pre_result) - 1, -1, -1):
            if _is_odds_line(pre_result[j]):
                odds_idx = j
                break

        if odds_idx is None:
            if debug:
                print(f"[parse] No odds in row {date_text}: {pre_result}")
            continue

        odds_text = pre_result[odds_idx]
        # risk is right after odds, to_win is after risk
        risk_text = pre_result[odds_idx + 1] if odds_idx + 1 < len(pre_result) else ""
        to_win_text = pre_result[odds_idx + 2] if odds_idx + 2 < len(pre_result) else ""

        # Everything before odds is matchup + pick
        before_odds = pre_result[:odds_idx]

        # Pick text: look for "• Player Over X Stat" pattern
        # Otherwise the last line before odds is the market label (Moneyline, Total, Spread, etc.)
        pick_lines = []
        matchup_lines = []

        # Find bullet point "• ..." as the pick indicator
        bullet_idx = None
        for j, bl in enumerate(before_odds):
            if bl.startswith("•"):
                bullet_idx = j
                break

        if bullet_idx is not None:
            matchup_lines = before_odds[:bullet_idx]
            # pick = bullet line + optional market label after it
            pick_lines = before_odds[bullet_idx:]
        else:
            # No bullet — last line is market type label, rest is matchup
            if before_odds:
                matchup_lines = before_odds[:-1]
                pick_lines = [before_odds[-1]]
            else:
                matchup_lines = []
                pick_lines = []

        matchup_text = " ".join(matchup_lines).strip()
        # For nukethebooks, matchup contains full prop desc — extract just the "Team vs Team" part
        vs_match = re.search(r"([A-Z][a-z].*?\s+vs\.?\s+[A-Z][a-z].*?)(?:\s*\||\s*\(|$)", matchup_text)
        if vs_match:
            matchup_text = vs_match.group(1).strip()

        # Pick text: use bullet line only (not the market label after it)
        pick_text = pick_lines[0].strip() if pick_lines else ""
        # Strip leading bullet
        pick_text = re.sub(r"^•\s*", "", pick_text).strip()
        # Strip trailing market type labels that bled in (e.g. "... Assists Total")
        pick_text = re.sub(r"\s+(Total|Spread|Moneyline|Money Line|Game Bet)\s*$", "", pick_text, flags=re.IGNORECASE).strip()

        row = {
            "date": date_text,
            "matchup": matchup_text,
            "pick": pick_text,
            "odds": odds_text,
            "risk": risk_text,
            "to_win": to_win_text,
            "result": result_text,
            "profit": profit_text,
        }
        rows.append(row)

        if debug and len(rows) <= 5:
            print(f"[parse] Row {len(rows)}: {row}")

    if debug:
        print(f"[parse] Parsed {len(rows)} rows from page text")

    # Apply since_date filter
    if since_date:
        filtered = []
        for r in rows:
            gd = _parse_date_from_row(r["date"])
            if gd:
                try:
                    if date.fromisoformat(gd) >= since_date:
                        filtered.append(r)
                except ValueError:
                    filtered.append(r)
            else:
                filtered.append(r)
        rows = filtered

    return rows


def build_records_from_rows(
    rows: List[Dict[str, str]],
    expert: Dict[str, Any],
    observed_at: datetime,
    debug: bool = False,
) -> List[RawPickRecord]:
    """Convert parsed table row dicts into RawPickRecord objects."""
    from src.normalizer_juicereel_nba import _parse_matchup, _map_team
    from store import data_store

    source_id = expert["source_id"]
    url = expert["url"]
    profit_type = expert.get("profit_type", "dollars")

    records: List[RawPickRecord] = []
    line_cache: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        try:
            game_date = _parse_date_from_row(row["date"])
            if not game_date:
                continue

            matchup_text = row["matchup"]
            pick_text = row["pick"]
            odds_text = row["odds"]
            risk_text = row["risk"]
            result_text = row["result"]

            odds_val = _parse_odds(odds_text)
            unit_size = _parse_risk_value(risk_text, profit_type)
            pregraded_result = _parse_result_text(result_text)

            # Parse market info from pick text
            market_type, selection, line_val, side, player_name, prop_line, stat_key = _infer_market_from_pick(pick_text)

            # Parse teams from matchup
            away_team, home_team = _parse_matchup(matchup_text, store=data_store, sport="NBA")

            # For moneyline/spread picks with just "Moneyline"/"Spread" as pick text,
            # infer selection from WIN/LOSS result + LGF game scores.
            if market_type in ("moneyline", "spread") and selection is None and away_team and home_team and pregraded_result in ("WIN", "LOSS"):
                lgf_game = _find_lgf_game(game_date, away_team, home_team)
                if lgf_game:
                    inferred = _infer_ml_selection_from_result(lgf_game, pregraded_result, away_team, home_team)
                    if inferred:
                        selection = inferred
                        side = inferred

            # For spread/total without line: fetch from Odds API
            if market_type in ("spread", "total") and away_team and home_team:
                needs_line = (market_type == "spread" and line_val is None) or \
                             (market_type == "total" and (line_val is None or selection is None))
                if needs_line:
                    if game_date not in line_cache:
                        line_cache[game_date] = fetch_historical_lines_for_date(game_date, debug=debug)
                    g_lines = line_cache[game_date]
                    game = g_lines.get(f"{away_team}@{home_team}") or g_lines.get(f"{home_team}@{away_team}")
                    if not game:
                        for v in g_lines.values():
                            if {v.get("away"), v.get("home")} == {away_team, home_team}:
                                game = v
                                break
                    if game:
                        if market_type == "total" and line_val is None:
                            line_val = game.get("total")
                        elif market_type == "spread" and line_val is None and selection:
                            sel_abbr = _map_team(selection, sport="NBA")
                            if sel_abbr:
                                line_val = game.get("spreads", {}).get(sel_abbr)
                                selection = sel_abbr
                            if line_val is None:
                                opp = home_team if sel_abbr == away_team else away_team
                                opp_line = game.get("spreads", {}).get(opp)
                                if opp_line is not None:
                                    line_val = -opp_line
                    elif debug:
                        print(f"[lines] No lines for {away_team}@{home_team} on {game_date}")

            actual_line = prop_line if market_type == "player_prop" else line_val

            try:
                event_dt = datetime.strptime(game_date, "%Y-%m-%d").replace(
                    hour=12, tzinfo=ZoneInfo("America/New_York")
                )
                event_dt_str = event_dt.isoformat()
            except Exception:
                event_dt_str = None

            fp_input = f"{source_id}|{game_date}|{matchup_text}|{pick_text}|{odds_text}"
            fingerprint = sha256_digest(fp_input)

            record = RawPickRecord(
                source_id=source_id,
                source_surface="juicereel_backfill",
                sport="NBA",
                market_family="player_prop" if market_type == "player_prop" else "standard",
                observed_at_utc=observed_at.isoformat(),
                canonical_url=url,
                raw_pick_text=pick_text,
                raw_block="|".join(row.values()),
                raw_fingerprint=fingerprint,
                event_start_time_utc=event_dt_str,
                matchup_hint=matchup_text,
                away_team=away_team,
                home_team=home_team,
                expert_name=expert["expert_name"],
                expert_handle=expert["expert_handle"],
                expert_profile=url,
                expert_slug=expert["expert_slug"],
                market_type=market_type if market_type != "unknown" else None,
                selection=selection,
                side=side,
                line=actual_line,
                odds=odds_val,
                player_name=player_name,
                stat_key=stat_key,
                unit_size=unit_size,
                pregraded_result=pregraded_result,
            )
            records.append(record)

        except Exception as e:
            if debug:
                import traceback
                print(f"[build] Error on row {row}: {e}")
                traceback.print_exc()
            continue

    return records


def get_table_text(page: "Page") -> str:
    """
    Extract text from the bets table element only — much faster than inner_text('body')
    on a React SPA. Finds the largest content block to avoid nav/header noise.
    Falls back to full body if no suitable element found.
    """
    try:
        result = page.evaluate("""() => {
            // Strategy: find the element with the most innerText that isn't the full body
            // JuiceReel uses divs, not <table>. We look for containers with bets data.
            const candidates = [];

            // Try specific class patterns first
            const classPatterns = [
                '[class*="BetsTable"]', '[class*="betsTable"]',
                '[class*="HistoryTable"]', '[class*="historyTable"]',
                '[class*="settled"]', '[class*="Settled"]',
                '[class*="PastBets"]', '[class*="pastBets"]',
                'table', 'main', '[role="main"]',
            ];
            for (const sel of classPatterns) {
                const el = document.querySelector(sel);
                if (el && el.innerText) {
                    candidates.push({el, len: el.innerText.length});
                }
            }

            // Also check all large divs (at least 1000 chars of text) that contain "Date"
            const divs = document.querySelectorAll('div');
            for (const div of divs) {
                const text = div.innerText;
                if (text && text.length > 1000 && text.includes('Date') &&
                    (text.includes('Matchup') || text.includes('Pick') || text.includes('Odds'))) {
                    candidates.push({el: div, len: text.length});
                }
            }

            if (candidates.length === 0) {
                return document.body.innerText;
            }

            // Pick the smallest element that still has the table headers
            // (smallest = most targeted = fastest)
            candidates.sort((a, b) => a.len - b.len);
            for (const {el, len} of candidates) {
                const t = el.innerText;
                if (t.includes('Date') && (t.includes('Matchup') || t.includes('Pick'))) {
                    return t;
                }
            }

            return document.body.innerText;
        }""")
        return result or ""
    except Exception:
        return page.inner_text("body")


def find_next_arrow(page: "Page", debug: bool = False):
    """
    Find the next-page arrow button at the bottom of the table.

    JuiceReel pagination structure (from DOM probe):
      [...prev_arrow(disabled), 1, 2, 3, ..., 1590, next_arrow(enabled)]
    The next arrow is the LAST button on the page with empty text.
    When on the last page, the next arrow is disabled.

    Returns the element if found and enabled, else None.
    """
    # Try common aria-label patterns first
    arrow_selectors = [
        "[aria-label='Next page']",
        "[aria-label='next page']",
        "[aria-label='Next']",
        "[aria-label='next']",
        "button[title='Next page']",
        "button[title='Next']",
        "[data-testid='next-page']",
        "[data-testid='pagination-next']",
    ]
    for sel in arrow_selectors:
        try:
            el = page.query_selector(sel)
            if el and el.is_visible() and el.is_enabled():
                return el
        except Exception:
            continue

    # JuiceReel-specific: the next arrow is the LAST button with empty text
    # (it's an SVG-only button with no text content)
    try:
        buttons = page.query_selector_all("button")
        # Check last few buttons for the next-arrow pattern
        for btn in reversed(buttons[-5:]):
            try:
                text = btn.inner_text().strip()
                # Named arrow characters
                if text in (">", "›", "→", "»", "Next", "next", "NEXT"):
                    if btn.is_visible() and btn.is_enabled():
                        if debug:
                            print(f"[paginate] Found next arrow by text: {repr(text)}", flush=True)
                        return btn
                # Empty-text SVG button pattern (JuiceReel specific)
                if text == "" and btn.is_visible() and btn.is_enabled():
                    # Confirm it's not disabled by checking the disabled attribute
                    disabled = btn.get_attribute("disabled")
                    if disabled is None:
                        if debug:
                            print("[paginate] Found next arrow: last empty-text enabled button", flush=True)
                        return btn
            except Exception:
                continue
    except Exception:
        pass

    return None


def click_next_arrow(page: "Page", debug: bool = False) -> bool:
    """
    Click the next-page arrow button. Returns True on success, False if not found
    (which means we've reached the last page).

    Uses JS dispatch_event to bypass any fixed overlay banners that intercept clicks.
    """
    btn = find_next_arrow(page, debug=debug)
    if btn is None:
        if debug:
            print("[paginate] Next arrow not found — likely on last page", flush=True)
        return False
    try:
        # Use JS click to bypass any fixed overlay banners (e.g. "Download App" banner)
        btn.dispatch_event("click")
        page.wait_for_timeout(PAGE_WAIT_MS)
        return True
    except Exception as e:
        if debug:
            print(f"[paginate] Next arrow click failed: {e}", flush=True)
        return False


def probe_pagination_dom(page: "Page") -> None:
    """
    Print all buttons and their aria-labels/text to help identify pagination selectors.
    Run with --probe flag before starting a full backfill.
    """
    print("\n[probe] All buttons on page (for identifying pagination arrow):")
    try:
        buttons = page.query_selector_all("button")
        print(f"[probe] Total buttons: {len(buttons)}")
        # Show last 30 buttons (pagination area is at the bottom)
        start = max(0, len(buttons) - 30)
        for i, btn in enumerate(buttons[start:], start=start):
            try:
                text = btn.inner_text().strip()
                aria = btn.get_attribute("aria-label") or ""
                title = btn.get_attribute("title") or ""
                disabled = btn.get_attribute("disabled")
                visible = btn.is_visible()
                print(f"  [{i}] text={repr(text[:40])} aria={repr(aria)} title={repr(title)} "
                      f"disabled={disabled is not None} visible={visible}")
            except Exception as e:
                print(f"  [{i}] (error: {e})")
    except Exception as e:
        print(f"[probe] Could not query buttons: {e}")

    print("\n[probe] Table container classes (for identifying table selector):")
    try:
        info = page.evaluate("""() => {
            const results = [];
            const selectors = ['main', '[role="main"]', 'table', '[class*="table"]',
                                '[class*="Table"]', '[class*="history"]', '[class*="History"]',
                                '[class*="settled"]', '[class*="bet"]'];
            for (const sel of selectors) {
                const el = document.querySelector(sel);
                if (el) {
                    results.push({
                        selector: sel,
                        className: el.className,
                        textLen: el.innerText ? el.innerText.length : 0
                    });
                }
            }
            return results;
        }""")
        for item in info:
            print(f"  {item['selector']}: class={repr(item['className'][:80])} textLen={item['textLen']}")
    except Exception as e:
        print(f"[probe] Could not query table containers: {e}")


CHECKPOINT_DIR = Path("data/cache/juicereel_checkpoints")
PAGE_WAIT_MS = 700  # ms to wait after each page click


def _checkpoint_path(expert_slug: str) -> Path:
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    return CHECKPOINT_DIR / f"{expert_slug}_checkpoint.jsonl"


def _load_checkpoint(expert_slug: str) -> Tuple[int, List[Dict], set]:
    """Load saved records and last completed page from checkpoint file."""
    cp = _checkpoint_path(expert_slug)
    if not cp.exists():
        return 0, [], set()
    records = []
    seen = set()
    last_page = 0
    with open(cp) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if obj.get("_checkpoint_page"):
                    last_page = obj["_checkpoint_page"]
                else:
                    fp = obj.get("raw_fingerprint", "")
                    if fp not in seen:
                        seen.add(fp)
                        records.append(obj)
            except Exception:
                continue
    print(f"[checkpoint] Loaded {len(records)} records, resuming from page {last_page + 1}", flush=True)
    return last_page, records, seen


def _save_checkpoint(expert_slug: str, new_records: List, page_num: int) -> None:
    """Append new records + page marker to checkpoint file."""
    cp = _checkpoint_path(expert_slug)
    with open(cp, "a") as f:
        for r in new_records:
            f.write(json.dumps(r) + "\n")
        f.write(json.dumps({"_checkpoint_page": page_num}) + "\n")


def extract_history_table_paginated(
    page: "Page",
    expert: Dict[str, Any],
    observed_at: datetime,
    since_date: Optional[date] = None,
    max_pages: Optional[int] = None,
    debug: bool = False,
) -> List[RawPickRecord]:
    """
    Extract historical picks using the next-page arrow button for pagination.
    Prints timing per page. Checkpoints every 25 pages — safe to kill and restart.
    """
    from dataclasses import asdict as _asdict

    slug = expert["expert_slug"]

    # Load checkpoint — resume where we left off
    last_completed_page, saved_dicts, seen_fingerprints = _load_checkpoint(slug)
    start_page = last_completed_page + 1

    print(f"[paginate] {expert['expert_name']}: starting at page {start_page}", flush=True)

    # Fast-forward to checkpoint page using next-arrow (much faster than numbered buttons)
    if start_page > 1:
        print(f"[paginate] Fast-forwarding to page {start_page} via next-arrow...", flush=True)
        for p in range(1, start_page):
            if not click_next_arrow(page, debug=False):
                print(f"[paginate] WARNING: Lost next arrow during fast-forward at page {p}", flush=True)
                break
        print(f"[paginate] Fast-forward complete", flush=True)

    new_records_since_checkpoint: List[Dict] = []
    page_num = start_page

    while True:
        if max_pages and page_num > max_pages:
            print(f"[paginate] Reached max_pages={max_pages} limit", flush=True)
            break

        t0 = time.time()
        page_text = get_table_text(page)
        extract_ms = int((time.time() - t0) * 1000)

        rows = parse_table_from_text(page_text, expert, observed_at, since_date=None,
                                     debug=debug and page_num <= start_page + 1)

        total_so_far = len(saved_dicts) + len(new_records_since_checkpoint)
        print(f"[paginate] Page {page_num}: {len(rows)} rows ({extract_ms}ms) | total={total_so_far}", flush=True)

        if not rows:
            print(f"[paginate] No rows on page {page_num} — stopping", flush=True)
            break

        # Early stop if all rows before since_date
        if since_date:
            page_dates = []
            for r in rows:
                gd = _parse_date_from_row(r["date"])
                if gd:
                    try:
                        page_dates.append(date.fromisoformat(gd))
                    except ValueError:
                        pass
            if page_dates and max(page_dates) < since_date:
                print(f"[paginate] All rows before since_date={since_date} — stopping", flush=True)
                break

        page_records = build_records_from_rows(rows, expert, observed_at, debug=False)
        for rec in page_records:
            if rec.raw_fingerprint not in seen_fingerprints:
                seen_fingerprints.add(rec.raw_fingerprint)
                if since_date and rec.event_start_time_utc:
                    try:
                        ev_date = datetime.fromisoformat(rec.event_start_time_utc).date()
                        if ev_date < since_date:
                            continue
                    except Exception:
                        pass
                new_records_since_checkpoint.append(_asdict(rec))

        # Checkpoint every 25 pages
        if page_num % 25 == 0:
            _save_checkpoint(slug, new_records_since_checkpoint, page_num)
            saved_dicts.extend(new_records_since_checkpoint)
            new_records_since_checkpoint = []
            print(f"[checkpoint] Saved at page {page_num} ({len(saved_dicts)} total)", flush=True)

        # Advance to next page via arrow
        page_num += 1
        if not click_next_arrow(page, debug=debug):
            print(f"[paginate] Reached last page at {page_num - 1}", flush=True)
            break

    # Final checkpoint save
    if new_records_since_checkpoint:
        _save_checkpoint(slug, new_records_since_checkpoint, page_num - 1)
        saved_dicts.extend(new_records_since_checkpoint)

    print(f"[paginate] {expert['expert_name']}: {len(saved_dicts)} total records extracted", flush=True)

    result = []
    for d in saved_dicts:
        try:
            result.append(RawPickRecord(**d))
        except Exception:
            pass
    return result


# ─── Main backfill logic ─────────────────────────────────────────────────────

def backfill_expert(
    expert: Dict[str, Any],
    storage_state: str,
    since_date: Optional[date] = None,
    max_pages: Optional[int] = None,
    debug: bool = False,
    probe: bool = False,
) -> List[RawPickRecord]:
    """Authenticate, navigate to expert's table view, and extract all history rows."""
    if sync_playwright is None:
        raise ImportError("playwright is not installed")

    if not Path(storage_state).exists():
        raise FileNotFoundError(
            f"No storage state at {storage_state}. "
            "Run: python3 scripts/create_juicereel_storage.py"
        )

    observed_at = datetime.now(timezone.utc)
    records: List[RawPickRecord] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=USER_AGENT,
            storage_state=storage_state,
            viewport={"width": 1400, "height": 900},
        )
        page = context.new_page()

        url = expert["url"]
        print(f"[backfill] Loading {expert['expert_name']}: {url}", flush=True)
        for attempt in range(3):
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                break
            except Exception as e:
                if attempt == 2:
                    raise
                print(f"[backfill] Page load attempt {attempt+1} failed: {e} — retrying...", flush=True)
                page.wait_for_timeout(5000)
        page.wait_for_timeout(2500)

        # Click table view button
        clicked = click_table_view(page, debug=debug)
        page.wait_for_timeout(2000)

        if probe:
            # Print DOM info for debugging selectors, then exit
            probe_pagination_dom(page)
            print("\n[probe] Table text sample (first 500 chars):")
            sample = get_table_text(page)
            print(repr(sample[:500]))
            page.close()
            context.close()
            browser.close()
            return []

        records = extract_history_table_paginated(
            page, expert, observed_at,
            since_date=since_date,
            max_pages=max_pages,
            debug=debug,
        )

        page.close()
        context.close()
        browser.close()

    return records


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill JuiceReel expert historical picks")
    ap.add_argument("--storage", default=DEFAULT_STORAGE_STATE, help="Path to Playwright storage state JSON")
    ap.add_argument("--debug", action="store_true", help="Enable verbose debug output")
    ap.add_argument("--dry-run", action="store_true", help="Parse and display picks without writing files")
    ap.add_argument("--expert", choices=["sxebets", "nukethebooks", "all"], default="all",
                    help="Which expert to backfill (default: all)")
    ap.add_argument("--since", default=None, metavar="YYYY-MM-DD",
                    help="Only include picks on or after this date")
    ap.add_argument("--no-lines", action="store_true",
                    help="Skip Odds API line fetching (faster, but spread/total picks lack lines)")
    ap.add_argument("--max-pages", type=int, default=None,
                    help="Limit pagination to this many pages per expert (for testing)")
    ap.add_argument("--clear-checkpoint", action="store_true",
                    help="Clear checkpoints and start from scratch")
    ap.add_argument("--probe", action="store_true",
                    help="Print DOM selectors (buttons, table containers) and exit — use before full run to verify pagination")
    args = ap.parse_args()

    if args.clear_checkpoint:
        for slug in ["sxebets", "nukethebooks"]:
            cp = _checkpoint_path(slug)
            if cp.exists():
                cp.unlink()
                print(f"[checkpoint] Cleared {cp}")

    since_date: Optional[date] = None
    if args.since:
        try:
            since_date = date.fromisoformat(args.since)
        except ValueError:
            print(f"Invalid --since date: {args.since}")
            sys.exit(1)

    experts_to_run = EXPERTS
    if args.expert != "all":
        experts_to_run = [e for e in EXPERTS if e["expert_slug"] == args.expert]
        if not experts_to_run:
            print(f"Unknown expert: {args.expert}")
            sys.exit(1)

    if not ODDS_API_KEY and not args.no_lines:
        print("[backfill] Warning: ODDS_API_KEY not set — spread/total lines will be None")
        print("[backfill] Set ODDS_API_KEY in .env or use --no-lines to suppress this warning")

    all_records: List[RawPickRecord] = []

    for expert in experts_to_run:
        print(f"\n[backfill] Processing {expert['expert_name']}...")
        try:
            records = backfill_expert(
                expert,
                storage_state=args.storage,
                since_date=since_date,
                max_pages=args.max_pages,
                debug=args.debug,
                probe=args.probe,
            )
            print(f"[backfill] {expert['expert_name']}: {len(records)} raw records")

            # Summary stats
            by_market: Dict[str, int] = {}
            graded_count = 0
            no_line_count = 0
            for r in records:
                mt = r.market_type or "unknown"
                by_market[mt] = by_market.get(mt, 0) + 1
                if r.pregraded_result:
                    graded_count += 1
                if r.line is None and mt in ("spread", "total"):
                    no_line_count += 1
            print(f"  Market breakdown: {by_market}")
            print(f"  Pre-graded: {graded_count}/{len(records)}")
            if no_line_count:
                print(f"  Missing lines (spread/total): {no_line_count} — will be ineligible for consensus")

            all_records.extend(records)

        except Exception as e:
            import traceback
            print(f"[backfill] Error processing {expert['expert_name']}: {e}")
            if args.debug:
                traceback.print_exc()

    if args.dry_run:
        print(f"\n[backfill] Dry run — {len(all_records)} total records (not written)")
        for r in all_records[:5]:
            print(f"  {r.expert_name} | {r.market_type} | {r.selection} @ {r.line} | {r.pregraded_result} | {r.matchup_hint}")
        return

    ensure_out_dir()

    # Write per-expert raw files so parallel runs don't overwrite each other
    expert_slugs_run = [e["expert_slug"] for e in experts_to_run]
    for slug, records in zip(expert_slugs_run, [all_records]):
        pass  # all_records already has everything when run sequentially

    # Write per-expert intermediate files
    by_expert: Dict[str, List[RawPickRecord]] = {}
    for r in all_records:
        key = r.source_id  # e.g. "juicereel_sxebets"
        by_expert.setdefault(key, []).append(r)

    for source_id, recs in by_expert.items():
        slug = source_id.replace("juicereel_", "")
        expert_raw_path = os.path.join(OUT_DIR, f"raw_juicereel_{slug}_backfill_nba.json")
        write_records(expert_raw_path, recs)
        print(f"[backfill] Wrote {len(recs)} records → {expert_raw_path}", flush=True)

    # Merge all per-expert files into combined file (safe to run after parallel runs)
    combined_raw_path = os.path.join(OUT_DIR, "raw_juicereel_backfill_nba.json")
    merged: List[Dict[str, Any]] = []
    seen_fps: set = set()
    for slug in ["sxebets", "nukethebooks"]:
        expert_raw_path = os.path.join(OUT_DIR, f"raw_juicereel_{slug}_backfill_nba.json")
        if not os.path.exists(expert_raw_path):
            print(f"[backfill] Skipping missing file: {expert_raw_path}")
            continue
        with open(expert_raw_path) as f:
            recs = json.load(f)
        before = len(merged)
        for r in recs:
            fp = r.get("raw_fingerprint", "")
            if fp not in seen_fps:
                seen_fps.add(fp)
                merged.append(r)
        print(f"[backfill] Merged {len(merged) - before} records from {os.path.basename(expert_raw_path)}")

    with open(combined_raw_path, "w") as f:
        json.dump(merged, f)
    print(f"[backfill] Combined raw → {combined_raw_path} ({len(merged)} total records)", flush=True)

    # Use merged for normalization
    deduped_records_for_norm = merged

    # Deduplicate count for display
    removed = len(all_records) - len([r for r in all_records if r.raw_fingerprint in seen_fps])
    print(f"\n[backfill] Total: {len(all_records)} raw → {len(merged)} after dedup ({len(all_records) - len(merged)} removed)")

    # Normalize
    from src.normalizer_juicereel_nba import normalize_juicereel_records
    normalized = normalize_juicereel_records(deduped_records_for_norm, debug=args.debug, sport="NBA")

    norm_path = os.path.join(OUT_DIR, "normalized_juicereel_backfill_nba.json")
    write_json(norm_path, normalized)
    eligible = sum(1 for r in normalized if r.get("eligible_for_consensus"))
    pregraded = sum(1 for r in normalized if r.get("result"))
    print(f"[backfill] Wrote normalized → {norm_path}")
    print(f"  Eligible for consensus: {eligible}/{len(normalized)}")
    print(f"  Pre-graded (has result): {pregraded}/{len(normalized)}")
    print()
    print("Next steps:")
    print("  1. Copy out/normalized_juicereel_backfill_nba.json to out/")
    print("  2. Run: python3 consensus_nba.py")
    print("  3. Run: python3 scripts/build_signal_ledger.py")
    print("  4. Run: python3 scripts/grade_signals_nba.py --regrade")
    print("  5. Run: python3 scripts/report_records.py")


if __name__ == "__main__":
    main()
