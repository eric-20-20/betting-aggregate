"""Fetch recent NBA game results from The Odds API scores endpoint.

Uses environment variable ODDS_API_KEY.
Outputs/updates `data/results/games.jsonl` with deduping by `game_key_core`.

Run from repo root:
    python3 scripts/fetch_game_results_oddsapi.py --days 3
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any, Dict, List, Optional

ODDS_API_KEY = os.getenv("ODDS_API_KEY")
if not ODDS_API_KEY:
    raise RuntimeError("ODDS_API_KEY not set in environment")

RESULTS_PATH = Path("data/results/games.jsonl")

# Minimal team name -> abbreviation map (common Odds API names).
TEAM_MAP = {
    "Atlanta Hawks": "ATL",
    "Boston Celtics": "BOS",
    "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA",
    "Chicago Bulls": "CHI",
    "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL",
    "Denver Nuggets": "DEN",
    "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW",
    "Houston Rockets": "HOU",
    "Indiana Pacers": "IND",
    "Los Angeles Clippers": "LAC",
    "LA Clippers": "LAC",
    "Los Angeles Lakers": "LAL",
    "LA Lakers": "LAL",
    "Memphis Grizzlies": "MEM",
    "Miami Heat": "MIA",
    "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN",
    "New Orleans Pelicans": "NOP",
    "New York Knicks": "NYK",
    "Oklahoma City Thunder": "OKC",
    "Orlando Magic": "ORL",
    "Philadelphia 76ers": "PHI",
    "Phoenix Suns": "PHX",
    "Portland Trail Blazers": "POR",
    "Sacramento Kings": "SAC",
    "San Antonio Spurs": "SAS",
    "Toronto Raptors": "TOR",
    "Utah Jazz": "UTA",
    "Washington Wizards": "WAS",
}


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not path.exists():
        return out
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
            f.write("\n")


def to_abbr(name: str) -> Optional[str]:
    if not name:
        return None
    if len(name) in {2, 3, 4} and name.isupper():
        return name
    return TEAM_MAP.get(name)


def fetch_scores(days_from: int) -> List[Dict[str, Any]]:
    api_key = os.environ.get("ODDS_API_KEY")
    if not api_key:
        raise SystemExit("ODDS_API_KEY not set")
    params = {"daysFrom": str(days_from), "apiKey": api_key, "dateFormat": "iso"}
    url = "https://api.the-odds-api.com/v4/sports/basketball_nba/scores/?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            data = resp.read()
            return json.loads(data)
    except urllib.error.HTTPError as e:
        raise SystemExit(f"HTTP error fetching scores: {e}") from e
    except Exception as e:
        raise SystemExit(f"Failed to fetch scores: {e}") from e


def build_game_key(commence_time: str, away_team: str, home_team: str) -> Optional[str]:
    try:
        dt = datetime.fromisoformat(commence_time.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None
    away = to_abbr(away_team)
    home = to_abbr(home_team)
    if not away or not home:
        return None
    return f"NBA:{dt.year:04d}:{dt.month:02d}:{dt.day:02d}:{away}@{home}"


def build_day_key(commence_time: str, tz: ZoneInfo) -> Optional[str]:
    try:
        dt_utc = datetime.fromisoformat(commence_time.replace("Z", "+00:00")).astimezone(timezone.utc)
        dt_tz = dt_utc.astimezone(tz)
    except Exception:
        return None
    return f"NBA:{dt_tz.year:04d}:{dt_tz.month:02d}:{dt_tz.day:02d}"


def normalize_score(entry: Dict[str, Any], tz: ZoneInfo, debug: bool = False, debug_cap: list[int] = None) -> Optional[Dict[str, Any]]:
    if not entry.get("completed"):
        return None
    scores = entry.get("scores") or []
    score_map = {s.get("name"): int(s.get("score")) for s in scores if s.get("name") and s.get("score") is not None}
    away_team = entry.get("away_team")
    home_team = entry.get("home_team")
    away_abbr = to_abbr(away_team)
    home_abbr = to_abbr(home_team)
    if not away_abbr or not home_abbr:
        return None
    game_key_core = build_game_key(entry.get("commence_time"), away_team, home_team)
    day_key = build_day_key(entry.get("commence_time"), tz)
    canonical_game_key = ["NBA", day_key, away_abbr, home_abbr] if day_key else None
    if not game_key_core or not canonical_game_key:
        return None
    if debug and debug_cap is not None and len(debug_cap) < 5:
        debug_cap.append(1)
        print(f"[DEBUG] commence_utc={entry.get('commence_time')} day_key_ET={day_key}")
    return {
        "sport": "NBA",
        "day_key": day_key,
        "game_key_core": game_key_core,
        "home_team": home_abbr,
        "away_team": away_abbr,
        "home_score": score_map.get(home_team),
        "away_score": score_map.get(away_team),
        "final": True,
        "commence_time": entry.get("commence_time"),
        "canonical_game_key": canonical_game_key,
    }


def dedupe(existing: List[Dict[str, Any]], new_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged = {}
    for row in existing:
        key = tuple(row.get("canonical_game_key")) if isinstance(row.get("canonical_game_key"), list) else None
        if key:
            merged[key] = row
    for row in new_rows:
        key = tuple(row.get("canonical_game_key")) if isinstance(row.get("canonical_game_key"), list) else None
        if not key:
            continue
        merged[key] = row
    # sort by commence_time descending for readability
    return sorted(merged.values(), key=lambda r: r.get("commence_time") or "", reverse=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch NBA results from The Odds API")
    ap.add_argument("--days", type=int, default=3, help="Look back N days (default 3)")
    ap.add_argument("--debug", action="store_true", help="Print sample day_key derivations")
    args = ap.parse_args()

    tz_et = ZoneInfo("America/New_York")
    raw_scores = fetch_scores(args.days)
    debug_cap: list[int] = []
    normalized = [r for r in (normalize_score(s, tz_et, args.debug, debug_cap) for s in raw_scores) if r]
    existing = read_jsonl(RESULTS_PATH)
    deduped = dedupe(existing, normalized)
    write_jsonl(RESULTS_PATH, deduped)
    print(f"Fetched {len(normalized)} completed games. Stored {len(deduped)} total records at {RESULTS_PATH}")


if __name__ == "__main__":
    main()
