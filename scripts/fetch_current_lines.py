"""Fetch current NBA market lines (spreads, totals) from The Odds API.

Writes a simple JSON lookup file that score_signals.py can use to annotate
picks with the current market line.

Uses environment variable ODDS_API_KEY.

Run from repo root:
    ODDS_API_KEY=your_key python3 scripts/fetch_current_lines.py
    ODDS_API_KEY=your_key python3 scripts/fetch_current_lines.py --debug
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

ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")

OUTPUT_PATH = Path("data/cache/current_lines.json")

# Same team map as fetch_game_results_oddsapi.py
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


def to_abbr(name: str) -> Optional[str]:
    if not name:
        return None
    if len(name) in {2, 3, 4} and name.isupper():
        return name
    return TEAM_MAP.get(name)


def fetch_odds() -> List[Dict[str, Any]]:
    """Fetch current NBA spreads and totals from The Odds API."""
    if not ODDS_API_KEY:
        raise SystemExit("ODDS_API_KEY not set in environment")

    params = {
        "apiKey": ODDS_API_KEY,
        "regions": "us",
        "markets": "spreads,totals",
        "oddsFormat": "american",
    }
    url = "https://api.the-odds-api.com/v4/sports/basketball_nba/odds/?" + urllib.parse.urlencode(params)

    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=15) as resp:
            remaining = resp.headers.get("x-requests-remaining", "?")
            used = resp.headers.get("x-requests-used", "?")
            print(f"  API quota: {remaining} remaining, {used} used")
            data = resp.read()
            return json.loads(data)
    except urllib.error.HTTPError as e:
        body = e.read().decode() if hasattr(e, "read") else ""
        raise SystemExit(f"HTTP {e.code} fetching odds: {body}") from e
    except Exception as e:
        raise SystemExit(f"Failed to fetch odds: {e}") from e


def build_event_key(commence_time: str, away_abbr: str, home_abbr: str) -> Optional[str]:
    """Build an event_key matching the signal pipeline format: NBA:YYYY:MM:DD:AWAY@HOME."""
    try:
        dt_utc = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
        dt_et = dt_utc.astimezone(ZoneInfo("America/New_York"))
    except Exception:
        return None
    return f"NBA:{dt_et.year:04d}:{dt_et.month:02d}:{dt_et.day:02d}:{away_abbr}@{home_abbr}"


def extract_consensus_line(bookmakers: List[Dict], market_key: str) -> Optional[float]:
    """Get the consensus (median) line across bookmakers for a market.

    For spreads, returns the home team's spread point.
    For totals, returns the over/under point.
    """
    points: List[float] = []
    for bk in bookmakers:
        for mkt in bk.get("markets", []):
            if mkt.get("key") != market_key:
                continue
            for outcome in mkt.get("outcomes", []):
                pt = outcome.get("point")
                if pt is not None:
                    if market_key == "totals":
                        # Both Over and Under have the same point; take one
                        points.append(float(pt))
                        break
                    elif market_key == "spreads":
                        points.append(float(pt))

    if not points:
        return None

    # Median
    points.sort()
    n = len(points)
    if n % 2 == 1:
        return points[n // 2]
    return (points[n // 2 - 1] + points[n // 2]) / 2


def extract_team_spreads(bookmakers: List[Dict]) -> Dict[str, List[float]]:
    """Get spread points by team name across all bookmakers."""
    team_points: Dict[str, List[float]] = {}
    for bk in bookmakers:
        for mkt in bk.get("markets", []):
            if mkt.get("key") != "spreads":
                continue
            for outcome in mkt.get("outcomes", []):
                name = outcome.get("name", "")
                pt = outcome.get("point")
                if pt is not None:
                    team_points.setdefault(name, []).append(float(pt))
    return team_points


def process_events(events: List[Dict], debug: bool = False) -> Dict[str, Any]:
    """Process API events into a lookup dict keyed by event_key.

    Returns:
        {
            "NBA:2026:02:26:CLE@MIL": {
                "away_team": "CLE",
                "home_team": "MIL",
                "spreads": {"CLE": 4.5, "MIL": -4.5},
                "total": 220.0,
                "commence_time": "2026-02-27T00:10:00Z",
            },
            ...
        }
    """
    lines: Dict[str, Any] = {}

    for event in events:
        away_name = event.get("away_team", "")
        home_name = event.get("home_team", "")
        away_abbr = to_abbr(away_name)
        home_abbr = to_abbr(home_name)
        if not away_abbr or not home_abbr:
            if debug:
                print(f"  [skip] Unknown team: {away_name} / {home_name}")
            continue

        commence = event.get("commence_time", "")
        event_key = build_event_key(commence, away_abbr, home_abbr)
        if not event_key:
            continue

        bookmakers = event.get("bookmakers", [])

        # Spreads: get each team's consensus spread
        team_spreads = extract_team_spreads(bookmakers)
        spreads: Dict[str, float] = {}
        for team_name, pts in team_spreads.items():
            abbr = to_abbr(team_name)
            if abbr:
                pts.sort()
                n = len(pts)
                median = pts[n // 2] if n % 2 == 1 else (pts[n // 2 - 1] + pts[n // 2]) / 2
                spreads[abbr] = median

        # Total
        total = extract_consensus_line(bookmakers, "totals")

        entry = {
            "away_team": away_abbr,
            "home_team": home_abbr,
            "spreads": spreads,
            "total": total,
            "commence_time": commence,
        }
        lines[event_key] = entry

        if debug:
            spread_str = ", ".join(f"{k} {v:+.1f}" for k, v in spreads.items())
            total_str = f"{total}" if total else "n/a"
            print(f"  {event_key}: spread=[{spread_str}]  total={total_str}")

    return lines


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch current NBA lines from The Odds API")
    ap.add_argument("--debug", action="store_true", help="Print detailed output")
    args = ap.parse_args()

    print("Fetching current NBA lines from The Odds API...")
    events = fetch_odds()
    print(f"  Got {len(events)} events")

    lines = process_events(events, debug=args.debug)
    print(f"  Processed {len(lines)} games with lines")

    # Write output
    output = {
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "games": lines,
    }
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)
    print(f"  Written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
