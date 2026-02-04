"""Quick check for API-Sports NBA games on a given date.

Usage:
  RESULTS_PROVIDER=api_sports API_SPORTS_KEY=... python3 scripts/check_api_sports_games.py --date 2026-01-26 [--refresh-cache] [--debug]
"""

from __future__ import annotations

import argparse
import sys
import os
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.results import nba_provider_api_sports as api  # type: ignore


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--refresh-cache", action="store_true", help="Ignore cached games_ file")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    print(f"API base: {api._base_url()} key_present={bool(os.environ.get('API_SPORTS_KEY'))} refresh_cache={args.refresh_cache}")
    league_id = api.get_nba_league_id()
    print(f"NBA league id: {league_id}")
    games, used_cache = api.get_games_by_date(args.date, refresh_cache=args.refresh_cache)
    print(f"games count={len(games)} used_cache={used_cache}")
    for g in games[:5]:
        teams = g.get("teams") or {}
        home = teams.get("home", {})
        away = teams.get("visitors") or teams.get("away") or {}
        print(f"game_id={g.get('id')} {away.get('name')} ({away.get('id')}) @ {home.get('name')} ({home.get('id')}) status={g.get('status')}")
    if args.debug:
        print("STATS:", api.STATS)


if __name__ == "__main__":
    main()
