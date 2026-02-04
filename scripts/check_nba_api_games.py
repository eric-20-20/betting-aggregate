"""Quick check for nba_api ScoreboardV2 games on a given date.

Usage:
  python3 scripts/check_nba_api_games.py --date 2026-01-26 [--refresh-cache] [--debug]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.results import nba_provider_nba_api as api  # type: ignore


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--refresh-cache", action="store_true", help="Ignore cached scoreboard for date.")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    games, meta = api.get_games_for_date(args.date, refresh_cache=args.refresh_cache)
    print(f"nba_api Scoreboard games_count={len(games)} used_cache={meta}")
    for g in games[:5]:
        print(
            f"{g.get('away_team_abbrev')}@{g.get('home_team_abbrev')} "
            f"{g.get('away_score')} - {g.get('home_score')} "
            f"status={g.get('status')} gameId={g.get('game_id')}"
        )
    if args.debug:
        print("STATS:", api.STATS)


if __name__ == "__main__":
    main()
