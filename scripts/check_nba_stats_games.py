"""Check NBA Stats games for a date using nba_api."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.results import nba_provider_nba_stats as prov  # type: ignore


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--refresh-cache", action="store_true")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    games = prov._games_from_scoreboard(args.date, refresh=args.refresh_cache)
    print(f"base=nba_api refresh_cache={args.refresh_cache} games={len(games)} cache_hits={prov.STATS['cache_hits']} fetches={prov.STATS['fetches']}")
    for g in games[:5]:
        print(
            {
                "game_id": g.get("GAME_ID"),
                "away": g.get("VISITORTEAMABBREVIATION"),
                "home": g.get("HOMETEAMABBREVIATION"),
                "status": g.get("GAME_STATUS_TEXT"),
            }
        )
    if args.debug:
        print("STATS:", prov.STATS)


if __name__ == "__main__":
    main()
