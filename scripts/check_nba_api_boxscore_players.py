"""Inspect nba_api BoxScoreTraditionalV2 player rows for a given game.

Usage:
  python3 scripts/check_nba_api_boxscore_players.py --game-id 0022500660 [--refresh-cache] [--debug]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.results import nba_provider_nba_api as api  # type: ignore


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--game-id", required=True)
    ap.add_argument("--refresh-cache", action="store_true")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    print("DEBUG FLAG:", args.debug)

    rows = []
    meta = {}
    try:
        # use the provider's debug flag if supported
        rows, meta = api.get_boxscore_player_rows(
            args.game_id,
            date_str="unknown",
            refresh_cache=args.refresh_cache,
            debug=args.debug,
        )
    except Exception as e:
        print("EXCEPTION calling provider:", repr(e))
        meta = {"exception": repr(e), "fetch_failed": True}

    if args.debug:
        print("Meta:", meta)
        if meta.get("body_preview"):
            print("body_preview:", meta.get("body_preview"))

    print(
        f"used_cache_mem={meta.get('used_cache_mem')} "
        f"used_cache_file={meta.get('used_cache_file')} "
        f"fetch_failed={meta.get('fetch_failed')}"
    )
    print("resultSet names:", meta.get("resultset_names"))
    print(f"PlayerStats rows: {len(rows)}")
    for r in rows[:15]:
        print(
            {
                "PLAYER_NAME": r.get("PLAYER_NAME"),
                "PLAYER_ID": r.get("PLAYER_ID"),
                "TEAM": r.get("TEAM_ABBREVIATION"),
            }
        )

    if len(rows) == 0:
        print("PlayerStats not found")
        raise SystemExit(1)


if __name__ == "__main__":
    main()