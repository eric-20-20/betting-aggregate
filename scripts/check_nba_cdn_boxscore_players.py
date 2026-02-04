"""Inspect nba.com CDN liveData boxscore players for a given game.

Usage:
  python3 scripts/check_nba_cdn_boxscore_players.py --game-id 0022500660 [--refresh-cache] [--debug]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.results import nba_provider_nba_cdn as cdn  # type: ignore


def _player_name(p: dict) -> str:
    # Preferred: name is a plain string
    nm = p.get("name")
    if isinstance(nm, str) and nm.strip():
        return nm.strip()
    # Alternative: firstName / familyName keys
    fn = p.get("firstName") or ""
    ln = p.get("familyName") or ""
    if fn or ln:
        return f"{fn} {ln}".strip()
    # Fallback: dict with first/last
    if isinstance(nm, dict):
        f = nm.get("first") or ""
        l = nm.get("last") or ""
        if f or l:
            return f"{f} {l}".strip()
    return ""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--game-id", required=True)
    ap.add_argument("--refresh-cache", action="store_true")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--find", help="Player key to resolve against CDN players (uses provider resolver)")
    args = ap.parse_args()

    data, meta = cdn._fetch_boxscore_json(args.game_id, refresh_cache=args.refresh_cache)
    if meta:
        print(
            f"used_cache_file={meta.get('used_cache_file')} status={meta.get('status')} "
            f"fetch_failed={meta.get('fetch_failed')} elapsed={meta.get('elapsed_sec')} "
            f"home={meta.get('home_tricode')} away={meta.get('away_tricode')} "
            f"home_players={meta.get('home_players_count')} away_players={meta.get('away_players_count')} total_players={meta.get('total_players_count')}"
        )
    if not data or meta.get("fetch_failed"):
        print("Fetch failed")
        if args.debug:
            print("Meta:", meta)
        raise SystemExit(1)

    game = data.get("game") or {}
    players = []
    for side in ("homeTeam", "awayTeam"):
        team = game.get(side) or {}
        for p in team.get("players") or []:
            players.append(p)

    print(f"players count={len(players)}")
    for p in players[:10]:
        stats = p.get("statistics") or {}
        print(
            {
                "personId": p.get("personId"),
                "name": _player_name(p),
                "team": p.get("teamTricode"),
                "points": stats.get("points"),
                "reboundsTotal": stats.get("reboundsTotal"),
                "assists": stats.get("assists"),
            }
        )
    if args.debug:
        print("Meta:", meta)
    if args.find:
        player, match_meta = cdn._resolve_player(players, args.find)
        print("Find:", args.find)
        print("Requested norm:", cdn._normalize_key(args.find))
        norm_samples = [cdn._normalize_key(p.get("name", "")) for p in players[:5]]
        print("Norm sample (first 5):", norm_samples)
        print("Match player:", player)
        print("Match meta:", match_meta)


if __name__ == "__main__":
    main()
