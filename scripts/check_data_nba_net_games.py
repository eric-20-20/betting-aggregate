"""Check data.nba.net scoreboard for a date."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.results import nba_provider_data_nba_net as prov  # type: ignore


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--refresh-cache", action="store_true")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--verbose-http", action="store_true", help="Print HTTP metadata")
    args = ap.parse_args()

    data, meta = prov.fetch_scoreboard_with_meta(
        args.date, refresh_cache=args.refresh_cache, verbose=args.verbose_http or args.debug
    )
    if data is None:
        print(f"FAIL date={args.date} tried={meta.get('tried')} last_status={meta.get('last_status')} error={meta.get('error')}")
        raise SystemExit(1)
    games = data.get("games") or []
    print(
        f"games_count={len(games)} cache_hits={prov.STATS['cache_hits']} fetches={prov.STATS['fetches']} used_cache={meta.get('used_cache')}"
    )
    for g in games[:5]:
        print(
            f"{g.get('vTeam', {}).get('triCode')}@{g.get('hTeam', {}).get('triCode')} id={g.get('gameId')} statusNum={g.get('statusNum')}"
        )
    if args.debug:
        print("stats", prov.STATS)


if __name__ == "__main__":
    main()
