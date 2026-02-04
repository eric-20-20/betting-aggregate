"""Check data.nba.net boxscore for a given game id."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.results import nba_provider_data_nba_net as prov  # type: ignore
from src.results.http_utils import http_get_with_retry  # type: ignore


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--game-id", help="gameId from scoreboard")
    ap.add_argument("--refresh-cache", action="store_true")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--verbose-http", action="store_true")
    args = ap.parse_args()

    if not args.game_id:
        print("Please provide --game-id (run the scoreboard check first).")
        raise SystemExit(1)
    day = args.date.replace("-", "")
    url = f"https://data.nba.net/prod/v1/{day}/{args.game_id}_boxscore.json"
    resp, err = http_get_with_retry(url, timeout=25, retries=2, backoff=0.6, allow_redirects=True)
    if err or resp is None:
        print(f"FAIL url={url} error={err}")
        raise SystemExit(1)
    if args.verbose_http or args.debug:
        print(f"HTTP status={resp.status_code} final_url={resp.url} content-type={resp.headers.get('content-type')}")
    try:
        data = resp.json()
    except Exception as exc:
        print(f"FAIL url={url} status={resp.status_code} json_error={exc} body_snippet={resp.text[:200]}")
        raise SystemExit(1)
    print(f"status={resp.status_code} keys={list(data.keys())[:5]} players={len(data.get('stats',{}).get('activePlayers',[]))}")
    if args.debug:
        print(f"cache_hits={prov.STATS['cache_hits']} fetches={prov.STATS['fetches']}")


if __name__ == "__main__":
    main()
