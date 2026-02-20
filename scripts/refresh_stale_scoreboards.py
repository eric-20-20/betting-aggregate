#!/usr/bin/env python3
"""Refresh stale or incomplete NBA scoreboard cache files.

Scans data/cache/results/nba_api_scoreboard_*.json for files where:
  - Any game has GAME_STATUS_ID != 3 (stale/SCHEDULED) and date is in the past
  - Fewer than --min-games games are cached (incomplete)

Re-fetches from NBA API with configurable delays between requests.

Usage:
    python3 scripts/refresh_stale_scoreboards.py --dry-run
    python3 scripts/refresh_stale_scoreboards.py --delay 3
    python3 scripts/refresh_stale_scoreboards.py --include-incomplete --min-games 4
"""
from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime, date
from pathlib import Path

import requests

CACHE_DIR = Path("data/cache/results")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://stats.nba.com/",
    "Origin": "https://stats.nba.com",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
    "Connection": "keep-alive",
}

SCOREBOARD_URL = "https://stats.nba.com/stats/scoreboardv2"


def find_stale_scoreboards(include_incomplete: bool = False, min_games: int = 4) -> list[tuple[Path, str, int, int, str]]:
    """Find scoreboard cache files that need refreshing.

    Returns list of (path, date_str, total_games, stale_games, reason).
    """
    today = date.today()
    results = []

    for path in sorted(CACHE_DIR.glob("nba_api_scoreboard_*.json")):
        m = re.search(r"nba_api_scoreboard_(\d{4}-\d{2}-\d{2})\.json$", path.name)
        if not m:
            continue
        date_str = m.group(1)
        try:
            file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        if file_date >= today:
            continue

        try:
            data = json.loads(path.read_text())
        except Exception:
            continue

        result_sets = data.get("resultSets") or []
        if not result_sets:
            continue

        game_header = None
        for rs in result_sets:
            if isinstance(rs, dict) and rs.get("name") == "GameHeader":
                game_header = rs
                break
        if not game_header:
            continue

        headers = game_header.get("headers") or []
        rows = game_header.get("rowSet") or []
        if not headers or not rows:
            continue

        try:
            status_idx = headers.index("GAME_STATUS_ID")
        except ValueError:
            continue

        total_games = len(rows)
        stale_games = sum(1 for row in rows if row[status_idx] != 3)

        if stale_games > 0:
            results.append((path, date_str, total_games, stale_games, "stale"))
        elif include_incomplete and total_games < min_games:
            results.append((path, date_str, total_games, 0, "incomplete"))

    return results


def refresh_scoreboard(date_str: str, path: Path) -> bool:
    """Fetch fresh scoreboard from NBA API using direct requests with proper headers."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    formatted = dt.strftime("%m/%d/%Y")

    params = {
        "GameDate": formatted,
        "LeagueID": "00",
        "DayOffset": 0,
    }

    try:
        resp = requests.get(
            SCOREBOARD_URL,
            params=params,
            headers=HEADERS,
            timeout=(5, 30),
        )
        if resp.status_code != 200:
            print(f"  ERROR: status {resp.status_code} for {date_str}")
            return False
        data = resp.json()
    except Exception as e:
        print(f"  ERROR fetching {date_str}: {e}")
        return False

    if not data or not isinstance(data, dict):
        print(f"  ERROR: empty response for {date_str}")
        return False

    result_sets = data.get("resultSets")
    if not result_sets:
        print(f"  ERROR: no resultSets for {date_str}")
        return False

    # Write atomically
    tmp = path.with_suffix(".json.tmp")
    try:
        tmp.write_text(json.dumps(data))
        tmp.replace(path)
        return True
    except Exception as e:
        print(f"  ERROR writing {path}: {e}")
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        return False


def main():
    parser = argparse.ArgumentParser(description="Refresh stale/incomplete NBA scoreboard caches")
    parser.add_argument("--dry-run", action="store_true", help="Only scan, don't fetch")
    parser.add_argument("--delay", type=float, default=2.0, help="Seconds between requests (default: 2)")
    parser.add_argument("--include-incomplete", action="store_true", help="Also refresh caches with fewer than --min-games games")
    parser.add_argument("--min-games", type=int, default=4, help="Minimum games for a cache to be considered complete (default: 4)")
    args = parser.parse_args()

    targets = find_stale_scoreboards(
        include_incomplete=args.include_incomplete,
        min_games=args.min_games,
    )
    print(f"Found {len(targets)} scoreboard cache files to refresh:")
    for path, date_str, total, stale_count, reason in targets:
        if reason == "stale":
            print(f"  {date_str}: {stale_count}/{total} games not FINAL")
        else:
            print(f"  {date_str}: only {total} games cached (incomplete)")

    if args.dry_run:
        print("\nDry run — no changes made.")
        return

    if not targets:
        print("No caches to refresh.")
        return

    print(f"\nRefreshing {len(targets)} files with {args.delay}s delays...")
    success = 0
    failed = 0
    for i, (path, date_str, total, stale_count, reason) in enumerate(targets):
        print(f"  [{i+1}/{len(targets)}] Refreshing {date_str} ({reason})...", end=" ", flush=True)
        if refresh_scoreboard(date_str, path):
            print("OK")
            success += 1
        else:
            print("FAILED")
            failed += 1
            if failed >= 3:
                print("\n  3 consecutive failures — NBA API may be rate-limiting. Try again later.")
                break
        if i < len(targets) - 1:
            time.sleep(args.delay)

    print(f"\nDone: {success} refreshed, {failed} failed, {len(targets) - success - failed} skipped")


if __name__ == "__main__":
    main()
