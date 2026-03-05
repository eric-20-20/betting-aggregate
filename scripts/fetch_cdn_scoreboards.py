#!/usr/bin/env python3
"""Fetch NBA scoreboards from CDN and write cache files for the grader."""
import json
import requests
import sys

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Referer": "https://www.nba.com/",
}

CACHE_DIR = "data/cache/results"


def main():
    dates = sys.argv[1:]
    if not dates:
        print("Usage: python3 scripts/fetch_cdn_scoreboards.py 2026-02-21 2026-02-22 ...")
        sys.exit(1)

    # Fetch schedule
    r = requests.get(
        "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json",
        headers=HEADERS, timeout=15,
    )
    sched = r.json()
    game_dates = sched.get("leagueSchedule", {}).get("gameDates", [])

    # Build lookup: "02/21/2026" -> games list
    target_set = set()
    for d in dates:
        parts = d.split("-")
        target_set.add(f"{parts[1]}/{parts[2]}/{parts[0]}")

    for gd in game_dates:
        raw_date = gd["gameDate"].split()[0]
        if raw_date not in target_set:
            continue

        games = gd["games"]
        parts = raw_date.split("/")
        iso_date = f"{parts[2]}-{parts[0]}-{parts[1]}"

        game_headers = []
        line_scores = []

        for g in games:
            game_id = g.get("gameId")
            away = g.get("awayTeam", {})
            home = g.get("homeTeam", {})
            status_text = g.get("gameStatusText", "")
            status_id = 3 if "Final" in status_text else 1

            game_headers.append([
                iso_date + "T00:00:00", 0, game_id, status_id, status_text,
                iso_date.replace("-", "") + "/" + away.get("teamTricode", "") + home.get("teamTricode", ""),
                home.get("teamId", 0), away.get("teamId", 0), "2025",
                4 if status_id == 3 else 0,
                "", None, None, None, None, g.get("arenaName", ""), 0, 0,
            ])

            for team in [away, home]:
                line_scores.append([
                    iso_date + "T00:00:00", 0, game_id,
                    team.get("teamId", 0), team.get("teamTricode", ""),
                    team.get("teamName", ""), team.get("teamSlug", ""),
                    0, 0, 0, 0,
                    team.get("score", 0),
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                ])

        cache_data = {
            "resource": "scoreboardV2",
            "parameters": {"LeagueID": "00", "GameDate": raw_date, "DayOffset": 0},
            "resultSets": [
                {
                    "name": "GameHeader",
                    "headers": [
                        "GAME_DATE_EST", "GAME_SEQUENCE", "GAME_ID",
                        "GAME_STATUS_ID", "GAME_STATUS_TEXT", "GAMECODE",
                        "HOME_TEAM_ID", "VISITOR_TEAM_ID", "SEASON",
                        "LIVE_PERIOD", "LIVE_PC_TIME",
                        "NATL_TV_BROADCASTER_ABBREVIATION",
                        "HOME_TV_BROADCASTER_ABBREVIATION",
                        "AWAY_TV_BROADCASTER_ABBREVIATION",
                        "LIVE_PERIOD_TIME_BCAST", "ARENA_NAME",
                        "WH_STATUS", "WNBA_COMMISSIONER_FLAG",
                    ],
                    "rowSet": game_headers,
                },
                {
                    "name": "LineScore",
                    "headers": [
                        "GAME_DATE_EST", "GAME_SEQUENCE", "GAME_ID",
                        "TEAM_ID", "TEAM_ABBREVIATION", "TEAM_NAME",
                        "TEAM_SLUG", "PTS_QTR1", "PTS_QTR2", "PTS_QTR3",
                        "PTS_QTR4", "PTS", "FG_PCT", "FT_PCT", "FG3_PCT",
                        "AST", "REB", "TOV", "PTS_PAINT", "PTS_2ND_CHANCE",
                        "PTS_FB", "LARGEST_LEAD", "LEAD_CHANGES",
                        "TIMES_TIED", "TEAM_TURNOVERS", "TOTAL_TURNOVERS",
                    ],
                    "rowSet": line_scores,
                },
            ] + [{"name": f"placeholder_{i}", "headers": [], "rowSet": []} for i in range(8)],
        }

        cache_path = f"{CACHE_DIR}/nba_api_scoreboard_{iso_date}.json"
        with open(cache_path, "w") as f:
            json.dump(cache_data, f)

        final_count = sum(1 for g in games if "Final" in g.get("gameStatusText", ""))
        print(f"[cdn] {cache_path}: {len(games)} games ({final_count} final)")


if __name__ == "__main__":
    main()
