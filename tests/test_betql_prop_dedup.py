from src.normalizer_betql_nba import normalize_props_file
from store import write_json
import os
import json


def test_betql_prop_dedup(tmp_path):
    raw_records = [
        {
            "player_name": "LeBron James",
            "stat": "Points",
            "direction": "OVER",
            "line": 23.5,
            "rating_stars": 4,
            "away_team": "LAL",
            "home_team": "CLE",
            "market_family": "player_prop",
            "observed_at_utc": "2026-01-28T00:00:00Z",
            "matchup_hint": "LAL@CLE",
        },
        {
            "player_name": "LeBron James",
            "stat": "Points",
            "direction": "OVER",
            "line": 24.0,
            "rating_stars": 4,
            "away_team": "LAL",
            "home_team": "CLE",
            "market_family": "player_prop",
            "observed_at_utc": "2026-01-28T00:00:00Z",
            "matchup_hint": "LAL@CLE",
        },
    ]
    raw_path = tmp_path / "raw.json"
    out_path = tmp_path / "norm.json"
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(raw_records, f)

    normalized = normalize_props_file(str(raw_path), str(out_path), debug=False)
    assert len(normalized) == 1
    market = normalized[0]["market"]
    assert market["player_key"] == "NBA:lebron_james"
    assert market["stat_key"] == "points"
    assert market["selection"] == "NBA:lebron_james::points::OVER"

