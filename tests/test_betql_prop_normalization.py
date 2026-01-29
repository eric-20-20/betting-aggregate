import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from betql_utils import slugify_player_name
from src.normalizer_betql_nba import normalize_betql_prop_record


def _build_raw(raw_pick_text: str, stat: str, direction: str) -> dict:
    return {
        "away_team": "LAL",
        "home_team": "CLE",
        "matchup_hint": "LAL@CLE",
        "observed_at_utc": "2026-01-28T22:43:43.351147+00:00",
        "market_family": "player_prop",
        "raw_pick_text": raw_pick_text,
        "stat": stat,
        "direction": direction,
        "line": 10.5,
        "source_id": "betql",
        "source_surface": "betql_prop_picks",
        "sport": "NBA",
    }


@pytest.mark.parametrize(
    "raw_text,stat,direction,expected_slug,expected_stat",
    [
        ("LAL@CLE Marcus Smart Under 10.5 Points (+102)", "Points", "UNDER", "marcus_smart", "points"),
        ("Luka Dončić Under 31.5 Points", "Points", "UNDER", "luka_doncic", "points"),
        ("De'Anthony Melton Over 1.5 Threes", "Made Threes", "OVER", "deanthony_melton", "threes"),
        ("O.G. Anunoby Over 2.5 Threes", "3pt", "OVER", "og_anunoby", "threes"),
        ("J.Brunson Under 24.5 Points", "Points", "UNDER", "j_brunson", "points"),
        ("Marcus Smart Over 7.5 Points+Rebounds+Assists", "points_rebounds_assists", "OVER", "marcus_smart", "pts_reb_ast"),
    ],
)
def test_betql_prop_player_and_stat_normalization(raw_text, stat, direction, expected_slug, expected_stat):
    raw = _build_raw(raw_text, stat, direction)
    rec = normalize_betql_prop_record(raw)
    market = rec["market"]

    assert rec["eligible_for_consensus"] is True
    assert market["player_key"] == f"NBA:{expected_slug}"
    assert market["stat_key"] == expected_stat
    assert market["selection"] == f"NBA:{expected_slug}::{expected_stat}::{direction}"


def test_slugify_player_name_preserves_letters():
    assert slugify_player_name("Luka Dončić") == "luka_doncic"
    assert slugify_player_name("De'Anthony Melton") == "deanthony_melton"
    assert slugify_player_name("O.G. Anunoby") == "og_anunoby"
