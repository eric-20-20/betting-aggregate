from src.normalizer_betql_nba import normalize_betql_prop_record


def test_betql_prop_ineligible_below_threshold():
    raw = {
        "player_name": "LeBron James",
        "stat": "Points",
        "direction": "OVER",
        "line": 23.5,
        "rating_stars": 2,
        "away_team": "LAL",
        "home_team": "CLE",
        "market_family": "player_prop",
    }
    rec = normalize_betql_prop_record(raw)
    assert rec["eligible_for_consensus"] is False
    assert rec["ineligibility_reason"] == "rating_below_threshold"


def test_betql_prop_eligible_at_threshold():
    raw = {
        "player_name": "LeBron James",
        "stat": "Points",
        "direction": "UNDER",
        "line": 23.5,
        "rating_stars": 4,
        "away_team": "LAL",
        "home_team": "CLE",
        "market_family": "player_prop",
    }
    rec = normalize_betql_prop_record(raw)
    assert rec["eligible_for_consensus"] is True
    assert rec["ineligibility_reason"] is None


def test_betql_prop_ineligible_missing_rating():
    raw = {
        "player_name": "Donovan Mitchell",
        "stat": "Points",
        "direction": "OVER",
        "line": 31.5,
        "away_team": "CLE",
        "home_team": "LAL",
        "market_family": "player_prop",
    }
    rec = normalize_betql_prop_record(raw)
    assert rec["eligible_for_consensus"] is False
    assert rec["ineligibility_reason"] == "missing_rating"
