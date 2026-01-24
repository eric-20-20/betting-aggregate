import datetime

from consensus_nba import group_hard


def make_prop_record(source: str, selection: str, player_key: str, line: float, odds: float, direction: str = "over", event_key: str = None):
    ev = event_key or "NBA:20260128:DEN@LAL:2000"
    return {
        "eligible_for_consensus": True,
        "event": {
            "event_key": ev,
            "away_team": "DEN",
            "home_team": "LAL",
            "event_start_time_utc": datetime.datetime.utcnow().isoformat(),
        },
        "market": {
            "market_type": "player_prop",
            "selection": selection,
            "side": direction,
            "line": line,
            "stat_key": "points",
            "player_key": player_key,
            "odds": odds,
        },
        "provenance": {"source_id": source, "raw_fingerprint": f"{source}-{selection}"},
    }


def test_prop_lines_within_tolerance_match():
    r1 = make_prop_record("action", "t_murphy::points::OVER", "t_murphy", 20.5, -110, direction="over")
    r2 = make_prop_record(
        "covers",
        "trey_murphy_projection::points::OVER",
        "trey_murphy_projection",
        21.5,
        -112,
        direction="over",
    )
    groups = group_hard([r1, r2])
    props = [g for g in groups if g.get("market_type") == "player_prop"]
    assert len(props) == 1
    g = props[0]
    assert g["player_id"] == "t_murphy"
    assert g["source_strength"] == 2
    assert g["line_min"] == 20.5
    assert g["line_max"] == 21.5


def test_prop_same_line_different_odds_single_group():
    r1 = make_prop_record("action", "t_murphy::points::OVER", "t_murphy", 20.5, -110, direction="over")
    r2 = make_prop_record("covers", "t_murphy::points::OVER", "t_murphy", 20.5, -125, direction="over")
    groups = group_hard([r1, r2])
    props = [g for g in groups if g.get("market_type") == "player_prop"]
    assert len(props) == 1
    g = props[0]
    assert g["source_strength"] == 2
    assert g["line_min"] == 20.5
    assert g["line_max"] == 20.5


def test_prop_line_diff_greater_than_tolerance_splits():
    r1 = make_prop_record("action", "t_murphy::points::OVER", "t_murphy", 20.5, -110, direction="over")
    r2 = make_prop_record(
        "covers",
        "trey_murphy_projection::points::OVER",
        "trey_murphy_projection",
        22.0,
        -112,
        direction="over",
    )
    groups = group_hard([r1, r2])
    props = [g for g in groups if g.get("market_type") == "player_prop"]
    # lines differ by 1.5 > 1.0 tolerance, so no hard consensus cluster with >=2 sources
    assert len(props) == 0
