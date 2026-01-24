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


def test_lines_by_source_present():
    r1 = make_prop_record("action", "t_murphy::points::OVER", "t_murphy", 20.5, -110)
    r2 = make_prop_record("covers", "trey_murphy_projection::points::OVER", "trey_murphy_projection", 21.0, -112)
    groups = group_hard([r1, r2])
    props = [g for g in groups if g.get("market_type") == "player_prop"]
    assert len(props) == 1
    g = props[0]
    assert g["lines_by_source"] == {"action": [20.5], "covers": [21.0]}

