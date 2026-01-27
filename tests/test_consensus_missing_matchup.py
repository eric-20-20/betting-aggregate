import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from consensus_nba import group_hard


def _mk_record(source_id: str):
    return {
        "event": {
            "event_key": "NBA:2026:01:23",
            "away_team": None,
            "home_team": None,
        },
        "market": {
            "market_type": "player_prop",
            "selection": "NBA:j_watson::points::UNDER",
            "player_key": "NBA:j_watson",
            "stat_key": "points",
            "line": 18.5,
            "odds": -115,
            "side": "UNDER",
        },
        "provenance": {
            "source_id": source_id,
            "canonical_url": f"https://example.com/{source_id}",
        },
        "eligible_for_consensus": True,
        "ineligibility_reason": None,
    }


def test_prop_hard_without_matchup():
    records = [_mk_record("action"), _mk_record("covers")]
    hard = group_hard(records)
    props = [g for g in hard if g.get("market_type") == "player_prop"]
    assert props, "expected at least one player_prop hard group"
    best = props[0]
    assert best.get("source_strength", 0) >= 2
    assert best.get("matchup") is None
