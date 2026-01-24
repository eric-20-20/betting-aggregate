import datetime

from consensus_nba import group_hard, group_soft


def make_prop_record(source: str, selection: str, player_key: str, event_key: str):
    return {
        "eligible_for_consensus": True,
        "event": {
            "event_key": event_key,
            "away_team": "DEN",
            "home_team": "LAL",
            "event_start_time_utc": datetime.datetime.utcnow().isoformat(),
        },
        "market": {
            "market_type": "player_prop",
            "selection": selection,
            "side": "under",
            "line": 18.5,
            "stat_key": "points",
            "player_key": player_key,
        },
        "provenance": {"source_id": source, "raw_fingerprint": f"{source}-{selection}"},
    }


def test_player_id_normalization_soft_single_source_only():
    ev = "NBA:20260128:DEN@LAL:2000"
    r1 = make_prop_record("action", "NBA:p_watson::points::UNDER", "NBA:Peyton_Watson", ev)
    # duplicate within same source counts toward count_total but keeps source_strength=1
    groups = group_soft([r1, r1])
    assert len(groups) == 1
    g = groups[0]
    assert g["player_id"] == "p_watson"
    assert set(g["sources"]) == {"action"}
    assert g["source_strength"] == 1
    assert g["count_total"] == 2


def test_soft_excludes_when_hard_overlap_exists():
    ev1 = "NBA:20260128:DEN@LAL:2000"
    ev2 = "NBA:20260128:DEN@LAL:2030"
    r1 = make_prop_record("action", "NBA:p_watson::points::UNDER", "NBA:Peyton_Watson", ev1)
    r2 = make_prop_record("covers", "peyton_watson_projection::points::UNDER", "peyton_watson_projection", ev2)
    hard = group_hard([r1, r2])
    hard_props = [g for g in hard if g.get("market_type") == "player_prop"]
    assert len(hard_props) == 1
    hard_ids = {
        (
            g.get("day_key"),
            g.get("matchup"),
            g.get("player_id"),
            g.get("atomic_stat"),
            g.get("direction") or g.get("side"),
            g.get("line_median"),
        )
        for g in hard_props
    }
    soft = group_soft([r1, r2], hard_prop_ids=hard_ids)
    assert len(soft) == 0
