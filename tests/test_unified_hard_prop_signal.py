import datetime

from consensus_nba import build_unified_signals, group_hard, group_soft, group_within_source


def make_prop_record(source: str, selection: str, player_key: str, line: float, odds: float, direction: str = "under", event_key: str = None):
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


def test_unified_hard_prop_fields_present():
    r1 = make_prop_record("action", "t_murphy::points::OVER", "t_murphy", 20.5, -110, direction="over")
    r2 = make_prop_record("covers", "trey_murphy_projection::points::OVER", "trey_murphy_projection", 21.0, -112, direction="over")
    hard = group_hard([r1, r2])
    soft = group_soft([])
    within = group_within_source([])
    unified = build_unified_signals(hard, soft, within)
    hard_signals = [u for u in unified if u.get("signal_type") == "hard_cross_source" and u.get("market_type") == "player_prop"]
    assert len(hard_signals) == 1
    sig = hard_signals[0]
    assert sig["player_id"] == "t_murphy"
    assert sig["atomic_stat"] == "points"
    assert sig["direction"].upper() == "OVER"
    assert sig["selection"] == "t_murphy::points::OVER"
    assert sig["lines"] == [20.5, 21.0] or sig["lines"] == [20.5, 21.0][::-1]
    assert sig["line_min"] == 20.5
    assert sig["line_max"] == 21.0
    assert sig["sources"] == ["action", "covers"] or set(sig["sources"]) == {"action", "covers"}
    assert sig.get("odds_list")
    assert sig.get("best_odds") is not None
