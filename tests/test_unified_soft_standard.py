import datetime

from consensus_nba import build_unified_signals, group_soft


def make_standard_record(source: str, away: str, home: str, team: str, line: float, odds: float, expert: str):
    ev_key = f"NBA:20260131:{away}@{home}:2000"
    return {
        "eligible_for_consensus": True,
        "event": {
            "event_key": ev_key,
            "away_team": away,
            "home_team": home,
            "event_start_time_utc": datetime.datetime.utcnow().isoformat(),
        },
        "market": {
            "market_type": "spread",
            "selection": f"{team}_spread",
            "side": team,
            "line": line,
            "odds": odds,
        },
        "provenance": {"source_id": source, "expert_handle": expert, "raw_fingerprint": f"{source}-{expert}-{team}-{line}"},
    }


def test_unified_soft_standard_market_type_and_lines():
    r1 = make_standard_record("action", "NOP", "MEM", "NOP", 5.5, -110, "exp1")
    r2 = make_standard_record("action", "NOP", "MEM", "NOP", 6.0, -112, "exp2")
    soft = group_soft([r1, r2])
    unified = build_unified_signals([], soft, [])
    soft_spread = [u for u in unified if u.get("signal_type") == "soft_standard"]
    assert len(soft_spread) == 1
    sig = soft_spread[0]
    assert sig["market_type"] == "spread"
    assert sig["selection"] == "NOP"
    assert sig["line_min"] == 5.5
    assert sig["line_max"] == 6.0
    assert sig["lines"] == [5.5, 6.0]
