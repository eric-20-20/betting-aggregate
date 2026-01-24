import datetime

from consensus_nba import group_soft


def make_standard_record(source: str, matchup: tuple[str, str], market_type: str, team: str, line, odds, expert_handle: str):
    away, home = matchup
    ev_key = f"NBA:20260130:{away}@{home}:2000"
    selection = f"{team}_{market_type}" if market_type != "total" else "game_total"
    side = team if market_type != "total" else ("over" if team == "OVER" else "under")
    return {
        "eligible_for_consensus": True,
        "event": {
            "event_key": ev_key,
            "away_team": away,
            "home_team": home,
            "event_start_time_utc": datetime.datetime.utcnow().isoformat(),
        },
        "market": {
            "market_type": market_type,
            "selection": selection,
            "side": side,
            "line": line,
            "odds": odds,
        },
        "provenance": {
            "source_id": source,
            "expert_handle": expert_handle,
            "raw_fingerprint": f"{source}-{expert_handle}-{selection}-{line}",
        },
    }


def test_soft_spread_clusters_within_tolerance():
    r1 = make_standard_record("action", ("BKN", "CHI"), "spread", "BKN", 8.0, -110, "exp1")
    r2 = make_standard_record("action", ("BKN", "CHI"), "spread", "BKN", 8.5, -112, "exp2")
    groups = group_soft([r1, r2])
    assert len(groups) == 1
    g = groups[0]
    assert g["market_type"] == "spread"
    assert g["selection"] == "BKN"
    assert g["source_strength"] == 1
    assert g["expert_strength"] == 2
    assert g["line_min"] == 8.0
    assert g["line_max"] == 8.5


def test_soft_spread_second_matchup():
    r1 = make_standard_record("action", ("NOP", "UTA"), "spread", "NOP", 6.0, -105, "exp1")
    r2 = make_standard_record("action", ("NOP", "UTA"), "spread", "NOP", 6.5, -108, "exp2")
    groups = group_soft([r1, r2])
    assert len(groups) == 1
    g = groups[0]
    assert g["line_min"] == 6.0
    assert g["line_max"] == 6.5
    assert g["selection"] == "NOP"


def test_soft_moneyline_ignores_odds():
    r1 = make_standard_record("action", ("DAL", "PHX"), "moneyline", "DAL", None, -150, "exp1")
    r2 = make_standard_record("action", ("DAL", "PHX"), "moneyline", "DAL", None, -130, "exp2")
    groups = group_soft([r1, r2])
    assert len(groups) == 1
    g = groups[0]
    assert g["market_type"] == "moneyline"
    assert g["selection"] == "DAL"
    assert g["source_strength"] == 1
    assert g["expert_strength"] == 2


def test_soft_suppressed_when_conflict_present():
    # TOR vs POR spread conflict within same source (two sides)
    r1 = make_standard_record("action", ("TOR", "POR"), "spread", "TOR", -2.0, -110, "exp1")
    r2 = make_standard_record("action", ("TOR", "POR"), "spread", "TOR", -2.5, -112, "exp2")
    r3 = make_standard_record("action", ("TOR", "POR"), "spread", "POR", +2.0, -108, "exp3")
    r4 = make_standard_record("action", ("TOR", "POR"), "spread", "POR", +2.5, -105, "exp4")
    groups = group_soft([r1, r2, r3, r4])
    # conflict should suppress all soft spread outputs for that day_key
    assert len([g for g in groups if g["market_type"] == "spread"]) == 0
