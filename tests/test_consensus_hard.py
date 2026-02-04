import datetime

import pytest

from consensus_nba import group_hard, day_key


def make_record(
    source: str,
    ev_key: str,
    away: str,
    home: str,
    market_type: str,
    selection: str,
    side: str,
    line,
    odds,
):
    return {
        "eligible_for_consensus": True,
        "event": {"event_key": ev_key, "away_team": away, "home_team": home, "event_start_time_utc": datetime.datetime.utcnow().isoformat()},
        "market": {"market_type": market_type, "selection": selection, "side": side, "line": line, "odds": odds},
        "provenance": {"source_id": source, "raw_fingerprint": f"{source}-{selection}"},
    }


def test_moneyline_overlap_best_odds():
    ev = "NBA:20260122:GSW@DAL:1700"
    rec_action = make_record("action", ev, "GSW", "DAL", "moneyline", "GSW_ml", "GSW", None, +120)
    rec_covers = make_record("covers", ev, "GSW", "DAL", "moneyline", "GSW moneyline", "GSW", None, +130)
    groups = group_hard([rec_action, rec_covers])
    assert len(groups) == 1
    g = groups[0]
    assert set(g["sources"]) == {"action", "covers"}
    assert g["source_strength"] == 2
    assert g["best_odds"] == 130 or g["best_odds"] == +130


def test_spread_bucket_strict():
    ev = "NBA:20260122:LAL@LAC:2000"
    r1 = make_record("action", ev, "LAL", "LAC", "spread", "LAL_spread", "LAL", -3.0, -110)
    r2 = make_record("covers", ev, "LAL", "LAC", "spread", "LAL_spread", "LAL", -3.0, -108)
    r3 = make_record("covers", ev, "LAL", "LAC", "spread", "LAL_spread", "LAL", -3.5, -105)
    groups = group_hard([r1, r2, r3])
    assert len(groups) == 1  # -3.0 and -3.5 within ±1.0 bucket together now
    g = groups[0]
    assert g["source_strength"] == 2


def test_total_over_under_bucket():
    ev = "NBA:20260123:BOS@NYK:1930"
    r1 = make_record("action", ev, "BOS", "NYK", "total", "OVER_225", "over", 225.0, -110)
    r2 = make_record("covers", ev, "BOS", "NYK", "total", "Over 225", "over", 225.1, -105)
    groups = group_hard([r1, r2])
    assert len(groups) == 1
    g = groups[0]
    assert g["line_bucket"] in (225.0, 225.5)  # rounding nearest 0.5
    assert g["source_strength"] == 2
    assert g["canonical_selection"] == "OVER"


def test_spread_bucket_overlap_only_if_bucket_matches():
    ev = "NBA:20260125:CHA@MIA:2000"
    r1 = make_record("action", ev, "CHA", "MIA", "spread", "CHA_spread", "CHA", +6.5, -110)
    r2 = make_record("covers", ev, "CHA", "MIA", "spread", "CHA_spread", "CHA", +6.0, -108)
    groups = group_hard([r1, r2])
    assert len(groups) == 1  # lines within ±1.0 now group
    g = groups[0]
    assert g["source_strength"] == 2


def test_moneyline_matches_across_sources_odds_differ():
    ev = "NBA:20260126:POR@DEN:1930"
    r1 = make_record("action", ev, "POR", "DEN", "moneyline", "POR_ml", "POR", None, +150)
    r2 = make_record("covers", ev, "POR", "DEN", "moneyline", "POR moneyline", "POR", None, +130)
    groups = group_hard([r1, r2])
    assert len(groups) == 1
    g = groups[0]
    assert g["source_strength"] == 2
    assert g["best_odds"] in (150, +150)


def test_total_over_under_matches_with_bucket():
    ev = "NBA:20260127:PHX@SAC:2000"
    r1 = make_record("action", ev, "PHX", "SAC", "total", "Under 222.8", "under", 222.8, -112)
    r2 = make_record("covers", ev, "PHX", "SAC", "total", "UNDER 223", "under", 223.0, -110)
    groups = group_hard([r1, r2])
    assert len(groups) == 1
    g = groups[0]
    assert g["source_strength"] == 2
    assert g["line_bucket"] == 223.0
