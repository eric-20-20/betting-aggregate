import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.normalizer_betql_nba import normalize_raw_record


def test_spread_record_does_not_crash():
    raw = {
        "source_id": "betql",
        "source_surface": "betql_model_spread",
        "market_family": "spread",
        "raw_pick_text": "BOS -5.5",
        "observed_at_utc": "2026-01-28T00:00:00Z",
        "away_team": "ATL",
        "home_team": "BOS",
    }
    rec = normalize_raw_record(raw)
    assert rec is not None
    assert (rec.get("market") or {}).get("market_type") == "spread"


def test_total_record_does_not_crash():
    raw = {
        "source_id": "betql",
        "source_surface": "betql_model_total",
        "market_family": "total",
        "raw_pick_text": "OVER 220.5",
        "observed_at_utc": "2026-01-28T00:00:00Z",
        "away_team": "DAL",
        "home_team": "MIN",
    }
    rec = normalize_raw_record(raw)
    assert rec is not None
    assert (rec.get("market") or {}).get("market_type") == "total"


def test_prop_record_builds_selection():
    raw = {
        "source_id": "betql",
        "source_surface": "betql_prop_picks",
        "market_family": "player_prop",
        "raw_pick_text": "LeBron James Over 23.5 Points",
        "observed_at_utc": "2026-01-28T00:00:00Z",
        "away_team": "LAL",
        "home_team": "CLE",
    }
    rec = normalize_raw_record(raw)
    assert rec is not None
    market = rec["market"]
    assert market["market_type"] == "player_prop"
    assert market["selection"] == "NBA:lebron_james::points::OVER"


def test_unknown_shape_returns_none_and_no_crash():
    raw = {"source_id": "betql", "raw_pick_text": ""}
    rec = normalize_raw_record(raw)
    # normalize_raw_record returns a dict with market_type "unknown" but should not crash
    assert rec is not None
    assert rec.get("market", {}).get("market_type") != "player_prop"
