import pathlib
import sys
from datetime import datetime, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.normalizer_nba import normalize_raw_record
from store import data_store


def _reset_store():
    data_store.reset()


def test_action_player_prop():
    _reset_store()
    raw = {
        "source_id": "action",
        "source_surface": "nba_game_expert_picks",
        "sport": "NBA",
        "market_family": "player_prop",
        "observed_at_utc": "2026-01-23T21:00:27.664977+00:00",
        "canonical_url": "https://www.actionnetwork.com/nba-game/pacers-thunder-score-odds-january-23-2026/263567",
        "raw_pick_text": "G.Antetokounmpo over 29.5 Pts -113",
        "raw_block": "G.Antetokounmpo o29.5 Pts -113",
        "raw_fingerprint": "deadbeef",
        "event_start_time_utc": "2026-01-23T22:00:00+00:00",
    }

    rec = normalize_raw_record(raw)

    assert rec["market"]["market_type"] == "player_prop"
    assert rec["market"]["side"] == "OVER"
    assert rec["market"]["line"] == 29.5
    assert rec["market"]["odds"] == -113
    assert rec["market"]["stat_key"] == "points"
    assert rec["eligible_for_consensus"] is True
    assert rec["event"]["event_key"].startswith("NBA:")


def test_covers_player_prop():
    _reset_store()
    raw = {
        "source_id": "covers",
        "source_surface": "nba_matchup_picks",
        "sport": "NBA",
        "market_family": "player_prop",
        "observed_at_utc": "2026-01-23T21:01:59.023932+00:00",
        "canonical_url": "https://www.covers.com/sport/basketball/nba/matchup/362683/picks",
        "raw_pick_text": "Josh Okogie over 4.5 Points Scored -120 Projection 6.66 (Over)",
        "raw_block": "Points Scored Josh Okogie o4.5 Points Scored (-120)",
        "raw_fingerprint": "cafebabe",
        "event_start_time_utc": None,
    }

    rec = normalize_raw_record(raw)

    assert rec["market"]["side"] == "OVER"
    assert rec["market"]["line"] == 4.5
    assert rec["market"]["odds"] == -120
    assert rec["market"]["stat_key"] == "points"
    assert rec["eligible_for_consensus"] is True
    assert rec["event"]["event_key"].startswith("NBA:")
