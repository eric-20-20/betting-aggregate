import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.normalizer_sportscapping_nba import normalize_raw_record  # noqa: E402
from store import data_store  # noqa: E402


def _reset_store():
    data_store.reset()


def test_spread_pick():
    _reset_store()
    raw = {
        "source_id": "sportscapping",
        "source_surface": "free_nba_picks",
        "sport": "NBA",
        "market_family": "spread",
        "observed_at_utc": "2026-01-26T00:00:00+00:00",
        "canonical_url": "https://www.sportscapping.com/free-nba-picks.html",
        "raw_pick_text": "Magic +6½ -110 at Buckeye",
        "raw_block": "NBA | Magic vs Cavs Play on: Magic +6½ -110 at Buckeye",
        "raw_fingerprint": "abc",
        "matchup_hint": "Magic vs Cavs",
        "source_updated_at_utc": None,
        "event_start_time_utc": "2026-01-26T00:00:00+00:00",
        "expert_name": "John Martin",
    }
    rec = normalize_raw_record(raw)
    assert rec["market"]["market_type"] == "spread"
    assert rec["market"]["selection"] == "ORL"
    assert rec["market"]["line"] == 6.5 or rec["market"]["line"] == +6.5
    assert rec["market"]["odds"] == -110
    assert rec["event"]["away_team"] == "ORL"
    assert rec["event"]["home_team"] == "CLE"
    assert rec["market"]["matchup"] == "ORL@CLE"
    assert rec["eligible_for_consensus"] is True


def test_moneyline_pick():
    _reset_store()
    raw = {
        "source_id": "sportscapping",
        "source_surface": "free_nba_picks",
        "sport": "NBA",
        "market_family": "moneyline",
        "observed_at_utc": "2026-01-26T00:00:00+00:00",
        "canonical_url": "https://www.sportscapping.com/free-nba-picks.html",
        "raw_pick_text": "Bulls +105",
        "raw_block": "Play on: Bulls +105",
        "raw_fingerprint": "ghi",
        "source_updated_at_utc": None,
        "event_start_time_utc": "2026-01-26T00:00:00+00:00",
        "expert_name": "John Martin",
    }
    rec = normalize_raw_record(raw)
    assert rec["market"]["market_type"] == "moneyline"
    assert rec["market"]["selection"] == "CHI"
    assert rec["market"]["line"] is None
    assert rec["market"]["odds"] == 105
    assert rec["eligible_for_consensus"] is True


def test_total_pick():
    _reset_store()
    raw = {
        "source_id": "sportscapping",
        "source_surface": "free_nba_picks",
        "sport": "NBA",
        "market_family": "total",
        "observed_at_utc": "2026-01-26T00:00:00+00:00",
        "canonical_url": "https://www.sportscapping.com/free-nba-picks.html",
        "raw_pick_text": "Over 224½ -110",
        "raw_block": "Play on: Over 224½ -110",
        "raw_fingerprint": "def",
        "source_updated_at_utc": None,
        "event_start_time_utc": "2026-01-26T00:00:00+00:00",
        "expert_name": "Jane Doe",
    }
    rec = normalize_raw_record(raw)
    assert rec["market"]["market_type"] == "total"
    assert rec["market"]["side"] == "OVER"
    assert rec["market"]["line"] == 224.5
    assert rec["eligible_for_consensus"] is True
