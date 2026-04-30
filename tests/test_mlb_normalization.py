"""Tests for MLB normalization — ensures MLB props use MLB: prefix, not NBA:."""
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.normalizer_bettingpros_props_mlb import normalize_bettingpros_props_record
from src.signal_keys import build_selection_key


# ---------------------------------------------------------------------------
# BettingPros props normalizer: MLB prefix
# ---------------------------------------------------------------------------

def _make_raw_mlb_prop(player="Mike Trout", stat="hits", side="OVER", line=0.5):
    return {
        "observed_at_utc": "2026-04-24T18:00:00+00:00",
        "event_start_time_utc": "2026-04-24T23:00:00+00:00",
        "away_team": "LAA",
        "home_team": "SEA",
        "market_type": "player_prop",
        "selection": None,
        "side": side,
        "line": line,
        "odds": -110,
        "player_name": player,
        "stat_key": stat,
        "projection_diff": 1.0,
        "source_surface": "bettingpros_prop_bets",
    }


def test_mlb_props_use_mlb_prefix():
    rec = normalize_bettingpros_props_record(_make_raw_mlb_prop(), sport="MLB")
    assert rec["eligible_for_consensus"]
    mk = rec["market"]
    assert mk["player_key"].startswith("MLB:"), f"Expected MLB: prefix, got {mk['player_key']}"
    assert mk["selection"].startswith("MLB:"), f"Expected MLB: prefix, got {mk['selection']}"
    assert "NBA:" not in mk["player_key"]
    assert "NBA:" not in mk["selection"]


def test_mlb_props_selection_format():
    rec = normalize_bettingpros_props_record(_make_raw_mlb_prop(), sport="MLB")
    mk = rec["market"]
    assert mk["selection"] == "MLB:mike_trout::hits::OVER"


def test_mlb_props_day_key_prefix():
    rec = normalize_bettingpros_props_record(_make_raw_mlb_prop(), sport="MLB")
    ev = rec["event"]
    assert ev["day_key"].startswith("MLB:"), f"Expected MLB: day_key, got {ev['day_key']}"


def test_nba_props_still_use_nba_prefix():
    """Prove NBA behavior is unchanged."""
    from src.normalizer_bettingpros_props_nba import normalize_bettingpros_props_record as nba_normalize
    raw = {
        "observed_at_utc": "2026-04-24T18:00:00+00:00",
        "event_start_time_utc": "2026-04-24T23:00:00+00:00",
        "away_team": "LAL",
        "home_team": "BOS",
        "market_type": "player_prop",
        "selection": None,
        "side": "OVER",
        "line": 25.5,
        "odds": -115,
        "player_name": "LeBron James",
        "stat_key": "points",
        "projection_diff": 3.0,
        "source_surface": "bettingpros_prop_bets",
    }
    rec = nba_normalize(raw, sport="NBA")
    assert rec["eligible_for_consensus"]
    mk = rec["market"]
    assert mk["player_key"].startswith("NBA:"), f"Expected NBA: prefix, got {mk['player_key']}"
    assert mk["selection"].startswith("NBA:"), f"Expected NBA: prefix, got {mk['selection']}"


# ---------------------------------------------------------------------------
# Signal keys: sport prefix stripping
# ---------------------------------------------------------------------------

def test_signal_key_strips_mlb_prefix():
    key1 = build_selection_key(
        day_key="MLB:2026:04:24",
        market_type="player_prop",
        selection="MLB:mike_trout::hits::OVER",
    )
    key2 = build_selection_key(
        day_key="MLB:2026:04:24",
        market_type="player_prop",
        player_id="mike_trout",
        atomic_stat="hits",
        direction="OVER",
    )
    assert key1 == key2, "MLB: prefix should be stripped so keys match"


def test_signal_key_strips_nba_prefix():
    key1 = build_selection_key(
        day_key="NBA:2026:04:24",
        market_type="player_prop",
        selection="NBA:lebron_james::points::OVER",
    )
    key2 = build_selection_key(
        day_key="NBA:2026:04:24",
        market_type="player_prop",
        player_id="lebron_james",
        atomic_stat="points",
        direction="OVER",
    )
    assert key1 == key2, "NBA: prefix should be stripped so keys match"


def test_signal_key_strips_ncaab_prefix():
    key1 = build_selection_key(
        day_key="NCAAB:2026:04:24",
        market_type="player_prop",
        selection="NCAAB:player_name::points::OVER",
    )
    key2 = build_selection_key(
        day_key="NCAAB:2026:04:24",
        market_type="player_prop",
        player_id="player_name",
        atomic_stat="points",
        direction="OVER",
    )
    assert key1 == key2, "NCAAB: prefix should be stripped so keys match"


# ---------------------------------------------------------------------------
# SportsLine MLB: matchup_key from raw_block
# ---------------------------------------------------------------------------

def test_sportsline_mlb_matchup_key_from_raw_block():
    """SportsLine MLB records should extract teams from raw_block when away/home are null."""
    from src.normalizer_sportsline_nba import normalize_sportsline_record
    rec = normalize_sportsline_record({
        "observed_at_utc": "2026-04-08T18:51:02+00:00",
        "event_start_time_utc": "2026-04-08T23:40:00+00:00",
        "away_team": None,
        "home_team": None,
        "matchup_hint": None,
        "raw_block": "Apr 08 2026, 4:40 pm PDT\nTigers\n@ Twins|Spread\nDetroit -1.5 +115\nUnit\n0.5||Analysis: blah",
        "market_type": "spread",
        "selection": "Detroit",
        "line": -1.5,
        "odds": 115,
        "raw_pick_text": "Spread\nDetroit -1.5 +115",
        "source_surface": "sportsline_expert_pages_backfill_v2",
    }, sport="MLB")
    ev = rec["event"]
    assert ev["away_team"] == "DET", f"Expected DET, got {ev['away_team']}"
    assert ev["home_team"] == "MIN", f"Expected MIN, got {ev['home_team']}"
    assert ev["matchup_key"] is not None, "matchup_key should not be None"
    assert "DET" in ev["matchup_key"]
    assert "MIN" in ev["matchup_key"]


def test_sportsline_mlb_with_scores_in_raw_block():
    """SportsLine MLB records with score lines should skip scores when extracting teams."""
    from src.normalizer_sportsline_nba import normalize_sportsline_record
    rec = normalize_sportsline_record({
        "observed_at_utc": "2026-04-05T18:00:00+00:00",
        "event_start_time_utc": "2026-04-05T20:00:00+00:00",
        "away_team": None,
        "home_team": None,
        "matchup_hint": None,
        "raw_block": "Apr 05 2026, 1:40 pm PDT\nCubs\n5\n@ Guardians\n6|Over/Under\nUnder 7.5 -105\nLOSS",
        "market_type": "total",
        "selection": "UNDER",
        "line": 7.5,
        "odds": -105,
        "raw_pick_text": "Over/Under\nUnder 7.5 -105",
        "source_surface": "sportsline_expert_pages_backfill_v2",
    }, sport="MLB")
    ev = rec["event"]
    assert ev["away_team"] == "CHC", f"Expected CHC, got {ev['away_team']}"
    assert ev["home_team"] == "CLE", f"Expected CLE, got {ev['home_team']}"
    assert ev["matchup_key"] is not None


def test_mlb_team_code_alignment():
    """BettingPros and DataStore must produce identical team codes for cross-source merging."""
    from src.normalizer_bettingpros_props_mlb import _map_mlb_team, MLB_TEAM_CODES
    from data_mlb import MLB_TEAM_SEED
    canonical = {t.code for t in MLB_TEAM_SEED}
    assert MLB_TEAM_CODES == canonical, f"Mismatch: BP-only={MLB_TEAM_CODES - canonical}, DataStore-only={canonical - MLB_TEAM_CODES}"
    # BettingPros API short codes must map to canonical
    assert _map_mlb_team("CWS") == "CHW"
    assert _map_mlb_team("SF") == "SFG"
    assert _map_mlb_team("KC") == "KCR"
    assert _map_mlb_team("SD") == "SDP"
    assert _map_mlb_team("TB") == "TBR"
    assert _map_mlb_team("WAS") == "WSN"
    # Canonical codes pass through
    assert _map_mlb_team("CHW") == "CHW"
    assert _map_mlb_team("SFG") == "SFG"


def test_sportsline_nba_unchanged():
    """NBA records should not trigger raw_block team parsing."""
    from src.normalizer_sportsline_nba import normalize_sportsline_record
    rec = normalize_sportsline_record({
        "observed_at_utc": "2026-04-08T18:51:02+00:00",
        "event_start_time_utc": "2026-04-08T23:40:00+00:00",
        "away_team": "LAL",
        "home_team": "BOS",
        "matchup_hint": None,
        "raw_block": "Apr 08 2026\nLakers\n@ Celtics|Spread\nBOS -5.5 -110",
        "market_type": "spread",
        "selection": "BOS",
        "line": -5.5,
        "odds": -110,
        "raw_pick_text": "Spread\nBOS -5.5 -110",
        "source_surface": "sportsline_expert_pages",
    }, sport="NBA")
    ev = rec["event"]
    assert ev["away_team"] == "LAL"
    assert ev["home_team"] == "BOS"
    assert ev["matchup_key"] is not None
