"""Tests for OddsTrader MLB normalization — team code canonicalization and matchup_key."""
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from oddstrader_ingest import normalize_pick, _canonicalize_mlb_team, _MLB_TEAM_CANON
from store import MLB_SPORT, NBA_SPORT


def _make_raw_ot_pick(
    away="CLE", home="TB", market_type="total", side="UNDER",
    selection="UNDER", line=8.0, event_time="2026-04-27T22:10:00+00:00",
    stars=5,
):
    return {
        "home_team": home,
        "away_team": away,
        "market_type": market_type,
        "side": side,
        "selection": selection,
        "line": line,
        "odds": -110,
        "event_time_utc": event_time,
        "event_id": None,
        "rating_stars": stars,
        "source_surface": f"oddstrader_ai_{market_type}",
        "ev_pct": 5.0,
        "cover_prob": 55.0,
    }


# ---------------------------------------------------------------------------
# _canonicalize_mlb_team: unit tests
# ---------------------------------------------------------------------------

class TestCanonicalizeMLBTeam:
    def test_tb_to_tbr(self):
        assert _canonicalize_mlb_team("TB") == "TBR"

    def test_cws_to_chw(self):
        assert _canonicalize_mlb_team("CWS") == "CHW"

    def test_sf_to_sfg(self):
        assert _canonicalize_mlb_team("SF") == "SFG"

    def test_sd_to_sdp(self):
        assert _canonicalize_mlb_team("SD") == "SDP"

    def test_kc_to_kcr(self):
        assert _canonicalize_mlb_team("KC") == "KCR"

    def test_wsh_to_wsn(self):
        assert _canonicalize_mlb_team("WSH") == "WSN"

    def test_ath_to_oak(self):
        assert _canonicalize_mlb_team("ATH") == "OAK"

    def test_canonical_passthrough(self):
        """Codes already canonical should pass through unchanged."""
        for code in ["TBR", "CHW", "SFG", "SDP", "KCR", "WSN", "OAK", "NYY", "LAD", "BOS"]:
            assert _canonicalize_mlb_team(code) == code

    def test_lowercase_input(self):
        assert _canonicalize_mlb_team("tb") == "TBR"
        assert _canonicalize_mlb_team("cws") == "CHW"

    def test_none_passthrough(self):
        assert _canonicalize_mlb_team(None) is None

    def test_empty_passthrough(self):
        assert _canonicalize_mlb_team("") == ""


# ---------------------------------------------------------------------------
# normalize_pick: MLB team codes in event dict
# ---------------------------------------------------------------------------

class TestNormalizePickMLBTeamCodes:
    def test_total_teams_canonicalized(self):
        raw = _make_raw_ot_pick(away="CLE", home="TB", market_type="total")
        rec = normalize_pick(raw, sport=MLB_SPORT)
        ev = rec["event"]
        assert ev["away_team"] == "CLE"
        assert ev["home_team"] == "TBR", f"Expected TBR, got {ev['home_team']}"

    def test_spread_teams_and_selection_canonicalized(self):
        raw = _make_raw_ot_pick(
            away="CLE", home="TB", market_type="spread",
            side="TB", selection="TB",
        )
        rec = normalize_pick(raw, sport=MLB_SPORT)
        ev = rec["event"]
        mk = rec["market"]
        assert ev["home_team"] == "TBR"
        assert mk["side"] == "TBR", f"Expected TBR, got {mk['side']}"
        assert mk["selection"] == "TBR", f"Expected TBR, got {mk['selection']}"

    def test_moneyline_selection_canonicalized(self):
        raw = _make_raw_ot_pick(
            away="SF", home="PHI", market_type="moneyline",
            side="SF", selection="SF",
        )
        rec = normalize_pick(raw, sport=MLB_SPORT)
        ev = rec["event"]
        mk = rec["market"]
        assert ev["away_team"] == "SFG"
        assert mk["side"] == "SFG"
        assert mk["selection"] == "SFG"

    def test_all_short_codes_in_event(self):
        """Every mapped code should canonicalize when used as away or home."""
        for short, canon in _MLB_TEAM_CANON.items():
            raw = _make_raw_ot_pick(away=short, home="NYY", market_type="total")
            rec = normalize_pick(raw, sport=MLB_SPORT)
            assert rec["event"]["away_team"] == canon, f"{short} should map to {canon}"

    def test_canonical_codes_passthrough(self):
        """Already-canonical codes should not be altered."""
        raw = _make_raw_ot_pick(away="CHW", home="LAA", market_type="total")
        rec = normalize_pick(raw, sport=MLB_SPORT)
        assert rec["event"]["away_team"] == "CHW"
        assert rec["event"]["home_team"] == "LAA"


# ---------------------------------------------------------------------------
# normalize_pick: event_key, day_key, matchup_key
# ---------------------------------------------------------------------------

class TestNormalizePickMLBKeys:
    def test_event_key_uses_canonical_codes(self):
        raw = _make_raw_ot_pick(away="CLE", home="TB")
        rec = normalize_pick(raw, sport=MLB_SPORT)
        assert rec["event"]["event_key"] == "MLB:2026:04:27:CLE@TBR"

    def test_day_key_present(self):
        raw = _make_raw_ot_pick()
        rec = normalize_pick(raw, sport=MLB_SPORT)
        assert rec["event"]["day_key"] == "MLB:2026:04:27"

    def test_matchup_key_present_and_sorted(self):
        raw = _make_raw_ot_pick(away="CLE", home="TB")
        rec = normalize_pick(raw, sport=MLB_SPORT)
        mk = rec["event"]["matchup_key"]
        assert mk is not None
        assert mk == "MLB:2026:04:27:CLE-TBR"

    def test_matchup_key_order_invariant(self):
        """matchup_key should be the same regardless of away/home orientation."""
        raw1 = _make_raw_ot_pick(away="SF", home="PHI")
        raw2 = _make_raw_ot_pick(away="PHI", home="SF")
        rec1 = normalize_pick(raw1, sport=MLB_SPORT)
        rec2 = normalize_pick(raw2, sport=MLB_SPORT)
        assert rec1["event"]["matchup_key"] == rec2["event"]["matchup_key"]

    def test_matchup_key_matches_action_covers_format(self):
        """Format must be SPORT:YYYY:MM:DD:TEAM1-TEAM2 (sorted alphabetically)."""
        raw = _make_raw_ot_pick(away="STL", home="PIT")
        rec = normalize_pick(raw, sport=MLB_SPORT)
        mk = rec["event"]["matchup_key"]
        assert mk == "MLB:2026:04:27:PIT-STL"


# ---------------------------------------------------------------------------
# NBA/NCAAB behavior unchanged
# ---------------------------------------------------------------------------

class TestNBAUnchanged:
    def test_nba_teams_not_remapped(self):
        raw = _make_raw_ot_pick(
            away="GSW", home="LAL", market_type="moneyline",
            side="LAL", selection="LAL",
            event_time="2026-04-27T02:00:00+00:00",
        )
        rec = normalize_pick(raw, sport=NBA_SPORT)
        ev = rec["event"]
        mk = rec["market"]
        assert ev["away_team"] == "GSW"
        assert ev["home_team"] == "LAL"
        assert mk["side"] == "LAL"
        assert mk["selection"] == "LAL"

    def test_nba_event_key_format(self):
        raw = _make_raw_ot_pick(
            away="BOS", home="MIA",
            event_time="2026-04-27T23:30:00+00:00",
        )
        rec = normalize_pick(raw, sport=NBA_SPORT)
        assert rec["event"]["event_key"] == "NBA:2026:04:27:BOS@MIA"

    def test_nba_matchup_key_present(self):
        raw = _make_raw_ot_pick(
            away="BOS", home="MIA",
            event_time="2026-04-27T23:30:00+00:00",
        )
        rec = normalize_pick(raw, sport=NBA_SPORT)
        assert rec["event"]["matchup_key"] == "NBA:2026:04:27:BOS-MIA"
