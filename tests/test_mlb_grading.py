"""Tests for MLB grading — provider, eligibility, and grade outcomes."""
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.results.mlb_provider_espn import (
    _to_espn_code,
    _from_espn_code,
    _ip_to_outs,
    _normalize_name,
    _compute_total_bases,
    _parse_stat_value,
    is_stat_supported,
    SUPPORTED_STATS,
)


# ---------------------------------------------------------------------------
# Team code mapping
# ---------------------------------------------------------------------------

class TestTeamCodeMapping:
    def test_canonical_to_espn(self):
        assert _to_espn_code("CHW") == "CWS"
        assert _to_espn_code("TBR") == "TB"
        assert _to_espn_code("SFG") == "SF"
        assert _to_espn_code("SDP") == "SD"
        assert _to_espn_code("KCR") == "KC"
        assert _to_espn_code("WSN") == "WSH"

    def test_espn_to_canonical(self):
        assert _from_espn_code("CWS") == "CHW"
        assert _from_espn_code("TB") == "TBR"
        assert _from_espn_code("SF") == "SFG"
        assert _from_espn_code("SD") == "SDP"
        assert _from_espn_code("KC") == "KCR"
        assert _from_espn_code("WSH") == "WSN"

    def test_passthrough_codes(self):
        for code in ["NYY", "LAD", "BOS", "ATL", "HOU", "CLE"]:
            assert _to_espn_code(code) == code
            assert _from_espn_code(code) == code

    def test_roundtrip(self):
        for our in ["CHW", "TBR", "SFG", "SDP", "KCR", "WSN"]:
            assert _from_espn_code(_to_espn_code(our)) == our


# ---------------------------------------------------------------------------
# Stat support
# ---------------------------------------------------------------------------

class TestStatSupport:
    def test_batting_stats_supported(self):
        for stat in ["hits", "runs", "rbi", "home_runs", "total_bases",
                      "doubles", "steals", "walks", "strikeouts"]:
            assert is_stat_supported(stat), f"{stat} should be supported"

    def test_pitching_stats_supported(self):
        for stat in ["hits_allowed", "earned_runs_allowed", "walks_allowed", "outs_recorded"]:
            assert is_stat_supported(stat), f"{stat} should be supported"

    def test_combo_stats_supported(self):
        assert is_stat_supported("total_bases")
        assert is_stat_supported("runs_hits_rbis")

    def test_unsupported_stat(self):
        assert not is_stat_supported("pitcher_win")
        assert not is_stat_supported("fantasy_points")
        assert not is_stat_supported("nonexistent")


# ---------------------------------------------------------------------------
# IP to outs conversion
# ---------------------------------------------------------------------------

class TestIPToOuts:
    def test_full_innings(self):
        assert _ip_to_outs("6.0") == 18.0
        assert _ip_to_outs("9.0") == 27.0

    def test_partial_innings(self):
        assert _ip_to_outs("5.1") == 16.0   # 5*3 + 1
        assert _ip_to_outs("5.2") == 17.0   # 5*3 + 2
        assert _ip_to_outs("6.2") == 20.0   # 6*3 + 2

    def test_zero(self):
        assert _ip_to_outs("0.0") == 0.0

    def test_invalid(self):
        assert _ip_to_outs("--") is None
        assert _ip_to_outs("") is None


# ---------------------------------------------------------------------------
# Name normalization
# ---------------------------------------------------------------------------

class TestNameNormalization:
    def test_basic(self):
        assert _normalize_name("Mike Trout") == "mike trout"

    def test_suffixes(self):
        assert _normalize_name("Luis Robert Jr.") == "luis robert"
        assert _normalize_name("Ronald Acuña Jr.") == "ronald acua"

    def test_punctuation(self):
        assert _normalize_name("J.P. Crawford") == "jp crawford"


# ---------------------------------------------------------------------------
# Total bases computation
# ---------------------------------------------------------------------------

class TestTotalBases:
    def test_basic_computation(self):
        labels = ["AB", "R", "H", "2B", "3B", "HR", "RBI"]
        stats = {2: "3", 3: "1", 4: "0", 5: "1"}  # H=3, 2B=1, 3B=0, HR=1
        # Singles = 3 - 1 - 0 - 1 = 1
        # TB = 1*1 + 1*2 + 0*3 + 1*4 = 7
        result = _compute_total_bases(stats, labels)
        assert result == 7.0

    def test_no_hits(self):
        labels = ["AB", "R", "H", "2B", "3B", "HR"]
        stats = {2: "0", 3: "0", 4: "0", 5: "0"}
        result = _compute_total_bases(stats, labels)
        assert result == 0.0

    def test_missing_hits(self):
        labels = ["AB", "R"]
        stats = {}
        result = _compute_total_bases(stats, labels)
        assert result is None  # Can't compute without H


# ---------------------------------------------------------------------------
# Stat value parsing
# ---------------------------------------------------------------------------

class TestParseStatValue:
    def test_plain_number(self):
        assert _parse_stat_value("3") == 3.0
        assert _parse_stat_value("0") == 0.0

    def test_made_attempted(self):
        assert _parse_stat_value("2-4") == 2.0  # 2 made of 4 attempts

    def test_dash(self):
        assert _parse_stat_value("--") is None

    def test_empty(self):
        assert _parse_stat_value("") is None


# ---------------------------------------------------------------------------
# Grading logic (unit-level, mocking provider)
# ---------------------------------------------------------------------------

class TestMLBGradeLogic:
    """Test the grade computation logic for MLB market types.

    These use the same grade_signal() function from grade_signals_nba.py
    but with MLB-specific parameters. We mock the provider to avoid
    network calls.
    """

    def _make_signal(self, market_type="moneyline", selection="NYY",
                     direction=None, line=None, away="NYY", home="BOS",
                     day_key="MLB:2026:04:25", atomic_stat=None,
                     player_id=None, odds=-110):
        ek = f"{day_key}:{away}@{home}"
        sig = {
            "signal_id": "test_signal",
            "signal_type": "hard_cross_source",
            "market_type": market_type,
            "selection": selection,
            "direction": direction,
            "line": line,
            "away_team": away,
            "home_team": home,
            "day_key": day_key,
            "event_key": ek,
            "atomic_stat": atomic_stat,
            "player_id": player_id,
            "best_odds": odds,
            "sources": ["action", "covers"],
            "sources_combo": "action|covers",
        }
        return sig

    def test_moneyline_win(self):
        """Selected team wins."""
        from scripts.grade_signals_nba import grade_signal, MLB_SPORT
        sig = self._make_signal(market_type="moneyline", selection="NYY",
                                away="NYY", home="BOS")
        mock_provider = MagicMock()
        mock_provider.fetch_game_result.return_value = {
            "home_score": 3, "away_score": 5, "status": "STATUS_FINAL",
            "espn_event_id": "1234",
            "games_info": {"provider": "espn_mlb", "date": "2026-04-25"},
        }
        result = grade_signal(sig, "2026-04-25T12:00:00Z", "2026-04-25",
                              False, "auto", sport=MLB_SPORT,
                              results_provider=mock_provider)
        assert result["status"] == "WIN"

    def test_moneyline_loss(self):
        from scripts.grade_signals_nba import grade_signal, MLB_SPORT
        sig = self._make_signal(market_type="moneyline", selection="NYY",
                                away="NYY", home="BOS")
        mock_provider = MagicMock()
        mock_provider.fetch_game_result.return_value = {
            "home_score": 5, "away_score": 3, "status": "STATUS_FINAL",
            "espn_event_id": "1234",
            "games_info": {"provider": "espn_mlb", "date": "2026-04-25"},
        }
        result = grade_signal(sig, "2026-04-25T12:00:00Z", "2026-04-25",
                              False, "auto", sport=MLB_SPORT,
                              results_provider=mock_provider)
        assert result["status"] == "LOSS"

    def test_spread_win(self):
        """NYY -1.5, NYY wins 5-3 (margin 2 > 1.5)."""
        from scripts.grade_signals_nba import grade_signal, MLB_SPORT
        sig = self._make_signal(market_type="spread", selection="NYY",
                                line=-1.5, away="NYY", home="BOS")
        mock_provider = MagicMock()
        mock_provider.fetch_game_result.return_value = {
            "home_score": 3, "away_score": 5, "status": "STATUS_FINAL",
            "espn_event_id": "1234",
            "games_info": {"provider": "espn_mlb", "date": "2026-04-25"},
        }
        result = grade_signal(sig, "2026-04-25T12:00:00Z", "2026-04-25",
                              False, "auto", sport=MLB_SPORT,
                              results_provider=mock_provider)
        assert result["status"] == "WIN"

    def test_spread_loss(self):
        """NYY -1.5, NYY wins 4-3 (margin 1 < 1.5)."""
        from scripts.grade_signals_nba import grade_signal, MLB_SPORT
        sig = self._make_signal(market_type="spread", selection="NYY",
                                line=-1.5, away="NYY", home="BOS")
        mock_provider = MagicMock()
        mock_provider.fetch_game_result.return_value = {
            "home_score": 3, "away_score": 4, "status": "STATUS_FINAL",
            "espn_event_id": "1234",
            "games_info": {"provider": "espn_mlb", "date": "2026-04-25"},
        }
        result = grade_signal(sig, "2026-04-25T12:00:00Z", "2026-04-25",
                              False, "auto", sport=MLB_SPORT,
                              results_provider=mock_provider)
        assert result["status"] == "LOSS"

    def test_total_over_win(self):
        """OVER 7.5, total runs = 9."""
        from scripts.grade_signals_nba import grade_signal, MLB_SPORT
        sig = self._make_signal(market_type="total", selection="OVER",
                                direction="OVER", line=7.5,
                                away="NYY", home="BOS")
        mock_provider = MagicMock()
        mock_provider.fetch_game_result.return_value = {
            "home_score": 5, "away_score": 4, "status": "STATUS_FINAL",
            "espn_event_id": "1234",
            "games_info": {"provider": "espn_mlb", "date": "2026-04-25"},
        }
        result = grade_signal(sig, "2026-04-25T12:00:00Z", "2026-04-25",
                              False, "auto", sport=MLB_SPORT,
                              results_provider=mock_provider)
        assert result["status"] == "WIN"

    def test_total_under_win(self):
        """UNDER 7.5, total runs = 5."""
        from scripts.grade_signals_nba import grade_signal, MLB_SPORT
        sig = self._make_signal(market_type="total", selection="UNDER",
                                direction="UNDER", line=7.5,
                                away="NYY", home="BOS")
        mock_provider = MagicMock()
        mock_provider.fetch_game_result.return_value = {
            "home_score": 3, "away_score": 2, "status": "STATUS_FINAL",
            "espn_event_id": "1234",
            "games_info": {"provider": "espn_mlb", "date": "2026-04-25"},
        }
        result = grade_signal(sig, "2026-04-25T12:00:00Z", "2026-04-25",
                              False, "auto", sport=MLB_SPORT,
                              results_provider=mock_provider)
        assert result["status"] == "WIN"

    def test_total_push(self):
        """OVER 8.0, total runs = 8."""
        from scripts.grade_signals_nba import grade_signal, MLB_SPORT
        sig = self._make_signal(market_type="total", selection="OVER",
                                direction="OVER", line=8.0,
                                away="NYY", home="BOS")
        mock_provider = MagicMock()
        mock_provider.fetch_game_result.return_value = {
            "home_score": 5, "away_score": 3, "status": "STATUS_FINAL",
            "espn_event_id": "1234",
            "games_info": {"provider": "espn_mlb", "date": "2026-04-25"},
        }
        result = grade_signal(sig, "2026-04-25T12:00:00Z", "2026-04-25",
                              False, "auto", sport=MLB_SPORT,
                              results_provider=mock_provider)
        assert result["status"] == "PUSH"

    def test_pending_game(self):
        """Game not yet final."""
        from scripts.grade_signals_nba import grade_signal, MLB_SPORT
        sig = self._make_signal(market_type="moneyline", selection="NYY",
                                away="NYY", home="BOS",
                                day_key="MLB:2026:12:25")  # future date
        mock_provider = MagicMock()
        mock_provider.fetch_game_result.return_value = {
            "home_score": None, "away_score": None, "status": "STATUS_SCHEDULED",
            "espn_event_id": "1234",
            "games_info": {"provider": "espn_mlb", "date": "2026-12-25"},
        }
        result = grade_signal(sig, "2026-04-25T12:00:00Z", "2026-12-25",
                              False, "auto", sport=MLB_SPORT,
                              results_provider=mock_provider)
        assert result["status"] == "PENDING"

    def test_player_prop_over_win(self):
        """Player prop: Mike Trout OVER 1.5 total_bases, actual = 3."""
        from scripts.grade_signals_nba import grade_signal, MLB_SPORT
        sig = self._make_signal(
            market_type="player_prop",
            selection="MLB:mike_trout::total_bases::OVER",
            direction="OVER",
            line=1.5,
            atomic_stat="total_bases",
            player_id="MLB:mike_trout",
            away="LAA", home="CHW",
        )
        mock_provider = MagicMock()
        mock_provider.fetch_game_result.return_value = {
            "home_score": 3, "away_score": 5, "status": "STATUS_FINAL",
            "espn_event_id": "5678",
            "games_info": {"provider": "espn_mlb", "date": "2026-04-25"},
        }
        mock_provider.fetch_player_statline.return_value = {
            "value": 3.0,
            "player_id": "12345",
            "meta": {"match_method": "name_match", "espn_name": "Mike Trout"},
        }
        result = grade_signal(sig, "2026-04-25T12:00:00Z", "2026-04-25",
                              False, "auto", sport=MLB_SPORT,
                              results_provider=mock_provider)
        assert result["status"] == "WIN"
        assert result.get("stat_value") == 3.0

    def test_player_prop_under_win(self):
        """Player prop: Dylan Cease UNDER 5.5 strikeouts, actual = 4."""
        from scripts.grade_signals_nba import grade_signal, MLB_SPORT
        sig = self._make_signal(
            market_type="player_prop",
            selection="MLB:dylan_cease::strikeouts::UNDER",
            direction="UNDER",
            line=5.5,
            atomic_stat="strikeouts",
            player_id="MLB:dylan_cease",
            away="SDP", home="CHC",
        )
        mock_provider = MagicMock()
        mock_provider.fetch_game_result.return_value = {
            "home_score": 4, "away_score": 2, "status": "STATUS_FINAL",
            "espn_event_id": "9999",
            "games_info": {"provider": "espn_mlb", "date": "2026-04-25"},
        }
        mock_provider.fetch_player_statline.return_value = {
            "value": 4.0,
            "player_id": "67890",
            "meta": {"match_method": "name_match", "espn_name": "Dylan Cease"},
        }
        result = grade_signal(sig, "2026-04-25T12:00:00Z", "2026-04-25",
                              False, "auto", sport=MLB_SPORT,
                              results_provider=mock_provider)
        assert result["status"] == "WIN"
        assert result.get("stat_value") == 4.0


# ---------------------------------------------------------------------------
# Eligibility checks
# ---------------------------------------------------------------------------

class TestMLBEligibility:
    def test_total_line_minimum(self):
        """MLB totals with line < 3.0 should be ineligible."""
        from scripts.grade_signals_nba import check_eligibility, MLB_SPORT
        eligible, reason, _ = check_eligibility(
            {"market_type": "total", "line": 2.5, "direction": "OVER",
             "day_key": "MLB:2026:04:25", "away_team": "NYY", "home_team": "BOS",
             "event_key": "MLB:2026:04:25:NYY@BOS"},
            "2026-04-25", MLB_SPORT,
        )
        assert not eligible
        assert reason == "implausible_total_line"

    def test_total_line_valid(self):
        """MLB totals with line >= 3.0 should pass."""
        from scripts.grade_signals_nba import check_eligibility, MLB_SPORT
        eligible, reason, _ = check_eligibility(
            {"market_type": "total", "line": 7.5, "direction": "OVER",
             "day_key": "MLB:2026:04:25", "away_team": "NYY", "home_team": "BOS",
             "event_key": "MLB:2026:04:25:NYY@BOS"},
            "2026-04-25", MLB_SPORT,
        )
        assert eligible

    def test_nba_total_line_unchanged(self):
        """NBA total line minimum should still be 150."""
        from scripts.grade_signals_nba import check_eligibility, NBA_SPORT
        eligible, reason, _ = check_eligibility(
            {"market_type": "total", "line": 140, "direction": "OVER",
             "day_key": "NBA:2026:04:25", "away_team": "BOS", "home_team": "MIA",
             "event_key": "NBA:2026:04:25:BOS@MIA"},
            "2026-04-25", NBA_SPORT,
        )
        assert not eligible
        assert reason == "implausible_total_line"
