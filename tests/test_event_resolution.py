from datetime import datetime

from event_resolution import SCHEDULE, resolve_event
from store import SPORT


def test_valid_resolution():
    observed = datetime(2026, 1, 14, 0, 0)
    result = resolve_event(SPORT, away_team="NYK", home_team="LAL", observed_at_utc=observed)

    assert result is not None
    assert result["event_key"] == "NBA:20260114:NYK@LAL:0330"
    assert result["event_start_time_utc"] == datetime(2026, 1, 14, 3, 30)
    assert result["home_team"] == "LAL"
    assert result["away_team"] == "NYK"


def test_wrong_date_returns_none():
    observed = datetime(2026, 1, 10, 0, 0)
    result = resolve_event(SPORT, away_team="NYK", home_team="LAL", observed_at_utc=observed)
    assert result is None


def test_ambiguous_resolution_returns_none():
    duplicate_schedule = SCHEDULE + [
        # Same teams on the next day (within +1 day window)
        SCHEDULE[0].__class__(
            game_date="2026-01-15",
            home_team="LAL",
            away_team="NYK",
            start_time_utc=datetime(2026, 1, 15, 3, 30),
            event_key="NBA:20260115:NYK@LAL:0330",
        )
    ]

    observed = datetime(2026, 1, 14, 23, 0)
    result = resolve_event(SPORT, away_team="NYK", home_team="LAL", observed_at_utc=observed, schedule=duplicate_schedule)
    assert result is None
