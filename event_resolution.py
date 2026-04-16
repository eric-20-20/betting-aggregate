from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional

from store import SPORT, NBA_SPORT


@dataclass
class ScheduledGame:
    game_date: str  # YYYY-MM-DD (UTC)
    home_team: str
    away_team: str
    start_time_utc: datetime
    event_key: str
    canonical_event_key: str  # New: canonical format


def build_event_key(start_time_utc: datetime, away_team: str, home_team: str, sport: str = SPORT) -> str:
    """Build legacy event key with timestamp (for backward compatibility)."""
    return f"{sport}:{start_time_utc:%Y%m%d}:{away_team}@{home_team}:{start_time_utc:%H%M}"


def build_canonical_event_key(start_time_utc: datetime, away_team: str, home_team: str, sport: str = SPORT) -> str:
    """Build canonical event key: SPORT:YYYY:MM:DD:AWAY@HOME."""
    return f"{sport}:{start_time_utc:%Y}:{start_time_utc:%m}:{start_time_utc:%d}:{away_team}@{home_team}"


def load_nba_schedule() -> List[ScheduledGame]:
    games: List[ScheduledGame] = []

    entries = [
        # After-midnight UTC example
        ("2026-01-14 03:30", "NYK", "LAL"),
        ("2026-01-14 20:00", "BOS", "MIA"),
        ("2026-01-15 01:00", "DEN", "UTA"),
        ("2026-01-15 02:00", "NYK", "GSW"),
        ("2026-01-15 01:30", "OKC", "HOU"),
    ]

    for start_str, away, home in entries:
        start_dt = datetime.strptime(start_str, "%Y-%m-%d %H:%M")
        games.append(
            ScheduledGame(
                game_date=start_dt.strftime("%Y-%m-%d"),
                home_team=home,
                away_team=away,
                start_time_utc=start_dt,
                event_key=build_event_key(start_dt, away_team=away, home_team=home, sport=NBA_SPORT),
                canonical_event_key=build_canonical_event_key(start_dt, away_team=away, home_team=home, sport=NBA_SPORT),
            )
        )

    return games


SCHEDULE: List[ScheduledGame] = load_nba_schedule()


def resolve_event(
    sport: str,
    away_team: str,
    home_team: str,
    observed_at_utc: datetime,
    schedule: Optional[List[ScheduledGame]] = None,
) -> Optional[dict]:
    # Only NBA has a built-in schedule (SCHEDULE/load_nba_schedule).
    # NCAAB and MLB callers synthesize event payloads in their ingest fallbacks
    # when this returns None.
    if sport != NBA_SPORT:
        return None

    games = schedule if schedule is not None else SCHEDULE

    candidate_dates = {
        observed_at_utc.date().strftime("%Y-%m-%d"),
        (observed_at_utc + timedelta(days=1)).date().strftime("%Y-%m-%d"),
    }

    matches = [
        game
        for game in games
        if game.game_date in candidate_dates
        and game.home_team == home_team
        and game.away_team == away_team
    ]

    if len(matches) != 1:
        return None

    game = matches[0]
    return {
        "event_key": game.event_key,
        "canonical_event_key": game.canonical_event_key,
        "event_start_time_utc": game.start_time_utc,
        "home_team": game.home_team,
        "away_team": game.away_team,
    }
