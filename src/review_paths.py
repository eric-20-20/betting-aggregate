"""Shared path utilities for the editorial review workflow.

All review artifacts are stored in a dated directory layout:

    data/review/{sport_lower}/{YYYY-MM-DD}/
        review_slate.json
        review_slate.csv
        overrides.json          (optional, human-created)
        approved_slate.json

For NBA the sport_lower is 'nba', for NCAAB it is 'ncaab'.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import List, Optional

REVIEW_ROOT = Path("data/review")

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def validate_date(date_str: str) -> str:
    """Validate YYYY-MM-DD format. Returns the date string or exits."""
    if not DATE_RE.match(date_str):
        print(f"[review] ERROR: invalid date format '{date_str}' — expected YYYY-MM-DD")
        sys.exit(1)
    return date_str


def sport_key(sport: str) -> str:
    """Normalize sport to lowercase key used in paths."""
    return sport.lower()


def review_dir(sport: str, date: str) -> Path:
    """Return the dated review directory for a sport/date.

    e.g. data/review/nba/2026-04-20/
    """
    return REVIEW_ROOT / sport_key(sport) / date


def slate_json_path(sport: str, date: str) -> Path:
    return review_dir(sport, date) / "review_slate.json"


def slate_csv_path(sport: str, date: str) -> Path:
    return review_dir(sport, date) / "review_slate.csv"


def overrides_path(sport: str, date: str) -> Path:
    return review_dir(sport, date) / "overrides.json"


def approved_path(sport: str, date: str) -> Path:
    return review_dir(sport, date) / "approved_slate.json"


def plays_path(sport: str, date: str) -> Path:
    """Return the upstream plays file path for a sport/date."""
    if sport.upper() == "NCAAB":
        return Path("data/plays/ncaab") / f"plays_{date}.json"
    return Path("data/plays") / f"plays_{date}.json"


def plays_dir(sport: str) -> Path:
    """Return the plays directory for a sport."""
    if sport.upper() == "NCAAB":
        return Path("data/plays/ncaab")
    return Path("data/plays")


def private_export_path(sport: str, date: str) -> Path:
    """Return the private web export file path for a sport/date."""
    if sport.upper() == "NCAAB":
        return Path("web/data/private/ncaab") / f"picks_{date}.json"
    return Path("web/data/private") / f"picks_{date}.json"


def ensure_review_dir(sport: str, date: str) -> Path:
    """Create the dated review directory if needed. Returns the path."""
    d = review_dir(sport, date)
    d.mkdir(parents=True, exist_ok=True)
    return d


# ── Date discovery ────────────────────────────────────────────────────

def find_latest_plays_date(sport: str) -> Optional[str]:
    """Find the most recent plays file date for a sport."""
    pdir = plays_dir(sport)
    files = sorted(pdir.glob("plays_*.json"), reverse=True)
    for f in files:
        d = f.stem.replace("plays_", "")
        if DATE_RE.match(d):
            return d
    return None


def find_all_plays_dates(sport: str) -> List[str]:
    """Return all dates with plays files, sorted ascending."""
    pdir = plays_dir(sport)
    dates = []
    for f in pdir.glob("plays_*.json"):
        d = f.stem.replace("plays_", "")
        if DATE_RE.match(d):
            dates.append(d)
    return sorted(dates)


def find_latest_slate_date(sport: str) -> Optional[str]:
    """Find the most recent slate file date for a sport."""
    root = REVIEW_ROOT / sport_key(sport)
    if not root.exists():
        return None
    # Look for dated subdirectories with review_slate.json
    dates = []
    for d in root.iterdir():
        if d.is_dir() and DATE_RE.match(d.name) and (d / "review_slate.json").exists():
            dates.append(d.name)
    return max(dates) if dates else None


def find_dates_with_slates(sport: str) -> List[str]:
    """Return all dates that have review slates, sorted ascending."""
    root = REVIEW_ROOT / sport_key(sport)
    if not root.exists():
        return []
    dates = []
    for d in root.iterdir():
        if d.is_dir() and DATE_RE.match(d.name) and (d / "review_slate.json").exists():
            dates.append(d.name)
    return sorted(dates)


def find_dates_with_approved(sport: str) -> List[str]:
    """Return all dates that have approved slates, sorted ascending."""
    root = REVIEW_ROOT / sport_key(sport)
    if not root.exists():
        return []
    dates = []
    for d in root.iterdir():
        if d.is_dir() and DATE_RE.match(d.name) and (d / "approved_slate.json").exists():
            dates.append(d.name)
    return sorted(dates)


def parse_date_range(start: str, end: str) -> List[str]:
    """Generate list of YYYY-MM-DD dates from start to end inclusive."""
    from datetime import date, timedelta
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    if s > e:
        print(f"[review] ERROR: start date {start} is after end date {end}")
        sys.exit(1)
    dates = []
    cur = s
    while cur <= e:
        dates.append(cur.isoformat())
        cur += timedelta(days=1)
    return dates
