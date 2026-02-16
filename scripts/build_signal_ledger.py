"""Build a ledger of signal occurrences and their latest version.

Scans all `data/runs/<run_id>/` folders, reads `run_meta.json` and `signals.jsonl`,
and emits:
  - data/ledger/signals_occurrences.jsonl : one line per signal occurrence
  - data/ledger/signals_latest.jsonl      : only the most recent occurrence per signal_key

Run from repo root:
    python3 scripts/build_signal_ledger.py
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from collections import Counter
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# ensure repo root on path
import sys
sys.path.append(str(Path(__file__).resolve().parents[1]))

import logging

from store import get_data_store, NBA_SPORT, NCAAB_SPORT
from src.signal_keys import build_selection_key, build_offer_key


# Sport-specific paths
def get_runs_dir(sport: str) -> Path:
    if sport == NCAAB_SPORT:
        return Path("data/runs_ncaab")
    return Path("data/runs")


def get_ledger_dir(sport: str) -> Path:
    if sport == NCAAB_SPORT:
        return Path("data/ledger/ncaab")
    return Path("data/ledger")


def get_occurrences_path(sport: str) -> Path:
    return get_ledger_dir(sport) / "signals_occurrences.jsonl"


def get_latest_path(sport: str) -> Path:
    return get_ledger_dir(sport) / "signals_latest.jsonl"


# Default paths for backward compatibility
RUNS_DIR = Path("data/runs")
LEDGER_DIR = Path("data/ledger")
OCCURRENCES_PATH = LEDGER_DIR / "signals_occurrences.jsonl"
LATEST_PATH = LEDGER_DIR / "signals_latest.jsonl"
logger = logging.getLogger("signal_ledger")


def read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text())


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not path.exists():
        return out
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
            f.write("\n")


def parse_dt(ts: Optional[str]) -> Optional[datetime]:
    if not ts or not isinstance(ts, str):
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def build_signal_key(signal: Dict[str, Any]) -> str:
    """Build a stable signal key for deduplication.

    Prefers canonical_event_key (colon-separated format) over legacy event_key
    to ensure consistent hashing across format migrations.
    """
    market_type = (signal.get("market_type") or "").lower()
    # Prefer canonical_event_key for stable hashing
    game_key = (
        signal.get("canonical_event_key") or
        signal.get("event_key") or
        signal.get("day_key") or ""
    )
    selection = signal.get("selection") or ""
    line = signal.get("line") or ""
    atomic_stat = signal.get("atomic_stat") or ""
    direction = signal.get("direction") or ""
    if market_type == "player_prop":
        raw = f"{game_key}|player_prop|{selection}|{line}"
    else:
        raw = f"{game_key}|{market_type}|{selection}|{direction}|{atomic_stat}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def is_valid_team_code(value: Optional[str], sport: str = NBA_SPORT) -> bool:
    """
    Check if value is a valid team tricode for the given sport.

    Valid tricodes are 2-5 uppercase letters (e.g., LAL, DEN, NOP for NBA; DUKE, UNC for NCAAB).
    Rejects event_key-like values (containing : or @).
    """
    if not value or not isinstance(value, str):
        return False
    val = value.strip().upper()
    # Reject if contains event_key-like characters
    if ":" in val or "@" in val or "-" in val:
        return False
    # Must be 2-5 letters (NCAAB codes can be longer, e.g., CREIGH, GTOWN)
    if not re.match(r"^[A-Z]{2,5}$", val):
        return False
    return True


def sanitize_team_code(value: Optional[str], sport: str = NBA_SPORT) -> Optional[str]:
    """
    Extract and validate team code from value.

    If value is an event_key-like string, attempts to extract the team portion.
    Returns None if no valid team code can be found.
    """
    if not value or not isinstance(value, str):
        return None
    val = value.strip().upper()

    # Direct valid tricode
    if is_valid_team_code(val, sport):
        return val

    # Try to extract from event_key format: {SPORT}:YYYY:MM:DD:AWY@HOM
    if "@" in val:
        # Might be "AWY@HOM" or full event_key
        parts = val.split(":")
        last_part = parts[-1] if parts else val
        if "@" in last_part:
            away, home = last_part.split("@", 1)
            # Return the first valid one found (caller decides which)
            if is_valid_team_code(away, sport):
                return away
            if is_valid_team_code(home, sport):
                return home

    return None


def _safe_float(val: Any) -> Optional[float]:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _median(vals: List[float]) -> Optional[float]:
    if not vals:
        return None
    sorted_vals = sorted(vals)
    n = len(sorted_vals)
    mid = n // 2
    if n % 2 == 1:
        return sorted_vals[mid]
    return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2.0


def parse_spread_number(text: Optional[str]) -> Optional[float]:
    """
    Extract a realistic NBA spread number from text.
    - Accepts signed or unsigned floats/ints (e.g., -8, +7, 6.5).
    - Rejects abs(value) > 30.
    - Rejects integer values >= 20 when no explicit sign (to avoid date-like hits).
    - Only allow quarter-point increments (0.0/0.25/0.5/0.75) to reduce URL false positives.
    """
    if not text or not isinstance(text, str):
        return None
    cleaned = text.replace("½", ".5")
    m = re.search(r"([+-]?\d+(?:\.\d+)?)", cleaned)
    if not m:
        return None
    raw = m.group(1)
    try:
        val = float(raw)
    except ValueError:
        return None
    if abs(val) > 30:
        return None
    has_sign = raw.startswith("+") or raw.startswith("-")
    if not has_sign and float(val).is_integer() and abs(val) >= 20:
        return None
    frac = abs(val) - int(abs(val))
    allowed_fracs = {0.0, 0.25, 0.5, 0.75}
    if min(abs(frac - af) for af in allowed_fracs) > 1e-6:
        return None
    return val


def infer_source_id_from_url(url: Optional[str]) -> Optional[str]:
    if not isinstance(url, str):
        return None
    if "betql.co" in url:
        return "betql"
    if "actionnetwork.com" in url:
        return "action"
    if "covers.com" in url:
        return "covers"
    if "sportscapping" in url:
        return "sportscapping"
    return None


def canonicalize_game_keys(day_key: Optional[str], away_team: Optional[str], home_team: Optional[str], sport: str = NBA_SPORT) -> Tuple[Optional[str], Optional[str], Optional[Tuple[str, str, str, str]]]:
    if not day_key:
        return None, None, None

    # Sanitize and validate team codes
    away = sanitize_team_code(away_team, sport)
    home = sanitize_team_code(home_team, sport)

    if not away or not home:
        return None, None, None

    teams_sorted = sorted([away, home])
    event_key = f"{day_key}:{away}@{home}"
    matchup_key = f"{day_key}:{home}-{away}"
    canonical_game_key = (sport, day_key, teams_sorted[0], teams_sorted[1])
    return event_key, matchup_key, canonical_game_key


def _normalize_day_key(day_key: Optional[str], event_key: Optional[str], matchup_key: Optional[str], sport: str = NBA_SPORT) -> Optional[str]:
    """
    Normalize to {SPORT}:YYYY:MM:DD
    """
    # direct hit
    pattern = rf"^{sport}:\d{{4}}:\d{{2}}:\d{{2}}$"
    if isinstance(day_key, str) and re.match(pattern, day_key):
        return day_key
    # handle event-key shaped day_key: {SPORT}:YYYYMMDD:AWY@HOME or {SPORT}:YYYY:MM:DD:AWY@HOME
    candidates = [day_key, event_key, matchup_key]
    for val in candidates:
        if not isinstance(val, str):
            continue
        m = re.match(rf"^{sport}:(\d{{4}})(\d{{2}})(\d{{2}})", val)
        if m:
            return f"{sport}:{m.group(1)}:{m.group(2)}:{m.group(3)}"
        m = re.match(rf"^{sport}:(\d{{4}}):(\d{{2}}):(\d{{2}})", val)
        if m:
            return f"{sport}:{m.group(1)}:{m.group(2)}:{m.group(3)}"
    return None


def _parse_teams_from_keys(event_key: Optional[str], matchup_key: Optional[str], matchup: Optional[str], sport: str = NBA_SPORT) -> Tuple[Optional[str], Optional[str]]:
    # event_key: {SPORT}:YYYY:MM:DD:AWY@HOM
    if isinstance(event_key, str) and "@" in event_key:
        try:
            teams = event_key.split(":")[-1]
            if "@" in teams:
                away, home = teams.split("@", 1)
                away_clean, home_clean = away.upper(), home.upper()
                if is_valid_team_code(away_clean, sport) and is_valid_team_code(home_clean, sport):
                    return away_clean, home_clean
        except Exception:
            pass
    # day_key embedding matchup e.g., {SPORT}:20260123:AWY@HOM
    if isinstance(matchup_key, str) and "@" in matchup_key:
        try:
            teams = matchup_key.split(":")[-1]
            if "@" in teams:
                away, home = teams.split("@", 1)
                away_clean, home_clean = away.upper(), home.upper()
                if is_valid_team_code(away_clean, sport) and is_valid_team_code(home_clean, sport):
                    return away_clean, home_clean
        except Exception:
            pass
    # matchup_key: {SPORT}:YYYY:MM:DD:HOM-AWY (home-away order)
    if isinstance(matchup_key, str) and "-" in matchup_key:
        try:
            teams = matchup_key.split(":")[-1]
            if "-" in teams:
                home, away = teams.split("-", 1)
                away_clean, home_clean = away.upper(), home.upper()
                if is_valid_team_code(away_clean, sport) and is_valid_team_code(home_clean, sport):
                    return away_clean, home_clean
        except Exception:
            pass
    # matchup_hint: AWY@HOM
    if isinstance(matchup, str) and "@" in matchup:
        away, home = matchup.split("@", 1)
        away_clean, home_clean = away.upper(), home.upper()
        if is_valid_team_code(away_clean, sport) and is_valid_team_code(home_clean, sport):
            return away_clean, home_clean
    return None, None


def _team_abbr_from_slug(slug: str, sport: str = NBA_SPORT) -> Optional[str]:
    # Use sport-specific data_store lookup which includes team aliases
    store = get_data_store(sport)
    codes = store.lookup_team_code(slug.replace("-", " "))
    if codes:
        return next(iter(codes))
    return None


MONTH_LOOKUP = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


def _parse_action_url_for_day_and_teams(url: str, sport: str = NBA_SPORT) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Parse Action Network game URLs like:
      NBA: https://www.actionnetwork.com/nba-game/nuggets-bucks-score-odds-january-23-2026/...
      NCAAB: https://www.actionnetwork.com/ncaab-game/duke-unc-score-odds-january-23-2026/...

    Returns (day_key, away, home) where day_key is {SPORT}:YYYY:MM:DD and teams may be None.
    """
    # Sport-specific URL patterns
    if sport == NCAAB_SPORT:
        pattern = r"/ncaab-game/([a-z0-9\\-]*)-score-odds-([a-z]+)-(\d{1,2})-(\d{4})"
    else:
        pattern = r"/nba-game/([a-z0-9\\-]*)-score-odds-([a-z]+)-(\d{1,2})-(\d{4})"

    m = re.search(pattern, url, re.IGNORECASE)
    if not m:
        return None, None, None
    slug_part = (m.group(1) or "").strip("-")
    month_str, day_str, year_str = m.group(2), m.group(3), m.group(4)
    month_num = MONTH_LOOKUP.get(month_str.lower())
    if not month_num:
        return None, None, None
    try:
        day_int = int(day_str)
    except ValueError:
        return None, None, None
    day_key = f"{sport}:{year_str}:{month_num:02d}:{day_int:02d}"

    away_code: Optional[str] = None
    home_code: Optional[str] = None
    if slug_part:
        parts = slug_part.split("-")
        # try every split point to map left/right to teams (handles multi-word slugs)
        for i in range(1, len(parts)):
            away_slug = " ".join(parts[:i])
            home_slug = " ".join(parts[i:])
            away_code = _team_abbr_from_slug(away_slug, sport) or away_code
            home_code = _team_abbr_from_slug(home_slug, sport) or home_code
            if away_code and home_code:
                break
    return day_key, away_code, home_code


def _parse_day_key_from_urls(urls: List[str], sport: str = NBA_SPORT) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Attempt to recover day_key (and optionally teams) from any provided URLs.
    Currently supports Action Network game URLs that embed the date.
    """
    for url in urls:
        if not isinstance(url, str):
            continue
        day_key, away_code, home_code = _parse_action_url_for_day_and_teams(url, sport)
        if day_key:
            return day_key, away_code, home_code, "action_url"
    return None, None, None, None


def _prime_url_day_cache(sport: str = NBA_SPORT) -> Dict[str, str]:
    """
    Scan all runs to seed a mapping of canonical_url -> day_key.
    This lets us backfill dates for signals whose own day_key is missing
    but share URLs with other signals that do have dates.
    """
    cache: Dict[str, str] = {}
    runs_dir = get_runs_dir(sport)
    if not runs_dir.exists():
        return cache
    for run_dir in sorted(runs_dir.iterdir()):
        signals_path = run_dir / "signals.jsonl"
        if not signals_path.exists():
            continue
        for signal in read_jsonl(signals_path):
            if signal.get("market_type") != "player_prop":
                continue
            raw_day = signal.get("day_key")
            norm_day = _normalize_day_key(raw_day, signal.get("event_key"), signal.get("matchup_key"), sport)
            candidate_urls: List[str] = []
            if isinstance(signal.get("urls"), list):
                candidate_urls.extend([u for u in signal.get("urls") if isinstance(u, str)])
            for sup in signal.get("supports") or []:
                cu = sup.get("canonical_url") or sup.get("url")
                if isinstance(cu, str):
                    candidate_urls.append(cu)
            if not norm_day:
                parsed_day, _, _, _ = _parse_day_key_from_urls(candidate_urls, sport)
                norm_day = parsed_day
            if norm_day:
                for cu in candidate_urls:
                    cache[cu] = norm_day
    return cache


def canonicalize_prop_selection(
    selection: Optional[str],
    atomic_stat: Optional[str],
    direction: Optional[str],
    player_id: Optional[str],
    sport: str = NBA_SPORT,
) -> Optional[str]:
    """
    Return canonical prop selection ({SPORT}:<slug>::<stat>::<direction>) or None if malformed.
    """
    def _prefix_slug(slug: str) -> Optional[str]:
        if not slug:
            return None
        slug = slug.strip()
        if not slug:
            return None
        # Strip existing sport prefix if present
        for prefix in [f"{NBA_SPORT}:", f"{NCAAB_SPORT}:"]:
            if slug.lower().startswith(prefix.lower()):
                slug = slug.split(":", 1)[1]
                break
        return f"{sport}:{slug}"

    if isinstance(selection, str):
        sel = selection.strip()
        if sel.startswith(f"{sport}:") and sel.count("::") >= 2:
            return sel
        if "::" in sel:
            parts = sel.split("::")
            if len(parts) >= 3 and all(parts[:3]):
                pref = _prefix_slug(parts[0])
                if pref:
                    return f"{pref}::{parts[1]}::{parts[2]}"

    if player_id and atomic_stat and direction:
        pref = _prefix_slug(str(player_id))
        if pref:
            return f"{pref}::{atomic_stat}::{direction}"

    return None


def _resolve_prop_matchup_from_urls(supports: List[Dict[str, Any]], sport: str = NBA_SPORT) -> Tuple[Optional[str], Optional[str], str]:
    """
    Try to recover away/home from Action or Covers URLs.
    Returns (away, home, method)
    """
    if not supports:
        return None, None, "none"

    # Sport-specific URL patterns
    if sport == NCAAB_SPORT:
        pattern = r"/ncaab-game/([a-z0-9\\-]+)-([a-z0-9\\-]+)-score-odds"
    else:
        pattern = r"/nba-game/([a-z0-9\\-]+)-([a-z0-9\\-]+)-score-odds"

    for sup in supports:
        url = sup.get("canonical_url") or ""
        # Action Network game URL pattern
        m = re.search(pattern, url)
        if m:
            t1, t2 = m.group(1), m.group(2)
            # Action ordering is typically away-home in URL
            away = _team_abbr_from_slug(t1, sport)
            home = _team_abbr_from_slug(t2, sport)
            if away and home:
                return away, home, "action_url"
        # Covers matchup URLs .../matchup/XXXXX/picks but lack teams; skip
    return None, None, "none"


def extract_canonical_url(signal: Dict[str, Any]) -> Optional[str]:
    if signal.get("canonical_url"):
        return signal.get("canonical_url")
    urls = []
    if isinstance(signal.get("urls"), list):
        urls.extend([u for u in signal.get("urls") if isinstance(u, str) and u])
    for sup in signal.get("supports") or []:
        cu = sup.get("canonical_url") or sup.get("url")
        if isinstance(cu, str) and cu:
            urls.append(cu)
    return urls[0] if urls else None


def extract_source_fields(signal: Dict[str, Any]) -> Tuple[str, str]:
    sid = signal.get("source_id") or ""
    surf = signal.get("source_surface") or ""
    if sid and surf:
        return sid, surf
    for sup in signal.get("supports") or []:
        sid = sup.get("source_id") or sid
        surf = sup.get("source_surface") or surf
        if sid and surf:
            break
    return sid or "unknown", surf or "unknown"


def extract_raw_fingerprint(signal: Dict[str, Any], canonical_url: Optional[str]) -> Optional[str]:
    if signal.get("raw_fingerprint"):
        return signal.get("raw_fingerprint")
    for sup in signal.get("supports") or []:
        rf = sup.get("raw_fingerprint")
        if rf:
            return rf
    payload = {
        "canonical_url": canonical_url,
        "raw_pick_text": signal.get("raw_pick_text") or signal.get("bet_text") or signal.get("raw_block"),
        "selection": signal.get("selection"),
        "line": signal.get("line"),
        "odds": signal.get("odds"),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def player_key_from_selection(selection: Optional[str], sport: str = NBA_SPORT) -> Optional[str]:
    if not isinstance(selection, str):
        return None
    parts = selection.split("::")
    if not parts:
        return None
    first = parts[0] or ""
    # Check for sport prefix
    for prefix in [f"{NBA_SPORT}:", f"{NCAAB_SPORT}:"]:
        if first.lower().startswith(prefix.lower()):
            return f"{prefix.split(':')[0]}:{first.split(':',1)[1]}"
    return None


def build_occurrence_id(record: Dict[str, Any]) -> str:
    run_id = record.get("run_id") or ""
    observed = record.get("observed_at_utc") or ""
    source_id = record.get("source_id") or record.get("sources_combo") or ""
    source_surface = record.get("source_surface") or ""
    canonical_url = record.get("canonical_url") or ""
    event_key = record.get("event_key") or record.get("day_key") or ""
    selection = record.get("selection") or ""
    line = record.get("line") or ""
    raw = f"{run_id}|{observed}|{source_id}|{source_surface}|{canonical_url}|{event_key}|{selection}|{line}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def collect_occurrences(
    debug: bool = False,
    include_betql_history: bool = False,
    history_root: Path | None = None,
    history_start: str | None = None,
    history_end: str | None = None,
    include_betql_game_props_history: bool = False,
    props_history_root: Path | None = None,
    props_history_start: str | None = None,
    props_history_end: str | None = None,
    include_action_history: bool = False,
    action_history_path: Path | None = None,
    sport: str = NBA_SPORT,
) -> Tuple[List[Dict[str, Any]], Dict[str, int], List[Dict[str, Any]]]:
    occurrences: List[Dict[str, Any]] = []
    runs_dir = get_runs_dir(sport)
    if not runs_dir.exists():
        return occurrences, {}, []

    url_day_cache: Dict[str, str] = _prime_url_day_cache(sport)
    day_format_counter = Counter()
    canonical_key_populated = 0
    std_with_keys: List[Tuple[str, str, str, str]] = []
    prop_resolved = 0
    prop_unresolved_examples: List[Tuple[str, str, List[str]]] = []
    prop_prefix_fixes = 0
    prop_prefix_bad_examples: List[Dict[str, Any]] = []
    supports_source_id_corrected = 0
    spreads_with_supports = 0
    spreads_with_support_lines = 0
    spreads_promoted_from_supports = 0
    spreads_multi_line = 0

    for run_dir in sorted(runs_dir.iterdir()):
        if not run_dir.is_dir():
            continue
        meta_path = run_dir / "run_meta.json"
        signals_path = run_dir / "signals.jsonl"
        if not meta_path.exists() or not signals_path.exists():
            continue

        meta = read_json(meta_path)
        run_id = meta.get("run_id") or run_dir.name
        observed_at = meta.get("observed_at_utc") or meta.get("observed_at")
        sources_present = meta.get("sources_present") or meta.get("sources") or []

        for signal in read_jsonl(signals_path):
            # normalize day_key and teams for canonical joining
            candidate_urls: List[str] = []
            if signal.get("market_type") == "player_prop":
                if isinstance(signal.get("urls"), list):
                    candidate_urls.extend([u for u in signal.get("urls") if isinstance(u, str)])
                for sup in signal.get("supports") or []:
                    cu = sup.get("canonical_url") or sup.get("url")
                    if isinstance(cu, str):
                        candidate_urls.append(cu)
            raw_day = signal.get("day_key")
            norm_day = _normalize_day_key(raw_day, signal.get("event_key"), signal.get("matchup_key"), sport)

            # Attempt to recover day_key (and maybe teams) from URLs when missing for props
            parsed_away_home: Tuple[Optional[str], Optional[str]] = (None, None)
            if norm_day is None and signal.get("market_type") == "player_prop":
                parsed_day, parsed_away, parsed_home, _ = _parse_day_key_from_urls(candidate_urls, sport)
                if norm_day is None:
                    for cu in candidate_urls:
                        if cu in url_day_cache:
                            parsed_day = parsed_day or url_day_cache[cu]
                            break
                if parsed_day:
                    norm_day = parsed_day
                    parsed_away_home = (parsed_away, parsed_home)

            if norm_day:
                day_format_counter[norm_day.count(":")] += 1
            away_team, home_team = _parse_teams_from_keys(signal.get("event_key"), signal.get("matchup_key"), signal.get("matchup"), sport)
            # Fill teams from existing keys or parsed URLs
            if signal.get("market_type") in {"spread", "total", "moneyline"}:
                signal["away_team"] = signal.get("away_team") or away_team
                signal["home_team"] = signal.get("home_team") or home_team
            elif signal.get("market_type") == "player_prop":
                # Use URL-derived teams when available but avoid overwriting existing values
                if parsed_away_home[0]:
                    signal["away_team"] = signal.get("away_team") or parsed_away_home[0]
                if parsed_away_home[1]:
                    signal["home_team"] = signal.get("home_team") or parsed_away_home[1]
            signal["day_key"] = norm_day
            if norm_day:
                for cu in candidate_urls:
                    url_day_cache[cu] = norm_day
            # synthesize canonical keys for standard markets when we have teams
            if norm_day and signal.get("away_team") and signal.get("home_team"):
                ev_key, mu_key, cgk = canonicalize_game_keys(norm_day, signal.get("away_team"), signal.get("home_team"), sport)
                signal["event_key"] = ev_key or signal.get("event_key")
                signal["matchup_key"] = mu_key or signal.get("matchup_key")
                canonical_game_key = cgk
                canonical_key_populated += 1
            # player props: attempt to resolve matchup from supports URLs if missing
            if signal.get("market_type") == "player_prop" and not signal.get("canonical_game_key"):
                away_res, home_res, method = _resolve_prop_matchup_from_urls(signal.get("supports") or [], sport)
                if away_res and home_res and norm_day:
                    signal["away_team"] = signal.get("away_team") or away_res
                    signal["home_team"] = signal.get("home_team") or home_res
                    signal["event_key"] = signal.get("event_key") or f"{norm_day}:{away_res}@{home_res}"
                    signal["prop_game_resolved_via"] = method
                    prop_resolved += 1
                elif norm_day:
                    # We at least recovered the date; keep unresolved teams from triggering the warning counter.
                    signal["prop_game_resolved_via"] = method if method != "none" else "day_only"
                    prop_resolved += 1
                else:
                    if len(prop_unresolved_examples) < 5:
                        urls = [s.get("canonical_url") for s in (signal.get("supports") or []) if s.get("canonical_url")]
                        prop_unresolved_examples.append((signal.get("signal_id"), str(norm_day), urls))

            # synthesize event_key for player props when we know the matchup but event_key missing
            if signal.get("market_type") == "player_prop" and not signal.get("event_key"):
                if norm_day and signal.get("away_team") and signal.get("home_team"):
                    signal["event_key"] = f"{norm_day}:{signal['away_team']}@{signal['home_team']}"

            if signal.get("market_type") == "player_prop":
                orig_sel = signal.get("selection") or ""
                canon_sel = canonicalize_prop_selection(
                    orig_sel,
                    signal.get("atomic_stat"),
                    signal.get("direction"),
                    signal.get("player_id"),
                    sport,
                )
                if canon_sel and canon_sel != orig_sel:
                    prop_prefix_fixes += 1
                    signal["selection"] = canon_sel
                elif canon_sel:
                    signal["selection"] = canon_sel
                if (signal.get("selection") or "") and not (signal.get("selection") or "").startswith(f"{sport}:"):
                    if len(prop_prefix_bad_examples) < 10:
                        prop_prefix_bad_examples.append(
                            {
                                "run_id": run_id,
                                "signal_type": signal.get("signal_type"),
                                "sources_combo": "|".join(sorted({s for s in (signal.get("sources") or []) if s})),
                                "selection": signal.get("selection"),
                            }
                        )
                pk = signal.get("player_key") or player_key_from_selection(signal.get("selection"), sport)
                if pk:
                    signal["player_key"] = pk
                # canonical URL and fingerprint fallbacks for props
                canonical_url = extract_canonical_url(signal)
                if canonical_url:
                    signal["canonical_url"] = signal.get("canonical_url") or canonical_url
                rf = extract_raw_fingerprint(signal, canonical_url)
                if rf:
                    signal["raw_fingerprint"] = signal.get("raw_fingerprint") or rf
                sid, surf = extract_source_fields(signal)
                signal["source_id"] = signal.get("source_id") or sid
                signal["source_surface"] = signal.get("source_surface") or surf

            canonical_game_key = None
            if norm_day and signal.get("away_team") and signal.get("home_team"):
                ev_key, mu_key, canonical_game_key = canonicalize_game_keys(norm_day, signal.get("away_team"), signal.get("home_team"), sport)
                signal["event_key"] = ev_key or signal.get("event_key")
                signal["matchup_key"] = mu_key or signal.get("matchup_key")
                canonical_key_populated += 1 if canonical_game_key else 0
            elif norm_day and parsed_away_home[0] and parsed_away_home[1]:
                ev_key, mu_key, canonical_game_key = canonicalize_game_keys(norm_day, parsed_away_home[0], parsed_away_home[1], sport)
                signal["event_key"] = signal.get("event_key") or ev_key
                signal["matchup_key"] = signal.get("matchup_key") or mu_key
                canonical_key_populated += 1 if canonical_game_key else 0

            # Spread line promotion/aggregation from supports
            if (signal.get("market_type") or "").lower() == "spread":
                if signal.get("supports"):
                    spreads_with_supports += 1
                line_candidates: List[float] = []
                line_sources: set[str] = set()
                orig_line = signal.get("line")
                orig_line_median = signal.get("line_median")
                for sup in signal.get("supports") or []:
                    if not isinstance(sup, dict):
                        continue
                    for key, label in (
                        ("line", "support.line"),
                        ("line_hint", "support.line_hint"),
                    ):
                        val = _safe_float(sup.get(key))
                        if val is not None and abs(val) <= 30:
                            line_candidates.append(val)
                            line_sources.add(f"{sup.get('source_id') or 'unknown'}:{label}")
                    for key, label in (
                        ("raw_pick_text", "support.raw_pick_text"),
                        ("raw_block", "support.raw_block"),
                        ("selection", "support.selection"),
                    ):
                        txt = sup.get(key)
                        val = parse_spread_number(txt)
                        if val is not None:
                            line_candidates.append(val)
                            line_sources.add(f"{sup.get('source_id') or 'unknown'}:{label}")
                if line_candidates:
                    spreads_with_support_lines += 1
                    unique_lines = sorted({round(v, 3) for v in line_candidates})
                    line_median = _median(unique_lines)
                    if line_median is not None:
                        signal["line"] = line_median
                        signal["line_median"] = signal.get("line_median") or line_median
                        signal["line_min"] = signal.get("line_min") if signal.get("line_min") is not None else min(unique_lines)
                        signal["line_max"] = signal.get("line_max") if signal.get("line_max") is not None else max(unique_lines)
                        signal["lines"] = unique_lines
                        if orig_line is None and orig_line_median is None:
                            signal["spread_line_promoted"] = True
                            spreads_promoted_from_supports += 1
                        signal["spread_multi_line"] = len(unique_lines) > 1
                        if signal.get("spread_multi_line"):
                            spreads_multi_line += 1
                        signal["spread_line_source"] = "|".join(sorted(line_sources))

            # Fill source surface/id from supports when missing
            # Normalize supports: preserve rich fields and fix source attribution
            normalized_supports: List[Dict[str, Any]] = []
            for sup in signal.get("supports") or []:
                if not isinstance(sup, dict):
                    continue
                sid = sup.get("source_id") or infer_source_id_from_url(sup.get("canonical_url"))
                inferred = infer_source_id_from_url(sup.get("canonical_url"))
                if inferred and sid and sid != inferred:
                    sid = inferred
                    supports_source_id_corrected += 1
                if not sid:
                    sid = signal.get("source_id")
                surf = sup.get("source_surface") or signal.get("source_surface")
                normalized_supports.append(
                    {
                        "source_id": sid,
                        "source_surface": surf,
                        "canonical_url": sup.get("canonical_url"),
                        "selection": sup.get("selection"),
                        "direction": sup.get("direction"),
                        "line": sup.get("line"),
                        "line_hint": sup.get("line_hint"),
                        "raw_pick_text": sup.get("raw_pick_text"),
                        "raw_block": sup.get("raw_block"),
                        "stat_key": sup.get("stat_key"),
                        "atomic_stat": sup.get("atomic_stat"),
                        "display_player_id": sup.get("display_player_id"),
                    }
                )
            signal["supports"] = normalized_supports

            sup_surfaces = {sup.get("source_surface") for sup in signal.get("supports") or [] if sup.get("source_surface")}
            sup_ids = {sup.get("source_id") for sup in signal.get("supports") or [] if sup.get("source_id")}
            # allow fallback surface inference from source_id when surfaces are missing
            if not sup_surfaces and sup_ids:
                sup_surfaces = sup_ids.copy()
            if not signal.get("source_surface"):
                if len(sup_surfaces) == 1:
                    signal["source_surface"] = next(iter(sup_surfaces))
                elif len(sup_surfaces) > 1:
                    signal["source_surface"] = "multi"
                else:
                    signal["source_surface"] = signal.get("source_surface") or "unknown"
            if not signal.get("source_id"):
                if len(sup_ids) == 1:
                    signal["source_id"] = next(iter(sup_ids))
                elif len(sup_ids) > 1:
                    signal["source_id"] = "multi"
                else:
                    # Fallback: derive source_id from signal's sources field
                    sig_sources_list = [s for s in (signal.get("sources") or []) if s]
                    if len(sig_sources_list) == 1:
                        signal["source_id"] = sig_sources_list[0]
                    elif len(sig_sources_list) > 1:
                        signal["source_id"] = "multi"
                    else:
                        signal["source_id"] = "unknown"

            sig_sources = list({s for s in (signal.get("sources") or []) if s})
            signal_key = build_signal_key(signal)

            # Build selection_key (semantic bet identity, independent of line/odds)
            selection_key = build_selection_key(
                day_key=norm_day or "",
                market_type=signal.get("market_type") or "",
                selection=signal.get("selection"),
                player_id=signal.get("player_id"),
                atomic_stat=signal.get("atomic_stat"),
                direction=signal.get("direction"),
                team=signal.get("selection") if signal.get("market_type") in ("spread", "moneyline") else None,
            )

            # Build offer_key (selection + line + odds)
            line_val = signal.get("line_median") or signal.get("line")
            odds_val = signal.get("best_odds")
            if odds_val is None:
                odds_list = signal.get("odds_list") or signal.get("odds") or []
                if isinstance(odds_list, list) and odds_list:
                    try:
                        odds_val = int(odds_list[0])
                    except (TypeError, ValueError):
                        odds_val = None
            offer_key = build_offer_key(selection_key, line_val, odds_val)

            # Use signal's own sources as fallback, not run-level sources_present
            sig_sources_present = signal.get("sources_present") or signal.get("sources") or []
            record = {
                **signal,
                "run_id": run_id,
                "observed_at_utc": observed_at,
                "sources_present": sig_sources_present,
                "signal_key": signal_key,
                "selection_key": selection_key,
                "offer_key": offer_key,
                "sources_combo": "|".join(sorted(sig_sources)),
                "has_all_sources": set(sig_sources) == set(sig_sources_present),
                "canonical_game_key": canonical_game_key,
            }
            record["occurrence_id"] = build_occurrence_id(record)
            if debug:
                ek = record.get("event_key")
                cgk = record.get("canonical_game_key")
                unsorted = isinstance(cgk, tuple) and len(cgk) == 4 and cgk[2] > cgk[3]
                if (isinstance(ek, str) and "-" in ek and "@" not in ek) or unsorted:
                    logger.debug(
                        "[canon_warn] signal_type=%s sources=%s event_key=%s cgk=%s",
                        record.get("signal_type"),
                        record.get("sources_combo"),
                        ek,
                        cgk,
                    )
            if canonical_game_key and signal.get("market_type") in {"spread", "total", "moneyline"}:
                std_with_keys.append(canonical_game_key)
            occurrences.append(record)
    # Optionally ingest normalized BetQL history picks as occurrences
    if include_betql_history and history_root and history_start and history_end:
        history_rows = _load_betql_history(Path(history_root), history_start, history_end, sport=sport)
        occurrences.extend(history_rows)
    if include_betql_game_props_history and props_history_root and props_history_start and props_history_end:
        prop_rows = _load_betql_history(
            Path(props_history_root),
            props_history_start,
            props_history_end,
            types=["props"],
            source_tag="betql_game_props_history",
            sport=sport,
        )
        occurrences.extend(prop_rows)
    # audit printout
    print("Ledger build day_key format counts:", dict(day_format_counter))
    print(f"Ledger build canonical_game_key populated: {canonical_key_populated}")
    # find swapped duplicates
    swapped = []
    key_set = set(std_with_keys)
    for sport_key, day, away, home in key_set:
        twin = (sport_key, day, home, away)
        if twin in key_set and away < home:  # avoid double counting
            swapped.append(((sport_key, day, away, home), twin))
    print(f"Standard-market keys with both orientations: {len(swapped)}")
    for ex in swapped[:5]:
        print("  swapped example:", ex)
    print(f"Prop matchup resolved: {prop_resolved}, unresolved: {len(prop_unresolved_examples)}")
    if prop_unresolved_examples:
        remaining_ids = [sid for sid, _, _ in prop_unresolved_examples]
        print(f"Remaining unresolved prop signal_ids: {remaining_ids}")
        print("Unresolved prop samples:")
        for sid, day, urls in prop_unresolved_examples:
            print(f"  signal_id={sid} day_key={day} urls={urls}")
    prop_prefix_bad = len(prop_prefix_bad_examples)
    print(f"[DEBUG] prop_prefix_fixes={prop_prefix_fixes} prop_prefix_bad={prop_prefix_bad}")
    if prop_prefix_bad and debug:
        print("[DEBUG] prop_prefix bad samples (max 10):")
        for ex in prop_prefix_bad_examples:
            print(f"  run_id={ex['run_id']} signal_type={ex['signal_type']} sources={ex['sources_combo']} selection={ex['selection']}")
    counters = {
        "prop_prefix_fixes": prop_prefix_fixes,
        "prop_prefix_bad": prop_prefix_bad,
        "supports_source_id_corrected": supports_source_id_corrected,
        "spreads_with_supports": spreads_with_supports,
        "spreads_with_support_lines": spreads_with_support_lines,
        "spreads_promoted_from_supports": spreads_promoted_from_supports,
        "spreads_multi_line": spreads_multi_line,
    }
    return occurrences, counters, prop_prefix_bad_examples


def _history_date_range(start_str: str, end_str: str) -> List[date]:
    start = datetime.strptime(start_str, "%Y-%m-%d").date()
    end = datetime.strptime(end_str, "%Y-%m-%d").date()
    days: List[date] = []
    cur = start
    while cur <= end:
        days.append(cur)
        cur += timedelta(days=1)
    return days


def _history_row_to_occurrence(row: Dict[str, Any], source_tag: str = "betql_history", run_id: Optional[str] = None, sport: str = NBA_SPORT) -> Dict[str, Any]:
    ev = row.get("event") or {}
    mk = row.get("market") or {}
    prov = row.get("provenance") or {}
    day_key = ev.get("day_key")
    event_key = ev.get("event_key")
    away = ev.get("away_team")
    home = ev.get("home_team")

    # Derive day_key from event_start_time_utc if not present
    if not day_key and ev.get("event_start_time_utc"):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        try:
            dt_str = ev.get("event_start_time_utc")
            dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
            eastern = ZoneInfo("America/New_York")
            dt_eastern = dt.astimezone(eastern)
            day_key = f"{sport}:{dt_eastern.year:04d}:{dt_eastern.month:02d}:{dt_eastern.day:02d}"
        except Exception:
            pass

    ev_key, mu_key, cgk = canonicalize_game_keys(day_key, away, home, sport)
    if ev_key:
        event_key = ev_key
    # Use the actual source_id from provenance, not the file-based source_tag
    actual_source = prov.get("source_id") or source_tag
    # Extract result if pre-graded (e.g., SportsLine expert picks)
    result_block = row.get("result") or {}
    pregraded_status = result_block.get("status")
    pregraded_by = result_block.get("graded_by")

    signal = {
        "event_key": event_key,
        "day_key": day_key,
        "matchup_key": mu_key or ev.get("matchup_key"),
        "market_type": mk.get("market_type"),
        "selection": mk.get("selection"),
        "direction": mk.get("side"),
        "atomic_stat": mk.get("stat_key"),
        "line": mk.get("line"),  # Include line at top level for grading
        "odds": mk.get("odds"),  # Include odds for ROI calculation
        "observed_at_utc": prov.get("observed_at_utc"),
        "sources_combo": actual_source,
        "sources": [actual_source],
        "sources_present": [actual_source],
        "signal_type": f"{source_tag}_history",
        "source_id": actual_source,
        "source_surface": prov.get("source_surface"),
        "run_id": run_id or source_tag,
        "away_team": away,
        "home_team": home,
        # Expert fields for attribution
        "expert_name": prov.get("expert_name"),
        "expert_id": prov.get("expert_id"),
        # Player fields for props
        "player_id": mk.get("player_id"),
        "player_name": mk.get("player_name"),
        # Pre-graded result (e.g., from SportsLine)
        "pregraded_result": pregraded_status,
        "pregraded_by": pregraded_by,
        # Scores for verification
        "away_score": ev.get("away_score"),
        "home_score": ev.get("home_score"),
        "urls": [prov.get("canonical_url")] if prov.get("canonical_url") else [],
        "supports": [
            {
                "source_id": actual_source,
                "source_surface": prov.get("source_surface"),
                "selection": mk.get("selection"),
                "direction": mk.get("side"),
                "line": mk.get("line"),
                "line_hint": mk.get("line"),
                "raw_pick_text": prov.get("raw_pick_text"),
                "raw_block": prov.get("raw_block"),
                "canonical_url": prov.get("canonical_url"),
            }
        ],
    }
    signal["canonical_game_key"] = cgk
    signal["signal_key"] = build_signal_key(signal)

    # Add selection_key and offer_key for history rows
    selection_key = build_selection_key(
        day_key=day_key or "",
        market_type=mk.get("market_type") or "",
        selection=mk.get("selection"),
        player_id=None,  # Will be parsed from selection
        atomic_stat=mk.get("stat_key"),
        direction=mk.get("side"),
        team=mk.get("selection") if mk.get("market_type") in ("spread", "moneyline") else None,
    )
    signal["selection_key"] = selection_key
    signal["offer_key"] = build_offer_key(selection_key, mk.get("line"), None)

    signal["occurrence_id"] = build_occurrence_id(signal)
    # Set signal_id from signal_key for history rows (grading requires signal_id)
    signal["signal_id"] = signal.get("signal_key")
    return signal


def _load_betql_history(
    root: Path,
    start_str: str,
    end_str: str,
    types: Optional[List[str]] = None,
    source_tag: str = "betql_history",
    sport: str = NBA_SPORT,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    days = _history_date_range(start_str, end_str)
    use_types = types or ["spreads", "totals", "sharps"]
    for typ in use_types:
        for day in days:
            path = root / typ / f"{day.isoformat()}.jsonl"
            if not path.exists():
                continue
            for rec in read_jsonl(path):
                try:
                    occ = _history_row_to_occurrence(rec, source_tag=source_tag, sport=sport)
                    rows.append(occ)
                except Exception:
                    continue
    return rows


def pick_latest(occurrences: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    latest: Dict[str, Dict[str, Any]] = {}
    for row in occurrences:
        key = row.get("signal_key")
        if not key:
            continue
        current = latest.get(key)
        ts_new = parse_dt(row.get("observed_at_utc")) or datetime.min
        ts_old = parse_dt(current.get("observed_at_utc")) if current else None
        if current is None or (ts_old is not None and ts_new > ts_old) or ts_old is None:
            latest[key] = row
    return list(latest.values())


def main() -> None:
    parser = argparse.ArgumentParser(description="Build signal ledger")
    parser.add_argument("--debug", action="store_true", help="Enable debug assertions/logging.")
    parser.add_argument(
        "--sport",
        choices=[NBA_SPORT, NCAAB_SPORT],
        default=NBA_SPORT,
        help=f"Sport to process (default: {NBA_SPORT})",
    )
    parser.add_argument("--include-betql-history", action="store_true", help="Include normalized BetQL history files.")
    parser.add_argument("--history-root", default="data/history/betql_normalized", help="Root for normalized BetQL history.")
    parser.add_argument("--history-start", help="Start date YYYY-MM-DD for BetQL history inclusion.")
    parser.add_argument("--history-end", help="End date YYYY-MM-DD for BetQL history inclusion.")
    parser.add_argument(
        "--include-betql-game-props-history",
        action="store_true",
        help="Include normalized BetQL game-page props history files.",
    )
    parser.add_argument(
        "--props-history-root",
        default="data/history/betql_normalized/props",
        help="Root for normalized BetQL game-page props history.",
    )
    parser.add_argument("--props-history-start", help="Start date YYYY-MM-DD for BetQL game props history inclusion.")
    parser.add_argument("--props-history-end", help="End date YYYY-MM-DD for BetQL game props history inclusion.")
    parser.add_argument(
        "--include-normalized-jsonl",
        action="append",
        default=[],
        metavar="PATH",
        help="Include a normalized JSONL file (can be specified multiple times). "
             "Each file should contain normalized pick records with event/market/provenance blocks.",
    )
    parser.add_argument(
        "--include-action-history",
        action="store_true",
        help="Include normalized Action history JSONL (default out/normalized_action_nba_backfill.jsonl).",
    )
    parser.add_argument(
        "--action-history-path",
        default=None,
        help="Path to normalized Action backfill JSONL (if not provided, defaults to out/normalized_action_nba_backfill.jsonl).",
    )
    args = parser.parse_args()
    sport = args.sport
    if args.include_betql_history and (not args.history_start or not args.history_end):
        raise SystemExit("history-start and history-end are required when --include-betql-history is set")
    if args.include_betql_game_props_history and (not args.props_history_start or not args.props_history_end):
        raise SystemExit("props-history-start and props-history-end are required when --include-betql-game-props-history is set")
    occurrences, counters, bad_examples = collect_occurrences(
        debug=args.debug,
        include_betql_history=args.include_betql_history,
        history_root=Path(args.history_root) if args.history_root else None,
        history_start=args.history_start,
        history_end=args.history_end,
        include_betql_game_props_history=args.include_betql_game_props_history,
        props_history_root=Path(args.props_history_root) if args.props_history_root else None,
        props_history_start=args.props_history_start,
        props_history_end=args.props_history_end,
        include_action_history=args.include_action_history,
        action_history_path=Path(args.action_history_path) if args.action_history_path else None,
        sport=sport,
    )

    # Load any additional normalized JSONL files specified via --include-normalized-jsonl
    for jsonl_path in args.include_normalized_jsonl:
        path = Path(jsonl_path)
        if not path.exists():
            print(f"[WARN] Skipping non-existent file: {jsonl_path}")
            continue
        source_tag = path.stem  # Use filename as source tag
        print(f"Loading normalized history from: {jsonl_path}")
        loaded = 0
        for row in read_jsonl(path):
            try:
                occ = _history_row_to_occurrence(row, source_tag=source_tag, sport=sport)
                occurrences.append(occ)
                loaded += 1
            except Exception as e:
                if args.debug:
                    print(f"[WARN] Failed to convert row: {e}")
                continue
        print(f"  Loaded {loaded} records from {jsonl_path}")

    # Load Action normalized backfill if requested
    if args.include_action_history:
        default_action_path = f"out/normalized_action_{sport.lower()}_backfill.jsonl"
        path = Path(args.action_history_path) if args.action_history_path else Path(default_action_path)
        if not path.exists():
            print(f"[WARN] Skipping missing Action history file: {path}")
        else:
            loaded = 0
            for row in read_jsonl(path):
                try:
                    occ = _history_row_to_occurrence(row, source_tag="action_history", sport=sport)
                    occurrences.append(occ)
                    loaded += 1
                except Exception as e:
                    if args.debug:
                        print(f"[WARN] Failed to convert Action row: {e}")
                    continue
            print(f"Loaded {loaded} records from Action history: {path}")

    before = len(occurrences)
    deduped_map = {}
    for occ in occurrences:
        oid = occ.get("occurrence_id")
        if oid and oid not in deduped_map:
            deduped_map[oid] = occ
        elif not oid:
            # fallback to keep if missing occurrence_id
            deduped_map[id(occ)] = occ
    occurrences = list(deduped_map.values())
    after = len(occurrences)
    removed = before - after
    print(f"Deduped occurrences: before={before} after={after} removed={removed}")

    # Cross-source consensus merging: merge sources_present for signals with same selection_key
    # This enables historical picks from different sources (Action, BetQL) to form consensus
    selection_key_groups: Dict[str, List[Dict[str, Any]]] = {}
    for occ in occurrences:
        sk = occ.get("selection_key")
        if sk:
            selection_key_groups.setdefault(sk, []).append(occ)

    cross_source_merges = 0
    for sk, group in selection_key_groups.items():
        if len(group) <= 1:
            continue
        # Collect all unique sources across all occurrences in this group
        all_sources: set[str] = set()
        for occ in group:
            for src in (occ.get("sources_present") or occ.get("sources") or []):
                if src:
                    all_sources.add(src)
        if len(all_sources) <= 1:
            continue
        # Update all occurrences in this group with merged sources
        merged_combo = "|".join(sorted(all_sources))
        for occ in group:
            old_combo = occ.get("sources_combo", "")
            if old_combo != merged_combo:
                occ["sources_combo"] = merged_combo
                occ["sources_present"] = sorted(all_sources)
                occ["cross_source_merged"] = True
                cross_source_merges += 1
    print(f"[ledger] Cross-source consensus merges: {cross_source_merges}")
    print(
        f"[ledger] support_id_corrected={counters.get('supports_source_id_corrected',0)} spreads_with_supports={counters.get('spreads_with_supports',0)} "
        f"spreads_with_support_lines={counters.get('spreads_with_support_lines',0)} spreads_promoted_from_supports={counters.get('spreads_promoted_from_supports',0)} spreads_multi_line={counters.get('spreads_multi_line',0)}"
    )

    # Spread diagnostics
    spread_total = 0
    spread_missing = 0
    spread_missing_by_surface = Counter()
    spread_promoted = 0
    spread_promoted_sources = Counter()
    for occ in occurrences:
        if (occ.get("market_type") or "").lower() != "spread":
            continue
        spread_total += 1
        if occ.get("spread_line_promoted"):
            spread_promoted += 1
            src = occ.get("spread_line_source") or "unknown"
            spread_promoted_sources[src] += 1
        if occ.get("line") is None and occ.get("line_median") is None:
            spread_missing += 1
            spread_missing_by_surface[occ.get("source_surface") or "unknown"] += 1
    print(f"[ledger] Spread totals: total={spread_total} missing_line={spread_missing} promoted={spread_promoted}")
    if spread_missing_by_surface:
        print(f"[ledger] missing_line by source_surface (top 10): {spread_missing_by_surface.most_common(10)}")
    if spread_promoted_sources:
        print(f"[ledger] spread_line promoted source tags (top 10): {spread_promoted_sources.most_common(10)}")

    # Use sport-specific output paths
    occurrences_path = get_occurrences_path(sport)
    latest_path = get_latest_path(sport)

    write_jsonl(occurrences_path, occurrences)
    latest_rows = pick_latest(occurrences)
    write_jsonl(latest_path, latest_rows)
    print(f"Wrote {len(occurrences)} occurrences to {occurrences_path}")
    print(f"Wrote {len(latest_rows)} latest rows to {latest_path}")
    if counters:
        print(f"[DEBUG] prop_prefix_fixes={counters.get('prop_prefix_fixes',0)} prop_prefix_bad={counters.get('prop_prefix_bad',0)}")
    if counters.get("prop_prefix_bad", 0) > 0 and args.debug:
        print("[DEBUG] prop_prefix bad samples (max 10):")
        for ex in bad_examples[:10]:
            print(f"  run_id={ex.get('run_id')} signal_type={ex.get('signal_type')} sources={ex.get('sources_combo')} selection={ex.get('selection')}")
        raise AssertionError(f"player_prop selections missing {sport} prefix in ledger output")


if __name__ == "__main__":
    main()
