from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.build_signal_ledger import canonicalize_game_keys, get_data_store  # type: ignore
from src.signal_keys import build_offer_key, build_selection_key


REPO_ROOT = Path(__file__).resolve().parents[1]
SIGNALS_PATH = REPO_ROOT / "data/ledger/signals_latest.jsonl"
GRADES_PATH = REPO_ROOT / "data/ledger/grades_latest.jsonl"
SPORT = "NBA"

LEGACY_EVENT_RE = re.compile(r"^(NBA|NCAAB):(\d{4})(\d{2})(\d{2}):([A-Z]{2,8})@([A-Z]{2,8})(?::\d{4})?$")
CANON_EVENT_RE = re.compile(r"^(NBA|NCAAB):(\d{4}):(\d{2}):(\d{2}):([A-Z]{2,8})@([A-Z]{2,8})$")
BETQL_URL_RE = re.compile(r"/nba/game-predictions/([a-z0-9\-]+)-(\d{2})-(\d{2})-(\d{4})", re.IGNORECASE)


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
            f.write("\n")


def pick_line(signal: Dict[str, Any]) -> Optional[float]:
    for key in ("line_median", "line", "line_max", "line_min"):
        val = signal.get(key)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                continue
    return None


def pick_odds(signal: Dict[str, Any]) -> Optional[int]:
    if signal.get("best_odds") is not None:
        try:
            return int(float(signal["best_odds"]))
        except (TypeError, ValueError):
            pass
    odds_list = signal.get("odds_list") or signal.get("odds") or []
    if isinstance(odds_list, list) and odds_list:
        try:
            return int(float(odds_list[0]))
        except (TypeError, ValueError):
            return None
    return None


def extract_stat_key(signal: Dict[str, Any]) -> Optional[str]:
    selection = signal.get("selection")
    if isinstance(selection, str) and "::" in selection:
        parts = selection.split("::")
        if len(parts) >= 3:
            return parts[1]
    stat = signal.get("atomic_stat")
    return str(stat) if isinstance(stat, str) and stat else None


def parse_direction(signal: Dict[str, Any]) -> Optional[str]:
    direction = signal.get("direction")
    if isinstance(direction, str) and direction:
        upper = direction.upper()
        if "OVER" in upper:
            return "OVER"
        if "UNDER" in upper:
            return "UNDER"
        return upper
    selection = signal.get("selection")
    if isinstance(selection, str) and "::" in selection:
        parts = selection.split("::")
        if len(parts) >= 3:
            return parts[2].upper()
    return None


def normalize_event_key(event_key: Optional[str], away_team: Optional[str], home_team: Optional[str]) -> Optional[str]:
    if not isinstance(event_key, str):
        return None
    if match := LEGACY_EVENT_RE.match(event_key):
        sport, yyyy, mm, dd, away, home = match.groups()
        event_key = f"{sport}:{yyyy}:{mm}:{dd}:{away}@{home}"
    if match := CANON_EVENT_RE.match(event_key):
        sport, yyyy, mm, dd, away, home = match.groups()
        away_fixed = (away_team or away).upper()
        home_fixed = (home_team or home).upper()
        return f"{sport}:{yyyy}:{mm}:{dd}:{away_fixed}@{home_fixed}"
    return None


def parse_betql_url(url: Optional[str]) -> Optional[Tuple[str, str, str]]:
    if not isinstance(url, str):
        return None
    match = BETQL_URL_RE.search(url)
    if not match:
        return None
    teams_slug, mm, dd, yyyy = match.groups()
    if "-vs-" not in teams_slug:
        return None
    away_slug, home_slug = teams_slug.split("-vs-", 1)
    store = get_data_store(SPORT)
    away_codes = store.lookup_team_code(away_slug.replace("-", " "))
    home_codes = store.lookup_team_code(home_slug.replace("-", " "))
    if not away_codes or not home_codes:
        return None
    away = next(iter(away_codes))
    home = next(iter(home_codes))
    return f"{SPORT}:{yyyy}:{mm}:{dd}", away, home


def build_selection_and_offer_keys(signal: Dict[str, Any]) -> Tuple[str, str]:
    market = signal.get("market_type") or ""
    selection_key = build_selection_key(
        day_key=signal.get("day_key") or "",
        market_type=market,
        selection=signal.get("selection"),
        player_id=signal.get("player_id"),
        atomic_stat=extract_stat_key(signal),
        direction=parse_direction(signal),
        team=signal.get("selection") if market in {"spread", "moneyline"} else None,
        event_key=signal.get("event_key"),
    )
    offer_key = build_offer_key(selection_key, pick_line(signal), pick_odds(signal))
    return selection_key, offer_key


def main() -> None:
    grades = read_jsonl(GRADES_PATH)
    signals = read_jsonl(SIGNALS_PATH)

    bad_signal_ids = {
        grade.get("signal_id")
        for grade in grades
        if grade.get("status") == "ERROR"
        and (grade.get("grade_notes") or grade.get("notes") or grade.get("error_reason")) == "game_not_found"
    }

    updated_rows = 0
    targeted_rows = 0
    repaired_signal_ids: List[str] = []

    for signal in signals:
        signal_id = signal.get("signal_id")
        if signal_id not in bad_signal_ids:
            continue
        if "betql" not in str(signal.get("sources_combo") or ""):
            continue
        targeted_rows += 1

        new_day_key: Optional[str] = None
        new_away: Optional[str] = None
        new_home: Optional[str] = None

        parsed_from_url = parse_betql_url(signal.get("canonical_url"))
        if parsed_from_url:
            new_day_key, new_away, new_home = parsed_from_url
        else:
            normalized_event = normalize_event_key(signal.get("event_key"), signal.get("away_team"), signal.get("home_team"))
            if normalized_event and (match := CANON_EVENT_RE.match(normalized_event)):
                _, yyyy, mm, dd, away, home = match.groups()
                new_day_key = f"{SPORT}:{yyyy}:{mm}:{dd}"
                new_away = away
                new_home = home

        if not (new_day_key and new_away and new_home):
            continue

        event_key, matchup_key, canonical_game_key = canonicalize_game_keys(new_day_key, new_away, new_home, sport=SPORT)
        if not (event_key and matchup_key and canonical_game_key):
            continue

        original = (
            signal.get("day_key"),
            signal.get("away_team"),
            signal.get("home_team"),
            signal.get("event_key"),
            signal.get("matchup_key"),
            tuple(signal.get("canonical_game_key")) if isinstance(signal.get("canonical_game_key"), list) else signal.get("canonical_game_key"),
        )
        updated = (
            new_day_key,
            new_away,
            new_home,
            event_key,
            matchup_key,
            canonical_game_key,
        )
        if original == updated:
            continue

        signal["day_key"] = new_day_key
        signal["away_team"] = new_away
        signal["home_team"] = new_home
        signal["event_key"] = event_key
        signal["canonical_event_key"] = event_key
        signal["matchup_key"] = matchup_key
        signal["canonical_game_key"] = list(canonical_game_key)
        if signal.get("matchup"):
            signal["matchup"] = f"{new_away}@{new_home}"

        selection_key, offer_key = build_selection_and_offer_keys(signal)
        signal["selection_key"] = selection_key
        signal["offer_key"] = offer_key

        updated_rows += 1
        repaired_signal_ids.append(str(signal_id))

    write_jsonl(SIGNALS_PATH, signals)

    print(json.dumps(
        {
            "targeted_bad_betql_rows": targeted_rows,
            "updated_signal_rows": updated_rows,
            "repaired_signal_ids": repaired_signal_ids,
        },
        indent=2,
    ))


if __name__ == "__main__":
    main()
