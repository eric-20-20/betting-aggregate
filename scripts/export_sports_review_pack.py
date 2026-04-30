#!/usr/bin/env python3
"""Export a read-only NBA/MLB review pack for manual data inspection.

This script does not mutate pipeline inputs or outputs. It reads normalized
records, ledger files, consensus outputs, and optional web exports, then writes
CSV review artifacts under data/reports/sports_review_pack/.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))

from data import TEAM_SEED
from data_mlb import MLB_TEAM_SEED


SPORTS = ("NBA", "MLB")
QUALITY_CLEAN = "CLEAN"
QUALITY_WARN = "WARN"
QUALITY_FAIL = "FAIL"

KNOWN_MARKET_TYPES = {
    "moneyline",
    "spread",
    "total",
    "player_prop",
    "team_total",
    "first_half_spread",
    "first_half_total",
    "first_half_moneyline",
}
LINE_REQUIRED_MARKETS = {"spread", "total", "player_prop", "team_total", "first_half_spread", "first_half_total"}

NBA_TEAMS = {team.code for team in TEAM_SEED}
MLB_TEAMS = {team.code for team in MLB_TEAM_SEED}

DAY_KEY_RE = re.compile(r"^(NBA|MLB):(\d{4}):(\d{2}):(\d{2})$")
EVENT_KEY_RE = re.compile(r"^(NBA|MLB):(\d{4}):(\d{2}):(\d{2}):")
COMPACT_EVENT_RE = re.compile(r"^(NBA|MLB):(\d{4})(\d{2})(\d{2}):")
FILE_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


@dataclass(frozen=True)
class SourceInput:
    path: Path
    category: str
    required: bool
    note: str = ""


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Export NBA/MLB review pack CSVs.")
    ap.add_argument("--sport", choices=("NBA", "MLB", "ALL"), default="ALL")
    ap.add_argument("--date", help="Start date YYYY-MM-DD or exact date if --to omitted")
    ap.add_argument("--to", help="End date YYYY-MM-DD")
    ap.add_argument("--out-dir", default="data/reports/sports_review_pack")
    ap.add_argument("--include-normalized", action="store_true", default=True)
    ap.add_argument("--include-ledger", action="store_true", default=True)
    ap.add_argument("--include-consensus", action="store_true", default=True)
    return ap.parse_args()


def validate_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    return date.fromisoformat(value)


def date_window(args: argparse.Namespace) -> Tuple[Optional[date], Optional[date]]:
    start = validate_date(args.date)
    end = validate_date(args.to)
    if start and not end:
        end = start
    if end and not start:
        start = end
    if start and end and start > end:
        raise SystemExit(f"--date {start} is after --to {end}")
    return start, end


def daterange(start: date, end: date) -> Iterable[date]:
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def as_rel(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def read_json_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    if path.suffix == ".jsonl":
        rows: List[Dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                parsed = json.loads(line)
                if isinstance(parsed, dict):
                    rows.append(parsed)
        return rows
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    data = json.loads(text)
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        return [data]
    return []


def infer_source_from_path(path: Path) -> str:
    name = path.name
    for prefix in ("normalized_", "consensus_"):
        if name.startswith(prefix):
            name = name[len(prefix):]
    for suffix in (".jsonl", ".json", ".csv"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
    return name


def parse_date_from_string(value: Optional[str]) -> Optional[str]:
    if not value or not isinstance(value, str):
        return None
    m = DAY_KEY_RE.match(value)
    if m:
        return f"{m.group(2)}-{m.group(3)}-{m.group(4)}"
    m = EVENT_KEY_RE.match(value)
    if m:
        return f"{m.group(2)}-{m.group(3)}-{m.group(4)}"
    m = COMPACT_EVENT_RE.match(value)
    if m:
        return f"{m.group(2)}-{m.group(3)}-{m.group(4)}"
    m = FILE_DATE_RE.search(value)
    if m:
        return m.group(1)
    if len(value) >= 10:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).date().isoformat()
        except ValueError:
            pass
    return None


def row_date(row: Dict[str, Any], path: Optional[Path] = None) -> Optional[str]:
    event = row.get("event") or {}
    for candidate in (
        event.get("day_key"),
        row.get("day_key"),
        event.get("canonical_event_key"),
        event.get("event_key"),
        row.get("event_key"),
        event.get("event_start_time_utc"),
        (row.get("provenance") or {}).get("observed_at_utc"),
        row.get("observed_at_utc"),
    ):
        parsed = parse_date_from_string(candidate)
        if parsed:
            return parsed
    if path:
        return parse_date_from_string(path.name)
    return None


def in_range(iso_date: Optional[str], start: Optional[date], end: Optional[date]) -> bool:
    if not iso_date:
        return start is None and end is None
    d = date.fromisoformat(iso_date)
    if start and d < start:
        return False
    if end and d > end:
        return False
    return True


def source_id_from_row(row: Dict[str, Any], path: Path) -> str:
    return (
        (row.get("provenance") or {}).get("source_id")
        or row.get("source_id")
        or infer_source_from_path(path)
    )


def market_type_from_row(row: Dict[str, Any]) -> Optional[str]:
    market = row.get("market") or {}
    return market.get("market_type") or row.get("market_type")


def event_key_from_row(row: Dict[str, Any]) -> Optional[str]:
    event = row.get("event") or {}
    return event.get("canonical_event_key") or event.get("event_key") or row.get("event_key")


def day_key_from_row(row: Dict[str, Any]) -> Optional[str]:
    event = row.get("event") or {}
    return event.get("day_key") or row.get("day_key")


def _derive_matchup_key(event_key: Optional[str], away: Optional[str], home: Optional[str]) -> Optional[str]:
    """Derive matchup_key from event_key and team codes when not explicit."""
    if not away or not home:
        return None
    # Extract day portion from event_key: "MLB:2026:04:27:CLE@TBR" or "MLB:20260427:CLE@TBR:1600"
    if not event_key:
        return None
    m = DAY_KEY_RE.match(event_key)
    if m:
        day_key = event_key
    else:
        m2 = EVENT_KEY_RE.match(event_key)
        if m2:
            day_key = f"{m2.group(1)}:{m2.group(2)}:{m2.group(3)}:{m2.group(4)}"
        else:
            m3 = COMPACT_EVENT_RE.match(event_key)
            if m3:
                day_key = f"{m3.group(1)}:{m3.group(2)}:{m3.group(3)}:{m3.group(4)}"
            else:
                return None
    t1, t2 = sorted([str(away).upper(), str(home).upper()])
    return f"{day_key}:{t1}-{t2}"


def matchup_key_from_row(row: Dict[str, Any]) -> Optional[str]:
    event = row.get("event") or {}
    mk = event.get("matchup_key") or row.get("matchup_key") or row.get("matchup")
    if mk:
        return mk
    # Derive from event_key + teams when not explicit
    ek = event.get("canonical_event_key") or event.get("event_key") or row.get("event_key")
    away = event.get("away_team") or row.get("away_team")
    home = event.get("home_team") or row.get("home_team")
    return _derive_matchup_key(ek, away, home)


def line_from_row(row: Dict[str, Any]) -> Any:
    market = row.get("market") or {}
    return market.get("line", row.get("line"))


def odds_from_row(row: Dict[str, Any]) -> Any:
    market = row.get("market") or {}
    odds = market.get("odds", row.get("best_odds"))
    if odds is None:
        odds_list = row.get("odds_list") or row.get("odds")
        if isinstance(odds_list, list) and odds_list:
            return odds_list[0]
    return odds


def selection_from_row(row: Dict[str, Any]) -> Optional[str]:
    market = row.get("market") or {}
    return market.get("selection") or row.get("selection")


def player_key_from_row(row: Dict[str, Any]) -> Optional[str]:
    market = row.get("market") or {}
    return market.get("player_key") or row.get("player_key") or row.get("player_id")


def stat_type_from_row(row: Dict[str, Any]) -> Optional[str]:
    market = row.get("market") or {}
    return market.get("stat_key") or row.get("atomic_stat")


def side_from_row(row: Dict[str, Any]) -> Optional[str]:
    market = row.get("market") or {}
    return market.get("side") or row.get("direction")


def eligible_from_row(row: Dict[str, Any]) -> bool:
    eligibility = row.get("eligibility") or {}
    value = eligibility.get("eligible_for_consensus")
    if value is None:
        value = row.get("eligible_for_consensus")
    return bool(value)


def ineligible_reason_from_row(row: Dict[str, Any]) -> Optional[str]:
    eligibility = row.get("eligibility") or {}
    return eligibility.get("ineligibility_reason") or row.get("ineligibility_reason")


def game_from_row(row: Dict[str, Any]) -> str:
    event = row.get("event") or {}
    away = event.get("away_team") or row.get("away_team") or ""
    home = event.get("home_team") or row.get("home_team") or ""
    if away and home:
        return f"{away} @ {home}"
    return ""


def parse_raw_block(raw_block: Any) -> Optional[Dict[str, Any]]:
    if isinstance(raw_block, dict):
        return raw_block
    if not isinstance(raw_block, str):
        return None
    raw_block = raw_block.strip()
    if not raw_block or not raw_block.startswith("{"):
        return None
    try:
        parsed = json.loads(raw_block)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def titleize_key(value: Optional[str]) -> str:
    if not value or not isinstance(value, str):
        return ""
    tail = value.split(":")[-1]
    return " ".join(part.capitalize() for part in tail.replace("-", "_").split("_") if part)


def extract_player_name(row: Dict[str, Any]) -> str:
    market = row.get("market") or {}
    provenance = row.get("provenance") or {}
    for candidate in (
        market.get("player_name"),
        row.get("player_name"),
        provenance.get("player_name"),
    ):
        if candidate:
            return str(candidate)
    raw = parse_raw_block(provenance.get("raw_block"))
    if raw:
        participant = raw.get("participant") or {}
        player = participant.get("player") or {}
        name = participant.get("name") or player.get("full_name")
        if name:
            return str(name)
    selection = selection_from_row(row)
    if selection and "::" in selection:
        return titleize_key(selection.split("::", 1)[0])
    return titleize_key(player_key_from_row(row))


def extract_player_team(row: Dict[str, Any]) -> str:
    market = row.get("market") or {}
    provenance = row.get("provenance") or {}
    for candidate in (
        market.get("team"),
        row.get("team"),
        provenance.get("team"),
    ):
        if candidate:
            return str(candidate)
    raw = parse_raw_block(provenance.get("raw_block"))
    if raw:
        participant = raw.get("participant") or {}
        player = participant.get("player") or {}
        for candidate in (player.get("team"), participant.get("team")):
            if candidate:
                return str(candidate)
    return ""


def extract_prop_team_opponent(row: Dict[str, Any]) -> Tuple[str, str]:
    event = row.get("event") or {}
    home = event.get("home_team") or row.get("home_team") or ""
    away = event.get("away_team") or row.get("away_team") or ""
    team = extract_player_team(row)
    if team == home:
        return team, away
    if team == away:
        return team, home
    return team, ""


def normalize_sources_field(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(v) for v in value if v is not None and str(v)]
    if isinstance(value, str) and value:
        return [part for part in value.split("|") if part]
    return []


def synthetic_pick_id(row: Dict[str, Any], path: Path) -> str:
    provenance = row.get("provenance") or {}
    return (
        provenance.get("raw_fingerprint")
        or row.get("offer_key")
        or row.get("selection_key")
        or "|".join(
            [
                source_id_from_row(row, path),
                row_date(row, path) or "",
                event_key_from_row(row) or "",
                matchup_key_from_row(row) or "",
                market_type_from_row(row) or "",
                selection_from_row(row) or "",
                str(line_from_row(row) if line_from_row(row) is not None else ""),
                str(odds_from_row(row) if odds_from_row(row) is not None else ""),
            ]
        )
    )


def signal_row_to_export(sport: str, row: Dict[str, Any]) -> Dict[str, Any]:
    sources = normalize_sources_field(row.get("sources") or row.get("sources_present") or row.get("sources_combo"))
    return {
        "sport": sport,
        "date": row_date(row),
        "signal_id": row.get("signal_id") or row.get("signal_key"),
        "signal_type": row.get("signal_type"),
        "event_key": row.get("event_key"),
        "matchup_key": row.get("matchup_key") or row.get("matchup"),
        "market_type": row.get("market_type"),
        "selection": row.get("selection") or row.get("selection_example"),
        "line": row.get("line"),
        "odds": odds_from_row(row),
        "sources": "|".join(sources),
        "source_count": len(sources),
        "tier": row.get("tier"),
        "pattern": row.get("pattern"),
        "score": row.get("score"),
        "expert_strength": row.get("expert_strength"),
        "source_strength": row.get("source_strength"),
        "count_total": row.get("count_total"),
        "best_odds": row.get("best_odds"),
        "edge_pct": row.get("edge_pct"),
        "probability_pct": row.get("probability_pct"),
    }


def normalized_row_to_export(sport: str, row: Dict[str, Any], path: Path) -> Dict[str, Any]:
    return {
        "sport": sport,
        "date": row_date(row, path),
        "source": source_id_from_row(row, path),
        "event_key": event_key_from_row(row),
        "matchup_key": matchup_key_from_row(row),
        "game": game_from_row(row),
        "market_type": market_type_from_row(row),
        "selection": selection_from_row(row),
        "line": line_from_row(row),
        "odds": odds_from_row(row),
        "player_name": extract_player_name(row),
        "player_key": player_key_from_row(row),
        "stat_type": stat_type_from_row(row),
        "eligible_for_consensus": eligible_from_row(row),
        "reason_if_ineligible": ineligible_reason_from_row(row),
    }


def player_prop_row_to_export(sport: str, row: Dict[str, Any], path: Path) -> Dict[str, Any]:
    team, opponent = extract_prop_team_opponent(row)
    return {
        "sport": sport,
        "date": row_date(row, path),
        "player_name": extract_player_name(row),
        "player_key": player_key_from_row(row),
        "team": team,
        "opponent": opponent,
        "stat_type": stat_type_from_row(row),
        "selection": selection_from_row(row),
        "line": line_from_row(row),
        "odds": odds_from_row(row),
        "source": source_id_from_row(row, path),
        "eligible_for_consensus": eligible_from_row(row),
        "event_key": event_key_from_row(row),
        "matchup_key": matchup_key_from_row(row),
    }


def coverage_row(
    sport: str,
    source: str,
    path: Path,
    rows: Sequence[Dict[str, Any]],
    notes: str = "",
) -> Dict[str, Any]:
    dates = [row_date(row, path) for row in rows if row_date(row, path)]
    market_types = sorted({market_type_from_row(row) for row in rows if market_type_from_row(row)})
    player_prop_rows = [row for row in rows if market_type_from_row(row) == "player_prop"]
    return {
        "sport": sport,
        "source": source,
        "file": as_rel(path),
        "row_count": len(rows),
        "eligible_count": sum(1 for row in rows if eligible_from_row(row)),
        "date_min": min(dates) if dates else "",
        "date_max": max(dates) if dates else "",
        "market_types": "|".join(market_types),
        "missing_event_key_count": sum(1 for row in rows if not event_key_from_row(row)),
        "missing_matchup_key_count": sum(1 for row in rows if not matchup_key_from_row(row)),
        "missing_player_key_count": sum(1 for row in player_prop_rows if not player_key_from_row(row)),
        "notes": notes,
    }


def write_csv(path: Path, fieldnames: Sequence[str], rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def expected_inputs_for_sport(sport: str, start: Optional[date], end: Optional[date], args: argparse.Namespace) -> List[SourceInput]:
    out = REPO_ROOT / "out"
    inputs: List[SourceInput] = []

    def add(path: str, category: str, required: bool, note: str = "") -> None:
        inputs.append(SourceInput(REPO_ROOT / path, category, required, note))

    if sport == "NBA":
        if args.include_normalized:
            for path in (
                "out/normalized_action_nba.json",
                "out/normalized_bettingpros_experts_nba.json",
                "out/normalized_bettingpros_prop_bets_nba.json",
                "out/normalized_betql_model_nba.json",
                "out/normalized_betql_prop_nba.json",
                "out/normalized_betql_sharp_nba.json",
                "out/normalized_betql_spread_nba.json",
                "out/normalized_betql_total_nba.json",
                "out/normalized_covers_nba.json",
                "out/normalized_dimers_nba.json",
                "out/normalized_juicereel_nba.json",
                "out/normalized_oddstrader_nba.json",
                "out/normalized_oddstrader_prop_nba.json",
                "out/normalized_sportscapping_nba.json",
                "out/normalized_sportsline_nba.json",
                "out/normalized_vegasinsider_nba.json",
            ):
                add(path, "normalized", True)
            for path in (
                "out/normalized_action_nba_backfill.jsonl",
                "out/normalized_juicereel_backfill_nba.json",
                "out/normalized_sportsline_expert_pages_nba_canonical.jsonl",
                "out/normalized_sportsline_nba_backfill.jsonl",
                "out/normalized_sportsline_nba_expert_picks.jsonl",
                "out/normalized_sportsline_nba_history.jsonl",
            ):
                add(path, "normalized", False, "optional backfill/history")
        if args.include_consensus:
            add("out/consensus_nba_v1.json", "consensus", True)
            for path in sorted(out.glob("consensus_nba_*.json")):
                if path.name != "consensus_nba_v1.json":
                    inputs.append(SourceInput(path, "consensus", False, "optional consensus variant"))
        if args.include_ledger:
            add("data/ledger/signals_latest.jsonl", "ledger", True)
            add("data/ledger/signals_occurrences.jsonl", "ledger", False, "ledger history")
        if start and end:
            for d in daterange(start, end):
                iso = d.isoformat()
                add(f"web/data/private/picks_{iso}.json", "web_export", False, "web private picks export")
                add(f"web/data/private/history/history_{iso}.json", "web_export", False, "web private history export")
    elif sport == "MLB":
        if args.include_normalized:
            # Persistent history files (append-only, comprehensive)
            add("out/normalized_action_mlb_backfill.jsonl", "normalized", True, "Action scoreboard backfill")
            add("out/normalized_sportsline_expert_pages_mlb_canonical.jsonl", "normalized", True, "SportsLine expert backfill")
            # Ephemeral live files (latest run snapshot — may be empty between runs)
            for path in (
                "out/normalized_action_mlb.json",
                "out/normalized_covers_mlb.json",
                "out/normalized_oddstrader_mlb.json",
                "out/normalized_bettingpros_prop_bets_mlb.json",
                "out/normalized_sportsline_mlb.json",
            ):
                add(path, "normalized", False, "live snapshot (may be empty)")
            add("out/normalized_dimers_mlb.json", "normalized", False, "deferred source")
        if args.include_consensus:
            add("out/consensus_mlb_v1.json", "consensus", True)
            for path in sorted(out.glob("consensus_mlb_*.json")):
                if path.name != "consensus_mlb_v1.json":
                    inputs.append(SourceInput(path, "consensus", False, "optional consensus variant"))
        if args.include_ledger:
            add("data/ledger/mlb/signals_latest.jsonl", "ledger", True)
            add("data/ledger/mlb/signals_occurrences.jsonl", "ledger", True)
    return inputs


def load_rows_for_input(
    source_input: SourceInput,
    start: Optional[date],
    end: Optional[date],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if not source_input.path.exists():
        return [], []
    if source_input.path.suffix == ".jsonl":
        filtered: List[Dict[str, Any]] = []
        with source_input.path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                if not isinstance(row, dict):
                    continue
                if in_range(row_date(row, source_input.path), start, end):
                    filtered.append(row)
        return [], filtered
    rows = read_json_rows(source_input.path)
    filtered = [row for row in rows if in_range(row_date(row, source_input.path), start, end)]
    return rows, filtered


def summarize_web_export_rows(path: Path, rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    notes = "web export"
    if rows and rows[0].get("meta"):
        notes = f"{notes}; exported_count={rows[0]['meta'].get('exported_count', '')}"
    return {
        "sport": "",
        "source": infer_source_from_path(path),
        "file": as_rel(path),
        "row_count": len(rows[0].get("plays", [])) if rows and isinstance(rows[0].get("plays"), list) else 0,
        "eligible_count": "",
        "date_min": rows[0].get("meta", {}).get("date", "") if rows else "",
        "date_max": rows[0].get("meta", {}).get("date", "") if rows else "",
        "market_types": "",
        "missing_event_key_count": "",
        "missing_matchup_key_count": "",
        "missing_player_key_count": "",
        "notes": notes,
    }


def issue_row(
    sport: str,
    severity: str,
    issue_type: str,
    file: Path,
    source: str = "",
    date_value: str = "",
    event_key: str = "",
    matchup_key: str = "",
    selection: str = "",
    player_key: str = "",
    details: str = "",
    count: int = 1,
    market_type: str = "",
    line: str = "",
    pick_id: str = "",
) -> Dict[str, Any]:
    return {
        "sport": sport,
        "severity": severity,
        "issue_type": issue_type,
        "file": as_rel(file),
        "source": source,
        "date": date_value,
        "event_key": event_key,
        "matchup_key": matchup_key,
        "market_type": market_type,
        "selection": selection,
        "line": line,
        "player_key": player_key,
        "pick_id": pick_id,
        "details": details,
        "count": count,
    }


def validate_normalized_rows(
    sport: str,
    path: Path,
    rows: Sequence[Dict[str, Any]],
    start: Optional[date],
    end: Optional[date],
) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    expected_prefix = f"{sport}:"
    wrong_prefix = "MLB:" if sport == "NBA" else "NBA:"
    canonical_teams = NBA_TEAMS if sport == "NBA" else MLB_TEAMS
    pick_ids: Counter[str] = Counter()

    for row in rows:
        row_source = source_id_from_row(row, path)
        dt = row_date(row, path) or ""
        event_key = event_key_from_row(row) or ""
        matchup_key = matchup_key_from_row(row) or ""
        selection = selection_from_row(row) or ""
        player_key = player_key_from_row(row) or ""
        market_type = market_type_from_row(row) or ""
        day_key = day_key_from_row(row) or ""
        line_val = line_from_row(row)
        line_str = str(line_val) if line_val is not None else ""
        pid = synthetic_pick_id(row, path)
        event = row.get("event") or {}
        home = event.get("home_team") or row.get("home_team")
        away = event.get("away_team") or row.get("away_team")

        pick_ids[pid] += 1

        # Common kwargs for issue_row in this loop
        def _issue(sev: str, itype: str, details: str = "") -> Dict[str, Any]:
            return issue_row(sport, sev, itype, path, row_source, dt, event_key, matchup_key,
                             selection, player_key, details, market_type=market_type,
                             line=line_str, pick_id=pid)

        if event_key and not event_key.startswith(expected_prefix):
            issues.append(_issue(QUALITY_FAIL, "wrong sport prefix", f"event_key={event_key}"))
        if day_key and not day_key.startswith(expected_prefix):
            issues.append(_issue(QUALITY_FAIL, "wrong sport prefix", f"day_key={day_key}"))
        if player_key.startswith(wrong_prefix) or selection.startswith(wrong_prefix):
            label = "NBA prefix in MLB" if sport == "MLB" else "MLB prefix in NBA"
            issues.append(_issue(QUALITY_FAIL, label, "wrong-sport player/selection prefix"))

        if not event_key:
            issues.append(_issue(QUALITY_WARN, "missing event_key"))
        if (home and away) and not matchup_key:
            issues.append(_issue(QUALITY_WARN, "missing matchup_key"))
        if market_type == "player_prop" and not player_key:
            issues.append(_issue(QUALITY_WARN, "missing player_key on prop"))

        for team_code in (home, away):
            if team_code and team_code not in canonical_teams:
                severity = QUALITY_FAIL if sport == "MLB" else QUALITY_WARN
                issues.append(_issue(severity, "noncanonical team code", f"team_code={team_code}"))

        if market_type and market_type not in KNOWN_MARKET_TYPES:
            issues.append(_issue(QUALITY_WARN, "unknown market type", f"market_type={market_type}"))
        if not selection:
            issues.append(_issue(QUALITY_WARN, "null selection"))
        if market_type in LINE_REQUIRED_MARKETS and line_val is None:
            issues.append(_issue(QUALITY_WARN, "null line where line is required", f"market_type={market_type}"))

        if (start or end) and dt and not in_range(dt, start, end):
            issues.append(_issue(QUALITY_WARN, "date outside requested range"))

    for pick_id, count in pick_ids.items():
        if count > 1:
            issues.append(issue_row(sport, QUALITY_WARN, "duplicate pick IDs", path, details=pick_id, count=count))
    return issues


def validate_signal_rows(
    sport: str,
    path: Path,
    rows: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    expected_prefix = f"{sport}:"
    wrong_prefix = "MLB:" if sport == "NBA" else "NBA:"
    signal_ids = Counter()

    for row in rows:
        dt = row_date(row, path) or ""
        event_key = row.get("event_key") or ""
        matchup_key = row.get("matchup_key") or row.get("matchup") or ""
        selection = row.get("selection") or row.get("selection_example") or ""
        player_key = row.get("player_key") or row.get("player_id") or ""
        day_key = row.get("day_key") or ""
        signal_id = row.get("signal_id") or row.get("signal_key") or ""
        if signal_id:
            signal_ids[signal_id] += 1

        if event_key and not event_key.startswith(expected_prefix):
            issues.append(issue_row(sport, QUALITY_FAIL, "wrong sport prefix", path, "ledger", dt, event_key, matchup_key, selection, player_key, f"event_key={event_key}"))
        if day_key and not day_key.startswith(expected_prefix):
            issues.append(issue_row(sport, QUALITY_FAIL, "wrong sport prefix", path, "ledger", dt, event_key, matchup_key, selection, player_key, f"day_key={day_key}"))
        if player_key.startswith(wrong_prefix) or selection.startswith(wrong_prefix):
            label = "NBA prefix in MLB" if sport == "MLB" else "MLB prefix in NBA"
            issues.append(issue_row(sport, QUALITY_FAIL, label, path, "ledger", dt, event_key, matchup_key, selection, player_key, "wrong-sport player/selection prefix"))

    for signal_id, count in signal_ids.items():
        if count > 1:
            issues.append(issue_row(sport, QUALITY_WARN, "duplicate signal IDs", path, "ledger", details=signal_id, count=count))
    return issues


def top_n(counter: Counter[str], limit: int = 10) -> str:
    return ", ".join(f"{key}={value}" for key, value in counter.most_common(limit))


def evaluate_quality_status(issues: Sequence[Dict[str, Any]]) -> str:
    severities = {issue["severity"] for issue in issues}
    if QUALITY_FAIL in severities:
        return QUALITY_FAIL
    if QUALITY_WARN in severities:
        return QUALITY_WARN
    return QUALITY_CLEAN


def expected_latest_date(start: Optional[date], end: Optional[date]) -> Optional[str]:
    if end:
        return end.isoformat()
    if start:
        return start.isoformat()
    return None


def export_sport(sport: str, args: argparse.Namespace, root_out: Path, start: Optional[date], end: Optional[date]) -> Dict[str, Any]:
    sport_out = root_out / sport
    source_inputs = expected_inputs_for_sport(sport, start, end, args)

    normalized_exports: List[Dict[str, Any]] = []
    signal_exports: List[Dict[str, Any]] = []
    cross_source_exports: List[Dict[str, Any]] = []
    player_prop_exports: List[Dict[str, Any]] = []
    coverage_exports: List[Dict[str, Any]] = []
    issues: List[Dict[str, Any]] = []
    latest_signal_rows: List[Dict[str, Any]] = []
    latest_signal_path: Optional[Path] = None

    normalized_market_counter: Counter[str] = Counter()
    prop_player_counter: Counter[str] = Counter()
    normalized_source_counter: Counter[str] = Counter()
    seen_pick_ids: set = set()  # Dedup normalized rows across files

    for source_input in source_inputs:
        all_rows, filtered_rows = load_rows_for_input(source_input, start, end)
        file_source = infer_source_from_path(source_input.path)

        if not source_input.path.exists():
            note = source_input.note or ""
            note = f"{note}; missing file".strip("; ")
            coverage_exports.append(
                {
                    "sport": sport,
                    "source": file_source,
                    "file": as_rel(source_input.path),
                    "row_count": 0,
                    "eligible_count": 0,
                    "date_min": "",
                    "date_max": "",
                    "market_types": "",
                    "missing_event_key_count": 0,
                    "missing_matchup_key_count": 0,
                    "missing_player_key_count": 0,
                    "notes": note,
                }
            )
            if source_input.required:
                issues.append(issue_row(sport, QUALITY_WARN, "suspiciously low source coverage",
                                        source_input.path, file_source, details="missing required file"))
            elif source_input.category != "web_export" and "snapshot" not in (source_input.note or "") and "deferred" not in (source_input.note or ""):
                issues.append(issue_row(sport, QUALITY_WARN, "suspiciously low source coverage",
                                        source_input.path, file_source,
                                        details=f"missing optional file; {source_input.note or 'optional'}"))
            continue

        if source_input.category == "web_export":
            wrapped = [{"meta": row.get("meta", {}), "plays": row.get("plays", [])} for row in filtered_rows]
            coverage = summarize_web_export_rows(source_input.path, wrapped)
            coverage["sport"] = sport
            coverage["source"] = file_source
            coverage_exports.append(coverage)
            continue

        coverage_exports.append(coverage_row(sport, file_source, source_input.path, filtered_rows, source_input.note))

        if source_input.category == "normalized":
            issues.extend(validate_normalized_rows(sport, source_input.path, filtered_rows, start, end))
            for row in filtered_rows:
                pid = synthetic_pick_id(row, source_input.path)
                if pid in seen_pick_ids:
                    continue  # Dedup across files (same pick in backfill + run dir)
                seen_pick_ids.add(pid)
                export_row = normalized_row_to_export(sport, row, source_input.path)
                normalized_exports.append(export_row)
                if export_row["market_type"]:
                    normalized_market_counter[export_row["market_type"]] += 1
                normalized_source_counter[export_row["source"] or file_source] += 1
                if export_row["market_type"] == "player_prop":
                    prop_export = player_prop_row_to_export(sport, row, source_input.path)
                    player_prop_exports.append(prop_export)
                    prop_label = prop_export["player_name"] or prop_export["player_key"] or ""
                    if prop_label:
                        prop_player_counter[prop_label] += 1

        if source_input.category == "ledger" and source_input.path.name == "signals_latest.jsonl":
            latest_signal_rows = filtered_rows
            latest_signal_path = source_input.path
        elif source_input.category == "consensus" and latest_signal_path is None and source_input.path.name.endswith("_v1.json"):
            latest_signal_rows = filtered_rows
            latest_signal_path = source_input.path

    if latest_signal_rows and latest_signal_path:
        issues.extend(validate_signal_rows(sport, latest_signal_path, latest_signal_rows))
        for row in latest_signal_rows:
            export_row = signal_row_to_export(sport, row)
            signal_exports.append(export_row)
            if export_row["source_count"] > 1 or str(export_row["signal_type"] or "").endswith("cross_source") or "cross_source" in str(export_row["signal_type"] or ""):
                cross_source_exports.append(
                    {
                        "sport": sport,
                        "date": export_row["date"],
                        "event_key": export_row["event_key"],
                        "matchup_key": export_row["matchup_key"],
                        "market_type": export_row["market_type"],
                        "selection": export_row["selection"],
                        "line": export_row["line"],
                        "sources": export_row["sources"],
                        "source_count": export_row["source_count"],
                        "signal_id": export_row["signal_id"],
                    }
                )

    identifiable_rows = [row for row in normalized_exports if row["game"]]
    missing_matchups = [row for row in normalized_exports if row["game"] and not row["matchup_key"]]
    if identifiable_rows:
        missing_ratio = len(missing_matchups) / len(identifiable_rows)
        if missing_ratio > 0.05:
            issues.append(
                issue_row(
                    sport,
                    QUALITY_WARN,
                    "missing matchup_key",
                    sport_out / "normalized_picks.csv",
                    details=f"coverage below threshold: {len(missing_matchups)}/{len(identifiable_rows)} ({missing_ratio:.1%})",
                    count=len(missing_matchups),
                )
            )

    for coverage in coverage_exports:
        notes_str = str(coverage.get("notes") or "")
        skip_notes = ("web export", "snapshot", "deferred", "optional", "backfill")
        if (coverage["row_count"] == 0
                and not any(s in notes_str for s in skip_notes)
                and ("missing required file" in notes_str or REPO_ROOT.joinpath(coverage["file"]).exists())
        ):
            issues.append(
                issue_row(
                    sport,
                    QUALITY_WARN,
                    "suspiciously low source coverage",
                    REPO_ROOT / coverage["file"],
                    coverage["source"],
                    details=notes_str or "zero rows in requested range",
                )
            )

    expected_latest = expected_latest_date(start, end)
    if expected_latest:
        normalized_dates = [row["date"] for row in normalized_exports if row.get("date")]
        signal_dates = [row["date"] for row in signal_exports if row.get("date")]
        latest_seen = max(normalized_dates + signal_dates) if (normalized_dates or signal_dates) else None
        if latest_seen and latest_seen < expected_latest:
            issues.append(
                issue_row(
                    sport,
                    QUALITY_WARN,
                    "latest data is stale",
                    sport_out / "signals_latest.csv",
                    details=f"latest_seen={latest_seen}; expected_at_least={expected_latest}",
                )
            )

    status = evaluate_quality_status(issues)
    issue_counts = Counter(issue["issue_type"] for issue in issues)

    write_csv(
        sport_out / "source_coverage.csv",
        [
            "sport", "source", "file", "row_count", "eligible_count", "date_min", "date_max",
            "market_types", "missing_event_key_count", "missing_matchup_key_count",
            "missing_player_key_count", "notes",
        ],
        coverage_exports,
    )
    write_csv(
        sport_out / "normalized_picks.csv",
        [
            "sport", "date", "source", "event_key", "matchup_key", "game", "market_type",
            "selection", "line", "odds", "player_name", "player_key", "stat_type",
            "eligible_for_consensus", "reason_if_ineligible",
        ],
        normalized_exports,
    )
    write_csv(
        sport_out / "signals_latest.csv",
        [
            "sport", "date", "signal_id", "signal_type", "event_key", "matchup_key", "market_type",
            "selection", "line", "odds", "sources", "source_count", "tier", "pattern",
            "score", "expert_strength", "source_strength", "count_total", "best_odds",
            "edge_pct", "probability_pct",
        ],
        signal_exports,
    )
    write_csv(
        sport_out / "cross_source_signals.csv",
        [
            "sport", "date", "event_key", "matchup_key", "market_type", "selection",
            "line", "sources", "source_count", "signal_id",
        ],
        cross_source_exports,
    )
    write_csv(
        sport_out / "player_props.csv",
        [
            "sport", "date", "player_name", "player_key", "team", "opponent", "stat_type",
            "selection", "line", "odds", "source", "eligible_for_consensus", "event_key",
            "matchup_key",
        ],
        player_prop_exports,
    )
    write_csv(
        sport_out / "data_quality_issues.csv",
        [
            "sport", "severity", "issue_type", "file", "source", "date", "event_key",
            "matchup_key", "market_type", "selection", "line", "player_key", "pick_id",
            "details", "count",
        ],
        issues,
    )

    date_values = [row["date"] for row in normalized_exports if row.get("date")]
    summary = {
        "sport": sport,
        "status": status,
        "normalized_rows": len(normalized_exports),
        "eligible_rows": sum(1 for row in normalized_exports if row["eligible_for_consensus"]),
        "date_min": min(date_values) if date_values else "",
        "date_max": max(date_values) if date_values else "",
        "signals": len(signal_exports),
        "cross_source": len(cross_source_exports),
        "player_props": len(player_prop_exports),
        "top_markets": top_n(normalized_market_counter, 8),
        "top_players": top_n(prop_player_counter, 8),
        "source_counts": dict(normalized_source_counter),
        "issue_counts": dict(issue_counts),
        "must_fix_next": [
            issue["details"] or issue["issue_type"]
            for issue in issues
            if issue["severity"] == QUALITY_FAIL
        ][:10],
        "source_coverage_rows": coverage_exports,
        "signal_rows": signal_exports,
        "cross_rows": cross_source_exports,
        "player_rows": player_prop_exports,
        "issue_rows": issues,
    }
    return summary


def write_combined(root_out: Path, summaries: Sequence[Dict[str, Any]]) -> None:
    combined_dir = root_out / "combined"
    write_csv(
        combined_dir / "source_coverage.csv",
        [
            "sport", "source", "file", "row_count", "eligible_count", "date_min", "date_max",
            "market_types", "missing_event_key_count", "missing_matchup_key_count",
            "missing_player_key_count", "notes",
        ],
        [row for summary in summaries for row in summary["source_coverage_rows"]],
    )
    write_csv(
        combined_dir / "signals_latest.csv",
        [
            "sport", "date", "signal_id", "signal_type", "event_key", "matchup_key", "market_type",
            "selection", "line", "odds", "sources", "source_count", "tier", "pattern",
            "score", "expert_strength", "source_strength", "count_total", "best_odds",
            "edge_pct", "probability_pct",
        ],
        [row for summary in summaries for row in summary["signal_rows"]],
    )
    write_csv(
        combined_dir / "cross_source_signals.csv",
        [
            "sport", "date", "event_key", "matchup_key", "market_type", "selection",
            "line", "sources", "source_count", "signal_id",
        ],
        [row for summary in summaries for row in summary["cross_rows"]],
    )
    write_csv(
        combined_dir / "player_props.csv",
        [
            "sport", "date", "player_name", "player_key", "team", "opponent", "stat_type",
            "selection", "line", "odds", "source", "eligible_for_consensus", "event_key",
            "matchup_key",
        ],
        [row for summary in summaries for row in summary["player_rows"]],
    )
    write_csv(
        combined_dir / "data_quality_issues.csv",
        [
            "sport", "severity", "issue_type", "file", "source", "date", "event_key",
            "matchup_key", "selection", "player_key", "details", "count",
        ],
        [row for summary in summaries for row in summary["issue_rows"]],
    )


def print_summary(summary: Dict[str, Any]) -> None:
    print(f"\n[{summary['sport']}] status={summary['status']}")
    print(f"  normalized_rows={summary['normalized_rows']} eligible_rows={summary['eligible_rows']}")
    print(f"  date_range={summary['date_min'] or 'n/a'}..{summary['date_max'] or 'n/a'}")
    print(f"  source_coverage={top_n(Counter(summary['source_counts']), 20) or 'n/a'}")
    print(f"  total_signals={summary['signals']} cross_source_signals={summary['cross_source']} player_props={summary['player_props']}")
    print(f"  top_markets={summary['top_markets'] or 'n/a'}")
    print(f"  top_players_by_prop_count={summary['top_players'] or 'n/a'}")
    issue_counts = Counter(summary["issue_counts"])
    print(f"  data_quality_issue_counts={top_n(issue_counts, 20) or 'n/a'}")
    if summary["must_fix_next"]:
        print(f"  must_fix_next={'; '.join(summary['must_fix_next'])}")


def main() -> None:
    args = parse_args()
    start, end = date_window(args)
    sports = list(SPORTS) if args.sport == "ALL" else [args.sport]
    root_out = REPO_ROOT / args.out_dir
    summaries = [export_sport(sport, args, root_out, start, end) for sport in sports]
    write_combined(root_out, summaries)
    for summary in summaries:
        print_summary(summary)


if __name__ == "__main__":
    main()
