from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]
SIGNALS_PATH = REPO_ROOT / "data/ledger/signals_latest.jsonl"
GRADES_PATH = REPO_ROOT / "data/ledger/grades_latest.jsonl"
DEFAULT_OUTPUT_PATH = REPO_ROOT / "data/ledger/expert_pick_outcomes_latest.jsonl"
RESULTS_CACHE_DIR = REPO_ROOT / "data/cache/results"
NBA_SPORT = "NBA"
NCAAB_SPORT = "NCAAB"


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
            f.write("\n")


def to_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def pick_signal_line(signal: Dict[str, Any], grade: Optional[Dict[str, Any]]) -> Optional[float]:
    if isinstance(grade, dict) and grade.get("line") is not None:
        coerced = to_float(grade["line"])
        if coerced is not None:
            return coerced
    for key in ("line_median", "line", "line_max", "line_min"):
        coerced = to_float(signal.get(key))
        if coerced is not None:
            return coerced
    return None


def pick_support_line(support: Dict[str, Any]) -> Optional[float]:
    for key in ("line", "line_hint"):
        coerced = to_float(support.get(key))
        if coerced is not None:
            return coerced
    return None


def infer_expert_name(support: Dict[str, Any]) -> Optional[str]:
    for key in ("expert_name", "source", "expert"):
        val = support.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    canonical_url = support.get("canonical_url") or ""
    if isinstance(canonical_url, str) and "/users/" in canonical_url:
        return canonical_url.split("/users/")[-1].split("/")[0].split("?")[0].strip() or None
    return None


def infer_grade_mode(signal: Dict[str, Any], grade: Dict[str, Any]) -> str:
    notes = str(grade.get("notes") or "")
    if signal.get("pregraded_result") or grade.get("graded_by") or notes.startswith("pregraded_by_"):
        return "pregraded"
    return "live"


def normalize_direction(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    upper = value.upper().strip()
    if "OVER" in upper:
        return "OVER"
    if "UNDER" in upper:
        return "UNDER"
    return upper


def normalize_team_code(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip().upper()


def normalize_rating_stars(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        rating = int(float(value))
    except (TypeError, ValueError):
        return None
    if rating < 0:
        return None
    return rating


def infer_rating_stars_from_support(support: Dict[str, Any]) -> Optional[int]:
    direct = normalize_rating_stars(support.get("rating_stars"))
    if direct is not None:
        return direct
    for key in ("raw_block", "raw_pick_text"):
        text = support.get(key)
        if not isinstance(text, str) or not text:
            continue
        match = re.search(r"rating:\s*([1-5])\s*out\s+of\s+5", text, re.IGNORECASE)
        if match:
            return int(match.group(1))
        match = re.search(r"\b([1-5])\s*[- ]?star\b", text, re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


def rating_bucket_label(value: Any) -> Optional[str]:
    rating = normalize_rating_stars(value)
    if rating is None:
        return None
    return f"{rating}_star"


def infer_sport(signal: Dict[str, Any], grade: Optional[Dict[str, Any]] = None) -> str:
    for candidate in (signal.get("day_key"), signal.get("event_key"), signal.get("canonical_event_key")):
        if isinstance(candidate, str) and candidate.startswith("NCAAB:"):
            return NCAAB_SPORT
    if grade:
        for candidate in (grade.get("day_key"), grade.get("event_key"), grade.get("canonical_event_key")):
            if isinstance(candidate, str) and candidate.startswith("NCAAB:"):
                return NCAAB_SPORT
    return NBA_SPORT


def extract_provider_game_id(grade: Dict[str, Any], sport: str) -> Optional[str]:
    value = grade.get("provider_game_id")
    if value:
        return str(value)
    games_info = grade.get("games_info")
    if isinstance(games_info, dict):
        if sport == NBA_SPORT and games_info.get("matched_game_id"):
            return str(games_info.get("matched_game_id"))
        if sport == NCAAB_SPORT and games_info.get("espn_event_id"):
            return str(games_info.get("espn_event_id"))
    return None


def load_game_result(game_id: Optional[str], sport: str, cache: Dict[Tuple[str, str], Optional[Dict[str, Any]]]) -> Optional[Dict[str, Any]]:
    if not game_id:
        return None
    cache_key = (sport, game_id)
    if cache_key in cache:
        return cache[cache_key]

    if sport == NCAAB_SPORT:
        path = RESULTS_CACHE_DIR / "ncaab" / f"espn_boxscore_{game_id}.json"
    else:
        path = RESULTS_CACHE_DIR / f"nba_cdn_boxscore_{game_id}.json"
    if not path.exists():
        cache[cache_key] = None
        return None

    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError:
        cache[cache_key] = None
        return None

    if sport == NCAAB_SPORT:
        header = data.get("header") or {}
        competitions = header.get("competitions") or []
        competition = competitions[0] if competitions else {}
        competitors = competition.get("competitors") or []
        home = next((team for team in competitors if team.get("homeAway") == "home"), {})
        away = next((team for team in competitors if team.get("homeAway") == "away"), {})
        result = {
            "game_status": competition.get("status", {}).get("type", {}).get("id"),
            "game_status_text": competition.get("status", {}).get("type", {}).get("detail"),
            "home_team": (((home.get("team") or {}).get("abbreviation")) or home.get("team", {}).get("shortDisplayName")),
            "away_team": (((away.get("team") or {}).get("abbreviation")) or away.get("team", {}).get("shortDisplayName")),
            "home_score": to_float(home.get("score")),
            "away_score": to_float(away.get("score")),
        }
    else:
        game = data.get("game") or {}
        home = game.get("homeTeam") or {}
        away = game.get("awayTeam") or {}
        result = {
            "game_status": game.get("gameStatus"),
            "game_status_text": game.get("gameStatusText"),
            "home_team": home.get("teamTricode"),
            "away_team": away.get("teamTricode"),
            "home_score": to_float(home.get("score")),
            "away_score": to_float(away.get("score")),
        }
    cache[cache_key] = result
    return result


def compute_binary_result(metric: Optional[float], line: Optional[float], direction: str) -> Optional[str]:
    if metric is None or line is None:
        return None
    direction = normalize_direction(direction)
    if direction == "OVER":
        if metric > line:
            return "WIN"
        if metric < line:
            return "LOSS"
        return "PUSH"
    if direction == "UNDER":
        if metric < line:
            return "WIN"
        if metric > line:
            return "LOSS"
        return "PUSH"
    return None


def compute_support_result(row: Dict[str, Any], game_cache: Dict[Tuple[str, str], Optional[Dict[str, Any]]]) -> Tuple[Optional[str], Dict[str, Any]]:
    market = row.get("market_type")
    support_line = to_float(row.get("support_line"))
    sport = str(row.get("sport") or NBA_SPORT)
    meta: Dict[str, Any] = {
        "support_result_source": None,
        "support_result_reason": None,
        "final_home_score": None,
        "final_away_score": None,
        "support_metric_value": None,
    }

    if market == "player_prop":
        stat_value = to_float(row.get("stat_value"))
        direction = row.get("support_direction") or row.get("direction")
        result = compute_binary_result(stat_value, support_line, str(direction or ""))
        meta["support_result_source"] = "stat_value"
        meta["support_result_reason"] = "computed_from_stat_value" if result is not None else "missing_stat_value_or_support_line"
        meta["support_metric_value"] = stat_value
        return result, meta

    if market not in {"spread", "total"}:
        meta["support_result_reason"] = "unsupported_market"
        return None, meta

    home_score = to_float(row.get("home_score"))
    away_score = to_float(row.get("away_score"))
    if home_score is None or away_score is None:
        game_result = load_game_result(row.get("provider_game_id"), sport, game_cache)
    else:
        game_result = None
    if game_result:
        home_score = to_float(game_result.get("home_score"))
        away_score = to_float(game_result.get("away_score"))
    if home_score is None or away_score is None:
        meta["support_result_reason"] = "missing_cached_game_result"
        return None, meta

    meta["final_home_score"] = home_score
    meta["final_away_score"] = away_score

    if market == "total":
        total_points = home_score + away_score
        direction = row.get("support_direction") or row.get("direction")
        result = compute_binary_result(total_points, support_line, str(direction or ""))
        meta["support_result_source"] = "final_score_total"
        meta["support_result_reason"] = "computed_from_total" if result is not None else "missing_total_direction_or_support_line"
        meta["support_metric_value"] = total_points
        return result, meta

    away_team = normalize_team_code(row.get("away_team"))
    home_team = normalize_team_code(row.get("home_team"))
    selection = normalize_team_code(row.get("support_selection") or row.get("selection"))
    if selection == away_team:
        margin = away_score - home_score
    elif selection == home_team:
        margin = home_score - away_score
    else:
        meta["support_result_reason"] = "spread_selection_not_team_code"
        return None, meta

    covered_margin = margin + support_line
    meta["support_metric_value"] = covered_margin
    meta["support_result_source"] = "final_score_spread"
    meta["support_result_reason"] = "computed_from_spread" if support_line is not None else "missing_spread_support_line"
    if support_line is None:
        return None, meta
    if covered_margin > 0:
        return "WIN", meta
    if covered_margin < 0:
        return "LOSS", meta
    return "PUSH", meta


def parse_player_subject(row: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], str]:
    selection = row.get("selection")
    if isinstance(selection, str) and "::" in selection:
        parts = selection.split("::")
        if len(parts) >= 3:
            return parts[0], parts[1], normalize_direction(parts[2])
    return row.get("player_id") or row.get("player_key"), row.get("atomic_stat"), normalize_direction(row.get("direction"))


def build_expert_pick_key(row: Dict[str, Any]) -> str:
    market = row.get("market_type") or "unknown"
    event_key = row.get("canonical_event_key") or row.get("event_key") or row.get("day_key") or "unknown"
    expert = row.get("expert_name") or "unknown"
    source_id = row.get("source_id") or "unknown"
    support_line = row.get("support_line")
    if market == "player_prop":
        player_id, stat_key, direction = parse_player_subject(row)
        payload = [expert, source_id, event_key, market, player_id or "unknown", stat_key or "unknown", direction, support_line]
    elif market in {"spread", "moneyline"}:
        payload = [expert, source_id, event_key, market, row.get("selection") or row.get("direction") or "unknown", normalize_direction(row.get("direction")), support_line]
    elif market == "total":
        payload = [expert, source_id, event_key, market, normalize_direction(row.get("direction")), support_line]
    else:
        payload = [expert, source_id, event_key, market, row.get("selection"), normalize_direction(row.get("direction")), support_line]
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def dedup_rank(row: Dict[str, Any]) -> Tuple[int, int, int, int]:
    has_source_grade = 1 if row.get("source_grades_available") else 0
    has_support_line = 1 if row.get("support_line") is not None else 0
    is_live = 1 if row.get("grade_mode") == "live" else 0
    metadata_score = sum(
        1
        for key in ("stat_value", "source_surface", "grade_source", "odds")
        if row.get(key) is not None and row.get(key) != ""
    )
    return (has_source_grade, has_support_line, is_live, metadata_score)


def apply_deduplication(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        row["expert_pick_key"] = build_expert_pick_key(row)
        grouped.setdefault(row["expert_pick_key"], []).append(row)

    deduped_rows: List[Dict[str, Any]] = []
    for key, bucket in grouped.items():
        ordered = sorted(
            bucket,
            key=lambda row: (
                dedup_rank(row),
                row.get("graded_at_utc") or "",
                row.get("signal_id") or "",
                -(row.get("support_idx") or 0),
            ),
            reverse=True,
        )
        keeper = ordered[0]
        for idx, row in enumerate(ordered):
            row["dedup_rank"] = idx + 1
            row["is_primary_record"] = idx == 0
            row["duplicate_count_for_key"] = len(ordered)
            deduped_rows.append(row)
        keeper["is_primary_record"] = True
    return deduped_rows


def build_outcome_row(
    signal: Dict[str, Any],
    grade: Dict[str, Any],
    support: Dict[str, Any],
    support_idx: int,
    game_cache: Dict[Tuple[str, str], Optional[Dict[str, Any]]],
) -> Optional[Dict[str, Any]]:
    expert_name = infer_expert_name(support)
    if not expert_name:
        return None

    sport = infer_sport(signal, grade)
    source_id = support.get("source_id") or signal.get("source_id") or "unknown"
    rating_stars = infer_rating_stars_from_support(support)
    signal_result = grade.get("result")
    provider_game_id = extract_provider_game_id(grade, sport)

    row = {
        "sport": sport,
        "signal_id": signal.get("signal_id"),
        "support_idx": support_idx,
        "expert_name": expert_name,
        "source_id": source_id,
        "canonical_event_key": signal.get("canonical_event_key") or signal.get("event_key"),
        "event_key": signal.get("event_key"),
        "day_key": signal.get("day_key"),
        "market_type": signal.get("market_type"),
        "selection": signal.get("selection"),
        "direction": signal.get("direction"),
        "selection_key": signal.get("selection_key"),
        "offer_key": signal.get("offer_key"),
        "player_id": signal.get("player_id"),
        "player_key": signal.get("player_key"),
        "atomic_stat": signal.get("atomic_stat"),
        "support_selection": support.get("selection"),
        "support_direction": support.get("direction"),
        "support_line": pick_support_line(support),
        "signal_line": pick_signal_line(signal, grade),
        "signal_result": signal_result,
        "grade_status": grade.get("status"),
        "grade_mode": infer_grade_mode(signal, grade),
        "grade_source": grade.get("graded_by") or grade.get("provider"),
        "graded_at_utc": grade.get("graded_at_utc"),
        "stat_value": grade.get("stat_value"),
        "provider_game_id": provider_game_id,
        "away_team": signal.get("away_team"),
        "home_team": signal.get("home_team"),
        "away_score": grade.get("away_score"),
        "home_score": grade.get("home_score"),
        "source_grades_available": bool(grade.get("source_grades")),
        "units": grade.get("units"),
        "stake": grade.get("stake"),
        "odds": grade.get("odds"),
        "rating_stars": rating_stars,
        "rating_bucket": rating_bucket_label(rating_stars),
        "grade_notes": grade.get("grade_notes") or grade.get("notes"),
        "sources_combo": signal.get("sources_combo"),
        "source_surface": support.get("source_surface"),
    }
    support_result, result_meta = compute_support_result(row, game_cache)
    row["support_result"] = support_result
    row.update(result_meta)
    return row


def build_rows(signals: List[Dict[str, Any]], grades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grades_by_signal = {row.get("signal_id"): row for row in grades if row.get("signal_id")}
    output_rows: List[Dict[str, Any]] = []
    game_cache: Dict[Tuple[str, str], Optional[Dict[str, Any]]] = {}

    for signal in signals:
        signal_id = signal.get("signal_id")
        if not signal_id:
            continue
        grade = grades_by_signal.get(signal_id)
        if not grade:
            continue
        supports = signal.get("supports") or []
        if not isinstance(supports, list):
            continue
        for idx, support in enumerate(supports):
            if not isinstance(support, dict):
                continue
            row = build_outcome_row(signal, grade, support, idx, game_cache)
            if row is not None:
                output_rows.append(row)
    return output_rows


def print_summary(rows: List[Dict[str, Any]]) -> None:
    grade_mode_counts = Counter(row.get("grade_mode") for row in rows)
    signal_result_counts = Counter(row.get("signal_result") for row in rows)
    support_result_counts = Counter(row.get("support_result") for row in rows if row.get("support_result"))
    source_counts = Counter(row.get("source_id") for row in rows)
    expert_counts = Counter((row.get("expert_name"), row.get("source_id")) for row in rows)
    primary_rows = [row for row in rows if row.get("is_primary_record")]
    duplicate_rows = len(rows) - len(primary_rows)
    mismatch_rows = [
        row for row in rows
        if row.get("support_result") in {"WIN", "LOSS", "PUSH"}
        and row.get("signal_result") in {"WIN", "LOSS", "PUSH"}
        and row.get("support_result") != row.get("signal_result")
    ]
    mismatch_by_expert = Counter((row.get("expert_name"), row.get("source_id")) for row in mismatch_rows)

    before_dups = Counter()
    after_dups = Counter()
    expert_examples: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    grouped_by_key: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        grouped_by_key.setdefault(row.get("expert_pick_key") or "", []).append(row)
    for bucket in grouped_by_key.values():
        if len(bucket) <= 1:
            continue
        first = bucket[0]
        expert_key = (str(first.get("expert_name") or "unknown"), str(first.get("source_id") or "unknown"))
        before_dups[expert_key] += len(bucket) - 1
        after_dups[expert_key] += 0
        if expert_key not in expert_examples:
            expert_examples[expert_key] = [
                {
                    "expert_pick_key": row.get("expert_pick_key"),
                    "signal_id": row.get("signal_id"),
                    "support_idx": row.get("support_idx"),
                    "support_line": row.get("support_line"),
                    "is_primary_record": row.get("is_primary_record"),
                }
                for row in bucket[:3]
            ]

    print(json.dumps(
        {
            "rows": len(rows),
            "primary_rows": len(primary_rows),
            "duplicate_audit_rows": duplicate_rows,
            "unique_signal_ids": len({row.get("signal_id") for row in rows}),
            "unique_expert_source_pairs": len(expert_counts),
            "grade_mode_counts": dict(grade_mode_counts),
            "signal_result_counts": dict(signal_result_counts),
            "support_result_counts": dict(support_result_counts),
            "support_signal_mismatch_count": len(mismatch_rows),
            "support_signal_mismatch_top": [
                {"expert_name": expert, "source_id": source, "mismatches": count}
                for (expert, source), count in mismatch_by_expert.most_common(15)
            ],
            "top_sources": source_counts.most_common(10),
            "top_expert_source_pairs": [
                {"expert_name": expert, "source_id": source, "rows": count}
                for (expert, source), count in expert_counts.most_common(10)
            ],
            "duplicate_rate_before_after_top": [
                {
                    "expert_name": expert,
                    "source_id": source,
                    "duplicate_rows_before": before_count,
                    "duplicate_rows_after": after_dups[(expert, source)],
                    "duplicate_rate_before": round(before_count / expert_counts[(expert, source)], 4) if expert_counts[(expert, source)] else None,
                    "duplicate_rate_after": 0.0,
                    "examples": expert_examples.get((expert, source), []),
                }
                for (expert, source), before_count in before_dups.most_common(15)
            ],
            "sample_rows": rows[:5],
            "mismatch_examples": mismatch_rows[:10],
        },
        indent=2,
        default=str,
    ))


def main() -> None:
    parser = argparse.ArgumentParser(description="Build expert pick outcomes from signal supports + grades")
    parser.add_argument("--signals", type=Path, default=SIGNALS_PATH, help="Signals JSONL input")
    parser.add_argument("--grades", type=Path, default=GRADES_PATH, help="Grades JSONL input")
    parser.add_argument("--write", action="store_true", help="Write output JSONL")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH, help="Output JSONL path")
    args = parser.parse_args()

    signals = read_jsonl(args.signals)
    grades = read_jsonl(args.grades)
    rows = build_rows(signals, grades)
    rows = apply_deduplication(rows)

    if args.write:
        write_jsonl(args.output, rows)

    print_summary(rows)


if __name__ == "__main__":
    main()
