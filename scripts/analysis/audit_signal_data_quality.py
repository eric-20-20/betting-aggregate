#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import re
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
REPORT_DIR = DATA_DIR / "reports"

PLAYER_SELECTION_RE = re.compile(
    r"^(NBA|NCAAB):[a-z_]+::[a-z_]+::(OVER|UNDER|player_over|player_under)$"
)
PLAYER_KEY_RE = re.compile(r"^(NBA|NCAAB):[a-z_]+$")
CANONICAL_EVENT_KEY_RE = re.compile(
    r"^(NBA|NCAAB):(\d{4}):(\d{2}):(\d{2}):([A-Z]{2,5})@([A-Z]{2,5})$"
)
LEGACY_EVENT_KEY_RE = re.compile(
    r"^(NBA|NCAAB):(\d{4})(\d{2})(\d{2}):([A-Z]{2,5})@([A-Z]{2,5})(?::\d{4})?$"
)


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    rows.append(obj)
    return rows


def classify_jsonl(path: Path, rows: List[Dict[str, Any]]) -> Optional[str]:
    if not rows:
        return None
    sample = rows[:10]
    keys = set().union(*(row.keys() for row in sample))
    if "signal_id" not in keys:
        return None
    if "result" in keys or "status" in keys or "stat_value" in keys:
        return "grade_like"
    if "source_id" in keys or "occ_source_id" in keys or "supports" in keys:
        return "signal_like"
    return None


def month_from_day_key(day_key: Optional[str]) -> Optional[str]:
    if not day_key:
        return None
    parts = day_key.split(":")
    if len(parts) != 4:
        return None
    sport, year, month, day = parts
    try:
        datetime(int(year), int(month), int(day))
    except ValueError:
        return None
    return f"{sport}:{year}:{month}"


def parse_event_key_date(event_key: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not event_key:
        return None, "missing"
    m = CANONICAL_EVENT_KEY_RE.match(event_key)
    if m:
        sport, year, month, day, _away, _home = m.groups()
        try:
            datetime(int(year), int(month), int(day))
        except ValueError:
            return None, "invalid_calendar_date"
        return f"{sport}:{year}:{month}:{day}", None
    m = LEGACY_EVENT_KEY_RE.match(event_key)
    if m:
        sport, year, month, day, _away, _home = m.groups()
        try:
            datetime(int(year), int(month), int(day))
        except ValueError:
            return None, "invalid_calendar_date"
        return f"{sport}:{year}:{month}:{day}", None
    return None, "format_invalid"


def stat_from_selection(selection: Optional[str]) -> Optional[str]:
    if not selection or "::" not in selection:
        return None
    parts = selection.split("::")
    if len(parts) < 3:
        return None
    return parts[1]


def direction_from_selection(selection: Optional[str]) -> Optional[str]:
    if not selection or "::" not in selection:
        return None
    parts = selection.split("::")
    if len(parts) < 3:
        return None
    return parts[2].upper()


def is_nullish(value: Any) -> bool:
    return value in (None, "", [], {})


def summarize_top(rows: List[Dict[str, Any]], limit: int = 10) -> List[Dict[str, Any]]:
    return rows[:limit]


def merge_examples(existing: List[Dict[str, Any]], new_rows: List[Dict[str, Any]], limit: int = 10) -> List[Dict[str, Any]]:
    merged = list(existing)
    for row in new_rows:
        if len(merged) >= limit:
            break
        merged.append(row)
    return merged


def find_invalid_player_prop_selection(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    bad: List[Dict[str, Any]] = []
    total = 0
    for row in rows:
        if row.get("market_type") != "player_prop":
            continue
        total += 1
        selection = row.get("selection")
        if not isinstance(selection, str) or not PLAYER_SELECTION_RE.match(selection):
            bad.append(
                {
                    "signal_id": row.get("signal_id"),
                    "selection": selection,
                    "player_key": row.get("player_key") or row.get("player_id"),
                    "source_id": row.get("source_id") or row.get("occ_source_id"),
                    "day_key": row.get("day_key"),
                }
            )
    return {
        "player_prop_rows": total,
        "bad_count": len(bad),
        "examples": summarize_top(bad),
    }


def find_win_loss_missing_stat_value(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    bad: List[Dict[str, Any]] = []
    total = 0
    for row in rows:
        result = row.get("result")
        if result not in {"WIN", "LOSS"}:
            continue
        total += 1
        stat_value = row.get("stat_value")
        actual_stat_value = row.get("actual_stat_value")
        if is_nullish(stat_value) and is_nullish(actual_stat_value):
            bad.append(
                {
                    "signal_id": row.get("signal_id"),
                    "result": result,
                    "market_type": row.get("market_type"),
                    "selection": row.get("selection"),
                    "day_key": row.get("day_key"),
                }
            )
    return {
        "win_loss_rows": total,
        "bad_count": len(bad),
        "examples": summarize_top(bad),
    }


def find_source_dup_clusters(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    counts: Counter[Tuple[str, str, str, str, str]] = Counter()
    for row in rows:
        if row.get("market_type") != "player_prop":
            continue
        source_id = row.get("occ_source_id") or row.get("source_id")
        day_key = row.get("day_key")
        player_key = row.get("player_key") or row.get("player_id")
        atomic_stat = row.get("atomic_stat") or stat_from_selection(row.get("selection"))
        direction = (row.get("direction") or direction_from_selection(row.get("selection")) or "").upper()
        if not all([source_id, day_key, player_key, atomic_stat, direction]):
            continue
        if not PLAYER_KEY_RE.match(str(player_key)):
            continue
        counts[(str(source_id), str(day_key), str(player_key), str(atomic_stat), str(direction))] += 1

    dupes = [
        {
            "source_id": source_id,
            "day_key": day_key,
            "player_key": player_key,
            "atomic_stat": atomic_stat,
            "direction": direction,
            "count": count,
        }
        for (source_id, day_key, player_key, atomic_stat, direction), count in counts.items()
        if count > 2
    ]
    dupes.sort(key=lambda r: (-r["count"], r["source_id"], r["day_key"], r["player_key"]))
    return {
        "clusters_gt_2": len(dupes),
        "examples": summarize_top(dupes),
    }


def find_bad_event_keys(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    bad: List[Dict[str, Any]] = []
    total = 0
    for row in rows:
        event_key = row.get("event_key")
        if not event_key:
            continue
        total += 1
        derived_day_key, err = parse_event_key_date(event_key)
        if err:
            bad.append(
                {
                    "signal_id": row.get("signal_id"),
                    "event_key": event_key,
                    "day_key": row.get("day_key"),
                    "issue": err,
                }
            )
            continue
        day_key = row.get("day_key")
        if day_key and derived_day_key and day_key != derived_day_key:
            bad.append(
                {
                    "signal_id": row.get("signal_id"),
                    "event_key": event_key,
                    "day_key": day_key,
                    "issue": "day_key_mismatch",
                }
            )
    return {
        "event_keys_checked": total,
        "bad_count": len(bad),
        "examples": summarize_top(bad),
    }


def identify_volume_spikes(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    per_source_month: Counter[Tuple[str, str]] = Counter()
    for row in rows:
        source_id = row.get("occ_source_id") or row.get("source_id")
        month = month_from_day_key(row.get("day_key"))
        if source_id and month:
            per_source_month[(str(source_id), month)] += 1

    grouped: Dict[str, List[Tuple[str, int]]] = defaultdict(list)
    for (source_id, month), count in per_source_month.items():
        grouped[source_id].append((month, count))

    spikes: List[Dict[str, Any]] = []
    counts_table: List[Dict[str, Any]] = []
    for source_id, items in grouped.items():
        items.sort()
        counts = [count for _month, count in items]
        median = statistics.median(counts)
        mad = statistics.median([abs(c - median) for c in counts]) if counts else 0
        threshold = max(median * 2.0, median + 3 * (mad or 1))
        for month, count in items:
            counts_table.append({"source_id": source_id, "month": month, "count": count})
            if len(counts) >= 3 and count >= 50 and count > threshold:
                spikes.append(
                    {
                        "source_id": source_id,
                        "month": month,
                        "count": count,
                        "median": median,
                        "threshold": round(threshold, 2),
                    }
                )

    spikes.sort(key=lambda r: (-r["count"], r["source_id"], r["month"]))
    counts_table.sort(key=lambda r: (r["source_id"], r["month"]))
    return {
        "source_month_counts": counts_table,
        "spikes": summarize_top(spikes, limit=50),
    }


def iter_target_jsonl_files() -> Iterable[Path]:
    candidates = [
        DATA_DIR / "ledger" / "signals_latest.jsonl",
        DATA_DIR / "ledger" / "signals_occurrences.jsonl",
        DATA_DIR / "ledger" / "grades_latest.jsonl",
        DATA_DIR / "ledger" / "grades_occurrences.jsonl",
        DATA_DIR / "ledger" / "ncaab" / "signals_latest.jsonl",
        DATA_DIR / "ledger" / "ncaab" / "signals_occurrences.jsonl",
        DATA_DIR / "ledger" / "ncaab" / "grades_latest.jsonl",
        DATA_DIR / "ledger" / "ncaab" / "grades_occurrences.jsonl",
        DATA_DIR / "analysis" / "graded_signals_latest.jsonl",
        DATA_DIR / "analysis" / "graded_occurrences_latest.jsonl",
        DATA_DIR / "analysis" / "ncaab" / "graded_signals_latest.jsonl",
        DATA_DIR / "analysis" / "ncaab" / "graded_occurrences_latest.jsonl",
    ]
    for path in candidates:
        if path.exists():
            yield path


def scan_local_data() -> Dict[str, Any]:
    report: Dict[str, Any] = {"files": [], "totals": {}}
    signal_totals = {
        "rows": 0,
        "invalid_player_prop_selection": {"player_prop_rows": 0, "bad_count": 0, "examples": []},
        "source_dup_clusters": {"clusters_gt_2": 0, "examples": []},
        "bad_event_keys": {"event_keys_checked": 0, "bad_count": 0, "examples": []},
        "volume": {"source_month_counts": [], "spikes": []},
    }
    grade_totals = {
        "rows": 0,
        "win_loss_missing_stat_value": {"win_loss_rows": 0, "bad_count": 0, "examples": []},
        "bad_event_keys": {"event_keys_checked": 0, "bad_count": 0, "examples": []},
    }
    combined_signal_rows: List[Dict[str, Any]] = []
    combined_grade_rows: List[Dict[str, Any]] = []

    for path in iter_target_jsonl_files():
        rows = load_jsonl(path)
        kind = classify_jsonl(path, rows)
        if not kind:
            continue
        rel = str(path.relative_to(REPO_ROOT))
        file_report: Dict[str, Any] = {"path": rel, "rows": len(rows), "kind": kind}
        if kind == "signal_like":
            inv = find_invalid_player_prop_selection(rows)
            dup = find_source_dup_clusters(rows)
            bad = find_bad_event_keys(rows)
            vol = identify_volume_spikes(rows)
            file_report["invalid_player_prop_selection"] = inv
            file_report["source_dup_clusters"] = dup
            file_report["bad_event_keys"] = bad
            file_report["volume"] = vol
            signal_totals["rows"] += len(rows)
            signal_totals["invalid_player_prop_selection"]["player_prop_rows"] += inv["player_prop_rows"]
            signal_totals["invalid_player_prop_selection"]["bad_count"] += inv["bad_count"]
            signal_totals["invalid_player_prop_selection"]["examples"] = merge_examples(
                signal_totals["invalid_player_prop_selection"]["examples"], inv["examples"]
            )
            signal_totals["source_dup_clusters"]["clusters_gt_2"] += dup["clusters_gt_2"]
            signal_totals["source_dup_clusters"]["examples"] = merge_examples(
                signal_totals["source_dup_clusters"]["examples"], dup["examples"]
            )
            signal_totals["bad_event_keys"]["event_keys_checked"] += bad["event_keys_checked"]
            signal_totals["bad_event_keys"]["bad_count"] += bad["bad_count"]
            signal_totals["bad_event_keys"]["examples"] = merge_examples(
                signal_totals["bad_event_keys"]["examples"], bad["examples"]
            )
            combined_signal_rows.extend(rows)
        else:
            missing = find_win_loss_missing_stat_value(rows)
            bad = find_bad_event_keys(rows)
            file_report["win_loss_missing_stat_value"] = missing
            file_report["bad_event_keys"] = bad
            grade_totals["rows"] += len(rows)
            grade_totals["win_loss_missing_stat_value"]["win_loss_rows"] += missing["win_loss_rows"]
            grade_totals["win_loss_missing_stat_value"]["bad_count"] += missing["bad_count"]
            grade_totals["win_loss_missing_stat_value"]["examples"] = merge_examples(
                grade_totals["win_loss_missing_stat_value"]["examples"], missing["examples"]
            )
            grade_totals["bad_event_keys"]["event_keys_checked"] += bad["event_keys_checked"]
            grade_totals["bad_event_keys"]["bad_count"] += bad["bad_count"]
            grade_totals["bad_event_keys"]["examples"] = merge_examples(
                grade_totals["bad_event_keys"]["examples"], bad["examples"]
            )
            combined_grade_rows.extend(rows)
        report["files"].append(file_report)

    signal_totals["volume"] = identify_volume_spikes(combined_signal_rows)
    report["totals"]["signals_combined"] = signal_totals
    report["totals"]["grades_combined"] = grade_totals
    return report


def load_env() -> None:
    env_path = REPO_ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())


def run_supabase_sql(sql: str) -> List[Dict[str, Any]]:
    load_env()
    url = os.environ.get("SUPABASE_URL", "").rstrip("/")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL / SUPABASE_KEY not configured")

    import urllib.error
    import urllib.request

    payload = json.dumps({"query": sql.strip().rstrip(";")}).encode()
    req = urllib.request.Request(
        f"{url}/rest/v1/rpc/execute_sql",
        data=payload,
        headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode()
        raise RuntimeError(f"Supabase SQL error {exc.code}: {body}") from exc


def build_supabase_report() -> Dict[str, Any]:
    queries = {
        "invalid_player_prop_selection": """
            SELECT 'signals' AS table_name, COUNT(*) AS bad_count
            FROM signals
            WHERE market_type = 'player_prop'
              AND (selection IS NULL OR selection !~ '^(NBA|NCAAB):[a-z_]+::[a-z_]+::(OVER|UNDER|player_over|player_under)$')
            UNION ALL
            SELECT 'graded_occurrences' AS table_name, COUNT(*) AS bad_count
            FROM graded_occurrences
            WHERE market_type = 'player_prop'
              AND (selection IS NULL OR selection !~ '^(NBA|NCAAB):[a-z_]+::[a-z_]+::(OVER|UNDER|player_over|player_under)$')
            ORDER BY table_name
        """,
        "invalid_player_prop_selection_examples": """
            SELECT 'signals' AS table_name, signal_id, source_id, day_key, selection, player_key
            FROM (
                SELECT s.signal_id, NULL::text AS source_id, s.day_key, s.selection, s.player_key
                FROM signals s
                WHERE s.market_type = 'player_prop'
                  AND (s.selection IS NULL OR s.selection !~ '^(NBA|NCAAB):[a-z_]+::[a-z_]+::(OVER|UNDER|player_over|player_under)$')
                LIMIT 10
            ) q
            UNION ALL
            SELECT 'graded_occurrences' AS table_name, signal_id, occ_source_id AS source_id, day_key, selection, player_key
            FROM (
                SELECT go.signal_id, go.occ_source_id, go.day_key, go.selection, go.player_key
                FROM graded_occurrences go
                WHERE go.market_type = 'player_prop'
                  AND (go.selection IS NULL OR go.selection !~ '^(NBA|NCAAB):[a-z_]+::[a-z_]+::(OVER|UNDER|player_over|player_under)$')
                LIMIT 10
            ) q
        """,
        "win_loss_missing_stat_value": """
            SELECT 'grades' AS table_name, COUNT(*) AS bad_count
            FROM grades
            WHERE result IN ('WIN','LOSS')
              AND stat_value IS NULL
            UNION ALL
            SELECT 'graded_occurrences' AS table_name, COUNT(*) AS bad_count
            FROM graded_occurrences
            WHERE result IN ('WIN','LOSS')
              AND stat_value IS NULL
            ORDER BY table_name
        """,
        "win_loss_missing_stat_value_examples": """
            SELECT 'grades' AS table_name, signal_id, NULL::text AS source_id, day_key, result, market_type, selection
            FROM grades
            WHERE result IN ('WIN','LOSS')
              AND stat_value IS NULL
            LIMIT 10
        """,
        "source_dup_clusters": """
            SELECT source_id, day_key, player_key, atomic_stat, direction, cnt
            FROM (
                SELECT
                    occ_source_id AS source_id,
                    day_key,
                    player_key,
                    COALESCE(atomic_stat, split_part(split_part(selection, '::', 2), '::', 1)) AS atomic_stat,
                    UPPER(COALESCE(direction, split_part(selection, '::', 3))) AS direction,
                    COUNT(*) AS cnt
                FROM graded_occurrences
                WHERE market_type = 'player_prop'
                  AND player_key ~ '^(NBA|NCAAB):[a-z_]+$'
                GROUP BY 1,2,3,4,5
            ) q
            WHERE cnt > 2
            ORDER BY cnt DESC, source_id, day_key
            LIMIT 50
        """,
        "bad_event_keys": """
            SELECT table_name, issue, COUNT(*) AS bad_count
            FROM (
                SELECT 'signals' AS table_name,
                       CASE
                         WHEN event_key !~ '^(NBA|NCAAB):(\\d{4}:\\d{2}:\\d{2}|\\d{8}):[A-Z]{2,5}@[A-Z]{2,5}(:\\d{4})?$' THEN 'format_invalid'
                         WHEN day_key IS NOT NULL
                              AND regexp_replace(split_part(event_key, ':', 2), '^(\\d{4})(\\d{2})(\\d{2})$', '\\1:\\2:\\3') IS NOT NULL
                              AND (
                                  CASE
                                    WHEN split_part(event_key, ':', 2) ~ '^\\d{8}$'
                                      THEN split_part(event_key, ':', 1) || ':' || substring(split_part(event_key, ':', 2),1,4) || ':' || substring(split_part(event_key, ':', 2),5,2) || ':' || substring(split_part(event_key, ':', 2),7,2)
                                    ELSE split_part(event_key, ':', 1) || ':' || split_part(event_key, ':', 2) || ':' || split_part(event_key, ':', 3) || ':' || split_part(event_key, ':', 4)
                                  END
                              ) <> day_key THEN 'day_key_mismatch'
                         ELSE NULL
                       END AS issue
                FROM signals
                WHERE event_key IS NOT NULL
                UNION ALL
                SELECT 'grades' AS table_name,
                       CASE
                         WHEN event_key !~ '^(NBA|NCAAB):(\\d{4}:\\d{2}:\\d{2}|\\d{8}):[A-Z]{2,5}@[A-Z]{2,5}(:\\d{4})?$' THEN 'format_invalid'
                         WHEN day_key IS NOT NULL
                              AND (
                                  CASE
                                    WHEN split_part(event_key, ':', 2) ~ '^\\d{8}$'
                                      THEN split_part(event_key, ':', 1) || ':' || substring(split_part(event_key, ':', 2),1,4) || ':' || substring(split_part(event_key, ':', 2),5,2) || ':' || substring(split_part(event_key, ':', 2),7,2)
                                    WHEN split_part(event_key, ':', 2) ~ '^\\d{4}$'
                                      THEN split_part(event_key, ':', 1) || ':' || split_part(event_key, ':', 2) || ':' || split_part(event_key, ':', 3) || ':' || split_part(event_key, ':', 4)
                                    ELSE NULL
                                  END
                              ) <> day_key THEN 'day_key_mismatch'
                         ELSE NULL
                       END AS issue
                FROM grades
                WHERE event_key IS NOT NULL
                UNION ALL
                SELECT 'graded_occurrences' AS table_name,
                       CASE
                         WHEN event_key !~ '^(NBA|NCAAB):(\\d{4}:\\d{2}:\\d{2}|\\d{8}):[A-Z]{2,5}@[A-Z]{2,5}(:\\d{4})?$' THEN 'format_invalid'
                         WHEN day_key IS NOT NULL
                              AND (
                                  CASE
                                    WHEN split_part(event_key, ':', 2) ~ '^\\d{8}$'
                                      THEN split_part(event_key, ':', 1) || ':' || substring(split_part(event_key, ':', 2),1,4) || ':' || substring(split_part(event_key, ':', 2),5,2) || ':' || substring(split_part(event_key, ':', 2),7,2)
                                    WHEN split_part(event_key, ':', 2) ~ '^\\d{4}$'
                                      THEN split_part(event_key, ':', 1) || ':' || split_part(event_key, ':', 2) || ':' || split_part(event_key, ':', 3) || ':' || split_part(event_key, ':', 4)
                                    ELSE NULL
                                  END
                              ) <> day_key THEN 'day_key_mismatch'
                         ELSE NULL
                       END AS issue
                FROM graded_occurrences
                WHERE event_key IS NOT NULL
            ) q
            WHERE issue IS NOT NULL
            GROUP BY 1,2
            ORDER BY table_name, issue
        """,
        "source_month_counts": """
            SELECT ss.source_id,
                   substring(s.day_key from '^[^:]+:(\\d{4}:\\d{2})') AS month_key,
                   COUNT(*) AS signal_count
            FROM signal_sources ss
            JOIN signals s ON s.signal_id = ss.signal_id
            WHERE s.day_key IS NOT NULL
            GROUP BY 1,2
            ORDER BY 1,2
        """,
    }
    out: Dict[str, Any] = {}
    for name, sql in queries.items():
        out[name] = run_supabase_sql(sql)
    return out


def render_markdown(local_report: Dict[str, Any], supabase_report: Optional[Dict[str, Any]], supabase_error: Optional[str]) -> str:
    lines: List[str] = []
    generated_at = datetime.now().isoformat(timespec="seconds")
    lines.append(f"# Data Quality Audit")
    lines.append("")
    lines.append(f"Generated: `{generated_at}`")
    lines.append("")

    sig_total = local_report["totals"]["signals_combined"]
    grd_total = local_report["totals"]["grades_combined"]
    lines.append("## Local Data")
    lines.append("")
    lines.append(f"- Signal-like rows scanned: `{sig_total['rows']}`")
    lines.append(f"- Grade-like rows scanned: `{grd_total['rows']}`")
    lines.append(f"- Bad player-prop selections: `{sig_total['invalid_player_prop_selection']['bad_count']}`")
    lines.append(f"- WIN/LOSS grades missing stat_value: `{grd_total['win_loss_missing_stat_value']['bad_count']}`")
    lines.append(f"- Source duplicate clusters >2: `{sig_total['source_dup_clusters']['clusters_gt_2']}`")
    lines.append(f"- Bad event_key rows: `{sig_total['bad_event_keys']['bad_count'] + grd_total['bad_event_keys']['bad_count']}`")
    lines.append(f"- Volume spikes flagged: `{len(sig_total['volume']['spikes'])}`")
    lines.append("")

    lines.append("### Local Examples")
    lines.append("")
    lines.append("#### Bad player-prop selections")
    for row in sig_total["invalid_player_prop_selection"]["examples"][:10]:
        lines.append(f"- `{row.get('signal_id')}` | `{row.get('selection')}` | `{row.get('source_id')}` | `{row.get('day_key')}`")
    if not sig_total["invalid_player_prop_selection"]["examples"]:
        lines.append("- None")
    lines.append("")

    lines.append("#### WIN/LOSS grades missing stat_value")
    for row in grd_total["win_loss_missing_stat_value"]["examples"][:10]:
        lines.append(f"- `{row.get('signal_id')}` | `{row.get('result')}` | `{row.get('market_type')}` | `{row.get('selection')}`")
    if not grd_total["win_loss_missing_stat_value"]["examples"]:
        lines.append("- None")
    lines.append("")

    lines.append("#### Duplicate clusters >2")
    for row in sig_total["source_dup_clusters"]["examples"][:10]:
        lines.append(
            f"- `{row['source_id']}` | `{row['day_key']}` | `{row['player_key']}` | `{row['atomic_stat']}` | `{row['direction']}` | count=`{row['count']}`"
        )
    if not sig_total["source_dup_clusters"]["examples"]:
        lines.append("- None")
    lines.append("")

    lines.append("#### Bad event_key rows")
    combined_event_examples = sig_total["bad_event_keys"]["examples"][:5] + grd_total["bad_event_keys"]["examples"][:5]
    for row in combined_event_examples:
        lines.append(f"- `{row.get('signal_id')}` | `{row.get('event_key')}` | `{row.get('day_key')}` | `{row.get('issue')}`")
    if not combined_event_examples:
        lines.append("- None")
    lines.append("")

    lines.append("#### Volume spikes")
    for row in sig_total["volume"]["spikes"][:20]:
        lines.append(
            f"- `{row['source_id']}` | `{row['month']}` | count=`{row['count']}` | median=`{row['median']}` | threshold=`{row['threshold']}`"
        )
    if not sig_total["volume"]["spikes"]:
        lines.append("- None")
    lines.append("")

    lines.append("## Supabase")
    lines.append("")
    if supabase_error:
        lines.append(f"- Query failed: `{supabase_error}`")
        return "\n".join(lines)

    assert supabase_report is not None
    for row in supabase_report.get("invalid_player_prop_selection", []):
        lines.append(f"- Bad player-prop selections in `{row['table_name']}`: `{row['bad_count']}`")
    for row in supabase_report.get("win_loss_missing_stat_value", []):
        lines.append(f"- WIN/LOSS rows missing stat_value in `{row['table_name']}`: `{row['bad_count']}`")
    lines.append(f"- Duplicate clusters >2 in `graded_occurrences`: `{len(supabase_report.get('source_dup_clusters', []))}` shown")
    lines.append(f"- Bad event_key issue buckets: `{len(supabase_report.get('bad_event_keys', []))}`")
    lines.append(f"- Source-month rows: `{len(supabase_report.get('source_month_counts', []))}`")
    lines.append("")

    lines.append("### Supabase Examples")
    lines.append("")
    lines.append("#### Bad player-prop selections")
    for row in supabase_report.get("invalid_player_prop_selection_examples", [])[:10]:
        lines.append(f"- `{row.get('table_name')}` | `{row.get('signal_id')}` | `{row.get('source_id')}` | `{row.get('selection')}`")
    if not supabase_report.get("invalid_player_prop_selection_examples"):
        lines.append("- None")
    lines.append("")

    lines.append("#### WIN/LOSS grades missing stat_value")
    for row in supabase_report.get("win_loss_missing_stat_value_examples", [])[:10]:
        lines.append(f"- `{row.get('table_name')}` | `{row.get('signal_id')}` | `{row.get('result')}` | `{row.get('selection')}`")
    if not supabase_report.get("win_loss_missing_stat_value_examples"):
        lines.append("- None")
    lines.append("")

    lines.append("#### Duplicate clusters >2")
    for row in supabase_report.get("source_dup_clusters", [])[:20]:
        lines.append(
            f"- `{row['source_id']}` | `{row['day_key']}` | `{row['player_key']}` | `{row['atomic_stat']}` | `{row['direction']}` | count=`{row['cnt']}`"
        )
    if not supabase_report.get("source_dup_clusters"):
        lines.append("- None")
    lines.append("")

    lines.append("#### Bad event_key issue buckets")
    for row in supabase_report.get("bad_event_keys", []):
        lines.append(f"- `{row['table_name']}` | `{row['issue']}` | `{row['bad_count']}`")
    if not supabase_report.get("bad_event_keys"):
        lines.append("- None")
    lines.append("")

    lines.append("#### Top source-month counts")
    month_counts = sorted(
        supabase_report.get("source_month_counts", []),
        key=lambda r: (-int(r["signal_count"]), r["source_id"], r["month_key"] or ""),
    )
    for row in month_counts[:20]:
        lines.append(f"- `{row['source_id']}` | `{row['month_key']}` | count=`{row['signal_count']}`")
    if not month_counts:
        lines.append("- None")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit local data and Supabase for signal quality issues")
    parser.add_argument(
        "--out",
        default=str(REPORT_DIR / "data_quality_audit.md"),
        help="Markdown report output path",
    )
    parser.add_argument(
        "--json-out",
        default=str(REPORT_DIR / "data_quality_audit.json"),
        help="JSON report output path",
    )
    parser.add_argument(
        "--skip-supabase",
        action="store_true",
        help="Only scan local data/",
    )
    args = parser.parse_args()

    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    local_report = scan_local_data()
    supabase_report: Optional[Dict[str, Any]] = None
    supabase_error: Optional[str] = None
    if not args.skip_supabase:
        try:
            supabase_report = build_supabase_report()
        except Exception as exc:  # noqa: BLE001
            supabase_error = str(exc)

    payload = {
        "generated_at": datetime.now().isoformat(),
        "local": local_report,
        "supabase": supabase_report,
        "supabase_error": supabase_error,
    }
    Path(args.json_out).write_text(json.dumps(payload, indent=2))
    Path(args.out).write_text(render_markdown(local_report, supabase_report, supabase_error))

    print(f"Wrote {args.out}")
    print(f"Wrote {args.json_out}")
    if supabase_error:
        print(f"Supabase query failed: {supabase_error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
