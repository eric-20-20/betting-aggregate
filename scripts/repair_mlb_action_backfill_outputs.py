from __future__ import annotations

import json
import shutil
import sys
from collections import Counter
from dataclasses import asdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from action_ingest import MLB_SPORT, RawPickRecord, normalize_pick, parse_teams_from_slug
from scripts.action_backfill_nba import classify_backfill_row


BACKUP_DIR = REPO_ROOT / "out" / "mlb_action_backfill_pre_cleanup_backup_20260408"
ACTIVE_DIR = REPO_ROOT / "out"

RAW_BACKUP = BACKUP_DIR / "raw_action_mlb_backfill.jsonl"
RAW_ACTIVE = ACTIVE_DIR / "raw_action_mlb_backfill.jsonl"
NORMALIZED_ACTIVE = ACTIVE_DIR / "normalized_action_mlb_backfill.jsonl"
QUARANTINED_ACTIVE = ACTIVE_DIR / "quarantined_action_mlb_backfill_props.jsonl"
SUMMARY_ACTIVE = ACTIVE_DIR / "action_backfill_run_summary_mlb_repair.json"
STATE_ACTIVE = ACTIVE_DIR / "action_backfill_state_mlb.json"
STATE_BACKUP = BACKUP_DIR / "action_backfill_state_mlb.json"


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            json.dump(row, f, ensure_ascii=False)
            f.write("\n")


def main() -> int:
    if not RAW_BACKUP.exists():
        raise FileNotFoundError(f"Missing backup raw file: {RAW_BACKUP}")

    kept: list[dict] = []
    quarantined: list[dict] = []
    drop_reasons: Counter[str] = Counter()
    quarantine_reasons: Counter[str] = Counter()
    normalized_market_types: Counter[str] = Counter()
    quarantine_types: Counter[str] = Counter()
    date_keys: set[str] = set()
    team_total_rescues = 0
    stl_steals_leaks = 0

    with RAW_BACKUP.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            if not line.strip():
                continue
            payload = json.loads(line)
            raw = RawPickRecord(**payload)
            away_team, home_team = parse_teams_from_slug(raw.canonical_url, sport=MLB_SPORT)
            if not (away_team and home_team):
                drop_reasons["missing_teams_from_slug"] += 1
                continue

            normalized = normalize_pick(raw, home_team=home_team, away_team=away_team, sport=MLB_SPORT)
            disposition, repaired_payload, reason = classify_backfill_row(raw, normalized, MLB_SPORT)
            if disposition == "keep" and repaired_payload is not None:
                market_type = ((repaired_payload.get("market") or {}).get("market_type")) or "unknown"
                normalized_market_types[market_type] += 1
                kept.append(repaired_payload)
                if market_type == "team_total":
                    team_total_rescues += 1
                event = repaired_payload.get("event") or {}
                if event.get("day_key"):
                    date_keys.add(event["day_key"])
            elif disposition == "quarantine" and repaired_payload is not None:
                quarantined.append(repaired_payload)
                quarantine_reasons[reason] += 1
                quarantine_type = repaired_payload.get("quarantine_type") or "unknown"
                quarantine_types[quarantine_type] += 1
                event = repaired_payload.get("event") or {}
                if event.get("day_key"):
                    date_keys.add(event["day_key"])
                if raw.raw_pick_text.startswith("STL ") and (((repaired_payload.get("market") or {}).get("market_type")) == "steals"):
                    stl_steals_leaks += 1
            else:
                drop_reasons[reason] += 1

    shutil.copyfile(RAW_BACKUP, RAW_ACTIVE)
    write_jsonl(NORMALIZED_ACTIVE, kept)
    write_jsonl(QUARANTINED_ACTIVE, quarantined)
    if STATE_BACKUP.exists():
        shutil.copyfile(STATE_BACKUP, STATE_ACTIVE)

    summary = {
        "repair_source_raw": str(RAW_BACKUP),
        "raw_rows_processed": sum(1 for _ in RAW_BACKUP.open("r", encoding="utf-8")),
        "normalized_rows_written": len(kept),
        "quarantined_rows_written": len(quarantined),
        "normalized_market_types": dict(sorted(normalized_market_types.items())),
        "quarantine_types": dict(sorted(quarantine_types.items())),
        "quarantine_reasons": dict(sorted(quarantine_reasons.items())),
        "dropped_rows_by_reason": dict(sorted(drop_reasons.items())),
        "day_keys_covered": len(date_keys),
        "earliest_day_key": min(date_keys) if date_keys else None,
        "latest_day_key": max(date_keys) if date_keys else None,
        "stl_rows_still_misclassified_as_steals": stl_steals_leaks,
        "team_total_rows_in_normalized": team_total_rescues,
    }
    SUMMARY_ACTIVE.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"processed raw rows: {summary['raw_rows_processed']}")
    print(f"normalized rows written: {summary['normalized_rows_written']}")
    print(f"quarantined rows written: {summary['quarantined_rows_written']}")
    print(f"normalized market types: {summary['normalized_market_types']}")
    print(f"quarantine reasons: {summary['quarantine_reasons']}")
    print(f"dropped rows by reason: {summary['dropped_rows_by_reason']}")
    print(f"STL rows still misclassified as steals: {summary['stl_rows_still_misclassified_as_steals']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
