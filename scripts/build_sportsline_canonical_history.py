#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "out"

# Read from the combined file that backfill_sportsline_experts_v2.py produces.
# This script does its own semantic dedup, so pre-dedup is not required.
_V2_COMBINED = OUT / "raw_sportsline_expert_pages_all_combined_v2.jsonl"
_V2_DEDUPED = OUT / "raw_sportsline_expert_pages_all_combined_v2_deduped.jsonl"
V2_PATH = _V2_COMBINED if _V2_COMBINED.exists() else _V2_DEDUPED
OLD_PATHS = [
    OUT / "raw_sportsline_expert_pages_all_combined.jsonl",
    OUT / "raw_sportsline_expert_pages_all_unknown.jsonl",
]

CANONICAL_PATH = OUT / "raw_sportsline_expert_pages_all_canonical.jsonl"
GAPFILL_PATH = OUT / "raw_sportsline_expert_pages_old_gapfill_canonical.jsonl"
REPORT_PATH = OUT / "sportsline_canonical_history_report.json"


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def title_from_slug(slug: Optional[str]) -> Optional[str]:
    if not slug:
        return None
    return " ".join(part.capitalize() for part in slug.split("-"))


def normalize_whitespace(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def build_semantic_key(row: Dict[str, Any]) -> str:
    event_time = row.get("event_start_time_utc") or ""
    away = (row.get("away_team") or "").upper()
    home = (row.get("home_team") or "").upper()
    matchup_hint = row.get("matchup_hint") or ""
    return "|".join(
        [
            (row.get("expert_slug") or "").lower(),
            (row.get("sport") or "").upper(),
            away,
            home,
            matchup_hint,
            event_time,
            normalize_whitespace(row.get("raw_pick_text")),
        ]
    )


def flatten_v2_row(row: Dict[str, Any]) -> Dict[str, Any]:
    parsed = row.get("parsed_preview") or {}
    matchup = row.get("matchup") or {}
    expert_slug = row.get("expert_slug")
    expert_name = title_from_slug(expert_slug) or row.get("expert_name")
    sport = row.get("sport") or "UNKNOWN"
    market_type = parsed.get("market_type")
    market_family = "player_prop" if market_type == "player_prop" else "standard"
    away_team = matchup.get("away_code")
    home_team = matchup.get("home_code")
    side = parsed.get("side")
    selection = parsed.get("team_raw")
    if market_type == "total":
        selection = side
    elif market_type == "team_total":
        team_code = None
        candidates = matchup.get("away_code_candidates") or {}
        team_raw = parsed.get("team_raw")
        if team_raw and away_team and team_raw.lower() in str(matchup.get("away_raw") or "").lower():
            team_code = away_team
        elif team_raw and home_team and team_raw.lower() in str(matchup.get("home_raw") or "").lower():
            team_code = home_team
        else:
            for candidate in candidates.values():
                if candidate:
                    team_code = candidate
                    break
        selection = f"{team_code}_{side}" if team_code and side else None
    elif market_type == "player_prop":
        selection = parsed.get("player_name")

    flat = {
        "source_id": "sportsline",
        "source_surface": row.get("source_surface") or "sportsline_expert_pages_backfill_v2",
        "sport": sport,
        "market_family": market_family,
        "observed_at_utc": row.get("observed_at_utc"),
        "canonical_url": row.get("canonical_url"),
        "raw_pick_text": row.get("raw_pick_text"),
        "raw_block": row.get("raw_block"),
        "raw_fingerprint": row.get("raw_fingerprint"),
        "event_start_time_utc": matchup.get("event_time_utc"),
        "matchup_hint": f"{away_team}@{home_team}" if away_team and home_team else None,
        "away_team": away_team,
        "home_team": home_team,
        "expert_name": expert_name,
        "expert_handle": expert_slug,
        "expert_profile": row.get("expert_profile") or row.get("canonical_url"),
        "expert_slug": expert_slug,
        "market_type": market_type,
        "selection": selection,
        "side": side,
        "line": parsed.get("line"),
        "odds": parsed.get("odds"),
        "player_name": parsed.get("player_name"),
        "stat_key": parsed.get("stat_key"),
        "unit_size": parsed.get("unit_size"),
        "pregraded_result": row.get("pregraded_result"),
        "pregraded_by": "sportsline" if row.get("pregraded_result") else None,
        "history_source_version": "v2",
        "profile_variant": row.get("profile_variant"),
    }
    flat["canonical_semantic_key"] = build_semantic_key(flat)
    return flat


def normalize_old_row(row: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(row)
    normalized.setdefault("source_id", "sportsline")
    normalized.setdefault("source_surface", "sportsline_expert_pages_backfill")
    normalized.setdefault("expert_profile", normalized.get("canonical_url"))
    normalized.setdefault("expert_handle", normalized.get("expert_slug"))
    normalized["expert_name"] = title_from_slug(normalized.get("expert_slug")) or normalized.get("expert_name")
    normalized["history_source_version"] = "v1_gapfill"
    normalized["canonical_semantic_key"] = build_semantic_key(normalized)
    return normalized


def main() -> int:
    if not V2_PATH.exists():
        raise SystemExit(f"Missing v2 combined file: {V2_PATH} (run backfill_sportsline_experts_v2.py first)")

    v2_rows = [flatten_v2_row(row) for row in read_jsonl(V2_PATH)]
    old_rows: List[Dict[str, Any]] = []
    for path in OLD_PATHS:
        old_rows.extend(normalize_old_row(row) for row in read_jsonl(path))

    canonical_rows: List[Dict[str, Any]] = []
    gapfill_rows: List[Dict[str, Any]] = []
    seen_semantic: set[str] = set()
    seen_raw_fp: set[str] = set()

    report: Dict[str, Any] = {
        "v2_rows_in": len(v2_rows),
        "old_rows_in": len(old_rows),
        "old_paths": [str(p) for p in OLD_PATHS if p.exists()],
    }

    v2_by_expert = Counter()
    v2_by_sport = Counter()
    for row in v2_rows:
        semantic = row["canonical_semantic_key"]
        if semantic in seen_semantic:
            continue
        seen_semantic.add(semantic)
        if row.get("raw_fingerprint"):
            seen_raw_fp.add(row["raw_fingerprint"])
        canonical_rows.append(row)
        v2_by_expert[row.get("expert_slug") or ""] += 1
        v2_by_sport[row.get("sport") or "UNKNOWN"] += 1

    old_gapfill_by_expert = Counter()
    old_gapfill_by_sport = Counter()
    old_skipped_semantic = 0
    old_skipped_raw_fp = 0

    for row in old_rows:
        semantic = row["canonical_semantic_key"]
        raw_fp = row.get("raw_fingerprint")
        if semantic in seen_semantic:
            old_skipped_semantic += 1
            continue
        if raw_fp and raw_fp in seen_raw_fp:
            old_skipped_raw_fp += 1
            continue
        seen_semantic.add(semantic)
        if raw_fp:
            seen_raw_fp.add(raw_fp)
        canonical_rows.append(row)
        gapfill_rows.append(row)
        old_gapfill_by_expert[row.get("expert_slug") or ""] += 1
        old_gapfill_by_sport[row.get("sport") or "UNKNOWN"] += 1

    write_jsonl(CANONICAL_PATH, canonical_rows)
    write_jsonl(GAPFILL_PATH, gapfill_rows)

    reconciliation = {}
    for expert_slug in sorted(set(v2_by_expert) | set(old_gapfill_by_expert)):
        reconciliation[expert_slug] = {
            "v2_rows_kept": v2_by_expert.get(expert_slug, 0),
            "old_gapfill_rows_kept": old_gapfill_by_expert.get(expert_slug, 0),
        }

    report.update(
        {
            "canonical_rows_out": len(canonical_rows),
            "gapfill_rows_out": len(gapfill_rows),
            "old_skipped_semantic_duplicates": old_skipped_semantic,
            "old_skipped_raw_fingerprint_duplicates": old_skipped_raw_fp,
            "canonical_by_sport": dict(Counter(row.get("sport") or "UNKNOWN" for row in canonical_rows)),
            "v2_by_sport": dict(v2_by_sport),
            "old_gapfill_by_sport": dict(old_gapfill_by_sport),
            "per_expert_reconciliation": reconciliation,
            "outputs": {
                "canonical_path": str(CANONICAL_PATH),
                "gapfill_path": str(GAPFILL_PATH),
            },
        }
    )
    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Wrote canonical SportsLine history -> {CANONICAL_PATH}")
    print(f"Wrote old-only gapfill rows -> {GAPFILL_PATH}")
    print(f"Wrote canonical history report -> {REPORT_PATH}")
    print(f"Canonical rows: {len(canonical_rows)}")
    print(f"Old gapfill rows kept: {len(gapfill_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
