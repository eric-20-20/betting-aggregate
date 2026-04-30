#!/usr/bin/env python3
from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "out"

import sys
sys.path.insert(0, str(ROOT))

from src.normalizer_sportsline_nba import normalize_sportsline_record

INPUT_PATH = OUT / "raw_sportsline_expert_pages_all_canonical.jsonl"
COMBINED_OUTPUT = OUT / "normalized_sportsline_expert_pages_all_canonical.jsonl"
OUTPUT_BY_SPORT = {
    "NBA": OUT / "normalized_sportsline_expert_pages_nba_canonical.jsonl",
    "MLB": OUT / "normalized_sportsline_expert_pages_mlb_canonical.jsonl",
    "NCAAB": OUT / "normalized_sportsline_expert_pages_ncaab_canonical.jsonl",
}
UNRESOLVED_RAW_OUTPUT = OUT / "raw_sportsline_expert_pages_unresolved_canonical.jsonl"
REPORT_PATH = OUT / "sportsline_canonical_normalization_report.json"


def title_from_slug(slug: str | None) -> str | None:
    if not slug:
        return None
    return " ".join(part.capitalize() for part in slug.split("-"))


def canonicalize_expert_name(rec: Dict[str, Any]) -> None:
    prov = rec.get("provenance") or {}
    slug = prov.get("expert_slug")
    title = title_from_slug(slug)
    if title:
        prov["expert_name"] = title
    if prov.get("expert_name") == "Prop Bet Guy":
        prov["expert_name"] = "Prop Bet Guy"
    rec["provenance"] = prov


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


def main() -> int:
    if not INPUT_PATH.exists():
        raise SystemExit(f"Missing canonical SportsLine raw file: {INPUT_PATH}")

    raw_rows = read_jsonl(INPUT_PATH)
    rows_by_sport: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    unresolved_rows: List[Dict[str, Any]] = []
    report: Dict[str, Any] = {
        "input_path": str(INPUT_PATH),
        "input_rows": len(raw_rows),
        "input_by_sport": dict(Counter((row.get("sport") or "UNKNOWN") for row in raw_rows)),
    }

    combined_normalized: List[Dict[str, Any]] = []
    report_by_sport: Dict[str, Dict[str, Any]] = {}

    for row in raw_rows:
        sport = (row.get("sport") or "UNKNOWN").upper()
        if sport not in OUTPUT_BY_SPORT:
            unresolved_rows.append(row)
            continue
        rows_by_sport[sport].append(row)

    for sport, path in OUTPUT_BY_SPORT.items():
        raw_sport_rows = rows_by_sport.get(sport, [])
        normalized_rows: List[Dict[str, Any]] = []
        ineligible = Counter()

        for raw in raw_sport_rows:
            rec = normalize_sportsline_record(raw, sport=sport)
            canonicalize_expert_name(rec)
            if rec.get("eligible_for_consensus"):
                normalized_rows.append(rec)
            else:
                ineligible[rec.get("ineligibility_reason") or "unknown"] += 1

        deduped: Dict[tuple, Dict[str, Any]] = {}
        for rec in normalized_rows:
            ev = rec.get("event") or {}
            mk = rec.get("market") or {}
            prov = rec.get("provenance") or {}
            key = (
                prov.get("expert_slug") or prov.get("expert_name") or "",
                ev.get("event_key"),
                mk.get("market_type"),
                mk.get("selection"),
                mk.get("line"),
            )
            if key in deduped:
                continue
            deduped[key] = rec

        deduped_rows = list(deduped.values())
        combined_normalized.extend(deduped_rows)
        write_jsonl(path, deduped_rows)
        report_by_sport[sport] = {
            "raw_rows": len(raw_sport_rows),
            "normalized_rows": len(deduped_rows),
            "normalized_market_types": dict(Counter((row.get("market") or {}).get("market_type") for row in deduped_rows)),
            "ineligible_reasons": dict(ineligible),
            "output_path": str(path),
        }

    write_jsonl(COMBINED_OUTPUT, combined_normalized)
    write_jsonl(UNRESOLVED_RAW_OUTPUT, unresolved_rows)

    report.update(
        {
            "combined_output_path": str(COMBINED_OUTPUT),
            "combined_output_rows": len(combined_normalized),
            "unresolved_raw_output_path": str(UNRESOLVED_RAW_OUTPUT),
            "unresolved_raw_rows": len(unresolved_rows),
            "by_sport": report_by_sport,
        }
    )
    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Wrote normalized SportsLine history -> {COMBINED_OUTPUT}")
    for sport, path in OUTPUT_BY_SPORT.items():
        print(f"  {sport}: {path}")
    print(f"Wrote unresolved raw rows -> {UNRESOLVED_RAW_OUTPUT}")
    print(f"Wrote normalization report -> {REPORT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
