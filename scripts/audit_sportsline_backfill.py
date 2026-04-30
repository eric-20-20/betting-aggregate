#!/usr/bin/env python3
from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "out"

V2_GLOB = "raw_sportsline_expert_*_all_backfill_v2.jsonl"
OLD_COMBINED = OUT / "raw_sportsline_expert_pages_all_combined.jsonl"
OLD_UNKNOWN = OUT / "raw_sportsline_expert_pages_all_unknown.jsonl"
REPORT_PATH = OUT / "sportsline_backfill_audit.json"
DEDUPED_COMBINED_PATH = OUT / "raw_sportsline_expert_pages_all_combined_v2_deduped.jsonl"


def semantic_key(row: Dict[str, Any]) -> str:
    raw_card = row.get("raw_card") or {}
    matchup = row.get("matchup") or {}
    return "|".join(
        [
            row.get("expert_slug") or "",
            raw_card.get("matchup_text") or "",
            raw_card.get("pick_text") or "",
            matchup.get("event_time_utc") or "",
        ]
    )


def load_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            yield json.loads(line)


def load_old_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for path in (OLD_COMBINED, OLD_UNKNOWN):
        if not path.exists():
            continue
        rows.extend(load_jsonl(path))
    return rows


def main() -> int:
    v2_paths = sorted(OUT.glob(V2_GLOB))
    if not v2_paths:
        raise SystemExit("No v2 sportsline files found")

    v2_rows_by_expert: Dict[str, List[Dict[str, Any]]] = {}
    deduped_rows: List[Dict[str, Any]] = []
    deduped_semantic_seen: set[str] = set()
    duplicate_examples: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    v2_totals = Counter()
    per_expert_report: Dict[str, Dict[str, Any]] = {}

    for path in v2_paths:
        rows = list(load_jsonl(path))
        if not rows:
            continue
        slug = path.name.replace("raw_sportsline_expert_", "").replace("_all_backfill_v2.jsonl", "")
        v2_rows_by_expert[slug] = rows

        raw_fp_counts = Counter(row.get("raw_fingerprint") for row in rows if row.get("raw_fingerprint"))
        semantic_counts = Counter(semantic_key(row) for row in rows)
        sports = Counter(row.get("sport", "UNKNOWN") for row in rows)
        variants = Counter(row.get("profile_variant", "ROOT?") for row in rows)

        raw_fp_dupes = sum(count - 1 for count in raw_fp_counts.values() if count > 1)
        semantic_dupes = sum(count - 1 for count in semantic_counts.values() if count > 1)

        if semantic_dupes:
            seen_local: set[str] = set()
            for row in rows:
                sk = semantic_key(row)
                if semantic_counts[sk] > 1 and sk not in seen_local and len(duplicate_examples[slug]) < 5:
                    duplicate_examples[slug].append(
                        {
                            "semantic_key": sk,
                            "matchup_text": (row.get("raw_card") or {}).get("matchup_text"),
                            "pick_text": (row.get("raw_card") or {}).get("pick_text"),
                            "sport": row.get("sport"),
                            "profile_variant": row.get("profile_variant"),
                        }
                    )
                    seen_local.add(sk)

        per_expert_report[slug] = {
            "path": str(path),
            "row_count": len(rows),
            "sports": dict(sports),
            "profile_variants": dict(variants),
            "duplicate_raw_fingerprint_rows": raw_fp_dupes,
            "duplicate_semantic_rows": semantic_dupes,
        }
        if duplicate_examples[slug]:
            per_expert_report[slug]["duplicate_examples"] = duplicate_examples[slug]

        for row in rows:
            v2_totals[row.get("sport", "UNKNOWN")] += 1
            sk = semantic_key(row)
            if sk in deduped_semantic_seen:
                continue
            deduped_semantic_seen.add(sk)
            deduped_rows.append(row)

    deduped_combined_count = len(deduped_rows)
    with DEDUPED_COMBINED_PATH.open("w", encoding="utf-8") as f:
        for row in deduped_rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    old_rows = load_old_rows()
    old_by_expert = defaultdict(list)
    for row in old_rows:
        slug = (row.get("expert_slug") or "").lower()
        if slug:
            old_by_expert[slug].append(row)

    reconciliation: Dict[str, Dict[str, Any]] = {}
    relevant_slugs = sorted(set(v2_rows_by_expert) | set(old_by_expert))
    for slug in relevant_slugs:
        old_rows_slug = old_by_expert.get(slug, [])
        v2_rows_slug = v2_rows_by_expert.get(slug, [])
        old_sports = Counter(row.get("sport", "UNKNOWN") for row in old_rows_slug)
        v2_sports = Counter(row.get("sport", "UNKNOWN") for row in v2_rows_slug)
        reconciliation[slug] = {
            "old_row_count": len(old_rows_slug),
            "v2_row_count": len(v2_rows_slug),
            "delta_v2_minus_old": len(v2_rows_slug) - len(old_rows_slug),
            "old_sports": dict(old_sports),
            "v2_sports": dict(v2_sports),
        }

    report = {
        "v2_files_found": len(v2_rows_by_expert),
        "v2_total_rows": sum(len(rows) for rows in v2_rows_by_expert.values()),
        "v2_total_by_sport": dict(v2_totals),
        "v2_deduped_combined_path": str(DEDUPED_COMBINED_PATH),
        "v2_deduped_combined_rows": deduped_combined_count,
        "per_expert": per_expert_report,
        "reconciliation_old_vs_v2": reconciliation,
    }
    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote audit -> {REPORT_PATH}")
    print(f"Wrote deduped combined -> {DEDUPED_COMBINED_PATH}")
    print(f"Experts audited: {len(v2_rows_by_expert)}")
    print(f"V2 total rows: {report['v2_total_rows']}")
    print(f"V2 deduped combined rows: {deduped_combined_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
