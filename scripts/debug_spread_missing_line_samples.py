"""Inspect spread occurrences that are missing lines and show supports evidence."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

INPUT_PATH = Path("data/ledger/signals_occurrences.jsonl")
OUTPUT_PATH = Path("data/reports/spread_missing_line_support_samples.json")
SAMPLE_LIMIT = 50


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not path.exists():
        return out
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except Exception:
                continue
    return out


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)


def main() -> None:
    rows = read_jsonl(INPUT_PATH)
    samples: List[Dict[str, Any]] = []
    combo_counts = Counter()
    surface_counts = Counter()

    for r in rows:
        if (r.get("market_type") or "").lower() != "spread":
            continue
        if r.get("line") is not None or r.get("line_median") is not None:
            continue
        combo = r.get("sources_combo") or "unknown"
        surface = r.get("source_surface") or "unknown"
        combo_counts[combo] += 1
        surface_counts[surface] += 1
        if len(samples) < SAMPLE_LIMIT:
            supports = []
            for sup in r.get("supports") or []:
                if not isinstance(sup, dict):
                    continue
                supports.append(
                    {
                        "source_id": sup.get("source_id"),
                        "source_surface": sup.get("source_surface"),
                        "selection": sup.get("selection"),
                        "direction": sup.get("direction"),
                        "line": sup.get("line"),
                        "line_hint": sup.get("line_hint"),
                        "raw_pick_text": sup.get("raw_pick_text"),
                        "raw_block": (sup.get("raw_block") or "")[:120] if sup.get("raw_block") else None,
                        "canonical_url": sup.get("canonical_url"),
                    }
                )
            samples.append(
                {
                    "day_key": r.get("day_key"),
                    "event_key": r.get("event_key"),
                    "selection": r.get("selection"),
                    "sources_combo": combo,
                    "source_surface": surface,
                    "supports": supports,
                }
            )

    write_json(
        OUTPUT_PATH,
        {
            "meta": {
                "missing_line_count": sum(combo_counts.values()),
                "missing_by_sources_combo": combo_counts.most_common(20),
                "missing_by_source_surface": surface_counts.most_common(20),
            },
            "rows": samples,
        },
    )

    print(f"[debug] missing_line spreads: {sum(combo_counts.values())}")
    print(f"[debug] by sources_combo (top 10): {combo_counts.most_common(10)}")
    print(f"[debug] by source_surface (top 10): {surface_counts.most_common(10)}")
    print("[debug] sample rows:")
    for row in samples[:10]:
        print(json.dumps(row, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
