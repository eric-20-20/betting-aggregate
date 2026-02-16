"""Inspect supports for spread occurrences missing lines to see available evidence."""

from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

INPUT_PATH = Path("data/ledger/signals_occurrences.jsonl")


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


def main() -> None:
    rows = read_jsonl(INPUT_PATH)
    missing_spreads = [
        r
        for r in rows
        if (r.get("market_type") or "").lower() == "spread" and r.get("line") is None and r.get("line_median") is None and r.get("supports")
    ]
    field_counts = Counter()
    line_re = re.compile(r"[+-]\d+(?:\.\d+)?")
    for r in missing_spreads:
        for sup in r.get("supports") or []:
            if not isinstance(sup, dict):
                continue
            if sup.get("line") is not None:
                field_counts["line"] += 1
            if sup.get("line_hint") is not None:
                field_counts["line_hint"] += 1
            for key in ("raw_pick_text", "raw_block"):
                val = sup.get(key)
                if isinstance(val, str) and line_re.search(val):
                    field_counts[key] += 1
    print(f"[supports] missing_line spreads with supports: {len(missing_spreads)}")
    print(f"[supports] evidence counts: {dict(field_counts)}")
    print("[supports] sample supports with evidence (up to 5):")
    shown = 0
    for r in missing_spreads:
        for sup in r.get("supports") or []:
            has_evidence = False
            if sup.get("line") is not None or sup.get("line_hint") is not None:
                has_evidence = True
            for key in ("raw_pick_text", "raw_block"):
                val = sup.get(key)
                if isinstance(val, str) and line_re.search(val):
                    has_evidence = True
            if not has_evidence:
                continue
            print(
                json.dumps(
                    {
                        "canonical_url": sup.get("canonical_url"),
                        "source_id": sup.get("source_id"),
                        "source_surface": sup.get("source_surface"),
                        "selection": sup.get("selection"),
                        "direction": sup.get("direction"),
                        "line": sup.get("line"),
                        "line_hint": sup.get("line_hint"),
                        "raw_pick_text": sup.get("raw_pick_text"),
                        "raw_block": (sup.get("raw_block") or "")[:120] if sup.get("raw_block") else None,
                    },
                    ensure_ascii=False,
                )
            )
            shown += 1
            if shown >= 5:
                break
        if shown >= 5:
            break


if __name__ == "__main__":
    main()
