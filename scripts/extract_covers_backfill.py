#!/usr/bin/env python3
"""Extract and deduplicate Covers picks from historical run data.

Walks data/runs/*/normalized_records.jsonl, extracts source_id=covers rows,
deduplicates by (event_key, market_type, selection), and writes to
data/latest/normalized_covers_backfill.jsonl in the standard normalized format
consumed by build_signal_ledger.py --include-normalized-jsonl.
"""

import json
import glob
import sys
from pathlib import Path


def main():
    run_pattern = "data/runs/*/normalized_records.jsonl"
    out_path = Path("data/latest/normalized_covers_backfill.jsonl")

    seen = set()
    deduped = []
    total_raw = 0

    for fpath in sorted(glob.glob(run_pattern)):
        with open(fpath) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                src = row.get("provenance", {}).get("source_id", "")
                if src != "covers":
                    continue
                total_raw += 1

                ev = row.get("event", {}) or {}
                mk = row.get("market", {}) or {}
                key = (
                    ev.get("event_key"),
                    mk.get("market_type"),
                    mk.get("selection"),
                    mk.get("line"),
                )
                if key in seen:
                    continue
                seen.add(key)
                deduped.append(row)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        for row in deduped:
            f.write(json.dumps(row) + "\n")

    print(f"Total raw covers records: {total_raw}")
    print(f"Unique after dedup: {len(deduped)}")
    print(f"Written to: {out_path}")


if __name__ == "__main__":
    main()
