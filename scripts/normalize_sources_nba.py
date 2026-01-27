#!/usr/bin/env python
from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.normalizer_nba import normalize_file
from src.normalizer_sportscapping_nba import normalize_file as normalize_sportscapping
from src.normalizer_betql_nba import normalize_file as normalize_betql


def summarize(records):
    total = len(records)
    eligible = sum(1 for r in records if r.get("eligible_for_consensus"))
    ineligible = total - eligible
    reasons = Counter()
    for r in records:
        if r.get("eligible_for_consensus"):
            continue
        reasons[r.get("ineligibility_reason") or "unknown"] += 1
    return total, eligible, ineligible, reasons


def main(out_dir: Path) -> None:
    raw_action = out_dir / "out" / "raw_action_nba.json"
    raw_covers = out_dir / "out" / "raw_covers_nba.json"
    raw_sportscapping = out_dir / "data" / "raw_sportscapping_nba.json"
    raw_betql_model = out_dir / "data" / "raw_betql_model_nba.json"
    raw_betql_sharps = out_dir / "data" / "raw_betql_sharps_nba.json"
    raw_betql_props = out_dir / "data" / "raw_betql_props_nba.json"
    norm_action = out_dir / "out" / "normalized_action_nba.json"
    norm_covers = out_dir / "out" / "normalized_covers_nba.json"
    norm_sportscapping = out_dir / "out" / "normalized_sportscapping_nba.json"
    norm_betql_model = out_dir / "out" / "normalized_betql_model_nba.json"
    norm_betql_sharps = out_dir / "out" / "normalized_betql_sharps_nba.json"
    norm_betql_props = out_dir / "out" / "normalized_betql_props_nba.json"

    action_records = normalize_file(str(raw_action), str(norm_action))
    covers_records = normalize_file(str(raw_covers), str(norm_covers))
    sport_records = normalize_sportscapping(str(raw_sportscapping), str(norm_sportscapping))
    betql_model_records = normalize_betql(str(raw_betql_model), str(norm_betql_model))
    betql_sharp_records = normalize_betql(str(raw_betql_sharps), str(norm_betql_sharps))
    betql_prop_records = normalize_betql(str(raw_betql_props), str(norm_betql_props))

    print("Action")
    total, eligible, ineligible, reasons = summarize(action_records)
    print(f"  total={total} eligible={eligible} ineligible={ineligible}")
    if ineligible:
        for reason, count in reasons.most_common():
            print(f"    {reason}: {count}")

    print("BetQL Model")
    total, eligible, ineligible, reasons = summarize(betql_model_records)
    print(f"  total={total} eligible={eligible} ineligible={ineligible}")
    for reason, count in reasons.most_common():
        print(f"    {reason}: {count}")

    print("BetQL Sharps")
    total, eligible, ineligible, reasons = summarize(betql_sharp_records)
    print(f"  total={total} eligible={eligible} ineligible={ineligible}")
    for reason, count in reasons.most_common():
        print(f"    {reason}: {count}")

    print("BetQL Props")
    total, eligible, ineligible, reasons = summarize(betql_prop_records)
    print(f"  total={total} eligible={eligible} ineligible={ineligible}")
    for reason, count in reasons.most_common():
        print(f"    {reason}: {count}")
    print("Sportscapping")
    total, eligible, ineligible, reasons = summarize(sport_records)
    print(f"  total={total} eligible={eligible} ineligible={ineligible}")
    if ineligible:
        for reason, count in reasons.most_common():
            print(f"    {reason}: {count}")

    print("Covers")
    total, eligible, ineligible, reasons = summarize(covers_records)
    print(f"  total={total} eligible={eligible} ineligible={ineligible}")
    if ineligible:
        for reason, count in reasons.most_common():
            print(f"    {reason}: {count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Normalize NBA sources (action + covers)")
    parser.add_argument("--root", default=".", help="project root (default: cwd)")
    args = parser.parse_args()
    main(Path(args.root))
