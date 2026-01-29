#!/usr/bin/env python
from __future__ import annotations

import argparse
import os
from collections import Counter
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.normalizer_nba import normalize_file
from src.normalizer_sportscapping_nba import normalize_file as normalize_sportscapping
from src.normalizer_betql_nba import normalize_file as normalize_betql
from src.normalizer_betql_nba import normalize_props_file as normalize_betql_props


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


def main(out_dir: Path, debug: bool = False) -> None:
    base = Path(os.getenv("NBA_OUT_DIR", "out"))
    raw_action = out_dir / base / "raw_action_nba.json"
    raw_covers = out_dir / base / "raw_covers_nba.json"
    raw_sportscapping = out_dir / base / "raw_sportscapping_nba.json"
    raw_betql_spread = out_dir / base / "raw_betql_spread_nba.json"
    raw_betql_total = out_dir / base / "raw_betql_total_nba.json"
    raw_betql_sharp = out_dir / base / "raw_betql_sharp_nba.json"
    raw_betql_prop = out_dir / base / "raw_betql_prop_nba.json"
    norm_action = out_dir / base / "normalized_action_nba.json"
    norm_covers = out_dir / base / "normalized_covers_nba.json"
    norm_sportscapping = out_dir / base / "normalized_sportscapping_nba.json"
    norm_betql_spread = out_dir / base / "normalized_betql_spread_nba.json"
    norm_betql_total = out_dir / base / "normalized_betql_total_nba.json"
    norm_betql_sharp = out_dir / base / "normalized_betql_sharp_nba.json"
    norm_betql_prop = out_dir / base / "normalized_betql_prop_nba.json"

    action_records = normalize_file(str(raw_action), str(norm_action), debug=debug)
    covers_records = normalize_file(str(raw_covers), str(norm_covers), debug=debug)
    sport_records = normalize_sportscapping(str(raw_sportscapping), str(norm_sportscapping))
    betql_spread_records = normalize_betql(str(raw_betql_spread), str(norm_betql_spread), debug=debug)
    betql_total_records = normalize_betql(str(raw_betql_total), str(norm_betql_total), debug=debug)
    betql_sharp_records = normalize_betql(str(raw_betql_sharp), str(norm_betql_sharp), debug=debug)
    betql_prop_records = normalize_betql_props(str(raw_betql_prop), str(norm_betql_prop), debug=debug)

    # Merge sharp (probet) into main BetQL outputs by market
    from store import write_json
    betql_spread_records.extend([r for r in betql_sharp_records if (r.get("market") or {}).get("market_type") == "spread"])
    betql_total_records.extend([r for r in betql_sharp_records if (r.get("market") or {}).get("market_type") == "total"])
    write_json(str(norm_betql_spread), betql_spread_records)
    write_json(str(norm_betql_total), betql_total_records)

    print("Action")
    total, eligible, ineligible, reasons = summarize(action_records)
    print(f"  total={total} eligible={eligible} ineligible={ineligible}")
    if ineligible:
        for reason, count in reasons.most_common():
            print(f"    {reason}: {count}")

    print("BetQL Spread")
    total, eligible, ineligible, reasons = summarize(betql_spread_records)
    print(f"  total={total} eligible={eligible} ineligible={ineligible}")
    for reason, count in reasons.most_common():
        print(f"    {reason}: {count}")

    print("BetQL Total")
    total, eligible, ineligible, reasons = summarize(betql_total_records)
    print(f"  total={total} eligible={eligible} ineligible={ineligible}")
    for reason, count in reasons.most_common():
        print(f"    {reason}: {count}")

    print("BetQL Props")
    total, eligible, ineligible, reasons = summarize(betql_prop_records)
    print(f"  total={total} eligible={eligible} ineligible={ineligible}")
    for reason, count in reasons.most_common():
        print(f"    {reason}: {count}")

    # Debug assertion for model+probet double emission
    def assert_double(records, label: str) -> None:
        combo = {}
        reasons = {}
        for r in records:
            if r.get("eligible_for_consensus") is False:
                continue
            ev = (r.get("event") or {}).get("event_key")
            mk = (r.get("market") or {}).get("market_type")
            sel = (r.get("market") or {}).get("selection")
            kind = r.get("provenance", {}).get("source_surface")
            key = (ev, mk, sel)
            combo.setdefault(key, set()).add(kind)
            reasons[key] = reasons.get(key, []) + [r.get("ineligibility_reason")]
        missing = [(k, v) for k, v in combo.items() if len(v) == 1]
        if missing:
            print(f"[DEBUG] betql {label}: model+probet not both present for {len(missing)} keys")
            for k, v in missing[:10]:
                print(f"  key={k} has={v} reasons={reasons.get(k)}")

    assert_double(betql_spread_records + betql_total_records, "double-check")

    print("BetQL Total")
    total, eligible, ineligible, reasons = summarize(betql_total_records)
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
    parser.add_argument("--debug", action="store_true", help="debug output")
    args = parser.parse_args()
    main(Path(args.root), debug=args.debug)
