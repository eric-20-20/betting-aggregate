#!/usr/bin/env python3
"""
Find statistically significant source combo × market patterns from graded occurrences.

Outputs ranked tables + candidate PATTERN_REGISTRY snippets for score_signals.py.

Usage:
    python3 scripts/find_combo_patterns.py
    python3 scripts/find_combo_patterns.py --min-n 25 --min-wilson 0.51
    python3 scripts/find_combo_patterns.py --market spread
    python3 scripts/find_combo_patterns.py --direction
    python3 scripts/find_combo_patterns.py --market total --direction --min-n 15
"""
from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

OCCURRENCES_PATH = Path("data/analysis/graded_occurrences_latest.jsonl")


def wilson_lower(wins: int, n: int, z: float = 1.96) -> float:
    if n == 0:
        return 0.0
    p = wins / n
    denom = 1 + z * z / n
    center = p + z * z / (2 * n)
    spread = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)
    return (center - spread) / denom


def parse_month(day_key: str) -> str:
    """Extract YYYY-MM from day_key format 'NBA:2026:01:23'."""
    parts = day_key.split(":")
    if len(parts) >= 3:
        return f"{parts[1]}-{parts[2]}"
    return "unknown"


def load_occurrences() -> List[dict]:
    rows = []
    with open(OCCURRENCES_PATH) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


DIRECTION_CANONICAL = {"OVER", "UNDER", "over", "under"}


def normalize_direction(direction) -> Optional[str]:
    """Normalize direction to OVER/UNDER/None. Team names and other values → None."""
    if direction is None:
        return None
    d = str(direction).upper()
    if d in ("OVER", "UNDER"):
        return d
    return None


def build_groups(
    rows: List[dict],
    by_direction: bool = False,
    market_filter: Optional[str] = None,
) -> Dict[Tuple, dict]:
    """
    Group occurrences by (sources_combo, market_type[, direction]).
    direction is normalized to OVER/UNDER/None only — team names are excluded.
    Returns dict keyed by group tuple with win/loss/monthly breakdown.
    """
    groups: Dict[Tuple, dict] = {}

    for row in rows:
        result = row.get("result")
        if result not in ("WIN", "LOSS", "PUSH"):
            continue

        combo = row.get("sources_combo") or row.get("occ_sources_combo")
        if not combo:
            continue

        mkt = row.get("market_type")
        if not mkt or mkt not in ("player_prop", "spread", "total", "moneyline"):
            continue

        if market_filter and mkt != market_filter:
            continue

        direction = normalize_direction(row.get("direction")) if by_direction else None

        key = (combo, mkt, direction)
        if key not in groups:
            groups[key] = {"wins": 0, "losses": 0, "pushes": 0, "monthly": defaultdict(lambda: [0, 0])}

        g = groups[key]
        month = parse_month(row.get("day_key", ""))

        if result == "WIN":
            g["wins"] += 1
            g["monthly"][month][0] += 1
        elif result == "LOSS":
            g["losses"] += 1
            g["monthly"][month][1] += 1
        elif result == "PUSH":
            g["pushes"] += 1

    return groups


def monthly_summary(monthly: dict) -> str:
    """Compact monthly win% string for consistency check."""
    parts = []
    for month in sorted(monthly.keys()):
        w, l = monthly[month]
        n = w + l
        if n >= 5:
            pct = round(w / n * 100)
            parts.append(f"{month}:{w}-{l}({pct}%)")
    return "  ".join(parts) if parts else "(no months with n>=5)"


def consistency_flag(monthly: dict) -> str:
    """Return WARNING if pattern is driven by single hot month."""
    month_pcts = []
    for month in monthly:
        w, l = monthly[month]
        n = w + l
        if n >= 5:
            month_pcts.append(w / n)
    if not month_pcts:
        return ""
    if len(month_pcts) == 1:
        return " [SINGLE MONTH]"
    above_55 = sum(1 for p in month_pcts if p >= 0.55)
    if above_55 <= 1 and len(month_pcts) >= 3:
        return " [INCONSISTENT]"
    return ""


def print_table(
    candidates: list,
    by_direction: bool,
    min_n: int,
    min_wilson: float,
):
    direction_col = "direction" if by_direction else None

    if not candidates:
        print(f"\nNo combos meeting n>={min_n}, wilson>={min_wilson:.2f}\n")
        return

    # Header
    cols = ["rank", "combo", "market"]
    if direction_col:
        cols.append("direction")
    cols += ["W-L", "win%", "wilson", "n", "flags"]

    widths = {
        "rank": 4, "combo": 40, "market": 11,
        "direction": 5, "W-L": 10, "win%": 6, "wilson": 6, "n": 5, "flags": 18,
    }

    header = "  ".join(c.ljust(widths[c]) for c in cols if c in widths)
    sep = "-" * len(header)
    print(f"\n{sep}")
    print(f"Candidate patterns  (n>={min_n}, wilson>={min_wilson:.2f}, sorted by wilson desc)")
    print(sep)
    print(header)
    print(sep)

    for i, c in enumerate(candidates, 1):
        combo, mkt, direction = c["key"]
        row_parts = {
            "rank": str(i),
            "combo": combo,
            "market": mkt,
            "direction": str(direction or ""),
            "W-L": f"{c['wins']}-{c['losses']}",
            "win%": f"{c['win_pct']:.1%}",
            "wilson": f"{c['wilson']:.3f}",
            "n": str(c["n"]),
            "flags": c.get("flags", ""),
        }
        line = "  ".join(row_parts[col].ljust(widths[col]) for col in cols if col in widths)
        print(line)

    print(sep)


def print_monthly_detail(candidates: list):
    print("\n\n=== Monthly breakdown for top candidates ===\n")
    for c in candidates[:15]:
        combo, mkt, direction = c["key"]
        dir_str = f" {direction}" if direction else ""
        print(f"  {combo} | {mkt}{dir_str} ({c['wins']}-{c['losses']}, {c['win_pct']:.1%})")
        print(f"    {monthly_summary(c['monthly'])}")
        print()


def print_registry_snippets(candidates: list):
    print("\n\n=== PATTERN_REGISTRY snippets (copy-paste candidates) ===\n")
    for c in candidates:
        combo, mkt, direction = c["key"]
        if c["n"] < 25:
            continue  # Too small for registry

        # Determine tier suggestion
        if c["wilson"] >= 0.52 and "[SINGLE MONTH]" not in c.get("flags", "") and "[INCONSISTENT]" not in c.get("flags", ""):
            tier = "A"
        elif c["wilson"] >= 0.50:
            tier = "B"
        else:
            continue

        slug = combo.replace("|", "_x_").replace("-", "_")
        slug_mkt = mkt.replace("player_prop", "prop")
        pattern_id = f"{slug}_{slug_mkt}"
        if direction:
            pattern_id += f"_{direction.lower()}"

        label_parts = [combo.replace("|", " + "), mkt.replace("player_prop", "player prop")]
        if direction:
            label_parts.append(direction)
        label = " ".join(label_parts)

        snippet_lines = [
            "    {",
            f'        "id": "{pattern_id}",',
            f'        "label": "{label}",',
        ]

        sources = combo.split("|")
        if len(sources) == 1:
            snippet_lines.append(f'        "exact_combo": "{combo}",')
        elif len(sources) == 2:
            snippet_lines.append(f'        "source_pair": {json.dumps(sources)},')
        else:
            snippet_lines.append(f'        "source_contains": {json.dumps(sources)},')

        snippet_lines.append(f'        "market_type": "{mkt}",')
        if direction:
            snippet_lines.append(f'        "direction": "{direction}",')
        if len(sources) >= 2:
            snippet_lines.append(f'        "min_sources": {len(sources)},')

        snippet_lines.append(f'        "hist": {{"record": "{c["wins"]}-{c["losses"]}", "win_pct": {c["win_pct"]:.3f}, "n": {c["n"]}}},')
        snippet_lines.append(f'        "tier_eligible": "{tier}",')
        snippet_lines.append("    },")

        print("\n".join(snippet_lines))
        print()


def main():
    ap = argparse.ArgumentParser(description="Find strong source combo × market patterns")
    ap.add_argument("--min-n", type=int, default=20, help="Minimum graded occurrences (default 20)")
    ap.add_argument("--min-wilson", type=float, default=0.50, help="Minimum Wilson lower bound (default 0.50)")
    ap.add_argument("--market", choices=["player_prop", "spread", "total", "moneyline"], help="Filter to one market type")
    ap.add_argument("--direction", action="store_true", help="Break down by direction (OVER/UNDER)")
    ap.add_argument("--monthly", action="store_true", help="Show monthly breakdown for top candidates")
    ap.add_argument("--snippets", action="store_true", default=True, help="Print PATTERN_REGISTRY snippets (default on)")
    ap.add_argument("--no-snippets", dest="snippets", action="store_false")
    args = ap.parse_args()

    print(f"Loading {OCCURRENCES_PATH} ...", flush=True)
    rows = load_occurrences()
    print(f"  {len(rows):,} total occurrence rows", flush=True)

    graded = [r for r in rows if r.get("result") in ("WIN", "LOSS", "PUSH")]
    print(f"  {len(graded):,} graded (WIN/LOSS/PUSH)", flush=True)

    groups = build_groups(graded, by_direction=args.direction, market_filter=args.market)

    candidates = []
    for key, g in groups.items():
        w, l = g["wins"], g["losses"]
        n = w + l
        if n == 0:
            continue
        wl = wilson_lower(w, n)
        win_pct = w / n

        if n < args.min_n or wl < args.min_wilson:
            continue

        flags = consistency_flag(g["monthly"])
        candidates.append({
            "key": key,
            "wins": w,
            "losses": l,
            "n": n,
            "win_pct": win_pct,
            "wilson": wl,
            "monthly": g["monthly"],
            "flags": flags,
        })

    candidates.sort(key=lambda c: c["wilson"], reverse=True)

    print_table(candidates, by_direction=args.direction, min_n=args.min_n, min_wilson=args.min_wilson)

    if args.monthly or len(candidates) > 0:
        print_monthly_detail(candidates)

    if args.snippets:
        print_registry_snippets(candidates)

    print(f"\nTotal candidates: {len(candidates)}")


if __name__ == "__main__":
    main()
