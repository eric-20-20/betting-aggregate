#!/usr/bin/env python3
"""
Comprehensive source-combo profitability analysis.
Joins signals to grades and breaks down win rates by every meaningful dimension.
"""

import json
import math
import sys
from collections import defaultdict
from pathlib import Path

SIGNALS_PATH = Path("data/ledger/signals_latest.jsonl")
GRADES_PATH = Path("data/ledger/grades_latest.jsonl")

def wilson_lower(wins, n, z=1.96):
    """Wilson score lower bound for 95% CI."""
    if n == 0:
        return 0.0
    p = wins / n
    denom = 1 + z * z / n
    center = p + z * z / (2 * n)
    spread = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (center - spread) / denom

def load_jsonl(path):
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records

def print_table(rows, headers, title, min_n=10, sort_key="win_pct", highlight_threshold=0.52):
    """Print a formatted table. rows is list of dicts with keys matching headers."""
    filtered = [r for r in rows if r.get("n", 0) >= min_n]
    if not filtered:
        print(f"\n{'='*80}")
        print(f"  {title}")
        print(f"{'='*80}")
        print("  (no combos with n >= {min_n})")
        return

    # Sort by sort_key descending
    filtered.sort(key=lambda r: r.get(sort_key, 0), reverse=True)

    # Calculate column widths
    col_widths = {}
    for h in headers:
        col_widths[h] = max(len(h), max(len(str(r.get(h, ""))) for r in filtered))

    # Print
    print(f"\n{'='*100}")
    print(f"  {title}")
    print(f"  (n >= {min_n}, sorted by {sort_key} desc)")
    print(f"{'='*100}")

    header_line = "  ".join(str(h).ljust(col_widths[h]) for h in headers)
    print(f"  {header_line}")
    print(f"  {'─' * len(header_line)}")

    for r in filtered:
        parts = []
        for h in headers:
            val = r.get(h, "")
            parts.append(str(val).ljust(col_widths[h]))
        line = "  ".join(parts)
        # Mark profitable lines
        wp = r.get("win_pct_raw", 0)
        marker = ""
        if wp >= 0.55:
            marker = " <<<< STRONG"
        elif wp >= highlight_threshold:
            marker = " << PROFITABLE"
        print(f"  {line}{marker}")

    print(f"  ({'─' * len(header_line)})")
    print(f"  Total rows shown: {len(filtered)}")


def main():
    print("Loading data...")
    signals = load_jsonl(SIGNALS_PATH)
    grades = load_jsonl(GRADES_PATH)
    print(f"  Signals: {len(signals)}")
    print(f"  Grades:  {len(grades)}")

    # Build grade lookup by signal_id
    grade_by_id = {}
    for g in grades:
        sid = g.get("signal_id")
        if sid:
            grade_by_id[sid] = g

    # Join signals to grades
    joined = []
    unmatched = 0
    for s in signals:
        sid = s.get("signal_id")
        g = grade_by_id.get(sid)
        if not g:
            unmatched += 1
            continue
        raw_result = g.get("result") or g.get("status") or ""
        if raw_result is None:
            continue
        result = str(raw_result).upper()
        if result not in ("WIN", "LOSS", "PUSH"):
            continue

        sources_combo = s.get("sources_combo", "")
        sources = s.get("sources", s.get("sources_present", []))
        market_type = s.get("market_type", "unknown")
        atomic_stat = s.get("atomic_stat", None)
        signal_type = s.get("signal_type", "unknown")
        direction = s.get("direction", None)
        source_count = len(sources) if sources else sources_combo.count("|") + 1

        joined.append({
            "signal_id": sid,
            "sources_combo": sources_combo,
            "sources": sorted(sources) if sources else [],
            "market_type": market_type,
            "atomic_stat": atomic_stat or "(none)",
            "result": result,
            "source_count": source_count,
            "signal_type": signal_type,
            "direction": direction,
            "day_key": s.get("day_key", ""),
            "score": s.get("score", 0),
        })

    print(f"  Joined:  {len(joined)} (unmatched: {unmatched})")

    # Exclude conflicts and unresolvable
    joined = [j for j in joined if "conflict" not in (j["sources_combo"] or "") and j["result"] in ("WIN", "LOSS")]
    pushes_excluded = sum(1 for j in joined if j["result"] == "PUSH")  # already filtered
    print(f"  After excluding conflicts and pushes: {len(joined)}")

    # ─────────────────────────────────────────────────────────
    # HELPER: aggregate stats from a list of records
    # ─────────────────────────────────────────────────────────
    def aggregate(records):
        wins = sum(1 for r in records if r["result"] == "WIN")
        losses = sum(1 for r in records if r["result"] == "LOSS")
        n = wins + losses
        wp = wins / n if n > 0 else 0
        wl = wilson_lower(wins, n)
        return {
            "wins": wins,
            "losses": losses,
            "n": n,
            "win_pct": f"{wp:.1%}",
            "win_pct_raw": wp,
            "wilson_lower": f"{wl:.3f}",
            "wilson_lower_raw": wl,
        }

    # ─────────────────────────────────────────────────────────
    # 1. OVERALL SUMMARY
    # ─────────────────────────────────────────────────────────
    print("\n" + "=" * 100)
    print("  OVERALL SUMMARY")
    print("=" * 100)
    overall = aggregate(joined)
    print(f"  Total graded (excl. push/conflict): {overall['n']}")
    print(f"  Wins: {overall['wins']}  Losses: {overall['losses']}")
    print(f"  Win Rate: {overall['win_pct']}  Wilson Lower: {overall['wilson_lower']}")

    # ─────────────────────────────────────────────────────────
    # 2. BY MARKET TYPE (overall)
    # ─────────────────────────────────────────────────────────
    by_market = defaultdict(list)
    for r in joined:
        by_market[r["market_type"]].append(r)

    rows = []
    for mt, recs in sorted(by_market.items()):
        stats = aggregate(recs)
        stats["market_type"] = mt
        rows.append(stats)

    print_table(rows,
        ["market_type", "wins", "losses", "n", "win_pct", "wilson_lower"],
        "BY MARKET TYPE (overall)", min_n=1, sort_key="win_pct")

    # ─────────────────────────────────────────────────────────
    # 3. BY SOURCE COUNT (1-source, 2-source, 3+)
    # ─────────────────────────────────────────────────────────
    by_count = defaultdict(list)
    for r in joined:
        c = r["source_count"]
        label = f"{c}-source" if c <= 3 else "4+-source"
        by_count[label].append(r)

    rows = []
    for label, recs in sorted(by_count.items()):
        stats = aggregate(recs)
        stats["source_count"] = label
        rows.append(stats)

    print_table(rows,
        ["source_count", "wins", "losses", "n", "win_pct", "wilson_lower"],
        "BY SOURCE COUNT", min_n=1, sort_key="win_pct")

    # ─────────────────────────────────────────────────────────
    # 4. BY SOURCE COUNT x MARKET TYPE
    # ─────────────────────────────────────────────────────────
    by_count_market = defaultdict(list)
    for r in joined:
        c = r["source_count"]
        label = f"{c}-source" if c <= 3 else "4+-source"
        by_count_market[(label, r["market_type"])].append(r)

    rows = []
    for (label, mt), recs in sorted(by_count_market.items()):
        stats = aggregate(recs)
        stats["source_count"] = label
        stats["market_type"] = mt
        rows.append(stats)

    print_table(rows,
        ["source_count", "market_type", "wins", "losses", "n", "win_pct", "wilson_lower"],
        "BY SOURCE COUNT x MARKET TYPE", min_n=5, sort_key="win_pct")

    # ─────────────────────────────────────────────────────────
    # 5. BY INDIVIDUAL SOURCE PARTICIPATION
    # ─────────────────────────────────────────────────────────
    by_source = defaultdict(list)
    for r in joined:
        for src in r["sources"]:
            by_source[src].append(r)

    rows = []
    for src, recs in sorted(by_source.items()):
        stats = aggregate(recs)
        stats["source"] = src
        rows.append(stats)

    print_table(rows,
        ["source", "wins", "losses", "n", "win_pct", "wilson_lower"],
        "BY INDIVIDUAL SOURCE PARTICIPATION (when source X is present in any combo)",
        min_n=1, sort_key="win_pct")

    # ─────────────────────────────────────────────────────────
    # 5b. BY INDIVIDUAL SOURCE x MARKET TYPE
    # ─────────────────────────────────────────────────────────
    by_source_market = defaultdict(list)
    for r in joined:
        for src in r["sources"]:
            by_source_market[(src, r["market_type"])].append(r)

    rows = []
    for (src, mt), recs in sorted(by_source_market.items()):
        stats = aggregate(recs)
        stats["source"] = src
        stats["market_type"] = mt
        rows.append(stats)

    print_table(rows,
        ["source", "market_type", "wins", "losses", "n", "win_pct", "wilson_lower"],
        "BY INDIVIDUAL SOURCE x MARKET TYPE",
        min_n=5, sort_key="win_pct")

    # ─────────────────────────────────────────────────────────
    # 6. BY SOURCES COMBO x MARKET TYPE (the main table)
    # ─────────────────────────────────────────────────────────
    by_combo_market = defaultdict(list)
    for r in joined:
        by_combo_market[(r["sources_combo"], r["market_type"])].append(r)

    rows = []
    for (combo, mt), recs in sorted(by_combo_market.items()):
        stats = aggregate(recs)
        stats["sources_combo"] = combo
        stats["market_type"] = mt
        stats["src_count"] = combo.count("|") + 1
        rows.append(stats)

    print_table(rows,
        ["sources_combo", "market_type", "src_count", "wins", "losses", "n", "win_pct", "wilson_lower"],
        "BY SOURCES COMBO x MARKET TYPE (THE MAIN TABLE)",
        min_n=10, sort_key="win_pct")

    # Also show with lower threshold for discovery
    print_table(rows,
        ["sources_combo", "market_type", "src_count", "wins", "losses", "n", "win_pct", "wilson_lower"],
        "BY SOURCES COMBO x MARKET TYPE (n >= 5, for discovery)",
        min_n=5, sort_key="win_pct")

    # ─────────────────────────────────────────────────────────
    # 7. BY SOURCES COMBO x MARKET TYPE x ATOMIC STAT (player props deep dive)
    # ─────────────────────────────────────────────────────────
    prop_records = [r for r in joined if r["market_type"] == "player_prop"]
    print(f"\n  Player prop records for deep dive: {len(prop_records)}")

    by_combo_stat = defaultdict(list)
    for r in prop_records:
        by_combo_stat[(r["sources_combo"], r["atomic_stat"])].append(r)

    rows = []
    for (combo, stat), recs in sorted(by_combo_stat.items()):
        stats = aggregate(recs)
        stats["sources_combo"] = combo
        stats["atomic_stat"] = stat
        stats["src_count"] = combo.count("|") + 1
        rows.append(stats)

    print_table(rows,
        ["sources_combo", "atomic_stat", "src_count", "wins", "losses", "n", "win_pct", "wilson_lower"],
        "PLAYER PROPS: BY SOURCES COMBO x ATOMIC STAT",
        min_n=10, sort_key="win_pct")

    print_table(rows,
        ["sources_combo", "atomic_stat", "src_count", "wins", "losses", "n", "win_pct", "wilson_lower"],
        "PLAYER PROPS: BY SOURCES COMBO x ATOMIC STAT (n >= 5)",
        min_n=5, sort_key="win_pct")

    # ─────────────────────────────────────────────────────────
    # 8. BY ATOMIC STAT (overall for props)
    # ─────────────────────────────────────────────────────────
    by_stat = defaultdict(list)
    for r in prop_records:
        by_stat[r["atomic_stat"]].append(r)

    rows = []
    for stat, recs in sorted(by_stat.items()):
        stats = aggregate(recs)
        stats["atomic_stat"] = stat
        rows.append(stats)

    print_table(rows,
        ["atomic_stat", "wins", "losses", "n", "win_pct", "wilson_lower"],
        "PLAYER PROPS: BY ATOMIC STAT (overall)",
        min_n=5, sort_key="win_pct")

    # ─────────────────────────────────────────────────────────
    # 9. BY DIRECTION (OVER/UNDER) for props
    # ─────────────────────────────────────────────────────────
    by_dir = defaultdict(list)
    for r in prop_records:
        d = r.get("direction") or "unknown"
        by_dir[d].append(r)

    rows = []
    for d, recs in sorted(by_dir.items()):
        stats = aggregate(recs)
        stats["direction"] = d
        rows.append(stats)

    print_table(rows,
        ["direction", "wins", "losses", "n", "win_pct", "wilson_lower"],
        "PLAYER PROPS: BY DIRECTION (OVER vs UNDER)",
        min_n=5, sort_key="win_pct")

    # ─────────────────────────────────────────────────────────
    # 10. WILSON LOWER RANKING - Top combos by statistical confidence
    # ─────────────────────────────────────────────────────────
    # Re-use the combo x market rows
    by_combo_market_rows = []
    for (combo, mt), recs in sorted(by_combo_market.items()):
        stats = aggregate(recs)
        stats["sources_combo"] = combo
        stats["market_type"] = mt
        stats["src_count"] = combo.count("|") + 1
        by_combo_market_rows.append(stats)

    # Filter n >= 10 and sort by wilson_lower
    print_table(by_combo_market_rows,
        ["sources_combo", "market_type", "src_count", "wins", "losses", "n", "win_pct", "wilson_lower"],
        "RANKED BY WILSON LOWER BOUND (statistical confidence, n >= 10)",
        min_n=10, sort_key="wilson_lower_raw")

    # ─────────────────────────────────────────────────────────
    # 11. BY SIGNAL TYPE
    # ─────────────────────────────────────────────────────────
    by_sigtype = defaultdict(list)
    for r in joined:
        by_sigtype[r["signal_type"]].append(r)

    rows = []
    for st, recs in sorted(by_sigtype.items()):
        stats = aggregate(recs)
        stats["signal_type"] = st
        rows.append(stats)

    print_table(rows,
        ["signal_type", "wins", "losses", "n", "win_pct", "wilson_lower"],
        "BY SIGNAL TYPE", min_n=1, sort_key="win_pct")

    # ─────────────────────────────────────────────────────────
    # 12. TIER RECOMMENDATION SUMMARY
    # ─────────────────────────────────────────────────────────
    print("\n" + "=" * 100)
    print("  TIER RECOMMENDATION SUMMARY")
    print("=" * 100)
    print()

    # Gather all combos with n >= 10, sorted by wilson_lower
    tier_candidates = []
    for (combo, mt), recs in by_combo_market.items():
        stats = aggregate(recs)
        if stats["n"] < 10:
            continue
        tier_candidates.append({
            "combo": combo,
            "market": mt,
            "n": stats["n"],
            "win_pct": stats["win_pct_raw"],
            "wilson": stats["wilson_lower_raw"],
        })

    tier_candidates.sort(key=lambda x: x["wilson"], reverse=True)

    print("  TIER 1 (Wilson Lower >= 0.52 -- statistically profitable):")
    print("  " + "-" * 80)
    t1 = [t for t in tier_candidates if t["wilson"] >= 0.52]
    if t1:
        for t in t1:
            print(f"    {t['combo']:40s} {t['market']:15s}  n={t['n']:4d}  WR={t['win_pct']:.1%}  WL={t['wilson']:.3f}")
    else:
        print("    (none)")

    print()
    print("  TIER 2 (Wilson Lower >= 0.48, win_pct >= 0.52 -- likely profitable):")
    print("  " + "-" * 80)
    t2 = [t for t in tier_candidates if t["wilson"] >= 0.48 and t["win_pct"] >= 0.52 and t["wilson"] < 0.52]
    if t2:
        for t in t2:
            print(f"    {t['combo']:40s} {t['market']:15s}  n={t['n']:4d}  WR={t['win_pct']:.1%}  WL={t['wilson']:.3f}")
    else:
        print("    (none)")

    print()
    print("  TIER 3 (Win pct >= 0.50, Wilson >= 0.45 -- break-even zone, needs more data):")
    print("  " + "-" * 80)
    t3 = [t for t in tier_candidates if t["win_pct"] >= 0.50 and t["wilson"] >= 0.45 and t["wilson"] < 0.48]
    if t3:
        for t in t3:
            print(f"    {t['combo']:40s} {t['market']:15s}  n={t['n']:4d}  WR={t['win_pct']:.1%}  WL={t['wilson']:.3f}")
    else:
        print("    (none)")

    print()
    print("  AVOID (Wilson Lower < 0.45 or win_pct < 0.50):")
    print("  " + "-" * 80)
    avoid = [t for t in tier_candidates if t["wilson"] < 0.45 or t["win_pct"] < 0.50]
    if avoid:
        for t in avoid:
            print(f"    {t['combo']:40s} {t['market']:15s}  n={t['n']:4d}  WR={t['win_pct']:.1%}  WL={t['wilson']:.3f}")
    else:
        print("    (none)")

    # ─────────────────────────────────────────────────────────
    # 13. SOURCES COMBO (all markets combined)
    # ─────────────────────────────────────────────────────────
    by_combo_all = defaultdict(list)
    for r in joined:
        by_combo_all[r["sources_combo"]].append(r)

    rows = []
    for combo, recs in sorted(by_combo_all.items()):
        stats = aggregate(recs)
        stats["sources_combo"] = combo
        stats["src_count"] = combo.count("|") + 1
        rows.append(stats)

    print_table(rows,
        ["sources_combo", "src_count", "wins", "losses", "n", "win_pct", "wilson_lower"],
        "BY SOURCES COMBO (all markets combined)",
        min_n=10, sort_key="win_pct")

    # ─────────────────────────────────────────────────────────
    # 14. GAME-TYPE (spread/total/ML) COMBO DEEP DIVE
    # ─────────────────────────────────────────────────────────
    game_records = [r for r in joined if r["market_type"] in ("spread", "total", "moneyline")]
    print(f"\n  Game-level records (spread/total/ML): {len(game_records)}")

    by_combo_game = defaultdict(list)
    for r in game_records:
        by_combo_game[(r["sources_combo"], r["market_type"])].append(r)

    rows = []
    for (combo, mt), recs in sorted(by_combo_game.items()):
        stats = aggregate(recs)
        stats["sources_combo"] = combo
        stats["market_type"] = mt
        stats["src_count"] = combo.count("|") + 1
        rows.append(stats)

    print_table(rows,
        ["sources_combo", "market_type", "src_count", "wins", "losses", "n", "win_pct", "wilson_lower"],
        "GAME-LEVEL MARKETS: BY SOURCES COMBO x MARKET TYPE",
        min_n=5, sort_key="win_pct")

    # ─────────────────────────────────────────────────────────
    # 15. PLAYER PROPS: OVER vs UNDER by SOURCE COMBO
    # ─────────────────────────────────────────────────────────
    for dir_label in ["UNDER", "OVER"]:
        dir_records = [r for r in prop_records if r.get("direction") == dir_label]
        by_combo_dir = defaultdict(list)
        for r in dir_records:
            by_combo_dir[r["sources_combo"]].append(r)

        rows = []
        for combo, recs in sorted(by_combo_dir.items()):
            stats = aggregate(recs)
            stats["sources_combo"] = combo
            stats["src_count"] = combo.count("|") + 1
            rows.append(stats)

        print_table(rows,
            ["sources_combo", "src_count", "wins", "losses", "n", "win_pct", "wilson_lower"],
            f"PLAYER PROPS {dir_label}: BY SOURCE COMBO",
            min_n=5, sort_key="win_pct")

    # ─────────────────────────────────────────────────────────
    # 16. PLAYER PROPS: OVER vs UNDER by ATOMIC STAT
    # ─────────────────────────────────────────────────────────
    for dir_label in ["UNDER", "OVER"]:
        dir_records = [r for r in prop_records if r.get("direction") == dir_label]
        by_stat_dir = defaultdict(list)
        for r in dir_records:
            by_stat_dir[r["atomic_stat"]].append(r)

        rows = []
        for stat, recs in sorted(by_stat_dir.items()):
            stats = aggregate(recs)
            stats["atomic_stat"] = stat
            rows.append(stats)

        print_table(rows,
            ["atomic_stat", "wins", "losses", "n", "win_pct", "wilson_lower"],
            f"PLAYER PROPS {dir_label}: BY ATOMIC STAT",
            min_n=5, sort_key="win_pct")

    # ─────────────────────────────────────────────────────────
    # 17. CROSS-SOURCE (2+ sources) vs SINGLE SOURCE breakdown
    # ─────────────────────────────────────────────────────────
    print("\n" + "=" * 100)
    print("  CROSS-SOURCE (2+ sources) vs SINGLE SOURCE DETAILED BREAKDOWN")
    print("=" * 100)

    single = [r for r in joined if r["source_count"] == 1]
    multi = [r for r in joined if r["source_count"] >= 2]

    for label, subset in [("SINGLE SOURCE (1)", single), ("MULTI SOURCE (2+)", multi)]:
        stats = aggregate(subset)
        print(f"\n  {label}: n={stats['n']}, WR={stats['win_pct']}, Wilson={stats['wilson_lower']}")

        by_mt = defaultdict(list)
        for r in subset:
            by_mt[r["market_type"]].append(r)

        for mt in ["spread", "total", "moneyline", "player_prop"]:
            if mt in by_mt:
                s = aggregate(by_mt[mt])
                print(f"    {mt:15s}: n={s['n']:5d}, W={s['wins']:4d}, L={s['losses']:4d}, WR={s['win_pct']}, Wilson={s['wilson_lower']}")

    # ─────────────────────────────────────────────────────────
    # 18. TIME ANALYSIS: Win rate by month
    # ─────────────────────────────────────────────────────────
    by_month = defaultdict(list)
    for r in joined:
        dk = r.get("day_key", "")
        # day_key format: NBA:YYYY:MM:DD
        parts = dk.split(":")
        if len(parts) >= 3:
            month_key = f"{parts[1]}-{parts[2]}"
            by_month[month_key].append(r)

    rows = []
    for month, recs in sorted(by_month.items()):
        stats = aggregate(recs)
        stats["month"] = month
        rows.append(stats)

    print_table(rows,
        ["month", "wins", "losses", "n", "win_pct", "wilson_lower"],
        "WIN RATE BY MONTH (all markets)",
        min_n=1, sort_key="win_pct")

    print("\n" + "=" * 100)
    print("  ANALYSIS COMPLETE")
    print("=" * 100)


if __name__ == "__main__":
    main()
