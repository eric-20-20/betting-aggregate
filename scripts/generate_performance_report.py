#!/usr/bin/env python3
"""
Generate comprehensive expert performance report from Supabase.
Reads expert_records and expert_pair_records tables and builds a 7-section report.
"""
from __future__ import annotations

import sys
import os
from pathlib import Path
from collections import defaultdict

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.supabase_writer import get_client

OUTPUT_FILE = REPO_ROOT / "data" / "reports" / "performance_report_2026-03-23.txt"


# ─────────────────────────────────────────────────────────────
# Pagination helpers
# ─────────────────────────────────────────────────────────────

def fetch_all(table: str, page_size: int = 1000) -> list:
    client = get_client()
    rows = []
    offset = 0
    while True:
        resp = (
            client.table(table)
            .select("*")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = resp.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows


# ─────────────────────────────────────────────────────────────
# Aggregation helpers
# ─────────────────────────────────────────────────────────────

def agg_expert_overall(records: list) -> dict:
    """Aggregate expert_records across all markets per (source_id, expert_slug)."""
    by_expert: dict = defaultdict(lambda: {
        "wins": 0, "losses": 0, "markets": set(),
        "hot": False, "cold": False,
        "expert_name": "",
    })
    for row in records:
        key = (row["source_id"], row["expert_slug"])
        d = by_expert[key]
        d["wins"] += row.get("wins") or 0
        d["losses"] += row.get("losses") or 0
        if row.get("market_type"):
            d["markets"].add(row["market_type"])
        if row.get("hot"):
            d["hot"] = True
        if row.get("cold"):
            d["cold"] = True
        if row.get("expert_name") and not d["expert_name"]:
            d["expert_name"] = row["expert_name"]
    # compute win_pct
    result = {}
    for key, d in by_expert.items():
        n = d["wins"] + d["losses"]
        result[key] = {
            "source_id": key[0],
            "expert_slug": key[1],
            "expert_name": d["expert_name"] or key[1],
            "wins": d["wins"],
            "losses": d["losses"],
            "n": n,
            "win_pct": d["wins"] / n if n > 0 else 0.0,
            "markets": sorted(d["markets"]),
            "hot": d["hot"],
            "cold": d["cold"],
        }
    return result


def agg_expert_by_market(records: list) -> dict:
    """Aggregate expert_records per (source_id, expert_slug, market_type)."""
    by_key: dict = defaultdict(lambda: {"wins": 0, "losses": 0, "expert_name": "", "hot": False, "cold": False})
    for row in records:
        key = (row["source_id"], row["expert_slug"], row.get("market_type", ""))
        d = by_key[key]
        d["wins"] += row.get("wins") or 0
        d["losses"] += row.get("losses") or 0
        if row.get("expert_name") and not d["expert_name"]:
            d["expert_name"] = row["expert_name"]
        if row.get("hot"):
            d["hot"] = True
        if row.get("cold"):
            d["cold"] = True
    result: dict = defaultdict(list)
    for (src, slug, mkt), d in by_key.items():
        n = d["wins"] + d["losses"]
        result[mkt].append({
            "source_id": src,
            "expert_slug": slug,
            "expert_name": d["expert_name"] or slug,
            "wins": d["wins"],
            "losses": d["losses"],
            "n": n,
            "win_pct": d["wins"] / n if n > 0 else 0.0,
            "hot": d["hot"],
            "cold": d["cold"],
        })
    return dict(result)


def agg_expert_by_stat(records: list) -> dict:
    """Aggregate player_prop records per (source_id, expert_slug, atomic_stat)."""
    by_key: dict = defaultdict(lambda: {"wins": 0, "losses": 0, "expert_name": ""})
    for row in records:
        if row.get("market_type") != "player_prop":
            continue
        stat = row.get("atomic_stat") or "unknown"
        key = (row["source_id"], row["expert_slug"], stat)
        d = by_key[key]
        d["wins"] += row.get("wins") or 0
        d["losses"] += row.get("losses") or 0
        if row.get("expert_name") and not d["expert_name"]:
            d["expert_name"] = row["expert_name"]
    result: dict = defaultdict(list)
    for (src, slug, stat), d in by_key.items():
        n = d["wins"] + d["losses"]
        result[stat].append({
            "source_id": src,
            "expert_slug": slug,
            "expert_name": d["expert_name"] or slug,
            "wins": d["wins"],
            "losses": d["losses"],
            "n": n,
            "win_pct": d["wins"] / n if n > 0 else 0.0,
        })
    return dict(result)


def agg_pair_overall(pair_records: list) -> list:
    """Aggregate pair_records across all markets per (source_a, slug_a, source_b, slug_b)."""
    by_key: dict = defaultdict(lambda: {"wins": 0, "losses": 0, "markets": set()})
    for row in pair_records:
        key = (row["source_a"], row["expert_slug_a"], row["source_b"], row["expert_slug_b"])
        d = by_key[key]
        d["wins"] += row.get("wins") or 0
        d["losses"] += row.get("losses") or 0
        if row.get("market_type"):
            d["markets"].add(row["market_type"])
    result = []
    for (sa, sla, sb, slb), d in by_key.items():
        n = d["wins"] + d["losses"]
        win_pct = d["wins"] / n if n > 0 else 0.0
        if win_pct >= 0.60 and n >= 15:
            tier = "A"
        elif win_pct >= 0.55 and n >= 15:
            tier = "B"
        elif win_pct >= 0.50 and n >= 15:
            tier = "C"
        else:
            tier = "unrated"
        result.append({
            "source_a": sa, "expert_slug_a": sla,
            "source_b": sb, "expert_slug_b": slb,
            "wins": d["wins"], "losses": d["losses"], "n": n,
            "win_pct": win_pct,
            "tier": tier,
            "markets": sorted(d["markets"]),
        })
    return result


def agg_pair_by_market(pair_records: list) -> dict:
    """Group raw pair rows by market_type."""
    by_market: dict = defaultdict(list)
    for row in pair_records:
        mkt = row.get("market_type") or "unknown"
        by_market[mkt].append(row)
    return dict(by_market)


# ─────────────────────────────────────────────────────────────
# Formatting helpers
# ─────────────────────────────────────────────────────────────

def flag(hot: bool, cold: bool) -> str:
    if hot:
        return "🔥"
    if cold:
        return "🧊"
    return ""


def trunc(s: str, n: int) -> str:
    if len(s) <= n:
        return s
    return s[:n - 1] + "…"


def tier_label(tier: str) -> str:
    labels = {"A": "A-TIER", "B": "B-TIER", "C": "C-TIER", "unrated": "UNRATED"}
    return labels.get(tier, tier)


# ─────────────────────────────────────────────────────────────
# Report sections
# ─────────────────────────────────────────────────────────────

def build_section1(expert_overall: dict) -> str:
    lines = []
    lines.append("SECTION 1 — TOP SOLO EXPERTS OVERALL (min 20 picks)")
    lines.append("=" * 74)

    candidates = [v for v in expert_overall.values() if v["n"] >= 20]
    candidates.sort(key=lambda x: x["win_pct"], reverse=True)
    top20 = candidates[:20]

    header = f"{'Rank':>4}  {'Expert':<25}  {'Source':<20}  {'W':>4}  {'L':>4}  {'Win%':>6}  {'Markets':<22}  {'Flag'}"
    sep = f"{'----':>4}  {'-'*25}  {'-'*20}  {'---':>4}  {'---':>4}  {'-----':>6}  {'-'*22}  {'----'}"
    lines.append(header)
    lines.append(sep)

    for rank, e in enumerate(top20, 1):
        name = trunc(e["expert_name"] or e["expert_slug"], 25)
        src = trunc(e["source_id"], 20)
        markets_str = trunc(",".join(e["markets"]), 22)
        fl = flag(e["hot"], e["cold"])
        pct = f"{e['win_pct'] * 100:.1f}%"
        lines.append(
            f"{rank:>4}  {name:<25}  {src:<20}  {e['wins']:>4}  {e['losses']:>4}  {pct:>6}  {markets_str:<22}  {fl}"
        )

    lines.append("")
    return "\n".join(lines)


def build_section2(expert_by_market: dict) -> str:
    lines = []
    lines.append("SECTION 2 — TOP SOLO EXPERTS BY MARKET")
    lines.append("=" * 74)

    for mkt in ["player_prop", "spread", "total", "moneyline"]:
        rows = expert_by_market.get(mkt, [])
        lines.append(f"\n--- {mkt} (min 15 picks) ---")
        candidates = [r for r in rows if r["n"] >= 15]
        candidates.sort(key=lambda x: x["win_pct"], reverse=True)
        top10 = candidates[:10]
        if not top10:
            lines.append("  (no data)")
            continue
        header = f"  {'Rank':>4}  {'Expert':<25}  {'Source':<20}  {'W':>4}  {'L':>4}  {'Win%':>6}  {'Flag'}"
        sep = f"  {'----':>4}  {'-'*25}  {'-'*20}  {'---':>4}  {'---':>4}  {'-----':>6}  {'----'}"
        lines.append(header)
        lines.append(sep)
        for rank, e in enumerate(top10, 1):
            name = trunc(e["expert_name"] or e["expert_slug"], 25)
            src = trunc(e["source_id"], 20)
            fl = flag(e["hot"], e["cold"])
            pct = f"{e['win_pct'] * 100:.1f}%"
            lines.append(
                f"  {rank:>4}  {name:<25}  {src:<20}  {e['wins']:>4}  {e['losses']:>4}  {pct:>6}  {fl}"
            )

    lines.append("")
    return "\n".join(lines)


def build_section3(expert_by_stat: dict) -> str:
    lines = []
    lines.append("SECTION 3 — TOP SOLO EXPERTS BY STAT TYPE (PROPS ONLY, min 10 picks)")
    lines.append("=" * 74)

    priority_stats = ["points", "rebounds", "assists", "threes", "blocks", "steals"]
    all_stats = sorted(expert_by_stat.keys())
    # priority stats first, then combo stats, then the rest
    combo_stats = [s for s in all_stats if "_" in s and s not in priority_stats]
    other_stats = [s for s in all_stats if s not in priority_stats and s not in combo_stats]
    stat_order = priority_stats + combo_stats + other_stats

    for stat in stat_order:
        if stat not in expert_by_stat:
            continue
        rows = expert_by_stat[stat]
        candidates = [r for r in rows if r["n"] >= 10]
        if not candidates:
            continue
        candidates.sort(key=lambda x: x["win_pct"], reverse=True)
        top5 = candidates[:5]
        lines.append(f"\n--- {stat} ---")
        header = f"  {'Rank':>4}  {'Expert':<25}  {'Source':<20}  {'W':>4}  {'L':>4}  {'Win%':>6}"
        sep = f"  {'----':>4}  {'-'*25}  {'-'*20}  {'---':>4}  {'---':>4}  {'-----':>6}"
        lines.append(header)
        lines.append(sep)
        for rank, e in enumerate(top5, 1):
            name = trunc(e["expert_name"] or e["expert_slug"], 25)
            src = trunc(e["source_id"], 20)
            pct = f"{e['win_pct'] * 100:.1f}%"
            lines.append(
                f"  {rank:>4}  {name:<25}  {src:<20}  {e['wins']:>4}  {e['losses']:>4}  {pct:>6}"
            )

    lines.append("")
    return "\n".join(lines)


def build_section4(pair_overall: list, expert_overall: dict) -> str:
    lines = []
    lines.append("SECTION 4 — TOP EXPERT PAIRS OVERALL (min 15 agreements)")
    lines.append("=" * 74)

    # Build lookup for expert hot/cold
    def expert_flag(sa, sla, sb, slb):
        ea = expert_overall.get((sa, sla), {})
        eb = expert_overall.get((sb, slb), {})
        h = ea.get("hot", False) or eb.get("hot", False)
        c = ea.get("cold", False) or eb.get("cold", False)
        return flag(h, c)

    qualified = [p for p in pair_overall if p["n"] >= 15]
    qualified.sort(key=lambda x: x["win_pct"], reverse=True)

    for tier_key, tier_min, tier_max, show_max in [
        ("A", 0.60, 1.01, None),
        ("B", 0.55, 0.60, None),
        ("C", 0.50, 0.55, 10),
    ]:
        tier_rows = [p for p in qualified if tier_min <= p["win_pct"] < tier_max]
        if tier_key == "A":
            lines.append(f"\n--- A-TIER (win_pct >= 60%) ---")
        elif tier_key == "B":
            lines.append(f"\n--- B-TIER (55-59%) ---")
        else:
            lines.append(f"\n--- C-TIER top 10 (50-54%) ---")

        display_rows = tier_rows[:show_max] if show_max else tier_rows

        if not display_rows:
            lines.append("  (none)")
            continue

        header = f"  {'Rank':>4}  {'Expert A':<22}  {'Expert B':<22}  {'W':>4}  {'L':>4}  {'Win%':>6}  {'Tier':>4}  {'Markets'}"
        sep = f"  {'----':>4}  {'-'*22}  {'-'*22}  {'---':>4}  {'---':>4}  {'-----':>6}  {'----':>4}  {'-------'}"
        lines.append(header)
        lines.append(sep)
        for rank, p in enumerate(display_rows, 1):
            ea_label = trunc(p["expert_slug_a"], 22)
            eb_label = trunc(p["expert_slug_b"], 22)
            fl = expert_flag(p["source_a"], p["expert_slug_a"], p["source_b"], p["expert_slug_b"])
            pct = f"{p['win_pct'] * 100:.1f}%"
            markets_str = trunc(",".join(p["markets"]), 30)
            lines.append(
                f"  {rank:>4}  {ea_label:<22}  {eb_label:<22}  {p['wins']:>4}  {p['losses']:>4}  {pct:>6}  {p['tier']:>4}  {markets_str} {fl}"
            )

    lines.append("")
    return "\n".join(lines)


def build_section5(pair_by_market: dict) -> str:
    lines = []
    lines.append("SECTION 5 — TOP EXPERT PAIRS BY MARKET (min 15 agreements per market)")
    lines.append("=" * 74)

    for mkt in ["player_prop", "spread", "total", "moneyline"]:
        rows = pair_by_market.get(mkt, [])
        lines.append(f"\n--- {mkt} ---")

        candidates = [r for r in rows if (r.get("wins", 0) + r.get("losses", 0)) >= 15]
        candidates.sort(key=lambda x: (x.get("wins", 0) / max(x.get("wins", 0) + x.get("losses", 0), 1)), reverse=True)
        top10 = candidates[:10]

        if not top10:
            lines.append("  (no data)")
            continue

        header = f"  {'Rank':>4}  {'Expert A':<22}  {'Expert B':<22}  {'W':>4}  {'L':>4}  {'Win%':>6}  {'Tier':>4}"
        sep = f"  {'----':>4}  {'-'*22}  {'-'*22}  {'---':>4}  {'---':>4}  {'-----':>6}  {'----':>4}"
        lines.append(header)
        lines.append(sep)
        for rank, p in enumerate(top10, 1):
            n = p.get("wins", 0) + p.get("losses", 0)
            win_pct = p.get("wins", 0) / n if n > 0 else 0.0
            tier = p.get("tier") or "?"
            ea = trunc(p.get("expert_slug_a", ""), 22)
            eb = trunc(p.get("expert_slug_b", ""), 22)
            pct = f"{win_pct * 100:.1f}%"
            lines.append(
                f"  {rank:>4}  {ea:<22}  {eb:<22}  {p.get('wins', 0):>4}  {p.get('losses', 0):>4}  {pct:>6}  {tier:>4}"
            )

    lines.append("")
    return "\n".join(lines)


def build_section6(expert_overall: dict, pair_overall: list) -> str:
    lines = []
    lines.append("SECTION 6 — EXPERT PAIR MATRIX (top 10 experts by total picks)")
    lines.append("=" * 74)
    lines.append("(pair win% when both experts agree, -- if n<5)")
    lines.append("")

    # Top 10 experts by total picks
    sorted_experts = sorted(expert_overall.values(), key=lambda x: x["n"], reverse=True)[:10]

    # Build pair lookup: (slug_a, slug_b) → aggregated
    pair_lookup: dict = {}
    for p in pair_overall:
        ka = (p["source_a"], p["expert_slug_a"])
        kb = (p["source_b"], p["expert_slug_b"])
        pair_lookup[(ka, kb)] = p
        pair_lookup[(kb, ka)] = p  # symmetric

    # Column headers (truncated to 10 chars for compactness)
    col_width = 10
    gap = 2
    label_width = 14

    expert_keys = [(e["source_id"], e["expert_slug"]) for e in sorted_experts]
    expert_labels = [trunc(e["expert_name"] or e["expert_slug"], col_width) for e in sorted_experts]
    row_labels = [trunc(e["expert_name"] or e["expert_slug"], label_width) for e in sorted_experts]

    # Header row
    header_parts = [f"{'':>{label_width}}"]
    for lbl in expert_labels:
        header_parts.append(f"  {lbl:>{col_width}}")
    lines.append("".join(header_parts))

    # Separator
    sep_parts = ["-" * label_width]
    for _ in expert_labels:
        sep_parts.append("  " + "-" * col_width)
    lines.append("".join(sep_parts))

    # Data rows
    for i, (ki, row_lbl) in enumerate(zip(expert_keys, row_labels)):
        row_parts = [f"{row_lbl:>{label_width}}"]
        for j, kj in enumerate(expert_keys):
            if i == j:
                cell = "--"
            else:
                p = pair_lookup.get((ki, kj))
                if p and p["n"] >= 5:
                    cell = f"{int(round(p['win_pct'] * 100))}%"
                else:
                    cell = "--"
            row_parts.append(f"  {cell:>{col_width}}")
        lines.append("".join(row_parts))

    lines.append("")
    return "\n".join(lines)


def build_section7(expert_records: list, expert_overall: dict, pair_overall: list) -> str:
    lines = []
    lines.append("SECTION 7 — SUMMARY STATS")
    lines.append("=" * 74)

    # Total raw expert-pick occurrences
    total_raw = sum((r.get("wins") or 0) + (r.get("losses") or 0) for r in expert_records)

    # Unique experts
    unique_experts = len(expert_overall)

    # Pairs with 15+ agreements
    qualified_pairs = [p for p in pair_overall if p["n"] >= 15]
    n_qualified_pairs = len(qualified_pairs)

    # Tier counts
    tier_counts = defaultdict(int)
    for p in qualified_pairs:
        tier_counts[p["tier"]] += 1

    # Most prolific expert
    prolific = max(expert_overall.values(), key=lambda x: x["n"]) if expert_overall else None

    # Most accurate (min 20 picks)
    accurate_candidates = [v for v in expert_overall.values() if v["n"] >= 20]
    most_accurate = max(accurate_candidates, key=lambda x: x["win_pct"]) if accurate_candidates else None

    # Best pair (min 15)
    best_pair = max(qualified_pairs, key=lambda x: x["win_pct"]) if qualified_pairs else None

    lines.append(f"  Data range:                    Oct 2025 – Mar 2026 (Season 2025-26)")
    lines.append(f"  Total expert-pick occurrences: {total_raw:,}")
    lines.append(f"  Unique experts:                {unique_experts:,}")
    lines.append(f"  Pairs with 15+ agreements:     {n_qualified_pairs:,}")
    lines.append(f"    A-tier pairs:                {tier_counts.get('A', 0):,}")
    lines.append(f"    B-tier pairs:                {tier_counts.get('B', 0):,}")
    lines.append(f"    C-tier pairs:                {tier_counts.get('C', 0):,}")
    lines.append(f"    Unrated pairs (n>=15, <50%): {tier_counts.get('unrated', 0):,}")

    if prolific:
        lines.append(
            f"\n  Most prolific expert:          {prolific['expert_name'] or prolific['expert_slug']} "
            f"({prolific['source_id']})  {prolific['n']} picks ({prolific['wins']}W-{prolific['losses']}L)"
        )
    if most_accurate:
        lines.append(
            f"  Most accurate expert:          {most_accurate['expert_name'] or most_accurate['expert_slug']} "
            f"({most_accurate['source_id']})  {most_accurate['wins']}W-{most_accurate['losses']}L  "
            f"{most_accurate['win_pct']*100:.1f}%"
        )
    if best_pair:
        lines.append(
            f"  Best pair overall:             {best_pair['expert_slug_a']} ({best_pair['source_a']}) + "
            f"{best_pair['expert_slug_b']} ({best_pair['source_b']})  "
            f"{best_pair['wins']}W-{best_pair['losses']}L  {best_pair['win_pct']*100:.1f}%  "
            f"({best_pair['tier']}-tier)"
        )

    lines.append("")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main():
    print("Fetching expert_records from Supabase...", flush=True)
    expert_records = fetch_all("expert_records")
    print(f"  -> {len(expert_records):,} rows", flush=True)

    print("Fetching expert_pair_records from Supabase...", flush=True)
    pair_records = fetch_all("expert_pair_records")
    print(f"  -> {len(pair_records):,} rows", flush=True)

    print("Aggregating...", flush=True)
    expert_overall = agg_expert_overall(expert_records)
    expert_by_market = agg_expert_by_market(expert_records)
    expert_by_stat = agg_expert_by_stat(expert_records)
    pair_overall = agg_pair_overall(pair_records)
    pair_by_market = agg_pair_by_market(pair_records)

    print("Building report...", flush=True)

    report_parts = []
    report_parts.append("=" * 74)
    report_parts.append("EXPERT PERFORMANCE REPORT — 2026-03-23")
    report_parts.append("=" * 74)
    report_parts.append("")

    report_parts.append(build_section1(expert_overall))
    report_parts.append(build_section2(expert_by_market))
    report_parts.append(build_section3(expert_by_stat))
    report_parts.append(build_section4(pair_overall, expert_overall))
    report_parts.append(build_section5(pair_by_market))
    report_parts.append(build_section6(expert_overall, pair_overall))
    report_parts.append(build_section7(expert_records, expert_overall, pair_overall))

    full_report = "\n".join(report_parts)

    # Print to stdout
    print("\n" + full_report)

    # Write to file
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(full_report, encoding="utf-8")
    print(f"\n[Saved to {OUTPUT_FILE}]", flush=True)


if __name__ == "__main__":
    main()
