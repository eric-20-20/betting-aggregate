#!/usr/bin/env python3
"""
Comprehensive trend analysis on graded_occurrences_latest.jsonl
"""
import json
import math
import sys
from collections import defaultdict
from pathlib import Path

DATA_PATH = Path("/Users/Ericevans/Betting Aggregate/data/analysis/graded_occurrences_latest.jsonl")

def load_data():
    records = []
    with open(DATA_PATH) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records

def graded(r):
    return r.get("result") in ("WIN", "LOSS", "PUSH")

def is_win(r):
    return r.get("result") == "WIN"

def is_loss(r):
    return r.get("result") == "LOSS"

def wl_stats(records):
    """Returns (wins, losses, pushes, win_rate) ignoring pushes in rate."""
    w = sum(1 for r in records if is_win(r))
    l = sum(1 for r in records if is_loss(r))
    p = sum(1 for r in records if r.get("result") == "PUSH")
    denom = w + l
    rate = w / denom if denom > 0 else None
    return w, l, p, rate

def fmt_rate(rate):
    if rate is None:
        return "  N/A  "
    return f"{rate*100:5.1f}%"

def quality_score(win_rate, n):
    """win_rate * sqrt(n) — balances performance with sample size."""
    if win_rate is None or n == 0:
        return 0
    return win_rate * math.sqrt(n)

def print_sep(char="=", width=90):
    print(char * width)

def print_table(headers, rows, min_widths=None):
    """Print a formatted table."""
    col_widths = [len(h) for h in headers]
    if min_widths:
        col_widths = [max(c, m) for c, m in zip(col_widths, min_widths)]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))
    fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
    print(fmt.format(*headers))
    print("  ".join("-" * w for w in col_widths))
    for row in rows:
        print(fmt.format(*[str(c) for c in row]))

# ─────────────────────────────────────────────────────────────
def analysis_1_source_combo_win_rates(records):
    print_sep()
    print("ANALYSIS 1: SOURCE COMBO WIN RATES (≥20 graded records)")
    print_sep()

    graded_records = [r for r in records if graded(r)]
    combo_buckets = defaultdict(list)
    for r in graded_records:
        combo_buckets[r.get("signal_sources_combo", "")].append(r)

    rows = []
    for combo, recs in combo_buckets.items():
        w, l, p, rate = wl_stats(recs)
        n = w + l
        if n < 20:
            continue
        jreel = "juicereel_sxebets" in combo or "juicereel_nukethebooks" in combo
        flag = " [JUICEREEL]" if jreel else ""
        rows.append((combo + flag, w, l, p, n, rate))

    rows.sort(key=lambda x: x[5] if x[5] is not None else 0, reverse=True)

    headers = ["Signal Sources Combo", "W", "L", "P", "N", "Win%"]
    table_rows = [(c, w, l, p, n, fmt_rate(r)) for c, w, l, p, n, r in rows]
    print_table(headers, table_rows)
    print(f"\nTotal combos with ≥20 graded: {len(rows)}")

# ─────────────────────────────────────────────────────────────
def analysis_2_juicereel_combos(records):
    print_sep()
    print("ANALYSIS 2: JUICEREEL-CONTAINING COMBOS (all sample sizes)")
    print_sep()

    graded_records = [r for r in records if graded(r)]
    combo_buckets = defaultdict(list)
    for r in graded_records:
        combo = r.get("signal_sources_combo", "")
        if "juicereel_sxebets" in combo or "juicereel_nukethebooks" in combo:
            combo_buckets[combo].append(r)

    rows = []
    for combo, recs in combo_buckets.items():
        w, l, p, rate = wl_stats(recs)
        n = w + l
        rows.append((combo, w, l, p, n, rate))

    rows.sort(key=lambda x: x[5] if x[5] is not None else 0, reverse=True)

    headers = ["Signal Sources Combo", "W", "L", "P", "N", "Win%"]
    table_rows = [(c, w, l, p, n, fmt_rate(r)) for c, w, l, p, n, r in rows]
    print_table(headers, table_rows)
    print(f"\nTotal juicereel combos: {len(rows)}")

    # Also show by market_type breakdown for juicereel combos
    print("\n--- JuiceReel combos broken down by market_type ---")
    market_buckets = defaultdict(list)
    for r in graded_records:
        combo = r.get("signal_sources_combo", "")
        if "juicereel_sxebets" in combo or "juicereel_nukethebooks" in combo:
            market_buckets[r.get("market_type", "unknown")].append(r)

    rows2 = []
    for mkt, recs in sorted(market_buckets.items()):
        w, l, p, rate = wl_stats(recs)
        n = w + l
        rows2.append((mkt, w, l, p, n, rate))
    rows2.sort(key=lambda x: x[5] if x[5] is not None else 0, reverse=True)
    headers = ["Market Type", "W", "L", "P", "N", "Win%"]
    table_rows = [(c, w, l, p, n, fmt_rate(r)) for c, w, l, p, n, r in rows2]
    print_table(headers, table_rows)

# ─────────────────────────────────────────────────────────────
def analysis_3_two_source_combos(records):
    print_sep()
    print("ANALYSIS 3: 2-SOURCE COMBO WIN RATES (≥10 graded records)")
    print_sep()

    graded_records = [r for r in records if graded(r)]
    combo_buckets = defaultdict(list)
    for r in graded_records:
        combo = r.get("signal_sources_combo", "")
        sources = r.get("signal_sources_present", [])
        if len(sources) == 2:
            combo_buckets[combo].append(r)

    rows = []
    for combo, recs in combo_buckets.items():
        w, l, p, rate = wl_stats(recs)
        n = w + l
        if n < 10:
            continue
        rows.append((combo, w, l, p, n, rate))

    rows.sort(key=lambda x: x[5] if x[5] is not None else 0, reverse=True)

    headers = ["2-Source Combo", "W", "L", "P", "N", "Win%"]
    table_rows = [(c, w, l, p, n, fmt_rate(r)) for c, w, l, p, n, r in rows]
    print_table(headers, table_rows)
    print(f"\nTotal 2-source combos with ≥10 graded: {len(rows)}")

# ─────────────────────────────────────────────────────────────
def analysis_4_standalone(records):
    print_sep()
    print("ANALYSIS 4: STANDALONE JUICEREEL SOURCES (solo only)")
    print_sep()

    for source_name, label in [
        ("juicereel_nukethebooks", "juicereel_nukethebooks (solo)"),
        ("juicereel_sxebets", "juicereel_sxebets (solo)"),
    ]:
        print(f"\n--- {label} ---")
        solo = [r for r in records if graded(r)
                and r.get("signal_sources_combo", "") == source_name]
        if not solo:
            print(f"  No graded records for solo {source_name}")
            continue

        market_buckets = defaultdict(list)
        for r in solo:
            market_buckets[r.get("market_type", "unknown")].append(r)

        rows = []
        for mkt, recs in sorted(market_buckets.items()):
            w, l, p, rate = wl_stats(recs)
            n = w + l
            rows.append((mkt, w, l, p, n, rate))
        rows.sort(key=lambda x: x[5] if x[5] is not None else 0, reverse=True)

        total = [r for r in solo if graded(r)]
        tw, tl, tp, trate = wl_stats(total)
        tn = tw + tl

        headers = ["Market Type", "W", "L", "P", "N", "Win%"]
        table_rows = [(c, w, l, p, n, fmt_rate(r)) for c, w, l, p, n, r in rows]
        print_table(headers, table_rows)
        print(f"  TOTAL: {tw}W-{tl}L-{tp}P (N={tn}, {fmt_rate(trate)})")

# ─────────────────────────────────────────────────────────────
def analysis_5_market_source_count(records):
    print_sep()
    print("ANALYSIS 5: MARKET TYPE × SOURCE COUNT CROSS-TAB")
    print_sep()

    graded_records = [r for r in records if graded(r)]
    buckets = defaultdict(list)
    for r in graded_records:
        mkt = r.get("market_type", "unknown")
        n_sources = len(r.get("signal_sources_present", []))
        buckets[(mkt, n_sources)].append(r)

    # Get unique markets and source counts
    markets = sorted(set(k[0] for k in buckets))
    source_counts = sorted(set(k[1] for k in buckets))

    rows = []
    for mkt in markets:
        for sc in source_counts:
            recs = buckets.get((mkt, sc), [])
            if not recs:
                continue
            w, l, p, rate = wl_stats(recs)
            n = w + l
            if n < 5:
                continue
            rows.append((mkt, sc, w, l, p, n, rate))

    rows.sort(key=lambda x: (x[0], x[1]))
    headers = ["Market Type", "Sources", "W", "L", "P", "N", "Win%"]
    table_rows = [(m, s, w, l, p, n, fmt_rate(r)) for m, s, w, l, p, n, r in rows]
    print_table(headers, table_rows)

    # Summary: for each market, does more sources = better?
    print("\n--- Summary: Market Win Rate by Source Count ≥ 3 ---")
    for mkt in markets:
        print(f"\n  {mkt}:")
        for sc in source_counts:
            recs = buckets.get((mkt, sc), [])
            w, l, p, rate = wl_stats(recs)
            n = w + l
            if n >= 5:
                print(f"    {sc} sources: {w}W-{l}L {fmt_rate(rate)} (N={n})")

# ─────────────────────────────────────────────────────────────
def analysis_6_direction(records):
    print_sep()
    print("ANALYSIS 6: DIRECTION ANALYSIS (OVER vs UNDER bias)")
    print_sep()

    graded_records = [r for r in records if graded(r)]

    for mkt_filter, label in [("player_prop", "PLAYER PROPS"), ("total", "GAME TOTALS")]:
        print(f"\n--- {label} ---")
        mkt_recs = [r for r in graded_records if r.get("market_type") == mkt_filter]
        buckets = defaultdict(list)
        for r in mkt_recs:
            d = r.get("direction", "unknown")
            buckets[d].append(r)

        rows = []
        for direction, recs in sorted(buckets.items()):
            w, l, p, rate = wl_stats(recs)
            n = w + l
            rows.append((direction, w, l, p, n, rate))
        rows.sort(key=lambda x: x[5] if x[5] is not None else 0, reverse=True)

        headers = ["Direction", "W", "L", "P", "N", "Win%"]
        table_rows = [(d, w, l, p, n, fmt_rate(r)) for d, w, l, p, n, r in rows]
        print_table(headers, table_rows)

    # For spreads: which teams/directions are covered
    print("\n--- SPREADS: Direction analysis (by direction = team abbr) ---")
    spread_recs = [r for r in graded_records if r.get("market_type") == "spread"]
    print(f"  Total graded spread occurrences: {len(spread_recs)}")
    buckets = defaultdict(list)
    for r in spread_recs:
        d = r.get("direction", "unknown")
        buckets[d].append(r)
    rows = []
    for direction, recs in sorted(buckets.items(), key=lambda x: x[0] or ""):
        w, l, p, rate = wl_stats(recs)
        n = w + l
        if n >= 5:
            rows.append((direction, w, l, p, n, rate))
    rows.sort(key=lambda x: x[5] if x[5] is not None else 0, reverse=True)
    if rows:
        headers = ["Team (Direction)", "W", "L", "P", "N", "Win%"]
        table_rows = [(d, w, l, p, n, fmt_rate(r)) for d, w, l, p, n, r in rows]
        print_table(headers, table_rows)

    # Overall OVER vs UNDER (props + totals combined)
    print("\n--- OVERALL: OVER vs UNDER (props + totals) ---")
    ou_recs = [r for r in graded_records
               if r.get("market_type") in ("player_prop", "total")]
    buckets = defaultdict(list)
    for r in ou_recs:
        d = r.get("direction", "unknown")
        buckets[d].append(r)
    rows = []
    for direction in ["OVER", "UNDER"]:
        recs = buckets.get(direction, [])
        w, l, p, rate = wl_stats(recs)
        n = w + l
        rows.append((direction, w, l, p, n, rate))
    headers = ["Direction", "W", "L", "P", "N", "Win%"]
    table_rows = [(d, w, l, p, n, fmt_rate(r)) for d, w, l, p, n, r in rows]
    print_table(headers, table_rows)

# ─────────────────────────────────────────────────────────────
def analysis_7_line_value(records):
    print_sep()
    print("ANALYSIS 7: LINE VALUE ANALYSIS (player props by line range)")
    print_sep()

    graded_records = [r for r in records if graded(r)]
    prop_recs = [r for r in graded_records if r.get("market_type") == "player_prop"]

    bins = [
        ("0-4.5", 0, 4.5),
        ("5-9.5", 5, 9.5),
        ("10-14.5", 10, 14.5),
        ("15-19.5", 15, 19.5),
        ("20-24.5", 20, 24.5),
        ("25-29.5", 25, 29.5),
        ("30+", 30, 9999),
    ]

    print("\n--- All props by line range ---")
    rows = []
    for label, lo, hi in bins:
        recs = [r for r in prop_recs
                if r.get("line") is not None and lo <= r["line"] <= hi]
        w, l, p, rate = wl_stats(recs)
        n = w + l
        if n > 0:
            rows.append((label, w, l, p, n, rate))
    headers = ["Line Range", "W", "L", "P", "N", "Win%"]
    table_rows = [(lb, w, l, p, n, fmt_rate(r)) for lb, w, l, p, n, r in rows]
    print_table(headers, table_rows)

    # By stat type within line ranges
    print("\n--- Props by stat type and line range ---")
    stat_types = sorted(set(r.get("atomic_stat", "unknown") for r in prop_recs
                           if r.get("atomic_stat")))
    for stat in stat_types:
        stat_recs = [r for r in prop_recs if r.get("atomic_stat") == stat]
        stat_rows = []
        for label, lo, hi in bins:
            recs = [r for r in stat_recs
                    if r.get("line") is not None and lo <= r["line"] <= hi]
            w, l, p, rate = wl_stats(recs)
            n = w + l
            if n >= 5:
                stat_rows.append((label, w, l, p, n, rate))
        if stat_rows:
            print(f"\n  {stat}:")
            for lb, w, l, p, n, r in stat_rows:
                print(f"    {lb}: {w}W-{l}L {fmt_rate(r)} (N={n})")

    # Also: game totals line ranges
    print("\n--- Game totals by line range ---")
    total_recs = [r for r in graded_records if r.get("market_type") == "total"]
    total_bins = [
        ("<215", 0, 214.9),
        ("215-219.5", 215, 219.5),
        ("220-224.5", 220, 224.5),
        ("225-229.5", 225, 229.5),
        ("230+", 230, 9999),
    ]
    rows = []
    for label, lo, hi in total_bins:
        recs = [r for r in total_recs
                if r.get("line") is not None and lo <= r["line"] <= hi]
        w, l, p, rate = wl_stats(recs)
        n = w + l
        if n >= 5:
            rows.append((label, w, l, p, n, rate))
    headers = ["Total Range", "W", "L", "P", "N", "Win%"]
    table_rows = [(lb, w, l, p, n, fmt_rate(r)) for lb, w, l, p, n, r in rows]
    print_table(headers, table_rows)

# ─────────────────────────────────────────────────────────────
def analysis_8_recent_vs_historical(records):
    print_sep()
    print("ANALYSIS 8: WIN RATE TREND BY MONTH")
    print_sep()

    graded_records = [r for r in records if graded(r)]

    def extract_month(day_key):
        # day_key format: "NBA:2026:01:15"
        parts = day_key.split(":")
        if len(parts) >= 3:
            return f"{parts[1]}-{parts[2]}"
        return "unknown"

    month_buckets = defaultdict(list)
    for r in graded_records:
        month = extract_month(r.get("day_key", ""))
        month_buckets[month].append(r)

    rows = []
    for month in sorted(month_buckets.keys()):
        recs = month_buckets[month]
        w, l, p, rate = wl_stats(recs)
        n = w + l
        rows.append((month, w, l, p, n, rate))

    headers = ["Month", "W", "L", "P", "N", "Win%"]
    table_rows = [(m, w, l, p, n, fmt_rate(r)) for m, w, l, p, n, r in rows]
    print_table(headers, table_rows)

    # Juicereel-specific months
    print("\n--- JuiceReel source monthly trend ---")
    jreel_records = [r for r in graded_records
                     if "juicereel" in r.get("signal_sources_combo", "")]
    if jreel_records:
        month_buckets2 = defaultdict(list)
        for r in jreel_records:
            month = extract_month(r.get("day_key", ""))
            month_buckets2[month].append(r)
        rows2 = []
        for month in sorted(month_buckets2.keys()):
            recs = month_buckets2[month]
            w, l, p, rate = wl_stats(recs)
            n = w + l
            rows2.append((month, w, l, p, n, rate))
        headers = ["Month", "W", "L", "P", "N", "Win%"]
        table_rows = [(m, w, l, p, n, fmt_rate(r)) for m, w, l, p, n, r in rows2]
        print_table(headers, table_rows)

        # Show earliest juicereel date
        jreel_days = sorted(set(r.get("day_key", "") for r in jreel_records))
        print(f"\n  JuiceReel earliest date: {jreel_days[0] if jreel_days else 'N/A'}")
        print(f"  JuiceReel latest date:   {jreel_days[-1] if jreel_days else 'N/A'}")
        print(f"  JuiceReel total dates:   {len(jreel_days)}")
    else:
        print("  No juicereel records found in graded data.")

    # By market type + month
    print("\n--- Monthly trend by market type ---")
    for mkt in ["player_prop", "total", "spread", "moneyline"]:
        mkt_recs = [r for r in graded_records if r.get("market_type") == mkt]
        if not mkt_recs:
            continue
        month_buckets3 = defaultdict(list)
        for r in mkt_recs:
            month = extract_month(r.get("day_key", ""))
            month_buckets3[month].append(r)
        print(f"\n  {mkt}:")
        for month in sorted(month_buckets3.keys()):
            recs = month_buckets3[month]
            w, l, p, rate = wl_stats(recs)
            n = w + l
            if n >= 5:
                print(f"    {month}: {w}W-{l}L {fmt_rate(rate)} (N={n})")

# ─────────────────────────────────────────────────────────────
def analysis_9_top_combos_quality(records):
    print_sep()
    print("ANALYSIS 9: TOP 20 COMBOS BY QUALITY SCORE (win_rate × sqrt(N))")
    print_sep()

    graded_records = [r for r in records if graded(r)]
    combo_buckets = defaultdict(list)
    for r in graded_records:
        combo_buckets[r.get("signal_sources_combo", "")].append(r)

    rows = []
    for combo, recs in combo_buckets.items():
        w, l, p, rate = wl_stats(recs)
        n = w + l
        if n < 5:
            continue
        qs = quality_score(rate, n)
        rows.append((combo, w, l, p, n, rate, qs))

    rows.sort(key=lambda x: x[6], reverse=True)

    print(f"\nTop 20 (quality = win_rate × sqrt(N)):\n")
    headers = ["Signal Sources Combo", "W", "L", "P", "N", "Win%", "Quality"]
    top_rows = rows[:20]
    table_rows = [(c, w, l, p, n, fmt_rate(r), f"{q:.2f}")
                  for c, w, l, p, n, r, q in top_rows]
    print_table(headers, table_rows)

    # Also top 20 excluding 1-source
    print(f"\nTop 20 multi-source only (2+ sources):\n")
    multi = [r for r in rows if len(r[0].split("|")) >= 2][:20]
    table_rows = [(c, w, l, p, n, fmt_rate(r), f"{q:.2f}")
                  for c, w, l, p, n, r, q in multi]
    print_table(headers, table_rows)

# ─────────────────────────────────────────────────────────────
def analysis_10_source_participation(records):
    print_sep()
    print("ANALYSIS 10: SOURCE PARTICIPATION WIN RATES")
    print_sep()

    graded_records = [r for r in records if graded(r)]
    source_buckets = defaultdict(list)
    for r in graded_records:
        source = r.get("occ_source_id", "")
        if source:
            source_buckets[source].append(r)

    rows = []
    for source, recs in source_buckets.items():
        w, l, p, rate = wl_stats(recs)
        n = w + l
        rows.append((source, w, l, p, n, rate))

    rows.sort(key=lambda x: x[5] if x[5] is not None else 0, reverse=True)

    headers = ["Source", "W", "L", "P", "N", "Win%"]
    table_rows = [(s, w, l, p, n, fmt_rate(r)) for s, w, l, p, n, r in rows]
    print_table(headers, table_rows)

    # Per-source breakdown by market type
    print("\n--- Source win rates by market type ---")
    for source, *_ in rows:
        src_recs = [r for r in graded_records if r.get("occ_source_id") == source]
        mkt_buckets = defaultdict(list)
        for r in src_recs:
            mkt_buckets[r.get("market_type", "unknown")].append(r)
        mkt_rows = []
        for mkt, recs in sorted(mkt_buckets.items()):
            w, l, p, rate = wl_stats(recs)
            n = w + l
            if n >= 5:
                mkt_rows.append(f"    {mkt}: {w}W-{l}L {fmt_rate(rate)} (N={n})")
        if mkt_rows:
            print(f"\n  {source}:")
            for row in mkt_rows:
                print(row)

# ─────────────────────────────────────────────────────────────
def analysis_bonus_atomic_stat(records):
    print_sep()
    print("BONUS: PLAYER PROP WIN RATES BY ATOMIC STAT TYPE")
    print_sep()

    graded_records = [r for r in records if graded(r) and r.get("market_type") == "player_prop"]
    stat_buckets = defaultdict(list)
    for r in graded_records:
        stat_buckets[r.get("atomic_stat", "unknown")].append(r)

    rows = []
    for stat, recs in stat_buckets.items():
        w, l, p, rate = wl_stats(recs)
        n = w + l
        rows.append((stat, w, l, p, n, rate))
    rows.sort(key=lambda x: x[5] if x[5] is not None else 0, reverse=True)

    headers = ["Stat Type", "W", "L", "P", "N", "Win%"]
    table_rows = [(s, w, l, p, n, fmt_rate(r)) for s, w, l, p, n, r in rows]
    print_table(headers, table_rows)

# ─────────────────────────────────────────────────────────────
def analysis_bonus_combo_by_market(records):
    print_sep()
    print("BONUS: BEST COMBOS BY MARKET TYPE (top 10 per market, ≥10 records)")
    print_sep()

    graded_records = [r for r in records if graded(r)]
    for mkt in ["player_prop", "total", "spread", "moneyline"]:
        mkt_recs = [r for r in graded_records if r.get("market_type") == mkt]
        combo_buckets = defaultdict(list)
        for r in mkt_recs:
            combo_buckets[r.get("signal_sources_combo", "")].append(r)

        rows = []
        for combo, recs in combo_buckets.items():
            w, l, p, rate = wl_stats(recs)
            n = w + l
            if n < 10:
                continue
            rows.append((combo, w, l, p, n, rate))
        rows.sort(key=lambda x: x[5] if x[5] is not None else 0, reverse=True)

        print(f"\n  {mkt.upper()} (top 10, ≥10 records):")
        headers = ["Combo", "W", "L", "P", "N", "Win%"]
        table_rows = [(c, w, l, p, n, fmt_rate(r)) for c, w, l, p, n, r in rows[:10]]
        if table_rows:
            print_table(headers, table_rows)
        else:
            print("    No combos with ≥10 records.")

# ─────────────────────────────────────────────────────────────
def analysis_bonus_signal_source_count_overall(records):
    print_sep()
    print("BONUS: OVERALL WIN RATE BY NUMBER OF AGREEING SOURCES")
    print_sep()

    graded_records = [r for r in records if graded(r)]
    count_buckets = defaultdict(list)
    for r in graded_records:
        n_src = len(r.get("signal_sources_present", []))
        count_buckets[n_src].append(r)

    rows = []
    for n_src in sorted(count_buckets.keys()):
        recs = count_buckets[n_src]
        w, l, p, rate = wl_stats(recs)
        n = w + l
        rows.append((n_src, w, l, p, n, rate))

    headers = ["Num Sources", "W", "L", "P", "N", "Win%"]
    table_rows = [(s, w, l, p, n, fmt_rate(r)) for s, w, l, p, n, r in rows]
    print_table(headers, table_rows)

# ─────────────────────────────────────────────────────────────
def analysis_bonus_juicereel_vs_others_props(records):
    print_sep()
    print("BONUS: JUICEREEL SOURCE WIN RATES FOR PROPS vs NON-JUICEREEL PROPS")
    print_sep()

    graded_records = [r for r in records if graded(r) and r.get("market_type") == "player_prop"]

    jreel_sx = [r for r in graded_records if r.get("occ_source_id") == "juicereel_sxebets"]
    jreel_nuke = [r for r in graded_records if r.get("occ_source_id") == "juicereel_nukethebooks"]
    others = [r for r in graded_records
              if r.get("occ_source_id") not in ("juicereel_sxebets", "juicereel_nukethebooks")]

    for label, recs in [
        ("juicereel_sxebets props", jreel_sx),
        ("juicereel_nukethebooks props", jreel_nuke),
        ("All other sources props", others),
    ]:
        w, l, p, rate = wl_stats(recs)
        n = w + l
        print(f"  {label}: {w}W-{l}L {fmt_rate(rate)} (N={n})")

    # Breakdown by direction for juicereel
    for source_name, recs in [("juicereel_sxebets", jreel_sx), ("juicereel_nukethebooks", jreel_nuke)]:
        if not recs:
            continue
        print(f"\n  {source_name} props by direction:")
        buckets = defaultdict(list)
        for r in recs:
            buckets[r.get("direction", "unknown")].append(r)
        for d in ["OVER", "UNDER"]:
            r_list = buckets.get(d, [])
            w, l, p, rate = wl_stats(r_list)
            n = w + l
            if n > 0:
                print(f"    {d}: {w}W-{l}L {fmt_rate(rate)} (N={n})")

# ─────────────────────────────────────────────────────────────
def summary_stats(records):
    print_sep("=")
    print("DATASET SUMMARY")
    print_sep("=")

    total = len(records)
    graded_count = sum(1 for r in records if graded(r))
    ungraded = total - graded_count

    print(f"  Total occurrences:   {total:,}")
    print(f"  Graded:              {graded_count:,}")
    print(f"  Ungraded/null:       {ungraded:,}")

    # Market breakdown
    mkt_counts = defaultdict(int)
    for r in records:
        if graded(r):
            mkt_counts[r.get("market_type", "unknown")] += 1
    print(f"\n  By market type (graded):")
    for mkt, cnt in sorted(mkt_counts.items(), key=lambda x: -x[1]):
        print(f"    {mkt}: {cnt:,}")

    # Source breakdown
    source_counts = defaultdict(int)
    for r in records:
        if graded(r):
            source_counts[r.get("occ_source_id", "unknown")] += 1
    print(f"\n  By source (graded):")
    for src, cnt in sorted(source_counts.items(), key=lambda x: -x[1]):
        print(f"    {src}: {cnt:,}")

    # Date range
    day_keys = [r.get("day_key", "") for r in records if r.get("day_key")]
    if day_keys:
        print(f"\n  Date range: {min(day_keys)} to {max(day_keys)}")

    # Overall win rate
    graded_records = [r for r in records if graded(r)]
    w, l, p, rate = wl_stats(graded_records)
    print(f"\n  Overall win rate: {w}W-{l}L-{p}P = {fmt_rate(rate)} (N={w+l})")
    print_sep("=")


def main():
    print("Loading data...")
    records = load_data()
    print(f"Loaded {len(records):,} records.\n")

    summary_stats(records)
    analysis_1_source_combo_win_rates(records)
    analysis_2_juicereel_combos(records)
    analysis_3_two_source_combos(records)
    analysis_4_standalone(records)
    analysis_5_market_source_count(records)
    analysis_6_direction(records)
    analysis_7_line_value(records)
    analysis_8_recent_vs_historical(records)
    analysis_9_top_combos_quality(records)
    analysis_10_source_participation(records)
    analysis_bonus_atomic_stat(records)
    analysis_bonus_combo_by_market(records)
    analysis_bonus_signal_source_count_overall(records)
    analysis_bonus_juicereel_vs_others_props(records)

    print_sep("=")
    print("ANALYSIS COMPLETE")
    print_sep("=")

if __name__ == "__main__":
    main()
