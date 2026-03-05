#!/usr/bin/env python3
"""
Deep Pattern Analysis: Find profitable multi-source consensus patterns
in the betting signal data.

Joins signals to grades via signal_id, then slices by:
  - exact sources_combo
  - market_type
  - direction (OVER/UNDER)
  - atomic_stat (for player_props)
  - 2-source pair aggregations
  - 3-source trio aggregations

Prints all patterns above various profitability thresholds.
"""

import json
import math
import sys
from collections import defaultdict, Counter
from itertools import combinations
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
SIGNALS_PATH = BASE / "data" / "ledger" / "signals_latest.jsonl"
GRADES_PATH = BASE / "data" / "ledger" / "grades_latest.jsonl"


# ── Wilson lower bound ──────────────────────────────────────────────────────
def wilson_lower(wins, n, z=1.96):
    if n == 0:
        return 0
    p = wins / n
    denominator = 1 + z**2 / n
    centre = p + z**2 / (2 * n)
    spread = z * math.sqrt((p * (1 - p) + z**2 / (4 * n)) / n)
    return (centre - spread) / denominator


# ── Load data ───────────────────────────────────────────────────────────────
def load_data():
    grades = {}
    with open(GRADES_PATH) as f:
        for line in f:
            g = json.loads(line)
            grades[g["signal_id"]] = g.get("result", g.get("status", ""))

    records = []
    with open(SIGNALS_PATH) as f:
        for line in f:
            s = json.loads(line)
            sid = s["signal_id"]
            result = grades.get(sid)
            if not result or result == "PUSH":
                continue
            sources_combo = s.get("sources_combo", "")
            sources = sources_combo.split("|") if sources_combo else []
            records.append({
                "signal_id": sid,
                "sources_combo": sources_combo,
                "sources": sources,
                "num_sources": len(sources),
                "market_type": s.get("market_type", ""),
                "direction": s.get("direction", ""),
                "atomic_stat": s.get("atomic_stat", ""),
                "day_key": s.get("day_key", ""),
                "selection": s.get("selection", ""),
                "result": result,
                "win": 1 if result == "WIN" else 0,
            })
    return records


# ── Accumulator ─────────────────────────────────────────────────────────────
class Acc:
    """Simple win/loss accumulator."""
    __slots__ = ("w", "n")

    def __init__(self):
        self.w = 0
        self.n = 0

    def add(self, win):
        self.w += win
        self.n += 1

    def pct(self):
        return self.w / self.n if self.n else 0

    def wilson(self):
        return wilson_lower(self.w, self.n)

    def row(self):
        return (self.w, self.n - self.w, self.n, self.pct(), self.wilson())


def fmt_row(label, acc, indent=0):
    w, l, n, pct, wil = acc.row()
    pad = "  " * indent
    return f"{pad}{label:<65s}  {w:>4d}-{l:<4d}  n={n:<5d}  {pct:6.1%}  Wilson={wil:.4f}"


# ── Build all buckets ───────────────────────────────────────────────────────
def analyse(records):
    # 1) By exact sources_combo
    by_combo = defaultdict(Acc)                            # combo -> Acc
    by_combo_market = defaultdict(Acc)                     # (combo, market) -> Acc
    by_combo_market_dir = defaultdict(Acc)                 # (combo, market, dir) -> Acc
    by_combo_market_dir_stat = defaultdict(Acc)            # (combo, market, dir, stat) -> Acc
    by_combo_market_stat = defaultdict(Acc)                # (combo, market, stat) -> Acc

    # 2) By individual source (baseline)
    by_source = defaultdict(Acc)                           # source -> Acc
    by_source_market = defaultdict(Acc)                    # (source, market) -> Acc
    by_source_market_dir = defaultdict(Acc)                # (source, market, dir) -> Acc
    by_source_market_dir_stat = defaultdict(Acc)           # (source, market, dir, stat) -> Acc

    # 3) By pair of sources present (aggregated across all combos containing both)
    by_pair = defaultdict(Acc)
    by_pair_market = defaultdict(Acc)
    by_pair_market_dir = defaultdict(Acc)
    by_pair_market_dir_stat = defaultdict(Acc)

    # 4) By trio of sources present
    by_trio = defaultdict(Acc)
    by_trio_market = defaultdict(Acc)
    by_trio_market_dir = defaultdict(Acc)

    # 5) By source count
    by_count = defaultdict(Acc)
    by_count_market = defaultdict(Acc)
    by_count_market_dir = defaultdict(Acc)

    # 6) Overall
    overall = Acc()

    for r in records:
        win = r["win"]
        combo = r["sources_combo"]
        market = r["market_type"]
        dirn = r["direction"]
        stat = r["atomic_stat"]
        sources = r["sources"]
        nsrc = r["num_sources"]

        overall.add(win)

        # exact combo
        by_combo[combo].add(win)
        by_combo_market[(combo, market)].add(win)
        if dirn:
            by_combo_market_dir[(combo, market, dirn)].add(win)
        if stat:
            by_combo_market_stat[(combo, market, stat)].add(win)
            if dirn:
                by_combo_market_dir_stat[(combo, market, dirn, stat)].add(win)

        # individual sources (for baseline)
        for src in sources:
            by_source[src].add(win)
            by_source_market[(src, market)].add(win)
            if dirn:
                by_source_market_dir[(src, market, dirn)].add(win)
            if stat and dirn:
                by_source_market_dir_stat[(src, market, dirn, stat)].add(win)

        # pairs
        if nsrc >= 2:
            for pair in combinations(sorted(sources), 2):
                pair_key = "|".join(pair)
                by_pair[pair_key].add(win)
                by_pair_market[(pair_key, market)].add(win)
                if dirn:
                    by_pair_market_dir[(pair_key, market, dirn)].add(win)
                if stat and dirn:
                    by_pair_market_dir_stat[(pair_key, market, dirn, stat)].add(win)

        # trios
        if nsrc >= 3:
            for trio in combinations(sorted(sources), 3):
                trio_key = "|".join(trio)
                by_trio[trio_key].add(win)
                by_trio_market[(trio_key, market)].add(win)
                if dirn:
                    by_trio_market_dir[(trio_key, market, dirn)].add(win)

        # source count
        by_count[nsrc].add(win)
        by_count_market[(nsrc, market)].add(win)
        if dirn:
            by_count_market_dir[(nsrc, market, dirn)].add(win)

    return {
        "overall": overall,
        "by_combo": by_combo,
        "by_combo_market": by_combo_market,
        "by_combo_market_dir": by_combo_market_dir,
        "by_combo_market_dir_stat": by_combo_market_dir_stat,
        "by_combo_market_stat": by_combo_market_stat,
        "by_source": by_source,
        "by_source_market": by_source_market,
        "by_source_market_dir": by_source_market_dir,
        "by_source_market_dir_stat": by_source_market_dir_stat,
        "by_pair": by_pair,
        "by_pair_market": by_pair_market,
        "by_pair_market_dir": by_pair_market_dir,
        "by_pair_market_dir_stat": by_pair_market_dir_stat,
        "by_trio": by_trio,
        "by_trio_market": by_trio_market,
        "by_trio_market_dir": by_trio_market_dir,
        "by_count": by_count,
        "by_count_market": by_count_market,
        "by_count_market_dir": by_count_market_dir,
    }


# ── Collect all labelled patterns into a flat list ──────────────────────────
def flatten_patterns(d):
    """Return list of (label, category, acc) tuples from all buckets."""
    patterns = []

    def add(bucket, fmt_fn, category):
        for key, acc in bucket.items():
            patterns.append((fmt_fn(key), category, acc))

    # Exact combo overall
    add(d["by_combo"], lambda k: f"combo={k}", "combo_overall")

    # Exact combo x market
    add(d["by_combo_market"],
        lambda k: f"combo={k[0]}  market={k[1]}", "combo_market")

    # Exact combo x market x direction
    add(d["by_combo_market_dir"],
        lambda k: f"combo={k[0]}  market={k[1]}  dir={k[2]}", "combo_market_dir")

    # Exact combo x market x direction x stat
    add(d["by_combo_market_dir_stat"],
        lambda k: f"combo={k[0]}  market={k[1]}  dir={k[2]}  stat={k[3]}",
        "combo_market_dir_stat")

    # Exact combo x market x stat
    add(d["by_combo_market_stat"],
        lambda k: f"combo={k[0]}  market={k[1]}  stat={k[2]}",
        "combo_market_stat")

    # Source baseline
    add(d["by_source"], lambda k: f"source={k}", "source_baseline")
    add(d["by_source_market"],
        lambda k: f"source={k[0]}  market={k[1]}", "source_market")
    add(d["by_source_market_dir"],
        lambda k: f"source={k[0]}  market={k[1]}  dir={k[2]}", "source_market_dir")
    add(d["by_source_market_dir_stat"],
        lambda k: f"source={k[0]}  market={k[1]}  dir={k[2]}  stat={k[3]}",
        "source_market_dir_stat")

    # Pair aggregations
    add(d["by_pair"], lambda k: f"pair={k}", "pair_overall")
    add(d["by_pair_market"],
        lambda k: f"pair={k[0]}  market={k[1]}", "pair_market")
    add(d["by_pair_market_dir"],
        lambda k: f"pair={k[0]}  market={k[1]}  dir={k[2]}", "pair_market_dir")
    add(d["by_pair_market_dir_stat"],
        lambda k: f"pair={k[0]}  market={k[1]}  dir={k[2]}  stat={k[3]}",
        "pair_market_dir_stat")

    # Trio aggregations
    add(d["by_trio"], lambda k: f"trio={k}", "trio_overall")
    add(d["by_trio_market"],
        lambda k: f"trio={k[0]}  market={k[1]}", "trio_market")
    add(d["by_trio_market_dir"],
        lambda k: f"trio={k[0]}  market={k[1]}  dir={k[2]}", "trio_market_dir")

    # Source count
    add(d["by_count"], lambda k: f"source_count={k}", "count_overall")
    add(d["by_count_market"],
        lambda k: f"source_count={k[0]}  market={k[1]}", "count_market")
    add(d["by_count_market_dir"],
        lambda k: f"source_count={k[0]}  market={k[1]}  dir={k[2]}",
        "count_market_dir")

    return patterns


# ── Pretty-print a list of patterns ────────────────────────────────────────
def print_table(title, rows, max_rows=None):
    """rows: list of (label, category, acc)"""
    sep = "=" * 130
    print(f"\n{sep}")
    print(f"  {title}  ({len(rows)} patterns)")
    print(sep)
    if not rows:
        print("  (none)")
        return
    header = f"  {'Pattern':<70s}  {'W-L':<10s}  {'N':<6s}  {'Win%':<8s}  {'Wilson':<8s}  {'Category'}"
    print(header)
    print("  " + "-" * 126)
    for i, (label, cat, acc) in enumerate(rows):
        if max_rows and i >= max_rows:
            print(f"  ... and {len(rows) - max_rows} more")
            break
        w, l, n, pct, wil = acc.row()
        print(f"  {label:<70s}  {w:>4d}-{l:<4d}  {n:<6d}  {pct:6.1%}   {wil:.4f}   {cat}")


# ── Multi-source improvement analysis ──────────────────────────────────────
def multi_source_improvement(d):
    """
    For each 2-source pair, compare pair performance to each individual
    source's performance in the same market/direction/stat.
    """
    results = []

    # Compare pair_market_dir_stat against individual source_market_dir_stat
    for key, pair_acc in d["by_pair_market_dir_stat"].items():
        pair_str, market, dirn, stat = key
        if pair_acc.n < 5:
            continue
        src_a, src_b = pair_str.split("|")
        acc_a = d["by_source_market_dir_stat"].get((src_a, market, dirn, stat))
        acc_b = d["by_source_market_dir_stat"].get((src_b, market, dirn, stat))
        if acc_a and acc_b and acc_a.n >= 5 and acc_b.n >= 5:
            best_single = max(acc_a.pct(), acc_b.pct())
            improvement = pair_acc.pct() - best_single
            if improvement > 0:
                results.append({
                    "pair": pair_str,
                    "market": market,
                    "dir": dirn,
                    "stat": stat,
                    "pair_record": f"{pair_acc.w}-{pair_acc.n - pair_acc.w}",
                    "pair_pct": pair_acc.pct(),
                    "pair_n": pair_acc.n,
                    "pair_wilson": pair_acc.wilson(),
                    "src_a": src_a,
                    "src_a_pct": acc_a.pct(),
                    "src_a_n": acc_a.n,
                    "src_b": src_b,
                    "src_b_pct": acc_b.pct(),
                    "src_b_n": acc_b.n,
                    "improvement": improvement,
                })

    # Also compare at pair_market_dir level
    for key, pair_acc in d["by_pair_market_dir"].items():
        pair_str, market, dirn = key
        if pair_acc.n < 10:
            continue
        src_a, src_b = pair_str.split("|")
        acc_a = d["by_source_market_dir"].get((src_a, market, dirn))
        acc_b = d["by_source_market_dir"].get((src_b, market, dirn))
        if acc_a and acc_b and acc_a.n >= 10 and acc_b.n >= 10:
            best_single = max(acc_a.pct(), acc_b.pct())
            improvement = pair_acc.pct() - best_single
            if improvement > 0:
                results.append({
                    "pair": pair_str,
                    "market": market,
                    "dir": dirn,
                    "stat": "(all)",
                    "pair_record": f"{pair_acc.w}-{pair_acc.n - pair_acc.w}",
                    "pair_pct": pair_acc.pct(),
                    "pair_n": pair_acc.n,
                    "pair_wilson": pair_acc.wilson(),
                    "src_a": src_a,
                    "src_a_pct": acc_a.pct(),
                    "src_a_n": acc_a.n,
                    "src_b": src_b,
                    "src_b_pct": acc_b.pct(),
                    "src_b_n": acc_b.n,
                    "improvement": improvement,
                })

    # Also compare at pair_market level
    for key, pair_acc in d["by_pair_market"].items():
        pair_str, market = key
        if pair_acc.n < 10:
            continue
        src_a, src_b = pair_str.split("|")
        acc_a = d["by_source_market"].get((src_a, market))
        acc_b = d["by_source_market"].get((src_b, market))
        if acc_a and acc_b and acc_a.n >= 10 and acc_b.n >= 10:
            best_single = max(acc_a.pct(), acc_b.pct())
            improvement = pair_acc.pct() - best_single
            if improvement > 0:
                results.append({
                    "pair": pair_str,
                    "market": market,
                    "dir": "(all)",
                    "stat": "(all)",
                    "pair_record": f"{pair_acc.w}-{pair_acc.n - pair_acc.w}",
                    "pair_pct": pair_acc.pct(),
                    "pair_n": pair_acc.n,
                    "pair_wilson": pair_acc.wilson(),
                    "src_a": src_a,
                    "src_a_pct": acc_a.pct(),
                    "src_a_n": acc_a.n,
                    "src_b": src_b,
                    "src_b_pct": acc_b.pct(),
                    "src_b_n": acc_b.n,
                    "improvement": improvement,
                })

    # Also compare at pair overall level
    for pair_str, pair_acc in d["by_pair"].items():
        if pair_acc.n < 10:
            continue
        src_a, src_b = pair_str.split("|")
        acc_a = d["by_source"].get(src_a)
        acc_b = d["by_source"].get(src_b)
        if acc_a and acc_b and acc_a.n >= 10 and acc_b.n >= 10:
            best_single = max(acc_a.pct(), acc_b.pct())
            improvement = pair_acc.pct() - best_single
            if improvement > 0:
                results.append({
                    "pair": pair_str,
                    "market": "(all)",
                    "dir": "(all)",
                    "stat": "(all)",
                    "pair_record": f"{pair_acc.w}-{pair_acc.n - pair_acc.w}",
                    "pair_pct": pair_acc.pct(),
                    "pair_n": pair_acc.n,
                    "pair_wilson": pair_acc.wilson(),
                    "src_a": src_a,
                    "src_a_pct": acc_a.pct(),
                    "src_a_n": acc_a.n,
                    "src_b": src_b,
                    "src_b_pct": acc_b.pct(),
                    "src_b_n": acc_b.n,
                    "improvement": improvement,
                })

    results.sort(key=lambda x: -x["improvement"])
    return results


# ── Main ────────────────────────────────────────────────────────────────────
def main():
    print("Loading data...")
    records = load_data()
    print(f"Loaded {len(records)} graded signals (excluding PUSHes)")

    result_counts = Counter(r["result"] for r in records)
    print(f"Results: {dict(result_counts)}")

    d = analyse(records)

    # Overall
    ov = d["overall"]
    print(f"\n{'='*130}")
    print(f"  OVERALL: {ov.w}-{ov.n - ov.w}  n={ov.n}  {ov.pct():.1%}  Wilson={ov.wilson():.4f}")
    print(f"{'='*130}")

    # ── Section 0: Source baselines ──────────────────────────────────────
    print(f"\n{'='*130}")
    print("  SECTION 0: SOURCE BASELINES (individual source performance)")
    print(f"{'='*130}")

    print("\n  --- By source (overall) ---")
    for src in sorted(d["by_source"].keys()):
        acc = d["by_source"][src]
        print(fmt_row(f"source={src}", acc, indent=1))

    print("\n  --- By source x market ---")
    for key in sorted(d["by_source_market"].keys()):
        acc = d["by_source_market"][key]
        if acc.n >= 10:
            print(fmt_row(f"source={key[0]}  market={key[1]}", acc, indent=1))

    print("\n  --- By source x market x direction (n>=10) ---")
    for key in sorted(d["by_source_market_dir"].keys()):
        acc = d["by_source_market_dir"][key]
        if acc.n >= 10:
            print(fmt_row(f"source={key[0]}  market={key[1]}  dir={key[2]}", acc, indent=1))

    print("\n  --- By source x market x direction x stat (n>=10) ---")
    for key in sorted(d["by_source_market_dir_stat"].keys()):
        acc = d["by_source_market_dir_stat"][key]
        if acc.n >= 10:
            print(fmt_row(f"source={key[0]}  market={key[1]}  dir={key[2]}  stat={key[3]}", acc, indent=1))

    # ── Section 1: By source count ──────────────────────────────────────
    print(f"\n{'='*130}")
    print("  SECTION 1: BY SOURCE COUNT")
    print(f"{'='*130}")
    for k in sorted(d["by_count"].keys()):
        print(fmt_row(f"source_count={k}", d["by_count"][k], indent=1))

    print("\n  --- By source count x market ---")
    for key in sorted(d["by_count_market"].keys()):
        acc = d["by_count_market"][key]
        if acc.n >= 5:
            print(fmt_row(f"source_count={key[0]}  market={key[1]}", acc, indent=1))

    print("\n  --- By source count x market x direction ---")
    for key in sorted(d["by_count_market_dir"].keys()):
        acc = d["by_count_market_dir"][key]
        if acc.n >= 5:
            print(fmt_row(f"source_count={key[0]}  market={key[1]}  dir={key[2]}", acc, indent=1))

    # ── Section 2: Exact combo breakdown ────────────────────────────────
    print(f"\n{'='*130}")
    print("  SECTION 2: EXACT COMBO BREAKDOWN (all combos with n>=5)")
    print(f"{'='*130}")

    print("\n  --- By combo (overall, n>=5) ---")
    for combo in sorted(d["by_combo"].keys()):
        acc = d["by_combo"][combo]
        if acc.n >= 5:
            print(fmt_row(f"combo={combo}", acc, indent=1))

    print("\n  --- By combo x market (n>=5) ---")
    for key in sorted(d["by_combo_market"].keys()):
        acc = d["by_combo_market"][key]
        if acc.n >= 5:
            print(fmt_row(f"combo={key[0]}  market={key[1]}", acc, indent=1))

    print("\n  --- By combo x market x direction (n>=5) ---")
    for key in sorted(d["by_combo_market_dir"].keys()):
        acc = d["by_combo_market_dir"][key]
        if acc.n >= 5:
            print(fmt_row(f"combo={key[0]}  market={key[1]}  dir={key[2]}", acc, indent=1))

    print("\n  --- By combo x market x stat (n>=5) ---")
    for key in sorted(d["by_combo_market_stat"].keys()):
        acc = d["by_combo_market_stat"][key]
        if acc.n >= 5:
            print(fmt_row(f"combo={key[0]}  market={key[1]}  stat={key[2]}", acc, indent=1))

    print("\n  --- By combo x market x direction x stat (n>=5) ---")
    for key in sorted(d["by_combo_market_dir_stat"].keys()):
        acc = d["by_combo_market_dir_stat"][key]
        if acc.n >= 5:
            print(fmt_row(f"combo={key[0]}  market={key[1]}  dir={key[2]}  stat={key[3]}", acc, indent=1))

    # ── Section 3: Pair aggregations ────────────────────────────────────
    print(f"\n{'='*130}")
    print("  SECTION 3: SOURCE PAIR AGGREGATIONS (all combos containing both sources)")
    print(f"{'='*130}")

    print("\n  --- Pairs overall (n>=5) ---")
    for pair in sorted(d["by_pair"].keys()):
        acc = d["by_pair"][pair]
        if acc.n >= 5:
            print(fmt_row(f"pair={pair}", acc, indent=1))

    print("\n  --- Pairs x market (n>=5) ---")
    for key in sorted(d["by_pair_market"].keys()):
        acc = d["by_pair_market"][key]
        if acc.n >= 5:
            print(fmt_row(f"pair={key[0]}  market={key[1]}", acc, indent=1))

    print("\n  --- Pairs x market x direction (n>=5) ---")
    for key in sorted(d["by_pair_market_dir"].keys()):
        acc = d["by_pair_market_dir"][key]
        if acc.n >= 5:
            print(fmt_row(f"pair={key[0]}  market={key[1]}  dir={key[2]}", acc, indent=1))

    print("\n  --- Pairs x market x direction x stat (n>=5) ---")
    for key in sorted(d["by_pair_market_dir_stat"].keys()):
        acc = d["by_pair_market_dir_stat"][key]
        if acc.n >= 5:
            print(fmt_row(f"pair={key[0]}  market={key[1]}  dir={key[2]}  stat={key[3]}", acc, indent=1))

    # ── Section 4: Trio aggregations ────────────────────────────────────
    print(f"\n{'='*130}")
    print("  SECTION 4: SOURCE TRIO AGGREGATIONS (n>=5)")
    print(f"{'='*130}")

    print("\n  --- Trios overall ---")
    for trio in sorted(d["by_trio"].keys()):
        acc = d["by_trio"][trio]
        if acc.n >= 5:
            print(fmt_row(f"trio={trio}", acc, indent=1))

    print("\n  --- Trios x market ---")
    for key in sorted(d["by_trio_market"].keys()):
        acc = d["by_trio_market"][key]
        if acc.n >= 5:
            print(fmt_row(f"trio={key[0]}  market={key[1]}", acc, indent=1))

    print("\n  --- Trios x market x direction ---")
    for key in sorted(d["by_trio_market_dir"].keys()):
        acc = d["by_trio_market_dir"][key]
        if acc.n >= 5:
            print(fmt_row(f"trio={key[0]}  market={key[1]}  dir={key[2]}", acc, indent=1))

    # ════════════════════════════════════════════════════════════════════
    # FILTERED / SORTED PATTERN TABLES
    # ════════════════════════════════════════════════════════════════════
    all_patterns = flatten_patterns(d)

    # Filter A: n>=10, win%>=55%, sorted by Wilson
    fa = [(l, c, a) for l, c, a in all_patterns if a.n >= 10 and a.pct() >= 0.55]
    fa.sort(key=lambda x: -x[2].wilson())
    print_table("FILTER A: n>=10, win%>=55% (sorted by Wilson lower bound)", fa)

    # Filter B: n>=20, win%>=53%, sorted by Wilson
    fb = [(l, c, a) for l, c, a in all_patterns if a.n >= 20 and a.pct() >= 0.53]
    fb.sort(key=lambda x: -x[2].wilson())
    print_table("FILTER B: n>=20, win%>=53% (sorted by Wilson lower bound)", fb)

    # Filter C: multi-source (2+) patterns, n>=10, win%>=55%
    def is_multi(label):
        """Check if label implies multi-source."""
        if "combo=" in label:
            combo_part = label.split("combo=")[1].split("  ")[0]
            return "|" in combo_part
        if "pair=" in label or "trio=" in label:
            return True
        if "source_count=" in label:
            cnt_part = label.split("source_count=")[1].split("  ")[0]
            try:
                return int(cnt_part) >= 2
            except ValueError:
                return False
        return False

    fc = [(l, c, a) for l, c, a in all_patterns if is_multi(l) and a.n >= 10 and a.pct() >= 0.55]
    fc.sort(key=lambda x: -x[2].wilson())
    print_table("FILTER C: MULTI-SOURCE (2+) patterns, n>=10, win%>=55%", fc)

    # Filter D: n>=10, win%>=50%, sorted by Wilson (broader view)
    fd = [(l, c, a) for l, c, a in all_patterns if a.n >= 10 and a.pct() >= 0.50]
    fd.sort(key=lambda x: -x[2].wilson())
    print_table("FILTER D: n>=10, win%>=50% (broader, sorted by Wilson)", fd, max_rows=80)

    # ── Section: Multi-source improvement analysis ──────────────────────
    print(f"\n{'='*130}")
    print("  SECTION 5: MULTI-SOURCE IMPROVEMENT ANALYSIS")
    print("  (Pairs where adding 2nd source IMPROVES over best single-source performance)")
    print(f"{'='*130}")

    improvements = multi_source_improvement(d)
    if not improvements:
        print("  (no improvements found)")
    else:
        print(f"\n  Found {len(improvements)} improvement patterns (sorted by improvement margin)")
        print(f"  {'Pair':<25s}  {'Market':<14s}  {'Dir':<8s}  {'Stat':<14s}  {'Pair Record':<12s}  {'Pair%':<8s}  {'N':<6s}  {'Wilson':<8s}  {'SrcA':<10s}  {'A%':<8s}  {'An':<5s}  {'SrcB':<10s}  {'B%':<8s}  {'Bn':<5s}  {'Improv'}")
        print("  " + "-" * 170)
        for r in improvements[:80]:
            print(f"  {r['pair']:<25s}  {r['market']:<14s}  {r['dir']:<8s}  {r['stat']:<14s}  "
                  f"{r['pair_record']:<12s}  {r['pair_pct']:6.1%}   {r['pair_n']:<6d}  {r['pair_wilson']:.4f}   "
                  f"{r['src_a']:<10s}  {r['src_a_pct']:6.1%}   {r['src_a_n']:<5d}  "
                  f"{r['src_b']:<10s}  {r['src_b_pct']:6.1%}   {r['src_b_n']:<5d}  "
                  f"+{r['improvement']:5.1%}")

    # ── Section 6: Focused summary of most actionable patterns ──────────
    print(f"\n{'='*130}")
    print("  SECTION 6: ACTIONABLE PATTERN SUMMARY")
    print(f"{'='*130}")

    # Find the best patterns that are both high-confidence and high-volume
    # Wilson >= 0.50 and n >= 15
    actionable = [(l, c, a) for l, c, a in all_patterns
                  if a.n >= 15 and a.wilson() >= 0.50]
    actionable.sort(key=lambda x: -x[2].wilson())
    print_table("ACTIONABLE: Wilson >= 0.50, n >= 15 (high confidence + volume)", actionable, max_rows=50)

    # Multi-source only actionable
    ms_act = [(l, c, a) for l, c, a in actionable if is_multi(l)]
    print_table("ACTIONABLE MULTI-SOURCE ONLY: Wilson >= 0.50, n >= 15", ms_act, max_rows=50)

    # Edge case: very high win rate with smaller samples
    high_wr = [(l, c, a) for l, c, a in all_patterns
               if a.n >= 8 and a.pct() >= 0.65]
    high_wr.sort(key=lambda x: -x[2].pct())
    print_table("HIGH WIN RATE: n>=8, win%>=65% (small sample size gems)", high_wr, max_rows=50)


if __name__ == "__main__":
    main()
