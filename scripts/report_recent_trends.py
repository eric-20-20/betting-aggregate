"""Compute recent rolling-window performance trends from graded signals.

Groups by source combo, consensus strength, market type, and stat type
across configurable time windows (7, 14, 30 days).

Outputs:
  - data/reports/recent_trends_lookup.json  (keyed lookup for score_signals.py)
  - data/reports/recent_trends.json         (dashboard report)

Run:
    python3 scripts/report_recent_trends.py
    python3 scripts/report_recent_trends.py --windows 7,14,30
    python3 scripts/report_recent_trends.py --min-n 5 --debug
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ── Paths ──────────────────────────────────────────────────────────────────

GRADED_SIGNALS_PATH = Path("data/analysis/graded_signals_latest.jsonl")
REPORTS_DIR = Path("data/reports")
LOOKUP_PATH = REPORTS_DIR / "recent_trends_lookup.json"
REPORT_PATH = REPORTS_DIR / "recent_trends.json"

DEFAULT_WINDOWS = [7, 14, 30]
DEFAULT_MIN_N = 5


# ── Helpers ────────────────────────────────────────────────────────────────

def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not path.exists():
        return out
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def parse_day_key(day_key: str) -> Optional[date]:
    """Parse 'NBA:2026:01:23' -> date(2026, 1, 23)."""
    if not day_key:
        return None
    parts = day_key.split(":")
    if len(parts) >= 4:
        try:
            return date(int(parts[1]), int(parts[2]), int(parts[3]))
        except (ValueError, IndexError):
            return None
    return None


def wilson_lower(wins: int, n: int, z: float = 1.96) -> float:
    if n == 0:
        return 0.0
    p = wins / n
    denom = 1 + z * z / n
    center = p + z * z / (2 * n)
    spread = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)
    return (center - spread) / denom


def consensus_label(sources_count: int) -> str:
    if sources_count >= 4:
        return "4+_sources"
    return f"{sources_count}_source"


def build_record(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute W-L record from graded rows."""
    wins = sum(1 for r in rows if r.get("result") == "WIN")
    losses = sum(1 for r in rows if r.get("result") == "LOSS")
    pushes = sum(1 for r in rows if r.get("result") == "PUSH")
    decided = wins + losses
    win_pct = wins / decided if decided > 0 else 0.0
    wl = wilson_lower(wins, decided) if decided > 0 else 0.0
    return {
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "n": wins + losses + pushes,
        "win_pct": round(win_pct, 4),
        "wilson_lower": round(wl, 4),
    }


# ── Core ───────────────────────────────────────────────────────────────────

def load_graded(path: Path) -> List[Dict[str, Any]]:
    """Load graded signals, keeping only those with a WIN/LOSS/PUSH result."""
    recs = read_jsonl(path)
    valid = []
    for r in recs:
        if r.get("result") in ("WIN", "LOSS", "PUSH") and r.get("day_key"):
            d = parse_day_key(r["day_key"])
            if d:
                r["_date"] = d
                valid.append(r)
    return valid


def filter_window(records: List[Dict], ref: date, days: int) -> List[Dict]:
    cutoff = ref - timedelta(days=days)
    return [r for r in records if r["_date"] > cutoff and r["_date"] <= ref]


def get_sources_count(r: Dict) -> int:
    sp = r.get("sources_present") or []
    return len(sp) if isinstance(sp, list) else 1


def build_lookup_tables(
    records: List[Dict], min_n: int
) -> Dict[str, Dict[str, Dict]]:
    """Build grouped lookup tables from records.

    Returns dict with keys:
      combo_market_stat, combo_market, combo_overall,
      consensus_market_stat, consensus_market, consensus_overall
    """
    groups: Dict[str, Dict[str, List[Dict]]] = {
        "combo_market_stat": defaultdict(list),
        "combo_market": defaultdict(list),
        "combo_overall": defaultdict(list),
        "consensus_market_stat": defaultdict(list),
        "consensus_market": defaultdict(list),
        "consensus_overall": defaultdict(list),
    }

    for r in records:
        combo = r.get("sources_combo", "")
        market = r.get("market_type", "")
        stat = r.get("atomic_stat") or "all"
        sc = get_sources_count(r)
        cs = consensus_label(sc)

        if combo:
            groups["combo_market_stat"][f"{combo}__{market}__{stat}"].append(r)
            groups["combo_market"][f"{combo}__{market}"].append(r)
            groups["combo_overall"][combo].append(r)

        groups["consensus_market_stat"][f"{cs}__{market}__{stat}"].append(r)
        groups["consensus_market"][f"{cs}__{market}"].append(r)
        groups["consensus_overall"][cs].append(r)

    # Build records, filtering by min_n
    tables: Dict[str, Dict[str, Dict]] = {}
    for level, group_dict in groups.items():
        table = {}
        for key, rows in group_dict.items():
            rec = build_record(rows)
            if rec["wins"] + rec["losses"] >= min_n:
                table[key] = rec
        tables[level] = table

    return tables


def build_report(
    records: List[Dict],
    windows: List[int],
    ref: date,
    min_n: int,
) -> Dict[str, Any]:
    """Build dashboard report grouped by consensus strength and market type."""
    report: Dict[str, Any] = {}

    # By consensus strength across windows
    by_cs: Dict[str, Dict] = {}
    for w in windows:
        subset = filter_window(records, ref, w)
        cs_groups: Dict[str, List] = defaultdict(list)
        for r in subset:
            cs = consensus_label(get_sources_count(r))
            cs_groups[cs].append(r)
        for cs, rows in cs_groups.items():
            rec = build_record(rows)
            if cs not in by_cs:
                by_cs[cs] = {"consensus_strength": cs}
            by_cs[cs][f"window_{w}"] = rec

    report["by_consensus_strength"] = list(by_cs.values())

    # By market type across windows
    by_mt: Dict[str, Dict] = {}
    for w in windows:
        subset = filter_window(records, ref, w)
        mt_groups: Dict[str, List] = defaultdict(list)
        for r in subset:
            mt = r.get("market_type", "unknown")
            mt_groups[mt].append(r)
        for mt, rows in mt_groups.items():
            rec = build_record(rows)
            if mt not in by_mt:
                by_mt[mt] = {"market_type": mt}
            by_mt[mt][f"window_{w}"] = rec

    report["by_market_type"] = list(by_mt.values())

    # Top hot streaks — find the best-performing combos recently
    streaks = []
    for w in windows:
        subset = filter_window(records, ref, w)
        # Group by combo x market
        cm_groups: Dict[str, List] = defaultdict(list)
        for r in subset:
            combo = r.get("sources_combo", "")
            market = r.get("market_type", "")
            if combo:
                cm_groups[f"{combo} / {market}"].append(r)

        for key, rows in cm_groups.items():
            rec = build_record(rows)
            decided = rec["wins"] + rec["losses"]
            if decided < min_n or rec["win_pct"] <= 0.50:
                continue

            # Build anonymous label
            combo_part = key.split(" / ")[0]
            market_part = key.split(" / ")[1] if " / " in key else ""
            sc = len(combo_part.split("|"))
            anon = f"{consensus_label(sc)} / {market_part}" if market_part else consensus_label(sc)

            streaks.append({
                "description": key,
                "anonymous_label": anon,
                "window": w,
                "wins": rec["wins"],
                "losses": rec["losses"],
                "n": rec["n"],
                "win_pct": rec["win_pct"],
                "wilson_lower": rec["wilson_lower"],
            })

    # Sort by wilson_lower descending, take top 15
    streaks.sort(key=lambda s: s["wilson_lower"], reverse=True)
    report["top_hot_streaks"] = streaks[:15]

    return report


def main() -> None:
    ap = argparse.ArgumentParser(description="Compute recent performance trends")
    ap.add_argument("--windows", type=str, default=",".join(str(w) for w in DEFAULT_WINDOWS),
                    help=f"Comma-separated window sizes in days (default: {','.join(str(w) for w in DEFAULT_WINDOWS)})")
    ap.add_argument("--min-n", type=int, default=DEFAULT_MIN_N,
                    help=f"Minimum decided bets to include (default: {DEFAULT_MIN_N})")
    ap.add_argument("--debug", action="store_true", help="Print detailed output")
    ap.add_argument("--ref-date", type=str, default=None,
                    help="Reference date (default: today)")
    args = ap.parse_args()

    windows = [int(w) for w in args.windows.split(",")]
    ref = date.fromisoformat(args.ref_date) if args.ref_date else date.today()

    print(f"  Loading graded signals from {GRADED_SIGNALS_PATH}...")
    all_records = load_graded(GRADED_SIGNALS_PATH)
    print(f"  Loaded {len(all_records)} graded records")

    # Build per-window lookup tables
    lookup_by_window: Dict[str, Dict] = {}
    for w in windows:
        subset = filter_window(all_records, ref, w)
        tables = build_lookup_tables(subset, args.min_n)
        lookup_by_window[str(w)] = tables
        total_keys = sum(len(t) for t in tables.values())
        print(f"  Window {w}d: {len(subset)} records -> {total_keys} lookup entries")

        if args.debug:
            for level, table in tables.items():
                if table:
                    print(f"    {level}: {len(table)} entries")
                    for key, rec in sorted(table.items(), key=lambda x: -x[1]["win_pct"])[:3]:
                        print(f"      {key}: {rec['wins']}-{rec['losses']} ({rec['win_pct']*100:.1f}%)")

    # Write lookup
    lookup_output = {
        "meta": {
            "ref_date": ref.isoformat(),
            "windows": windows,
            "min_n": args.min_n,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        },
        "by_window": lookup_by_window,
    }
    write_json(LOOKUP_PATH, lookup_output)
    print(f"  Wrote lookup: {LOOKUP_PATH}")

    # Build and write report
    report = build_report(all_records, windows, ref, args.min_n)
    report["meta"] = {
        "ref_date": ref.isoformat(),
        "windows": windows,
        "min_n": args.min_n,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    write_json(REPORT_PATH, report)
    print(f"  Wrote report: {REPORT_PATH}")

    # Print hot streaks summary
    if report.get("top_hot_streaks"):
        print(f"\n  Top Recent Streaks:")
        for s in report["top_hot_streaks"][:10]:
            desc = s["description"]
            w = s["window"]
            print(f"    {desc:40s} {s['wins']}-{s['losses']} ({s['win_pct']*100:.1f}%) last {w}d")


if __name__ == "__main__":
    main()
