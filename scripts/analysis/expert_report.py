#!/usr/bin/env python3
"""
Expert deep-dive analysis using Supabase.

Queries the signal_sources + signals + grades tables to answer:
  1. Which experts are profitable solo? (win rate by source/expert/market/stat)
  2. Which source combos beat solo picks?
  3. Hot/cold recency streaks (last 30 days vs all-time)
  4. Expert breakdown by stat type

Usage:
  python3 scripts/expert_report.py                      # all reports
  python3 scripts/expert_report.py --report solo        # solo expert win rates
  python3 scripts/expert_report.py --report combos      # source combo win rates
  python3 scripts/expert_report.py --report recency     # 30d vs all-time
  python3 scripts/expert_report.py --report stats       # breakdown by stat type
  python3 scripts/expert_report.py --min-n 30           # require at least 30 picks
  python3 scripts/expert_report.py --market player_prop
  python3 scripts/expert_report.py --source juicereel_nukethebooks
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

logger = logging.getLogger("expert_report")


def get_client():
    try:
        from supabase import create_client
    except ImportError:
        raise RuntimeError("supabase package not installed. Run: pip install supabase")

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        env_file = REPO_ROOT / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if "=" in line and not line.startswith("#"):
                    k, _, v = line.partition("=")
                    os.environ.setdefault(k.strip(), v.strip())
            url = os.environ.get("SUPABASE_URL")
            key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_KEY not found in env or .env")
    return create_client(url, key)


# ──────────────────────────────────────────────
# Report 1: Solo expert win rates
# ──────────────────────────────────────────────

def report_solo_experts(client, min_n: int = 30, market: Optional[str] = None,
                        source: Optional[str] = None) -> None:
    """Win rate per expert per market type (solo picks only — sources_count = 1)."""
    print(f"\n{'='*70}")
    print(f"SOLO EXPERT WIN RATES  (min_n={min_n})")
    print(f"{'='*70}")

    try:
        rows = client.rpc("expert_record_solo", {}).execute().data
    except Exception:
        # Fallback: use view directly
        query = client.table("expert_record").select("*").gte("n", min_n)
        if market:
            query = query.eq("market_type", market)
        if source:
            query = query.eq("source_id", source)
        rows = query.order("win_pct", desc=True).execute().data or []

    if not rows:
        print("  (no data — run load_supabase_historical.py first)")
        return

    _print_win_rate_table(rows, ["source_id", "expert_slug", "market_type", "atomic_stat",
                                  "n", "wins", "losses", "win_pct"])


# ──────────────────────────────────────────────
# Report 2: Source combo win rates
# ──────────────────────────────────────────────

def report_combos(client, min_n: int = 30, market: Optional[str] = None) -> None:
    """Win rate by sources_combo per market type."""
    print(f"\n{'='*70}")
    print(f"SOURCE COMBO WIN RATES  (min_n={min_n})")
    print(f"{'='*70}")

    query = client.table("combo_record").select("*").gte("n", min_n)
    if market:
        query = query.eq("market_type", market)
    rows = query.order("win_pct", desc=True).execute().data or []

    if not rows:
        print("  (no data)")
        return

    _print_win_rate_table(rows, ["sources_combo", "market_type", "atomic_stat",
                                  "direction", "n", "wins", "losses", "win_pct"])


# ──────────────────────────────────────────────
# Report 3: Recency — last 30 days vs all-time
# ──────────────────────────────────────────────

def report_recency(client, days: int = 30, min_n_recent: int = 10) -> None:
    """Compare last N days win rate vs all-time win rate per source."""
    print(f"\n{'='*70}")
    print(f"RECENCY REPORT — last {days} days vs all-time  (min_n_recent={min_n_recent})")
    print(f"{'='*70}")

    since = (date.today() - timedelta(days=days)).isoformat()
    since_key = f"NBA:{since.replace('-', ':')}"

    # Get all signal_sources rows with grades
    resp = client.table("signal_sources").select(
        "source_id, expert_slug, signal_id"
    ).execute()
    ss_rows = resp.data or []

    # Get all signal day_keys
    resp2 = client.table("signals").select("signal_id, day_key").execute()
    day_by_sid = {r["signal_id"]: r["day_key"] for r in (resp2.data or [])}

    # Get all grades
    resp3 = client.table("grades").select("signal_id, result").execute()
    grade_by_sid = {r["signal_id"]: r["result"] for r in (resp3.data or [])}

    acc: Dict[str, Dict] = {}
    for row in ss_rows:
        sid = row["signal_id"]
        result = grade_by_sid.get(sid)
        if result not in ("WIN", "LOSS"):
            continue
        day_key = day_by_sid.get(sid, "")
        src = row["source_id"] or ""
        slug = row.get("expert_slug") or ""
        key = f"{src}||{slug}"
        if key not in acc:
            acc[key] = {"source_id": src, "expert_slug": slug,
                        "n_all": 0, "wins_all": 0, "n_30d": 0, "wins_30d": 0}
        acc[key]["n_all"] += 1
        if result == "WIN":
            acc[key]["wins_all"] += 1
        if day_key >= since_key:
            acc[key]["n_30d"] += 1
            if result == "WIN":
                acc[key]["wins_30d"] += 1

    rows = []
    for v in acc.values():
        if v["n_30d"] < min_n_recent:
            continue
        wp_all = round(v["wins_all"] / v["n_all"], 3) if v["n_all"] else None
        wp_30d = round(v["wins_30d"] / v["n_30d"], 3) if v["n_30d"] else None
        delta = round((wp_30d or 0) - (wp_all or 0), 3)
        rows.append({**v, "win_pct_all": wp_all, "win_pct_30d": wp_30d, "delta": delta})

    rows.sort(key=lambda r: -(r["win_pct_30d"] or 0))

    print(f"\n{'SOURCE':<30} {'EXPERT':<20} {'N_30D':>6} {'30D%':>7} {'ALL%':>7} {'DELTA':>7}")
    print("-" * 78)
    for r in rows:
        src = r["source_id"][:28]
        slug = (r["expert_slug"] or "")[:18]
        n30 = r["n_30d"]
        wp30 = f"{r['win_pct_30d']:.1%}" if r["win_pct_30d"] else "  —  "
        wall = f"{r['win_pct_all']:.1%}" if r["win_pct_all"] else "  —  "
        delta_str = f"{r['delta']:+.1%}" if r["delta"] else "  —  "
        hot = " 🔥" if (r["win_pct_30d"] or 0) >= 0.60 else (" ❄️ " if (r["win_pct_30d"] or 0) <= 0.40 and n30 >= 20 else "")
        print(f"{src:<30} {slug:<20} {n30:>6}  {wp30:>7}  {wall:>7}  {delta_str:>7}{hot}")


# ──────────────────────────────────────────────
# Report 4: Expert breakdown by stat type
# ──────────────────────────────────────────────

def report_by_stat(client, source: Optional[str] = None, min_n: int = 20) -> None:
    """Win rate per expert per atomic stat (player props only)."""
    print(f"\n{'='*70}")
    print(f"EXPERT STATS BY STAT TYPE  (player_prop, min_n={min_n})")
    print(f"{'='*70}")

    query = (
        client.table("expert_record")
        .select("*")
        .eq("market_type", "player_prop")
        .gte("n", min_n)
    )
    if source:
        query = query.eq("source_id", source)

    rows = query.order("win_pct", desc=True).execute().data or []

    if not rows:
        print("  (no data)")
        return

    _print_win_rate_table(rows, ["source_id", "expert_slug", "atomic_stat",
                                  "n", "wins", "losses", "win_pct"])


# ──────────────────────────────────────────────
# Shared helpers
# ──────────────────────────────────────────────

def _print_win_rate_table(rows: List[Dict], cols: List[str]) -> None:
    """Generic table printer."""
    widths = {c: max(len(c), 8) for c in cols}
    for r in rows:
        for c in cols:
            widths[c] = max(widths[c], len(str(r.get(c, "") or "")))

    header = "  ".join(c.upper().ljust(widths[c]) for c in cols)
    print(f"\n{header}")
    print("-" * len(header))
    for r in rows:
        parts = []
        for c in cols:
            v = r.get(c, "")
            if c == "win_pct" and v is not None:
                parts.append(f"{float(v):.1%}".ljust(widths[c]))
            else:
                parts.append(str(v or "").ljust(widths[c]))
        print("  ".join(parts))
    print(f"\n  Total rows: {len(rows)}")


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Expert deep-dive analysis from Supabase")
    parser.add_argument("--report", choices=["solo", "combos", "recency", "stats", "all"],
                        default="all")
    parser.add_argument("--min-n", type=int, default=30,
                        help="Minimum picks required (default: 30)")
    parser.add_argument("--market", default=None,
                        help="Filter to market type (player_prop, spread, total)")
    parser.add_argument("--source", default=None,
                        help="Filter to source_id (e.g. juicereel_nukethebooks)")
    parser.add_argument("--days", type=int, default=30,
                        help="Days for recency window (default: 30)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    try:
        client = get_client()
    except RuntimeError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    if args.report in ("solo", "all"):
        report_solo_experts(client, min_n=args.min_n, market=args.market, source=args.source)

    if args.report in ("combos", "all"):
        report_combos(client, min_n=args.min_n, market=args.market)

    if args.report in ("recency", "all"):
        report_recency(client, days=args.days, min_n_recent=max(10, args.min_n // 3))

    if args.report in ("stats", "all"):
        report_by_stat(client, source=args.source, min_n=args.min_n)

    print("")


if __name__ == "__main__":
    main()
