#!/usr/bin/env python3
"""
SQL-based pattern discovery using Supabase's graded_occurrences table.

Runs a configurable sweep across dimensions (source, market_type, direction,
atomic_stat, expert_name) and surfaces candidates that meet Wilson lower-bound
thresholds for A-tier (0.52) or B-tier (0.46).

Usage:
  python3 scripts/pattern_analysis_supabase.py
  python3 scripts/pattern_analysis_supabase.py --min-n 30 --tier B
  python3 scripts/pattern_analysis_supabase.py --dimension expert
  python3 scripts/pattern_analysis_supabase.py --sport NCAAB
  python3 scripts/pattern_analysis_supabase.py --sql "SELECT ..."  # run raw SQL
"""
from __future__ import annotations

import argparse
import math
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

# ──────────────────────────────────────────────────────────────────────────────
# Wilson lower bound (same formula used in score_signals.py)
# ──────────────────────────────────────────────────────────────────────────────
def wilson_lower(wins: int, n: int, z: float = 1.645) -> float:
    if n == 0:
        return 0.0
    p = wins / n
    return (p + z * z / (2 * n) - z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))) / (
        1 + z * z / n
    )


TIER_A_WILSON = 0.52
TIER_B_WILSON = 0.46

# ──────────────────────────────────────────────────────────────────────────────
# Pre-built query templates
# ──────────────────────────────────────────────────────────────────────────────

# Sweep by: source × market_type × direction
QUERY_SOURCE_MARKET = """
SELECT
  occ_source_id,
  market_type,
  direction,
  atomic_stat,
  COUNT(*) FILTER (WHERE result IN ('WIN','LOSS'))           AS n,
  COUNT(*) FILTER (WHERE result = 'WIN')                     AS wins,
  COUNT(*) FILTER (WHERE result = 'LOSS')                    AS losses,
  ROUND(
    COUNT(*) FILTER (WHERE result = 'WIN')::numeric /
    NULLIF(COUNT(*) FILTER (WHERE result IN ('WIN','LOSS')), 0), 3
  )                                                          AS win_pct
FROM graded_occurrences
WHERE sport = '{sport}'
  AND result IN ('WIN', 'LOSS')
GROUP BY occ_source_id, market_type, direction, atomic_stat
HAVING COUNT(*) FILTER (WHERE result IN ('WIN','LOSS')) >= {min_n}
ORDER BY win_pct DESC NULLS LAST;
"""

# Sweep by: signal_sources_combo (cross-source pairs) × market × direction
QUERY_COMBO_MARKET = """
SELECT
  signal_sources_combo,
  market_type,
  direction,
  atomic_stat,
  COUNT(*) FILTER (WHERE result IN ('WIN','LOSS'))           AS n,
  COUNT(*) FILTER (WHERE result = 'WIN')                     AS wins,
  COUNT(*) FILTER (WHERE result = 'LOSS')                    AS losses,
  ROUND(
    COUNT(*) FILTER (WHERE result = 'WIN')::numeric /
    NULLIF(COUNT(*) FILTER (WHERE result IN ('WIN','LOSS')), 0), 3
  )                                                          AS win_pct
FROM graded_occurrences
WHERE sport = '{sport}'
  AND result IN ('WIN', 'LOSS')
  AND sources_count >= 2
GROUP BY signal_sources_combo, market_type, direction, atomic_stat
HAVING COUNT(*) FILTER (WHERE result IN ('WIN','LOSS')) >= {min_n}
ORDER BY win_pct DESC NULLS LAST;
"""

# Sweep by: expert_name × market_type × direction
QUERY_EXPERT = """
SELECT
  expert_name,
  occ_source_id,
  market_type,
  direction,
  atomic_stat,
  COUNT(*) FILTER (WHERE result IN ('WIN','LOSS'))           AS n,
  COUNT(*) FILTER (WHERE result = 'WIN')                     AS wins,
  COUNT(*) FILTER (WHERE result = 'LOSS')                    AS losses,
  ROUND(
    COUNT(*) FILTER (WHERE result = 'WIN')::numeric /
    NULLIF(COUNT(*) FILTER (WHERE result IN ('WIN','LOSS')), 0), 3
  )                                                          AS win_pct
FROM graded_occurrences
WHERE sport = '{sport}'
  AND result IN ('WIN', 'LOSS')
  AND expert_name IS NOT NULL
GROUP BY expert_name, occ_source_id, market_type, direction, atomic_stat
HAVING COUNT(*) FILTER (WHERE result IN ('WIN','LOSS')) >= {min_n}
ORDER BY win_pct DESC NULLS LAST;
"""

# Sweep by: source × stat_key × direction (fine-grained prop analysis)
QUERY_STAT = """
SELECT
  occ_source_id,
  stat_key,
  direction,
  COUNT(*) FILTER (WHERE result IN ('WIN','LOSS'))           AS n,
  COUNT(*) FILTER (WHERE result = 'WIN')                     AS wins,
  COUNT(*) FILTER (WHERE result = 'LOSS')                    AS losses,
  ROUND(
    COUNT(*) FILTER (WHERE result = 'WIN')::numeric /
    NULLIF(COUNT(*) FILTER (WHERE result IN ('WIN','LOSS')), 0), 3
  )                                                          AS win_pct
FROM graded_occurrences
WHERE sport = '{sport}'
  AND result IN ('WIN', 'LOSS')
  AND market_type = 'player_prop'
GROUP BY occ_source_id, stat_key, direction
HAVING COUNT(*) FILTER (WHERE result IN ('WIN','LOSS')) >= {min_n}
ORDER BY win_pct DESC NULLS LAST;
"""

# Expert-pair agreement: when expert A and B both picked the same game+market+stat+direction.
# Joins on event_key+market_type+direction+atomic_stat because experts span different sources
# and will never share the same signal_id (each source has its own occurrence).
QUERY_EXPERT_PAIRS = """
SELECT
  a.expert_name                                      AS expert_a,
  b.expert_name                                      AS expert_b,
  a.market_type,
  a.direction,
  a.atomic_stat,
  COUNT(*)  FILTER (WHERE a.result IN ('WIN','LOSS')) AS n,
  COUNT(*)  FILTER (WHERE a.result = 'WIN')           AS wins,
  COUNT(*)  FILTER (WHERE a.result = 'LOSS')          AS losses,
  ROUND(
    COUNT(*) FILTER (WHERE a.result = 'WIN')::numeric /
    NULLIF(COUNT(*) FILTER (WHERE a.result IN ('WIN','LOSS')), 0), 3
  )                                                   AS win_pct
FROM graded_occurrences a
JOIN graded_occurrences b
  ON  a.event_key   = b.event_key
  AND a.player_key  = b.player_key
  AND a.market_type = b.market_type
  AND a.direction   = b.direction
  AND COALESCE(a.atomic_stat, '') = COALESCE(b.atomic_stat, '')
  AND a.expert_name < b.expert_name
WHERE a.sport = '{sport}'
  AND a.result IN ('WIN', 'LOSS')
  AND a.expert_name IS NOT NULL
  AND b.expert_name IS NOT NULL
  AND a.event_key IS NOT NULL
  AND a.player_key IS NOT NULL
GROUP BY expert_a, expert_b, a.market_type, a.direction, a.atomic_stat
HAVING COUNT(*) FILTER (WHERE a.result IN ('WIN','LOSS')) >= {min_n}
ORDER BY win_pct DESC NULLS LAST
"""

# Recent form (last 60 days): same as expert sweep but time-filtered
QUERY_RECENT_EXPERT = """
SELECT
  expert_name,
  occ_source_id,
  market_type,
  direction,
  atomic_stat,
  COUNT(*) FILTER (WHERE result IN ('WIN','LOSS'))           AS n,
  COUNT(*) FILTER (WHERE result = 'WIN')                     AS wins,
  COUNT(*) FILTER (WHERE result = 'LOSS')                    AS losses,
  ROUND(
    COUNT(*) FILTER (WHERE result = 'WIN')::numeric /
    NULLIF(COUNT(*) FILTER (WHERE result IN ('WIN','LOSS')), 0), 3
  )                                                          AS win_pct
FROM graded_occurrences
WHERE sport = '{sport}'
  AND result IN ('WIN', 'LOSS')
  AND expert_name IS NOT NULL
  AND observed_at_utc >= NOW() - INTERVAL '60 days'
GROUP BY expert_name, occ_source_id, market_type, direction, atomic_stat
HAVING COUNT(*) FILTER (WHERE result IN ('WIN','LOSS')) >= {min_n}
ORDER BY win_pct DESC NULLS LAST
"""

# Recent form (last 60 days): source-level
QUERY_RECENT_SOURCE = """
SELECT
  occ_source_id,
  market_type,
  direction,
  atomic_stat,
  COUNT(*) FILTER (WHERE result IN ('WIN','LOSS'))           AS n,
  COUNT(*) FILTER (WHERE result = 'WIN')                     AS wins,
  COUNT(*) FILTER (WHERE result = 'LOSS')                    AS losses,
  ROUND(
    COUNT(*) FILTER (WHERE result = 'WIN')::numeric /
    NULLIF(COUNT(*) FILTER (WHERE result IN ('WIN','LOSS')), 0), 3
  )                                                          AS win_pct
FROM graded_occurrences
WHERE sport = '{sport}'
  AND result IN ('WIN', 'LOSS')
  AND observed_at_utc >= NOW() - INTERVAL '60 days'
GROUP BY occ_source_id, market_type, direction, atomic_stat
HAVING COUNT(*) FILTER (WHERE result IN ('WIN','LOSS')) >= {min_n}
ORDER BY win_pct DESC NULLS LAST
"""

# Month-by-month distribution for a specific pattern (temporal concentration check)
QUERY_TEMPORAL = """
SELECT
  SUBSTRING(day_key, 5, 7)   AS yyyymm,
  COUNT(*) FILTER (WHERE result IN ('WIN','LOSS'))  AS n,
  COUNT(*) FILTER (WHERE result = 'WIN')             AS wins,
  ROUND(
    COUNT(*) FILTER (WHERE result = 'WIN')::numeric /
    NULLIF(COUNT(*) FILTER (WHERE result IN ('WIN','LOSS')), 0), 3
  )                                                  AS win_pct
FROM graded_occurrences
WHERE sport = '{sport}'
  AND result IN ('WIN', 'LOSS')
  AND {filter_clause}
GROUP BY yyyymm
ORDER BY yyyymm;
"""

QUERY_MAP = {
    "source":       QUERY_SOURCE_MARKET,
    "combo":        QUERY_COMBO_MARKET,
    "expert":       QUERY_EXPERT,
    "stat":         QUERY_STAT,
    "expert_pairs": QUERY_EXPERT_PAIRS,
    "recent":       None,  # runs two sub-queries (recent_source + recent_expert)
}


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def get_client():
    sys.path.insert(0, str(REPO_ROOT))
    from src.supabase_writer import get_client as _gc
    return _gc()


def run_query(sql: str) -> List[Dict[str, Any]]:
    client = get_client()
    resp = client.rpc("run_sql", {"query": sql}).execute()
    # Supabase Python SDK: use postgrest-py raw query for arbitrary SQL
    # Fall back to direct postgrest if rpc not configured
    if hasattr(resp, "data") and resp.data:
        return resp.data
    return []


def run_raw_sql(sql: str) -> List[Dict[str, Any]]:
    """Execute arbitrary SQL via the Supabase REST API using postgrest."""
    import os
    url = os.environ.get("SUPABASE_URL") or ""
    key = os.environ.get("SUPABASE_KEY") or ""
    if not url or not key:
        env_file = REPO_ROOT / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if "=" in line and not line.startswith("#"):
                    k, _, v = line.partition("=")
                    os.environ.setdefault(k.strip(), v.strip())
        url = os.environ.get("SUPABASE_URL", "")
        key = os.environ.get("SUPABASE_KEY", "")

    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_KEY not set")

    import urllib.request
    import json as _json

    # Strip trailing semicolon — Postgres EXECUTE chokes on it inside a function
    sql = sql.strip().rstrip(";").strip()

    rpc_url = f"{url}/rest/v1/rpc/execute_sql"
    payload = _json.dumps({"query": sql}).encode()
    req = urllib.request.Request(
        rpc_url,
        data=payload,
        headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return _json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        raise RuntimeError(f"Supabase SQL error {e.code}: {body}") from e


def query_via_supabase_client(sql: str) -> List[Dict[str, Any]]:
    """
    Run raw SQL against Supabase using the Python client's postgrest interface.
    This uses the /rest/v1/rpc/execute_sql endpoint — you must create this
    function in Supabase first (see print_setup_sql()).
    """
    try:
        return run_raw_sql(sql)
    except RuntimeError as e:
        if "404" in str(e) or "does not exist" in str(e):
            print("\n[ERROR] execute_sql RPC not found in Supabase.")
            print("Run this in the Supabase SQL editor first:\n")
            print_setup_sql()
            sys.exit(1)
        raise


def print_setup_sql():
    print("""
-- Run this once in Supabase SQL editor to enable raw SQL queries:
CREATE OR REPLACE FUNCTION execute_sql(query text)
RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  result json;
BEGIN
  EXECUTE 'SELECT json_agg(t) FROM (' || query || ') t' INTO result;
  RETURN COALESCE(result, '[]'::json);
END;
$$;
""")


def add_wilson(rows: List[Dict]) -> List[Dict]:
    for row in rows:
        wins = int(row.get("wins") or 0)
        n = int(row.get("n") or 0)
        row["wilson"] = round(wilson_lower(wins, n), 3)
    return rows


def tier_label(wilson: float) -> str:
    if wilson >= TIER_A_WILSON:
        return "A"
    if wilson >= TIER_B_WILSON:
        return "B"
    return "-"


def print_results(rows: List[Dict], title: str, min_wilson: float = TIER_B_WILSON) -> None:
    candidates = [r for r in rows if r.get("wilson", 0) >= min_wilson]
    print(f"\n{'='*72}")
    print(f"  {title}")
    print(f"  {len(candidates)} candidates (Wilson >= {min_wilson}) out of {len(rows)} groups")
    print(f"{'='*72}")
    if not candidates:
        print("  (none)")
        return

    # Determine key columns dynamically
    dim_cols = [k for k in candidates[0].keys()
                if k not in ("n", "wins", "losses", "win_pct", "wilson")]
    header = "  " + "  ".join(f"{c:<28}" for c in dim_cols) + f"  {'n':>5}  {'W-L':>9}  {'win%':>6}  {'wilson':>7}  tier"
    print(header)
    print("  " + "-" * (len(header) - 2))

    for r in sorted(candidates, key=lambda x: -x.get("wilson", 0)):
        dim_vals = "  ".join(f"{str(r.get(c, '') or ''):<28}" for c in dim_cols)
        wins = int(r.get("wins") or 0)
        losses = int(r.get("losses") or 0)
        n = int(r.get("n") or 0)
        wp = float(r.get("win_pct") or 0)
        w = float(r.get("wilson") or 0)
        tier = tier_label(w)
        print(f"  {dim_vals}  {n:>5}  {wins:>4}-{losses:<4}  {wp:>6.3f}  {w:>7.3f}  {tier}")


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def _temporal_check(rows: List[Dict]) -> str:
    """Return a concentration warning string, or '' if clean."""
    total = sum(int(r.get("n") or 0) for r in rows)
    if total == 0:
        return ""
    months_with_picks = [r for r in rows if int(r.get("n") or 0) > 0]
    if not months_with_picks:
        return ""
    top_month = max(months_with_picks, key=lambda r: int(r.get("n") or 0))
    top_pct = int(top_month.get("n") or 0) / total
    if top_pct >= 0.80:
        return f"  ⚠ CONCENTRATION: {top_pct:.0%} from {top_month.get('yyyymm')} — likely noise"
    if top_pct >= 0.60:
        return f"  ~ WARN: {top_pct:.0%} from {top_month.get('yyyymm')} — check manually"
    return f"  ✓ spread across {len(months_with_picks)} months (top month {top_pct:.0%})"


def main():
    parser = argparse.ArgumentParser(description="SQL-based pattern discovery via Supabase")
    parser.add_argument("--dimension",
                        choices=["source", "combo", "expert", "stat",
                                 "expert_pairs", "recent", "all"],
                        default="all", help="Which dimension to sweep (default: all)")
    parser.add_argument("--min-n", type=int, default=20,
                        help="Minimum graded picks for a group to show (default: 20)")
    parser.add_argument("--tier", choices=["A", "B", "any"], default="B",
                        help="Minimum tier to display: A=Wilson>=0.52, B>=0.46 (default: B)")
    parser.add_argument("--sport", default="NBA", help="Sport filter (default: NBA)")
    parser.add_argument("--sql", default=None,
                        help="Run a raw SQL query instead of the built-in sweep")
    parser.add_argument("--setup", action="store_true",
                        help="Print the Supabase SQL needed to enable raw queries, then exit")
    parser.add_argument("--temporal", default=None, metavar="FILTER",
                        help="Run temporal concentration check with a WHERE clause fragment. "
                             "Example: \"expert_name='Kyle Murray' AND atomic_stat='pts_reb' AND direction='UNDER'\"")
    args = parser.parse_args()

    if args.setup:
        print_setup_sql()
        return

    min_wilson = TIER_A_WILSON if args.tier == "A" else (TIER_B_WILSON if args.tier == "B" else 0.0)
    fmt_args = {"sport": args.sport, "min_n": args.min_n}

    # ── Raw SQL shortcut ──────────────────────────────────────────────────────
    if args.sql:
        print("Running custom SQL...")
        rows = query_via_supabase_client(args.sql)
        rows = add_wilson(rows) if "wins" in (rows[0] if rows else {}) else rows
        print_results(rows, "Custom Query", min_wilson)
        return

    # ── Temporal concentration shortcut ──────────────────────────────────────
    if args.temporal:
        sql = QUERY_TEMPORAL.format(sport=args.sport, filter_clause=args.temporal)
        print(f"Temporal check: {args.temporal}")
        rows = query_via_supabase_client(sql)
        total = sum(int(r.get("n") or 0) for r in rows)
        print(f"\n  {'Month':<8}  {'n':>4}  {'W':>4}  {'win%':>6}")
        print("  " + "-" * 28)
        for r in rows:
            n = int(r.get("n") or 0)
            w = int(r.get("wins") or 0)
            wp = float(r.get("win_pct") or 0)
            bar = "█" * int(n / max(total, 1) * 20)
            print(f"  {r.get('yyyymm','?'):<8}  {n:>4}  {w:>4}  {wp:>6.3f}  {bar}")
        print(_temporal_check(rows))
        return

    # ── Dimension sweeps ─────────────────────────────────────────────────────
    if args.dimension == "all":
        dimensions = ["source", "combo", "expert", "stat", "expert_pairs", "recent"]
    else:
        dimensions = [args.dimension]

    for dim in dimensions:
        print(f"\nQuerying Supabase: dimension={dim} sport={args.sport} min_n={args.min_n} ...")

        if dim == "recent":
            # Two sub-queries: recent source + recent expert
            for sub_q, label in [
                (QUERY_RECENT_SOURCE, "RECENT (60d) — SOURCE"),
                (QUERY_RECENT_EXPERT, "RECENT (60d) — EXPERT"),
            ]:
                sql = sub_q.format(**fmt_args)
                try:
                    rows = query_via_supabase_client(sql)
                except RuntimeError as e:
                    print(f"[ERROR] {e}")
                    continue
                rows = add_wilson(rows)
                print_results(rows, f"{label}  ({args.sport})", min_wilson)
            continue

        query_template = QUERY_MAP.get(dim)
        if query_template is None:
            continue
        sql = query_template.format(**fmt_args)
        try:
            rows = query_via_supabase_client(sql)
        except RuntimeError as e:
            print(f"[ERROR] {e}")
            continue
        rows = add_wilson(rows)
        print_results(rows, f"Dimension: {dim.upper()}  ({args.sport})", min_wilson)

    print("\nDone.")
    print("\nTips:")
    print("  Temporal check:  python3 scripts/pattern_analysis_supabase.py --temporal \"expert_name='Kyle Murray' AND atomic_stat='pts_reb' AND direction='UNDER'\"")
    print("  Expert pairs:    python3 scripts/pattern_analysis_supabase.py --dimension expert_pairs")
    print("  Recent 60d:      python3 scripts/pattern_analysis_supabase.py --dimension recent")
    print("  NCAAB sweep:     python3 scripts/pattern_analysis_supabase.py --sport NCAAB --min-n 10")


if __name__ == "__main__":
    main()
