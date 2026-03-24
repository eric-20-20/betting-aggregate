"""
Build expert_records and expert_pair_records tables in Supabase.

Reads directly from Supabase:
  - signal_sources (expert_result for player_prop)
  - signals + grades (result for spread/total/moneyline)

Usage:
    python3 scripts/build_pattern_analysis.py            # build + upsert
    python3 scripts/build_pattern_analysis.py --dry-run  # compute only, print summary
    python3 scripts/build_pattern_analysis.py --min-n 5  # lower threshold (default 1)
"""
from __future__ import annotations

import argparse
import logging
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("build_pattern_analysis")

SUPPORTED_MARKETS = {"player_prop", "spread", "total", "moneyline"}

# Tier thresholds for expert_pair_records (win_pct based)
TIER_MIN_N = 15

# Hot/cold: 30-day window, min 10 picks, abs deviation >= 0.08 from career
RECENT_DAYS   = 30
HOT_COLD_MIN_N = 10
HOT_COLD_DELTA = 0.08

# Supabase pagination
PAGE_SIZE  = 1000
BATCH_SIZE = 500


# ──────────────────────────────────────────────────────────────
# Statistics helpers
# ──────────────────────────────────────────────────────────────

def wilson_lower(wins: int, n: int, z: float = 1.96) -> Optional[float]:
    """Wilson score 95% confidence interval lower bound."""
    if n == 0:
        return None
    p = wins / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    spread = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return round((centre - spread) / denom, 4)


def _assign_tier(win_pct: Optional[float], n: int) -> Optional[str]:
    """Assign tier A/B/C based on win_pct + minimum n."""
    if win_pct is None or n < TIER_MIN_N:
        return None
    if win_pct >= 0.60:
        return "A"
    if win_pct >= 0.55:
        return "B"
    if win_pct >= 0.50:
        return "C"
    return None


def _parse_day_key_date(day_key: str) -> Optional[datetime]:
    """Parse 'NBA:2026:03:23' → datetime."""
    try:
        parts = day_key.split(":")
        return datetime(int(parts[1]), int(parts[2]), int(parts[3]), tzinfo=timezone.utc)
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────
# Fetch occurrences from Supabase
# ──────────────────────────────────────────────────────────────

def _fetch_all(client, table: str, select: str, filters: list = None) -> List[dict]:
    """Paginate through a Supabase table query."""
    rows: List[dict] = []
    offset = 0
    while True:
        q = client.table(table).select(select)
        if filters:
            for method, col, val in filters:
                q = getattr(q, method)(col, val)
        resp = q.order("id" if table == "signal_sources" else "signal_id") \
                .range(offset, offset + PAGE_SIZE - 1) \
                .execute()
        batch = resp.data or []
        rows.extend(batch)
        logger.debug("  %s: fetched %d so far", table, len(rows))
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return rows


def fetch_prop_occurrences(client) -> List[dict]:
    """
    Fetch per-expert graded player_prop occurrences from signal_sources + signals.

    SELECT ss.source_id, ss.expert_slug, ss.expert_name, ss.expert_result, ss.line,
           s.market_type, s.atomic_stat, s.day_key, s.signal_id
    FROM signal_sources ss
    JOIN signals s ON s.signal_id = ss.signal_id
    WHERE ss.expert_result IN ('WIN','LOSS','PUSH')
      AND ss.expert_slug IS NOT NULL
      AND s.market_type = 'player_prop'
    """
    logger.info("Fetching player_prop occurrences from signal_sources...")
    rows: List[dict] = []
    offset = 0
    while True:
        resp = (
            client.table("signal_sources")
            .select(
                "source_id,expert_slug,expert_name,expert_result,line,"
                "signals(signal_id,market_type,atomic_stat,day_key)"
            )
            .in_("expert_result", ["WIN", "LOSS", "PUSH"])
            .not_.is_("expert_slug", "null")
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        batch = resp.data or []
        for row in batch:
            sig = row.get("signals") or {}
            if sig.get("market_type") != "player_prop":
                continue
            rows.append({
                "source_id":   row["source_id"],
                "expert_slug": row["expert_slug"],
                "expert_name": row.get("expert_name") or row["expert_slug"],
                "result":      row["expert_result"],
                "market_type": "player_prop",
                "atomic_stat": sig.get("atomic_stat") or "",
                "day_key":     sig.get("day_key") or "",
                "signal_id":   sig.get("signal_id") or "",
            })
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        if offset % 5000 == 0:
            logger.info("  ... fetched %d prop rows so far", len(rows))

    logger.info("  %d player_prop expert occurrences", len(rows))
    return rows


def fetch_nonprop_occurrences(client) -> List[dict]:
    """
    Fetch spread/total/moneyline occurrences: one row per (expert, signal).

    SELECT ss.source_id, ss.expert_slug, ss.expert_name,
           s.signal_id, s.market_type, s.atomic_stat, s.day_key,
           g.result
    FROM signal_sources ss
    JOIN signals s ON s.signal_id = ss.signal_id
    JOIN grades  g ON g.signal_id = s.signal_id
    WHERE g.result IN ('WIN','LOSS','PUSH')
      AND ss.expert_slug IS NOT NULL
      AND s.market_type IN ('spread','total','moneyline')
    """
    logger.info("Fetching spread/total/moneyline occurrences from signal_sources...")
    rows: List[dict] = []
    offset = 0
    while True:
        resp = (
            client.table("signal_sources")
            .select(
                "source_id,expert_slug,expert_name,"
                "signals(signal_id,market_type,atomic_stat,day_key,"
                "grades(result))"
            )
            .not_.is_("expert_slug", "null")
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        batch = resp.data or []
        for row in batch:
            sig = row.get("signals") or {}
            mt = sig.get("market_type")
            if mt not in ("spread", "total", "moneyline"):
                continue
            grade = sig.get("grades") or {}
            result = grade.get("result")
            if result not in ("WIN", "LOSS", "PUSH"):
                continue
            rows.append({
                "source_id":   row["source_id"],
                "expert_slug": row["expert_slug"],
                "expert_name": row.get("expert_name") or row["expert_slug"],
                "result":      result,
                "market_type": mt,
                "atomic_stat": "",  # not applicable for non-props
                "day_key":     sig.get("day_key") or "",
                "signal_id":   sig.get("signal_id") or "",
            })
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        if offset % 5000 == 0:
            logger.info("  ... fetched %d non-prop rows so far", len(rows))

    logger.info("  %d spread/total/moneyline expert occurrences", len(rows))
    return rows


# ──────────────────────────────────────────────────────────────
# Build solo expert_records
# ──────────────────────────────────────────────────────────────

def build_expert_records(occurrences: List[dict], today: datetime) -> List[dict]:
    """
    Aggregate occurrences into expert_records rows.
    Hot/cold based on last-30-day win_pct vs career win_pct.
    """
    groups: Dict[Tuple, dict] = {}
    recent: Dict[Tuple, dict] = {}

    for occ in occurrences:
        key = (occ["source_id"], occ["expert_slug"], occ["market_type"], occ["atomic_stat"])

        if key not in groups:
            groups[key] = {"wins": 0, "losses": 0, "pushes": 0,
                           "expert_name": occ.get("expert_name") or occ["expert_slug"]}
        g = groups[key]
        if occ["result"] == "WIN":
            g["wins"] += 1
        elif occ["result"] == "LOSS":
            g["losses"] += 1
        else:
            g["pushes"] += 1

        # 30-day window
        occ_date = _parse_day_key_date(occ.get("day_key", ""))
        if occ_date and (today - occ_date).days <= RECENT_DAYS:
            if key not in recent:
                recent[key] = {"wins": 0, "losses": 0}
            if occ["result"] == "WIN":
                recent[key]["wins"] += 1
            elif occ["result"] == "LOSS":
                recent[key]["losses"] += 1

    ts = datetime.now(tz=timezone.utc).isoformat()
    rows: List[dict] = []

    for key, g in groups.items():
        src, slug, mt, atomic = key
        wins = g["wins"]
        losses = g["losses"]
        total = wins + losses
        wp = round(wins / total, 4) if total > 0 else None
        wl = wilson_lower(wins, total)

        rg = recent.get(key, {"wins": 0, "losses": 0})
        r30_wins = rg["wins"]
        r30_losses = rg["losses"]
        r30_n = r30_wins + r30_losses
        r30_wp = r30_wins / r30_n if r30_n > 0 else None

        # hot/cold: 30d min n, abs deviation >= 0.08 from career
        hot = cold = False
        if r30_n >= HOT_COLD_MIN_N and wp is not None and r30_wp is not None:
            delta = r30_wp - wp
            if abs(delta) >= HOT_COLD_DELTA:
                hot = delta > 0
                cold = delta < 0

        rows.append({
            "source_id":    src,
            "expert_slug":  slug,
            "expert_name":  g["expert_name"],
            "market_type":  mt,
            "atomic_stat":  atomic,
            "n":            total + g["pushes"],
            "wins":         wins,
            "losses":       losses,
            "pushes":       g["pushes"],
            "win_pct":      wp,
            "wilson_lower": wl,
            "hot":          hot,
            "cold":         cold,
            "last_14_n":    r30_n,    # column still named last_14_n in schema
            "last_14_wins": r30_wins, # column still named last_14_wins in schema
            "updated_at":   ts,
        })

    return rows


# ──────────────────────────────────────────────────────────────
# Build expert_pair_records
# ──────────────────────────────────────────────────────────────

def build_pair_records(occurrences: List[dict]) -> List[dict]:
    """
    For each signal with 2+ experts, emit all (A, B) combos.
    Pair result: WIN if both WIN, LOSS if either LOSS, else skip.
    """
    # Deduplicate occurrences by (signal_id, source_id, expert_slug) — keep first
    dedup: Dict[Tuple, dict] = {}
    for occ in occurrences:
        key = (occ["signal_id"], occ["source_id"], occ["expert_slug"])
        if key not in dedup:
            dedup[key] = occ

    by_signal: Dict[str, List[dict]] = defaultdict(list)
    for occ in dedup.values():
        by_signal[occ["signal_id"]].append(occ)

    pair_groups: Dict[Tuple, dict] = {}

    for sid, occs in by_signal.items():
        if len(occs) < 2:
            continue
        mt     = occs[0]["market_type"]
        atomic = occs[0]["atomic_stat"]

        for i in range(len(occs)):
            for j in range(i + 1, len(occs)):
                a = occs[i]
                b = occs[j]

                # Enforce canonical ordering
                if (a["source_id"], a["expert_slug"]) > (b["source_id"], b["expert_slug"]):
                    a, b = b, a

                ra, rb = a["result"], b["result"]
                if ra == "LOSS" or rb == "LOSS":
                    pair_result = "LOSS"
                elif ra == "WIN" and rb == "WIN":
                    pair_result = "WIN"
                else:
                    continue  # skip PUSH-only outcomes

                key = (a["source_id"], a["expert_slug"],
                       b["source_id"], b["expert_slug"], mt, atomic)
                if key not in pair_groups:
                    pair_groups[key] = {"wins": 0, "losses": 0, "pushes": 0}
                pg = pair_groups[key]
                if pair_result == "WIN":
                    pg["wins"] += 1
                else:
                    pg["losses"] += 1

    ts = datetime.now(tz=timezone.utc).isoformat()
    rows: List[dict] = []

    for key, pg in pair_groups.items():
        sa, sla, sb, slb, mt, atomic = key
        wins   = pg["wins"]
        losses = pg["losses"]
        total  = wins + losses
        wp  = round(wins / total, 4) if total > 0 else None
        wl  = wilson_lower(wins, total)
        tier = _assign_tier(wp, total)

        rows.append({
            "source_a":      sa,
            "expert_slug_a": sla,
            "source_b":      sb,
            "expert_slug_b": slb,
            "market_type":   mt,
            "atomic_stat":   atomic,
            "n":             total,
            "wins":          wins,
            "losses":        losses,
            "pushes":        pg["pushes"],
            "win_pct":       wp,
            "wilson_lower":  wl,
            "tier":          tier,
            "updated_at":    ts,
        })

    return rows


# ──────────────────────────────────────────────────────────────
# Supabase upsert
# ──────────────────────────────────────────────────────────────

def _upsert(client, table: str, rows: List[dict], conflict_cols: str) -> int:
    if not rows:
        return 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        client.table(table).upsert(batch, on_conflict=conflict_cols).execute()
    return len(rows)


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Build expert_records and expert_pair_records in Supabase"
    )
    ap.add_argument("--dry-run", action="store_true",
                    help="Compute only; do not write to Supabase")
    ap.add_argument("--min-n",  type=int, default=1,
                    help="Minimum occurrences to include (default: 1)")
    ap.add_argument("--today",  default=None,
                    help="Override today's date key (NBA:YYYY:MM:DD)")
    args = ap.parse_args()

    from src.supabase_writer import get_client
    client = get_client()

    # Fetch all graded expert occurrences from Supabase
    prop_occs    = fetch_prop_occurrences(client)
    nonprop_occs = fetch_nonprop_occurrences(client)
    occurrences  = prop_occs + nonprop_occs
    logger.info("Total graded expert occurrences: %d", len(occurrences))

    # Determine today for hot/cold window
    if args.today:
        today = _parse_day_key_date(args.today) or datetime.now(tz=timezone.utc)
    else:
        today = datetime.now(tz=timezone.utc)

    logger.info("Building expert_records (today=%s, window=%dd)...",
                today.strftime("%Y-%m-%d"), RECENT_DAYS)
    expert_rows = build_expert_records(occurrences, today)
    if args.min_n > 1:
        expert_rows = [r for r in expert_rows if r["n"] >= args.min_n]
    logger.info("  %d expert_records rows", len(expert_rows))

    logger.info("Building expert_pair_records...")
    pair_rows = build_pair_records(occurrences)
    if args.min_n > 1:
        pair_rows = [r for r in pair_rows if r["n"] >= args.min_n]
    logger.info("  %d expert_pair_records rows", len(pair_rows))

    # Summary stats
    tier_counts = {"A": 0, "B": 0, "C": 0, None: 0}
    for r in pair_rows:
        tier_counts[r["tier"]] = tier_counts.get(r["tier"], 0) + 1
    logger.info("  Pair tiers — A:%d  B:%d  C:%d  unrated:%d",
                tier_counts.get("A", 0), tier_counts.get("B", 0),
                tier_counts.get("C", 0), tier_counts.get(None, 0))

    hot_experts  = [r for r in expert_rows if r["hot"]]
    cold_experts = [r for r in expert_rows if r["cold"]]
    logger.info("  Hot experts: %d, Cold experts: %d", len(hot_experts), len(cold_experts))

    if args.dry_run:
        logger.info("Dry-run — not writing to Supabase.")

        print("\n--- Top 20 expert pairs by win_pct (n >= 15) ---")
        top_pairs = sorted(
            [r for r in pair_rows if (r["n"] or 0) >= 15],
            key=lambda r: (r["win_pct"] or 0),
            reverse=True,
        )[:20]
        hdr = f"{'source_a':28s} {'slug_a':28s} {'source_b':28s} {'slug_b':28s} {'market':12s} {'stat':12s}  {'n':>4}   W%    WL   tier"
        print(hdr)
        print("-" * len(hdr))
        for r in top_pairs:
            wp  = f"{r['win_pct']:.3f}"  if r["win_pct"]      else "  —  "
            wl  = f"{r['wilson_lower']:.3f}" if r["wilson_lower"] else "  —  "
            print(f"{r['source_a']:28s} {r['expert_slug_a']:28s} {r['source_b']:28s} {r['expert_slug_b']:28s} "
                  f"{r['market_type']:12s} {r['atomic_stat']:12s}  {r['n']:>4}  {wp}  {wl}  {r['tier'] or ''}")

        print("\n--- Top 20 hot experts (30-day) ---")
        top_hot = sorted(
            hot_experts,
            key=lambda r: r["last_14_wins"] / max(r["last_14_n"], 1),
            reverse=True,
        )[:20]
        for r in top_hot:
            r30_wp = f"{r['last_14_wins']/r['last_14_n']:.3f}" if r["last_14_n"] else "—"
            career = f"{r['win_pct']:.3f}" if r["win_pct"] else "—"
            print(f"  {r['source_id']:28s} {r['expert_slug']:30s} {r['market_type']:12s} {r['atomic_stat']:12s}"
                  f"  30d={r['last_14_n']}:{r30_wp}  career={career}")
        return

    # Write to Supabase
    logger.info("Upserting %d expert_records rows...", len(expert_rows))
    n1 = _upsert(client, "expert_records", expert_rows,
                 "source_id,expert_slug,market_type,atomic_stat")
    logger.info("  Done: %d rows upserted", n1)

    logger.info("Upserting %d expert_pair_records rows...", len(pair_rows))
    n2 = _upsert(client, "expert_pair_records", pair_rows,
                 "source_a,expert_slug_a,source_b,expert_slug_b,market_type,atomic_stat")
    logger.info("  Done: %d rows upserted", n2)

    logger.info("build_pattern_analysis complete. Total rows written: %d", n1 + n2)


if __name__ == "__main__":
    main()
