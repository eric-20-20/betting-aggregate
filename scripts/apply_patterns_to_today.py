"""
Apply expert pair patterns to today's signals and rank plays.

Reads today's signals from data/ledger/signals_latest.jsonl (filtered by today's day_key),
looks up expert_pair_records and expert_records from Supabase (or local build if --local),
and outputs a ranked pick list.

Output:
  - Console table of ranked plays
  - data/daily_picks/YYYY-MM-DD_ranked_picks.txt (text file)

Usage:
    python3 scripts/apply_patterns_to_today.py                  # full blended mode (default)
    python3 scripts/apply_patterns_to_today.py --mode pair      # pair history only
    python3 scripts/apply_patterns_to_today.py --mode full      # blended pair + solo convergence
    python3 scripts/apply_patterns_to_today.py --date 2026-03-23
    python3 scripts/apply_patterns_to_today.py --dry-run        # print only, no file write
    python3 scripts/apply_patterns_to_today.py --local          # build patterns locally, skip Supabase
    python3 scripts/apply_patterns_to_today.py --min-pair-n 5   # lower pair n threshold (default 3)
"""
from __future__ import annotations

import argparse
import json
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
logger = logging.getLogger("apply_patterns_to_today")

SIGNALS_FILE  = REPO_ROOT / "data" / "ledger" / "signals_latest.jsonl"
PICKS_OUT_DIR = REPO_ROOT / "data" / "daily_picks"

SUPPORTED_MARKETS = {"player_prop", "spread", "total", "moneyline"}

DEFAULT_MIN_PAIR_N  = 3
SOLO_MIN_N          = 10   # minimum expert career n for solo_convergence
PAIR_MIN_N_BLEND    = 15   # minimum pair n to use pair history in blend
PAIR_WEIGHT_SCALE   = 50   # pair_weight = min(0.70, pair_n / PAIR_WEIGHT_SCALE)
PAIR_WEIGHT_MAX     = 0.70


# ──────────────────────────────────────────────────────────────
# Statistics
# ──────────────────────────────────────────────────────────────

def wilson_lower(wins: int, n: int, z: float = 1.96) -> Optional[float]:
    if n == 0:
        return None
    p = wins / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    spread = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return round((centre - spread) / denom, 4)


def _assign_tier(score: Optional[float]) -> Optional[str]:
    if score is None:
        return None
    if score >= 0.60:
        return "A"
    if score >= 0.55:
        return "B"
    if score >= 0.50:
        return "C"
    return None


def _expert_slug(expert_name: str) -> str:
    return expert_name.lower().replace(" ", "_").replace(".", "").replace("-", "_")


# ──────────────────────────────────────────────────────────────
# Load today's signals
# ──────────────────────────────────────────────────────────────

def load_today_signals(day_key: str) -> List[dict]:
    """Return signals matching day_key that have at least one expert_name in supports."""
    results = []
    with open(SIGNALS_FILE) as f:
        for line in f:
            if not line.strip():
                continue
            r = json.loads(line)
            if (r.get("day_key") or "") != day_key:
                continue
            mt = r.get("market_type")
            if mt not in SUPPORTED_MARKETS:
                continue
            experts = []
            seen = set()
            for sup in (r.get("supports") or []):
                en = sup.get("expert_name")
                if not en:
                    continue
                slug = _expert_slug(en)
                src  = sup.get("source_id") or ""
                if (src, slug) in seen:
                    continue
                seen.add((src, slug))
                experts.append({
                    "source_id":   src,
                    "expert_name": en,
                    "expert_slug": slug,
                    "line":        sup.get("line"),
                })
            if not experts:
                continue
            r["_experts"] = experts
            results.append(r)
    return results


# ──────────────────────────────────────────────────────────────
# Load records from Supabase
# ──────────────────────────────────────────────────────────────

PAGE_SIZE = 1000


def _fetch_all(client, table: str, select: str = "*") -> List[dict]:
    rows: List[dict] = []
    offset = 0
    while True:
        resp = (
            client.table(table)
            .select(select)
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        batch = resp.data or []
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return rows


def load_pair_records_supabase(client, min_n: int) -> Dict[Tuple, dict]:
    logger.info("Fetching expert_pair_records from Supabase...")
    rows = _fetch_all(client, "expert_pair_records")
    logger.info("  %d pair records fetched", len(rows))
    result: Dict[Tuple, dict] = {}
    for r in rows:
        if (r.get("n") or 0) < min_n:
            continue
        key = (r["source_a"], r["expert_slug_a"], r["source_b"], r["expert_slug_b"],
               r["market_type"], r.get("atomic_stat") or "")
        result[key] = r
    return result


def load_expert_records_supabase(client) -> Dict[Tuple, dict]:
    logger.info("Fetching expert_records from Supabase...")
    rows = _fetch_all(client, "expert_records")
    logger.info("  %d expert records fetched", len(rows))
    result: Dict[Tuple, dict] = {}
    for r in rows:
        key = (r["source_id"], r["expert_slug"], r["market_type"], r.get("atomic_stat") or "")
        result[key] = r
    return result


# ──────────────────────────────────────────────────────────────
# Load records locally (no Supabase)
# ──────────────────────────────────────────────────────────────

def load_records_local(day_key: str, min_n: int) -> Tuple[Dict, Dict]:
    """Build records in-memory from local ledger files (skips Supabase)."""
    import json as _json
    from scripts.build_pattern_analysis import (
        build_expert_records, build_pair_records,
    )

    SIGNALS_F = REPO_ROOT / "data" / "ledger" / "signals_latest.jsonl"
    GRADES_F  = REPO_ROOT / "data" / "ledger" / "grades_latest.jsonl"

    logger.info("Building records locally from ledger (excluding today)...")

    grades: Dict[str, dict] = {}
    with open(GRADES_F) as f:
        for line in f:
            if line.strip():
                r = _json.loads(line)
                grades[r["signal_id"]] = r

    # Build occurrences inline (same logic as build_pattern_analysis.build_occurrences)
    SUPPORTED = {"player_prop", "spread", "total", "moneyline"}
    occs: List[dict] = []
    with open(SIGNALS_F) as f:
        for line in f:
            if not line.strip():
                continue
            s = _json.loads(line)
            if (s.get("day_key") or "") == day_key:
                continue  # exclude today
            g = grades.get(s["signal_id"])
            if not g or g.get("result") not in ("WIN", "LOSS", "PUSH"):
                continue
            mt = s.get("market_type")
            if mt not in SUPPORTED:
                continue
            atomic = s.get("atomic_stat") or "" if mt == "player_prop" else ""
            seen: set = set()
            for sup in (s.get("supports") or []):
                en = sup.get("expert_name")
                if not en:
                    continue
                slug = _expert_slug(en)
                src  = sup.get("source_id") or ""
                if (src, slug) in seen:
                    continue
                seen.add((src, slug))
                if mt == "player_prop":
                    sv   = g.get("stat_value") or g.get("actual_stat_value")
                    line_s = sup.get("line")
                    lv   = line_s if isinstance(line_s, (int, float)) else s.get("line")
                    dir_ = (s.get("direction") or "").upper()
                    if sv is not None and isinstance(lv, (int, float)) and dir_ in ("OVER","UNDER"):
                        result = ("WIN" if sv > lv else ("PUSH" if sv == lv else "LOSS")) \
                                 if "OVER" in dir_ else \
                                 ("WIN" if sv < lv else ("PUSH" if sv == lv else "LOSS"))
                    else:
                        result = g.get("result")
                else:
                    result = g.get("result")
                if result not in ("WIN","LOSS","PUSH"):
                    continue
                occs.append({
                    "source_id": src, "expert_slug": slug,
                    "expert_name": en, "result": result,
                    "market_type": mt, "atomic_stat": atomic,
                    "day_key": s.get("day_key") or "",
                    "signal_id": s["signal_id"],
                })

    today_dt = datetime.now(tz=timezone.utc)
    er_list  = build_expert_records(occs, today_dt)
    pr_list  = build_pair_records(occs)

    pair_lookup: Dict[Tuple, dict] = {}
    for r in pr_list:
        if (r.get("n") or 0) < min_n:
            continue
        key = (r["source_a"], r["expert_slug_a"], r["source_b"], r["expert_slug_b"],
               r["market_type"], r.get("atomic_stat") or "")
        pair_lookup[key] = r

    expert_lookup: Dict[Tuple, dict] = {}
    for r in er_list:
        key = (r["source_id"], r["expert_slug"], r["market_type"], r.get("atomic_stat") or "")
        expert_lookup[key] = r

    logger.info("  Built %d pair records, %d expert records", len(pair_lookup), len(expert_lookup))
    return pair_lookup, expert_lookup


# ──────────────────────────────────────────────────────────────
# Scoring
# ──────────────────────────────────────────────────────────────

def _atomic(signal: dict) -> str:
    if signal.get("market_type") == "player_prop":
        return signal.get("atomic_stat") or ""
    return ""


def _solo_convergence(
    experts: List[dict],
    mt: str,
    atomic: str,
    expert_lookup: Dict[Tuple, dict],
) -> Tuple[Optional[float], List[dict]]:
    """
    Compute geometric mean of all experts' wilson_lower for this market.
    Only include experts with career n >= SOLO_MIN_N.
    Returns (score, [per_expert_detail]).
    """
    detail: List[dict] = []
    for exp in experts:
        key = (exp["source_id"], exp["expert_slug"], mt, atomic)
        er  = expert_lookup.get(key)
        if not er:
            # Also try market-level key (no atomic_stat) for non-props
            if atomic:
                er = expert_lookup.get((exp["source_id"], exp["expert_slug"], mt, ""))
        career_n = (er.get("wins", 0) + er.get("losses", 0)) if er else 0
        detail.append({
            "source_id":   exp["source_id"],
            "expert_slug": exp["expert_slug"],
            "expert_name": exp["expert_name"],
            "n":           career_n,
            "wins":        er.get("wins", 0) if er else 0,
            "losses":      er.get("losses", 0) if er else 0,
            "win_pct":     er.get("win_pct") if er else None,
            "wilson_lower": er.get("wilson_lower") if er else None,
            "hot":         bool(er.get("hot")) if er else False,
            "cold":        bool(er.get("cold")) if er else False,
        })

    # Only use experts with sufficient history
    eligible = [d for d in detail if d["n"] >= SOLO_MIN_N and d["wilson_lower"] is not None]
    if len(eligible) < 2:
        return None, detail

    # Geometric mean of wilson_lower values
    product = 1.0
    for d in eligible:
        product *= d["wilson_lower"]
    geo_mean = product ** (1.0 / len(eligible))
    return round(geo_mean, 4), detail


def score_signal(
    signal: dict,
    pair_lookup: Dict[Tuple, dict],
    expert_lookup: Dict[Tuple, dict],
    mode: str = "full",
) -> dict:
    """
    Score a signal.  mode='pair' uses pair history only (original behavior).
    mode='full' blends pair history with solo convergence.
    """
    experts = signal.get("_experts", [])
    mt      = signal.get("market_type") or ""
    atomic  = _atomic(signal)

    # ── 1. Pair history ──────────────────────────────────────
    matched_pairs: List[dict] = []
    for i in range(len(experts)):
        for j in range(i + 1, len(experts)):
            a, b = experts[i], experts[j]
            if (a["source_id"], a["expert_slug"]) > (b["source_id"], b["expert_slug"]):
                a, b = b, a
            key = (a["source_id"], a["expert_slug"], b["source_id"], b["expert_slug"], mt, atomic)
            pr  = pair_lookup.get(key)
            if not pr:
                continue
            matched_pairs.append({
                "slug_a":       a["expert_slug"],
                "slug_b":       b["expert_slug"],
                "source_a":     a["source_id"],
                "source_b":     b["source_id"],
                "n":            pr["n"],
                "win_pct":      pr.get("win_pct"),
                "wilson_lower": pr.get("wilson_lower"),
                "tier":         pr.get("tier"),
            })

    # Best pair (by win_pct then wilson_lower)
    best_pair: Optional[dict] = None
    if matched_pairs:
        best_pair = max(matched_pairs,
                        key=lambda p: (p["win_pct"] or 0, p["wilson_lower"] or 0))

    # ── 2. Solo convergence ──────────────────────────────────
    solo_score, expert_detail = _solo_convergence(experts, mt, atomic, expert_lookup)

    # ── 3. Final score + source label ────────────────────────
    if mode == "pair":
        # Legacy: use best pair wilson_lower + hot/cold adjustments
        if not matched_pairs:
            final_score = None
            scoring_source = "insufficient_data"
        else:
            sorted_pairs = sorted(matched_pairs,
                                  key=lambda p: (p["wilson_lower"] or 0), reverse=True)
            base = sorted_pairs[0]["wilson_lower"] or 0.0
            for k, extra in enumerate(sorted_pairs[1:], 1):
                base += (extra["wilson_lower"] or 0) * 0.1 / k
            # hot/cold adj
            adj = 0.0
            for d in expert_detail:
                if d["hot"]:
                    adj += 0.05
                elif d["cold"]:
                    adj -= 0.05
            adj = max(-0.10, min(0.10, adj))
            final_score = round(base + adj, 4)
            scoring_source = "pair_history_only"

    else:  # mode == "full"
        pair_n = best_pair["n"] if best_pair else 0
        has_pair = best_pair is not None and pair_n >= PAIR_MIN_N_BLEND

        if has_pair and solo_score is not None:
            pair_weight = min(PAIR_WEIGHT_MAX, pair_n / PAIR_WEIGHT_SCALE)
            solo_weight = 1.0 - pair_weight
            final_score = round(
                (best_pair["win_pct"] or 0) * pair_weight + solo_score * solo_weight, 4
            )
            scoring_source = "blended"
        elif solo_score is not None:
            final_score = solo_score
            scoring_source = "solo_convergence"
        elif has_pair:
            # pair exists but not enough solo data — use pair win_pct directly
            final_score = best_pair.get("win_pct")
            scoring_source = "pair_history_only"
        else:
            final_score = None
            scoring_source = "insufficient_data"

    tier = _assign_tier(final_score)

    return {
        "signal_id":       signal["signal_id"],
        "final_score":     final_score,
        "scoring_source":  scoring_source,
        "solo_score":      solo_score,
        "best_pair":       best_pair,
        "matched_pairs":   matched_pairs,
        "expert_detail":   expert_detail,
        "tier":            tier,
        # kept for backward compat with JSON output
        "composite_score": final_score or 0.0,
        "n_pairs_matched": len(matched_pairs),
    }


# ──────────────────────────────────────────────────────────────
# Formatting
# ──────────────────────────────────────────────────────────────

def _team_label(signal: dict) -> str:
    away = signal.get("away_team") or ""
    home = signal.get("home_team") or ""
    if away and home:
        return f"{away}@{home}"
    ek = signal.get("event_key") or ""
    parts = ek.split(":")
    return parts[-1] if parts else ek


def _pick_label(signal: dict) -> str:
    sel       = signal.get("selection") or ""
    direction = signal.get("direction") or ""
    line      = signal.get("line")
    mt        = signal.get("market_type") or ""
    atomic    = signal.get("atomic_stat") or ""

    parts = sel.split("::")
    if len(parts) >= 3 and mt == "player_prop":
        player_raw = parts[0].split(":")[-1].replace("_", " ").title()
        return f"{player_raw} {atomic.upper()} {direction} {line}"
    elif mt in ("spread", "moneyline"):
        return f"{direction} {line if line else ''}"
    elif mt == "total":
        return f"Total {direction} {line}"
    return sel


def _wp(v: Optional[float]) -> str:
    return f"{v:.3f}" if v is not None else "  —  "


def _wl(v: Optional[float]) -> str:
    return f"{v:.3f}" if v is not None else "  —  "


def format_table(scored: List[dict], signals_map: Dict[str, dict], mode: str) -> str:
    lines: List[str] = []

    for rank, item in enumerate(scored, 1):
        sig          = signals_map.get(item["signal_id"], {})
        tier_str     = item["tier"] or "—"
        score_str    = f"{item['final_score']:.4f}" if item["final_score"] is not None else "    —"
        game_str     = _team_label(sig)[:14]
        pick_str     = _pick_label(sig)[:44]
        src_str      = item["scoring_source"]

        # ── Header row ──────────────────────────────────────
        lines.append(
            f"{rank:>3}. [{tier_str}]  {score_str}  {game_str:<14}  {pick_str:<44}  [{src_str}]"
        )

        # ── Expert detail ────────────────────────────────────
        for d in item["expert_detail"]:
            hot_cold = " 🔥" if d["hot"] else (" ❄️" if d["cold"] else "")
            if d["n"] >= SOLO_MIN_N:
                solo_wl = f"WL={_wl(d['wilson_lower'])}"
                rec_str = f"{d['wins']}-{d['losses']}  {_wp(d['win_pct'])}  {solo_wl}{hot_cold}"
            else:
                rec_str = f"{d['wins']}-{d['losses']}  (n<{SOLO_MIN_N}, excluded from solo){hot_cold}"
            lines.append(
                f"     ↳ {d['source_id']:24s}  {d['expert_slug']:30s}  {rec_str}"
            )

        # ── Solo convergence score ───────────────────────────
        solo = item["solo_score"]
        lines.append(
            f"     solo_convergence_score = {_wl(solo)}"
        )

        # ── Pair record (if any) ─────────────────────────────
        bp = item["best_pair"]
        if bp:
            lines.append(
                f"     pair_record  [{bp['source_a']}]{bp['slug_a']} + "
                f"[{bp['source_b']}]{bp['slug_b']}  "
                f"n={bp['n']}  W={_wp(bp['win_pct'])}  WL={_wl(bp['wilson_lower'])}  "
                f"tier={bp['tier'] or '—'}"
            )
            if mode == "full" and bp["n"] >= PAIR_MIN_N_BLEND and solo is not None:
                pw = min(PAIR_WEIGHT_MAX, bp["n"] / PAIR_WEIGHT_SCALE)
                sw = 1.0 - pw
                lines.append(
                    f"     blend  pair_weight={pw:.2f}  solo_weight={sw:.2f}  "
                    f"=> {(bp['win_pct'] or 0)*pw:.4f} + {solo*sw:.4f} = {item['final_score']:.4f}"
                )
        else:
            lines.append("     pair_record  none")

        lines.append("")  # blank separator

    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Apply expert patterns to today's signals")
    ap.add_argument("--date",       default=None,
                    help="Date YYYY-MM-DD (default: today)")
    ap.add_argument("--dry-run",    action="store_true",
                    help="Print only; do not write files")
    ap.add_argument("--local",      action="store_true",
                    help="Build records locally, skip Supabase fetch")
    ap.add_argument("--min-pair-n", type=int, default=DEFAULT_MIN_PAIR_N,
                    help=f"Min pair n to load (default {DEFAULT_MIN_PAIR_N})")
    ap.add_argument("--min-score",  type=float, default=0.0,
                    help="Only show picks with final_score >= this value")
    ap.add_argument("--mode",       choices=["pair", "full"], default="full",
                    help="'pair' = pair history only; 'full' = blended (default)")
    args = ap.parse_args()

    # Resolve date
    if args.date:
        try:
            dt = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            logger.error("Invalid date format: %s (expected YYYY-MM-DD)", args.date)
            sys.exit(1)
    else:
        dt = datetime.now(tz=timezone.utc)

    day_key  = f"NBA:{dt.year}:{dt.month:02d}:{dt.day:02d}"
    date_str = dt.strftime("%Y-%m-%d")

    logger.info("Applying patterns (%s mode) to %s", args.mode, date_str)

    if not SIGNALS_FILE.exists():
        logger.error("Signals file not found: %s", SIGNALS_FILE)
        sys.exit(1)

    signals_today = load_today_signals(day_key)
    logger.info("  %d signals with expert names for %s", len(signals_today), date_str)

    if not signals_today:
        print(f"No expert-backed signals found for {date_str}.")
        return

    signals_map = {s["signal_id"]: s for s in signals_today}

    # Load pair/expert records
    if args.local:
        pair_lookup, expert_lookup = load_records_local(day_key, args.min_pair_n)
    else:
        from src.supabase_writer import get_client
        client = get_client()
        pair_lookup   = load_pair_records_supabase(client, args.min_pair_n)
        expert_lookup = load_expert_records_supabase(client)

    logger.info("  %d usable pair records, %d expert records",
                len(pair_lookup), len(expert_lookup))

    # Score
    scored_list: List[dict] = []
    for sig in signals_today:
        result = score_signal(sig, pair_lookup, expert_lookup, mode=args.mode)
        if result["scoring_source"] == "insufficient_data":
            continue
        if result["final_score"] is None:
            continue
        if result["final_score"] < args.min_score:
            continue
        scored_list.append(result)

    # Sort: tier A→C→unrated, then score descending
    tier_order = {"A": 0, "B": 1, "C": 2, None: 3}
    scored_list.sort(key=lambda r: (tier_order.get(r["tier"], 3), -(r["final_score"] or 0)))

    logger.info("  %d scoreable picks", len(scored_list))

    if not scored_list:
        print(f"No scoreable picks found for {date_str}.")
        return

    # Tier counts
    tier_counts = {t: sum(1 for r in scored_list if r["tier"] == t)
                   for t in ("A", "B", "C")}
    src_counts  = {}
    for r in scored_list:
        src_counts[r["scoring_source"]] = src_counts.get(r["scoring_source"], 0) + 1

    header = (
        f"\n{'='*80}\n"
        f"  Expert Pattern Picks — {date_str}  [mode={args.mode}]\n"
        f"  {len(scored_list)} picks  "
        f"A:{tier_counts['A']}  B:{tier_counts['B']}  C:{tier_counts['C']}  "
        f"| " + "  ".join(f"{k}:{v}" for k, v in src_counts.items()) + "\n"
        f"{'='*80}\n"
    )

    body   = format_table(scored_list, signals_map, mode=args.mode)
    output = header + body

    print(output)

    if not args.dry_run:
        PICKS_OUT_DIR.mkdir(parents=True, exist_ok=True)
        out_file = PICKS_OUT_DIR / f"{date_str}_ranked_picks.txt"
        out_file.write_text(output)
        logger.info("Wrote picks to %s", out_file)

        json_out = PICKS_OUT_DIR / f"{date_str}_ranked_picks.json"
        payload = {
            "date":     date_str,
            "day_key":  day_key,
            "mode":     args.mode,
            "n_picks":  len(scored_list),
            "picks": [
                {
                    **{k: v for k, v in item.items()
                       if k not in ("matched_pairs",)},
                    "game":       _team_label(signals_map.get(item["signal_id"], {})),
                    "pick_label": _pick_label(signals_map.get(item["signal_id"], {})),
                    "market_type": (signals_map.get(item["signal_id"]) or {}).get("market_type"),
                    "line":        (signals_map.get(item["signal_id"]) or {}).get("line"),
                }
                for item in scored_list
            ],
        }
        json_out.write_text(json.dumps(payload, indent=2))
        logger.info("Wrote JSON to %s", json_out)


if __name__ == "__main__":
    main()
