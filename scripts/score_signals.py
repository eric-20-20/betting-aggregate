#!/usr/bin/env python3
"""Score today's signals across historical dimensions and produce tiered plays of the day.

Each signal is scored across 7 dimensions using pre-computed report JSONs as lookup
tables. Edge contributions (wilson_lower - 0.50) are summed into a composite score,
and signals are assigned tiers A/B/C/D based on the composite and number of positive
dimensions.

Usage:
    python3 scripts/score_signals.py
    python3 scripts/score_signals.py --date 2026-02-20
    python3 scripts/score_signals.py --date 2026-02-20 --verbose
    python3 scripts/score_signals.py --min-n 50
"""
from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ── Paths ──────────────────────────────────────────────────────────────────

REPORTS_DIR = Path("data/reports")
SIGNALS_PATH = Path("data/ledger/signals_latest.jsonl")
PLAYS_DIR = Path("data/plays")

CROSS_TAB_PATH = REPORTS_DIR / "cross_tabulation.json"
CONSENSUS_PATH = REPORTS_DIR / "trends" / "consensus_strength.json"
LINE_BUCKET_PATH = REPORTS_DIR / "by_line_bucket.json"
STAT_TYPE_PATH = REPORTS_DIR / "by_stat_type.json"
DAY_OF_WEEK_PATH = REPORTS_DIR / "trends" / "by_day_of_week.json"
EXPERT_PATH = REPORTS_DIR / "by_expert_record.json"

EXCLUDED_SIGNAL_TYPES = {"avoid_conflict"}

MIN_SAMPLE_DEFAULT = 30
EDGE_THRESHOLD = 0.005  # |edge| > this to count as positive/negative


# ── Utility functions ──────────────────────────────────────────────────────

def wilson_lower(wins: int, n: int, z: float = 1.96) -> float:
    """Wilson score lower bound -- conservative win% estimate."""
    if n == 0:
        return 0.0
    p = wins / n
    denom = 1 + z * z / n
    center = p + z * z / (2 * n)
    spread = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)
    return (center - spread) / denom


def line_bucket(line: Optional[float], market_type: str) -> str:
    """Bucket a line value by market type (mirrors report_records.py:716-751)."""
    if line is None:
        return "no_line"
    try:
        line = float(line)
    except (TypeError, ValueError):
        return "no_line"
    if market_type == "spread":
        a = abs(line)
        if a <= 2.5:
            return "0-2.5"
        if a <= 5.5:
            return "3-5.5"
        if a <= 9.5:
            return "6-9.5"
        return "10+"
    if market_type == "total":
        if line < 215:
            return "<215"
        if line <= 225:
            return "215-225"
        if line <= 235:
            return "225-235"
        return "235+"
    if market_type == "player_prop":
        if line <= 10.5:
            return "0-10.5"
        if line <= 20.5:
            return "11-20.5"
        if line <= 30.5:
            return "21-30.5"
        return "31+"
    return "other"


def parse_day_key(day_key: Optional[str]) -> Optional[date]:
    """Parse 'NBA:YYYY:MM:DD' into a date object."""
    if not day_key or not isinstance(day_key, str):
        return None
    parts = day_key.split(":")
    if len(parts) < 4:
        return None
    try:
        return date(int(parts[1]), int(parts[2]), int(parts[3]))
    except (ValueError, IndexError):
        return None


def strip_expert_prefix(expert_str: str) -> str:
    """Strip source prefix from expert string.

    'action:/picks/profile/sandyplashkes' -> 'sandyplashkes'
    'covers:EV Model Rating Star' -> 'EV Model Rating Star'
    'Bet Labs' -> 'Bet Labs'
    """
    if ":" in expert_str:
        # Handle URL-style prefixes like 'action:/picks/profile/name'
        prefix, _, rest = expert_str.partition(":")
        # If the rest starts with / it's a URL path - extract the last segment
        if rest.startswith("/"):
            segments = [s for s in rest.split("/") if s]
            return segments[-1] if segments else expert_str
        return rest.strip()
    return expert_str


def consensus_strength_key(sources_present: List[str]) -> str:
    """Map source count to consensus_strength lookup key."""
    n = len(sources_present) if sources_present else 1
    if n >= 3:
        return "3_source"
    if n == 2:
        return "2_source"
    return "1_source"


# ── Lookup table loaders ──────────────────────────────────────────────────

def _make_entry(row: Dict[str, Any], min_n: int) -> Dict[str, Any]:
    """Build a standard lookup entry from a report row."""
    n = row.get("n", 0)
    wins = row.get("wins", 0)
    wp = row.get("win_pct") or 0
    wl = row.get("wilson_lower")
    if wl is None:
        wl = wilson_lower(wins, n) if n > 0 else 0.0
    return {"win_pct": wp, "n": n, "wins": wins, "wilson_lower": wl}


def _load_combo_market(cross_tab: Dict) -> Dict[Tuple[str, str], Dict]:
    table: Dict[Tuple[str, str], Dict] = {}
    for row in cross_tab.get("by_combo_market", []):
        key = (row.get("sources_combo", ""), row.get("market_type", ""))
        table[key] = _make_entry(row, 0)
    return table


def _load_consensus(data: Dict) -> Dict[str, Dict]:
    table: Dict[str, Dict] = {}
    for row in data.get("rows", []):
        key = row.get("consensus_strength", "")
        table[key] = _make_entry(row, 0)
    return table


def _load_line_bucket(data: Dict) -> Dict[Tuple[str, str], Dict]:
    table: Dict[Tuple[str, str], Dict] = {}
    for row in data.get("rows", []):
        key = (row.get("market_type", ""), row.get("line_bucket", ""))
        table[key] = _make_entry(row, 0)
    return table


def _load_stat_type(data: Dict) -> Dict[str, Dict]:
    table: Dict[str, Dict] = {}
    for row in data.get("rows", []):
        key = row.get("stat_type", "")
        table[key] = _make_entry(row, 0)
    return table


def _load_day_of_week(data: Dict) -> Dict[str, Dict]:
    table: Dict[str, Dict] = {}
    for row in data.get("rows", []):
        key = row.get("day_of_week", "")
        table[key] = _make_entry(row, 0)
    return table


def _load_experts(data: Dict) -> Dict[str, Dict]:
    table: Dict[str, Dict] = {}
    for row in data.get("rows", []):
        key = row.get("expert", "")
        if key and key not in ("unknown", "MULTI"):
            table[key] = _make_entry(row, 0)
    return table


def _load_combo_stat(cross_tab: Dict) -> Dict[Tuple[str, str], Dict]:
    table: Dict[Tuple[str, str], Dict] = {}
    for row in cross_tab.get("by_combo_stat", []):
        key = (row.get("sources_combo", ""), row.get("stat_type", ""))
        table[key] = _make_entry(row, 0)
    return table


def load_lookup_tables() -> Dict[str, Any]:
    """Load all report JSONs into lookup dictionaries."""
    tables: Dict[str, Any] = {}
    loaded = 0

    if CROSS_TAB_PATH.exists():
        ct = json.loads(CROSS_TAB_PATH.read_text())
        tables["combo_market"] = _load_combo_market(ct)
        tables["combo_stat"] = _load_combo_stat(ct)
        loaded += 1
    else:
        tables["combo_market"] = {}
        tables["combo_stat"] = {}

    for name, path, loader in [
        ("consensus", CONSENSUS_PATH, _load_consensus),
        ("line_bucket", LINE_BUCKET_PATH, _load_line_bucket),
        ("stat_type", STAT_TYPE_PATH, _load_stat_type),
        ("day_of_week", DAY_OF_WEEK_PATH, _load_day_of_week),
        ("experts", EXPERT_PATH, _load_experts),
    ]:
        if path.exists():
            tables[name] = loader(json.loads(path.read_text()))
            loaded += 1
        else:
            tables[name] = {}

    tables["_loaded"] = loaded
    return tables


# ── Signal loading ─────────────────────────────────────────────────────────

def load_signals(date_str: str) -> List[Dict[str, Any]]:
    """Load signals for a specific date from signals_latest.jsonl."""
    target_key = f"NBA:{date_str.replace('-', ':')}"
    signals = []
    if not SIGNALS_PATH.exists():
        return signals
    with SIGNALS_PATH.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            if row.get("day_key") == target_key:
                signals.append(row)
    return signals


def filter_scorable(signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Exclude avoid_conflict and other non-actionable signals."""
    return [
        s for s in signals
        if s.get("signal_type") not in EXCLUDED_SIGNAL_TYPES
        and s.get("market_type") in ("player_prop", "spread", "total", "moneyline")
    ]


# ── Dimension scorers ─────────────────────────────────────────────────────

def _edge_from_entry(entry: Optional[Dict], min_n: int) -> Tuple[float, Dict[str, Any]]:
    """Compute edge contribution from a lookup entry.

    Uses raw win_pct for edge calculation (not Wilson lower, which is too
    conservative for additive scoring at moderate sample sizes). Wilson lower
    is still reported for reference.

    Returns (edge, detail_dict).
    """
    if not entry or entry.get("n", 0) < min_n:
        return 0.0, {
            "win_pct": entry.get("win_pct") if entry else None,
            "n": entry.get("n", 0) if entry else 0,
            "verdict": "no_data",
        }
    wp = entry["win_pct"]
    edge = wp - 0.50
    verdict = "positive" if edge > EDGE_THRESHOLD else ("negative" if edge < -EDGE_THRESHOLD else "neutral")
    return edge, {
        "win_pct": round(wp, 4),
        "wilson_lower": round(entry.get("wilson_lower", 0), 4),
        "n": entry["n"],
        "verdict": verdict,
    }


def score_signal(
    signal: Dict[str, Any],
    tables: Dict[str, Any],
    min_n: int,
    day_of_week: str,
) -> Dict[str, Any]:
    """Score a single signal across all 7 dimensions."""
    market = signal.get("market_type", "")
    sources_combo = signal.get("sources_combo", "")
    sources_present = signal.get("sources_present") or signal.get("sources") or []
    atomic_stat = signal.get("atomic_stat")
    sig_line = signal.get("line")
    experts_raw = signal.get("experts") or []

    factors = []
    edges = []

    # Dimension 1: Source combo x market type
    entry = tables["combo_market"].get((sources_combo, market))
    edge, detail = _edge_from_entry(entry, min_n)
    edges.append(edge)
    factors.append({"dimension": "combo_x_market", "lookup_key": f"{sources_combo} / {market}", "edge": round(edge, 4), **detail})

    # Dimension 2: Consensus strength
    cs_key = consensus_strength_key(sources_present)
    entry = tables["consensus"].get(cs_key)
    edge, detail = _edge_from_entry(entry, min_n)
    edges.append(edge)
    factors.append({"dimension": "consensus", "lookup_key": cs_key, "edge": round(edge, 4), **detail})

    # Dimension 3: Line bucket
    lb = line_bucket(sig_line, market)
    entry = tables["line_bucket"].get((market, lb))
    edge, detail = _edge_from_entry(entry, min_n)
    edges.append(edge)
    factors.append({"dimension": "line_bucket", "lookup_key": f"{market} / {lb}", "edge": round(edge, 4), **detail})

    # Dimension 4: Stat type (props only)
    if market == "player_prop" and atomic_stat:
        entry = tables["stat_type"].get(atomic_stat)
        edge, detail = _edge_from_entry(entry, min_n)
        edges.append(edge)
        factors.append({"dimension": "stat_type", "lookup_key": atomic_stat, "edge": round(edge, 4), **detail})

    # Dimension 5: Day of week
    entry = tables["day_of_week"].get(day_of_week)
    edge, detail = _edge_from_entry(entry, min_n)
    edges.append(edge)
    factors.append({"dimension": "day_of_week", "lookup_key": day_of_week, "edge": round(edge, 4), **detail})

    # Dimension 6: Best expert
    best_expert_edge = 0.0
    best_expert_detail: Dict[str, Any] = {"verdict": "no_data", "n": 0}
    best_expert_name = None
    for expert_raw in experts_raw:
        name = strip_expert_prefix(expert_raw)
        entry = tables["experts"].get(name)
        if entry and entry.get("n", 0) >= min_n:
            e, d = _edge_from_entry(entry, min_n)
            if e > best_expert_edge or best_expert_name is None:
                best_expert_edge = e
                best_expert_detail = d
                best_expert_name = name
    edges.append(best_expert_edge)
    factors.append({
        "dimension": "best_expert",
        "lookup_key": best_expert_name or "none",
        "edge": round(best_expert_edge, 4),
        **best_expert_detail,
    })

    # Dimension 7: Source combo x stat type (props only)
    if market == "player_prop" and atomic_stat:
        entry = tables["combo_stat"].get((sources_combo, atomic_stat))
        edge, detail = _edge_from_entry(entry, min_n)
        edges.append(edge)
        factors.append({"dimension": "combo_x_stat", "lookup_key": f"{sources_combo} / {atomic_stat}", "edge": round(edge, 4), **detail})

    composite = compute_composite(edges)
    positive_dims = sum(1 for e in edges if e > EDGE_THRESHOLD)
    negative_dims = sum(1 for e in edges if e < -EDGE_THRESHOLD)
    tier = assign_tier(composite, positive_dims, negative_dims)

    # Build human-readable summary of positive factors
    summary_parts = []
    for f in factors:
        if f["edge"] > EDGE_THRESHOLD:
            dim_short = {
                "combo_x_market": "combo",
                "consensus": "consensus",
                "line_bucket": "line",
                "stat_type": "stat",
                "day_of_week": "day",
                "best_expert": "expert",
                "combo_x_stat": "combo_stat",
            }.get(f["dimension"], f["dimension"])
            summary_parts.append(f"+{f['edge']*100:.1f}% {dim_short}")
    summary = ", ".join(summary_parts) if summary_parts else "no positive factors"

    return {
        "tier": tier,
        "composite_score": round(composite, 4),
        "positive_dimensions": positive_dims,
        "negative_dimensions": negative_dims,
        "factors": factors,
        "summary": summary,
    }


# ── Composite score & tier ─────────────────────────────────────────────────

def compute_composite(edges: List[float]) -> float:
    """Sum edge contributions with diminishing returns cap after 4 positive dims."""
    positive = sorted([e for e in edges if e > EDGE_THRESHOLD], reverse=True)
    negative = [e for e in edges if e <= -EDGE_THRESHOLD]
    neutral = [e for e in edges if -EDGE_THRESHOLD <= e <= EDGE_THRESHOLD]

    if len(positive) > 4:
        pos_sum = sum(positive[:4]) + 0.5 * sum(positive[4:])
    else:
        pos_sum = sum(positive)

    return pos_sum + sum(negative) + sum(neutral)


def assign_tier(composite: float, positive_dims: int, negative_dims: int) -> str:
    """Assign A/B/C/D tier based on composite score and dimension counts.

    Backtested on 8,424 graded signals:
      A: 56.8% (n=951)  — strong multi-factor edge
      B: 51.9% (n=860)  — net positive composite
      C: 50.9% (n=945)  — borderline / neutral
      D: 46.8% (n=5,668) — avoid
    """
    if positive_dims >= 3 and composite >= 0.04:
        return "A"
    if composite >= 0.0:
        return "B"
    if composite >= -0.03:
        return "C"
    return "D"


# ── Output ─────────────────────────────────────────────────────────────────

def format_selection(signal: Dict[str, Any]) -> str:
    """Build a concise display string for a signal."""
    market = signal.get("market_type", "")
    selection = signal.get("selection", "?")
    sig_line = signal.get("line")
    direction = signal.get("direction", "")

    if market == "player_prop":
        # "NBA:p_watson::points::UNDER" -> "p_watson pts U18.5"
        parts = selection.replace("NBA:", "").split("::")
        player = parts[0] if parts else "?"
        stat = parts[1] if len(parts) > 1 else "?"
        d = parts[2][0] if len(parts) > 2 else "?"  # O or U
        line_str = f" {sig_line}" if sig_line is not None else ""
        return f"{player} {stat} {d}{line_str}"
    elif market == "spread":
        line_str = f" {'+' if sig_line and sig_line > 0 else ''}{sig_line}" if sig_line is not None else ""
        return f"{selection} spread{line_str}"
    elif market == "total":
        d = direction[0] if direction else "?"
        line_str = f" {sig_line}" if sig_line is not None else ""
        return f"total {d}{line_str}"
    elif market == "moneyline":
        return f"{selection} ML"
    return selection


def build_signal_summary(signal: Dict[str, Any]) -> Dict[str, Any]:
    """Extract key fields from a signal for output."""
    return {
        "signal_id": signal.get("signal_id"),
        "selection": signal.get("selection"),
        "market_type": signal.get("market_type"),
        "sources_combo": signal.get("sources_combo"),
        "sources_present": signal.get("sources_present") or signal.get("sources") or [],
        "line": signal.get("line"),
        "score": signal.get("score"),
        "direction": signal.get("direction"),
        "atomic_stat": signal.get("atomic_stat"),
        "experts": signal.get("experts") or [],
        "best_odds": signal.get("best_odds"),
        "away_team": signal.get("away_team"),
        "home_team": signal.get("home_team"),
        "event_key": signal.get("event_key"),
        "day_key": signal.get("day_key"),
    }


def build_output(
    scored: List[Tuple[Dict[str, Any], Dict[str, Any]]],
    date_str: str,
    day_of_week: str,
    total_signals: int,
    scorable_count: int,
) -> Dict[str, Any]:
    """Build the final output JSON structure."""
    # Sort by composite score descending
    scored.sort(key=lambda x: x[1]["composite_score"], reverse=True)

    tier_counts: Dict[str, int] = {"A": 0, "B": 0, "C": 0, "D": 0}
    plays = []
    for rank, (signal, score_data) in enumerate(scored, 1):
        tier = score_data["tier"]
        tier_counts[tier] = tier_counts.get(tier, 0) + 1
        plays.append({
            "rank": rank,
            "tier": tier,
            "composite_score": score_data["composite_score"],
            "positive_dimensions": score_data["positive_dimensions"],
            "negative_dimensions": score_data["negative_dimensions"],
            "signal": build_signal_summary(signal),
            "factors": score_data["factors"],
            "summary": score_data["summary"],
        })

    return {
        "meta": {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "date": date_str,
            "day_of_week": day_of_week,
            "total_signals": total_signals,
            "scorable_signals": scorable_count,
            "tier_counts": tier_counts,
        },
        "plays": plays,
    }


def print_summary(output: Dict[str, Any], verbose: bool = False) -> None:
    """Print a concise console summary."""
    meta = output["meta"]
    plays = output["plays"]
    tc = meta["tier_counts"]

    print()
    print(f"  PLAYS OF THE DAY -- {meta['date']} ({meta['day_of_week']})")
    print(f"  Signals: {meta['total_signals']} loaded, {meta['scorable_signals']} scored")
    print()

    for tier, label in [("A", "Strong Edge (56.8% hist.)"), ("B", "Positive (51.9% hist.)"), ("C", "Neutral (50.9% hist.)"), ("D", "Avoid (46.8% hist.)")]:
        tier_plays = [p for p in plays if p["tier"] == tier]
        if not tier_plays:
            continue
        if tier in ("C", "D") and not verbose:
            print(f"  TIER {tier} -- {label} ({tc.get(tier, 0)} picks) [use --verbose to show]")
            continue

        print(f"  TIER {tier} -- {label} ({tc.get(tier, 0)} picks)")
        for p in tier_plays:
            sig = p["signal"]
            sel_str = format_selection(sig)
            combo = sig.get("sources_combo", "?")
            comp = p["composite_score"]
            sign = "+" if comp >= 0 else ""
            print(f"    {p['rank']:3d}. {sel_str:35s} [{combo}]  {sign}{comp*100:.1f}%  ({p['summary']})")
        print()


# ── Main ───────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Score signals and produce tiered plays of the day.")
    parser.add_argument("--date", type=str, default=None, help="Date to score (YYYY-MM-DD). Default: today")
    parser.add_argument("--min-n", type=int, default=MIN_SAMPLE_DEFAULT, help=f"Min sample size to trust a dimension (default: {MIN_SAMPLE_DEFAULT})")
    parser.add_argument("--verbose", action="store_true", help="Show all tiers and full factor details")
    args = parser.parse_args()

    date_str = args.date or date.today().strftime("%Y-%m-%d")
    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        print(f"ERROR: Invalid date format: {date_str}")
        return
    day_of_week = target_date.strftime("%A")

    # Load lookup tables
    tables = load_lookup_tables()
    loaded = tables.pop("_loaded", 0)
    table_sizes = {k: len(v) for k, v in tables.items()}

    # Load and filter signals
    signals = load_signals(date_str)
    total_signals = len(signals)
    scorable = filter_scorable(signals)
    scorable_count = len(scorable)

    if not scorable:
        print(f"  No scorable signals found for {date_str}.")
        print(f"  (Total signals for date: {total_signals})")
        return

    # Score each signal
    scored: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    for signal in scorable:
        score_data = score_signal(signal, tables, args.min_n, day_of_week)
        scored.append((signal, score_data))

    # Build output
    output = build_output(scored, date_str, day_of_week, total_signals, scorable_count)

    # Write JSON
    PLAYS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = PLAYS_DIR / f"plays_{date_str}.json"
    out_path.write_text(json.dumps(output, indent=2, ensure_ascii=False))

    # Print summary
    print_summary(output, verbose=args.verbose)
    print(f"  Output: {out_path}")
    print()


if __name__ == "__main__":
    main()
