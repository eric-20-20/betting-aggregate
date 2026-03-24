#!/usr/bin/env python3
"""
PATTERN_REGISTRY integrity validator.

Two phases:
  Phase 0 — Graded data quality summary (per-source error/ungraded counts, date gaps)
  Phase 1 — Per-pattern audit against graded_occurrences_latest.jsonl

Flags per pattern:
  FAIL  — Wilson below tier threshold, or >80% picks from one month, or <2 months covered
  WARN  — n drift >20%, wilson drift >0.02, top-month >60%, recent 30-day <47%
  PASS  — all checks clean

Exit code: 0 if all patterns PASS/WARN, 1 if any FAIL.

Usage:
    python3 scripts/validate_patterns.py             # full report
    python3 scripts/validate_patterns.py --tier A    # A-tier only
    python3 scripts/validate_patterns.py --fail-only # show only FAIL and WARN
    python3 scripts/validate_patterns.py --phase 0   # data quality summary only
    python3 scripts/validate_patterns.py --phase 1   # pattern audit only
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
OCCURRENCES_PATH = REPO_ROOT / "data" / "analysis" / "graded_occurrences_latest.jsonl"
NCAAB_OCCURRENCES_PATH = REPO_ROOT / "data" / "analysis" / "ncaab" / "graded_occurrences_latest.jsonl"
REPORTS_DIR = REPO_ROOT / "data" / "reports"

# Import PATTERN_REGISTRY from score_signals.py
sys.path.insert(0, str(REPO_ROOT / "scripts"))
from score_signals import PATTERN_REGISTRY, PATTERN_REGISTRY_NCAAB  # type: ignore

# ── Wilson lower bound ────────────────────────────────────────────────────────

def wilson_lower(wins: int, n: int, z: float = 1.645) -> float:
    if n == 0:
        return 0.0
    p = wins / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    spread = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (centre - spread) / denom


# ── Load data ─────────────────────────────────────────────────────────────────

def load_occurrences(path: Path) -> List[Dict]:
    rows = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


# ── Phase 0: Data quality summary ─────────────────────────────────────────────

# Sources we know have large backfills; their error rates may be high for specific periods.
KNOWN_HIGH_ERROR_SOURCES = {
    # betql: ~4K game_not_found errors in Oct-Dec 2025 from NBA API gaps
    "betql": "Oct-Dec 2025 NBA API game_not_found (~4K props ungraded)",
    # sportscapping: tiny sample (9 rows total), high error rate is expected
    "sportscapping": "Tiny sample (9 rows) — high error rate is expected at this scale",
}


def run_phase0(rows: List[Dict]) -> bool:
    """Print a data quality summary. Returns True if no unexpected issues found."""
    print("=" * 72)
    print("PHASE 0 — GRADED DATA QUALITY SUMMARY")
    print("=" * 72)

    total = len(rows)
    graded = [r for r in rows if r.get("result") in {"WIN", "LOSS"}]
    pushes = [r for r in rows if r.get("result") == "PUSH"]
    errors = [r for r in rows if r.get("grade_status") == "ERROR"]
    ineligible = [r for r in rows if r.get("grade_status") == "INELIGIBLE"]
    ungraded = [r for r in rows if r.get("grade_status") == "UNGRADED"]
    null_result = [r for r in rows if not r.get("result") and r.get("grade_status") not in
                   {"ERROR", "INELIGIBLE", "UNGRADED"}]

    print(f"\nTotal rows:     {total:>7,}")
    print(f"WIN/LOSS:       {len(graded):>7,}")
    print(f"PUSH:           {len(pushes):>7,}")
    print(f"ERROR:          {len(errors):>7,}")
    print(f"INELIGIBLE:     {len(ineligible):>7,}")
    print(f"UNGRADED:       {len(ungraded):>7,}")
    if null_result:
        print(f"NULL result:    {len(null_result):>7,}  ← UNEXPECTED")

    # Date range
    graded_days = sorted({r["day_key"] for r in graded})
    if graded_days:
        print(f"\nDate range: {graded_days[0]}  →  {graded_days[-1]}")
        print(f"Distinct graded days: {len(graded_days)}")

    # Date continuity gaps (>7 consecutive days with no graded picks)
    gaps = []
    for i in range(1, len(graded_days)):
        d0 = datetime.strptime(graded_days[i - 1], "NBA:%Y:%m:%d")
        d1 = datetime.strptime(graded_days[i], "NBA:%Y:%m:%d")
        gap_days = (d1 - d0).days - 1
        if gap_days > 7:
            gaps.append((graded_days[i - 1], graded_days[i], gap_days))
    if gaps:
        print(f"\n  Date gaps >7 days (could be data issues or NBA schedule breaks):")
        for a, b, g in gaps[:10]:
            print(f"    {a} → {b}  ({g} day gap)")
    else:
        print("\n  No date gaps >7 days found.")

    # Per-source breakdown
    print("\n  Per-source breakdown (graded | error | error_rate):")
    source_graded: Counter = Counter()
    source_error: Counter = Counter()
    source_ungraded: Counter = Counter()
    for r in graded:
        source_graded[r.get("occ_sources_combo", "?")] += 1
    for r in errors:
        source_error[r.get("occ_sources_combo", "?")] += 1
    for r in ungraded:
        source_ungraded[r.get("occ_sources_combo", "?")] += 1

    all_sources = sorted(set(source_graded) | set(source_error))
    unexpected_errors = False
    for src in all_sources:
        g = source_graded[src]
        e = source_error[src]
        u = source_ungraded[src]
        total_src = g + e + u
        err_rate = e / total_src if total_src > 0 else 0
        known = KNOWN_HIGH_ERROR_SOURCES.get(src, "")
        flag = ""
        if err_rate > 0.30 and not known:
            flag = "  ← HIGH ERROR RATE"
            unexpected_errors = True
        elif err_rate > 0.30 and known:
            flag = f"  (KNOWN: {known})"
        print(f"    {src:<35} {g:>5} graded  {e:>5} errors  {u:>4} ungraded  {err_rate:.1%}{flag}")

    # Win% plausibility check per source
    print("\n  Win% plausibility check (should be 30%-75%):")
    source_wins: Counter = Counter()
    for r in graded:
        if r.get("result") == "WIN":
            source_wins[r.get("occ_sources_combo", "?")] += 1
    implausible = False
    for src in sorted(source_graded):
        g = source_graded[src]
        if g < 20:
            continue
        wp = source_wins[src] / g
        if wp < 0.30 or wp > 0.75:
            print(f"    WARN: {src} win_pct={wp:.1%} (n={g}) — outside plausible range")
            implausible = True
    if not implausible:
        print("    All sources within plausible 30%-75% win rate range.")

    ok = not unexpected_errors and not implausible and not null_result
    print(f"\nPhase 0 result: {'OK' if ok else 'ISSUES FOUND — review above'}\n")
    return ok


# ── Phase 1: Pattern matching ─────────────────────────────────────────────────

def expert_in_row(name: str, row: Dict) -> bool:
    """Check if expert name appears anywhere in the row (expert, expert_name, or supports)."""
    if row.get("expert") == name:
        return True
    if row.get("expert_name") == name:
        return True
    for sup in row.get("supports") or []:
        if isinstance(sup, dict) and sup.get("expert_name") == name:
            return True
    return False


def match_pattern(pattern: Dict, rows: List[Dict]) -> List[Dict]:
    """
    Filter and deduplicate rows matching the given pattern.
    Returns signal-level deduplicated rows (by signal_id).
    """
    ptype = None
    if "exact_combo" in pattern:
        ptype = "exact_combo"
        value = pattern["exact_combo"]
    elif "source_pair" in pattern:
        ptype = "source_pair"
        required = set(pattern["source_pair"])
    elif "source_contains" in pattern:
        ptype = "source_contains"
        required = set(pattern["source_contains"])
    elif "expert" in pattern:
        ptype = "expert"
        expert_name = pattern["expert"]

    market_type = pattern.get("market_type")
    direction = pattern.get("direction")
    stat_type = pattern.get("stat_type")
    team = pattern.get("team")

    seen_signals: Set[str] = set()
    matched: List[Dict] = []

    for r in rows:
        result = r.get("result")
        if result not in {"WIN", "LOSS"}:
            continue

        # Source/expert matching
        if ptype == "exact_combo":
            if r.get("occ_sources_combo") != value:
                continue
        elif ptype in ("source_pair", "source_contains"):
            signal_sources = set(r.get("signal_sources_present") or [])
            if not required <= signal_sources:
                continue
        elif ptype == "expert":
            if not expert_in_row(expert_name, r):
                continue

        # Market type filter
        if market_type and r.get("market_type") != market_type:
            continue

        # Direction filter
        if direction and r.get("direction") != direction:
            continue

        # Stat type filter (matches atomic_stat field)
        if stat_type and r.get("atomic_stat") != stat_type:
            continue

        # Team filter (matches spread_team field)
        if team and r.get("spread_team") != team:
            continue

        # Deduplicate by signal_id
        sid = r.get("signal_id") or r.get("occurrence_id") or id(r)
        if sid in seen_signals:
            continue
        seen_signals.add(sid)
        matched.append(r)

    return matched


def parse_hist_record(record: str) -> Tuple[int, int]:
    """Parse '188-122' → (188, 122)."""
    parts = record.split("-")
    return int(parts[0]), int(parts[1])


def day_key_to_date(day_key: str) -> Optional[datetime]:
    """Parse 'NBA:2026:03:14' or 'NCAAB:2026:03:14' → datetime."""
    try:
        parts = day_key.split(":")
        if len(parts) < 4:
            return None
        return datetime(int(parts[1]), int(parts[2]), int(parts[3]))
    except Exception:
        return None


def compute_checks(pattern: Dict, matched: List[Dict]) -> Dict:
    """Compute all validation metrics for a pattern against its matched rows."""
    hist_wins, hist_losses = parse_hist_record(pattern["hist"]["record"])
    hist_n = pattern["hist"]["n"]
    tier = pattern["tier_eligible"]
    tier_threshold = 0.52 if tier == "A" else 0.46

    actual_wins = sum(1 for r in matched if r.get("result") == "WIN")
    actual_losses = sum(1 for r in matched if r.get("result") == "LOSS")
    actual_n = actual_wins + actual_losses
    actual_win_pct = actual_wins / actual_n if actual_n > 0 else 0.0
    actual_wilson = wilson_lower(actual_wins, actual_n)

    hist_wilson = wilson_lower(hist_wins, hist_n)
    n_drift_pct = ((actual_n - hist_n) / hist_n * 100) if hist_n > 0 else 0.0
    wilson_drift = actual_wilson - hist_wilson

    # Temporal analysis
    month_counts: Counter = Counter()
    for r in matched:
        dk = r.get("day_key", "")
        parts = dk.split(":")
        if len(parts) >= 3:
            ym = f"{parts[1]}-{parts[2]}"
            month_counts[ym] += 1

    months_covered = len(month_counts)
    top_month_count = max(month_counts.values()) if month_counts else 0
    top_month = max(month_counts, key=month_counts.get) if month_counts else "N/A"
    top_month_pct = (top_month_count / actual_n * 100) if actual_n > 0 else 0.0

    # Recent 30 days
    cutoff = datetime.now() - timedelta(days=30)
    recent = [r for r in matched if (d := day_key_to_date(r.get("day_key", ""))) and d >= cutoff]
    recent_wins = sum(1 for r in recent if r.get("result") == "WIN")
    recent_n = len(recent)
    recent_win_pct = recent_wins / recent_n if recent_n > 0 else None

    return {
        "actual_n": actual_n,
        "actual_wins": actual_wins,
        "actual_losses": actual_losses,
        "actual_win_pct": actual_win_pct,
        "actual_wilson": actual_wilson,
        "hist_n": hist_n,
        "hist_wins": hist_wins,
        "hist_losses": hist_losses,
        "hist_wilson": hist_wilson,
        "n_drift_pct": n_drift_pct,
        "wilson_drift": wilson_drift,
        "months_covered": months_covered,
        "top_month": top_month,
        "top_month_pct": top_month_pct,
        "recent_n": recent_n,
        "recent_win_pct": recent_win_pct,
        "tier": tier,
        "tier_threshold": tier_threshold,
        "wilson_below_threshold": actual_wilson < tier_threshold,
    }


def grade_checks(checks: Dict) -> Tuple[str, List[str]]:
    """
    Returns overall status ("PASS", "WARN", "FAIL") and list of flag messages.
    """
    flags = []
    worst = "PASS"

    def flag(level: str, msg: str):
        nonlocal worst
        flags.append(f"  [{level}] {msg}")
        if level == "FAIL" or (level == "WARN" and worst == "PASS"):
            worst = level

    # 1. Wilson below tier threshold
    if checks["wilson_below_threshold"] and checks["actual_n"] >= 20:
        flag("FAIL", f"Wilson {checks['actual_wilson']:.3f} below {checks['tier']}-tier threshold "
             f"{checks['tier_threshold']:.2f}")

    # 2. n drift
    drift = checks["n_drift_pct"]
    if abs(drift) > 50:
        flag("FAIL", f"n drift {drift:+.0f}% (actual={checks['actual_n']}, hist={checks['hist_n']})")
    elif abs(drift) > 20:
        flag("WARN", f"n drift {drift:+.0f}% (actual={checks['actual_n']}, hist={checks['hist_n']})")

    # 3. Wilson drift (only meaningful when actual_n >= 20)
    if checks["actual_n"] >= 20:
        wd = checks["wilson_drift"]
        if abs(wd) > 0.05:
            flag("FAIL", f"Wilson drift {wd:+.3f} (actual={checks['actual_wilson']:.3f}, "
                 f"hist={checks['hist_wilson']:.3f})")
        elif abs(wd) > 0.02:
            flag("WARN", f"Wilson drift {wd:+.3f} (actual={checks['actual_wilson']:.3f}, "
                 f"hist={checks['hist_wilson']:.3f})")

    # 4. Monthly concentration
    if checks["top_month_pct"] > 80 and checks["actual_n"] >= 20:
        flag("FAIL", f"Top month '{checks['top_month']}' has {checks['top_month_pct']:.0f}% of picks "
             f"({checks['actual_n']} total)")
    elif checks["top_month_pct"] > 60 and checks["actual_n"] >= 20:
        flag("WARN", f"Top month '{checks['top_month']}' has {checks['top_month_pct']:.0f}% of picks")

    # 5. Months covered
    mc = checks["months_covered"]
    tier = checks["tier"]
    if tier == "A":
        if mc < 2:
            flag("FAIL", f"Only {mc} month(s) of data — A-tier requires ≥2 months")
        elif mc < 3:
            flag("WARN", f"Only {mc} month(s) of data — A-tier should have ≥3 months")
    else:  # B-tier
        if mc < 1:
            flag("FAIL", f"Only {mc} month(s) of data — B-tier requires ≥1 month")
        elif mc < 2:
            flag("WARN", f"Only {mc} month(s) of data — B-tier should have ≥2 months")

    # 6. Recent cold streak
    if checks["recent_win_pct"] is not None and checks["recent_n"] >= 10:
        rwp = checks["recent_win_pct"]
        if rwp < 0.40:
            flag("FAIL", f"Recent 30-day win% {rwp:.1%} (n={checks['recent_n']}) — COLD STREAK")
        elif rwp < 0.47:
            flag("WARN", f"Recent 30-day win% {rwp:.1%} (n={checks['recent_n']}) — cooling")

    # 7. No data found
    if checks["actual_n"] == 0:
        flag("FAIL", "No graded records found matching pattern criteria")

    return worst, flags


def format_pattern_report(pattern: Dict, checks: Dict, status: str, flags: List[str]) -> str:
    lines = []
    bar = "─" * 60
    color = {"PASS": "✓", "WARN": "!", "FAIL": "✗"}.get(status, "?")
    lines.append(f"\n[{status}] {color} {pattern['id']}  ({pattern.get('label', '')})")
    lines.append(f"  Tier: {checks['tier']}  |  "
                 f"Registry: {checks['hist_wins']}-{checks['hist_losses']} n={checks['hist_n']} "
                 f"W={checks['hist_wilson']:.3f}")
    lines.append(f"  Actual:   {checks['actual_wins']}-{checks['actual_losses']} "
                 f"n={checks['actual_n']} W={checks['actual_wilson']:.3f} "
                 f"({checks['actual_win_pct']:.1%})")
    lines.append(f"  Months: {checks['months_covered']}  |  "
                 f"Top month: {checks['top_month']} ({checks['top_month_pct']:.0f}%)  |  "
                 f"Recent 30d: "
                 + (f"{checks['recent_win_pct']:.1%} n={checks['recent_n']}"
                    if checks["recent_win_pct"] is not None else "n/a"))
    for f in flags:
        lines.append(f)
    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

def run_phase1(rows: List[Dict], ncaab_rows: List[Dict], tier_filter: Optional[str], fail_only: bool) -> bool:
    """Run per-pattern audit. Returns True if all patterns pass (no FAIL)."""
    print("=" * 72)
    print("PHASE 1 — PATTERN REGISTRY AUDIT")
    print("=" * 72)

    # Pair each pattern with the appropriate data rows
    pattern_data: List[Tuple[Dict, List[Dict]]] = [
        (p, rows) for p in PATTERN_REGISTRY
    ] + [
        (p, ncaab_rows) for p in PATTERN_REGISTRY_NCAAB
    ]

    if tier_filter:
        pattern_data = [(p, r) for p, r in pattern_data if p.get("tier_eligible") == tier_filter]

    counts = Counter()
    any_fail = False

    for pattern, data_rows in pattern_data:
        matched = match_pattern(pattern, data_rows)
        checks = compute_checks(pattern, matched)
        status, flags = grade_checks(checks)
        counts[status] += 1
        if status == "FAIL":
            any_fail = True

        if fail_only and status == "PASS":
            continue

        print(format_pattern_report(pattern, checks, status, flags))

    print("\n" + "=" * 72)
    print(f"SUMMARY: {counts['PASS']} PASS  |  {counts['WARN']} WARN  |  {counts['FAIL']} FAIL")
    print("=" * 72)
    return not any_fail


def main():
    parser = argparse.ArgumentParser(description="Validate PATTERN_REGISTRY against graded data")
    parser.add_argument("--tier", choices=["A", "B"], help="Filter to specific tier")
    parser.add_argument("--fail-only", action="store_true", help="Only show FAIL and WARN patterns")
    parser.add_argument("--phase", choices=["0", "1"], help="Run only phase 0 or phase 1")
    args = parser.parse_args()

    if not OCCURRENCES_PATH.exists():
        print(f"ERROR: {OCCURRENCES_PATH} not found. Run build_research_dataset_nba_occurrences.py first.")
        sys.exit(1)

    print(f"Loading {OCCURRENCES_PATH} ...")
    rows = load_occurrences(OCCURRENCES_PATH)
    print(f"Loaded {len(rows):,} NBA rows.")

    ncaab_rows: List[Dict] = []
    if NCAAB_OCCURRENCES_PATH.exists():
        print(f"Loading {NCAAB_OCCURRENCES_PATH} ...")
        ncaab_rows = load_occurrences(NCAAB_OCCURRENCES_PATH)
        print(f"Loaded {len(ncaab_rows):,} NCAAB rows.")
    else:
        print(f"NCAAB occurrences not found at {NCAAB_OCCURRENCES_PATH} — NCAAB patterns will show 0 records.")
    print()

    all_ok = True

    if args.phase != "1":
        ok0 = run_phase0(rows)
        all_ok = all_ok and ok0

    if args.phase != "0":
        ok1 = run_phase1(rows, ncaab_rows, args.tier, args.fail_only)
        all_ok = all_ok and ok1

    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
