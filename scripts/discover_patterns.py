#!/usr/bin/env python3
"""
discover_patterns.py — Automated pattern health check and candidate discovery.

Reads data/reports/by_sources_combo_market_direction.json (updated every daily run)
and cross-references against the current PATTERN_REGISTRY in score_signals.py.

Modes:
  Default (report only):   print health of existing patterns + new candidates
  --apply:                 auto-update PATTERN_REGISTRY hist blocks with fresh stats
  --min-n N:               minimum n for candidate consideration (default: 50)
  --min-wilson FLOAT:      minimum Wilson lower bound for B-tier (default: 0.46)
  --sport SPORT:           NBA (default) or NCAAB

Exit codes:
  0 — all patterns healthy (or --apply succeeded)
  1 — degradation warning (one or more patterns below warning threshold)
  2 — degradation critical (one or more patterns recommended for removal)
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ── Paths ──────────────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR = REPO_ROOT / "data" / "reports"
SCORE_SIGNALS = REPO_ROOT / "scripts" / "score_signals.py"


# ── Wilson lower bound ─────────────────────────────────────────────────────────

def wilson_lower(wins: int, n: int, z: float = 1.645) -> float:
    """Wilson score lower bound (one-sided, 90% CI)."""
    if n == 0:
        return 0.0
    p = wins / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    spread = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return round((centre - spread) / denom, 4)


# ── Load report data ───────────────────────────────────────────────────────────

def load_combo_direction_report(sport: str = "NBA") -> List[Dict[str, Any]]:
    if sport == "NCAAB":
        path = REPORTS_DIR / "ncaab" / "by_sources_combo_market_direction.json"
    else:
        path = REPORTS_DIR / "by_sources_combo_market_direction.json"
    if not path.exists():
        print(f"  [warn] Report not found: {path}")
        return []
    data = json.loads(path.read_text())
    return data.get("rows", [])


def load_recent_trends(sport: str = "NBA") -> Dict[str, Any]:
    if sport == "NCAAB":
        path = REPORTS_DIR / "ncaab" / "recent_trends_lookup.json"
    else:
        path = REPORTS_DIR / "recent_trends_lookup.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text())


# ── Load PATTERN_REGISTRY from score_signals.py via import ───────────────────

def load_registry(sport: str = "NBA") -> List[Dict[str, Any]]:
    import importlib.util
    spec = importlib.util.spec_from_file_location("score_signals", SCORE_SIGNALS)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    if sport == "NCAAB":
        return getattr(mod, "PATTERN_REGISTRY_NCAAB", [])
    return getattr(mod, "PATTERN_REGISTRY", [])


# ── Pattern → report row matching ─────────────────────────────────────────────

def _combo_for_pattern(pat: Dict[str, Any]) -> Optional[str]:
    """Return the sources_combo string this pattern maps to in the report."""
    if "exact_combo" in pat:
        return pat["exact_combo"]
    if "source_pair" in pat:
        return "|".join(sorted(pat["source_pair"]))
    if "source_contains" in pat:
        # source_contains patterns match any combo that includes all listed sources.
        # The exact report key is the sorted join of those sources.
        return "|".join(sorted(pat["source_contains"]))
    return None


def find_report_row(
    pat: Dict[str, Any],
    rows: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Find the matching report row for a pattern."""
    combo = _combo_for_pattern(pat)
    market = pat.get("market_type")
    direction = pat.get("direction")
    team = pat.get("team")
    stat_type = pat.get("stat_type")

    # Stat-type patterns (e.g. assists-only) can't be matched: the report has
    # no stat_type breakdown — return None so health check skips them.
    if stat_type:
        return None

    for row in rows:
        if combo and row.get("sources_combo") != combo:
            continue
        if market and row.get("market_type") != market:
            continue

        row_dir = row.get("direction", "")
        if team:
            # Team-specific spread/ML: direction in report is the team abbreviation
            if row_dir != team:
                continue
        elif direction:
            if row_dir != direction:
                continue

        return row
    return None


# ── Recent trend lookup ────────────────────────────────────────────────────────

def _recent_wilson(
    pat: Dict[str, Any],
    recent: Dict[str, Any],
    window: int = 30,
) -> Optional[float]:
    """Look up the recent-window Wilson for a pattern's combo×market key."""
    combo = _combo_for_pattern(pat)
    market = pat.get("market_type")
    if not combo or not market:
        return None

    by_window = recent.get("by_window", {})
    w = by_window.get(str(window), {})
    cm = w.get("combo_market", {})
    key = f"{combo}__{market}"
    entry = cm.get(key)
    if entry and entry.get("n", 0) >= 5:
        return entry.get("wilson_lower")
    return None


# ── Main logic ─────────────────────────────────────────────────────────────────

# Thresholds
WARN_WILSON = 0.40       # Pattern wilson below this → warning
REMOVE_WILSON = 0.35     # Pattern wilson below this → recommend removal
REMOVE_WINPCT = 0.45     # Pattern win_pct below this → recommend removal
B_TIER_WILSON = 0.46     # Minimum Wilson for B-tier candidate
A_TIER_WILSON = 0.50     # Minimum Wilson for A-tier candidate
MIN_N_DEFAULT = 50       # Minimum sample for any candidate
A_TIER_MIN_N = 75        # Minimum n for A-tier candidate


def check_health(
    registry: List[Dict[str, Any]],
    rows: List[Dict[str, Any]],
    recent: Dict[str, Any],
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Returns (healthy, warnings, critical) lists of dicts with keys:
      pat, row, live_wilson, live_winpct, recent_wilson, issue
    """
    healthy, warnings, critical = [], [], []

    for pat in registry:
        row = find_report_row(pat, rows)
        if row is None:
            # No data found — treat as unknown, skip
            continue

        wins = row.get("wins", 0)
        n = row.get("n", 0)
        live_wilson = wilson_lower(wins, n)
        live_winpct = row.get("win_pct", 0.0)
        recent_w = _recent_wilson(pat, recent, window=30)

        info = {
            "pat": pat,
            "row": row,
            "live_wilson": live_wilson,
            "live_winpct": live_winpct,
            "recent_wilson": recent_w,
        }

        if live_wilson < REMOVE_WILSON or live_winpct < REMOVE_WINPCT:
            info["issue"] = "recommend_removal"
            critical.append(info)
        elif live_wilson < WARN_WILSON:
            info["issue"] = "degraded_warning"
            warnings.append(info)
        else:
            info["issue"] = None
            healthy.append(info)

    return healthy, warnings, critical


_PROP_DIRECTIONS = {"OVER", "UNDER"}
_TOTAL_DIRECTIONS = {"OVER", "UNDER"}

def _is_team_code(direction: str) -> bool:
    """Return True if direction is a team abbreviation, not OVER/UNDER."""
    if direction in _PROP_DIRECTIONS:
        return False
    return bool(direction and direction.isupper() and 2 <= len(direction) <= 4
                and direction.isalpha())


def find_candidates(
    rows: List[Dict[str, Any]],
    registry: List[Dict[str, Any]],
    recent: Dict[str, Any],
    min_n: int = MIN_N_DEFAULT,
    min_wilson: float = B_TIER_WILSON,
) -> List[Dict[str, Any]]:
    """Find report rows that qualify as new patterns but aren't in the registry yet."""
    # Build set of (combo, market, direction/team) already in registry.
    # For team-specific patterns, key uses the team value.
    existing_keys: set = set()
    for pat in registry:
        combo = _combo_for_pattern(pat)
        market = pat.get("market_type")
        team = pat.get("team")
        direction = pat.get("direction")
        # No team, no direction → direction-agnostic (matches all teams for spreads)
        if team:
            existing_keys.add((combo, market, team))
        elif direction:
            existing_keys.add((combo, market, direction))
        else:
            # direction-agnostic pattern — mark all team codes as covered for this combo+market
            existing_keys.add((combo, market, None))

    candidates = []
    for row in rows:
        combo = row.get("sources_combo")
        market = row.get("market_type")
        direction = row.get("direction", "")
        wins = row.get("wins", 0)
        n = row.get("n", 0)

        if n < min_n:
            continue

        live_wilson = wilson_lower(wins, n)
        if live_wilson < min_wilson:
            continue

        # Determine the effective key for dedup check
        is_team = _is_team_code(direction)
        if is_team:
            key = (combo, market, direction)
            # Also skip if a direction-agnostic pattern covers this combo+market
            if (combo, market, None) in existing_keys:
                continue
        else:
            key = (combo, market, direction)

        if key in existing_keys:
            continue

        # Check recent trend (30d) also clears threshold
        recent_w = _recent_wilson(
            {"exact_combo": combo, "market_type": market, "direction": direction},
            recent,
            window=30,
        )

        tier = "A" if (live_wilson >= A_TIER_WILSON and n >= A_TIER_MIN_N) else "B"
        candidates.append({
            "row": row,
            "live_wilson": live_wilson,
            "recent_wilson": recent_w,
            "suggested_tier": tier,
            "is_team_specific": is_team,
        })

    # Sort by wilson descending
    candidates.sort(key=lambda x: x["live_wilson"], reverse=True)
    return candidates


def print_report(
    healthy: List[Dict],
    warnings: List[Dict],
    critical: List[Dict],
    candidates: List[Dict],
) -> int:
    """Print the health report. Returns exit code (0/1/2)."""
    total = len(healthy) + len(warnings) + len(critical)
    print()
    print("=" * 70)
    print("  PATTERN HEALTH REPORT")
    print("=" * 70)
    print(f"  Registry: {total} patterns  |  Healthy: {len(healthy)}  "
          f"Warnings: {len(warnings)}  Critical: {len(critical)}")
    print()

    if critical:
        print(f"  CRITICAL — {len(critical)} pattern(s) recommended for removal:")
        for info in critical:
            pat = info["pat"]
            label = pat.get("label") or pat.get("name") or pat.get("id", "?")
            row = info["row"]
            orig = pat.get("hist", {})
            print(f"    ✗ [{pat.get('tier_eligible','?')}] {label}")
            print(f"        Original: {orig.get('record','?')}  win={orig.get('win_pct',0):.1%}  Wilson(orig)≈--")
            print(f"        Current:  {row.get('wins',0)}-{row.get('losses',0)}  win={info['live_winpct']:.1%}  "
                  f"Wilson={info['live_wilson']:.3f}  (threshold={REMOVE_WILSON})")
            if info.get("recent_wilson") is not None:
                print(f"        30d trend: Wilson={info['recent_wilson']:.3f}")
        print()

    if warnings:
        print(f"  WARNINGS — {len(warnings)} pattern(s) degrading:")
        for info in warnings:
            pat = info["pat"]
            label = pat.get("label") or pat.get("name") or pat.get("id", "?")
            row = info["row"]
            print(f"    ⚠ [{pat.get('tier_eligible','?')}] {label}")
            print(f"        Current: {row.get('wins',0)}-{row.get('losses',0)}  win={info['live_winpct']:.1%}  "
                  f"Wilson={info['live_wilson']:.3f}  (warn threshold={WARN_WILSON})")
            if info.get("recent_wilson") is not None:
                print(f"        30d trend: Wilson={info['recent_wilson']:.3f}")
        print()

    if healthy:
        print(f"  HEALTHY — {len(healthy)} pattern(s) performing as expected:")
        for info in healthy:
            pat = info["pat"]
            label = pat.get("label") or pat.get("name") or pat.get("id", "?")
            row = info["row"]
            rw = info.get("recent_wilson")
            rw_str = f"  30d={rw:.3f}" if rw is not None else ""
            print(f"    ✓ [{pat.get('tier_eligible','?')}] {label:45s}  "
                  f"{row.get('wins',0)}-{row.get('losses',0)}  "
                  f"Wilson={info['live_wilson']:.3f}{rw_str}")
        print()

    if candidates:
        print(f"  NEW CANDIDATES — {len(candidates)} pattern(s) qualifying for registry:")
        for c in candidates:
            row = c["row"]
            rw = c.get("recent_wilson")
            rw_str = f"  30d={rw:.3f}" if rw is not None else "  30d=n/a"
            direction = row.get("direction", "?")
            is_team = c.get("is_team_specific", False)
            dir_label = f"team={direction}" if is_team else direction
            print(f"    + [{c['suggested_tier']}] {row.get('sources_combo')} / "
                  f"{row.get('market_type')} / {dir_label}")
            print(f"        {row.get('wins',0)}-{row.get('losses',0)}  n={row.get('n',0)}  "
                  f"win={row.get('win_pct',0):.1%}  Wilson={c['live_wilson']:.3f}{rw_str}")
        print()
    else:
        print("  No new candidates above threshold.")
        print()

    print("=" * 70)
    print()

    if critical:
        return 2
    if warnings:
        return 1
    return 0


def apply_hist_updates(
    registry: List[Dict],
    rows: List[Dict[str, Any]],
) -> bool:
    """
    Update the `hist` block inside each PATTERN_REGISTRY entry in score_signals.py
    with fresh wins/losses/win_pct/n from the latest report. Does NOT add or remove
    patterns — only refreshes the statistics.

    Returns True if any changes were made.
    """
    updates: List[Tuple[str, str, str]] = []  # (pat_id, old_hist_str, new_hist_str)

    for pat in registry:
        row = find_report_row(pat, rows)
        if row is None:
            continue

        wins = row.get("wins", 0)
        losses = row.get("losses", 0)
        n = row.get("n", 0)
        win_pct = round(row.get("win_pct", 0.0), 3)

        pat_id = pat.get("id") or pat.get("name") or pat.get("label", "")
        old_hist = pat.get("hist", {})

        # Only update if numbers have changed
        if (old_hist.get("wins") == wins and
                old_hist.get("losses") == losses and
                old_hist.get("n") == n):
            continue

        old_record = old_hist.get("record", "?-?")
        new_record = f"{wins}-{losses}"
        old_hist_repr = (
            f'"record": "{old_record}", '
            f'"win_pct": {old_hist.get("win_pct", 0)}, '
            f'"n": {old_hist.get("n", 0)}'
        )
        new_hist_repr = (
            f'"record": "{new_record}", '
            f'"win_pct": {win_pct}, '
            f'"n": {n}'
        )
        updates.append((pat_id, old_record, new_record))

    if not updates:
        print("  No hist updates needed — all patterns already up to date.")
        return False

    # Re-write score_signals.py: find each pattern's hist block and update it.
    # We use a targeted regex on the "record" line within each pattern block.
    src = SCORE_SIGNALS.read_text()

    changed = False
    for pat in registry:
        row = find_report_row(pat, rows)
        if row is None:
            continue

        wins = row.get("wins", 0)
        losses = row.get("losses", 0)
        n = row.get("n", 0)
        win_pct = round(row.get("win_pct", 0.0), 3)
        old_hist = pat.get("hist", {})

        if (old_hist.get("wins") == wins and
                old_hist.get("losses") == losses and
                old_hist.get("n") == n):
            continue

        old_record = old_hist.get("record", "")
        new_record = f"{wins}-{losses}"
        old_n = old_hist.get("n", 0)
        old_wp = old_hist.get("win_pct", 0)

        # Match the exact hist dict literal in source: {"record": "X-Y", "win_pct": Z, "n": N}
        # Use a pattern that's specific enough to avoid false matches.
        pat_re = (
            r'("record":\s*"' + re.escape(old_record) + r'"'
            r',\s*"win_pct":\s*' + re.escape(str(old_wp)) + r','
            r'\s*"n":\s*' + re.escape(str(old_n)) + r')'
        )
        replacement = (
            f'"record": "{new_record}", '
            f'"win_pct": {win_pct}, '
            f'"n": {n}'
        )
        new_src, count = re.subn(pat_re, replacement, src)
        if count > 0:
            src = new_src
            changed = True
            label = pat.get("label") or pat.get("name") or pat.get("id", "?")
            print(f"  Updated [{label}]: {old_record} → {new_record}  n={old_n}→{n}  win_pct={old_wp}→{win_pct}")

    if changed:
        SCORE_SIGNALS.write_text(src)
        print(f"  Wrote updated PATTERN_REGISTRY to {SCORE_SIGNALS}")
    else:
        print("  No matching hist blocks found in source — check regex patterns.")

    return changed


# ── CLI ────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Pattern health check and candidate discovery.")
    parser.add_argument("--apply", action="store_true",
                        help="Update hist blocks in PATTERN_REGISTRY with fresh stats.")
    parser.add_argument("--min-n", type=int, default=MIN_N_DEFAULT,
                        help=f"Min sample size for candidates (default: {MIN_N_DEFAULT})")
    parser.add_argument("--min-wilson", type=float, default=B_TIER_WILSON,
                        help=f"Min Wilson lower bound for B-tier (default: {B_TIER_WILSON})")
    parser.add_argument("--sport", choices=["NBA", "NCAAB"], default="NBA")
    args = parser.parse_args()

    rows = load_combo_direction_report(args.sport)
    if not rows:
        print(f"  [warn] No report rows found for {args.sport}. Skipping.")
        sys.exit(0)

    recent = load_recent_trends(args.sport)
    registry = load_registry(args.sport)

    healthy, warnings, critical = check_health(registry, rows, recent)
    candidates = find_candidates(rows, registry, recent, args.min_n, args.min_wilson)

    if args.apply:
        print(f"\n  Applying hist updates for {args.sport} PATTERN_REGISTRY...")
        apply_hist_updates(registry, rows)
    else:
        exit_code = print_report(healthy, warnings, critical, candidates)
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
