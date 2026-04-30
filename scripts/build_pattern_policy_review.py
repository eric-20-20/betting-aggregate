#!/usr/bin/env python3
"""Build a pattern policy review pack for manual decision-making.

Reads the pattern audit CSVs and produces:
  - pattern_policy_candidates.csv  (every pattern with proposed policy)
  - pattern_policy_review.md       (human-readable summary)

Does NOT change production scoring, pattern registry, or score_signals.py.

Usage:
    python3 scripts/build_pattern_policy_review.py
"""

from __future__ import annotations

import csv
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
AUDIT_DIR = REPO_ROOT / "data" / "reports" / "pattern_audit"


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def safe_float(val: str, default: float = 0.0) -> float:
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def safe_int(val: str, default: int = 0) -> int:
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# Policy rules
# ---------------------------------------------------------------------------

def propose_policy(row: Dict[str, str], sport_specific: Dict[str, Dict]) -> tuple:
    """Return (proposed_policy, proposed_reason, audit_flag)."""
    sport = row.get("sport", "")
    wilson = safe_float(row.get("wilson_lower"))
    n = safe_int(row.get("n"))
    wins = safe_int(row.get("wins"))
    losses = safe_int(row.get("losses"))
    win_rate = safe_float(row.get("win_rate"))
    market = row.get("market_type", "")
    combo = row.get("sources_combo", "")
    total = wins + losses

    # MLB: watchlist-only unless very strong
    if sport == "MLB":
        if total >= 150 and wilson >= 0.52:
            return "keep_auto_a", "MLB strong pattern exceeds threshold", "mlb_strong"
        if total >= 75 and wilson >= 0.50:
            return "watchlist_only", "MLB approaching but needs more sample", "mlb_watchlist"
        return "watchlist_only", "MLB patterns not yet mature", "mlb_immature"

    # Insufficient sample
    if total < 25:
        return "needs_more_sample", f"n={total} < 25", "low_sample"

    # Severe losers
    if wilson < 0.20 and total >= 25:
        return "exclude_from_auto_a", f"Severe: wilson={wilson:.3f}, {wins}W-{losses}L", "severe_loser"

    if wilson < 0.35 and total >= 30:
        return "exclude_from_auto_a", f"Very weak: wilson={wilson:.3f}, {wins}W-{losses}L", "very_weak"

    if wilson < 0.42 and total >= 30:
        return "exclude_from_auto_a", f"Below break-even: wilson={wilson:.3f}", "below_breakeven"

    if wilson < 0.46 and total >= 25:
        return "exclude_from_auto_a", f"Marginal: wilson={wilson:.3f}, insufficient edge", "marginal"

    # Sport-specific divergence
    sp = sport_specific.get(combo)
    if sp:
        nba_w = safe_float(sp.get("nba_wilson"))
        mlb_w = safe_float(sp.get("mlb_wilson"))
        nba_n = safe_int(sp.get("nba_n"))
        mlb_n = safe_int(sp.get("mlb_n"))
        if nba_n >= 25 and mlb_n >= 25 and abs(nba_w - mlb_w) > 0.08:
            return "sport_specific_only", f"NBA wilson={nba_w:.3f} vs MLB wilson={mlb_w:.3f}", "sport_divergence"

    # Strong keepers
    if wilson >= 0.52 and total >= 75:
        return "keep_auto_a", f"Strong: wilson={wilson:.3f}, n={total}", "strong"

    if wilson >= 0.50 and total >= 50:
        return "keep_auto_a", f"Solid: wilson={wilson:.3f}, n={total}", "solid"

    # Moderate — investigate
    if wilson >= 0.46 and total >= 50:
        return "keep_auto_a", f"Acceptable edge: wilson={wilson:.3f}", "acceptable"

    if wilson >= 0.46 and total < 50:
        return "needs_more_sample", f"Edge present but n={total} < 50", "promising_low_n"

    return "investigate_data_quality", f"Unclassified: wilson={wilson:.3f}, n={total}", "unclassified"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("  PATTERN POLICY REVIEW BUILDER")
    print("=" * 60)

    # Load audit data
    by_sport = read_csv(AUDIT_DIR / "pattern_performance_by_sport.csv")
    sport_specific_raw = read_csv(AUDIT_DIR / "sport_specific_patterns.csv")
    strong = read_csv(AUDIT_DIR / "strong_patterns.csv")
    weak = read_csv(AUDIT_DIR / "weak_patterns.csv")
    suspicious = read_csv(AUDIT_DIR / "suspicious_a_tier_patterns.csv")

    # Build sport-specific lookup by combo
    sport_specific = {r["sources_combo"]: r for r in sport_specific_raw}

    # Build strong/weak/suspicious sets for cross-referencing
    strong_ids = {r["pattern_id"] for r in strong}
    weak_ids = {r["pattern_id"] for r in weak}
    suspicious_ids = {r["pattern_id"] for r in suspicious}

    # Process each pattern
    candidates = []
    for row in by_sport:
        pid = row.get("pattern_id", "")
        policy, reason, flag = propose_policy(row, sport_specific)

        # Determine current tier hint
        current_tier = ""
        if pid in strong_ids:
            current_tier = "strong_list"
        elif pid in suspicious_ids:
            current_tier = "suspicious_list"
        elif pid in weak_ids:
            current_tier = "weak_list"

        candidates.append({
            "pattern_id": pid,
            "sport": row.get("sport", ""),
            "market_type": row.get("market_type", ""),
            "source_combo": row.get("sources_combo", ""),
            "current_tier_if_known": current_tier,
            "n": row.get("n", ""),
            "wins": row.get("wins", ""),
            "losses": row.get("losses", ""),
            "pushes": row.get("pushes", ""),
            "win_rate": row.get("win_rate", ""),
            "wilson_lower": row.get("wilson_lower", ""),
            "units": row.get("units", ""),
            "roi": row.get("roi", ""),
            "confidence_bucket": row.get("confidence_bucket", ""),
            "audit_flag": flag,
            "proposed_policy": policy,
            "proposed_reason": reason,
            "manual_decision": "",
            "manual_note": "",
        })

    # Write CSV
    csv_path = AUDIT_DIR / "pattern_policy_candidates.csv"
    columns = [
        "pattern_id", "sport", "market_type", "source_combo", "current_tier_if_known",
        "n", "wins", "losses", "pushes", "win_rate", "wilson_lower", "units", "roi",
        "confidence_bucket", "audit_flag", "proposed_policy", "proposed_reason",
        "manual_decision", "manual_note",
    ]
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for c in candidates:
            writer.writerow(c)
    print(f"  Wrote {csv_path.name} ({len(candidates)} rows)")

    # Count policies
    policy_counts = Counter(c["proposed_policy"] for c in candidates)
    flag_counts = Counter(c["audit_flag"] for c in candidates)

    # Build markdown
    lines = []
    lines.append("# Pattern Policy Review\n")
    lines.append(f"Generated: {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n")
    lines.append("**WARNING: No production scoring changes have been applied.**")
    lines.append("This is a read-only review. Edit `manual_decision` and `manual_note` columns")
    lines.append("in `pattern_policy_candidates.csv` to record your decisions.\n")

    lines.append("## Policy Distribution\n")
    lines.append("| Policy | Count |")
    lines.append("|--------|------:|")
    for policy, count in sorted(policy_counts.items(), key=lambda x: -x[1]):
        lines.append(f"| {policy} | {count} |")

    lines.append(f"\n## Audit Flag Distribution\n")
    lines.append("| Flag | Count |")
    lines.append("|------|------:|")
    for flag, count in sorted(flag_counts.items(), key=lambda x: -x[1]):
        lines.append(f"| {flag} | {count} |")

    # Top exclusions
    exclusions = [c for c in candidates if c["proposed_policy"] == "exclude_from_auto_a"]
    exclusions.sort(key=lambda c: safe_float(c["wilson_lower"]))
    lines.append(f"\n## Top Proposed Exclusions ({len(exclusions)} total)\n")
    lines.append("These patterns have wilson below acceptable thresholds and should not be in A-tier.\n")
    for c in exclusions[:15]:
        lines.append(
            f"- **[{c['sport']}] {c['source_combo']} / {c['market_type']}** — "
            f"{c['wins']}W-{c['losses']}L ({safe_float(c['win_rate']):.1%}) "
            f"wilson={c['wilson_lower']} n={c['n']} — _{c['proposed_reason']}_"
        )

    # Top keeps
    keeps = [c for c in candidates if c["proposed_policy"] == "keep_auto_a"]
    keeps.sort(key=lambda c: -safe_float(c["wilson_lower"]))
    lines.append(f"\n## Top Proposed Keeps ({len(keeps)} total)\n")
    lines.append("These patterns have demonstrated positive edge at sufficient sample.\n")
    for c in keeps[:15]:
        lines.append(
            f"- **[{c['sport']}] {c['source_combo']} / {c['market_type']}** — "
            f"{c['wins']}W-{c['losses']}L ({safe_float(c['win_rate']):.1%}) "
            f"wilson={c['wilson_lower']} n={c['n']} [{c['confidence_bucket']}]"
        )

    # Sport-specific
    sport_only = [c for c in candidates if c["proposed_policy"] == "sport_specific_only"]
    if sport_only:
        lines.append(f"\n## Sport-Specific Patterns ({len(sport_only)} total)\n")
        lines.append("These combos perform differently in NBA vs MLB.\n")
        for c in sport_only:
            sp = sport_specific.get(c["source_combo"], {})
            lines.append(
                f"- **{c['source_combo']}** — NBA: {sp.get('nba_wins','?')}W-{sp.get('nba_losses','?')}L "
                f"(wilson={sp.get('nba_wilson','?')}) / "
                f"MLB: {sp.get('mlb_wins','?')}W-{sp.get('mlb_losses','?')}L "
                f"(wilson={sp.get('mlb_wilson','?')})"
            )

    # MLB watchlist
    mlb_watch = [c for c in candidates if c["sport"] == "MLB"]
    lines.append(f"\n## MLB Status ({len(mlb_watch)} patterns)\n")
    mlb_policy_counts = Counter(c["proposed_policy"] for c in mlb_watch)
    for p, cnt in mlb_policy_counts.most_common():
        lines.append(f"- {p}: {cnt}")
    lines.append("\n**No MLB patterns are recommended for A-tier promotion at this time.**")
    lines.append("MLB data is too young for production scoring decisions.")

    # Needs manual review
    investigate = [c for c in candidates if c["proposed_policy"] == "investigate_data_quality"]
    if investigate:
        lines.append(f"\n## Needs Investigation ({len(investigate)} patterns)\n")
        for c in investigate[:10]:
            lines.append(
                f"- [{c['sport']}] {c['source_combo']} / {c['market_type']} — "
                f"wilson={c['wilson_lower']} n={c['n']} — {c['proposed_reason']}"
            )

    lines.append("\n## What Requires Your Manual Review\n")
    lines.append("1. **Exclusion list**: Confirm the proposed exclusions are correct before applying")
    lines.append("2. **Sport-specific patterns**: Decide whether to split scoring by sport")
    lines.append("3. **Keep list**: Verify strong patterns aren't benefiting from data bias")
    lines.append("4. **MLB watchlist**: Decide when to re-evaluate (recommended: after 4+ weeks of grading)")
    lines.append("5. **Edge cases**: Patterns with wilson 0.42-0.46 need judgment call\n")
    lines.append("## How to Apply Decisions\n")
    lines.append("1. Open `pattern_policy_candidates.csv`")
    lines.append("2. Fill in `manual_decision` column with your chosen policy")
    lines.append("3. Add notes in `manual_note` column")
    lines.append("4. Save and share for implementation")
    lines.append("5. A separate script will read your decisions and update the pattern registry\n")
    lines.append("**No changes have been made to production scoring.**")

    md_path = AUDIT_DIR / "pattern_policy_review.md"
    md_path.write_text("\n".join(lines) + "\n")
    print(f"  Wrote {md_path.name}")

    # Print summary
    print(f"\n  Policy distribution:")
    for policy, count in sorted(policy_counts.items(), key=lambda x: -x[1]):
        print(f"    {policy:30s} {count:4d}")

    print(f"\n  Top 10 proposed exclusions:")
    for c in exclusions[:10]:
        print(f"    [{c['sport']}] {c['source_combo']:40s} / {c['market_type']:15s} "
              f"wilson={c['wilson_lower']:>6s} n={c['n']:>5s} — {c['audit_flag']}")

    print(f"\n  Top 10 proposed keeps:")
    for c in keeps[:10]:
        print(f"    [{c['sport']}] {c['source_combo']:40s} / {c['market_type']:15s} "
              f"wilson={c['wilson_lower']:>6s} n={c['n']:>5s}")


if __name__ == "__main__":
    main()
