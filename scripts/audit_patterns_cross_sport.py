#!/usr/bin/env python3
"""Cross-sport pattern audit — read-only analysis of NBA + MLB graded signals.

Joins signals with grades, computes performance by source_combo × market × sport,
and outputs CSV + summary reports.  Does NOT modify scoring, tiers, or registries.

Usage:
    python3 scripts/audit_patterns_cross_sport.py
    python3 scripts/audit_patterns_cross_sport.py --since 2026-01-01
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

OUT_DIR = REPO_ROOT / "data" / "reports" / "pattern_audit"

TERMINAL_RESULTS = {"WIN", "LOSS", "PUSH"}


# ---------------------------------------------------------------------------
# Wilson lower bound
# ---------------------------------------------------------------------------

def wilson_lower(wins: int, n: int, z: float = 1.96) -> float:
    if n == 0:
        return 0.0
    p = wins / n
    denom = 1 + z * z / n
    center = p + z * z / (2 * n)
    spread = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (center - spread) / denom


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows = []
    if not path.exists():
        return rows
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def load_sport_data(sport: str, since: Optional[str] = None) -> List[Dict[str, Any]]:
    """Load joined signal+grade rows for a sport."""
    if sport == "NBA":
        signals_path = REPO_ROOT / "data" / "ledger" / "signals_latest.jsonl"
        grades_path = REPO_ROOT / "data" / "ledger" / "grades_latest.jsonl"
    elif sport == "MLB":
        signals_path = REPO_ROOT / "data" / "ledger" / "mlb" / "signals_latest.jsonl"
        grades_path = REPO_ROOT / "data" / "ledger" / "mlb" / "grades_latest.jsonl"
    else:
        return []

    signals = read_jsonl(signals_path)
    grades = read_jsonl(grades_path)

    grade_by_id = {g["signal_id"]: g for g in grades if g.get("signal_id")}

    joined = []
    for sig in signals:
        sid = sig.get("signal_id")
        if not sid:
            continue
        grade = grade_by_id.get(sid)
        if not grade:
            continue
        status = grade.get("status", "")
        result = grade.get("result")
        if status not in TERMINAL_RESULTS:
            continue

        day_key = sig.get("day_key", "")
        date_str = _day_key_to_date(day_key)
        if since and date_str and date_str < since:
            continue

        joined.append({
            "sport": sport,
            "signal_id": sid,
            "sources_combo": sig.get("sources_combo", ""),
            "market_type": sig.get("market_type", ""),
            "direction": sig.get("direction", ""),
            "atomic_stat": sig.get("atomic_stat", ""),
            "signal_type": sig.get("signal_type", ""),
            "day_key": day_key,
            "date": date_str,
            "result": result,
            "units": grade.get("units"),
            "odds": grade.get("odds") or sig.get("best_odds"),
            "roi_eligible": grade.get("roi_eligible", False),
            "source_count": len((sig.get("sources_combo") or "").split("|")) if sig.get("sources_combo") else 0,
            "line": sig.get("line"),
        })

    return joined


def _day_key_to_date(dk: str) -> Optional[str]:
    if not dk:
        return None
    parts = dk.split(":")
    if len(parts) >= 4:
        return f"{parts[1]}-{parts[2]}-{parts[3]}"
    return None


# ---------------------------------------------------------------------------
# Bucketing
# ---------------------------------------------------------------------------

@dataclass
class Bucket:
    sport: str = ""
    pattern_id: str = ""
    sources_combo: str = ""
    market_type: str = ""
    direction: str = ""
    stat_type: str = ""
    n: int = 0
    wins: int = 0
    losses: int = 0
    pushes: int = 0
    units_sum: float = 0.0
    units_count: int = 0
    odds_sum: float = 0.0
    odds_count: int = 0
    roi_eligible_count: int = 0

    @property
    def win_rate(self) -> float:
        denom = self.wins + self.losses
        return self.wins / denom if denom > 0 else 0.0

    @property
    def wilson(self) -> float:
        return wilson_lower(self.wins, self.wins + self.losses)

    @property
    def roi(self) -> float:
        if self.units_count == 0:
            return 0.0
        return self.units_sum / self.units_count

    @property
    def avg_odds(self) -> Optional[float]:
        return self.odds_sum / self.odds_count if self.odds_count > 0 else None

    @property
    def confidence(self) -> str:
        total = self.wins + self.losses
        if total >= 150:
            return "strong"
        if total >= 75:
            return "actionable"
        if total >= 25:
            return "watchlist"
        return "insufficient_sample"

    def add(self, row: Dict[str, Any]) -> None:
        self.n += 1
        r = row.get("result")
        if r == "WIN":
            self.wins += 1
        elif r == "LOSS":
            self.losses += 1
        elif r == "PUSH":
            self.pushes += 1
        u = row.get("units")
        if u is not None:
            self.units_sum += u
            self.units_count += 1
        odds = row.get("odds")
        if odds is not None:
            try:
                self.odds_sum += float(odds)
                self.odds_count += 1
            except (ValueError, TypeError):
                pass
        if row.get("roi_eligible"):
            self.roi_eligible_count += 1

    def to_dict(self, recommendation: str = "") -> Dict[str, Any]:
        return {
            "sport": self.sport,
            "pattern_id": self.pattern_id,
            "sources_combo": self.sources_combo,
            "market_type": self.market_type,
            "direction": self.direction,
            "stat_type": self.stat_type,
            "n": self.n,
            "wins": self.wins,
            "losses": self.losses,
            "pushes": self.pushes,
            "win_rate": round(self.win_rate, 4),
            "wilson_lower": round(self.wilson, 4),
            "units": round(self.units_sum, 2),
            "roi": round(self.roi, 4),
            "avg_odds": round(self.avg_odds, 1) if self.avg_odds else "",
            "roi_eligible_count": self.roi_eligible_count,
            "confidence_bucket": self.confidence,
            "recommendation": recommendation,
        }


def recommend(b: Bucket) -> str:
    total = b.wins + b.losses
    if total < 25:
        return "needs_more_sample"
    if b.wilson >= 0.52 and total >= 75:
        return "keep"
    if b.wilson >= 0.50 and total >= 50:
        return "promote_candidate"
    if b.wilson < 0.42 and total >= 30:
        return "demote_candidate"
    if 0.42 <= b.wilson < 0.46 and total >= 50:
        return "investigate_data_quality"
    return "needs_more_sample"


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def aggregate(rows: List[Dict[str, Any]], key_fn, sport_override: str = "") -> Dict[str, Bucket]:
    buckets: Dict[str, Bucket] = {}
    for row in rows:
        key = key_fn(row)
        if key not in buckets:
            b = Bucket()
            b.sport = sport_override or row.get("sport", "")
            b.sources_combo = row.get("sources_combo", "")
            b.market_type = row.get("market_type", "")
            b.direction = row.get("direction", "")
            b.pattern_id = str(key)
            buckets[key] = b
        buckets[key].add(row)
    return buckets


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------

COLUMNS = [
    "sport", "pattern_id", "sources_combo", "market_type", "direction", "stat_type",
    "n", "wins", "losses", "pushes", "win_rate", "wilson_lower",
    "units", "roi", "avg_odds", "roi_eligible_count",
    "confidence_bucket", "recommendation",
]


def write_csv(path: Path, rows: List[Dict[str, Any]], columns: List[str] = COLUMNS):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"  Wrote {path.name} ({len(rows)} rows)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Cross-sport pattern audit (read-only)")
    ap.add_argument("--since", help="Only include grades from YYYY-MM-DD onward")
    args = ap.parse_args()

    print("=" * 60)
    print("  CROSS-SPORT PATTERN AUDIT")
    print("=" * 60)

    # Load data
    nba_rows = load_sport_data("NBA", args.since)
    mlb_rows = load_sport_data("MLB", args.since)
    all_rows = nba_rows + mlb_rows

    # MLB: separate game markets from props for headline metrics
    mlb_game = [r for r in mlb_rows if r["market_type"] in ("moneyline", "spread", "total")]
    mlb_prop = [r for r in mlb_rows if r["market_type"] == "player_prop"]
    nba_game = [r for r in nba_rows if r["market_type"] in ("moneyline", "spread", "total")]
    nba_prop = [r for r in nba_rows if r["market_type"] == "player_prop"]

    print(f"\n  NBA: {len(nba_rows)} graded ({len(nba_game)} game, {len(nba_prop)} prop)")
    print(f"  MLB: {len(mlb_rows)} graded ({len(mlb_game)} game, {len(mlb_prop)} prop)")

    # ── 1. Pattern performance by sport ──
    def combo_market_key(r):
        return (r["sport"], r["sources_combo"], r["market_type"])

    sport_buckets = aggregate(all_rows, combo_market_key)
    sport_rows = []
    for key, b in sorted(sport_buckets.items(), key=lambda x: -(x[1].wins + x[1].losses)):
        b.pattern_id = f"{key[0]}|{key[1]}|{key[2]}"
        b.sport = key[0]
        b.sources_combo = key[1]
        b.market_type = key[2]
        sport_rows.append(b.to_dict(recommend(b)))
    write_csv(OUT_DIR / "pattern_performance_by_sport.csv", sport_rows)

    # ── 2. Combined (cross-sport) ──
    def combo_key(r):
        return (r["sources_combo"], r["market_type"])

    combined_buckets = aggregate(all_rows, combo_key, sport_override="ALL")
    combined_rows = []
    for key, b in sorted(combined_buckets.items(), key=lambda x: -(x[1].wins + x[1].losses)):
        b.pattern_id = f"{key[0]}|{key[1]}"
        b.sources_combo = key[0]
        b.market_type = key[1]
        combined_rows.append(b.to_dict(recommend(b)))
    write_csv(OUT_DIR / "pattern_performance_combined.csv", combined_rows)

    # ── 3. Source combo performance ──
    def combo_only_key(r):
        return (r["sport"], r["sources_combo"])

    source_buckets = aggregate(all_rows, combo_only_key)
    source_rows = []
    for key, b in sorted(source_buckets.items(), key=lambda x: -(x[1].wins + x[1].losses)):
        b.pattern_id = f"{key[0]}|{key[1]}"
        b.sport = key[0]
        b.sources_combo = key[1]
        source_rows.append(b.to_dict(recommend(b)))
    write_csv(OUT_DIR / "source_combo_performance.csv", source_rows)

    # ── 4. Market performance by sport ──
    def market_key(r):
        return (r["sport"], r["market_type"])

    market_buckets = aggregate(all_rows, market_key)
    market_rows = []
    for key, b in sorted(market_buckets.items()):
        b.pattern_id = f"{key[0]}|{key[1]}"
        b.sport = key[0]
        b.market_type = key[1]
        market_rows.append(b.to_dict())
    write_csv(OUT_DIR / "market_performance_by_sport.csv", market_rows)

    # ── 5. Strong patterns (wilson >= 0.50, n >= 50) ──
    strong = [r for r in sport_rows if r["wilson_lower"] >= 0.50 and r["n"] >= 50]
    strong.sort(key=lambda r: -r["wilson_lower"])
    write_csv(OUT_DIR / "strong_patterns.csv", strong)

    # ── 6. Weak patterns (wilson < 0.42, n >= 30) ──
    weak = [r for r in sport_rows if r["wilson_lower"] < 0.42 and r["n"] >= 30]
    weak.sort(key=lambda r: r["wilson_lower"])
    write_csv(OUT_DIR / "weak_patterns.csv", weak)

    # ── 7. Suspicious A-tier (low wilson or low sample in patterns labeled A-tier) ──
    # Load pattern registry if available
    suspicious = [r for r in sport_rows
                  if r["wilson_lower"] < 0.46
                  and r["n"] >= 25
                  and r["confidence_bucket"] in ("watchlist", "actionable", "strong")]
    suspicious.sort(key=lambda r: r["wilson_lower"])
    write_csv(OUT_DIR / "suspicious_a_tier_patterns.csv", suspicious)

    # ── 8. Sport-specific patterns ──
    # Find combos that perform differently between NBA and MLB
    nba_combo_buckets = aggregate(nba_game, lambda r: r["sources_combo"])
    mlb_combo_buckets = aggregate(mlb_game, lambda r: r["sources_combo"])

    sport_specific = []
    all_combos = set(nba_combo_buckets) | set(mlb_combo_buckets)
    for combo in sorted(all_combos):
        nba_b = nba_combo_buckets.get(combo)
        mlb_b = mlb_combo_buckets.get(combo)
        nba_n = (nba_b.wins + nba_b.losses) if nba_b else 0
        mlb_n = (mlb_b.wins + mlb_b.losses) if mlb_b else 0
        if nba_n < 10 and mlb_n < 10:
            continue
        nba_wr = nba_b.win_rate if nba_b and nba_n > 0 else None
        mlb_wr = mlb_b.win_rate if mlb_b and mlb_n > 0 else None
        nba_w = nba_b.wilson if nba_b else 0.0
        mlb_w = mlb_b.wilson if mlb_b else 0.0
        sport_specific.append({
            "sources_combo": combo,
            "nba_n": nba_n,
            "nba_wins": nba_b.wins if nba_b else 0,
            "nba_losses": nba_b.losses if nba_b else 0,
            "nba_win_rate": round(nba_wr, 4) if nba_wr is not None else "",
            "nba_wilson": round(nba_w, 4),
            "mlb_n": mlb_n,
            "mlb_wins": mlb_b.wins if mlb_b else 0,
            "mlb_losses": mlb_b.losses if mlb_b else 0,
            "mlb_win_rate": round(mlb_wr, 4) if mlb_wr is not None else "",
            "mlb_wilson": round(mlb_w, 4),
            "delta_wilson": round(nba_w - mlb_w, 4) if nba_n >= 10 and mlb_n >= 10 else "",
            "recommendation": "sport_specific_only" if abs(nba_w - mlb_w) > 0.08 and nba_n >= 25 and mlb_n >= 25 else "",
        })
    sport_specific_cols = [
        "sources_combo", "nba_n", "nba_wins", "nba_losses", "nba_win_rate", "nba_wilson",
        "mlb_n", "mlb_wins", "mlb_losses", "mlb_win_rate", "mlb_wilson",
        "delta_wilson", "recommendation",
    ]
    write_csv(OUT_DIR / "sport_specific_patterns.csv", sport_specific, sport_specific_cols)

    # ── 9. Tier performance (approximate from source count) ──
    def tier_key(r):
        sc = r.get("source_count", 0)
        if sc >= 4:
            tier = "A (4+ sources)"
        elif sc >= 3:
            tier = "B (3 sources)"
        elif sc >= 2:
            tier = "C (2 sources)"
        else:
            tier = "D (1 source)"
        return (r["sport"], tier, r["market_type"])

    tier_buckets = aggregate(all_rows, tier_key)
    tier_rows = []
    for key, b in sorted(tier_buckets.items()):
        b.pattern_id = f"{key[0]}|{key[1]}|{key[2]}"
        b.sport = key[0]
        b.sources_combo = key[1]
        b.market_type = key[2]
        tier_rows.append(b.to_dict())
    write_csv(OUT_DIR / "tier_performance.csv", tier_rows)

    # ── 10. Summary markdown ──
    summary_lines = []
    summary_lines.append("# Cross-Sport Pattern Audit Summary\n")
    summary_lines.append(f"Generated: {datetime.now(tz=__import__('datetime').timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n")
    if args.since:
        summary_lines.append(f"Date filter: since {args.since}\n")

    summary_lines.append(f"\n## Data Coverage\n")
    summary_lines.append(f"- NBA graded rows: {len(nba_rows)} ({len(nba_game)} game, {len(nba_prop)} prop)")
    summary_lines.append(f"- MLB graded rows: {len(mlb_rows)} ({len(mlb_game)} game, {len(mlb_prop)} prop)")

    # Headline win rates
    nba_wr = sum(1 for r in nba_game if r["result"] == "WIN") / max(1, sum(1 for r in nba_game if r["result"] in ("WIN", "LOSS")))
    mlb_wr = sum(1 for r in mlb_game if r["result"] == "WIN") / max(1, sum(1 for r in mlb_game if r["result"] in ("WIN", "LOSS")))
    summary_lines.append(f"\n## Headline Win Rates (game markets only)")
    summary_lines.append(f"- NBA: {nba_wr:.1%}")
    summary_lines.append(f"- MLB: {mlb_wr:.1%}")

    # Top 10 strongest
    summary_lines.append(f"\n## Top 10 Strongest Patterns (by Wilson, n >= 50)")
    strong_top = sorted(
        [r for r in sport_rows if r["n"] >= 50],
        key=lambda r: -r["wilson_lower"]
    )[:10]
    for i, r in enumerate(strong_top, 1):
        summary_lines.append(
            f"  {i:2d}. [{r['sport']}] {r['sources_combo']} / {r['market_type']} — "
            f"{r['wins']}W-{r['losses']}L ({r['win_rate']:.1%}) wilson={r['wilson_lower']:.3f} n={r['n']} [{r['confidence_bucket']}]"
        )

    # Top 10 weakest
    summary_lines.append(f"\n## Top 10 Weakest Patterns (by Wilson, n >= 30)")
    weak_top = sorted(
        [r for r in sport_rows if r["wins"] + r["losses"] >= 30],
        key=lambda r: r["wilson_lower"]
    )[:10]
    for i, r in enumerate(weak_top, 1):
        summary_lines.append(
            f"  {i:2d}. [{r['sport']}] {r['sources_combo']} / {r['market_type']} — "
            f"{r['wins']}W-{r['losses']}L ({r['win_rate']:.1%}) wilson={r['wilson_lower']:.3f} n={r['n']} [{r['confidence_bucket']}]"
        )

    # A-tier risk list
    summary_lines.append(f"\n## A-Tier Risk List (wilson < 0.46, n >= 25)")
    if suspicious:
        for r in suspicious[:10]:
            summary_lines.append(
                f"  - [{r['sport']}] {r['sources_combo']} / {r['market_type']} — "
                f"{r['wins']}W-{r['losses']}L ({r['win_rate']:.1%}) wilson={r['wilson_lower']:.3f} [{r['confidence_bucket']}]"
            )
    else:
        summary_lines.append("  (none)")

    # Market observations
    summary_lines.append(f"\n## Market Observations")
    for r in market_rows:
        label = f"[{r['sport']}] {r['market_type']}"
        summary_lines.append(
            f"  - {label:30s} {r['wins']}W-{r['losses']}L ({r['win_rate']:.1%}) "
            f"wilson={r['wilson_lower']:.3f} n={r['n']}"
        )

    # Sport-specific observations
    summary_lines.append(f"\n## Sport-Specific Divergences (|delta_wilson| > 0.05)")
    divergent = [r for r in sport_specific if r.get("delta_wilson") and abs(float(r["delta_wilson"])) > 0.05]
    for r in divergent[:10]:
        nba_wr_str = f"{float(r['nba_win_rate']):.1%}" if r["nba_win_rate"] else "n/a"
        mlb_wr_str = f"{float(r['mlb_win_rate']):.1%}" if r["mlb_win_rate"] else "n/a"
        summary_lines.append(
            f"  - {r['sources_combo']:40s} NBA={nba_wr_str} (n={r['nba_n']}) "
            f"MLB={mlb_wr_str} (n={r['mlb_n']}) delta={r['delta_wilson']}"
        )

    # Recommendations
    summary_lines.append(f"\n## Recommended Next Changes (do NOT apply automatically)")
    summary_lines.append(f"  1. Review A-tier risk list — any pattern with wilson < 0.46 at n >= 50 should be investigated")
    summary_lines.append(f"  2. Review sport-specific divergences — patterns that work in one sport but not the other")
    summary_lines.append(f"  3. MLB player props (n={len(mlb_prop)}) need player name matching fix before inclusion")
    summary_lines.append(f"  4. Run this audit weekly as more MLB data accumulates")
    summary_lines.append(f"  5. Do NOT change production scoring until at least 2 weeks of clean MLB grading data")

    summary_text = "\n".join(summary_lines) + "\n"
    summary_path = OUT_DIR / "pattern_audit_summary.md"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(summary_text)
    print(f"  Wrote pattern_audit_summary.md")

    # Print summary to stdout
    print("\n" + summary_text)


if __name__ == "__main__":
    main()
