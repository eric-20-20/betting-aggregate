#!/usr/bin/env python3
"""
Export pipeline data for the web application.

Reads the daily plays file and report JSONs, sanitizes source/expert names
and ROI data, then writes public (teaser) and private (full) datasets
to the web/ directory structure.

Usage:
    python3 scripts/export_web_data.py
    python3 scripts/export_web_data.py --date 2026-02-24
"""

import argparse
import copy
import json
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

PLAYS_DIR = Path("data/plays")
REPORTS_DIR = Path("data/reports")
WEB_PUBLIC_DIR = Path("web/public/data")
WEB_PRIVATE_DIR = Path("web/data/private")

NUM_TEASER_PICKS = 3

# Source names that must never appear in web-facing data
SOURCE_NAMES = {
    "action", "betql", "covers", "dimers", "sportsline",
    "oddstrader", "vegasinsider", "sportscapping",
}

# Fields to strip from all exported data (incomplete ROI data)
ROI_FIELDS = {"roi", "net_units", "bets_with_units", "units_missing_count",
              "avg_odds", "odds_count", "odds_sum"}


# ── Sanitization helpers ────────────────────────────────────────────

def _consensus_label(count: int) -> str:
    if count >= 3:
        return "3+_source"
    return f"{count}_source"


def _sanitize_lookup_key(dimension: str, lookup_key: str, strength_label: str) -> str:
    """Replace source combo portion in factor lookup_key with strength label."""
    if dimension == "combo_overall":
        return strength_label
    if dimension == "combo_x_market":
        parts = lookup_key.split(" / ")
        if len(parts) > 1:
            parts[0] = strength_label
            return " / ".join(parts)
    return lookup_key


def _strip_roi(d: dict) -> dict:
    """Remove ROI-related fields from a dict."""
    return {k: v for k, v in d.items() if k not in ROI_FIELDS}


def _sanitize_label(label: str, strength_label: str) -> str:
    """Replace source combo names in a label with the consensus strength label."""
    label_lower = label.lower()
    if not any(src in label_lower for src in SOURCE_NAMES):
        return label
    parts = label.split(" / ")
    parts[0] = strength_label + (" overall" if "overall" in parts[0] else "")
    return " / ".join(parts)


def sanitize_play(play: dict) -> dict:
    """Strip source names, expert names, and ROI from a play for web display."""
    play = copy.deepcopy(play)
    sig = play["signal"]

    # Replace named sources with anonymous count
    source_count = len(sig.get("sources_present", []))
    strength_label = _consensus_label(source_count)
    sig["sources_count"] = source_count
    sig["consensus_strength"] = strength_label
    sig.pop("sources_combo", None)
    sig.pop("sources_present", None)
    sig["experts"] = []

    # Sanitize factor/supporting_factors lookup_keys
    for key in ("factors", "supporting_factors"):
        for factor in play.get(key, []):
            lk = factor.get("lookup_key", "")
            factor["lookup_key"] = _sanitize_label(lk, strength_label)
            for k in list(factor.keys()):
                if k in ROI_FIELDS:
                    del factor[k]

    # Sanitize primary_record label
    pr = play.get("primary_record")
    if pr and "label" in pr:
        pr["label"] = _sanitize_label(pr["label"], strength_label)

    # Sanitize historical_record label
    hr = play.get("historical_record")
    if hr and "label" in hr:
        hr["label"] = _sanitize_label(hr["label"], strength_label)

    # Sanitize recent_trend label
    rt = play.get("recent_trend")
    if rt and "label" in rt:
        rt["label"] = _sanitize_label(rt["label"], strength_label)

    # Sanitize expert_detail — strip expert name, keep numeric fields
    ed = play.get("expert_detail")
    if ed:
        play["expert_detail"] = {
            "win_pct": ed.get("win_pct"),
            "n": ed.get("n"),
            "adjustment": ed.get("adjustment"),
            "role": ed.get("role"),
        }

    # Sanitize matched_pattern — strip source names from label, keep hist
    mp = play.get("matched_pattern")
    if mp:
        safe_label = mp.get("label", "")
        for src in SOURCE_NAMES:
            safe_label = re.sub(re.escape(src), "source", safe_label, flags=re.IGNORECASE)
        # Deduplicate repeated "source" words (e.g. "source+source" → "multi-source")
        safe_label = re.sub(r"source\+source", "multi-source", safe_label)
        play["matched_pattern"] = {
            "label": safe_label,
            "tier_eligible": mp.get("tier_eligible"),
            "hist": mp.get("hist"),
        }

    # Strip summary if it contains source names
    summary = play.get("summary", "")
    for src in SOURCE_NAMES:
        if src in summary.lower():
            play["summary"] = ""
            break

    return play


def _sanitize_recent_trends(data: dict) -> dict:
    """Remove source-combo-specific entries from recent trends report."""
    safe = {
        "meta": data.get("meta", {}),
        "by_consensus_strength": data.get("by_consensus_strength", []),
        "by_market_type": data.get("by_market_type", []),
    }
    # For hot streaks, use anonymous_label and strip source info
    streaks = []
    for s in data.get("top_hot_streaks", []):
        streaks.append({
            "description": s.get("anonymous_label", s.get("description", "")),
            "window": s.get("window"),
            "wins": s.get("wins"),
            "losses": s.get("losses"),
            "n": s.get("n"),
            "win_pct": s.get("win_pct"),
        })
    safe["top_hot_streaks"] = streaks
    return safe


def sanitize_report_rows(rows: list) -> list:
    """Strip ROI fields from report rows."""
    return [_strip_roi(row) for row in rows]


def sanitize_trends(trends_data: dict) -> dict:
    """Remove trends that reveal source or expert names, strip ROI."""
    BLOCKED_REPORTS = {"by_expert", "by_source_surface"}

    safe_trends = []
    for t in trends_data.get("trends", []):
        report = t.get("report", "")
        desc = t.get("description", "").lower()

        # Skip expert/source-revealing trends
        if report in BLOCKED_REPORTS:
            continue
        if any(src in desc for src in SOURCE_NAMES):
            continue

        safe_trends.append(_strip_roi(t))

    trends_data["trends"] = safe_trends
    trends_data["count"] = len(safe_trends)
    return trends_data


# ── Export functions ─────────────────────────────────────────────────

def find_latest_plays_date() -> str:
    """Find the most recent plays file date."""
    plays_files = sorted(PLAYS_DIR.glob("plays_*.json"), reverse=True)
    if not plays_files:
        print("[export] No plays files found in data/plays/")
        sys.exit(1)
    return plays_files[0].stem.replace("plays_", "")


def export_picks(date: str) -> None:
    """Split plays into public teaser + locked summaries and private full data."""
    plays_path = PLAYS_DIR / f"plays_{date}.json"
    if not plays_path.exists():
        print(f"[export] Missing {plays_path}")
        sys.exit(1)

    with open(plays_path) as f:
        data = json.load(f)

    meta = data["meta"]
    plays = data["plays"]

    # Sort by tier (A first) then Wilson score descending
    tier_order = {"A": 0, "B": 1, "C": 2, "D": 3}
    plays.sort(key=lambda p: (
        tier_order.get(p["tier"], 9),
        -(p.get("wilson_score", p.get("composite_score", 0))),
    ))

    # Sanitize quality-gated plays only (A and B tiers)
    sanitized_plays = [sanitize_play(p) for p in plays if p["tier"] in ("A", "B")]

    # Teaser picks: top N A-tier picks with full details
    teaser_picks = []
    for p in sanitized_plays:
        if p["tier"] == "A" and len(teaser_picks) < NUM_TEASER_PICKS:
            teaser_picks.append(p)

    teaser_ids = {p["signal"]["signal_id"] for p in teaser_picks}

    # Locked picks: everything else, stripped of details
    locked_picks = []
    for p in sanitized_plays:
        if p["signal"]["signal_id"] in teaser_ids:
            continue
        sig = p["signal"]
        matchup = f"{sig['away_team']}@{sig['home_team']}"
        locked_picks.append({
            "rank": p["rank"],
            "tier": p["tier"],
            "market_type": sig["market_type"],
            "matchup": matchup,
            "confidence": p.get("confidence", "low"),
            "positive_dimensions": p.get("positive_dimensions", 0),
        })

    public_data = {
        "meta": meta,
        "teaser_picks": teaser_picks,
        "locked_picks": locked_picks,
    }

    # Write public picks
    public_dir = WEB_PUBLIC_DIR / "picks"
    public_dir.mkdir(parents=True, exist_ok=True)
    public_path = public_dir / f"public_{date}.json"
    with open(public_path, "w") as f:
        json.dump(public_data, f, indent=2)
    print(f"[export] Public picks: {public_path} ({len(teaser_picks)} teaser, {len(locked_picks)} locked)")

    # Write private full picks (also sanitized — subscribers see consensus strength, not source names)
    private_data = {
        "meta": meta,
        "plays": sanitized_plays,
    }
    WEB_PRIVATE_DIR.mkdir(parents=True, exist_ok=True)
    private_path = WEB_PRIVATE_DIR / f"picks_{date}.json"
    with open(private_path, "w") as f:
        json.dump(private_data, f, indent=2)
    print(f"[export] Private picks: {private_path} ({len(sanitized_plays)} total)")


def export_reports() -> None:
    """Export sanitized report JSONs to web public directory."""
    reports_dir = WEB_PUBLIC_DIR / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    copied = 0

    # Consensus strength — already anonymous, just strip ROI
    src = REPORTS_DIR / "trends" / "consensus_strength.json"
    if src.exists():
        with open(src) as f:
            data = json.load(f)
        if "rows" in data:
            data["rows"] = sanitize_report_rows(data["rows"])
        dst = reports_dir / "consensus_strength.json"
        with open(dst, "w") as f:
            json.dump(data, f, indent=2)
        copied += 1

    # Market type — already clean, just strip ROI
    src = REPORTS_DIR / "trends" / "market_type.json"
    if src.exists():
        with open(src) as f:
            data = json.load(f)
        for key in ("by_market", "by_market_x_consensus"):
            if key in data:
                data[key] = sanitize_report_rows(data[key])
        dst = reports_dir / "market_type.json"
        with open(dst, "w") as f:
            json.dump(data, f, indent=2)
        copied += 1

    # Stat type — already clean, just strip ROI
    src = REPORTS_DIR / "by_stat_type.json"
    if src.exists():
        with open(src) as f:
            data = json.load(f)
        if "rows" in data:
            data["rows"] = sanitize_report_rows(data["rows"])
        dst = reports_dir / "by_stat_type.json"
        with open(dst, "w") as f:
            json.dump(data, f, indent=2)
        copied += 1

    # Top trends — sanitize out source/expert trends + strip ROI
    src = REPORTS_DIR / "trends" / "top_trends_summary.json"
    if src.exists():
        with open(src) as f:
            data = json.load(f)
        data = sanitize_trends(data)
        dst = reports_dir / "top_trends_summary.json"
        with open(dst, "w") as f:
            json.dump(data, f, indent=2)
        copied += 1

    # Recent trends — strip source-specific data, keep consensus/market
    src = REPORTS_DIR / "recent_trends.json"
    if src.exists():
        with open(src) as f:
            data = json.load(f)
        sanitized = _sanitize_recent_trends(data)
        dst = reports_dir / "recent_trends.json"
        with open(dst, "w") as f:
            json.dump(sanitized, f, indent=2)
        copied += 1

    print(f"[export] Reports: exported {copied} sanitized files to {reports_dir}")


def export_tier_performance() -> None:
    """Generate tier performance summary by aggregating history files."""
    reports_dir = WEB_PUBLIC_DIR / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    dst = reports_dir / "tier_performance.json"

    history_dir = WEB_PRIVATE_DIR / "history"
    if not history_dir.exists():
        with open(dst, "w") as f:
            json.dump([], f)
        print("[export] Tier performance: placeholder (no history dir)")
        return

    tier_stats: dict[str, dict[str, int]] = {}
    for hf in sorted(history_dir.glob("history_*.json")):
        try:
            with open(hf) as f:
                day = json.load(f)
        except Exception:
            continue
        for play in day.get("plays", []):
            tier = play.get("tier")
            result = play.get("result")
            if not tier or result not in ("WIN", "LOSS", "PUSH"):
                continue
            if tier not in tier_stats:
                tier_stats[tier] = {"wins": 0, "losses": 0, "pushes": 0}
            if result == "WIN":
                tier_stats[tier]["wins"] += 1
            elif result == "LOSS":
                tier_stats[tier]["losses"] += 1
            elif result == "PUSH":
                tier_stats[tier]["pushes"] += 1

    tier_data = []
    for tier in sorted(tier_stats):
        s = tier_stats[tier]
        decided = s["wins"] + s["losses"]
        win_pct = s["wins"] / decided if decided > 0 else 0.0
        tier_data.append({
            "tier": tier,
            "wins": s["wins"],
            "losses": s["losses"],
            "pushes": s["pushes"],
            "n": decided + s["pushes"],
            "win_pct": round(win_pct, 4),
        })

    with open(dst, "w") as f:
        json.dump(tier_data, f, indent=2)
    print(f"[export] Tier performance: {dst} ({len(tier_data)} tiers from {len(list(history_dir.glob('history_*.json')))} days)")


def cleanup_stale_reports() -> None:
    """Remove report files that contain source/expert names."""
    reports_dir = WEB_PUBLIC_DIR / "reports"
    stale_files = [
        "by_source_record.json",
        "by_sources_combo_record.json",
        "by_expert_record.json",
        "by_source_record_market.json",
        "coverage_summary.json",
    ]
    removed = 0
    for f in stale_files:
        p = reports_dir / f
        if p.exists():
            p.unlink()
            removed += 1
    if removed:
        print(f"[export] Cleaned up {removed} stale report files")


GRADES_PATH = Path("data/ledger/grades_latest.jsonl")


def _load_grades_index() -> dict:
    """Load grades keyed by signal_id → result string."""
    grades: dict = {}
    if not GRADES_PATH.exists():
        return grades
    with open(GRADES_PATH) as f:
        for line in f:
            g = json.loads(line)
            sid = g.get("signal_id", "")
            if not sid or sid in grades:
                continue
            result = g.get("result")
            if result in ("WIN", "LOSS", "PUSH"):
                grades[sid] = result
            else:
                grades[sid] = "PENDING"
    return grades


def export_history() -> None:
    """Generate graded history files for all past dates (paid members only)."""
    grades = _load_grades_index()
    if not grades:
        print("[export] History: no grades found, skipping")
        return

    history_dir = WEB_PRIVATE_DIR / "history"
    history_dir.mkdir(parents=True, exist_ok=True)

    index_entries = []

    for plays_file in sorted(PLAYS_DIR.glob("plays_*.json")):
        date = plays_file.stem.replace("plays_", "")
        with open(plays_file) as f:
            data = json.load(f)

        meta = data["meta"]
        plays = data["plays"]

        # Sort by tier then score
        tier_order = {"A": 0, "B": 1, "C": 2, "D": 3}
        plays.sort(key=lambda p: (
            tier_order.get(p.get("tier", "D"), 9),
            -(p.get("wilson_score") or p.get("composite_score", 0) or 0),
        ))

        graded_plays = []
        wins, losses, pushes, pending = 0, 0, 0, 0
        a_wins, a_losses = 0, 0

        for p in plays:
            tier = p.get("tier")
            if tier not in ("A", "B"):
                continue

            sanitized = sanitize_play(p)

            sig = p.get("signal", {})
            sid = sig.get("signal_id", "")
            result = grades.get(sid, "PENDING")
            sanitized["result"] = result
            graded_plays.append(sanitized)

            if result == "WIN":
                wins += 1
                if tier == "A":
                    a_wins += 1
            elif result == "LOSS":
                losses += 1
                if tier == "A":
                    a_losses += 1
            elif result == "PUSH":
                pushes += 1
            else:
                pending += 1

        if not graded_plays:
            continue

        day_data = {
            "meta": meta,
            "plays": graded_plays,
            "summary": {
                "wins": wins,
                "losses": losses,
                "pushes": pushes,
                "pending": pending,
            },
        }
        day_path = history_dir / f"history_{date}.json"
        with open(day_path, "w") as f:
            json.dump(day_data, f, indent=2)

        index_entries.append({
            "date": date,
            "day_of_week": meta.get("day_of_week", ""),
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "pending": pending,
            "a_wins": a_wins,
            "a_losses": a_losses,
            "total_picks": len(graded_plays),
        })

    # Remove stale history files not in current index
    indexed_dates = {e["date"] for e in index_entries}
    for old_file in history_dir.glob("history_*.json"):
        old_date = old_file.stem.replace("history_", "")
        if old_date not in indexed_dates:
            old_file.unlink()

    # Write index (most recent first)
    index_entries.sort(key=lambda x: x["date"], reverse=True)
    index_path = history_dir / "index.json"
    with open(index_path, "w") as f:
        json.dump({"dates": index_entries}, f, indent=2)

    print(f"[export] History: {len(index_entries)} days exported to {history_dir}")


def main():
    ap = argparse.ArgumentParser(description="Export pipeline data for web")
    ap.add_argument("--date", type=str, default=None,
                    help="Date to export (default: latest)")
    args = ap.parse_args()

    date = args.date or find_latest_plays_date()
    print(f"[export] Exporting data for {date}")

    export_picks(date)
    export_history()
    export_reports()
    export_tier_performance()
    cleanup_stale_reports()

    print(f"[export] Done. Web data ready in web/public/data/ and web/data/private/")


if __name__ == "__main__":
    main()
