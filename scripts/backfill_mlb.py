#!/usr/bin/env python3
"""MLB backfill — run the MLB pipeline for a date range.

Source backfill capabilities:
  - BettingPros props: --date per-day (API supports arbitrary dates)
  - Action Network:    scoreboard API backfill via action_backfill_nba.py
                       (fetches all games for a date range, appends to backfill JSONL)
  - SportsLine:        expert-page scraper via backfill_sportsline_experts_v2.py
                       (clicks "Load More" on each expert page, captures all new picks)
  - Covers:            live-only (today's page), no historical API
  - OddsTrader:        live-only (today's page), no historical API

Usage:
    # Today only — all live sources + consensus + ledger
    python3 scripts/backfill_mlb.py --date 2026-04-27 --debug

    # Date range — BettingPros per-day + Action bulk backfill + consensus + ledger
    python3 scripts/backfill_mlb.py --date 2026-04-10 --to 2026-04-27 --debug

    # Include SportsLine expert-page backfill (slow, clicks "Load More" per expert)
    python3 scripts/backfill_mlb.py --date 2026-04-10 --to 2026-04-27 --with-sportsline-backfill --debug

    # Dry run
    python3 scripts/backfill_mlb.py --date 2026-04-10 --to 2026-04-27 --dry-run --debug

    # Validate only
    python3 scripts/backfill_mlb.py --date 2026-04-27 --validate-only
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
os.chdir(REPO_ROOT)
sys.path.insert(0, str(REPO_ROOT))

from store import MLB_SPORT

# Canonical team codes from data_mlb.py
CANONICAL_MLB_CODES = {
    "ARI", "ATL", "BAL", "BOS", "CHC", "CHW", "CIN", "CLE", "COL", "DET",
    "HOU", "KCR", "LAA", "LAD", "MIA", "MIL", "MIN", "NYM", "NYY", "OAK",
    "PHI", "PIT", "SDP", "SEA", "SFG", "STL", "TBR", "TEX", "TOR", "WSN",
}

OUT = REPO_ROOT / "out"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_date(s: str) -> date:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Bad date format: {s!r} (expected YYYY-MM-DD)")


def date_range(start: date, end: date) -> List[date]:
    if end < start:
        raise ValueError(f"--to ({end}) is before --date ({start})")
    days = []
    cur = start
    while cur <= end:
        days.append(cur)
        cur += timedelta(days=1)
    return days


def run_cmd(args: List[str], label: str, dry_run: bool = False) -> bool:
    cmd_str = " ".join(args)
    if dry_run:
        print(f"  [DRY-RUN] {cmd_str}")
        return True
    print(f"  $ {cmd_str}")
    result = subprocess.run(args, cwd=str(REPO_ROOT))
    if result.returncode != 0:
        print(f"  ⚠ {label} failed (exit {result.returncode})")
        return False
    return True


def today_et() -> date:
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("America/New_York")).date()


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_normalized(path: Path) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "file": str(path.name),
        "exists": path.exists(),
        "total": 0,
        "eligible": 0,
        "nba_prefix_count": 0,
        "non_canonical_teams": [],
        "missing_matchup_key": 0,
        "errors": [],
    }
    if not path.exists():
        return result

    try:
        data = json.loads(path.read_text())
    except Exception as e:
        result["errors"].append(f"JSON parse error: {e}")
        return result

    if not isinstance(data, list):
        result["errors"].append("Expected list, got " + type(data).__name__)
        return result

    result["total"] = len(data)
    teams_seen: set = set()

    for rec in data:
        elig = rec.get("eligible_for_consensus") or (rec.get("eligibility") or {}).get("eligible_for_consensus")
        if elig:
            result["eligible"] += 1
        ev = rec.get("event") or {}
        mk = rec.get("market") or {}
        for field in ["player_key", "selection"]:
            val = mk.get(field, "")
            if isinstance(val, str) and val.startswith("NBA:"):
                result["nba_prefix_count"] += 1
        for t in [ev.get("away_team"), ev.get("home_team")]:
            if t and isinstance(t, str):
                teams_seen.add(t)
        if ev.get("away_team") and ev.get("home_team") and not ev.get("matchup_key"):
            result["missing_matchup_key"] += 1

    bad_teams = teams_seen - CANONICAL_MLB_CODES
    if bad_teams:
        result["non_canonical_teams"] = sorted(bad_teams)

    return result


def validate_backfill_jsonl(path: Path, label: str) -> Dict[str, Any]:
    """Validate a JSONL backfill file (Action/SportsLine history)."""
    result: Dict[str, Any] = {
        "file": str(path.name),
        "label": label,
        "exists": path.exists(),
        "total": 0,
        "nba_prefix_count": 0,
        "non_canonical_teams": [],
        "day_range": None,
        "errors": [],
    }
    if not path.exists():
        return result

    teams_seen: set = set()
    day_keys: set = set()
    try:
        with path.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                result["total"] += 1
                rec = json.loads(line)
                ev = rec.get("event") or {}
                mk = rec.get("market") or {}
                for field in ["player_key", "selection"]:
                    val = mk.get(field, "")
                    if isinstance(val, str) and val.startswith("NBA:"):
                        result["nba_prefix_count"] += 1
                for t in [ev.get("away_team"), ev.get("home_team")]:
                    if t and isinstance(t, str):
                        teams_seen.add(t)
                dk = ev.get("day_key") or ""
                if dk:
                    day_keys.add(dk)
    except Exception as e:
        result["errors"].append(f"Read error: {e}")

    bad_teams = teams_seen - CANONICAL_MLB_CODES
    if bad_teams:
        result["non_canonical_teams"] = sorted(bad_teams)
    if day_keys:
        result["day_range"] = f"{min(day_keys)} .. {max(day_keys)}"

    return result


def validate_run_dir(run_dir: Path) -> Dict[str, Any]:
    result: Dict[str, Any] = {"dir": str(run_dir.name), "errors": []}
    if not run_dir.exists():
        result["errors"].append("Run dir does not exist")
        return result

    meta_path = run_dir / "run_meta.json"
    signals_path = run_dir / "signals.jsonl"

    if not meta_path.exists():
        result["errors"].append("Missing run_meta.json")
    else:
        meta = json.loads(meta_path.read_text())
        result["run_id"] = meta.get("run_id")
        result["sources_present"] = meta.get("sources_present", [])
        counts = meta.get("counts", {})
        result["eligible_records"] = counts.get("eligible_records", 0)
        result["total_unified_signals"] = counts.get("total_unified_signals", 0)

    if not signals_path.exists():
        result["errors"].append("Missing signals.jsonl")
    else:
        signals = [json.loads(l) for l in signals_path.read_text().strip().split("\n") if l.strip()]
        result["signal_count"] = len(signals)
        nba_count = 0
        non_canonical = set()
        for s in signals:
            for f in ["selection", "player_id"]:
                val = s.get(f, "")
                if isinstance(val, str) and val.startswith("NBA:"):
                    nba_count += 1
            for t in [s.get("away_team"), s.get("home_team")]:
                if t and isinstance(t, str) and t not in CANONICAL_MLB_CODES and len(t) <= 4 and t.isalpha():
                    non_canonical.add(t)
        result["nba_prefix_count"] = nba_count
        result["non_canonical_teams"] = sorted(non_canonical)
        type_counts = Counter(s.get("signal_type", "?") for s in signals)
        result["signal_types"] = dict(type_counts)

    if "runs_mlb" not in str(run_dir):
        result["errors"].append(f"Run dir not in data/runs_mlb/: {run_dir}")

    return result


def print_validation_report(
    norm_results: List[Dict],
    backfill_results: Optional[List[Dict]] = None,
    run_result: Optional[Dict] = None,
):
    print("\n╔══════════════════════════════════════════╗")
    print("║       MLB VALIDATION REPORT              ║")
    print("╚══════════════════════════════════════════╝")

    all_clean = True

    print("\n── Normalized Files ──")
    for r in norm_results:
        status = "OK" if not r["errors"] and r["nba_prefix_count"] == 0 and not r["non_canonical_teams"] else "FAIL"
        if status == "FAIL":
            all_clean = False
        if r["exists"]:
            print(f"  [{status}] {r['file']}: {r['total']} total, {r['eligible']} eligible")
            if r["nba_prefix_count"]:
                print(f"       ✗ NBA-prefixed keys: {r['nba_prefix_count']}")
            if r["non_canonical_teams"]:
                print(f"       ✗ Non-canonical teams: {r['non_canonical_teams']}")
            if r["missing_matchup_key"]:
                print(f"       ⚠ Missing matchup_key (consensus derives): {r['missing_matchup_key']}")
            for e in r["errors"]:
                print(f"       ✗ {e}")
        else:
            print(f"  [SKIP] {r['file']}: not present")

    if backfill_results:
        print("\n── Backfill History Files ──")
        for r in backfill_results:
            status = "OK" if not r["errors"] and r["nba_prefix_count"] == 0 and not r["non_canonical_teams"] else "FAIL"
            if status == "FAIL":
                all_clean = False
            if r["exists"]:
                print(f"  [{status}] {r['label']}: {r['total']} rows")
                if r["day_range"]:
                    print(f"       Date range: {r['day_range']}")
                if r["nba_prefix_count"]:
                    print(f"       ✗ NBA-prefixed keys: {r['nba_prefix_count']}")
                if r["non_canonical_teams"]:
                    print(f"       ✗ Non-canonical teams: {r['non_canonical_teams']}")
            else:
                print(f"  [SKIP] {r['label']}: not present")

    if run_result:
        print("\n── Run Directory ──")
        status = "OK" if not run_result["errors"] and run_result.get("nba_prefix_count", 0) == 0 else "FAIL"
        if status == "FAIL":
            all_clean = False
        print(f"  [{status}] {run_result['dir']}")
        if run_result.get("sources_present"):
            print(f"       Sources: {run_result['sources_present']}")
        if run_result.get("signal_types"):
            print(f"       Signals: {run_result['signal_types']}")
        if run_result.get("nba_prefix_count"):
            print(f"       ✗ NBA-prefixed in signals: {run_result['nba_prefix_count']}")
        if run_result.get("non_canonical_teams"):
            print(f"       ✗ Non-canonical teams: {run_result['non_canonical_teams']}")
        for e in run_result["errors"]:
            print(f"       ✗ {e}")

    print(f"\n  {'✓ ALL CLEAN' if all_clean else '✗ ISSUES FOUND'}")
    return all_clean


NORM_FILES = [
    "normalized_action_mlb.json",
    "normalized_covers_mlb.json",
    "normalized_sportsline_mlb.json",
    "normalized_oddstrader_mlb.json",
    "normalized_bettingpros_prop_bets_mlb.json",
    "normalized_dimers_mlb.json",
    "normalized_vegasinsider_mlb.json",
]

BACKFILL_FILES = [
    (OUT / "normalized_action_mlb_backfill.jsonl", "Action backfill"),
    (OUT / "normalized_sportsline_expert_pages_mlb_canonical.jsonl", "SportsLine canonical"),
]


def run_validation(debug: bool) -> bool:
    norm_results = [validate_normalized(OUT / f) for f in NORM_FILES]
    backfill_results = [validate_backfill_jsonl(p, label) for p, label in BACKFILL_FILES]

    runs_dir = REPO_ROOT / "data" / "runs_mlb"
    run_result = None
    if runs_dir.exists():
        run_dirs = sorted([d for d in runs_dir.iterdir() if d.is_dir()])
        if run_dirs:
            run_result = validate_run_dir(run_dirs[-1])

    return print_validation_report(norm_results, backfill_results, run_result)


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------

def run_live_ingest(debug: bool, dry_run: bool, include_deferred: bool = False):
    """Run live scrapers (today only)."""
    ok = True
    ok &= run_cmd(["python3", "action_ingest.py", "--sport", "MLB"] + (["--debug"] if debug else []),
                   "Action (live)", dry_run)
    ok &= run_cmd(["python3", "covers_ingest.py", "--sport", "MLB"] + (["--debug"] if debug else []),
                   "Covers (live)", dry_run)
    ok &= run_cmd(["python3", "oddstrader_ingest.py", "--sport", "MLB"] + (["--debug"] if debug else []),
                   "OddsTrader (live)", dry_run)
    run_cmd(["python3", "sportsline_ingest.py", "--sport", "MLB",
             "--storage", "data/sportsline_storage_state.json"] + (["--debug"] if debug else []),
            "SportsLine (live)", dry_run)

    if include_deferred:
        run_cmd(["python3", "dimers_ingest.py", "--sport", "MLB", "--bets-only",
                 "--storage-state", "dimers_storage_state.json"] + (["--debug"] if debug else []),
                "Dimers", dry_run)
        run_cmd(["python3", "vegasinsider_ingest.py", "--sport", "MLB",
                 "--storage", "data/vegasinsider_storage_state.json"] + (["--debug"] if debug else []),
                "VegasInsider", dry_run)
    return ok


def run_bettingpros_ingest(target_date: str, debug: bool, dry_run: bool):
    args = [
        "python3", "scripts/bettingpros_prop_bets_ingest_mlb.py",
        "--storage", "data/bettingpros_storage_state.json",
        "--date", target_date,
    ]
    if debug:
        args.append("--debug")
    return run_cmd(args, f"BettingPros (date={target_date})", dry_run)


def run_action_backfill(start_date: date, stop_date: date, debug: bool, dry_run: bool):
    """Run Action Network scoreboard-API backfill for a date range.

    action_backfill_nba.py walks dates backward from --start-date to --stop-date.
    It appends to out/raw_action_mlb_backfill.jsonl and
    out/normalized_action_mlb_backfill.jsonl.  State file prevents re-visiting
    dates/games already processed.
    """
    num_days = (start_date - stop_date).days + 1
    args = [
        "python3", "scripts/action_backfill_nba.py",
        "--sport", "MLB",
        "--start-date", start_date.strftime("%Y-%m-%d"),
        "--stop-date", stop_date.strftime("%Y-%m-%d"),
        "--max-days", str(num_days),
    ]
    if debug:
        args.append("--debug")
    return run_cmd(args, f"Action backfill ({stop_date} → {start_date})", dry_run)


def run_sportsline_backfill(debug: bool, dry_run: bool, max_clicks: int = 5000):
    """Run SportsLine expert-page backfill.

    Scrapes all expert profile pages, clicking "Load More" to capture recent
    picks.  Uses --resume to skip already-scraped picks.  Output goes to
    per-expert JSONL files under out/raw_sportsline_expert_pages_v2/, then
    combined into out/raw_sportsline_expert_pages_all_combined_v2_deduped.jsonl.

    After scraping, runs build + normalize to update the canonical MLB file.
    """
    # Step 1: Scrape expert pages (incremental via --resume)
    args = [
        "python3", "scripts/backfill_sportsline_experts_v2.py",
        "--storage", "data/sportsline_storage_state.json",
        "--max-clicks", str(max_clicks),
        "--resume",
    ]
    if debug:
        args.append("--debug")
    ok = run_cmd(args, "SportsLine expert backfill", dry_run)

    # Step 2: Rebuild canonical history (merge v2 with old data, dedup)
    ok &= run_cmd(
        ["python3", "scripts/build_sportsline_canonical_history.py"],
        "SportsLine build canonical", dry_run,
    )

    # Step 3: Re-normalize (produces per-sport JSONL including MLB)
    ok &= run_cmd(
        ["python3", "scripts/normalize_sportsline_canonical_history.py"],
        "SportsLine normalize canonical", dry_run,
    )

    return ok


def run_consensus(debug: bool, dry_run: bool):
    args = ["python3", "consensus_nba.py", "--sport", "MLB"]
    if debug:
        args.append("--debug")
    return run_cmd(args, "Consensus", dry_run)


def run_ledger(debug: bool, dry_run: bool, include_action_history: bool = False,
               include_sportsline_history: bool = True):
    args = ["python3", "scripts/build_signal_ledger.py", "--sport", "MLB"]
    if debug:
        args.append("--debug")
    if include_action_history:
        args.append("--include-action-history")
    if include_sportsline_history:
        sl_path = "out/normalized_sportsline_expert_pages_mlb_canonical.jsonl"
        if Path(sl_path).exists():
            args.extend(["--include-normalized-jsonl", sl_path])
    return run_cmd(args, "Ledger", dry_run)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="MLB backfill — controlled date-range pipeline runs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--date", type=parse_date, required=True,
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to", type=parse_date, default=None,
                        help="End date (YYYY-MM-DD). If omitted, runs only --date.")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show commands without executing")
    parser.add_argument("--bettingpros-only", action="store_true",
                        help="Only run BettingPros props (skip all other sources)")
    parser.add_argument("--skip-action-backfill", action="store_true",
                        help="Skip Action Network scoreboard-API backfill for past dates")
    parser.add_argument("--with-sportsline-backfill", action="store_true",
                        help="Run SportsLine expert-page backfill (slow, requires auth)")
    parser.add_argument("--sportsline-max-clicks", type=int, default=5000,
                        help="Max 'Load More' clicks per expert for SportsLine backfill (default: 5000)")
    parser.add_argument("--include-deferred", action="store_true",
                        help="Include dimers and vegasinsider (broken/deferred)")
    parser.add_argument("--include-action-history", action="store_true",
                        help="Include Action Network backfill history in ledger build")
    parser.add_argument("--skip-ledger", action="store_true",
                        help="Skip the ledger build step")
    parser.add_argument("--validate-only", action="store_true",
                        help="Only run validation on existing out/ files")
    args = parser.parse_args()

    end = args.to or args.date
    days = date_range(args.date, end)
    today = today_et()

    if args.validate_only:
        ok = run_validation(args.debug)
        sys.exit(0 if ok else 1)

    has_past_dates = any(d < today for d in days)
    has_today = today in days

    print("=========================================")
    print(f"  MLB Backfill — {args.date} to {end}")
    print(f"  Days: {len(days)}  |  Today(ET): {today}")
    print(f"  Mode: {'DRY-RUN' if args.dry_run else 'LIVE'}")
    if args.bettingpros_only:
        print(f"  Sources: BettingPros only")
    else:
        sources = ["BettingPros (per-day)"]
        if has_past_dates and not args.skip_action_backfill:
            sources.append("Action (scoreboard backfill)")
        if has_today:
            sources.extend(["Action (live)", "Covers (live)", "OddsTrader (live)", "SportsLine (live)"])
        if args.with_sportsline_backfill:
            sources.append("SportsLine (expert backfill)")
        print(f"  Sources: {', '.join(sources)}")
    print("=========================================")

    # ── Phase 1: Action Network backfill for past dates (bulk, one call) ──
    if has_past_dates and not args.bettingpros_only and not args.skip_action_backfill:
        past_dates = sorted(d for d in days if d < today)
        start = max(past_dates)  # action_backfill walks backward from start
        stop = min(past_dates)
        print(f"\n{'='*50}")
        print(f"  ACTION BACKFILL — {stop} to {start}")
        print(f"  (scoreboard API, {len(past_dates)} days, state-aware)")
        print(f"{'='*50}")
        run_action_backfill(start, stop, args.debug, args.dry_run)

    # ── Phase 2: SportsLine expert-page backfill (bulk, one call) ──
    if args.with_sportsline_backfill and not args.bettingpros_only:
        print(f"\n{'='*50}")
        print(f"  SPORTSLINE EXPERT BACKFILL")
        print(f"  (expert pages, --resume, max-clicks={args.sportsline_max_clicks})")
        print(f"{'='*50}")
        run_sportsline_backfill(args.debug, args.dry_run, args.sportsline_max_clicks)

    # ── Phase 3: Per-day BettingPros + live sources for today ──
    for d in days:
        ds = d.strftime("%Y-%m-%d")
        is_today = (d == today)

        print(f"\n{'='*50}")
        if is_today:
            print(f"  DATE: {ds} (today — live sources + BettingPros)")
        else:
            print(f"  DATE: {ds} (BettingPros per-day)")
        print(f"{'='*50}")

        # BettingPros: always runs per-day (supports --date for any date)
        print(f"\n── BettingPros props ({ds}) ──")
        run_bettingpros_ingest(ds, args.debug, args.dry_run)

        # Live sources: only for today
        if is_today and not args.bettingpros_only:
            print(f"\n── Live sources ({ds}) ──")
            run_live_ingest(args.debug, args.dry_run, args.include_deferred)

        # Consensus after each date
        print(f"\n── Consensus ({ds}) ──")
        run_consensus(args.debug, args.dry_run)

        # Per-date validation
        if not args.dry_run:
            print(f"\n── Validation ({ds}) ──")
            run_validation(args.debug)

    # ── Phase 4: Ledger build (once at end) ──
    if not args.skip_ledger:
        # Auto-enable --include-action-history when Action backfill was run
        use_action_history = args.include_action_history or (
            has_past_dates and not args.bettingpros_only and not args.skip_action_backfill
        )
        print(f"\n{'='*50}")
        print(f"  LEDGER BUILD")
        print(f"  action-history={'yes' if use_action_history else 'no'}"
              f"  sportsline-history={'yes' if (OUT / 'normalized_sportsline_expert_pages_mlb_canonical.jsonl').exists() else 'no'}")
        print(f"{'='*50}")
        run_ledger(args.debug, args.dry_run, use_action_history)
    else:
        print("\n  [SKIP] Ledger build (--skip-ledger)")

    # ── Final validation ──
    if not args.dry_run:
        print(f"\n{'='*50}")
        print(f"  FINAL VALIDATION")
        print(f"{'='*50}")
        ok = run_validation(args.debug)
        if not ok:
            print("\n  ⚠ Validation issues found — review before continuing")
            sys.exit(1)

    print("\n  MLB backfill complete.")


if __name__ == "__main__":
    main()
