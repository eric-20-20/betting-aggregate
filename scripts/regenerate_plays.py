#!/usr/bin/env python3
"""
Regenerate corrupt plays files from the current clean ledger.

The 108 plays files generated on 2026-03-04 (JuiceReel backfill run) contain
inflated source counts due to a cross-source merge bug that merged OVER and UNDER
sources for the same game. This script re-runs score_signals.py for each of those
dates using the current clean ledger.

Usage:
    python3 scripts/regenerate_plays.py [--dry-run] [--sport NBA] [--verbose]
    python3 scripts/regenerate_plays.py --date 2025-10-27   # single date
    python3 scripts/regenerate_plays.py --since 2026-01-01  # only from this date forward

After running backfills (juicereel, sportsline, dimers) and rebuilding the ledger,
run this to get clean plays files before re-exporting web data.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PLAYS_DIR = ROOT / "data" / "plays"


def get_corrupt_dates(sport: str = "NBA") -> list[str]:
    """Return sorted list of plays dates generated on 2026-03-04 (the corrupt backfill run)."""
    if sport == "NCAAB":
        plays_dir = ROOT / "data" / "plays" / "ncaab"
    else:
        plays_dir = PLAYS_DIR

    corrupt = []
    if not plays_dir.exists():
        return corrupt

    for pf in sorted(plays_dir.glob("plays_*.json")):
        try:
            with open(pf) as f:
                data = json.load(f)
            gen = data.get("meta", {}).get("generated_at_utc", "")[:10]
            if gen == "2026-03-04":
                d = pf.name.replace("plays_", "").replace(".json", "")
                corrupt.append(d)
        except Exception:
            pass
    return sorted(corrupt)


def regenerate_date(date_str: str, sport: str = "NBA", verbose: bool = False) -> bool:
    """Run score_signals.py for a single date. Returns True on success."""
    cmd = [sys.executable, str(ROOT / "scripts" / "score_signals.py"), "--date", date_str]
    if sport != "NBA":
        cmd += ["--sport", sport]

    result = subprocess.run(
        cmd,
        cwd=str(ROOT),
        capture_output=not verbose,
        text=True,
    )

    if result.returncode != 0:
        if not verbose:
            # Show last few lines of stderr on failure
            lines = (result.stderr or result.stdout or "").strip().split("\n")
            for line in lines[-3:]:
                print(f"    {line}")
        return False
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Regenerate corrupt plays files")
    parser.add_argument("--dry-run", action="store_true",
                        help="List dates that would be regenerated without running")
    parser.add_argument("--sport", default="NBA", choices=["NBA", "NCAAB"],
                        help="Sport (default: NBA)")
    parser.add_argument("--date", type=str, default=None,
                        help="Regenerate a single date only")
    parser.add_argument("--since", type=str, default=None,
                        help="Only regenerate dates >= this date (YYYY-MM-DD)")
    parser.add_argument("--verbose", action="store_true",
                        help="Show full score_signals output per date")
    args = parser.parse_args()

    if args.date:
        dates = [args.date]
    else:
        dates = get_corrupt_dates(args.sport)

    if args.since:
        dates = [d for d in dates if d >= args.since]

    if not dates:
        print("No corrupt plays dates found — nothing to do.")
        return

    print(f"Plays regeneration — {len(dates)} dates to process ({args.sport})")
    if args.since:
        print(f"  Filtered to: >= {args.since}")
    print(f"  First: {dates[0]}   Last: {dates[-1]}")
    print()

    if args.dry_run:
        print("DRY RUN — would regenerate:")
        for d in dates:
            print(f"  {d}")
        return

    ok = 0
    fail = 0
    empty = 0

    for i, d in enumerate(dates, 1):
        print(f"[{i:>3}/{len(dates)}] {d} ...", end=" ", flush=True)
        success = regenerate_date(d, sport=args.sport, verbose=args.verbose)
        if success:
            # Check output
            if args.sport == "NCAAB":
                out_path = ROOT / "data" / "plays" / "ncaab" / f"plays_{d}.json"
            else:
                out_path = PLAYS_DIR / f"plays_{d}.json"

            if out_path.exists():
                with open(out_path) as f:
                    data = json.load(f)
                n = data.get("meta", {}).get("exported_count", 0)
                gen = data.get("meta", {}).get("generated_at_utc", "")[:10]
                if n == 0:
                    print(f"ok (0 plays — no scorable signals)")
                    empty += 1
                else:
                    print(f"ok ({n} plays, gen={gen})")
                ok += 1
            else:
                print("ok (no output file written)")
                ok += 1
        else:
            print("FAILED")
            fail += 1

    print()
    print(f"Done: {ok} succeeded ({empty} with 0 plays), {fail} failed")
    if fail > 0:
        print(f"  Re-run with --verbose to see error details for failed dates")
        sys.exit(1)


if __name__ == "__main__":
    main()
