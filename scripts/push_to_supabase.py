#!/usr/bin/env python3
"""
Post-pipeline Supabase push.

Called at the end of run_daily.sh to sync today's pipeline output to Supabase.
Non-blocking: exits 0 even on Supabase errors so the pipeline itself isn't disrupted.

Delta mode (default): only rows newer than the last successful push are sent.
Full mode (--full): push all rows regardless of state file. Always rewrites state file.

Usage:
  python3 scripts/push_to_supabase.py                # delta push
  python3 scripts/push_to_supabase.py --full         # push everything
  python3 scripts/push_to_supabase.py --date 2026-03-17
  python3 scripts/push_to_supabase.py --date 2026-03-17 --debug
"""
from __future__ import annotations

import argparse
import datetime
import json
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
STATE_FILE = REPO_ROOT / "data" / "ledger" / ".push_state.json"

sys.path.insert(0, str(REPO_ROOT))


# ──────────────────────────────────────────────
# State file helpers
# ──────────────────────────────────────────────

def _load_state() -> Optional[Dict[str, Any]]:
    """Return the push state dict, or None if the state file doesn't exist / is corrupt."""
    if not STATE_FILE.exists():
        return None
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return None


def _write_state(
    signals_total: int,
    grades_total: int,
    occurrences_total: int,
) -> None:
    """Write the push state file with the current UTC time and row counts."""
    now_utc = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    state = {
        "last_push_utc": now_utc,
        "signals_pushed": signals_total,
        "grades_pushed": grades_total,
        "occurrences_pushed": occurrences_total,
    }
    STATE_FILE.write_text(json.dumps(state, indent=2) + "\n")


# ──────────────────────────────────────────────
# Delta filter helpers
# ──────────────────────────────────────────────

def _filter_delta(
    rows: List[Dict],
    ts_field: str,
    last_push_utc: Optional[str],
    logger: logging.Logger,
    label: str,
) -> List[Dict]:
    """
    Return the subset of rows whose `ts_field` value is strictly greater than
    `last_push_utc`.  Rows missing the field are conservatively included.

    If last_push_utc is None (no prior state), all rows are returned.
    """
    total = len(rows)
    if last_push_utc is None:
        logger.info("%s: full push — %d rows (no prior state)", label, total)
        return rows

    delta: List[Dict] = []
    missing_ts = 0
    for row in rows:
        ts = row.get(ts_field)
        if ts is None:
            # Conservative: include rows with no timestamp
            delta.append(row)
            missing_ts += 1
        elif ts > last_push_utc:
            delta.append(row)

    logger.info(
        "Delta push: %d new %s (of %d total), last push was %s%s",
        len(delta),
        label,
        total,
        last_push_utc,
        f" ({missing_ts} rows had no {ts_field!r} — included conservatively)" if missing_ts else "",
    )
    return delta


# ──────────────────────────────────────────────
# File loading helper
# ──────────────────────────────────────────────

def _load_jsonl(path: str) -> List[Dict]:
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Push today's pipeline output to Supabase")
    parser.add_argument("--date", default=None, help="Date in YYYY-MM-DD format (default: today)")
    parser.add_argument("--full", action="store_true",
                        help="Ignore state file and push all rows (rewrites state file at end)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger("supabase.push")

    date_str = args.date or datetime.date.today().isoformat()

    signals_file     = str(REPO_ROOT / "data" / "ledger" / "signals_latest.jsonl")
    grades_file      = str(REPO_ROOT / "data" / "ledger" / "grades_latest.jsonl")
    plays_file       = str(REPO_ROOT / "data" / "plays" / f"plays_{date_str}.json")
    occurrences_file = str(REPO_ROOT / "data" / "analysis" / "graded_occurrences_latest.jsonl")

    # Determine delta cutoff
    state = _load_state()
    if args.full:
        last_push_utc = None
        logger.info("--full flag set: ignoring state file, pushing all rows")
    elif state is None:
        last_push_utc = None
        logger.info("No state file found at %s — pushing all rows", STATE_FILE)
    else:
        last_push_utc = state.get("last_push_utc")
        logger.info(
            "State file loaded: last_push_utc=%s, signals=%s, grades=%s, occurrences=%s",
            last_push_utc,
            state.get("signals_pushed"),
            state.get("grades_pushed"),
            state.get("occurrences_pushed"),
        )

    logger.info("Pushing pipeline output for %s to Supabase", date_str)
    logger.info("  signals:     %s", signals_file)
    logger.info("  grades:      %s", grades_file)
    logger.info("  plays:       %s", plays_file)
    logger.info("  occurrences: %s", occurrences_file)

    try:
        from src.supabase_writer import upsert_signals, upsert_grades, upsert_plays, upsert_occurrences

        signals_total = 0
        grades_total = 0
        occurrences_total = 0

        # ── Signals (delta by observed_at_utc) ────────────────────────────────
        if os.path.exists(signals_file):
            all_signals = _load_jsonl(signals_file)
            signals_total = len(all_signals)
            delta_signals = _filter_delta(
                all_signals, "observed_at_utc", last_push_utc, logger, "signals"
            )
            if delta_signals:
                n = upsert_signals(delta_signals)
                logger.info("Pushed %d signal rows (%d new records)", n, len(delta_signals))
            else:
                logger.info("No new signals to push (0 rows)")
        else:
            logger.debug("Signals file not found: %s", signals_file)

        # ── Grades (delta by graded_at_utc) ───────────────────────────────────
        if os.path.exists(grades_file):
            all_grades = _load_jsonl(grades_file)
            grades_total = len(all_grades)
            delta_grades = _filter_delta(
                all_grades, "graded_at_utc", last_push_utc, logger, "grades"
            )
            if delta_grades:
                n = upsert_grades(delta_grades)
                logger.info("Pushed %d grade rows (%d new records)", n, len(delta_grades))
            else:
                logger.info("No new grades to push (0 rows)")
        else:
            logger.debug("Grades file not found: %s", grades_file)

        # ── Plays (always push today's file — unchanged behaviour) ─────────────
        if os.path.exists(plays_file):
            with open(plays_file) as f:
                data = json.load(f)
            date_key = data.get("meta", {}).get("date") or Path(plays_file).stem.replace("plays_", "")
            n = upsert_plays(date_key, data.get("plays", []))
            logger.info("Pushed %d play rows from %s", n, plays_file)
        else:
            logger.debug("Plays file not found: %s", plays_file)

        # ── Occurrences (delta by graded_at_utc) ──────────────────────────────
        if os.path.exists(occurrences_file):
            all_occ = _load_jsonl(occurrences_file)
            occurrences_total = len(all_occ)
            delta_occ = _filter_delta(
                all_occ, "graded_at_utc", last_push_utc, logger, "occurrences"
            )
            if delta_occ:
                n = upsert_occurrences(delta_occ)
                logger.info("Pushed %d occurrence rows (%d new records)", n, len(delta_occ))
            else:
                logger.info("No new occurrences to push (0 rows)")
        else:
            logger.debug("Occurrences file not found: %s", occurrences_file)

        # ── Write state file ──────────────────────────────────────────────────
        _write_state(signals_total, grades_total, occurrences_total)
        logger.info(
            "State file updated: %s (signals=%d, grades=%d, occurrences=%d)",
            STATE_FILE,
            signals_total,
            grades_total,
            occurrences_total,
        )

        # ── Trusted NBA expert pattern tables ────────────────────────────────
        # Bounded subprocess so a stuck child doesn't extend the push
        # indefinitely. Failure is logged but non-fatal — the main push
        # above already committed signals/grades/plays/occurrences.
        try:
            logger.info("Refreshing trusted NBA expert_records / expert_pair_records in Supabase")
            subprocess.run(
                [sys.executable, str(REPO_ROOT / "scripts" / "build_pattern_analysis.py")],
                check=True,
                cwd=str(REPO_ROOT),
                timeout=300,  # 5 minutes; child should complete in seconds
            )
        except subprocess.TimeoutExpired as e:
            logger.warning("Trusted NBA pattern table refresh timed out after %ss", e.timeout)
        except Exception as e:
            logger.warning("Trusted NBA pattern table refresh failed: %s", e)

        logger.info("Supabase push complete for %s", date_str)

    except RuntimeError as e:
        # Missing credentials — warn but don't fail the pipeline
        logger.warning("Supabase push skipped: %s", e)
        sys.exit(0)
    except Exception as e:
        logger.error("Supabase push failed: %s", e, exc_info=args.debug)
        # Exit 0 so run_daily.sh continues even if Supabase is down
        sys.exit(0)


if __name__ == "__main__":
    main()
