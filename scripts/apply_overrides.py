#!/usr/bin/env python3
"""Apply manual overrides to the review slate and produce an approved slate.

Reads the review slate, optionally reads an override file, resolves
published_tier / published_included for every candidate, and writes the
approved slate consumed by the export pipeline.

Usage:
    python3 scripts/apply_overrides.py
    python3 scripts/apply_overrides.py --date 2026-04-20
    python3 scripts/apply_overrides.py --date 2026-03-01 --to 2026-03-31
    python3 scripts/apply_overrides.py --sport NCAAB
"""
from __future__ import annotations

import argparse
import copy
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from review_paths import (
    approved_path,
    ensure_review_dir,
    find_dates_with_slates,
    find_latest_slate_date,
    overrides_path,
    parse_date_range,
    slate_json_path,
    validate_date,
)

VALID_TIERS = {"A", "B", "C", "D"}
ALLOWED_OVERRIDE_FIELDS = {"pick_id", "published_tier", "published_included", "override_note"}


# ── Override validation ───────────────────────────────────────────────

def validate_overrides(overrides: List[Dict[str, Any]],
                       valid_pick_ids: Set[str]) -> List[str]:
    """Validate override entries. Returns list of error messages (empty = valid)."""
    errors: List[str] = []
    seen_ids: Set[str] = set()

    for i, entry in enumerate(overrides):
        prefix = f"override[{i}]"

        # Check for unknown fields
        unknown = set(entry.keys()) - ALLOWED_OVERRIDE_FIELDS
        if unknown:
            errors.append(f"{prefix}: unknown fields {unknown}")

        # pick_id required
        pick_id = entry.get("pick_id")
        if not pick_id:
            errors.append(f"{prefix}: missing pick_id")
            continue

        if not isinstance(pick_id, str):
            errors.append(f"{prefix}: pick_id must be a string")
            continue

        # pick_id must exist in slate
        if pick_id not in valid_pick_ids:
            errors.append(f"{prefix}: pick_id '{pick_id[:16]}...' not found in slate")

        # No duplicates
        if pick_id in seen_ids:
            errors.append(f"{prefix}: duplicate pick_id '{pick_id[:16]}...'")
        seen_ids.add(pick_id)

        # published_tier validation
        if "published_tier" in entry:
            tier = entry["published_tier"]
            if tier not in VALID_TIERS:
                errors.append(f"{prefix}: invalid published_tier '{tier}' "
                              f"(must be one of {VALID_TIERS})")

        # published_included validation
        if "published_included" in entry:
            included = entry["published_included"]
            if not isinstance(included, bool):
                errors.append(f"{prefix}: published_included must be boolean, "
                              f"got {type(included).__name__}")

        # override_note validation
        if "override_note" in entry:
            note = entry["override_note"]
            if not isinstance(note, str):
                errors.append(f"{prefix}: override_note must be a string")

    return errors


def load_and_validate_overrides(path: Path,
                                valid_pick_ids: Set[str]) -> List[Dict[str, Any]]:
    """Load and validate an override file. Exits on error."""
    with open(path) as f:
        data = json.load(f)

    if not isinstance(data, dict) or "overrides" not in data:
        print(f"[overrides] ERROR: override file must have an 'overrides' key")
        sys.exit(1)

    overrides = data["overrides"]
    if not isinstance(overrides, list):
        print(f"[overrides] ERROR: 'overrides' must be an array")
        sys.exit(1)

    errors = validate_overrides(overrides, valid_pick_ids)
    if errors:
        print(f"[overrides] ERROR: {len(errors)} validation error(s):")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)

    return overrides


# ── Published value resolution ────────────────────────────────────────

def resolve_published_values(candidate: Dict[str, Any],
                             override: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Apply override to candidate, or fall back to auto values.

    Returns a new candidate dict with published_tier and published_included
    resolved (never null).
    """
    c = copy.deepcopy(candidate)

    if override:
        # Apply override values
        if "published_tier" in override:
            c["published_tier"] = override["published_tier"]
        else:
            c["published_tier"] = c["auto_tier"]

        if "published_included" in override:
            c["published_included"] = override["published_included"]
        elif "published_tier" in override:
            # If tier was overridden but included wasn't, derive from new tier
            c["published_included"] = override["published_tier"] in ("A", "B", "C")
        else:
            c["published_included"] = c["auto_included"]

        if "override_note" in override:
            c["override_note"] = override["override_note"]
    else:
        # Fall back to auto values
        c["published_tier"] = c["auto_tier"]
        c["published_included"] = c["auto_included"]

    return c


# ── Approved slate builder ────────────────────────────────────────────

def build_approved_slate(date: str, sport: str = "NBA") -> Dict[str, Any]:
    """Build approved slate from slate + optional overrides."""
    sp = slate_json_path(sport, date)
    op = overrides_path(sport, date)

    if not sp.exists():
        print(f"[overrides] Missing slate file: {sp}")
        sys.exit(1)

    with open(sp) as f:
        slate = json.load(f)

    candidates = slate["candidates"]
    valid_pick_ids = {c["pick_id"] for c in candidates}

    # Load overrides if they exist
    override_map: Dict[str, Dict[str, Any]] = {}
    override_count = 0
    override_file_used = None

    if op.exists():
        overrides = load_and_validate_overrides(op, valid_pick_ids)
        override_map = {o["pick_id"]: o for o in overrides}
        override_count = len(overrides)
        override_file_used = str(op)
        print(f"[overrides] Loaded {override_count} override(s) from {op}")
    else:
        print(f"[overrides] No override file found — using auto values for all picks")

    # Resolve published values for every candidate
    resolved = []
    overrides_applied = 0
    for c in candidates:
        override = override_map.get(c["pick_id"])
        if override:
            overrides_applied += 1
        resolved_candidate = resolve_published_values(c, override)
        resolved.append(resolved_candidate)

    published_included_count = sum(1 for c in resolved if c["published_included"])
    published_excluded_count = len(resolved) - published_included_count

    approved = {
        "meta": {
            "date": date,
            "sport": sport,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_slate_file": str(sp),
            "override_file": override_file_used,
            "override_count": override_count,
            "total_candidates": len(resolved),
            "published_included_count": published_included_count,
            "published_excluded_count": published_excluded_count,
            "overrides_applied": overrides_applied,
            "auto_fallback_count": len(resolved) - overrides_applied,
        },
        "candidates": resolved,
    }

    return approved


def write_approved_slate(approved: Dict[str, Any], sport: str) -> Path:
    """Write approved slate to JSON. Returns path."""
    date = approved["meta"]["date"]
    ensure_review_dir(sport, date)
    path = approved_path(sport, date)
    with open(path, "w") as f:
        json.dump(approved, f, indent=2, ensure_ascii=False)
    return path


def _process_single_date(date: str, sport: str) -> bool:
    """Apply overrides for one date. Returns True on success."""
    sp = slate_json_path(sport, date)
    if not sp.exists():
        print(f"[overrides] Skipping {date} — no slate file")
        return False

    approved = build_approved_slate(date, sport)
    write_approved_slate(approved, sport)

    meta = approved["meta"]
    print(f"[overrides] {date}: {meta['published_included_count']} included, "
          f"{meta['published_excluded_count']} excluded "
          f"({meta['overrides_applied']} overrides)")
    return True


# ── Main ──────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Apply overrides and produce approved slate")
    ap.add_argument("--date", type=str, default=None,
                    help="Date (default: latest slate)")
    ap.add_argument("--to", type=str, default=None, dest="end_date",
                    help="End date for range (requires --date as start)")
    ap.add_argument("--sport", type=str, default="NBA",
                    choices=["NBA", "NCAAB"],
                    help="Sport (default: NBA)")
    args = ap.parse_args()

    if args.end_date and not args.date:
        print("[overrides] ERROR: --to requires --date as start date")
        sys.exit(1)

    if args.end_date:
        # Date range mode
        validate_date(args.date)
        validate_date(args.end_date)
        dates = parse_date_range(args.date, args.end_date)
        print(f"[overrides] Processing {len(dates)} dates "
              f"({args.date} to {args.end_date}, {args.sport})")
        success = 0
        for d in dates:
            if _process_single_date(d, args.sport):
                success += 1
        print(f"[overrides] Done: {success}/{len(dates)} approved slates generated")
    else:
        # Single date mode
        date = args.date
        if date:
            validate_date(date)
        else:
            date = find_latest_slate_date(args.sport)
        if not date:
            print("[overrides] No slate files found")
            sys.exit(1)

        print(f"[overrides] Building approved slate for {date} ({args.sport})")

        approved = build_approved_slate(date, args.sport)
        path = write_approved_slate(approved, args.sport)

        meta = approved["meta"]
        print(f"[overrides] Approved: {meta['published_included_count']} included, "
              f"{meta['published_excluded_count']} excluded "
              f"({meta['overrides_applied']} overrides applied)")
        print(f"[overrides] Output: {path}")


if __name__ == "__main__":
    main()
