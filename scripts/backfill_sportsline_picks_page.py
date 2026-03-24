#!/usr/bin/env python3
"""
SportsLine NBA picks backfill via the /nba/picks/experts/ page.

The daily ingest (sportsline_ingest.py) only extracts TODAY's picks and
skips past (graded) picks. This script loads the same page but:
  - Clicks "Load More Picks" hundreds of times to reach historical dates
  - Includes already-graded (past) picks
  - Stops when it reaches --stop-date or no more Load More button

Output:
    out/raw_sportsline_nba_backfill.jsonl  (appends per session)
    out/sportsline_backfill_picks_state.json  (resume support)

After running:
    python3 scripts/normalize_sportsline_expert_picks.py  (if needed)
    OR use the output directly with build_signal_ledger.py via
    --include-normalized-jsonl out/normalized_sportsline_nba_backfill.jsonl

Usage:
    python3 scripts/backfill_sportsline_picks_page.py
    python3 scripts/backfill_sportsline_picks_page.py --stop-date 2025-10-22
    python3 scripts/backfill_sportsline_picks_page.py --max-clicks 200
    python3 scripts/backfill_sportsline_picks_page.py --resume   # continue from checkpoint
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from dataclasses import asdict
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    from playwright.sync_api import sync_playwright
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
except ImportError:
    print("ERROR: playwright not installed. Run: pip install playwright && playwright install chromium")
    sys.exit(1)

from sportsline_ingest import (
    RawPickRecord,
    EXTRACT_EXPERT_PICKS_JS,
    STAT_TYPE_MAP,
    _parse_matchup_text,
    _parse_pick_text,
    _parse_expert_text,
    _map_team,
    sha256_digest,
    write_records,
)

PICKS_URL = "https://www.sportsline.com/nba/picks/experts/?sc=p"
DEFAULT_STORAGE = "data/sportsline_storage_state.json"
DEFAULT_OUTPUT = "out/raw_sportsline_nba_backfill.jsonl"
DEFAULT_STATE = "out/sportsline_backfill_picks_state.json"
DEFAULT_STOP_DATE = "2025-10-22"
DEFAULT_MAX_CLICKS = 1000


def load_state(path: str) -> Dict[str, Any]:
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            pass
    return {"seen_fingerprints": [], "clicks_done": 0, "earliest_date_seen": None}


def save_state(path: str, state: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(state, f, indent=2)


def append_records(path: str, records: List[Dict]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def extract_all_cards(page) -> List[Dict]:
    """Extract all pick cards from current page state using existing JS extractor."""
    try:
        return page.evaluate(EXTRACT_EXPERT_PICKS_JS) or []
    except Exception as e:
        print(f"  [warn] JS extraction error: {e}")
        return []


def parse_card_to_record(
    card: Dict,
    observed_at: datetime,
    seen_fps: Set[str],
) -> Optional[Dict]:
    """Parse a pick card dict into a normalized record. Returns None if unparseable."""
    matchup_text = card.get("matchupText", "")
    pick_text = card.get("pickText", "")
    expert_text = card.get("expertText", "")

    if not pick_text:
        return None

    matchup = _parse_matchup_text(matchup_text)
    away_raw = matchup.get("away_raw")
    home_raw = matchup.get("home_raw")
    away_code = _map_team(away_raw) if away_raw else None
    home_code = _map_team(home_raw) if home_raw else None
    event_time = matchup.get("event_time_utc")

    pick = _parse_pick_text(pick_text)
    market_type = pick.get("market_type")
    if not market_type:
        return None

    expert = _parse_expert_text(expert_text)
    expert_name = expert.get("expert_name")
    expert_slug = expert.get("expert_slug")

    line_val = pick.get("line")
    odds_val = pick.get("odds")
    side = pick.get("side")
    stat_key = pick.get("stat_key")
    player_name = pick.get("player_name")
    team_raw = pick.get("team_raw")

    # For spreads/moneylines, map team_raw to team code to use as side/selection
    if market_type in ("spread", "moneyline") and not side and team_raw:
        side = _map_team(team_raw) or team_raw

    # Build selection
    selection = None
    if market_type == "player_prop" and player_name and stat_key and side:
        selection = f"NBA:{player_name}::{stat_key}::{side.upper()}"
    elif market_type == "spread" and side:
        selection = side.upper()
    elif market_type == "total" and side:
        selection = side.upper()
    elif market_type == "moneyline" and side:
        selection = side.upper()

    matchup_hint = None
    if away_code and home_code:
        matchup_hint = f"{away_code}@{home_code}"

    # Fingerprint
    fp_parts = [
        "sportsline", "backfill",
        matchup_hint or "",
        market_type or "",
        str(selection or ""),
        str(line_val or ""),
        expert_slug or expert_name or "",
        event_time or "",
    ]
    fp = sha256_digest("|".join(fp_parts))

    if fp in seen_fps:
        return None
    seen_fps.add(fp)

    raw_pick_text = pick_text[:500]
    raw_block = card.get("fullText", "")[:500]

    # Include result if present (past pick)
    result = pick.get("result")

    rec = {
        "source_id": "sportsline",
        "source_surface": "sportsline_nba_backfill",
        "sport": "NBA",
        "market_family": "standard",
        "observed_at_utc": observed_at.isoformat(),
        "canonical_url": PICKS_URL,
        "raw_pick_text": raw_pick_text,
        "raw_block": raw_block,
        "raw_fingerprint": fp,
        "event_start_time_utc": event_time,
        "matchup_hint": matchup_hint,
        "away_team": away_code,
        "home_team": home_code,
        "expert_name": expert_name,
        "expert_handle": expert_slug,
        "expert_slug": expert_slug,
        "market_type": market_type,
        "selection": selection,
        "side": side,
        "line": line_val,
        "odds": odds_val,
        "player_name": player_name,
        "stat_key": stat_key,
        "pregraded_result": result,
    }
    return rec


def get_earliest_date(page) -> Optional[str]:
    """Extract the earliest date visible on the page."""
    import re
    try:
        body = page.locator("body").text_content(timeout=3000) or ""
        dates = re.findall(
            r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d+\s+202[456])',
            body
        )
        if not dates:
            return None
        # Parse and return earliest
        parsed = []
        month_map = {
            "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
            "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
        }
        for d in dates:
            parts = d.split()
            if len(parts) == 3:
                m = month_map.get(parts[0], 0)
                day = int(parts[1])
                yr = int(parts[2])
                if m:
                    parsed.append(date(yr, m, day))
        if parsed:
            earliest = min(parsed)
            return earliest.isoformat()
    except Exception:
        pass
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill SportsLine picks from /nba/picks/experts/")
    parser.add_argument("--storage", default=DEFAULT_STORAGE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--stop-date", default=DEFAULT_STOP_DATE,
                        help="Stop when earliest pick on page reaches this date (default: 2025-10-22)")
    parser.add_argument("--max-clicks", type=int, default=DEFAULT_MAX_CLICKS,
                        help="Max Load More clicks (default: 1000)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from checkpoint (skip already-seen fingerprints)")
    parser.add_argument("--delay-ms", type=int, default=1200,
                        help="Delay between Load More clicks in ms (default: 1200)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    stop_date = date.fromisoformat(args.stop_date)

    state = load_state(args.state) if args.resume else {"seen_fingerprints": [], "clicks_done": 0}
    seen_fps: Set[str] = set(state.get("seen_fingerprints", []))
    start_clicks = state.get("clicks_done", 0) if args.resume else 0

    print(f"SportsLine Picks Backfill")
    print(f"  Stop date:  {stop_date}")
    print(f"  Max clicks: {args.max_clicks}")
    print(f"  Resume:     {args.resume} ({len(seen_fps)} known fingerprints)")
    print()

    if not os.path.exists(args.storage):
        print(f"ERROR: Storage state not found: {args.storage}")
        sys.exit(1)

    observed_at = datetime.now(timezone.utc)
    total_new = 0
    total_skipped = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            storage_state=args.storage,
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )
        page = context.new_page()

        print(f"Loading {PICKS_URL} ...")
        page.goto(PICKS_URL, wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(3000)

        # Fast-forward if resuming
        if start_clicks > 0:
            print(f"Fast-forwarding {start_clicks} clicks to resume position...")
            for i in range(start_clicks):
                lb = page.locator("button:has-text('Load More Picks')")
                if lb.count() == 0:
                    print(f"  Lost Load More at click {i}")
                    break
                lb.first.scroll_into_view_if_needed()
                lb.first.click()
                page.wait_for_timeout(400)
                if (i + 1) % 50 == 0:
                    print(f"  Fast-forward: {i+1}/{start_clicks}")

        clicks_done = start_clicks
        consecutive_no_new = 0

        while clicks_done < args.max_clicks:
            # Extract all cards currently on page
            cards = extract_all_cards(page)
            new_records = []
            for card in cards:
                rec = parse_card_to_record(card, observed_at, seen_fps)
                if rec:
                    new_records.append(rec)
                else:
                    total_skipped += 1

            if new_records:
                append_records(args.output, new_records)
                total_new += len(new_records)
                consecutive_no_new = 0
            else:
                consecutive_no_new += 1

            # Check earliest date
            earliest = get_earliest_date(page)

            if clicks_done % 20 == 0 or new_records:
                print(
                    f"  clicks={clicks_done:>4}  new={total_new:>5}  "
                    f"earliest={earliest or '?'}  cards_on_page={len(cards)}",
                    flush=True
                )

            # Check if we've reached stop date
            if earliest and earliest <= args.stop_date:
                print(f"\nReached stop date {args.stop_date} (earliest={earliest}) — done.")
                break

            # Click Load More
            lb = page.locator("button:has-text('Load More Picks')")
            if lb.count() == 0:
                print(f"\nNo more 'Load More Picks' button after {clicks_done} clicks — done.")
                break

            try:
                lb.first.scroll_into_view_if_needed()
                lb.first.click()
                page.wait_for_timeout(args.delay_ms)
                clicks_done += 1
            except Exception as e:
                print(f"\nError clicking Load More: {e}")
                break

            # Save checkpoint every 50 clicks
            if clicks_done % 50 == 0:
                state["seen_fingerprints"] = list(seen_fps)
                state["clicks_done"] = clicks_done
                state["earliest_date_seen"] = earliest
                save_state(args.state, state)
                print(f"  [checkpoint] saved at click {clicks_done}")

        # Final extraction pass
        cards = extract_all_cards(page)
        new_records = []
        for card in cards:
            rec = parse_card_to_record(card, observed_at, seen_fps)
            if rec:
                new_records.append(rec)
        if new_records:
            append_records(args.output, new_records)
            total_new += len(new_records)

        context.close()
        browser.close()

    # Save final state
    state["seen_fingerprints"] = list(seen_fps)
    state["clicks_done"] = clicks_done
    state["last_run"] = observed_at.isoformat()
    save_state(args.state, state)

    print()
    print(f"Done.")
    print(f"  Total new records: {total_new}")
    print(f"  Total skipped (dupes): {total_skipped}")
    print(f"  Load More clicks: {clicks_done}")
    print(f"  Output: {args.output}")
    print()

    if total_new == 0:
        print("WARNING: 0 new records extracted. Check auth or page structure.")
        sys.exit(1)

    # Now normalize using the existing sportsline normalizer
    print("Normalizing records...")
    try:
        from scripts.normalize_sportsline_expert_picks import normalize_records
        norm_path = args.output.replace("raw_", "normalized_").replace(".jsonl", ".jsonl")

        raw_records = []
        with open(args.output) as f:
            for line in f:
                line = line.strip()
                if line:
                    raw_records.append(json.loads(line))

        normalized = normalize_records(raw_records)
        eligible = sum(1 for r in normalized if r.get("eligible_for_consensus"))
        with open(norm_path, "w") as f:
            for r in normalized:
                f.write(json.dumps(r) + "\n")
        print(f"  Normalized: {len(normalized)} records ({eligible} eligible)")
        print(f"  Normalized output: {norm_path}")
        print()
        print("Next steps:")
        print(f"  ./run_daily.sh --skip-ingest")
        print(f"  (or manually: python3 scripts/build_signal_ledger.py --include-normalized-jsonl {norm_path})")
    except Exception as e:
        print(f"  Normalization skipped: {e}")
        print(f"  Run manually: python3 scripts/normalize_sportsline_expert_picks.py {args.output}")


if __name__ == "__main__":
    main()
