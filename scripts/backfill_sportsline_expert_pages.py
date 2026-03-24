#!/usr/bin/env python3
"""
Backfill SportsLine picks from individual expert profile pages.

Expert home pages (no league filter) return ALL sports — NBA, NCAAB, NFL, etc.
Each pick is tagged with an inferred sport so downstream normalizers can route it.

Usage:
    # All-sports backfill (recommended) — scrape every expert's home page
    python3 scripts/backfill_sportsline_expert_pages.py --sport ALL

    # Legacy single-sport modes still work
    python3 scripts/backfill_sportsline_expert_pages.py --sport NBA
    python3 scripts/backfill_sportsline_expert_pages.py --sport NCAAB

    # Go back further / limit clicks
    python3 scripts/backfill_sportsline_expert_pages.py --sport ALL --stop-date 2023-10-01
    python3 scripts/backfill_sportsline_expert_pages.py --sport ALL --max-clicks 3000

    # Run specific experts only
    python3 scripts/backfill_sportsline_expert_pages.py --sport ALL --experts mike-barner prop-bet-guy

    # Resume from checkpoint
    python3 scripts/backfill_sportsline_expert_pages.py --sport ALL --resume

Output (ALL):
    out/raw_sportsline_expert_{slug}_all_backfill.jsonl   (one file per expert)
    out/raw_sportsline_expert_pages_all_combined.jsonl    (all experts merged)

Output (NBA legacy):
    out/raw_sportsline_expert_{slug}_backfill.jsonl
    out/raw_sportsline_expert_pages_combined.jsonl

Output (NCAAB legacy):
    out/raw_sportsline_expert_{slug}_ncaab_backfill.jsonl
    out/raw_sportsline_expert_pages_ncaab_combined.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    from playwright.sync_api import sync_playwright
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
except ImportError:
    print("ERROR: playwright not installed. Run: pip install playwright && playwright install chromium")
    sys.exit(1)

from sportsline_ingest import (
    EXTRACT_EXPERT_PICKS_JS,
    _parse_matchup_text,
    _parse_pick_text,
    _parse_expert_text,
    _map_team,
    sha256_digest,
)

# ---------------------------------------------------------------------------
# Expert definitions
# ---------------------------------------------------------------------------

# All-sports list: home pages with no league filter.
# These return every sport the expert covers (NBA, NCAAB, NFL, MLB, etc.)
# Sport is inferred per-pick from the matchup teams.
EXPERTS_ALL = [
    {"slug": "mike-barner",     "url": "https://www.sportsline.com/experts/33404218/mike-barner/"},
    {"slug": "prop-bet-guy",    "url": "https://www.sportsline.com/experts/51314525/prop-bet-guy/"},
    {"slug": "micah-roberts",   "url": "https://www.sportsline.com/experts/7950/micah-roberts/"},
    {"slug": "matt-severance",  "url": "https://www.sportsline.com/experts/50774572/matt-severance/"},
    {"slug": "zack-cimini",     "url": "https://www.sportsline.com/experts/15770/zack-cimini/"},
    {"slug": "larry-hartstein", "url": "https://www.sportsline.com/experts/13773/larry-hartstein/"},
    {"slug": "alex-selesnick",  "url": "https://www.sportsline.com/experts/51243297/alex-selesnick/"},
    {"slug": "stephen-oh",      "url": "https://www.sportsline.com/experts/15210/stephen-oh/"},
    {"slug": "bob-konarski",    "url": "https://www.sportsline.com/experts/51283976/bob-konarski/"},
    {"slug": "josh-nagel",      "url": "https://www.sportsline.com/experts/15733/josh-nagel/"},
    {"slug": "eric-cohen",      "url": "https://www.sportsline.com/experts/51282681/eric-cohen/"},
    {"slug": "mike-tierney",    "url": "https://www.sportsline.com/experts/15731/mike-tierney/"},
    {"slug": "chip-patterson",  "url": "https://www.sportsline.com/experts/13230/chip-patterson/"},
    {"slug": "thomas-casale",   "url": "https://www.sportsline.com/experts/51306423/thomas-casale/"},
    {"slug": "jeff-hochman",    "url": "https://www.sportsline.com/experts/51190044/jeff-hochman/"},
]

# Legacy sport-specific lists (kept for --sport NBA / --sport NCAAB backward compat)
EXPERTS_NBA = [
    {"slug": "mike-barner",     "url": "https://www.sportsline.com/experts/33404218/mike-barner/?league=nba"},
    {"slug": "prop-bet-guy",    "url": "https://www.sportsline.com/experts/51314525/prop-bet-guy/?league=nba"},
    {"slug": "micah-roberts",   "url": "https://www.sportsline.com/experts/7950/micah-roberts/?league=nba"},
    {"slug": "matt-severance",  "url": "https://www.sportsline.com/experts/50774572/matt-severance/?league=nba"},
    {"slug": "larry-hartstein", "url": "https://www.sportsline.com/experts/13773/larry-hartstein/?league=nba"},
]

EXPERTS_NCAAB = [
    {"slug": "micah-roberts",   "url": "https://www.sportsline.com/experts/7950/micah-roberts/?league=cbb"},
    {"slug": "bob-konarski",    "url": "https://www.sportsline.com/experts/51283976/bob-konarski/?league=cbb"},
    {"slug": "chip-patterson",  "url": "https://www.sportsline.com/experts/13230/chip-patterson/?league=cbb"},
    {"slug": "thomas-casale",   "url": "https://www.sportsline.com/experts/51306423/thomas-casale/?league=cbb"},
    {"slug": "eric-cohen",      "url": "https://www.sportsline.com/experts/51282681/eric-cohen/?league=cbb"},
]

# Keep backward-compatible alias
EXPERTS = EXPERTS_NBA

DEFAULT_STORAGE = "data/sportsline_storage_state.json"
DEFAULT_STOP_DATE = "2023-10-01"
DEFAULT_MAX_CLICKS = 3000
DEFAULT_DELAY_MS = 1200
COMBINED_OUTPUT_NBA = "out/raw_sportsline_expert_pages_combined.jsonl"
COMBINED_OUTPUT_NCAAB = "out/raw_sportsline_expert_pages_ncaab_combined.jsonl"
COMBINED_OUTPUT_ALL = "out/raw_sportsline_expert_pages_all_combined.jsonl"
COMBINED_OUTPUT = COMBINED_OUTPUT_NBA  # backward compat
STATE_DIR = "out"

# ---------------------------------------------------------------------------
# Sport inference from matchup teams
# ---------------------------------------------------------------------------

# Known NBA team codes (canonical abbreviations we use)
_NBA_TEAMS = {
    "ATL","BOS","BKN","CHA","CHI","CLE","DAL","DEN","DET","GSW",
    "HOU","IND","LAC","LAL","MEM","MIA","MIL","MIN","NOP","NYK",
    "OKC","ORL","PHI","PHX","POR","SAC","SAS","TOR","UTA","WAS",
}

# Known NCAAB conferences / keywords that strongly suggest college ball
# (We can't list all 350 teams, so we use a heuristic: if neither team
# maps to NBA, NFL, MLB, or NHL we call it NCAAB/NCAAF based on season timing)
_NFL_TEAMS = {
    "ARI","ATL","BAL","BUF","CAR","CHI","CIN","CLE","DAL","DEN",
    "DET","GB","HOU","IND","JAX","KC","LV","LAC","LAR","MIA",
    "MIN","NE","NO","NYG","NYJ","PHI","PIT","SF","SEA","TB",
    "TEN","WAS",
}
_MLB_TEAMS = {
    "ARI","ATL","BAL","BOS","CHC","CWS","CIN","CLE","COL","DET",
    "HOU","KC","LAA","LAD","MIA","MIL","MIN","NYM","NYY","OAK",
    "PHI","PIT","SD","SEA","SF","STL","TB","TEX","TOR","WAS",
}
_NHL_TEAMS = {
    "ANA","ARI","BOS","BUF","CGY","CAR","CHI","COL","CBJ","DAL",
    "DET","EDM","FLA","LAK","MIN","MTL","NSH","NJD","NYI","NYR",
    "OTT","PHI","PIT","SJS","SEA","STL","TBL","TOR","VAN","VGK",
    "WSH","WPG",
}


def _infer_sport(away_code: Optional[str], home_code: Optional[str], event_time: Optional[str]) -> str:
    """Infer the sport from team codes and event timing.

    Returns one of: NBA, NCAAB, NFL, NCAAF, MLB, NHL, or UNKNOWN.
    """
    codes = {c for c in (away_code, home_code) if c}
    if codes & _NBA_TEAMS:
        return "NBA"
    if codes & _NFL_TEAMS and not (codes & _NBA_TEAMS):
        # Distinguish NFL vs NBA for shared abbreviations (ATL, CHI, etc.)
        # Use event time: NFL plays Sep-Feb, NBA Oct-Jun
        if event_time:
            try:
                from datetime import datetime, timezone
                dt = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
                # NFL regular season: Sep-Jan; playoffs extend to Feb
                if dt.month in (9, 10, 11, 12, 1, 2):
                    # Could be NFL or NBA start — check if ANY code is purely NFL
                    nfl_only = codes & (_NFL_TEAMS - _NBA_TEAMS)
                    if nfl_only:
                        return "NFL"
            except Exception:
                pass
    if codes & _MLB_TEAMS and not (codes & _NBA_TEAMS):
        return "MLB"
    if codes & _NHL_TEAMS and not (codes & _NBA_TEAMS):
        return "NHL"
    # If we couldn't map to a known pro league, tag as UNKNOWN so downstream
    # can handle it later (NCAAB, NCAAF, etc. don't have reliable team codes yet)
    return "UNKNOWN"


# ---------------------------------------------------------------------------
# State helpers (per-expert checkpoint)
# ---------------------------------------------------------------------------

def _state_path(slug: str, sport: str = "NBA") -> str:
    if sport == "ALL":
        return f"{STATE_DIR}/sportsline_expert_{slug}_all_state.json"
    suffix = "" if sport == "NBA" else f"_{sport.lower()}"
    return f"{STATE_DIR}/sportsline_expert_{slug}{suffix}_state.json"


def _output_path(slug: str, sport: str = "NBA") -> str:
    if sport == "ALL":
        return f"out/raw_sportsline_expert_{slug}_all_backfill.jsonl"
    suffix = "" if sport == "NBA" else f"_{sport.lower()}"
    return f"out/raw_sportsline_expert_{slug}{suffix}_backfill.jsonl"


def load_state(slug: str, sport: str = "NBA") -> Dict[str, Any]:
    p = _state_path(slug, sport)
    if os.path.exists(p):
        try:
            with open(p) as f:
                return json.load(f)
        except Exception:
            pass
    return {"seen_fingerprints": [], "clicks_done": 0, "earliest_date_seen": None, "total_records": 0}


def save_state(slug: str, state: Dict[str, Any], sport: str = "NBA") -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(_state_path(slug, sport), "w") as f:
        json.dump(state, f, indent=2)


def append_records(path: str, records: List[Dict]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------------------
# Extraction helpers (same as backfill_sportsline_picks_page.py)
# ---------------------------------------------------------------------------

def extract_all_cards(page) -> List[Dict]:
    try:
        return page.evaluate(EXTRACT_EXPERT_PICKS_JS) or []
    except Exception as e:
        print(f"  [warn] JS extraction error: {e}")
        return []


def parse_card_to_record(
    card: Dict,
    observed_at: datetime,
    seen_fps: Set[str],
    expert_slug: str,
    canonical_url: str,
    sport: str = "NBA",
) -> Optional[Dict]:
    matchup_text = card.get("matchupText", "")
    pick_text = card.get("pickText", "")
    # expert_text intentionally ignored — it contains ROI figures like "+3519"
    # not the expert's name. We always derive the name from expert_slug.

    if not pick_text:
        return None

    matchup = _parse_matchup_text(matchup_text)
    away_raw = matchup.get("away_raw")
    home_raw = matchup.get("home_raw")
    away_code = _map_team(away_raw) if away_raw else None
    home_code = _map_team(home_raw) if home_raw else None
    event_time = matchup.get("event_time_utc")

    # When sport="ALL" (all-sports home page), infer the sport from team codes.
    # For legacy sport-specific scrapes, use the passed sport value.
    if sport == "ALL":
        inferred_sport = _infer_sport(away_code, home_code, event_time)
    else:
        inferred_sport = sport

    pick = _parse_pick_text(pick_text)
    market_type = pick.get("market_type")
    if not market_type:
        return None

    # Always derive expert_name from the URL slug — never from expert_text which
    # shows ROI profit figures (e.g. "+3519.75") instead of the expert's name.
    expert_name = " ".join(w.capitalize() for w in expert_slug.split("-"))

    line_val = pick.get("line")
    odds_val = pick.get("odds")
    side = pick.get("side")
    stat_key = pick.get("stat_key")
    player_name = pick.get("player_name")
    team_raw = pick.get("team_raw")

    if market_type in ("spread", "moneyline") and not side and team_raw:
        side = _map_team(team_raw) or team_raw

    # Use inferred_sport prefix for player_prop selections so downstream
    # normalizers can route them to the right sport pipeline.
    selection = None
    if market_type == "player_prop" and player_name and stat_key and side:
        selection = f"{inferred_sport}:{player_name}::{stat_key}::{side.upper()}"
    elif market_type in ("spread", "total", "moneyline") and side:
        selection = side.upper()

    matchup_hint = None
    if away_code and home_code:
        matchup_hint = f"{away_code}@{home_code}"

    # Fingerprint uses inferred_sport (not the raw "ALL") so NBA and NCAAB
    # picks from the same expert don't collide with legacy sport-specific backfills.
    fp_parts = [
        "sportsline", "expert_pages", inferred_sport.lower(), expert_slug,
        matchup_hint or "",
        market_type or "",
        str(selection or ""),
        str(line_val or ""),
        event_time or "",
    ]
    fp = sha256_digest("|".join(fp_parts))

    if fp in seen_fps:
        return None
    seen_fps.add(fp)

    result = pick.get("result")

    # Surface name encodes the mode used ("all" vs legacy "nba"/"ncaab")
    if sport == "ALL":
        surface = "sportsline_expert_pages_backfill"
    elif sport == "NBA":
        surface = "sportsline_expert_pages_backfill"
    else:
        surface = f"sportsline_expert_pages_backfill_{sport.lower()}"

    rec = {
        "source_id": "sportsline",
        "source_surface": surface,
        "sport": inferred_sport,
        "market_family": "standard",
        "observed_at_utc": observed_at.isoformat(),
        "canonical_url": canonical_url,
        "raw_pick_text": pick_text[:500],
        "raw_block": card.get("fullText", "")[:500],
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
    import re
    try:
        body = page.locator("body").text_content(timeout=3000) or ""
        # Cover 2023–2029 (was 202[456] which missed 2023 backfill)
        dates = re.findall(
            r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d+\s+202[3-9])',
            body
        )
        if not dates:
            return None
        month_map = {
            "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
            "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
        }
        parsed = []
        for d in dates:
            parts = d.split()
            if len(parts) == 3:
                m = month_map.get(parts[0], 0)
                day = int(parts[1])
                yr = int(parts[2])
                if m:
                    parsed.append(date(yr, m, day))
        if parsed:
            return min(parsed).isoformat()
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Per-expert scrape
# ---------------------------------------------------------------------------

def scrape_expert(
    expert: Dict[str, str],
    storage_path: str,
    stop_date: date,
    max_clicks: int,
    delay_ms: int,
    resume: bool,
    observed_at: datetime,
    sport: str = "NBA",
) -> int:
    slug = expert["slug"]
    url = expert["url"]
    out_path = _output_path(slug, sport)

    state = load_state(slug, sport) if resume else {"seen_fingerprints": [], "clicks_done": 0}
    seen_fps: Set[str] = set(state.get("seen_fingerprints", []))
    start_clicks = state.get("clicks_done", 0) if resume else 0

    print(f"\n{'='*60}")
    print(f"Expert: {slug}")
    print(f"URL:    {url}")
    print(f"Stop:   {stop_date}  Max clicks: {max_clicks}")
    print(f"Resume: {resume} ({len(seen_fps)} known fps, {start_clicks} clicks done)")
    print(f"Output: {out_path}")
    print(f"{'='*60}")

    total_new = 0
    total_skipped = 0
    clicks_done = start_clicks

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            storage_state=storage_path,
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )
        page = context.new_page()

        print(f"Loading page...")
        try:
            page.goto(url, wait_until="networkidle", timeout=40000)
        except PlaywrightTimeoutError:
            print(f"  [warn] networkidle timeout — continuing anyway")
        page.wait_for_timeout(3000)

        # Fast-forward if resuming
        if start_clicks > 0:
            print(f"Fast-forwarding {start_clicks} clicks...")
            for i in range(start_clicks):
                lb = page.locator("button:has-text('Load More')")
                if lb.count() == 0:
                    print(f"  Lost Load More at click {i}")
                    break
                lb.first.scroll_into_view_if_needed()
                lb.first.click()
                page.wait_for_timeout(400)
                if (i + 1) % 50 == 0:
                    print(f"  Fast-forward: {i+1}/{start_clicks}")

        while clicks_done < max_clicks:
            cards = extract_all_cards(page)
            new_records = []
            for card in cards:
                rec = parse_card_to_record(card, observed_at, seen_fps, slug, url, sport)
                if rec:
                    new_records.append(rec)
                else:
                    total_skipped += 1

            if new_records:
                append_records(out_path, new_records)
                total_new += len(new_records)

            earliest = get_earliest_date(page)

            if clicks_done % 20 == 0 or new_records:
                print(
                    f"  clicks={clicks_done:>4}  new={total_new:>5}  "
                    f"earliest={earliest or '?'}  cards_on_page={len(cards)}",
                    flush=True
                )

            if earliest and earliest <= stop_date.isoformat():
                print(f"\n  Reached stop date {stop_date} (earliest={earliest}) — done.")
                break

            lb = page.locator("button:has-text('Load More')")
            if lb.count() == 0:
                print(f"\n  No more 'Load More' button after {clicks_done} clicks — done.")
                break

            try:
                lb.first.scroll_into_view_if_needed()
                lb.first.click()
                page.wait_for_timeout(delay_ms)
                clicks_done += 1
            except Exception as e:
                print(f"\n  Error clicking Load More: {e}")
                break

            # Checkpoint every 50 clicks
            if clicks_done % 50 == 0:
                state["seen_fingerprints"] = list(seen_fps)
                state["clicks_done"] = clicks_done
                state["earliest_date_seen"] = earliest
                state["total_records"] = total_new
                save_state(slug, state, sport)
                print(f"  [checkpoint] saved at click {clicks_done}")

        # Final extraction pass
        cards = extract_all_cards(page)
        for card in cards:
            rec = parse_card_to_record(card, observed_at, seen_fps, slug, url, sport)
            if rec:
                append_records(out_path, [rec])
                total_new += 1

        context.close()
        browser.close()

    # Save final state
    state["seen_fingerprints"] = list(seen_fps)
    state["clicks_done"] = clicks_done
    state["last_run"] = observed_at.isoformat()
    state["total_records"] = total_new
    save_state(slug, state, sport)

    print(f"\n  {slug}: {total_new} new records, {total_skipped} skipped, {clicks_done} clicks")
    return total_new


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill SportsLine picks from expert profile pages")
    parser.add_argument("--storage", default=DEFAULT_STORAGE)
    parser.add_argument("--sport", choices=["NBA", "NCAAB", "ALL"], default="ALL",
                        help="Sport mode: ALL=home page (all sports, recommended), NBA/NCAAB=legacy league filter (default: ALL)")
    parser.add_argument("--stop-date", default=DEFAULT_STOP_DATE,
                        help="Stop when earliest pick reaches this date (default: 2023-10-01)")
    parser.add_argument("--max-clicks", type=int, default=DEFAULT_MAX_CLICKS,
                        help="Max Load More clicks per expert (default: 2000)")
    parser.add_argument("--delay-ms", type=int, default=DEFAULT_DELAY_MS,
                        help="Delay between clicks in ms (default: 1200)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from per-expert checkpoints")
    parser.add_argument("--experts", nargs="+", metavar="SLUG",
                        help="Only run these expert slugs (default: all experts for sport)")
    parser.add_argument("--no-combine", action="store_true",
                        help="Skip combining per-expert files into combined output")
    parser.add_argument("--no-normalize", action="store_true",
                        help="Skip normalization step")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    if not os.path.exists(args.storage):
        print(f"ERROR: Storage state not found: {args.storage}")
        print("Run scripts/create_sportsline_storage.py first")
        sys.exit(1)

    sport = args.sport
    if sport == "ALL":
        expert_list = EXPERTS_ALL
        combined_output = COMBINED_OUTPUT_ALL
    elif sport == "NBA":
        expert_list = EXPERTS_NBA
        combined_output = COMBINED_OUTPUT_NBA
    else:
        expert_list = EXPERTS_NCAAB
        combined_output = COMBINED_OUTPUT_NCAAB

    stop_date = date.fromisoformat(args.stop_date)
    experts_to_run = expert_list
    if args.experts:
        slugs = set(args.experts)
        experts_to_run = [e for e in expert_list if e["slug"] in slugs]
        if not experts_to_run:
            print(f"ERROR: No matching experts for slugs: {args.experts}")
            print(f"Available: {[e['slug'] for e in expert_list]}")
            sys.exit(1)

    observed_at = datetime.now(timezone.utc)
    grand_total = 0

    print(f"Sport: {sport}  |  Experts: {[e['slug'] for e in experts_to_run]}")

    for expert in experts_to_run:
        n = scrape_expert(
            expert=expert,
            storage_path=args.storage,
            stop_date=stop_date,
            max_clicks=args.max_clicks,
            delay_ms=args.delay_ms,
            resume=args.resume,
            observed_at=observed_at,
            sport=sport,
        )
        grand_total += n

    print(f"\n{'='*60}")
    print(f"All experts done. Grand total new records: {grand_total}")

    # Combine per-expert files into one
    if not args.no_combine:
        print(f"\nCombining per-expert files → {combined_output}")
        os.makedirs(os.path.dirname(combined_output) or ".", exist_ok=True)
        combined_fps: Set[str] = set()
        combined_count = 0
        with open(combined_output, "w", encoding="utf-8") as out_f:
            for expert in experts_to_run:
                slug = expert["slug"]
                per_file = _output_path(slug, sport)
                if not os.path.exists(per_file):
                    print(f"  [skip] {per_file} not found")
                    continue
                with open(per_file, encoding="utf-8") as in_f:
                    for line in in_f:
                        line = line.strip()
                        if not line:
                            continue
                        rec = json.loads(line)
                        fp = rec.get("raw_fingerprint", "")
                        if fp and fp in combined_fps:
                            continue
                        combined_fps.add(fp)
                        out_f.write(line + "\n")
                        combined_count += 1
        print(f"  Combined: {combined_count} unique records → {combined_output}")

    norm_path = combined_output.replace("raw_", "normalized_")

    # Normalize
    if not args.no_normalize and grand_total > 0:
        print(f"\nNormalizing combined output...")
        try:
            from src.normalizer_sportsline_nba import normalize_sportsline_records

            raw_records = []
            with open(combined_output) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        raw_records.append(json.loads(line))

            if sport == "ALL":
                # Split by inferred sport and normalize each separately.
                # Unknown/non-implemented sports are written to a separate file for later.
                by_sport: Dict[str, List[Dict]] = {}
                for rec in raw_records:
                    s = rec.get("sport", "UNKNOWN")
                    by_sport.setdefault(s, []).append(rec)

                print(f"  Sport breakdown: { {s: len(v) for s, v in sorted(by_sport.items())} }")

                all_normalized = []
                for s, recs in sorted(by_sport.items()):
                    if s in ("NBA", "NCAAB"):
                        normed = normalize_sportsline_records(recs, sport=s)
                        all_normalized.extend(normed)
                        print(f"  {s}: {len(normed)} normalized (from {len(recs)} raw)")
                    else:
                        # Write unimplemented sports to a separate file for future use
                        other_path = norm_path.replace("_all_", f"_{s.lower()}_")
                        with open(other_path, "w") as f:
                            for r in recs:
                                f.write(json.dumps(r) + "\n")
                        print(f"  {s}: {len(recs)} raw records saved to {other_path} (not yet implemented)")

                with open(norm_path, "w") as f:
                    for r in all_normalized:
                        f.write(json.dumps(r) + "\n")
                print(f"  Total normalized (NBA+NCAAB): {len(all_normalized)}")
            else:
                normalized = normalize_sportsline_records(raw_records, sport=sport)
                with open(norm_path, "w") as f:
                    for r in normalized:
                        f.write(json.dumps(r) + "\n")
                all_normalized = normalized
                print(f"  Normalized: {len(normalized)} eligible records (from {len(raw_records)} raw)")

            print(f"  Output: {norm_path}")
            print(f"\nNext step:")
            print(f"  python3 scripts/build_signal_ledger.py --include-normalized-jsonl {norm_path}")
        except Exception as e:
            print(f"  Normalization error: {e}")
            import traceback
            traceback.print_exc()
    elif grand_total == 0:
        print("\nWARNING: 0 new records extracted. Check auth or page structure.")
    else:
        print(f"\nSkipping normalization (--no-normalize). Run manually.")
        print(f"  norm_path: {norm_path}")


if __name__ == "__main__":
    main()
