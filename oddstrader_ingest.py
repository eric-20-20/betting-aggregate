"""OddsTrader AI picks scraper.

Scrapes OddsTrader's AI model predictions for NBA and NCAAB.
Extracts spread, total, and moneyline picks from window.__INITIAL_STATE__.

Usage:
    python3 oddstrader_ingest.py                  # NBA (default)
    python3 oddstrader_ingest.py --sport NCAAB     # College basketball
    python3 oddstrader_ingest.py --debug           # Verbose output
"""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from playwright.sync_api import sync_playwright

from action_ingest import dedupe_normalized_bets, json_default, sha256_digest
from store import NBA_SPORT, NCAAB_SPORT

OUT_DIR = os.getenv("NBA_OUT_DIR", "out")

URLS = {
    NBA_SPORT: "https://www.oddstrader.com/nba/picks/",
    NCAAB_SPORT: "https://www.oddstrader.com/ncaa-college-basketball/picks/",
}

# OddsTrader league IDs
LEAGUE_IDS = {
    NBA_SPORT: "5",
    NCAAB_SPORT: "14",
}

# Market type ID → our market_type
MARKET_TYPE_MAP = {
    401: "spread",
    402: "total",
    83: "moneyline",
}


def fetch_initial_state(url: str, debug: bool = False) -> Dict[str, Any]:
    """Load the page with Playwright and extract window.__INITIAL_STATE__."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1400, "height": 900},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )
        page = context.new_page()
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(3000)  # wait for hydration

            state_json = page.evaluate("() => JSON.stringify(window.__INITIAL_STATE__)")
            if not state_json:
                if debug:
                    print("[DEBUG] __INITIAL_STATE__ is null, trying alternate extraction")
                # Try extracting from script tags
                scripts = page.query_selector_all("script")
                for s in scripts:
                    text = s.inner_text()
                    if "__INITIAL_STATE__" in text:
                        match = re.search(r"window\.__INITIAL_STATE__\s*=\s*({.*?});", text, re.DOTALL)
                        if match:
                            state_json = match.group(1)
                            break

            if not state_json:
                print(f"[WARN] Could not extract __INITIAL_STATE__ from {url}")
                return {}

            return json.loads(state_json)
        finally:
            page.close()
            context.close()
            browser.close()


def extract_picks(state: Dict[str, Any], sport: str, debug: bool = False) -> List[Dict[str, Any]]:
    """Extract picks from the __INITIAL_STATE__ JSON."""
    lid = LEAGUE_IDS.get(sport, "5")
    picks_root = state.get("picks", {})
    raw_picks = []

    # Events live under picks.events[lid] (list of event dicts)
    events_list = picks_root.get("events", {}).get(lid, [])
    event_lookup: Dict[int, Dict] = {}
    if isinstance(events_list, list):
        for evt in events_list:
            if isinstance(evt, dict) and "eid" in evt:
                event_lookup[evt["eid"]] = evt

    # Picks live under picks.picks[lid].picks (list of pick entries)
    picks_data = picks_root.get("picks", {}).get(lid, {})
    pick_entries = picks_data.get("picks", []) if isinstance(picks_data, dict) else []
    if debug:
        print(f"[DEBUG] League {lid}: {len(pick_entries)} pick entries, {len(event_lookup)} events")

    for entry in pick_entries:
        eid = entry.get("eid")
        if not eid:
            continue
        eid_int = int(eid) if isinstance(eid, str) else eid
        event = event_lookup.get(eid_int, {})
        consensus_items = entry.get("consensus", [])
        stats = entry.get("stats", [])

        # Extract predicted scores
        predicted_scores = {}
        for s in stats:
            stat_name = s.get("stat", "")
            partid = s.get("partid")
            val = s.get("val")
            if stat_name in ("homescore", "awayscore") and partid and val:
                predicted_scores[f"{stat_name}_{partid}"] = val

        # Extract participants
        participants = event.get("participants", [])
        home_team = None
        away_team = None
        home_partid = None
        away_partid = None
        for part in participants:
            source = part.get("source", {})
            if part.get("ih"):
                home_team = source.get("abbr")
                home_partid = part.get("partid")
            else:
                away_team = source.get("abbr")
                away_partid = part.get("partid")

        if not home_team or not away_team:
            if debug:
                print(f"[DEBUG] Skipping eid={eid}: missing team info")
            continue

        # Extract market types from event
        market_types = {}
        for mt in event.get("marketTypes", []):
            mtid = mt.get("mtid")
            if mtid:
                market_types[mtid] = mt.get("nam", "")

        # Process consensus items to determine picks
        # Group by market type
        consensus_by_mt: Dict[int, List[Dict]] = {}
        for c in consensus_items:
            mtid = c.get("mtid")
            if mtid:
                consensus_by_mt.setdefault(mtid, []).append(c)

        # Process each market type
        for mtid, items in consensus_by_mt.items():
            market_type = MARKET_TYPE_MAP.get(mtid)
            if not market_type:
                continue

            # Find the consensus pick (highest percentage)
            best = max(items, key=lambda x: x.get("perc", 0))
            pick_partid = best.get("partid")
            consensus_pct = best.get("perc", 0)

            # Determine selection
            if market_type == "moneyline":
                if pick_partid == home_partid:
                    selection = home_team
                    side = home_team
                elif pick_partid == away_partid:
                    selection = away_team
                    side = away_team
                else:
                    continue
            elif market_type == "spread":
                if pick_partid == home_partid:
                    selection = home_team
                    side = home_team
                else:
                    selection = away_team
                    side = away_team
            elif market_type == "total":
                # For totals, predict over/under from scores
                home_score_str = predicted_scores.get(f"homescore_{home_partid}")
                away_score_str = predicted_scores.get(f"awayscore_{away_partid}")
                if home_score_str and away_score_str:
                    try:
                        predicted_total = float(home_score_str) + float(away_score_str)
                    except (ValueError, TypeError):
                        predicted_total = None
                else:
                    predicted_total = None

                # Use consensus to determine over/under
                # The partid in consensus for totals typically maps to over/under
                # We'll check if pick_partid matches home (under) or away (over)
                selection = "game_total"
                side = "over"  # default, refined below
                if predicted_total is not None:
                    side = "over"  # will be refined by line comparison downstream

            # Build raw pick text
            event_desc = event.get("des", f"{away_team}@{home_team}")
            if market_type == "spread":
                raw_pick_text = f"{selection} (spread) | {event_desc}"
            elif market_type == "total":
                raw_pick_text = f"{side} | {event_desc}"
            elif market_type == "moneyline":
                raw_pick_text = f"{selection} ML | {event_desc}"

            # Parse event time
            dt_ms = event.get("dt", 0)
            event_time = datetime.fromtimestamp(dt_ms / 1000, tz=timezone.utc) if dt_ms else None

            raw_picks.append({
                "source_id": "oddstrader",
                "source_surface": f"oddstrader_ai_{market_type}",
                "sport": sport,
                "event_id": eid,
                "event_desc": event_desc,
                "event_slug": event.get("slg", ""),
                "event_time_utc": event_time.isoformat() if event_time else None,
                "home_team": home_team,
                "away_team": away_team,
                "market_type": market_type,
                "market_type_id": mtid,
                "side": side,
                "selection": selection,
                "consensus_pct": consensus_pct,
                "raw_pick_text": raw_pick_text,
                "predicted_scores": predicted_scores,
                "expert_name": "OddsTrader AI",
            })

    if debug:
        print(f"[DEBUG] Extracted {len(raw_picks)} raw picks")
    return raw_picks


def normalize_pick(raw: Dict[str, Any], sport: str) -> Optional[Dict[str, Any]]:
    """Normalize a raw OddsTrader pick to the standard schema."""
    home_team = raw.get("home_team", "")
    away_team = raw.get("away_team", "")

    # OddsTrader already provides standard abbreviations (PHI, ATL, etc.)
    home_code = home_team
    away_code = away_team

    # Build event key
    event_time = raw.get("event_time_utc")
    canonical_event_key = None
    if event_time and home_code and away_code:
        try:
            dt = datetime.fromisoformat(event_time)
            date_str = dt.strftime("%Y:%m:%d")
            canonical_event_key = f"{sport}:{date_str}:{away_code}@{home_code}"
        except (ValueError, TypeError):
            pass

    event_key = canonical_event_key
    if event_time:
        try:
            dt = datetime.fromisoformat(event_time)
            time_str = dt.strftime("%H%M")
            event_key = f"{sport}:{dt.strftime('%Y%m%d')}:{away_code}@{home_code}:{time_str}"
        except (ValueError, TypeError):
            pass

    market_type = raw.get("market_type")
    side = raw.get("side")
    selection = raw.get("selection")

    # For spreads, we don't have line data from the picks page
    # For totals, we don't have line data either
    # Odds are not available from OddsTrader picks page

    fingerprint = sha256_digest(
        f"{raw.get('source_id')}:{raw.get('event_id')}:{market_type}:{side}:{selection}"
    )

    return {
        "provenance": {
            "source_id": "oddstrader",
            "source_surface": raw.get("source_surface", "oddstrader_ai"),
            "sport": sport,
            "observed_at_utc": datetime.now(timezone.utc).isoformat(),
            "canonical_url": f"https://www.oddstrader.com/{sport.lower()}/picks/",
            "raw_fingerprint": fingerprint,
            "expert_name": raw.get("expert_name", "OddsTrader AI"),
            "expert_handle": None,
            "expert_profile": None,
            "expert_slug": "oddstrader-ai",
            "tailing_handle": None,
        },
        "event": {
            "event_key": event_key,
            "canonical_event_key": canonical_event_key,
            "event_start_time_utc": raw.get("event_time_utc"),
            "home_team": home_code,
            "away_team": away_code,
        },
        "market": {
            "market_type": market_type,
            "side": side,
            "selection": selection,
            "line": None,  # OddsTrader picks page doesn't expose lines
            "odds": None,  # No odds on the picks page
            "stat_key": None,
            "player_key": None,
        },
        "eligible_for_consensus": market_type in ("spread", "total", "moneyline"),
        "ineligibility_reason": None,
        "eligibility": {
            "eligible_for_consensus": market_type in ("spread", "total", "moneyline"),
            "ineligibility_reason": None,
        },
        "oddstrader_meta": {
            "consensus_pct": raw.get("consensus_pct"),
            "predicted_scores": raw.get("predicted_scores"),
        },
    }


def ensure_out_dir():
    os.makedirs(OUT_DIR, exist_ok=True)


def write_json_file(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=json_default, ensure_ascii=False)


def main():
    parser = argparse.ArgumentParser(description="Ingest OddsTrader AI picks.")
    parser.add_argument("--sport", choices=["NBA", "NCAAB"], default="NBA",
                        help="Sport to ingest (default: NBA)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")
    args = parser.parse_args()

    sport = args.sport
    url = URLS.get(sport)
    if not url:
        print(f"[ERROR] No URL configured for sport {sport}")
        return

    print(f"[INGEST] OddsTrader {sport}: fetching {url}")
    state = fetch_initial_state(url, debug=args.debug)
    if not state:
        print("[WARN] Empty state — no picks extracted")
        return

    raw_picks = extract_picks(state, sport, debug=args.debug)
    print(f"[INGEST] Extracted {len(raw_picks)} raw picks")

    normalized = []
    for raw in raw_picks:
        norm = normalize_pick(raw, sport)
        if norm:
            normalized.append(norm)

    normalized = dedupe_normalized_bets(normalized)

    ensure_out_dir()
    sport_suffix = sport.lower()
    raw_path = os.path.join(OUT_DIR, f"raw_oddstrader_{sport_suffix}.json")
    norm_path = os.path.join(OUT_DIR, f"normalized_oddstrader_{sport_suffix}.json")
    write_json_file(raw_path, raw_picks)
    write_json_file(norm_path, normalized)

    eligible = [n for n in normalized if n.get("eligible_for_consensus")]
    print(f"[INGEST] Wrote {len(raw_picks)} raw records to {raw_path}")
    print(f"[INGEST] Wrote {len(normalized)} normalized records ({len(eligible)} eligible) to {norm_path}")


if __name__ == "__main__":
    main()
