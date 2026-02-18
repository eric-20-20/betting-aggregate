"""
SportsLine NBA picks ingestion script.

Fetches expert picks from SportsLine's /nba/picks/ page using Playwright
with authenticated session state.

Usage:
    python3 sportsline_ingest.py [--storage PATH] [--debug]

Output:
    - out/raw_sportsline_nba.json
    - out/normalized_sportsline_nba.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

try:
    from playwright.sync_api import sync_playwright, Page, BrowserContext
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
except ImportError:
    sync_playwright = None
    PlaywrightTimeoutError = Exception

from store import data_store, write_json
from utils import normalize_text

OUT_DIR = os.getenv("NBA_OUT_DIR", "out")
DEFAULT_STORAGE_STATE = "data/sportsline_storage_state.json"

PICKS_URLS = {
    "NBA": "https://www.sportsline.com/nba/picks/",
    "NCAAB": "https://www.sportsline.com/college-basketball/picks/",
}
PICKS_URL = PICKS_URLS["NBA"]  # backward compatibility

# Market type tab identifiers
MARKET_TABS = {
    "spread": "Against the Spread",
    "moneyline": "Money Line",
    "total": "Over / Under",
}


@dataclass
class RawPickRecord:
    source_id: str
    source_surface: str
    sport: str
    market_family: str
    observed_at_utc: str
    canonical_url: str
    raw_pick_text: str
    raw_block: str
    raw_fingerprint: str
    source_updated_at_utc: Optional[str] = None
    event_start_time_utc: Optional[str] = None
    matchup_hint: Optional[str] = None
    away_team: Optional[str] = None
    home_team: Optional[str] = None
    expert_name: Optional[str] = None
    expert_handle: Optional[str] = None
    expert_profile: Optional[str] = None
    expert_slug: Optional[str] = None
    market_type: Optional[str] = None
    selection: Optional[str] = None
    line: Optional[float] = None
    odds: Optional[int] = None


def sha256_digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def ensure_out_dir() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)


def json_default(o):
    if isinstance(o, datetime):
        return o.isoformat()
    return str(o)


def write_records(path: str, records: List[Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        prepared = []
        for item in records:
            prepared.append(asdict(item) if hasattr(item, "__dataclass_fields__") else item)
        json.dump(prepared, f, ensure_ascii=False, indent=2, default=json_default)


def _map_team(alias: Optional[str]) -> Optional[str]:
    """Map team name or abbreviation to standard team code."""
    if not alias:
        return None
    codes = data_store.lookup_team_code(alias)
    if len(codes) == 1:
        return next(iter(codes))
    # Common team name fallbacks
    fallback = {
        "bucks": "MIL",
        "76ers": "PHI",
        "sixers": "PHI",
        "knicks": "NYK",
        "nets": "BKN",
        "trail blazers": "POR",
        "blazers": "POR",
        "suns": "PHX",
        "spurs": "SAS",
        "warriors": "GSW",
        "pelicans": "NOP",
        "lakers": "LAL",
        "clippers": "LAC",
        "celtics": "BOS",
        "heat": "MIA",
        "bulls": "CHI",
        "cavaliers": "CLE",
        "cavs": "CLE",
        "pistons": "DET",
        "raptors": "TOR",
        "mavericks": "DAL",
        "mavs": "DAL",
        "wolves": "MIN",
        "timberwolves": "MIN",
        "thunder": "OKC",
        "magic": "ORL",
        "rockets": "HOU",
        "jazz": "UTA",
        "kings": "SAC",
        "hawks": "ATL",
        "hornets": "CHA",
        "grizzlies": "MEM",
        "nuggets": "DEN",
        "pacers": "IND",
        "wizards": "WAS",
    }
    norm = normalize_text(alias)
    if norm in fallback:
        return fallback[norm]
    # Try 3-letter abbreviations
    if len(alias) <= 3:
        return alias.upper()
    return None


def _parse_matchup(text: str) -> Tuple[Optional[str], Optional[str]]:
    """Parse matchup text like 'Lakers vs Warriors' or 'LAL @ GSW'."""
    patterns = [
        r"([A-Za-z\s]+?)\s*(?:vs\.?|@|at)\s*([A-Za-z\s]+)",
        r"([A-Z]{2,3})\s*(?:vs\.?|@|at)\s*([A-Z]{2,3})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            away_raw, home_raw = match.group(1).strip(), match.group(2).strip()
            away = _map_team(away_raw)
            home = _map_team(home_raw)
            if away and home:
                return away, home
    return None, None


def _parse_spread(text: str) -> Tuple[Optional[str], Optional[float], Optional[int]]:
    """Parse spread pick like 'LAL -7.5 (-110)'."""
    # Pattern: TEAM +/-SPREAD (ODDS)
    match = re.search(
        r"([A-Z]{2,3})\s*([+-]?\d+(?:\.\d+)?)\s*\(([+-]\d+)\)",
        text.upper()
    )
    if match:
        team = match.group(1)
        line = float(match.group(2))
        odds = int(match.group(3))
        return _map_team(team), line, odds

    # Pattern without parentheses: TEAM +/-SPREAD ODDS
    match = re.search(
        r"([A-Z]{2,3})\s*([+-]?\d+(?:\.\d+)?)\s*([+-]\d{3})",
        text.upper()
    )
    if match:
        team = match.group(1)
        line = float(match.group(2))
        odds = int(match.group(3))
        return _map_team(team), line, odds

    return None, None, None


def _parse_total(text: str) -> Tuple[Optional[str], Optional[float], Optional[int]]:
    """Parse total pick like 'Over 220.5 (-110)'."""
    match = re.search(
        r"(Over|Under)\s*(\d+(?:\.\d+)?)\s*\(([+-]\d+)\)",
        text,
        re.IGNORECASE
    )
    if match:
        direction = match.group(1).upper()
        line = float(match.group(2))
        odds = int(match.group(3))
        return direction, line, odds

    # Without parentheses
    match = re.search(
        r"(Over|Under)\s*(\d+(?:\.\d+)?)\s*([+-]\d{3})",
        text,
        re.IGNORECASE
    )
    if match:
        direction = match.group(1).upper()
        line = float(match.group(2))
        odds = int(match.group(3))
        return direction, line, odds

    return None, None, None


def _parse_moneyline(text: str) -> Tuple[Optional[str], Optional[int]]:
    """Parse moneyline pick like 'LAL (-150)'."""
    match = re.search(
        r"([A-Z]{2,3})\s*\(([+-]\d+)\)",
        text.upper()
    )
    if match:
        team = match.group(1)
        odds = int(match.group(2))
        return _map_team(team), odds

    # Pattern: TEAM ODDS (without parentheses)
    match = re.search(
        r"([A-Z]{2,3})\s*([+-]\d{3})",
        text.upper()
    )
    if match:
        team = match.group(1)
        odds = int(match.group(2))
        return _map_team(team), odds

    return None, None


def fetch_picks_page(
    context: BrowserContext,
    url: str = PICKS_URL,
    debug: bool = False
) -> Tuple[str, Dict[str, Any]]:
    """Fetch the SportsLine picks page using Playwright with authentication."""
    dbg: Dict[str, Any] = {"url": url}
    page = context.new_page()

    try:
        page.goto(url, wait_until="networkidle", timeout=30000)
        dbg["status"] = "loaded"

        # Wait for React to hydrate
        page.wait_for_timeout(2000)

        # Scroll to load lazy content
        for _ in range(3):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(500)

        content = page.content()
        dbg["content_length"] = len(content)

        return content, dbg

    except PlaywrightTimeoutError as e:
        dbg["error"] = str(e)
        return "", dbg
    finally:
        page.close()


def extract_picks_from_page(
    page: Page,
    observed_at: datetime,
    canonical_url: str,
    market_type: str,
    debug: bool = False
) -> List[RawPickRecord]:
    """
    Extract picks from a SportsLine picks page.

    This is a placeholder implementation that needs to be updated
    once we can observe the actual page structure with games.
    """
    records: List[RawPickRecord] = []

    # SportsLine uses React with styled-components
    # The exact selectors need to be determined from actual page inspection
    # Common patterns to look for:
    # - Game cards with team names, odds, and expert picks
    # - Expert avatars and names with their picks
    # - Tab content for different market types

    # Placeholder: try to extract any visible pick information
    # This will be refined when actual game pages are available

    try:
        # Look for game containers
        game_containers = page.locator("[class*='game'], [class*='matchup'], [class*='pick']")
        count = game_containers.count()

        if debug:
            print(f"[DEBUG] Found {count} potential game containers")

        for i in range(min(count, 50)):  # Limit to prevent infinite loops
            try:
                container = game_containers.nth(i)
                container_text = container.text_content(timeout=1000) or ""

                if len(container_text) < 10:
                    continue

                # Try to parse matchup
                away_team, home_team = _parse_matchup(container_text)

                # Try to parse pick based on market type
                selection = None
                line = None
                odds = None

                if market_type == "spread":
                    selection, line, odds = _parse_spread(container_text)
                elif market_type == "total":
                    selection, line, odds = _parse_total(container_text)
                elif market_type == "moneyline":
                    selection, odds = _parse_moneyline(container_text)

                if not selection and not (away_team and home_team):
                    continue

                # Create raw pick text
                raw_pick_text = f"{selection or ''} {line or ''} {odds or ''}".strip()
                if not raw_pick_text:
                    raw_pick_text = container_text[:100]

                fingerprint_source = f"sportsline|nba_picks|{canonical_url}|{raw_pick_text}|{i}"

                record = RawPickRecord(
                    source_id="sportsline",
                    source_surface=f"sportsline_nba_{market_type}",
                    sport="NBA",
                    market_family="standard",
                    observed_at_utc=observed_at.isoformat(),
                    canonical_url=canonical_url,
                    raw_pick_text=raw_pick_text,
                    raw_block=container_text[:500],
                    raw_fingerprint=sha256_digest(fingerprint_source),
                    matchup_hint=f"{away_team}@{home_team}" if away_team and home_team else None,
                    away_team=away_team,
                    home_team=home_team,
                    market_type=market_type,
                    selection=selection,
                    line=line,
                    odds=odds,
                )
                records.append(record)

                if debug and len(records) <= 3:
                    print(f"[DEBUG] Pick {i}: {raw_pick_text[:50]}")

            except Exception as e:
                if debug:
                    print(f"[DEBUG] Error extracting pick {i}: {e}")
                continue

    except Exception as e:
        if debug:
            print(f"[DEBUG] Error in extract_picks_from_page: {e}")

    return records


def ingest_sportsline_nba(
    storage_state: str = DEFAULT_STORAGE_STATE,
    debug: bool = False,
    picks_url: str = PICKS_URL,
) -> Tuple[List[RawPickRecord], Dict[str, Any]]:
    """
    Main ingestion function for SportsLine NBA picks.

    Args:
        storage_state: Path to Playwright storage state JSON with auth cookies
        debug: Enable debug output

    Returns:
        Tuple of (raw_records, debug_info)
    """
    if sync_playwright is None:
        raise RuntimeError("playwright is not installed. Install with `pip install playwright` then `playwright install chromium`.")

    if not os.path.exists(storage_state):
        raise FileNotFoundError(
            f"Storage state not found at {storage_state}. "
            "Run the login script first to create authentication state."
        )

    observed_at = datetime.now(timezone.utc)
    all_records: List[RawPickRecord] = []
    dbg: Dict[str, Any] = {
        "storage_state": storage_state,
        "observed_at": observed_at.isoformat(),
        "markets": {},
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            storage_state=storage_state,
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )

        try:
            page = context.new_page()
            page.goto(picks_url, wait_until="networkidle", timeout=30000)

            # Wait for React hydration
            page.wait_for_timeout(3000)

            # Check if we're logged in by looking for member content
            is_logged_in = page.locator("text=Log Out, text=Sign Out, [class*='user'], [class*='member']").count() > 0
            dbg["logged_in"] = is_logged_in

            if debug:
                print(f"[DEBUG] Logged in: {is_logged_in}")

            # Try to extract picks for each market type
            for market_type, tab_text in MARKET_TABS.items():
                market_records: List[RawPickRecord] = []

                try:
                    # Try to click the market tab
                    tab = page.locator(f"text={tab_text}").first
                    if tab.count() > 0:
                        tab.click()
                        page.wait_for_timeout(1500)

                        if debug:
                            print(f"[DEBUG] Switched to {market_type} tab")

                    # Extract picks
                    records = extract_picks_from_page(
                        page=page,
                        observed_at=observed_at,
                        canonical_url=picks_url,
                        market_type=market_type,
                        debug=debug
                    )
                    market_records.extend(records)

                except Exception as e:
                    if debug:
                        print(f"[DEBUG] Error processing {market_type}: {e}")
                    dbg["markets"][market_type] = {"error": str(e)}
                    continue

                dbg["markets"][market_type] = {"count": len(market_records)}
                all_records.extend(market_records)

            page.close()

        finally:
            context.close()
            browser.close()

    dbg["total_records"] = len(all_records)
    return all_records, dbg


def main():
    parser = argparse.ArgumentParser(description="Ingest SportsLine picks")
    parser.add_argument(
        "--sport",
        choices=["NBA", "NCAAB"],
        default="NBA",
        help="Sport to ingest (default: NBA)",
    )
    parser.add_argument(
        "--storage",
        default=DEFAULT_STORAGE_STATE,
        help="Path to Playwright storage state JSON"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output"
    )
    parser.add_argument(
        "--create-storage",
        action="store_true",
        help="Launch browser to create storage state (manual login)"
    )
    args = parser.parse_args()

    if args.create_storage:
        # Interactive login to create storage state
        if sync_playwright is None:
            print("Error: playwright is not installed")
            return

        print("Opening browser for manual login...")
        print("After logging in, press Enter in this terminal to save the session.")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context()
            page = context.new_page()
            page.goto("https://www.sportsline.com/login/")

            input("Press Enter after logging in to save session...")

            os.makedirs(os.path.dirname(args.storage) or ".", exist_ok=True)
            context.storage_state(path=args.storage)
            browser.close()

        print(f"Saved storage state to {args.storage}")
        return

    # Run ingestion
    sport = args.sport
    sport_suffix = sport.lower()
    picks_url = PICKS_URLS.get(sport, PICKS_URL)

    try:
        raw_records, dbg = ingest_sportsline_nba(
            storage_state=args.storage,
            debug=args.debug,
            picks_url=picks_url,
        )

        # Write raw output
        ensure_out_dir()
        raw_path = os.path.join(OUT_DIR, f"raw_sportsline_{sport_suffix}.json")
        write_records(raw_path, raw_records)

        print(f"[INGEST] Wrote {len(raw_records)} raw records to {raw_path}")

        if args.debug:
            print(f"[DEBUG] Markets: {dbg.get('markets', {})}")

        # Normalize records
        from src.normalizer_sportsline_nba import normalize_sportsline_records
        normalized = normalize_sportsline_records(raw_records, debug=args.debug)

        norm_path = os.path.join(OUT_DIR, f"normalized_sportsline_{sport_suffix}.json")
        write_json(norm_path, normalized)

        eligible = sum(1 for r in normalized if r.get("eligible_for_consensus"))
        print(f"[INGEST] Wrote {len(normalized)} normalized records ({eligible} eligible) to {norm_path}")

    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("\nTo create storage state, run:")
        print(f"  python3 {__file__} --create-storage --storage {args.storage}")
        return 1
    except Exception as e:
        print(f"Error during ingestion: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
