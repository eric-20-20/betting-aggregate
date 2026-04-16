"""OddsTrader AI picks scraper.

Scrapes OddsTrader's AI model predictions for NBA and NCAAB.
Extracts star-rated picks with EV and cover probability from DOM elements,
cross-referenced with __INITIAL_STATE__ for team/event metadata.

Usage:
    python3 oddstrader_ingest.py                  # NBA (default)
    python3 oddstrader_ingest.py --sport NCAAB     # College basketball
    python3 oddstrader_ingest.py --min-stars 4     # Only 4+ star picks
    python3 oddstrader_ingest.py --debug           # Verbose output
"""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from playwright.sync_api import Page, sync_playwright

from action_ingest import dedupe_normalized_bets, json_default, sha256_digest
from store import NBA_SPORT, NCAAB_SPORT, MLB_SPORT

OUT_DIR = os.getenv("NBA_OUT_DIR", "out")

URLS = {
    NBA_SPORT: "https://www.oddstrader.com/nba/picks/",
    NCAAB_SPORT: "https://www.oddstrader.com/ncaa-college-basketball/picks/",
    MLB_SPORT: "https://www.oddstrader.com/mlb/picks/",
}

PROP_URLS = {
    NBA_SPORT: "https://www.oddstrader.com/nba/player-props/",
    NCAAB_SPORT: "https://www.oddstrader.com/ncaa-college-basketball/player-props/",
}

LEAGUE_IDS = {
    NBA_SPORT: "5",
    NCAAB_SPORT: "14",
}

# Minimum star rating to be eligible for consensus (0-5 scale).
MIN_STARS = 3

# Minimum EV% for prop picks to be scraped (green bar threshold).
MIN_PROP_EV = 15.0

# Map OddsTrader prop type names → our canonical stat_key.
PROP_TYPE_MAP = {
    "Points": "points",
    "Rebounds": "rebounds",
    "Assists": "assists",
    "Steals": "steals",
    "Blocks": "blocks",
    "Three Pointers": "threes",
    "Turnovers": "turnovers",
    "Points + Rebounds + Assists": "pts_reb_ast",
    "Points + Assists + Rebounds": "pts_reb_ast",
    "Points + Rebounds": "pts_reb",
    "Points + Assists": "pts_ast",
    "Assists + Rebounds": "reb_ast",
    "Rebounds + Assists": "reb_ast",
    "Steals + Blocks": "stl_blk",
}

# JS code to extract pick data from the DOM.
# Returns: [{stars, marketType, ev, coverProb, bestLine, matchup, homeTeam,
#            awayTeam, gameTime, teamLogoUUID, totalDirection}]
EXTRACT_PICKS_JS = """() => {
    const results = [];
    const ratings = document.querySelectorAll('[class*="Ratingstyles__RatingContainer"]');

    ratings.forEach((rating) => {
        const filled = rating.querySelectorAll('[class*="ot-full-star"]').length;
        const half = rating.querySelectorAll('[class*="ot-half-star"]').length;

        // Walk up to find the pick row containing EV/line data
        let row = rating;
        let rowText = '';
        for (let i = 0; i < 12; i++) {
            row = row.parentElement;
            if (!row) break;
            const text = row.innerText || '';
            if (text.includes('EV') && text.length < 600) {
                rowText = text;
                break;
            }
        }

        // Extract team logo UUID or over/under direction
        let teamLogoUUID = '';
        let totalDirection = '';
        if (row) {
            const teamImg = row.querySelector('img[alt="team"]');
            const overImg = row.querySelector('img[alt="over"]');
            const underImg = row.querySelector('img[alt="under"]');
            if (teamImg) {
                const src = teamImg.getAttribute('src') || '';
                const match = src.match(/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/);
                if (match) teamLogoUUID = match[1];
            }
            if (overImg) totalDirection = 'over';
            if (underImg) totalDirection = 'under';
        }

        // Walk up further to find the game/event context
        let gameEl = row || rating;
        let matchup = '';
        let homeTeam = '';
        let awayTeam = '';
        let gameTime = '';
        for (let i = 0; i < 20; i++) {
            gameEl = gameEl.parentElement;
            if (!gameEl) break;
            const text = gameEl.innerText || '';
            if (text.includes('@') && text.includes(':')) {
                const lines = text.split('\\n');
                for (const line of lines) {
                    const matchupMatch = line.match(/([A-Z]{2,5})\\s*@\\s*([A-Z]{2,5})/);
                    if (matchupMatch) {
                        awayTeam = matchupMatch[1];
                        homeTeam = matchupMatch[2];
                        matchup = line.trim();
                    }
                    const timeMatch = line.match(/(\\w{3}\\s+)?\\d{2}\\/\\d{2}\\s+\\d{1,2}:\\d{2}\\s*(AM|PM)/i);
                    if (timeMatch) {
                        gameTime = line.trim();
                    }
                }
                if (matchup) break;
            }
        }

        // Parse the row text
        const lines = (rowText || '').split('\\n').filter(l => l.trim());
        let marketType = '';
        let ev = '';
        let coverProb = '';
        let bestLine = '';

        for (const line of lines) {
            const trimmed = line.trim();
            if (['Money', 'Spread', 'Total'].includes(trimmed)) {
                marketType = trimmed;
            } else if (trimmed.match(/^[+-]?\\d+\\.?\\d*%$/) || trimmed === '0.0%') {
                if (!ev) ev = trimmed;
                else coverProb = trimmed;
            } else if (trimmed.match(/^[ou]?\\d/) || trimmed.match(/^[+-]\\d/) || trimmed.match(/^-\\d+$/)) {
                bestLine = trimmed;
            } else if (trimmed.match(/^\\(-?\\d+\\)$/)) {
                bestLine = bestLine + ' ' + trimmed;
            }
        }

        results.push({
            stars: filled + half * 0.5,
            marketType,
            ev,
            coverProb,
            bestLine: bestLine.trim(),
            matchup,
            homeTeam,
            awayTeam,
            gameTime,
            teamLogoUUID,
            totalDirection
        });
    });
    return results;
}"""


def _build_uuid_to_team(state: Dict[str, Any], lid: str) -> Dict[str, str]:
    """Build a mapping from team logo UUID to team abbreviation."""
    uuid_map: Dict[str, str] = {}
    events = state.get("picks", {}).get("events", {}).get(lid, [])
    for evt in events:
        for part in evt.get("participants", []):
            source = part.get("source", {})
            logo = source.get("imageurl", "")
            abbr = source.get("abbr", "")
            if logo and abbr:
                match = re.search(
                    r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
                    logo,
                )
                if match:
                    uuid_map[match.group(1)] = abbr
    return uuid_map


def _build_event_lookup(
    state: Dict[str, Any], lid: str
) -> Dict[str, Dict[str, Any]]:
    """Build a matchup-key → event metadata lookup from __INITIAL_STATE__."""
    lookup: Dict[str, Dict[str, Any]] = {}
    events = state.get("picks", {}).get("events", {}).get(lid, [])
    for evt in events:
        parts = evt.get("participants", [])
        home = away = None
        for p in parts:
            if p.get("ih"):
                home = p.get("source", {}).get("abbr")
            else:
                away = p.get("source", {}).get("abbr")
        if home and away:
            key = f"{away}@{home}"
            dt_ms = evt.get("dt", 0)
            event_time = (
                datetime.fromtimestamp(dt_ms / 1000, tz=timezone.utc) if dt_ms else None
            )
            lookup[key] = {
                "eid": evt.get("eid"),
                "des": evt.get("des", key),
                "slg": evt.get("slg", ""),
                "event_time_utc": event_time.isoformat() if event_time else None,
                "home_team": home,
                "away_team": away,
            }
    return lookup


def _parse_line(best_line: str) -> Tuple[Optional[float], Optional[int]]:
    """Parse a best line string like '+4½ (-108)' into (line, odds)."""
    line_val = None
    odds_val = None

    # Extract odds from parentheses
    odds_match = re.search(r"\(([+-]?\d+)\)", best_line)
    if odds_match:
        odds_val = int(odds_match.group(1))

    # Extract line value (handle ½ fractions)
    line_part = re.sub(r"\([^)]+\)", "", best_line).strip()
    # Remove o/u prefix for totals
    line_part = re.sub(r"^[ou]", "", line_part)
    if line_part:
        line_part = line_part.replace("½", ".5")
        try:
            line_val = float(line_part)
        except ValueError:
            pass

    return line_val, odds_val


def _parse_ev(ev_str: str) -> Optional[float]:
    """Parse EV string like '+2.6%' to float 2.6."""
    if not ev_str:
        return None
    try:
        return float(ev_str.replace("%", "").replace("+", ""))
    except ValueError:
        return None


def _parse_cover_prob(cp_str: str) -> Optional[float]:
    """Parse cover probability string like '57%' to float 57.0."""
    if not cp_str:
        return None
    try:
        return float(cp_str.replace("%", ""))
    except ValueError:
        return None


def _market_type_from_dom(dom_type: str) -> Optional[str]:
    """Map DOM market type text to our market type."""
    return {"Money": "moneyline", "Spread": "spread", "Total": "total"}.get(dom_type)


def _parse_game_time(game_time_str: str) -> Optional[str]:
    """Parse DOM gameTime like 'Tue 02/18 7:00 PM' to UTC ISO string.

    OddsTrader displays game times in US Eastern. Format variants:
      'Tue 02/18 7:00 PM'
      '02/18 7:00 PM'
    """
    if not game_time_str:
        return None
    # Strip optional leading day-of-week
    s = re.sub(r"^[A-Za-z]{3}\s+", "", game_time_str.strip())
    # Expected: "MM/DD H:MM AM/PM"
    m = re.match(r"(\d{1,2})/(\d{1,2})\s+(\d{1,2}):(\d{2})\s*(AM|PM)", s, re.IGNORECASE)
    if not m:
        return None
    month, day = int(m.group(1)), int(m.group(2))
    hour, minute = int(m.group(3)), int(m.group(4))
    ampm = m.group(5).upper()
    if ampm == "PM" and hour != 12:
        hour += 12
    elif ampm == "AM" and hour == 12:
        hour = 0
    # Use current year
    year = datetime.now().year
    try:
        eastern = ZoneInfo("America/New_York")
        dt_eastern = datetime(year, month, day, hour, minute, tzinfo=eastern)
        dt_utc = dt_eastern.astimezone(timezone.utc)
        return dt_utc.isoformat()
    except (ValueError, OverflowError):
        return None


def scrape_picks(
    url: str, sport: str, debug: bool = False
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Load page, extract __INITIAL_STATE__ and scrape DOM picks.

    Returns (raw_picks, state_metadata).
    """
    lid = LEAGUE_IDS.get(sport, "5")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1400, "height": 900},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )
        page = context.new_page()
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(6000)  # wait for dynamic content to render

            # 1. Extract __INITIAL_STATE__ for metadata
            state_json = page.evaluate(
                "() => JSON.stringify(window.__INITIAL_STATE__)"
            )
            state = json.loads(state_json) if state_json else {}
            uuid_to_team = _build_uuid_to_team(state, lid)
            event_lookup = _build_event_lookup(state, lid)

            if debug:
                print(f"[DEBUG] UUID→team map: {len(uuid_to_team)} teams")
                print(f"[DEBUG] Event lookup: {len(event_lookup)} games")

            # 2. Scrape DOM for AI picks
            dom_picks = page.evaluate(EXTRACT_PICKS_JS)
            if debug:
                print(f"[DEBUG] DOM picks scraped: {len(dom_picks)}")

            # 3. Convert DOM picks to raw pick records
            raw_picks = []
            for dp in dom_picks:
                market_type = _market_type_from_dom(dp.get("marketType", ""))
                if not market_type:
                    continue

                home_team = dp.get("homeTeam", "")
                away_team = dp.get("awayTeam", "")

                # Get event metadata from __INITIAL_STATE__
                # Try both orientations since DOM display order may not match
                evt_meta = (
                    event_lookup.get(f"{away_team}@{home_team}")
                    or event_lookup.get(f"{home_team}@{away_team}")
                    or {}
                )
                # Use the state's canonical team assignments if available
                if evt_meta:
                    home_team = evt_meta.get("home_team", home_team)
                    away_team = evt_meta.get("away_team", away_team)

                # Determine selection (which team / direction)
                if market_type == "total":
                    selection = "game_total"
                    # Use totalDirection from DOM (over/under icon alt text)
                    side = dp.get("totalDirection", "")
                    if not side:
                        # Fallback: parse from best line prefix (o/u)
                        bl = dp.get("bestLine", "")
                        if bl.startswith("o"):
                            side = "over"
                        elif bl.startswith("u"):
                            side = "under"
                        else:
                            if debug:
                                print(
                                    f"[DEBUG] Cannot determine over/under for {away_team}@{home_team}, skipping"
                                )
                            continue
                else:
                    # For spread/moneyline, resolve team from logo UUID
                    logo_uuid = dp.get("teamLogoUUID", "")
                    picked_team = uuid_to_team.get(logo_uuid, "")
                    if not picked_team:
                        if debug:
                            print(
                                f"[DEBUG] Unknown logo UUID {logo_uuid} for {away_team}@{home_team}, skipping"
                            )
                        continue
                    selection = picked_team
                    side = picked_team

                # Parse line and odds
                line_val, odds_val = _parse_line(dp.get("bestLine", ""))
                ev = _parse_ev(dp.get("ev", ""))
                cover_prob = _parse_cover_prob(dp.get("coverProb", ""))
                stars = dp.get("stars", 0)

                event_desc = evt_meta.get("des", f"{away_team}@{home_team}")

                # Build raw pick text
                if market_type == "spread":
                    raw_pick_text = f"{selection} {dp.get('bestLine', '')} | {event_desc}"
                elif market_type == "total":
                    raw_pick_text = f"{dp.get('bestLine', '')} | {event_desc}"
                elif market_type == "moneyline":
                    raw_pick_text = f"{selection} ML {dp.get('bestLine', '')} | {event_desc}"

                # event_time_utc: prefer __INITIAL_STATE__, fall back to DOM gameTime
                event_time_utc = evt_meta.get("event_time_utc")
                if not event_time_utc:
                    event_time_utc = _parse_game_time(dp.get("gameTime", ""))

                raw_picks.append(
                    {
                        "source_id": "oddstrader",
                        "source_surface": f"oddstrader_ai_{market_type}",
                        "sport": sport,
                        "event_id": evt_meta.get("eid"),
                        "event_desc": event_desc,
                        "event_slug": evt_meta.get("slg", ""),
                        "event_time_utc": event_time_utc,
                        "home_team": home_team or evt_meta.get("home_team", ""),
                        "away_team": away_team or evt_meta.get("away_team", ""),
                        "market_type": market_type,
                        "side": side,
                        "selection": selection,
                        "line": line_val,
                        "odds": odds_val,
                        "rating_stars": stars,
                        "ev_pct": ev,
                        "cover_prob": cover_prob,
                        "raw_pick_text": raw_pick_text,
                        "expert_name": "OddsTrader AI",
                    }
                )

            return raw_picks, state

        finally:
            page.close()
            context.close()
            browser.close()


def scrape_props(
    url: str, sport: str, min_ev: float = MIN_PROP_EV,
    max_load_more: int = 30, debug: bool = False,
) -> List[Dict[str, Any]]:
    """Scrape OddsTrader player props page.

    Clicks LOAD MORE until exhausted, then extracts all prop picks.
    Filters to picks with EV >= min_ev (green bar threshold).

    Returns list of raw prop pick dicts.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1400, "height": 900},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )
        page = context.new_page()

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(8000)

            # Click LOAD MORE repeatedly to get all picks
            for _ in range(max_load_more):
                clicked = page.evaluate("""() => {
                    const btns = document.querySelectorAll('button');
                    for (const b of btns) {
                        if (b.innerText.trim() === 'LOAD MORE') {
                            b.click();
                            return true;
                        }
                    }
                    return false;
                }""")
                if not clicked:
                    break
                page.wait_for_timeout(1500)

            # Extract all prop picks from the DOM
            raw_picks = page.evaluate("""() => {
                const rows = document.querySelectorAll('a[class*="betWithBottomRow"]');
                const results = [];

                const propKeywords = ['Points', 'Rebounds', 'Assists', 'Steals', 'Blocks',
                    'Three Pointers', 'Turnovers', 'First Field Goal', 'To score', 'To Record',
                    'Double Double', 'Triple Double'];

                function isPropType(line) {
                    return propKeywords.some(k => line.startsWith(k)) || (line.includes('+') && /^[A-Z]/.test(line));
                }

                for (const row of rows) {
                    const text = row.innerText.trim();
                    const lines = text.split('\\n').map(l => l.trim()).filter(l => l);

                    const fullStars = row.querySelectorAll('.ot-full-star').length;
                    const halfStars = row.querySelectorAll('.ot-half-star').length;

                    let gameTime = '';
                    let matchup = '';
                    let playerName = '';
                    let propType = '';
                    let evPct = '';
                    let coverProb = '';
                    let bestLine = '';

                    for (let i = 0; i < lines.length; i++) {
                        const l = lines[i];
                        if (/^[A-Z]{3}\\s+\\d{2}\\/\\d{2}/.test(l)) gameTime = l;
                        else if (/^[A-Z]{2,5}\\s*@\\s*[A-Z]{2,5}$/.test(l)) matchup = l;
                        else if (l === 'EV') continue;
                        else if (/^[+-]\\d+\\.?\\d*%$/.test(l)) evPct = l;
                        else if (/^\\d+%$/.test(l)) coverProb = l;
                        else if (/^[ou]\\d/.test(l)) bestLine = l;
                        else if (/^\\([+-]?\\d+\\)$/.test(l)) bestLine = bestLine ? bestLine + ' ' + l : l;
                        else if (isPropType(l)) propType = l;
                        else if (!playerName && /^[A-Z][a-z]/.test(l) && !isPropType(l)) playerName = l;
                    }

                    results.push({
                        gameTime, matchup, playerName, propType,
                        evPct, coverProb, bestLine,
                        stars: fullStars + halfStars * 0.5,
                    });
                }
                return results;
            }""")

            if debug:
                print(f"[DEBUG] Props page: {len(raw_picks)} raw picks extracted")

            # Parse and filter by EV threshold
            result = []
            for dp in raw_picks:
                ev = _parse_ev(dp.get("evPct", ""))
                if ev is None or ev < min_ev:
                    continue

                # Parse matchup
                matchup = dp.get("matchup", "")
                away_team = home_team = ""
                m = re.match(r"([A-Z]{2,5})\s*@\s*([A-Z]{2,5})", matchup)
                if m:
                    away_team = m.group(1)
                    home_team = m.group(2)

                # Parse line and odds
                line_val, odds_val = _parse_line(dp.get("bestLine", ""))

                # Direction from line prefix
                bl = dp.get("bestLine", "").strip()
                direction = ""
                if bl.startswith("o"):
                    direction = "over"
                elif bl.startswith("u"):
                    direction = "under"

                # Map prop type to stat_key
                prop_type = dp.get("propType", "")
                stat_key = PROP_TYPE_MAP.get(prop_type)
                if not stat_key:
                    if debug:
                        print(f"[DEBUG] Unknown prop type: {prop_type!r}")
                    continue

                player_name = dp.get("playerName", "").strip()
                if not player_name:
                    continue

                # Build player_key
                player_slug = re.sub(r"[^a-z0-9]+", "_", player_name.lower()).strip("_")
                player_key = f"{sport.lower()}:{player_slug}"

                cover_prob = _parse_cover_prob(dp.get("coverProb", ""))

                event_time_utc = _parse_game_time(dp.get("gameTime", ""))

                result.append({
                    "source_id": "oddstrader",
                    "source_surface": f"oddstrader_ai_player_prop",
                    "sport": sport,
                    "event_time_utc": event_time_utc,
                    "home_team": home_team,
                    "away_team": away_team,
                    "market_type": "player_prop",
                    "stat_key": stat_key,
                    "player_name": player_name,
                    "player_key": player_key,
                    "side": direction,
                    "selection": f"{player_name} {direction} {line_val}" if line_val else player_name,
                    "line": line_val,
                    "odds": odds_val,
                    "rating_stars": dp.get("stars", 0),
                    "ev_pct": ev,
                    "cover_prob": cover_prob,
                    "raw_pick_text": f"{player_name} {prop_type} {dp.get('bestLine', '')} | {matchup}",
                    "expert_name": "OddsTrader AI",
                })

            if debug:
                print(f"[DEBUG] After EV>={min_ev}% filter: {len(result)} props")

            return result

        finally:
            page.close()
            context.close()
            browser.close()


def normalize_pick(
    raw: Dict[str, Any], sport: str, min_stars: int = MIN_STARS
) -> Optional[Dict[str, Any]]:
    """Normalize a raw OddsTrader pick to the standard schema."""
    home_team = raw.get("home_team", "")
    away_team = raw.get("away_team", "")
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

    # event_key must use canonical format NBA:YYYY:MM:DD:AWAY@HOME for cross-source merging.
    # The time-appended format (NBA:20260312:AWAY@HOME:2300) is incompatible with other sources.
    event_key = canonical_event_key

    market_type = raw.get("market_type")
    side = raw.get("side")
    selection = raw.get("selection")

    # For totals: side is "over"/"under"; normalize both to uppercase and use as selection.
    # "game_total" is not a valid selection in the rest of the pipeline.
    if market_type == "total" and side:
        side = side.upper()
        selection = side  # "OVER" or "UNDER"

    fingerprint = sha256_digest(
        f"oddstrader:{raw.get('event_id')}:{market_type}:{side}:{selection}:{raw.get('player_key', '')}"
    )

    # Eligibility
    is_prop = market_type == "player_prop"
    eligible = market_type in ("spread", "total", "moneyline", "player_prop")
    inelig_reason = None
    stars = raw.get("rating_stars", 0)
    if eligible and not is_prop and stars < min_stars:
        eligible = False
        inelig_reason = "rating_below_threshold"
    # Props are pre-filtered by EV in scrape_props(); still mark eligible
    if is_prop and not raw.get("stat_key"):
        eligible = False
        inelig_reason = "unknown_stat_key"

    canonical_url = f"https://www.oddstrader.com/{sport.lower()}/picks/"
    if is_prop:
        canonical_url = f"https://www.oddstrader.com/{sport.lower()}/player-props/"

    return {
        "provenance": {
            "source_id": "oddstrader",
            "source_surface": raw.get("source_surface", "oddstrader_ai"),
            "sport": sport,
            "observed_at_utc": datetime.now(timezone.utc).isoformat(),
            "canonical_url": canonical_url,
            "raw_fingerprint": fingerprint,
            "expert_name": "OddsTrader AI",
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
            "line": raw.get("line"),
            "odds": raw.get("odds"),
            "stat_key": raw.get("stat_key"),
            "player_key": raw.get("player_key"),
            "player_name": raw.get("player_name"),
            "player_id": raw.get("player_key"),
        },
        "eligible_for_consensus": eligible,
        "ineligibility_reason": inelig_reason,
        "eligibility": {
            "eligible_for_consensus": eligible,
            "ineligibility_reason": inelig_reason,
        },
        "oddstrader_meta": {
            "rating_stars": stars,
            "ev_pct": raw.get("ev_pct"),
            "cover_prob": raw.get("cover_prob"),
        },
    }


def ensure_out_dir():
    os.makedirs(OUT_DIR, exist_ok=True)


def write_json_file(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=json_default, ensure_ascii=False)


def main():
    parser = argparse.ArgumentParser(description="Ingest OddsTrader AI picks.")
    parser.add_argument(
        "--sport",
        choices=["NBA", "NCAAB", "MLB"],
        default="NBA",
        help="Sport to ingest (default: NBA)",
    )
    parser.add_argument(
        "--min-stars",
        type=int,
        default=MIN_STARS,
        help=f"Minimum star rating to be eligible (default: {MIN_STARS})",
    )
    parser.add_argument(
        "--props", action="store_true",
        help="Also scrape player props (EV >= 15%% green bar picks).",
    )
    parser.add_argument(
        "--props-only", action="store_true",
        help="Only scrape player props, skip spread/total/moneyline.",
    )
    parser.add_argument(
        "--min-ev",
        type=float,
        default=MIN_PROP_EV,
        help=f"Minimum EV%% for props (default: {MIN_PROP_EV})",
    )
    parser.add_argument(
        "--debug", action="store_true", help="Enable debug logging."
    )
    args = parser.parse_args()

    sport = args.sport
    ensure_out_dir()
    sport_suffix = sport.lower()

    all_normalized = []

    # ── Standard picks (spread/total/moneyline) ──
    if not args.props_only:
        url = URLS.get(sport)
        if not url:
            print(f"[ERROR] No URL configured for sport {sport}")
            return

        print(f"[INGEST] OddsTrader {sport}: fetching {url}")
        raw_picks, state = scrape_picks(url, sport, debug=args.debug)
        print(f"[INGEST] Extracted {len(raw_picks)} raw picks")

        min_stars = args.min_stars
        normalized = []
        for raw in raw_picks:
            norm = normalize_pick(raw, sport, min_stars=min_stars)
            if norm:
                normalized.append(norm)

        normalized = dedupe_normalized_bets(normalized)

        raw_path = os.path.join(OUT_DIR, f"raw_oddstrader_{sport_suffix}.json")
        norm_path = os.path.join(OUT_DIR, f"normalized_oddstrader_{sport_suffix}.json")
        write_json_file(raw_path, raw_picks)
        write_json_file(norm_path, normalized)

        eligible = [n for n in normalized if n.get("eligible_for_consensus")]
        filtered = len(normalized) - len(eligible)

        star_counts: Dict[float, int] = {}
        for raw in raw_picks:
            s = raw.get("rating_stars", 0)
            star_counts[s] = star_counts.get(s, 0) + 1
        dist = " | ".join(f"{s}*:{c}" for s, c in sorted(star_counts.items()))

        print(f"[INGEST] Stars: {dist}")
        print(
            f"[INGEST] Wrote {len(normalized)} normalized "
            f"({len(eligible)} eligible >= {min_stars}*, "
            f"{filtered} below threshold) to {norm_path}"
        )
        all_normalized.extend(normalized)

    # ── Player props ──
    if args.props or args.props_only:
        prop_url = PROP_URLS.get(sport)
        if not prop_url:
            print(f"[ERROR] No prop URL configured for sport {sport}")
            return

        print(f"\n[INGEST] OddsTrader {sport} props: fetching {prop_url}")
        raw_props = scrape_props(prop_url, sport, min_ev=args.min_ev, debug=args.debug)
        print(f"[INGEST] Extracted {len(raw_props)} raw props (EV >= {args.min_ev}%)")

        prop_normalized = []
        for raw in raw_props:
            norm = normalize_pick(raw, sport)
            if norm:
                prop_normalized.append(norm)

        prop_normalized = dedupe_normalized_bets(prop_normalized)

        raw_prop_path = os.path.join(OUT_DIR, f"raw_oddstrader_prop_{sport_suffix}.json")
        norm_prop_path = os.path.join(OUT_DIR, f"normalized_oddstrader_prop_{sport_suffix}.json")
        write_json_file(raw_prop_path, raw_props)
        write_json_file(norm_prop_path, prop_normalized)

        eligible_props = [n for n in prop_normalized if n.get("eligible_for_consensus")]

        # Star distribution for props
        prop_star_counts: Dict[float, int] = {}
        for raw in raw_props:
            s = raw.get("rating_stars", 0)
            prop_star_counts[s] = prop_star_counts.get(s, 0) + 1
        prop_dist = " | ".join(f"{s}*:{c}" for s, c in sorted(prop_star_counts.items()))

        # Stat key distribution
        stat_counts: Dict[str, int] = {}
        for raw in raw_props:
            sk = raw.get("stat_key", "unknown")
            stat_counts[sk] = stat_counts.get(sk, 0) + 1
        stat_dist = " | ".join(f"{k}:{v}" for k, v in sorted(stat_counts.items(), key=lambda x: -x[1]))

        print(f"[INGEST] Props stars: {prop_dist}")
        print(f"[INGEST] Props stats: {stat_dist}")
        print(
            f"[INGEST] Wrote {len(prop_normalized)} normalized props "
            f"({len(eligible_props)} eligible) to {norm_prop_path}"
        )
        all_normalized.extend(prop_normalized)


if __name__ == "__main__":
    main()
