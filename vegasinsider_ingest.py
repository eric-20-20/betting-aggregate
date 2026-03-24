"""VegasInsider NBA + NCAAB picks scraper.

Scrapes VegasInsider's free and premium picks from expert analysts.
The page at https://www.vegasinsider.com/picks/ contains both NBA and NCAAB
sections; use --sport to select which sport to ingest.

Usage:
    python3 vegasinsider_ingest.py                       # NBA (default)
    python3 vegasinsider_ingest.py --sport NCAAB         # NCAAB picks
    python3 vegasinsider_ingest.py --debug               # verbose output
    python3 vegasinsider_ingest.py --snapshot            # save page HTML for selector dev
    python3 vegasinsider_ingest.py --featured-only       # skip premium section
    python3 vegasinsider_ingest.py --create-storage      # open browser to log in
"""

from __future__ import annotations

import argparse
import json
import os
import re
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from playwright.sync_api import Page, sync_playwright

from action_ingest import dedupe_normalized_bets, json_default, sha256_digest
from store import get_data_store, write_json, NBA_SPORT, NCAAB_SPORT
from utils import normalize_text

OUT_DIR = os.getenv("NBA_OUT_DIR", "out")

SOURCE_ID = "vegasinsider"
PICKS_URL = "https://www.vegasinsider.com/picks/"
DEFAULT_STORAGE_STATE = "data/vegasinsider_storage_state.json"

# Market tag text → canonical market_type
MARKET_TAG_MAP = {
    "spread": "spread",
    "total": "total",
    "moneyline": "moneyline",
    "money line": "moneyline",
    "milestones": "player_prop",
}

# Milestone stat text → canonical stat_key
MILESTONE_STAT_MAP = {
    "points": "points",
    "rebounds": "rebounds",
    "assists": "assists",
    "steals": "steals",
    "blocks": "blocks",
    "threes": "threes",
    "three-pointers": "threes",
    "3-pointers": "threes",
    "three pointers": "threes",
}

# Team nickname fallback — NBA
TEAM_FALLBACK_NBA = {
    "bucks": "MIL", "76ers": "PHI", "sixers": "PHI", "knicks": "NYK",
    "nets": "BKN", "trail blazers": "POR", "blazers": "POR", "suns": "PHX",
    "spurs": "SAS", "warriors": "GSW", "pelicans": "NOP", "lakers": "LAL",
    "clippers": "LAC", "celtics": "BOS", "heat": "MIA", "bulls": "CHI",
    "cavaliers": "CLE", "cavs": "CLE", "pistons": "DET", "raptors": "TOR",
    "mavericks": "DAL", "mavs": "DAL", "wolves": "MIN", "timberwolves": "MIN",
    "thunder": "OKC", "magic": "ORL", "rockets": "HOU", "jazz": "UTA",
    "kings": "SAC", "hawks": "ATL", "hornets": "CHA", "grizzlies": "MEM",
    "nuggets": "DEN", "pacers": "IND", "wizards": "WAS",
}

# Team nickname fallback — NCAAB (common nicknames that may not be in data store)
TEAM_FALLBACK_NCAAB = {
    "tar heels": "UNC", "heels": "UNC",
    "blue devils": "DUKE", "devils": "DUKE",
    "wildcats": "UK",  # Kentucky is most common; overridden by data store when unambiguous
    "hoosiers": "IND",
    "wolverines": "MICH",
    "buckeyes": "OSU",
    "longhorns": "TEX",
    "jayhawks": "KAN",
    "zags": "GONZ", "bulldogs": "GONZ",
    "orangemen": "SYR", "orange": "SYR",
    "seminoles": "FSU",
    "gators": "FLA",
    "volunteers": "TENN", "vols": "TENN",
    "aggies": "TXAM",
    "sooners": "OKLA",
    "cowboys": "OKST",
    "mountaineers": "WVU",
    "hokies": "VT",
    "cavaliers": "UVA",
    "demon deacons": "WAKE",
    "cardinals": "LOU",
    "golden eagles": "MU",
    "golden flashes": "KENT",
    "panthers": "PITT",
    "tigers": "CLEM",
    "razorbacks": "ARK",
    "huskers": "NEB",
    "hawkeyes": "IOWA",
    "badgers": "WISC",
    "spartans": "MICH-ST",
    "illini": "ILL",
    "boilermakers": "PUR",
    "nittany lions": "PSU",
    "terrapins": "MD",
    "hurricanes": "MIA-FL",
    "eagles": "BC",
    "scarlet knights": "RUT",
    "yellow jackets": "GT",
    "crimson tide": "ALA",
    "auburn tigers": "AUB",
    "rebels": "MISS",
    "bulldogs": "MS-ST",
    "golden bears": "CAL",
    "trojans": "USC",
    "bruins": "UCLA",
    "ducks": "ORE",
    "beavers": "ORE-ST",
    "huskies": "WASH",
    "cougars": "WASH-ST",
    "utes": "UTAH",
    "lobos": "NM",
    "wolf pack": "NEV",
    "mountain hawks": "LEHIGH",
}

# Active fallback — set per-invocation based on sport
TEAM_FALLBACK: Dict[str, str] = TEAM_FALLBACK_NBA


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
    expert_slug: Optional[str] = None
    expert_handle: Optional[str] = None
    expert_profile: Optional[str] = None
    market_type: Optional[str] = None
    selection: Optional[str] = None
    side: Optional[str] = None
    line: Optional[float] = None
    odds: Optional[int] = None
    player_name: Optional[str] = None
    stat_key: Optional[str] = None
    section: Optional[str] = None
    analysis_text: Optional[str] = None


# ── Team mapping ──────────────────────────────────────────────────────────


def _map_team(alias: Optional[str], store=None) -> Optional[str]:
    """Map team name or abbreviation to standard team code."""
    if not alias:
        return None
    if store is None:
        store = get_data_store(NBA_SPORT)
    codes = store.lookup_team_code(alias)
    if len(codes) == 1:
        return next(iter(codes))
    norm = normalize_text(alias)
    if norm in TEAM_FALLBACK:
        return TEAM_FALLBACK[norm]
    # Try short abbreviations
    if alias and len(alias) <= 5:
        return alias.upper()
    return None


# ── Time parsing ──────────────────────────────────────────────────────────


def _parse_game_time(date_str: str, time_str: str, ampm: str) -> Optional[str]:
    """Parse 'MM/DD' + 'H:MM' + 'PM' in ET to UTC ISO string."""
    try:
        month, day = map(int, date_str.split("/"))
        hour, minute = map(int, time_str.split(":"))
        ampm = ampm.upper()
        if ampm == "PM" and hour != 12:
            hour += 12
        elif ampm == "AM" and hour == 12:
            hour = 0
        year = datetime.now().year
        eastern = ZoneInfo("America/New_York")
        dt_eastern = datetime(year, month, day, hour, minute, tzinfo=eastern)
        dt_utc = dt_eastern.astimezone(timezone.utc)
        return dt_utc.isoformat()
    except (ValueError, OverflowError):
        return None


# ── Pick headline parsing ─────────────────────────────────────────────────


# Total: "Utah Jazz @ Memphis Grizzlies o236 (-110)"
TOTAL_RE = re.compile(
    r"(.+?)\s*@\s*(.+?)\s+([ou])(\d+(?:\.\d+)?)\s*\(([+-]\d+)\)",
    re.IGNORECASE,
)

# Spread: "Cleveland Cavaliers -4.5 (-110)" or "CLE +6.5 (-110)"
SPREAD_RE = re.compile(
    r"(.+?)\s+([+-]\d+(?:\.\d+)?)\s*\(([+-]\d+)\)",
)

# Moneyline: "Cleveland Cavaliers (-150)"
ML_RE = re.compile(
    r"(.+?)\s*\(([+-]\d+)\)\s*$",
)

# Milestone: "Miller 30+ Points (+300)"
MILESTONE_RE = re.compile(
    r"(.+?)\s+(\d+)\+\s*(Points|Rebounds|Assists|Steals|Blocks|Threes|Three[- ]?Pointers|3[- ]?Pointers)\s*\(([+-]\d+)\)",
    re.IGNORECASE,
)

# Standard O/U player prop: "Jarrett Allen o15.5 Points (-110)"
PROP_OU_RE = re.compile(
    r"(.+?)\s+([ou])(\d+(?:\.\d+)?)\s+(Points|Rebounds|Assists|Steals|Blocks|Threes|Three[- ]?Pointers|3[- ]?Pointers)\s*\(([+-]\d+)\)",
    re.IGNORECASE,
)


def _parse_headline(headline: str, market_type: Optional[str]) -> Dict[str, Any]:
    """Parse a pick headline into structured fields based on market type."""
    result: Dict[str, Any] = {}

    if market_type == "player_prop":
        # Try standard O/U first: "Jarrett Allen o15.5 Points (-110)"
        m = PROP_OU_RE.search(headline)
        if m:
            result["player_name"] = m.group(1).strip()
            result["side"] = "OVER" if m.group(2).lower() == "o" else "UNDER"
            result["line"] = float(m.group(3))
            stat_text = m.group(4).strip().lower()
            result["stat_key"] = MILESTONE_STAT_MAP.get(stat_text)
            result["odds"] = int(m.group(5))
            return result

        # Try milestone: "Miller 30+ Points (+300)"
        m = MILESTONE_RE.search(headline)
        if m:
            result["player_name"] = m.group(1).strip()
            threshold = int(m.group(2))
            result["line"] = threshold - 0.5  # "30+" means >= 30 → line 29.5 OVER
            result["side"] = "OVER"
            stat_text = m.group(3).strip().lower()
            result["stat_key"] = MILESTONE_STAT_MAP.get(stat_text)
            result["odds"] = int(m.group(4))
        return result

    if market_type == "total":
        m = TOTAL_RE.search(headline)
        if m:
            result["away_team_raw"] = m.group(1).strip()
            result["home_team_raw"] = m.group(2).strip()
            result["side"] = "OVER" if m.group(3).lower() == "o" else "UNDER"
            result["line"] = float(m.group(4))
            result["odds"] = int(m.group(5))
        return result

    if market_type == "spread":
        m = SPREAD_RE.search(headline)
        if m:
            result["team_raw"] = m.group(1).strip()
            result["line"] = float(m.group(2))
            result["odds"] = int(m.group(3))
        return result

    if market_type == "moneyline":
        m = ML_RE.search(headline)
        if m:
            result["team_raw"] = m.group(1).strip()
            result["odds"] = int(m.group(2))
        return result

    # Unknown market — try total pattern first (most distinctive)
    m = TOTAL_RE.search(headline)
    if m:
        result["away_team_raw"] = m.group(1).strip()
        result["home_team_raw"] = m.group(2).strip()
        result["side"] = "OVER" if m.group(3).lower() == "o" else "UNDER"
        result["line"] = float(m.group(4))
        result["odds"] = int(m.group(5))
        result["inferred_market"] = "total"
        return result

    # Try standard O/U prop
    m = PROP_OU_RE.search(headline)
    if m:
        result["player_name"] = m.group(1).strip()
        result["side"] = "OVER" if m.group(2).lower() == "o" else "UNDER"
        result["line"] = float(m.group(3))
        stat_text = m.group(4).strip().lower()
        result["stat_key"] = MILESTONE_STAT_MAP.get(stat_text)
        result["odds"] = int(m.group(5))
        result["inferred_market"] = "player_prop"
        return result

    # Try milestone
    m = MILESTONE_RE.search(headline)
    if m:
        result["player_name"] = m.group(1).strip()
        threshold = int(m.group(2))
        result["line"] = threshold - 0.5
        result["side"] = "OVER"
        stat_text = m.group(3).strip().lower()
        result["stat_key"] = MILESTONE_STAT_MAP.get(stat_text)
        result["odds"] = int(m.group(4))
        result["inferred_market"] = "player_prop"
        return result

    # Try spread
    m = SPREAD_RE.search(headline)
    if m:
        result["team_raw"] = m.group(1).strip()
        result["line"] = float(m.group(2))
        result["odds"] = int(m.group(3))
        result["inferred_market"] = "spread"
        return result

    return result


# ── JS extraction code ────────────────────────────────────────────────────

# Extract structured pick data using VegasInsider's actual DOM classes.
# Key classes discovered from page snapshot:
#   .module.pick         — pick card container
#   .pick-info-title     — headline h2 (e.g. "Utah Jazz @ Memphis Grizzlies o236 (-110)")
#   .pick-tags-type      — market tag (e.g. "Total", "Spread", "Points milestones")
#   .pick-expert-name    — expert name (may include record suffix)
#   .pick-pill-totalspread — line value from odds badge
#   .pick-pill-odds      — odds from badge
#   .pick-header-matchup — game matchup header (in parent container)
#   .pick-analysis-text  — analysis text
def _make_extract_picks_js(sport: str) -> str:
    """Build the JS extraction script filtered to the given sport (NBA or NCAAB)."""
    sport_tag = sport.upper()  # 'NBA' or 'NCAAB'
    return f"""() => {{
    const picks = [];
    const sportTag = '{sport_tag}';
    // All sport tags that are NOT the target sport — any card tagged with one of these is skipped.
    const NON_TARGET_SPORTS = ['NHL','NFL','MLB','NCAAB','NCAAF','CFB','CBB','Soccer','MLS','NBA','NCAAB'].filter(s => s !== sportTag);
    const ALL_SPORT_TAGS = new Set(['NHL','NFL','MLB','NCAAB','NCAAF','CFB','CBB','Soccer','MLS','NBA','NCAAB']);
    const cards = document.querySelectorAll('.module.pick');

    cards.forEach(card => {{
        // 1. Headline
        const titleEl = card.querySelector('.pick-info-title');
        const headline = titleEl ? titleEl.innerText.trim() : '';

        // Skip locked/placeholder picks
        if (headline.includes('My Pick Title')) return;

        // 2. Market tags — check sport label first
        const tagEls = card.querySelectorAll('.pick-tags-type');
        const allTags = [];
        tagEls.forEach(t => {{
            const text = t.innerText.trim();
            if (text) allTags.push(text);
        }});

        // Determine which sport this card belongs to.
        // Skip if tagged with ANY known sport that isn't the target sport.
        // A card with no sport tag at all is assumed to belong to the target sport
        // (featured free picks sometimes lack explicit sport tags).
        const hasTargetTag = allTags.some(t => t === sportTag);
        const hasNonTargetSportTag = allTags.some(t => ALL_SPORT_TAGS.has(t) && t !== sportTag);
        if (hasNonTargetSportTag && !hasTargetTag) return;

        // Strip all sport tags from the list passed downstream
        const tags = allTags.filter(t => !ALL_SPORT_TAGS.has(t));

        // 3. Expert name (may have record appended like "Stephen Nover11-10-1 NBA Last 14 Days")
        const expertEl = card.querySelector('.pick-expert-name');
        const expertRaw = expertEl ? expertEl.innerText.trim() : '';

        // 4. Odds badge values
        const pillLine = card.querySelector('.pick-pill-totalspread');
        const pillOdds = card.querySelector('.pick-pill-odds');
        const badgeLine = pillLine ? pillLine.innerText.trim() : '';
        const badgeOdds = pillOdds ? pillOdds.innerText.trim() : '';

        // 5. Analysis text
        const analysisEl = card.querySelector('.pick-analysis-text');
        const analysis = analysisEl ? analysisEl.innerText.trim() : '';

        // 6. Find matchup header by walking up to parent pick-row / container
        let matchupText = '';
        let parent = card;
        for (let i = 0; i < 10; i++) {{
            parent = parent.parentElement;
            if (!parent) break;
            const header = parent.querySelector('.pick-header-matchup');
            if (header) {{
                matchupText = header.innerText.trim();
                break;
            }}
        }}

        // 7. Check if this is a "Free Pick" or premium
        const isFree = tags.some(t => t.toLowerCase().includes('free'));

        picks.push({{
            headline,
            tags,
            expertRaw,
            badgeLine,
            badgeOdds,
            analysis: analysis.substring(0, 500),
            matchupText,
            isFree,
            fullText: card.innerText.trim().substring(0, 800),
        }});
    }});

    return picks;
}}"""

# JS to click all carousel right arrows to reveal hidden picks
CLICK_CAROUSELS_JS = """() => {
    const arrows = document.querySelectorAll('.arrows button, .arrows a');
    let clicked = 0;
    arrows.forEach(btn => {
        try {
            btn.click();
            clicked++;
        } catch(e) {}
    });
    return clicked;
}""".strip()


# ── Scraping ──────────────────────────────────────────────────────────────


def _parse_expert_raw(expert_raw: str) -> Tuple[Optional[str], Optional[str]]:
    """Parse expert name from raw text, stripping record suffix.

    Input examples:
        "Bet Labs"
        "Stephen Nover11-10-1 NBA Last 14 Days"
        "Micah Roberts3-0 NBA Last 14 Days"
        "Prop Model"
        "Bill Marzano"
    """
    if not expert_raw:
        return None, None

    # If innerText has newline between name and record ("Brian Edwards\n50-47 Last 30 Days")
    expert_raw = expert_raw.split("\n")[0].strip()

    # Strip record suffix: digits-digits(-digits)? followed by NBA/...
    name = re.sub(r"\d+-\d+(?:-\d+)?\s+(?:NBA|NCAAB|NFL|MLB).*$", "", expert_raw).strip()
    if not name:
        name = expert_raw.strip()

    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return name, slug


def _market_type_from_tags(tags: List[str]) -> Optional[str]:
    """Map pick card tags to canonical market_type."""
    for tag in tags:
        tag_lower = tag.lower().strip()
        if "milestones" in tag_lower:
            return "player_prop"
        if tag_lower == "points":
            return "player_prop"  # "Points" tag = player prop
        result = MARKET_TAG_MAP.get(tag_lower)
        if result:
            return result
    return None


_TIME_ONLY_RE = re.compile(r"^\d{3,4}$")  # bare time like "2330" or "0300"


def _parse_matchup_header(text: str, store=None) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Parse matchup header like 'Jazz @ Grizzlies 02/20 7:00 PM ET'.

    Returns (away_code, home_code, event_time_utc).
    """
    m = re.search(
        r"([A-Za-z\s]+?)\s*@\s*([A-Za-z\s]+?)\s+(\d{1,2}/\d{1,2})\s+(\d{1,2}:\d{2})\s*(AM|PM)",
        text, re.IGNORECASE,
    )
    if not m:
        return None, None, None

    away_raw = m.group(1).strip()
    home_raw = m.group(2).strip()
    date_str = m.group(3)
    time_str = m.group(4)
    ampm = m.group(5)

    # If either team token is purely numeric (e.g. "2330", "0300") the regex matched a
    # time string rather than a team name — treat the whole parse as failed.
    if _TIME_ONLY_RE.match(away_raw) or _TIME_ONLY_RE.match(home_raw):
        return None, None, None

    away_code = _map_team(away_raw, store)
    home_code = _map_team(home_raw, store)
    event_time_utc = _parse_game_time(date_str, time_str, ampm)

    return away_code, home_code, event_time_utc


def _extract_matchup_from_headline(headline: str, store=None) -> Tuple[Optional[str], Optional[str]]:
    """Try to extract away@home teams from card headline text."""
    m = re.search(
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s*@\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
        headline,
    )
    if m:
        away_raw = m.group(1).strip()
        home_raw = m.group(2).strip()
        # Extract last word as team nickname for mapping
        away_code = _map_team(away_raw.split()[-1], store) or _map_team(away_raw, store)
        home_code = _map_team(home_raw.split()[-1], store) or _map_team(home_raw, store)
        return away_code, home_code
    return None, None


def scrape_vegasinsider(
    storage_state: str = DEFAULT_STORAGE_STATE,
    debug: bool = False,
    snapshot: bool = False,
    featured_only: bool = False,
    sport: str = NBA_SPORT,
) -> Tuple[List[RawPickRecord], Dict[str, Any]]:
    """Scrape VegasInsider picks page for the given sport (NBA or NCAAB).

    Returns (raw_records, debug_info).
    """
    observed_at = datetime.now(timezone.utc)
    dbg: Dict[str, Any] = {"observed_at": observed_at.isoformat(), "sport": sport}

    use_storage = os.path.exists(storage_state) and not featured_only
    if not use_storage and not featured_only:
        print(f"[WARN] No storage state at {storage_state} — premium picks may not be visible")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context_kwargs: Dict[str, Any] = {
            "viewport": {"width": 1400, "height": 900},
            "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
        if use_storage:
            context_kwargs["storage_state"] = storage_state
            dbg["storage_state"] = storage_state

        context = browser.new_context(**context_kwargs)
        page = context.new_page()

        try:
            print(f"[INGEST] VegasInsider: fetching {PICKS_URL}")
            page.goto(PICKS_URL, wait_until="networkidle", timeout=30000)
            page.wait_for_timeout(4000)

            # Scroll to load lazy content
            for _ in range(5):
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(800)

            # Click carousel arrows to reveal hidden picks
            for click_round in range(10):
                clicked = page.evaluate(CLICK_CAROUSELS_JS)
                if debug and clicked > 0:
                    print(f"[DEBUG] Carousel round {click_round+1}: clicked {clicked} arrows")
                if clicked == 0:
                    break
                page.wait_for_timeout(600)

            # Save snapshot if requested
            if snapshot:
                os.makedirs(OUT_DIR, exist_ok=True)
                snap_path = os.path.join(OUT_DIR, "vegasinsider_snapshot.html")
                with open(snap_path, "w", encoding="utf-8") as f:
                    f.write(page.content())
                print(f"[SNAPSHOT] Saved page HTML to {snap_path}")

            # Extract structured pick data via JS using actual DOM classes
            extract_js = _make_extract_picks_js(sport)
            pick_cards = page.evaluate(extract_js)
            dbg["pick_cards_extracted"] = len(pick_cards)

            if debug:
                print(f"[DEBUG] Extracted {len(pick_cards)} pick cards (locked picks excluded)")

            # Set up sport-specific lookups
            sport_lower = sport.lower()
            source_surface = f"vegasinsider_{sport_lower}_picks"
            store = get_data_store(sport)

            # Parse pick cards into raw records
            records: List[RawPickRecord] = []
            seen_fps: set = set()

            for card in pick_cards:
                headline = card.get("headline", "").strip()
                tags = card.get("tags", [])
                expert_raw = card.get("expertRaw", "")
                badge_line = card.get("badgeLine", "")
                badge_odds = card.get("badgeOdds", "")
                analysis = card.get("analysis", "")
                matchup_text = card.get("matchupText", "")
                is_free = card.get("isFree", False)

                if not headline:
                    continue

                # Parse expert name (strip record suffix)
                expert_name, expert_slug = _parse_expert_raw(expert_raw)

                # Determine market type from tags
                market_type = _market_type_from_tags(tags)

                # Parse matchup header for teams + time
                away_code, home_code, event_time_utc = _parse_matchup_header(matchup_text, store)

                # Also try to extract teams from headline (total headlines have "Away @ Home")
                if not away_code or not home_code:
                    h_away, h_home = _extract_matchup_from_headline(headline, store)
                    away_code = away_code or h_away
                    home_code = home_code or h_home

                # Parse the headline for structured fields
                parsed = _parse_headline(headline, market_type)

                # Use inferred market type from headline if tags didn't provide one
                if not market_type and "inferred_market" in parsed:
                    market_type = parsed["inferred_market"]

                if not market_type:
                    if debug:
                        print(f"[DEBUG] Unknown market: tags={tags} headline={headline[:60]!r}")
                    continue

                # Resolve teams from parsed data if still missing
                if not away_code and parsed.get("away_team_raw"):
                    last_word = parsed["away_team_raw"].split()[-1]
                    away_code = _map_team(last_word, store) or _map_team(parsed["away_team_raw"], store)
                if not home_code and parsed.get("home_team_raw"):
                    last_word = parsed["home_team_raw"].split()[-1]
                    home_code = _map_team(last_word, store) or _map_team(parsed["home_team_raw"], store)

                # Extract fields from parsed headline
                side = parsed.get("side")
                line_val = parsed.get("line")
                odds_val = parsed.get("odds")
                player_name = parsed.get("player_name")
                stat_key = parsed.get("stat_key")

                # Use badge values as fallback/override for line and odds
                if badge_line and line_val is None:
                    bl = re.sub(r"^[ou]", "", badge_line)
                    try:
                        line_val = float(bl)
                    except ValueError:
                        pass
                    if badge_line.lower().startswith("o") and not side:
                        side = "OVER"
                    elif badge_line.lower().startswith("u") and not side:
                        side = "UNDER"

                if badge_odds and odds_val is None:
                    try:
                        odds_val = int(badge_odds)
                    except ValueError:
                        pass

                # Build selection
                selection = None
                if market_type in ("spread", "moneyline") and parsed.get("team_raw"):
                    team_raw = parsed["team_raw"]
                    last_word = team_raw.split()[-1]
                    selection = _map_team(last_word, store) or _map_team(team_raw, store)
                elif market_type == "total":
                    selection = "game_total"
                elif market_type == "player_prop":
                    selection = player_name

                market_family = "player_prop" if market_type == "player_prop" else "standard"

                # Build fingerprint for dedup
                fp_source = f"{SOURCE_ID}|{sport}|{headline}|{expert_name or ''}"
                fp = sha256_digest(fp_source)
                if fp in seen_fps:
                    continue
                seen_fps.add(fp)

                section = "featured" if is_free else "premium"

                record = RawPickRecord(
                    source_id=SOURCE_ID,
                    source_surface=source_surface,
                    sport=sport,
                    market_family=market_family,
                    observed_at_utc=observed_at.isoformat(),
                    canonical_url=PICKS_URL,
                    raw_pick_text=headline,
                    raw_block=card.get("fullText", "")[:500],
                    raw_fingerprint=fp,
                    event_start_time_utc=event_time_utc,
                    matchup_hint=f"{away_code}@{home_code}" if away_code and home_code else None,
                    away_team=away_code,
                    home_team=home_code,
                    expert_name=expert_name,
                    expert_slug=expert_slug,
                    market_type=market_type,
                    selection=selection,
                    side=side,
                    line=line_val,
                    odds=odds_val,
                    player_name=player_name,
                    stat_key=stat_key,
                    section=section,
                    analysis_text=analysis if analysis else None,
                )
                records.append(record)

                if debug:
                    print(
                        f"[DEBUG] {market_type:12s} | {away_code or '?':>3s}@{home_code or '?':<3s} | "
                        f"{expert_name or 'unknown':15s} | {headline[:50]}"
                    )

            dbg["records_parsed"] = len(records)
            return records, dbg

        finally:
            page.close()
            context.close()
            browser.close()


# ── Normalization ─────────────────────────────────────────────────────────


def normalize_pick(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize a raw VegasInsider pick to the standard schema."""
    sport = raw.get("sport") or "NBA"
    home_code = raw.get("home_team") or ""
    away_code = raw.get("away_team") or ""
    market_type = raw.get("market_type")
    side = raw.get("side")
    selection = raw.get("selection")
    line_val = raw.get("line")
    odds_val = raw.get("odds")
    player_name = raw.get("player_name")
    stat_key = raw.get("stat_key")
    expert_name = raw.get("expert_name")
    # Strip any embedded performance badge (e.g. "Bruce Marshall\n102-88 Last 14 Days")
    if expert_name:
        expert_name = expert_name.split("\n")[0].strip()
    expert_slug = raw.get("expert_slug")

    # Build event keys using Eastern time for day assignment
    # (Games at 9pm ET on Feb 20 should be day_key NBA:2026:02:20, not Feb 21)
    event_time = raw.get("event_start_time_utc")
    canonical_event_key = None
    event_key = None
    day_key = None
    matchup_key = None

    if event_time and home_code and away_code:
        try:
            dt = datetime.fromisoformat(event_time)
            eastern = ZoneInfo("America/New_York")
            dt_eastern = dt.astimezone(eastern)
            date_str = f"{dt_eastern.year:04d}:{dt_eastern.month:02d}:{dt_eastern.day:02d}"
            canonical_event_key = f"{sport}:{date_str}:{away_code}@{home_code}"
            event_key = f"{sport}:{dt_eastern.strftime('%Y%m%d')}:{away_code}@{home_code}:{dt.strftime('%H%M')}"
            day_key = f"{sport}:{date_str}"
            teams_sorted = "-".join(sorted([away_code, home_code]))
            matchup_key = f"{sport}:{date_str}:{teams_sorted}"
        except (ValueError, TypeError):
            pass

    # Build player_key for props
    player_key = None
    if market_type == "player_prop" and player_name:
        player_slug = re.sub(r"[^a-z0-9]+", "_", player_name.lower()).strip("_")
        player_key = f"{sport.lower()}:{player_slug}"
        # Build selection in standard format
        if stat_key and side:
            selection = f"{player_key}::{stat_key}::{side}"

    # For total, use direction as selection
    if market_type == "total" and side:
        selection = side

    # Fingerprint
    fingerprint = raw.get("raw_fingerprint") or sha256_digest(
        f"{SOURCE_ID}|{canonical_event_key}|{market_type}|{selection}|{line_val}|{expert_name}"
    )

    # Eligibility
    eligible = True
    inelig_reason = None

    if market_type == "player_prop":
        if not player_name:
            eligible, inelig_reason = False, "unparsed_player"
        elif not stat_key:
            eligible, inelig_reason = False, "unparsed_stat"
        elif side not in ("OVER", "UNDER"):
            eligible, inelig_reason = False, "unparsed_direction"
        elif line_val is None:
            eligible, inelig_reason = False, "unparsed_line"
    elif market_type in ("spread", "moneyline"):
        if not (away_code and home_code):
            eligible, inelig_reason = False, "unparsed_team"
        elif not selection:
            eligible, inelig_reason = False, "missing_selection"
        elif market_type == "spread" and line_val is None:
            eligible, inelig_reason = False, "unparsed_line"
    elif market_type == "total":
        if not (away_code and home_code):
            eligible, inelig_reason = False, "unparsed_team"
        elif side not in ("OVER", "UNDER"):
            eligible, inelig_reason = False, "invalid_direction"
        elif line_val is None:
            eligible, inelig_reason = False, "unparsed_line"
    else:
        eligible, inelig_reason = False, "unsupported_market"

    if eligible and not canonical_event_key:
        eligible, inelig_reason = False, "missing_event_key"

    return {
        "provenance": {
            "source_id": SOURCE_ID,
            "source_surface": raw.get("source_surface", "vegasinsider_nba_picks"),
            "sport": sport,
            "observed_at_utc": raw.get("observed_at_utc"),
            "canonical_url": raw.get("canonical_url", PICKS_URL),
            "raw_fingerprint": fingerprint,
            "raw_pick_text": raw.get("raw_pick_text"),
            "raw_block": raw.get("raw_block"),
            "matchup_hint": raw.get("matchup_hint"),
            "expert_name": expert_name,
            "expert_slug": expert_slug,
            "expert_handle": None,
            "expert_profile": None,
        },
        "event": {
            "sport": sport,
            "event_key": event_key,
            "canonical_event_key": canonical_event_key,
            "day_key": day_key,
            "matchup_key": matchup_key,
            "away_team": away_code or None,
            "home_team": home_code or None,
            "event_start_time_utc": event_time,
        },
        "market": {
            "market_type": market_type,
            "market_family": "player_prop" if market_type == "player_prop" else "standard",
            "selection": selection,
            "side": side,
            "line": line_val,
            "odds": odds_val,
            "stat_key": stat_key,
            "player_key": player_key,
            "player_name": player_name,
        },
        "eligible_for_consensus": eligible,
        "ineligibility_reason": inelig_reason,
        "eligibility": {
            "eligible_for_consensus": eligible,
            "ineligibility_reason": inelig_reason,
        },
    }


# ── Helpers ───────────────────────────────────────────────────────────────


def ensure_out_dir():
    os.makedirs(OUT_DIR, exist_ok=True)


def write_json_file(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=json_default, ensure_ascii=False)


def _create_storage(storage_path: str) -> None:
    """Open headed browser for manual login to create storage state."""
    print("Opening browser for manual login...")
    print("After logging in, close the browser window to save the session.")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )
        page = context.new_page()
        page.goto("https://www.vegasinsider.com/")

        try:
            page.wait_for_event("close", timeout=600000)
        except Exception:
            pass

        os.makedirs(os.path.dirname(storage_path) or ".", exist_ok=True)
        context.storage_state(path=storage_path)

        try:
            context.close()
            browser.close()
        except Exception:
            pass

    print(f"Saved storage state to {storage_path}")


# ── Main ──────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Ingest VegasInsider NBA or NCAAB picks.")
    parser.add_argument(
        "--sport",
        default="NBA",
        choices=["NBA", "NCAAB"],
        help="Sport to ingest picks for (default: NBA)",
    )
    parser.add_argument(
        "--storage",
        default=DEFAULT_STORAGE_STATE,
        help=f"Path to Playwright storage state JSON (default: {DEFAULT_STORAGE_STATE})",
    )
    parser.add_argument(
        "--create-storage",
        action="store_true",
        help="Launch browser for manual login to create storage state",
    )
    parser.add_argument(
        "--debug", action="store_true", help="Enable debug logging."
    )
    parser.add_argument(
        "--snapshot", action="store_true",
        help="Save page HTML to out/vegasinsider_snapshot.html for selector development",
    )
    parser.add_argument(
        "--featured-only", action="store_true",
        help="Only scrape featured picks (no auth required)",
    )
    args = parser.parse_args()

    if args.create_storage:
        _create_storage(args.storage)
        return 0

    sport = args.sport

    # Set sport-specific team fallback
    global TEAM_FALLBACK
    TEAM_FALLBACK = TEAM_FALLBACK_NCAAB if sport == NCAAB_SPORT else TEAM_FALLBACK_NBA

    # 1. Scrape
    raw_records, dbg = scrape_vegasinsider(
        storage_state=args.storage,
        debug=args.debug,
        snapshot=args.snapshot,
        featured_only=args.featured_only,
        sport=sport,
    )
    print(f"[INGEST] Extracted {len(raw_records)} raw {sport} picks")

    if not raw_records:
        print("[INGEST] No picks found. Try --snapshot --debug to inspect page HTML.")
        return 1

    sport_lower = sport.lower()

    # 2. Write raw output
    ensure_out_dir()
    raw_path = os.path.join(OUT_DIR, f"raw_vegasinsider_{sport_lower}.json")
    write_json_file(raw_path, [asdict(r) for r in raw_records])
    print(f"[INGEST] Wrote {len(raw_records)} raw records to {raw_path}")

    # 3. Normalize
    normalized = []
    for raw in raw_records:
        norm = normalize_pick(asdict(raw))
        if norm:
            normalized.append(norm)

    # 4. Dedupe
    normalized = dedupe_normalized_bets(normalized)

    # 5. Write normalized output
    norm_path = os.path.join(OUT_DIR, f"normalized_vegasinsider_{sport_lower}.json")
    write_json_file(norm_path, normalized)

    eligible = sum(1 for r in normalized if r.get("eligible_for_consensus"))
    ineligible = len(normalized) - eligible
    print(f"[INGEST] Wrote {len(normalized)} normalized ({eligible} eligible, {ineligible} ineligible) to {norm_path}")

    # 6. Summary
    markets = Counter(r.market_type for r in raw_records)
    experts = Counter(r.expert_name for r in raw_records if r.expert_name)
    sections = Counter(r.section for r in raw_records)

    print(f"[INGEST] Markets: {dict(markets)}")
    if experts:
        print(f"[INGEST] Experts: {dict(experts)}")
    if args.debug:
        print(f"[DEBUG] Sections: {dict(sections)}")
        print(f"[DEBUG] Debug info: {dbg}")

    return 0


if __name__ == "__main__":
    exit(main())
