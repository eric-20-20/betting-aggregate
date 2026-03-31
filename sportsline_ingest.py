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

from store import data_store, get_data_store, write_json
from utils import normalize_text

OUT_DIR = os.getenv("NBA_OUT_DIR", "out")
DEFAULT_STORAGE_STATE = "data/sportsline_storage_state.json"

PICKS_URLS = {
    "NBA": "https://www.sportsline.com/nba/picks/experts/?sc=p",
    "NCAAB": "https://www.sportsline.com/college-basketball/picks/experts/?sc=p",
}
PICKS_URL = PICKS_URLS["NBA"]  # backward compatibility


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
    side: Optional[str] = None
    line: Optional[float] = None
    odds: Optional[int] = None
    player_name: Optional[str] = None
    stat_key: Optional[str] = None
    unit_size: Optional[float] = None


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


def _map_team(alias: Optional[str], store=None, sport: str = "NBA") -> Optional[str]:
    """Map team name or abbreviation to standard team code.

    Uses the sport-specific data store for lookups, with NBA-specific
    city fallbacks as a last resort (only for NBA sport).
    """
    if not alias:
        return None
    store = store or data_store
    norm = normalize_text(alias)
    # Try the sport-specific data store first
    codes = store.lookup_team_code(norm)
    if len(codes) == 1:
        return next(iter(codes))
    # Also try the raw alias (handles abbreviations the store might know)
    if norm != alias:
        codes = store.lookup_team_code(alias)
        if len(codes) == 1:
            return next(iter(codes))
    # NBA-specific city/nickname fallbacks — only for NBA sport
    if sport != "NBA":
        # For non-NBA, only try short codes against the store
        if len(alias) <= 4:
            upper = alias.upper()
            if upper in store.teams:
                return upper
        return None
    nba_fallback = {
        # Team nicknames
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
        # City/market names (SportsLine uses these)
        "milwaukee": "MIL",
        "philadelphia": "PHI",
        "new york": "NYK",
        "brooklyn": "BKN",
        "portland": "POR",
        "phoenix": "PHX",
        "san antonio": "SAS",
        "golden state": "GSW",
        "golden st.": "GSW",
        "golden st": "GSW",
        "new orleans": "NOP",
        "l.a. lakers": "LAL",
        "l a lakers": "LAL",
        "los angeles lakers": "LAL",
        "l.a. clippers": "LAC",
        "l a clippers": "LAC",
        "los angeles clippers": "LAC",
        "boston": "BOS",
        "miami": "MIA",
        "chicago": "CHI",
        "cleveland": "CLE",
        "detroit": "DET",
        "toronto": "TOR",
        "dallas": "DAL",
        "minnesota": "MIN",
        "oklahoma city": "OKC",
        "orlando": "ORL",
        "houston": "HOU",
        "utah": "UTA",
        "sacramento": "SAC",
        "atlanta": "ATL",
        "charlotte": "CHA",
        "memphis": "MEM",
        "denver": "DEN",
        "indiana": "IND",
        "washington": "WAS",
    }
    if norm in nba_fallback:
        return nba_fallback[norm]
    # Try short codes (3-4 letter abbreviations) if they exist in the store
    if len(alias) <= 4:
        upper = alias.upper()
        if upper in store.teams:
            return upper
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


# JS to extract pick cards from the SportsLine expert picks page.
# SportsLine uses styled-components with hashed class names that change on redeploy.
# Strategy: find containers with 5 child divs where text matches known patterns.
EXTRACT_EXPERT_PICKS_JS = """() => {
    const cards = [];
    // Walk all divs looking for pick card pattern:
    // - Has 4-6 direct child divs
    // - One child contains "Pick Made:" (timestamp)
    // - One child matches date pattern "Feb|Mar|Apr|... \\d+ \\d{4}"
    const allDivs = document.querySelectorAll('div');

    for (const div of allDivs) {
        const children = Array.from(div.children).filter(c => c.tagName === 'DIV');
        if (children.length < 4 || children.length > 7) continue;

        const childTexts = children.map(c => c.innerText.trim());
        const fullText = childTexts.join('|');

        // Must have "Pick Made:" in one child
        const hasPickMade = childTexts.some(t => t.includes('Pick Made:'));
        if (!hasPickMade) continue;

        // Must have a date pattern in first child
        const hasDate = /(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\\s+\\d+\\s+\\d{4}/.test(childTexts[0]);
        if (!hasDate) continue;

        // Must have a market keyword in second child
        const pickText = childTexts[1] || '';
        const hasMarket = /^(Spread|Over\\/Under|Money Line|1st Half|Home Team Total|Away Team Total|Points|Rebounds|Assists|Steals|Blocks|Threes|PTS|REB|AST|STL|BLK)/i.test(pickText);
        if (!hasMarket) continue;

        cards.push({
            matchupText: childTexts[0] || '',
            pickText: childTexts[1] || '',
            expertText: childTexts[2] || '',
            analysisText: childTexts[3] || '',
            timestampText: childTexts[4] || '',
            fullText: fullText.substring(0, 1000),
        });
    }
    return cards;
}""".strip()

# Stat type label → canonical stat_key
STAT_TYPE_MAP = {
    "points": "points",
    "rebounds": "rebounds",
    "assists": "assists",
    "steals": "steals",
    "blocks": "blocks",
    "threes": "threes",
    "three-pointers": "threes",
    "3-pointers": "threes",
    "pts + ast": "pts_ast",
    "pts + reb": "pts_reb",
    "reb + ast": "reb_ast",
    "pts + reb + ast": "pts_reb_ast",
    # Long-form variants captured from pick body text
    "points + assists": "pts_ast",
    "points + rebounds": "pts_reb",
    "rebounds + assists": "reb_ast",
    "points + assists + rebounds": "pts_reb_ast",
    "points + rebounds + assists": "pts_reb_ast",
}

# Regex for date/time line: "Feb 20 2026, 4:00 pm PST"
DATE_TIME_RE = re.compile(
    r"(\w+ \d+ \d{4}),?\s+(\d+:\d+)\s*(am|pm)\s*(PDT|EDT|CDT|MDT|PST|EST|CST|MST|PT|ET|CT|MT)",
    re.IGNORECASE,
)

# Regex for spread: "Spread TeamName +/-line odds"
SPREAD_PICK_RE = re.compile(
    r"Spread\s+(.+?)\s+([+-]?\d+(?:\.\d+)?)\s+([+-]\d+)",
    re.IGNORECASE,
)

# Regex for 1st half spread: "1st Half Spread 1st Half TeamName line odds"
HALF_SPREAD_PICK_RE = re.compile(
    r"1st Half Spread\s+1st Half\s+(.+?)\s+([+-]?\d+(?:\.\d+)?)\s+([+-]\d+)",
    re.IGNORECASE,
)

# Regex for total: "Over/Under Over|Under line odds"
TOTAL_PICK_RE = re.compile(
    r"Over/Under\s+(Over|Under)\s+(\d+(?:\.\d+)?)\s+([+-]\d+)",
    re.IGNORECASE,
)

# Regex for team total: "Home|Away Team Total TeamName Over|Under line Total Pts odds"
TEAM_TOTAL_PICK_RE = re.compile(
    r"(?:Home|Away) Team Total\s+(.+?)\s+(Over|Under)\s+(\d+(?:\.\d+)?)\s+Total\s+Pts\s+([+-]\d+)",
    re.IGNORECASE,
)

# Regex for moneyline: "Money Line TeamName odds"
ML_PICK_RE = re.compile(
    r"Money Line\s+(.+?)\s+([+-]\d+)",
    re.IGNORECASE,
)

# Regex for player prop: "StatLabel PlayerName Over|Under line Total StatName odds"
PROP_PICK_RE = re.compile(
    r"(?:PTS \+ (?:AST \+ REB|REB \+ AST)|PTS \+ (?:AST|REB)|REB \+ AST|Points|Rebounds|Assists|Steals|Blocks|Threes)\s+"
    r"(.+?)\s+(Over|Under)\s+(\d+(?:\.\d+)?)\s+Total\s+(.+?)\s+([+-]\d+)",
    re.IGNORECASE,
)


def _parse_matchup_text(text: str) -> Dict[str, Any]:
    """Parse SportsLine matchup header text.

    The DOM innerText has newlines between elements:
        "Feb 20 2026, 4:00 pm PST\\nUtah\\n@ Memphis"
        "Feb 19 2026, 7:00 pm PST\\nBoston\\n121\\n@ Golden St.\\n110"
    """
    result: Dict[str, Any] = {}

    # Parse date/time from first line
    m = DATE_TIME_RE.search(text)
    if m:
        date_str = m.group(1)  # "Feb 20 2026"
        time_str = m.group(2)  # "4:00"
        ampm = m.group(3)      # "pm"
        tz_str = m.group(4)    # "PST"

        try:
            hour, minute = map(int, time_str.split(":"))
            if ampm.lower() == "pm" and hour != 12:
                hour += 12
            elif ampm.lower() == "am" and hour == 12:
                hour = 0

            tz_map = {
                "pst": "America/Los_Angeles", "pdt": "America/Los_Angeles", "pt": "America/Los_Angeles",
                "est": "America/New_York", "edt": "America/New_York", "et": "America/New_York",
                "cst": "America/Chicago", "cdt": "America/Chicago", "ct": "America/Chicago",
                "mst": "America/Denver", "mdt": "America/Denver", "mt": "America/Denver",
            }
            tz_name = tz_map.get(tz_str.lower(), "America/Los_Angeles")

            from dateutil.parser import parse as dateparse
            dt_local = dateparse(f"{date_str} {hour}:{minute:02d}")
            tz = ZoneInfo(tz_name)
            dt_local = dt_local.replace(tzinfo=tz)
            dt_utc = dt_local.astimezone(timezone.utc)
            result["event_time_utc"] = dt_utc.isoformat()

            # Day key uses Eastern time
            dt_eastern = dt_local.astimezone(ZoneInfo("America/New_York"))
            result["day_date"] = f"{dt_eastern.year:04d}:{dt_eastern.month:02d}:{dt_eastern.day:02d}"
        except (ValueError, ImportError):
            pass

    # Parse teams from "Away\n@ Home" or "Away\nscore\n@ Home\nscore"
    # Find the line with "@" and work from there
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    at_idx = None
    for i, ln in enumerate(lines):
        if ln.startswith("@") or ln.startswith("@ "):
            at_idx = i
            break

    if at_idx is not None:
        # Home team is on the @ line (after "@")
        home_raw = lines[at_idx].lstrip("@").strip()
        # Check if next line is a score
        if at_idx + 1 < len(lines) and lines[at_idx + 1].isdigit():
            result["home_score"] = int(lines[at_idx + 1])

        # Away team is above the @ line — scan backwards for team name
        # Skip score lines (pure digits)
        for j in range(at_idx - 1, -1, -1):
            if lines[j].isdigit():
                result["away_score"] = int(lines[j])
                continue
            # Skip date/time lines
            if DATE_TIME_RE.search(lines[j]):
                continue
            result["away_raw"] = lines[j]
            break

        if home_raw:
            result["home_raw"] = home_raw
    else:
        # Fallback: find "Away @ Home" on a single line
        m2 = re.search(r"([A-Za-z.\s]+?)\s*(?:\d+)?\s*@\s*([A-Za-z.\s]+?)(?:\s*\d+)?\s*$", text)
        if m2:
            result["away_raw"] = m2.group(1).strip()
            result["home_raw"] = m2.group(2).strip()

    return result


def _parse_pick_text(text: str) -> Dict[str, Any]:
    """Parse the pick detail text from a SportsLine expert pick card.

    DOM innerText has newlines between elements:
        "Spread\\nNew Orleans -4 -110\\nUnit\\n1.0"
        "Over/Under\\nOver 239.5 -110\\nUnit\\n1.0"
        "Money Line\\nNew York -165\\nLOSS\\nUnit\\n1.0"
        "Points\\nSaddiq Bey Over 19.5 Total Points +100\\nUnit\\n1.0"
        "PTS + AST\\nMarcus Smart Over 14.5 Total Points + Assists -108\\nWIN\\nUnit\\n1.0"
    """
    result: Dict[str, Any] = {}

    # Check for WIN/LOSS/PUSH result (past picks)
    if re.search(r"\bWIN\b", text):
        result["result"] = "WIN"
    elif re.search(r"\bLOSS\b", text):
        result["result"] = "LOSS"
    elif re.search(r"\bPUSH\b", text):
        result["result"] = "PUSH"

    # Collapse newlines to spaces for regex matching, strip Unit suffix and result tags
    clean = re.sub(r"\n+", " ", text)
    clean = re.sub(r"\b(?:WIN|LOSS|PUSH)\b", "", clean)
    # Extract unit size before stripping (e.g., "Unit 1.0" → 1.0)
    unit_match = re.search(r"Unit\s*(\d+\.?\d*)", clean)
    if unit_match:
        result["unit_size"] = float(unit_match.group(1))
    clean = re.sub(r"Unit\s*\d+\.?\d*", "", clean).strip()

    # Try 1st half spread (must be before regular spread to avoid false match)
    m = HALF_SPREAD_PICK_RE.search(clean)
    if m:
        result["market_type"] = "1st_half_spread"
        result["team_raw"] = m.group(1).strip()
        result["line"] = float(m.group(2))
        result["odds"] = int(m.group(3))
        return result

    # Try spread
    m = SPREAD_PICK_RE.search(clean)
    if m:
        result["market_type"] = "spread"
        result["team_raw"] = m.group(1).strip()
        result["line"] = float(m.group(2))
        result["odds"] = int(m.group(3))
        return result

    # Try team total (must be before regular total to avoid false match)
    m = TEAM_TOTAL_PICK_RE.search(clean)
    if m:
        result["market_type"] = "team_total"
        result["team_raw"] = m.group(1).strip()
        result["side"] = m.group(2).upper()
        result["line"] = float(m.group(3))
        result["odds"] = int(m.group(4))
        return result

    # Try total
    m = TOTAL_PICK_RE.search(clean)
    if m:
        result["market_type"] = "total"
        result["side"] = m.group(1).upper()
        result["line"] = float(m.group(2))
        result["odds"] = int(m.group(3))
        return result

    # Try moneyline
    m = ML_PICK_RE.search(clean)
    if m:
        result["market_type"] = "moneyline"
        result["team_raw"] = m.group(1).strip()
        result["odds"] = int(m.group(2))
        return result

    # Try player prop
    m = PROP_PICK_RE.search(clean)
    if m:
        result["market_type"] = "player_prop"
        result["player_name"] = m.group(1).strip()
        result["side"] = m.group(2).upper()
        result["line"] = float(m.group(3))
        stat_raw = m.group(4).strip().lower()
        result["odds"] = int(m.group(5))
        result["stat_key"] = STAT_TYPE_MAP.get(stat_raw)
        return result

    return result


def _parse_expert_text(text: str) -> Dict[str, Any]:
    """Parse expert info text.

    DOM innerText has newlines:
        "Zack Cimini\\nContrarian with Chutzpah\\nProfile →\\n+90\\n4-1 in Last 5 NBA Picks"
        "Prop Bet Guy\\nDoug\\nProfile →\\n+1496\\n94-68 in Last 162 NBA Player Props Picks"
        "Micah Roberts\\nFormer Vegas Bookmaker\\nProfile →"

    Guard against profit figures (e.g. "+2390.75") appearing as the first line
    when the DOM renders the ROI/profit stat before the expert name.
    """
    result: Dict[str, Any] = {}

    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    if not lines:
        return result

    # Find the first line that looks like a real name:
    # - Not purely numeric / profit figure (e.g. "+2390.75", "-45", "1496")
    # - Not a record string (e.g. "4-1 in Last 5 NBA Picks")
    # - Not "Profile →" navigation text
    # A valid name has at least one letter and doesn't start with a digit or sign
    # followed only by digits/decimals.
    import re as _re
    _profit_re = _re.compile(r'^[+\-]?[\d,]+(\.\d+)?$')
    _record_re = _re.compile(r'^\d+-\d+\s+in\s+', _re.IGNORECASE)
    _nav_re = _re.compile(r'^Profile\s*[→>]?$', _re.IGNORECASE)

    name = None
    for line in lines:
        if _profit_re.match(line):
            continue
        if _record_re.match(line):
            continue
        if _nav_re.match(line):
            continue
        if any(c.isalpha() for c in line):
            name = line
            break

    if not name:
        return result

    # Handle "Prop Bet Guy" specifically (DOM sometimes has "Prop Bet Guy\nDoug")
    if name.startswith("Prop Bet Guy"):
        result["expert_name"] = "Prop Bet Guy"
    else:
        result["expert_name"] = name

    if result.get("expert_name"):
        result["expert_slug"] = re.sub(r"[^a-z0-9]+", "-", result["expert_name"].lower()).strip("-")

    return result


def extract_expert_picks(
    page: Page,
    observed_at: datetime,
    canonical_url: str,
    sport: str = "NBA",
    debug: bool = False,
) -> List[RawPickRecord]:
    """Extract expert picks from SportsLine expert picks page using JS extraction."""
    store = get_data_store(sport)
    records: List[RawPickRecord] = []

    # Scroll to load all content
    for _ in range(5):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(800)

    # Click "Load More Picks" button if present
    for _ in range(5):
        try:
            load_more = page.locator("text=Load More Picks").first
            if load_more.is_visible(timeout=1000):
                load_more.click()
                page.wait_for_timeout(1500)
                if debug:
                    print("[DEBUG] Clicked 'Load More Picks'")
            else:
                break
        except Exception:
            break

    # Extract structured card data via JS
    cards = page.evaluate(EXTRACT_EXPERT_PICKS_JS)

    if debug:
        print(f"[DEBUG] Extracted {len(cards)} expert pick cards from DOM")

    seen_fps: set = set()

    for i, card in enumerate(cards):
        matchup_text = card.get("matchupText", "")
        pick_text = card.get("pickText", "")
        expert_text = card.get("expertText", "")
        analysis_text = card.get("analysisText", "")

        if not pick_text:
            continue

        # Parse matchup
        matchup = _parse_matchup_text(matchup_text)
        away_raw = matchup.get("away_raw")
        home_raw = matchup.get("home_raw")
        away_code = _map_team(away_raw, store=store, sport=sport) if away_raw else None
        home_code = _map_team(home_raw, store=store, sport=sport) if home_raw else None
        event_time = matchup.get("event_time_utc")

        # Parse pick details
        pick = _parse_pick_text(pick_text)
        market_type = pick.get("market_type")

        if not market_type:
            if debug:
                print(f"[DEBUG] Skipping unparsed pick: {pick_text[:80]}")
            continue

        # Skip past picks (already graded)
        if pick.get("result"):
            if debug:
                print(f"[DEBUG] Skipping past pick ({pick['result']}): {pick_text[:60]}")
            continue

        # Parse expert
        expert = _parse_expert_text(expert_text)
        expert_name = expert.get("expert_name")
        expert_slug = expert.get("expert_slug")

        # Build selection and line
        selection = None
        line_val = pick.get("line")
        odds_val = pick.get("odds")
        side = pick.get("side")

        if market_type in ("spread", "moneyline", "1st_half_spread"):
            team_raw = pick.get("team_raw", "")
            # Try mapping the full name, then last word
            selection = _map_team(team_raw, store=store, sport=sport)
            if not selection and team_raw:
                last_word = team_raw.split()[-1] if team_raw.split() else ""
                selection = _map_team(last_word, store=store, sport=sport)
        elif market_type in ("total", "team_total"):
            if market_type == "team_total":
                # For team totals, map the team and use side (OVER/UNDER)
                team_raw = pick.get("team_raw", "")
                team_code = _map_team(team_raw, store=store, sport=sport)
                if not team_code and team_raw:
                    last_word = team_raw.split()[-1] if team_raw.split() else ""
                    team_code = _map_team(last_word, store=store, sport=sport)
                selection = f"{team_code}_{side}" if team_code and side else side
            else:
                selection = side  # OVER or UNDER
        elif market_type == "player_prop":
            selection = pick.get("player_name")

        market_family = "player_prop" if market_type == "player_prop" else "standard"

        # Build raw pick text
        raw_pick_text = pick_text.strip()
        # Clean up concatenated text (SportsLine has no spaces between elements)
        raw_pick_text = re.sub(r"(WIN|LOSS|PUSH)", r" \1", raw_pick_text)
        raw_pick_text = re.sub(r"Unit\s*\d+\.?\d*", "", raw_pick_text).strip()

        # Fingerprint for dedup
        fp_source = f"sportsline|{canonical_url}|{raw_pick_text}|{expert_name or ''}"
        fp = sha256_digest(fp_source)
        if fp in seen_fps:
            continue
        seen_fps.add(fp)

        record = RawPickRecord(
            source_id="sportsline",
            source_surface=f"sportsline_{sport.lower()}_expert_picks",
            sport=sport,
            market_family=market_family,
            observed_at_utc=observed_at.isoformat(),
            canonical_url=canonical_url,
            raw_pick_text=raw_pick_text,
            raw_block=card.get("fullText", "")[:500],
            raw_fingerprint=fp,
            event_start_time_utc=event_time,
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
            player_name=pick.get("player_name"),
            stat_key=pick.get("stat_key"),
            unit_size=pick.get("unit_size"),
        )
        records.append(record)

        if debug:
            print(
                f"[DEBUG] {market_type:12s} | {away_code or '?':>3s}@{home_code or '?':<3s} | "
                f"{expert_name or 'unknown':20s} | {raw_pick_text[:50]}"
            )

    return records


def ingest_sportsline_nba(
    storage_state: str = DEFAULT_STORAGE_STATE,
    debug: bool = False,
    picks_url: str = PICKS_URL,
    sport: str = "NBA",
) -> Tuple[List[RawPickRecord], Dict[str, Any]]:
    """
    Main ingestion function for SportsLine expert picks.

    Navigates to the expert picks page and extracts all pick cards from the
    single-page feed (no tab switching needed).

    Args:
        storage_state: Path to Playwright storage state JSON with auth cookies
        debug: Enable debug output
        picks_url: URL of the expert picks page
        sport: Sport to ingest (NBA or NCAAB)

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
    dbg: Dict[str, Any] = {
        "storage_state": storage_state,
        "observed_at": observed_at.isoformat(),
        "url": picks_url,
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            storage_state=storage_state,
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )

        try:
            page = context.new_page()
            print(f"[INGEST] SportsLine: fetching {picks_url}")
            page.goto(picks_url, wait_until="networkidle", timeout=30000)

            # Wait for React hydration
            page.wait_for_timeout(3000)

            # Extract all expert picks from the page
            records = extract_expert_picks(
                page=page,
                observed_at=observed_at,
                canonical_url=picks_url,
                sport=sport,
                debug=debug,
            )

            dbg["records_extracted"] = len(records)
            page.close()

        finally:
            context.close()
            browser.close()

    return records, dbg


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
            sport=sport,
        )

        # Write raw output
        ensure_out_dir()
        raw_path = os.path.join(OUT_DIR, f"raw_sportsline_{sport_suffix}.json")
        write_records(raw_path, raw_records)

        print(f"[INGEST] Wrote {len(raw_records)} raw records to {raw_path}")

        if args.debug:
            print(f"[DEBUG] Markets: {dbg.get('markets', {})}")

        # Normalize records (convert dataclasses to dicts)
        from src.normalizer_sportsline_nba import normalize_sportsline_records
        raw_dicts = [asdict(r) if hasattr(r, "__dataclass_fields__") else r for r in raw_records]
        normalized = normalize_sportsline_records(raw_dicts, debug=args.debug, sport=sport)

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
