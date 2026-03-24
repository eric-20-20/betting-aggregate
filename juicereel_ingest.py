"""
JuiceReel NBA + NCAAB expert picks ingestion script (daily open bets).

Fetches current open bets from JuiceReel experts:
  - sXeBets: https://app.juicereel.com/users/sXeBets
  - nukethebooks: https://app.juicereel.com/users/nukethebooks
  - UnitVacuum: https://app.juicereel.com/users/UnitVacuum

Requires authenticated session state at data/juicereel_storage_state.json.
Run scripts/create_juicereel_storage.py first to create it.

Usage:
    python3 juicereel_ingest.py [--debug] [--storage PATH]

Output:
    - out/raw_juicereel_nba.json        (all raw records, sport tagged)
    - out/normalized_juicereel_nba.json  (NBA-only normalized)
    - out/normalized_juicereel_ncaab.json (NCAAB-only normalized)
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

try:
    from playwright.sync_api import sync_playwright, Page, BrowserContext
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
except ImportError:
    sync_playwright = None
    PlaywrightTimeoutError = Exception

from store import write_json
from utils import normalize_text

OUT_DIR = os.getenv("NBA_OUT_DIR", "out")
DEFAULT_STORAGE_STATE = "data/juicereel_storage_state.json"

# Expert definitions
EXPERTS = [
    {
        "source_id": "juicereel_sxebets",
        "expert_name": "sXeBets",
        "expert_slug": "sxebets",
        "expert_handle": "sXeBets",
        "url": "https://app.juicereel.com/users/sXeBets",
        # Risk/profit are in dollars ($350.00, +$332.26)
        # unit_size = risk / 100.0 (e.g. $350 risk → 3.5u)
        "profit_type": "dollars",
    },
    {
        "source_id": "juicereel_nukethebooks",
        "expert_name": "nukethebooks",
        "expert_slug": "nukethebooks",
        "expert_handle": "nukethebooks",
        "url": "https://app.juicereel.com/users/nukethebooks",
        # Risk/profit are in units (0.9u, +0.9u)
        "profit_type": "units",
    },
    {
        "source_id": "juicereel_unitvacuum",
        "expert_name": "UnitVacuum",
        "expert_slug": "unitvacuum",
        "expert_handle": "UnitVacuum",
        "url": "https://app.juicereel.com/users/UnitVacuum",
        # Risk/profit are in dollars ($350.00, +$332.26)
        # unit_size = risk / 100.0
        "profit_type": "dollars",
    },
]

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


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
    pregraded_result: Optional[str] = None


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


def _parse_odds(text: str) -> Optional[int]:
    """Parse American odds from text like '-114', '+150', '100'."""
    if not text:
        return None
    text = text.strip().replace(" ", "")
    m = re.match(r"^([+-]?\d+)$", text)
    if m:
        val = int(m.group(1))
        # Ensure proper sign convention
        if val > 0 and not text.startswith("+"):
            return val  # treat unsigned positive as positive odds
        return val
    return None


def _parse_dollar_amount(text: str) -> Optional[float]:
    """Parse dollar amount like '$228.00', '228', '$10'."""
    if not text:
        return None
    cleaned = re.sub(r"[$,\s]", "", text)
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_unit_amount(text: str) -> Optional[float]:
    """Parse unit amount like '2.28u', '-$228', '+$272.73'."""
    if not text:
        return None
    # Try numeric unit (e.g., "2.28u" or "-1.5")
    cleaned = re.sub(r"[u\s]", "", text.lower())
    cleaned = re.sub(r"[$,]", "", cleaned)
    try:
        return float(cleaned)
    except ValueError:
        return None


def _infer_market_from_pick(pick_text: str) -> Tuple[str, Optional[str], Optional[float], Optional[str], Optional[str], Optional[float], Optional[str]]:
    """
    Infer market type and details from pick text.

    Returns: (market_type, selection, line, side, player_name, prop_line, stat_key)

    Examples:
      "Total" → ("total", None, None, None, None, None, None)  # needs API line
      "Golden State Warriors -3.5" → ("spread", "GSW", -3.5, "GSW", None, None, None)
      "GSW -3.5" → ("spread", "GSW", -3.5, "GSW", None, None, None)
      "Over 228.5" → ("total", "OVER", 228.5, "OVER", None, None, None)
      "Under 228.5" → ("total", "UNDER", 228.5, "UNDER", None, None, None)
      "Golden State Warriors" → ("moneyline", None, None, None, None, None, None)
      "Darius Garland Over 12.5 Points" → ("player_prop", ..., ...)
    """
    if not pick_text:
        return "unknown", None, None, None, None, None, None

    text = pick_text.strip()
    lower = text.lower()

    # Detect bare market label words (no team/line info — side inferred from result later)
    if re.match(r"^spread\s*$", lower):
        return "spread", None, None, None, None, None, None

    if re.match(r"^moneyline\s*$", lower) or re.match(r"^money line\s*$", lower):
        return "moneyline", None, None, None, None, None, None

    # Detect "Parlay" — skip, can't grade individual legs
    if re.match(r"^parlay\s*$", lower):
        return "parlay", None, None, None, None, None, None

    # Detect "Prop" alone — player prop without parseable details
    if re.match(r"^prop\s*$", lower):
        return "player_prop", None, None, None, None, None, None

    # Detect "Total" alone (game total, no direction/line in backfill history)
    if re.match(r"^total\s*$", lower):
        return "total", None, None, None, None, None, None

    # Detect "Over/Under <line>" (game total with line and direction)
    m = re.match(r"^(over|under)\s+([\d.]+)\s*$", lower)
    if m:
        direction = m.group(1).upper()
        line = float(m.group(2))
        return "total", direction, line, direction, None, None, None

    # Detect spread/moneyline: "TEAM ±line" or just "TEAM"
    # First try to match a prop (Over/Under + number + stat word)
    prop_m = re.match(
        r"^(.+?)\s+(over|under)\s+([\d.]+)\s+(.+)$",
        text,
        re.IGNORECASE,
    )
    if prop_m:
        player_name = prop_m.group(1).strip()
        direction = prop_m.group(2).upper()
        try:
            prop_line = float(prop_m.group(3))
        except ValueError:
            prop_line = None
        stat_raw = prop_m.group(4).strip().lower()
        # Import stat map from normalizer
        from src.normalizer_juicereel_nba import STAT_KEY_MAP
        stat_key = STAT_KEY_MAP.get(stat_raw)
        if not stat_key:
            for k, v in STAT_KEY_MAP.items():
                if stat_raw.startswith(k) or k.startswith(stat_raw):
                    stat_key = v
                    break
        return "player_prop", None, None, direction, player_name, prop_line, stat_key

    # Spread: "TEAM ±line" — signed (e.g. "Utah Jazz -3.5", "NDAKST +16.5")
    spread_m = re.match(
        r"^(.+?)\s+([+-][\d.]+)\s*$",
        text,
    )
    if spread_m:
        team_raw = spread_m.group(1).strip()
        try:
            line = float(spread_m.group(2))
        except ValueError:
            line = None
        return "spread", team_raw, line, team_raw, None, None, None

    # Spread: "TEAM <decimal>" — unsigned (e.g. "St Louis 2.5" means +2.5 underdog)
    spread_unsigned_m = re.match(
        r"^(.+?)\s+(\d+\.\d+)\s*$",
        text,
    )
    if spread_unsigned_m:
        team_raw = spread_unsigned_m.group(1).strip()
        try:
            line = float(spread_unsigned_m.group(2))
        except ValueError:
            line = None
        return "spread", team_raw, line, team_raw, None, None, None

    # If no spread marker, treat as moneyline
    return "moneyline", text, None, None, None, None, None


def scroll_to_load_all(page: "Page", max_scrolls: int = 30, debug: bool = False) -> int:
    """Scroll down within the open bets container to load all bets."""
    scroll_count = 0
    prev_height = 0
    for _ in range(max_scrolls):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(800)
        height = page.evaluate("document.body.scrollHeight")
        if height == prev_height:
            break
        prev_height = height
        scroll_count += 1
    if debug:
        print(f"[scroll] scrolled {scroll_count} times, final height={prev_height}")
    return scroll_count


def _parse_card_lines(lines: List[str]) -> dict:
    """
    Parse structured fields from a JuiceReel BetCell card.

    Card layout (observed live):
      Risked $X
      To Win $Y
      <odds>
      Placed: M/D/YYYY
      <pick content lines...>  ← player name / direction / line / stat  OR  team / spread
      <odds again>
      Team @ Team  (or "Team vs Team" in matchup description)
      SPREAD / PLAYER PROP / OVER / UNDER / MONEYLINE
      -
      Today at H:MM PM  (or actual date)
      <numeric IDs>
    """
    # Strip header lines: Risked, To Win, odds, Placed:
    skip_prefixes = ("risked", "to win", "placed:")
    MARKET_LABELS = {"SPREAD", "PLAYER PROP", "MONEYLINE", "OVER / UNDER", "PARLAY"}
    SKIP_TOKENS = {"NOVIG", "-"}

    risk_val = None
    odds_val = None
    event_dt_str = None
    matchup_hint = None  # "Team @ Team" line
    matchup_vs = None    # "Team vs Team" from description block
    pick_lines = []      # content after the header block

    header_done = False
    for line in lines:
        lower = line.lower()

        # Harvest risk from header
        if lower.startswith("risked"):
            m = re.search(r"\$([\d,.]+)", line)
            if m:
                risk_val = _parse_dollar_amount(m.group(0))
            continue

        # Skip "To Win" line
        if lower.startswith("to win"):
            continue

        # Skip "Placed: ..." line
        if lower.startswith("placed:"):
            header_done = True
            continue

        # Skip market label lines
        if line.upper() in MARKET_LABELS or line.upper() in SKIP_TOKENS:
            continue

        # Skip pure numeric IDs (6+ digit numbers)
        if re.match(r"^\d{6,}$", line):
            continue

        # Grab odds (±3-4 digit number, first occurrence)
        if re.match(r"^[+-]\d{3,4}$", line):
            if odds_val is None:
                odds_val = _parse_odds(line)
            continue  # don't add to pick_lines

        # Event time line
        if re.search(r"\d{1,2}:\d{2}\s*(?:AM|PM)", line, re.IGNORECASE):
            event_dt_str = line
            continue

        # Bare "@" separator line — team names are on separate pick_lines; join them
        if line == "@":
            # Join as "prev @ next" when we see the next team name
            pick_lines.append("@")
            continue

        # "@" matchup line (e.g. "76ers @ Jazz" or "Clippers @ Pacers")
        if "@" in line:
            matchup_hint = line
            continue

        # "vs" matchup line (from description)
        if " vs " in lower and matchup_vs is None:
            matchup_vs = line
            continue

        # Skip if header not yet done and line looks like a stray odds line
        if not header_done:
            continue

        pick_lines.append(line)

    return {
        "risk_val": risk_val,
        "odds_val": odds_val,
        "event_dt_str": event_dt_str,
        "matchup_hint": matchup_hint or matchup_vs,
        "pick_lines": pick_lines,
    }


def _extract_matchup_from_pick_lines(pick_lines: List[str]) -> Tuple[Optional[str], List[str]]:
    """
    Find "Team @ Team" pattern inside pick_lines and extract as matchup.

    pick_lines may contain: [...prop lines..., "Clippers", "@", "Pacers", "ProphetX"]
    Returns (matchup_str, cleaned_pick_lines) where matchup_str = "Clippers @ Pacers"
    and cleaned_pick_lines has the team names + "@" removed.
    """
    # Find index of bare "@"
    at_idx = None
    for i, line in enumerate(pick_lines):
        if line == "@":
            at_idx = i
            break

    if at_idx is None:
        return None, pick_lines

    # Check if prev and next lines look like team names (short, mostly uppercase/title)
    prev_line = pick_lines[at_idx - 1] if at_idx > 0 else None
    next_line = pick_lines[at_idx + 1] if at_idx + 1 < len(pick_lines) else None

    if prev_line and next_line:
        matchup = f"{prev_line} @ {next_line}"
        # Remove the three lines: team1, @, team2
        cleaned = pick_lines[:at_idx - 1] + pick_lines[at_idx + 2:]
        # Also strip trailing known non-pick tokens (sportsbook names, etc.)
        trailing_skip = {"ProphetX", "HardRock", "FanDuel", "DraftKings", "BetMGM", "Caesars", "ESPN Bet", "NoVig"}
        while cleaned and cleaned[-1] in trailing_skip:
            cleaned.pop()
        return matchup, cleaned

    return None, pick_lines


def _build_pick_text(pick_lines: List[str]) -> str:
    """
    Reconstruct pick text from parsed card lines (after matchup extraction).

    Props:     ["Andrew Nembhard", "Under", "16.5", "Points"] → "Andrew Nembhard Under 16.5 Points"
    Spreads:   ["Utah Jazz", "8.5"]                           → "Utah Jazz 8.5"
    Totals:    ["Over", "228.5"]                              → "Over 228.5"
    """
    if not pick_lines:
        return ""
    return " ".join(pick_lines)


def _detect_sport_from_matchup(matchup_hint: Optional[str]) -> str:
    """
    Detect whether a pick is NBA or NCAAB by trying to resolve the matchup teams.

    Strategy:
    - Split matchup_hint on '@' or 'vs' to get team names
    - Try to resolve both teams against NBA store → if both resolve → NBA
    - Try to resolve both teams against NCAAB store → if both resolve → NCAAB
    - If NCAAB resolves and NBA doesn't → NCAAB
    - Default: NBA
    """
    if not matchup_hint:
        return "NBA"

    # Parse teams from matchup
    if "@" in matchup_hint:
        parts = matchup_hint.split("@", 1)
    else:
        import re as _re
        parts = _re.split(r"\s+vs\.?\s+", matchup_hint, flags=_re.IGNORECASE)

    if len(parts) != 2:
        return "NBA"

    t1_raw = parts[0].strip()
    t2_raw = parts[1].strip()

    from store import get_data_store
    from utils import normalize_text

    def _try_resolve(team_raw: str, store) -> bool:
        norm = normalize_text(team_raw)
        codes = store.lookup_team_code(norm)
        if len(codes) == 1:
            return True
        # Try short abbrev
        upper = team_raw.upper()
        if upper in store.teams:
            return True
        return False

    nba_store = get_data_store("NBA")
    ncaab_store = get_data_store("NCAAB")

    nba_t1 = _try_resolve(t1_raw, nba_store)
    nba_t2 = _try_resolve(t2_raw, nba_store)
    ncaab_t1 = _try_resolve(t1_raw, ncaab_store)
    ncaab_t2 = _try_resolve(t2_raw, ncaab_store)

    if ncaab_t1 and ncaab_t2 and not (nba_t1 and nba_t2):
        return "NCAAB"
    if nba_t1 and nba_t2:
        return "NBA"
    if ncaab_t1 and ncaab_t2:
        return "NCAAB"
    # If NCAAB resolves at least one team that NBA doesn't → lean NCAAB
    ncaab_score = int(ncaab_t1) + int(ncaab_t2)
    nba_score = int(nba_t1) + int(nba_t2)
    if ncaab_score > nba_score:
        return "NCAAB"
    return "NBA"


def extract_open_bets(
    page: "Page",
    expert: Dict[str, Any],
    observed_at: datetime,
    debug: bool = False,
) -> List[RawPickRecord]:
    """
    Extract open bets from the expert's JuiceReel profile page.

    Uses the confirmed CSS class pattern: [class*="BetCell_betCard"]
    """
    records: List[RawPickRecord] = []
    source_id = expert["source_id"]
    url = expert["url"]
    profit_type = expert.get("profit_type", "unknown")

    # Confirmed selector from live DOM inspection (class="BetCell_betCard__CeSBO")
    bet_elements = page.query_selector_all('[class*="BetCell_betCard"]')

    if not bet_elements:
        if debug:
            body_text = page.inner_text("body")
            print(f"[extract] No bet elements found. Page text (first 2000 chars):")
            print(body_text[:2000])
        return records

    if debug:
        print(f"[extract] Found {len(bet_elements)} bet cards for {expert['expert_name']}")

    for elem in bet_elements:
        try:
            raw_text = elem.inner_text()
            if not raw_text or not raw_text.strip():
                continue

            lines = [l.strip() for l in raw_text.split("\n") if l.strip()]
            if not lines:
                continue

            # Skip parlay cards — can't grade individual legs
            if lines[0].upper() == "PARLAY":
                continue

            parsed = _parse_card_lines(lines)
            risk_val = parsed["risk_val"]
            odds_val = parsed["odds_val"]
            event_dt_str = parsed["event_dt_str"]
            matchup_hint = parsed["matchup_hint"]
            pick_lines = parsed["pick_lines"]

            # Extract matchup from pick_lines if not already found
            extracted_matchup, pick_lines = _extract_matchup_from_pick_lines(pick_lines)
            if not matchup_hint and extracted_matchup:
                matchup_hint = extracted_matchup

            pick_text = _build_pick_text(pick_lines)

            if debug:
                print(f"[card] pick_text={repr(pick_text)} matchup={repr(matchup_hint)} lines={pick_lines}")

            # Parse market info from pick text
            market_type, selection, line_val, side, player_name, prop_line, stat_key = _infer_market_from_pick(pick_text)
            actual_line = prop_line if market_type == "player_prop" else line_val

            # Compute unit_size from risk
            unit_size = None
            if risk_val is not None:
                if profit_type == "dollars":
                    unit_size = round(risk_val / 100.0, 3)
                elif profit_type == "units":
                    unit_size = risk_val  # risk is already in units
                else:
                    # Default: assume $100 base
                    unit_size = round(risk_val / 100.0, 3)

            raw_block = raw_text
            fp_input = f"{source_id}|{url}|{pick_text}|{matchup_hint or ''}|{observed_at.date()}"
            fingerprint = sha256_digest(fp_input)

            # Detect sport from matchup teams (try NCAAB store first)
            detected_sport = _detect_sport_from_matchup(matchup_hint)

            record = RawPickRecord(
                source_id=source_id,
                source_surface="juicereel_picks",
                sport=detected_sport,
                market_family="player_prop" if market_type == "player_prop" else "standard",
                observed_at_utc=observed_at.isoformat(),
                canonical_url=url,
                raw_pick_text=pick_text,
                raw_block=raw_block,
                raw_fingerprint=fingerprint,
                matchup_hint=matchup_hint,
                expert_name=expert["expert_name"],
                expert_handle=expert["expert_handle"],
                expert_profile=url,
                expert_slug=expert["expert_slug"],
                market_type=market_type if market_type != "unknown" else None,
                selection=selection,
                side=side,
                line=actual_line,
                odds=odds_val,
                player_name=player_name,
                stat_key=stat_key,
                unit_size=unit_size,
                pregraded_result=None,
            )
            records.append(record)

        except Exception as e:
            if debug:
                print(f"[extract] Error parsing bet element: {e}")
            continue

    if debug:
        print(f"[extract] Extracted {len(records)} open bets for {expert['expert_name']}")

    return records


def ingest_juicereel_nba(
    storage_state: str = DEFAULT_STORAGE_STATE,
    debug: bool = False,
) -> Tuple[List[RawPickRecord], Dict[str, Any]]:
    """Main ingestion function for JuiceReel daily open bets."""
    if sync_playwright is None:
        raise ImportError("playwright is not installed. Run: pip install playwright && playwright install chromium")

    observed_at = datetime.now(timezone.utc)
    all_records: List[RawPickRecord] = []
    dbg: Dict[str, Any] = {
        "observed_at": observed_at.isoformat(),
        "experts": [],
    }

    if not Path(storage_state).exists():
        raise FileNotFoundError(
            f"No storage state at {storage_state}. "
            "Run: python3 scripts/create_juicereel_storage.py"
        )

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=USER_AGENT,
            storage_state=storage_state,
            viewport={"width": 1400, "height": 900},
        )

        for expert in EXPERTS:
            expert_name = expert["expert_name"]
            url = expert["url"]
            if debug:
                print(f"[juicereel] Fetching open bets for {expert_name}: {url}")

            try:
                page = context.new_page()
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(2000)

                # Scroll to load all open bets
                scroll_to_load_all(page, debug=debug)
                page.wait_for_timeout(1000)

                records = extract_open_bets(page, expert, observed_at, debug=debug)
                all_records.extend(records)
                dbg["experts"].append({
                    "name": expert_name,
                    "url": url,
                    "count": len(records),
                })

                page.close()

            except Exception as e:
                if debug:
                    print(f"[juicereel] Error fetching {expert_name}: {e}")
                dbg["experts"].append({
                    "name": expert_name,
                    "url": url,
                    "error": str(e),
                    "count": 0,
                })

        context.close()
        browser.close()

    # Deduplicate by fingerprint
    seen: set = set()
    deduped: List[RawPickRecord] = []
    for r in all_records:
        if r.raw_fingerprint not in seen:
            seen.add(r.raw_fingerprint)
            deduped.append(r)

    dbg["total_raw"] = len(all_records)
    dbg["total_deduped"] = len(deduped)

    return deduped, dbg


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest JuiceReel expert picks (daily open bets)")
    ap.add_argument("--storage", default=DEFAULT_STORAGE_STATE, help="Path to Playwright storage state JSON")
    ap.add_argument("--debug", action="store_true", help="Enable verbose debug output")
    ap.add_argument("--dry-run", action="store_true", help="Don't write output files")
    args = ap.parse_args()

    print(f"[juicereel] Starting daily ingest (storage={args.storage})")
    records, dbg = ingest_juicereel_nba(storage_state=args.storage, debug=args.debug)

    expert_summary = dbg.get("experts", [])
    for exp in expert_summary:
        err = exp.get("error", "")
        print(f"  {exp['name']}: {exp.get('count', 0)} picks" + (f" [ERROR: {err}]" if err else ""))

    print(f"[juicereel] Total: {dbg.get('total_deduped', 0)} records (after dedup)")

    if args.dry_run:
        print("[juicereel] Dry run — not writing files")
        return

    ensure_out_dir()

    raw_path = os.path.join(OUT_DIR, "raw_juicereel_nba.json")
    write_records(raw_path, records)
    print(f"[juicereel] Wrote raw → {raw_path}")

    # Split by sport
    from dataclasses import asdict as _asdict
    nba_recs = [r for r in records if r.sport == "NBA"]
    ncaab_recs = [r for r in records if r.sport == "NCAAB"]
    other_recs = [r for r in records if r.sport not in ("NBA", "NCAAB")]
    print(f"[juicereel] Sport split: NBA={len(nba_recs)}, NCAAB={len(ncaab_recs)}, other={len(other_recs)}")

    # Normalize NBA
    from src.normalizer_juicereel_nba import normalize_juicereel_records
    nba_dicts = [asdict(r) for r in nba_recs]
    normalized_nba = normalize_juicereel_records(nba_dicts, debug=args.debug, sport="NBA")
    norm_nba_path = os.path.join(OUT_DIR, "normalized_juicereel_nba.json")
    write_json(norm_nba_path, normalized_nba)
    eligible_nba = sum(1 for r in normalized_nba if r.get("eligible_for_consensus"))
    print(f"[juicereel] Wrote NBA normalized → {norm_nba_path} ({eligible_nba}/{len(normalized_nba)} eligible)")

    # Normalize NCAAB
    ncaab_dicts = [asdict(r) for r in ncaab_recs]
    normalized_ncaab = normalize_juicereel_records(ncaab_dicts, debug=args.debug, sport="NCAAB")
    norm_ncaab_path = os.path.join(OUT_DIR, "normalized_juicereel_ncaab.json")
    write_json(norm_ncaab_path, normalized_ncaab)
    eligible_ncaab = sum(1 for r in normalized_ncaab if r.get("eligible_for_consensus"))
    print(f"[juicereel] Wrote NCAAB normalized → {norm_ncaab_path} ({eligible_ncaab}/{len(normalized_ncaab)} eligible)")


if __name__ == "__main__":
    main()
