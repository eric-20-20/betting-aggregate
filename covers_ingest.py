from __future__ import annotations

"""
How extraction works (summary for devs):
- DOM-first: we search for analyst pick cards via stable pick-related class names, heading text, and “Pick made” labels.
- Each card yields one or more picks from child elements holding pick text; we validate picks via parse_player_prop / parse_standard_market.
- Regex fallback: if no DOM picks found, we run bounded regex on a nearby text window.
- Validation rejects consensus/computer/model picks and malformed markets before producing RawPickRecord.

How to add new fixtures:
- Place HTML files under tests/fixtures/covers/.
- Add a pytest case in tests/test_covers_ingest.py pointing to the new fixture and asserting extracted raw_pick_text and expert_name.
"""

import argparse
import json
import re
import os
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Tuple, Dict
from zoneinfo import ZoneInfo
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

from action_ingest import (
    IncrementalJsonWriter,
    RawPickRecord,
    dedupe_normalized_bets,
    json_default,
    parse_player_prop,
    parse_standard_market,
    sha256_digest,
)
from event_resolution import SCHEDULE, ScheduledGame, build_event_key, build_canonical_event_key, resolve_event
from store import NBA_SPORT, NCAAB_SPORT, data_store, get_data_store
from utils import normalize_text

OUT_DIR = os.getenv("NBA_OUT_DIR", "out")

# Sport-specific URL patterns
COVERS_LISTING_URLS = {
    NBA_SPORT: "https://www.covers.com/picks/nba",
    NCAAB_SPORT: "https://www.covers.com/picks/ncaab",
}

COVERS_MATCHUP_PATTERNS = {
    NBA_SPORT: re.compile(r"/sport/basketball/nba/matchup/(\d+)/picks", re.IGNORECASE),
    NCAAB_SPORT: re.compile(r"/sport/basketball/ncaab/matchup/(\d+)/picks", re.IGNORECASE),
}

COVERS_MATCHUP_URL_TEMPLATES = {
    NBA_SPORT: "https://www.covers.com/sport/basketball/nba/matchup/{matchup_id}/picks",
    NCAAB_SPORT: "https://www.covers.com/sport/basketball/ncaab/matchup/{matchup_id}/picks",
}

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.covers.com/",
        "Connection": "keep-alive",
    }
)


def fetch_html(url: str) -> str:
    response = SESSION.get(url, timeout=15)
    response.raise_for_status()
    return response.text


# --- DOM helper selectors (conservative defaults, resilient to minor class changes) ---
PICK_CARD_CLASSES = [
    "cover-pick-card",
    "covers-pick-card",
    "pick-card",
    "pickcard",
    "expert-pick",
    "expertPick",
]
PICK_TEXT_CLASSES = [
    "pick-text",
    "pick-text__value",
    "pick-value",
    "pick",
    "selection",
    "bet-text",
    "betText",
]
EXPERT_NAME_CLASSES = [
    "pick-author",
    "pick-card__author",
    "analyst-name",
    "expert-name",
]
EXCLUDE_TOKENS = {"computer pick", "consensus", "model rating", "best odds"}
PICK_LABEL_PATTERNS = [re.compile(r"pick made", re.IGNORECASE)]


def discover_matchup_pick_urls(listing_url: str = None, sport: str = NBA_SPORT) -> list[str]:
    if listing_url is None:
        listing_url = COVERS_LISTING_URLS.get(sport, COVERS_LISTING_URLS[NBA_SPORT])
    try:
        listing_html = fetch_html(listing_url)
    except Exception:
        return []
    soup = BeautifulSoup(listing_html, "html.parser")
    pattern = COVERS_MATCHUP_PATTERNS.get(sport, COVERS_MATCHUP_PATTERNS[NBA_SPORT])
    urls: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        match = pattern.search(href)
        if not match:
            continue
        full = urljoin(listing_url, href)
        if full not in urls:
            urls.append(full)
    return urls

def extract_matchup_teams_from_header(soup: BeautifulSoup, store=None) -> tuple[Optional[str], Optional[str]]:
    store = store or data_store
    text = soup.get_text(" ", strip=True)
    fallback_map = {
        "PHO": "PHX",
        "PHX": "PHX",
        "NOP": "NOP",
        "NO": "NOP",
    }

    def to_code(token: str) -> Optional[str]:
        code = fallback_map.get(token.upper())
        if code:
            return code
        mapped = store.lookup_team_code(token)
        if len(mapped) == 1:
            return next(iter(mapped))
        if len(token) == 3 and token.upper() in store.teams:
            return token.upper()
        return None

    # Abbreviation patterns
    for pattern in [r"\b([A-Z]{2,3})\s*@\s*([A-Z]{2,3})\b", r"\b([A-Z]{2,3})\s+vs\s+([A-Z]{2,3})\b"]:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            away_raw, home_raw = m.group(1), m.group(2)
            away_code, home_code = to_code(away_raw), to_code(home_raw)
            if away_code and home_code:
                return away_code, home_code

    # Full team names
    def map_full_name(token: str) -> Optional[str]:
        normalized = normalize_text(token)
        matches = store.lookup_team_code(normalized)
        if len(matches) == 1:
            return next(iter(matches))
        for code, team in store.teams.items():
            if normalize_text(team.city) == normalized or normalize_text(team.nickname) == normalized:
                return code
        return None

    for pattern in [r"\b([A-Za-z]+)\s*@\s*([A-Za-z]+)\b", r"\b([A-Za-z]+)\s+vs\s+([A-Za-z]+)\b"]:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            away_raw, home_raw = m.group(1), m.group(2)
            away_code, home_code = map_full_name(away_raw), map_full_name(home_raw)
            if away_code and home_code:
                return away_code, home_code

    # Existing header scan as fallback
    header_text = None
    for tag in soup.find_all(["h1", "h2", "h3"]):
        txt = tag.get_text(" ", strip=True)
        if "vs" in txt.lower() or "@" in txt:
            header_text = txt
            break
    if header_text:
        m = re.search(r"([A-Za-z\.]{2,})\s*(?:vs|@)\s*([A-Za-z\.]{2,})", header_text, re.IGNORECASE)
        if m:
            away_raw, home_raw = m.group(1), m.group(2)
            return to_code(away_raw), to_code(home_raw)

    return None, None


def team_search_tokens(team_code: str, store=None) -> set[str]:
    store = store or data_store
    tokens = {team_code.upper()}
    team = store.teams.get(team_code.upper())
    if team:
        tokens.add(team.city.upper())
        tokens.add(team.nickname.upper())
        tokens.add(f"{team.city.upper()} {team.nickname.upper()}")

    for alias in store.team_aliases:
        if alias.team_code == team_code:
            tokens.add(alias.alias.upper())
    return tokens


def search_matchup_url(away_team: str, home_team: str, listing_html: Optional[str] = None, sport: str = NBA_SPORT, store=None) -> Optional[str]:
    store = store or get_data_store(sport)
    listing_url = COVERS_LISTING_URLS.get(sport, COVERS_LISTING_URLS[NBA_SPORT])
    try:
        main_html = listing_html or fetch_html(listing_url)
    except Exception:
        return None

    away_tokens = team_search_tokens(away_team, store=store)
    home_tokens = team_search_tokens(home_team, store=store)
    pattern = COVERS_MATCHUP_PATTERNS.get(sport, COVERS_MATCHUP_PATTERNS[NBA_SPORT])
    matches = list(pattern.finditer(main_html))

    def snippet_has_match(start: int, end: int) -> bool:
        snippet = main_html[start:end].upper()
        away_hit = any(token in snippet for token in away_tokens)
        home_hit = any(token in snippet for token in home_tokens)
        return away_hit and home_hit

    url_template = COVERS_MATCHUP_URL_TEMPLATES.get(sport, COVERS_MATCHUP_URL_TEMPLATES[NBA_SPORT])
    for m in matches:
        start = max(0, m.start() - 240)
        end = min(len(main_html), m.end() + 240)
        if snippet_has_match(start, end):
            matchup_id = m.group(1)
            return url_template.format(matchup_id=matchup_id)
    return None


def get_covers_matchup_url(away_team: str, home_team: str, game_date: str, sport: str = NBA_SPORT) -> Optional[str]:
    listing_url = COVERS_LISTING_URLS.get(sport, COVERS_LISTING_URLS[NBA_SPORT])
    try:
        listing_html = fetch_html(listing_url)
    except Exception:
        listing_html = None
    return search_matchup_url(away_team, home_team, listing_html=listing_html, sport=sport)


def normalize_ou_prefix(text: str) -> str:
    def repl(match: re.Match) -> str:
        prefix = match.group(1).lower()
        number = match.group(2)
        word = "over" if prefix == "o" else "under"
        return f"{word} {number}"

    return re.sub(r"\b([ou])\s*(\d+(?:\.\d+)?)\b", repl, text, flags=re.IGNORECASE)


def normalize_fractional_line(text: str) -> str:
    return text.replace("½", ".5").replace(" 1/2", ".5").replace("1/2", ".5")


def parse_event_start_utc(html: str, observed_at_utc: datetime) -> datetime:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    eastern = ZoneInfo("America/New_York")

    dt_pattern = re.compile(
        r"([A-Z][a-z]+)\s+(\d{1,2}),\s*(\d{4})[^\d]{0,40}?(\d{1,2}:\d{2})\s*(AM|PM)",
        re.IGNORECASE,
    )
    dt_match = dt_pattern.search(text)
    if dt_match:
        month, day, year, clock, ampm = dt_match.groups()
        try:
            local_dt = datetime.strptime(f"{month} {day} {year} {clock} {ampm}", "%B %d %Y %I:%M %p").replace(tzinfo=eastern)
            return local_dt.astimezone(timezone.utc)
        except ValueError:
            pass

    date_only_pattern = re.compile(r"([A-Z][a-z]+)\s+(\d{1,2}),\s*(\d{4})", re.IGNORECASE)
    date_match = date_only_pattern.search(text)
    if date_match:
        month, day, year = date_match.groups()
        try:
            local_dt = datetime.strptime(f"{month} {day} {year} 12:00 PM", "%B %d %Y %I:%M %p").replace(tzinfo=eastern)
            return local_dt.astimezone(timezone.utc)
        except ValueError:
            pass

    return observed_at_utc if observed_at_utc.tzinfo else observed_at_utc.replace(tzinfo=timezone.utc)


def extract_picks_from_html(html: str, canonical_url: str, observed_at_utc: datetime, debug: bool = False, debug_stats: Optional[dict] = None, sport: str = NBA_SPORT) -> List[RawPickRecord]:
    soup = BeautifulSoup(html, "html.parser")
    debug_stats = debug_stats if debug_stats is not None else {}
    cards: List[Tag] = []
    card_reasons_excluded: Dict[str, int] = {}

    def mark_excluded(reason: str) -> None:
        card_reasons_excluded[reason] = card_reasons_excluded.get(reason, 0) + 1

    scope = soup.select_one("#Expert_Picks") or soup
    cards = scope.select("div.pick-cards-expert-component")
    if not cards:
        # fallback to old heuristics
        for cls in PICK_CARD_CLASSES:
            cards.extend(scope.select(f".{cls}"))
    seen_ids = set()
    cards = [c for c in cards if not (id(c) in seen_ids or seen_ids.add(id(c)))]

    def extract_expert_from_card(card: Tag) -> Optional[str]:
        txt = card.get_text(" ", strip=True)
        m = re.search(
            r"(?:Pick made:.*?)([A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+)+)\s+(?:Betting Analyst|Publishing Editor|.+Analyst|.+Editor)\s+Analysis",
            txt,
        )
        if m:
            name = m.group(1).strip()
            name = re.sub(r"^(AM|PM)\s+", "", name)
            return name
        # fallback: look near "Pick made:" prefix
        m2 = re.search(r"Pick made:.*?\b([A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+)+)\b", txt)
        if m2:
            name = m2.group(1).strip()
            name = re.sub(r"^(AM|PM)\s+", "", name)
            return name
        for cls in EXPERT_NAME_CLASSES:
            node = card.select_one(f".{cls}")
            if node:
                t = node.get_text(" ", strip=True)
                if t:
                    return t
        return None

    def extract_pick_texts(card: Tag) -> List[str]:
        texts: List[str] = []
        main_pick_nodes = card.select("div.w-100.fw-bold.small")
        for node in main_pick_nodes:
            raw = node.get_text(" ", strip=True)
            if raw:
                raw = re.sub(r"\bBest Odds\b.*", "", raw, flags=re.IGNORECASE)
                texts.append(raw)
        if not texts:
            for cls in PICK_TEXT_CLASSES:
                for node in card.select(f".{cls}"):
                    txt = node.get_text(" ", strip=True)
                    if txt:
                        texts.append(txt)
        return texts

    def normalize_moneyline_pick(team_code: str, odds: Optional[str]) -> str:
        odds_clean = odds.strip() if odds else ""
        odds_clean = odds_clean.replace("(", "").replace(")", "").replace(" ", "")
        if odds_clean and not odds_clean.startswith(("+", "-")):
            odds_clean = f"+{odds_clean}"
        return f"{team_code} ML {odds_clean}".strip()

    def clean_pick_string(text: str) -> str:
        txt = normalize_fractional_line(text)
        txt = normalize_ou_prefix(txt)
        txt = re.sub(r"\s*\(\s*([+-]\s?\d{3,})\s*\)", r" \1", txt)
        txt = re.sub(r"\s+", " ", txt).strip()
        return txt

    def validate_pick(pick_text: str) -> Optional[Tuple[str, str]]:
        # returns (normalized_pick, market_family) or None
        normalized_pick = clean_pick_string(pick_text)
        prop_parsed = parse_player_prop(normalized_pick)
        if prop_parsed.get("market_type") == "player_prop" and prop_parsed.get("selection") and prop_parsed.get("line"):
            return normalized_pick, "player_prop"
        std_parsed = parse_standard_market(normalized_pick)
        if std_parsed.get("market_type") and (std_parsed.get("selection") or std_parsed.get("side")):
            if std_parsed.get("market_type") == "moneyline":
                team_code = std_parsed.get("selection").replace("_ml", "").upper() if std_parsed.get("selection") else None
                odds = std_parsed.get("odds")
                if team_code:
                    normalized_pick = normalize_moneyline_pick(team_code, odds)
            return normalized_pick, "standard"
        return None

    records: List[RawPickRecord] = []
    seen: set[str] = set()
    candidate_samples: List[str] = []
    picks_dom = 0
    picks_regex = 0

    for card in cards:
        text_all = card.get_text(" ", strip=True)
        if not text_all:
            mark_excluded("empty_card")
            continue
        expert_name = extract_expert_from_card(card)
        pick_texts = extract_pick_texts(card)
        if not pick_texts:
            mark_excluded("no_pick_text")
            continue
        for raw_pick in pick_texts:
            validated = validate_pick(raw_pick)
            if not validated:
                continue
            normalized_pick, market_family = validated
            if normalized_pick in seen:
                continue
            seen.add(normalized_pick)
            picks_dom += 1
            source_surface = f"{sport.lower()}_matchup_picks"
            record = RawPickRecord(
                source_id="covers",
                source_surface=source_surface,
                sport=sport,
                market_family=market_family,
                observed_at_utc=observed_at_utc.isoformat(),
                canonical_url=canonical_url,
                raw_pick_text=normalized_pick,
                raw_block=text_all,
                raw_fingerprint=sha256_digest(f"covers|{source_surface}|{canonical_url}|{normalized_pick}"),
                expert_name=expert_name,
            )
            records.append(record)
            if len(candidate_samples) < 10:
                candidate_samples.append(normalized_pick)

    # Regex fallback if no DOM picks found
    if not records:
        pick_made_matches = list(re.finditer(r"Pick made:", html, re.IGNORECASE))
        for m in pick_made_matches:
            start = max(0, m.start() - 2000)
            end = min(len(html), m.end() + 2000)
            window = BeautifulSoup(html[start:end], "html.parser").get_text(" ", strip=True)
            for token in window.split("."):
                validated = validate_pick(token)
                if not validated:
                    continue
                normalized_pick, market_family = validated
                if normalized_pick in seen:
                    continue
                seen.add(normalized_pick)
                picks_regex += 1
                source_surface = f"{sport.lower()}_matchup_picks"
                record = RawPickRecord(
                    source_id="covers",
                    source_surface=source_surface,
                    sport=sport,
                    market_family=market_family,
                    observed_at_utc=observed_at_utc.isoformat(),
                    canonical_url=canonical_url,
                    raw_pick_text=normalized_pick,
                    raw_block=token.strip(),
                    raw_fingerprint=sha256_digest(f"covers|{source_surface}|{canonical_url}|{normalized_pick}"),
                    expert_name=None,
                )
                records.append(record)

    debug_stats.update(
        {
            "total_cards_found": len(cards),
            "analyst_cards_included": len(records),
            "cards_excluded_reason_counts": card_reasons_excluded,
            "picks_extracted_dom": picks_dom,
            "picks_extracted_regex": picks_regex,
            "validation_reject_counts": {},  # placeholder for future granular reasons
            "candidate_samples": candidate_samples,
        }
    )

    return records


def normalize_covers_pick(raw_pick: RawPickRecord, home_team: str, away_team: str, event_start_time_utc: Optional[datetime] = None, sport: str = NBA_SPORT) -> dict:
    observed_dt = datetime.fromisoformat(raw_pick.observed_at_utc)
    event_time = event_start_time_utc or observed_dt
    if isinstance(event_time, str):
        event_time = datetime.fromisoformat(event_time)
    if isinstance(event_time, datetime) and event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=timezone.utc)
    event = resolve_event(sport, away_team=away_team, home_team=home_team, observed_at_utc=event_time)
    if event:
        event_copy = dict(event)
        start_time = event_copy.get("event_start_time_utc")
        if isinstance(start_time, datetime):
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=timezone.utc)
            event_copy["event_start_time_utc"] = start_time.isoformat()
        event = event_copy
    else:
        # Coverage pages may lack schedule alignment; fallback keeps consensus intact.
        event = {
            "event_key": build_event_key(event_time, away_team=away_team, home_team=home_team),
            "canonical_event_key": build_canonical_event_key(event_time, away_team=away_team, home_team=home_team),
            "event_start_time_utc": event_time if isinstance(event_time, str) else event_time.isoformat(),
            "home_team": home_team,
            "away_team": away_team,
        }
    market_details = (
        parse_player_prop(raw_pick.raw_pick_text)
        if raw_pick.market_family == "player_prop"
        else parse_standard_market(raw_pick.raw_pick_text)
    )
    expert_name = getattr(raw_pick, "expert_name", None)

    eligible = True
    reason = None
    if market_details.get("ineligibility_reason"):
        eligible = False
        reason = market_details.get("ineligibility_reason")
    elif raw_pick.market_family == "player_prop":
        if not (
            market_details.get("side")
            and market_details.get("selection")
            and market_details.get("line")
            and market_details.get("stat_key")
            and market_details.get("player_key")
        ):
            eligible = False
            reason = "missing_prop_fields"
    else:
        if not (market_details.get("market_type") and market_details.get("side") and market_details.get("selection")):
            eligible = False
            reason = market_details.get("ineligibility_reason") or "missing_market_fields"

    normalized = {
        "provenance": {
            "source_id": raw_pick.source_id,
            "source_surface": raw_pick.source_surface,
            "sport": raw_pick.sport,
            "observed_at_utc": raw_pick.observed_at_utc,
            "canonical_url": raw_pick.canonical_url,
            "raw_fingerprint": raw_pick.raw_fingerprint,
            "raw_pick_text": raw_pick.raw_pick_text,
            "raw_block": raw_pick.raw_block,
            "expert_name": expert_name,
            "expert_handle": getattr(raw_pick, "expert_handle", None),
            "expert_profile": getattr(raw_pick, "expert_profile", None),
            "expert_slug": getattr(raw_pick, "expert_slug", None),
            "matchup_hint": getattr(raw_pick, "matchup_hint", None),
        },
        "event": event,
        "market": {
            "market_type": market_details["market_type"],
            "side": market_details["side"],
            "selection": market_details["selection"],
            "line": market_details["line"],
            "odds": market_details["odds"],
            "stat_key": market_details.get("stat_key"),
            "player_key": market_details.get("player_key"),
        },
        "eligible_for_consensus": eligible,
        "ineligibility_reason": reason,
    }
    return normalized


def ensure_out_dir() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)


def write_json(path: str, payload: Iterable[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        prepared = []
        for item in payload:
            if hasattr(item, "__dataclass_fields__"):
                base = asdict(item)
                extras = {k: v for k, v in getattr(item, "__dict__", {}).items() if k not in base}
                base.update(extras)
                prepared.append(base)
            else:
                prepared.append(item)
        json.dump(prepared, f, ensure_ascii=False, indent=2, default=json_default)


def ingest_covers_games(matchup_urls: Optional[List[str]] = None, schedule=None, debug: bool = False, sport: str = NBA_SPORT) -> None:
    schedule = schedule if schedule is not None else SCHEDULE
    observed_at = datetime.now(timezone.utc)
    all_normalized: List[dict] = []

    # Get sport-specific store
    store = get_data_store(sport)

    entries: List[tuple[str, Optional[str], Optional[str]]] = []
    if matchup_urls:
        entries = [(u, None, None) for u in matchup_urls]
    else:
        discovered = discover_matchup_pick_urls(sport=sport)
        if debug:
            print(f"[DEBUG] discovered_matchup_urls={len(discovered)} sport={sport}")
            for url in discovered[:10]:
                print(f"[DEBUG] matchup_url: {url}")
        entries = [(u, None, None) for u in discovered]
        if not entries and schedule:
            for game in schedule:
                url = get_covers_matchup_url(game.away_team, game.home_team, game.game_date, sport=sport)
                if url:
                    entries.append((url, game.away_team, game.home_team))
    if debug:
        print("[DEBUG] entries_count=", len(entries))
        for ent in entries[:3]:
            print(f"[DEBUG] entry: {ent}")

    ensure_out_dir()
    sport_suffix = sport.lower()
    raw_path = os.path.join(OUT_DIR, f"raw_covers_{sport_suffix}.json")

    with IncrementalJsonWriter(raw_path, games_total=len(entries)) as raw_writer:
        for idx, (url, scheduled_away, scheduled_home) in enumerate(entries):
            try:
                html = fetch_html(url)
            except Exception:
                raw_writer.append_game([])
                continue
            event_start_time_utc = parse_event_start_utc(html, observed_at_utc=observed_at)
            soup = BeautifulSoup(html, "html.parser")
            page_away, page_home = extract_matchup_teams_from_header(soup, store=store)

            away_code = page_away or scheduled_away
            home_code = page_home or scheduled_home
            if scheduled_away and scheduled_home and page_away and page_home:
                if {scheduled_away, scheduled_home} != {page_away, page_home}:
                    raw_writer.append_game([])
                    continue
            if debug and idx < 3:
                print(f"[DEBUG] page[{idx}] detected away={away_code} home={home_code} has_pick_made={bool(re.search(r'Pick made', html, re.IGNORECASE))}")
            if not away_code or not home_code:
                if debug:
                    print(f"[DEBUG] page[{idx}] could not detect matchup teams, url={url}")
                raw_writer.append_game([])
                continue

            if debug and idx == 0:
                print("[DEBUG] === PAGE 0 START ===")
                print(f"[DEBUG] page[{idx}] url={url}")
                print(f"[DEBUG] page[{idx}] html_len={len(html)} has_pick_made={bool(re.search(r'Pick made', html, re.IGNORECASE))}")

            debug_stats = {} if (debug and idx == 0) else None
            raw_records = extract_picks_from_html(
                html,
                canonical_url=url,
                observed_at_utc=observed_at,
                debug=True if idx == 0 else bool(debug_stats),
                debug_stats=debug_stats if idx == 0 else ({} if (debug and idx < 3) else None),
                sport=sport,
            )
            raw_writer.append_game(raw_records)

            normalized_batch: List[dict] = []
            for record in raw_records:
                normalized = normalize_covers_pick(
                    record,
                    home_team=home_code,
                    away_team=away_code,
                    event_start_time_utc=event_start_time_utc,
                    sport=sport,
                )
                normalized_batch.append(normalized)
                all_normalized.append(normalized)

            if debug and idx < 3:
                eligible_count = sum(1 for n in normalized_batch if n.get("eligible_for_consensus"))
                print(f"[DEBUG] page[{idx}] fetched_ok len={len(html)} url={url}")
                print(f"[DEBUG] page[{idx}] matchup detected away={away_code} home={home_code}")
            if debug_stats:
                print(
                    f"[DEBUG] page[{idx}] cards_found={debug_stats.get('total_cards_found', 0)} "
                    f"analyst_cards_included={debug_stats.get('analyst_cards_included', 0)} "
                    f"picks_dom={debug_stats.get('picks_extracted_dom', 0)} "
                    f"picks_regex={debug_stats.get('picks_extracted_regex', 0)} "
                    f"raw_records={len(raw_records)} normalized={len(normalized_batch)} eligible={eligible_count} "
                    f"cards_excluded={debug_stats.get('cards_excluded_reason_counts', {})}"
                )
                if idx < 3 and debug_stats and debug_stats.get("first_card_preview"):
                    preview = debug_stats["first_card_preview"].replace("\n", " ")
                    print(f"[DEBUG] page[{idx}] first_card_preview: {preview}")
                if idx < 3 and debug_stats.get("candidate_samples"):
                    print(f"[DEBUG] page[{idx}] candidate_samples: {debug_stats['candidate_samples']}")
                if len(raw_records) == 0:
                    match = re.search(r"Pick made", html, re.IGNORECASE)
                    if match:
                        start = max(0, match.start() - 120)
                        end = min(len(html), match.end() + 120)
                        snippet = html[start:end].replace("\\n", " ")
                        print(f"[DEBUG] page[{idx}] snippet around 'Pick made': {snippet}")
                    else:
                        print(f"[DEBUG] page[{idx}] contains 'Pick made' text? {bool(match)}")
            if debug and idx == 0:
                import json as _json

                print("[DEBUG] page[0] debug_stats:", _json.dumps(debug_stats or {}, indent=2))
                matches = [m.start() for m in re.finditer(r"Pick made", html, re.IGNORECASE)]
                if matches:
                    pos = matches[0]
                    start = max(0, pos - 2500)
                    end = min(len(html), pos + 2500)
                    window = html[start:end]
                    window_text = BeautifulSoup(window, "html.parser").get_text(" ", strip=True)
                    print(f"[DEBUG] page[0] window_text preview: {window_text[:500]}")
                    prop_re = re.compile(
                        r"[A-Z][a-z]+(?:\\s+[A-Z][a-z]+){1,2}\\s+[ouOU]\\d+(?:\\.\\d+)?\\s+(?:Points|Points Scored|Rebounds|Assists|Total Points|Total Rebounds|Total Assists)",
                        re.IGNORECASE,
                    )
                    total_re = re.compile(r"(?:Over|Under|[ouOU])\\s*\\d+(?:\\.\\d+)?", re.IGNORECASE)
                    spread_re = re.compile(r"\\b[A-Z]{2,3}\\s*[+-]\\d+(?:\\.\\d+)?", re.IGNORECASE)
                    money_re = re.compile(r"Moneyline", re.IGNORECASE)
                    print(
                        f"[DEBUG] page[0] window counts: props={len(list(prop_re.finditer(window_text)))} "
                        f"totals={len(list(total_re.finditer(window_text)))} spreads={len(list(spread_re.finditer(window_text)))} "
                        f"moneyline_labels={len(list(money_re.finditer(window_text)))}"
                    )
                print("[DEBUG] === PAGE 0 END ===")

    all_normalized = dedupe_normalized_bets(all_normalized)
    write_json(os.path.join(OUT_DIR, f"normalized_covers_{sport_suffix}.json"), all_normalized)

    eligible_records = [n for n in all_normalized if n.get("eligible_for_consensus")]
    unique_event_keys = {n.get("event", {}).get("event_key") for n in all_normalized if n.get("event")}
    print(
        json.dumps(
            {
                "sport": sport,
                "matchup_pages_processed": len(entries),
                "raw_records_written": len(all_normalized),
                "normalized_records": len(all_normalized),
                "eligible_records": len(eligible_records),
                "unique_event_keys": len(unique_event_keys),
            }
        )
    )
    print(f"[INGEST] wrote {sport} Covers outputs to OUT_DIR={OUT_DIR}")


# Backward compatibility alias
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest Covers analyst picks.")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")
    parser.add_argument(
        "--sport",
        choices=["NBA", "NCAAB"],
        default="NBA",
        help="Sport to ingest (default: NBA)",
    )
    args = parser.parse_args()
    ingest_covers_games(debug=args.debug, sport=args.sport)
