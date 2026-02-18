from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass, asdict
from datetime import datetime, date, timezone
from typing import Iterable, List, Optional, Tuple
from urllib.parse import urljoin
from zoneinfo import ZoneInfo
from pathlib import Path

OUT_DIR = str(Path(os.getenv("NBA_OUT_DIR", "out")))

import requests
from bs4 import BeautifulSoup
def ensure_out_dir():
    OUT_DIR.mkdir(parents=True, exist_ok=True)


try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
except ImportError:
    sync_playwright = None
    PlaywrightTimeoutError = Exception

from event_resolution import SCHEDULE, build_event_key, build_canonical_event_key, resolve_event
from mappings import map_player, map_prop_stat
from store import NBA_SPORT, NCAAB_SPORT, data_store, get_data_store
from utils import normalize_text

# Sport-specific URL patterns
ACTION_LISTING_URLS = {
    NBA_SPORT: "https://www.actionnetwork.com/picks/game",
    NCAAB_SPORT: "https://www.actionnetwork.com/ncaab/picks",
}

ACTION_GAME_PATTERNS = {
    NBA_SPORT: re.compile(r"/nba-game/[^/]+/\d+"),
    NCAAB_SPORT: re.compile(r"/ncaab-game/[^/]+/\d+"),
}


SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.actionnetwork.com/",
        "Connection": "keep-alive",
    }
)


def fetch_html(url: str) -> str:
    response = SESSION.get(url, timeout=15)
    response.raise_for_status()
    return response.text


# For Playwright: pip install playwright && playwright install chromium
def fetch_html_playwright(url: str, scroll_picks_container: bool = True) -> str:
    if sync_playwright is None:
        raise RuntimeError("playwright is not installed. Install with `pip install playwright` then `playwright install chromium`.")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        page = context.new_page()
        page.goto(url, wait_until="networkidle", timeout=30000)

        # Scroll within the picks container to load all lazy-loaded pick cards
        if scroll_picks_container:
            try:
                # Incrementally scroll the picks container with waits for lazy-loading
                # Each scroll step triggers content loading, so we need pauses
                for scroll_round in range(5):  # Multiple rounds to ensure all content loads
                    found_container = page.evaluate("""
                        () => {
                            // Find scrollable container with pick cards
                            const allDivs = document.querySelectorAll('div');
                            for (const div of allDivs) {
                                const style = window.getComputedStyle(div);
                                const hasOverflow = style.overflowY === 'auto' || style.overflowY === 'scroll';
                                const hasPickCards = div.querySelector('.pick-card__header') !== null;

                                if (hasOverflow && hasPickCards && div.scrollHeight > div.clientHeight) {
                                    // Scroll incrementally - each step is ~1 card height
                                    const scrollStep = 200;
                                    const prevScrollTop = div.scrollTop;
                                    div.scrollTop += scrollStep;
                                    // Return info about whether we scrolled and if there's more
                                    return {
                                        found: true,
                                        scrolledFrom: prevScrollTop,
                                        scrolledTo: div.scrollTop,
                                        scrollHeight: div.scrollHeight,
                                        clientHeight: div.clientHeight,
                                        canScrollMore: div.scrollTop + div.clientHeight < div.scrollHeight
                                    };
                                }
                            }
                            return {found: false};
                        }
                    """)
                    # Wait for lazy content to load after each scroll
                    page.wait_for_timeout(300)

                    if found_container and found_container.get("found"):
                        # Keep scrolling until we've reached the bottom
                        scroll_attempts = 0
                        while found_container.get("canScrollMore") and scroll_attempts < 30:
                            page.evaluate("""
                                () => {
                                    const allDivs = document.querySelectorAll('div');
                                    for (const div of allDivs) {
                                        const style = window.getComputedStyle(div);
                                        const hasOverflow = style.overflowY === 'auto' || style.overflowY === 'scroll';
                                        const hasPickCards = div.querySelector('.pick-card__header') !== null;
                                        if (hasOverflow && hasPickCards && div.scrollHeight > div.clientHeight) {
                                            div.scrollTop += 200;
                                            return;
                                        }
                                    }
                                }
                            """)
                            page.wait_for_timeout(250)  # Wait for content to load
                            scroll_attempts += 1
                            # Check if we can still scroll
                            found_container = page.evaluate("""
                                () => {
                                    const allDivs = document.querySelectorAll('div');
                                    for (const div of allDivs) {
                                        const style = window.getComputedStyle(div);
                                        const hasOverflow = style.overflowY === 'auto' || style.overflowY === 'scroll';
                                        const hasPickCards = div.querySelector('.pick-card__header') !== null;
                                        if (hasOverflow && hasPickCards && div.scrollHeight > div.clientHeight) {
                                            return {
                                                found: true,
                                                canScrollMore: div.scrollTop + div.clientHeight < div.scrollHeight - 10
                                            };
                                        }
                                    }
                                    return {found: false, canScrollMore: false};
                                }
                            """)
                        # After reaching bottom, scroll back to top and to bottom again
                        # This can trigger any remaining lazy loads
                        page.evaluate("""
                            () => {
                                const allDivs = document.querySelectorAll('div');
                                for (const div of allDivs) {
                                    const style = window.getComputedStyle(div);
                                    const hasOverflow = style.overflowY === 'auto' || style.overflowY === 'scroll';
                                    const hasPickCards = div.querySelector('.pick-card__header') !== null;
                                    if (hasOverflow && hasPickCards) {
                                        div.scrollTop = 0;
                                        return;
                                    }
                                }
                            }
                        """)
                        page.wait_for_timeout(300)
                        page.evaluate("""
                            () => {
                                const allDivs = document.querySelectorAll('div');
                                for (const div of allDivs) {
                                    const style = window.getComputedStyle(div);
                                    const hasOverflow = style.overflowY === 'auto' || style.overflowY === 'scroll';
                                    const hasPickCards = div.querySelector('.pick-card__header') !== null;
                                    if (hasOverflow && hasPickCards) {
                                        div.scrollTop = div.scrollHeight;
                                        return;
                                    }
                                }
                            }
                        """)
                        page.wait_for_timeout(500)
                        break  # Done with scroll rounds
                    else:
                        # No scrollable container found, try scrolling main page
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        page.wait_for_timeout(500)
            except Exception as e:
                # If scrolling fails, continue with whatever content we have
                pass

        content = page.content()
        context.close()
        browser.close()
        return content


def fetch_html_in_context(context, url: str, wait_until: str = "domcontentloaded", timeout_ms: int = 30000) -> str:
    """
    Fetch a rendered HTML document using an existing Playwright browser context.
    """
    if context is None:
        raise RuntimeError("fetch_html_in_context requires an existing Playwright context.")
    page = context.new_page()
    try:
        try:
            page.goto(url, wait_until=wait_until, timeout=timeout_ms)
        except PlaywrightTimeoutError:
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(750)
        return page.content()
    except KeyboardInterrupt:
        raise
    finally:
        try:
            page.close()
        except Exception:
            pass


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
    event_start_time_utc: Optional[datetime] = None
    player_hint: Optional[str] = None
    stat_hint: Optional[str] = None
    line_hint: Optional[float] = None
    odds_hint: Optional[str] = None
    expert_name: Optional[str] = None
    expert_handle: Optional[str] = None
    expert_profile: Optional[str] = None
    expert_slug: Optional[str] = None
    tailing_handle: Optional[str] = None


def sha256_digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def get_action_game_url(away_team: str, home_team: str, game_date: date) -> Optional[str]:
    """
    Hardcoded URL mapping for stub schedule games.
    TODO: Replace with dynamic mapping once available.
    """
    key = (away_team, home_team, game_date.isoformat())
    mapping = {
        ("NYK", "GSW", "2026-01-15"): "https://www.actionnetwork.com/nba-game/knicks-warriors-score-odds-january-15-2026/263220",
        ("OKC", "HOU", "2026-01-15"): "https://www.actionnetwork.com/nba-game/thunder-rockets-score-odds-january-15-2026/263216",
    }
    return mapping.get(key)


def fetch_expert_picks_html(url: str) -> str:
    return fetch_html_playwright(url)


def discover_action_game_urls(listing_url: str = None, sport: str = NBA_SPORT) -> List[str]:
    if listing_url is None:
        listing_url = ACTION_LISTING_URLS.get(sport, ACTION_LISTING_URLS[NBA_SPORT])
    try:
        listing_html = fetch_html(listing_url)
    except Exception:
        return []
    soup = BeautifulSoup(listing_html, "html.parser")
    pattern = ACTION_GAME_PATTERNS.get(sport, ACTION_GAME_PATTERNS[NBA_SPORT])
    urls: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not pattern.search(href):
            continue
        full = urljoin(listing_url, href)
        if full not in urls:
            urls.append(full)
    return urls


def parse_game_date_from_url(url: str) -> Optional[date]:
    match = re.search(r"([a-z]+-\d{1,2}-\d{4})", url, re.IGNORECASE)
    if not match:
        return None
    month_day_year = match.group(1).replace("-", " ")
    try:
        return datetime.strptime(month_day_year, "%B %d %Y").date()
    except ValueError:
        return None


def parse_game_start_utc(html: str, url: str, schedule_game: Optional[object], observed_at_utc: datetime) -> datetime:
    """
    Prefer the game date/time embedded in the Action page, fall back to schedule or observed timestamp.
    """
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    eastern = ZoneInfo("America/New_York")

    # Try to extract date and time from visible text like "January 15, 2026 10:00 PM ET"
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

    # Try combining a date from the URL with a time in the page text.
    url_date = parse_game_date_from_url(url)
    time_match = re.search(r"(\d{1,2}:\d{2})\s*(AM|PM)", text, re.IGNORECASE)
    if url_date and time_match:
        clock, ampm = time_match.groups()
        try:
            local_dt = datetime.strptime(
                f"{url_date.strftime('%B %d %Y')} {clock} {ampm.upper()}",
                "%B %d %Y %I:%M %p",
            ).replace(tzinfo=eastern)
            return local_dt.astimezone(timezone.utc)
        except ValueError:
            pass

    # Fall back to schedule time but adjust the date from the URL if present.
    if schedule_game:
        start = schedule_game.start_time_utc
        if isinstance(start, datetime):
            if start.tzinfo is None:
                start = start.replace(tzinfo=timezone.utc)
            if url_date:
                start = datetime.combine(url_date, start.time(), tzinfo=start.tzinfo)
            return start

    # Last resort: use observed timestamp so event resolution does not end up empty.
    return observed_at_utc if observed_at_utc.tzinfo else observed_at_utc.replace(tzinfo=timezone.utc)


def extract_picks_from_html(
    html: str,
    canonical_url: str,
    observed_at_utc: datetime,
    debug: bool = False,
    sport: str = NBA_SPORT,
) -> Tuple[List[RawPickRecord], List[dict]]:
    soup = BeautifulSoup(html, "html.parser")
    debug = debug or bool(os.environ.get("ACTION_DEBUG"))

    def normalize_ou_prefix(text_val: str) -> str:
        def repl(match: re.Match) -> str:
            prefix = match.group(1).lower()
            number = match.group(2)
            word = "over" if prefix == "o" else "under"
            return f"{word} {number}"

        return re.sub(r"\b([ou])(\d+(?:\.\d+)?)\b", repl, text_val, flags=re.IGNORECASE)

    def clean_pick_text(raw: str) -> str:
        cleaned = raw.split("http", 1)[0]
        cleaned = re.sub(r"[⭐★]", "", cleaned)
        cleaned = " ".join(cleaned.split())
        return cleaned.strip()

    player_prop_pattern = re.compile(
        r"(?P<player>[A-Z]\.[A-Za-z]+)\s+(?P<ou>[ouOU])(?P<line>\d+(?:\.\d+)?)\s+(?P<stat>Pts\+Rebs\+Asts|Pts\+Rebs|Pts\+Asts|Rebs\+Asts|Pts|Rebs|Asts)\s+(?P<odds>[+-]\d{3,})"
    )
    spread_pattern = re.compile(r"\b(?P<team>[A-Z]{2,3})\s+(?P<line>[+-]\d+(?:\.\d+)?)\s+(?P<odds>[+-]\d{3,})\b")
    total_pattern = re.compile(r"\b(?P<ou>Over|Under)\s+(?P<line>\d+(?:\.\d+)?)\s+(?P<odds>[+-]\d{3,})", re.IGNORECASE)
    moneyline_pattern = re.compile(r"\b(?P<team>[A-Z]{2,3})\s+(?P<odds>[+-]\d{2,4})\b(?!\s*[+-]\d)")
    unit_pattern = re.compile(r"(\d+(?:\.\d+)?)u", re.IGNORECASE)
    pick_text_pattern = re.compile(
        r"(?:[A-Z]\.[A-Za-z]+\s+[ouOU]\d+(?:\.\d+)?\s+(?:Pts\+Rebs\+Asts|Pts\+Rebs|Pts\+Asts|Rebs\+Asts|Pts|Rebs|Asts|3pt\s*M|Threes)\s+[+-]\d{3,}"
        r"|(?:Over|Under)\s+\d+(?:\.\d+)?\s+[+-]\d{3,}"
        r"|[A-Z]{2,3}\s+[+-]\d+(?:\.\d+)?\s+[+-]\d{3,}"
        r"|[A-Z]{2,3}\s+[ouOU]\d+(?:\.\d+)?\s+[+-]\d{3,}"  # Team totals like "OKC u114.5 -115"
        r"|[A-Z]{2,3}\s+[+-]\d{2,4}(?!\s*[+-]\d))",
        re.IGNORECASE,
    )

    generic_name_tokens = {"sports", "picks", "betting", "analysis", "follow", "instagram", "expert", "senior", "editor", "odds"}

    def is_generic_name(name: Optional[str]) -> bool:
        if not name:
            return True
        lowered = name.strip().lower()
        if not lowered or len(lowered) < 3:
            return True
        for token in generic_name_tokens:
            if token in lowered:
                return True
        return False

    def clean_expert_name(name: Optional[str]) -> Optional[str]:
        if not name:
            return None
        name = re.sub(r"\bon\s+instagram\b", "", name, flags=re.IGNORECASE)
        name = re.sub(r"\bfollow\b", "", name, flags=re.IGNORECASE)
        name = re.sub(r"\blast\s*\d{1,3}d.*", "", name, flags=re.IGNORECASE)
        name = re.sub(r"\d+(?:\.\d+)?u", "", name, flags=re.IGNORECASE)
        name = re.sub(r"[+-]\d{2,4}", "", name)
        name = re.sub(r"[|,/]", " ", name)
        name = " ".join(name.strip(" -").split())
        if is_generic_name(name):
            return None
        return name

    def extract_expert_meta(container: BeautifulSoup) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        header = container.select_one(".pick-card__header")
        expert_a = header.select_one('.pick-card__expert-info a[href^="/picks/profile/"]') if header else None
        expert_name = clean_expert_name(expert_a.get_text(" ", strip=True)) if expert_a else None
        if is_generic_name(expert_name):
            expert_name = None
        expert_profile = expert_a["href"] if expert_a and expert_a.has_attr("href") else None
        expert_slug = None
        if expert_profile:
            expert_slug = expert_profile.rstrip("/").split("/")[-1]
        return expert_name, expert_profile, expert_slug

    def extract_pick_strings(card: BeautifulSoup) -> List[dict]:
        picks: List[dict] = []
        for pick_el in card.select(".base-pick__pick"):
            pick_name_el = pick_el.select_one(".base-pick__name")
            if not pick_name_el:
                continue
            pick_name = pick_name_el.get_text(" ", strip=True)
            odds_el = pick_el.select_one(".base-pick__secondary-text")
            odds = odds_el.get_text(" ", strip=True) if odds_el else ""
            pick_text = f"{pick_name} {odds}".strip()
            units_el = pick_el.select_one(".base-pick__units")
            notes_el = pick_el.find_next_sibling(class_="base-pick__notes") or pick_el.parent.select_one(".base-pick__notes")
            picks.append(
                {
                    "text": pick_text,
                    "pick_name": pick_name,
                    "odds": odds,
                    "units": units_el.get_text(" ", strip=True) if units_el else None,
                    "notes": notes_el.get_text(" ", strip=True) if notes_el else None,
                    "context": pick_el.get_text(" ", strip=True),
                }
            )
        return picks

    def find_pick_containers_with_experts() -> List[Tuple[BeautifulSoup, Optional[str], Optional[str], Optional[str]]]:
        """
        Find all pick containers and associate them with expert info.
        Returns list of (container, expert_name, expert_profile, expert_slug) tuples.

        Action Network structures picks as sibling .euiy7v30 containers.
        The first container for each expert has a .pick-card__header with expert info.
        Subsequent containers (without headers) belong to the same expert until a new header.
        """
        results: List[Tuple[BeautifulSoup, Optional[str], Optional[str], Optional[str]]] = []

        # Find all euiy7v30 containers (the pick container class)
        all_containers = soup.select(".euiy7v30")

        if not all_containers:
            # Fallback to old method
            headers = soup.select(".pick-card__header")
            for header in headers:
                card = header.parent
                if not card:
                    continue
                card_text = card.get_text(" ", strip=True)
                if not card_text or len(card_text) < 20:
                    continue
                if not pick_text_pattern.search(card_text):
                    continue
                expert_name, expert_profile, expert_slug = extract_expert_meta(card)
                results.append((card, expert_name, expert_profile, expert_slug))
            return results

        current_expert_name: Optional[str] = None
        current_expert_profile: Optional[str] = None
        current_expert_slug: Optional[str] = None

        for container in all_containers:
            # Check if this container has a pick-card__header (marks new expert)
            header = container.select_one(".pick-card__header")
            if header:
                # Extract expert info from the header
                links = header.select('a[href^="/picks/profile/"]')
                for link in links:
                    text = link.get_text(" ", strip=True)
                    if text:  # Only use links with actual text (skip avatar links)
                        current_expert_name = clean_expert_name(text)
                        current_expert_profile = link.get("href")
                        current_expert_slug = current_expert_profile.rstrip("/").split("/")[-1] if current_expert_profile else None
                        break

            # Check if this container has picks
            picks = container.select(".base-pick__pick")
            if picks:
                # Verify at least one pick matches our patterns
                container_text = container.get_text(" ", strip=True)
                if pick_text_pattern.search(container_text):
                    results.append((container, current_expert_name, current_expert_profile, current_expert_slug))

        return results

    records: List[RawPickRecord] = []
    card_infos: List[dict] = []
    seen_pick_texts: set[str] = set()
    debug_samples: List[str] = []
    containers_with_experts = find_pick_containers_with_experts()

    # Track unique experts for card_infos
    seen_experts: set[str] = set()

    for idx, (container, container_expert_name, container_expert_profile, container_expert_slug) in enumerate(containers_with_experts):
        container_text = container.get_text(" ", strip=True)
        picks = extract_pick_strings(container)

        # Only add to card_infos once per expert
        expert_key = container_expert_profile or container_expert_name or f"unknown_{idx}"
        if expert_key not in seen_experts:
            seen_experts.add(expert_key)
            card_infos.append(
                {
                    "expert_name": container_expert_name,
                    "expert_profile": container_expert_profile,
                    "expert_slug": container_expert_slug,
                    "picks": [p["text"] for p in picks[:2]],
                }
            )

        for pick in picks:
            pick_text = pick["text"]
            cleaned = clean_pick_text(pick_text)
            odds_only = pick.get("odds")
            normalized_pick = " ".join(normalize_ou_prefix(cleaned).split())
            if odds_only and odds_only not in normalized_pick:
                normalized_pick = f"{normalized_pick} {odds_only}"
            if not normalized_pick or normalized_pick in seen_pick_texts:
                continue
            seen_pick_texts.add(normalized_pick)

            tailing_handle = None
            note_text = pick.get("notes")
            if note_text:
                hm = re.search(r"@([A-Za-z0-9_]{2,})", note_text)
                if hm:
                    tailing_handle = f"@{hm.group(1)}"

            units_val = pick.get("units")
            stake_hint = None
            if units_val and units_val.strip().lower().endswith("u"):
                stake_hint = units_val.strip()

            context = pick.get("context") or pick_text
            if stake_hint:
                context = f"{context}\n[stake_hint={stake_hint}]"

            market_family = "player_prop" if map_prop_stat(normalized_pick) else "standard"
            source_surface = f"{sport.lower()}_game_expert_picks"
            fingerprint_source = f"action|{source_surface}|{canonical_url}|{normalized_pick}"
            record = RawPickRecord(
                source_id="action",
                source_surface=source_surface,
                sport=sport,
                market_family=market_family,
                observed_at_utc=observed_at_utc.isoformat(),
                canonical_url=canonical_url,
                raw_pick_text=normalized_pick,
                raw_block=context,
                raw_fingerprint=sha256_digest(fingerprint_source),
                expert_name=container_expert_name,
                expert_handle=None,
                expert_profile=container_expert_profile,
                expert_slug=container_expert_slug,
                tailing_handle=tailing_handle,
            )
            records.append(record)
            if len(debug_samples) < 10:
                debug_samples.append(normalized_pick)

        if debug and idx < 3:
            first_pick = picks[0]["text"] if picks else None
            first_odds = picks[0].get("odds") if picks else None
            print(
                f"[DEBUG] action card idx={idx} expert_name={container_expert_name} profile={container_expert_profile} "
                f"picks={len(picks)} first_pick={first_pick} odds={first_odds}"
            )

    if debug:
        print(f"[DEBUG] action cards_found={len(containers_with_experts)} matches={len(records)} samples={debug_samples}")

    return records, card_infos


def map_team_text_to_code(text: str, store=None) -> Optional[str]:
    store = store or data_store
    normalized = normalize_text(text)
    matches = store.lookup_team_code(normalized)
    if len(matches) == 1:
        return next(iter(matches))
    if len(normalized) == 3 and normalized.upper() in store.teams:
        return normalized.upper()
    return None


def parse_teams_from_slug(url: str, sport: str = NBA_SPORT, store=None) -> Tuple[Optional[str], Optional[str]]:
    # Match both NBA and NCAAB game URL patterns
    match = re.search(r"/(?:nba-game|ncaab-game)/([^/]+)/\d+", url)
    if not match:
        return None, None
    slug = match.group(1)
    slug_prefix = re.split("-(score|odds|lines|line|preview|analysis|prediction|picks|betting)", slug, maxsplit=1)[0]
    tokens = [t for t in slug_prefix.split("-") if t]
    if len(tokens) < 2:
        return None, None

    store = store or get_data_store(sport)
    for split_idx in range(1, len(tokens)):
        away_raw = " ".join(tokens[:split_idx])
        home_raw = " ".join(tokens[split_idx:])
        away_code = map_team_text_to_code(away_raw, store=store)
        home_code = map_team_text_to_code(home_raw, store=store)
        if away_code and home_code and away_code != home_code:
            return away_code, home_code

    return None, None


def parse_teams_from_page(soup: BeautifulSoup, sport: str = NBA_SPORT, store=None) -> Tuple[Optional[str], Optional[str]]:
    store = store or get_data_store(sport)
    candidates = []
    for tag in soup.find_all(["h1", "h2", "h3", "title"]):
        txt = tag.get_text(" ", strip=True)
        if "vs" in txt.lower() or "@" in txt:
            candidates.append(txt)
    for text in candidates:
        match = re.search(r"([A-Za-z\.\s]{2,})\s*(?:vs|@)\s*([A-Za-z\.\s]{2,})", text, re.IGNORECASE)
        if not match:
            continue
        away_raw, home_raw = match.group(1), match.group(2)
        away_code = map_team_text_to_code(away_raw, store=store)
        home_code = map_team_text_to_code(home_raw, store=store)
        if away_code and home_code and away_code != home_code:
            return away_code, home_code
    return None, None


def parse_standard_market(raw_pick_text: str) -> dict:
    lower = raw_pick_text.lower()
    first_float_match = re.search(r"([+-]?\d+(?:\.\d+)?)", raw_pick_text)
    odds_match = re.search(r"([+-]\d{3,})", raw_pick_text)
    team_code_match = re.search(r"\b([A-Z]{3})\b", raw_pick_text)
    team_code = team_code_match.group(1) if team_code_match else None
    signed_numbers = re.findall(r"[+-]\d+(?:\.\d+)?", raw_pick_text)

    if ("over" in lower or "under" in lower) and first_float_match:
        side = "over" if "over" in lower else "under" if "under" in lower else None
        return {
            "market_type": "total",
            "side": side,
            "selection": "game_total",
            "line": float(first_float_match.group(1)) if first_float_match else None,
            "odds": odds_match.group(1) if odds_match else None,
            "eligible_for_consensus": True,
            "ineligibility_reason": None,
            "team_code": None,
        }

    if team_code:
        # Check for team total first (e.g., "OKC u114.5 -115" or "MIL over 115.5 -110")
        team_total_match = re.search(rf"{team_code}\s+([ouOU])(\d+(?:\.\d+)?)", raw_pick_text)
        if team_total_match:
            ou_char = team_total_match.group(1).lower()
            side = "over" if ou_char == "o" else "under"
            line_val = float(team_total_match.group(2))
            return {
                "market_type": "team_total",
                "side": side,
                "selection": f"{team_code}_team_total",
                "line": line_val,
                "odds": odds_match.group(1) if odds_match else None,
                "eligible_for_consensus": True,
                "ineligibility_reason": None,
                "team_code": team_code,
            }

        if odds_match and len(signed_numbers) == 1:
            return {
                "market_type": "moneyline",
                "side": "team",
                "selection": f"{team_code}_ml",
                "line": None,
                "odds": odds_match.group(1),
                "eligible_for_consensus": True,
                "ineligibility_reason": None,
                "team_code": team_code,
            }

        spread_match = re.search(rf"{team_code}\s*([+-]\d+(?:\.\d+)?)", raw_pick_text)
        if spread_match:
            line_val = float(spread_match.group(1))
            return {
                "market_type": "spread",
                "side": "team",
                "selection": f"{team_code}_spread",
                "line": line_val,
                "odds": odds_match.group(1) if odds_match else None,
                "eligible_for_consensus": True,
                "ineligibility_reason": None,
                "team_code": team_code,
            }

        if odds_match:
            return {
                "market_type": "moneyline",
                "side": "team",
                "selection": f"{team_code}_ml",
                "line": None,
                "odds": odds_match.group(1),
                "eligible_for_consensus": True,
                "ineligibility_reason": None,
                "team_code": team_code,
            }

    return {
        "market_type": None,
        "side": None,
        "selection": None,
        "line": None,
        "odds": None,
        "eligible_for_consensus": False,
        "ineligibility_reason": "ambiguous_market",
        "team_code": None,
    }


def parse_player_prop(raw_pick_text: str, sport: str = NBA_SPORT) -> dict:
    stat_key = map_prop_stat(raw_pick_text)
    if not stat_key:
        return {
            "market_type": "player_prop",
            "side": None,
            "selection": None,
            "line": None,
            "odds": None,
            "eligible_for_consensus": False,
            "ineligibility_reason": "missing_stat_key",
            "stat_key": None,
            "player_key": None,
        }

    player_key = map_player(raw_pick_text)
    lower = raw_pick_text.lower()
    side = None
    if "over" in lower or " o " in lower:
        side = "player_over"
    elif "under" in lower or " u " in lower:
        side = "player_under"

    first_float_match = re.search(r"(\d+(?:\.\d+)?)", lower)
    selection = None
    # Check if player_key starts with any known sport prefix
    if player_key and ":" in player_key and stat_key and side:
        player_slug = player_key.split(":", 1)[1]
        selection = f"{player_slug}::{stat_key}::{side.split('_')[1].upper()}"

    if not stat_key or not side or not first_float_match:
        return {
            "market_type": "player_prop",
            "side": side,
            "selection": selection,
            "line": float(first_float_match.group(1)) if first_float_match else None,
            "odds": None,
            "eligible_for_consensus": False,
            "ineligibility_reason": "missing_prop_fields",
            "stat_key": stat_key,
            "player_key": player_key,
        }

    return {
        "market_type": "player_prop",
        "side": side,
        "selection": selection,
        "line": float(first_float_match.group(1)),
        "odds": None,
        "eligible_for_consensus": True,
        "ineligibility_reason": None,
        "stat_key": stat_key,
        "player_key": player_key,
    }


def normalize_pick(raw_pick: RawPickRecord, home_team: str, away_team: str, stats: Optional[dict] = None, sport: str = NBA_SPORT) -> dict:
    observed_dt = datetime.fromisoformat(raw_pick.observed_at_utc)
    event_time = raw_pick.event_start_time_utc or observed_dt
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
        # Schedule drift or new-day runs can miss resolve_event; construct a stable event payload so consensus stays intact.
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

    eligible = True
    reason = None
    if market_details.get("ineligibility_reason"):
        eligible = False
        reason = market_details.get("ineligibility_reason")
    elif raw_pick.market_family == "player_prop":
        if not (market_details.get("side") and market_details.get("selection") and market_details.get("line") and market_details.get("stat_key") and market_details.get("player_key")):
            eligible = False
            reason = "missing_prop_fields"
    else:
        if not (market_details.get("market_type") and market_details.get("side") and market_details.get("selection")):
            eligible = False
            reason = market_details.get("ineligibility_reason") or "missing_market_fields"
        elif market_details.get("market_type") == "total":
            side_val = market_details.get("side")
            selection_val = market_details.get("selection")
            line_val = market_details.get("line")
            side_ok = isinstance(side_val, str) and side_val.lower() in {"over", "under"}
            sel_ok = isinstance(selection_val, str) and ("total" in selection_val.lower())
            line_ok = isinstance(line_val, (int, float))
            teams_ok = bool(home_team and away_team)
            if not (side_ok and sel_ok and line_ok and teams_ok):
                eligible = False
                reason = "bad_total"
        else:
            team_code = market_details.get("team_code")
            if not team_code and market_details.get("selection"):
                sel = market_details["selection"]
                if isinstance(sel, str) and "_" in sel:
                    team_code = sel.split("_", 1)[0]
            if team_code and team_code not in {home_team, away_team}:
                eligible = False
                reason = "team_not_in_game"
                if stats is not None:
                    stats["team_not_in_game"] = stats.get("team_not_in_game", 0) + 1

    normalized = {
        "provenance": {
            "source_id": raw_pick.source_id,
            "source_surface": raw_pick.source_surface,
            "sport": raw_pick.sport,
            "observed_at_utc": raw_pick.observed_at_utc,
            "canonical_url": raw_pick.canonical_url,
            "raw_fingerprint": raw_pick.raw_fingerprint,
            "expert_name": raw_pick.expert_name,
            "expert_handle": raw_pick.expert_handle,
            "expert_profile": raw_pick.expert_profile,
            "expert_slug": raw_pick.expert_slug,
            "tailing_handle": raw_pick.tailing_handle,
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
        "eligibility": {
            "eligible_for_consensus": eligible,
            "ineligibility_reason": reason,
        },
    }
    return normalized


def ensure_out_dir() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)


def json_default(o):
    if isinstance(o, datetime):
        return o.isoformat()
    return str(o)


def write_json(path: str, payload: Iterable[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        prepared = []
        for item in payload:
            prepared.append(asdict(item) if hasattr(item, "__dataclass_fields__") else item)
        json.dump(prepared, f, ensure_ascii=False, indent=2, default=json_default)


def dedupe_normalized_bets(bets: List[dict]) -> List[dict]:
    seen = set()
    deduped: List[dict] = []
    for bet in bets:
        prov = bet.get("provenance", {})
        event = bet.get("event", {}) or {}
        market = bet.get("market", {}) or {}
        key = (
            event.get("event_key"),
            market.get("market_type"),
            market.get("selection"),
            market.get("line"),
            market.get("odds"),
            prov.get("source_id"),
            prov.get("source_surface"),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(bet)
    return deduped


def ingest_action_games(schedule=None, sport: str = NBA_SPORT, debug: bool = False) -> None:
    schedule = schedule if schedule is not None else SCHEDULE
    observed_at = datetime.now(timezone.utc)
    all_raw: List[RawPickRecord] = []
    all_normalized: List[dict] = []
    stats = {
        "team_not_in_game": 0,
        "cards_found": 0,
        "raw_picks_total": 0,
        "normalized_total": 0,
        "normalized_eligible": 0,
    }

    # Get sport-specific store
    store = get_data_store(sport)

    game_urls = discover_action_game_urls(sport=sport)
    if debug:
        print(f"[DEBUG] discovered {len(game_urls)} {sport} game URLs")
        for url in game_urls[:5]:
            print(f"[DEBUG] game_url: {url}")

    for url in game_urls:
        try:
            html = fetch_expert_picks_html(url)
        except Exception:
            continue
        event_start_time_utc = parse_game_start_utc(
            html=html, url=url, schedule_game=None, observed_at_utc=observed_at
        )
        soup = BeautifulSoup(html, "html.parser")
        away_team, home_team = parse_teams_from_page(soup, sport=sport, store=store)
        if not (away_team and home_team):
            slug_away, slug_home = parse_teams_from_slug(url, sport=sport, store=store)
            away_team = away_team or slug_away
            home_team = home_team or slug_home
        if not (away_team and home_team):
            continue

        raw_records, card_infos = extract_picks_from_html(
            html,
            canonical_url=url,
            observed_at_utc=observed_at,
            debug=debug or bool(os.environ.get("ACTION_DEBUG")),
            sport=sport,
        )
        # Debug: show picks per game
        game_slug_match = re.search(r"/(?:nba-game|college-basketball-game)/([^/]+)/", url)
        game_slug = game_slug_match.group(1) if game_slug_match else url
        print(f"[DEBUG] {game_slug}: found {len(card_infos)} expert cards, {len(raw_records)} picks")
        all_raw.extend(raw_records)
        stats["cards_found"] += len(card_infos)
        stats["raw_picks_total"] += len(raw_records)

        for record in raw_records:
            record.event_start_time_utc = event_start_time_utc
            normalized = normalize_pick(record, home_team=home_team, away_team=away_team, stats=stats, sport=sport)
            all_normalized.append(normalized)
            stats["normalized_total"] += 1
            if normalized.get("eligible_for_consensus"):
                stats["normalized_eligible"] += 1

        is_cle_cha = {away_team, home_team} == {"CLE", "CHA"}
        if is_cle_cha:
            print(f"[DEBUG] CLE@CHA expert cards found: {len(card_infos)}")
            for idx, card in enumerate(card_infos[:5]):
                picks_preview = card.get("picks", [])
                print(
                    f"[DEBUG] card {idx} expert_name={card.get('expert_name')} "
                    f"handle={card.get('expert_handle')} picks={picks_preview}"
                )

    all_normalized = dedupe_normalized_bets(all_normalized)
    ensure_out_dir()
    sport_suffix = sport.lower()
    write_json(os.path.join(OUT_DIR, f"raw_action_{sport_suffix}.json"), (asdict(r) for r in all_raw))
    write_json(os.path.join(OUT_DIR, f"normalized_action_{sport_suffix}.json"), all_normalized)
    print(
        f"[DEBUG] cards_found={stats['cards_found']} raw_picks_total={stats['raw_picks_total']} "
        f"normalized_total={stats['normalized_total']} normalized_eligible={stats['normalized_eligible']} "
        f"team_not_in_game_rejected={stats.get('team_not_in_game', 0)}"
    )
    print(f"[INGEST] wrote {sport} Action outputs to OUT_DIR={OUT_DIR}")


# Backward compatibility alias
def ingest_action_nba_games(schedule=None) -> None:
    """Backward compatibility wrapper for ingest_action_games with NBA sport."""
    ingest_action_games(schedule=schedule, sport=NBA_SPORT)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Ingest Action Network analyst picks.")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")
    parser.add_argument(
        "--sport",
        choices=["NBA", "NCAAB"],
        default="NBA",
        help="Sport to ingest (default: NBA)",
    )
    args = parser.parse_args()
    ingest_action_games(sport=args.sport, debug=args.debug)
