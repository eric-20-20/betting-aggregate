#!/usr/bin/env python3
"""
SportsLine NBA picks historical backfill script.

Scrapes historical picks from SportsLine's /insiders/ articles by:
1. Loading the insiders page
2. Clicking "Load More" to paginate through articles
3. Filtering for NBA prediction articles
4. Navigating into each article to extract the actual picks

Usage:
    python3 scripts/backfill_sportsline_history.py [options]

Options:
    --storage PATH      Path to Playwright storage state (default: data/sportsline_storage_state.json)
    --max-articles N    Maximum articles to process (default: 100)
    --delay SECONDS     Delay between requests (default: 1.5)
    --output PATH       Output JSONL file (default: out/raw_sportsline_nba_history.jsonl)
    --state PATH        State file for resume support (default: out/sportsline_backfill_state.json)
    --debug             Enable debug output

Output:
    - out/raw_sportsline_nba_history.jsonl (one record per pick)
    - out/sportsline_backfill_state.json (for resuming)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# Add repo root to path
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

try:
    from playwright.sync_api import sync_playwright, Page, BrowserContext
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
except ImportError:
    sync_playwright = None
    PlaywrightTimeoutError = Exception

from store import data_store
from utils import normalize_text

DEFAULT_STORAGE_STATE = "data/sportsline_storage_state.json"
DEFAULT_OUTPUT = "out/raw_sportsline_nba_history.jsonl"
DEFAULT_STATE_FILE = "out/sportsline_backfill_state.json"
INSIDERS_URL = "https://www.sportsline.com/insiders/"

# NBA team patterns to identify NBA articles
NBA_TEAMS = {
    "lakers", "celtics", "warriors", "heat", "nets", "knicks", "bulls", "mavs",
    "mavericks", "suns", "bucks", "sixers", "76ers", "raptors", "nuggets",
    "thunder", "rockets", "jazz", "kings", "hawks", "hornets", "magic",
    "pistons", "pacers", "cavaliers", "cavs", "clippers", "grizzlies",
    "pelicans", "spurs", "blazers", "trail blazers", "timberwolves", "wolves",
    "wizards", "nba"
}

# Article URL patterns for NBA predictions
NBA_ARTICLE_PATTERNS = [
    r"-nba-picks-",
    r"-prediction-odds-line-.*nba",
    r"nba-.*-prediction",
]


@dataclass
class ArticleInfo:
    url: str
    title: str
    date_text: Optional[str] = None
    matchup: Optional[str] = None


@dataclass
class HistoricalPick:
    source_id: str
    source_surface: str
    sport: str
    market_family: str
    observed_at_utc: str
    article_url: str
    article_title: str
    article_date: Optional[str]
    matchup_hint: Optional[str]
    away_team: Optional[str]
    home_team: Optional[str]
    market_type: Optional[str]
    selection: Optional[str]
    line: Optional[float]
    odds: Optional[int]
    raw_pick_text: str
    raw_block: str
    raw_fingerprint: str


def sha256_digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _map_team(alias: Optional[str]) -> Optional[str]:
    """Map team name or abbreviation to standard team code."""
    if not alias:
        return None
    codes = data_store.lookup_team_code(alias)
    if len(codes) == 1:
        return next(iter(codes))
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
    if len(alias) <= 3:
        return alias.upper()
    return None


def is_nba_article(url: str, title: str) -> bool:
    """Check if an article URL/title indicates NBA content."""
    combined = f"{url} {title}".lower()

    # Check for NBA-specific patterns
    for pattern in NBA_ARTICLE_PATTERNS:
        if re.search(pattern, combined, re.IGNORECASE):
            return True

    # Check for team mentions
    for team in NBA_TEAMS:
        if team in combined:
            return True

    return False


def parse_matchup_from_url(url: str) -> Tuple[Optional[str], Optional[str]]:
    """Extract team names from article URL like 'warriors-vs-spurs-prediction'."""
    # Pattern: team1-vs-team2 or team1-at-team2
    match = re.search(
        r"/([a-z-]+?)-(?:vs|at)-([a-z-]+?)-(?:prediction|picks|odds)",
        url.lower()
    )
    if match:
        away_raw = match.group(1).replace("-", " ")
        home_raw = match.group(2).replace("-", " ")
        away = _map_team(away_raw)
        home = _map_team(home_raw)
        if away and home:
            return away, home

    return None, None


def parse_date_from_url(url: str) -> Optional[str]:
    """Extract date from article URL like '...-february-11' or '...-feb-9-2026'."""
    # Full and abbreviated month names
    months = {
        "january": "01", "jan": "01",
        "february": "02", "feb": "02",
        "march": "03", "mar": "03",
        "april": "04", "apr": "04",
        "may": "05",
        "june": "06", "jun": "06",
        "july": "07", "jul": "07",
        "august": "08", "aug": "08",
        "september": "09", "sep": "09", "sept": "09",
        "october": "10", "oct": "10",
        "november": "11", "nov": "11",
        "december": "12", "dec": "12",
    }

    url_lower = url.lower()

    # Try: month-day-year format (e.g., "feb-9-2026")
    match = re.search(r"-(\w+)-(\d{1,2})-(\d{4})(?:/|$|\?)", url_lower)
    if match:
        month_name = match.group(1)
        day = match.group(2).zfill(2)
        year = match.group(3)
        if month_name in months:
            month = months[month_name]
            return f"{year}-{month}-{day}"

    # Try: for-day-month-day format (e.g., "for-wednesday-february-11")
    match = re.search(r"for-(?:\w+day-)?(\w+)-(\d{1,2})/?", url_lower)
    if match:
        month_name = match.group(1)
        day = match.group(2).zfill(2)
        if month_name in months:
            month = months[month_name]
            year = datetime.now().year
            return f"{year}-{month}-{day}"

    # Try: month-day at end without weekday
    match = re.search(r"-(\w+)-(\d{1,2})/?$", url_lower)
    if match:
        month_name = match.group(1)
        day = match.group(2).zfill(2)
        if month_name in months:
            month = months[month_name]
            year = datetime.now().year
            return f"{year}-{month}-{day}"

    return None


def load_state(state_path: str) -> Dict[str, Any]:
    """Load backfill state from file."""
    if os.path.exists(state_path):
        try:
            with open(state_path, "r") as f:
                return json.load(f)
        except json.JSONDecodeError:
            pass
    return {"processed_urls": [], "last_load_more_count": 0}


def save_state(state_path: str, state: Dict[str, Any]) -> None:
    """Save backfill state to file."""
    os.makedirs(os.path.dirname(state_path) or ".", exist_ok=True)
    with open(state_path, "w") as f:
        json.dump(state, f, indent=2)


def append_jsonl(path: str, record: Dict[str, Any]) -> None:
    """Append a record to a JSONL file."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def discover_nba_articles(
    page: Page,
    max_load_more_clicks: int = 20,
    delay_ms: int = 1000,
    debug: bool = False
) -> List[ArticleInfo]:
    """
    Discover NBA prediction articles from the insiders page.

    Clicks "Load More" to paginate through articles and filters for NBA content.
    """
    articles: List[ArticleInfo] = []
    seen_urls: Set[str] = set()

    def extract_visible_articles() -> List[ArticleInfo]:
        """Extract article links currently visible on the page."""
        found = []
        links = page.locator("a[href*='/insiders/']")
        count = links.count()

        for i in range(count):
            try:
                link = links.nth(i)
                href = link.get_attribute("href", timeout=500)
                title = link.text_content(timeout=500) or ""

                if not href or href in seen_urls:
                    continue

                if not is_nba_article(href, title):
                    continue

                full_url = href if href.startswith("http") else f"https://www.sportsline.com{href}"
                seen_urls.add(href)

                found.append(ArticleInfo(
                    url=full_url,
                    title=title.strip()[:200],
                ))

            except Exception:
                continue

        return found

    # Initial extraction
    initial = extract_visible_articles()
    articles.extend(initial)

    if debug:
        print(f"[DEBUG] Initial articles found: {len(initial)}")

    # Scroll to bottom to make Load More button visible
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1000)

    # Click "Load More" to get more articles
    for click_num in range(max_load_more_clicks):
        try:
            load_more = page.locator("button:has-text('Load More Articles')")
            if load_more.count() == 0:
                if debug:
                    print(f"[DEBUG] No more 'Load More' button after {click_num} clicks")
                break

            load_more.first.click()
            page.wait_for_timeout(delay_ms)

            # Extract new articles
            new_articles = extract_visible_articles()
            articles.extend(new_articles)

            if debug and new_articles:
                print(f"[DEBUG] Click {click_num + 1}: found {len(new_articles)} new NBA articles (total: {len(articles)})")

            if not new_articles:
                # No new articles found, may have reached the end
                consecutive_empty = getattr(page, "_consecutive_empty", 0) + 1
                setattr(page, "_consecutive_empty", consecutive_empty)
                if consecutive_empty >= 3:
                    if debug:
                        print(f"[DEBUG] Stopping after {consecutive_empty} empty clicks")
                    break
            else:
                setattr(page, "_consecutive_empty", 0)

        except Exception as e:
            if debug:
                print(f"[DEBUG] Error during Load More click {click_num}: {e}")
            break

    return articles


def extract_picks_from_article(
    page: Page,
    article: ArticleInfo,
    observed_at: datetime,
    debug: bool = False
) -> List[HistoricalPick]:
    """
    Extract picks from a single SportsLine article page.

    The actual pick direction is revealed only to authenticated users.
    """
    picks: List[HistoricalPick] = []

    try:
        # Use domcontentloaded instead of networkidle for faster loading
        page.goto(article.url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(2000)

        # Try to wait for content, but don't fail if it times out
        try:
            page.wait_for_load_state("networkidle", timeout=5000)
        except Exception:
            pass

        # Get page content
        content = page.content()
        text = page.locator("body").text_content(timeout=5000) or ""

        # Parse matchup from URL
        away_team, home_team = parse_matchup_from_url(article.url)
        matchup_hint = f"{away_team}@{home_team}" if away_team and home_team else None

        # Skip non-NBA games (if we can't find NBA teams, it's likely college/other)
        if not away_team or not home_team:
            if debug:
                print(f"[DEBUG] Skipping {article.url}: no NBA teams found in URL")
            return picks

        # Parse game date
        game_date = parse_date_from_url(article.url)

        # SportsLine format: "Against The Spread Pick: Warriors +6.5"
        # and "Over/Under Pick: Over 220.5"
        # and "Money line Pick: Warriors +186"

        # Pattern 1: "Against The Spread Pick: TEAM +/-LINE"
        spread_match = re.search(
            r"Against\s+The\s+Spread\s*(?:Pick)?[:\s]+([A-Za-z]+)\s*([+-]\d+(?:\.\d+)?)",
            text,
            re.IGNORECASE
        )
        if spread_match:
            team_name = spread_match.group(1)
            line = float(spread_match.group(2))
            team = _map_team(team_name)
            if team:
                fingerprint = sha256_digest(f"sportsline|history|{article.url}|spread|{team}|{line}")
                picks.append(HistoricalPick(
                    source_id="sportsline",
                    source_surface="sportsline_nba_history",
                    sport="NBA",
                    market_family="standard",
                    observed_at_utc=observed_at.isoformat(),
                    article_url=article.url,
                    article_title=article.title,
                    article_date=game_date,
                    matchup_hint=matchup_hint,
                    away_team=away_team,
                    home_team=home_team,
                    market_type="spread",
                    selection=team,
                    line=line,
                    odds=None,
                    raw_pick_text=f"{team} {line:+.1f}",
                    raw_block=spread_match.group(0)[:200],
                    raw_fingerprint=fingerprint,
                ))

        # Pattern 2: "Over/Under Pick: Over/Under LINE"
        total_match = re.search(
            r"Over/Under\s*(?:Pick)?[:\s]+(Over|Under)\s+(\d+(?:\.\d+)?)",
            text,
            re.IGNORECASE
        )
        if total_match:
            direction = total_match.group(1).upper()
            line = float(total_match.group(2))
            fingerprint = sha256_digest(f"sportsline|history|{article.url}|total|{direction}|{line}")
            picks.append(HistoricalPick(
                source_id="sportsline",
                source_surface="sportsline_nba_history",
                sport="NBA",
                market_family="standard",
                observed_at_utc=observed_at.isoformat(),
                article_url=article.url,
                article_title=article.title,
                article_date=game_date,
                matchup_hint=matchup_hint,
                away_team=away_team,
                home_team=home_team,
                market_type="total",
                selection=direction,
                line=line,
                odds=None,
                raw_pick_text=f"{direction} {line}",
                raw_block=total_match.group(0)[:200],
                raw_fingerprint=fingerprint,
            ))

        # Pattern 3: "Money line Pick: TEAM +/-ODDS"
        ml_match = re.search(
            r"Money\s*line\s*(?:Pick)?[:\s]+([A-Za-z]+)\s*([+-]\d+)",
            text,
            re.IGNORECASE
        )
        if ml_match:
            team_name = ml_match.group(1)
            odds = int(ml_match.group(2))
            team = _map_team(team_name)
            if team:
                fingerprint = sha256_digest(f"sportsline|history|{article.url}|moneyline|{team}|{odds}")
                picks.append(HistoricalPick(
                    source_id="sportsline",
                    source_surface="sportsline_nba_history",
                    sport="NBA",
                    market_family="standard",
                    observed_at_utc=observed_at.isoformat(),
                    article_url=article.url,
                    article_title=article.title,
                    article_date=game_date,
                    matchup_hint=matchup_hint,
                    away_team=away_team,
                    home_team=home_team,
                    market_type="moneyline",
                    selection=team,
                    line=None,
                    odds=odds,
                    raw_pick_text=f"{team} {odds:+d}",
                    raw_block=ml_match.group(0)[:200],
                    raw_fingerprint=fingerprint,
                ))

        if debug:
            print(f"[DEBUG] Article {article.url}: extracted {len(picks)} picks")

    except Exception as e:
        if debug:
            print(f"[DEBUG] Error extracting from {article.url}: {e}")

    return picks


def backfill_sportsline_history(
    storage_state: str = DEFAULT_STORAGE_STATE,
    max_articles: int = 100,
    delay: float = 1.5,
    output_path: str = DEFAULT_OUTPUT,
    state_path: str = DEFAULT_STATE_FILE,
    debug: bool = False
) -> Tuple[int, int, Dict[str, Any]]:
    """
    Main backfill function.

    Returns:
        Tuple of (articles_processed, picks_extracted, debug_info)
    """
    if sync_playwright is None:
        raise RuntimeError("playwright is not installed")

    if not os.path.exists(storage_state):
        raise FileNotFoundError(
            f"Storage state not found at {storage_state}. "
            "Create it using: python3 sportsline_ingest.py --create-storage"
        )

    # Load state for resume support
    state = load_state(state_path)
    processed_urls = set(state.get("processed_urls", []))

    observed_at = datetime.now(timezone.utc)
    articles_processed = 0
    picks_extracted = 0
    dbg: Dict[str, Any] = {
        "start_time": observed_at.isoformat(),
        "storage_state": storage_state,
        "max_articles": max_articles,
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            storage_state=storage_state,
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )

        try:
            page = context.new_page()

            # Navigate to insiders page
            page.goto(INSIDERS_URL, wait_until="networkidle", timeout=30000)
            page.wait_for_timeout(2000)

            # Discover NBA articles
            print("[BACKFILL] Discovering NBA articles...")
            articles = discover_nba_articles(
                page,
                max_load_more_clicks=max(5, max_articles // 5),
                debug=debug
            )

            dbg["articles_discovered"] = len(articles)
            print(f"[BACKFILL] Found {len(articles)} NBA articles")

            # Filter out already processed
            new_articles = [a for a in articles if a.url not in processed_urls]
            print(f"[BACKFILL] {len(new_articles)} new articles to process")

            # Process articles
            for i, article in enumerate(new_articles[:max_articles]):
                if debug:
                    print(f"[DEBUG] Processing article {i + 1}/{min(len(new_articles), max_articles)}: {article.url[:80]}")

                picks = extract_picks_from_article(page, article, observed_at, debug=debug)

                for pick in picks:
                    append_jsonl(output_path, asdict(pick))
                    picks_extracted += 1

                articles_processed += 1
                processed_urls.add(article.url)

                # Save state periodically
                if articles_processed % 10 == 0:
                    state["processed_urls"] = list(processed_urls)
                    save_state(state_path, state)

                # Rate limiting
                time.sleep(delay)

            # Final state save
            state["processed_urls"] = list(processed_urls)
            state["last_run"] = observed_at.isoformat()
            save_state(state_path, state)

        finally:
            context.close()
            browser.close()

    dbg["articles_processed"] = articles_processed
    dbg["picks_extracted"] = picks_extracted
    dbg["end_time"] = datetime.now(timezone.utc).isoformat()

    return articles_processed, picks_extracted, dbg


def main():
    parser = argparse.ArgumentParser(description="Backfill SportsLine NBA picks history")
    parser.add_argument(
        "--storage",
        default=DEFAULT_STORAGE_STATE,
        help="Path to Playwright storage state JSON"
    )
    parser.add_argument(
        "--max-articles",
        type=int,
        default=100,
        help="Maximum number of articles to process"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.5,
        help="Delay between requests in seconds"
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help="Output JSONL file path"
    )
    parser.add_argument(
        "--state",
        default=DEFAULT_STATE_FILE,
        help="State file for resume support"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output"
    )
    args = parser.parse_args()

    try:
        articles, picks, dbg = backfill_sportsline_history(
            storage_state=args.storage,
            max_articles=args.max_articles,
            delay=args.delay,
            output_path=args.output,
            state_path=args.state,
            debug=args.debug
        )

        print(f"\n[BACKFILL] Complete!")
        print(f"  Articles processed: {articles}")
        print(f"  Picks extracted: {picks}")
        print(f"  Output: {args.output}")
        print(f"  State: {args.state}")

    except FileNotFoundError as e:
        print(f"Error: {e}")
        return 1
    except Exception as e:
        print(f"Error during backfill: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
