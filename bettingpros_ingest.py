"""
BettingPros NBA picks ingestion script (daily).

BettingPros is an algorithmic model that assigns bet_rating (1-5 stars),
cover_probability, expected_value, and projection to every NBA spread,
total, and moneyline.

Strategy: visit each game's matchup page — this triggers the API to return
all market types (spread + total + moneyline) with full metrics. We capture
the intercepted API responses without needing direct API access.

We ingest picks with bet_rating >= DEFAULT_MIN_RATING (default: 3 stars).

Requires authenticated session state at data/bettingpros_storage_state.json.
Run scripts/create_bettingpros_storage.py first to create it.

Usage:
    python3 bettingpros_ingest.py [--debug] [--storage PATH]
    python3 bettingpros_ingest.py --min-rating 4  # only 4+ star picks
    python3 bettingpros_ingest.py --date 2026-03-10

Output:
    - out/raw_bettingpros_nba.json
    - out/normalized_bettingpros_nba.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[0]
sys.path.insert(0, str(ROOT))

try:
    from playwright.sync_api import sync_playwright, Page, BrowserContext
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
except ImportError:
    sync_playwright = None
    PlaywrightTimeoutError = Exception

from store import write_json

OUT_DIR = os.getenv("NBA_OUT_DIR", "out")
DEFAULT_STORAGE_STATE = "data/bettingpros_storage_state.json"

# BettingPros API market IDs
MARKET_SPREAD = 129
MARKET_TOTAL = 128
MARKET_MONEYLINE = 127

# Prop market IDs (used for player prop cheat sheet)
PROP_MARKET_IDS = {156, 157, 151, 162, 147, 160, 152, 142, 136, 335, 336, 337, 338}
PROP_STAT_MAP = {
    136: "points",
    142: "rebounds",
    147: "assists",
    151: "steals",
    152: "blocks",
    156: "pts_reb",
    157: "pts_ast",
    160: "reb_ast",
    162: "pts_reb_ast",
    335: "three_pointers",
    336: "turnovers",
    337: "fantasy_score",
    338: "double_double",
}

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

# Minimum bet_rating to include (1-5 scale)
DEFAULT_MIN_RATING = 3


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
    market_type: Optional[str] = None
    selection: Optional[str] = None
    side: Optional[str] = None
    line: Optional[float] = None
    odds: Optional[int] = None
    bet_rating: Optional[int] = None          # 1-5 stars (BettingPros model)
    cover_probability: Optional[float] = None
    expected_value: Optional[float] = None
    projection: Optional[float] = None
    game_date: Optional[str] = None
    pregraded_result: Optional[str] = None
    player_name: Optional[str] = None         # player props only
    stat_key: Optional[str] = None            # player props only (points/rebounds/etc.)


def sha256_digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def ensure_out_dir() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)


def write_records(path: str, records: List[Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        prepared = [asdict(r) if hasattr(r, "__dataclass_fields__") else r for r in records]
        json.dump(prepared, f, ensure_ascii=False, indent=2)


def _get_consensus_line(selection: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract the consensus line (book_id=0) from a selection's books array."""
    for book in selection.get('books', []):
        if book.get('id') == 0:
            lines = book.get('lines', [])
            if lines:
                return lines[0]
    return None


def _extract_picks_from_offers(
    offers: List[Dict[str, Any]],
    away_team: str,
    home_team: str,
    scheduled: str,
    game_date: str,
    observed_at: datetime,
    min_rating: int,
    matchup_page_url: str,
    debug: bool = False,
) -> List[RawPickRecord]:
    """Extract RawPickRecord objects from a list of BettingPros offer objects."""
    records: List[RawPickRecord] = []
    matchup = f"{away_team}@{home_team}"

    for offer in offers:
        market_id = offer.get('market_id')
        market_type = {
            MARKET_SPREAD: 'spread',
            MARKET_TOTAL: 'total',
            MARKET_MONEYLINE: 'moneyline',
        }.get(market_id)

        if not market_type:
            continue

        for sel in offer.get('selections', []):
            consensus_line = _get_consensus_line(sel)
            if not consensus_line:
                continue

            metrics = consensus_line.get('metrics', {})
            bet_rating = metrics.get('bet_rating')
            if bet_rating is None or bet_rating < min_rating:
                continue

            line = consensus_line.get('line')
            cost = consensus_line.get('cost')   # American odds integer
            cover_prob = metrics.get('cover_probability')
            ev = metrics.get('expected_value')
            projection = metrics.get('projection')

            participant = sel.get('participant')    # team abbreviation
            short_label = sel.get('short_label', '')
            raw_sel = sel.get('selection', '')      # 'over'/'under' for totals

            if market_type == 'total':
                direction = (raw_sel or '').upper()
                if direction not in ('OVER', 'UNDER'):
                    continue
                pick_text = f"{direction} {line}"
                selection_val = direction
                side = direction
            elif market_type in ('spread', 'moneyline'):
                sign = '+' if (line is not None and line > 0) else ''
                pick_text = f"{short_label} {sign}{line}" if line is not None else short_label
                selection_val = participant or short_label
                side = selection_val
            else:
                continue

            raw_block = json.dumps({
                "offer_id": offer.get('id'),
                "market_id": market_id,
                "selection": sel,
                "metrics": metrics,
                "away_team": away_team,
                "home_team": home_team,
            })
            fp = sha256_digest(f"bettingpros|{market_type}|{pick_text}|{matchup}|{game_date}")

            records.append(RawPickRecord(
                source_id="bettingpros",
                source_surface="bettingpros_model",
                sport="NBA",
                market_family="standard",
                observed_at_utc=observed_at.isoformat(),
                canonical_url=matchup_page_url,
                raw_pick_text=pick_text,
                raw_block=raw_block,
                raw_fingerprint=fp,
                event_start_time_utc=scheduled + 'Z' if scheduled and not scheduled.endswith('Z') else scheduled,
                matchup_hint=matchup,
                away_team=away_team,
                home_team=home_team,
                market_type=market_type,
                selection=selection_val,
                side=side,
                line=line,
                odds=cost,
                bet_rating=bet_rating,
                cover_probability=cover_prob,
                expected_value=ev,
                projection=projection,
                game_date=game_date,
            ))

    if debug and records:
        for r in records:
            print(f"  [pick] {r.market_type} {r.raw_pick_text} rating={r.bet_rating} line={r.line} odds={r.odds}")

    return records


def _extract_props_from_offers(
    offers: List[Dict[str, Any]],
    away_team: str,
    home_team: str,
    scheduled: str,
    game_date: str,
    observed_at: datetime,
    min_rating: int,
    matchup_page_url: str,
    debug: bool = False,
) -> List[RawPickRecord]:
    """Extract player prop RawPickRecord objects from BettingPros offer objects."""
    records: List[RawPickRecord] = []
    matchup = f"{away_team}@{home_team}"

    for offer in offers:
        market_id = offer.get('market_id')
        if market_id not in PROP_MARKET_IDS:
            continue

        stat_key = PROP_STAT_MAP.get(market_id)
        if not stat_key:
            continue

        for sel in offer.get('selections', []):
            consensus_line = _get_consensus_line(sel)
            if not consensus_line:
                continue

            metrics = consensus_line.get('metrics', {})
            bet_rating = metrics.get('bet_rating')
            if bet_rating is None or bet_rating < min_rating:
                continue

            line = consensus_line.get('line')
            cost = consensus_line.get('cost')
            cover_prob = metrics.get('cover_probability')
            ev = metrics.get('expected_value')
            projection = metrics.get('projection')

            # Player name comes from participant field
            player_name = sel.get('participant') or sel.get('short_label') or ''
            direction = (sel.get('selection') or '').upper()
            if direction not in ('OVER', 'UNDER'):
                continue

            pick_text = f"{player_name} {direction} {line} {stat_key}" if player_name else f"{direction} {line} {stat_key}"
            fp = sha256_digest(f"bettingpros|player_prop|{player_name}|{stat_key}|{direction}|{line}|{matchup}|{game_date}")

            raw_block = json.dumps({
                "offer_id": offer.get('id'),
                "market_id": market_id,
                "selection": sel,
                "metrics": metrics,
                "away_team": away_team,
                "home_team": home_team,
            })

            records.append(RawPickRecord(
                source_id="bettingpros",
                source_surface="bettingpros_model",
                sport="NBA",
                market_family="player_prop",
                observed_at_utc=observed_at.isoformat(),
                canonical_url=matchup_page_url,
                raw_pick_text=pick_text,
                raw_block=raw_block,
                raw_fingerprint=fp,
                event_start_time_utc=scheduled + 'Z' if scheduled and not scheduled.endswith('Z') else scheduled,
                matchup_hint=matchup,
                away_team=away_team,
                home_team=home_team,
                market_type='player_prop',
                selection=direction,
                side=direction,
                line=line,
                odds=cost,
                bet_rating=bet_rating,
                cover_probability=cover_prob,
                expected_value=ev,
                projection=projection,
                game_date=game_date,
                player_name=player_name,
                stat_key=stat_key,
            ))

    if debug and records:
        for r in records:
            print(f"  [prop] {r.player_name} {r.stat_key} {r.side} {r.line} rating={r.bet_rating}")

    return records


# ─── Main ingest ──────────────────────────────────────────────────────────────

def ingest_bettingpros_nba(
    storage_state: str = DEFAULT_STORAGE_STATE,
    debug: bool = False,
    min_rating: int = DEFAULT_MIN_RATING,
    date: Optional[str] = None,
) -> Tuple[List[RawPickRecord], Dict[str, Any]]:
    """
    Main ingestion function.

    For each NBA game today:
      1. Load the game's matchup page on bettingpros.com
      2. Capture the intercepted /v3/offers?market_id=129:128:127 API response
      3. Extract spread + total + moneyline picks with bet_rating >= min_rating
    """
    if sync_playwright is None:
        raise ImportError("playwright not installed. Run: pip install playwright && playwright install chromium")

    if not Path(storage_state).exists():
        raise FileNotFoundError(
            f"No storage state at {storage_state}. "
            "Run: python3 scripts/create_bettingpros_storage.py"
        )

    observed_at = datetime.now(timezone.utc)
    from zoneinfo import ZoneInfo
    target_date = date or datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    dbg: Dict[str, Any] = {
        "observed_at": observed_at.isoformat(),
        "date": target_date,
        "min_rating": min_rating,
        "games": [],
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=USER_AGENT,
            storage_state=storage_state,
            viewport={"width": 1400, "height": 900},
        )
        page = context.new_page()

        # --- Step 1: Get today's events ---
        events_cache: Dict[str, Any] = {}

        def capture_events(resp):
            url = resp.url
            if '/v3/events?sport=NBA&date=' in url:
                try:
                    data = resp.json()
                    events_cache['data'] = data
                except Exception:
                    pass

        page.on("response", capture_events)

        # Load the picks index page to get today's event list
        page.goto("https://www.bettingpros.com/nba/picks/game-picks/", wait_until="networkidle", timeout=40000)
        page.wait_for_timeout(2000)

        events = events_cache.get('data', {}).get('events', [])
        if not events:
            # Fallback: try fetching events directly via page.evaluate
            events_raw = page.evaluate(f"""
                async () => {{
                    const resp = await fetch('https://api.bettingpros.com/v3/events?sport=NBA&date={target_date}');
                    if (!resp.ok) return [];
                    const data = await resp.json();
                    return data.events || [];
                }}
            """)
            events = events_raw or []

        if debug:
            print(f"[bettingpros] Date={target_date} found {len(events)} events")

        dbg["event_count"] = len(events)
        all_records: List[RawPickRecord] = []

        # --- Step 2: Visit each game's matchup page to capture offers ---
        for event in events:
            eid = event.get('id')
            away_team = event.get('visitor', '')
            home_team = event.get('home', '')
            scheduled = event.get('scheduled', '')
            game_date = scheduled[:10] if scheduled else target_date
            link = event.get('link', '')

            if not link:
                if debug:
                    print(f"[bettingpros] No matchup link for event {eid}")
                continue

            matchup_url = f"https://www.bettingpros.com{link}"
            game_offers: List[Dict] = []

            def capture_offers(resp, _eid=eid):
                url = resp.url
                if 'api.bettingpros.com' in url and 'offers' in url and f'event_id={_eid}' in url:
                    if 'market_id=' in url:
                        try:
                            data = resp.json()
                            for offer in data.get('offers', []):
                                if offer.get('event_id') == _eid:
                                    game_offers.append(offer)
                        except Exception:
                            pass

            page.on("response", capture_offers)

            try:
                if debug:
                    print(f"[bettingpros] Loading {away_team}@{home_team}: {matchup_url}")

                page.goto(matchup_url, wait_until="networkidle", timeout=25000)
                page.wait_for_timeout(1500)

            except Exception as e:
                if debug:
                    print(f"[bettingpros] Error loading {matchup_url}: {e}")
            finally:
                page.remove_listener("response", capture_offers)

            # Deduplicate offers by market_id
            seen_markets: set = set()
            unique_offers: List[Dict] = []
            for offer in game_offers:
                mid = offer.get('market_id')
                if mid not in seen_markets:
                    seen_markets.add(mid)
                    unique_offers.append(offer)

            picks = _extract_picks_from_offers(
                unique_offers, away_team, home_team, scheduled, game_date,
                observed_at, min_rating, matchup_url, debug=debug
            )
            prop_picks = _extract_props_from_offers(
                unique_offers, away_team, home_team, scheduled, game_date,
                observed_at, min_rating, matchup_url, debug=debug
            )
            all_records.extend(picks + prop_picks)

            dbg["games"].append({
                "event_id": eid,
                "matchup": f"{away_team}@{home_team}",
                "picks": len(picks),
                "prop_picks": len(prop_picks),
            })

            if debug:
                print(f"  → {away_team}@{home_team}: {len(unique_offers)} markets, {len(picks)} picks")

        context.close()
        browser.close()

    # Deduplicate by fingerprint
    seen: set = set()
    deduped: List[RawPickRecord] = []
    for r in all_records:
        if r.raw_fingerprint not in seen:
            seen.add(r.raw_fingerprint)
            deduped.append(r)

    dbg["total"] = len(deduped)
    return deduped, dbg


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest BettingPros NBA picks (algorithmic model, 1-5 star ratings)")
    ap.add_argument("--storage", default=DEFAULT_STORAGE_STATE, help="Path to Playwright storage state JSON")
    ap.add_argument("--debug", action="store_true", help="Enable verbose debug output")
    ap.add_argument("--dry-run", action="store_true", help="Don't write output files")
    ap.add_argument("--min-rating", type=int, default=DEFAULT_MIN_RATING,
                    help=f"Minimum bet_rating to include (1-5, default={DEFAULT_MIN_RATING})")
    ap.add_argument("--date", default=None, help="Date to fetch (YYYY-MM-DD, default: today ET)")
    args = ap.parse_args()

    print(f"[bettingpros] Starting ingest (storage={args.storage}, min_rating={args.min_rating})")
    records, dbg = ingest_bettingpros_nba(
        storage_state=args.storage,
        debug=args.debug,
        min_rating=args.min_rating,
        date=args.date,
    )

    print(f"[bettingpros] Date={dbg.get('date')} events={dbg.get('event_count')} total_picks={dbg.get('total')}")
    for game in dbg.get('games', []):
        if game.get('picks'):
            print(f"  {game['matchup']}: {game['picks']} picks")

    if args.debug and records:
        by_market: Dict[str, List] = {}
        for r in records:
            by_market.setdefault(r.market_type, []).append(r)
        for mkt, recs in sorted(by_market.items()):
            print(f"\n{mkt.upper()} ({len(recs)}):")
            for r in sorted(recs, key=lambda x: -(x.bet_rating or 0)):
                print(f"  {r.raw_pick_text} [{r.away_team}@{r.home_team}] rating={r.bet_rating} odds={r.odds}")

    if args.dry_run:
        print("[bettingpros] Dry run — not writing files")
        return

    ensure_out_dir()

    raw_path = os.path.join(OUT_DIR, "raw_bettingpros_nba.json")
    write_records(raw_path, records)
    print(f"[bettingpros] Wrote raw → {raw_path}")

    # Normalize
    from src.normalizer_bettingpros_nba import normalize_bettingpros_records
    raw_dicts = [asdict(r) for r in records]
    normalized = normalize_bettingpros_records(raw_dicts, debug=args.debug)

    norm_path = os.path.join(OUT_DIR, "normalized_bettingpros_nba.json")
    write_json(norm_path, normalized)
    eligible = sum(1 for r in normalized if r.get("eligible_for_consensus"))
    print(f"[bettingpros] Wrote normalized → {norm_path} ({eligible}/{len(normalized)} eligible)")


if __name__ == "__main__":
    main()
