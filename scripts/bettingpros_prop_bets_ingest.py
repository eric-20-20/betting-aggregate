"""
BettingPros NBA prop-bets board ingestion script.

Targets:
  https://www.bettingpros.com/nba/picks/prop-bets/

This is a separate source from the existing matchup-model ingest in
`bettingpros_ingest.py`. It captures the prop-bets board rows driven by the
`/v3/props` API, preserving projection, projected diff, star rating, EV, and
performance splits.

Output:
  - out/raw_bettingpros_prop_bets_nba.json
  - out/normalized_bettingpros_prop_bets_nba.json
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
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    sync_playwright = None

from store import write_json

OUT_DIR = os.getenv("NBA_OUT_DIR", "out")
DEFAULT_STORAGE_STATE = "data/bettingpros_storage_state.json"
PROPS_PAGE_URL = "https://www.bettingpros.com/nba/picks/prop-bets/"
DEFAULT_MIN_PROJ_DIFF = 1.5
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

PROP_STAT_MAP = {
    156: "points",
    157: "rebounds",
    151: "assists",
    162: "threes",
    160: "steals",
    152: "blocks",
    335: "pts_ast",
    336: "pts_reb",
    337: "reb_ast",
    338: "pts_reb_ast",
}


@dataclass
class RawPropBetRecord:
    source_id: str
    source_surface: str
    sport: str
    market_family: str
    observed_at_utc: str
    canonical_url: str
    raw_pick_text: str
    raw_block: str
    raw_fingerprint: str
    event_start_time_utc: Optional[str] = None
    matchup_hint: Optional[str] = None
    away_team: Optional[str] = None
    home_team: Optional[str] = None
    market_type: Optional[str] = None
    selection: Optional[str] = None
    side: Optional[str] = None
    line: Optional[float] = None
    odds: Optional[int] = None
    bet_rating: Optional[int] = None
    expected_value: Optional[float] = None
    projection: Optional[float] = None
    projection_diff: Optional[float] = None
    cover_probability: Optional[float] = None
    projection_probability: Optional[float] = None
    game_date: Optional[str] = None
    player_name: Optional[str] = None
    stat_key: Optional[str] = None
    event_id: Optional[int] = None
    participant_id: Optional[str] = None
    market_id: Optional[int] = None
    recommended_side: Optional[str] = None
    affiliate_link: Optional[str] = None
    analyzer_path: Optional[str] = None
    odds_path: Optional[str] = None
    opposition_rank: Optional[int] = None
    over_percentage_last_5: Optional[float] = None
    over_percentage_last_10: Optional[float] = None
    over_percentage_last_15: Optional[float] = None
    over_percentage_season: Optional[float] = None
    over_percentage_h2h: Optional[float] = None


def sha256_digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def ensure_out_dir() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)


def write_records(path: str, records: List[Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        prepared = [asdict(r) if hasattr(r, "__dataclass_fields__") else r for r in records]
        json.dump(prepared, f, ensure_ascii=False, indent=2)


def _to_float(val: Any) -> Optional[float]:
    try:
        return None if val is None else float(val)
    except Exception:
        return None


def _to_int(val: Any) -> Optional[int]:
    try:
        return None if val is None else int(val)
    except Exception:
        return None


def _normalize_side(val: Any) -> Optional[str]:
    if not isinstance(val, str):
        return None
    upper = val.strip().upper()
    return upper if upper in {"OVER", "UNDER"} else None


def _pct_from_counts(block: Optional[Dict[str, Any]]) -> Optional[float]:
    if not isinstance(block, dict):
        return None
    over = _to_int(block.get("over"))
    under = _to_int(block.get("under"))
    push = _to_int(block.get("push")) or 0
    if over is None or under is None:
        return None
    denom = over + under + push
    if denom <= 0:
        return None
    return over / denom


def _fetch_json(page, url: str) -> Dict[str, Any]:
    return page.evaluate(
        """async (url) => {
          const r = await fetch(url, { credentials: 'include' });
          return await r.json();
        }""",
        url,
    )


def _absolute_next(next_path: Optional[str]) -> Optional[str]:
    if not next_path:
        return None
    return urljoin("https://api.bettingpros.com", next_path)


def _build_props_url(initial_url: str, date: str) -> str:
    parsed = urlparse(initial_url)
    qs = parse_qs(parsed.query, keep_blank_values=True)
    qs["date"] = [date]
    qs["limit"] = ["200"]
    qs["page"] = ["1"]
    qs["include_events"] = ["true"]
    new_query = urlencode(qs, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def _capture_initial_payloads(page, date: str) -> Tuple[Dict[str, Any], Dict[str, Any], str]:
    captured: Dict[str, Any] = {"events": None, "markets": None, "props_url": None}

    def on_response(resp):
        url = resp.url
        try:
            if captured["events"] is None and url.startswith("https://api.bettingpros.com/v3/events?sport=NBA"):
                captured["events"] = resp.json()
            elif captured["markets"] is None and url.startswith("https://api.bettingpros.com/v3/markets?sport=NBA&market_category=player-props"):
                captured["markets"] = resp.json()
            elif captured["props_url"] is None and url.startswith("https://api.bettingpros.com/v3/props?"):
                captured["props_url"] = url
        except Exception:
            pass

    page.on("response", on_response)
    page.goto(PROPS_PAGE_URL, wait_until="networkidle", timeout=60000)
    page.wait_for_timeout(1500)
    page.remove_listener("response", on_response)

    if not isinstance(captured.get("events"), dict):
        raise RuntimeError("Failed to capture BettingPros events payload from prop-bets page")
    if not isinstance(captured.get("markets"), dict):
        raise RuntimeError("Failed to capture BettingPros markets payload from prop-bets page")
    if not captured.get("props_url"):
        raise RuntimeError("Failed to capture BettingPros props API URL from prop-bets page")

    return captured["events"], captured["markets"], _build_props_url(captured["props_url"], date)


def _extract_market_map(markets_payload: Dict[str, Any]) -> Dict[int, str]:
    market_map: Dict[int, str] = {}
    for market in markets_payload.get("markets") or []:
        market_id = _to_int(market.get("id"))
        slug = market.get("slug")
        if market_id is None or not slug:
            continue
        stat_key = PROP_STAT_MAP.get(market_id)
        if stat_key:
            market_map[market_id] = stat_key
    return market_map


def _extract_event_map(events_payload: Dict[str, Any], target_date: str) -> Dict[int, Dict[str, Any]]:
    event_map: Dict[int, Dict[str, Any]] = {}
    for event in events_payload.get("events") or []:
        event_id = _to_int(event.get("id"))
        if event_id is None:
            continue
        scheduled = event.get("scheduled")
        if isinstance(scheduled, str):
            try:
                dt = datetime.fromisoformat(scheduled.replace(" ", "T"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                game_day = dt.astimezone(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
                if game_day != target_date:
                    continue
            except ValueError:
                if len(scheduled) >= 10 and scheduled[:10] != target_date:
                    continue
        event_map[event_id] = event
    return event_map


def _extract_team_event_map(event_map: Dict[int, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    team_event_map: Dict[str, Dict[str, Any]] = {}
    for event in event_map.values():
        for team in (event.get("home"), event.get("visitor")):
            if isinstance(team, str) and team:
                team_event_map[team] = event
    return team_event_map


def _iter_all_props(page, first_url: str, target_date: str) -> Tuple[List[Dict[str, Any]], Dict[int, Dict[str, Any]], Dict[str, Any]]:
    pages = 0
    total_items = 0
    all_props: List[Dict[str, Any]] = []
    merged_events_payload: Dict[str, Any] = {"events": []}
    merged_event_ids: set[int] = set()
    seen_urls: set[str] = set()
    url = first_url

    while url and url not in seen_urls:
        seen_urls.add(url)
        payload = _fetch_json(page, url)
        pages += 1
        props = payload.get("props") or []
        all_props.extend([p for p in props if isinstance(p, dict)])
        for event in payload.get("events") or []:
            event_id = _to_int((event or {}).get("id"))
            if event_id is None or event_id in merged_event_ids:
                continue
            merged_event_ids.add(event_id)
            merged_events_payload["events"].append(event)
        pagination = payload.get("_pagination") or {}
        total_items = _to_int(pagination.get("total_items")) or total_items
        url = _absolute_next(pagination.get("next"))

    return all_props, _extract_event_map(merged_events_payload, target_date), {
        "pages_fetched": pages,
        "total_items_reported": total_items,
        "raw_props_fetched": len(all_props),
        "events_from_props": len(merged_event_ids),
    }


def _event_not_started(scheduled: Optional[str], observed_at: datetime) -> bool:
    if not scheduled or not isinstance(scheduled, str):
        return False
    try:
        dt = datetime.fromisoformat(scheduled.replace(" ", "T"))
    except ValueError:
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc) > observed_at


def _scheduled_game_day(scheduled: Optional[str]) -> Optional[str]:
    if not scheduled or not isinstance(scheduled, str):
        return None
    try:
        dt = datetime.fromisoformat(scheduled.replace(" ", "T"))
    except ValueError:
        return scheduled[:10] if len(scheduled) >= 10 else None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")


def _extract_records(
    props: List[Dict[str, Any]],
    event_map: Dict[int, Dict[str, Any]],
    team_event_map: Dict[str, Dict[str, Any]],
    market_map: Dict[int, str],
    observed_at: datetime,
    target_date: str,
    include_started: bool,
    debug: bool = False,
) -> Tuple[List[RawPropBetRecord], Dict[str, Any]]:
    records: List[RawPropBetRecord] = []
    skipped = {
        "unsupported_market": 0,
        "missing_event": 0,
        "wrong_date": 0,
        "started": 0,
        "missing_projection": 0,
        "missing_side": 0,
    }
    side_counts: Dict[str, int] = {"OVER": 0, "UNDER": 0}
    stat_counts: Dict[str, int] = {}

    for prop in props:
        market_id = _to_int(prop.get("market_id"))
        stat_key = market_map.get(market_id or -1)
        if not stat_key:
            skipped["unsupported_market"] += 1
            continue

        event_id = _to_int(prop.get("event_id"))
        participant = prop.get("participant") or {}
        player = participant.get("player") or {}
        player_team = player.get("team")
        event = event_map.get(event_id or -1)
        if not event and isinstance(player_team, str):
            event = team_event_map.get(player_team)
        if not event:
            skipped["missing_event"] += 1
            continue

        scheduled = event.get("scheduled")
        game_date = _scheduled_game_day(scheduled)
        if game_date != target_date:
            skipped["wrong_date"] += 1
            continue
        if not include_started and not _event_not_started(scheduled, observed_at):
            skipped["started"] += 1
            continue

        projection = prop.get("projection") or {}
        proj_val = _to_float(projection.get("value"))
        proj_diff = _to_float(projection.get("diff"))
        recommended_side = _normalize_side(projection.get("recommended_side"))
        if proj_val is None or proj_diff is None:
            skipped["missing_projection"] += 1
            continue
        if recommended_side not in {"OVER", "UNDER"}:
            skipped["missing_side"] += 1
            continue

        side_block = prop.get(recommended_side.lower()) or {}
        line = _to_float(side_block.get("line"))
        odds = _to_int(side_block.get("odds"))
        bet_rating = _to_int(side_block.get("bet_rating") or projection.get("bet_rating"))
        ev = _to_float(side_block.get("expected_value") or projection.get("expected_value"))
        probability = _to_float(side_block.get("probability"))

        player_name = participant.get("name") or player.get("short_name")
        participant_id = participant.get("id")
        away_team = event.get("visitor")
        home_team = event.get("home")
        matchup = f"{away_team}@{home_team}" if away_team and home_team else None

        raw_pick_text = f"{player_name} {recommended_side} {line} {stat_key}"
        raw_fp = sha256_digest(
            f"bettingpros_props|{event_id}|{player_name}|{stat_key}|{recommended_side}|{line}|{target_date}"
        )

        performance = prop.get("performance") or {}
        data_points = prop.get("data_points") or {}

        record = RawPropBetRecord(
            source_id="bettingpros_props",
            source_surface="bettingpros_prop_bets",
            sport="NBA",
            market_family="player_prop",
            observed_at_utc=observed_at.isoformat(),
            canonical_url=urljoin("https://www.bettingpros.com", (prop.get("links") or {}).get("analyzer") or PROPS_PAGE_URL),
            raw_pick_text=raw_pick_text,
            raw_block=json.dumps(prop, ensure_ascii=False),
            raw_fingerprint=raw_fp,
            event_start_time_utc=scheduled,
            matchup_hint=matchup,
            away_team=away_team,
            home_team=home_team,
            market_type="player_prop",
            selection=recommended_side,
            side=recommended_side,
            line=line,
            odds=odds,
            bet_rating=bet_rating,
            expected_value=ev,
            projection=proj_val,
            projection_diff=proj_diff,
            cover_probability=probability,
            projection_probability=_to_float(projection.get("probability")),
            game_date=game_date,
            player_name=player_name,
            stat_key=stat_key,
            event_id=event_id,
            participant_id=str(participant_id) if participant_id is not None else None,
            market_id=market_id,
            recommended_side=recommended_side,
            affiliate_link=(prop.get("links") or {}).get("affiliate"),
            analyzer_path=(prop.get("links") or {}).get("analyzer"),
            odds_path=(prop.get("links") or {}).get("odds"),
            opposition_rank=_to_int(((prop.get("extra") or {}).get("opposition_rank") or {}).get("rank")),
            over_percentage_last_5=_pct_from_counts(performance.get("last_5")),
            over_percentage_last_10=_pct_from_counts(performance.get("last_10")),
            over_percentage_last_15=_pct_from_counts(performance.get("last_15")),
            over_percentage_season=_pct_from_counts(performance.get("season")),
            over_percentage_h2h=_pct_from_counts(performance.get("h2h")),
        )
        records.append(record)
        side_counts[recommended_side] = side_counts.get(recommended_side, 0) + 1
        stat_counts[stat_key] = stat_counts.get(stat_key, 0) + 1

    if debug:
        print(f"[bettingpros_props] extracted={len(records)} skipped={skipped}")

    return records, {"skipped": skipped, "side_counts": side_counts, "stat_counts": stat_counts}


def ingest_bettingpros_prop_bets_nba(
    storage_state: str = DEFAULT_STORAGE_STATE,
    debug: bool = False,
    date: Optional[str] = None,
    include_started: bool = False,
) -> Tuple[List[RawPropBetRecord], Dict[str, Any]]:
    if sync_playwright is None:
        raise ImportError("playwright not installed. Run: pip install playwright && playwright install chromium")
    if not Path(storage_state).exists():
        raise FileNotFoundError(
            f"No storage state at {storage_state}. Run: python3 scripts/create_bettingpros_storage.py"
        )

    from zoneinfo import ZoneInfo

    observed_at = datetime.now(timezone.utc)
    target_date = date or datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    dbg: Dict[str, Any] = {
        "observed_at": observed_at.isoformat(),
        "date": target_date,
        "include_started": include_started,
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=USER_AGENT,
            storage_state=storage_state,
            viewport={"width": 1400, "height": 900},
        )
        page = context.new_page()

        events_payload, markets_payload, first_props_url = _capture_initial_payloads(page, target_date)
        market_map = _extract_market_map(markets_payload)
        prop_rows, props_event_map, props_meta = _iter_all_props(page, first_props_url, target_date)
        page_event_map = _extract_event_map(events_payload, target_date)
        event_map = {**page_event_map, **props_event_map}
        team_event_map = _extract_team_event_map(event_map)
        records, extract_meta = _extract_records(
            prop_rows,
            event_map=event_map,
            team_event_map=team_event_map,
            market_map=market_map,
            observed_at=observed_at,
            target_date=target_date,
            include_started=include_started,
            debug=debug,
        )

        context.close()
        browser.close()

    deduped: List[RawPropBetRecord] = []
    seen: set[str] = set()
    for record in records:
        if record.raw_fingerprint in seen:
            continue
        seen.add(record.raw_fingerprint)
        deduped.append(record)

    dbg["events_available"] = len(event_map)
    dbg["markets_available"] = market_map
    dbg["props_meta"] = props_meta
    dbg["extract_meta"] = extract_meta
    dbg["total"] = len(deduped)
    return deduped, dbg


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest BettingPros NBA prop-bets board")
    ap.add_argument("--storage", default=DEFAULT_STORAGE_STATE, help="Path to Playwright storage state JSON")
    ap.add_argument("--debug", action="store_true", help="Enable verbose debug output")
    ap.add_argument("--dry-run", action="store_true", help="Do not write output files")
    ap.add_argument("--date", default=None, help="Date to fetch (YYYY-MM-DD, default: today ET)")
    ap.add_argument("--include-started", action="store_true", help="Keep same-day props for games that already started")
    args = ap.parse_args()

    records, dbg = ingest_bettingpros_prop_bets_nba(
        storage_state=args.storage,
        debug=args.debug,
        date=args.date,
        include_started=args.include_started,
    )

    print(
        f"[bettingpros_props] date={dbg.get('date')} total={dbg.get('total')} "
        f"events={dbg.get('events_available')} props_fetched={dbg.get('props_meta', {}).get('raw_props_fetched')}"
    )
    side_counts = (dbg.get("extract_meta") or {}).get("side_counts") or {}
    stat_counts = (dbg.get("extract_meta") or {}).get("stat_counts") or {}
    if side_counts:
        print(f"  sides={side_counts}")
    if stat_counts:
        print(f"  stat_counts={stat_counts}")

    if args.dry_run:
        print("[bettingpros_props] Dry run — not writing output files")
        return

    ensure_out_dir()
    raw_path = os.path.join(OUT_DIR, "raw_bettingpros_prop_bets_nba.json")
    write_records(raw_path, records)
    print(f"[bettingpros_props] Wrote raw → {raw_path}")

    from src.normalizer_bettingpros_props_nba import normalize_bettingpros_props_records

    raw_dicts = [asdict(r) for r in records]
    normalized = normalize_bettingpros_props_records(raw_dicts, debug=args.debug, min_proj_diff=DEFAULT_MIN_PROJ_DIFF)
    norm_path = os.path.join(OUT_DIR, "normalized_bettingpros_prop_bets_nba.json")
    write_json(norm_path, normalized)
    eligible = sum(1 for r in normalized if r.get("eligible_for_consensus"))
    print(f"[bettingpros_props] Wrote normalized → {norm_path} ({eligible}/{len(normalized)} eligible)")


if __name__ == "__main__":
    main()
