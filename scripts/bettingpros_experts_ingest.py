"""
BettingPros user-expert picks ingestion script (daily).

Dynamically discovers active experts from the BettingPros NBA accuracy leaderboard
(season + all-time periods), then fetches their upcoming/live picks.
On first encounter, also backfills full pick history (all sports, all time).

Strategy:
  1. Discover experts from two leaderboard periods (season → all-time):
       - Each period is fully paginated via API replay (not just scroll-triggered pages)
       - Union of the periods is built; experts with ≥MIN_ACTIVE_PICKS_PRIORITY
         active picks are placed at the front of the run queue
     (The monthly period is intentionally excluded — too-small samples surface
     fluky streaks. Re-enable only with a written rationale.)
  2. Merge discovered experts with previously known experts (data/bettingpros_experts_known.json)
  3. For each expert:
     - New expert: fetch full scored history (all pages, all sports) + upcoming/live
     - Known expert: fetch upcoming/live + incremental scored since last_scored_at
  4. Write two output files:
     - out/raw_bettingpros_experts_nba.json       — upcoming/live (feeds daily consensus)
     - out/raw_bettingpros_experts_history_nba.json — scored history (feeds ledger builder)

Requires authenticated session state at data/bettingpros_storage_state.json.
Run scripts/create_bettingpros_storage.py first to create it.

Usage:
    python3 scripts/bettingpros_experts_ingest.py [--debug]
    python3 scripts/bettingpros_experts_ingest.py --user matthewnoonan --debug
    python3 scripts/bettingpros_experts_ingest.py --no-discovery        # use known file only
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    from playwright.sync_api import sync_playwright
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
except ImportError:
    sync_playwright = None
    PlaywrightTimeoutError = Exception

from store import write_json

OUT_DIR = os.getenv("NBA_OUT_DIR", "out")
DEFAULT_STORAGE_STATE = "data/bettingpros_storage_state.json"
KNOWN_EXPERTS_FILE = "data/bettingpros_experts_known.json"
RUN_SUMMARY_FILE = os.path.join(OUT_DIR, "bettingpros_experts_run_summary.json")

# Leaderboard URLs.
# Multiple periods are loaded in order: season → all-time.
# The leaderboard page URL controls which period the site loads by default.
LEADERBOARD_PERIODS = [
    # (label, url) — monthly period intentionally excluded.
    # Prior analysis showed the current-month leaderboard surfaces fluky
    # streaks (too-small samples) and inflates experts with a hot few weeks.
    # Re-enable only with a written rationale and a fresh sample-size analysis.
    ("season",   "https://www.bettingpros.com/nba/accuracy/game-picks/?period=season"),
    ("all_time", "https://www.bettingpros.com/nba/accuracy/game-picks/?period=all"),
]
# Convenience alias used where a single URL is still needed (e.g. session seeding)
LEADERBOARD_URL = LEADERBOARD_PERIODS[0][1]
API_BASE = "https://api.bettingpros.com"

# Min active picks an expert must have to be placed at the front of the run queue.
# Experts with active_pick_count >= this are processed first; the rest follow in
# leaderboard order (already sorted by win rate for the period).
MIN_ACTIVE_PICKS_PRIORITY = 3

# Curated experts always scraped regardless of leaderboard position / quality gate
BETTINGPROS_EXPERT_WHITELIST = [
    "carmenciorra",
    "shefdog33",
    "craig.r.trujillo",
    "mypickswithnoresearch",
]

# Dynamic discovery quality gate
MAX_DYNAMIC_EXPERTS = 15
MIN_WIN_PCT = 0.58
MIN_N_PICKS = 20

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

# BettingPros market_id → market type
MARKET_ID_MAP: Dict[int, str] = {
    129: "spread",
    128: "total",
    127: "moneyline",
}
PROP_MARKET_IDS = {156, 157, 151, 162, 147, 160, 152, 142, 136, 335, 336, 337, 338}


@dataclass
class RawExpertPickRecord:
    source_id: str
    source_surface: str
    sport: str
    market_family: str
    observed_at_utc: str
    canonical_url: str
    raw_pick_text: str
    raw_block: str
    raw_fingerprint: str
    expert_slug: str
    expert_name: str
    user_id: Optional[int] = None
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
    wager_units: Optional[float] = None
    game_date: Optional[str] = None
    pregraded_result: Optional[str] = None
    parlay_id: Optional[str] = None


def sha256_digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def ensure_out_dir() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)


def _bucket_record(record: "RawExpertPickRecord") -> str:
    sport = (record.sport or "").upper()
    is_history = record.pregraded_result in {"WIN", "LOSS", "PUSH"}
    has_date = bool(record.game_date or record.event_start_time_utc)

    if record.parlay_id:
        return "parlays"
    if not has_date:
        return "undated"
    if sport != "NBA":
        return "non_nba_history" if is_history else "non_nba_upcoming"
    return "nba_history" if is_history else "nba_upcoming"


def _bucket_file_map() -> Dict[str, str]:
    return {
        "nba_upcoming": os.path.join(OUT_DIR, "raw_bettingpros_experts_nba.json"),
        "nba_history": os.path.join(OUT_DIR, "raw_bettingpros_experts_history_nba.json"),
        "non_nba_upcoming": os.path.join(OUT_DIR, "raw_bettingpros_experts_non_nba_upcoming.json"),
        "non_nba_history": os.path.join(OUT_DIR, "raw_bettingpros_experts_non_nba_history.json"),
        "parlays": os.path.join(OUT_DIR, "raw_bettingpros_experts_parlays.json"),
        "undated": os.path.join(OUT_DIR, "raw_bettingpros_experts_undated.json"),
    }


def _write_bucketed_records(records: List["RawExpertPickRecord"]) -> Dict[str, int]:
    ensure_out_dir()
    file_map = _bucket_file_map()
    bucketed: Dict[str, List["RawExpertPickRecord"]] = {name: [] for name in file_map}
    for record in records:
        bucketed[_bucket_record(record)].append(record)

    for bucket, path in file_map.items():
        write_json(path, [asdict(r) for r in bucketed[bucket]])

    return {bucket: len(rows) for bucket, rows in bucketed.items()}


def _load_known_experts() -> Dict[str, Any]:
    """Load known experts dict from persistent file."""
    path = Path(KNOWN_EXPERTS_FILE)
    if not path.exists():
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_known_experts(known: Dict[str, Any]) -> None:
    """Save known experts dict to persistent file."""
    path = Path(KNOWN_EXPERTS_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(known, f, indent=2, ensure_ascii=False)


def _result_map(raw_result: Optional[str]) -> Optional[str]:
    """Map BettingPros result strings to WIN/LOSS/PUSH."""
    if not raw_result:
        return None
    r = raw_result.lower()
    if r == "correct":
        return "WIN"
    if r == "incorrect":
        return "LOSS"
    if r == "push":
        return "PUSH"
    return None  # "unscored", "active", etc.


def _parse_pick(
    pick: Dict[str, Any],
    expert_slug: str,
    expert_name: str,
    user_id: int,
    observed_at: datetime,
    parlay_id: Optional[str] = None,
    debug: bool = False,
) -> Optional[RawExpertPickRecord]:
    """Parse a single pick dict from /v3/picks API into a RawExpertPickRecord."""
    sport = pick.get("sport") or ""

    event = pick.get("event") or {}
    away_team = event.get("visitor") or ""
    home_team = event.get("home") or ""
    scheduled = event.get("scheduled") or ""
    game_date = scheduled[:10] if scheduled else ""
    matchup = f"{away_team}@{home_team}" if away_team and home_team else ""

    # Market type — check market_id first (most authoritative for props)
    line_obj = pick.get("line") or {}
    line_val = line_obj.get("line")
    odds = line_obj.get("cost")
    mid = pick.get("market_id")

    if mid in PROP_MARKET_IDS:
        market_type = "player_prop"
    elif mid in MARKET_ID_MAP:
        market_type = MARKET_ID_MAP[mid]
    else:
        raw_type = line_obj.get("type") or ""
        market_type_map = {
            "spread": "spread",
            "moneyline": "moneyline",
            "total": "total",
            "over": "total",
            "under": "total",
        }
        market_type = market_type_map.get(raw_type, raw_type)

    # Selection / direction
    sel_obj = pick.get("selection") or {}
    participant = sel_obj.get("participant") or ""
    direction = (sel_obj.get("selection") or "").upper()

    # For game totals, line.type may encode direction when selection.selection is empty
    if not direction:
        raw_line_type = (line_obj.get("type") or "").upper()
        if raw_line_type in ("OVER", "UNDER"):
            direction = raw_line_type

    if market_type == "total":
        selection = direction if direction in ("OVER", "UNDER") else None
        side = selection
    elif market_type in ("spread", "moneyline"):
        selection = participant or None
        side = selection
    elif market_type == "player_prop":
        note = pick.get("note") or ""
        selection = note or participant or None
        side = direction or None
    else:
        selection = participant or direction or None
        side = selection

    # Skip parlay containers (their legs are handled by the caller)
    parlay_legs = pick.get("parlay") or []
    if len(parlay_legs) > 0 and parlay_id is None:
        return None

    # Risk / wager
    risk_obj = pick.get("risk") or {}
    wager_units = risk_obj.get("units")

    # Result
    result_obj = pick.get("result") or {}
    raw_result = result_obj.get("result")
    pregraded = _result_map(raw_result)

    # Pick text
    note = pick.get("note") or ""
    if note:
        raw_pick_text = note
    elif market_type == "total":
        raw_pick_text = f"{direction} {line_val}" if line_val is not None else direction or "?"
    elif market_type in ("spread", "moneyline"):
        sign = "+" if (line_val is not None and line_val > 0) else ""
        raw_pick_text = f"{participant} {sign}{line_val}" if line_val is not None else participant or "?"
    else:
        raw_pick_text = f"{market_type} {selection}"

    canonical_url = f"https://www.bettingpros.com/u/{expert_slug}/bets/"

    fp_parts = f"bettingpros_experts|{expert_slug}|{sport}|{market_type}|{selection}|{line_val}|{game_date}"
    if parlay_id:
        fp_parts += f"|{parlay_id}"
    fp = sha256_digest(fp_parts)

    raw_block = json.dumps({
        "pick_id": pick.get("id"),
        "pick": pick,
        "expert_slug": expert_slug,
        "expert_name": expert_name,
    })

    event_start_utc = scheduled.replace(" ", "T") + "Z" if scheduled else None
    market_family = "player_prop" if market_type == "player_prop" else "standard"

    record = RawExpertPickRecord(
        source_id="bettingpros_experts",
        source_surface="bettingpros_user",
        sport=sport,
        market_family=market_family,
        observed_at_utc=observed_at.isoformat(),
        canonical_url=canonical_url,
        raw_pick_text=raw_pick_text,
        raw_block=raw_block,
        raw_fingerprint=fp,
        expert_slug=expert_slug,
        expert_name=expert_name,
        user_id=user_id,
        event_start_time_utc=event_start_utc,
        matchup_hint=matchup,
        away_team=away_team or None,
        home_team=home_team or None,
        market_type=market_type or None,
        selection=selection,
        side=side,
        line=float(line_val) if line_val is not None else None,
        odds=int(odds) if odds is not None else None,
        wager_units=float(wager_units) if wager_units is not None else None,
        game_date=game_date or None,
        pregraded_result=pregraded,
        parlay_id=parlay_id,
    )

    if debug:
        print(f"    [pick] {sport} {market_type} {raw_pick_text} line={line_val} result={raw_result}")

    return record


def _process_raw_picks(
    raw_picks: List[Dict],
    slug: str,
    display: str,
    user_id: int,
    observed_at: datetime,
    debug: bool = False,
) -> List[RawExpertPickRecord]:
    """Convert raw pick dicts (with possible parlay containers) to records."""
    records: List[RawExpertPickRecord] = []
    for pick in raw_picks:
        parlay_legs = pick.get("parlay") or []
        if parlay_legs:
            parlay_id = str(uuid.uuid4())
            for leg in parlay_legs:
                rec = _parse_pick(leg, slug, display, user_id, observed_at,
                                  parlay_id=parlay_id, debug=debug)
                if rec:
                    records.append(rec)
        else:
            rec = _parse_pick(pick, slug, display, user_id, observed_at, debug=debug)
            if rec:
                records.append(rec)
    return records


def _scrape_leaderboard_period(
    page: Any,
    label: str,
    url: str,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    """
    Load one leaderboard period page and collect all expert entries via API interception.

    Strategy:
      1. Navigate to the leaderboard page — the site fires /v3/accuracy/picks?page=1.
      2. Read _pagination.total_pages from the first response.
      3. Directly request subsequent pages by clicking the "next page" control or by
         intercepting the XHR and re-requesting with page=N.  We use route interception
         to replay the same request with incrementing page numbers, which is faster and
         more reliable than UI scrolling.
    """
    all_entries: List[Dict] = []
    total_pages = 1  # updated after first response
    next_path: Optional[str] = None

    def capture(resp):
        nonlocal total_pages, next_path
        resp_url = resp.url
        if "/v3/accuracy/picks" not in resp_url:
            return
        try:
            data = resp.json()
            entries = data.get("accuracy") or []
            all_entries.extend(entries)
            pg = data.get("_pagination") or {}
            if not next_path:
                total_pages = pg.get("total_pages") or 1
                next_path = pg.get("next") or ""
                if debug:
                    print(
                        f"  [{label}] page=1 got={len(entries)} "
                        f"total_items={pg.get('total_items')} total_pages={total_pages}"
                    )
            else:
                if debug:
                    print(f"  [{label}] page={pg.get('page')} got={len(entries)}")
        except Exception:
            pass

    page.on("response", capture)
    try:
        page.goto(url, wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(800)
    except Exception as e:
        if debug:
            print(f"  [{label}] page load error: {e}")
    finally:
        page.remove_listener("response", capture)

    # Fetch remaining pages by following the API-provided next links.
    if next_path and total_pages > 1:
        seen_next: Set[str] = set()
        page_num = 2
        cur_next = next_path
        while cur_next and page_num <= total_pages and cur_next not in seen_next:
            seen_next.add(cur_next)
            next_url = cur_next if cur_next.startswith("http") else f"{API_BASE}{cur_next}"
            try:
                resp = page.request.get(next_url, timeout=15000)
                data = resp.json()
                entries = data.get("accuracy") or []
                all_entries.extend(entries)
                pg = data.get("_pagination") or {}
                cur_next = pg.get("next") or ""
                if debug:
                    print(f"  [{label}] page={page_num} got={len(entries)}")
                page_num += 1
            except Exception as e:
                if debug:
                    print(f"  [{label}] page={page_num} fetch error: {e}")
                break

    # Deduplicate and normalise
    experts: List[Dict[str, Any]] = []
    seen_slugs: Set[str] = set()
    for entry in all_entries:
        usr = entry.get("user") or {}
        profile_url = usr.get("profile_url") or ""
        slug = profile_url.strip("/").split("/")[-1] if profile_url else ""
        if not slug or slug in seen_slugs:
            continue
        seen_slugs.add(slug)
        experts.append({
            "slug": slug,
            "display_name": usr.get("name") or usr.get("username") or slug,
            "user_id": usr.get("id"),
            "active_pick_count": usr.get("active_pick_count", 0),
            "periods_seen": [label],
            # Quality gate fields — try multiple possible field names from the API
            "wins":    entry.get("wins") or entry.get("correct") or usr.get("wins") or 0,
            "losses":  entry.get("losses") or entry.get("incorrect") or usr.get("losses") or 0,
            "win_pct": entry.get("win_pct") or entry.get("accuracy") or usr.get("win_pct"),
            "n_picks": entry.get("total_picks") or entry.get("pick_count") or usr.get("total_picks") or 0,
        })

    if debug:
        print(f"  [{label}] {len(experts)} unique experts from {len(all_entries)} entries")

    return experts


def _discover_experts(page: Any, debug: bool = False) -> List[Dict[str, Any]]:
    """
    Load season + all-time leaderboard periods and return quality-gated experts.

    Quality gate: win_pct >= MIN_WIN_PCT AND n_picks >= MIN_N_PICKS.
    Cap: at most MAX_DYNAMIC_EXPERTS returned.
    High-volume experts (active_pick_count >= MIN_ACTIVE_PICKS_PRIORITY) are sorted first.
    """
    all_experts: Dict[str, Dict[str, Any]] = {}

    for label, url in LEADERBOARD_PERIODS:
        try:
            period_experts = _scrape_leaderboard_period(page, label, url, debug=debug)
        except Exception as e:
            if debug:
                print(f"  [discover] {label} error: {e}")
            continue
        for e in period_experts:
            slug = e["slug"]
            if slug not in all_experts:
                all_experts[slug] = e
            else:
                periods = set(all_experts[slug].get("periods_seen") or [])
                periods.update(e.get("periods_seen") or [])
                all_experts[slug]["periods_seen"] = sorted(periods)
                # Keep higher active_pick_count; merge quality stats (take max n_picks)
                if e["active_pick_count"] > all_experts[slug]["active_pick_count"]:
                    all_experts[slug]["active_pick_count"] = e["active_pick_count"]
                if (e.get("win_pct") or 0) > (all_experts[slug].get("win_pct") or 0):
                    all_experts[slug]["win_pct"] = e["win_pct"]
                if (e.get("n_picks") or 0) > (all_experts[slug].get("n_picks") or 0):
                    all_experts[slug]["n_picks"] = e["n_picks"]
                    all_experts[slug]["wins"] = e["wins"]
                    all_experts[slug]["losses"] = e["losses"]
            all_experts[slug]["best_win_pct"] = max(
                e.get("win_pct") or 0,
                all_experts[slug].get("best_win_pct") or 0,
            )
            all_experts[slug]["best_n_picks"] = max(
                e.get("n_picks") or 0,
                all_experts[slug].get("best_n_picks") or 0,
            )

    experts = list(all_experts.values())

    # Apply quality gate
    qualified = []
    skipped = []
    for e in experts:
        wp = e.get("win_pct")
        n = e.get("n_picks") or ((e.get("wins") or 0) + (e.get("losses") or 0))
        if wp is not None and wp >= MIN_WIN_PCT and n >= MIN_N_PICKS:
            qualified.append(e)
        else:
            skipped.append(e)

    # Sort: high-volume first, then by win_pct desc
    high_volume = [e for e in qualified if e["active_pick_count"] >= MIN_ACTIVE_PICKS_PRIORITY]
    rest = [e for e in qualified if e["active_pick_count"] < MIN_ACTIVE_PICKS_PRIORITY]
    high_volume.sort(key=lambda e: -e["active_pick_count"])
    rest.sort(key=lambda e: -(e.get("win_pct") or 0))
    experts = (high_volume + rest)[:MAX_DYNAMIC_EXPERTS]

    if debug:
        print(f"  [discover] {len(all_experts)} total, {len(qualified)} passed quality gate "
              f"(win_pct≥{MIN_WIN_PCT} n≥{MIN_N_PICKS}), {len(skipped)} skipped, "
              f"capped at {len(experts)}/{MAX_DYNAMIC_EXPERTS}")

    return experts


def _fetch_upcoming_picks(
    page: Any,
    slug: str,
    display: str,
    user_id: int,
    observed_at: datetime,
    debug: bool = False,
) -> Tuple[List[RawExpertPickRecord], Dict[str, Any]]:
    """Fetch all upcoming + live picks for a user via intercepted API."""
    picks_store: Dict[str, List[Dict]] = {}

    def capture(resp):
        url = resp.url
        if "api.bettingpros" not in url or "/v3/picks" not in url:
            return
        if f"user={user_id}" not in url:
            return
        if "status=upcoming" in url or "status=live" in url:
            try:
                data = resp.json()
                label = "upcoming" if "upcoming" in url else "live"
                picks_store.setdefault(label, []).extend(data.get("picks") or [])
                if debug:
                    pg = data.get("_pagination", {})
                    print(f"    [{label}] total_items={pg.get('total_items')} in_resp={len(data.get('picks', []))}")
            except Exception:
                pass

    page.on("response", capture)
    try:
        page.goto(f"https://www.bettingpros.com/u/{slug}/bets/",
                  wait_until="networkidle", timeout=20000)
        page.wait_for_timeout(1000)
    except Exception as e:
        if debug:
            print(f"    [warn] page load error for {slug}: {e}")
    finally:
        page.remove_listener("response", capture)

    raw_picks: List[Dict] = []
    for picks in picks_store.values():
        raw_picks.extend(picks)

    # Filter to NBA only — drop non-NBA sports before processing.
    before_sport = len(raw_picks)
    raw_picks = [p for p in raw_picks if (p.get("sport") or "").upper() == "NBA"]

    # Filter to today's games only and not-yet-started only.
    from zoneinfo import ZoneInfo as _ZI
    now_et = datetime.now(_ZI("America/New_York"))
    _today = now_et.strftime("%Y-%m-%d")
    before_date = len(raw_picks)
    raw_picks = [
        p for p in raw_picks
        if ((p.get("event") or {}).get("scheduled") or p.get("event_time") or "")[:10] == _today
    ]
    before_future = len(raw_picks)
    filtered_future: List[Dict] = []
    for p in raw_picks:
        scheduled = ((p.get("event") or {}).get("scheduled") or p.get("event_time") or "").strip()
        if not scheduled:
            continue
        try:
            sched_dt = datetime.fromisoformat(scheduled.replace(" ", "T")).astimezone(_ZI("America/New_York"))
        except Exception:
            # Fallback: if parsing fails but the date matched today, keep the row.
            filtered_future.append(p)
            continue
        if sched_dt > now_et:
            filtered_future.append(p)
    raw_picks = filtered_future

    if debug and (before_sport > len(raw_picks) or before_date > len(raw_picks) or before_future > len(raw_picks)):
        print(
            f"    [filter] {before_sport} total → {before_date} NBA "
            f"→ {before_future} today({_today}) → {len(raw_picks)} future_only"
        )

    stats = {
        "upcoming_raw": len(picks_store["upcoming"]),
        "live_raw": len(picks_store["live"]),
        "total_raw": len(picks_store["upcoming"]) + len(picks_store["live"]),
        "nba_raw": before_date,
        "nba_same_day": before_future,
        "nba_same_day_future": len(raw_picks),
    }

    return _process_raw_picks(raw_picks, slug, display, user_id, observed_at, debug=debug), stats


def _fetch_scored_history(
    ctx: Any,
    page: Any,
    slug: str,
    display: str,
    user_id: int,
    observed_at: datetime,
    start_date: str = "2020-01-01",
    debug: bool = False,
    max_pages: int = 0,
) -> Tuple[List[RawExpertPickRecord], Optional[str], Dict[str, Any]]:
    """
    Fetch full scored pick history for a user.
    Returns (records, latest_game_date) where latest_game_date is the most recent
    game_date seen (for storing in known_experts as last_scored_at).

    Strategy:
      1. Load bets page to get page 1 + capture request headers (x-api-key etc.)
      2. Construct full history URL with start_date=2020-01-01
      3. Paginate through all pages using ctx.request.get with captured headers
    """
    api_headers: Dict[str, str] = {}
    page1_data: Dict[str, Any] = {}
    meta: Dict[str, Any] = {
        "pages_fetched": 0,
        "total_pages_reported": 0,
        "total_items_reported": 0,
        "raw_picks_fetched": 0,
        "sports_seen": {},
        "market_types_seen": {},
        "earliest_game_date": None,
        "latest_game_date": None,
    }

    def cap_req(req):
        url = req.url
        # Capture headers from ANY /v3/picks request for this user — upcoming and
        # scored share the same x-api-key auth headers.
        if f"user={user_id}" in url and "/v3/picks" in url:
            api_headers.update(req.headers)

    def cap_resp(resp):
        url = resp.url
        if f"user={user_id}" in url and "status=scored" in url and "/v3/picks" in url:
            try:
                data = resp.json()
                pg = data.get("_pagination", {})
                picks = data.get("picks") or []
                page1_data["pg"] = pg
                page1_data["picks"] = picks
                if debug:
                    print(f"    [history page1] total={pg.get('total_items')} pages={pg.get('total_pages')} got={len(picks)}")
            except Exception:
                pass

    page.on("request", cap_req)
    page.on("response", cap_resp)
    try:
        page.goto(f"https://www.bettingpros.com/u/{slug}/bets/",
                  wait_until="networkidle", timeout=20000)
        page.wait_for_timeout(1000)
        # If no scored XHR fired yet (user has active upcoming picks, so page
        # shows upcoming tab by default), click the Analysis tab to trigger
        # the scored picks API call. Then press 1Y for full history.
        if not api_headers:
            try:
                # Try clicking the Analysis tab first (per BettingPros UI)
                for selector in [
                    "button:has-text('Analysis')",
                    "[role='tab']:has-text('Analysis')",
                    "button:has-text('History')",
                    "button:has-text('Past')",
                    "[role='tab']:has-text('History')",
                    "a:has-text('History')",
                ]:
                    btn = page.query_selector(selector)
                    if btn:
                        if debug:
                            print(f"    [history] clicking tab: {selector}")
                        btn.click()
                        page.wait_for_timeout(1500)
                        break
                # After clicking Analysis tab, try to click "1Y" period button
                if not api_headers:
                    for yr_sel in [
                        "button:has-text('1Y')",
                        "[aria-label='1Y']",
                        "button:has-text('1 Year')",
                    ]:
                        yr_btn = page.query_selector(yr_sel)
                        if yr_btn:
                            if debug:
                                print(f"    [history] clicking period: {yr_sel}")
                            yr_btn.click()
                            page.wait_for_timeout(1500)
                            break
            except Exception as e:
                if debug:
                    print(f"    [history] tab click error: {e}")
    except Exception as e:
        if debug:
            print(f"    [warn] bets page load error: {e}")
    finally:
        page.remove_listener("request", cap_req)
        page.remove_listener("response", cap_resp)

    if not api_headers:
        if debug:
            print(f"    [warn] no API headers captured for {slug}")
        return [], None, meta

    # Build the full-history URL (extend start date to get all history)
    today = observed_at.strftime("%Y-%m-%d")
    history_url = (
        f"{API_BASE}/v3/picks?events=true&user={user_id}&status=scored"
        f"&include_counts=sport&stats=true&start={start_date}&end={today}&page=1"
    )

    all_raw_picks: List[Dict] = []

    # Fetch page 1 (full range)
    try:
        resp = ctx.request.get(history_url, headers=api_headers)
        if resp.status != 200:
            if debug:
                print(f"    [warn] history page 1 HTTP {resp.status}")
            # Fall back to page1_data from bets page (30-day window)
            all_raw_picks.extend(page1_data.get("picks", []))
        else:
            data = resp.json()
            pg = data.get("_pagination", {})
            picks = data.get("picks") or []
            all_raw_picks.extend(picks)
            total_pages = pg.get("total_pages") or 1
            meta["pages_fetched"] = 1
            meta["total_pages_reported"] = total_pages
            meta["total_items_reported"] = pg.get("total_items") or 0
            next_path = pg.get("next") or ""
            if debug:
                print(f"    [history] total={pg.get('total_items')} pages={total_pages} page1={len(picks)}")

            # Paginate through remaining pages
            if total_pages > 1 and next_path:
                # next_path looks like: /v3/picks?...&page=2&archiveIndexStart=0
                # Replace page=2 with page=N for each subsequent page
                base_next = next_path.replace("&page=2", "&page={pg_num}")
                cap = min(total_pages, max_pages) if max_pages > 0 else total_pages
                for pg_num in range(2, cap + 1):
                    url = API_BASE + base_next.format(pg_num=pg_num)
                    try:
                        r = ctx.request.get(url, headers=api_headers)
                        if r.status == 200:
                            d = r.json()
                            p = d.get("picks") or []
                            all_raw_picks.extend(p)
                            meta["pages_fetched"] += 1
                            if debug and pg_num % 10 == 0:
                                print(f"    [history] page {pg_num}/{total_pages} ({len(all_raw_picks)} total)")
                        else:
                            if debug:
                                print(f"    [warn] history page {pg_num} HTTP {r.status}")
                    except Exception as e:
                        if debug:
                            print(f"    [warn] history page {pg_num} error: {e}")
    except Exception as e:
        if debug:
            print(f"    [warn] history fetch error: {e}")
        all_raw_picks.extend(page1_data.get("picks", []))

    if debug:
        print(f"    [history] {len(all_raw_picks)} total scored picks fetched")

    records = _process_raw_picks(all_raw_picks, slug, display, user_id, observed_at, debug=False)
    meta["raw_picks_fetched"] = len(all_raw_picks)

    # Find most recent game_date for last_scored_at tracking
    latest_date: Optional[str] = None
    earliest_date: Optional[str] = None
    sports_seen: Dict[str, int] = {}
    market_types_seen: Dict[str, int] = {}
    for pick in all_raw_picks:
        sched = (pick.get("event") or {}).get("scheduled") or ""
        gd = sched[:10] if sched else ""
        if gd and (latest_date is None or gd > latest_date):
            latest_date = gd
        if gd and (earliest_date is None or gd < earliest_date):
            earliest_date = gd
        sport = (pick.get("sport") or "UNKNOWN").upper()
        sports_seen[sport] = sports_seen.get(sport, 0) + 1
        mt = str(pick.get("market_id") or pick.get("line", {}).get("type") or "unknown")
        market_types_seen[mt] = market_types_seen.get(mt, 0) + 1

    meta["sports_seen"] = sports_seen
    meta["market_types_seen"] = market_types_seen
    meta["earliest_game_date"] = earliest_date
    meta["latest_game_date"] = latest_date

    return records, latest_date, meta


def ingest_bettingpros_experts(
    storage_state: str = DEFAULT_STORAGE_STATE,
    debug: bool = False,
    filter_user: Optional[str] = None,
    no_discovery: bool = False,
    max_history_pages: int = 0,
) -> Tuple[List[RawExpertPickRecord], List[RawExpertPickRecord], Dict[str, Any]]:
    """
    Main ingestion function.

    Returns:
      (upcoming_records, history_records, dbg)
      - upcoming_records: upcoming/live picks for all experts (feeds daily consensus)
      - history_records: scored picks for new/incremental experts (feeds ledger builder)
    """
    if sync_playwright is None:
        raise ImportError("playwright not installed. Run: pip install playwright && playwright install chromium")

    if not Path(storage_state).exists():
        raise FileNotFoundError(
            f"No storage state at {storage_state}. "
            "Run: python3 scripts/create_bettingpros_storage.py"
        )

    observed_at = datetime.now(timezone.utc)
    today = observed_at.strftime("%Y-%m-%d")
    dbg: Dict[str, Any] = {
        "observed_at": observed_at.isoformat(),
        "users": [],
    }

    known_experts = _load_known_experts()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=USER_AGENT,
            storage_state=storage_state,
            viewport={"width": 1400, "height": 900},
        )
        page = context.new_page()

        # Seed session
        try:
            page.goto("https://www.bettingpros.com/nba/picks/game-picks/",
                      wait_until="domcontentloaded", timeout=15000)
        except Exception:
            pass

        # Step 1: Discover experts from leaderboard (unless --no-discovery)
        discovered: List[Dict[str, Any]] = []
        if not no_discovery:
            if debug:
                print("[bettingpros_experts] Discovering experts from leaderboard...")
            discovered = _discover_experts(page, debug=debug)
            if debug:
                print(f"[bettingpros_experts] {len(discovered)} experts on leaderboard")

        # Step 2: Build run list = discovered ∪ known
        run_experts: Dict[str, Dict[str, Any]] = {}

        # Add all discovered experts (gives us user_id directly from API)
        for e in discovered:
            slug = e["slug"]
            run_experts[slug] = {
                "slug": slug,
                "display_name": e["display_name"],
                "user_id": e["user_id"],
                "active_pick_count": e["active_pick_count"],
            }

        # Merge in known experts (may have user_ids for slugs not on current leaderboard)
        for slug, info in known_experts.items():
            if slug not in run_experts:
                run_experts[slug] = {
                    "slug": slug,
                    "display_name": info.get("display_name") or slug,
                    "user_id": info.get("user_id"),
                    "active_pick_count": 0,
                }

        # Inject whitelist experts — always scraped regardless of leaderboard / quality gate
        for slug in BETTINGPROS_EXPERT_WHITELIST:
            if slug not in run_experts:
                known = known_experts.get(slug, {})
                run_experts[slug] = {
                    "slug": slug,
                    "display_name": known.get("display_name") or slug,
                    "user_id": known.get("user_id"),
                    "active_pick_count": known.get("active_pick_count_last_seen", 0),
                }
            run_experts[slug]["from_whitelist"] = True

        # Filter to single user if --user flag
        if filter_user:
            run_experts = {
                slug: e for slug, e in run_experts.items()
                if slug == filter_user or e["display_name"] == filter_user
            }
            if not run_experts:
                print(f"[bettingpros_experts] No expert found matching --user {filter_user!r}")
                context.close()
                browser.close()
                return [], [], dbg

        daily_experts = [e for e in run_experts.values() if (e.get("active_pick_count") or 0) > 0 or e.get("from_whitelist")]
        history_experts = [
            e for e in run_experts.values()
            if e.get("from_whitelist")
            or not known_experts.get(e["slug"], {}).get("backfilled")
            or (e.get("n_picks") or 0) >= MIN_N_PICKS
        ]
        dbg["discovery"] = {
            "run_experts_total": len(run_experts),
            "daily_experts_total": len(daily_experts),
            "history_experts_total": len(history_experts),
        }

        if debug:
            print(f"[bettingpros_experts] Processing {len(run_experts)} experts")

        all_upcoming: List[RawExpertPickRecord] = []
        all_history: List[RawExpertPickRecord] = []
        other_records: List[RawExpertPickRecord] = []

        # Load existing output files to resume from partial runs
        def _load_existing(path: str) -> List[RawExpertPickRecord]:
            if not os.path.exists(path):
                return []
            try:
                with open(path) as _f:
                    import json as _json
                    raw = _json.load(_f)
                out = []
                for d in raw:
                    try:
                        out.append(RawExpertPickRecord(**d))
                    except Exception:
                        pass
                return out
            except Exception:
                return []
        # Rebuild all non-history buckets from scratch each run so stale upcoming
        # rows never persist across days. Only history is resumable.
        existing_paths = [
            _bucket_file_map()["nba_history"],
            _bucket_file_map()["non_nba_history"],
        ]
        existing_records: List[RawExpertPickRecord] = []
        for path in existing_paths:
            existing_records.extend(_load_existing(path))

        for record in existing_records:
            bucket = _bucket_record(record)
            if bucket == "nba_upcoming":
                all_upcoming.append(record)
            elif bucket == "nba_history":
                all_history.append(record)
            else:
                other_records.append(record)

        _seen_upcoming: Set[str] = {r.raw_fingerprint for r in all_upcoming}
        _seen_history: Set[str] = {r.raw_fingerprint for r in all_history}
        _seen_other: Set[str] = {r.raw_fingerprint for r in other_records}

        for slug, expert in run_experts.items():
            display = expert["display_name"]
            user_id = expert.get("user_id")

            if debug:
                active = expert.get("active_pick_count", 0)
                wl_tag = " [WHITELIST]" if expert.get("from_whitelist") else ""
                print(f"\n[bettingpros_experts] {display} ({slug}) user_id={user_id} active={active}{wl_tag}")

            if not user_id:
                # Try to resolve user_id by loading profile page
                uid_store: Dict[str, int] = {}

                def cap_uid(resp, _slug=slug):
                    url = resp.url
                    if "api.bettingpros" not in url:
                        return
                    if "/v3/user/achievements" in url and "user_id=" in url:
                        try:
                            uid_store["id"] = int(url.split("user_id=")[1].split("&")[0])
                        except Exception:
                            pass
                    if "/v3/picks-summary" in url and "user=" in url:
                        try:
                            uid_str = url.split("user=")[1].split("&")[0]
                            if uid_str.isdigit():
                                uid_store.setdefault("id", int(uid_str))
                        except Exception:
                            pass

                page.on("response", cap_uid)
                try:
                    page.goto(f"https://www.bettingpros.com/u/{slug}/",
                              wait_until="networkidle", timeout=20000)
                    page.wait_for_timeout(500)
                except Exception as e:
                    if debug:
                        print(f"  [warn] profile load error: {e}")
                finally:
                    page.remove_listener("response", cap_uid)

                user_id = uid_store.get("id")
                if not user_id:
                    if debug:
                        print(f"  [skip] could not resolve user_id for {slug}")
                    dbg["users"].append({"slug": slug, "error": "no_user_id"})
                    continue
                expert["user_id"] = user_id

            is_new = slug not in known_experts or not known_experts[slug].get("backfilled")

            # Fetch upcoming / live picks
            upcoming, upcoming_meta = _fetch_upcoming_picks(page, slug, display, user_id, observed_at, debug=debug)

            # Fetch scored history
            history_records: List[RawExpertPickRecord] = []
            history_meta: Dict[str, Any] = {
                "pages_fetched": 0,
                "total_pages_reported": 0,
                "total_items_reported": 0,
                "raw_picks_fetched": 0,
                "sports_seen": {},
                "market_types_seen": {},
                "earliest_game_date": None,
                "latest_game_date": None,
            }
            latest_scored_date: Optional[str] = None

            if is_new:
                # Full history backfill from 2020-01-01
                if debug:
                    print(f"  [new expert] fetching full history from 2020-01-01")
                history_records, latest_scored_date, history_meta = _fetch_scored_history(
                    context, page, slug, display, user_id, observed_at,
                    start_date="2020-01-01", debug=debug, max_pages=max_history_pages,
                )
            else:
                history_records = []
                # Incremental: fetch scored picks newer than last_scored_at
                last_scored = known_experts[slug].get("last_scored_at") or "2020-01-01"
                if last_scored < today:
                    history_records, latest_scored_date, history_meta = _fetch_scored_history(
                        context, page, slug, display, user_id, observed_at,
                        start_date=last_scored, debug=debug, max_pages=max_history_pages,
                    )

            # Dedup and route records into cleaned buckets.
            for r in upcoming:
                bucket = _bucket_record(r)
                if bucket == "nba_upcoming":
                    if r.raw_fingerprint not in _seen_upcoming:
                        _seen_upcoming.add(r.raw_fingerprint)
                        all_upcoming.append(r)
                elif r.raw_fingerprint not in _seen_other:
                    _seen_other.add(r.raw_fingerprint)
                    other_records.append(r)

            for r in history_records:
                bucket = _bucket_record(r)
                if bucket == "nba_history":
                    if r.raw_fingerprint not in _seen_history:
                        _seen_history.add(r.raw_fingerprint)
                        all_history.append(r)
                elif r.raw_fingerprint not in _seen_other:
                    _seen_other.add(r.raw_fingerprint)
                    other_records.append(r)

            # Update known_experts
            known_experts[slug] = {
                "user_id": user_id,
                "display_name": display,
                "first_seen": known_experts.get(slug, {}).get("first_seen") or today,
                "last_active": today,
                "last_scored_at": latest_scored_date or known_experts.get(slug, {}).get("last_scored_at"),
                "earliest_scored_at": history_meta.get("earliest_game_date") or known_experts.get(slug, {}).get("earliest_scored_at"),
                "backfilled": True,
                "active_pick_count_last_seen": expert.get("active_pick_count", 0),
                "periods_seen_last": expert.get("periods_seen", []),
                "best_win_pct_last": expert.get("best_win_pct") or expert.get("win_pct"),
                "best_n_picks_last": expert.get("best_n_picks") or expert.get("n_picks"),
                "last_sports_seen": history_meta.get("sports_seen", {}),
            }

            dbg["users"].append({
                "slug": slug,
                "display_name": display,
                "user_id": user_id,
                "periods_seen": expert.get("periods_seen", []),
                "active_pick_count": expert.get("active_pick_count", 0),
                "n_picks": expert.get("n_picks"),
                "win_pct": expert.get("win_pct"),
                "upcoming_picks": len(upcoming),
                "upcoming_meta": upcoming_meta,
                "daily_nba_active": bool(upcoming_meta.get("nba_same_day_future")),
                "history_picks": len(history_records),
                "is_new": is_new,
                "from_whitelist": expert.get("from_whitelist", False),
                "history_meta": history_meta,
            })

            if debug:
                print(f"  → upcoming={len(upcoming)} history={len(history_records)}")

            # Save incrementally after each expert so partial runs preserve progress
            dbg["bucket_counts"] = _write_bucketed_records(all_upcoming + all_history + other_records)
            _save_known_experts(known_experts)

        context.close()
        browser.close()

    dbg["total_upcoming"] = len(all_upcoming)
    dbg["total_history"] = len(all_history)
    dbg["bucket_counts"] = _write_bucketed_records(all_upcoming + all_history + other_records)
    dbg["daily_nba_active_experts"] = sum(1 for u in dbg.get("users", []) if u.get("daily_nba_active"))
    return all_upcoming, all_history, dbg


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Ingest BettingPros user expert picks (dynamic discovery + full history)"
    )
    ap.add_argument("--storage", default=DEFAULT_STORAGE_STATE,
                    help="Path to Playwright storage state JSON")
    ap.add_argument("--debug", action="store_true", help="Enable verbose debug output")
    ap.add_argument("--dry-run", action="store_true", help="Don't write output files")
    ap.add_argument("--user", default=None,
                    help="Only ingest a single user by slug or display_name (for testing)")
    ap.add_argument("--no-discovery", action="store_true",
                    help="Skip leaderboard discovery; use whitelist + known experts file only")
    ap.add_argument("--max-history-pages", type=int, default=0,
                    help="Cap history backfill at N pages per expert (0=unlimited, default=0)")
    args = ap.parse_args()

    print(f"[bettingpros_experts] Starting ingest (storage={args.storage})")
    upcoming, history, dbg = ingest_bettingpros_experts(
        storage_state=args.storage,
        debug=args.debug,
        filter_user=args.user,
        no_discovery=args.no_discovery,
        max_history_pages=args.max_history_pages,
    )

    wl_count = sum(1 for u in dbg.get("users", []) if u.get("from_whitelist"))
    dyn_count = len(dbg.get("users", [])) - wl_count
    print(f"[bettingpros_experts] experts: {wl_count} whitelist + {dyn_count} dynamic")
    print(f"[bettingpros_experts] upcoming={dbg.get('total_upcoming', len(upcoming))} history={dbg.get('total_history', len(history))}")
    for u in dbg.get("users", []):
        up = u.get("upcoming_picks", 0)
        hist = u.get("history_picks", 0)
        if up or hist:
            new_tag = " [NEW]" if u.get("is_new") else ""
            wl_tag = " [WL]" if u.get("from_whitelist") else ""
            print(f"  {u.get('display_name', u['slug'])}: upcoming={up} history={hist}{new_tag}{wl_tag}")

    if args.dry_run:
        print("[bettingpros_experts] Dry run — not writing files")
        return

    ensure_out_dir()
    write_json(RUN_SUMMARY_FILE, dbg)

    # Raw files already written incrementally during the run
    raw_upcoming_path = os.path.join(OUT_DIR, "raw_bettingpros_experts_nba.json")
    raw_history_path = os.path.join(OUT_DIR, "raw_bettingpros_experts_history_nba.json")
    upcoming_dicts = [asdict(r) for r in upcoming]
    print(f"[bettingpros_experts] Raw upcoming → {raw_upcoming_path} ({len(upcoming)} records)")
    print(f"[bettingpros_experts] Raw history  → {raw_history_path} ({len(history)} records)")

    # Normalize upcoming for consensus — filter to today's games only
    from zoneinfo import ZoneInfo
    target_date = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    from src.normalizer_bettingpros_experts_nba import normalize_bettingpros_experts_records
    normalized = normalize_bettingpros_experts_records(
        upcoming_dicts, debug=args.debug, target_date=target_date
    )
    norm_path = os.path.join(OUT_DIR, "normalized_bettingpros_experts_nba.json")
    write_json(norm_path, normalized)
    eligible = sum(1 for r in normalized if r.get("eligible_for_consensus"))
    print(f"[bettingpros_experts] Wrote normalized → {norm_path} ({eligible}/{len(normalized)} eligible, date={target_date})")
    print(f"[bettingpros_experts] Wrote summary → {RUN_SUMMARY_FILE}")


if __name__ == "__main__":
    main()
