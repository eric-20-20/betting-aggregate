#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    from playwright.sync_api import sync_playwright
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
except ImportError:
    print("ERROR: playwright not installed. Run: pip install playwright && playwright install chromium")
    sys.exit(1)

from sportsline_ingest import EXTRACT_EXPERT_PICKS_JS, _parse_matchup_text, _parse_pick_text, _map_team, sha256_digest
from store import get_data_store, MLB_SPORT, NBA_SPORT, NCAAB_SPORT


DEFAULT_STORAGE = "data/sportsline_storage_state.json"
DEFAULT_INDEX = "out/raw_sportsline_experts_index_v2.json"
DEFAULT_STATE_DIR = "out"
DEFAULT_OUTPUT_DIR = "out"
DEFAULT_DELAY_MS = 1200
DEFAULT_MAX_CLICKS = 3000
DEFAULT_STOP_DATE = "2023-10-01"
LEAGUE_PARAM_MAP = {
    "NBA": "nba",
    "MLB": "mlb",
    "NHL": "nhl",
    "NCAAB": "cbb",
    "NFL": "nfl",
    "NCAAF": "cfb",
}

SUPPORTED_SPORTS = {"NBA", "MLB", "NCAAB", "NFL", "NCAAF", "NHL", "UNKNOWN"}

NBA_TEAMS = {
    "ATL","BOS","BKN","CHA","CHI","CLE","DAL","DEN","DET","GSW","HOU","IND","LAC","LAL","MEM","MIA",
    "MIL","MIN","NOP","NYK","OKC","ORL","PHI","PHX","POR","SAC","SAS","TOR","UTA","WAS",
}
MLB_TEAMS = {
    "ARI","ATL","BAL","BOS","CHC","CWS","CIN","CLE","COL","DET","HOU","KC","LAA","LAD","MIA","MIL",
    "MIN","NYM","NYY","OAK","PHI","PIT","SD","SEA","SF","STL","TB","TEX","TOR","WAS",
}
NFL_TEAMS = {
    "ARI","ATL","BAL","BUF","CAR","CHI","CIN","CLE","DAL","DEN","DET","GB","HOU","IND","JAX","KC","LV",
    "LAC","LAR","MIA","MIN","NE","NO","NYG","NYJ","PHI","PIT","SEA","SF","TB","TEN","WAS",
}
NHL_TEAMS = {
    "ANA","ARI","BOS","BUF","CGY","CAR","CHI","COL","CBJ","DAL","DET","EDM","FLA","LAK","MIN","MTL",
    "NSH","NJD","NYI","NYR","OTT","PHI","PIT","SEA","SJS","STL","TBL","TOR","VAN","VGK","WSH","WPG",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SportsLine expert backfill v2 (raw-first, all sports)")
    parser.add_argument("--storage", default=DEFAULT_STORAGE)
    parser.add_argument("--experts-index", default=DEFAULT_INDEX)
    parser.add_argument("--experts", nargs="+", metavar="SLUG")
    parser.add_argument("--max-experts", type=int, default=None)
    parser.add_argument("--max-clicks", type=int, default=DEFAULT_MAX_CLICKS)
    parser.add_argument("--delay-ms", type=int, default=DEFAULT_DELAY_MS)
    parser.add_argument("--stop-date", default=DEFAULT_STOP_DATE)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--no-combine", action="store_true")
    parser.add_argument("--headful", action="store_true")
    parser.add_argument("--debug", action="store_true")
    return parser.parse_args()


def load_experts(index_path: Path) -> List[Dict[str, Any]]:
    payload = json.loads(index_path.read_text(encoding="utf-8"))
    return payload.get("experts") or []


def state_path(slug: str) -> Path:
    return ROOT / DEFAULT_STATE_DIR / f"sportsline_expert_{slug}_all_state_v2.json"


def output_path(slug: str) -> Path:
    return ROOT / DEFAULT_OUTPUT_DIR / f"raw_sportsline_expert_{slug}_all_backfill_v2.jsonl"


def combined_output_path() -> Path:
    return ROOT / DEFAULT_OUTPUT_DIR / "raw_sportsline_expert_pages_all_combined_v2.jsonl"


def summary_output_path() -> Path:
    return ROOT / DEFAULT_OUTPUT_DIR / "sportsline_backfill_v2_summary.json"


def load_state(slug: str) -> Dict[str, Any]:
    p = state_path(slug)
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return {
        "seen_fingerprints": [],
        "clicks_done": 0,
        "total_records": 0,
        "earliest_date_seen": None,
        "last_run_at_utc": None,
        "stop_reason": None,
    }


def save_state(slug: str, state: Dict[str, Any]) -> None:
    p = state_path(slug)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def append_records(path: Path, records: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def write_records(path: Path, records: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_existing_records(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    records: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            records.append(json.loads(line))
    return records


def min_iso_date(*values: Optional[str]) -> Optional[str]:
    dates = [value for value in values if value]
    return min(dates) if dates else None


def semantic_fingerprint(
    expert_slug: str,
    matchup_text: str,
    pick_text: str,
    event_time: Optional[str],
) -> str:
    return sha256_digest(
        "|".join(
            [
                "sportsline",
                "expert_pages_v2",
                expert_slug,
                matchup_text,
                pick_text,
                event_time or "",
            ]
        )
    )


def summarize_records(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_sport = Counter()
    parseable_market_types = Counter()
    earliest_seen: Optional[str] = None
    for record in records:
        sport = record.get("sport")
        if sport:
            by_sport[sport] += 1
        market_type = ((record.get("parsed_preview") or {}).get("market_type"))
        if market_type:
            parseable_market_types[market_type] += 1
        day_date = ((record.get("matchup") or {}).get("day_date"))
        earliest_seen = min_iso_date(earliest_seen, day_date)
    return {
        "provisional_sport_counts": dict(by_sport),
        "parseable_market_types": dict(parseable_market_types),
        "earliest_date_seen": earliest_seen,
    }


def build_profile_variants(base_url: str) -> List[tuple[str, str]]:
    variants: List[tuple[str, str]] = [("ROOT", base_url)]
    base = base_url.rstrip("/")
    for label, param in LEAGUE_PARAM_MAP.items():
        variants.append((label, f"{base}/?league={param}"))
    return variants


def click_profile_filter(page, label: str) -> bool:
    script = """
    (label) => {
      const marker = Array.from(document.querySelectorAll('body *')).find((el) => {
        const txt = (el.innerText || '').trim();
        return txt.includes('All Against the Spread Money Line Over / Under Props');
      });
      const markerTop = marker ? marker.getBoundingClientRect().top : 1000;
      const candidates = Array.from(document.querySelectorAll('button, a, div, span'))
        .filter((el) => {
          const txt = (el.innerText || '').trim();
          if (txt !== label) return false;
          const rect = el.getBoundingClientRect();
          if (rect.width < 20 || rect.height < 10) return false;
          if (rect.top < 0 || rect.top > markerTop) return false;
          if (marker && markerTop - rect.top > 260) return false;
          return true;
        })
        .sort((a, b) => b.getBoundingClientRect().top - a.getBoundingClientRect().top);
      if (!candidates.length) return false;
      candidates[0].click();
      return true;
    }
    """
    try:
        return bool(page.evaluate(script, label))
    except Exception:
        return False


def extract_all_cards(page) -> List[Dict[str, Any]]:
    try:
        return page.evaluate(EXTRACT_EXPERT_PICKS_JS) or []
    except Exception:
        return []


def _map_team_for_sport(alias: Optional[str], sport: str) -> Optional[str]:
    if not alias:
        return None
    if sport in {NBA_SPORT, NCAAB_SPORT, MLB_SPORT}:
        store = get_data_store(sport)
        codes = store.lookup_team_code(alias)
        if len(codes) == 1:
            return next(iter(codes))
    return None


def _map_team_multi_sport(alias: Optional[str]) -> Dict[str, Optional[str]]:
    return {
        "NBA": _map_team_for_sport(alias, NBA_SPORT),
        "NCAAB": _map_team_for_sport(alias, NCAAB_SPORT),
        "MLB": _map_team_for_sport(alias, MLB_SPORT),
        "GENERIC": _map_team(alias, sport="NBA") if alias else None,
    }


def card_fingerprint(cards: List[Dict[str, Any]]) -> str:
    key = "||".join(
        sorted(
            f"{card.get('matchupText','')}|{card.get('pickText','')}|{card.get('timestampText','')}"
            for card in cards
        )
    )
    return sha256_digest(key)


def infer_sport_from_card(card: Dict[str, Any], away_code: Optional[str], home_code: Optional[str]) -> str:
    full = " | ".join(
        [
            card.get("fullText", ""),
            card.get("matchupText", ""),
            card.get("pickText", ""),
            card.get("analysisText", ""),
        ]
    )
    full_upper = full.upper()
    if " MLB " in f" {full_upper} " or "MLB PICKS" in full_upper or "MLB ML PICKS" in full_upper or "MLB PLAYER PROPS PICKS" in full_upper:
        return "MLB"
    if " NBA " in f" {full_upper} " or "NBA PICKS" in full_upper or "NBA PLAYER PROPS PICKS" in full_upper:
        return "NBA"
    if " NCAAB " in f" {full_upper} " or " CBB " in f" {full_upper} " or "COLLEGE BASKETBALL" in full_upper:
        return "NCAAB"
    if " NFL " in f" {full_upper} " or "NFL PICKS" in full_upper or "NFL PLAYER PROPS PICKS" in full_upper:
        return "NFL"
    if " NCAAF " in f" {full_upper} " or "COLLEGE FOOTBALL" in full_upper or "CFB PICKS" in full_upper:
        return "NCAAF"
    if " NHL " in f" {full_upper} " or "NHL PICKS" in full_upper:
        return "NHL"
    if any(tag in full_upper for tag in ("CHLG", "LIGA", "SERI", "BUND", "EPL", "SOCCER", "DRAW NO BET")):
        return "UNKNOWN"

    codes = {c for c in (away_code, home_code) if c}
    if codes and codes <= NBA_TEAMS:
        return "NBA"
    if codes and codes <= MLB_TEAMS and not (codes <= NBA_TEAMS):
        return "MLB"
    if codes and codes <= NHL_TEAMS and not (codes <= NBA_TEAMS):
        return "NHL"
    if codes and codes <= NFL_TEAMS and not (codes <= NBA_TEAMS):
        return "NFL"
    return "UNKNOWN"


def parse_card_record(
    card: Dict[str, Any],
    expert: Dict[str, Any],
    observed_at: datetime,
    seen_fps: Set[str],
    canonical_url: str,
) -> Optional[Dict[str, Any]]:
    matchup_text = card.get("matchupText", "")
    pick_text = card.get("pickText", "")
    full_text = card.get("fullText", "")
    if not pick_text:
        return None

    matchup = _parse_matchup_text(matchup_text)
    away_raw = matchup.get("away_raw")
    home_raw = matchup.get("home_raw")
    away_map = _map_team_multi_sport(away_raw)
    home_map = _map_team_multi_sport(home_raw)
    away_code = away_map["GENERIC"]
    home_code = home_map["GENERIC"]

    pick = _parse_pick_text(pick_text)
    inferred_sport = infer_sport_from_card(card, away_code, home_code)
    if inferred_sport == "UNKNOWN":
        if away_map["MLB"] and home_map["MLB"]:
            inferred_sport = "MLB"
        elif away_map["NBA"] and home_map["NBA"]:
            inferred_sport = "NBA"
        elif away_map["NCAAB"] and home_map["NCAAB"]:
            inferred_sport = "NCAAB"
    event_time = matchup.get("event_time_utc")
    fp = semantic_fingerprint(
        expert.get("expert_slug", ""),
        matchup_text,
        pick_text,
        event_time,
    )
    if fp in seen_fps:
        return None
    seen_fps.add(fp)

    result = pick.get("result")
    coverage_lines = []
    for line in full_text.split("|"):
        line = line.strip()
        if "PICKS" in line.upper() or re.search(r"\bIN LAST \d+\b", line, re.IGNORECASE):
            coverage_lines.append(line)

    return {
        "source_id": "sportsline",
        "source_surface": "sportsline_expert_pages_backfill_v2",
        "sport": inferred_sport,
        "observed_at_utc": observed_at.isoformat(),
        "canonical_url": canonical_url,
        "expert_slug": expert.get("expert_slug"),
        "expert_name": expert.get("expert_name"),
        "expert_profile": expert.get("profile_url"),
        "raw_fingerprint": fp,
        "raw_pick_text": pick_text[:1000],
        "raw_block": full_text[:4000],
        "raw_card": {
            "matchup_text": matchup_text,
            "pick_text": pick_text,
            "analysis_text": card.get("analysisText", "")[:4000],
            "timestamp_text": card.get("timestampText", ""),
            "expert_text": card.get("expertText", ""),
            "full_text": full_text[:4000],
            "coverage_lines": coverage_lines[:20],
        },
        "parsed_preview": {
            "market_type": pick.get("market_type"),
            "team_raw": pick.get("team_raw"),
            "side": pick.get("side"),
            "line": pick.get("line"),
            "odds": pick.get("odds"),
            "player_name": pick.get("player_name"),
            "stat_key": pick.get("stat_key"),
            "result": result,
        },
        "matchup": {
            "away_raw": away_raw,
            "home_raw": home_raw,
            "away_code": away_code,
            "home_code": home_code,
            "away_code_candidates": away_map,
            "home_code_candidates": home_map,
            "event_time_utc": event_time,
            "day_date": matchup.get("day_date"),
            "away_score": matchup.get("away_score"),
            "home_score": matchup.get("home_score"),
        },
        "provisional_sport_reason": inferred_sport,
        "pregraded_result": result,
    }


def get_earliest_date_iso(page) -> Optional[str]:
    body = page.locator("body").text_content(timeout=3000) or ""
    dates = re.findall(r"((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d+\s+202[3-9])", body)
    if not dates:
        return None
    month_map = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,"Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
    parsed = []
    for item in dates:
        mon, day, year = item.split()
        parsed.append(date(int(year), month_map[mon], int(day)))
    return min(parsed).isoformat() if parsed else None


def scrape_expert(
    expert: Dict[str, Any],
    storage_path: str,
    stop_date: date,
    max_clicks: int,
    delay_ms: int,
    resume: bool,
    observed_at: datetime,
    headful: bool,
    debug: bool,
) -> Dict[str, Any]:
    slug = expert["expert_slug"]
    url = expert["profile_url"]
    output = output_path(slug)
    state = load_state(slug)
    existing_records = load_existing_records(output)
    existing_fps = set(state.get("seen_fingerprints", []))
    for record in existing_records:
        raw_fp = record.get("raw_fingerprint")
        if raw_fp:
            existing_fps.add(raw_fp)
        raw_card = record.get("raw_card") or {}
        matchup = record.get("matchup") or {}
        existing_fps.add(
            semantic_fingerprint(
                expert.get("expert_slug", ""),
                raw_card.get("matchup_text", ""),
                raw_card.get("pick_text", ""),
                matchup.get("event_time_utc"),
            )
        )
    seen_fps: Set[str] = existing_fps
    clicks_done = state.get("clicks_done", 0) if resume else 0
    total_new = 0
    stop_reason = None
    by_sport = Counter()
    parseable_market_types = Counter()
    parse_failures = 0
    stalled_iterations = 0
    all_new_records: List[Dict[str, Any]] = []
    existing_summary = summarize_records(existing_records)
    earliest_seen = min_iso_date(state.get("earliest_date_seen"), existing_summary.get("earliest_date_seen"))

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not headful)
        context = browser.new_context(
            storage_state=storage_path,
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )
        page = context.new_page()
        variant_stop_reason = None
        for variant_label, variant_url in build_profile_variants(url):
            variant_clicks = 0
            stalled_iterations = 0
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=45000)
            except PlaywrightTimeoutError:
                page.goto(url, wait_until="load", timeout=45000)
            page.wait_for_timeout(2500)
            if variant_label != "ROOT":
                clicked = click_profile_filter(page, variant_label)
                page.wait_for_timeout(1200)
                if not clicked or "/experts/" not in page.url:
                    try:
                        page.goto(variant_url, wait_until="domcontentloaded", timeout=45000)
                    except PlaywrightTimeoutError:
                        page.goto(variant_url, wait_until="load", timeout=45000)
                    page.wait_for_timeout(1800)

            while variant_clicks < max_clicks:
                cards = extract_all_cards(page)
                before_fp = card_fingerprint(cards)
                new_records: List[Dict[str, Any]] = []
                for card in cards:
                    pick = _parse_pick_text(card.get("pickText", ""))
                    if pick.get("market_type"):
                        parseable_market_types[pick["market_type"]] += 1
                    else:
                        parse_failures += 1
                    record = parse_card_record(card, expert, observed_at, seen_fps, variant_url)
                    if record:
                        record["profile_variant"] = variant_label
                        new_records.append(record)
                        by_sport[record["sport"]] += 1
                if new_records:
                    all_new_records.extend(new_records)
                    total_new += len(new_records)

                earliest = get_earliest_date_iso(page)
                earliest_seen = min_iso_date(earliest_seen, earliest)
                total_clicks = clicks_done + variant_clicks
                if debug or total_clicks % 20 == 0 or new_records:
                    print(
                        f"{slug}[{variant_label}]: clicks={variant_clicks} new={total_new} earliest={earliest or '?'} "
                        f"cards={len(cards)} sports={dict(by_sport)} parse_failures={parse_failures}"
                    )
                if earliest and earliest <= stop_date.isoformat():
                    variant_stop_reason = "stop_date"
                    break

                lb = page.locator("button:has-text('Load More')")
                if lb.count() == 0:
                    variant_stop_reason = "no_more_cards"
                    break

                try:
                    lb.first.scroll_into_view_if_needed()
                    lb.first.click()
                    page.wait_for_timeout(delay_ms)
                    variant_clicks += 1
                    after_cards = extract_all_cards(page)
                    after_fp = card_fingerprint(after_cards)
                    if after_fp == before_fp:
                        stalled_iterations += 1
                    else:
                        stalled_iterations = 0
                    if stalled_iterations >= 3:
                        variant_stop_reason = "stalled_no_new_cards"
                        break
                except Exception:
                    variant_stop_reason = "load_more_error"
                    break

            clicks_done += variant_clicks
            if variant_stop_reason == "stop_date":
                stop_reason = "stop_date"
                break

        context.close()
        browser.close()

    final_records = existing_records + all_new_records
    final_summary = summarize_records(final_records)

    save_state(
        slug,
        {
            "seen_fingerprints": list(seen_fps),
            "clicks_done": clicks_done,
            "total_records": len(final_records),
            "earliest_date_seen": min_iso_date(earliest_seen, final_summary.get("earliest_date_seen")),
            "last_run_at_utc": observed_at.isoformat(),
            "stop_reason": stop_reason or "max_clicks",
            "provisional_sport_counts": final_summary.get("provisional_sport_counts"),
            "parseable_market_types": final_summary.get("parseable_market_types"),
            "parse_failures": parse_failures,
            "stalled_iterations": stalled_iterations,
        },
    )
    if final_records:
        write_records(output, final_records)
    return {
        "expert_slug": slug,
        "expert_name": expert.get("expert_name"),
        "profile_url": url,
        "output_path": str(output),
        "new_records": total_new,
        "existing_records": len(existing_records),
        "final_records": len(final_records),
        "clicks_done": clicks_done,
        "stop_reason": stop_reason or "max_clicks",
        "provisional_sport_counts": final_summary.get("provisional_sport_counts"),
        "parseable_market_types": final_summary.get("parseable_market_types"),
        "parse_failures": parse_failures,
        "stalled_iterations": stalled_iterations,
    }


def combine_outputs(experts: List[Dict[str, Any]]) -> int:
    seen: Set[str] = set()
    out = combined_output_path()
    count = 0
    with out.open("w", encoding="utf-8") as wf:
        for expert in experts:
            path = output_path(expert["expert_slug"])
            if not path.exists():
                continue
            with path.open("r", encoding="utf-8") as rf:
                for line in rf:
                    if not line.strip():
                        continue
                    rec = json.loads(line)
                    fp = rec.get("raw_fingerprint")
                    if fp and fp in seen:
                        continue
                    seen.add(fp)
                    wf.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    count += 1
    return count


def main() -> int:
    args = parse_args()
    storage_path = ROOT / args.storage
    index_path = ROOT / args.experts_index
    if not storage_path.exists():
        print(f"ERROR: Storage state not found: {storage_path}")
        return 1
    if not index_path.exists():
        print(f"ERROR: Experts index not found: {index_path}")
        print("Run scripts/discover_sportsline_experts_v2.py first")
        return 1

    experts = load_experts(index_path)
    if args.experts:
        wanted = set(args.experts)
        experts = [expert for expert in experts if expert.get("expert_slug") in wanted]
    if args.max_experts is not None:
        experts = experts[: args.max_experts]
    stop_date = date.fromisoformat(args.stop_date)
    observed_at = datetime.now(timezone.utc)

    summaries = []
    for expert in experts:
        summaries.append(
            scrape_expert(
                expert=expert,
                storage_path=str(storage_path),
                stop_date=stop_date,
                max_clicks=args.max_clicks,
                delay_ms=args.delay_ms,
                resume=args.resume,
                observed_at=observed_at,
                headful=args.headful,
                debug=args.debug,
            )
        )

    combined_count = None
    if not args.no_combine:
        combined_count = combine_outputs(experts)

    summary = {
        "observed_at_utc": observed_at.isoformat(),
        "experts_run": summaries,
        "combined_output_path": str(combined_output_path()) if combined_count is not None else None,
        "combined_output_count": combined_count,
    }
    summary_output_path().write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote summary -> {summary_output_path()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
