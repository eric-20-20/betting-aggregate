from __future__ import annotations

import argparse
import hashlib
import json
import random
import sys
import time
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import re
from dataclasses import asdict

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bs4 import BeautifulSoup

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
except ImportError as exc:
    print("Playwright is required for Action backfill. Install with `pip install playwright` then `playwright install chromium`.", file=sys.stderr)
    raise

from action_ingest import (
    OUT_DIR,
    ACTION_LISTING_URLS,
    ACTION_GAME_PATTERNS,
    NBA_SPORT,
    NCAAB_SPORT,
    RawPickRecord,
    dedupe_normalized_bets,
    extract_picks_from_html,
    fetch_html_in_context,
    json_default,
    normalize_pick,
    parse_game_date_from_url,
    parse_game_start_utc,
    parse_teams_from_page,
    parse_teams_from_slug,
)


LEFT_ARROW_PATH_D = "M15.41 7.41L14 6l-6 6 6 6 1.41-1.41L10.83 12z"
DEFAULT_MAX_DAYS = 7


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill Action picks (NBA or NCAAB) by walking the By Game listing backwards.")
    parser.add_argument("--sport", type=str, default="NBA", choices=["NBA", "NCAAB"], help="Sport to backfill (default: NBA).")
    parser.add_argument("--max-days", type=int, default=DEFAULT_MAX_DAYS, help="Number of day steps to traverse backward (default: 7).")
    parser.add_argument("--stop-date", type=str, default=None, help="Stop once listing day is older than this date (YYYY-MM-DD).")
    parser.add_argument("--max-games-per-day", type=int, default=None, help="Optional cap on games processed per day.")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")
    parser.add_argument("--headful", action="store_true", help="Run browser in headed mode for debugging.")
    return parser.parse_args()


def load_state(state_path: Path) -> Dict:
    if not state_path.exists():
        return {
            "visited_game_urls": set(),
            "visited_day_keys": set(),
            "last_day_label": None,
            "last_run_at_utc": None,
        }
    with state_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return {
        "visited_game_urls": set(data.get("visited_game_urls", [])),
        "visited_day_keys": set(data.get("visited_day_keys", [])),
        "last_day_label": data.get("last_day_label"),
        "last_run_at_utc": data.get("last_run_at_utc"),
    }


def href_fingerprint(hrefs: List[str]) -> str:
    if not hrefs:
        return ""
    return hashlib.sha256("||".join(sorted(hrefs)).encode("utf-8")).hexdigest()


def save_state(state_path: Path, state: Dict) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "visited_game_urls": sorted(state.get("visited_game_urls", [])),
        "visited_day_keys": sorted(state.get("visited_day_keys", [])),
        "last_day_label": state.get("last_day_label"),
        "last_run_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    with state_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def append_jsonl(path: Path, items: List[dict]) -> None:
    if not items:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for item in items:
            json.dump(item, f, ensure_ascii=False, default=json_default)
            f.write("\n")


def read_day_label(page) -> Optional[str]:
    label_el = page.query_selector("span.day-nav__display")
    if not label_el:
        return None
    try:
        return label_el.inner_text().strip()
    except Exception:
        return None


def parse_label_month_day(label: Optional[str]) -> Optional[Tuple[int, int]]:
    if not label:
        return None
    cleaned = " ".join(label.strip().split()).upper()
    match = re.search(r"\b([A-Z]{3})\s+([A-Z]{3})\s+(\d{1,2})\b", cleaned)
    if not match:
        return None
    _, mon, day = match.groups()
    months = {
        "JAN": 1,
        "FEB": 2,
        "MAR": 3,
        "APR": 4,
        "MAY": 5,
        "JUN": 6,
        "JUL": 7,
        "AUG": 8,
        "SEP": 9,
        "OCT": 10,
        "NOV": 11,
        "DEC": 12,
    }
    if mon not in months:
        return None
    return months[mon], int(day)


def maybe_dismiss_popups(page) -> None:
    """
    Best-effort click for common consent/notice buttons.
    """
    selectors = [
        "button:has-text(\"Accept\")",
        "button:has-text(\"Agree\")",
        "button:has-text(\"Got it\")",
        "text=Accept",
        "text=AGREE",
    ]
    for sel in selectors:
        try:
            btn = page.query_selector(sel)
            if btn:
                btn.click()
                page.wait_for_timeout(300)
        except Exception:
            continue


def collect_visible_game_hrefs(page, game_slug: str = "nba-game") -> Tuple[List[str], List[str]]:
    preferred = page.locator(f"a.game-picks-header[href*=\"/{game_slug}/\"]")
    locator = preferred
    try:
        preferred.first.wait_for(state="attached", timeout=10000)
    except PlaywrightTimeoutError:
        locator = page.locator(f"a[href*=\"/{game_slug}/\"]")
        try:
            locator.first.wait_for(state="attached", timeout=10000)
        except PlaywrightTimeoutError:
            return [], []

    try:
        all_hrefs: List[str] = locator.evaluate_all("els => els.map(e => e.getAttribute('href')).filter(Boolean)")
        visible_hrefs: List[str] = locator.evaluate_all(
            """
            els => els
              .filter(el => {
                const style = window.getComputedStyle(el);
                return style && style.visibility !== 'hidden' && style.display !== 'none' && el.offsetParent !== null;
              })
              .map(el => el.getAttribute('href'))
              .filter(Boolean)
            """
        )
    except Exception:
        return [], []
    return visible_hrefs, all_hrefs


def scrape_game_urls_for_current_day(page, debug: bool = False, day_label: Optional[str] = None, game_pattern=None, game_slug: str = "nba-game") -> List[str]:
    visible_hrefs, all_hrefs = collect_visible_game_hrefs(page, game_slug=game_slug)

    urls: List[str] = []
    for href in visible_hrefs:
        if not href:
            continue
        if not (game_pattern.search(href) if game_pattern else re.search(r"/nba-game/[^/]+/\d+", href)):
            continue
        full = href
        if href.startswith("/"):
            full = f"https://www.actionnetwork.com{href}"
        if full not in urls:
            urls.append(full)
    if debug:
        print(f"[DEBUG] listing links visible={len(visible_hrefs)} total={len(all_hrefs)}")
    if debug and len(urls) == 0:
        safe_day = (day_label or "unknown").replace(" ", "_").replace("/", "-")
        debug_path = Path(OUT_DIR) / f"debug_listing_{safe_day}.html"
        debug_path.parent.mkdir(parents=True, exist_ok=True)
        with debug_path.open("w", encoding="utf-8") as f:
            f.write(page.content())
        print(f"[DEBUG] No URLs found; wrote debug HTML to {debug_path}")
    return urls


def find_previous_day_button_by_svg_path(page):
    try:
        page.wait_for_selector("span.day-nav__display", state="visible", timeout=30000)
    except Exception:
        return None

    day_nav = page.locator(".day-nav")
    if day_nav.count() == 0:
        day_nav = page.locator("[class*='day-nav']")

    path_locator = day_nav.locator(f"path[d=\"{LEFT_ARROW_PATH_D}\"]").first
    try:
        path_locator.wait_for(state="attached", timeout=3000)
    except Exception:
        path_locator = page.locator(f"path[d=\"{LEFT_ARROW_PATH_D}\"]").first
        try:
            path_locator.wait_for(state="attached", timeout=5000)
        except Exception:
            return None

    ancestors = [
        path_locator.locator("xpath=ancestor::button[1]").first,
        path_locator.locator("xpath=ancestor::*[@role='button'][1]").first,
        path_locator.locator("xpath=ancestor::a[1]").first,
        path_locator.locator("xpath=ancestor::*[@tabindex][1]").first,
    ]
    for anc in ancestors:
        try:
            anc.wait_for(state="attached", timeout=1500)
            return anc
        except Exception:
            continue
    return None


def find_previous_day_button_structural(page):
    nav = page.locator(".day-nav")
    if nav.count() == 0:
        nav = page.locator("[class*='day-nav']")
    try:
        nav.first.wait_for(state="visible", timeout=5000)
    except Exception:
        return None

    preferred = nav.locator(
        "button[aria-label*='prev' i], button[aria-label*='back' i], "
        "[role='button'][aria-label*='prev' i], [role='button'][aria-label*='back' i], "
        "button[title*='prev' i], button[title*='back' i]"
    )
    candidates = preferred
    if preferred.count() == 0:
        candidates = nav.locator("button, [role='button']")

    handles = candidates.element_handles()
    best_idx = None
    best_x = None
    for idx, handle in enumerate(handles):
        try:
            if not handle.is_visible() or not handle.is_enabled():
                continue
            box = handle.bounding_box()
            if not box:
                continue
            if best_x is None or box["x"] < best_x:
                best_x = box["x"]
                best_idx = idx
        except Exception:
            continue
    return candidates.nth(best_idx) if best_idx is not None else None


def wait_for_listing_update(
    page,
    prev_fingerprint: Optional[str],
    prev_first_href: Optional[str],
    target_label: Optional[str],
    timeout_ms: int = 15000,
    debug: bool = False,
    game_slug: str = "nba-game",
) -> Tuple[bool, List[str]]:
    target_md = parse_label_month_day(target_label)
    end_time = time.time() + timeout_ms / 1000.0
    last_visible: List[str] = []

    def infer_md_from_hrefs(hrefs: List[str]) -> Optional[Tuple[int, int]]:
        for href in hrefs[:6]:
            dt = parse_game_date_from_url(href)
            if dt:
                return (dt.month, dt.day)
        return None

    while time.time() < end_time:
        visible_hrefs, _ = collect_visible_game_hrefs(page, game_slug=game_slug)
        last_visible = visible_hrefs
        new_fp = href_fingerprint(visible_hrefs)
        first_href = visible_hrefs[0] if visible_hrefs else None
        fp_changed = bool(prev_fingerprint and new_fp and new_fp != prev_fingerprint)
        first_changed = bool(prev_first_href and first_href and first_href != prev_first_href)
        inferred_md = infer_md_from_hrefs(visible_hrefs)

        if target_md:
            md_matches = (inferred_md == target_md)
            change_signal = fp_changed or first_changed or (prev_fingerprint is None and prev_first_href is None)
            if md_matches and change_signal:
                if debug:
                    fp_from = (prev_fingerprint or "")[:8]
                    fp_to = (new_fp or "")[:8]
                    print(f"[DEBUG] listing ready (target {target_md}) inferred={inferred_md} fp {fp_from} -> {fp_to}")
                return True, visible_hrefs
            if debug and (fp_changed or first_changed):
                print(f"[DEBUG] listing changed but not target. Target={target_md} inferred={inferred_md} fp_changed={fp_changed} first_changed={first_changed}")
        else:
            if fp_changed:
                if debug:
                    print(f"[DEBUG] listing updated fp {prev_fingerprint[:8]} -> {new_fp[:8]}")
                return True, visible_hrefs
            if first_changed:
                if debug:
                    print(f"[DEBUG] listing updated via first href change.")
                return True, visible_hrefs
        page.wait_for_timeout(500)
    return False, last_visible


def go_to_previous_day(page, current_label: Optional[str], listing_url: str, debug: bool = False, game_slug: str = "nba-game") -> bool:
    prev_label = current_label or read_day_label(page)
    prev_visible, _ = collect_visible_game_hrefs(page, game_slug=game_slug)
    prev_fp = href_fingerprint(prev_visible)
    prev_first_href = prev_visible[0] if prev_visible else None

    def attempt_find_button() -> Optional[object]:
        btn_loc = find_previous_day_button_by_svg_path(page)
        if btn_loc:
            return btn_loc
        return find_previous_day_button_structural(page)

    def reload_listing():
        try:
            page.goto(listing_url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(500)
        except Exception:
            page.goto(listing_url, wait_until="load", timeout=30000)
            page.wait_for_timeout(800)
        maybe_dismiss_popups(page)

    def wait_for_change(label: Optional[str], timeout: int) -> bool:
        if label:
            try:
                page.wait_for_function(
                    "(expected) => document.querySelector('span.day-nav__display')?.innerText.trim() !== expected",
                    arg=label,
                    timeout=timeout,
                )
                return True
            except PlaywrightTimeoutError:
                return False
        else:
            page.wait_for_timeout(800)
            return True

    def click_and_wait(btn_loc) -> Tuple[bool, Optional[str]]:
        try:
            btn_loc.click()
        except Exception:
            try:
                btn_loc.click(force=True)
            except Exception:
                return False, None
        if not wait_for_change(prev_label, 8000):
            if debug:
                print("[DEBUG] day label unchanged; retrying click with force.")
            try:
                btn_loc.click(force=True)
            except Exception:
                return False, None
            if not wait_for_change(prev_label, 6000):
                return False, None
        return True, read_day_label(page)

    def navigate(btn_loc) -> bool:
        label_changed, new_label = click_and_wait(btn_loc)
        if not label_changed:
            return False
        updated, _ = wait_for_listing_update(
            page,
            prev_fingerprint=prev_fp,
            prev_first_href=prev_first_href,
            target_label=new_label,
            timeout_ms=15000,
            debug=debug,
            game_slug=game_slug,
        )
        return updated

    btn = attempt_find_button()
    if not btn:
        reload_listing()
        prev_label = read_day_label(page)
        btn = attempt_find_button()
    if not btn:
        safe_label = (prev_label or "unknown").replace(" ", "_").replace("/", "-")
        debug_path = Path(OUT_DIR) / f"nav_debug_{safe_label}.html"
        debug_path.parent.mkdir(parents=True, exist_ok=True)
        with debug_path.open("w", encoding="utf-8") as f:
            f.write(page.content())
        print("[WARN] Could not find previous-day button after retries; wrote debug snapshot.")
        return False

    if navigate(btn):
        return True
    if debug:
        print("[DEBUG] listing stale after navigation; retrying click.")
    if navigate(btn):
        return True

    reload_listing()
    btn = attempt_find_button()
    if btn and navigate(btn):
        return True

    safe_label = (prev_label or "unknown").replace(" ", "_").replace("/", "-")
    debug_path = Path(OUT_DIR) / f"nav_debug_{safe_label}.html"
    debug_path.parent.mkdir(parents=True, exist_ok=True)
    with debug_path.open("w", encoding="utf-8") as f:
        f.write(page.content())
    print("[WARN] Listing did not update after navigation attempts; wrote debug snapshot.")
    return False


def infer_date_from_urls(urls: List[str]) -> Optional[date]:
    for url in urls:
        parsed = parse_game_date_from_url(url)
        if parsed:
            return parsed
    return None


def ingest_game(
    context,
    url: str,
    observed_at: datetime,
    stats: Dict[str, int],
    sport: str = "NBA",
    debug: bool = False,
) -> Tuple[List[RawPickRecord], List[dict]]:
    last_html: Optional[str] = None
    last_exc: Optional[Exception] = None
    for attempt in range(1, 4):
        try:
            if debug:
                print(f"[DEBUG] fetch attempt {attempt} for {url}")
            wait_until = "domcontentloaded" if attempt < 3 else "load"
            html = fetch_html_in_context(context, url, wait_until=wait_until, timeout_ms=15000)
            last_html = html
            break
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            last_exc = exc
            if debug:
                print(f"[DEBUG] fetch attempt {attempt} failed: {type(exc).__name__} {exc}")
            time.sleep(0.5 * attempt)
    else:
        raise RuntimeError(f"Failed to fetch game page after retries: {last_exc}")

    soup = BeautifulSoup(html, "html.parser")
    away_team, home_team = parse_teams_from_page(soup, sport=sport)
    if not (away_team and home_team):
        slug_away, slug_home = parse_teams_from_slug(url, sport=sport)
        away_team = away_team or slug_away
        home_team = home_team or slug_home
    if not (away_team and home_team):
        raise RuntimeError("Could not resolve teams for game page.")

    event_start_time_utc = parse_game_start_utc(
        html=html,
        url=url,
        schedule_game=None,
        observed_at_utc=observed_at,
    )
    raw_records, card_infos = extract_picks_from_html(
        html,
        canonical_url=url,
        observed_at_utc=observed_at,
        debug=debug,
    )

    normalized: List[dict] = []
    for record in raw_records:
        record.event_start_time_utc = event_start_time_utc
        norm = normalize_pick(record, home_team=home_team, away_team=away_team, stats=stats, sport=sport)
        normalized.append(norm)
    if debug:
        preview = [r.raw_pick_text for r in raw_records[:3]]
        print(f"[DEBUG] {url} picks={len(raw_records)} preview={preview}")
    return raw_records, normalized


def main() -> None:
    args = parse_args()
    sport = args.sport.upper()
    sport_key = NBA_SPORT if sport == "NBA" else NCAAB_SPORT
    listing_url = ACTION_LISTING_URLS[sport_key]
    game_pattern = ACTION_GAME_PATTERNS[sport_key]
    # Derive slug from pattern (e.g. "nba-game" or "ncaab-game")
    game_slug = "ncaab-game" if sport == "NCAAB" else "nba-game"

    out_dir = Path(OUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    stop_date = None
    if args.stop_date:
        stop_date = datetime.strptime(args.stop_date, "%Y-%m-%d").date()

    sport_lower = sport.lower()
    state_path = out_dir / f"action_backfill_state_{sport_lower}.json"
    state = load_state(state_path)

    raw_jsonl_path = out_dir / f"raw_action_{sport_lower}_backfill.jsonl"
    normalized_jsonl_path = out_dir / f"normalized_action_{sport_lower}_backfill.jsonl"
    summary_path = out_dir / f"action_backfill_run_summary_{sport_lower}.json"

    observed_at = datetime.now(timezone.utc)
    summary = {
        "sport": sport,
        "days_processed": 0,
        "games_discovered": 0,
        "games_ingested": 0,
        "raw_picks": 0,
        "normalized_total": 0,
        "normalized_eligible": 0,
        "errors": [],
        "per_day": [],
        "stop_date": stop_date.isoformat() if stop_date else None,
        "max_days": args.max_days,
        "max_games_per_day": args.max_games_per_day,
    }

    stats = {"team_not_in_game": 0}
    seen_day_labels: set[str] = set()
    last_inferred_date: Optional[date] = None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not args.headful)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        page = context.new_page()
        try:
            try:
                page.goto(listing_url, wait_until="networkidle", timeout=45000)
                page.wait_for_timeout(2000)
            except PlaywrightTimeoutError:
                page.goto(listing_url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(3000)
            page.wait_for_selector("span.day-nav__display", timeout=30000)
            maybe_dismiss_popups(page)
        except Exception as exc:
            print(f"[ERROR] Could not load listing page: {exc}")
            try:
                browser.close()
            except Exception:
                pass
            raise

        def write_debug_snapshot(label: Optional[str], prefix: str = "loop_debug") -> Path:
            safe_label = (label or "unknown").replace(" ", "_").replace("/", "-")
            debug_path = out_dir / f"{prefix}_{safe_label}.html"
            debug_path.parent.mkdir(parents=True, exist_ok=True)
            with debug_path.open("w", encoding="utf-8") as f:
                f.write(page.content())
            return debug_path

        def write_game_snapshot(url: str, label: Optional[str]) -> Path:
            slug = url.rstrip("/").split("/")[-1][:40]
            safe_label = (label or "unknown").replace(" ", "_").replace("/", "-")
            debug_path = out_dir / f"game_fail_{slug}_{safe_label}.html"
            debug_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                with debug_path.open("w", encoding="utf-8") as f:
                    f.write(page.content())
            except Exception:
                pass
            return debug_path

        try:
            for day_idx in range(args.max_days):
                current_label = read_day_label(page)
                maybe_dismiss_popups(page)
                if current_label in seen_day_labels:
                    snap = write_debug_snapshot(current_label, prefix="loop_debug")
                    print(f"[WARN] Detected repeated day label {current_label}; stopping. Snapshot: {snap}")
                    break
                seen_day_labels.add(current_label)
                try:
                    day_game_urls = scrape_game_urls_for_current_day(page, debug=args.debug, day_label=current_label, game_pattern=game_pattern, game_slug=game_slug)
                except PlaywrightTimeoutError:
                    day_game_urls = []
                if args.max_games_per_day:
                    day_game_urls = day_game_urls[: args.max_games_per_day]

                date_inferred = infer_date_from_urls(day_game_urls)
                if last_inferred_date and date_inferred and date_inferred > last_inferred_date:
                    snap = write_debug_snapshot(current_label, prefix="loop_debug")
                    print(f"[WARN] Inferred date moved forward ({date_inferred} > {last_inferred_date}); stopping. Snapshot: {snap}")
                    break
                if date_inferred:
                    last_inferred_date = date_inferred
                if stop_date and date_inferred and date_inferred < stop_date:
                    if args.debug:
                        print(f"[DEBUG] Stop-date reached at {date_inferred.isoformat()} (label={current_label}).")
                    break

                day_key = f"{current_label}|{date_inferred.isoformat() if date_inferred else ''}"
                state["visited_day_keys"].add(day_key)
                summary["days_processed"] += 1
                summary["games_discovered"] += len(day_game_urls)

                if args.debug:
                    preview_urls = day_game_urls[:3]
                    print(f"[DEBUG] Day {day_idx + 1}: {current_label} urls={len(day_game_urls)} preview={preview_urls}")

                day_raw: List[RawPickRecord] = []
                day_normalized: List[dict] = []
                day_errors: List[dict] = []
                games_ingested = 0

                for url in day_game_urls:
                    if url in state["visited_game_urls"]:
                        continue
                    try:
                        raw_records, normalized = ingest_game(
                            context=context,
                            url=url,
                            observed_at=observed_at,
                            stats=stats,
                            sport=sport,
                            debug=args.debug,
                        )
                        day_raw.extend(raw_records)
                        day_normalized.extend(normalized)
                        state["visited_game_urls"].add(url)
                        games_ingested += 1
                        time.sleep(random.uniform(0.2, 0.6))
                    except Exception as exc:
                        day_errors.append({"url": url, "error": str(exc)})
                        summary["errors"].append({"url": url, "error": str(exc)})
                        snap = write_game_snapshot(url, current_label)
                        if args.debug:
                            print(f"[DEBUG] Error ingesting {url}: {exc} snapshot={snap}")
                        continue

                day_normalized = dedupe_normalized_bets(day_normalized)
                append_jsonl(raw_jsonl_path, [asdict(r) for r in day_raw])
                append_jsonl(normalized_jsonl_path, day_normalized)

                summary["games_ingested"] += games_ingested
                summary["raw_picks"] += len(day_raw)
                summary["normalized_total"] += len(day_normalized)
                summary["normalized_eligible"] += sum(1 for item in day_normalized if item.get("eligible_for_consensus"))
                summary["per_day"].append(
                    {
                        "day_label": current_label,
                        "date_inferred": date_inferred.isoformat() if date_inferred else None,
                        "games_count": len(day_game_urls),
                        "games_ingested": games_ingested,
                        "picks_count": len(day_raw),
                        "errors": day_errors,
                    }
                )
                state["last_day_label"] = current_label
                save_state(state_path, state)

                if day_idx + 1 >= args.max_days:
                    break
                if not go_to_previous_day(page, current_label=current_label, listing_url=listing_url, debug=args.debug, game_slug=game_slug):
                    break
        except KeyboardInterrupt:
            print("[WARN] Interrupted by user; saving state and exiting.")
        finally:
            save_state(state_path, state)
            with summary_path.open("w", encoding="utf-8") as f:
                json.dump(summary, f, ensure_ascii=False, indent=2, default=json_default)
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass
    print(
        f"[RUN] days={summary['days_processed']} games_seen={summary['games_discovered']} "
        f"games_ingested={summary['games_ingested']} raw_picks={summary['raw_picks']} "
        f"normalized={summary['normalized_total']} eligible={summary['normalized_eligible']} "
        f"errors={len(summary['errors'])}"
    )


if __name__ == "__main__":
    main()
