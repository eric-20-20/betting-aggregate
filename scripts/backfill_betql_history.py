#!/usr/bin/env python3
"""Historical backfill for BetQL NBA spreads/totals/sharps.

Examples:
  python3 scripts/backfill_betql_history.py --start 2026-01-20 --end 2026-02-04 --types spreads,totals,sharps
  python3 scripts/backfill_betql_history.py --start 2026-01-25 --end 2026-01-27 --types spreads --dry-run --debug
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.betql_playwright import BetQLSession, wait_for_ready, require_storage_state
from src.betql_extractors import (
    extract_spread_columns,
    extract_totals_columns,
    extract_sharp_spreads_totals,
)
from src.ingest.betql_datepicker import switch_date
from src.ingest.betql_backfill_ready import wait_for_extractor_ready
from store import ensure_dir


logger = logging.getLogger("betql.backfill")


def write_jsonl_atomic(path: Path, rows: Iterable[Dict[str, object]]) -> None:
    """Atomically write iterable rows to JSONL."""
    ensure_dir(str(path.parent))
    fd, tmp_path = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
                f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        raise


def date_range(start: date, end: date, max_days: int | None = None) -> List[date]:
    if end < start:
        raise ValueError("end date must be >= start date")
    today = date.today()
    end = min(end, today)
    dates: List[date] = []
    cur = start
    while cur <= end:
        dates.append(cur)
        if max_days and len(dates) >= max_days:
            break
        cur += timedelta(days=1)
    return dates


def is_no_events(page, row_selector: str, row_timeout: int = 8000) -> Tuple[bool, bool]:
    """
    Fast check for a no-events page.
    Returns (no_events_detected, row_found_early).
    """
    try:
        status_locator = page.locator("p.games-view__status-text")
        status_locator.wait_for(state="visible", timeout=2000)
        status_text = (status_locator.inner_text() or "").lower()
        if any(tok in status_text for tok in ("no events", "no games", "no picks")):
            return True, False
    except Exception:
        pass
    try:
        locator = page.locator(row_selector)
        locator.wait_for(state="attached", timeout=row_timeout)
        return False, locator.count() > 0
    except Exception:
        pass
    return False, False


@dataclass
class SurfaceConfig:
    url: str
    surface: str
    extractor: Callable
    out_name: str
    expected_substrings: List[str]
    row_selector: str


TYPE_CONFIG: Dict[str, SurfaceConfig] = {
    "spreads": SurfaceConfig(
        url="https://betql.co/nba/odds",
        surface="model",
        extractor=extract_spread_columns,
        out_name="spreads",
        expected_substrings=["/_next/data/", "/nba/odds"],
        row_selector="div.games-table-column__team-cell",
    ),
    "totals": SurfaceConfig(
        url="https://betql.co/nba/odds/totals-spread",
        surface="model",
        extractor=extract_totals_columns,
        out_name="totals",
        expected_substrings=["/_next/data/", "/nba/odds/totals-spread"],
        row_selector="div.games-table-column__team-cell",
    ),
    "sharps": SurfaceConfig(
        url="https://betql.co/nba/sharp-picks",
        surface="sharps",
        extractor=extract_sharp_spreads_totals,
        out_name="sharps",
        expected_substrings=["/_next/data/", "/nba/sharp-picks"],
        row_selector="div.games-table-column__team-cell",
    ),
}


def _select_page(sess: BetQLSession, cfg: SurfaceConfig, refresh_cache: bool, page):
    if page and not refresh_cache:
        return page
    if page:
        try:
            page.close()
        except Exception:
            pass
    new_page = sess.open(cfg.url)
    # Don't require wait_for_ready here - if today has no games, the page will show
    # marketing content. The date picker will be used to navigate to dates with games.
    # Just wait for the page to be minimally loaded.
    try:
        new_page.wait_for_load_state("domcontentloaded", timeout=15000)
    except Exception:
        pass
    # Try wait_for_ready but don't fail if it times out (no games today)
    try:
        wait_for_ready(new_page, cfg.surface, timeout=10000)
    except Exception:
        # Page loaded but no games today - that's OK, we'll switch dates
        logger.debug("Initial page has no games (possibly no games today), continuing with date picker")
    return new_page


def _ingest_date(
    page,
    cfg: SurfaceConfig,
    target_date: date,
    dry_run: bool,
    debug: bool,
    retries: int,
) -> Tuple[Dict[str, object], List[Dict[str, object]]]:
    attempt = 0
    last_error = None
    while attempt < retries:
        attempt += 1
        try:
            info = switch_date(
                page,
                target_date,
                logger=logger,
                expected_url_substrings=cfg.expected_substrings,
            )
            # Fast gate: detect no-games or early row presence
            no_events, row_found = is_no_events(page, cfg.row_selector)
            if no_events:
                logger.info("[%s] %s NO_GAMES (fast gate)", cfg.out_name, target_date.isoformat())
                return (
                    {
                        "status": "NO_GAMES",
                        "date": target_date.isoformat(),
                        "rows": 0,
                        "response_url": info.get("triggered_url"),
                        "button_text": info.get("final_button_text"),
                        "debug_rows": 0,
                        "ready": {"ready": True, "attempts": 0, "elapsed_ms": 0, "timeout": False},
                        "dbg": {"fast_gate": "no_events"},
                        "row_selector_count": 0,
                    },
                    [],
                )
            # If no rows found yet, continue to full wait logic instead of bailing early
            if not row_found:
                logger.debug("[%s] %s no rows in fast gate, continuing to full wait", cfg.out_name, target_date.isoformat())

            # Ensure page-level readiness (reuse existing signals)
            try:
                wait_for_ready(page, cfg.surface)
            except Exception:
                page.wait_for_selector(cfg.row_selector, state="attached", timeout=15000)

            row_selector_count = None
            selector_found = False
            try:
                page.wait_for_selector(cfg.row_selector, state="attached", timeout=15000)
                row_selector_count = page.locator(cfg.row_selector).count()
                selector_found = row_selector_count > 0
            except Exception:
                selector_found = False

            def _ready_pred(records, dbg):
                return len(records) > 0

            extractor_args = (page, cfg.url)
            records, dbg, ready_meta = wait_for_extractor_ready(
                page=page,
                extractor_fn=cfg.extractor,
                extractor_args=extractor_args,
                ready_predicate=_ready_pred,
                timeout_ms=15000,
                poll_ms=500,
                logger=logger,
            )

            if dry_run:
                return (
                    {
                        "status": "DRY_RUN",
                        "date": target_date.isoformat(),
                        "rows": 0,
                        "response_url": info.get("triggered_url"),
                        "button_text": info.get("final_button_text"),
                        "ready": ready_meta,
                        "dbg": dbg,
                        "row_selector_count": row_selector_count,
                    },
                    [],
                )

            rows = list(records)
            if ready_meta.get("ready"):
                if len(rows) > 0:
                    status = "OK"
                elif selector_found:
                    status = "NO_GAMES"
                else:
                    status = "TIMEOUT_NOT_READY"
            else:
                status = "TIMEOUT_NOT_READY"
            if len(rows) == 0 and logger.isEnabledFor(logging.DEBUG):
                try:
                    snippet = page.locator("body").inner_text()[:300]
                    logger.debug("[body-snippet] %s %s %s", cfg.out_name, target_date.isoformat(), snippet)
                except Exception:
                    pass
            return (
                {
                    "status": status,
                    "date": target_date.isoformat(),
                    "rows": len(rows),
                    "response_url": info.get("triggered_url"),
                    "button_text": info.get("final_button_text"),
                    "debug_rows": dbg.get("rows") if isinstance(dbg, dict) else None,
                    "ready": ready_meta,
                    "dbg": dbg,
                    "row_selector_count": row_selector_count,
                },
                rows,
            )
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            logger.warning(
                "[%s] attempt %s failed for %s: %s",
                cfg.out_name,
                attempt,
                target_date.isoformat(),
                exc,
            )
            page.wait_for_timeout(500)
            if attempt >= retries:
                raise
            # Refresh page state before retrying to avoid stale DOM.
            page.goto(cfg.url, wait_until="domcontentloaded")
            wait_for_ready(page, cfg.surface)
    raise last_error  # pragma: no cover


def _already_scraped(out_root: Path, cfg: SurfaceConfig, target_date: date) -> bool:
    """Check if we already have data for this date (file exists - empty means no games that day)."""
    out_path = out_root / cfg.out_name / f"{target_date.isoformat()}.jsonl"
    return out_path.exists()


def run_for_type(
    typ: str,
    dates: List[date],
    storage_state: str,
    headless: bool,
    dry_run: bool,
    debug: bool,
    refresh_cache: bool,
    retries: int,
    out_root: Path,
    skip_existing: bool = True,
) -> List[Dict[str, object]]:
    cfg = TYPE_CONFIG[typ]
    typ_logger = logging.getLogger(f"betql.backfill.{typ}")
    records_meta: List[Dict[str, object]] = []

    # Filter out already-scraped dates if skip_existing is True
    dates_to_scrape = dates
    if skip_existing and not dry_run:
        dates_to_scrape = [d for d in dates if not _already_scraped(out_root, cfg, d)]
        skipped = len(dates) - len(dates_to_scrape)
        if skipped > 0:
            typ_logger.info("[%s] Skipping %s already-scraped dates, %s remaining", typ, skipped, len(dates_to_scrape))

    if not dates_to_scrape:
        typ_logger.info("[%s] All dates already scraped, nothing to do", typ)
        return records_meta

    with BetQLSession(storage_state_path=storage_state, headless=headless) as sess:
        page = None
        for i, target_date in enumerate(dates_to_scrape):
            page = _select_page(sess, cfg, refresh_cache, page)
            try:
                meta, rows = _ingest_date(
                    page=page,
                    cfg=cfg,
                    target_date=target_date,
                    dry_run=dry_run,
                    debug=debug,
                    retries=retries,
                )
            except Exception as exc:
                meta = {
                    "status": "TIMEOUT_NOT_READY",
                    "date": target_date.isoformat(),
                    "rows": 0,
                    "error": str(exc),
                }
                rows = []
                typ_logger.warning("[%s] timeout/not ready for %s: %s", typ, target_date.isoformat(), exc)
            records_meta.append(meta)
            typ_logger.debug(
                "[%s] ready_wait ready=%s attempts=%s elapsed_ms=%s dbg=%s len=%s selector_count=%s current_url=%s trigger=%s",
                typ,
                (meta.get("ready") or {}).get("ready"),
                (meta.get("ready") or {}).get("attempts"),
                (meta.get("ready") or {}).get("elapsed_ms"),
                meta.get("dbg"),
                meta.get("rows"),
                meta.get("row_selector_count"),
                page.url if page else None,
                meta.get("response_url"),
            )
            progress = f"[{i+1}/{len(dates_to_scrape)}]"
            typ_logger.info(
                "%s [%s] %s rows=%s status=%s trigger=%s btn=%s ready=%s attempts=%s elapsed_ms=%s",
                progress,
                typ,
                target_date.isoformat(),
                meta.get("rows"),
                meta.get("status"),
                meta.get("response_url"),
                meta.get("button_text"),
                (meta.get("ready") or {}).get("ready"),
                (meta.get("ready") or {}).get("attempts"),
                (meta.get("ready") or {}).get("elapsed_ms"),
            )

            if dry_run:
                continue

            out_path = out_root / cfg.out_name / f"{target_date.isoformat()}.jsonl"
            write_jsonl_atomic(out_path, rows)
    return records_meta


def main(args: argparse.Namespace) -> None:
    import time as _time
    require_storage_state(args.storage)
    types = [t.strip().lower() for t in args.types.split(",") if t.strip()]
    unknown = [t for t in types if t not in TYPE_CONFIG]
    if unknown:
        raise ValueError(f"Unsupported type(s): {unknown}. Choose from {list(TYPE_CONFIG)}")

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()
    dates = date_range(start, end, max_days=args.max_days)
    if not dates:
        logger.warning("No dates to process (start=%s end=%s today=%s)", start, end, date.today())
        return

    # Pre-check how many dates need scraping
    out_root = Path("data/history/betql")
    if not args.force:
        for typ in types:
            cfg = TYPE_CONFIG[typ]
            existing = sum(1 for d in dates if _already_scraped(out_root, cfg, d))
            remaining = len(dates) - existing
            est_minutes = remaining * 8 / 60  # ~8s per date average
            logger.info(
                "[%s] %s dates total, %s already scraped, %s to scrape (est. %.1f min)",
                typ, len(dates), existing, remaining, est_minutes
            )

    logger.info(
        "Starting BetQL backfill types=%s dates=%s..%s (count=%s) headless=%s dry_run=%s refresh_cache=%s force=%s",
        types,
        dates[0],
        dates[-1],
        len(dates),
        args.headless,
        args.dry_run,
        args.refresh_cache,
        args.force,
    )
    backfill_start = _time.time()

    all_meta: List[Dict[str, object]] = []
    for typ in types:
        meta = run_for_type(
            typ=typ,
            dates=dates,
            storage_state=args.storage,
            headless=args.headless,
            dry_run=args.dry_run,
            debug=args.debug,
            refresh_cache=args.refresh_cache,
            retries=args.retries,
            out_root=out_root,
            skip_existing=not args.force,
        )
        all_meta.extend(meta)

    total_rows = sum(m.get("rows", 0) for m in all_meta if m.get("status") == "OK")
    elapsed = _time.time() - backfill_start
    ok_count = sum(1 for m in all_meta if m.get("status") == "OK")
    avg_per_date = elapsed / ok_count if ok_count > 0 else 0
    logger.info(
        "Backfill complete: %s rows across %s tasks in %.1fs (%.1fs avg per date)",
        total_rows, len(all_meta), elapsed, avg_per_date
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill BetQL NBA history via date picker")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD (inclusive)")
    parser.add_argument(
        "--types",
        default="spreads,totals,sharps",
        help="Comma list from: spreads, totals, sharps",
    )
    parser.add_argument("--storage", default="data/betql_storage_state.json")
    parser.add_argument("--headless", default=True, action=argparse.BooleanOptionalAction)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--dry-run", dest="dry_run", action="store_true")
    parser.add_argument("--refresh-cache", dest="refresh_cache", action="store_true")
    parser.add_argument("--max-days", type=int, default=None, help="Safety cap on total days to run")
    parser.add_argument("--retries", type=int, default=3, help="Retries per date/type")
    parser.add_argument("--force", action="store_true", help="Re-scrape even if data already exists")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    main(args)
