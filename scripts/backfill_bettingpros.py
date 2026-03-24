"""
BettingPros expert picks backfill script.

Fetches 1-year historical picks from each expert's analysis page:
  https://www.bettingpros.com/nba/picks/experts/{slug}/analysis/

The analysis page shows a table of historical picks with W/L/P results.
Uses infinite scroll to load all records, then "show picks" button if needed.

Usage:
    python3 scripts/backfill_bettingpros.py [--debug] [--storage PATH]
    python3 scripts/backfill_bettingpros.py --expert dimers --debug
    python3 scripts/backfill_bettingpros.py --probe --expert dimers
    python3 scripts/backfill_bettingpros.py --start-date 2026-01-01 --end-date 2026-03-01

Output:
    - out/raw_bettingpros_backfill_nba.json
    - out/normalized_bettingpros_backfill_nba.json

After running, re-run consensus + ledger builder + grader to incorporate
historical BettingPros picks into signal history.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import asdict
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    from playwright.sync_api import sync_playwright, Page
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
except ImportError:
    sync_playwright = None
    PlaywrightTimeoutError = Exception

from store import write_json
from bettingpros_ingest import (
    RawPickRecord,
    sha256_digest,
    write_records,
    EXPERTS,
    USER_AGENT,
    _infer_market_from_pick,
    _parse_odds,
    _parse_amount,
    ensure_out_dir,
    _probe_page,
    scroll_to_load_all,
    STAT_KEY_MAP,
)

OUT_DIR = os.getenv("NBA_OUT_DIR", "out")
DEFAULT_STORAGE_STATE = str(ROOT / "data" / "bettingpros_storage_state.json")

# Analysis page URL template
ANALYSIS_URL_TEMPLATE = "https://www.bettingpros.com/nba/picks/experts/{slug}/analysis/"


def _get_analysis_url(expert_slug: str) -> str:
    return ANALYSIS_URL_TEMPLATE.format(slug=expert_slug)


def _set_timeframe_1y(page: "Page", debug: bool = False) -> bool:
    """
    Click the 1Y timeframe filter on the analysis page.

    BettingPros analysis pages have a timeframe selector (1W, 1M, 3M, 6M, 1Y, All).
    Try to find and click the '1Y' option.
    """
    # Try common selector patterns
    selectors = [
        'button:has-text("1Y")',
        '[data-testid*="1y"]',
        '[data-testid*="timeframe"]',
        'button:has-text("1 Year")',
        '[class*="timeframe"] button:has-text("1Y")',
        '[class*="filter"] button:has-text("1Y")',
        '[role="tab"]:has-text("1Y")',
    ]
    for sel in selectors:
        try:
            btn = page.query_selector(sel)
            if btn:
                btn.click()
                page.wait_for_timeout(1500)
                if debug:
                    print(f"[timeframe] Clicked 1Y via: {sel}")
                return True
        except Exception:
            continue

    # Try evaluating — look for any button/tab with text "1Y"
    try:
        clicked = page.evaluate("""
            () => {
                const buttons = document.querySelectorAll('button, [role="tab"], [role="option"], li, a');
                for (const b of buttons) {
                    if (b.innerText.trim() === '1Y' || b.innerText.trim() === '1 Year') {
                        b.click();
                        return true;
                    }
                }
                return false;
            }
        """)
        if clicked:
            page.wait_for_timeout(1500)
            if debug:
                print("[timeframe] Clicked 1Y via evaluate")
            return True
    except Exception:
        pass

    if debug:
        print("[timeframe] Could not find 1Y filter — using default timeframe")
    return False


def _expand_all_picks(page: "Page", debug: bool = False) -> int:
    """
    Click any 'Show Picks' / expand buttons to reveal pick details.
    Returns count of buttons clicked.
    """
    clicked = 0
    selectors = [
        'button:has-text("Show Picks")',
        'button:has-text("show picks")',
        'button:has-text("Show picks")',
        '[data-testid*="show-picks"]',
        '[data-testid*="expand"]',
        'button:has-text("Show")',
        '[class*="expand"] button',
    ]
    for sel in selectors:
        try:
            buttons = page.query_selector_all(sel)
            for btn in buttons:
                try:
                    if btn.is_visible():
                        btn.click()
                        clicked += 1
                        page.wait_for_timeout(200)
                except Exception:
                    continue
            if clicked > 0:
                if debug:
                    print(f"[expand] Clicked {clicked} expand buttons via: {sel}")
                break
        except Exception:
            continue
    return clicked


def _extract_historical_picks(page: "Page", expert: Dict[str, Any], debug: bool = False) -> List[Dict[str, Any]]:
    """
    Extract historical picks from the analysis page.

    BettingPros analysis shows a table/list of past bets with:
    - Pick description (team, over/under, player prop)
    - Result (W/L/P)
    - Odds
    - Date

    Returns list of raw pick dicts.
    """
    picks = []

    # Strategy 1: evaluate-based extraction (fast, SPA-friendly)
    try:
        raw_picks = page.evaluate("""
            () => {
                const results = [];

                // Look for table rows
                const rows = document.querySelectorAll('table tbody tr, [role="row"]');
                if (rows.length > 1) {  // >1 to skip header row
                    for (const row of rows) {
                        const cells = row.querySelectorAll('td, [role="cell"]');
                        if (cells.length < 2) continue;
                        const texts = Array.from(cells).map(c => c.innerText.trim());
                        if (!texts[0] || texts[0].length < 2) continue;
                        results.push({
                            pick_text: texts[0] || '',
                            result: texts[1] || '',
                            odds: texts[2] || '',
                            game_date: texts[3] || '',
                            matchup: texts[4] || '',
                            _source: 'table_row',
                        });
                    }
                    if (results.length > 0) return results;
                }

                // Look for card/list items
                const cardSelectors = [
                    '[data-testid*="bet"]',
                    '[data-testid*="pick"]',
                    '[class*="bet-row"]',
                    '[class*="pick-row"]',
                    '[class*="bet-card"]',
                    '[class*="pick-card"]',
                    '[class*="history-row"]',
                    '[class*="HistoryRow"]',
                ];
                for (const sel of cardSelectors) {
                    const cards = document.querySelectorAll(sel);
                    if (cards.length === 0) continue;
                    for (const card of cards) {
                        const text = card.innerText.trim();
                        if (!text || text.length < 3) continue;
                        const lines = text.split('\\n').map(l => l.trim()).filter(Boolean);
                        // Look for result indicator W/L/P in the text
                        const result = lines.find(l => /^[WLP]$|^WIN$|^LOSS$|^PUSH$/i.test(l)) || '';
                        const oddsLine = lines.find(l => /^[+-]\\d{3,4}$/.test(l)) || '';
                        const dateLine = lines.find(l => /\\d{1,2}\\/\\d{1,2}|\d{4}-\d{2}-\d{2}/.test(l)) || '';
                        results.push({
                            pick_text: lines[0] || text,
                            result: result,
                            odds: oddsLine,
                            game_date: dateLine,
                            matchup: '',
                            raw_text: text,
                            _source: `card:${sel}`,
                        });
                    }
                    if (results.length > 0) return results;
                }

                // Text scan fallback — scan body text for pick patterns
                const allLines = document.body.innerText.split('\\n').map(l => l.trim()).filter(Boolean);
                const pickPat = /^(Over|Under)\\s+[\\d.]+|^[A-Z][a-z]+.*(Over|Under)\\s+[\\d.]+|[+-][\\d.]+$/;
                const resultPat = /^[WLP]$|^WIN$|^LOSS$|^PUSH$/i;
                const oddsPat = /^[+-]\\d{3,4}$/;
                let i = 0;
                while (i < allLines.length) {
                    if (pickPat.test(allLines[i])) {
                        const pick = {
                            pick_text: allLines[i],
                            result: '',
                            odds: '',
                            game_date: '',
                            matchup: '',
                            _source: 'text_scan',
                        };
                        // Look ahead for result, odds
                        for (let j = i + 1; j < Math.min(i + 5, allLines.length); j++) {
                            if (resultPat.test(allLines[j])) pick.result = allLines[j];
                            if (oddsPat.test(allLines[j])) pick.odds = allLines[j];
                        }
                        results.push(pick);
                        i += 2;
                        continue;
                    }
                    i++;
                }
                return results;
            }
        """)
        if raw_picks:
            picks = raw_picks
            if debug:
                print(f"[backfill] {expert['expert_name']}: extracted {len(picks)} raw picks via {picks[0].get('_source', '?')}")
    except Exception as e:
        if debug:
            print(f"[backfill] evaluate error: {e}")

    return picks


def _parse_result(result_str: str) -> Optional[str]:
    """Parse result string to WIN/LOSS/PUSH."""
    if not result_str:
        return None
    upper = result_str.upper().strip()
    if upper in ("W", "WIN", "WON"):
        return "WIN"
    if upper in ("L", "LOSS", "LOST"):
        return "LOSS"
    if upper in ("P", "PUSH", "VOID", "CANCELLED"):
        return "PUSH"
    return None


def _parse_date_str(date_str: str) -> Optional[str]:
    """Parse various date string formats to YYYY-MM-DD."""
    if not date_str:
        return None
    date_str = date_str.strip()
    # Try YYYY-MM-DD
    if re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        return date_str
    # Try M/D/YYYY or M/D/YY
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{2,4})$", date_str)
    if m:
        month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if year < 100:
            year += 2000
        return f"{year:04d}-{month:02d}-{day:02d}"
    # Try "Mar 10", "March 10", etc.
    m = re.match(r"^([A-Za-z]+)\s+(\d{1,2})(?:,?\s*(\d{4}))?$", date_str)
    if m:
        MONTH_MAP = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
            "january": 1, "february": 2, "march": 3, "april": 4,
            "june": 6, "july": 7, "august": 8, "september": 9,
            "october": 10, "november": 11, "december": 12,
        }
        month_name = m.group(1).lower()
        month = MONTH_MAP.get(month_name)
        if month:
            day = int(m.group(2))
            year = int(m.group(3)) if m.group(3) else datetime.now().year
            return f"{year:04d}-{month:02d}-{day:02d}"
    return None


def backfill_expert(
    page: "Page",
    expert: Dict[str, Any],
    observed_at: datetime,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    debug: bool = False,
    probe: bool = False,
) -> List[RawPickRecord]:
    """Scrape and return historical picks for one expert."""
    url = _get_analysis_url(expert["expert_slug"])
    source_id = expert["source_id"]
    expert_name = expert["expert_name"]
    expert_slug = expert["expert_slug"]

    if debug:
        print(f"[backfill] {expert_name}: {url}")

    page.goto(url, wait_until="domcontentloaded", timeout=30000)
    page.wait_for_timeout(2000)

    # Wait for content to hydrate
    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass
    page.wait_for_timeout(1000)

    if probe:
        _probe_page(page)
        return []

    # Set 1Y timeframe
    _set_timeframe_1y(page, debug=debug)
    page.wait_for_timeout(1000)

    # Expand any collapsed pick rows
    _expand_all_picks(page, debug=debug)
    page.wait_for_timeout(500)

    # Scroll to load all (infinite scroll)
    scroll_to_load_all(page, max_scrolls=50, debug=debug)
    page.wait_for_timeout(500)

    # Extract picks
    raw_picks = _extract_historical_picks(page, expert, debug=debug)

    records: List[RawPickRecord] = []
    for raw_pick in raw_picks:
        pick_text = (raw_pick.get("pick_text") or "").strip()
        if not pick_text:
            continue

        result_str = raw_pick.get("result", "")
        pregraded_result = _parse_result(result_str)

        game_date_str = _parse_date_str(raw_pick.get("game_date", ""))
        odds_str = raw_pick.get("odds", "")
        matchup = raw_pick.get("matchup", "")

        # Date filter
        if game_date_str and (start_date or end_date):
            try:
                gd = date.fromisoformat(game_date_str)
                if start_date and gd < start_date:
                    continue
                if end_date and gd > end_date:
                    continue
            except (ValueError, TypeError):
                pass

        market_type, selection, line_val, side, player_name, prop_line, stat_key = _infer_market_from_pick(pick_text)
        actual_line = prop_line if market_type == "player_prop" else line_val
        odds_val = _parse_odds(odds_str)

        raw_block = json.dumps(raw_pick, ensure_ascii=False)
        fp_input = f"backfill|{source_id}|{pick_text}|{matchup}|{game_date_str or observed_at.date()}"
        fingerprint = sha256_digest(fp_input)

        record = RawPickRecord(
            source_id=source_id,
            source_surface="bettingpros_backfill",
            sport="NBA",
            market_family="player_prop" if market_type == "player_prop" else "standard",
            observed_at_utc=observed_at.isoformat(),
            canonical_url=url,
            raw_pick_text=pick_text,
            raw_block=raw_block,
            raw_fingerprint=fingerprint,
            matchup_hint=matchup or None,
            expert_name=expert_name,
            expert_slug=expert_slug,
            market_type=market_type if market_type not in ("unknown",) else market_type,
            selection=selection,
            side=side,
            line=actual_line,
            odds=odds_val,
            player_name=player_name,
            stat_key=stat_key,
            game_date=game_date_str,
            pregraded_result=pregraded_result,
        )
        records.append(record)

    if debug:
        print(f"[backfill] {expert_name}: {len(records)} historical picks")
    return records


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill BettingPros expert historical picks (1-year analysis page)")
    ap.add_argument("--storage", default=DEFAULT_STORAGE_STATE, help="Path to Playwright storage state JSON")
    ap.add_argument("--debug", action="store_true", help="Enable verbose debug output")
    ap.add_argument("--dry-run", action="store_true", help="Don't write output files")
    ap.add_argument("--expert", default=None, help="Only backfill this expert slug")
    ap.add_argument("--probe", action="store_true", help="Dump DOM info to identify selectors, then exit")
    ap.add_argument("--start-date", default=None, help="Only include picks on/after YYYY-MM-DD")
    ap.add_argument("--end-date", default=None, help="Only include picks on/before YYYY-MM-DD")
    args = ap.parse_args()

    if sync_playwright is None:
        print("ERROR: playwright not installed. Run: pip install playwright && playwright install chromium")
        sys.exit(1)

    if not Path(args.storage).exists():
        print(f"ERROR: No storage state at {args.storage}")
        print("Run: python3 scripts/create_bettingpros_storage.py")
        sys.exit(1)

    start_date = date.fromisoformat(args.start_date) if args.start_date else None
    end_date = date.fromisoformat(args.end_date) if args.end_date else None

    experts = EXPERTS
    if args.expert:
        experts = [e for e in EXPERTS if e["expert_slug"] == args.expert]
        if not experts:
            print(f"ERROR: No expert with slug '{args.expert}'")
            sys.exit(1)

    observed_at = datetime.now(timezone.utc)
    all_records: List[RawPickRecord] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=USER_AGENT,
            storage_state=args.storage,
            viewport={"width": 1400, "height": 900},
        )

        for expert in experts:
            try:
                page = context.new_page()
                records = backfill_expert(
                    page, expert, observed_at,
                    start_date=start_date,
                    end_date=end_date,
                    debug=args.debug,
                    probe=args.probe,
                )
                all_records.extend(records)
                print(f"  {expert['expert_name']}: {len(records)} historical picks")
                page.close()
            except Exception as e:
                print(f"  {expert['expert_name']}: ERROR — {e}")

        context.close()
        browser.close()

    if args.probe:
        print("[backfill] Probe complete. Update _extract_historical_picks() with correct selectors.")
        return

    # Deduplicate by fingerprint
    seen: set = set()
    deduped: List[RawPickRecord] = []
    for r in all_records:
        if r.raw_fingerprint not in seen:
            seen.add(r.raw_fingerprint)
            deduped.append(r)

    print(f"\n[backfill] Total: {len(deduped)} records (deduped from {len(all_records)})")

    if args.dry_run:
        print("[backfill] Dry run — not writing files")
        return

    ensure_out_dir()

    raw_path = os.path.join(OUT_DIR, "raw_bettingpros_backfill_nba.json")
    write_records(raw_path, deduped)
    print(f"[backfill] Wrote raw → {raw_path}")

    # Normalize
    from src.normalizer_bettingpros_nba import normalize_bettingpros_records
    raw_dicts = [asdict(r) for r in deduped]
    normalized = normalize_bettingpros_records(raw_dicts, debug=args.debug)

    norm_path = os.path.join(OUT_DIR, "normalized_bettingpros_backfill_nba.json")
    write_json(norm_path, normalized)
    eligible = sum(1 for r in normalized if r.get("eligible_for_consensus"))
    print(f"[backfill] Wrote normalized → {norm_path} ({eligible}/{len(normalized)} eligible)")
    print()
    print("Next steps:")
    print("  1. Run consensus:      python3 consensus_nba.py")
    print("  2. Build ledger:       python3 scripts/build_signal_ledger.py")
    print("  3. Grade signals:      python3 scripts/grade_signals_nba.py")


if __name__ == "__main__":
    main()
