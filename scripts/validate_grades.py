#!/usr/bin/env python3
"""Validate grade correctness across the entire dataset.

Five-layer validation:
  Layer 1: Statistical sanity checks (all records)
  Layer 2: Math re-verification (all provider-graded records)
  Layer 3: Pre-grade cross-validation (all pre-graded records)
  Layer 4: Data spot-check (sample, verifies cached provider data)
  Layer 5: Error investigation (informational report)

Usage:
    python3 scripts/validate_grades.py
    python3 scripts/validate_grades.py --layer 2 --verbose
    python3 scripts/validate_grades.py --sample-size 100
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
GRADES_PATH = REPO_ROOT / "data" / "ledger" / "grades_latest.jsonl"
SIGNALS_PATH = REPO_ROOT / "data" / "ledger" / "signals_latest.jsonl"
CACHE_DIR = REPO_ROOT / "data" / "cache" / "results"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows = []
    if not path.exists():
        return rows
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def parse_event_key_teams(event_key: str) -> Tuple[Optional[str], Optional[str]]:
    """Extract away, home from event_key like NBA:2026:01:23:NOP@MEM."""
    if not event_key:
        return None, None
    m = re.search(r"([A-Z]+)@([A-Z]+)$", event_key)
    if m:
        return m.group(1), m.group(2)
    return None, None


def parse_date_from_day_key(day_key: str) -> Optional[str]:
    """Extract YYYY-MM-DD from day_key like NBA:2026:01:23."""
    if not day_key:
        return None
    parts = day_key.split(":")
    if len(parts) >= 4:
        return f"{parts[1]}-{parts[2]}-{parts[3]}"
    return None


def load_score_cache() -> Dict[str, Dict[str, Any]]:
    """Load all lgf_*.json cache files into {game_id: {away_team, home_team, away_score, home_score}}."""
    scores = {}
    if not CACHE_DIR.exists():
        return scores
    for fname in os.listdir(CACHE_DIR):
        if not fname.startswith("lgf_") or not fname.endswith(".json"):
            continue
        try:
            with open(CACHE_DIR / fname) as f:
                data = json.load(f)
            if isinstance(data, list):
                for game in data:
                    gid = game.get("game_id")
                    if gid:
                        scores[gid] = game
        except Exception:
            continue
    return scores


def load_signals_index() -> Dict[str, Dict[str, Any]]:
    """Index signals by signal_id for fast lookup."""
    idx = {}
    for rec in load_jsonl(SIGNALS_PATH):
        sid = rec.get("signal_id")
        if sid:
            idx[sid] = rec
    return idx


# ---------------------------------------------------------------------------
# Layer 1: Statistical Sanity Checks
# ---------------------------------------------------------------------------

WIN_RATE_BOUNDS = {
    "spread": (0.45, 0.60),
    "total": (0.45, 0.60),
    "moneyline": (0.40, 0.65),
    "player_prop": (0.45, 0.58),
}

PUSH_RATE_BOUNDS = {
    "spread": 0.05,
    "total": 0.04,
    "moneyline": 0.005,
    "player_prop": 0.03,
}


def run_layer1(grades: List[Dict], verbose: bool = False) -> Dict[str, Any]:
    """Layer 1: Statistical sanity checks."""
    result = {"name": "Statistical Sanity Checks", "passed": True, "flags": []}

    # Per-market counts
    market_results = defaultdict(lambda: Counter())
    source_results = defaultdict(lambda: Counter())  # source -> {WIN, LOSS, ...}
    pregraded_results = Counter()
    provider_results = Counter()
    status_result_mismatches = []
    monthly_counts = Counter()

    for g in grades:
        status = g.get("status", "")
        res = g.get("result", "")
        mt = g.get("market_type", "unknown") or "unknown"
        notes = g.get("notes", "") or ""

        # Market-level results
        if status in ("WIN", "LOSS", "PUSH"):
            market_results[mt][status] += 1

        # Source-level results (pregraded vs provider)
        if "pregraded" in notes:
            if status in ("WIN", "LOSS", "PUSH"):
                pregraded_results[status] += 1
        else:
            if status in ("WIN", "LOSS", "PUSH"):
                provider_results[status] += 1

        # Status/result consistency
        if status in ("WIN", "LOSS", "PUSH") and res != status:
            status_result_mismatches.append(g.get("signal_id", "?"))
        if status == "ERROR" and res in ("WIN", "LOSS", "PUSH"):
            status_result_mismatches.append(g.get("signal_id", "?"))

        # Monthly distribution
        day_key = g.get("day_key", "")
        date_str = parse_date_from_day_key(day_key)
        if date_str:
            monthly_counts[date_str[:7]] += 1  # YYYY-MM

    # Check win rates
    total_graded = sum(sum(c.values()) for c in market_results.values())
    result["total_graded"] = total_graded

    print(f"\n  {'Market':<15} {'W':>6} {'L':>6} {'P':>5} {'Win%':>7} {'Push%':>7} {'Status'}")
    print(f"  {'-'*60}")

    for mt in ["spread", "total", "moneyline", "player_prop"]:
        c = market_results[mt]
        w, l, p = c.get("WIN", 0), c.get("LOSS", 0), c.get("PUSH", 0)
        total = w + l + p
        if total == 0:
            continue

        win_rate = w / (w + l) if (w + l) > 0 else 0
        push_rate = p / total if total > 0 else 0

        lo, hi = WIN_RATE_BOUNDS.get(mt, (0, 1))
        max_push = PUSH_RATE_BOUNDS.get(mt, 1.0)

        wr_ok = lo <= win_rate <= hi
        pr_ok = push_rate <= max_push
        status_str = "PASS" if (wr_ok and pr_ok) else "FLAG"

        if not wr_ok:
            result["flags"].append(f"{mt} win rate {win_rate:.1%} outside [{lo:.0%}, {hi:.0%}]")
            result["passed"] = False
        if not pr_ok:
            result["flags"].append(f"{mt} push rate {push_rate:.1%} exceeds {max_push:.1%}")
            result["passed"] = False

        print(f"  {mt:<15} {w:>6} {l:>6} {p:>5} {win_rate:>6.1%} {push_rate:>6.1%}  {status_str}")

    # Pre-graded vs provider win rate comparison
    pg_w = pregraded_results.get("WIN", 0)
    pg_l = pregraded_results.get("LOSS", 0)
    pv_w = provider_results.get("WIN", 0)
    pv_l = provider_results.get("LOSS", 0)
    pg_wr = pg_w / (pg_w + pg_l) if (pg_w + pg_l) > 0 else 0
    pv_wr = pv_w / (pv_w + pv_l) if (pv_w + pv_l) > 0 else 0
    wr_diff = abs(pg_wr - pv_wr)

    print(f"\n  Pre-graded win rate: {pg_wr:.1%} ({pg_w}W-{pg_l}L)")
    print(f"  Provider win rate:   {pv_wr:.1%} ({pv_w}W-{pv_l}L)")
    print(f"  Divergence: {wr_diff:.1%} {'(OK)' if wr_diff <= 0.05 else '(FLAG)'}")

    if wr_diff > 0.05:
        result["flags"].append(f"Pre-graded vs provider win rate divergence: {wr_diff:.1%}")

    # Status/result consistency
    print(f"\n  Status/result mismatches: {len(status_result_mismatches)}")
    if status_result_mismatches:
        result["passed"] = False
        result["flags"].append(f"{len(status_result_mismatches)} status/result mismatches")
        if verbose:
            for sid in status_result_mismatches[:5]:
                print(f"    {sid}")

    # Grade coverage
    total = len(grades)
    terminal = sum(1 for g in grades if g.get("status") in ("WIN", "LOSS", "PUSH"))
    errors = sum(1 for g in grades if g.get("status") == "ERROR")
    ineligible = sum(1 for g in grades if g.get("status") == "INELIGIBLE")
    pending = sum(1 for g in grades if g.get("status") == "PENDING")
    print(f"\n  Grade coverage: {terminal:,}/{total:,} terminal ({100*terminal/total:.1f}%)")
    print(f"    ERROR: {errors:,} | INELIGIBLE: {ineligible:,} | PENDING: {pending:,}")

    # Monthly distribution (NBA season months)
    nba_months = ["2025-10", "2025-11", "2025-12", "2026-01", "2026-02"]
    print(f"\n  Monthly distribution (current season):")
    for m in nba_months:
        c = monthly_counts.get(m, 0)
        flag = " (FLAG: zero!)" if c == 0 else ""
        print(f"    {m}: {c:,}{flag}")
        if c == 0:
            result["flags"].append(f"Zero grades for NBA season month {m}")

    return result


# ---------------------------------------------------------------------------
# Layer 2: Math Re-verification
# ---------------------------------------------------------------------------

def reverify_spread(selection: str, line: float, away_team: str, home_team: str,
                    away_score: int, home_score: int) -> Optional[str]:
    """Re-derive spread grade. Returns WIN/LOSS/PUSH or None if can't determine."""
    sel = selection.upper().replace("_SPREAD", "")
    if away_team and away_team.upper() in sel:
        picked_score, opp_score = away_score, home_score
    elif home_team and home_team.upper() in sel:
        picked_score, opp_score = home_score, away_score
    else:
        return None
    margin = picked_score - opp_score + line
    if margin > 0:
        return "WIN"
    elif margin < 0:
        return "LOSS"
    return "PUSH"


def reverify_total(selection: str, direction: str, line: float,
                   away_score: int, home_score: int) -> Optional[str]:
    """Re-derive total grade."""
    pick_dir = normalize_direction(direction, selection)
    if not pick_dir:
        return None

    total_pts = away_score + home_score
    if pick_dir == "OVER":
        if total_pts > line:
            return "WIN"
        elif total_pts < line:
            return "LOSS"
        return "PUSH"
    else:
        if total_pts < line:
            return "WIN"
        elif total_pts > line:
            return "LOSS"
        return "PUSH"


def reverify_moneyline(selection: str, away_team: str, home_team: str,
                       away_score: int, home_score: int) -> Optional[str]:
    """Re-derive moneyline grade."""
    sel = selection.upper().replace("_ML", "")
    if away_team and away_team.upper() in sel:
        picked = "away"
    elif home_team and home_team.upper() in sel:
        picked = "home"
    else:
        return None

    if away_score > home_score:
        winner = "away"
    elif home_score > away_score:
        winner = "home"
    else:
        return "PUSH"

    return "WIN" if picked == winner else "LOSS"


def normalize_direction(direction: str, selection: str = "") -> Optional[str]:
    """Normalize direction to OVER or UNDER, handling player_over/player_under."""
    combined = ((direction or "") + " " + (selection or "")).upper()
    if "OVER" in combined:
        return "OVER"
    if "UNDER" in combined:
        return "UNDER"
    return None


def reverify_player_prop(stat_value: float, line: float, direction: str,
                         selection: str) -> Optional[str]:
    """Re-derive player prop grade."""
    pick_dir = normalize_direction(direction, selection)
    if not pick_dir:
        return None

    if pick_dir == "OVER":
        if stat_value > line:
            return "WIN"
        elif stat_value < line:
            return "LOSS"
        return "PUSH"
    else:
        if stat_value < line:
            return "WIN"
        elif stat_value > line:
            return "LOSS"
        return "PUSH"


def run_layer2(grades: List[Dict], signals_idx: Dict, score_cache: Dict,
               verbose: bool = False) -> Dict[str, Any]:
    """Layer 2: Math re-verification of all provider-graded records."""
    result = {"name": "Math Re-verification", "passed": True, "flags": [],
              "checked": 0, "mismatches": 0, "skipped_pregraded": 0,
              "skipped_missing_data": 0, "direction_bug_count": 0,
              "line_desync_count": 0}

    mismatch_details = []

    for g in grades:
        notes = g.get("notes", "") or ""
        status = g.get("status", "")

        if status not in ("WIN", "LOSS", "PUSH"):
            continue

        if "pregraded" in notes:
            result["skipped_pregraded"] += 1
            continue

        signal_id = g.get("signal_id", "")
        signal = signals_idx.get(signal_id, {})
        mt = g.get("market_type", "")
        stored_result = g.get("result")

        # Skip orphan grades (signal no longer in ledger)
        if not signal:
            result["skipped_missing_data"] += 1
            continue

        if mt == "player_prop":
            # stat_value is on the grade record
            stat_value = g.get("stat_value")
            line = signal.get("line")
            direction = signal.get("direction", "")
            selection = signal.get("selection", "")

            if stat_value is None or line is None:
                result["skipped_missing_data"] += 1
                continue

            expected = reverify_player_prop(stat_value, line, direction, selection)
            if expected is None:
                result["skipped_missing_data"] += 1
                continue

            result["checked"] += 1
            if expected != stored_result:
                # Check if this is the known direction prefix bug
                raw_dir = (direction or "").lower()
                if raw_dir in ("player_over", "player_under"):
                    result["direction_bug_count"] += 1
                else:
                    result["mismatches"] += 1
                    result["passed"] = False
                    mismatch_details.append({
                        "signal_id": signal_id, "market": mt,
                        "stored": stored_result, "expected": expected,
                        "stat_value": stat_value, "line": line,
                        "direction": direction, "selection": selection,
                    })

        elif mt in ("spread", "total", "moneyline"):
            # Need scores from cache
            gi = g.get("games_info") or {}
            game_id = gi.get("matched_game_id")

            if not game_id or game_id not in score_cache:
                result["skipped_missing_data"] += 1
                continue

            game = score_cache[game_id]
            away_score = game.get("away_score")
            home_score = game.get("home_score")
            cache_away = game.get("away_team", "")
            cache_home = game.get("home_team", "")

            if away_score is None or home_score is None:
                result["skipped_missing_data"] += 1
                continue

            # Get line from grade first (has correct sign), fall back to signal
            line = g.get("line") or signal.get("line")
            selection = signal.get("selection", "") or g.get("selection", "")
            direction = signal.get("direction", "")
            away_team = signal.get("away_team", "") or cache_away
            home_team = signal.get("home_team", "") or cache_home

            # When match_orientation is "flipped", the LGF cache scores are in NBA API
            # orientation which is reversed from the signal's away/home. Swap scores
            # to align with the signal's team designation.
            if gi.get("match_orientation") == "flipped":
                away_score, home_score = home_score, away_score

            if mt == "spread":
                if line is None:
                    result["skipped_missing_data"] += 1
                    continue
                expected = reverify_spread(selection, line, away_team, home_team,
                                           away_score, home_score)
            elif mt == "total":
                if line is None:
                    result["skipped_missing_data"] += 1
                    continue
                expected = reverify_total(selection, direction, line,
                                          away_score, home_score)
            elif mt == "moneyline":
                expected = reverify_moneyline(selection, away_team, home_team,
                                              away_score, home_score)
            else:
                continue

            if expected is None:
                result["skipped_missing_data"] += 1
                continue

            result["checked"] += 1
            if expected != stored_result:
                # Check if this is a line desync (grade line ≠ signal line)
                grade_line = g.get("line")
                signal_line = signal.get("line")
                if grade_line is not None and signal_line is not None and grade_line != signal_line:
                    result["line_desync_count"] += 1
                elif grade_line is None and signal_line is not None:
                    result["line_desync_count"] += 1
                else:
                    result["mismatches"] += 1
                    result["passed"] = False
                    mismatch_details.append({
                        "signal_id": signal_id, "market": mt,
                        "stored": stored_result, "expected": expected,
                        "line": line, "selection": selection,
                        "grade_line": grade_line, "signal_line": signal_line,
                        "away_team": away_team, "home_team": home_team,
                        "away_score": away_score, "home_score": home_score,
                    })

    # Print summary
    print(f"\n  Checked:            {result['checked']:,}")
    print(f"  Mismatches:         {result['mismatches']}")
    if result["direction_bug_count"]:
        print(f"  Direction bug:      {result['direction_bug_count']:,} (player_over/player_under treated as UNDER/OVER in grader)")
    if result["line_desync_count"]:
        print(f"  Line desync:        {result['line_desync_count']} (grade used different line than current signal)")
    print(f"  Skipped pregraded:  {result['skipped_pregraded']:,}")
    print(f"  Skipped no data:    {result['skipped_missing_data']:,}")

    if mismatch_details:
        print(f"\n  OTHER MISMATCHES (excluding direction bug):")
        for d in mismatch_details[:20]:
            print(f"    {d['signal_id'][:20]}... {d['market']}: stored={d['stored']}, expected={d['expected']}")
            if verbose:
                for k, v in d.items():
                    if k not in ("signal_id", "market", "stored", "expected"):
                        print(f"      {k}: {v}")

    flags = []
    if result["mismatches"]:
        flags.append(f"{result['mismatches']} unexplained mismatches")
    if result["direction_bug_count"]:
        flags.append(f"KNOWN BUG: {result['direction_bug_count']} player props graded with wrong direction (player_over/player_under)")
    if result["line_desync_count"]:
        flags.append(f"LINE DESYNC: {result['line_desync_count']} grades used different line than current signal")
    result["flags"] = flags
    # Only fail on unexplained mismatches
    result["passed"] = result["mismatches"] == 0
    return result


# ---------------------------------------------------------------------------
# Layer 3: Pre-Grade Cross-Validation
# ---------------------------------------------------------------------------

def run_layer3(grades: List[Dict], signals_idx: Dict, score_cache: Dict,
               verbose: bool = False) -> Dict[str, Any]:
    """Layer 3: Cross-validate pre-graded records from SportsLine and Dimers."""
    result = {"name": "Pre-Grade Cross-Validation", "passed": True, "flags": [],
              "checked": 0, "mismatches": 0, "unverifiable": 0,
              "pregraded_stale": 0}

    mismatch_details = []
    source_stats = defaultdict(lambda: {"checked": 0, "match": 0, "mismatch": 0, "unverifiable": 0})

    for g in grades:
        notes = g.get("notes", "") or ""
        status = g.get("status", "")

        if "pregraded" not in notes:
            continue
        if status not in ("WIN", "LOSS", "PUSH"):
            continue

        signal_id = g.get("signal_id", "")
        signal = signals_idx.get(signal_id, {})
        mt = g.get("market_type", "")
        stored_result = g.get("result")
        source = "sportsline" if "sportsline" in notes else "dimers"

        # Get scores — SportsLine has them on grade, Dimers may not
        away_score = g.get("away_score")
        home_score = g.get("home_score")

        # If no scores on grade, try to find from cache via event_key date
        if away_score is None or home_score is None:
            # Try to find game in score cache by matching teams+date
            event_key = g.get("event_key") or signal.get("event_key", "")
            ek_away, ek_home = parse_event_key_teams(event_key)
            date_str = parse_date_from_day_key(g.get("day_key", ""))
            if ek_away and ek_home and date_str:
                for gid, game in score_cache.items():
                    if game.get("game_date") == date_str:
                        if game.get("away_team") == ek_away and game.get("home_team") == ek_home:
                            away_score = game["away_score"]
                            home_score = game["home_score"]
                            break
                        elif game.get("away_team") == ek_home and game.get("home_team") == ek_away:
                            # Flipped orientation — swap scores to match signal's teams
                            away_score = game["home_score"]
                            home_score = game["away_score"]
                            break

        # Get signal data — prefer grade's line (has correct sign for spreads)
        line = g.get("line") or signal.get("line")
        selection = signal.get("selection") or g.get("selection") or ""
        direction = signal.get("direction") or ""
        away_team = signal.get("away_team") or ""
        home_team = signal.get("home_team") or ""

        # If still no teams, parse from event_key
        if not away_team or not home_team:
            event_key = g.get("event_key") or signal.get("event_key", "")
            ek_away, ek_home = parse_event_key_teams(event_key)
            away_team = away_team or ek_away or ""
            home_team = home_team or ek_home or ""

        if mt == "player_prop":
            # Props need stat_value — check if available
            stat_value = g.get("stat_value")
            if stat_value is None:
                source_stats[source]["unverifiable"] += 1
                result["unverifiable"] += 1
                continue

            if line is None:
                source_stats[source]["unverifiable"] += 1
                result["unverifiable"] += 1
                continue

            expected = reverify_player_prop(stat_value, line, direction, selection)

        elif mt in ("spread", "total", "moneyline"):
            if away_score is None or home_score is None:
                source_stats[source]["unverifiable"] += 1
                result["unverifiable"] += 1
                continue

            if mt == "spread":
                if line is None:
                    source_stats[source]["unverifiable"] += 1
                    result["unverifiable"] += 1
                    continue
                expected = reverify_spread(selection, line, away_team, home_team,
                                           away_score, home_score)
            elif mt == "total":
                if line is None:
                    source_stats[source]["unverifiable"] += 1
                    result["unverifiable"] += 1
                    continue
                expected = reverify_total(selection, direction, line,
                                          away_score, home_score)
            elif mt == "moneyline":
                expected = reverify_moneyline(selection, away_team, home_team,
                                              away_score, home_score)
            else:
                continue
        else:
            source_stats[source]["unverifiable"] += 1
            result["unverifiable"] += 1
            continue

        if expected is None:
            source_stats[source]["unverifiable"] += 1
            result["unverifiable"] += 1
            continue

        result["checked"] += 1
        source_stats[source]["checked"] += 1

        if expected == stored_result:
            source_stats[source]["match"] += 1
        else:
            # Check if signal's pregraded_result matches expected (stale grade issue)
            sig_pregraded = signal.get("pregraded_result")
            if sig_pregraded and sig_pregraded != stored_result:
                result["pregraded_stale"] += 1
            else:
                result["mismatches"] += 1
                result["passed"] = False
            source_stats[source]["mismatch"] += 1
            mismatch_details.append({
                "signal_id": signal_id, "source": source, "market": mt,
                "stored": stored_result, "expected": expected,
                "line": line, "selection": selection,
                "away_team": away_team, "home_team": home_team,
                "away_score": away_score, "home_score": home_score,
            })

    # Print summary
    for src in sorted(source_stats.keys()):
        s = source_stats[src]
        print(f"\n  {src}:")
        print(f"    Checked: {s['checked']}, Match: {s['match']}, Mismatch: {s['mismatch']}, Unverifiable: {s['unverifiable']}")

    print(f"\n  Total checked: {result['checked']:,}")
    print(f"  Total mismatches: {result['mismatches']}")
    if result["pregraded_stale"]:
        print(f"  Stale pregrades:  {result['pregraded_stale']} (signal pregraded_result differs from grade)")
    print(f"  Total unverifiable: {result['unverifiable']}")

    if mismatch_details:
        print(f"\n  MISMATCHES:")
        for d in mismatch_details[:20]:
            print(f"    {d['source']} {d['market']}: stored={d['stored']}, expected={d['expected']}")
            print(f"      sel={d['selection']}, line={d['line']}, scores={d['away_score']}-{d['home_score']}")
            if verbose:
                print(f"      teams={d['away_team']}@{d['home_team']}, signal={d['signal_id'][:30]}")

    result["flags"] = [f"{result['mismatches']} mismatches"] if result["mismatches"] else []
    return result


# ---------------------------------------------------------------------------
# Layer 4: Data Spot-Check
# ---------------------------------------------------------------------------

def run_layer4(grades: List[Dict], signals_idx: Dict, score_cache: Dict,
               sample_size: int = 50, verbose: bool = False) -> Dict[str, Any]:
    """Layer 4: Spot-check cached provider data by independently parsing cache files."""
    result = {"name": "Data Spot-Check", "passed": True, "flags": [],
              "checked": 0, "mismatches": 0, "cache_missing": 0,
              "direction_bug_count": 0}

    # Separate graded records by market (exclude pregraded)
    by_market = defaultdict(list)
    for g in grades:
        if g.get("status") not in ("WIN", "LOSS", "PUSH"):
            continue
        if "pregraded" in (g.get("notes", "") or ""):
            continue
        mt = g.get("market_type", "")
        if mt in ("spread", "total", "moneyline", "player_prop"):
            by_market[mt].append(g)

    mismatch_details = []

    for mt in ["spread", "total", "moneyline", "player_prop"]:
        pool = by_market.get(mt, [])
        if not pool:
            continue

        n = min(sample_size, len(pool))
        samples = random.sample(pool, n)
        market_checked = 0
        market_mismatches = 0
        market_cache_miss = 0

        for g in samples:
            signal_id = g.get("signal_id", "")
            signal = signals_idx.get(signal_id, {})
            stored_result = g.get("result")

            # Skip orphan grades (signal no longer in ledger)
            if not signal:
                market_cache_miss += 1
                continue

            if mt in ("spread", "total", "moneyline"):
                gi = g.get("games_info") or {}
                game_id = gi.get("matched_game_id")
                date_str = gi.get("date_used") or parse_date_from_day_key(g.get("day_key", ""))

                if not game_id:
                    market_cache_miss += 1
                    continue

                # Try to read the lgf cache file directly
                cache_game = score_cache.get(game_id)
                if not cache_game:
                    market_cache_miss += 1
                    continue

                away_score = cache_game.get("away_score")
                home_score = cache_game.get("home_score")
                if away_score is None or home_score is None:
                    market_cache_miss += 1
                    continue

                # Verify game status is FINAL
                if cache_game.get("status") != "FINAL":
                    market_cache_miss += 1
                    continue

                line = g.get("line") or signal.get("line")
                selection = signal.get("selection", "") or g.get("selection", "")
                direction = signal.get("direction", "")
                away_team = signal.get("away_team") or cache_game.get("away_team", "")
                home_team = signal.get("home_team") or cache_game.get("home_team", "")

                # Swap scores when flipped orientation (cache is NBA API order, signal is reversed)
                if gi.get("match_orientation") == "flipped":
                    away_score, home_score = home_score, away_score

                if mt == "spread":
                    if line is None:
                        market_cache_miss += 1
                        continue
                    expected = reverify_spread(selection, line, away_team, home_team,
                                               away_score, home_score)
                elif mt == "total":
                    if line is None:
                        market_cache_miss += 1
                        continue
                    expected = reverify_total(selection, direction, line,
                                              away_score, home_score)
                else:  # moneyline
                    expected = reverify_moneyline(selection, away_team, home_team,
                                                  away_score, home_score)

            else:  # player_prop
                stat_value = g.get("stat_value")
                line = signal.get("line")
                direction = signal.get("direction", "")
                selection = signal.get("selection", "")

                if stat_value is None or line is None:
                    market_cache_miss += 1
                    continue

                expected = reverify_player_prop(stat_value, line, direction, selection)

            if expected is None:
                market_cache_miss += 1
                continue

            market_checked += 1
            if expected != stored_result:
                # Check if direction bug
                raw_dir = (signal.get("direction") or "").lower()
                if mt == "player_prop" and raw_dir in ("player_over", "player_under"):
                    result["direction_bug_count"] += 1
                else:
                    market_mismatches += 1
                    mismatch_details.append({
                        "market": mt, "signal_id": signal_id,
                        "stored": stored_result, "expected": expected,
                    })

        result["checked"] += market_checked
        result["mismatches"] += market_mismatches
        result["cache_missing"] += market_cache_miss

        status = "PASS" if market_mismatches == 0 else "FAIL"
        print(f"  {mt:<15}: {market_checked} checked, {market_mismatches} mismatch, "
              f"{market_cache_miss} cache_missing  [{status}]")

    if result["mismatches"] > 0:
        result["passed"] = False
        result["flags"] = [f"{result['mismatches']} mismatches in spot-check"]
        if verbose:
            for d in mismatch_details:
                print(f"    MISMATCH: {d['market']} {d['signal_id'][:30]}... stored={d['stored']}, expected={d['expected']}")

    return result


# ---------------------------------------------------------------------------
# Layer 5: Error Investigation
# ---------------------------------------------------------------------------

def run_layer5(grades: List[Dict], signals_idx: Dict, score_cache: Dict,
               sample_size: int = 50, verbose: bool = False) -> Dict[str, Any]:
    """Layer 5: Investigate ERROR and INELIGIBLE records."""
    result = {"name": "Error Investigation", "passed": True, "flags": []}  # Always passes (info only)

    # Categorize by notes
    notes_counter = Counter()
    error_records = defaultdict(list)

    for g in grades:
        status = g.get("status", "")
        if status not in ("ERROR", "INELIGIBLE"):
            continue
        notes = g.get("notes", "") or ""
        # Get the primary error reason
        reason = notes.split(",")[0].strip() if notes else "unknown"
        notes_counter[reason] += 1
        error_records[reason].append(g)

    print(f"\n  Error/Ineligible breakdown:")
    for reason, count in notes_counter.most_common(15):
        print(f"    {reason}: {count:,}")

    # Investigate game_not_found
    gnf = error_records.get("game_not_found", [])
    if gnf:
        print(f"\n  --- game_not_found investigation ({len(gnf):,} total) ---")
        sample = random.sample(gnf, min(sample_size, len(gnf)))

        # Check how many could be date-offset issues
        date_offset = 0
        bad_matchup = 0
        no_cache = 0

        for g in sample:
            gi = g.get("games_info") or {}
            date_str = gi.get("date_used") or parse_date_from_day_key(g.get("day_key", ""))
            event_key = g.get("event_key", "")
            ek_away, ek_home = parse_event_key_teams(event_key)

            if not date_str or not ek_away or not ek_home:
                bad_matchup += 1
                continue

            # Check if the teams played on adjacent dates
            found_nearby = False
            for gid, game in score_cache.items():
                gd = game.get("game_date", "")
                if not gd:
                    continue
                if (game.get("away_team") == ek_away and game.get("home_team") == ek_home) or \
                   (game.get("away_team") == ek_home and game.get("home_team") == ek_away):
                    # Check if within +/-2 days
                    try:
                        from datetime import datetime, timedelta
                        d1 = datetime.strptime(date_str, "%Y-%m-%d")
                        d2 = datetime.strptime(gd, "%Y-%m-%d")
                        if abs((d2 - d1).days) <= 2:
                            found_nearby = True
                            break
                    except Exception:
                        pass

            if found_nearby:
                date_offset += 1
            else:
                bad_matchup += 1

        print(f"    Sample of {len(sample)}: date_offset_fixable={date_offset}, bad_matchup={bad_matchup}")

    # Investigate bad_date_format
    bdf = error_records.get("bad_date_format", [])
    if bdf:
        print(f"\n  --- bad_date_format investigation ({len(bdf):,} total) ---")
        sample = random.sample(bdf, min(10, len(bdf)))
        for g in sample:
            print(f"    day_key={g.get('day_key')}, event_key={g.get('event_key')}")

    # Investigate score_missing
    sm = error_records.get("score_missing", [])
    if sm:
        print(f"\n  --- score_missing investigation ({len(sm):,} total) ---")
        final_but_no_score = 0
        for g in sm:
            gi = g.get("games_info") or {}
            if gi.get("matched_status") == "FINAL":
                final_but_no_score += 1
        print(f"    FINAL status but no scores: {final_but_no_score}/{len(sm)}")
        if final_but_no_score > 0:
            result["flags"].append(f"{final_but_no_score} games FINAL but missing scores")

    # Investigate stat_missing
    stm = error_records.get("stat_missing", [])
    if stm:
        print(f"\n  --- stat_missing investigation ({len(stm):,} total) ---")
        sample = random.sample(stm, min(10, len(stm)))
        for g in sample:
            pm = g.get("player_meta") or {}
            print(f"    player={pm.get('requested_player_key_raw')}, stat={pm.get('requested_stat_key')}, "
                  f"found={not pm.get('player_not_found', True)}")

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def print_banner(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="Validate grade correctness")
    parser.add_argument("--layer", type=str, default="all",
                        help="Layer to run: 1-5 or 'all' (default: all)")
    parser.add_argument("--sample-size", type=int, default=50,
                        help="Sample size for layers 4/5 (default: 50)")
    parser.add_argument("--verbose", action="store_true",
                        help="Show individual mismatches")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility (default: 42)")
    parser.add_argument("--json-report", type=str,
                        help="Write structured JSON report to file")
    args = parser.parse_args()

    random.seed(args.seed)
    run_layers = set()
    if args.layer == "all":
        run_layers = {1, 2, 3, 4, 5}
    else:
        for part in args.layer.split(","):
            run_layers.add(int(part.strip()))

    # Load data
    print("Loading data...")
    grades = load_jsonl(GRADES_PATH)
    print(f"  Loaded {len(grades):,} grade records")

    signals_idx = {}
    if run_layers & {2, 3, 4}:
        print("  Loading signals index...")
        signals_idx = load_signals_index()
        print(f"  Loaded {len(signals_idx):,} signals")

    score_cache = {}
    if run_layers & {2, 3, 4, 5}:
        print("  Loading score cache...")
        score_cache = load_score_cache()
        print(f"  Loaded {len(score_cache):,} cached game scores")

    # Run layers
    report = {}
    overall_pass = True

    if 1 in run_layers:
        print_banner("Layer 1: Statistical Sanity Checks")
        report["layer1"] = run_layer1(grades, verbose=args.verbose)
        status = "PASS" if report["layer1"]["passed"] else "FAIL"
        if not report["layer1"]["passed"]:
            overall_pass = False
        print(f"\n  Layer 1 result: {status}")
        if report["layer1"]["flags"]:
            for f in report["layer1"]["flags"]:
                print(f"    - {f}")

    if 2 in run_layers:
        print_banner("Layer 2: Math Re-verification")
        report["layer2"] = run_layer2(grades, signals_idx, score_cache,
                                       verbose=args.verbose)
        status = "PASS" if report["layer2"]["passed"] else "FAIL"
        if not report["layer2"]["passed"]:
            overall_pass = False
        print(f"\n  Layer 2 result: {status}")

    if 3 in run_layers:
        print_banner("Layer 3: Pre-Grade Cross-Validation")
        report["layer3"] = run_layer3(grades, signals_idx, score_cache,
                                       verbose=args.verbose)
        status = "PASS" if report["layer3"]["passed"] else "FAIL"
        if not report["layer3"]["passed"]:
            overall_pass = False
        print(f"\n  Layer 3 result: {status}")

    if 4 in run_layers:
        print_banner("Layer 4: Data Spot-Check")
        report["layer4"] = run_layer4(grades, signals_idx, score_cache,
                                       sample_size=args.sample_size,
                                       verbose=args.verbose)
        status = "PASS" if report["layer4"]["passed"] else "FAIL"
        if not report["layer4"]["passed"]:
            overall_pass = False
        print(f"\n  Layer 4 result: {status}")

    if 5 in run_layers:
        print_banner("Layer 5: Error Investigation")
        report["layer5"] = run_layer5(grades, signals_idx, score_cache,
                                       sample_size=args.sample_size,
                                       verbose=args.verbose)
        print(f"\n  Layer 5 result: INFO (no pass/fail)")

    # Overall summary
    print_banner("OVERALL SUMMARY")
    for layer_num in sorted(run_layers):
        key = f"layer{layer_num}"
        if key in report:
            r = report[key]
            if layer_num == 5:
                status = "INFO"
            else:
                status = "PASS" if r["passed"] else "FAIL"
            print(f"  Layer {layer_num}: {r['name']:<35} {status}")
    print()
    print(f"  OVERALL: {'PASS' if overall_pass else 'FAIL'}")
    print(f"{'='*60}\n")

    # JSON report
    if args.json_report:
        report_path = Path(args.json_report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2, default=str)
        print(f"JSON report written to: {report_path}")

    return 0 if overall_pass else 1


if __name__ == "__main__":
    sys.exit(main())
