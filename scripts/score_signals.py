#!/usr/bin/env python3
"""Score today's signals using Wilson-based combo×market ranking.

Primary signal = Wilson lower bound from the best matching source combo × market
historical record. Picks are ranked by this score and assigned tiers A/B/C/D.
Recent trends apply a small additive modifier.

Usage:
    python3 scripts/score_signals.py
    python3 scripts/score_signals.py --date 2026-02-20
    python3 scripts/score_signals.py --date 2026-02-20 --verbose
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# ── Paths ──────────────────────────────────────────────────────────────────

REPORTS_DIR = Path("data/reports")
SIGNALS_PATH = Path("data/ledger/signals_latest.jsonl")
PLAYS_DIR = Path("data/plays")
PATTERN_CANDIDATES_PATH = REPORTS_DIR / "pattern_registry_active.json"

CURRENT_LINES_PATH = Path("data/cache/current_lines.json")
CACHE_DIR = Path("data/cache/results")

def get_sport_paths(sport: str) -> Tuple[Path, Path, Path]:
    """Return (reports_dir, signals_path, plays_dir) for the given sport."""
    if sport == "NCAAB":
        return (
            Path("data/reports/ncaab"),
            Path("data/ledger/ncaab/signals_latest.jsonl"),
            Path("data/plays/ncaab"),
        )
    return (
        Path("data/reports"),
        Path("data/ledger/signals_latest.jsonl"),
        Path("data/plays"),
    )


DYNAMIC_PATTERNS_ENV = "ENABLE_DYNAMIC_PATTERNS"


def _dynamic_patterns_enabled() -> bool:
    """Dynamic pattern registry is OFF by default (Q6).

    Must be explicitly enabled by setting ENABLE_DYNAMIC_PATTERNS to a
    truthy value (1/true/yes/on, case-insensitive). When disabled, only
    the hardcoded PATTERN_REGISTRY / PATTERN_REGISTRY_NCAAB is used.
    """
    raw = os.environ.get(DYNAMIC_PATTERNS_ENV, "")
    return raw.strip().lower() in {"1", "true", "yes", "on"}


# Core shape check for a dynamic pattern row. Unknown keys are allowed
# (forward-compat); only the critical shape is enforced.
_PATTERN_ROW_REQUIRED_KEYS = {"pattern_type", "tier_eligible"}
_PATTERN_TIER_VALUES = {"A", "B"}


def validate_dynamic_pattern_registry(data: Any) -> Tuple[bool, List[str], List[Dict[str, Any]]]:
    """Validate a loaded pattern_registry_active.json payload.

    Returns (valid, errors, rows). On any error valid is False, rows is []
    — the caller must fall back to the hardcoded registry.
    """
    errors: List[str] = []
    if not isinstance(data, dict):
        return False, ["top-level JSON is not an object"], []
    if "rows" not in data:
        return False, ["missing 'rows' key"], []
    rows = data.get("rows")
    if not isinstance(rows, list):
        return False, ["'rows' is not a list"], []

    clean_rows: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            errors.append(f"row[{idx}] is not an object")
            continue
        missing = _PATTERN_ROW_REQUIRED_KEYS - set(row.keys())
        if missing:
            errors.append(f"row[{idx}] missing required keys: {sorted(missing)}")
            continue
        tier = row.get("tier_eligible")
        if tier not in _PATTERN_TIER_VALUES:
            errors.append(f"row[{idx}] invalid tier_eligible={tier!r}")
            continue
        hist = row.get("hist")
        if hist is not None and not isinstance(hist, dict):
            errors.append(f"row[{idx}] 'hist' is not an object")
            continue
        if isinstance(hist, dict):
            n = hist.get("n")
            if n is not None and not isinstance(n, (int, float)):
                errors.append(f"row[{idx}] 'hist.n' must be numeric, got {type(n).__name__}")
                continue
        clean_rows.append(row)

    valid = len(errors) == 0
    return valid, errors, clean_rows


def load_dynamic_pattern_registry(sport: str, reports_dir: Path) -> List[Dict[str, Any]]:
    """Load dynamic NBA pattern candidates, gated by ENABLE_DYNAMIC_PATTERNS (Q6).

    Default (env unset or falsy): disabled. Returns [] with a log line.
    Enabled (env truthy): load + validate + return rows; fail-closed on
    any validation/parse error (hardcoded registry stays authoritative).
    """
    if sport != "NBA":
        return []
    if not _dynamic_patterns_enabled():
        print(
            f"  [patterns] Dynamic registry disabled ({DYNAMIC_PATTERNS_ENV} unset or falsy). "
            f"Using hardcoded PATTERN_REGISTRY only."
        )
        return []
    path = reports_dir / PATTERN_CANDIDATES_PATH.name
    if not path.exists():
        print(f"  [patterns] Dynamic registry enabled but file not found: {path}")
        return []
    try:
        data = json.loads(path.read_text())
    except Exception as exc:
        print(f"  [patterns] Dynamic registry {path} failed to parse: {exc}. Using hardcoded only.")
        return []
    valid, errors, rows = validate_dynamic_pattern_registry(data)
    if not valid:
        print(f"  [patterns] Dynamic registry {path} failed schema validation; using hardcoded only.")
        for err in errors[:5]:
            print(f"    - {err}")
        if len(errors) > 5:
            print(f"    ... (+{len(errors) - 5} more errors)")
        return []
    return rows


def _pattern_specificity(pattern: Dict[str, Any]) -> Tuple[int, int]:
    keys = ("experts_all", "expert", "exact_combo", "source_pair", "source_contains", "market_type", "stat_type", "direction", "team")
    score = sum(1 for key in keys if pattern.get(key))
    if pattern.get("experts_all"):
        score += len(pattern.get("experts_all") or [])
    return score, int(pattern.get("hist", {}).get("n", 0) or 0)


def merge_pattern_registries(base: List[Dict[str, Any]], dynamic: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not dynamic:
        return base
    merged = list(dynamic) + list(base)
    merged.sort(key=_pattern_specificity, reverse=True)
    return merged

EXCLUDED_SIGNAL_TYPES = {"avoid_conflict"}

# ── Wilson-based scoring constants ────────────────────────────────────────

MIN_SAMPLE_PRIMARY = 15   # min n to accept a primary combo lookup
MIN_SAMPLE_TREND = 5      # min decided bets for recent trend modifier

WILSON_TIER_B = 0.46      # positive edge
WILSON_TIER_C = 0.45      # marginal — not exported
WILSON_HARD_EXCLUDE = 0.42  # hard-exclude if n >= 30 and Wilson below this

# Confidence score (0-100): maps wilson_score to a human-readable scale
# Floor = WILSON_TIER_B (0.46) → 0,  Ceiling = 0.65 → 100
CONFIDENCE_WILSON_FLOOR = 0.46
CONFIDENCE_WILSON_CEIL  = 0.65

def wilson_to_confidence(wilson: float) -> int:
    """Map wilson_score (0.46–0.65) to a 0–100 confidence score."""
    clamped = max(CONFIDENCE_WILSON_FLOOR, min(CONFIDENCE_WILSON_CEIL, wilson))
    return round((clamped - CONFIDENCE_WILSON_FLOOR) / (CONFIDENCE_WILSON_CEIL - CONFIDENCE_WILSON_FLOOR) * 100)

# A-tier: pattern registry (replaces old combo×market gate)
# Old thresholds removed — A-tier now driven by PATTERN_REGISTRY below

# Recency modifiers (additive)
TREND_BOOST_HOT = 0.03    # recent wp >= 60%
TREND_BOOST_WARM = 0.02   # recent wp >= 55%
TREND_PENALTY_COOL = -0.01  # recent wp <= 45%
TREND_PENALTY_COLD = -0.02  # recent wp <= 40%

# Multi-factor modifiers (additive)
EXPERT_BOOST = 0.015       # best expert >55% wp (n>=30)
EXPERT_PENALTY = -0.015    # worst expert <45% wp (n>=30)
EXPERT_MIN_N = 30
STAT_TYPE_BOOST = 0.01     # stat wp >52%
STAT_TYPE_PENALTY = -0.01  # stat wp <47%
LINE_BUCKET_BOOST = 0.01   # bucket wp >51%
LINE_BUCKET_PENALTY = -0.01  # bucket wp <48%

# ── Quality filters ──────────────────────────────────────────────────────

EXCLUDED_STAT_TYPES = {"steals", "unknown", "pts_reb"}

# Direction-aware stat exclusions: exclude (stat, direction) combos that are bad,
# while allowing the other direction through (e.g. pts_reb_ast UNDER is 57.9%).
EXCLUDED_STAT_DIR: Set[Tuple[str, str]] = {
    # ("reb_ast", "OVER") removed — Mike Barner reb_ast OVER is 37-21 (63.8%), A-tier pattern
    ("pts_reb_ast", "OVER"),   # 203-247 (45.1%) — clearly negative edge
}
MIN_DAILY_PICKS = 3

# ── Player prop selection validation ─────────────────────────────────────────
TEAM_SLUGS: Set[str] = {
    "atlanta_hawks", "boston_celtics", "brooklyn_nets", "charlotte_hornets",
    "chicago_bulls", "cleveland_cavaliers", "dallas_mavericks", "denver_nuggets",
    "detroit_pistons", "golden_state_warriors", "houston_rockets", "indiana_pacers",
    "la_clippers", "los_angeles_clippers", "los_angeles_lakers", "memphis_grizzlies",
    "miami_heat", "milwaukee_bucks", "minnesota_timberwolves", "new_orleans_pelicans",
    "new_york_knicks", "oklahoma_city_thunder", "orlando_magic", "philadelphia_76ers",
    "phoenix_suns", "portland_trail_blazers", "sacramento_kings", "san_antonio_spurs",
    "toronto_raptors", "utah_jazz", "washington_wizards",
    "home_team_total", "away_team_total",
}
_COVERS_GARBAGE_PREFIX = re.compile(r"^(cs_|sc_|jt_)")

# ── Player availability: ruled-out players ────────────────────────────────
# Add (player_key, "YYYY-MM-DD") tuples for players confirmed out on a specific date.
# Only affects scoring for that exact date — future games are unaffected automatically.
RULED_OUT_PLAYERS: Set[Tuple[str, str]] = {
    ("russell_westbrook", "2026-03-11"),
}

# ── Pattern Registry ─────────────────────────────────────────────────────
# Curated profitable patterns from analysis of 6,547 graded signals.
# Ordered most-specific first — first match wins.
# Source matching: source_pair = both must be present; source_contains = all must be present.

PATTERN_REGISTRY: List[Dict[str, Any]] = [
    # ── Patterns validated on full grades_latest.jsonl (2026-03-05) ──
    # Ordered most-specific first — first match wins.
    # Dataset: 22,426 graded WIN/LOSS records across 2024-25 and 2025-26 NBA seasons.
    # Wilson scores use z=1.645 (90% CI lower bound).

    # nukethebooks rebounds UNDER: 19-11 (63.3%) n=30 Wilson=0.465 — B-tier (added 2026-03-25)
    # juicereel_nukethebooks source. MUST be before nukethebooks_solo_props_under (more specific).
    {
        "id": "nukethebooks_rebounds_under",
        "label": "nukethebooks rebounds prop UNDER",
        "exact_combo": "juicereel_nukethebooks",
        "market_type": "player_prop",
        "direction": "UNDER",
        "stat_type": "rebounds",
        "hist": {"record": "19-11", "win_pct": 0.633, "n": 30},
        "tier_eligible": "B",
    },

    # nukethebooks solo UNDER props: 100-69 (59.2%) n=169 Wilson=0.529
    # Updated 2026-03-23: removed line-movement duplicates (803 rows cleaned up)
    # Consistent across all months: Nov(76%), Dec(57%), Jan(62%), Feb(61%)
    {
        "id": "nukethebooks_solo_props_under",
        "label": "nukethebooks player prop UNDER",
        "exact_combo": "juicereel_nukethebooks",
        "market_type": "player_prop",
        "direction": "UNDER",
        "hist": {"record": "100-69", "win_pct": 0.592, "n": 169},
        "tier_eligible": "B",
    },

    # nukethebooks solo OVER props: 149-121 (55.2%) n=270 Wilson=0.502
    # Updated 2026-03-23: post dedup cleanup
    # Consistent: Nov(65%), Dec(64%), Jan(52%), Feb(54%)
    {
        "id": "nukethebooks_solo_props_over",
        "label": "nukethebooks player prop OVER",
        "exact_combo": "juicereel_nukethebooks",
        "market_type": "player_prop",
        "direction": "OVER",
        "hist": {"record": "149-121", "win_pct": 0.552, "n": 270},
        "tier_eligible": "B",
    },

    # REMOVED 2026-03-18: five_source_total_under (51-29) — also derived from corrupt
    # 2026-03-04 backfill plays files (inflated source counts). Needs re-evaluation after
    # plays files are regenerated from clean ledger. Real clean data: only 15 plays (8W-7L).

    # REMOVED 2026-03-18: four_source_total_under_any (698-596) — derived from corrupt
    # plays files generated during 2026-03-04 JuiceReel backfill. Bug in cross-source merge
    # stamped all 4 sources onto both OVER and UNDER for the same game, inflating source counts.
    # Real clean record (17 real-time dates): 5W-6L (45.5%) — no edge. Pattern removed.

    # REMOVED 2026-03-18: four_source_total_under (622-533) — same corrupt data source as above.
    # action+betql+dimers+oddstrader record was also inflated by the same merge bug.

    # REMOVED 2026-03-18: betql_oddstrader_total_under (32-17) — derived from corrupt
    # plays files. betql+oddstrader is a 2-source combo that also appeared in the inflated
    # 4-source plays. Needs re-evaluation after plays regeneration.

    # action_betql_total_under REMOVED 2026-03-11: current data shows 13-21 (38.2%) — negative edge.
    # Was 134-70 when added; collapsed after bad-line cleanup and dedup fixes.
    # These signals are now caught by four_source_total_under_any (if 4+ sources) or not scored.

    # action UNDER pts_reb props: 144-52 (73.5%) n=196 Wilson=0.680 — A-tier
    # Exceptional consistency: Oct(70%), Nov(73%), Dec(65%), Jan(78%), Feb(83%)
    # MUST be before action_solo_props_under (more specific — stat_type constraint).
    {
        "id": "action_props_pts_reb_under",
        "label": "action pts+reb prop UNDER",
        "exact_combo": "action",
        "market_type": "player_prop",
        "direction": "UNDER",
        "stat_type": "pts_reb",
        "hist": {"record": "144-52", "win_pct": 0.735, "n": 196},
        "tier_eligible": "A",
    },

    # action UNDER assists props: 59-30 (66.3%) n=89 Wilson=0.577 — B-tier
    # Consistent across all months: Oct(69%), Nov(67%), Dec(67%), Jan(68%), Feb(50%), Mar(62%).
    # MUST be before action_solo_props_under (more specific — stat_type constraint).
    {
        "id": "action_props_assists_under",
        "label": "action assists prop UNDER",
        "exact_combo": "action",
        "market_type": "player_prop",
        "direction": "UNDER",
        "stat_type": "assists",
        "hist": {"record": "59-30", "win_pct": 0.663, "n": 89},
        "tier_eligible": "B",
    },

    # action UNDER pts_ast props: 29-19 (60.4%) n=48 Wilson=0.485 — B-tier
    # Updated 2026-03-23: post dedup cleanup. Limited per-month samples; watch for regression.
    # MUST be before action_solo_props_under (more specific — stat_type constraint).
    {
        "id": "action_props_pts_ast_under",
        "label": "action pts+ast prop UNDER",
        "exact_combo": "action",
        "market_type": "player_prop",
        "direction": "UNDER",
        "stat_type": "pts_ast",
        "hist": {"record": "29-19", "win_pct": 0.604, "n": 48},
        "tier_eligible": "B",
    },

    # sxebets (JuiceReel) solo UNDER props: REMOVED 2026-03-20
    # validate_patterns.py: actual n=184 (was n=1068), Wilson=0.456 — below B-tier threshold 0.46.
    # The large historical n was from the pre-fix ledger. With clean rebuild data, no edge confirmed.
    # Re-evaluate when more live data accumulates.

    # betql UNDER rebounds props: 325-254 (56.1%) n=579 Wilson=0.527 — B-tier (updated 2026-03-20)
    # Data grew from n=335 to n=579 in clean rebuild. Wilson dropped slightly but still B-tier.
    # MUST be before betql_solo_props_under (more specific — stat_type constraint).
    {
        "id": "betql_props_rebounds_under",
        "label": "betql rebounds prop UNDER",
        "exact_combo": "betql",
        "market_type": "player_prop",
        "direction": "UNDER",
        "stat_type": "rebounds",
        "hist": {"record": "325-254", "win_pct": 0.561, "n": 579},
        "tier_eligible": "B",
    },

    # betql assists UNDER props: 309-261 (54.2%) n=570 Wilson=0.508 — B-tier (updated 2026-03-20)
    # Data grew from n=172 to n=570 in clean rebuild. Still positive B-tier edge.
    # MUST be before betql_solo_props_under (more specific — stat_type constraint).
    {
        "id": "betql_props_assists_under",
        "label": "betql assists prop UNDER",
        "exact_combo": "betql",
        "market_type": "player_prop",
        "direction": "UNDER",
        "stat_type": "assists",
        "hist": {"record": "309-261", "win_pct": 0.542, "n": 570},
        "tier_eligible": "B",
    },

    # betql solo UNDER props: 2832-2336 (54.8%) n=5172 Wilson=0.534
    # Largest single-source sample; consistent UNDER edge across all months.
    # Catches all betql UNDER props not matched by a more-specific betql pattern above.
    {
        "id": "betql_solo_props_under",
        "label": "betql player prop UNDER",
        "exact_combo": "betql",
        "market_type": "player_prop",
        "direction": "UNDER",
        "hist": {"record": "2832-2336", "win_pct": 0.548, "n": 5172},
        "tier_eligible": "B",
    },

    # action + oddstrader spread: REMOVED 2026-03-12
    # Was 125-87 (59%) overall but completely collapsed: Feb26 3W-14L (17.6%), Jan26 31-35 (47%).
    # Early-season hot streak (Oct25: 22-2) inflated historical numbers. No longer reliable.

    # ── JuiceReel cross-source patterns ──────────────────────────────────────
    # NOTE: Many multi-source JuiceReel patterns from commit cadac6b (2026-03-12) were
    # validated on a pre-fix ledger that included the cross-game source merge bug
    # (build_selection_key() didn't include event_key for totals/spreads/ML, causing
    # same-direction picks from different games on the same day to fuse into fake
    # multi-source signals). After the clean rebuild (2026-03-20), these cross-source
    # combos have near-zero real records. The patterns below have been REMOVED because
    # validate_patterns.py confirmed 0-1 actual graded records:
    #
    # REMOVED 2026-03-20: action_nukethebooks_moneyline (11 real records, was n=45)
    # REMOVED 2026-03-20: action_betql_nukethebooks_spread (0 records, was n=40)
    # REMOVED 2026-03-20: all_jr_total (0 records, was n=80)
    # REMOVED 2026-03-20: nukethebooks_sxebets_total (1 record, was n=176)
    # REMOVED 2026-03-20: action_betql_sxebets_total (0 records, was n=94)
    # REMOVED 2026-03-20: sxebets_unitvacuum_spread (1 record, was n=62)
    # REMOVED 2026-03-20: action_betql_unitvacuum_spread (0 records, was n=57)
    # REMOVED 2026-03-20: betql_nukethebooks_total (few records, Wilson<0.46)
    # REMOVED 2026-03-20: betql_sxebets_props_over (Wilson=0.348, below hard exclude)
    # REMOVED 2026-03-20: action_betql_total_under (Wilson=0.444, below threshold)
    # REMOVED 2026-03-20: nukethebooks_solo_spread (14 records, was n=334)
    #
    # These patterns need to re-accumulate real live data before being re-added.

    # unitvacuum solo total: actual record from clean rebuild (validate_patterns.py)
    # WARN: n drift from hist — accumulating live data, update when n>=50.
    {
        "id": "unitvacuum_solo_total",
        "label": "unitvacuum total",
        "exact_combo": "juicereel_unitvacuum",
        "market_type": "total",
        "hist": {"record": "106-89", "win_pct": 0.544, "n": 195},
        "tier_eligible": "B",
    },

    # unitvacuum solo moneyline: REMOVED 2026-03-20
    # validate_patterns.py: actual n=63, Wilson=0.452 — below B-tier threshold 0.46.
    # The historical n=98 was from pre-fix data. Re-evaluate when 50+ live records accumulate.

    # action solo spread: 553-490 n=1043 W=0.505 — B-tier (updated 2026-03-20 from validate_patterns)
    # Data grew significantly; new n=1043 with Wilson=0.505. Still B-tier positive edge.
    {
        "id": "action_solo_spread",
        "label": "action spread",
        "exact_combo": "action",
        "market_type": "spread",
        "hist": {"record": "553-490", "win_pct": 0.530, "n": 1043},
        "tier_eligible": "B",
    },

    # ── BetQL team-specific spread patterns ──────────────────────────────────
    # BetQL solo is unreliable overall on spreads (~50%) but highly accurate on
    # specific teams. Patterns use the "team" key to match signal.direction.
    # Data: grades_latest.jsonl (2026-03-05). All records are current-season validated.
    # A-tier requires Wilson >= 0.50 AND consistent across multiple months.

    # betql BOS spread: 63-33 (65.6%) n=96 Wilson=0.564 — A-tier
    # Updated 2026-03-25
    {
        "id": "betql_spread_bos",
        "label": "betql spread BOS",
        "exact_combo": "betql",
        "market_type": "spread",
        "team": "BOS",
        "hist": {"record": "63-33", "win_pct": 0.656, "n": 96},
        "tier_eligible": "A",
    },
    # betql MIN spread: 59-27 (68.6%) n=86 Wilson=0.591 — A-tier
    # Updated 2026-03-25
    {
        "id": "betql_spread_min",
        "label": "betql spread MIN",
        "exact_combo": "betql",
        "market_type": "spread",
        "team": "MIN",
        "hist": {"record": "59-27", "win_pct": 0.686, "n": 86},
        "tier_eligible": "A",
    },
    # betql GSW spread: 58-37 (61.1%) n=97 Wilson=0.510 — B-tier (25-26: degraded)
    # Current season deteriorated; overall still positive edge at B-tier level.
    {
        "id": "betql_spread_gsw",
        "label": "betql spread GSW",
        "exact_combo": "betql",
        "market_type": "spread",
        "team": "GSW",
        "hist": {"record": "58-37", "win_pct": 0.611, "n": 97},
        "tier_eligible": "B",
    },
    # REMOVED 2026-03-18: betql_spread_nyk — full-dataset audit (clean_graded_picks) shows
    # 75-82 (47.8%) Wilson=0.413 — below WILSON_HARD_EXCLUDE=0.42 with n=157. Removed.
    # REMOVED 2026-03-20: betql_spread_chi — full clean rebuild shows 33-29 (53.2%) n=65
    # Wilson=0.410, well below WILSON_HARD_EXCLUDE=0.42. Earlier n=13 data was too small.
    # The edge was illusory (small sample in 2024-25 playoffs inflated the record).

    # ── OddsTrader team-specific moneyline patterns ───────────────────────────
    # REMOVED 2026-03-20: oddstrader_ml_nyk (0 real records in clean rebuild, was n=27)
    # REMOVED 2026-03-20: oddstrader_ml_sas (0 real records in clean rebuild, was n=26)
    # REMOVED 2026-03-20: oddstrader_ml_hou (0 real records in clean rebuild, was n=19)
    # These patterns were based on pre-fix data. Re-evaluate after accumulating 20+ live records.

    # action + oddstrader moneyline: 46-16 (74.2%) n=62 Wilson=0.642 — B-tier (updated 2026-03-20)
    # validate_patterns.py: actual n=62 (data grew; registry was stale at n=27). Wilson=0.642.
    # Promoted to A-tier candidate — check monthly breakdown before upgrading.
    # MUST be before solo action or oddstrader moneyline patterns (more specific).
    {
        "id": "action_oddstrader_moneyline",
        "label": "action + oddstrader moneyline",
        "source_pair": ["action", "oddstrader"],
        "market_type": "moneyline",
        "min_sources": 2,
        "hist": {"record": "46-16", "win_pct": 0.742, "n": 62},
        "tier_eligible": "B",
    },

    # ── OddsTrader player prop UNDER ─────────────────────────────────────────
    # Validated on full clean rebuild 2026-03-20 (35,130 graded records).
    # oddstrader solo UNDER props: 155-94 (62.2%) n=249 Wilson=0.561 — B-tier (was A)
    # REMOVED 2026-03-23: 270 picks are 91% from March 2026 only (2 calendar months).
    # Single-month concentration fails temporal integrity check. Re-add after multi-month accumulation.
    # Pattern was: oddstrader solo UNDER props, exact_combo=oddstrader, direction=UNDER, 167-103 (61.9%).

    # ── SportsLine player prop patterns ─────────────────────────────────────
    # Validated on full clean rebuild 2026-03-23. SportsLine expert picks backfill included.
    # Wilson scores use z=1.645 (90% CI lower bound).

    # sportsline assists OVER: 57-30 (65.5%) n=87 Wilson=0.568 — B-tier (added 2026-03-23)
    # 11 months of data, top month 23%. WARN: recent 30d 40% (n=20) — cooling. Keep B-tier.
    # MUST be before sportsline_solo_props_under and sportsline_solo_props_over (more specific).
    {
        "id": "sportsline_assists_over",
        "label": "sportsline assists prop OVER",
        "exact_combo": "sportsline",
        "market_type": "player_prop",
        "direction": "OVER",
        "stat_type": "assists",
        "hist": {"record": "57-30", "win_pct": 0.655, "n": 87},
        "tier_eligible": "B",
    },

    # sportsline pts_reb_ast UNDER: 81-48 (62.8%) n=129 Wilson=0.556 — B-tier (added 2026-03-23)
    # 11 months of data, top month 26%. NOTE: recent March 2026 cold (40% win, 6-9). Keep B-tier.
    # MUST be before sportsline_solo_props_under (more specific).
    {
        "id": "sportsline_pts_reb_ast_under",
        "label": "sportsline pts+reb+ast prop UNDER",
        "exact_combo": "sportsline",
        "market_type": "player_prop",
        "direction": "UNDER",
        "stat_type": "pts_reb_ast",
        "hist": {"record": "81-48", "win_pct": 0.628, "n": 129},
        "tier_eligible": "B",
    },

    # sportsline UNDER props: 189-130 (59.2%) n=319 Wilson=0.538 — B-tier
    # Strong UNDER edge; OVER is 53.2% (also positive but weaker).
    # MUST be before any generic sportsline pattern (more specific direction).
    {
        "id": "sportsline_solo_props_under",
        "label": "sportsline player prop UNDER",
        "exact_combo": "sportsline",
        "market_type": "player_prop",
        "direction": "UNDER",
        "hist": {"record": "189-130", "win_pct": 0.592, "n": 319},
        "tier_eligible": "B",
    },

    # ── SportsLine expert-level patterns ─────────────────────────────────────
    # These MUST come before sportsline_solo_props_over / sportsline_solo_props_under
    # (first-match-wins; generic sportsline pattern would fire first otherwise).
    # All sportsline source. Expert field matched via signal.experts list.

    # Larry Hartstein assists OVER: 20-6 (76.9%) n=26 Wilson=0.593 — A-tier (added 2026-03-25)
    {
        "id": "expert_larry_hartstein_assists_over",
        "label": "Larry Hartstein assists prop OVER",
        "expert": "Larry Hartstein",
        "market_type": "player_prop",
        "direction": "OVER",
        "stat_type": "assists",
        "hist": {"record": "20-6", "win_pct": 0.769, "n": 26},
        "tier_eligible": "A",
    },

    # Mike Barner assists OVER: 17-8 (68.0%) n=25 Wilson=0.505 — A-tier (added 2026-03-25)
    # MUST be before expert_mike_barner (more specific).
    {
        "id": "expert_mike_barner_assists_over",
        "label": "Mike Barner assists prop OVER",
        "expert": "Mike Barner",
        "market_type": "player_prop",
        "direction": "OVER",
        "stat_type": "assists",
        "hist": {"record": "17-8", "win_pct": 0.680, "n": 25},
        "tier_eligible": "A",
    },

    # Mike Barner reb+ast OVER: 37-21 (63.8%) n=58 Wilson=0.519 — A-tier (added 2026-03-25)
    # MUST be before expert_mike_barner (more specific).
    {
        "id": "expert_mike_barner_reb_ast_over",
        "label": "Mike Barner reb+ast prop OVER",
        "expert": "Mike Barner",
        "market_type": "player_prop",
        "direction": "OVER",
        "stat_type": "reb_ast",
        "hist": {"record": "37-21", "win_pct": 0.638, "n": 58},
        "tier_eligible": "A",
    },

    # Mike Barner points: 164-121 (57.5%) n=285 Wilson=0.518 — A-tier (added 2026-03-25)
    # MUST be before expert_mike_barner (more specific).
    {
        "id": "expert_mike_barner_points",
        "label": "Mike Barner points prop",
        "expert": "Mike Barner",
        "market_type": "player_prop",
        "stat_type": "points",
        "hist": {"record": "164-121", "win_pct": 0.575, "n": 285},
        "tier_eligible": "A",
    },

    # Mike Barner: 294-211 (58.2%) n=505 Wilson=0.546 — B-tier (catch-all for other stats)
    {
        "id": "expert_mike_barner",
        "label": "Mike Barner player prop",
        "expert": "Mike Barner",
        "market_type": "player_prop",
        "hist": {"record": "294-211", "win_pct": 0.582, "n": 505},
        "tier_eligible": "B",
    },

    # Matt Severance points UNDER: 27-9 (75.0%) n=36 Wilson=0.601 — A-tier (added 2026-03-25)
    # MUST be before expert_matt_severance (more specific).
    {
        "id": "expert_matt_severance_points_under",
        "label": "Matt Severance points prop UNDER",
        "expert": "Matt Severance",
        "market_type": "player_prop",
        "direction": "UNDER",
        "stat_type": "points",
        "hist": {"record": "27-9", "win_pct": 0.750, "n": 36},
        "tier_eligible": "A",
    },

    # Matt Severance: 61-31 (66.3%) n=92 Wilson=0.562 — B-tier (catch-all; spreads excluded)
    {
        "id": "expert_matt_severance",
        "label": "Matt Severance player prop",
        "expert": "Matt Severance",
        "market_type": "player_prop",
        "hist": {"record": "61-31", "win_pct": 0.663, "n": 92},
        "tier_eligible": "B",
    },

    # Prop Bet Guy pts+ast OVER: 33-24 (57.9%) n=57 Wilson=0.461 — A-tier (added 2026-03-25)
    # MUST be before expert_prop_bet_guy (more specific).
    {
        "id": "expert_prop_bet_guy_pts_ast_over",
        "label": "Prop Bet Guy pts+ast prop OVER",
        "expert": "Prop Bet Guy",
        "market_type": "player_prop",
        "direction": "OVER",
        "stat_type": "pts_ast",
        "hist": {"record": "33-24", "win_pct": 0.579, "n": 57},
        "tier_eligible": "A",
    },

    # Prop Bet Guy rebounds OVER: 28-18 (60.9%) n=46 Wilson=0.467 — A-tier (added 2026-03-25)
    # MUST be before expert_prop_bet_guy (more specific).
    {
        "id": "expert_prop_bet_guy_rebounds_over",
        "label": "Prop Bet Guy rebounds prop OVER",
        "expert": "Prop Bet Guy",
        "market_type": "player_prop",
        "direction": "OVER",
        "stat_type": "rebounds",
        "hist": {"record": "28-18", "win_pct": 0.609, "n": 46},
        "tier_eligible": "A",
    },

    # Prop Bet Guy pts+reb+ast UNDER: 34-16 (68.0%) n=50 Wilson=0.548 — A-tier (added 2026-03-25)
    # MUST be before expert_prop_bet_guy (more specific).
    {
        "id": "expert_prop_bet_guy_pts_reb_ast_under",
        "label": "Prop Bet Guy pts+reb+ast prop UNDER",
        "expert": "Prop Bet Guy",
        "market_type": "player_prop",
        "direction": "UNDER",
        "stat_type": "pts_reb_ast",
        "hist": {"record": "34-16", "win_pct": 0.680, "n": 50},
        "tier_eligible": "A",
    },

    # Prop Bet Guy: 319-281 (53.2%) n=600 Wilson=0.492 — B-tier (catch-all)
    {
        "id": "expert_prop_bet_guy",
        "label": "Prop Bet Guy player prop",
        "expert": "Prop Bet Guy",
        "market_type": "player_prop",
        "hist": {"record": "319-281", "win_pct": 0.532, "n": 600},
        "tier_eligible": "B",
    },

    # sportsline OVER props: 775-682 (53.2%) n=1457 Wilson=0.506 — B-tier
    # Large sample with positive edge; not as strong as UNDER but still above threshold.
    {
        "id": "sportsline_solo_props_over",
        "label": "sportsline player prop OVER",
        "exact_combo": "sportsline",
        "market_type": "player_prop",
        "direction": "OVER",
        "hist": {"record": "775-682", "win_pct": 0.532, "n": 1457},
        "tier_eligible": "B",
    },

    # ── BettingPros Experts patterns ─────────────────────────────────────────
    # Validated on bettingpros_experts backfill (2024–2026).
    # Wilson scores use z=1.645 (90% CI lower bound).

    # REMOVED 2026-03-20: action_bettingpros_total_under (0 real records in clean rebuild, was n=125)
    # REMOVED 2026-03-20: action_bettingpros_spread (0 real records in clean rebuild, was n=160)
    # The action|bettingpros_experts exact_combo pattern matched on exact sources_combo field
    # but the graded data uses a different sources_combo format — these had zero real records.
    # Re-evaluate the matching logic before re-adding.

    # bettingpros_experts assists OVER: 55-15 (78.6%) n=70 Wilson=0.695 — A-tier (added 2026-03-20)
    # Exceptional consistency on assists OVER props. Must be before general bettingpros props pattern.
    {
        "id": "bettingpros_experts_assists_over",
        "label": "bettingpros experts assists OVER",
        "exact_combo": "bettingpros_experts",
        "market_type": "player_prop",
        "direction": "OVER",
        "stat_type": "assists",
        "hist": {"record": "55-15", "win_pct": 0.786, "n": 70},
        "tier_eligible": "A",
    },

    # bettingpros_experts rebounds OVER: 66-31 (68.0%) n=97 Wilson=0.599 — A-tier (added 2026-03-20)
    # Strong rebounds OVER signal. Must be before general bettingpros props pattern.
    {
        "id": "bettingpros_experts_rebounds_over",
        "label": "bettingpros experts rebounds OVER",
        "exact_combo": "bettingpros_experts",
        "market_type": "player_prop",
        "direction": "OVER",
        "stat_type": "rebounds",
        "hist": {"record": "66-31", "win_pct": 0.680, "n": 97},
        "tier_eligible": "A",
    },

    # bettingpros_experts player prop OVER: 381-207 (64.8%) n=588 Wilson=0.615 — A-tier (added 2026-03-20)
    # Exceptionally strong OVER edge on player props. Validated on full clean rebuild.
    # OVER direction only (UNDER is different market behavior). Must be before generic total pattern.
    # MUST be before action_bettingpros_total_under and bettingpros_experts_total (more specific).
    {
        "id": "bettingpros_experts_props",
        "label": "bettingpros experts player prop OVER",
        "exact_combo": "bettingpros_experts",
        "market_type": "player_prop",
        "direction": "OVER",
        "hist": {"record": "381-207", "win_pct": 0.648, "n": 588},
        "tier_eligible": "B",
    },

    # bettingpros_experts total OVER: 43-25 (63.2%) n=68 Wilson=0.533 — B-tier (added 2026-03-23)
    # OVER direction only: 8 months data, top month 25%. UNDER is weaker (61.2% n=62).
    # NOTE: Apr-May 2025 show soft performance (29-33%) — keep at B-tier.
    # MUST be before bettingpros_experts_total (more specific — direction constraint).
    {
        "id": "bettingpros_experts_total_over",
        "label": "bettingpros experts total OVER",
        "exact_combo": "bettingpros_experts",
        "market_type": "total",
        "direction": "OVER",
        "hist": {"record": "43-25", "win_pct": 0.632, "n": 68},
        "tier_eligible": "B",
    },

    # bettingpros_experts solo totals: 76-47 (61.8%) n=123 Wilson=0.544 — B-tier (added 2026-03-18)
    # Both OVER (64.3%) and UNDER (61.2%) show edge — direction-agnostic combined pattern.
    # No heavy-favorite bias (2.9% at odds < -200). Consistent across market types.
    {
        "id": "bettingpros_experts_total",
        "label": "bettingpros experts total",
        "exact_combo": "bettingpros_experts",
        "market_type": "total",
        "hist": {"record": "76-47", "win_pct": 0.618, "n": 123},
        "tier_eligible": "B",
    },

    # bettingpros_experts moneyline: 107-57 (65.2%) n=164 Wilson=0.589 — A-tier (added 2026-03-23)
    # 8 months of data (Nov 2024 – May 2025, then Mar 2026). Top month 41%. No single-month concentration.
    # NOTE: Jun 2025 – Feb 2026 gap is off-season + early 2025-26 season (no ML picks posted then).
    {
        "id": "bettingpros_experts_moneyline",
        "label": "bettingpros experts moneyline",
        "exact_combo": "bettingpros_experts",
        "market_type": "moneyline",
        "hist": {"record": "107-57", "win_pct": 0.652, "n": 164},
        "tier_eligible": "A",
    },

    # ── Expert-level patterns (added 2026-03-18) ─────────────────────────────
    # Validated on by_expert_record.json (data/reports). Wilson z=1.645 (90% CI lower bound).
    # Match criterion: signal.experts list contains the named expert (any source).
    # Ordered most specific first. Source-agnostic — expert appears on any source.
    # Only experts with n>=30 and Wilson>=0.46 are included.
    # Expert patterns are intentionally placed LAST in registry — source-combo patterns
    # (which are more specific) take priority. Expert match fires only when no source
    # pattern matched first.

    # REMOVED 2026-03-24: expert_carmenciorra (all-markets) — replaced by market-specific entries
    # expert_carmenciorra_moneyline and expert_carmenciorra_total added below.

    # REMOVED 2026-03-23: parkerpaduck — 100% of 130 picks from March 2026 only (1 month).
    # Re-add after accumulating multi-month data. Was: 84-46 (64.6%) Wilson=0.575.

    # REMOVED 2026-03-23: MyPicksWithNoResearch — 100% of 64 picks from March 2026 only (1 month).
    # Re-add after accumulating multi-month data. Was: 40-24 (62.5%) Wilson=0.522.

    # carmenciorra moneyline: 63-34 (65.0%) n=97 Wilson=0.551 — A-tier (added 2026-03-24)
    # bettingpros_experts source. Replaces all-markets expert_carmenciorra entry.
    {
        "id": "expert_carmenciorra_moneyline",
        "label": "carmenciorra moneyline",
        "expert": "carmenciorra",
        "market_type": "moneyline",
        "hist": {"record": "63-34", "win_pct": 0.650, "n": 97},
        "tier_eligible": "A",
    },

    # carmenciorra total: 56-30 (65.1%) n=86 Wilson=0.546 — A-tier (added 2026-03-24)
    # bettingpros_experts source.
    {
        "id": "expert_carmenciorra_total",
        "label": "carmenciorra total",
        "expert": "carmenciorra",
        "market_type": "total",
        "hist": {"record": "56-30", "win_pct": 0.651, "n": 86},
        "tier_eligible": "A",
    },

    # Kyle Murray rebounds prop: 17-3 (85.0%) n=20 Wilson=0.640 — A-tier (added 2026-03-24)
    # action source. Very thin sample — monitor closely. MUST be before expert_kyle_murray_pts_reb_under.
    {
        "id": "expert_kyle_murray_rebounds",
        "label": "Kyle Murray rebounds prop",
        "expert": "Kyle Murray",
        "market_type": "player_prop",
        "stat_type": "rebounds",
        "hist": {"record": "17-3", "win_pct": 0.850, "n": 20},
        "tier_eligible": "A",
    },

    # Kyle Murray pts+reb UNDER: 50-13 (79.4%) n=63 Wilson=0.699 — A-tier (added 2026-03-23)
    # action source. Temporal: 6 months, top month 38% (Jan 2026). Nov dip (1-3) then very hot.
    # Very high Wilson even with conservative estimate. Props only, pts_reb UNDER specifically.
    # MUST be before expert_kyle_murray (more specific) if that broader pattern is ever added.
    {
        "id": "expert_kyle_murray_pts_reb_under",
        "label": "Kyle Murray pts+reb UNDER",
        "expert": "Kyle Murray",
        "market_type": "player_prop",
        "direction": "UNDER",
        "stat_type": "pts_reb",
        "hist": {"record": "50-13", "win_pct": 0.794, "n": 63},
        "tier_eligible": "A",
    },

    # Bruce Marshall spreads: 123-94 (56.7%) n=218 Wilson=0.500 — B-tier
    # covers source; large sample. Totals record is 3-5 (small/bad) — spread-only.
    {
        "id": "expert_bruce_marshall",
        "label": "Bruce Marshall spread",
        "expert": "Bruce Marshall",
        "market_type": "spread",
        "hist": {"record": "123-94", "win_pct": 0.567, "n": 218},
        "tier_eligible": "B",
    },

    # Alex Selesnick: 118-98 (54.6%) n=216 Wilson=0.480 — B-tier (player props)
    # sportsline source; backfill 2024-present. Player props: 116-95 (55.0%) W=0.482.
    {
        "id": "expert_alex_selesnick",
        "label": "Alex Selesnick player prop",
        "expert": "Alex Selesnick",
        "market_type": "player_prop",
        "hist": {"record": "116-95", "win_pct": 0.550, "n": 211},
        "tier_eligible": "B",
    },

    # Mjaybrod: 20-9 (69.0%) n=29 Wilson=0.508 — B-tier (totals only)
    # bettingpros_experts source. Spread record: 13-14 (48.1%) — negative. Totals only.
    {
        "id": "expert_mjaybrod",
        "label": "Mjaybrod total",
        "expert": "Mjaybrod",
        "market_type": "total",
        "hist": {"record": "20-9", "win_pct": 0.690, "n": 29},
        "tier_eligible": "B",
    },

    # Markus Markets pts+ast prop: 19-6 (76.0%) n=25 Wilson=0.566 — B-tier (added 2026-03-24)
    # action source. Thin sample but strong Wilson. MUST be before expert_markus_markets (more specific).
    {
        "id": "expert_markus_markets_pts_ast",
        "label": "Markus Markets pts+ast prop",
        "expert": "Markus Markets",
        "market_type": "player_prop",
        "stat_type": "pts_ast",
        "hist": {"record": "19-6", "win_pct": 0.760, "n": 25},
        "tier_eligible": "B",
    },

    # Markus Markets: 131-93 (58.5%) n=224 Wilson=0.530 — B-tier (updated 2026-03-20)
    # validate_patterns.py: data grew from n=168 to n=224, Wilson improved to 0.530.
    # action source; exclusively player props.
    {
        "id": "expert_markus_markets",
        "label": "Markus Markets (expert)",
        "expert": "Markus Markets",
        "hist": {"record": "131-93", "win_pct": 0.585, "n": 224},
        "tier_eligible": "B",
    },

    # Allan Lem assists prop: 29-14 (67.4%) n=43 Wilson=0.525 — B-tier (added 2026-03-24)
    # action source. MUST be before expert_allan_lem (more specific).
    {
        "id": "expert_allan_lem_assists",
        "label": "Allan Lem assists prop",
        "expert": "allan_lem",
        "market_type": "player_prop",
        "stat_type": "assists",
        "hist": {"record": "29-14", "win_pct": 0.674, "n": 43},
        "tier_eligible": "B",
    },

    # Allan Lem: 146-106 (57.9%) n=252 Wilson=0.528 — B-tier (player props only) (updated 2026-03-20)
    # validate_patterns.py: data grew from n=223 to n=252, Wilson improved to 0.528.
    # covers source; large prop sample. Total record: 4-8 (33.3%), spread: 3-5 (37.5%) — bad.
    {
        "id": "expert_allan_lem",
        "label": "Allan Lem player prop",
        "expert": "Allan Lem",
        "market_type": "player_prop",
        "hist": {"record": "146-106", "win_pct": 0.579, "n": 252},
        "tier_eligible": "B",
    },

    # ── OddsTrader team-specific spread patterns ─────────────────────────────
    # Validated 2026-03-25. oddstrader source. MUST be before oddstrader_ai_spread (more specific).
    # Wilson scores use z=1.645 (90% CI lower bound).

    # oddstrader SAC spread: 41-8 (83.7%) n=49 Wilson=0.731 — A-tier
    {
        "id": "oddstrader_spread_sac",
        "label": "OddsTrader spread SAC",
        "exact_combo": "oddstrader",
        "market_type": "spread",
        "team": "SAC",
        "hist": {"record": "41-8", "win_pct": 0.837, "n": 49},
        "tier_eligible": "A",
    },
    # oddstrader NOP spread: 34-7 (82.9%) n=41 Wilson=0.706 — A-tier
    {
        "id": "oddstrader_spread_nop",
        "label": "OddsTrader spread NOP",
        "exact_combo": "oddstrader",
        "market_type": "spread",
        "team": "NOP",
        "hist": {"record": "34-7", "win_pct": 0.829, "n": 41},
        "tier_eligible": "A",
    },
    # oddstrader IND spread: 26-7 (78.8%) n=33 Wilson=0.645 — A-tier
    {
        "id": "oddstrader_spread_ind",
        "label": "OddsTrader spread IND",
        "exact_combo": "oddstrader",
        "market_type": "spread",
        "team": "IND",
        "hist": {"record": "26-7", "win_pct": 0.788, "n": 33},
        "tier_eligible": "A",
    },
    # oddstrader CHA spread: 20-6 (76.9%) n=26 Wilson=0.593 — A-tier
    {
        "id": "oddstrader_spread_cha",
        "label": "OddsTrader spread CHA",
        "exact_combo": "oddstrader",
        "market_type": "spread",
        "team": "CHA",
        "hist": {"record": "20-6", "win_pct": 0.769, "n": 26},
        "tier_eligible": "A",
    },
    # oddstrader MEM spread: 21-7 (75.0%) n=28 Wilson=0.579 — A-tier
    {
        "id": "oddstrader_spread_mem",
        "label": "OddsTrader spread MEM",
        "exact_combo": "oddstrader",
        "market_type": "spread",
        "team": "MEM",
        "hist": {"record": "21-7", "win_pct": 0.750, "n": 28},
        "tier_eligible": "A",
    },
    # oddstrader WAS spread: 31-12 (72.1%) n=43 Wilson=0.584 — A-tier
    {
        "id": "oddstrader_spread_was",
        "label": "OddsTrader spread WAS",
        "exact_combo": "oddstrader",
        "market_type": "spread",
        "team": "WAS",
        "hist": {"record": "31-12", "win_pct": 0.721, "n": 43},
        "tier_eligible": "A",
    },
    # oddstrader CHI spread: 26-10 (72.2%) n=36 Wilson=0.574 — A-tier
    {
        "id": "oddstrader_spread_chi",
        "label": "OddsTrader spread CHI",
        "exact_combo": "oddstrader",
        "market_type": "spread",
        "team": "CHI",
        "hist": {"record": "26-10", "win_pct": 0.722, "n": 36},
        "tier_eligible": "A",
    },
    # oddstrader MIL spread: 18-8 (69.2%) n=26 Wilson=0.511 — A-tier (borderline)
    {
        "id": "oddstrader_spread_mil",
        "label": "OddsTrader spread MIL",
        "exact_combo": "oddstrader",
        "market_type": "spread",
        "team": "MIL",
        "hist": {"record": "18-8", "win_pct": 0.692, "n": 26},
        "tier_eligible": "A",
    },
    # oddstrader DAL spread: 23-13 (63.9%) n=36 Wilson=0.487 — B-tier
    {
        "id": "oddstrader_spread_dal",
        "label": "OddsTrader spread DAL",
        "exact_combo": "oddstrader",
        "market_type": "spread",
        "team": "DAL",
        "hist": {"record": "23-13", "win_pct": 0.639, "n": 36},
        "tier_eligible": "B",
    },
    # oddstrader BKN spread: 22-13 (62.9%) n=35 Wilson=0.474 — B-tier
    {
        "id": "oddstrader_spread_bkn",
        "label": "OddsTrader spread BKN",
        "exact_combo": "oddstrader",
        "market_type": "spread",
        "team": "BKN",
        "hist": {"record": "22-13", "win_pct": 0.629, "n": 35},
        "tier_eligible": "B",
    },

    # ── OddsTrader player prop UNDER patterns ────────────────────────────────
    # Validated 2026-03-25. oddstrader source. Stat-specific patterns before generic.

    # oddstrader rebounds UNDER: 22-10 (68.8%) n=32 Wilson=0.531 — A-tier
    {
        "id": "oddstrader_props_rebounds_under",
        "label": "OddsTrader rebounds prop UNDER",
        "exact_combo": "oddstrader",
        "market_type": "player_prop",
        "direction": "UNDER",
        "stat_type": "rebounds",
        "hist": {"record": "22-10", "win_pct": 0.688, "n": 32},
        "tier_eligible": "A",
    },
    # oddstrader assists UNDER: 23-11 (67.6%) n=34 Wilson=0.521 — A-tier
    {
        "id": "oddstrader_props_assists_under",
        "label": "OddsTrader assists prop UNDER",
        "exact_combo": "oddstrader",
        "market_type": "player_prop",
        "direction": "UNDER",
        "stat_type": "assists",
        "hist": {"record": "23-11", "win_pct": 0.676, "n": 34},
        "tier_eligible": "A",
    },
    # oddstrader reb+ast UNDER: 16-8 (66.7%) n=24 Wilson=0.484 — B-tier
    {
        "id": "oddstrader_props_reb_ast_under",
        "label": "OddsTrader reb+ast prop UNDER",
        "exact_combo": "oddstrader",
        "market_type": "player_prop",
        "direction": "UNDER",
        "stat_type": "reb_ast",
        "hist": {"record": "16-8", "win_pct": 0.667, "n": 24},
        "tier_eligible": "B",
    },
    # oddstrader pts+reb+ast UNDER: 30-18 (62.5%) n=48 Wilson=0.491 — B-tier
    {
        "id": "oddstrader_props_pts_reb_ast_under",
        "label": "OddsTrader pts+reb+ast prop UNDER",
        "exact_combo": "oddstrader",
        "market_type": "player_prop",
        "direction": "UNDER",
        "stat_type": "pts_reb_ast",
        "hist": {"record": "30-18", "win_pct": 0.625, "n": 48},
        "tier_eligible": "B",
    },
    # oddstrader points UNDER: 31-19 (62.0%) n=50 Wilson=0.491 — B-tier
    {
        "id": "oddstrader_props_points_under",
        "label": "OddsTrader points prop UNDER",
        "exact_combo": "oddstrader",
        "market_type": "player_prop",
        "direction": "UNDER",
        "stat_type": "points",
        "hist": {"record": "31-19", "win_pct": 0.620, "n": 50},
        "tier_eligible": "B",
    },

    # OddsTrader AI spread: 399-212 (65.3%) n=611 Wilson=0.614 — B-tier (added 2026-03-24)
    # oddstrader source. Very large sample, strong Wilson. Cold-flagged as of 2026-03; monitor for A-tier.
    {
        "id": "oddstrader_ai_spread",
        "label": "OddsTrader AI spread",
        "exact_combo": "oddstrader",
        "market_type": "spread",
        "hist": {"record": "399-212", "win_pct": 0.653, "n": 611},
        "tier_eligible": "B",
    },

    # Cam Is Money: 106-89 (54.4%) n=195 Wilson=0.485 — B-tier (player props only) (updated 2026-03-20)
    # validate_patterns.py: 107-91 n=198 Wilson=0.482. WARN: recent 30d 40% (n=10) cooling.
    # action source. ML: 42.9%, spread: 42.9% — only props have edge.
    {
        "id": "expert_cam_is_money",
        "label": "Cam Is Money player prop",
        "expert": "Cam Is Money",
        "market_type": "player_prop",
        "hist": {"record": "107-91", "win_pct": 0.540, "n": 198},
        "tier_eligible": "B",
    },
]

# ── NCAAB Pattern Registry ────────────────────────────────────────────────────
# Separate registry for NCAAB signals. Same schema as PATTERN_REGISTRY.
# Currently empty — need to accumulate graded NCAAB history before patterns
# can be validated. Add patterns here as data grows (aim for n≥50 per pattern).
# Sources active for NCAAB: action, covers, sportsline, dimers, oddstrader, juicereel.
PATTERN_REGISTRY_NCAAB: List[Dict[str, Any]] = [
    # Updated 2026-03-20: validate_patterns.py audit against NCAAB graded occurrences.
    # Wilson lower bound (z=1.645): A-tier ≥ 0.52, B-tier ≥ 0.46
    #
    # REMOVED 2026-03-20: ncaab_both_experts_total (1 real record vs registry n=217 — merge bug data)
    # REMOVED 2026-03-20: ncaab_nukethebooks_ml (Wilson=0.419, below threshold; n=32 vs n=219)
    # REMOVED 2026-03-20: ncaab_sxebets_ml (2 real records vs registry n=158 — likely merge bug)
    # REMOVED 2026-03-20: ncaab_sxebets_props (1 real record vs registry n=35 — merge bug data)
    #
    # Remaining patterns: only those with real graded data and Wilson >= 0.46.

    # nukethebooks NCAAB totals: 18-12 n=30 W=0.451 — ACCUMULATING (not yet B-tier)
    # REMOVED from active patterns 2026-03-20: Wilson=0.451 just below B-tier threshold 0.46.
    # Re-add when n>=50 and Wilson >= 0.46. Record is promising (60%); needs more data.
    # Previous registry n=154 was from a pre-fix ledger (NCAAB occurrence data was different format).
    # {
    #     "id": "ncaab_nukethebooks_total",
    #     "label": "NCAAB total (nukethebooks)",
    #     "exact_combo": "juicereel_nukethebooks",
    #     "market_type": "total",
    #     "hist": {"record": "18-12", "win_pct": 0.600, "n": 30},
    #     "tier_eligible": "B",
    # },
]

# Known bad combos by market type — exclude from scoring regardless of other criteria
# These have large sample sizes showing negative/coin-flip results
EXCLUDED_COMBOS_BY_MARKET: set = {
    # NBA JuiceReel solo moneylines: juicereel_nukethebooks 43.6% (n=303), juicereel_sxebets ~40%
    # Both are below the 42% hard-exclude threshold with large samples.
    # unitvacuum moneyline is 57.1% (n=98) — NOT excluded, has its own B-tier pattern.
    ("juicereel_nukethebooks", "moneyline"),
    ("juicereel_sxebets", "moneyline"),
}

# NCAAB-specific excluded combos (nukethebooks moneylines: n=642, 47.7%, Wilson=0.444)
EXCLUDED_COMBOS_BY_MARKET_NCAAB: set = {
    ("juicereel_nukethebooks", "moneyline"),
}


# ── Utility functions ──────────────────────────────────────────────────────

def wilson_lower(wins: int, n: int, z: float = 1.96) -> float:
    """Wilson score lower bound -- conservative win% estimate."""
    if n == 0:
        return 0.0
    p = wins / n
    denom = 1 + z * z / n
    center = p + z * z / (2 * n)
    spread = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)
    return (center - spread) / denom


def line_bucket(line: Optional[float], market_type: str) -> str:
    """Bucket a line value by market type (mirrors report_records.py:716-751)."""
    if line is None:
        return "no_line"
    try:
        line = float(line)
    except (TypeError, ValueError):
        return "no_line"
    if market_type == "spread":
        a = abs(line)
        if a <= 2.5:
            return "0-2.5"
        if a <= 5.5:
            return "3-5.5"
        if a <= 9.5:
            return "6-9.5"
        return "10+"
    if market_type == "total":
        if line < 215:
            return "<215"
        if line <= 225:
            return "215-225"
        if line <= 235:
            return "225-235"
        return "235+"
    if market_type == "player_prop":
        if line <= 10.5:
            return "0-10.5"
        if line <= 20.5:
            return "11-20.5"
        if line <= 30.5:
            return "21-30.5"
        return "31+"
    return "other"


def parse_day_key(day_key: Optional[str]) -> Optional[date]:
    """Parse 'NBA:YYYY:MM:DD' or 'NCAAB:YYYY:MM:DD' into a date object."""
    if not day_key or not isinstance(day_key, str):
        return None
    parts = day_key.split(":")
    if len(parts) < 4:
        return None
    try:
        return date(int(parts[1]), int(parts[2]), int(parts[3]))
    except (ValueError, IndexError):
        return None


def strip_expert_prefix(expert_str: str) -> str:
    """Strip source prefix from expert string.

    'action:/picks/profile/sandyplashkes' -> 'sandyplashkes'
    'covers:EV Model Rating Star' -> 'EV Model Rating Star'
    'Bet Labs' -> 'Bet Labs'
    """
    if ":" in expert_str:
        # Handle URL-style prefixes like 'action:/picks/profile/name'
        prefix, _, rest = expert_str.partition(":")
        # If the rest starts with / it's a URL path - extract the last segment
        if rest.startswith("/"):
            segments = [s for s in rest.split("/") if s]
            return segments[-1] if segments else expert_str
        return rest.strip()
    return expert_str


def consensus_strength_key(sources_present: List[str]) -> str:
    """Map source count to consensus_strength lookup key."""
    n = len(sources_present) if sources_present else 1
    if n >= 4:
        return "4+_sources"
    if n == 3:
        return "3_source"
    if n == 2:
        return "2_source"
    return "1_source"


def _normalize_direction(signal: Dict[str, Any]) -> Optional[str]:
    """Normalize direction to 'OVER'/'UNDER' for props/totals, None for spreads/ML.

    Handles known case inconsistency ('under' vs 'UNDER' from different normalizers).
    """
    market = signal.get("market_type", "")
    if market not in ("player_prop", "total"):
        return None
    d = (signal.get("direction") or signal.get("selection") or "").upper()
    if "OVER" in d:
        return "OVER"
    if "UNDER" in d:
        return "UNDER"
    return None


# ── Lookup table loaders ──────────────────────────────────────────────────

def _make_entry(row: Dict[str, Any], min_n: int) -> Dict[str, Any]:
    """Build a standard lookup entry from a report row."""
    n = row.get("n", 0)
    wins = row.get("wins", 0)
    wp = row.get("win_pct") or 0
    wl = row.get("wilson_lower")
    if wl is None:
        wl = wilson_lower(wins, n) if n > 0 else 0.0
    return {"win_pct": wp, "n": n, "wins": wins, "wilson_lower": wl}


def _load_combo_market(cross_tab: Dict) -> Dict[Tuple[str, str], Dict]:
    table: Dict[Tuple[str, str], Dict] = {}
    for row in cross_tab.get("by_combo_market", []):
        key = (row.get("sources_combo", ""), row.get("market_type", ""))
        table[key] = _make_entry(row, 0)
    return table


def _load_consensus(data: Dict) -> Dict[str, Dict]:
    table: Dict[str, Dict] = {}
    for row in data.get("rows", []):
        key = row.get("consensus_strength", "")
        table[key] = _make_entry(row, 0)
    return table


def _load_line_bucket(data: Dict) -> Dict[Tuple[str, str], Dict]:
    table: Dict[Tuple[str, str], Dict] = {}
    for row in data.get("rows", []):
        key = (row.get("market_type", ""), row.get("line_bucket", ""))
        table[key] = _make_entry(row, 0)
    return table


def _load_stat_type(data: Dict) -> Dict[str, Dict]:
    table: Dict[str, Dict] = {}
    for row in data.get("rows", []):
        key = row.get("stat_type", "")
        table[key] = _make_entry(row, 0)
    return table


def _load_day_of_week(data: Dict) -> Dict[str, Dict]:
    table: Dict[str, Dict] = {}
    for row in data.get("rows", []):
        key = row.get("day_of_week", "")
        table[key] = _make_entry(row, 0)
    return table


def _load_experts(data: Dict) -> Dict[str, Dict]:
    table: Dict[str, Dict] = {}
    for row in data.get("rows", []):
        key = row.get("expert", "")
        if key and key not in ("unknown", "MULTI"):
            table[key] = _make_entry(row, 0)
    return table


def _load_combo_stat(cross_tab: Dict) -> Dict[Tuple[str, str], Dict]:
    table: Dict[Tuple[str, str], Dict] = {}
    for row in cross_tab.get("by_combo_stat", []):
        key = (row.get("sources_combo", ""), row.get("stat_type", ""))
        table[key] = _make_entry(row, 0)
    return table


def _load_combo_market_stat(cross_tab: Dict) -> Dict[Tuple[str, str, str], Dict]:
    table: Dict[Tuple[str, str, str], Dict] = {}
    for row in cross_tab.get("by_combo_market_stat", []):
        key = (
            row.get("sources_combo", ""),
            row.get("market_type", ""),
            row.get("stat_type", "all"),
        )
        table[key] = _make_entry(row, 0)
    return table


def _load_consensus_market_stat(cross_tab: Dict) -> Dict[Tuple[str, str, str], Dict]:
    """Load cross_tabulation by_consensus_market_stat → {(strength, market, stat): entry}."""
    table: Dict[Tuple[str, str, str], Dict] = {}
    for row in cross_tab.get("by_consensus_market_stat", []):
        key = (
            row.get("consensus_strength", ""),
            row.get("market_type", ""),
            row.get("stat_type", "all"),
        )
        table[key] = _make_entry(row, 0)
    return table


def _load_combo_overall(data: Dict) -> Dict[str, Dict]:
    """Load by_sources_combo_record.json → {sources_combo: entry}."""
    table: Dict[str, Dict] = {}
    for row in data.get("rows", []):
        key = row.get("sources_combo", "")
        if key:
            table[key] = _make_entry(row, 0)
    return table


def _load_consensus_market(data: Dict) -> Dict[Tuple[str, str], Dict]:
    """Load market_type.json by_market_x_consensus → {(strength, market): entry}."""
    table: Dict[Tuple[str, str], Dict] = {}
    for row in data.get("by_market_x_consensus", []):
        key = (row.get("consensus_strength", ""), row.get("market_type", ""))
        table[key] = _make_entry(row, 0)
    return table


def _load_combo_market_signals(data: Dict) -> Dict[Tuple[str, str], Dict]:
    """Load by_sources_combo_market.json (signal-based) → {(combo, market): entry}."""
    table: Dict[Tuple[str, str], Dict] = {}
    for row in data.get("rows", []):
        key = (row.get("sources_combo", ""), row.get("market_type", ""))
        table[key] = _make_entry(row, 0)
    return table


def _load_combo_market_direction(data: Dict) -> Dict[Tuple[str, str, str], Dict]:
    """Load by_sources_combo_market_direction.json → {(combo, market, direction): entry}."""
    table: Dict[Tuple[str, str, str], Dict] = {}
    for row in data.get("rows", []):
        key = (
            row.get("sources_combo", ""),
            row.get("market_type", ""),
            row.get("direction", ""),
        )
        table[key] = _make_entry(row, 0)
    return table


def load_lookup_tables(reports_dir: Optional[Path] = None) -> Dict[str, Any]:
    """Load all report JSONs into lookup dictionaries."""
    rd = reports_dir or REPORTS_DIR
    tables: Dict[str, Any] = {}
    loaded = 0

    # Primary: signals-based combo × market (from by_sources_combo_market.json)
    cms_path = rd / "by_sources_combo_market.json"
    if cms_path.exists():
        tables["combo_market_signals"] = _load_combo_market_signals(json.loads(cms_path.read_text()))
        loaded += 1
    else:
        tables["combo_market_signals"] = {}

    # Direction-aware: combo × market × direction
    cmd_path = rd / "by_sources_combo_market_direction.json"
    if cmd_path.exists():
        tables["combo_market_direction"] = _load_combo_market_direction(json.loads(cmd_path.read_text()))
        loaded += 1
    else:
        tables["combo_market_direction"] = {}

    cross_tab_path = rd / "cross_tabulation.json"
    if cross_tab_path.exists():
        ct = json.loads(cross_tab_path.read_text())
        tables["combo_market"] = _load_combo_market(ct)
        tables["combo_stat"] = _load_combo_stat(ct)
        tables["combo_market_stat"] = _load_combo_market_stat(ct)
        tables["consensus_market_stat"] = _load_consensus_market_stat(ct)
        loaded += 1
    else:
        tables["combo_market"] = {}
        tables["combo_stat"] = {}
        tables["combo_market_stat"] = {}
        tables["consensus_market_stat"] = {}

    combo_record_path = rd / "by_sources_combo_record.json"
    if combo_record_path.exists():
        tables["combo_overall"] = _load_combo_overall(json.loads(combo_record_path.read_text()))
        loaded += 1
    else:
        tables["combo_overall"] = {}

    market_type_path = rd / "trends" / "market_type.json"
    if market_type_path.exists():
        mt = json.loads(market_type_path.read_text())
        tables["consensus_market"] = _load_consensus_market(mt)
        loaded += 1
    else:
        tables["consensus_market"] = {}

    for name, path, loader in [
        ("consensus", rd / "trends" / "consensus_strength.json", _load_consensus),
        ("line_bucket", rd / "by_line_bucket.json", _load_line_bucket),
        ("stat_type", rd / "by_stat_type.json", _load_stat_type),
        ("day_of_week", rd / "trends" / "by_day_of_week.json", _load_day_of_week),
    ]:
        if path.exists():
            tables[name] = loader(json.loads(path.read_text()))
            loaded += 1
        else:
            tables[name] = {}

    # Expert records: merge by_expert_record.json (occurrences) and by_expert.json (signals),
    # keeping whichever has the higher n per expert — gives the most complete record.
    experts_occ_path = rd / "by_expert_record.json"
    experts_sig_path = rd / "by_expert.json"
    experts_occ = _load_experts(json.loads(experts_occ_path.read_text())) if experts_occ_path.exists() else {}
    experts_sig = _load_experts(json.loads(experts_sig_path.read_text())) if experts_sig_path.exists() else {}
    merged_experts: Dict[str, Dict] = {}
    for key in set(experts_occ) | set(experts_sig):
        occ = experts_occ.get(key)
        sig = experts_sig.get(key)
        if occ and sig:
            merged_experts[key] = occ if occ.get("n", 0) >= sig.get("n", 0) else sig
        else:
            merged_experts[key] = occ or sig
    tables["experts"] = merged_experts
    loaded += 1

    # Composite scoring: per-source × market and per-expert × market tables
    source_market_path = rd / "by_source_record_market.json"
    if source_market_path.exists():
        src_rows = json.loads(source_market_path.read_text()).get("rows", [])
        tables["source_market"] = {
            f"{r['source_id']}|{r['market_type']}": r
            for r in src_rows
            if r.get("source_id") and r.get("market_type")
        }
        loaded += 1
    else:
        tables["source_market"] = {}

    expert_market_path = rd / "by_expert_record_market.json"
    if expert_market_path.exists():
        exp_rows = json.loads(expert_market_path.read_text()).get("rows", [])
        tables["expert_market"] = {
            f"{r['expert']}|{r['market_type']}": r
            for r in exp_rows
            if r.get("expert") and r.get("market_type") and r.get("n", 0) >= 5
        }
        loaded += 1
    else:
        tables["expert_market"] = {}

    tables["_loaded"] = loaded
    return tables


# ── Signal loading ─────────────────────────────────────────────────────────

def load_signals(date_str: str, sport: str = "NBA", signals_path: Optional[Path] = None) -> List[Dict[str, Any]]:
    """Load signals for a specific date from signals_latest.jsonl."""
    target_key = f"{sport}:{date_str.replace('-', ':')}"
    path = signals_path or SIGNALS_PATH
    signals = []
    if not path.exists():
        return signals
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            if row.get("day_key") == target_key:
                signals.append(row)
    return signals


def _normalize_dedup_key(signal: Dict[str, Any]) -> tuple:
    """Return a canonical (selection, event_key, market_type) for dedup grouping.

    Handles three known normalization issues:
    1. Action Network suffixes: SAC_spread → SAC, DAL_ml → DAL
    2. Event key direction: NBA:2026:01:01:BOS@SAC ↔ SAC@BOS → sorted alphabetically
    3. Totals selection: game_total / UNDER / OVER → normalized to direction
    """
    selection = signal.get("selection") or ""
    event_key = signal.get("event_key") or ""
    market = signal.get("market_type") or ""
    direction = (signal.get("direction") or "").upper()

    # 1. Normalize totals selection FIRST (before suffix strip)
    #    game_total → direction (UNDER/OVER), normalize case
    if market == "total":
        if selection.lower() == "game_total":
            if "OVER" in direction:
                selection = "OVER"
            elif "UNDER" in direction:
                selection = "UNDER"
        if selection.upper() in ("UNDER", "OVER"):
            selection = selection.upper()

    # 2. Strip _spread, _ml, _total suffixes from team selections
    #    (e.g. SAC_spread → SAC, DAL_ml → DAL from Action Network)
    #    Only strip from team abbreviations (3-letter codes), not from
    #    semantic names like game_total
    selection_norm = re.sub(r'^([A-Z]{2,4})_(spread|ml|total|moneyline)$', r'\1', selection, flags=re.IGNORECASE)

    # 3. Normalize event_key direction — sort the two teams alphabetically
    #    Format: NBA:2026:01:01:AWAY@HOME
    if "@" in event_key:
        prefix_end = event_key.rfind(":")
        if prefix_end > 0:
            prefix = event_key[:prefix_end + 1]
            matchup = event_key[prefix_end + 1:]
            if "@" in matchup:
                teams = matchup.split("@")
                sorted_teams = sorted(teams)
                event_key = prefix + "@".join(sorted_teams)

    # For spreads/moneylines, also normalize selection to uppercase
    if market in ("spread", "moneyline"):
        selection_norm = selection_norm.upper()

    return (selection_norm, event_key, market)


# ── NBA team abbreviation aliases ────────────────────────────────────────
# Maps non-standard abbreviations to canonical 3-letter codes used in LGF cache
_TEAM_ALIASES: Dict[str, str] = {
    "GS": "GSW", "SA": "SAS", "NY": "NYK", "NO": "NOP",
    "UTAH": "UTA", "PHO": "PHX", "BRK": "BKN", "CHO": "CHA",
}


def _canon_team(abbr: str) -> str:
    """Normalize a team abbreviation to the canonical form."""
    return _TEAM_ALIASES.get(abbr, abbr)


_NBA_TEAMS = {
    "ATL", "BOS", "BKN", "CHA", "CHI", "CLE", "DAL", "DEN", "DET", "GSW",
    "HOU", "IND", "LAC", "LAL", "MEM", "MIA", "MIL", "MIN", "NOP", "NYK",
    "OKC", "ORL", "PHI", "PHX", "POR", "SAC", "SAS", "TOR", "UTA", "WAS",
    # Also accept aliases before canonicalization
    "GS", "SA", "NY", "NO", "UTAH", "PHO", "BRK", "CHO",
}


def load_game_schedule(date_str: str, sport: str = "NBA") -> Optional[Set[Tuple[str, str]]]:
    """Load the set of (away, home) matchups that actually occurred on a date.

    For NBA: tries LGF cache first, then NBA API scoreboard cache.
    For NCAAB: reads ESPN scoreboard cache from data/cache/results/ncaab/.
    Returns set of (away_team, home_team) tuples with canonical abbreviations,
    or None if no cache data exists for this date (can't validate).
    An empty set means cache exists but 0 games were found.
    """
    matchups: Set[Tuple[str, str]] = set()
    has_cache = False

    # NCAAB: read ESPN scoreboard cache
    if sport == "NCAAB":
        ncaab_cache = CACHE_DIR / "ncaab" / f"espn_scoreboard_{date_str}.json"
        if ncaab_cache.exists():
            has_cache = True
            try:
                import sys as _sys
                _repo_root = str(Path(__file__).parent.parent)
                if _repo_root not in _sys.path:
                    _sys.path.insert(0, _repo_root)
                from store import get_data_store as _get_data_store
                from utils import normalize_text as _normalize_text
                _ncaab_store = _get_data_store("NCAAB")

                def _ncaab_canon(espn_abbr: str) -> str:
                    """Map an ESPN NCAAB abbreviation to our internal team code."""
                    if espn_abbr in _ncaab_store.teams:
                        return espn_abbr
                    norm = _normalize_text(espn_abbr)
                    codes = _ncaab_store.lookup_team_code(norm)
                    if len(codes) == 1:
                        return next(iter(codes))
                    return espn_abbr  # keep ESPN code if no match

                events = json.loads(ncaab_cache.read_text())
                for event in events:
                    comps = event.get("competitions", [{}])[0]
                    competitors = comps.get("competitors", [])
                    away_abbr = home_abbr = None
                    for c in competitors:
                        abbr = c.get("team", {}).get("abbreviation", "")
                        if c.get("homeAway") == "away":
                            away_abbr = _ncaab_canon(abbr)
                        elif c.get("homeAway") == "home":
                            home_abbr = _ncaab_canon(abbr)
                    if away_abbr and home_abbr:
                        matchups.add((away_abbr, home_abbr))
            except (json.JSONDecodeError, KeyError, IndexError):
                pass
        return matchups if has_cache else None

    # Try LGF cache
    lgf_path = CACHE_DIR / f"lgf_{date_str}.json"
    if lgf_path.exists():
        has_cache = True
        try:
            games = json.loads(lgf_path.read_text())
            for g in games:
                raw_away = g.get("away_team", "")
                raw_home = g.get("home_team", "")
                # Skip non-NBA teams
                if raw_away not in _NBA_TEAMS and _canon_team(raw_away) not in _NBA_TEAMS:
                    continue
                if raw_home not in _NBA_TEAMS and _canon_team(raw_home) not in _NBA_TEAMS:
                    continue
                # Skip pre-game placeholder entries (both scores < 40 = not a real NBA final)
                h_score = g.get("home_score") or 0
                a_score = g.get("away_score") or 0
                if g.get("status") == "FINAL" and h_score < 40 and a_score < 40:
                    continue
                away = _canon_team(raw_away)
                home = _canon_team(raw_home)
                if away and home:
                    matchups.add((away, home))
            # LGF only contains FINAL games — always also check the scoreboard
            # to pick up scheduled/in-progress games not yet in LGF.
        except (json.JSONDecodeError, KeyError):
            pass

    # Fallback: NBA API scoreboard cache
    sb_path = CACHE_DIR / f"nba_api_scoreboard_{date_str}.json"
    if sb_path.exists():
        has_cache = True
        try:
            data = json.loads(sb_path.read_text())
            # Extract team abbreviations from LineScore resultSet
            for rs in data.get("resultSets", []):
                if rs.get("name") == "LineScore":
                    headers = rs.get("headers", [])
                    rows = rs.get("rowSet", [])
                    abbr_idx = headers.index("TEAM_ABBREVIATION") if "TEAM_ABBREVIATION" in headers else None
                    gid_idx = headers.index("GAME_ID") if "GAME_ID" in headers else None
                    if abbr_idx is None or gid_idx is None:
                        break
                    # Group by game_id: first row = away, second = home
                    game_teams: Dict[str, List[str]] = {}
                    for row in rows:
                        gid = row[gid_idx]
                        team = _canon_team(row[abbr_idx])
                        game_teams.setdefault(gid, []).append(team)
                    for gid, teams in game_teams.items():
                        if len(teams) == 2:
                            matchups.add((teams[0], teams[1]))
                    break
        except (json.JSONDecodeError, KeyError, ValueError):
            pass

    # Return None if no cache files exist (can't validate)
    # Return empty set if cache exists but 0 NBA games (e.g. All-Star break)
    return matchups if has_cache else None


def filter_wrong_date(
    signals: List[Dict[str, Any]], date_str: str, sport: str = "NBA"
) -> Tuple[List[Dict[str, Any]], int]:
    """Drop signals whose matchup didn't actually occur on the given date.

    Uses LGF/scoreboard cache to build the actual game schedule, then filters
    out signals with event_keys pointing to games that happened on a different day.
    Returns (kept_signals, dropped_count).
    """
    schedule = load_game_schedule(date_str, sport=sport)
    if schedule is None:
        # No cache files exist — can't validate, keep everything
        return signals, 0
    if not schedule:
        # Cache exists but 0 NBA games (All-Star break, off day) — drop all
        return [], len(signals)

    # Build a set of matchups for fast lookup (both directions)
    valid_matchups: Set[Tuple[str, str]] = set()
    for away, home in schedule:
        valid_matchups.add((away, home))
        valid_matchups.add((home, away))  # accept either direction

    kept = []
    dropped = 0
    for s in signals:
        event_key = s.get("event_key") or ""
        # Match TEAM@TEAM at end of string, or TEAM@TEAM followed by :HHMM suffix
        # (e.g. NBA:20260311:MIN@LAL:0300 has a trailing time component)
        match = re.search(r'([A-Z]{2,4})@([A-Z]{2,4})(?::\d+)?$', event_key)
        if not match:
            # No TEAM@TEAM in event_key. Two cases:
            # 1. event_key is None/empty — genuinely missing, keep (can't validate)
            # 2. event_key is a date-only key like "NBA:2026:03:19" — this happens when a
            #    source (e.g. JuiceReel) picks a player prop without game context. Could be
            #    a non-NBA sport (NCAAB) misclassified as NBA. Require ≥2 sources as a
            #    corroboration guard: if another NBA source (OddsTrader, BetQL, etc.) also
            #    has the same player, it's almost certainly a real NBA player.
            if event_key and s.get("market_type") == "player_prop":
                n_src = len(s.get("sources_present") or s.get("sources") or [])
                if n_src < 2:
                    dropped += 1
                    continue
            kept.append(s)
            continue

        away = _canon_team(match.group(1))
        home = _canon_team(match.group(2))
        if (away, home) in valid_matchups:
            kept.append(s)
        else:
            dropped += 1

    return kept, dropped


def filter_scorable(
    signals: List[Dict[str, Any]],
    pattern_registry: Optional[List[Dict[str, Any]]] = None,
    excluded_combos: Optional[set] = None,
) -> List[Dict[str, Any]]:
    """Exclude single-source, avoid_conflict, and other non-actionable signals.

    Consensus picks require at least 2 agreeing sources — single-source picks
    are just regurgitating one source's opinion.

    Exception: single-source signals that match an A-tier PATTERN_REGISTRY entry
    via exact_combo are allowed through (e.g. nukethebooks_solo_props).
    """
    _registry = pattern_registry if pattern_registry is not None else PATTERN_REGISTRY
    _excluded = excluded_combos if excluded_combos is not None else EXCLUDED_COMBOS_BY_MARKET

    # Build sets of exact_combo values from registry for fast solo checks.
    # A-tier unconditional: patterns with no team/direction constraint (e.g. action UNDER props)
    # All other solo patterns (A or B tier with team/direction constraints) go into the
    # constrained list and are checked against the signal's market, direction, and team.
    solo_a_tier_unconditional = {
        p["exact_combo"]
        for p in _registry
        if p.get("exact_combo") and p.get("tier_eligible") == "A" and not p.get("team")
    }
    # Constrained solo patterns: require market, direction, and/or team to match.
    # Includes: A-tier with team constraint (betql OKC), all B-tier exact_combo patterns.
    solo_constrained_patterns = [
        p for p in _registry
        if p.get("exact_combo") and (p.get("tier_eligible") == "B" or p.get("team"))
    ]
    # Expert patterns: solo picks from a known A-tier expert are allowed through.
    solo_expert_names = {
        p["expert"]
        for p in _registry
        if p.get("expert") and p.get("tier_eligible") == "A"
    }

    def _is_scorable(s: Dict[str, Any]) -> bool:
        if s.get("signal_type") in EXCLUDED_SIGNAL_TYPES:
            return False
        mkt = s.get("market_type")
        if mkt not in ("player_prop", "spread", "total", "moneyline"):
            return False
        # Exclude known-bad combo×market combinations
        combo = s.get("sources_combo", "")
        if (combo, mkt) in _excluded:
            return False
        n_sources = len(s.get("sources_present") or s.get("sources") or [])
        if n_sources >= 2:
            return True
        # Allow single-source A-tier sources with no constraints (e.g. action, nukethebooks)
        if combo in solo_a_tier_unconditional:
            return True
        # Allow single-source picks from A-tier experts (e.g. Mike Barner, Larry Hartstein)
        sig_experts = s.get("experts") or []
        if any(e in solo_expert_names for e in sig_experts):
            return True
        # Allow single-source constrained patterns only when signal matches all constraints
        # (e.g. betql OKC spread only, betql UNDER props only)
        direction = _normalize_direction(s)
        sel = (s.get("selection") or "").upper().strip()
        for pat in solo_constrained_patterns:
            if pat.get("exact_combo") != combo:
                continue
            if pat.get("market_type") and pat["market_type"] != mkt:
                continue
            if pat.get("direction") and pat["direction"] != direction:
                continue
            if pat.get("team") and pat["team"].upper() != sel:
                continue
            return True
        return False

    filtered = [s for s in signals if _is_scorable(s)]
    return _dedup_per_game(filtered)


def _dedup_per_game(signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Keep only the best signal per (team/player, event, market_type).

    For spreads/totals/moneylines, multiple signals can exist for the same
    team in the same game with different lines. Keep the one with the most
    agreeing sources; break ties by preferring the majority line.

    For spreads, also normalise the line sign: if the selected team is
    the away team the line should normally be positive (getting points),
    and if the selected team is the home favourite the line is negative.
    When conflicting signs exist, prefer the line that appears most often
    across all occurrences.
    """
    from collections import defaultdict

    # Group signals by normalized (selection, event, market)
    groups: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)
    for s in signals:
        key = _normalize_dedup_key(s)
        groups[key].append(s)

    result = []
    for key, sigs in groups.items():
        if len(sigs) == 1:
            result.append(sigs[0])
            continue

        # Pick the representative: most sources, then most common line
        # Count how often each line value appears
        line_counts: Dict[float, int] = defaultdict(int)
        for s in sigs:
            ln = s.get("line")
            if ln is not None:
                line_counts[ln] += 1

        # Find the majority line (most common across all occurrences)
        majority_line = max(line_counts, key=line_counts.get) if line_counts else None

        # Score each signal: (sources_count, line_matches_majority, score)
        def sort_key(s: Dict) -> tuple:
            sc = len(s.get("sources_present") or s.get("sources") or [])
            ln_match = 1 if s.get("line") == majority_line else 0
            return (sc, ln_match, s.get("score", 0))

        best = max(sigs, key=sort_key)

        # If the best signal's line doesn't match majority, override it
        if majority_line is not None and best.get("line") != majority_line:
            best = dict(best)  # copy to avoid mutating original
            best["line"] = majority_line

        # Merge sources_present from all duplicate signals
        if len(sigs) > 1:
            all_sources: set = set()
            for sig in sigs:
                all_sources.update(sig.get("sources_present") or sig.get("sources") or [])
            merged_sources = sorted(all_sources)
            if merged_sources != (best.get("sources_present") or []):
                best = dict(best)
                best["sources_present"] = merged_sources
                # Update combo string to reflect merged sources
                best["sources_combo"] = "|".join(merged_sources)

        result.append(best)

    return result


# ── Exclusion filters ─────────────────────────────────────────────────────

def apply_exclusion_filters(
    signals: List[Dict[str, Any]],
    tables: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Hard-remove signals with known losing patterns.

    Returns (passed, excluded) where each excluded item has an _exclusion_reason.
    """
    passed = []
    excluded = []
    for s in signals:
        reason = _check_exclusion(s, tables)
        if reason:
            excluded.append({**s, "_exclusion_reason": reason})
        else:
            passed.append(s)
    return passed, excluded


def _check_exclusion(signal: Dict[str, Any], tables: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """Check if a signal should be hard-excluded. Returns reason string or None."""
    market = signal.get("market_type", "")
    stat = signal.get("atomic_stat")
    line = signal.get("line")

    # Player availability: skip props for ruled-out players
    if RULED_OUT_PLAYERS and market == "player_prop":
        # selection format: "NBA:player_key::stat::DIRECTION"
        sel = signal.get("selection") or ""
        player_key = sel.split("::")[0].split(":")[-1] if "::" in sel else ""
        # day_key format: "NBA:2026:03:11" → "2026-03-11"
        dk = signal.get("day_key") or ""
        parts = dk.split(":")
        signal_date = f"{parts[1]}-{parts[2]}-{parts[3]}" if len(parts) == 4 else (signal.get("derived_date") or "")[:10]
        if player_key and (player_key, signal_date) in RULED_OUT_PLAYERS:
            return f"player_ruled_out:{player_key}"

    # Stat type blacklist
    if stat and stat in EXCLUDED_STAT_TYPES:
        return f"excluded_stat:{stat}"

    # Direction-aware stat exclusion: some combo stats are bad in one direction only
    direction = (signal.get("direction") or "").upper()
    if stat and (stat, direction) in EXCLUDED_STAT_DIR:
        return f"excluded_stat_dir:{stat}_{direction}"

    # OVER player props — no longer hard-excluded; Wilson gate + combo×market
    # lookup tables handle filtering. Original 43.8% stat was from flawed
    # grade-matching that didn't properly handle direction joins.
    # Actual signal-level OVER props: 53.3% (98-86, n=184).

    # Spread line sanity — catch data quality issues
    if market == "spread" and line is not None:
        try:
            absline = abs(float(line))
            if absline == 0:
                return "zero_spread"
            if absline > 25:
                return f"unrealistic_spread:{line}"
        except (TypeError, ValueError):
            pass

    # Total line sanity — NBA game totals are ~200-250
    if market == "total" and line is not None:
        try:
            lv = float(line)
            if lv < 150 or lv > 280:
                return f"unrealistic_total:{line}"
        except (TypeError, ValueError):
            pass

    # Player prop selection validation
    if market == "player_prop":
        sel = signal.get("selection") or ""
        # Must have canonical format SPORT:<slug>::<stat>::OVER|UNDER
        m = re.match(r"^[A-Z]+:([a-z0-9_]+)::[a-z_]+::(OVER|UNDER)$", sel)
        if not m:
            return f"invalid_prop_selection:{sel[:40]}"
        slug = m.group(1)
        # 1. Strip live_ prefix — real player prop, keep it
        if slug.startswith("live_"):
            slug = slug[5:]
        # 2. Drop _projection / _total_projection suffix (covers artifacts)
        if slug.endswith("_projection"):
            return f"invalid_prop_selection:{sel[:40]}"
        # 3. Drop covers scraper prefix garbage (cs_, sc_, jt_)
        if _COVERS_GARBAGE_PREFIX.match(slug):
            return f"invalid_prop_selection:{sel[:40]}"
        # 4. Drop team name slugs and home/away total variants
        if slug in TEAM_SLUGS:
            return f"invalid_prop_selection:{sel[:40]}"

    # Hard-exclude proven losing combo × market (Wilson < 0.42 with n >= 30)
    # Try direction-aware table first, then fall back to direction-agnostic
    if tables:
        combo = signal.get("sources_combo", "")
        raw_dir = (signal.get("direction") or "").upper()
        if "OVER" in raw_dir:
            sig_dir = "OVER"
        elif "UNDER" in raw_dir:
            sig_dir = "UNDER"
        else:
            sig_dir = raw_dir or "unknown"

        # Prefer direction-aware entry when available with enough data
        entry = tables.get("combo_market_direction", {}).get((combo, market, sig_dir))
        if not entry or entry.get("n", 0) < 30:
            entry = tables.get("combo_market_signals", {}).get((combo, market))
        if entry and entry.get("n", 0) >= 30:
            wl = entry.get("wilson_lower")
            if wl is None:
                wl = wilson_lower(entry.get("wins", 0), entry["n"])
            if wl < WILSON_HARD_EXCLUDE:
                return f"losing_combo_market:wilson={wl:.3f}"

    # Exclude signals where ALL named experts are proven losers (<45% wp, n>=30)
    if tables:
        experts_table = tables.get("experts", {})
        signal_experts = signal.get("experts") or []
        if experts_table and signal_experts:
            named = [strip_expert_prefix(e) for e in signal_experts
                     if e not in ("unknown", "MULTI", "")]
            qualified = [e for e in named
                         if experts_table.get(e, {}).get("n", 0) >= EXPERT_MIN_N]
            if qualified:
                all_losers = all(
                    experts_table.get(e, {}).get("win_pct", 0.5) < 0.45
                    for e in qualified
                )
                if all_losers:
                    return f"all_losing_experts:{','.join(qualified[:3])}"

    return None


# ── Primary scoring ───────────────────────────────────────────────────────

def _build_primary_result(entry: Dict, level: str, label: str) -> Dict[str, Any]:
    """Build a primary lookup result dict."""
    n = entry.get("n", 0)
    wins = entry.get("wins", 0)
    wp = entry.get("win_pct", 0)
    wl = entry.get("wilson_lower")
    if wl is None:
        wl = wilson_lower(wins, n) if n > 0 else 0.0
    return {
        "wilson_lower": round(wl, 4),
        "win_pct": round(wp, 4),
        "n": n,
        "wins": wins,
        "losses": n - wins,
        "lookup_level": level,
        "label": label,
    }


def _cascading_primary_lookup(
    signal: Dict[str, Any],
    tables: Dict[str, Any],
    min_n: int,
) -> Dict[str, Any]:
    """Find the best matching historical record via cascading fallback.

    Cascade (most specific → least specific):
      1.  combo × market × stat       (player props only)
      2.  combo × market × direction   (direction-aware — preferred for totals)
      3.  combo × market               (signals-based — fallback)
      4.  combo overall
      5.  consensus × market × stat   (player props only — retains stat specificity)
      6.  consensus × market
      7.  consensus overall
    """
    combo = signal.get("sources_combo", "")
    market = signal.get("market_type", "")
    atomic_stat = signal.get("atomic_stat")
    stat_key = atomic_stat if market == "player_prop" and atomic_stat else "all"
    sources_count = len(signal.get("sources_present") or signal.get("sources") or [])
    strength = f"{sources_count}_source" if sources_count < 4 else "4+_sources"

    # Normalize direction for lookup
    raw_dir = (signal.get("direction") or "").upper()
    if "OVER" in raw_dir:
        direction = "OVER"
    elif "UNDER" in raw_dir:
        direction = "UNDER"
    else:
        direction = raw_dir or "unknown"

    # Level 1: combo × market × stat (player props only)
    if stat_key != "all":
        entry = tables.get("combo_market_stat", {}).get((combo, market, stat_key))
        if entry and entry.get("n", 0) >= min_n:
            return _build_primary_result(entry, "combo_market_stat",
                                         f"{combo} / {market} / {stat_key}")

    # Level 2: combo × market × direction (direction-aware — preferred)
    entry = tables.get("combo_market_direction", {}).get((combo, market, direction))
    if entry and entry.get("n", 0) >= min_n:
        return _build_primary_result(entry, "combo_market_direction",
                                     f"{combo} / {market} / {direction}")

    # Level 3: combo × market (signals-based — fallback when direction table missing or n too low)
    entry = tables.get("combo_market_signals", {}).get((combo, market))
    if entry and entry.get("n", 0) >= min_n:
        return _build_primary_result(entry, "combo_market",
                                     f"{combo} / {market}")

    # Level 4: combo overall
    entry = tables.get("combo_overall", {}).get(combo)
    if entry and entry.get("n", 0) >= min_n:
        return _build_primary_result(entry, "combo_overall",
                                     f"{combo} overall")

    # Level 5: consensus × market × stat (player props — retains stat specificity)
    if stat_key != "all":
        entry = tables.get("consensus_market_stat", {}).get((strength, market, stat_key))
        if entry and entry.get("n", 0) >= min_n:
            return _build_primary_result(entry, "consensus_market_stat",
                                         f"{strength} / {market} / {stat_key}")

    # Level 6: consensus × market
    entry = tables.get("consensus_market", {}).get((strength, market))
    if entry and entry.get("n", 0) >= min_n:
        return _build_primary_result(entry, "consensus_market",
                                     f"{strength} / {market}")

    # Level 7: consensus overall
    entry = tables.get("consensus", {}).get(strength)
    if entry and entry.get("n", 0) >= min_n:
        return _build_primary_result(entry, "consensus_overall", strength)

    return {"wilson_lower": 0.0, "win_pct": 0.0, "n": 0, "wins": 0,
            "losses": 0, "lookup_level": "none", "label": "n/a"}


def _compute_recency_adjustment(trend: Optional[Dict[str, Any]]) -> float:
    """Compute additive recency modifier from recent trend data."""
    if not trend:
        return 0.0
    wp = trend.get("win_pct", 0.5)
    if wp >= 0.60:
        return TREND_BOOST_HOT
    if wp >= 0.55:
        return TREND_BOOST_WARM
    if wp <= 0.40:
        return TREND_PENALTY_COLD
    if wp <= 0.45:
        return TREND_PENALTY_COOL
    return 0.0


# ── Pattern-based A-tier matching ─────────────────────────────────────────

def _match_patterns(
    signal: Dict[str, Any],
    registry: List[Dict[str, Any]] = PATTERN_REGISTRY,
) -> Optional[Dict[str, Any]]:
    """Check signal against pattern registry. Return first matching pattern or None.

    Patterns are checked in order (most specific first). A signal matches if
    ALL non-None criteria in the pattern are satisfied.

    Source matching modes:
      - source_pair: signal must contain BOTH sources
      - source_contains: signal must contain ALL listed sources
      - exact_combo: signal's sources_combo must match exactly
    """
    sources = set(signal.get("sources_present") or signal.get("sources") or [])
    combo = signal.get("sources_combo", "")
    market = signal.get("market_type", "")
    direction = _normalize_direction(signal)
    stat = signal.get("atomic_stat")
    # For team-specific patterns (spreads/MLs): selection holds the picked team abbrev
    selection = (signal.get("selection") or "").upper().strip()
    n_sources = len(sources)

    for pat in registry:
        # Check min_sources
        if pat.get("min_sources") and n_sources < pat["min_sources"]:
            continue

        # Source matching (three modes)
        if pat.get("source_pair"):
            pair = pat["source_pair"]
            if pair[0] not in sources or pair[1] not in sources:
                continue
        elif pat.get("source_contains"):
            if not all(s in sources for s in pat["source_contains"]):
                continue
        elif pat.get("exact_combo"):
            if combo != pat["exact_combo"]:
                continue

        # Market type filter
        if pat.get("market_type") and pat["market_type"] != market:
            continue

        # Direction filter (OVER/UNDER for props and totals)
        if pat.get("direction") and pat["direction"] != direction:
            continue

        # Team filter: for spread/ML patterns that apply only to specific teams.
        # Matches against the signal's selection field (the picked team abbreviation).
        if pat.get("team") and pat["team"].upper() != selection:
            continue

        # Stat type filter (atomic_stat for player props)
        if pat.get("stat_type") and pat["stat_type"] != stat:
            continue

        # Agreement filter: all listed experts must be present on the signal
        if pat.get("experts_all"):
            signal_experts = set(signal.get("experts") or [])
            signal_experts_stripped = {strip_expert_prefix(e) for e in signal_experts}
            required_experts = set(pat["experts_all"])
            if not all(e in signal_experts or e in signal_experts_stripped for e in required_experts):
                continue

        # Expert filter: at least one of the named experts must be on the signal
        if pat.get("expert"):
            signal_experts = set(signal.get("experts") or [])
            # Strip source prefix (e.g. "action:scott_rickenbach" → "scott_rickenbach")
            signal_experts_stripped = {strip_expert_prefix(e) for e in signal_experts}
            pat_experts = pat["expert"] if isinstance(pat["expert"], list) else [pat["expert"]]
            if not any(e in signal_experts or e in signal_experts_stripped for e in pat_experts):
                continue

        return pat  # first match wins

    return None


# ── Multi-factor scoring modifiers ────────────────────────────────────────

def _compute_expert_adjustment(
    signal: Dict[str, Any],
    tables: Dict[str, Any],
) -> Tuple[float, Optional[Dict[str, Any]]]:
    """Compute expert quality modifier.

    Boost if best expert >55% wp (n>=30); penalize if worst <45%.
    """
    experts_table = tables.get("experts", {})
    signal_experts = signal.get("experts") or []
    if not signal_experts or not experts_table:
        return 0.0, None

    best_wp = 0.0
    worst_wp = 1.0
    best_expert: Optional[Dict[str, Any]] = None
    worst_expert: Optional[Dict[str, Any]] = None

    for exp_raw in signal_experts:
        exp_key = strip_expert_prefix(exp_raw)
        entry = experts_table.get(exp_key)
        if not entry or entry.get("n", 0) < EXPERT_MIN_N:
            continue
        wp = entry.get("win_pct", 0.5)
        if wp > best_wp:
            best_wp = wp
            best_expert = {"expert": exp_key, "win_pct": round(wp, 4), "n": entry["n"]}
        if wp < worst_wp:
            worst_wp = wp
            worst_expert = {"expert": exp_key, "win_pct": round(wp, 4), "n": entry["n"]}

    if best_expert and best_wp >= 0.55:
        return EXPERT_BOOST, {**best_expert, "adjustment": EXPERT_BOOST, "role": "boost"}
    if worst_expert and worst_wp < 0.45:
        return EXPERT_PENALTY, {**worst_expert, "adjustment": EXPERT_PENALTY, "role": "penalty"}
    return 0.0, None


def _compute_stat_type_adjustment(
    signal: Dict[str, Any],
    tables: Dict[str, Any],
) -> float:
    """Small modifier based on stat type historical performance."""
    if signal.get("market_type") != "player_prop":
        return 0.0
    stat = signal.get("atomic_stat")
    if not stat:
        return 0.0
    entry = tables.get("stat_type", {}).get(stat)
    if not entry or entry.get("n", 0) < 30:
        return 0.0
    wp = entry.get("win_pct", 0.5)
    if wp >= 0.52:
        return STAT_TYPE_BOOST
    if wp < 0.47:
        return STAT_TYPE_PENALTY
    return 0.0


def _compute_day_of_week_adjustment(day_of_week: str) -> float:
    """Day-of-week adjustment removed — not a reliable signal."""
    return 0.0


def _compute_line_bucket_adjustment(
    signal: Dict[str, Any],
    tables: Dict[str, Any],
) -> float:
    """Small modifier based on line bucket historical performance."""
    market = signal.get("market_type", "")
    sig_line = signal.get("line")
    lb = line_bucket(sig_line, market)
    entry = tables.get("line_bucket", {}).get((market, lb))
    if not entry or entry.get("n", 0) < 30:
        return 0.0
    wp = entry.get("win_pct", 0.5)
    if wp >= 0.51:
        return LINE_BUCKET_BOOST
    if wp < 0.48:
        return LINE_BUCKET_PENALTY
    return 0.0


def _build_supporting_factors(
    signal: Dict[str, Any],
    tables: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Build supplementary factors for display (not used in ranking)."""
    market = signal.get("market_type", "")
    sources_combo = signal.get("sources_combo", "")
    sources_present = signal.get("sources_present") or signal.get("sources") or []
    atomic_stat = signal.get("atomic_stat")
    stat_key = atomic_stat if market == "player_prop" and atomic_stat else "all"
    sig_line = signal.get("line")
    cs_key = consensus_strength_key(sources_present)

    factors = []

    # Consensus × market
    entry = tables.get("consensus_market", {}).get((cs_key, market))
    if entry and entry.get("n", 0) >= MIN_SAMPLE_PRIMARY:
        factors.append({
            "dimension": "consensus_market",
            "lookup_key": f"{cs_key} / {market}",
            "win_pct": round(entry.get("win_pct", 0), 4),
            "n": entry["n"],
            "wilson_lower": round(entry.get("wilson_lower", 0), 4),
        })

    # Stat type (for props)
    if stat_key != "all":
        entry = tables.get("stat_type", {}).get(stat_key)
        if entry and entry.get("n", 0) >= MIN_SAMPLE_PRIMARY:
            factors.append({
                "dimension": "stat_type",
                "lookup_key": stat_key,
                "win_pct": round(entry.get("win_pct", 0), 4),
                "n": entry["n"],
                "wilson_lower": round(entry.get("wilson_lower", 0), 4),
            })

    # Line bucket
    lb = line_bucket(sig_line, market)
    entry = tables.get("line_bucket", {}).get((market, lb))
    if entry and entry.get("n", 0) >= MIN_SAMPLE_PRIMARY:
        factors.append({
            "dimension": "line_bucket",
            "lookup_key": f"{market} / {lb}",
            "win_pct": round(entry.get("win_pct", 0), 4),
            "n": entry["n"],
            "wilson_lower": round(entry.get("wilson_lower", 0), 4),
        })

    return factors


# ── Composite scoring constants ────────────────────────────────────────────
MIN_SOLO_SOURCE = 15       # min n for solo source evidence in composite
MIN_SOLO_EXPERT = 10       # min n for solo expert evidence in composite
MIN_CONSENSUS_N = 20       # min n for N-source consensus evidence in composite
CONSENSUS_WEIGHT_MULT = 1.2  # combo/primary record multiplier (most specific)
INDEP_BLEND = 0.30         # weight of independent evidence model when primary is thin

# Expert vote consensus boost — accelerating returns, qualified experts only.
# Only experts with overall wilson >= threshold AND n >= MIN_SOLO_EXPERT contribute.
# Boost table (qualified votes → proportional multiplier added to composite):
#   1: 0%,  2: +5%,  3: +11%,  4: +18%,  5+: +25%
EXPERT_QUALIFIED_WILSON = 0.46   # minimum wilson for an expert to count as "qualified"
EXPERT_VOTE_BOOST_TABLE = {1: 0.0, 2: 0.05, 3: 0.11, 4: 0.18, 5: 0.25}
EXPERT_VOTE_BOOST_MAX = 0.25     # cap at 5+ qualified experts

# Dissent penalty — asymmetric, soft floor.
# multiplier = DISSENT_FLOOR + (1 - DISSENT_FLOOR) * (net_ratio ** DISSENT_EXPONENT)
# floor=0.55, exponent=0.7 produces:
#   even (1v1): ~0.777x  |  2v1: ~0.870x  |  4v1: ~0.936x
#   minority (1v2): ~0.700x  |  (1v3): ~0.650x
EXPERT_DISSENT_FLOOR = 0.55
EXPERT_DISSENT_EXPONENT = 0.7

# ── Conflict resolution constants ─────────────────────────────────────────
DISSENT_MIN_WILSON = 0.50  # dissenter solo wilson threshold to be taken seriously
DISSENT_MIN_VOTES = 2      # OR dissenter expert vote count threshold
OVERRIDE_VOTE_RATIO = 4.0  # vote ratio at which winner overrides credible dissent
OVERRIDE_WILSON_GAP = 0.08 # wilson gap at which winner overrides credible dissent


def _compute_composite_wilson(
    signal: Dict[str, Any],
    primary: Dict[str, Any],
    tables: Dict[str, Any],
    n_experts_against: int = 0,
) -> float:
    """Combine multiple evidence sources into a single composite Wilson score.

    Weighted average of:
      1. Primary combo record (weight=n, multiplier=1.2 — most specific)
      2. Per-source solo records by market_type (weight=n, multiplier=1.0)
      3. Per-expert solo records by market_type (weight=n, multiplier=1.0)
      4. N-source consensus record by market_type (weight=n, multiplier=0.8)

    Expert vote boost: each additional expert agreeing beyond the first adds
    EXPERT_VOTE_BOOST_PER to the composite, capped at EXPERT_VOTE_BOOST_MAX.

    Dissent penalty (n_experts_against > 0): score is scaled by
    EXPERT_DISSENT_FLOOR + (1-EXPERT_DISSENT_FLOOR) * for/(for+against).
    A 50/50 split → 0.75x; 4v1 → 0.9x; unanimous → 1.0x.

    When primary is thin (lookup_level not combo-specific), also applies
    the independent evidence model and blends it in at INDEP_BLEND weight.
    Falls back to primary["wilson_lower"] if no additional evidence found.
    """
    mkt = signal.get("market_type", "")
    primary_wilson = primary.get("wilson_lower", 0.0)
    primary_n = primary.get("n", 0)
    primary_level = primary.get("lookup_level", "none")

    # Evidence list: (wilson, n, multiplier)
    evidence = []

    # 1. Primary combo record
    if primary_n > 0 and primary_wilson > 0:
        evidence.append((primary_wilson, primary_n, CONSENSUS_WEIGHT_MULT))

    # 2. Per-source solo records
    source_market_table = tables.get("source_market", {})
    solo_source_wilsons = []
    for src in (signal.get("sources_present") or []):
        key = f"{src}|{mkt}"
        row = source_market_table.get(key)
        if row and row.get("n", 0) >= MIN_SOLO_SOURCE:
            w = row.get("wilson_lower", 0.0)
            n = row["n"]
            if w > 0:
                evidence.append((w, n, 1.0))
                if n >= 15:
                    solo_source_wilsons.append(w)

    # 3. Per-expert solo records
    # Collect expert names from supports array AND top-level experts list
    expert_market_table = tables.get("expert_market", {})
    experts_overall_table = tables.get("experts", {})
    seen_experts: set = set()
    expert_names: list = []
    n_qualified_experts: int = 0  # experts meeting wilson threshold for consensus boost
    for support in (signal.get("supports") or []):
        exp = (support.get("expert_name") or support.get("expert_slug") or "").strip()
        if exp and exp not in ("unknown", "MULTI") and exp not in seen_experts:
            seen_experts.add(exp)
            expert_names.append(exp)
    for exp in (signal.get("experts") or []):
        exp = exp.strip() if isinstance(exp, str) else ""
        if exp and exp not in ("unknown", "MULTI") and exp not in seen_experts:
            seen_experts.add(exp)
            expert_names.append(exp)
    for exp in expert_names:
        # Try per-market record first
        key = f"{exp}|{mkt}"
        row = expert_market_table.get(key)
        if row and row.get("n", 0) >= MIN_SOLO_EXPERT:
            w = row.get("wilson_lower", 0.0)
            n = row["n"]
            if w > 0:
                evidence.append((w, n, 1.0))
                if w >= EXPERT_QUALIFIED_WILSON:
                    n_qualified_experts += 1
        else:
            # Fall back to overall expert record
            overall_row = experts_overall_table.get(exp)
            if overall_row and overall_row.get("n", 0) >= MIN_SOLO_EXPERT:
                w = overall_row.get("wilson_lower", 0.0)
                n = overall_row["n"]
                if w > 0:
                    evidence.append((w, n, 0.8))  # slightly lower weight — less specific
                    if w >= EXPERT_QUALIFIED_WILSON:
                        n_qualified_experts += 1

    # 4. N-source consensus record
    n_sources = len(signal.get("sources_present") or [])
    if n_sources >= 2:
        consensus_key = f"{n_sources}_source" if n_sources <= 4 else "4+_sources"
        consensus_market = tables.get("consensus_market", {})
        c_row = consensus_market.get((consensus_key, mkt)) or consensus_market.get((consensus_key, ""))
        if c_row and c_row.get("n", 0) >= MIN_CONSENSUS_N:
            w = c_row.get("wilson_lower", 0.0)
            n = c_row["n"]
            if w > 0:
                evidence.append((w, n, 0.8))

    # Need evidence to blend. If primary has no history (n=0), allow even 1 solo
    # record to drive the score. Otherwise require 2+ to avoid over-indexing on
    # a single weak data point when a real combo record exists.
    if len(evidence) == 0:
        return primary_wilson, 0
    if len(evidence) == 1 and primary_n > 0:
        return primary_wilson, 0

    # Weighted average
    total_weight = sum(n * m for (_, n, m) in evidence)
    if total_weight == 0:
        return primary_wilson, 0
    weighted_avg = sum(w * n * m for (w, n, m) in evidence) / total_weight

    # Independent evidence boost when primary lookup is thin AND primary is winning.
    # Gated on primary win_pct >= 0.50: if history already says this combo loses,
    # the independent model has nothing to add — don't let source solo records
    # override a known losing pattern.
    thin_primary = primary_level in ("none", "consensus", "consensus_market",
                                     "consensus_market_stat", "consensus_overall")
    primary_winning = primary.get("win_pct", 0.0) >= 0.50
    if thin_primary and primary_winning and len(solo_source_wilsons) >= 2:
        p_all_wrong = 1.0
        for sw in solo_source_wilsons:
            p_all_wrong *= max(0.0, 1.0 - sw)
        independent_wilson = 1.0 - p_all_wrong
        composite = (1.0 - INDEP_BLEND) * weighted_avg + INDEP_BLEND * independent_wilson
    else:
        composite = weighted_avg

    # Expert vote consensus boost — accelerating returns, qualified experts only.
    # Qualified = wilson >= EXPERT_QUALIFIED_WILSON AND n >= MIN_SOLO_EXPERT.
    # Table: 1→0%, 2→+5%, 3→+11%, 4→+18%, 5+→+25% (proportional to composite).
    if n_qualified_experts >= 2:
        boost = EXPERT_VOTE_BOOST_TABLE.get(
            min(n_qualified_experts, max(EXPERT_VOTE_BOOST_TABLE.keys())),
            EXPERT_VOTE_BOOST_MAX,
        )
        composite = composite + boost * composite

    # Dissent penalty: scale down when credible experts are on the opposing side.
    # multiplier = FLOOR + (1-FLOOR) * (net_ratio ** EXPONENT)
    # Produces asymmetric curve: majority side recovers faster than minority is hurt.
    n_experts_for = len(expert_names)
    if n_experts_against > 0 and n_experts_for > 0:
        total_votes = n_experts_for + n_experts_against
        net_ratio = n_experts_for / total_votes
        dissent_multiplier = (
            EXPERT_DISSENT_FLOOR
            + (1.0 - EXPERT_DISSENT_FLOOR) * (net_ratio ** EXPERT_DISSENT_EXPONENT)
        )
        composite = composite * dissent_multiplier

    return round(composite, 4), n_qualified_experts


def _count_signal_experts(signal: Dict[str, Any]) -> int:
    """Count distinct named experts on a signal (supports + top-level experts list)."""
    seen: set = set()
    for support in (signal.get("supports") or []):
        exp = (support.get("expert_name") or support.get("expert_slug") or "").strip()
        if exp and exp not in ("unknown", "MULTI"):
            seen.add(exp)
    for exp in (signal.get("experts") or []):
        exp = exp.strip() if isinstance(exp, str) else ""
        if exp and exp not in ("unknown", "MULTI"):
            seen.add(exp)
    return len(seen)


def _count_qualified_experts(signal: Dict[str, Any], tables: Dict[str, Any]) -> int:
    """Count distinct experts on a signal who meet the qualified threshold."""
    mkt = signal.get("market_type", "")
    expert_market_table = tables.get("expert_market", {})
    experts_overall_table = tables.get("experts", {})
    seen: set = set()
    names: list = []
    for support in (signal.get("supports") or []):
        exp = (support.get("expert_name") or support.get("expert_slug") or "").strip()
        if exp and exp not in ("unknown", "MULTI") and exp not in seen:
            seen.add(exp); names.append(exp)
    for exp in (signal.get("experts") or []):
        exp = exp.strip() if isinstance(exp, str) else ""
        if exp and exp not in ("unknown", "MULTI") and exp not in seen:
            seen.add(exp); names.append(exp)
    count = 0
    for exp in names:
        row = expert_market_table.get(f"{exp}|{mkt}")
        if row and row.get("n", 0) >= MIN_SOLO_EXPERT and row.get("wilson_lower", 0) >= EXPERT_QUALIFIED_WILSON:
            count += 1
            continue
        overall = experts_overall_table.get(exp)
        if overall and overall.get("n", 0) >= MIN_SOLO_EXPERT and overall.get("wilson_lower", 0) >= EXPERT_QUALIFIED_WILSON:
            count += 1
    return count


def _apply_vote_adjustments(
    score_data: Dict[str, Any],
    signal: Dict[str, Any],
    n_experts_against: int,
    tables: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Re-apply expert vote boost and dissent penalty to an already-scored signal.

    Called from build_output after game grouping when we know how many experts
    are on the opposing side. Returns an updated score_data dict.
    """
    base = score_data.get("composite_wilson_base", score_data.get("wilson_score", 0.0))
    total_adj = score_data.get("total_adjustment", 0.0)

    n_experts_for = _count_signal_experts(signal)
    n_qualified = _count_qualified_experts(signal, tables or {}) if tables else 0

    # Strip prior boost from composite_wilson_base before reapplying,
    # to avoid double-counting the boost already applied during initial scoring.
    prior_n_qualified = score_data.get("n_qualified_experts", 0)
    prior_boost = EXPERT_VOTE_BOOST_TABLE.get(
        min(prior_n_qualified, max(EXPERT_VOTE_BOOST_TABLE.keys())), 0.0
    ) if prior_n_qualified >= 2 else 0.0
    raw_composite = base / (1.0 + prior_boost) if prior_boost > 0 else base

    # Reapply consensus boost with current qualified count
    new_composite = raw_composite
    if n_qualified >= 2:
        boost = EXPERT_VOTE_BOOST_TABLE.get(
            min(n_qualified, max(EXPERT_VOTE_BOOST_TABLE.keys())),
            EXPERT_VOTE_BOOST_MAX,
        )
        new_composite = new_composite + boost * new_composite

    # Apply dissent penalty using asymmetric curve
    if n_experts_against > 0 and n_experts_for > 0:
        total_votes = n_experts_for + n_experts_against
        net_ratio = n_experts_for / total_votes
        dissent_multiplier = (
            EXPERT_DISSENT_FLOOR
            + (1.0 - EXPERT_DISSENT_FLOOR) * (net_ratio ** EXPERT_DISSENT_EXPONENT)
        )
        new_composite = new_composite * dissent_multiplier

    new_composite = round(new_composite, 4)
    new_wilson = round(new_composite + total_adj, 4)

    updated = dict(score_data)
    updated["composite_wilson_base"] = new_composite
    updated["wilson_score"] = new_wilson
    updated["composite_score"] = new_wilson
    updated["confidence_score"] = wilson_to_confidence(new_wilson)
    updated["n_experts_for"] = n_experts_for
    updated["n_qualified_experts"] = n_qualified
    updated["n_experts_against"] = n_experts_against
    return updated


def _pick_team(sig: Dict[str, Any], game_key: str) -> Optional[str]:
    """Return the team this pick is betting ON for spread/moneyline signals."""
    mkt = sig.get("market_type", "")
    if mkt == "total":
        return None
    sel = sig.get("selection", "")
    return _canon_team(sel) if sel else None


def _resolve_side_conflict(
    side_idxs: List[int],
    passed_raw: List[tuple],
    tables: Dict[str, Any],
    game_key: tuple,
) -> tuple:
    """Resolve a spread/moneyline game conflict with three-way outcome.

    Returns:
        ("keep", best_idx)  — one side clearly wins
        ("avoid", None)     — no-bet zone: both sides have credible evidence
    """
    away, home = game_key

    # Group signals by team
    sides: Dict[str, List[tuple]] = {}
    for i in side_idxs:
        sig, score_data, tier = passed_raw[i]
        t = _pick_team(sig, f"{away}@{home}")
        if t:
            t = _canon_team(t)
        sides.setdefault(t or "unknown", []).append((i, sig, score_data, tier))

    # Remove "unknown" team entries — can't resolve without knowing team
    sides = {k: v for k, v in sides.items() if k != "unknown"}
    if len(sides) < 2:
        # No real conflict determinable
        best = min(side_idxs, key=lambda i: (
            {"A": 0, "B": 1, "C": 2, "D": 3}.get(passed_raw[i][2], 9),
            -passed_raw[i][1]["wilson_score"]
        ))
        return ("keep", best)

    mkt = passed_raw[side_idxs[0]][0].get("market_type", "spread")
    expert_market_table = tables.get("expert_market", {})

    def _side_score(entries):
        best_wilson = max(score_data["wilson_score"] for _, _, score_data, _ in entries)
        expert_votes = sum(
            (sig.get("expert_strength") or 0) for _, sig, _, _ in entries
        )
        expert_solo_wilsons = []
        for _, sig, _, _ in entries:
            for exp in (sig.get("expert_names") or sig.get("experts") or []):
                if not exp or exp in ("unknown", "MULTI"):
                    continue
                key = f"{exp}|{mkt}"
                row = expert_market_table.get(key)
                if row and row.get("n", 0) >= 10:
                    w = row.get("wilson_lower", 0.0)
                    if w > 0:
                        expert_solo_wilsons.append(w)
        avg_expert_solo = (sum(expert_solo_wilsons) / len(expert_solo_wilsons)
                           if expert_solo_wilsons else None)
        best_idx = max(
            [i for i, _, _, _ in entries],
            key=lambda i: passed_raw[i][1]["wilson_score"]
        )
        return {
            "wilson": best_wilson,
            "expert_votes": expert_votes,
            "avg_expert_solo": avg_expert_solo,
            "best_idx": best_idx,
        }

    team_list = list(sides.keys())
    scores = {t: _side_score(sides[t]) for t in team_list}

    # Ensure A = stronger side by Wilson
    A, B = team_list[0], team_list[1]
    if scores[B]["wilson"] > scores[A]["wilson"]:
        A, B = B, A
    sA, sB = scores[A], scores[B]

    wilson_gap = sA["wilson"] - sB["wilson"]
    ev_b = sB["expert_votes"]
    ev_a = sA["expert_votes"]
    vote_ratio = ev_a / ev_b if ev_b > 0 else float("inf")

    # Step 1: Is the dissent meaningful?
    dissent_solo = sB.get("avg_expert_solo") or 0
    dissent_has_weight = (
        dissent_solo >= DISSENT_MIN_WILSON
        or sB["expert_votes"] >= DISSENT_MIN_VOTES
        or sB["wilson"] >= 0.48
    )
    if not dissent_has_weight:
        print(f"  [conflict] {away}@{home}|{mkt}: {A} wins (dissent too weak: votes={ev_b} solo={dissent_solo:.3f})")
        return ("keep", sA["best_idx"])

    # Step 2: Is winner dominant enough to override?
    if vote_ratio >= OVERRIDE_VOTE_RATIO or wilson_gap >= OVERRIDE_WILSON_GAP:
        print(f"  [conflict] {away}@{home}|{mkt}: {A} wins (dominant: vote_ratio={vote_ratio:.1f} gap={wilson_gap:.3f})")
        return ("keep", sA["best_idx"])

    # Step 3: No-bet zone — both sides have credible evidence
    winner_solo = sA.get("avg_expert_solo") or sA["wilson"]
    if winner_solo < 0.48:
        print(f"  [conflict] {away}@{home}|{mkt}: AVOID (winner solo={winner_solo:.3f} not compelling, votes={ev_a}v{ev_b})")
        return ("avoid", None)
    if vote_ratio >= 2.0:
        print(f"  [conflict] {away}@{home}|{mkt}: {A} wins (vote edge {ev_a}v{ev_b}, winner_solo={winner_solo:.3f})")
        return ("keep", sA["best_idx"])
    print(f"  [conflict] {away}@{home}|{mkt}: AVOID (gap={wilson_gap:.3f}, votes={ev_a}v{ev_b}, both credible)")
    return ("avoid", None)


def score_signal(
    signal: Dict[str, Any],
    tables: Dict[str, Any],
    recent_tables: Optional[Dict[str, Any]] = None,
    day_of_week: str = "",
    pattern_registry: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Score a signal using Wilson-based combo×market ranking.

    Primary score = Wilson lower bound from the best matching historical
    record via cascading lookup. Multi-factor modifiers are applied:
    recency, expert, stat type, day-of-week, line bucket.
    """
    # 1. Cascading primary lookup
    primary = _cascading_primary_lookup(signal, tables, MIN_SAMPLE_PRIMARY)

    # 2. Recent trend modifier
    trend = _lookup_recent_trend(signal, recent_tables or {}, MIN_SAMPLE_TREND) if recent_tables else None
    recency_adj = _compute_recency_adjustment(trend)

    # 3. Multi-factor modifiers
    expert_adj, expert_detail = _compute_expert_adjustment(signal, tables)
    stat_adj = _compute_stat_type_adjustment(signal, tables)
    day_adj = _compute_day_of_week_adjustment(day_of_week)
    line_adj = _compute_line_bucket_adjustment(signal, tables)
    total_adjustment = recency_adj + expert_adj + stat_adj + day_adj + line_adj

    # 4. Final score
    # 5. Pattern registry matching (needed before final score to allow pattern-seeded Wilson)
    _registry = pattern_registry if pattern_registry is not None else PATTERN_REGISTRY
    matched_pattern = _match_patterns(signal, registry=_registry)
    a_tier_eligible = (matched_pattern is not None
                       and matched_pattern.get("tier_eligible") == "A")

    # Seed primary from pattern hist in two cases:
    #   1. No lookup table data at all (e.g. NCAAB early season)
    #   2. Expert pattern matched but raw combo Wilson is below B-tier floor —
    #      the expert's own historical record is the relevant baseline, not the
    #      generic combo table which aggregates all experts good and bad.
    _use_pattern_seed = False
    if matched_pattern:
        pat_hist = matched_pattern.get("hist", {})
        pat_n = pat_hist.get("n", 0)
        if pat_n >= MIN_SAMPLE_PRIMARY:
            if primary["lookup_level"] == "none":
                _use_pattern_seed = True
            elif (matched_pattern.get("id", "").startswith("expert_")
                  and primary["wilson_lower"] < WILSON_TIER_B):
                _use_pattern_seed = True
    if _use_pattern_seed:
        pat_hist = matched_pattern.get("hist", {})
        pat_n = pat_hist.get("n", 0)
        pat_wins = round(pat_hist.get("win_pct", 0) * pat_n)
        pat_wilson = wilson_lower(pat_wins, pat_n, z=1.645)
        primary = {
            "lookup_level": "pattern_hist",
            "label": matched_pattern.get("label", matched_pattern.get("id", "")),
            "win_pct": pat_hist.get("win_pct", 0),
            "n": pat_n,
            "wins": pat_wins,
            "losses": pat_n - pat_wins,
            "wilson_lower": pat_wilson,
        }

    composite_base, n_qualified_experts = _compute_composite_wilson(signal, primary, tables)
    wilson_score = composite_base + total_adjustment

    # 6. Confidence level
    n = primary["n"]
    confidence = "high" if n >= 50 else ("medium" if n >= 30 else "low")

    # 7. Supporting factors (for display only)
    factors = _build_supporting_factors(signal, tables)

    # 8. Human-readable summary
    if primary["lookup_level"] != "none":
        parts = primary["label"].split(" / ")
        if len(parts) >= 2:
            market_label = parts[-1] if len(parts) == 2 else f"{parts[-1]} {parts[-2]}"
        else:
            market_label = "picks"
        summary = (f"{primary['win_pct']*100:.1f}% win rate in "
                   f"{primary['n']} similar {market_label} picks "
                   f"(Wilson: {primary['wilson_lower']*100:.1f}%)")
        adj_parts = []
        if recency_adj != 0:
            direction = "hot" if recency_adj > 0 else "cold"
            adj_parts.append(f"{direction}: {recency_adj*100:+.0f}%")
        if expert_adj != 0:
            adj_parts.append(f"analyst: {expert_adj*100:+.1f}%")
        if stat_adj != 0:
            adj_parts.append(f"stat: {stat_adj*100:+.1f}%")
        if day_adj != 0:
            adj_parts.append(f"day: {day_adj*100:+.1f}%")
        if line_adj != 0:
            adj_parts.append(f"line: {line_adj*100:+.1f}%")
        if adj_parts:
            summary += f" [{' | '.join(adj_parts)}]"
    else:
        summary = "insufficient historical data"

    result: Dict[str, Any] = {
        "wilson_score": round(wilson_score, 4),
        "composite_score": round(wilson_score, 4),  # backward compat alias
        "composite_wilson_base": round(composite_base, 4),
        "n_qualified_experts": n_qualified_experts,
        "primary_record": primary,
        "recency_adjustment": recency_adj,
        "expert_adjustment": expert_adj,
        "stat_adjustment": stat_adj,
        "day_adjustment": day_adj,
        "line_bucket_adjustment": line_adj,
        "total_adjustment": round(total_adjustment, 4),
        "recent_trend": trend,
        "confidence": confidence,
        "confidence_score": wilson_to_confidence(wilson_score),
        "a_tier_eligible": a_tier_eligible,
        "positive_dimensions": sum(1 for f in factors if f.get("win_pct", 0) > 0.505),
        "negative_dimensions": sum(1 for f in factors if f.get("win_pct", 0) < 0.495),
        "total_dimensions": len(factors),
        "factors": factors,
        "supporting_factors": factors,
        "summary": summary,
    }
    if expert_detail:
        result["expert_detail"] = expert_detail
    if matched_pattern:
        result["matched_pattern"] = matched_pattern
    return result


# ── Composite score & tier ─────────────────────────────────────────────────

def assign_tier(
    wilson_score: float,
    n: int,
    lookup_level: str,
    a_tier_eligible: bool = False,
    has_pattern: bool = False,
    sources_count: int = 0,
    n_qualified_experts: int = 0,
) -> str:
    """Assign tier based on pattern matching + Wilson lower bound.

      A: Matched a profitable pattern in PATTERN_REGISTRY AND Wilson >= 0.46
         OR 4+ sources AND Wilson >= 0.62
         OR 5+ sources AND Wilson >= 0.58
      B: Positive edge — Wilson >= 0.46 AND (named pattern OR 2+ sources)
      C: Marginal — Wilson >= 0.45, or B-worthy Wilson but single-source no-pattern
      D: Insufficient data or losing pattern
    """
    if n < MIN_SAMPLE_PRIMARY or lookup_level == "none":
        return "D"
    if a_tier_eligible and wilson_score >= WILSON_TIER_B:
        return "A"
    # Dynamic A-tier: composite engine has enough independent evidence
    # 4+ sources with Wilson >= 0.62, or 5+ sources with Wilson >= 0.58
    if sources_count >= 4 and wilson_score >= 0.62:
        return "A"
    if sources_count >= 5 and wilson_score >= 0.58:
        return "A"
    if wilson_score >= WILSON_TIER_B:
        # Require named pattern OR 2+ sources for B-tier
        if has_pattern or sources_count >= 2:
            return "B"
        # Single-source no-pattern: demote to C (not exported)
        if wilson_score >= WILSON_TIER_C:
            return "C"
        return "D"
    if wilson_score >= WILSON_TIER_C:
        return "C"
    return "D"


def passes_quality_gate(
    signal: Dict[str, Any],
    score_data: Dict[str, Any],
) -> Tuple[bool, str]:
    """Check if a scored signal meets the quality gate.

    Simplified criteria:
      1. Enough historical data (n >= 15)
      2. Wilson score >= C-tier threshold (0.45)
      3. Not a proven losing pattern (n >= 30 with wp < 48%)
    """
    primary = score_data["primary_record"]
    n = primary["n"]
    wilson = score_data["wilson_score"]
    win_pct = primary["win_pct"]

    composite_base = score_data.get("composite_wilson_base", wilson)
    composite_has_evidence = composite_base > 0.01 and composite_base != primary.get("wilson_lower", 0.0)

    if n < MIN_SAMPLE_PRIMARY:
        # Allow through if composite scoring found real independent evidence
        if not composite_has_evidence:
            return False, f"insufficient_data:n={n}"

    if wilson < WILSON_TIER_C:
        return False, f"low_wilson:{wilson:.3f}"

    if n >= 30 and win_pct < 0.48:
        return False, f"losing_history:{win_pct:.3f}"

    return True, "passed"


# ── Output ─────────────────────────────────────────────────────────────────

def format_selection(signal: Dict[str, Any]) -> str:
    """Build a concise display string for a signal."""
    market = signal.get("market_type", "")
    selection = signal.get("selection") or "?"
    sig_line = signal.get("line")
    direction = signal.get("direction", "")

    if market == "player_prop":
        # "NBA:p_watson::points::UNDER" -> "p_watson pts U18.5"
        parts = selection.replace("NBA:", "").split("::")
        player = parts[0] if parts else "?"
        stat = parts[1] if len(parts) > 1 else "?"
        d = parts[2][0] if len(parts) > 2 else "?"  # O or U
        line_str = f" {sig_line}" if sig_line is not None else ""
        return f"{player} {stat} {d}{line_str}"
    elif market == "spread":
        # Prefer the signed market_line (from Odds API) over the unsigned consensus line.
        # consensus `line` is always a magnitude (e.g. 6.5), so it can never show
        # a team as a favorite. market_line carries the real +/- sign.
        display_line = signal.get("market_line")
        if display_line is None:
            display_line = sig_line
        if display_line is not None:
            prefix = "+" if display_line > 0 else ""
            line_str = f" {prefix}{display_line}"
        else:
            line_str = ""
        return f"{selection} spread{line_str}"
    elif market == "total":
        d = direction[0] if direction else "?"
        line_str = f" {sig_line}" if sig_line is not None else ""
        return f"total {d}{line_str}"
    elif market == "moneyline":
        return f"{selection} ML"
    return selection


def load_current_lines() -> Dict[str, Any]:
    """Load current market lines from The Odds API cache file."""
    if not CURRENT_LINES_PATH.exists():
        return {}
    try:
        with open(CURRENT_LINES_PATH) as f:
            data = json.load(f)
        return data.get("games", {})
    except (json.JSONDecodeError, IOError):
        return {}


def load_game_times(date_str: str) -> Dict[str, str]:
    """Load game start times from the NBA API scoreboard cache.

    Returns a dict mapping canonical event_key (e.g. 'NBA:2026:03:09:OKC@DEN')
    to a time string like '7:30 pm ET'.
    """
    sb_path = CACHE_DIR / f"nba_api_scoreboard_{date_str}.json"
    if not sb_path.exists():
        return {}
    try:
        data = json.loads(sb_path.read_text())
        # Build game_id -> time from GameHeader
        gid_to_time: Dict[str, str] = {}
        for rs in data.get("resultSets", []):
            if rs.get("name") == "GameHeader":
                hdrs = rs.get("headers", [])
                rows = rs.get("rowSet", [])
                gid_idx = hdrs.index("GAME_ID") if "GAME_ID" in hdrs else None
                status_idx = hdrs.index("GAME_STATUS_TEXT") if "GAME_STATUS_TEXT" in hdrs else None
                if gid_idx is None or status_idx is None:
                    break
                for row in rows:
                    gid = row[gid_idx]
                    status = (row[status_idx] or "").strip()
                    # Only capture pre-game time strings (e.g. "7:00 pm ET"), not "Final"
                    if "pm ET" in status or "am ET" in status:
                        gid_to_time[gid] = status
                break

        # Build game_id -> (away, home) from LineScore
        gid_to_teams: Dict[str, List[str]] = {}
        for rs in data.get("resultSets", []):
            if rs.get("name") == "LineScore":
                hdrs = rs.get("headers", [])
                rows = rs.get("rowSet", [])
                gid_idx = hdrs.index("GAME_ID") if "GAME_ID" in hdrs else None
                abbr_idx = hdrs.index("TEAM_ABBREVIATION") if "TEAM_ABBREVIATION" in hdrs else None
                if gid_idx is None or abbr_idx is None:
                    break
                for row in rows:
                    gid = row[gid_idx]
                    team = _canon_team(row[abbr_idx])
                    gid_to_teams.setdefault(gid, []).append(team)
                break

        # Build event_key -> time
        result: Dict[str, str] = {}
        year = date_str[:4]
        month = date_str[5:7]
        day = date_str[8:10]
        for gid, time_str in gid_to_time.items():
            teams = gid_to_teams.get(gid, [])
            if len(teams) == 2:
                away, home = teams[0], teams[1]
                ek = f"NBA:{year}:{month}:{day}:{away}@{home}"
                result[ek] = time_str
        return result
    except (json.JSONDecodeError, KeyError, ValueError):
        return {}


def lookup_market_line(
    signal: Dict[str, Any], market_lines: Dict[str, Any]
) -> Optional[float]:
    """Look up the current market line for a signal.

    For spreads: returns the team's current spread from the market.
    For totals: returns the current total line from the market.
    For player props: not supported yet (would need per-event API calls).
    """
    event_key = signal.get("event_key", "")
    market = signal.get("market_type", "")
    selection = signal.get("selection", "")

    game = market_lines.get(event_key)
    if not game:
        return None

    if market == "spread":
        spreads = game.get("spreads", {})
        val = spreads.get(selection)
        # Snap to nearest .5 — Odds API averages across books and can
        # produce .25/.75 lines that aren't real NBA spreads.
        if val is not None:
            val = round(val * 2) / 2
        return val

    if market == "total":
        return game.get("total")

    return None


def compute_ev(win_pct: float, american_odds: int) -> float:
    """Expected value per unit staked, given a win probability and American odds.

    Returns a decimal (e.g. 0.042 = 4.2% EV). Multiply by 100 for percentage.
    """
    if american_odds >= 0:
        decimal = (american_odds / 100) + 1
    else:
        decimal = (100 / abs(american_odds)) + 1
    return round((win_pct * (decimal - 1)) - (1 - win_pct), 4)


def lookup_market_odds(
    signal: Dict[str, Any], market_lines: Dict[str, Any]
) -> Optional[int]:
    """Look up the best available market odds for a signal from current_lines cache.

    Reads the 'odds' block added by fetch_current_lines.py.
    Returns American-format integer odds, or None if not available.
    Player props always return None (not in this API).
    """
    event_key = signal.get("event_key", "")
    market = signal.get("market_type", "")
    selection = signal.get("selection", "")
    direction = signal.get("direction", "")

    game = market_lines.get(event_key)
    if not game:
        return None

    odds_block = game.get("odds")
    if not odds_block:
        return None

    if market == "spread":
        side = odds_block.get("spread", {}).get(selection)
        return side.get("odds") if side else None

    if market == "total":
        # direction is "OVER" or "UNDER"
        side_key = direction.upper() if direction else None
        if not side_key:
            return None
        side = odds_block.get("total", {}).get(side_key)
        return side.get("odds") if side else None

    if market == "moneyline":
        return odds_block.get("moneyline", {}).get(selection)

    return None


def enrich_play_odds(
    play: Dict[str, Any],
    signal: Dict[str, Any],
    score_data: Dict[str, Any],
    market_lines: Dict[str, Any],
) -> None:
    """Add an 'odds' block to a play with market odds and EV calculations.

    Mutates play in-place. Uses matched_pattern win_pct when available,
    falls back to primary_record win_pct.
    """
    expert_odds = signal.get("best_odds")
    if expert_odds is not None:
        try:
            expert_odds = int(expert_odds)
        except (ValueError, TypeError):
            expert_odds = None
    market_odds = lookup_market_odds(signal, market_lines)

    # Determine win_pct: prefer matched pattern's validated hist record
    matched = score_data.get("matched_pattern")
    if matched and matched.get("hist", {}).get("win_pct"):
        win_pct = matched["hist"]["win_pct"]
    else:
        win_pct = score_data.get("primary_record", {}).get("win_pct", 0.0)

    ev_pct: Optional[float] = None
    expert_ev_pct: Optional[float] = None

    if market_odds is not None and win_pct:
        ev_pct = round(compute_ev(win_pct, market_odds) * 100, 2)
    if expert_odds is not None and win_pct:
        expert_ev_pct = round(compute_ev(win_pct, expert_odds) * 100, 2)

    play["odds"] = {
        "expert_odds": expert_odds,
        "market_odds": market_odds,
        "odds_source": "best_available" if market_odds is not None else None,
        "ev_pct": ev_pct,
        "expert_ev_pct": expert_ev_pct,
    }


def load_recent_trends_lookup(reports_dir: Optional[Path] = None) -> Dict[str, Any]:
    """Load recent trends lookup JSON."""
    path = (reports_dir or REPORTS_DIR) / "recent_trends_lookup.json"
    if not path.exists():
        return {}
    try:
        with open(path) as f:
            data = json.load(f)
        return data.get("by_window", {})
    except (json.JSONDecodeError, IOError):
        return {}


def _lookup_recent_trend(
    signal: Dict[str, Any],
    recent_lookup: Dict[str, Any],
    min_n: int = 5,
) -> Optional[Dict[str, Any]]:
    """Cascading fallback recent trend lookup for a signal.

    Strategy: try preferred window (14d) first, then widen (30d) before
    falling down the cascade. Within each window, cascade:
      1. combo × market × stat
      2. combo × market
      3. combo overall
      4. consensus × market × stat
      5. consensus × market
      6. consensus overall

    Returns dict with: wins, losses, n, win_pct, window, source, label.
    Returns None if nothing qualifies.
    """
    combo = signal.get("sources_combo", "")
    market = signal.get("market_type", "")
    atomic_stat = signal.get("atomic_stat")
    stat_key = atomic_stat if market == "player_prop" and atomic_stat else "all"
    sources_count = len(signal.get("sources_present") or signal.get("sources") or [])
    cs = f"{sources_count}_source" if sources_count < 4 else "4+_sources"

    # Cascade levels: (table_name, lookup_key, label)
    levels = []
    if combo:
        levels.append(("combo_market_stat", f"{combo}__{market}__{stat_key}", f"{combo} / {market} / {stat_key}"))
        levels.append(("combo_market", f"{combo}__{market}", f"{combo} / {market}"))
        levels.append(("combo_overall", combo, f"{combo} overall"))
    levels.append(("consensus_market_stat", f"{cs}__{market}__{stat_key}", f"{cs} / {market} / {stat_key}"))
    levels.append(("consensus_market", f"{cs}__{market}", f"{cs} / {market}"))
    levels.append(("consensus_overall", cs, cs))

    # Try preferred windows in order for each level
    preferred_windows = ["14", "30", "7"]

    for level_name, lookup_key, label in levels:
        for w in preferred_windows:
            window_tables = recent_lookup.get(w, {})
            table = window_tables.get(level_name, {})
            entry = table.get(lookup_key)
            if entry and (entry.get("wins", 0) + entry.get("losses", 0)) >= min_n:
                return {
                    "wins": entry["wins"],
                    "losses": entry["losses"],
                    "n": entry["n"],
                    "win_pct": entry["win_pct"],
                    "window": int(w),
                    "source": level_name,
                    "label": label,
                }

    return None


def build_signal_summary(signal: Dict[str, Any]) -> Dict[str, Any]:
    """Extract key fields from a signal for output."""
    return {
        "signal_id": signal.get("signal_id"),
        "selection": signal.get("selection"),
        "market_type": signal.get("market_type"),
        "sources_combo": signal.get("sources_combo"),
        "sources_present": signal.get("sources_present") or signal.get("sources") or [],
        "line": signal.get("line"),
        "score": signal.get("score"),
        "direction": signal.get("direction"),
        "atomic_stat": signal.get("atomic_stat"),
        "experts": signal.get("experts") or [],
        "expert_odds": signal.get("best_odds"),
        "away_team": signal.get("away_team"),
        "home_team": signal.get("home_team"),
        "event_key": signal.get("event_key"),
        "day_key": signal.get("day_key"),
        "line_min": signal.get("line_min"),
        "line_max": signal.get("line_max"),
        "market_line": signal.get("market_line"),
        "line_diff": signal.get("line_diff"),
        "sources_count": signal.get("sources_count"),
        "consensus_strength": signal.get("consensus_strength"),
        "game_time_et": signal.get("game_time_et"),
    }


def build_output(
    scored: List[Tuple[Dict[str, Any], Dict[str, Any]]],
    date_str: str,
    day_of_week: str,
    total_signals: int,
    scorable_count: int,
    excluded_count: int = 0,
    tables: Optional[Dict[str, Any]] = None,
    market_lines: Optional[Dict[str, Any]] = None,
    recent_tables: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build the final output JSON with Wilson-based quality gate filtering."""
    mkt_lines = market_lines or {}

    passed_raw = []
    filtered = []

    for signal, score_data in scored:
        primary = score_data["primary_record"]

        # Quality gate
        gate_pass, gate_reason = passes_quality_gate(signal, score_data)

        # Assign tier
        _sources = signal.get("sources_present") or signal.get("sources") or []
        tier = assign_tier(
            score_data["wilson_score"],
            primary["n"],
            primary["lookup_level"],
            a_tier_eligible=score_data.get("a_tier_eligible", False),
            has_pattern=score_data.get("matched_pattern") is not None,
            sources_count=len(_sources),
            n_qualified_experts=score_data.get("n_qualified_experts", 0),
        )

        if not gate_pass:
            tier = max(tier, "C", key=lambda t: {"A": 0, "B": 1, "C": 2, "D": 3}.get(t, 9))

        # Hard block: picks where the player identity is unresolved ("undefined_undefined").
        # These occur when JuiceReel/other sources post picks without matchup metadata and
        # the normalizer cannot resolve the player. Never publish unidentified player props.
        _selection = signal.get("selection") or ""
        if "undefined_undefined" in _selection:
            filtered.append({
                "signal": build_signal_summary(signal),
                "wilson_score": score_data["wilson_score"],
                "composite_score": score_data["wilson_score"],
                "tier": "D",
                "gate_reason": "undefined_player_identity",
                "summary": score_data.get("summary", ""),
            })
            continue

        if tier in ("A", "B", "C"):
            passed_raw.append((signal, score_data, tier))
        else:
            reason = gate_reason if not gate_pass else f"below_tier_b:wilson={score_data['wilson_score']:.3f}"
            filtered.append({
                "signal": build_signal_summary(signal),
                "wilson_score": score_data["wilson_score"],
                "composite_score": score_data["wilson_score"],
                "tier": tier,
                "gate_reason": reason,
                "summary": score_data["summary"],
            })

    # Deduplicate picks with identical normalized selection + event + market
    # (e.g. same team spread from two signals with different event_key directions)
    seen_picks: Dict[str, int] = {}
    deduped_passed: List[Tuple[Dict[str, Any], Dict[str, Any], str]] = []
    for signal, score_data, tier in passed_raw:
        norm_sel, norm_ek, norm_mkt = _normalize_dedup_key(signal)
        dedup_key = f"{norm_sel}|{norm_ek}|{norm_mkt}"
        if dedup_key in seen_picks:
            # Keep the one with more sources or better score
            existing_idx = seen_picks[dedup_key]
            existing_sig = deduped_passed[existing_idx][0]
            existing_sc = len(existing_sig.get("sources_present") or existing_sig.get("sources") or [])
            new_sc = len(signal.get("sources_present") or signal.get("sources") or [])
            if new_sc > existing_sc or (new_sc == existing_sc and score_data["wilson_score"] > deduped_passed[existing_idx][1]["wilson_score"]):
                deduped_passed[existing_idx] = (signal, score_data, tier)
            continue
        seen_picks[dedup_key] = len(deduped_passed)
        deduped_passed.append((signal, score_data, tier))
    if len(deduped_passed) < len(passed_raw):
        print(f"  Deduped {len(passed_raw) - len(deduped_passed)} duplicate picks")
    passed_raw = deduped_passed

    # Conflict filter: remove picks where the same player+stat has both OVER and UNDER.
    # This prevents showing users contradictory picks (e.g. Filipowski OVER 13 pts AND UNDER 16.5 pts).
    # Keep the higher-Wilson pick; drop the other.
    def _prop_conflict_key(sig: Dict[str, Any]) -> Optional[str]:
        """Return (player, stat, game) key for player props, else None.
        Game is normalized to sorted TEAM@TEAM so CHA@SAC == SAC@CHA."""
        if sig.get("market_type") != "player_prop":
            return None
        sel = sig.get("selection") or ""
        parts = sel.replace("NBA:", "").replace("NCAAB:", "").split("::")
        player = parts[0] if parts else ""
        stat = sig.get("atomic_stat") or (parts[1] if len(parts) > 1 else "")
        ek = sig.get("event_key") or ""
        # Normalize team order: extract TEAM@TEAM and sort so CHA@SAC == SAC@CHA
        m = re.search(r'([A-Z]{2,4})@([A-Z]{2,4})(?::\d+)?$', ek)
        if m:
            t1, t2 = _canon_team(m.group(1)), _canon_team(m.group(2))
            game = "@".join(sorted([t1, t2]))
        else:
            game = ek
        return f"{player}|{stat}|{game}" if player else None

    conflict_map: Dict[str, List[int]] = {}  # key -> list of indices in passed_raw
    for idx, (signal, score_data, tier) in enumerate(passed_raw):
        ck = _prop_conflict_key(signal)
        if ck:
            conflict_map.setdefault(ck, []).append(idx)

    conflict_drop: set = set()
    for ck, indices in conflict_map.items():
        if len(indices) < 2:
            continue
        # Multiple picks for same player+stat — check if directions conflict
        directions = [_normalize_direction(passed_raw[i][0]) for i in indices]
        if len(set(directions)) > 1:
            # Conflict: keep highest-Wilson, drop rest
            best_idx = max(indices, key=lambda i: passed_raw[i][1]["wilson_score"])
            for i in indices:
                if i != best_idx:
                    conflict_drop.add(i)
            sig = passed_raw[best_idx][0]
            sel = sig.get("selection", "")
            print(f"  Conflict: {ck} — kept {_normalize_direction(sig)}, dropped {len(indices)-1} conflicting pick(s)")

    if conflict_drop:
        passed_raw = [item for i, item in enumerate(passed_raw) if i not in conflict_drop]
        print(f"  Removed {len(conflict_drop)} conflicting opposite-direction picks")

    # Game-level conflict filter: same event + market type, opposite sides.
    # e.g. CLE@ORL total UNDER (A-tier) vs CLE@ORL total OVER (B-tier) — keep higher Wilson.
    # Also handles spread (two different teams same game) and ML (two teams same game).
    def _game_key(sig: Dict[str, Any]) -> Optional[str]:
        """Return canonical TEAM@TEAM key for game-level conflict detection, or None."""
        mkt = sig.get("market_type", "")
        if mkt not in ("total", "spread", "moneyline"):
            return None
        ek = sig.get("event_key", "")
        m = re.search(r'([A-Z]{2,4})@([A-Z]{2,4})(?::\d+)?$', ek)
        if not m:
            return None
        away, home = _canon_team(m.group(1)), _canon_team(m.group(2))
        return f"{away}@{home}"

    # Group all game picks by canonical game key
    game_conflict_map: Dict[str, List[int]] = {}
    for idx, (signal, score_data, tier) in enumerate(passed_raw):
        gk = _game_key(signal)
        if gk:
            game_conflict_map.setdefault(gk, []).append(idx)

    game_conflict_drop: set = set()
    tier_rank = {"A": 0, "B": 1, "C": 2, "D": 3}

    # --- Expert vote/dissent re-scoring pass ---
    # For any game where picks exist on both sides, apply dissent penalty to each
    # side proportional to how many experts are backing the opposing side.
    # Also applies consensus boost for multi-expert signals with no opposition.
    for gk, indices in game_conflict_map.items():
        if len(indices) < 2:
            continue
        away, home = gk.split("@")
        side_idxs = [i for i in indices if passed_raw[i][0].get("market_type") in ("spread", "moneyline")]
        if len(side_idxs) < 2:
            continue
        # Build per-team expert counts
        team_experts: Dict[str, int] = {}
        for i in side_idxs:
            sig = passed_raw[i][0]
            t = _pick_team(sig, gk)
            if t:
                t = _canon_team(t)
                team_experts[t] = team_experts.get(t, 0) + _count_signal_experts(sig)
        teams_present = set(team_experts.keys())
        has_conflict = away in teams_present and home in teams_present
        if not has_conflict:
            continue
        # Re-score each side with opposing expert count
        for i in side_idxs:
            sig, score_data, tier = passed_raw[i]
            t = _pick_team(sig, gk)
            if not t:
                continue
            t = _canon_team(t)
            n_against = sum(v for k, v in team_experts.items() if k != t)
            updated_score = _apply_vote_adjustments(score_data, sig, n_against, tables=tables)
            primary = updated_score.get("primary_record", {})
            new_tier = assign_tier(
                updated_score["wilson_score"],
                primary.get("n", 0),
                primary.get("lookup_level", "none"),
                a_tier_eligible=updated_score.get("a_tier_eligible", False),
                has_pattern=updated_score.get("matched_pattern") is not None,
                sources_count=len(sig.get("sources_present") or []),
                n_qualified_experts=updated_score.get("n_qualified_experts", 0),
            )
            passed_raw[i] = (sig, updated_score, new_tier)

    for gk, indices in game_conflict_map.items():
        if len(indices) < 2:
            continue

        away, home = gk.split("@")

        # --- Total conflicts: OVER vs UNDER in same game ---
        total_idxs = [i for i in indices if passed_raw[i][0].get("market_type") == "total"]
        if len(total_idxs) >= 2:
            dirs = set(_normalize_direction(passed_raw[i][0]) for i in total_idxs)
            if "OVER" in dirs and "UNDER" in dirs:
                best_idx = min(total_idxs, key=lambda i: (tier_rank.get(passed_raw[i][2], 9), -passed_raw[i][1]["wilson_score"]))
                best_tier = passed_raw[best_idx][2]
                dropped_tiers = []
                for i in total_idxs:
                    if i != best_idx:
                        game_conflict_drop.add(i)
                        dropped_tiers.append(passed_raw[i][2])
                kept_sel = _normalize_direction(passed_raw[best_idx][0])
                print(f"  Game conflict {gk}|total: kept {kept_sel} (Tier {best_tier}), dropped {len(total_idxs)-1} pick(s) (Tier {'/'.join(dropped_tiers)})")

        # --- Spread/ML team conflicts: picks on opposite teams in the same game ---
        # A spread on LAC and a moneyline on MIN are mutually exclusive outcomes.
        # Uses composite evidence (expert votes, solo records, combo records) to resolve.
        side_idxs = [i for i in indices if passed_raw[i][0].get("market_type") in ("spread", "moneyline")]
        if len(side_idxs) >= 2:
            # Detect whether picks are actually on opposite teams
            pick_teams = []
            for i in side_idxs:
                sig = passed_raw[i][0]
                t = _pick_team(sig, gk)
                if t:
                    t = _canon_team(t)
                pick_teams.append((i, t))
            teams_present = set(t for _, t in pick_teams if t)
            has_conflict = away in teams_present and home in teams_present

            if has_conflict:
                score_tables = tables or {}
                result_type, best_idx = _resolve_side_conflict(
                    side_idxs, passed_raw, score_tables, (away, home)
                )
                if result_type == "avoid":
                    # No-bet: drop all signals on both sides of this conflict
                    for i in side_idxs:
                        game_conflict_drop.add(i)
                    mkt_label = passed_raw[side_idxs[0]][0].get("market_type", "side")
                    print(f"  Game conflict {gk}|{mkt_label}: AVOID (both sides credible, no bet)")
                else:
                    best_sig = passed_raw[best_idx][0]
                    best_tier = passed_raw[best_idx][2]
                    dropped_tiers = []
                    for i in side_idxs:
                        if i != best_idx:
                            game_conflict_drop.add(i)
                            dropped_tiers.append(passed_raw[i][2])
                    kept_sel = best_sig.get("selection", "")
                    kept_mkt = best_sig.get("market_type", "")
                    print(f"  Game conflict {gk}|side: kept {kept_sel} {kept_mkt} (Tier {best_tier}), dropped {len(side_idxs)-1} pick(s) (Tier {'/'.join(dropped_tiers)})")

    if game_conflict_drop:
        passed_raw = [item for i, item in enumerate(passed_raw) if i not in game_conflict_drop]
        print(f"  Removed {len(game_conflict_drop)} conflicting game-level picks")

    # Minimum floor: relax gate if too few pass
    if len(passed_raw) < MIN_DAILY_PICKS and scored:
        all_by_wilson = sorted(scored, key=lambda x: -x[1]["wilson_score"])
        for signal, score_data in all_by_wilson:
            if len(passed_raw) >= MIN_DAILY_PICKS:
                break
            sig_id = signal.get("signal_id")
            already = any(s.get("signal_id") == sig_id for s, _, _ in passed_raw)
            if already:
                continue
            # Also skip if same normalized pick already in passed picks
            norm_sel, norm_ek, norm_mkt = _normalize_dedup_key(signal)
            dedup_key = f"{norm_sel}|{norm_ek}|{norm_mkt}"
            if dedup_key in seen_picks:
                continue
            # Relax to C-tier threshold
            if score_data["wilson_score"] >= WILSON_TIER_C:
                seen_picks[dedup_key] = len(passed_raw)
                passed_raw.append((signal, score_data, "B"))
                filtered = [f for f in filtered
                            if f["signal"].get("signal_id") != sig_id]

    # Sort by tier then wilson_score descending
    tier_order = {"A": 0, "B": 1, "C": 2}
    passed_raw.sort(key=lambda x: (tier_order.get(x[2], 9), -x[1]["wilson_score"]))

    tier_counts: Dict[str, int] = {"A": 0, "B": 0, "C": 0, "D": len(filtered)}
    for f in filtered:
        if f.get("tier") not in ("C", "D"):
            tier_counts["D"] -= 1

    plays = []
    for rank, (signal, score_data, tier) in enumerate(passed_raw, 1):
        tier_counts[tier] = tier_counts.get(tier, 0) + 1
        primary = score_data["primary_record"]

        # Build historical_record for backward compat (same data as primary_record)
        hist = {
            "wins": primary["wins"],
            "losses": primary["losses"],
            "n": primary["n"],
            "win_pct": primary["win_pct"],
            "source": primary["lookup_level"],
            "label": primary["label"],
        }

        play: Dict[str, Any] = {
            "rank": rank,
            "tier": tier,
            "wilson_score": score_data["wilson_score"],
            "composite_score": score_data["wilson_score"],  # backward compat
            "confidence": score_data["confidence"],
            "confidence_score": score_data.get("confidence_score", wilson_to_confidence(score_data["wilson_score"])),
            "a_tier_eligible": score_data.get("a_tier_eligible", False),
            "positive_dimensions": score_data["positive_dimensions"],
            "negative_dimensions": score_data["negative_dimensions"],
            "total_dimensions": score_data["total_dimensions"],
            "signal": build_signal_summary(signal),
            "primary_record": primary,
            "historical_record": hist,
            "factors": score_data["factors"],
            "supporting_factors": score_data["supporting_factors"],
            "summary": score_data["summary"],
            "small_sample": primary["n"] < 30,
            "recency_adjustment": score_data["recency_adjustment"],
            "expert_adjustment": score_data.get("expert_adjustment", 0),
            "stat_adjustment": score_data.get("stat_adjustment", 0),
            "day_adjustment": score_data.get("day_adjustment", 0),
            "line_bucket_adjustment": score_data.get("line_bucket_adjustment", 0),
            "total_adjustment": score_data.get("total_adjustment", 0),
        }
        if score_data.get("expert_detail"):
            play["expert_detail"] = score_data["expert_detail"]

        # Attach matched pattern info for A-tier picks
        if score_data.get("matched_pattern"):
            pat = score_data["matched_pattern"]
            play["matched_pattern"] = {
                "id": pat["id"],
                "label": pat["label"],
                "tier_eligible": pat.get("tier_eligible"),
                "hist": pat.get("hist"),
            }

        # Attach current market line if available
        mkt_line = lookup_market_line(signal, mkt_lines)
        if mkt_line is not None:
            play["signal"]["market_line"] = mkt_line
            pick_line = signal.get("line")
            if pick_line is not None:
                play["signal"]["line_diff"] = round(pick_line - mkt_line, 1)

        # Attach market odds + EV
        enrich_play_odds(play, signal, score_data, mkt_lines)

        # Attach recent trend
        if score_data.get("recent_trend"):
            play["recent_trend"] = score_data["recent_trend"]

        plays.append(play)

    return {
        "meta": {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "date": date_str,
            "day_of_week": day_of_week,
            "total_signals": total_signals,
            "scorable_signals": scorable_count,
            "excluded_count": excluded_count,
            "filtered_count": len(filtered),
            "exported_count": len(plays),
            "tier_counts": tier_counts,
        },
        "plays": plays,
        "filtered": filtered,
    }


def _format_record_str(primary: Optional[Dict[str, Any]]) -> str:
    """Format primary record as 'W-L (pct%)' with confidence tag."""
    if not primary or primary.get("n", 0) == 0 or primary.get("lookup_level") == "none":
        return "no history"
    w = primary.get("wins", 0)
    l = primary.get("losses", 0)
    wp = primary.get("win_pct", 0)
    wl = primary.get("wilson_lower", 0)
    return f"{w}-{l} ({wp*100:.1f}%) W={wl*100:.1f}%"


def print_summary(output: Dict[str, Any], verbose: bool = False) -> None:
    """Print a concise console summary with Wilson-based rankings."""
    meta = output["meta"]
    plays = output["plays"]
    filtered = output.get("filtered", [])

    excluded = meta.get("excluded_count", 0)
    filtered_count = meta.get("filtered_count", 0)
    exported = meta.get("exported_count", len(plays))

    print()
    print(f"  PLAYS OF THE DAY -- {meta['date']} ({meta['day_of_week']})")
    print(f"  Pipeline: {meta['total_signals']} signals → "
          f"{meta['scorable_signals']} scorable → "
          f"{meta['scorable_signals'] - excluded} after exclusions → "
          f"{exported} exported ({filtered_count} gated out)")
    print()

    for tier, label in [("A", "STRONG EDGE"), ("B", "POSITIVE EDGE"), ("C", "MARGINAL EDGE")]:
        tier_plays = [p for p in plays if p["tier"] == tier]
        if not tier_plays:
            continue

        print(f"  TIER {tier} -- {label} ({len(tier_plays)} picks)")
        for p in tier_plays:
            sig = p["signal"]
            sel_str = format_selection(sig)
            primary = p.get("primary_record", {})
            combo = sig.get("sources_combo", "?")
            wilson = p.get("wilson_score", 0)
            record_str = _format_record_str(primary)
            conf = p.get("confidence", "?")
            level = primary.get("lookup_level", "?")
            recent_str = ""
            rt = p.get("recent_trend")
            if rt:
                recent_str = f" | {rt['window']}d: {rt['wins']}-{rt['losses']} ({rt['win_pct']*100:.0f}%)"
            total_adj = p.get("total_adjustment", 0)
            adj_parts = []
            if p.get("recency_adjustment", 0) != 0:
                adj_parts.append(f"trend={p['recency_adjustment']*100:+.0f}")
            if p.get("expert_adjustment", 0) != 0:
                adj_parts.append(f"analyst={p['expert_adjustment']*100:+.1f}")
            if p.get("stat_adjustment", 0) != 0:
                adj_parts.append(f"stat={p['stat_adjustment']*100:+.1f}")
            if p.get("day_adjustment", 0) != 0:
                adj_parts.append(f"day={p['day_adjustment']*100:+.1f}")
            if p.get("line_bucket_adjustment", 0) != 0:
                adj_parts.append(f"line={p['line_bucket_adjustment']*100:+.1f}")
            adj_str = f" [{', '.join(adj_parts)}]" if adj_parts else ""
            pat_str = ""
            mp = p.get("matched_pattern")
            if mp:
                pat_str = f" ★ {mp['label']}"
                if mp.get("hist"):
                    pat_str += f" ({mp['hist']['record']})"
            cscore = p.get("confidence_score", 0)
            print(f"    {p['rank']:3d}. {sel_str:30s} {combo:30s} {record_str:22s}{recent_str}  Score={cscore:3d}/100 Wilson={wilson*100:.1f}%{adj_str} [{conf}/{level}]{pat_str}")
        print()

    # Filter summary
    if filtered:
        if verbose:
            print(f"  FILTERED ({len(filtered)} picks removed):")
            from collections import Counter
            reasons = Counter(f.get("gate_reason", "unknown").split(":")[0]
                              for f in filtered)
            for reason, count in reasons.most_common():
                print(f"    {reason}: {count}")
            print()
            for f in filtered:
                sig = f["signal"]
                sel_str = format_selection(sig)
                wilson = f.get("wilson_score", 0)
                print(f"    ✗ {sel_str:30s} Wilson={wilson*100:.1f}%  ({f.get('gate_reason', '?')})")
            print()
        else:
            print(f"  [{len(filtered)} picks filtered out — use --verbose to see reasons]")
            print()


# ── Main ───────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Score signals using Wilson-based combo×market ranking.")
    parser.add_argument("--date", type=str, default=None, help="Date to score (YYYY-MM-DD). Default: today")
    parser.add_argument("--verbose", action="store_true", help="Show all tiers and full factor details")
    parser.add_argument("--sport", choices=["NBA", "NCAAB"], default="NBA", help="Sport to score (default: NBA)")
    args = parser.parse_args()

    sport = args.sport
    reports_dir, signals_path, plays_dir = get_sport_paths(sport)
    if sport == "NCAAB":
        active_pattern_registry = PATTERN_REGISTRY_NCAAB
        active_excluded_combos = EXCLUDED_COMBOS_BY_MARKET_NCAAB
    else:
        dynamic_pattern_registry = load_dynamic_pattern_registry(sport, reports_dir)
        active_pattern_registry = merge_pattern_registries(PATTERN_REGISTRY, dynamic_pattern_registry)
        active_excluded_combos = EXCLUDED_COMBOS_BY_MARKET
        # Q6: log merged registry composition at startup for audit.
        print(
            f"  [patterns] NBA pattern registry — hardcoded={len(PATTERN_REGISTRY)}  "
            f"dynamic={len(dynamic_pattern_registry)}  total_merged={len(active_pattern_registry)}"
        )

    date_str = args.date or date.today().strftime("%Y-%m-%d")
    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        print(f"ERROR: Invalid date format: {date_str}")
        return
    day_of_week = target_date.strftime("%A")

    # Load lookup tables
    tables = load_lookup_tables(reports_dir=reports_dir)
    loaded = tables.pop("_loaded", 0)

    # Load recent trends lookup
    recent_tables = load_recent_trends_lookup(reports_dir=reports_dir)
    if recent_tables:
        print(f"  Loaded recent trends ({len(recent_tables)} windows)")

    # Load and filter signals
    signals = load_signals(date_str, sport=sport, signals_path=signals_path)
    total_signals = len(signals)
    scorable = filter_scorable(signals, pattern_registry=active_pattern_registry,
                               excluded_combos=active_excluded_combos)

    # Filter out signals whose matchup didn't actually happen on this date
    scorable, wrong_date_count = filter_wrong_date(scorable, date_str, sport=sport)
    if wrong_date_count:
        print(f"  Dropped {wrong_date_count} signals with wrong game date")
    scorable_count = len(scorable)

    if not scorable:
        print(f"  No scorable {sport} signals found for {date_str}.")
        print(f"  (Total {sport} signals for date: {total_signals})")
        # Write empty plays file so stale data doesn't persist
        plays_dir.mkdir(parents=True, exist_ok=True)
        out_path = plays_dir / f"plays_{date_str}.json"
        empty_output = {
            "meta": {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "date": date_str,
                "day_of_week": day_of_week,
                "total_signals": total_signals,
                "scorable_signals": 0,
                "excluded_count": 0,
                "filtered_count": 0,
                "exported_count": 0,
                "tier_counts": {},
            },
            "plays": [],
            "filtered": [],
        }
        out_path.write_text(json.dumps(empty_output, indent=2, ensure_ascii=False))
        print(f"  Output: {out_path} (empty)")
        return

    # Apply exclusion filters (stat blacklist, high prop lines, losing combos)
    after_exclusions, excluded = apply_exclusion_filters(scorable, tables=tables)
    if excluded:
        print(f"  Excluded {len(excluded)} signals (stat blacklist / high lines / losing combos)")

    # Score each signal
    scored: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    for signal in after_exclusions:
        score_data = score_signal(signal, tables, recent_tables=recent_tables,
                                  day_of_week=day_of_week,
                                  pattern_registry=active_pattern_registry)
        scored.append((signal, score_data))

    # Load current market lines (if available)
    market_lines = load_current_lines()
    if market_lines:
        print(f"  Loaded current market lines for {len(market_lines)} games")

    # Inject game start times from scoreboard cache
    game_times = load_game_times(date_str)
    if game_times:
        # Build reverse lookup so we match regardless of which team is listed first
        game_times_both: Dict[str, str] = {}
        for ek, t in game_times.items():
            game_times_both[ek] = t
            # Also add reversed matchup: NBA:Y:M:D:A@H -> NBA:Y:M:D:H@A
            parts = ek.rsplit(":", 1)
            if len(parts) == 2 and "@" in parts[1]:
                away, home = parts[1].split("@", 1)
                game_times_both[f"{parts[0]}:{home}@{away}"] = t
        for signal in after_exclusions:
            ek = signal.get("event_key", "")
            if ek in game_times_both:
                signal["game_time_et"] = game_times_both[ek]

    # Build output with quality gate
    output = build_output(
        scored, date_str, day_of_week, total_signals, scorable_count,
        excluded_count=len(excluded),
        tables=tables, market_lines=market_lines, recent_tables=recent_tables,
    )

    # Write JSON
    plays_dir.mkdir(parents=True, exist_ok=True)
    out_path = plays_dir / f"plays_{date_str}.json"
    out_path.write_text(json.dumps(output, indent=2, ensure_ascii=False))

    # Print summary
    print_summary(output, verbose=args.verbose)
    print(f"  Output: {out_path}")
    print()


if __name__ == "__main__":
    main()
