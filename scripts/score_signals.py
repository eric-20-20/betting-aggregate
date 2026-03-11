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
import re
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# ── Paths ──────────────────────────────────────────────────────────────────

REPORTS_DIR = Path("data/reports")
SIGNALS_PATH = Path("data/ledger/signals_latest.jsonl")
PLAYS_DIR = Path("data/plays")

CURRENT_LINES_PATH = Path("data/cache/current_lines.json")
RECENT_TRENDS_PATH = REPORTS_DIR / "recent_trends_lookup.json"
CACHE_DIR = Path("data/cache/results")

CROSS_TAB_PATH = REPORTS_DIR / "cross_tabulation.json"
CONSENSUS_PATH = REPORTS_DIR / "trends" / "consensus_strength.json"
LINE_BUCKET_PATH = REPORTS_DIR / "by_line_bucket.json"
STAT_TYPE_PATH = REPORTS_DIR / "by_stat_type.json"
DAY_OF_WEEK_PATH = REPORTS_DIR / "trends" / "by_day_of_week.json"
EXPERT_PATH = REPORTS_DIR / "by_expert_record.json"
MARKET_TYPE_PATH = REPORTS_DIR / "trends" / "market_type.json"


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

EXCLUDED_SIGNAL_TYPES = {"avoid_conflict"}

# ── Wilson-based scoring constants ────────────────────────────────────────

MIN_SAMPLE_PRIMARY = 15   # min n to accept a primary combo lookup
MIN_SAMPLE_TREND = 5      # min decided bets for recent trend modifier

# Tier thresholds (Wilson lower bound)
WILSON_TIER_A = 0.52      # strong edge
WILSON_TIER_B = 0.46      # positive edge
WILSON_TIER_C = 0.45      # marginal — not exported
WILSON_HARD_EXCLUDE = 0.42  # hard-exclude if n >= 30 and Wilson below this

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
DAY_BOOST_THURS = 0.01
DAY_PENALTY_FRI = -0.01
LINE_BUCKET_BOOST = 0.01   # bucket wp >51%
LINE_BUCKET_PENALTY = -0.01  # bucket wp <48%

# ── Quality filters ──────────────────────────────────────────────────────

EXCLUDED_STAT_TYPES = {"steals", "unknown", "pts_reb"}

# Direction-aware stat exclusions: exclude (stat, direction) combos that are bad,
# while allowing the other direction through (e.g. pts_reb_ast UNDER is 57.9%).
EXCLUDED_STAT_DIR: Set[Tuple[str, str]] = {
    ("reb_ast", "OVER"),       # 35-32 (52%), small sample — not reliable
    ("pts_reb_ast", "OVER"),   # 203-247 (45.1%) — clearly negative edge
}
MAX_PROP_LINE = 30.5
MIN_DAILY_PICKS = 3

# ── Pattern Registry ─────────────────────────────────────────────────────
# Curated profitable patterns from analysis of 6,547 graded signals.
# Ordered most-specific first — first match wins.
# Source matching: source_pair = both must be present; source_contains = all must be present.

PATTERN_REGISTRY: List[Dict[str, Any]] = [
    # ── Patterns validated on full grades_latest.jsonl (2026-03-05) ──
    # Ordered most-specific first — first match wins.
    # Dataset: 22,426 graded WIN/LOSS records across 2024-25 and 2025-26 NBA seasons.
    # Wilson scores use z=1.645 (90% CI lower bound).

    # nukethebooks solo UNDER props: 188-122 (60.6%) n=310 Wilson=0.560
    # Consistent across all months: Nov(76%), Dec(57%), Jan(62%), Feb(61%)
    {
        "id": "nukethebooks_solo_props_under",
        "label": "nukethebooks player prop UNDER",
        "exact_combo": "juicereel_nukethebooks",
        "market_type": "player_prop",
        "direction": "UNDER",
        "hist": {"record": "188-122", "win_pct": 0.606, "n": 310},
        "tier_eligible": "A",
    },

    # nukethebooks solo OVER props: 234-186 (55.7%) n=420 Wilson=0.517
    # Consistent: Nov(65%), Dec(64%), Jan(52%), Feb(54%)
    {
        "id": "nukethebooks_solo_props_over",
        "label": "nukethebooks player prop OVER",
        "exact_combo": "juicereel_nukethebooks",
        "market_type": "player_prop",
        "direction": "OVER",
        "hist": {"record": "235-187", "win_pct": 0.557, "n": 422},
        "tier_eligible": "B",
    },

    # 5-source UNDER totals (action+betql+covers+dimers+oddstrader): 51-32 (61.4%) n=83 Wilson=0.518
    # Elevated to A-tier: Wilson 0.518 > 0.46 threshold, consistent across sample.
    # Note: OVER direction has degraded to 10-10 (coin flip) — removed from registry.
    # MUST be before four_source and betql_oddstrader patterns (more specific).
    {
        "id": "five_source_total_under",
        "label": "UNDER total (5+ sources)",
        "source_contains": ["action", "betql", "covers", "dimers", "oddstrader"],
        "market_type": "total",
        "direction": "UNDER",
        "min_sources": 5,
        "hist": {"record": "51-32", "win_pct": 0.614, "n": 83},
        "tier_eligible": "A",
    },

    # Any 4+ source UNDER total (any combo): 812-709 (53.6%) n=1521 Wilson=0.510
    # Large sample, consistent positive edge. Any 4+ sources agreeing on UNDER is A-tier.
    # MUST be before the specific action+betql+dimers+oddstrader pattern below.
    {
        "id": "four_source_total_under_any",
        "label": "UNDER total (4+ sources)",
        "market_type": "total",
        "direction": "UNDER",
        "min_sources": 4,
        "hist": {"record": "812-709", "win_pct": 0.536, "n": 1521},
        "tier_eligible": "A",
    },

    # 4-source total UNDER (action+betql+dimers+oddstrader): 623-549 (53.2%) n=1172 Wilson=0.507
    # Kept for historical reference; now superseded by four_source_total_under_any above.
    # MUST be before betql_oddstrader pattern (more specific).
    {
        "id": "four_source_total_under",
        "label": "UNDER total (action+betql+dimers+oddstrader)",
        "source_contains": ["action", "betql", "dimers", "oddstrader"],
        "market_type": "total",
        "direction": "UNDER",
        "min_sources": 4,
        "hist": {"record": "623-549", "win_pct": 0.532, "n": 1172},
        "tier_eligible": "B",
    },

    # betql + oddstrader UNDER totals: 32-17 (65.3%) n=50 Wilson=0.524
    # Feb25(75%), Nov25(44%), Dec25(79%), Feb26(50%) — inconsistent, small sample
    # B-tier; needs more data before A-tier consideration.
    # NOTE: only matches signals that have ONLY betql+oddstrader (not 4- or 5-source,
    # which are caught by the more-specific patterns above).
    {
        "id": "betql_oddstrader_total_under",
        "label": "betql + oddstrader total UNDER",
        "source_pair": ["betql", "oddstrader"],
        "market_type": "total",
        "direction": "UNDER",
        "min_sources": 2,
        "hist": {"record": "32-17", "win_pct": 0.653, "n": 50},
        "tier_eligible": "B",
    },

    # action + betql UNDER totals: 134-70 (65.7%) n=204 Wilson=0.600 — A-tier
    # Oct25(41%) was an outlier; Nov-Dec-Jan: 62/76/100%. Strong 2025-26 trend.
    # exact_combo excludes 3+ source signals (caught by 4-source patterns above).
    # Cleaned 2026-03-11: removed 9 bad-line (<150) BettingPros history records.
    {
        "id": "action_betql_total_under",
        "label": "action + betql total UNDER",
        "exact_combo": "action|betql",
        "market_type": "total",
        "direction": "UNDER",
        "hist": {"record": "134-70", "win_pct": 0.657, "n": 204},
        "tier_eligible": "A",
    },

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

    # action UNDER assists props: 46-28 (62.2%) n=74 Wilson=0.526 — B-tier
    # 2024-25: 67% (33-16), but 2025-26 regressing to 52% (13-12). Watch.
    # MUST be before action_solo_props_under (more specific — stat_type constraint).
    {
        "id": "action_props_assists_under",
        "label": "action assists prop UNDER",
        "exact_combo": "action",
        "market_type": "player_prop",
        "direction": "UNDER",
        "stat_type": "assists",
        "hist": {"record": "46-28", "win_pct": 0.622, "n": 74},
        "tier_eligible": "B",
    },

    # action UNDER pts_ast props: 30-17 (63.8%) n=47 Wilson=0.518 — B-tier
    # Limited per-month samples; consistent direction but watch for regression.
    # MUST be before action_solo_props_under (more specific — stat_type constraint).
    {
        "id": "action_props_pts_ast_under",
        "label": "action pts+ast prop UNDER",
        "exact_combo": "action",
        "market_type": "player_prop",
        "direction": "UNDER",
        "stat_type": "pts_ast",
        "hist": {"record": "30-17", "win_pct": 0.638, "n": 47},
        "tier_eligible": "B",
    },

    # action solo UNDER props: 399-257 (60.8%) n=656 Wilson=0.576
    # Consistent across all months; strong A-tier solo.
    # Catches all action UNDER props not matched by a more-specific stat_type pattern above.
    {
        "id": "action_solo_props_under",
        "label": "action player prop UNDER",
        "exact_combo": "action",
        "market_type": "player_prop",
        "direction": "UNDER",
        "hist": {"record": "399-257", "win_pct": 0.608, "n": 656},
        "tier_eligible": "A",
    },

    # sxebets (JuiceReel) solo UNDER props: 571-476 (54.5%) n=1047 Wilson=0.520
    # Large sample, consistent positive UNDER edge across months.
    {
        "id": "sxebets_solo_props_under",
        "label": "juicereel_sxebets player prop UNDER",
        "exact_combo": "juicereel_sxebets",
        "market_type": "player_prop",
        "direction": "UNDER",
        "hist": {"record": "571-476", "win_pct": 0.545, "n": 1068},
        "tier_eligible": "B",
    },

    # betql UNDER rebounds props: 200-135 (59.7%) n=335 Wilson=0.552 — B-tier
    # 2024-25: strong Feb(78%), but crashed Mar(33%) and Apr(49%). 2025-26 recovering.
    # Not consistent enough for A-tier but Wilson clears threshold comfortably.
    # MUST be before betql_solo_props_under (more specific — stat_type constraint).
    {
        "id": "betql_props_rebounds_under",
        "label": "betql rebounds prop UNDER",
        "exact_combo": "betql",
        "market_type": "player_prop",
        "direction": "UNDER",
        "stat_type": "rebounds",
        "hist": {"record": "200-135", "win_pct": 0.597, "n": 335},
        "tier_eligible": "B",
    },

    # betql assists UNDER props: 94-78 (54.7%) n=172 Wilson=0.484 — B-tier
    # Outperforms betql general prop UNDER (Wilson=0.512); consistent 2024-25 season.
    # 2025-26 season has regressed (monthly: Dec 37.5%, Jan 50.0%, Feb 48.3%) — watch.
    # MUST be before betql_solo_props_under (more specific — stat_type constraint).
    {
        "id": "betql_props_assists_under",
        "label": "betql assists prop UNDER",
        "exact_combo": "betql",
        "market_type": "player_prop",
        "direction": "UNDER",
        "stat_type": "assists",
        "hist": {"record": "94-78", "win_pct": 0.547, "n": 172},
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

    # action + oddstrader spread: 80-55 (59.3%) n=135 Wilson=0.522
    # Strong through Dec25, but weakened Jan-Feb26 (22-21 combined 51.2%).
    # B-tier; watch for continued degradation.
    {
        "id": "action_oddstrader_spread",
        "label": "action + oddstrader spread",
        "source_pair": ["action", "oddstrader"],
        "market_type": "spread",
        "min_sources": 2,
        "hist": {"record": "125-87", "win_pct": 0.59, "n": 212},
        "tier_eligible": "B",
    },

    # action solo spread: 148-115 (56.3%) n=263 Wilson=0.512
    # Real edge in spreads; no team restriction — direction-agnostic.
    {
        "id": "action_solo_spread",
        "label": "action spread",
        "exact_combo": "action",
        "market_type": "spread",
        "hist": {"record": "258-205", "win_pct": 0.557, "n": 465},
        "tier_eligible": "B",
    },

    # ── BetQL team-specific spread patterns ──────────────────────────────────
    # BetQL solo is unreliable overall on spreads (~50%) but highly accurate on
    # specific teams. Patterns use the "team" key to match signal.direction.
    # Data: grades_latest.jsonl (2026-03-05). All records are current-season validated.
    # A-tier requires Wilson >= 0.50 AND consistent across multiple months.

    # betql OKC spread: 81-28 (74.3%) n=109 Wilson=0.669 — A-tier (25-26: 33-16, 67.3%)
    {
        "id": "betql_spread_okc",
        "label": "betql spread OKC",
        "exact_combo": "betql",
        "market_type": "spread",
        "team": "OKC",
        "hist": {"record": "81-28", "win_pct": 0.743, "n": 109},
        "tier_eligible": "A",
    },
    # betql BOS spread: 58-20 (74.4%) n=78 Wilson=0.655 — A-tier (25-26: 27-15, 64.3%)
    {
        "id": "betql_spread_bos",
        "label": "betql spread BOS",
        "exact_combo": "betql",
        "market_type": "spread",
        "team": "BOS",
        "hist": {"record": "57-20", "win_pct": 0.74, "n": 77},
        "tier_eligible": "A",
    },
    # betql MIN spread: 48-17 (73.8%) n=65 Wilson=0.641 — A-tier (25-26: 27-9, 75.0%)
    {
        "id": "betql_spread_min",
        "label": "betql spread MIN",
        "exact_combo": "betql",
        "market_type": "spread",
        "team": "MIN",
        "hist": {"record": "48-17", "win_pct": 0.738, "n": 67},
        "tier_eligible": "A",
    },
    # betql LAC spread: 59-23 (72.0%) n=82 Wilson=0.632 — A-tier (25-26: 29-15, 65.9%)
    {
        "id": "betql_spread_lac",
        "label": "betql spread LAC",
        "exact_combo": "betql",
        "market_type": "spread",
        "team": "LAC",
        "hist": {"record": "59-23", "win_pct": 0.720, "n": 82},
        "tier_eligible": "A",
    },
    # betql DET spread: 51-26 (66.2%) n=77 Wilson=0.570 — A-tier (25-26: 28-16, 63.6%)
    {
        "id": "betql_spread_det",
        "label": "betql spread DET",
        "exact_combo": "betql",
        "market_type": "spread",
        "team": "DET",
        "hist": {"record": "51-26", "win_pct": 0.662, "n": 77},
        "tier_eligible": "A",
    },
    # betql PHX spread: 37-18 (67.3%) n=55 Wilson=0.563 — A-tier (25-26: 24-6, 80.0%)
    {
        "id": "betql_spread_phx",
        "label": "betql spread PHX",
        "exact_combo": "betql",
        "market_type": "spread",
        "team": "PHX",
        "hist": {"record": "37-18", "win_pct": 0.673, "n": 57},
        "tier_eligible": "A",
    },
    # betql HOU spread: 56-35 (61.5%) n=91 Wilson=0.529 — A-tier (25-26: 33-16, 67.3%)
    {
        "id": "betql_spread_hou",
        "label": "betql spread HOU",
        "exact_combo": "betql",
        "market_type": "spread",
        "team": "HOU",
        "hist": {"record": "56-35", "win_pct": 0.615, "n": 91},
        "tier_eligible": "A",
    },
    # betql GSW spread: 59-36 (62.1%) n=95 Wilson=0.537 — B-tier (25-26: 17-18, degraded)
    # Current season has deteriorated to 48.6%; keeping in registry at B-tier.
    {
        "id": "betql_spread_gsw",
        "label": "betql spread GSW",
        "exact_combo": "betql",
        "market_type": "spread",
        "team": "GSW",
        "hist": {"record": "58-35", "win_pct": 0.624, "n": 95},
        "tier_eligible": "B",
    },
    # betql NYK spread: 49-39 (55.7%) n=88 Wilson=0.469 — B-tier
    # Current season (25-26): 26-13 (66.7%); all-time Wilson just clears 0.46 threshold.
    {
        "id": "betql_spread_nyk",
        "label": "betql spread NYK",
        "exact_combo": "betql",
        "market_type": "spread",
        "team": "NYK",
        "hist": {"record": "49-39", "win_pct": 0.557, "n": 91},
        "tier_eligible": "B",
    },
    # betql CHI spread: 11-2 (84.6%) n=13 Wilson=0.625 — B-tier (small sample, caution)
    # 2024-25 playoffs: 6-0, 2-0; 2025-26: 3-2. Very small n; B-tier until more data.
    {
        "id": "betql_spread_chi",
        "label": "betql spread CHI",
        "exact_combo": "betql",
        "market_type": "spread",
        "team": "CHI",
        "hist": {"record": "11-2", "win_pct": 0.846, "n": 13},
        "tier_eligible": "B",
    },

    # ── OddsTrader team-specific moneyline patterns ───────────────────────────
    # OddsTrader solo moneylines are generally driven by heavy favorites (avg -346 odds).
    # Net unit profit is minimal (~3-4 units per 27 picks) — these are tracked for
    # pattern matching but kept at B-tier due to low ROI despite high win rates.
    # Patterns use the "team" key to match signal.direction.

    # oddstrader NYK moneyline: 22-5 (81.5%) n=27 Wilson=0.665 — B-tier (heavy favorites)
    {
        "id": "oddstrader_ml_nyk",
        "label": "oddstrader moneyline NYK",
        "exact_combo": "oddstrader",
        "market_type": "moneyline",
        "team": "NYK",
        "hist": {"record": "22-5", "win_pct": 0.815, "n": 27},
        "tier_eligible": "B",
    },
    # oddstrader SAS moneyline: 21-5 (80.8%) n=26 Wilson=0.654 — B-tier (consistent all months)
    {
        "id": "oddstrader_ml_sas",
        "label": "oddstrader moneyline SAS",
        "exact_combo": "oddstrader",
        "market_type": "moneyline",
        "team": "SAS",
        "hist": {"record": "21-5", "win_pct": 0.808, "n": 26},
        "tier_eligible": "B",
    },
    # oddstrader HOU moneyline: 15-4 (78.9%) n=19 Wilson=0.605 — B-tier (small sample)
    {
        "id": "oddstrader_ml_hou",
        "label": "oddstrader moneyline HOU",
        "exact_combo": "oddstrader",
        "market_type": "moneyline",
        "team": "HOU",
        "hist": {"record": "15-4", "win_pct": 0.789, "n": 19},
        "tier_eligible": "B",
    },

    # ── BettingPros Experts patterns ─────────────────────────────────────────
    # Validated on 3,805 graded records from bettingpros_experts backfill (2024–2026).
    # Wilson scores use z=1.645 (90% CI lower bound).
    # Key finding: action+bettingpros_experts is the sweet spot — adding betql collapses
    # totals performance from 60.6% to 48.8%. Must be exact_combo to exclude betql.

    # action + bettingpros_experts UNDER totals: 60-39 (60.6%) n=99 Wilson=0.523
    # OVER direction is a coin flip (49.2%) — direction-specific pattern, UNDER only.
    {
        "id": "action_bettingpros_total_under",
        "label": "action + bettingpros experts total UNDER",
        "exact_combo": "action|bettingpros_experts",
        "market_type": "total",
        "direction": "UNDER",
        "hist": {"record": "76-49", "win_pct": 0.608, "n": 125},
        "tier_eligible": "B",
    },

    # action + bettingpros_experts spreads: 87-73 (54.4%) n=160 Wilson=0.479
    # No direction edge detected — applies to both OVER/UNDER spread sides.
    {
        "id": "action_bettingpros_spread",
        "label": "action + bettingpros experts spread",
        "exact_combo": "action|bettingpros_experts",
        "market_type": "spread",
        "hist": {"record": "87-73", "win_pct": 0.544, "n": 160},
        "tier_eligible": "B",
    },
]

# ── NCAAB Pattern Registry ────────────────────────────────────────────────────
# Separate registry for NCAAB signals. Same schema as PATTERN_REGISTRY.
# Currently empty — need to accumulate graded NCAAB history before patterns
# can be validated. Add patterns here as data grows (aim for n≥50 per pattern).
# Sources active for NCAAB: action, covers, sportsline, dimers, oddstrader, juicereel.
PATTERN_REGISTRY_NCAAB: List[Dict[str, Any]] = [
    # Validated 2026-03-06 from JuiceReel NCAAB backfill (Oct 2023 – Mar 2026)
    # All B-tier: Wilson lower bound (z=1.645) ≥ 0.46
    {
        "name": "nukethebooks_ncaab_total",
        "exact_combo": "juicereel_nukethebooks",
        "market_type": "total",
        "hist": {"record": "151-122", "win_pct": 0.553, "n": 273},
        "tier_eligible": "B",
    },
    {
        "name": "sxebets_ncaab_total",
        "exact_combo": "juicereel_sxebets",
        "market_type": "total",
        "hist": {"record": "165-153", "win_pct": 0.519, "n": 318},
        "tier_eligible": "B",
    },
    {
        "name": "sxebets_ncaab_moneyline",
        "exact_combo": "juicereel_sxebets",
        "market_type": "moneyline",
        "hist": {"record": "237-210", "win_pct": 0.530, "n": 447},
        "tier_eligible": "B",
    },
    {
        "name": "sxebets_ncaab_props",
        "exact_combo": "juicereel_sxebets",
        "market_type": "player_prop",
        "hist": {"record": "39-26", "win_pct": 0.600, "n": 65},
        "tier_eligible": "B",
    },
]

# Known bad combos by market type — exclude from scoring regardless of other criteria
# These have large sample sizes showing negative/coin-flip results
EXCLUDED_COMBOS_BY_MARKET: set = {
    # NBA JuiceReel solo moneylines: juicereel_nukethebooks ~38%, juicereel_sxebets ~40%
    # Both are below the 42% hard-exclude threshold with large samples
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


COMBO_RECORD_PATH = REPORTS_DIR / "by_sources_combo_record.json"
COMBO_MARKET_SIGNALS_PATH = REPORTS_DIR / "by_sources_combo_market.json"


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
        ("experts", rd / "by_expert_record.json", _load_experts),
    ]:
        if path.exists():
            tables[name] = loader(json.loads(path.read_text()))
            loaded += 1
        else:
            tables[name] = {}

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


def load_game_schedule(date_str: str) -> Optional[Set[Tuple[str, str]]]:
    """Load the set of (away, home) matchups that actually occurred on a date.

    Tries LGF cache first (cleanest), then falls back to NBA API scoreboard cache.
    Returns set of (away_team, home_team) tuples with canonical abbreviations,
    or None if no cache data exists for this date (can't validate).
    An empty set means cache exists but no NBA games were played (e.g. All-Star break).
    """
    matchups: Set[Tuple[str, str]] = set()
    has_cache = False

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
            if matchups:
                return matchups
            # LGF had no NBA games — fall through to scoreboard
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
    signals: List[Dict[str, Any]], date_str: str
) -> Tuple[List[Dict[str, Any]], int]:
    """Drop signals whose matchup didn't actually occur on the given date.

    Uses LGF/scoreboard cache to build the actual game schedule, then filters
    out signals with event_keys pointing to games that happened on a different day.
    Returns (kept_signals, dropped_count).
    """
    schedule = load_game_schedule(date_str)
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
            # Can't extract teams (e.g. player props without event_key) — keep
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

    # High player prop lines
    if market == "player_prop" and line is not None:
        try:
            if float(line) > MAX_PROP_LINE:
                return f"high_prop_line:{line}"
        except (TypeError, ValueError):
            pass

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
    """Small modifier based on day-of-week historical performance."""
    if day_of_week == "Thursday":
        return DAY_BOOST_THURS
    if day_of_week == "Friday":
        return DAY_PENALTY_FRI
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

    # When no lookup table data exists (e.g. NCAAB early season), seed from pattern hist.
    # This lets B-tier pattern signals pass the quality gate based on historical record.
    if primary["lookup_level"] == "none" and matched_pattern:
        pat_hist = matched_pattern.get("hist", {})
        pat_n = pat_hist.get("n", 0)
        if pat_n >= MIN_SAMPLE_PRIMARY:
            pat_wins = round(pat_hist.get("win_pct", 0) * pat_n)
            pat_wilson = wilson_lower(pat_wins, pat_n, z=1.645)
            primary = {
                "lookup_level": "pattern_hist",
                "label": matched_pattern.get("name", ""),
                "win_pct": pat_hist.get("win_pct", 0),
                "n": pat_n,
                "wilson_lower": pat_wilson,
            }

    wilson_score = primary["wilson_lower"] + total_adjustment

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
        "primary_record": primary,
        "recency_adjustment": recency_adj,
        "expert_adjustment": expert_adj,
        "stat_adjustment": stat_adj,
        "day_adjustment": day_adj,
        "line_bucket_adjustment": line_adj,
        "total_adjustment": round(total_adjustment, 4),
        "recent_trend": trend,
        "confidence": confidence,
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
) -> str:
    """Assign tier based on pattern matching + Wilson lower bound.

      A: Matched a profitable pattern in PATTERN_REGISTRY AND Wilson >= 0.46
      B: Positive edge — Wilson >= 0.46 AND (named pattern OR 2+ sources)
      C: Marginal — Wilson >= 0.45, or B-worthy Wilson but single-source no-pattern
      D: Insufficient data or losing pattern
    """
    if n < MIN_SAMPLE_PRIMARY or lookup_level == "none":
        return "D"
    if a_tier_eligible and wilson_score >= WILSON_TIER_B:
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

    if n < MIN_SAMPLE_PRIMARY:
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
        "best_odds": signal.get("best_odds"),
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
        )

        if not gate_pass:
            tier = max(tier, "C", key=lambda t: {"A": 0, "B": 1, "C": 2, "D": 3}.get(t, 9))

        if tier in ("A", "B"):
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
        """Return (player, stat) key for player props, else None."""
        if sig.get("market_type") != "player_prop":
            return None
        sel = sig.get("selection", "")
        parts = sel.replace("NBA:", "").replace("NCAAB:", "").split("::")
        player = parts[0] if parts else ""
        stat = sig.get("atomic_stat") or (parts[1] if len(parts) > 1 else "")
        ek = sig.get("event_key", "")
        return f"{player}|{stat}|{ek}" if player else None

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
    def _game_conflict_key(sig: Dict[str, Any]) -> Optional[str]:
        mkt = sig.get("market_type", "")
        if mkt not in ("total", "spread", "moneyline"):
            return None
        ek = sig.get("event_key", "")
        # Normalize event_key to canonical TEAM@TEAM form for grouping
        m = re.search(r'([A-Z]{2,4})@([A-Z]{2,4})(?::\d+)?$', ek)
        if not m:
            return None
        away, home = _canon_team(m.group(1)), _canon_team(m.group(2))
        teams = f"{away}@{home}"
        return f"{teams}|{mkt}"

    game_conflict_map: Dict[str, List[int]] = {}
    for idx, (signal, score_data, tier) in enumerate(passed_raw):
        ck = _game_conflict_key(signal)
        if ck:
            game_conflict_map.setdefault(ck, []).append(idx)

    game_conflict_drop: set = set()
    for ck, indices in game_conflict_map.items():
        if len(indices) < 2:
            continue
        # Check if any two picks are on opposite sides
        sels = [(i, passed_raw[i][0].get("selection", ""), passed_raw[i][0].get("direction", "")) for i in indices]
        # For totals: directions OVER vs UNDER conflict
        # For spreads/ML: different team selections conflict
        mkt = passed_raw[indices[0]][0].get("market_type", "")
        if mkt == "total":
            dirs = set(_normalize_direction(passed_raw[i][0]) for i in indices)
            has_conflict = "OVER" in dirs and "UNDER" in dirs
        else:
            # spread or moneyline: conflicting if more than one distinct team
            teams = set(passed_raw[i][0].get("selection", "") for i in indices)
            has_conflict = len(teams) > 1
        if not has_conflict:
            continue
        # Keep best pick: tier first (A > B), then Wilson score
        tier_rank = {"A": 0, "B": 1, "C": 2, "D": 3}
        best_idx = min(indices, key=lambda i: (tier_rank.get(passed_raw[i][2], 9), -passed_raw[i][1]["wilson_score"]))
        best_sig = passed_raw[best_idx][0]
        best_tier = passed_raw[best_idx][2]
        dropped_tiers = []
        for i in indices:
            if i != best_idx:
                game_conflict_drop.add(i)
                dropped_tiers.append(passed_raw[i][2])
        kept_sel = best_sig.get("selection", "")
        print(f"  Game conflict {ck}: kept {kept_sel} (Tier {best_tier}), dropped {len(indices)-1} pick(s) (Tier {'/'.join(dropped_tiers)})")

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
    tier_order = {"A": 0, "B": 1}
    passed_raw.sort(key=lambda x: (tier_order.get(x[2], 9), -x[1]["wilson_score"]))

    tier_counts: Dict[str, int] = {"A": 0, "B": 0, "C": len(filtered), "D": 0}
    for f in filtered:
        if f.get("tier") == "D":
            tier_counts["D"] += 1
            tier_counts["C"] -= 1

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

    for tier, label in [("A", "STRONG EDGE"), ("B", "POSITIVE EDGE")]:
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
            print(f"    {p['rank']:3d}. {sel_str:30s} {combo:30s} {record_str:22s}{recent_str}  Wilson={wilson*100:.1f}%{adj_str} [{conf}/{level}]{pat_str}")
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
        active_pattern_registry = PATTERN_REGISTRY
        active_excluded_combos = EXCLUDED_COMBOS_BY_MARKET

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
    scorable, wrong_date_count = filter_wrong_date(scorable, date_str)
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
