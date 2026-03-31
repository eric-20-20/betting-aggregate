#!/usr/bin/env python3
"""Edge Miner: Find and rank profitable betting patterns with stability validation.

This script analyzes consensus patterns to find profitable edges, specifically:
- Source pair/triplet combinations (when A+B agree, when A+B+C agree)
- Stratified by market type, prop stat, odds bucket, line bucket
- With out-of-sample validation for stability scoring

Outputs:
- data/edges/edges_leaderboard.json (top patterns ranked by edge score)
- data/edges/edges_by_pair.csv (quick inspection)

Usage:
    python3 scripts/mine_edges.py
    python3 scripts/mine_edges.py --min-samples 20 --min-roi 0.02
    python3 scripts/mine_edges.py --sport NCAAB
"""
from __future__ import annotations

import argparse
import csv
import json
import random
import sys
from collections import defaultdict
from dataclasses import dataclass, asdict, field
from datetime import datetime
from itertools import combinations
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

EDGES_DIR = Path("data/edges")
EDGES_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class EdgePattern:
    """A betting edge pattern with performance metrics."""
    pattern_id: str
    sources: List[str]
    market_type: Optional[str] = None
    prop_stat: Optional[str] = None
    odds_bucket: Optional[str] = None
    line_bucket: Optional[str] = None
    team: Optional[str] = None      # Team being bet on (spreads)
    player: Optional[str] = None    # Player being bet on (player_props)
    expert: Optional[str] = None    # Expert/tipster making the pick

    # Core metrics
    n: int = 0
    wins: int = 0
    losses: int = 0
    pushes: int = 0
    hit_rate: float = 0.0
    roi: float = 0.0
    net_units: float = 0.0

    # Stability metrics (out-of-sample)
    oos_n: int = 0
    oos_hit_rate: float = 0.0
    oos_roi: float = 0.0
    stability_score: float = 0.0

    # Additional metrics
    avg_odds: Optional[float] = None
    max_drawdown: float = 0.0
    sharpe_ratio: float = 0.0

    # Edge score (composite ranking)
    edge_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def load_graded_data(sport: str = "NBA") -> List[Dict[str, Any]]:
    """Load graded signals/occurrences data, deduplicated by unique pick.

    The source data may have multiple rows for the same pick (different signal
    windows, expert combinations, etc.). We deduplicate to count each bet once.
    """
    if sport.upper() == "NCAAB":
        path = Path("data/analysis/ncaab/graded_occurrences_latest.jsonl")
    else:
        path = Path("data/analysis/graded_occurrences_latest.jsonl")

    rows = []
    if not path.exists():
        print(f"Warning: {path} not found")
        return rows

    seen_picks = set()
    duplicates_skipped = 0

    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    row = json.loads(line)
                    # Only include graded rows with terminal results
                    if row.get("grade_status") == "GRADED" and row.get("result") in ("WIN", "LOSS", "PUSH"):
                        # Create unique pick key: event + selection (the bet)
                        event_key = row.get("event_key", "")
                        selection = row.get("selection", "")
                        pick_key = f"{event_key}|{selection}"

                        if pick_key in seen_picks:
                            duplicates_skipped += 1
                            continue

                        seen_picks.add(pick_key)
                        rows.append(row)
                except json.JSONDecodeError:
                    continue

    if duplicates_skipped > 0:
        print(f"  (Deduplicated: skipped {duplicates_skipped} duplicate picks)")

    return rows


def load_backfill_consensus_grades() -> List[Dict[str, Any]]:
    """
    Load and grade backfill consensus data using the same logic as analyze_backfill_consensus.py.
    This provides additional consensus data beyond the daily pipeline.
    """
    import re
    from collections import defaultdict

    REPO_ROOT = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(REPO_ROOT))

    try:
        from src.results import nba_provider
    except ImportError:
        print("Warning: Could not import nba_provider")
        return []

    def normalize_team_code(team: str) -> str:
        team = (team or "").upper().strip()
        team_map = {
            "TIMBERWOLVES": "MIN", "PISTONS": "DET", "CELTICS": "BOS", "HEAT": "MIA",
            "LAKERS": "LAL", "CLIPPERS": "LAC", "WARRIORS": "GSW", "SUNS": "PHX",
            "NUGGETS": "DEN", "JAZZ": "UTA", "TRAIL BLAZERS": "POR", "BLAZERS": "POR",
            "THUNDER": "OKC", "MAVERICKS": "DAL", "ROCKETS": "HOU", "SPURS": "SAS",
            "GRIZZLIES": "MEM", "PELICANS": "NOP", "KINGS": "SAC", "HAWKS": "ATL",
            "HORNETS": "CHA", "BULLS": "CHI", "CAVALIERS": "CLE", "PACERS": "IND",
            "BUCKS": "MIL", "NETS": "BKN", "KNICKS": "NYK", "76ERS": "PHI",
            "SIXERS": "PHI", "RAPTORS": "TOR", "WIZARDS": "WAS", "MAGIC": "ORL",
            "MIN": "MIN", "DET": "DET", "BOS": "BOS", "MIA": "MIA", "LAL": "LAL",
            "LAC": "LAC", "GSW": "GSW", "GS": "GSW", "PHX": "PHX", "PHO": "PHX",
            "DEN": "DEN", "UTA": "UTA", "POR": "POR", "OKC": "OKC", "DAL": "DAL",
            "HOU": "HOU", "SAS": "SAS", "SA": "SAS", "MEM": "MEM", "NOP": "NOP",
            "NO": "NOP", "SAC": "SAC", "ATL": "ATL", "CHA": "CHA", "CHO": "CHA",
            "CHI": "CHI", "CLE": "CLE", "IND": "IND", "MIL": "MIL", "BKN": "BKN",
            "BRK": "BKN", "NYK": "NYK", "NY": "NYK", "PHI": "PHI", "TOR": "TOR",
            "WAS": "WAS", "ORL": "ORL",
        }
        return team_map.get(team, team[:3] if len(team) >= 3 else team)

    def parse_date_from_event_key(event_key: str):
        from datetime import date as dt_date
        m = re.match(r'NBA:(\d{4})(\d{2})(\d{2}):', event_key)
        if m:
            return dt_date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        return None

    def extract_teams_from_event_key(event_key: str):
        m = re.match(r'NBA:\d{8}:([A-Z]+)@([A-Z]+)', event_key)
        if m:
            return m.group(1), m.group(2)
        return None, None

    def extract_game_key(row):
        event = row.get("event", {})
        event_key = event.get("event_key", "")
        game_date = parse_date_from_event_key(event_key)
        away, home = extract_teams_from_event_key(event_key)
        if not away or not home:
            away = normalize_team_code(event.get("away_team", ""))
            home = normalize_team_code(event.get("home_team", ""))
        if not game_date:
            day_key = event.get("day_key", "")
            m = re.match(r'NBA:(\d{4}):(\d{2}):(\d{2})', day_key)
            if m:
                from datetime import date as dt_date
                game_date = dt_date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        if game_date and away and home:
            return f"{game_date.isoformat()}:{away}@{home}"
        return None

    def extract_spread_pick(row):
        market = row.get("market", {})
        event = row.get("event", {})
        line = market.get("line")
        if line is None:
            return None
        away = normalize_team_code(event.get("away_team", ""))
        home = normalize_team_code(event.get("home_team", ""))
        if not away or not home:
            event_key = event.get("event_key", "")
            ek_away, ek_home = extract_teams_from_event_key(event_key)
            if ek_away:
                away = normalize_team_code(ek_away)
            if ek_home:
                home = normalize_team_code(ek_home)
        direction = (market.get("direction") or "").upper()
        side = (market.get("side") or "").upper()
        selection = market.get("selection") or ""
        picked_team = None
        if direction in ("AWAY", "VISITOR") or side in ("AWAY", "VISITOR"):
            picked_team = away
        elif direction in ("HOME",) or side in ("HOME",):
            picked_team = home
        else:
            sel_upper = (selection or "").upper()
            if away in sel_upper:
                picked_team = away
            elif home in sel_upper:
                picked_team = home
        if picked_team:
            return (picked_team, float(line))
        return None

    def extract_total_pick(row):
        market = row.get("market", {})
        line = market.get("line")
        if line is None:
            return None
        direction = (market.get("direction") or "").upper()
        if not direction or direction in ("GAME_TOTAL", ""):
            direction = (market.get("side") or "").upper()
        if not direction or direction not in ("OVER", "UNDER"):
            direction = (market.get("selection") or "").upper()
        if direction in ("OVER", "UNDER"):
            return (direction, float(line))
        return None

    # Load data
    action_path = Path("out/normalized_action_nba_backfill.jsonl")
    if not action_path.exists():
        return []

    action_rows = []
    with open(action_path) as f:
        for line in f:
            if line.strip():
                try:
                    action_rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    action_spreads = [r for r in action_rows if r.get("market", {}).get("market_type") == "spread"]
    action_totals = [r for r in action_rows if r.get("market", {}).get("market_type") == "total"]

    # Load BetQL data
    betql_spreads = []
    for f in sorted(Path("data/history/betql_normalized/spreads").glob("*.jsonl")):
        if f.stat().st_size > 0:
            with open(f) as fp:
                for line in fp:
                    if line.strip():
                        try:
                            betql_spreads.append(json.loads(line))
                        except:
                            continue

    betql_totals = []
    for f in sorted(Path("data/history/betql_normalized/totals").glob("*.jsonl")):
        if f.stat().st_size > 0:
            with open(f) as fp:
                for line in fp:
                    if line.strip():
                        try:
                            betql_totals.append(json.loads(line))
                        except:
                            continue

    # Find consensus
    graded_rows = []
    from datetime import date as dt_date

    # Index Action and BetQL by game key + pick
    for market_type, action_data, betql_data, extract_fn in [
        ("spread", action_spreads, betql_spreads, extract_spread_pick),
        ("total", action_totals, betql_totals, extract_total_pick),
    ]:
        action_by_game = defaultdict(list)
        for row in action_data:
            gk = extract_game_key(row)
            pick = extract_fn(row)
            if gk and pick:
                action_by_game[gk].append((pick, row))

        betql_by_game = defaultdict(list)
        for row in betql_data:
            gk = extract_game_key(row)
            pick = extract_fn(row)
            if gk and pick:
                betql_by_game[gk].append((pick, row))

        # Find consensus
        for gk in action_by_game:
            if gk not in betql_by_game:
                continue

            for a_pick, a_row in action_by_game[gk]:
                for b_pick, b_row in betql_by_game[gk]:
                    # Check if picks agree
                    if market_type == "spread":
                        if a_pick[0] == b_pick[0] and abs(a_pick[1] - b_pick[1]) <= 1.0:
                            # Same team, similar line
                            pass
                        else:
                            continue
                    else:  # total
                        if a_pick[0] == b_pick[0] and abs(a_pick[1] - b_pick[1]) <= 1.0:
                            # Same direction, similar line
                            pass
                        else:
                            continue

                    # Grade the consensus pick
                    parts = gk.split(":")
                    date_str = parts[0]
                    matchup = parts[1]
                    away, home = matchup.split("@")
                    game_date = dt_date.fromisoformat(date_str)

                    if game_date > dt_date.today():
                        continue

                    try:
                        game_result = nba_provider.fetch_game_result(
                            event_key=f"NBA:{game_date.year}:{game_date.month:02d}:{game_date.day:02d}:{away}@{home}",
                            away=away,
                            home=home,
                            date_str=date_str,
                        )

                        if not game_result or game_result.get("status") == "ERROR":
                            continue

                        away_score = game_result.get("away_score")
                        home_score = game_result.get("home_score")
                        if away_score is None or home_score is None:
                            continue

                        # Grade
                        if market_type == "spread":
                            picked_team = a_pick[0]
                            avg_line = (a_pick[1] + b_pick[1]) / 2
                            if picked_team == away:
                                adjusted = away_score + avg_line
                                opponent = home_score
                            else:
                                adjusted = home_score + avg_line
                                opponent = away_score
                            if adjusted > opponent:
                                result = "WIN"
                            elif adjusted == opponent:
                                result = "PUSH"
                            else:
                                result = "LOSS"
                        else:  # total
                            direction = a_pick[0]
                            avg_line = (a_pick[1] + b_pick[1]) / 2
                            total_score = away_score + home_score
                            if direction == "OVER":
                                if total_score > avg_line:
                                    result = "WIN"
                                elif total_score == avg_line:
                                    result = "PUSH"
                                else:
                                    result = "LOSS"
                            else:
                                if total_score < avg_line:
                                    result = "WIN"
                                elif total_score == avg_line:
                                    result = "PUSH"
                                else:
                                    result = "LOSS"

                        graded_rows.append({
                            "day_key": f"NBA:{game_date.year}:{game_date.month:02d}:{game_date.day:02d}",
                            "sources_combo": "action|betql",
                            "market_type": market_type,
                            "result": result,
                            "grade_status": "GRADED",
                            "line": avg_line,
                        })
                        break  # Found a consensus, move to next action pick

                    except Exception:
                        continue

    return graded_rows


def get_odds_bucket(odds: Optional[float]) -> str:
    """Categorize odds into buckets."""
    if odds is None:
        return "unknown"
    try:
        odds = float(odds)
    except (ValueError, TypeError):
        return "unknown"

    if odds <= -200:
        return "heavy_fav"  # -200 or worse
    elif odds <= -140:
        return "moderate_fav"  # -140 to -199
    elif odds <= -105:
        return "slight_fav"  # -105 to -139
    elif odds <= 105:
        return "pick_em"  # -104 to +105
    elif odds <= 150:
        return "slight_dog"  # +106 to +150
    elif odds <= 200:
        return "moderate_dog"  # +151 to +200
    else:
        return "heavy_dog"  # +201 or better


def get_line_bucket(line: Optional[float], market_type: str) -> str:
    """Categorize line into buckets based on market type."""
    if line is None:
        return "unknown"
    try:
        line = float(line)
    except (ValueError, TypeError):
        return "unknown"

    if market_type == "spread":
        abs_line = abs(line)
        if abs_line <= 2.5:
            return "tight"  # 0 to 2.5
        elif abs_line <= 5.5:
            return "small"  # 3 to 5.5
        elif abs_line <= 9.5:
            return "medium"  # 6 to 9.5
        else:
            return "large"  # 10+
    elif market_type == "total":
        if line <= 210:
            return "low"
        elif line <= 225:
            return "medium"
        elif line <= 240:
            return "high"
        else:
            return "very_high"
    elif market_type == "player_prop":
        if line <= 10:
            return "low"
        elif line <= 20:
            return "medium"
        elif line <= 30:
            return "high"
        else:
            return "very_high"

    return "unknown"


def normalize_prop_stat(stat: Optional[str]) -> str:
    """Normalize prop stat names."""
    if not stat:
        return "unknown"

    stat = stat.lower().strip()

    stat_map = {
        "points": "PTS",
        "pts": "PTS",
        "rebounds": "REB",
        "rebs": "REB",
        "reb": "REB",
        "assists": "AST",
        "ast": "AST",
        "threes": "3PM",
        "3pm": "3PM",
        "three pointers made": "3PM",
        "steals": "STL",
        "stl": "STL",
        "blocks": "BLK",
        "blk": "BLK",
        "turnovers": "TO",
        "to": "TO",
        "tov": "TO",
        "pra": "PRA",
        "pts + reb + ast": "PRA",
        "pa": "PA",
        "pts + ast": "PA",
        "pr": "PR",
        "pts + reb": "PR",
        "ra": "RA",
        "reb + ast": "RA",
    }

    return stat_map.get(stat, stat.upper()[:5])


# Team code normalization (module-level version)
TEAM_CODE_MAP = {
    "TIMBERWOLVES": "MIN", "PISTONS": "DET", "CELTICS": "BOS", "HEAT": "MIA",
    "LAKERS": "LAL", "CLIPPERS": "LAC", "WARRIORS": "GSW", "SUNS": "PHX",
    "NUGGETS": "DEN", "JAZZ": "UTA", "TRAIL BLAZERS": "POR", "BLAZERS": "POR",
    "THUNDER": "OKC", "MAVERICKS": "DAL", "ROCKETS": "HOU", "SPURS": "SAS",
    "GRIZZLIES": "MEM", "PELICANS": "NOP", "KINGS": "SAC", "HAWKS": "ATL",
    "HORNETS": "CHA", "BULLS": "CHI", "CAVALIERS": "CLE", "PACERS": "IND",
    "BUCKS": "MIL", "NETS": "BKN", "KNICKS": "NYK", "76ERS": "PHI",
    "SIXERS": "PHI", "RAPTORS": "TOR", "WIZARDS": "WAS", "MAGIC": "ORL",
    "MIN": "MIN", "DET": "DET", "BOS": "BOS", "MIA": "MIA", "LAL": "LAL",
    "LAC": "LAC", "GSW": "GSW", "GS": "GSW", "PHX": "PHX", "PHO": "PHX",
    "DEN": "DEN", "UTA": "UTA", "POR": "POR", "OKC": "OKC", "DAL": "DAL",
    "HOU": "HOU", "SAS": "SAS", "SA": "SAS", "MEM": "MEM", "NOP": "NOP",
    "NO": "NOP", "SAC": "SAC", "ATL": "ATL", "CHA": "CHA", "CHO": "CHA",
    "CHI": "CHI", "CLE": "CLE", "IND": "IND", "MIL": "MIL", "BKN": "BKN",
    "BRK": "BKN", "NYK": "NYK", "NY": "NYK", "PHI": "PHI", "TOR": "TOR",
    "WAS": "WAS", "ORL": "ORL",
}


def normalize_team_code_global(team: str) -> Optional[str]:
    """Normalize team code to standard 3-letter format."""
    if not team:
        return None
    team = team.upper().strip()
    return TEAM_CODE_MAP.get(team, team[:3] if len(team) >= 3 else None)


def extract_team(row: Dict[str, Any], market_type: str) -> Optional[str]:
    """Extract the team being bet on from a row.

    For spreads: the team being picked (spread_team or selection)
    For totals/player_props: return None (don't pick a specific team)
    """
    if market_type != "spread":
        return None

    # Primary: spread_team field
    team = row.get("spread_team")
    if team and team not in ("spread_conflict",):
        return normalize_team_code_global(team)

    # Fallback: selection field (sometimes contains team code)
    selection = row.get("selection", "")
    if selection and selection not in ("spread_conflict",) and not selection.startswith("NBA:"):
        return normalize_team_code_global(selection)

    return None


def extract_player(row: Dict[str, Any], market_type: str) -> Optional[str]:
    """Extract the player being bet on from a player_prop row."""
    if market_type != "player_prop":
        return None

    player_key = row.get("player_key")
    if player_key:
        # Strip NBA: prefix and normalize
        slug = player_key.replace("NBA:", "").lower().strip()
        # Replace special chars with underscore
        import re
        slug = re.sub(r'[^a-z0-9_]+', '_', slug).strip('_')
        return slug if slug else None

    # Fallback: player_name
    player_name = row.get("player_name")
    if player_name:
        import re
        slug = player_name.lower().strip()
        slug = re.sub(r'[^a-z0-9]+', '_', slug).strip('_')
        return slug if slug else None

    return None


def normalize_expert_slug(expert_str: str) -> Optional[str]:
    """Normalize an expert string to a clean slug.

    Handles various formats:
    - "action:/picks/profile/HPBasketball" -> "hpbasketball"
    - "covers:Douglas Farmer" -> "douglas_farmer"
    - "BetQL Model" -> "betql_model"
    - "sportscapping:/john-martin-free-picks-archive.html" -> skip
    """
    import re

    if not expert_str:
        return None

    expert_str = expert_str.strip()

    # Handle action profile URLs
    if expert_str.startswith("action:/picks/profile/"):
        slug = expert_str.replace("action:/picks/profile/", "")
        slug = slug.lower().replace("-", "_")
        return slug if slug else None

    # Handle action: prefix (other formats)
    if expert_str.startswith("action:"):
        name = expert_str.replace("action:", "").strip()
        slug = re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')
        return slug if slug else None

    # Handle covers: prefix
    if expert_str.startswith("covers:"):
        name = expert_str.replace("covers:", "").strip()
        slug = re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')
        return slug if slug else None

    # Handle betql: prefix
    if expert_str.startswith("betql:"):
        name = expert_str.replace("betql:", "").strip()
        slug = re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')
        return slug if slug else None

    # Handle sportscapping URLs - skip these (they're URLs not expert names)
    if expert_str.startswith("sportscapping:"):
        return None

    # Handle sportsline: prefix
    if expert_str.startswith("sportsline:"):
        name = expert_str.replace("sportsline:", "").strip()
        slug = re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')
        return slug if slug else None

    # Handle plain names (most common for direct expert_name field)
    slug = re.sub(r'[^a-z0-9]+', '_', expert_str.lower()).strip('_')
    return slug if slug else None


def extract_experts(row: Dict[str, Any]) -> List[str]:
    """Extract and normalize all experts from a row.

    Returns a list of normalized expert slugs.
    """
    experts_raw = row.get("experts", [])
    if not experts_raw:
        # Check for single expert_name field
        expert_name = row.get("expert_name")
        if expert_name:
            slug = normalize_expert_slug(expert_name)
            return [slug] if slug else []
        return []

    normalized = []
    for exp in experts_raw:
        if isinstance(exp, str):
            slug = normalize_expert_slug(exp)
            if slug:
                normalized.append(slug)

    return normalized


def american_odds_to_units(odds: Optional[float], outcome: str) -> float:
    """Calculate units won/lost based on American odds."""
    if odds is None:
        odds = -110.0
    try:
        odds = float(odds)
    except (ValueError, TypeError):
        odds = -110.0

    if outcome == "PUSH":
        return 0.0
    if outcome == "LOSS":
        return -1.0
    if outcome == "WIN":
        if odds > 0:
            return odds / 100.0
        else:
            return 100.0 / abs(odds)
    return 0.0


def calculate_max_drawdown(results: List[float]) -> float:
    """Calculate maximum drawdown from a series of unit results."""
    if not results:
        return 0.0

    cumsum = 0.0
    peak = 0.0
    max_dd = 0.0

    for r in results:
        cumsum += r
        if cumsum > peak:
            peak = cumsum
        dd = peak - cumsum
        if dd > max_dd:
            max_dd = dd

    return max_dd


def calculate_sharpe_ratio(results: List[float]) -> float:
    """Calculate Sharpe-like ratio (mean / std)."""
    if len(results) < 2:
        return 0.0

    import statistics
    mean = statistics.mean(results)
    try:
        std = statistics.stdev(results)
    except statistics.StatisticsError:
        return 0.0

    if std == 0:
        return 0.0 if mean == 0 else float('inf') if mean > 0 else float('-inf')

    return mean / std


def parse_sources_combo(sources_combo: str) -> List[str]:
    """Parse sources_combo string into list of sources."""
    if not sources_combo:
        return []
    return sorted(sources_combo.split("|"))


def generate_pattern_id(
    sources: List[str],
    market_type: Optional[str] = None,
    prop_stat: Optional[str] = None,
    odds_bucket: Optional[str] = None,
    line_bucket: Optional[str] = None,
    team: Optional[str] = None,
    player: Optional[str] = None,
    expert: Optional[str] = None,
) -> str:
    """Generate unique pattern ID."""
    parts = ["|".join(sorted(sources))]
    if market_type:
        parts.append(f"mkt:{market_type}")
    if prop_stat:
        parts.append(f"stat:{prop_stat}")
    if odds_bucket:
        parts.append(f"odds:{odds_bucket}")
    if line_bucket:
        parts.append(f"line:{line_bucket}")
    if team:
        parts.append(f"team:{team}")
    if player:
        parts.append(f"player:{player}")
    if expert:
        parts.append(f"expert:{expert}")
    return "__".join(parts)


def split_train_test(
    rows: List[Dict[str, Any]],
    test_ratio: float = 0.3,
    seed: int = 42,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Split data into train/test sets by time (out-of-sample)."""
    if not rows:
        return [], []

    # Sort by date
    def get_date(row: Dict[str, Any]) -> str:
        return row.get("day_key", "") or row.get("event_key", "")[:15] or "9999"

    sorted_rows = sorted(rows, key=get_date)

    # Time-based split (last N% is test set)
    split_idx = int(len(sorted_rows) * (1 - test_ratio))

    return sorted_rows[:split_idx], sorted_rows[split_idx:]


def mine_patterns(
    rows: List[Dict[str, Any]],
    min_sources: int = 2,
    max_sources: int = 3,
    stratify_market: bool = True,
    stratify_prop_stat: bool = True,
    stratify_odds: bool = False,
    stratify_line: bool = False,
    stratify_team: bool = False,
    stratify_player: bool = False,
    stratify_expert: bool = False,
    include_single_source: bool = False,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Group rows by pattern (source combinations + stratifications).

    Args:
        include_single_source: If True, also create patterns for each individual source
            that participated in a pick. E.g., an action|betql consensus pick will count
            toward "action" pattern, "betql" pattern, AND "action|betql" pattern.

    Returns: Dict[pattern_id -> List[rows matching pattern]]
    """
    patterns: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for row in rows:
        sources_combo = row.get("sources_combo", "")
        sources = parse_sources_combo(sources_combo)

        if len(sources) < min_sources:
            continue

        market_type = row.get("market_type", "").lower() if stratify_market else None

        prop_stat = None
        if stratify_prop_stat and market_type == "player_prop":
            prop_stat = normalize_prop_stat(row.get("atomic_stat") or row.get("stat_key"))

        odds_bucket = None
        if stratify_odds:
            odds_bucket = get_odds_bucket(row.get("best_odds") or row.get("odds"))

        line_bucket = None
        if stratify_line:
            line_bucket = get_line_bucket(row.get("line"), market_type or "")

        # New stratifications
        team = None
        if stratify_team:
            team = extract_team(row, market_type or "")

        player = None
        if stratify_player:
            player = extract_player(row, market_type or "")

        # Expert stratification (handles multi-expert rows)
        experts_list = []
        if stratify_expert:
            experts_list = extract_experts(row)

        # If include_single_source, also create patterns for each individual source
        # This allows tracking "how does Action perform overall" including consensus picks
        if include_single_source:
            for source in sources:
                pattern_id = generate_pattern_id(
                    [source],
                    market_type=market_type,
                    prop_stat=prop_stat,
                    odds_bucket=odds_bucket,
                    line_bucket=line_bucket,
                    team=team,
                    player=player,
                    expert=None,
                )
                patterns[pattern_id].append(row)

                # Also create expert-specific single-source patterns
                if stratify_expert and experts_list:
                    for expert in experts_list:
                        pattern_id = generate_pattern_id(
                            [source],
                            market_type=market_type,
                            prop_stat=prop_stat,
                            odds_bucket=odds_bucket,
                            line_bucket=line_bucket,
                            team=team,
                            player=player,
                            expert=expert,
                        )
                        patterns[pattern_id].append(row)

        # Generate all source combinations (pairs, triplets, etc.)
        for k in range(min_sources, min(max_sources + 1, len(sources) + 1)):
            for combo in combinations(sources, k):
                combo_list = list(combo)

                # Always create the aggregate pattern (without expert)
                pattern_id = generate_pattern_id(
                    combo_list,
                    market_type=market_type,
                    prop_stat=prop_stat,
                    odds_bucket=odds_bucket,
                    line_bucket=line_bucket,
                    team=team,
                    player=player,
                    expert=None,
                )
                patterns[pattern_id].append(row)

                # Additionally create expert-specific patterns if requested
                if stratify_expert and experts_list:
                    for expert in experts_list:
                        pattern_id = generate_pattern_id(
                            combo_list,
                            market_type=market_type,
                            prop_stat=prop_stat,
                            odds_bucket=odds_bucket,
                            line_bucket=line_bucket,
                            team=team,
                            player=player,
                            expert=expert,
                        )
                        patterns[pattern_id].append(row)

    return patterns


def evaluate_pattern(
    pattern_id: str,
    train_rows: List[Dict[str, Any]],
    test_rows: List[Dict[str, Any]],
) -> Optional[EdgePattern]:
    """Evaluate a pattern on train and test data."""
    if not train_rows:
        return None

    # Parse pattern_id
    parts = pattern_id.split("__")
    sources = parts[0].split("|")
    market_type = None
    prop_stat = None
    odds_bucket = None
    line_bucket = None
    team = None
    player = None
    expert = None

    for part in parts[1:]:
        if part.startswith("mkt:"):
            market_type = part[4:]
        elif part.startswith("stat:"):
            prop_stat = part[5:]
        elif part.startswith("odds:"):
            odds_bucket = part[5:]
        elif part.startswith("line:"):
            line_bucket = part[5:]
        elif part.startswith("team:"):
            team = part[5:]
        elif part.startswith("player:"):
            player = part[7:]
        elif part.startswith("expert:"):
            expert = part[7:]

    # Evaluate on training data
    train_wins = sum(1 for r in train_rows if r.get("result") == "WIN")
    train_losses = sum(1 for r in train_rows if r.get("result") == "LOSS")
    train_pushes = sum(1 for r in train_rows if r.get("result") == "PUSH")
    train_n = train_wins + train_losses + train_pushes

    if train_n == 0:
        return None

    train_hit_rate = train_wins / (train_wins + train_losses) if (train_wins + train_losses) > 0 else 0

    # Calculate units
    train_results = []
    train_odds_sum = 0.0
    train_odds_count = 0
    for r in train_rows:
        odds = r.get("best_odds") or r.get("odds")
        units = american_odds_to_units(odds, r.get("result", ""))
        train_results.append(units)
        if odds is not None:
            try:
                train_odds_sum += float(odds)
                train_odds_count += 1
            except (ValueError, TypeError):
                pass

    train_net_units = sum(train_results)
    train_roi = train_net_units / train_n if train_n > 0 else 0
    train_avg_odds = train_odds_sum / train_odds_count if train_odds_count > 0 else None
    train_max_dd = calculate_max_drawdown(train_results)
    train_sharpe = calculate_sharpe_ratio(train_results)

    # Evaluate on test data (out-of-sample)
    test_wins = sum(1 for r in test_rows if r.get("result") == "WIN")
    test_losses = sum(1 for r in test_rows if r.get("result") == "LOSS")
    test_pushes = sum(1 for r in test_rows if r.get("result") == "PUSH")
    test_n = test_wins + test_losses + test_pushes

    test_hit_rate = test_wins / (test_wins + test_losses) if (test_wins + test_losses) > 0 else 0

    test_results = []
    for r in test_rows:
        odds = r.get("best_odds") or r.get("odds")
        units = american_odds_to_units(odds, r.get("result", ""))
        test_results.append(units)

    test_net_units = sum(test_results)
    test_roi = test_net_units / test_n if test_n > 0 else 0

    # Calculate stability score
    # High stability = similar performance in-sample and out-of-sample
    stability_score = calculate_stability_score(
        train_hit_rate, test_hit_rate,
        train_roi, test_roi,
        train_n, test_n,
    )

    # Calculate edge score (composite ranking)
    edge_score = calculate_edge_score(
        train_n=train_n,
        train_hit_rate=train_hit_rate,
        train_roi=train_roi,
        test_n=test_n,
        test_hit_rate=test_hit_rate,
        test_roi=test_roi,
        stability_score=stability_score,
        sharpe_ratio=train_sharpe,
    )

    return EdgePattern(
        pattern_id=pattern_id,
        sources=sources,
        market_type=market_type,
        prop_stat=prop_stat,
        odds_bucket=odds_bucket,
        line_bucket=line_bucket,
        team=team,
        player=player,
        expert=expert,
        n=train_n,
        wins=train_wins,
        losses=train_losses,
        pushes=train_pushes,
        hit_rate=round(train_hit_rate, 4),
        roi=round(train_roi, 4),
        net_units=round(train_net_units, 2),
        avg_odds=round(train_avg_odds, 1) if train_avg_odds else None,
        max_drawdown=round(train_max_dd, 2),
        sharpe_ratio=round(train_sharpe, 3),
        oos_n=test_n,
        oos_hit_rate=round(test_hit_rate, 4),
        oos_roi=round(test_roi, 4),
        stability_score=round(stability_score, 3),
        edge_score=round(edge_score, 3),
    )


def calculate_stability_score(
    train_hr: float,
    test_hr: float,
    train_roi: float,
    test_roi: float,
    train_n: int,
    test_n: int,
) -> float:
    """
    Calculate stability score based on train/test consistency.

    Score from 0 to 1, where:
    - 1.0 = perfect consistency
    - 0.0 = complete inconsistency (train profitable, test losing)
    """
    if test_n < 5:
        # Not enough OOS data to assess stability
        return 0.5

    # Hit rate consistency (penalize large differences)
    hr_diff = abs(train_hr - test_hr)
    hr_score = max(0, 1 - hr_diff * 2)  # 0.5 diff -> 0 score

    # ROI consistency (penalize sign changes heavily)
    if train_roi > 0 and test_roi > 0:
        # Both profitable
        roi_ratio = min(train_roi, test_roi) / max(train_roi, test_roi) if max(train_roi, test_roi) > 0 else 1
        roi_score = 0.5 + 0.5 * roi_ratio
    elif train_roi <= 0 and test_roi <= 0:
        # Both unprofitable (consistent, but not good)
        roi_score = 0.3
    else:
        # Sign change (worst case)
        roi_score = 0.0

    # Sample size bonus (more OOS data = more confident in stability)
    sample_score = min(1.0, test_n / 30)  # Max out at 30 samples

    # Combined score
    return 0.4 * hr_score + 0.4 * roi_score + 0.2 * sample_score


def calculate_edge_score(
    train_n: int,
    train_hit_rate: float,
    train_roi: float,
    test_n: int,
    test_hit_rate: float,
    test_roi: float,
    stability_score: float,
    sharpe_ratio: float,
) -> float:
    """
    Calculate composite edge score for ranking patterns.

    Factors:
    - Sample size (need enough bets)
    - ROI (in-sample)
    - ROI (out-of-sample, heavily weighted)
    - Stability
    - Sharpe ratio
    """
    # Sample size factor (diminishing returns)
    import math
    sample_factor = min(1.0, math.log10(max(train_n, 1) + 1) / 2.5)  # Max at ~300 samples

    # ROI factors
    train_roi_factor = max(0, min(1, (train_roi + 0.1) / 0.3))  # 0 at -10% ROI, 1 at +20% ROI
    oos_roi_factor = max(0, min(1, (test_roi + 0.1) / 0.3)) if test_n >= 5 else 0.5

    # Sharpe factor (risk-adjusted returns)
    sharpe_factor = max(0, min(1, (sharpe_ratio + 0.5) / 1.5))

    # Combine with weights
    # Out-of-sample performance and stability are most important
    edge_score = (
        0.15 * sample_factor +
        0.15 * train_roi_factor +
        0.30 * oos_roi_factor +
        0.25 * stability_score +
        0.15 * sharpe_factor
    )

    # Bonus for profitable OOS
    if test_n >= 10 and test_roi > 0.02:
        edge_score *= 1.2

    # Penalty for losing OOS
    if test_n >= 10 and test_roi < -0.05:
        edge_score *= 0.5

    return edge_score


def main():
    parser = argparse.ArgumentParser(description="Mine betting edges from consensus data")
    parser.add_argument("--sport", type=str, default="NBA", choices=["NBA", "NCAAB"])
    parser.add_argument("--min-samples", type=int, default=15,
                       help="Minimum sample size for a pattern (default: 15)")
    parser.add_argument("--min-roi", type=float, default=0.0,
                       help="Minimum ROI to include in leaderboard (default: 0)")
    parser.add_argument("--test-ratio", type=float, default=0.3,
                       help="Ratio of data for out-of-sample testing (default: 0.3)")
    parser.add_argument("--stratify-odds", action="store_true",
                       help="Also stratify by odds bucket")
    parser.add_argument("--stratify-line", action="store_true",
                       help="Also stratify by line bucket")
    parser.add_argument("--stratify-team", action="store_true",
                       help="Stratify by team being picked (spreads only)")
    parser.add_argument("--stratify-player", action="store_true",
                       help="Stratify by player (player_props only)")
    parser.add_argument("--stratify-expert", action="store_true",
                       help="Stratify by expert/tipster")
    parser.add_argument("--include-single-source", action="store_true",
                       help="Include single-source patterns (e.g., 'action' gets credit for action|betql picks)")
    parser.add_argument("--min-samples-team", type=int, default=10,
                       help="Minimum samples per team pattern (default: 10)")
    parser.add_argument("--min-samples-player", type=int, default=8,
                       help="Minimum samples per player pattern (default: 8)")
    parser.add_argument("--min-samples-expert", type=int, default=15,
                       help="Minimum samples per expert pattern (default: 15)")
    parser.add_argument("--top-n", type=int, default=50,
                       help="Number of top patterns to include in leaderboard (default: 50)")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    print(f"Loading graded data for {args.sport}...")
    rows = load_graded_data(args.sport)
    print(f"  Loaded {len(rows)} graded rows from pipeline")

    # Also load backfill consensus data
    if args.sport.upper() == "NBA":
        print("Loading backfill consensus data...")
        backfill_rows = load_backfill_consensus_grades()
        print(f"  Loaded {len(backfill_rows)} backfill consensus grades")

        # Deduplicate when combining (backfill may overlap with pipeline)
        seen_picks = set()
        for row in rows:
            day_key = row.get("day_key", "")
            market_type = row.get("market_type", "")
            # Use day_key + market_type + sources as dedup key for combined data
            pick_key = f"{day_key}|{market_type}|{row.get('sources_combo', '')}"
            seen_picks.add(pick_key)

        added = 0
        for row in backfill_rows:
            day_key = row.get("day_key", "")
            market_type = row.get("market_type", "")
            pick_key = f"{day_key}|{market_type}|{row.get('sources_combo', '')}"
            if pick_key not in seen_picks:
                rows.append(row)
                seen_picks.add(pick_key)
                added += 1

        print(f"  Added {added} unique backfill grades (skipped {len(backfill_rows) - added} overlaps)")
        print(f"  Total: {len(rows)} rows")

    if not rows:
        print("No data to analyze. Run grading scripts first.")
        return

    # Split into train/test
    print(f"\nSplitting data (test ratio: {args.test_ratio})...")
    train_rows, test_rows = split_train_test(rows, test_ratio=args.test_ratio)
    print(f"  Train: {len(train_rows)} rows")
    print(f"  Test: {len(test_rows)} rows")

    # Mine patterns
    print("\nMining patterns...")
    train_patterns = mine_patterns(
        train_rows,
        min_sources=2,
        max_sources=3,
        stratify_market=True,
        stratify_prop_stat=True,
        stratify_odds=args.stratify_odds,
        stratify_line=args.stratify_line,
        stratify_team=args.stratify_team,
        stratify_player=args.stratify_player,
        stratify_expert=args.stratify_expert,
        include_single_source=args.include_single_source,
    )

    test_patterns = mine_patterns(
        test_rows,
        min_sources=2,
        max_sources=3,
        stratify_market=True,
        stratify_prop_stat=True,
        stratify_odds=args.stratify_odds,
        stratify_line=args.stratify_line,
        stratify_team=args.stratify_team,
        stratify_player=args.stratify_player,
        stratify_expert=args.stratify_expert,
        include_single_source=args.include_single_source,
    )

    print(f"  Found {len(train_patterns)} unique patterns in training data")

    # Evaluate patterns
    print("\nEvaluating patterns...")
    edges: List[EdgePattern] = []

    for pattern_id, pattern_train_rows in train_patterns.items():
        # Determine appropriate minimum sample size based on stratification
        min_samples = args.min_samples
        if args.stratify_team and "__team:" in pattern_id:
            min_samples = max(min_samples, args.min_samples_team)
        if args.stratify_player and "__player:" in pattern_id:
            min_samples = max(min_samples, args.min_samples_player)
        if args.stratify_expert and "__expert:" in pattern_id:
            min_samples = max(min_samples, args.min_samples_expert)

        if len(pattern_train_rows) < min_samples:
            continue

        pattern_test_rows = test_patterns.get(pattern_id, [])

        edge = evaluate_pattern(pattern_id, pattern_train_rows, pattern_test_rows)
        if edge and edge.roi >= args.min_roi:
            edges.append(edge)

    print(f"  Evaluated {len(edges)} patterns meeting criteria")

    # Sort by edge score
    edges.sort(key=lambda e: e.edge_score, reverse=True)

    # Display top patterns
    print(f"\n{'='*80}")
    print("TOP EDGE PATTERNS (by edge score)")
    print(f"{'='*80}")

    print(f"\n{'Pattern':<45} {'N':>5} {'Hit%':>6} {'ROI':>7} {'OOS_N':>6} {'OOS%':>6} {'OOS_ROI':>8} {'Stab':>5} {'Score':>6}")
    print("-" * 100)

    for edge in edges[:args.top_n]:
        pattern_short = edge.pattern_id[:44]
        print(f"{pattern_short:<45} {edge.n:>5} {edge.hit_rate*100:>5.1f}% {edge.roi*100:>6.1f}% "
              f"{edge.oos_n:>6} {edge.oos_hit_rate*100:>5.1f}% {edge.oos_roi*100:>7.1f}% "
              f"{edge.stability_score:>5.2f} {edge.edge_score:>6.3f}")

    # Filter to profitable patterns with good stability
    profitable_edges = [e for e in edges if e.roi > 0 and e.oos_roi > 0 and e.stability_score > 0.5]

    print(f"\n{'='*80}")
    print(f"PROFITABLE PATTERNS (ROI > 0, OOS_ROI > 0, Stability > 0.5)")
    print(f"{'='*80}")
    print(f"Found {len(profitable_edges)} patterns")

    for edge in profitable_edges[:20]:
        print(f"\n  {edge.pattern_id}")
        print(f"    Sources: {', '.join(edge.sources)}")
        if edge.market_type:
            print(f"    Market: {edge.market_type}")
        if edge.prop_stat:
            print(f"    Prop Stat: {edge.prop_stat}")
        if edge.team:
            print(f"    Team: {edge.team}")
        if edge.player:
            print(f"    Player: {edge.player}")
        if edge.expert:
            print(f"    Expert: {edge.expert}")
        print(f"    In-Sample: {edge.n} bets, {edge.hit_rate*100:.1f}% hit, {edge.roi*100:.1f}% ROI")
        print(f"    Out-of-Sample: {edge.oos_n} bets, {edge.oos_hit_rate*100:.1f}% hit, {edge.oos_roi*100:.1f}% ROI")
        print(f"    Stability: {edge.stability_score:.2f}, Edge Score: {edge.edge_score:.3f}")

    # Save outputs
    print(f"\n{'='*80}")
    print("SAVING OUTPUTS")
    print(f"{'='*80}")

    # Save JSON leaderboard
    leaderboard_path = EDGES_DIR / "edges_leaderboard.json"
    leaderboard_data = {
        "meta": {
            "sport": args.sport,
            "generated_at_utc": datetime.utcnow().isoformat(),
            "train_rows": len(train_rows),
            "test_rows": len(test_rows),
            "test_ratio": args.test_ratio,
            "min_samples": args.min_samples,
            "patterns_evaluated": len(edges),
            "profitable_patterns": len(profitable_edges),
        },
        "top_edges": [e.to_dict() for e in edges[:args.top_n]],
        "profitable_edges": [e.to_dict() for e in profitable_edges],
    }

    with open(leaderboard_path, "w") as f:
        json.dump(leaderboard_data, f, indent=2)
    print(f"  Saved: {leaderboard_path}")

    # Save CSV for quick inspection
    csv_path = EDGES_DIR / "edges_by_pair.csv"
    with open(csv_path, "w", newline="") as f:
        fieldnames = [
            "pattern_id", "sources", "market_type", "prop_stat",
            "team", "player", "expert",
            "n", "hit_rate", "roi", "net_units",
            "oos_n", "oos_hit_rate", "oos_roi",
            "stability_score", "edge_score",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for edge in edges:
            writer.writerow({
                "pattern_id": edge.pattern_id,
                "sources": "|".join(edge.sources),
                "market_type": edge.market_type or "",
                "prop_stat": edge.prop_stat or "",
                "team": edge.team or "",
                "player": edge.player or "",
                "expert": edge.expert or "",
                "n": edge.n,
                "hit_rate": f"{edge.hit_rate:.4f}",
                "roi": f"{edge.roi:.4f}",
                "net_units": f"{edge.net_units:.2f}",
                "oos_n": edge.oos_n,
                "oos_hit_rate": f"{edge.oos_hit_rate:.4f}",
                "oos_roi": f"{edge.oos_roi:.4f}",
                "stability_score": f"{edge.stability_score:.3f}",
                "edge_score": f"{edge.edge_score:.3f}",
            })
    print(f"  Saved: {csv_path}")

    # Summary stats
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")

    print(f"\nTotal patterns evaluated: {len(edges)}")
    print(f"Profitable in-sample: {len([e for e in edges if e.roi > 0])}")
    print(f"Profitable out-of-sample: {len([e for e in edges if e.oos_roi > 0])}")
    print(f"Both profitable (stable): {len(profitable_edges)}")

    if profitable_edges:
        best = profitable_edges[0]
        print(f"\nBest stable edge:")
        print(f"  Pattern: {best.pattern_id}")
        print(f"  In-Sample: {best.n} bets, {best.roi*100:.1f}% ROI")
        print(f"  Out-of-Sample: {best.oos_n} bets, {best.oos_roi*100:.1f}% ROI")


if __name__ == "__main__":
    main()
