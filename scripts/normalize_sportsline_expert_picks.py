#!/usr/bin/env python3
"""
Normalize SportsLine expert picks to standard schema.

These picks come with pre-graded results (Win/Loss/Push).

Usage:
    python3 scripts/normalize_sportsline_expert_picks.py
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from store import data_store, get_data_store, NBA_SPORT, NCAAB_SPORT

# Sport-specific paths
def get_input_path(sport: str) -> Path:
    suffix = sport.lower()
    return REPO_ROOT / "out" / f"raw_sportsline_{suffix}_expert_picks.jsonl"

def get_output_path(sport: str) -> Path:
    suffix = sport.lower()
    return REPO_ROOT / "out" / f"normalized_sportsline_{suffix}_expert_picks.jsonl"


def sha256_digest(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


def parse_datetime(ts: Optional[str]) -> Optional[datetime]:
    """Parse ISO datetime string."""
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except:
        return None


def map_team(abbrev: Optional[str]) -> Optional[str]:
    """Map team abbreviation to standard code."""
    if not abbrev:
        return None
    # SportsLine uses standard NBA abbreviations
    return abbrev.upper()


def map_market_type(raw_type: str) -> str:
    """Map SportsLine market type to standard type."""
    mapping = {
        "PROP": "player_prop",
        "POINT_SPREAD": "spread",
        "OVER_UNDER": "total",
        "MONEY_LINE": "moneyline",
    }
    return mapping.get(raw_type, raw_type.lower())


def map_result_status(status: str) -> str:
    """Map result status to standard outcome."""
    mapping = {
        "Win": "WIN",
        "Loss": "LOSS",
        "Push": "PUSH",
        "Pending": "PENDING",
    }
    return mapping.get(status, status.upper())


def extract_player_stat(label: str) -> Optional[str]:
    """Extract stat type from pick label like 'Over 14.5 Total Points + Assists'."""
    label_lower = label.lower()

    # Common stat patterns
    if "points + assists + rebounds" in label_lower or "pts + ast + reb" in label_lower:
        return "points_assists_rebounds"
    if "points + assists" in label_lower or "pts + ast" in label_lower:
        return "points_assists"
    if "points + rebounds" in label_lower or "pts + reb" in label_lower:
        return "points_rebounds"
    if "assists + rebounds" in label_lower or "ast + reb" in label_lower:
        return "assists_rebounds"
    if "total points" in label_lower or "pts" in label_lower:
        return "points"
    if "total assists" in label_lower or "ast" in label_lower:
        return "assists"
    if "total rebounds" in label_lower or "reb" in label_lower:
        return "rebounds"
    if "3-pointers" in label_lower or "threes" in label_lower:
        return "threes"
    if "steals" in label_lower:
        return "steals"
    if "blocks" in label_lower:
        return "blocks"
    if "turnovers" in label_lower:
        return "turnovers"

    return None


def normalize_expert_pick(raw: Dict[str, Any], sport: str = NBA_SPORT, store=None) -> Optional[Dict[str, Any]]:
    """Normalize a single expert pick record."""
    try:
        pick_id = raw.get("id", "")
        expert = raw.get("expert", {})
        game = raw.get("game", {})
        selection = raw.get("selection", {})

        # Parse game info
        away_team = map_team(game.get("awayTeam", {}).get("abbrev"))
        home_team = map_team(game.get("homeTeam", {}).get("abbrev"))
        scheduled_time = parse_datetime(game.get("scheduledTime"))

        if not away_team or not home_team or not scheduled_time:
            return None

        # Build event keys with sport prefix
        day_key = f"{sport}:{scheduled_time.year:04d}:{scheduled_time.month:02d}:{scheduled_time.day:02d}"
        event_key = f"{day_key}:{away_team}@{home_team}"
        matchup_key = f"{day_key}:{home_team}-{away_team}"

        # Parse market
        market_type = map_market_type(selection.get("marketType", ""))
        line = selection.get("value")
        odds = selection.get("odds")
        side = selection.get("side")  # OVER/UNDER for totals, team for spreads
        label = selection.get("label", "")

        # Player info for props
        player = selection.get("player", {})
        player_id = player.get("id") if player else None
        player_name = None
        if player:
            player_name = f"{player.get('firstName', '')} {player.get('lastName', '')}".strip()

        # Determine selection based on market type
        if market_type == "player_prop":
            # For props, selection is OVER/UNDER
            pick_selection = side.upper() if side else None
            stat_key = extract_player_stat(label)
        elif market_type == "total":
            pick_selection = side.upper() if side else None
            stat_key = None
        elif market_type == "spread":
            # Extract team from label (e.g., "Milwaukee +12 -110")
            team_match = re.match(r"([A-Za-z\s\.]+)\s*[+-]", label)
            if team_match:
                team_name = team_match.group(1).strip()
                # Map team name to code
                pick_selection = _map_team_name(team_name, away_team, home_team, sport=sport, store=store)
            else:
                pick_selection = None
            stat_key = None
        elif market_type == "moneyline":
            # Extract team from label
            team_match = re.match(r"([A-Za-z\s\.]+)\s*[+-]", label)
            if team_match:
                team_name = team_match.group(1).strip()
                pick_selection = _map_team_name(team_name, away_team, home_team, sport=sport, store=store)
            else:
                pick_selection = None
            stat_key = None
        else:
            pick_selection = None
            stat_key = None

        # Expert info
        expert_name = f"{expert.get('firstName', '')} {expert.get('lastName', '')}".strip()
        expert_id = expert.get("id", "")

        # Result
        result_status = map_result_status(raw.get("resultStatus", ""))

        # Game scores
        away_score = game.get("awayTeamScore")
        home_score = game.get("homeTeamScore")

        # Build fingerprint
        fingerprint = sha256_digest(f"sportsline|expert|{pick_id}")

        return {
            "provenance": {
                "source_id": "sportsline",
                "source_surface": f"sportsline_{sport.lower()}_expert_picks",
                "sport": sport,
                "observed_at_utc": raw.get("createdAt"),
                "pick_id": pick_id,
                "expert_id": expert_id,
                "expert_name": expert_name,
                "raw_fingerprint": fingerprint,
                "raw_pick_text": label,
                "writeup": raw.get("writeup"),
            },
            "event": {
                "sport": sport,
                "event_key": event_key,
                "day_key": day_key,
                "matchup_key": matchup_key,
                "away_team": away_team,
                "home_team": home_team,
                "event_start_time_utc": game.get("scheduledTime"),
                "away_score": away_score,
                "home_score": home_score,
            },
            "market": {
                "market_type": market_type,
                "market_family": "prop" if market_type == "player_prop" else "standard",
                "selection": pick_selection,
                "side": side,
                "line": line,
                "odds": odds,
                "player_id": player_id,
                "player_name": player_name,
                "stat_key": stat_key,
                "unit": raw.get("unit", 1.0),
            },
            "result": {
                "status": result_status,
                "graded_by": "sportsline",  # Pre-graded by source
            },
            "eligible_for_consensus": True,
            "ineligibility_reason": None,
        }
    except Exception as e:
        print(f"[WARN] Failed to normalize pick: {e}")
        return None


def _map_team_name(name: str, away: str, home: str, sport: str = NBA_SPORT, store=None) -> Optional[str]:
    """Map team name from label to team code using sport-specific store."""
    name_lower = name.lower().strip()

    # Use sport-specific store for lookups
    store = store or get_data_store(sport)

    # Try lookup via store first
    from utils import normalize_text
    normalized = normalize_text(name)
    matches = store.lookup_team_code(normalized)
    if len(matches) == 1:
        return next(iter(matches))

    # For NBA, use hardcoded mappings as fallback
    if sport == NBA_SPORT:
        team_names = {
            "milwaukee": "MIL", "bucks": "MIL",
            "toronto": "TOR", "raptors": "TOR",
            "portland": "POR", "trail blazers": "POR", "blazers": "POR",
            "utah": "UTA", "jazz": "UTA",
            "oklahoma city": "OKC", "thunder": "OKC",
            "san antonio": "SAS", "spurs": "SAS",
            "golden state": "GSW", "warriors": "GSW",
            "sacramento": "SAC", "kings": "SAC",
            "miami": "MIA", "heat": "MIA",
            "new orleans": "NOP", "pelicans": "NOP",
            "dallas": "DAL", "mavericks": "DAL", "mavs": "DAL",
            "l.a. lakers": "LAL", "los angeles lakers": "LAL", "lakers": "LAL",
            "l.a. clippers": "LAC", "los angeles clippers": "LAC", "clippers": "LAC",
            "detroit": "DET", "pistons": "DET",
            "new york": "NYK", "knicks": "NYK",
            "philadelphia": "PHI", "76ers": "PHI", "sixers": "PHI",
            "boston": "BOS", "celtics": "BOS",
            "chicago": "CHI", "bulls": "CHI",
            "cleveland": "CLE", "cavaliers": "CLE", "cavs": "CLE",
            "indiana": "IND", "pacers": "IND",
            "atlanta": "ATL", "hawks": "ATL",
            "charlotte": "CHA", "hornets": "CHA",
            "memphis": "MEM", "grizzlies": "MEM",
            "denver": "DEN", "nuggets": "DEN",
            "minnesota": "MIN", "timberwolves": "MIN", "wolves": "MIN",
            "phoenix": "PHX", "suns": "PHX",
            "houston": "HOU", "rockets": "HOU",
            "washington": "WAS", "wizards": "WAS",
            "brooklyn": "BKN", "nets": "BKN",
            "orlando": "ORL", "magic": "ORL",
        }

        for pattern, code in team_names.items():
            if pattern in name_lower:
                return code

    # If no match, check if it matches away/home directly
    if away.lower() in name_lower:
        return away
    if home.lower() in name_lower:
        return home

    return None


def main():
    parser = argparse.ArgumentParser(description="Normalize SportsLine expert picks")
    parser.add_argument("--input", help="Input JSONL file (default: auto-generated based on sport)")
    parser.add_argument("--output", help="Output JSONL file (default: auto-generated based on sport)")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    parser.add_argument(
        "--sport",
        choices=["NBA", "NCAAB"],
        default="NBA",
        help="Sport to normalize (default: NBA)",
    )
    args = parser.parse_args()

    # Use sport-specific paths if not explicitly provided
    input_path = Path(args.input) if args.input else get_input_path(args.sport)
    output_path = Path(args.output) if args.output else get_output_path(args.sport)

    # Get sport-specific store
    store = get_data_store(args.sport)

    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return 1

    # Read raw picks
    raw_picks = []
    with open(input_path) as f:
        for line in f:
            line = line.strip()
            if line:
                raw_picks.append(json.loads(line))

    print(f"Read {len(raw_picks)} raw {args.sport} picks from {input_path}")

    # Normalize
    normalized = []
    results_summary = {}
    markets_summary = {}

    for raw in raw_picks:
        rec = normalize_expert_pick(raw, sport=args.sport, store=store)
        if rec:
            normalized.append(rec)

            # Track stats
            result = rec["result"]["status"]
            results_summary[result] = results_summary.get(result, 0) + 1

            market = rec["market"]["market_type"]
            markets_summary[market] = markets_summary.get(market, 0) + 1

    print(f"Normalized {len(normalized)} picks")
    print(f"Results: {results_summary}")
    print(f"Markets: {markets_summary}")

    # Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        for rec in normalized:
            f.write(json.dumps(rec) + "\n")

    print(f"Wrote {len(normalized)} normalized {args.sport} picks to {output_path}")

    # Show sample
    if normalized and args.debug:
        print("\nSample normalized pick:")
        print(json.dumps(normalized[0], indent=2))

    return 0


if __name__ == "__main__":
    sys.exit(main())
