"""Quick smoke test for BetQL spread normalization."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.normalizer_betql_nba import normalize_raw_record


def run_case(label: str, raw: dict, expect_selection: str, expect_away: str, expect_home: str, expect_line: float) -> None:
    rec = normalize_raw_record(raw)
    market = rec.get("market") or {}
    event = rec.get("event") or {}
    selection = market.get("selection")
    line = market.get("line")
    away = event.get("away_team")
    home = event.get("home_team")
    errors = []
    if selection != expect_selection:
        errors.append(f"selection expected {expect_selection} got {selection}")
    if away != expect_away or home != expect_home:
        errors.append(f"teams expected {expect_away}@{expect_home} got {away}@{home}")
    if line != expect_line:
        errors.append(f"line expected {expect_line} got {line}")
    if errors:
        raise AssertionError(f"{label} failed: {'; '.join(errors)}")
    print(f"[ok] {label}: selection={selection} line={line} away@home={away}@{home}")


def main() -> None:
    sample_a = {
        "source_id": "betql",
        "source_surface": "betql_model_spread",
        "market_family": "spread",
        "selection_hint": "Cavaliers",
        "line_hint": -8.0,
        "raw_pick_text": "-8",
        "away_team": "Timberwolves",
        "home_team": "Cavaliers",
        "matchup_hint": "Timberwolves@Cavaliers",
    }
    sample_b = {
        "source_id": "betql",
        "source_surface": "betql_model_spread",
        "market_family": "spread",
        "selection_hint": "Warriors",
        "line_hint": 7.0,
        "raw_pick_text": "+7",
        "away_team": "Warriors",
        "home_team": "Bucks",
        "matchup_hint": "Warriors@Bucks",
    }
    run_case("Cavs -8", sample_a, expect_selection="CLE", expect_away="MIN", expect_home="CLE", expect_line=-8.0)
    run_case("Warriors +7", sample_b, expect_selection="GSW", expect_away="GSW", expect_home="MIL", expect_line=7.0)


if __name__ == "__main__":
    main()
