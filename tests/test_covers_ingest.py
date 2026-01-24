import json
from pathlib import Path

import pytest

from covers_ingest import extract_picks_from_html, normalize_covers_pick, extract_matchup_teams_from_header
from action_ingest import dedupe_normalized_bets
from datetime import datetime, timezone


FIXTURES_DIR = Path("tests/fixtures/covers")


def load_fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


def test_extract_standard_pick_card():
    html = load_fixture("standard_pick.html")
    records = extract_picks_from_html(html, "https://example.com/game1", datetime.now(timezone.utc))
    assert len(records) == 1
    assert records[0].raw_pick_text.startswith("NYK -3.5")
    assert records[0].expert_name == "Jane Analyst"


def test_extract_player_prop_pick():
    html = load_fixture("player_prop_pick.html")
    records = extract_picks_from_html(html, "https://example.com/game2", datetime.now(timezone.utc))
    assert len(records) == 1
    assert "over 23.5 points" in records[0].raw_pick_text.lower()
    assert records[0].expert_name == "John Doe"


def test_multiple_picks_dedup():
    html = load_fixture("multi_pick.html")
    records = extract_picks_from_html(html, "https://example.com/game3", datetime.now(timezone.utc))
    assert len(records) == 2
    normalized = dedupe_normalized_bets(
        [normalize_covers_pick(r, home_team="LAL", away_team="BOS") for r in records]
    )
    assert len(normalized) == 2


def test_no_pick_made_label_dom_fallback():
    html = load_fixture("no_pick_made.html")
    records = extract_picks_from_html(html, "https://example.com/game4", datetime.now(timezone.utc))
    assert len(records) == 1
    assert "over 218.5" in records[0].raw_pick_text.lower()


def test_expert_cards_component():
    html = load_fixture("expert_cards.html")
    records = extract_picks_from_html(html, "https://example.com/game5", datetime.now(timezone.utc))
    assert len(records) == 2
    picks = sorted([r.raw_pick_text for r in records])
    assert picks == ["CHA ML +120", "CLE ML -142"]
    experts = {r.expert_name for r in records}
    assert "Phil Naessens" in experts
    assert "Chris Vasile" in experts


@pytest.mark.parametrize(
    "fixture,expected",
    [
        ("standard_pick.html", ("NYK", "BOS")),
        ("player_prop_pick.html", ("GSW", "LAL")),
    ],
)
def test_extract_matchup_teams_from_header(fixture, expected):
    from bs4 import BeautifulSoup
    html = load_fixture(fixture)
    soup = BeautifulSoup(html, "html.parser")
    away, home = extract_matchup_teams_from_header(soup)
    assert (away, home) == expected
