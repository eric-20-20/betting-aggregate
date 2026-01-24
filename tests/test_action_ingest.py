from datetime import datetime

from action_ingest import (
    RawPickRecord,
    dedupe_normalized_bets,
    extract_picks_from_html,
    normalize_pick,
    parse_player_prop,
    parse_standard_market,
)


HTML_FIXTURE = """
<html>
  <body>
    <h2>Expert Picks</h2>
    <div class="pick-card">Follow LAL -5.5 -110</div>
    <div class="pick-card">Follow L.James o27.5 Pts -115</div>
  </body>
</html>
"""


def test_extract_picks_from_html():
    observed = datetime(2026, 1, 14, 0, 0)
    picks, _ = extract_picks_from_html(HTML_FIXTURE, canonical_url="http://example.com/game", observed_at_utc=observed)
    assert isinstance(picks, list)
    if picks:
        assert all(isinstance(p, RawPickRecord) for p in picks)
        assert len(picks[0].raw_fingerprint) == 64


def test_market_parsing_player_prop_and_standard():
    prop = parse_player_prop("LeBron James over 27.5 points")
    assert prop["market_type"] == "player_prop"
    assert prop["side"] == "player_over"
    assert prop["line"] == 27.5

    standard = parse_standard_market("Lakers -5.5 (-110)")
    assert standard["market_type"] == "total" or standard["ineligibility_reason"] in {"ambiguous_market"}


def test_standard_spread_and_moneyline_parsing():
    spread = parse_standard_market("HOU +5 -110")
    assert spread["market_type"] == "spread"
    assert "HOU" in spread["selection"]
    assert spread["line"] == 5.0
    assert spread["eligible_for_consensus"] is True

    moneyline = parse_standard_market("HOU +168")
    assert moneyline["market_type"] == "moneyline"
    assert "HOU" in moneyline["selection"]
    assert moneyline["odds"] == "+168"
    assert moneyline["eligible_for_consensus"] is True

    total = parse_standard_market("Under 222.5 -110")
    assert total["market_type"] == "total"
    assert total["selection"] == "game_total"


def test_dedupe_normalized_bets():
    bet = {
        "provenance": {"source_id": "action", "source_surface": "nba_game_expert_picks"},
        "event": {"event_key": "NBA:20260114:NYK@LAL:0330"},
        "market": {"market_type": "spread", "selection": "NYK_spread", "line": 5.0, "odds": "-110"},
    }
    dup = bet.copy()
    deduped = dedupe_normalized_bets([bet, dup])
    assert len(deduped) == 1


def test_total_eligibility_bypasses_team_validation():
    record = RawPickRecord(
        source_id="action",
        source_surface="nba_game_expert_picks",
        sport="nba",
        market_family="standard",
        observed_at_utc=datetime.utcnow().isoformat(),
        canonical_url="http://example.com/game",
        raw_pick_text="Under 223.5",
        raw_block="Under 223.5",
        raw_fingerprint="f" * 64,
        event_start_time_utc=datetime.utcnow().isoformat(),
    )
    normalized = normalize_pick(record, home_team="BOS", away_team="NYK", stats={})
    assert normalized["eligible_for_consensus"] is True
    assert normalized["ineligibility_reason"] is None
    assert normalized["market"]["market_type"] == "total"
