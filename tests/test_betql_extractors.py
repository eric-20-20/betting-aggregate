import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.betql_extractors import extract_model_rows, extract_sharp_rows, extract_prop_cards
from src.normalizer_betql_nba import normalize_raw_record
from store import data_store


MODEL_HTML = """
<div class="game-row">
  <button class="games-table-column__rating-button"><svg></svg><svg></svg><svg></svg><svg></svg></button>
  <div class="games-table-column__team-cell"><img src="/team/logo/NBA/LAL.png"></div>
  <div class="games-table-column__team-cell"><img src="/team/logo/NBA/BOS.png"></div>
  <div class="games-table-column__current-line-cell">+3.5</div>
  Lakers vs Celtics +3.5 -110
</div>
<div class="game-row">
  <button class="games-table-column__rating-button"><svg></svg><svg></svg></button>
  <div class="games-table-column__team-cell"><img src="/team/logo/NBA/NYK.png"></div>
  <div class="games-table-column__team-cell"><img src="/team/logo/NBA/BKN.png"></div>
  <div class="games-table-column__current-line-cell">-2.5</div>
  Knicks vs Nets -2.5 -110
</div>
"""

SHARP_HTML = """
<div class="game-row">
  <div class="game">Bulls vs Cavs</div>
  <div class="pro">Pro Betting 12%</div>
  <div class="pick">CHI +4.5 -108</div>
</div>
<div class="game-row">
  <div class="game">Heat vs Magic</div>
  <div class="pro">Pro Betting 5%</div>
  <div class="pick">MIA -3.5 -110</div>
</div>
"""

PROP_HTML = """
<div class="player-prop-card">
  <div class="rating">4 Stars</div>
  <div class="body">Anthony Edwards OVER 26.5 Points -111</div>
</div>
<div class="player-prop-card">
  <div class="rating">3 Stars</div>
  <div class="body">Klay Thompson OVER 3.5 Threes -110</div>
</div>
"""


def test_model_extractor_and_normalize():
    data_store.reset()
    res = extract_model_rows(MODEL_HTML, "https://betql.co/nba/odds")
    assert len(res["candidates"]) > 0
    records = res["records"]
    assert len(records) == 2  # two rows -> model records; pro_pct missing in fixture
    elig = [normalize_raw_record(r) for r in records if normalize_raw_record(r)["eligible_for_consensus"]]
    assert len(elig) == 1
    assert elig[0]["market"]["market_type"] == "spread"
    assert records[0].get("rating_stars") is not None


def test_sharp_extractor_threshold():
    data_store.reset()
    res = extract_sharp_rows(SHARP_HTML, "https://betql.co/nba/sharp-picks")
    assert len(res["candidates"]) > 0
    records = res["records"]
    elig = [normalize_raw_record(r) for r in records if normalize_raw_record(r)["eligible_for_consensus"]]
    assert len(elig) == 1
    assert elig[0]["market"]["market_type"] == "spread"


def test_prop_extractor_and_normalize():
    data_store.reset()
    res = extract_prop_cards(PROP_HTML, "https://betql.co/nba/game")
    assert len(res["candidates"]) > 0
    records = res["records"]
    elig = [normalize_raw_record(r) for r in records if normalize_raw_record(r)["eligible_for_consensus"]]
    assert len(elig) == 1
    assert elig[0]["market"]["market_type"] == "player_prop"
