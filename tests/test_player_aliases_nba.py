import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.player_aliases_nba import normalize_player_key, normalize_player_slug


def test_alias_expansion():
    assert normalize_player_key("NBA:l_jame") == "NBA:lebron_james"
    assert normalize_player_key("NBA:d_mitchell") == "NBA:donovan_mitchell"
    assert normalize_player_key("NBA:lebron_james") == "NBA:lebron_james"


def test_slug_passthrough():
    assert normalize_player_slug("lebron_james") == "lebron_james"


def test_unknown_abbrev_retains_slug():
    assert normalize_player_key("NBA:x_foo") == "NBA:x_foo"
