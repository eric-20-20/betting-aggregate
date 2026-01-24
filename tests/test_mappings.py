import pytest

from mappings import map_player, map_prop_stat, map_teams
from store import data_store


@pytest.fixture(autouse=True)
def reset_data_store():
    data_store.reset()
    yield


def test_map_teams_knick_lakers():
    assert map_teams("Knick @ Lakers") == ("NYK", "LAL")


def test_map_player_and_stat_points():
    player_key = map_player("LeBron James OVER 27.5 points")
    assert player_key == "NBA:lebron_james"
    assert map_prop_stat("LeBron James OVER 27.5 points") == "points"


def test_map_prop_stat_rebounds():
    map_player("Anthony Davis o/u 10.5 rebounds")
    assert map_prop_stat("Anthony Davis o/u 10.5 rebounds") == "rebounds"
