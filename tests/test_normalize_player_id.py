from consensus_nba import normalize_player_id


def test_generational_suffix_removed():
    assert normalize_player_id("NBA:trey_murphy_iii") == "t_murphy"
    assert normalize_player_id("trey_murphy_iii_projection") == "t_murphy"


def test_watson_still_normalizes():
    assert normalize_player_id("NBA:p_watson") == "p_watson"
    assert normalize_player_id("peyton_watson_projection") == "p_watson"
