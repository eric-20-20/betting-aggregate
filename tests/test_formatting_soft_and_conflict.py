from consensus_nba import format_soft_row, format_conflict_row


def test_format_soft_spread_shows_lines_and_range():
    item = {
        "day_key": "NBA:20260130:BKN@CHI",
        "market_type": "spread",
        "selection": "BKN",
        "lines": [8.0, 8.5],
        "line_median": 8.25,
        "line_min": 8.0,
        "line_max": 8.5,
        "expert_strength": 2,
        "count_total": 2,
        "sources": ["action"],
    }
    out = format_soft_row(item)
    assert "+8.0" in out and "+8.5" in out
    assert "median=+8.2" in out
    assert "range=[+8.0,+8.5]" in out
    assert out.count("median=") == 1


def test_format_conflict_spread_shows_both_sides_lines():
    item = {
        "day_key": "NBA:20260130:TOR@POR",
        "market_type": "spread",
        "selections": ["TOR", "POR"],
        "sources": ["action"],
        "count_total": 4,
        "experts_by_selection": {"TOR": ["exp1", "exp2"], "POR": ["exp3", "exp4"]},
        "lines_by_selection": {"TOR": [-4.0, -3.0], "POR": [2.5, 3.5]},
    }
    out = format_conflict_row(item)
    assert "TOR" in out and "POR" in out
    assert "-4.0" in out and "-3.0" in out
    assert "+2.5" in out and "+3.5" in out
    assert "experts=2" in out
