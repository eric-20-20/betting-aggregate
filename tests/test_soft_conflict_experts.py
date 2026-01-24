from consensus_nba import soft_conflict_rows


def test_soft_conflict_contains_experts_by_selection():
    groups = [
        {
            "signal_type": "soft_standard",
            "day_key": "NBA:20260130:TOR@POR",
            "market_type": "spread",
            "selection": "TOR",
            "sources": ["action"],
            "experts_raw": ["action:exp1", "action:exp2"],
            "count_total": 2,
        },
        {
            "signal_type": "soft_standard",
            "day_key": "NBA:20260130:TOR@POR",
            "market_type": "spread",
            "selection": "POR",
            "sources": ["action"],
            "experts_raw": ["action:exp3", "action:exp4"],
            "count_total": 2,
        },
    ]
    conflicts = soft_conflict_rows(groups)
    assert len(conflicts) == 1
    c = conflicts[0]
    assert c["market_type"] == "spread"
    assert set(c["selections"]) == {"TOR", "POR"}
    assert c["experts_total"] == 4
    assert set(c["experts_by_selection"].keys()) == {"TOR", "POR"}
