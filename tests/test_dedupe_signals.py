from consensus_nba import dedupe_signals


def test_dedupe_signals_avoids_duplicates():
    sig = {
        "signal_type": "avoid_conflict",
        "day_key": "NBA:20260130",
        "market_type": "player_prop",
        "selection": "p_watson::points::CONFLICT",
        "selections": ["OVER", "UNDER"],
        "sources": ["action", "covers"],
        "player_id": "p_watson",
        "atomic_stat": "points",
        "direction": "CONFLICT",
        "line_median": None,
    }
    dup_list = [sig, dict(sig)]
    deduped = dedupe_signals(dup_list)
    assert len(deduped) == 1
    assert deduped[0]["selection"] == sig["selection"]
