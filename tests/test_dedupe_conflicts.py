from consensus_nba import dedupe_signals


def test_dedupe_conflict_rows_collapses_duplicates():
    a = {
        "signal_type": "avoid_conflict",
        "day_key": "NBA:20260130",
        "market_type": "moneyline",
        "selection": "moneyline_conflict",
        "selections": ["LAL", "BOS"],
        "sources": ["action", "covers"],
        "event_key": "E1",
        "experts": ["x"],
    }
    b = dict(a)
    b["event_key"] = "E2"
    b["experts"] = ["y"]
    b["lines_by_selection"] = {"LAL": [-110], "BOS": [+105]}
    deduped = dedupe_signals([a, b])
    assert len(deduped) == 1
    assert deduped[0]["selection"] == "moneyline_conflict"
    assert deduped[0].get("lines_by_selection") == {"LAL": [-110], "BOS": [+105]}
