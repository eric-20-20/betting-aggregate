import datetime

from consensus_nba import compute_evidence_id, compute_signal_id, day_key


def make_norm_record(observed_at: str):
    return {
        "provenance": {
            "source_id": "action",
            "source_surface": "action",
            "observed_at_utc": observed_at,
            "canonical_url": "https://example.com/card",
            "raw_fingerprint": "abc123",
            "expert_name": "Capper",
        },
        "event": {"event_key": "NBA:20260120:SAS@HOU:1700"},
        "market": {
            "market_type": "player_prop",
            "selection": "alperen_sengun::points::OVER",
            "side": "player_over",
            "stat_key": "points",
            "player_key": "NBA:alperen_sengun",
            "line": 21.5,
            "odds": -110,
        },
        "eligible_for_consensus": True,
    }


def test_evidence_id_stable_across_observed_time():
    rec1 = make_norm_record("2026-01-20T12:00:00Z")
    rec2 = make_norm_record("2026-01-20T12:30:00Z")
    evid1 = compute_evidence_id(rec1)
    evid2 = compute_evidence_id(rec2)
    assert evid1 == evid2


def test_signal_id_stable_for_same_signal():
    sig = {
        "signal_type": "soft_atomic",
        "day_key": day_key("NBA:20260120:SAS@HOU:1700"),
        "market_type": "player_prop",
        "player_id": "a_sengun",
        "atomic_stat": "points",
        "direction": "OVER",
        "selection": "a_sengun::points::OVER",
    }
    sid1 = compute_signal_id(sig)
    sid2 = compute_signal_id(sig.copy())
    assert sid1 == sid2
