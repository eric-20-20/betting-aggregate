def test_imports_smoke():
    import action_ingest  # noqa: F401
    import consensus_nba  # noqa: F401
    from src.player_aliases_nba import normalize_player_slug  # noqa: F401

    # simple sanity: callable exists
    assert callable(normalize_player_slug)
