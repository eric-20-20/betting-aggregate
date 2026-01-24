import json

from consensus_nba import append_and_fsync, main


def test_append_jsonl_round_trips(tmp_path):
    path = tmp_path / "records.jsonl"
    rows = [{"a": 1}, {"b": 2}]
    append_and_fsync(str(path), rows)
    with path.open("r", encoding="utf-8") as f:
        lines = [json.loads(line) for line in f.read().splitlines() if line.strip()]
    assert len(lines) == 2
    assert lines[0]["a"] == 1
    assert lines[1]["b"] == 2


def test_run_meta_written(tmp_path, capsys):
    out_dir = tmp_path / "out"
    data_dir = tmp_path / "data"
    out_dir.mkdir()
    # minimal normalized + raw inputs so consensus main can run
    (out_dir / "normalized_action_nba.json").write_text("[]", encoding="utf-8")
    (out_dir / "normalized_covers_nba.json").write_text("[]", encoding="utf-8")
    (out_dir / "raw_action_nba.json").write_text("[]", encoding="utf-8")
    (out_dir / "raw_covers_nba.json").write_text("[]", encoding="utf-8")

    main(track=True, data_dir=str(data_dir), out_dir=str(out_dir))
    # locate newest run dir
    runs_dir = data_dir / "runs"
    run_dirs = sorted(runs_dir.iterdir())
    assert run_dirs, "run directory should be created"
    meta_path = run_dirs[-1] / "run_meta.json"
    assert meta_path.exists()
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    for key in ["run_id", "observed_at_utc", "counts", "schema_version", "python_version"]:
        assert key in meta
