import json
import pathlib

from store import write_json


def test_probe_output_is_list():
    sample = [{"a": 1}, {"b": 2}]
    path = pathlib.Path("tests/fixtures/tmp_probe.json")
    write_json(path, sample)
    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(loaded, list)
    assert loaded == sample
