#!/usr/bin/env python
from __future__ import annotations

import argparse
from pathlib import Path
import os, sys
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.betql_playwright import BetQLSession, wait_for_ready, require_storage_state


def main(storage: str) -> None:
    storage = require_storage_state(storage)
    with BetQLSession(storage, headless=True) as sess:
        page = sess.open("https://betql.co/nba/odds")
        wait_for_ready(page, "model")
        html = page.content()
        Path("tests/fixtures").mkdir(parents=True, exist_ok=True)
        Path("tests/fixtures/betql_odds.html").write_text(html, encoding="utf-8")
        page.close()

        page = sess.open("https://betql.co/nba/sharp-picks")
        wait_for_ready(page, "sharps")
        html = page.content()
        Path("tests/fixtures/betql_sharp_picks.html").write_text(html, encoding="utf-8")
        page.close()

    print("Snapshots saved. For props snapshot, run probe_betql_props_nba.py with a specific game URL.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--storage", default="data/betql_storage_state.json")
    args = parser.parse_args()
    main(args.storage)
