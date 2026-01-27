#!/usr/bin/env python
from __future__ import annotations

import argparse
from pathlib import Path
import os, sys
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.betql_playwright import BetQLSession, wait_for_ready, require_storage_state
from src.betql_extractors import extract_model_rows
from store import write_json


def main(out_path: str, storage_state: str) -> None:
    storage_state = require_storage_state(storage_state)
    url = "https://betql.co/nba/odds"
    with BetQLSession(storage_state, headless=True) as sess:
        page = sess.open(url)
        wait_for_ready(page, "model")
        html = page.content()
        page.close()
    res = extract_model_rows(html, url)
    records = res["records"]
    write_json(out_path, records)
    print(f"wrote {len(records)} model/pro-bet records -> {out_path}")
    if not records:
        print(f"candidates_seen={len(res.get('candidates', []))}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Probe BetQL NBA model bets")
    parser.add_argument("--out", default="data/raw_betql_model_nba.json")
    parser.add_argument("--storage", default="data/betql_storage_state.json")
    args = parser.parse_args()
    main(args.out, args.storage)
