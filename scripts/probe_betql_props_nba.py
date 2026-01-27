#!/usr/bin/env python
from __future__ import annotations

import argparse
import os, sys
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.betql_playwright import BetQLSession, wait_for_ready, require_storage_state
from src.betql_extractors import extract_prop_cards
from store import write_json


def main(out_path: str, storage_state: str, url: str) -> None:
    storage_state = require_storage_state(storage_state)
    with BetQLSession(storage_state, headless=True) as sess:
        page = sess.open(url)
        wait_for_ready(page, "props")
        html = page.content()
        page.close()
    rows = extract_prop_cards(html, url)
    write_json(out_path, rows)
    print(f"wrote {len(rows)} props -> {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Probe BetQL NBA player props (provide game predictions URL)")
    parser.add_argument("--url", required=True, help="BetQL game predictions URL")
    parser.add_argument("--out", default="data/raw_betql_props_nba.json")
    parser.add_argument("--storage", default="data/betql_storage_state.json")
    args = parser.parse_args()
    main(args.out, args.storage, args.url)
