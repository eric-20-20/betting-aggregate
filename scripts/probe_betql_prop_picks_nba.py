#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.betql_playwright import BetQLSession, wait_for_ready, require_storage_state
from src.betql_prop_picks import extract_prop_picks, CANONICAL_URL
from store import write_json


def main(out_path: str, storage_state: str, headless: bool = True, debug: bool = False) -> None:
    records = []
    with BetQLSession(storage_state_path=storage_state, headless=headless) as sess:
        page = sess.open(CANONICAL_URL)
        wait_for_ready(page, surface="props")
        recs = extract_prop_picks(page, canonical_url=CANONICAL_URL, debug=debug)
        records.extend(recs)
        page.close()
    write_json(out_path, records)
    print(f"wrote {len(records)} records to {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Probe BetQL NBA prop picks carousel")
    parser.add_argument("--out", default="data/raw_betql_prop_nba.json", help="output JSON path")
    parser.add_argument("--storage", default="data/betql_storage_state.json", help="playwright storage state path")
    parser.add_argument("--headed", action="store_true", help="run browser headed for debugging")
    parser.add_argument("--debug", action="store_true", help="enable verbose debug prints")
    args = parser.parse_args()
    require_storage_state(args.storage)
    main(args.out, args.storage, headless=not args.headed, debug=args.debug)
