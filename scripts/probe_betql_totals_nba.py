#!/usr/bin/env python
from __future__ import annotations

import argparse
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.betql_playwright import BetQLSession, wait_for_ready, require_storage_state
from src.betql_extractors import extract_totals_columns
from store import write_json
from collections import Counter

URL_TOTAL = "https://betql.co/nba/odds/totals-spread"


def main(out_path: str, storage_state: str, debug: bool = False) -> None:
    storage_state = require_storage_state(storage_state)
    with BetQLSession(storage_state, headless=True) as sess:
        page = sess.open(URL_TOTAL)
        wait_for_ready(page, "model")
        records, dbg = extract_totals_columns(page, URL_TOTAL, debug=debug)
        page.close()

    write_json(out_path, records)
    model_elig = sum(1 for r in records if r["source_surface"] == "betql_model_total" and (r.get("rating_stars") or 0) >= 4)
    pro_elig = sum(1 for r in records if r["source_surface"] == "betql_probet_total" and (r.get("pro_pct") or 0) >= 10)
    print(f"totals: buttons={dbg.get('ratings',0)} rows={len(records)} model_elig={model_elig} pro_elig={pro_elig} -> {out_path}")
    if debug:
        hist = Counter(r.get("rating_stars") for r in records)
        print(f"rating_hist={dict(hist)}")
        for i, row in enumerate(records[:3]):
            print(f"[debug row {i}] rating={row.get('rating_stars')} pro_pct={row.get('pro_pct')} pick={row.get('raw_pick_text')}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Probe BetQL NBA totals (O/U)")
    parser.add_argument("--out", default="data/raw_betql_total_nba.json")
    parser.add_argument("--storage", default="data/betql_storage_state.json")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    main(args.out, args.storage, debug=args.debug)
