#!/usr/bin/env python
from __future__ import annotations

import argparse
import os
import sys
import logging

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.betql_playwright import BetQLSession, wait_for_ready, require_storage_state
from src.betql_extractors import extract_totals_columns
from store import write_json
from collections import Counter

URL_TOTAL = "https://betql.co/nba/odds/totals-spread"


def main(out_path: str, storage_state: str, debug: bool = False) -> None:
    logger = logging.getLogger("betql.probe.totals")
    storage_state = require_storage_state(storage_state)
    with BetQLSession(storage_state, headless=True) as sess:
        page = sess.open(URL_TOTAL)
        wait_for_ready(page, "model")
        records, dbg = extract_totals_columns(page, URL_TOTAL, debug=debug)
        page.close()

    write_json(out_path, records)
    model_elig = sum(1 for r in records if r["source_surface"] == "betql_model_total" and (r.get("rating_stars") or 0) >= 4)
    pro_elig = sum(1 for r in records if r["source_surface"] == "betql_probet_total" and (r.get("pro_pct") or 0) >= 10)
    logger.info(
        "totals: buttons=%s rows=%s model_elig=%s pro_elig=%s -> %s",
        dbg.get('ratings',0),
        len(records),
        model_elig,
        pro_elig,
        out_path,
    )
    if debug:
        hist = Counter(r.get("rating_stars") for r in records)
        logger.debug("rating_hist=%s", dict(hist))
        for i, row in enumerate(records[:3]):
            logger.debug(
                "[debug row %s] rating=%s pro_pct=%s pick=%s",
                i,
                row.get('rating_stars'),
                row.get('pro_pct'),
                row.get('raw_pick_text'),
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Probe BetQL NBA totals (O/U)")
    parser.add_argument("--out", default=os.path.join(os.getenv("NBA_OUT_DIR", "out"), "raw_betql_total_nba.json"))
    parser.add_argument("--storage", default="data/betql_storage_state.json")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    main(args.out, args.storage, debug=args.debug)
