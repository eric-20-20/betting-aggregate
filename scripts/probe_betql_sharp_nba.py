#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
import logging
import os

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.betql_playwright import BetQLSession, wait_for_ready, require_storage_state
from src.betql_extractors import extract_sharp_spreads_totals
from store import write_json


def main(out_path: str, storage_state: str, debug: bool = False) -> None:
    url = "https://betql.co/nba/sharp-picks"
    logger = logging.getLogger("betql.probe.sharps")
    records = []
    with BetQLSession(storage_state_path=storage_state, headless=True) as sess:
        page = sess.open(url)
        wait_for_ready(page, surface="sharps")
        recs, dbg = extract_sharp_spreads_totals(page, canonical_url=url, debug=debug)
        records.extend(recs)
        if debug:
            logger.debug("sharp rows=%s extracted=%s", dbg.get('rows'), len(recs))
            kinds = {}
            for r in recs:
                kinds.setdefault(r.get('market_family'), 0)
                kinds[r.get('market_family')] += 1
            logger.debug("market split %s", kinds)
        page.close()
    write_json(out_path, records)
    logger.info("wrote %s records to %s", len(records), out_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Probe BetQL Sharp Picks (NBA) for spreads/totals")
    parser.add_argument("--out", default=os.path.join(os.getenv("NBA_OUT_DIR", "out"), "raw_betql_sharp_nba.json"), help="output JSON path")
    parser.add_argument("--storage", default="data/betql_storage_state.json", help="playwright storage state path")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    require_storage_state(args.storage)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    main(args.out, args.storage, debug=args.debug)
