#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import sys
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.betql_prop_normalizer import normalize_betql_prop_pick
from store import write_json


def main(inp: str, out: str) -> None:
    logger = logging.getLogger("betql.normalize.props")
    with open(inp, "r", encoding="utf-8") as f:
        raw_records = json.load(f)
    normalized = [normalize_betql_prop_pick(rec) for rec in raw_records]
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    write_json(out, normalized)
    logger.info("normalized %s records -> %s", len(normalized), out)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Normalize BetQL NBA prop picks")
    parser.add_argument("--in", dest="inp", default=os.path.join(os.getenv("NBA_OUT_DIR", "out"), "raw_betql_prop_nba.json"))
    parser.add_argument("--out", dest="out", default=os.path.join(os.getenv("NBA_OUT_DIR", "out"), "normalized_betql_prop_nba.json"))
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    main(args.inp, args.out)
