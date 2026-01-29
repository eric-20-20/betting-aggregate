#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import sys
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.normalizer_sportscapping_nba import normalize_file


def main(raw_path: str, out_path: str) -> None:
    normalize_file(raw_path, out_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Normalize Sportscapping NBA picks")
    parser.add_argument("--in", dest="inp", default=os.path.join(os.getenv("NBA_OUT_DIR", "out"), "raw_sportscapping_nba.json"))
    parser.add_argument("--out", dest="out", default=os.path.join(os.getenv("NBA_OUT_DIR", "out"), "normalized_sportscapping_nba.json"))
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    main(args.inp, args.out)
