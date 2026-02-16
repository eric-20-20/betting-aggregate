"""Smoke test grading via API-Sports provider."""

from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone
import sys

# ensure repo root on path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.grade_signals_nba import grade_signal, read_jsonl, derive_date_str  # type: ignore

SIGNALS_LATEST_PATH = Path("data/ledger/signals_latest.jsonl")


def main() -> None:
    signals = read_jsonl(SIGNALS_LATEST_PATH)
    signals = sorted(signals, key=lambda r: r.get("observed_at_utc") or "")[-5:]
    now = datetime.now(timezone.utc).isoformat()
    for sig in signals:
        res = grade_signal(sig, now, derive_date_str(sig), refresh_cache=False, provider_name="api_sports", debug=False)
        print(
            json.dumps(
                {
                    "signal_id": res.get("signal_id"),
                    "market": res.get("market_type"),
                    "status": res.get("status"),
                    "result": res.get("result"),
                    "notes": res.get("notes"),
                },
                sort_keys=True,
            )
        )


if __name__ == "__main__":
    main()
