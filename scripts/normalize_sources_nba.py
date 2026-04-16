#!/usr/bin/env python
from __future__ import annotations

import argparse
import os
from collections import Counter
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.normalizer_nba import normalize_file
from src.normalizer_sportscapping_nba import normalize_file as normalize_sportscapping
from src.normalizer_betql_nba import normalize_file as normalize_betql
from src.normalizer_betql_nba import normalize_props_file as normalize_betql_props
from src.normalizer_juicereel_nba import normalize_file as normalize_juicereel
from src.normalizer_bettingpros_nba import normalize_file as normalize_bettingpros
from src.normalizer_bettingpros_experts_nba import normalize_file as normalize_bettingpros_experts
from src.normalizer_bettingpros_props_nba import normalize_file as normalize_bettingpros_props


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value in {"1", "true", "TRUE", "yes", "YES", "on", "ON"}


def _check_freshness(raw_path: Path, source_label: str, today: str) -> bool:
    """Return True if raw_path was modified today. Log a warning and return False if stale."""
    if not raw_path.exists():
        print(f"⚠️  Skipping {source_label} normalization — raw file not found: {raw_path}")
        return False
    from datetime import datetime
    mtime = datetime.fromtimestamp(raw_path.stat().st_mtime).strftime("%Y-%m-%d")
    if mtime != today:
        print(
            f"⚠️  Skipping {source_label} normalization — raw file is stale "
            f"(last modified: {mtime}). Ingest likely failed."
        )
        return False
    return True


def summarize(records):
    total = len(records)
    eligible = sum(1 for r in records if r.get("eligible_for_consensus"))
    ineligible = total - eligible
    reasons = Counter()
    for r in records:
        if r.get("eligible_for_consensus"):
            continue
        reasons[r.get("ineligibility_reason") or "unknown"] += 1
    return total, eligible, ineligible, reasons


def main(out_dir: Path, debug: bool = False) -> None:
    base = Path(os.getenv("NBA_OUT_DIR", "out"))
    raw_action = out_dir / base / "raw_action_nba.json"
    raw_covers = out_dir / base / "raw_covers_nba.json"
    raw_sportscapping = out_dir / base / "raw_sportscapping_nba.json"
    raw_betql_spread = out_dir / base / "raw_betql_spread_nba.json"
    raw_betql_total = out_dir / base / "raw_betql_total_nba.json"
    raw_betql_sharp = out_dir / base / "raw_betql_sharp_nba.json"
    raw_betql_prop = out_dir / base / "raw_betql_prop_nba.json"
    norm_action = out_dir / base / "normalized_action_nba.json"
    norm_covers = out_dir / base / "normalized_covers_nba.json"
    norm_sportscapping = out_dir / base / "normalized_sportscapping_nba.json"
    norm_betql_spread = out_dir / base / "normalized_betql_spread_nba.json"
    norm_betql_total = out_dir / base / "normalized_betql_total_nba.json"
    norm_betql_sharp = out_dir / base / "normalized_betql_sharp_nba.json"
    norm_betql_prop = out_dir / base / "normalized_betql_prop_nba.json"
    raw_juicereel = out_dir / base / "raw_juicereel_nba.json"
    norm_juicereel = out_dir / base / "normalized_juicereel_nba.json"
    norm_juicereel_ncaab = out_dir / base / "normalized_juicereel_ncaab.json"
    raw_bettingpros = out_dir / base / "raw_bettingpros_nba.json"
    norm_bettingpros = out_dir / base / "normalized_bettingpros_nba.json"
    raw_bettingpros_props = out_dir / base / "raw_bettingpros_prop_bets_nba.json"
    norm_bettingpros_props = out_dir / base / "normalized_bettingpros_prop_bets_nba.json"
    raw_bpe = out_dir / base / "raw_bettingpros_experts_nba.json"
    norm_bpe = out_dir / base / "normalized_bettingpros_experts_nba.json"
    juicereel_enabled = _env_flag("ENABLE_JUICEREEL", default=False)

    from datetime import datetime
    from zoneinfo import ZoneInfo
    _today_date = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")

    from store import write_json as _wj
    if _check_freshness(raw_action, "Action", _today_date):
        action_records = normalize_file(str(raw_action), str(norm_action), debug=debug)
    else:
        _wj(str(norm_action), [])
        action_records = []

    if _check_freshness(raw_covers, "Covers", _today_date):
        covers_records = normalize_file(str(raw_covers), str(norm_covers), debug=debug)
    else:
        _wj(str(norm_covers), [])
        covers_records = []
    sport_records = normalize_sportscapping(str(raw_sportscapping), str(norm_sportscapping))
    betql_spread_records = normalize_betql(str(raw_betql_spread), str(norm_betql_spread), debug=debug)
    betql_total_records = normalize_betql(str(raw_betql_total), str(norm_betql_total), debug=debug)
    betql_sharp_records = normalize_betql(str(raw_betql_sharp), str(norm_betql_sharp), debug=debug)
    betql_prop_records = normalize_betql_props(str(raw_betql_prop), str(norm_betql_prop), debug=debug)
    if juicereel_enabled and _check_freshness(raw_juicereel, "JuiceReel", _today_date):
        juicereel_records = normalize_juicereel(str(raw_juicereel), str(norm_juicereel), debug=debug, sport="NBA")
    else:
        if not juicereel_enabled:
            print("⚠️  Skipping JuiceReel normalization — source disabled by ENABLE_JUICEREEL")
        _wj(str(norm_juicereel), [])
        juicereel_records = []
    bettingpros_records = normalize_bettingpros(str(raw_bettingpros), str(norm_bettingpros), debug=debug)
    bettingpros_prop_records = normalize_bettingpros_props(str(raw_bettingpros_props), str(norm_bettingpros_props), debug=debug)
    bpe_records = normalize_bettingpros_experts(str(raw_bpe), str(norm_bpe), debug=debug, target_date=_today_date)

    # Also normalize JuiceReel NCAAB picks from the same raw file (picks are mixed by sport)
    # The normalizer uses sport param to route team lookups and build event keys correctly.
    # Pre-filter to only NCAAB-tagged raw records so NBA and NCAAB outputs are cleanly separated.
    import json as _json
    _raw_jr_path = str(raw_juicereel)
    if juicereel_enabled and os.path.exists(_raw_jr_path):
        with open(_raw_jr_path) as _f:
            _raw_jr_all = _json.load(_f) if _f.readable() else []
        _raw_jr_ncaab = [r for r in _raw_jr_all if isinstance(r, dict) and r.get("sport") == "NCAAB"]
        if _raw_jr_ncaab:
            import tempfile
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as _tmp:
                _json.dump(_raw_jr_ncaab, _tmp)
                _tmp_path = _tmp.name
            juicereel_ncaab_records = normalize_juicereel(_tmp_path, str(norm_juicereel_ncaab), debug=debug, sport="NCAAB")
            os.unlink(_tmp_path)
        else:
            from store import write_json as _wj
            _wj(str(norm_juicereel_ncaab), [])
            juicereel_ncaab_records = []
    else:
        _wj(str(norm_juicereel_ncaab), [])
        juicereel_ncaab_records = []

    # Merge sharp (probet) into main BetQL outputs by market
    from store import write_json
    betql_spread_records.extend([r for r in betql_sharp_records if (r.get("market") or {}).get("market_type") == "spread"])
    betql_total_records.extend([r for r in betql_sharp_records if (r.get("market") or {}).get("market_type") == "total"])
    write_json(str(norm_betql_spread), betql_spread_records)
    write_json(str(norm_betql_total), betql_total_records)

    print("Action")
    total, eligible, ineligible, reasons = summarize(action_records)
    print(f"  total={total} eligible={eligible} ineligible={ineligible}")
    if ineligible:
        for reason, count in reasons.most_common():
            print(f"    {reason}: {count}")

    print("BetQL Spread")
    total, eligible, ineligible, reasons = summarize(betql_spread_records)
    print(f"  total={total} eligible={eligible} ineligible={ineligible}")
    for reason, count in reasons.most_common():
        print(f"    {reason}: {count}")

    print("BetQL Total")
    total, eligible, ineligible, reasons = summarize(betql_total_records)
    print(f"  total={total} eligible={eligible} ineligible={ineligible}")
    for reason, count in reasons.most_common():
        print(f"    {reason}: {count}")

    print("BetQL Props")
    total, eligible, ineligible, reasons = summarize(betql_prop_records)
    print(f"  total={total} eligible={eligible} ineligible={ineligible}")
    for reason, count in reasons.most_common():
        print(f"    {reason}: {count}")

    # Debug assertion for model+probet double emission
    def assert_double(records, label: str) -> None:
        combo = {}
        reasons = {}
        for r in records:
            if r.get("eligible_for_consensus") is False:
                continue
            ev = (r.get("event") or {}).get("event_key")
            mk = (r.get("market") or {}).get("market_type")
            sel = (r.get("market") or {}).get("selection")
            kind = r.get("provenance", {}).get("source_surface")
            key = (ev, mk, sel)
            combo.setdefault(key, set()).add(kind)
            reasons[key] = reasons.get(key, []) + [r.get("ineligibility_reason")]
        missing = [(k, v) for k, v in combo.items() if len(v) == 1]
        if missing:
            print(f"[DEBUG] betql {label}: model+probet not both present for {len(missing)} keys")
            for k, v in missing[:10]:
                print(f"  key={k} has={v} reasons={reasons.get(k)}")

    assert_double(betql_spread_records + betql_total_records, "double-check")

    print("BetQL Total")
    total, eligible, ineligible, reasons = summarize(betql_total_records)
    print(f"  total={total} eligible={eligible} ineligible={ineligible}")
    for reason, count in reasons.most_common():
        print(f"    {reason}: {count}")
    print("Sportscapping")
    total, eligible, ineligible, reasons = summarize(sport_records)
    print(f"  total={total} eligible={eligible} ineligible={ineligible}")
    if ineligible:
        for reason, count in reasons.most_common():
            print(f"    {reason}: {count}")

    print("Covers")
    total, eligible, ineligible, reasons = summarize(covers_records)
    print(f"  total={total} eligible={eligible} ineligible={ineligible}")
    if ineligible:
        for reason, count in reasons.most_common():
            print(f"    {reason}: {count}")

    print("JuiceReel (NBA)")
    total, eligible, ineligible, reasons = summarize(juicereel_records)
    print(f"  total={total} eligible={eligible} ineligible={ineligible}")
    if ineligible:
        for reason, count in reasons.most_common():
            print(f"    {reason}: {count}")

    print("JuiceReel (NCAAB)")
    total, eligible, ineligible, reasons = summarize(juicereel_ncaab_records)
    print(f"  total={total} eligible={eligible} ineligible={ineligible}")
    if ineligible:
        for reason, count in reasons.most_common():
            print(f"    {reason}: {count}")

    print("BettingPros")
    total, eligible, ineligible, reasons = summarize(bettingpros_records)
    print(f"  total={total} eligible={eligible} ineligible={ineligible}")
    if ineligible:
        for reason, count in reasons.most_common():
            print(f"    {reason}: {count}")

    print("BettingPros Props")
    total, eligible, ineligible, reasons = summarize(bettingpros_prop_records)
    print(f"  total={total} eligible={eligible} ineligible={ineligible}")
    if ineligible:
        for reason, count in reasons.most_common():
            print(f"    {reason}: {count}")

    print("BettingPros Experts")
    total, eligible, ineligible, reasons = summarize(bpe_records)
    print(f"  total={total} eligible={eligible} ineligible={ineligible}")
    if ineligible:
        for reason, count in reasons.most_common():
            print(f"    {reason}: {count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Normalize NBA sources (action + covers)")
    parser.add_argument("--root", default=".", help="project root (default: cwd)")
    parser.add_argument("--debug", action="store_true", help="debug output")
    args = parser.parse_args()
    main(Path(args.root), debug=args.debug)
