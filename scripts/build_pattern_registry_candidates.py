#!/usr/bin/env python3
"""
Build trusted NBA pattern candidate files from expert/agreement combined reports.

This does not mutate score_signals.py directly. It creates two daily artifacts:
  - pattern_registry_active.json   (n >= 50)
  - pattern_registry_watchlist.json (35 <= n < 50)

Both are derived from the trusted combined-deduped expert and agreement reports.
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR = REPO_ROOT / "data" / "reports"

WATCHLIST_MIN_N = 35
ACTIVE_MIN_N = 50
A_TIER_MIN_N = 50
B_TIER_WILSON = 0.46
A_TIER_WILSON = 0.50


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text()) if path.exists() else {"rows": []}


def _safe_id_part(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return cleaned or "unknown"


def _looks_malformed_name(name: str) -> bool:
    if not name:
        return True
    if "\n" in name:
        return True
    if len(name.strip()) < 3:
        return True
    if re.search(r"\b(last\s+\d+\s+days|last\s+\d+)\b", name, re.IGNORECASE):
        return True
    if re.fullmatch(r"[+\-]?\d+", name.strip()):
        return True
    return False


def _suggested_tier(row: Dict[str, Any], active_min_n: int) -> str:
    n = int(row.get("n", 0) or 0)
    wilson = float(row.get("wilson_lower", 0.0) or 0.0)
    if n >= max(active_min_n, A_TIER_MIN_N) and wilson >= A_TIER_WILSON:
        return "A"
    return "B"


def _candidate_status(row: Dict[str, Any], active_min_n: int) -> str:
    return "active" if int(row.get("n", 0) or 0) >= active_min_n else "watchlist"


def _base_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "market_type": row.get("market_type"),
        "atomic_stat": row.get("atomic_stat") or "",
        "direction": row.get("direction") or "",
        "wins": row.get("wins", 0),
        "losses": row.get("losses", 0),
        "pushes": row.get("pushes", 0),
        "n": row.get("n", 0),
        "win_pct": row.get("win_pct"),
        "wilson_lower": row.get("wilson_lower"),
        "roi": row.get("roi"),
    }


def _expert_pattern_entry(row: Dict[str, Any], active_min_n: int) -> Dict[str, Any]:
    expert_name = str(row.get("expert_name") or "").strip()
    source_id = str(row.get("source_id") or "").strip()
    stat_type = str(row.get("atomic_stat") or "").strip()
    direction = str(row.get("direction") or "").strip()
    entry: Dict[str, Any] = {
        "pattern_type": "expert",
        "id": "expert_" + "_".join(
            part for part in (
                _safe_id_part(expert_name),
                _safe_id_part(source_id),
                _safe_id_part(str(row.get("market_type") or "")),
                _safe_id_part(stat_type) if stat_type else "",
                _safe_id_part(direction) if direction else "",
            )
            if part
        ),
        "label": f"{expert_name} [{source_id}] {row.get('market_type')}",
        "expert": expert_name,
        "source_id": source_id,
        "tier_eligible": _suggested_tier(row, active_min_n),
        "candidate_status": _candidate_status(row, active_min_n),
    }
    if row.get("market_type"):
        entry["market_type"] = row.get("market_type")
    if stat_type:
        entry["stat_type"] = stat_type
    if direction:
        entry["direction"] = direction
    entry["hist"] = {
        "record": f"{row.get('wins', 0)}-{row.get('losses', 0)}",
        "win_pct": round(float(row.get("win_pct", 0.0) or 0.0), 3),
        "n": int(row.get("n", 0) or 0),
    }
    entry.update(_base_row(row))
    return entry


def _parse_agreement_experts(values: Sequence[str]) -> List[str]:
    experts: List[str] = []
    for value in values:
        name = str(value)
        if "[" in name and name.endswith("]"):
            name = name.rsplit("[", 1)[0].strip()
        experts.append(name.strip())
    return [e for e in experts if e]


def _agreement_pattern_entry(row: Dict[str, Any], active_min_n: int) -> Dict[str, Any]:
    experts = _parse_agreement_experts(row.get("experts") or [])
    stat_type = str(row.get("atomic_stat") or "").strip()
    direction = str(row.get("direction") or "").strip()
    display_group = str(row.get("display_group") or "agreement").strip()
    entry: Dict[str, Any] = {
        "pattern_type": "agreement",
        "id": "agreement_" + "_".join(
            [_safe_id_part(name) for name in experts]
            + [_safe_id_part(str(row.get("market_type") or ""))]
            + ([_safe_id_part(stat_type)] if stat_type else [])
            + ([_safe_id_part(direction)] if direction else [])
        ),
        "label": display_group,
        "experts_all": experts,
        "group_size": int(row.get("group_size", 0) or 0),
        "tier_eligible": _suggested_tier(row, active_min_n),
        "candidate_status": _candidate_status(row, active_min_n),
    }
    if row.get("market_type"):
        entry["market_type"] = row.get("market_type")
    if stat_type:
        entry["stat_type"] = stat_type
    if direction:
        entry["direction"] = direction
    entry["hist"] = {
        "record": f"{row.get('wins', 0)}-{row.get('losses', 0)}",
        "win_pct": round(float(row.get("win_pct", 0.0) or 0.0), 3),
        "n": int(row.get("n", 0) or 0),
    }
    entry.update(_base_row(row))
    return entry


def _filter_rows(rows: Iterable[Dict[str, Any]], min_n: int) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    for row in rows:
        n = int(row.get("n", 0) or 0)
        wilson = float(row.get("wilson_lower", 0.0) or 0.0)
        if n < min_n or wilson < B_TIER_WILSON:
            continue
        output.append(row)
    output.sort(key=lambda row: (-float(row.get("wilson_lower", 0.0) or 0.0), -int(row.get("n", 0) or 0)))
    return output


def build_candidates(watchlist_min_n: int, active_min_n: int) -> Dict[str, Dict[str, Any]]:
    expert_rows = _read_json(REPORTS_DIR / "by_expert_supports_market_stat_direction_combined.json").get("rows", [])
    agreement_rows = _read_json(REPORTS_DIR / "by_expert_agreement_market_stat_direction_combined.json").get("rows", [])

    expert_rows = [
        row for row in expert_rows
        if not _looks_malformed_name(str(row.get("expert_name") or ""))
    ]
    agreement_rows = [
        row for row in agreement_rows
        if not any(_looks_malformed_name(name) for name in _parse_agreement_experts(row.get("experts") or []))
    ]

    filtered_expert = _filter_rows(expert_rows, watchlist_min_n)
    filtered_agreement = _filter_rows(agreement_rows, watchlist_min_n)

    candidates = [
        _expert_pattern_entry(row, active_min_n) for row in filtered_expert
    ] + [
        _agreement_pattern_entry(row, active_min_n) for row in filtered_agreement
    ]
    candidates.sort(key=lambda row: (
        row.get("candidate_status") != "active",
        -(float(row.get("wilson_lower", 0.0) or 0.0)),
        -(int(row.get("n", 0) or 0)),
        row.get("id", ""),
    ))

    active_rows = [row for row in candidates if row.get("candidate_status") == "active"]
    watchlist_rows = [row for row in candidates if row.get("candidate_status") == "watchlist"]
    return {
        "active": {
            "meta": {
                "source": "trusted_combined_reports",
                "watchlist_min_n": watchlist_min_n,
                "active_min_n": active_min_n,
                "b_tier_wilson": B_TIER_WILSON,
                "a_tier_wilson": A_TIER_WILSON,
                "a_tier_min_n": A_TIER_MIN_N,
                "row_count": len(active_rows),
            },
            "rows": active_rows,
        },
        "watchlist": {
            "meta": {
                "source": "trusted_combined_reports",
                "watchlist_min_n": watchlist_min_n,
                "active_min_n": active_min_n,
                "b_tier_wilson": B_TIER_WILSON,
                "a_tier_wilson": A_TIER_WILSON,
                "a_tier_min_n": A_TIER_MIN_N,
                "row_count": len(watchlist_rows),
            },
            "rows": watchlist_rows,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build trusted NBA pattern registry candidate files")
    parser.add_argument("--watchlist-min-n", type=int, default=WATCHLIST_MIN_N)
    parser.add_argument("--active-min-n", type=int, default=ACTIVE_MIN_N)
    args = parser.parse_args()

    payload = build_candidates(args.watchlist_min_n, args.active_min_n)
    active_path = REPORTS_DIR / "pattern_registry_active.json"
    watchlist_path = REPORTS_DIR / "pattern_registry_watchlist.json"
    active_path.write_text(json.dumps(payload["active"], indent=2, ensure_ascii=False) + "\n")
    watchlist_path.write_text(json.dumps(payload["watchlist"], indent=2, ensure_ascii=False) + "\n")
    print(json.dumps({
        "active_path": str(active_path),
        "watchlist_path": str(watchlist_path),
        "active_rows": payload["active"]["meta"]["row_count"],
        "watchlist_rows": payload["watchlist"]["meta"]["row_count"],
    }, indent=2))


if __name__ == "__main__":
    main()
