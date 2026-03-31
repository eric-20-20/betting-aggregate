"""Audit NBA spread occurrences that are missing lines in signals_occurrences.jsonl.

Outputs:
  - data/reports/spread_missing_line_audit.json
Prints:
  - Top missing-line breakdowns by sources_combo, support source_id, source_surface
"""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


INPUT_PATH = Path("data/ledger/signals_occurrences.jsonl")
OUTPUT_PATH = Path("data/reports/spread_missing_line_audit.json")
LINE_LIKE_RE = re.compile(r"[+-]\d+(?:\.\d+)?")
SAMPLE_LIMIT = 200


def read_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)


def normalize_occ_source_id(raw: Any) -> str:
    if not isinstance(raw, str):
        return "unknown"
    val = raw.strip().lower()
    if not val:
        return "unknown"
    if val == "actionnetwork":
        return "action"
    if val in {"action", "covers", "betql", "sportscapping"}:
        return val
    return val


def _safe_float(x: Any) -> Optional[float]:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def supports_summary(supports: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for sup in supports or []:
        if not isinstance(sup, dict):
            continue
        out.append(
            {
                "source_id": sup.get("source_id") or sup.get("source"),
                "selection": sup.get("selection"),
                "direction": sup.get("direction"),
                "line": sup.get("line"),
                "stat_key": sup.get("stat_key"),
                "atomic_stat": sup.get("atomic_stat"),
                "canonical_url": sup.get("canonical_url"),
            }
        )
    return out


def collect_line_like_strings(row: Dict[str, Any]) -> List[str]:
    hits: List[str] = []

    def _check(val: Any) -> None:
        if not val:
            return
        if not isinstance(val, str):
            return
        m = LINE_LIKE_RE.search(val)
        if m:
            hits.append(m.group(0))

    # top-level selection_example
    _check(row.get("selection_example"))

    # supports fields
    for sup in row.get("supports") or []:
        if not isinstance(sup, dict):
            continue
        _check(sup.get("selection"))
        _check(sup.get("canonical_url"))
        _check(str(sup))

    return hits


def main() -> None:
    spreads_total = 0
    missing_count = 0
    missing_by_sources_combo: Counter[str] = Counter()
    missing_by_occ_source: Counter[str] = Counter()
    missing_by_support_source: Counter[str] = Counter()
    missing_by_source_surface: Counter[str] = Counter()
    missing_with_urls = 0
    missing_with_line_like = 0
    samples: List[Dict[str, Any]] = []

    for row in read_jsonl(INPUT_PATH):
        market = (row.get("market_type") or "").lower()
        if market != "spread":
            continue
        spreads_total += 1
        line_val = _safe_float(row.get("line"))
        if line_val is not None:
            continue

        missing_count += 1
        sources_combo = row.get("sources_combo") or "unknown"
        missing_by_sources_combo[sources_combo] += 1

        occ_source_id = normalize_occ_source_id(row.get("source_id") or row.get("source_surface"))
        missing_by_occ_source[occ_source_id] += 1

        missing_by_source_surface[(row.get("source_surface") or "unknown")] += 1

        supports = row.get("supports") or []
        for sup in supports:
            if not isinstance(sup, dict):
                continue
            sid = sup.get("source_id") or sup.get("source")
            if sid:
                missing_by_support_source[sid] += 1

        if row.get("urls") or row.get("canonical_url"):
            missing_with_urls += 1

        line_like_hits = collect_line_like_strings(row)
        has_line_like = bool(line_like_hits)
        if has_line_like:
            missing_with_line_like += 1

        if len(samples) < SAMPLE_LIMIT:
            samples.append(
                {
                    "day_key": row.get("day_key"),
                    "event_key": row.get("event_key"),
                    "selection": row.get("selection"),
                    "direction": row.get("direction"),
                    "market_type": row.get("market_type"),
                    "away_team": row.get("away_team"),
                    "home_team": row.get("home_team"),
                    "sources_present": row.get("sources_present"),
                    "sources_combo": row.get("sources_combo"),
                    "signal_id": row.get("signal_id"),
                    "occurrence_id": row.get("occurrence_id"),
                    "source_surface": row.get("source_surface"),
                    "canonical_url": row.get("canonical_url"),
                    "urls": row.get("urls"),
                    "supports_summary": supports_summary(supports),
                    "line_like_strings": line_like_hits,
                    "has_line_like": has_line_like,
                }
            )

    meta = {
        "total_spread_occurrences": spreads_total,
        "missing_line_count": missing_count,
        "missing_line_by_signal_sources_combo": missing_by_sources_combo.most_common(20),
        "missing_line_by_occ_source_id": missing_by_occ_source.most_common(20),
        "missing_line_by_support_source_id": missing_by_support_source.most_common(20),
        "missing_line_by_source_surface": missing_by_source_surface.most_common(20),
        "missing_line_with_urls_count": missing_with_urls,
        "missing_line_with_support_line_like_text_count": missing_with_line_like,
        "missing_line_with_support_line_like_pct": (missing_with_line_like / missing_count) if missing_count else 0.0,
    }

    write_json(
        OUTPUT_PATH,
        {
            "meta": meta,
            "rows": samples,
        },
    )

    print(f"[audit] total_spreads={spreads_total} missing_line={missing_count}")
    print(f"[audit] missing_line_by_sources_combo top10: {missing_by_sources_combo.most_common(10)}")
    print(f"[audit] missing_line_by_support_source_id top10: {missing_by_support_source.most_common(10)}")
    print(f"[audit] missing_line_by_source_surface top10: {missing_by_source_surface.most_common(10)}")
    pct_line_like = (missing_with_line_like / missing_count * 100.0) if missing_count else 0.0
    print(f"[audit] missing_line with line-like strings: {missing_with_line_like}/{missing_count} ({pct_line_like:.1f}%)")


if __name__ == "__main__":
    main()
