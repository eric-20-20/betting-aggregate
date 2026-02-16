# scripts/debug_betql_dupes.py
import json
from collections import defaultdict, Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# --- EDIT THESE ---
FILE_28 = Path("data/history/betql_game_props_raw/2026-01-28.jsonl")
FILE_29 = Path("data/history/betql_game_props_raw/2026-01-29.jsonl")
OUT_DIR = Path("data/history/betql_game_props/debug")
# ------------------

OUT_DIR.mkdir(parents=True, exist_ok=True)

def norm_side(x):
    if not x:
        return None
    x = str(x).strip().lower()
    if x in ("o", "over"):
        return "over"
    if x in ("u", "under"):
        return "under"
    # BetQL raw seems to use "UNDER"/"OVER" too
    if x in ("under", "over"):
        return x
    return x

def norm_line(x):
    if x is None:
        return None
    try:
        v = float(x)
        s = f"{v:.2f}".rstrip("0").rstrip(".")
        return s
    except Exception:
        return str(x).strip()

def norm_text(x):
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None

def parse_dt(x: Optional[str]) -> Optional[datetime]:
    """Parse ISO8601 with timezone; returns None if missing/bad."""
    if not x:
        return None
    try:
        # Works for '2026-02-06T01:18:51.009167+00:00'
        return datetime.fromisoformat(x)
    except Exception:
        return None

def iter_jsonl(path: Path) -> Iterable[Tuple[int, Dict[str, Any]]]:
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield i, json.loads(line)
            except Exception as e:
                yield i, {"_parse_error": str(e), "_raw": line}

def betql_prop_key(rec: Dict[str, Any]) -> Tuple[str, List[str]]:
    """
    Stable identity for BetQL game props.
    Key: canonical_url | player_name | stat | side | line
    Returns (key, missing_parts)
    """
    parts = []
    missing = []

    canonical_url = norm_text(rec.get("canonical_url"))
    if not canonical_url:
        missing.append("canonical_url")
    parts.append(canonical_url or "None")

    player_name = norm_text(rec.get("player_name"))
    if not player_name:
        missing.append("player_name")
    parts.append(player_name or "None")

    stat = norm_text(rec.get("stat") or rec.get("stat_raw"))
    if not stat:
        missing.append("stat")
    parts.append(stat or "None")

    side = norm_side(rec.get("direction") or rec.get("side"))
    if not side:
        missing.append("direction")
    parts.append(side or "None")

    line = norm_line(rec.get("line") or rec.get("handicap") or rec.get("total"))
    if not line:
        missing.append("line")
    parts.append(line or "None")

    return "|".join(parts), missing

@dataclass
class RowRef:
    tag: str
    path: str
    lineno: int
    rec: Dict[str, Any]

def pick_best(rows: List[RowRef]) -> RowRef:
    """
    Keep the row with latest observed_at_utc.
    Tie-breaker: prefer having odds, then keep first.
    """
    def score(r: RowRef):
        dt = parse_dt(r.rec.get("observed_at_utc"))
        odds_present = 1 if r.rec.get("odds") is not None else 0
        return (dt or datetime.min, odds_present)

    return sorted(rows, key=score, reverse=True)[0]

files = [("2026-01-28", FILE_28), ("2026-01-29", FILE_29)]

# per-file and combined buckets
seen_by_file: Dict[str, Dict[str, List[RowRef]]] = {tag: defaultdict(list) for tag, _ in files}
seen_all: Dict[str, List[RowRef]] = defaultdict(list)

totals = Counter()
missing_parts_counter = Counter()
parse_errors = 0

for tag, path in files:
    if not path.exists():
        raise SystemExit(f"Missing file: {path}")

    for lineno, rec in iter_jsonl(path):
        totals[tag] += 1

        if "_parse_error" in rec:
            parse_errors += 1
            continue

        k, missing = betql_prop_key(rec)
        if missing:
            missing_parts_counter.update(missing)

        ref = RowRef(tag=tag, path=str(path), lineno=lineno, rec=rec)
        seen_by_file[tag][k].append(ref)
        seen_all[k].append(ref)

# dupes within each file
dupes_within = {
    tag: {k: rows for k, rows in bucket.items() if len(rows) > 1}
    for tag, bucket in seen_by_file.items()
}

# dupes across combined
dupes_across = {k: rows for k, rows in seen_all.items() if len(rows) > 1}

summary = {
    "file_counts": dict(totals),
    "total_rows": sum(totals.values()),
    "parse_errors": parse_errors,
    "unique_prop_keys_within_files": {tag: len(seen_by_file[tag]) for tag, _ in files},
    "duplicate_prop_keys_within_files": {tag: len(dupes_within[tag]) for tag, _ in files},
    "unique_prop_keys_across_files": len(seen_all),
    "duplicate_prop_keys_across_files": len(dupes_across),
    "missing_key_parts_counts": dict(missing_parts_counter),
}

summary_path = OUT_DIR / "betql_dupe_summary.json"
summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

def write_dupe_sample(path: Path, dupes: Dict[str, List[RowRef]], max_keys: int = 200):
    written = 0
    with path.open("w", encoding="utf-8") as out:
        for k, rows in sorted(dupes.items(), key=lambda kv: len(kv[1]), reverse=True):
            if written >= max_keys:
                break

            instances = []
            for rr in rows:
                rec = rr.rec
                instances.append({
                    "tag": rr.tag,
                    "path": rr.path,
                    "lineno": rr.lineno,
                    "canonical_url": rec.get("canonical_url"),
                    "player_name": rec.get("player_name"),
                    "stat": rec.get("stat") or rec.get("stat_raw"),
                    "direction": rec.get("direction"),
                    "line": rec.get("line"),
                    "odds": rec.get("odds"),
                    "observed_at_utc": rec.get("observed_at_utc"),
                    "raw_fingerprint": rec.get("raw_fingerprint"),
                })

            out.write(json.dumps({
                "prop_key": k,
                "count": len(rows),
                "instances": instances,
            }) + "\n")
            written += 1

within_sample_path = OUT_DIR / "betql_dupes_within_files_sample.jsonl"
across_sample_path = OUT_DIR / "betql_dupes_across_files_sample.jsonl"
write_dupe_sample(within_sample_path, {k: v for tag in dupes_within for k, v in dupes_within[tag].items()})
write_dupe_sample(across_sample_path, dupes_across)

# Write a deduped combined file (latest observed_at_utc wins)
deduped_path = OUT_DIR / "betql_props_deduped_combined.jsonl"
kept = 0
with deduped_path.open("w", encoding="utf-8") as out:
    for k, rows in seen_all.items():
        best = pick_best(rows)
        out.write(json.dumps(best.rec, ensure_ascii=False) + "\n")
        kept += 1

print("Wrote:", summary_path)
print("Wrote:", within_sample_path)
print("Wrote:", across_sample_path)
print("Wrote:", deduped_path)
print("Deduped rows written:", kept)