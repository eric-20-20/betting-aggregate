import json
from collections import defaultdict, Counter
from pathlib import Path

# EDIT: point this at the file(s) where duplicates show up
INPUTS = [
    Path("data/ledger/signals_occurrences.jsonl"),   # example
    # Path("data/runs/<RUN_ID>/signals.jsonl"),
]

OUT = Path("data/history/betql_game_props/debug")
OUT.mkdir(parents=True, exist_ok=True)

def norm_side(x):
    if not x: return None
    x = str(x).strip().lower()
    if x in ("over","o"): return "over"
    if x in ("under","u"): return "under"
    return x

def norm_line(x):
    if x is None: return None
    try:
        v = float(x)
        return f"{v:.2f}".rstrip("0").rstrip(".")
    except Exception:
        return str(x).strip()

def get(rec, *keys):
    for k in keys:
        v = rec.get(k)
        if v not in (None, "", []):
            return v
    return None

def canonical_key(rec):
    """
    This should match how YOU think a unique prop should be identified downstream.
    Adjust keys if your schema differs.
    """
    event_key  = get(rec, "event_key", "event", "match_key")
    player_key = get(rec, "player_key", "player_id", "player")
    stat_key   = get(rec, "atomic_stat", "stat_key", "market_key", "stat")
    side       = norm_side(get(rec, "direction", "side"))
    line       = norm_line(get(rec, "line", "handicap", "total"))
    source     = get(rec, "source", "source_id", "source_name")

    return f"{event_key}|{player_key}|{stat_key}|{side}|{line}|{source}"

seen = defaultdict(list)
tot = 0
missing = Counter()

for p in INPUTS:
    if not p.exists():
        raise SystemExit(f"Missing: {p}")
    with p.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            tot += 1

            k = canonical_key(rec)
            if "None" in k:
                missing["missing_parts_in_key"] += 1

            seen[k].append((str(p), i, rec))

dupes = {k:v for k,v in seen.items() if len(v) > 1}

summary = {
    "inputs": [str(x) for x in INPUTS],
    "total_rows": tot,
    "unique_keys": len(seen),
    "duplicate_keys": len(dupes),
    "missing": dict(missing),
}
(OUT / "canonical_dupe_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

sample_path = OUT / "canonical_dupes_sample.jsonl"
MAX = 200
with sample_path.open("w", encoding="utf-8") as out:
    for k, rows in sorted(dupes.items(), key=lambda kv: len(kv[1]), reverse=True)[:MAX]:
        out.write(json.dumps({
            "key": k,
            "count": len(rows),
            "instances": [
                {"path": path, "lineno": ln,
                 "selection": r.get("selection"),
                 "event_key": r.get("event_key"),
                 "player_key": r.get("player_key"),
                 "atomic_stat": r.get("atomic_stat"),
                 "direction": r.get("direction"),
                 "line": r.get("line"),
                 "source": r.get("source") or r.get("source_id"),
                 "canonical_url": r.get("canonical_url"),
                 "raw_fingerprint": r.get("raw_fingerprint")}
                for (path, ln, r) in rows
            ]
        }) + "\n")

print("Wrote summary:", OUT / "canonical_dupe_summary.json")
print("Wrote sample:", sample_path)
print("Top counts:", sorted([len(v) for v in dupes.values()], reverse=True)[:10])