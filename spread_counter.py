import json
from collections import Counter, defaultdict

path = "data/analysis/graded_occurrences_latest.jsonl"

rows = []
with open(path) as f:
    for line in f:
        rows.append(json.loads(line))

spreads = [r for r in rows if r.get("market_type") == "spread"]

def is_team_only(sel):
    return isinstance(sel, str) and sel.isalpha() and len(sel) <= 4

stats = {
    "total_spreads": len(spreads),
    "line_missing": 0,
    "line_present": 0,
    "team_only_selection": 0,
    "has_direction": 0,
    "ineligible": 0,
}

dupe_keys = Counter()
dupe_occ_ids = Counter()
dupe_signal_ids = Counter()

for r in spreads:
    if r.get("line") is None:
        stats["line_missing"] += 1
    else:
        stats["line_present"] += 1

    if is_team_only(r.get("selection")):
        stats["team_only_selection"] += 1

    if r.get("direction"):
        stats["has_direction"] += 1

    if r.get("status") == "INELIGIBLE":
        stats["ineligible"] += 1

    key = (r.get("event_key"), r.get("selection"), r.get("line"))
    dupe_keys[key] += 1
    dupe_occ_ids[r.get("occurrence_id")] += 1
    dupe_signal_ids[r.get("signal_id")] += 1

print("=== Spread Stats ===")
for k, v in stats.items():
    print(f"{k}: {v}")

print("\n=== Duplication ===")
print("duplicate (event, selection, line):",
      sum(1 for v in dupe_keys.values() if v > 1))
print("duplicate occurrence_id:",
      sum(1 for v in dupe_occ_ids.values() if v > 1))
print("duplicate signal_id:",
      sum(1 for v in dupe_signal_ids.values() if v > 1))