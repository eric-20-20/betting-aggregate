#!/usr/bin/env python3
"""Show all ingested signals for a date, one per block."""
import json
import sys
from collections import Counter

date = sys.argv[1] if len(sys.argv) > 1 else "2026-02-25"
target_key = f"NBA:{date.replace('-', ':')}"

signals = []
with open("data/ledger/signals_latest.jsonl") as f:
    for line in f:
        row = json.loads(line.strip())
        if row.get("day_key") == target_key:
            signals.append(row)

# Sort: multi-source first, then by market, then selection
signals.sort(key=lambda s: (
    -len(s.get("sources_present") or s.get("sources") or []),
    s.get("market_type", ""),
    s.get("selection", ""),
))

print(f"=== ALL INGESTED SIGNALS FOR {date} ===")
print(f"Total: {len(signals)}")

src_counts = Counter(len(s.get("sources_present") or s.get("sources") or []) for s in signals)
for k in sorted(src_counts, reverse=True):
    print(f"  {k} source(s): {src_counts[k]}")
print("=" * 70)

for i, s in enumerate(signals, 1):
    mkt = s.get("market_type", "?")
    sel = s.get("selection", "?")
    line = s.get("line")
    direction = s.get("direction") or ""
    stat = s.get("atomic_stat", "")
    away = s.get("away_team") or ""
    home = s.get("home_team") or ""
    ek = s.get("event_key") or ""
    odds = s.get("best_odds")
    sig_type = s.get("signal_type", "")

    # Matchup
    if away and home:
        matchup = f"{away} @ {home}"
    elif "@" in ek:
        at_idx = ek.rfind("@")
        before = ek[:at_idx]
        colon_idx = before.rfind(":")
        a = before[colon_idx + 1:] if colon_idx >= 0 else before
        h = ek[at_idx + 1:]
        matchup = f"{a} @ {h}"
    else:
        matchup = "?"

    # Pick display
    if mkt == "player_prop":
        parts = sel.replace("NBA:", "").split("::")
        player = parts[0].replace("_", " ").title() if parts else "?"
        d = "Over" if "OVER" in direction else "Under" if "UNDER" in direction else direction
        line_str = f" {line}" if line is not None else ""
        stat_str = stat.replace("_", " ").title() if stat else ""
        pick_str = f"{player} {d}{line_str} {stat_str}"
    elif mkt == "spread":
        sign = "+" if line and line > 0 else ""
        line_str = f" {sign}{line}" if line is not None else ""
        pick_str = f"{sel}{line_str} (Spread)"
    elif mkt == "total":
        d = direction.title() if direction else "?"
        if "over" in d.lower():
            d = "Over"
        elif "under" in d.lower():
            d = "Under"
        line_str = f" {line}" if line is not None else ""
        pick_str = f"{d}{line_str} (Total)"
    elif mkt == "moneyline":
        pick_str = f"{sel} ML"
    else:
        pick_str = sel

    sources = s.get("sources_present") or s.get("sources") or []
    src_str = " + ".join(sorted(sources))
    n_src = len(sources)

    experts = s.get("experts") or []
    expert_str = ", ".join(experts) if experts else "none"

    odds_str = f"{'+' if odds and odds > 0 else ''}{odds}" if odds else "n/a"

    conflict = " [CONFLICT]" if "conflict" in sig_type else ""
    consensus = f" [CONSENSUS {n_src}x]" if n_src >= 2 and not conflict else ""

    print(f"\n  #{i:3d}  {pick_str}{conflict}{consensus}")
    print(f"        Game:    {matchup}")
    print(f"        Market:  {mkt}")
    print(f"        Sources: {src_str} ({n_src})")
    print(f"        Experts: {expert_str}")
    print(f"        Odds:    {odds_str}")
    if conflict:
        print(f"        Type:    {sig_type}")
