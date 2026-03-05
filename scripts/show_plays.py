#!/usr/bin/env python3
"""Pretty-print today's plays for review."""
import json
import sys

date = sys.argv[1] if len(sys.argv) > 1 else "2026-02-25"
with open(f"data/plays/plays_{date}.json") as f:
    data = json.load(f)

plays = data["plays"]
meta = data["meta"]
print(f'=== PLAYS OF THE DAY: {meta["date"]} ({meta["day_of_week"]}) ===')
print(f'Total: {len(plays)} picks  |  A: {meta["tier_counts"]["A"]}  B: {meta["tier_counts"]["B"]}  C: {meta["tier_counts"]["C"]}  D: {meta["tier_counts"]["D"]}')
print("=" * 80)

current_tier = None
for p in plays:
    tier = p["tier"]
    if tier != current_tier:
        current_tier = tier
        labels = {
            "A": "A-TIER (55%+ Win Rate)",
            "B": "B-TIER (52-55%)",
            "C": "C-TIER (50-52%)",
            "D": "D-TIER (Below 50%)",
        }
        print(f'\n  --- {labels.get(tier, tier)} ---')

    sig = p["signal"]
    hr = p.get("historical_record", {})

    mkt = sig["market_type"]
    sel = sig.get("selection", "")
    line = sig.get("line")
    direction = sig.get("direction", "")
    stat = sig.get("atomic_stat", "")
    away = sig.get("away_team") or ""
    home = sig.get("home_team") or ""
    ek = sig.get("event_key") or ""

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

    if mkt == "player_prop":
        parts = sel.replace("NBA:", "").split("::")
        player = parts[0].replace("_", " ").title() if parts else "?"
        d = "Over" if "OVER" in direction else "Under"
        line_str = f" {line}" if line is not None else ""
        stat_str = stat.replace("_", " ").title() if stat else ""
        pick_str = f"{player} {d}{line_str} {stat_str}"
    elif mkt == "spread":
        sign = "+" if line and line > 0 else ""
        line_str = f" {sign}{line}" if line is not None else ""
        pick_str = f"{sel}{line_str} (Spread)"
    elif mkt == "total":
        d = "Over" if "OVER" in direction.upper() or "over" in direction else "Under"
        line_str = f" {line}" if line is not None else ""
        pick_str = f"{d}{line_str} (Total)"
    elif mkt == "moneyline":
        pick_str = f"{sel} ML"
    else:
        pick_str = sel

    sources = sig.get("sources_present", [])
    src_str = " + ".join(sorted(sources))

    if hr and hr.get("n", 0) > 0:
        wp = hr["win_pct"] * 100
        record_str = f'{hr["wins"]}-{hr["losses"]} ({wp:.1f}%)  n={hr["n"]}  from: {hr.get("label", "?")}'
    else:
        record_str = "No history"

    odds = sig.get("best_odds")
    if odds:
        odds_str = f"+{odds}" if odds > 0 else str(odds)
    else:
        odds_str = ""

    factors = p.get("factors", [])
    f_parts = []
    for f in factors:
        v = f["verdict"]
        icon = "+" if v == "positive" else ("-" if v == "negative" else "~")
        n = f.get("n", 0)
        wp = f.get("win_pct")
        wp_s = f"{wp * 100:.1f}%" if wp else "n/a"
        f_parts.append(f"{icon}{f['dimension']}({wp_s}, n={n})")
    factors_str = "  ".join(f_parts)

    print(f'  #{p["rank"]:2d}  {pick_str}')
    print(f"       {matchup}  |  {src_str}  |  {odds_str}")
    print(f"       Record: {record_str}")
    print(f"       Factors: {factors_str}")
    print()
