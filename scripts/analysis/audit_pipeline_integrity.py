#!/usr/bin/env python3
"""
Comprehensive Data Integrity Audit for Betting Signal Pipeline.
Checks: signal counts, consensus combos, grade re-verification,
        cross-source merges, report math, dimers distribution.
"""
import json
import os
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path

random.seed(42)  # Reproducible sampling

BASE = Path("/Users/Ericevans/Betting Aggregate")
LEDGER = BASE / "data" / "ledger"
CACHE  = BASE / "data" / "cache" / "results"
REPORTS = BASE / "data" / "reports"

# Helpers
PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"
WARN = "\033[93mWARN\033[0m"
INFO = "\033[94mINFO\033[0m"

results_summary = []

def record(section, name, ok, detail=""):
    tag = PASS if ok else FAIL
    results_summary.append((section, name, "PASS" if ok else "FAIL", detail))
    print(f"  [{tag}] {name}" + (f" -- {detail}" if detail else ""))

def record_warn(section, name, detail=""):
    results_summary.append((section, name, "WARN", detail))
    print(f"  [{WARN}] {name}" + (f" -- {detail}" if detail else ""))

def record_info(section, name, detail=""):
    results_summary.append((section, name, "INFO", detail))
    print(f"  [{INFO}] {name}" + (f" -- {detail}" if detail else ""))

def load_jsonl(path):
    rows = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows

# Load data
print("Loading data files...")
signals = load_jsonl(LEDGER / "signals_latest.jsonl")
print(f"  Loaded {len(signals)} signals")

occurrences = load_jsonl(LEDGER / "signals_occurrences.jsonl")
print(f"  Loaded {len(occurrences)} occurrences")

grades = load_jsonl(LEDGER / "grades_latest.jsonl")
print(f"  Loaded {len(grades)} grades")

grades_by_signal_id = {g["signal_id"]: g for g in grades}
signals_by_id = {s["signal_id"]: s for s in signals}

print()

# ===================================================================
# SECTION 1: Signal Count Validation
# ===================================================================
print("=" * 70)
print("SECTION 1: Signal Count Validation")
print("=" * 70)

record("1", f"Total signals: {len(signals)}", len(signals) > 0)
record("1", f"Total occurrences: {len(occurrences)}", len(occurrences) > 0)

signal_ids = [s["signal_id"] for s in signals]
dup_count = len(signal_ids) - len(set(signal_ids))
record("1", f"No duplicate signal_id values (dupes={dup_count})", dup_count == 0)
if dup_count > 0:
    id_counts = Counter(signal_ids)
    top_dupes = id_counts.most_common(5)
    for sid, cnt in top_dupes:
        if cnt > 1:
            print(f"    Duplicate: {sid[:40]}... x{cnt}")

actionnetwork_count = sum(1 for s in signals if s.get("source_id") == "actionnetwork")
record("1", f"No source_id='actionnetwork' signals (found={actionnetwork_count})", actionnetwork_count == 0)

actionnetwork_in_sources = sum(1 for s in signals if "actionnetwork" in (s.get("sources") or []))
record("1", f"No 'actionnetwork' in sources arrays (found={actionnetwork_in_sources})", actionnetwork_in_sources == 0)

actionnetwork_in_combo = sum(1 for s in signals if "actionnetwork" in (s.get("sources_combo") or ""))
record("1", f"No 'actionnetwork' in sources_combo (found={actionnetwork_in_combo})", actionnetwork_in_combo == 0)

print()

# ===================================================================
# SECTION 2: Consensus Combo Validation
# ===================================================================
print("=" * 70)
print("SECTION 2: Consensus Combo Validation")
print("=" * 70)

multi_source_signals = [s for s in signals if "|" in (s.get("sources_combo") or "")]
record_info("2", f"Multi-source signals count: {len(multi_source_signals)}")

combo_mismatches = 0
mismatch_examples = []
for s in multi_source_signals:
    combo_sources = set(s["sources_combo"].split("|"))
    present_sources = set(s.get("sources_present") or s.get("sources") or [])
    if not combo_sources.issubset(present_sources):
        combo_mismatches += 1
        if len(mismatch_examples) < 3:
            mismatch_examples.append({
                "signal_id": s["signal_id"][:30],
                "combo": s["sources_combo"],
                "present": list(present_sources),
                "missing": list(combo_sources - present_sources)
            })

record("2", f"All combo sources exist in sources_present (mismatches={combo_mismatches})",
       combo_mismatches == 0)
for ex in mismatch_examples:
    print(f"    Example: combo={ex['combo']}, present={ex['present']}, missing={ex['missing']}")

combo_counts = Counter(s["sources_combo"] for s in multi_source_signals)
print(f"  Multi-source combo distribution:")
for combo, cnt in combo_counts.most_common(15):
    print(f"    {combo}: {cnt}")

sample_multi = random.sample(multi_source_signals, min(10, len(multi_source_signals)))
support_check_failures = 0
for s in sample_multi:
    combo_sources = set(s["sources_combo"].split("|"))
    present = set(s.get("sources_present") or s.get("sources") or [])
    if not combo_sources.issubset(present):
        support_check_failures += 1
        print(f"    FAIL spot-check: signal {s['signal_id'][:30]} combo={s['sources_combo']} "
              f"present={present}")

record("2", f"Spot-check 10 multi-source: supports match combo (failures={support_check_failures})",
       support_check_failures == 0)

with open(REPORTS / "by_sources_combo_record.json") as f:
    combo_report = json.load(f)

combo_report_meta = combo_report["meta"]
combo_report_rows = combo_report["rows"]

total_w = sum(r.get("wins", 0) for r in combo_report_rows)
total_l = sum(r.get("losses", 0) for r in combo_report_rows)
total_p = sum(r.get("pushes", 0) for r in combo_report_rows)
total_n = sum(r.get("n", 0) for r in combo_report_rows)
report_graded = combo_report_meta.get("graded_rows", 0)

record("2", f"Sum combo N ({total_n}) == report graded_rows ({report_graded})",
       total_n == report_graded, f"W={total_w} L={total_l} P={total_p}")

wlp_sum = total_w + total_l + total_p
record("2", f"W+L+P ({wlp_sum}) == total N ({total_n})",
       wlp_sum == total_n)

record_info("2", f"Report combo buckets: {len(combo_report_rows)} unique combos")

print()

# ===================================================================
# SECTION 3: Grade Re-verification
# ===================================================================
print("=" * 70)
print("SECTION 3: Grade Re-verification")
print("=" * 70)

STAT_MAP = {
    "points": "points",
    "rebounds": "reboundsTotal",
    "assists": "assists",
    "steals": "steals",
    "blocks": "blocks",
    "turnovers": "turnovers",
    "threes": "threePointersMade",
    "three_pointers_made": "threePointersMade",
    "pts_reb": ["points", "reboundsTotal"],
    "pts_ast": ["points", "assists"],
    "reb_ast": ["reboundsTotal", "assists"],
    "pts_reb_ast": ["points", "reboundsTotal", "assists"],
}

def find_player_in_boxscore(boxscore_data, player_id_hint, team_hint=None):
    game = boxscore_data.get("game", {})
    all_players = []
    for team_key in ["homeTeam", "awayTeam"]:
        team = game.get(team_key, {})
        tricode = team.get("teamTricode", "")
        for p in team.get("players", []):
            all_players.append({
                "name": p.get("name", ""),
                "nameI": p.get("nameI", ""),
                "familyName": p.get("familyName", ""),
                "firstName": p.get("firstName", ""),
                "stats": p.get("statistics", {}),
                "team": tricode
            })
    
    clean_name = player_id_hint.replace("p_", "").replace("_", " ").lower()
    
    for p in all_players:
        last = p.get("familyName", "").lower()
        full = p.get("name", "").lower()
        nameI = p.get("nameI", "").lower()
        if clean_name in full or clean_name == last or clean_name in nameI.lower():
            return p
    return None

def get_stat_value(stats, stat_key):
    mapping = STAT_MAP.get(stat_key)
    if mapping is None:
        return None
    if isinstance(mapping, list):
        total = 0
        for field in mapping:
            val = stats.get(field)
            if val is None:
                return None
            total += val
        return total
    else:
        return stats.get(mapping)

def parse_selection(selection_str):
    parts = selection_str.split("::")
    if len(parts) >= 3:
        player_part = parts[0]
        stat_key = parts[1]
        direction = parts[2]
        player_id = player_part.split(":")[-1] if ":" in player_part else player_part
        return player_id, stat_key, direction
    return None, None, None

def compute_grade(stat_value, line, direction):
    if stat_value is None or line is None:
        return None
    if direction == "OVER":
        if stat_value > line:
            return "WIN"
        elif stat_value < line:
            return "LOSS"
        else:
            return "PUSH"
    elif direction == "UNDER":
        if stat_value < line:
            return "WIN"
        elif stat_value > line:
            return "LOSS"
        else:
            return "PUSH"
    return None

rederivable_grades = []
for g in grades:
    if g.get("status") in ("WIN", "LOSS", "PUSH") and g.get("provider_game_id"):
        signal = signals_by_id.get(g["signal_id"])
        if signal:
            rederivable_grades.append((g, signal))

print(f"  Rederivable grades (with game_id): {len(rederivable_grades)}")

by_market = defaultdict(list)
for g, s in rederivable_grades:
    by_market[s.get("market_type", "unknown")].append((g, s))

print(f"  Market type distribution in rederivable grades:")
for mt, items in sorted(by_market.items()):
    print(f"    {mt}: {len(items)}")

sample_grades = []
for mt in ["spread", "total", "moneyline", "player_prop"]:
    avail = by_market.get(mt, [])
    take = min(5, len(avail))
    sample_grades.extend(random.sample(avail, take))

while len(sample_grades) < 20 and len(by_market.get("player_prop", [])) > len(sample_grades):
    extra = random.choice(by_market["player_prop"])
    if extra not in sample_grades:
        sample_grades.append(extra)

random.shuffle(sample_grades)
sample_grades = sample_grades[:20]

regrade_pass = 0
regrade_fail = 0
regrade_skip = 0
regrade_details = []

for g, s in sample_grades:
    game_id = g.get("provider_game_id")
    market = s.get("market_type")
    selection = g.get("selection") or s.get("selection")
    recorded_result = g.get("result") or g.get("status")
    
    boxscore_path = CACHE / f"nba_cdn_boxscore_{game_id}.json"
    
    if market == "player_prop":
        player_id, stat_key, direction = parse_selection(selection)
        line = s.get("line")
        if line is None:
            line = g.get("line")
        
        if not os.path.exists(boxscore_path):
            regrade_skip += 1
            regrade_details.append(f"  SKIP: {selection} -- no boxscore file for {game_id}")
            continue
        
        with open(boxscore_path) as f:
            box = json.load(f)
        
        player_data = find_player_in_boxscore(box, player_id)
        if player_data is None:
            regrade_skip += 1
            regrade_details.append(f"  SKIP: {selection} -- player {player_id} not found in boxscore")
            continue
        
        actual_stat = get_stat_value(player_data["stats"], stat_key)
        if actual_stat is None:
            regrade_skip += 1
            regrade_details.append(f"  SKIP: {selection} -- stat '{stat_key}' not available")
            continue
        
        expected = compute_grade(actual_stat, line, direction)
        if expected is None:
            regrade_skip += 1
            regrade_details.append(f"  SKIP: {selection} -- could not compute grade")
            continue
        
        if expected == recorded_result:
            regrade_pass += 1
            regrade_details.append(
                f"  OK: {selection} line={line} stat={actual_stat} => {expected} (recorded={recorded_result})")
        else:
            regrade_fail += 1
            regrade_details.append(
                f"  MISMATCH: {selection} line={line} stat={actual_stat} => {expected} (recorded={recorded_result})")
    
    elif market in ("spread", "total"):
        if not os.path.exists(boxscore_path):
            regrade_skip += 1
            regrade_details.append(f"  SKIP: {market} {selection} -- no boxscore for {game_id}")
            continue
        
        with open(boxscore_path) as f:
            box = json.load(f)
        
        game = box.get("game", {})
        home_score = game.get("homeTeam", {}).get("score")
        away_score = game.get("awayTeam", {}).get("score")
        home_tri = game.get("homeTeam", {}).get("teamTricode")
        away_tri = game.get("awayTeam", {}).get("teamTricode")
        
        if home_score is None or away_score is None:
            regrade_skip += 1
            continue
        
        line = s.get("line")
        if line is None:
            line = g.get("line")
        direction = s.get("direction") or g.get("direction")
        
        if market == "total":
            actual_total = home_score + away_score
            if direction == "OVER":
                if actual_total > line:
                    expected = "WIN"
                elif actual_total < line:
                    expected = "LOSS"
                else:
                    expected = "PUSH"
            elif direction == "UNDER":
                if actual_total < line:
                    expected = "WIN"
                elif actual_total > line:
                    expected = "LOSS"
                else:
                    expected = "PUSH"
            else:
                regrade_skip += 1
                regrade_details.append(f"  SKIP: total -- direction={direction}")
                continue
            
            if expected == recorded_result:
                regrade_pass += 1
                regrade_details.append(
                    f"  OK: total {direction} {line} actual={actual_total} => {expected}")
            else:
                regrade_fail += 1
                regrade_details.append(
                    f"  MISMATCH: total {direction} {line} actual={actual_total} => {expected} (recorded={recorded_result})")
        
        elif market == "spread":
            sel = selection or ""
            sig_home = s.get("home_team")
            sig_away = s.get("away_team")
            
            if sig_home == home_tri and sig_away == away_tri:
                h_score, a_score = home_score, away_score
            elif sig_home == away_tri and sig_away == home_tri:
                h_score, a_score = away_score, home_score
            else:
                regrade_skip += 1
                regrade_details.append(
                    f"  SKIP: spread -- team mismatch sig({sig_home}/{sig_away}) vs box({home_tri}/{away_tri})")
                continue
            
            if "HOME" in sel.upper() or (sig_home and sig_home in sel):
                margin = h_score - a_score
            elif "AWAY" in sel.upper() or (sig_away and sig_away in sel):
                margin = a_score - h_score
            else:
                if direction and "HOME" in str(direction).upper():
                    margin = h_score - a_score
                elif direction and "AWAY" in str(direction).upper():
                    margin = a_score - h_score
                else:
                    regrade_skip += 1
                    regrade_details.append(f"  SKIP: spread -- cannot determine team side from '{sel}' dir='{direction}'")
                    continue
            
            covered = margin + line
            if covered > 0:
                expected = "WIN"
            elif covered < 0:
                expected = "LOSS"
            else:
                expected = "PUSH"
            
            if expected == recorded_result:
                regrade_pass += 1
                regrade_details.append(
                    f"  OK: spread line={line} margin={margin} covered={covered} => {expected}")
            else:
                regrade_fail += 1
                regrade_details.append(
                    f"  MISMATCH: spread line={line} margin={margin} covered={covered} => {expected} (recorded={recorded_result})")
    
    elif market == "moneyline":
        if not os.path.exists(boxscore_path):
            regrade_skip += 1
            continue
        
        with open(boxscore_path) as f:
            box = json.load(f)
        
        game = box.get("game", {})
        home_score = game.get("homeTeam", {}).get("score")
        away_score = game.get("awayTeam", {}).get("score")
        home_tri = game.get("homeTeam", {}).get("teamTricode")
        away_tri = game.get("awayTeam", {}).get("teamTricode")
        sig_home = s.get("home_team")
        sig_away = s.get("away_team")
        sel = selection or ""
        direction = s.get("direction") or g.get("direction") or ""
        
        if home_score is None or away_score is None:
            regrade_skip += 1
            continue
        
        if sig_home == home_tri and sig_away == away_tri:
            h_score, a_score = home_score, away_score
        elif sig_home == away_tri and sig_away == home_tri:
            h_score, a_score = away_score, home_score
        else:
            regrade_skip += 1
            continue
        
        if "HOME" in sel.upper() or "HOME" in direction.upper() or (sig_home and sig_home in sel):
            won = h_score > a_score
        elif "AWAY" in sel.upper() or "AWAY" in direction.upper() or (sig_away and sig_away in sel):
            won = a_score > h_score
        else:
            regrade_skip += 1
            regrade_details.append(f"  SKIP: ML -- cannot determine side from '{sel}' dir='{direction}'")
            continue
        
        expected = "WIN" if won else "LOSS"
        if expected == recorded_result:
            regrade_pass += 1
            regrade_details.append(
                f"  OK: ML {sel[:50]} => {expected}")
        else:
            regrade_fail += 1
            regrade_details.append(
                f"  MISMATCH: ML {sel[:50]} scores={h_score}-{a_score} => {expected} (recorded={recorded_result})")
    else:
        regrade_skip += 1

for d in regrade_details:
    print(d)

record("3", f"Grade re-verification: {regrade_pass} pass, {regrade_fail} fail, {regrade_skip} skip",
       regrade_fail == 0)

# Combo stat grades
print()
print("  --- Combo Stat Grade Check ---")
COMBO_STATS = {"pts_reb", "pts_ast", "reb_ast", "pts_reb_ast"}

combo_stat_grades = []
for g, s in rederivable_grades:
    sel = g.get("selection") or s.get("selection") or ""
    parts = sel.split("::")
    if len(parts) >= 2 and parts[1] in COMBO_STATS:
        combo_stat_grades.append((g, s))

print(f"  Total combo stat grades found: {len(combo_stat_grades)}")

sample_combo = random.sample(combo_stat_grades, min(10, len(combo_stat_grades)))
combo_pass = 0
combo_fail = 0
combo_skip = 0

for g, s in sample_combo:
    game_id = g.get("provider_game_id")
    selection = g.get("selection") or s.get("selection")
    player_id, stat_key, direction = parse_selection(selection)
    line = s.get("line") or g.get("line")
    recorded_result = g.get("result") or g.get("status")
    recorded_stat_value = g.get("stat_value")
    
    boxscore_path = CACHE / f"nba_cdn_boxscore_{game_id}.json"
    if not os.path.exists(boxscore_path):
        combo_skip += 1
        print(f"    SKIP: {selection} -- no boxscore")
        continue
    
    with open(boxscore_path) as f:
        box = json.load(f)
    
    player_data = find_player_in_boxscore(box, player_id)
    if player_data is None:
        combo_skip += 1
        print(f"    SKIP: {selection} -- player not found")
        continue
    
    actual_combo_val = get_stat_value(player_data["stats"], stat_key)
    if actual_combo_val is None:
        combo_skip += 1
        print(f"    SKIP: {selection} -- stat not available")
        continue
    
    stat_match = (recorded_stat_value is not None and abs(recorded_stat_value - actual_combo_val) < 0.01)
    expected_grade = compute_grade(actual_combo_val, line, direction)
    grade_match = (expected_grade == recorded_result)
    
    if stat_match and grade_match:
        combo_pass += 1
        print(f"    OK: {selection} combo_stat={stat_key} actual={actual_combo_val} "
              f"recorded_sv={recorded_stat_value} line={line} => {expected_grade}")
    else:
        combo_fail += 1
        print(f"    MISMATCH: {selection} combo_stat={stat_key} actual_combo={actual_combo_val} "
              f"recorded_sv={recorded_stat_value} line={line} "
              f"expected={expected_grade} recorded={recorded_result} "
              f"stat_match={stat_match} grade_match={grade_match}")

record("3", f"Combo stat grades: {combo_pass} pass, {combo_fail} fail, {combo_skip} skip",
       combo_fail == 0)

print()

# ===================================================================
# SECTION 4: Cross-Source Merge Verification
# ===================================================================
print("=" * 70)
print("SECTION 4: Cross-Source Merge Verification")
print("=" * 70)

multi_with_supports = [s for s in multi_source_signals if s.get("supports")]
print(f"  Multi-source signals with supports: {len(multi_with_supports)}")

sample_merge = random.sample(multi_with_supports, min(10, len(multi_with_supports)))
merge_check_pass = 0
merge_check_fail = 0

for s in sample_merge:
    combo_sources = set(s["sources_combo"].split("|"))
    supports = s.get("supports", [])
    support_sources_found = set(sup.get("source_id") for sup in supports if sup.get("source_id"))
    
    missing_in_supports = combo_sources - support_sources_found
    
    ok = True
    issues = []
    
    if missing_in_supports:
        issues.append(f"sources {missing_in_supports} not in supports array (may be pruned)")
    
    if len(support_sources_found) == 1 and len(combo_sources) > 1:
        ok = False
        issues.append(f"PHANTOM: combo={s['sources_combo']} but only {support_sources_found} in supports")
    
    if len(support_sources_found) == 0 and len(combo_sources) > 1:
        ok = False
        issues.append(f"PHANTOM: combo={s['sources_combo']} but NO sources in supports")
    
    if ok:
        merge_check_pass += 1
        print(f"    OK: {s['signal_id'][:25]} combo={s['sources_combo']} "
              f"supports_srcs={support_sources_found}" +
              (f" ({'; '.join(issues)})" if issues else ""))
    else:
        merge_check_fail += 1
        print(f"    FAIL: {s['signal_id'][:25]} combo={s['sources_combo']} -- {'; '.join(issues)}")

record("4", f"Cross-source merge verification: {merge_check_pass} pass, {merge_check_fail} fail",
       merge_check_fail == 0)

phantom_combos = 0
for s in multi_source_signals:
    supports = s.get("supports", [])
    support_src = set(sup.get("source_id") for sup in supports if sup.get("source_id"))
    combo_src = set(s["sources_combo"].split("|"))
    if len(support_src) < len(combo_src) and len(support_src) <= 1:
        phantom_combos += 1

record("4", f"No phantom single-source multi-combos (found={phantom_combos})",
       phantom_combos == 0)

# Additional: verify selection_key matches between supports
print()
print("  --- Selection Key Consistency ---")
sample_selkey = random.sample(multi_with_supports, min(10, len(multi_with_supports)))
selkey_pass = 0
selkey_fail = 0

for s in sample_selkey:
    signal_selection = s.get("selection", "")
    supports = s.get("supports", [])
    
    # Each support should agree on direction and stat
    signal_parts = signal_selection.split("::")
    if len(signal_parts) < 3:
        continue
    signal_stat = signal_parts[1] if len(signal_parts) >= 2 else None
    signal_dir = signal_parts[2] if len(signal_parts) >= 3 else None
    
    mismatch = False
    for sup in supports:
        sup_dir = sup.get("direction")
        sup_stat = sup.get("atomic_stat")
        if sup_dir and signal_dir and sup_dir != signal_dir:
            mismatch = True
            print(f"    DIRECTION MISMATCH: signal dir={signal_dir} support dir={sup_dir} "
                  f"source={sup.get('source_id')}")
        if sup_stat and signal_stat and sup_stat != signal_stat:
            mismatch = True
            print(f"    STAT MISMATCH: signal stat={signal_stat} support stat={sup_stat} "
                  f"source={sup.get('source_id')}")
    
    if mismatch:
        selkey_fail += 1
    else:
        selkey_pass += 1

record("4", f"Selection key consistency: {selkey_pass} pass, {selkey_fail} fail",
       selkey_fail == 0)

print()

# ===================================================================
# SECTION 5: Report Math Consistency
# ===================================================================
print("=" * 70)
print("SECTION 5: Report Math Consistency")
print("=" * 70)

with open(REPORTS / "by_sources_combo_record.json") as f:
    combo_rpt = json.load(f)

with open(REPORTS / "by_source_record.json") as f:
    source_rpt = json.load(f)

combo_total_n = sum(r["n"] for r in combo_rpt["rows"])
combo_graded_rows = combo_rpt["meta"]["graded_rows"]
record("5", f"Combo report: sum(N)={combo_total_n} == graded_rows={combo_graded_rows}",
       combo_total_n == combo_graded_rows)

source_graded_rows = source_rpt["meta"]["graded_rows"]
record("5", f"Source report graded_rows={source_graded_rows} == Combo report graded_rows={combo_graded_rows}",
       source_graded_rows == combo_graded_rows)

combo_total_w = sum(r["wins"] for r in combo_rpt["rows"])
combo_total_l = sum(r["losses"] for r in combo_rpt["rows"])
combo_total_p = sum(r["pushes"] for r in combo_rpt["rows"])

source_total_n = sum(r["n"] for r in source_rpt["rows"])
source_total_w = sum(r["wins"] for r in source_rpt["rows"])

print(f"  Combo report totals: W={combo_total_w} L={combo_total_l} P={combo_total_p} N={combo_total_n}")
print(f"  Source report totals: W={source_total_w} N={source_total_n}")

record_info("5", f"Source N ({source_total_n}) >= Combo N ({combo_total_n}) "
            f"(source counts per-source, combo counts uniquely)")

wlp = combo_total_w + combo_total_l + combo_total_p
record("5", f"Combo W+L+P ({wlp}) == N ({combo_total_n})",
       wlp == combo_total_n)

internal_issues = 0
for r in combo_rpt["rows"]:
    row_wlp = r["wins"] + r["losses"] + r["pushes"]
    if row_wlp != r["n"]:
        internal_issues += 1
        print(f"    Row {r['sources_combo']}: W+L+P={row_wlp} != N={r['n']}")

record("5", f"All combo rows internally consistent W+L+P==N (issues={internal_issues})",
       internal_issues == 0)

# Cross-check: signals that are graded vs grades file
graded_signal_ids = set(g["signal_id"] for g in grades if g.get("status") in ("WIN","LOSS","PUSH"))
signals_with_grade = set(s["signal_id"] for s in signals if s["signal_id"] in graded_signal_ids)
orphan_grades = graded_signal_ids - set(s["signal_id"] for s in signals)
record("5", f"Orphan grades (grade exists but no signal): {len(orphan_grades)}",
       len(orphan_grades) == 0)
if orphan_grades:
    for oid in list(orphan_grades)[:5]:
        print(f"    Orphan grade signal_id: {oid[:40]}")

print()

# ===================================================================
# SECTION 6: Dimers Consensus Check
# ===================================================================
print("=" * 70)
print("SECTION 6: Dimers Consensus Check")
print("=" * 70)

dimers_signals = [s for s in signals if "dimers" in (s.get("sources_combo") or "")]
dimers_as_source = [s for s in signals if "dimers" in (s.get("sources") or [])]

record_info("6", f"Signals mentioning 'dimers' in sources_combo: {len(dimers_signals)}")
record_info("6", f"Signals with 'dimers' in sources array: {len(dimers_as_source)}")

dimers_combo_dist = Counter(s["sources_combo"] for s in dimers_signals)
print(f"  Dimers combo distribution:")
for combo, cnt in dimers_combo_dist.most_common(20):
    print(f"    {combo}: {cnt}")

dimers_solo = sum(1 for s in dimers_signals if s.get("sources_combo") == "dimers")
dimers_consensus = len(dimers_signals) - dimers_solo
record_info("6", f"Dimers solo: {dimers_solo}, Dimers in consensus: {dimers_consensus}")

print(f"\n  Historical reference (before dedup):")
print(f"    dimers solo:             299")
print(f"    betql|dimers:             81")
print(f"    action|dimers:           217")
print(f"    action|betql|dimers:   1,063")
print(f"  Current:")
for combo in ["dimers", "betql|dimers", "action|dimers", "action|betql|dimers"]:
    cnt = dimers_combo_dist.get(combo, 0)
    print(f"    {combo}: {cnt}")

all_dimers_combos = set(dimers_combo_dist.keys())
print(f"\n  All unique combos containing 'dimers': {sorted(all_dimers_combos)}")

# Check: dimers signals should not have source_id="actionnetwork"
dimers_bad_source = sum(1 for s in dimers_signals if s.get("source_id") == "actionnetwork")
record("6", f"No dimers signals with source_id=actionnetwork (found={dimers_bad_source})",
       dimers_bad_source == 0)

print()

# ===================================================================
# FINAL SUMMARY
# ===================================================================
print("=" * 70)
print("FINAL AUDIT SUMMARY")
print("=" * 70)

pass_count = sum(1 for _, _, status, _ in results_summary if status == "PASS")
fail_count = sum(1 for _, _, status, _ in results_summary if status == "FAIL")
warn_count = sum(1 for _, _, status, _ in results_summary if status == "WARN")
info_count = sum(1 for _, _, status, _ in results_summary if status == "INFO")

print(f"\n  Total checks: {pass_count + fail_count}")
print(f"  PASS: {pass_count}")
print(f"  FAIL: {fail_count}")
print(f"  WARN: {warn_count}")
print(f"  INFO: {info_count}")
print()

if fail_count > 0:
    print("FAILURES:")
    for section, name, status, detail in results_summary:
        if status == "FAIL":
            print(f"  Section {section}: {name}")
            if detail:
                print(f"    -> {detail}")
    print()

if fail_count == 0:
    print("ALL CHECKS PASSED.")
else:
    print(f"{fail_count} CHECK(S) FAILED -- review above.")
