#!/usr/bin/env python3
"""
Grade Audit Script
==================
Independently re-derives WIN/LOSS/PUSH from raw score data and compares
against the recorded grade result. Flags any mismatches.

Covers all market types: spread, total, moneyline, player_prop.
Handles both standard and BetQL-style selections (e.g., "LAL_spread", "PHI_ml", "game_total").
"""

import json
import glob
import random
from collections import Counter, defaultdict

BASE = "/Users/Ericevans/Betting Aggregate"
GRADES_FILE = f"{BASE}/data/ledger/grades_latest.jsonl"
SIGNALS_FILE = f"{BASE}/data/ledger/signals_latest.jsonl"
CACHE_DIR = f"{BASE}/data/cache/results"

# Stat key -> CDN boxscore field mapping
STAT_MAP = {
    "points": ["points"],
    "rebounds": ["reboundsTotal"],
    "assists": ["assists"],
    "steals": ["steals"],
    "blocks": ["blocks"],
    "threes": ["threePointersMade"],
    "turnovers": ["turnovers"],
    "pts_reb": ["points", "reboundsTotal"],
    "pts_ast": ["points", "assists"],
    "reb_ast": ["reboundsTotal", "assists"],
    "pts_reb_ast": ["points", "reboundsTotal", "assists"],
    "points_assists_rebounds": ["points", "assists", "reboundsTotal"],
}


def load_scores():
    """Build game_id -> {home, away, home_score, away_score} from cache."""
    scores = {}
    for f in glob.glob(f"{CACHE_DIR}/lgf_*.json"):
        with open(f) as fh:
            for g in json.load(fh):
                gid = g.get("game_id")
                if gid:
                    scores[gid] = {
                        "home": g["home_team"], "away": g["away_team"],
                        "home_score": int(g["home_score"]), "away_score": int(g["away_score"]),
                    }
    for f in glob.glob(f"{CACHE_DIR}/nba_cdn_boxscore_*.json"):
        with open(f) as fh:
            data = json.load(fh)
        game = data.get("game", {})
        gid = game.get("gameId")
        ht = game.get("homeTeam", {})
        at = game.get("awayTeam", {})
        if gid and ht.get("score") is not None and at.get("score") is not None:
            scores[gid] = {
                "home": ht.get("teamTricode"), "away": at.get("teamTricode"),
                "home_score": int(ht.get("score", 0)), "away_score": int(at.get("score", 0)),
            }
    return scores


def load_player_stats():
    """Build game_id -> {personId -> {name, stats}} from CDN boxscores."""
    player_stats = {}
    for f in glob.glob(f"{CACHE_DIR}/nba_cdn_boxscore_*.json"):
        with open(f) as fh:
            data = json.load(fh)
        game = data.get("game", {})
        gid = game.get("gameId")
        if not gid:
            continue
        players = {}
        for team_key in ("homeTeam", "awayTeam"):
            team = game.get(team_key, {})
            for p in team.get("players", []):
                pid = p.get("personId")
                if pid:
                    players[pid] = {"name": p.get("name", ""), "stats": p.get("statistics", {})}
        player_stats[gid] = players
    return player_stats


def parse_event_key_teams(event_key):
    """Extract (away_team, home_team) from event_key like NBA:YYYY:MM:DD:AWAY@HOME."""
    parts = event_key.split(":")
    if len(parts) >= 5:
        matchup = parts[4]
        if "@" in matchup:
            return matchup.split("@")
    return None, None


def normalize_selection(selection, market_type):
    """Strip BetQL suffixes: 'LAL_spread' -> 'LAL', 'PHI_ml' -> 'PHI', 'game_total' -> direction."""
    if not selection:
        return selection
    if market_type == "spread" and selection.endswith("_spread"):
        return selection.replace("_spread", "")
    if market_type == "moneyline" and selection.endswith("_ml"):
        return selection.replace("_ml", "")
    return selection


def parse_prop_selection(selection):
    """Parse player prop selection: NBA:player_id::stat_key::DIRECTION."""
    if not selection:
        return {}
    parts = selection.split("::")
    if len(parts) >= 3:
        return {"player": parts[0], "stat_key": parts[1], "direction": parts[2]}
    return {}


def rederive_spread(grade, scores):
    gid = grade.get("provider_game_id")
    if not gid or gid not in scores:
        return None, "no_score_data"

    s = scores[gid]
    selection_raw = grade.get("selection")
    selection = normalize_selection(selection_raw, "spread")
    line = grade.get("line")
    event_key = grade.get("event_key", "")
    orientation = grade.get("games_info", {}).get("match_orientation", "direct")

    if selection is None or line is None:
        return None, f"missing_selection_or_line (sel={selection_raw}, line={line})"

    away_team, home_team = parse_event_key_teams(event_key)
    if not away_team:
        return None, "bad_event_key"

    home_score = s["home_score"]
    away_score = s["away_score"]
    if orientation == "flipped":
        home_score, away_score = away_score, home_score

    if selection == home_team:
        team_raw = home_score
        adjusted = home_score + line
        opponent = away_score
    elif selection == away_team:
        team_raw = away_score
        adjusted = away_score + line
        opponent = home_score
    else:
        return None, f"selection={selection_raw} not in {away_team}/{home_team}"

    if adjusted > opponent:
        return "WIN", f"{selection} {team_raw}+{line}={adjusted} vs {opponent}"
    elif adjusted < opponent:
        return "LOSS", f"{selection} {team_raw}+{line}={adjusted} vs {opponent}"
    else:
        return "PUSH", f"{selection} {team_raw}+{line}={adjusted} vs {opponent}"


def rederive_total(grade, scores):
    gid = grade.get("provider_game_id")
    if not gid or gid not in scores:
        return None, "no_score_data"

    s = scores[gid]
    selection = grade.get("selection", "") or ""
    direction = grade.get("direction", "") or ""
    line = grade.get("line")

    if line is None:
        return None, f"missing_line (sel={selection}, dir={direction})"

    # Determine direction
    dir_val = None
    for candidate in [selection.upper(), direction.upper()]:
        if "OVER" in candidate:
            dir_val = "OVER"
            break
        if "UNDER" in candidate:
            dir_val = "UNDER"
            break

    if not dir_val:
        return None, f"cannot determine over/under: sel={selection}, dir={direction}"

    combined = s["home_score"] + s["away_score"]

    if dir_val == "OVER":
        if combined > line: return "WIN", f"combined={combined} > line={line}"
        elif combined < line: return "LOSS", f"combined={combined} < line={line}"
        else: return "PUSH", f"combined={combined} == line={line}"
    else:
        if combined < line: return "WIN", f"combined={combined} < line={line}"
        elif combined > line: return "LOSS", f"combined={combined} > line={line}"
        else: return "PUSH", f"combined={combined} == line={line}"


def rederive_moneyline(grade, scores):
    gid = grade.get("provider_game_id")
    if not gid or gid not in scores:
        return None, "no_score_data"

    s = scores[gid]
    selection_raw = grade.get("selection")
    selection = normalize_selection(selection_raw, "moneyline")
    event_key = grade.get("event_key", "")
    orientation = grade.get("games_info", {}).get("match_orientation", "direct")

    if not selection:
        return None, "missing_selection"

    away_team, home_team = parse_event_key_teams(event_key)
    if not away_team:
        return None, "bad_event_key"

    home_score = s["home_score"]
    away_score = s["away_score"]
    if orientation == "flipped":
        home_score, away_score = away_score, home_score

    if selection == home_team:
        if home_score > away_score: return "WIN", f"{selection}(home)={home_score} vs {away_score}"
        elif home_score < away_score: return "LOSS", f"{selection}(home)={home_score} vs {away_score}"
        else: return "PUSH", f"{selection}(home)={home_score} vs {away_score}"
    elif selection == away_team:
        if away_score > home_score: return "WIN", f"{selection}(away)={away_score} vs {home_score}"
        elif away_score < home_score: return "LOSS", f"{selection}(away)={away_score} vs {home_score}"
        else: return "PUSH", f"{selection}(away)={away_score} vs {home_score}"
    else:
        return None, f"selection={selection_raw} not in {away_team}/{home_team}"


def rederive_player_prop(grade, player_stats_db):
    stat_value_recorded = grade.get("stat_value")
    selection = grade.get("selection", "")
    line = grade.get("line")
    direction = grade.get("direction")
    gid = grade.get("provider_game_id")

    parsed = parse_prop_selection(selection)
    stat_key = parsed.get("stat_key")
    dir_val = parsed.get("direction") or direction

    if line is None:
        return None, "missing_line", None
    if not dir_val or dir_val.upper() not in ("OVER", "UNDER"):
        return None, f"bad_direction={dir_val}", None

    dir_val = dir_val.upper()

    # Independently look up stat from CDN boxscore
    independent_stat = None
    if gid and gid in player_stats_db and stat_key:
        person_id = grade.get("player_meta", {}).get("matched_personId")
        if person_id and person_id in player_stats_db[gid]:
            p = player_stats_db[gid][person_id]
            cdn_fields = STAT_MAP.get(stat_key, [])
            if cdn_fields:
                total = 0
                all_found = True
                for field in cdn_fields:
                    val = p["stats"].get(field)
                    if val is not None:
                        total += val
                    else:
                        all_found = False
                if all_found:
                    independent_stat = total

    stat_val = stat_value_recorded
    if stat_val is None and independent_stat is not None:
        stat_val = independent_stat
    if stat_val is None:
        return None, "no_stat_value", None

    stat_val = float(stat_val)
    line = float(line)

    stat_mismatch = None
    if independent_stat is not None and stat_value_recorded is not None:
        if abs(float(stat_value_recorded) - float(independent_stat)) > 0.01:
            stat_mismatch = f"recorded={stat_value_recorded} vs cdn={independent_stat}"

    if dir_val == "OVER":
        if stat_val > line: result = "WIN"
        elif stat_val < line: result = "LOSS"
        else: result = "PUSH"
    else:
        if stat_val < line: result = "WIN"
        elif stat_val > line: result = "LOSS"
        else: result = "PUSH"

    cmp = ">" if stat_val > line else "<" if stat_val < line else "=="
    detail = f"stat={stat_val} {cmp} line={line} ({dir_val})"
    if stat_mismatch:
        detail += f" [STAT_MISMATCH: {stat_mismatch}]"

    return result, detail, stat_mismatch


def main():
    print("=" * 80)
    print("NBA GRADE AUDIT")
    print("=" * 80)

    # Load data
    print("\nLoading grades...")
    grades = []
    with open(GRADES_FILE) as f:
        for line in f:
            grades.append(json.loads(line))
    print(f"  Total grade records: {len(grades)}")

    print("Loading signals...")
    signals = {}
    with open(SIGNALS_FILE) as f:
        for line in f:
            s = json.loads(line)
            signals[s["signal_id"]] = s
    print(f"  Total signal records: {len(signals)}")

    print("Loading score cache...")
    score_db = load_scores()
    print(f"  Games with scores: {len(score_db)}")

    print("Loading player stats from CDN boxscores...")
    player_stats_db = load_player_stats()
    print(f"  Games with player stats: {len(player_stats_db)}")

    # Overall counts
    result_counts = Counter()
    market_result_counts = Counter()
    for g in grades:
        r = g.get("result")
        mt = g.get("market_type", "unknown")
        result_counts[r] += 1
        market_result_counts[(mt, r)] += 1

    print("\n" + "=" * 80)
    print("OVERALL GRADE DISTRIBUTION")
    print("=" * 80)
    for r, c in sorted(result_counts.items(), key=lambda x: -x[1]):
        print(f"  {str(r):12s}: {c}")

    print("\nBy market type:")
    for mt in sorted(set(mt for mt, _ in market_result_counts), key=lambda x: str(x)):
        counts_for_mt = {r: c for (m, r), c in market_result_counts.items() if m == mt}
        print(f"  {str(mt):15s}: {dict(counts_for_mt)}")

    # Signals without grades
    graded_signal_ids = set(g["signal_id"] for g in grades)
    ungraded = [sid for sid in signals if sid not in graded_signal_ids]
    grade_ids_not_in_signals = [g["signal_id"] for g in grades if g["signal_id"] not in signals]

    print("\n" + "=" * 80)
    print("SIGNAL <-> GRADE COVERAGE")
    print("=" * 80)
    print(f"  Total signals: {len(signals)}")
    print(f"  Signals with a grade record: {len(graded_signal_ids & set(signals.keys()))}")
    print(f"  Signals with NO grade record: {len(ungraded)}")
    print(f"  Grade records with NO matching signal: {len(grade_ids_not_in_signals)}")

    if ungraded:
        ungraded_by_market = Counter()
        for sid in ungraded:
            s = signals[sid]
            ungraded_by_market[s.get("market_type", "unknown")] += 1
        print(f"\n  Ungraded by market type:")
        for mt, c in sorted(ungraded_by_market.items(), key=lambda x: -x[1]):
            print(f"    {mt}: {c}")
        print(f"\n  Sample ungraded signals:")
        for sid in ungraded[:8]:
            s = signals[sid]
            print(f"    {sid[:16]}... market={s.get('market_type')} event={s.get('event_key')}")

    # Sampled detailed audit
    print("\n" + "=" * 80)
    print("DETAILED AUDIT: SAMPLED GRADES")
    print("=" * 80)

    random.seed(42)
    buckets = defaultdict(list)
    for g in grades:
        r = g.get("result")
        mt = g.get("market_type")
        if r in ("WIN", "LOSS") and mt in ("spread", "total", "moneyline", "player_prop"):
            buckets[(mt, r)].append(g)

    sampled = []
    for (mt, r), bucket in sorted(buckets.items()):
        n = min(4, len(bucket))
        sampled.extend(random.sample(bucket, n))

    print(f"\nAuditing {len(sampled)} sampled grades...\n")

    for i, g in enumerate(sampled):
        mt = g.get("market_type")
        recorded_result = g.get("result")
        signal_id = g.get("signal_id", "?")
        event_key = g.get("event_key", "?")
        gid = g.get("provider_game_id", "?")
        selection = g.get("selection", "?")
        line = g.get("line")
        direction = g.get("direction")

        print(f"--- Sample {i+1}: {mt} / {recorded_result} ---")
        print(f"  signal_id:  {signal_id}")
        print(f"  event_key:  {event_key}")
        print(f"  game_id:    {gid}")
        print(f"  selection:  {selection}")
        print(f"  line:       {line}")
        print(f"  direction:  {direction}")

        if gid and gid in score_db:
            s = score_db[gid]
            print(f"  score:      {s['away']} {s['away_score']} @ {s['home']} {s['home_score']}")
        else:
            print(f"  score:      NOT FOUND in cache")

        stat_mm = None
        if mt == "spread":
            derived_result, detail = rederive_spread(g, score_db)
        elif mt == "total":
            derived_result, detail = rederive_total(g, score_db)
        elif mt == "moneyline":
            derived_result, detail = rederive_moneyline(g, score_db)
        elif mt == "player_prop":
            derived_result, detail, stat_mm = rederive_player_prop(g, player_stats_db)
            print(f"  player:     {g.get('player_meta', {}).get('matched_name_raw', '?')}")
            parsed = parse_prop_selection(selection)
            print(f"  stat_key:   {parsed.get('stat_key', '?')}")
            print(f"  stat_value: {g.get('stat_value')}")
        else:
            derived_result, detail = None, "unknown_market"

        print(f"  derivation: {detail}")
        print(f"  recorded:   {recorded_result}")
        print(f"  re-derived: {derived_result}")

        if derived_result is None:
            print(f"  ** COULD NOT VERIFY")
        elif derived_result != recorded_result:
            print(f"  ** MISMATCH! recorded={recorded_result}, derived={derived_result}")
        else:
            print(f"  >> OK")
        print()

    # Full audit
    print("=" * 80)
    print("FULL AUDIT: ALL GRADED RECORDS")
    print("=" * 80)

    total_checked = 0
    total_ok = 0
    total_mismatch = 0
    total_unverifiable = 0
    all_mismatches = []
    mismatch_by_market = Counter()
    unverifiable_by_reason = Counter()

    for g in grades:
        mt = g.get("market_type")
        recorded = g.get("result")
        if recorded not in ("WIN", "LOSS", "PUSH"):
            continue
        if mt not in ("spread", "total", "moneyline", "player_prop"):
            continue

        total_checked += 1
        stat_mm = None

        if mt == "spread":
            derived, detail = rederive_spread(g, score_db)
        elif mt == "total":
            derived, detail = rederive_total(g, score_db)
        elif mt == "moneyline":
            derived, detail = rederive_moneyline(g, score_db)
        elif mt == "player_prop":
            derived, detail, stat_mm = rederive_player_prop(g, player_stats_db)
        else:
            continue

        if derived is None:
            total_unverifiable += 1
            # Bucket the reason
            reason = detail.split("(")[0].strip() if "(" in detail else detail
            unverifiable_by_reason[reason] += 1
        elif derived != recorded:
            total_mismatch += 1
            mismatch_by_market[mt] += 1
            all_mismatches.append({
                "signal_id": g.get("signal_id"),
                "market_type": mt,
                "event_key": g.get("event_key"),
                "selection": g.get("selection"),
                "line": g.get("line"),
                "recorded": recorded,
                "derived": derived,
                "detail": detail,
                "stat_value": g.get("stat_value"),
                "provider_game_id": g.get("provider_game_id"),
                "player": g.get("player_meta", {}).get("matched_name_raw") if mt == "player_prop" else None,
            })
        else:
            total_ok += 1

    print(f"\n  Total graded records checked:  {total_checked}")
    print(f"  Verified OK:                   {total_ok}")
    print(f"  MISMATCHES:                    {total_mismatch}")
    print(f"  Unverifiable (missing data):   {total_unverifiable}")

    if total_unverifiable > 0:
        print(f"\n  Unverifiable breakdown:")
        for reason, c in sorted(unverifiable_by_reason.items(), key=lambda x: -x[1]):
            print(f"    {reason}: {c}")

    if total_mismatch > 0:
        print(f"\n  Mismatches by market type:")
        for mt, c in sorted(mismatch_by_market.items(), key=lambda x: -x[1]):
            print(f"    {mt}: {c}")
        print(f"\n  All {len(all_mismatches)} mismatches:")
        for j, m in enumerate(all_mismatches[:50]):
            print(f"    [{j+1}] [{m['market_type']}] signal={m['signal_id'][:16]}... "
                  f"event={m['event_key']} sel={str(m['selection'])[:40]} line={m['line']} "
                  f"recorded={m['recorded']} derived={m['derived']}")
            print(f"        detail: {m['detail']}")
            if m.get('player'):
                print(f"        player: {m['player']}, stat_value: {m.get('stat_value')}")

    # Player prop stat value cross-check
    print("\n" + "=" * 80)
    print("PLAYER PROP STAT VALUE CROSS-CHECK")
    print("=" * 80)

    stat_check_count = 0
    stat_ok = 0
    stat_mismatch_list = []

    for g in grades:
        if g.get("market_type") != "player_prop" or g.get("result") not in ("WIN", "LOSS", "PUSH"):
            continue
        stat_value_recorded = g.get("stat_value")
        if stat_value_recorded is None:
            continue
        gid = g.get("provider_game_id")
        parsed = parse_prop_selection(g.get("selection", ""))
        stat_key = parsed.get("stat_key")
        person_id = g.get("player_meta", {}).get("matched_personId")
        if not gid or gid not in player_stats_db or not person_id or not stat_key:
            continue
        if person_id not in player_stats_db[gid]:
            continue
        cdn_fields = STAT_MAP.get(stat_key, [])
        if not cdn_fields:
            continue
        p = player_stats_db[gid][person_id]
        total = 0
        all_found = True
        for field in cdn_fields:
            val = p["stats"].get(field)
            if val is not None:
                total += val
            else:
                all_found = False
        if not all_found:
            continue

        stat_check_count += 1
        if abs(float(stat_value_recorded) - float(total)) < 0.01:
            stat_ok += 1
        else:
            stat_mismatch_list.append({
                "signal_id": g["signal_id"],
                "player": g.get("player_meta", {}).get("matched_name_raw", "?"),
                "stat_key": stat_key,
                "recorded": stat_value_recorded,
                "cdn": total,
                "line": g.get("line"),
                "direction": parsed.get("direction"),
                "result": g.get("result"),
                "event": g.get("event_key"),
                "game_id": gid,
            })

    print(f"\n  Player props with verifiable stat values: {stat_check_count}")
    print(f"  Stat values matching CDN boxscore:        {stat_ok}")
    print(f"  Stat value MISMATCHES:                    {len(stat_mismatch_list)}")

    # Analyze stat mismatches
    if stat_mismatch_list:
        # Check if these cause grade errors
        grade_errors_from_stat = 0
        for m in stat_mismatch_list:
            cdn = float(m["cdn"])
            line = float(m["line"]) if m["line"] is not None else None
            dir_val = m.get("direction", "").upper()
            if line is None or dir_val not in ("OVER", "UNDER"):
                continue
            if dir_val == "OVER":
                correct = "WIN" if cdn > line else ("LOSS" if cdn < line else "PUSH")
            else:
                correct = "WIN" if cdn < line else ("LOSS" if cdn > line else "PUSH")
            if correct != m["result"]:
                grade_errors_from_stat += 1

        print(f"  Of those, grades that would CHANGE with CDN stat: {grade_errors_from_stat}")

        # Show by stat_key
        by_stat = Counter()
        for m in stat_mismatch_list:
            by_stat[m["stat_key"]] += 1
        print(f"\n  Stat mismatches by stat_key:")
        for sk, c in by_stat.most_common():
            print(f"    {sk}: {c}")

        print(f"\n  First 20 stat mismatches:")
        for m in stat_mismatch_list[:20]:
            print(f"    player={m['player']}, stat={m['stat_key']}, "
                  f"recorded_stat={m['recorded']}, cdn_stat={m['cdn']}, "
                  f"line={m['line']}, dir={m.get('direction')}, result={m['result']}")
            # Show what grade would be with CDN value
            cdn = float(m["cdn"])
            line = float(m["line"]) if m["line"] is not None else None
            d = m.get("direction", "").upper()
            if line and d in ("OVER", "UNDER"):
                if d == "OVER":
                    correct = "WIN" if cdn > line else ("LOSS" if cdn < line else "PUSH")
                else:
                    correct = "WIN" if cdn < line else ("LOSS" if cdn > line else "PUSH")
                flag = " *** GRADE ERROR" if correct != m["result"] else ""
                print(f"      -> CDN would grade: {correct} (recorded: {m['result']}){flag}")

    # Summary
    print("\n" + "=" * 80)
    print("AUDIT SUMMARY")
    print("=" * 80)
    print(f"  Total grade records:             {len(grades)}")
    print(f"  Total signal records:            {len(signals)}")
    print(f"  Signals without any grade:       {len(ungraded)}")
    print(f"  Grades without matching signal:  {len(grade_ids_not_in_signals)}")
    print()
    print(f"  Graded (WIN/LOSS/PUSH) checked:  {total_checked}")
    print(f"  Verified correct:                {total_ok}")
    print(f"  Grade result MISMATCHES:         {total_mismatch}")
    print(f"  Unverifiable (missing data):     {total_unverifiable}")
    print()
    print(f"  Player prop stat cross-checked:  {stat_check_count}")
    print(f"  Stat values correct:             {stat_ok}")
    print(f"  Stat value mismatches:           {len(stat_mismatch_list)}")

    verifiable = total_ok + total_mismatch
    accuracy = total_ok / verifiable * 100 if verifiable > 0 else 0
    print(f"\n  Grade accuracy (where verifiable): {accuracy:.2f}% ({total_ok}/{verifiable})")

    if total_mismatch == 0:
        print("\n  VERDICT: All verifiable grades are CORRECT -- no result errors found.")
    else:
        print(f"\n  VERDICT: Found {total_mismatch} grade result errors!")

    if stat_mismatch_list:
        grade_errors = sum(1 for m in stat_mismatch_list
                          if m.get("line") is not None and m.get("direction","").upper() in ("OVER","UNDER")
                          and (("WIN" if float(m["cdn"]) > float(m["line"]) else "LOSS" if float(m["cdn"]) < float(m["line"]) else "PUSH")
                               if m.get("direction","").upper() == "OVER"
                               else ("WIN" if float(m["cdn"]) < float(m["line"]) else "LOSS" if float(m["cdn"]) > float(m["line"]) else "PUSH"))
                          != m["result"])
        if grade_errors > 0:
            print(f"\n  WARNING: {grade_errors} grades would change result if CDN boxscore stat were used instead")
            print(f"           of the recorded stat_value. This suggests potential stat lookup issues.")
        else:
            print(f"\n  NOTE: {len(stat_mismatch_list)} stat values differ from CDN, but all grades remain")
            print(f"        correct regardless (the difference doesn't cross the line).")


if __name__ == "__main__":
    main()
