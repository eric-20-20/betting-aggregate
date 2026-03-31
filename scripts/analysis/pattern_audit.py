#!/usr/bin/env python3
"""
Pattern Registry Audit — queries Supabase clean_graded_picks and related tables
to verify every pattern in PATTERN_REGISTRY against live data.

Usage:
    python3 scripts/pattern_audit.py
"""
from __future__ import annotations

import sys
import math
from collections import defaultdict, Counter
from typing import Any, Dict, List, Optional, Set, Tuple

sys.path.insert(0, '/Users/Ericevans/Betting Aggregate')
from src.supabase_writer import get_client

# ── Wilson lower bound ────────────────────────────────────────────────────────

def wilson_lower(wins: int, n: int, z: float = 1.645) -> float:
    if n == 0:
        return 0.0
    p = wins / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    spread = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (centre - spread) / denom


# ── Supabase fetch helpers ────────────────────────────────────────────────────

def fetch_all(client, table: str, select: str = "*") -> List[Dict]:
    rows: List[Dict] = []
    offset = 0
    page = 1000
    while True:
        resp = client.table(table).select(select).range(offset, offset + page - 1).execute()
        batch = resp.data or []
        rows.extend(batch)
        if len(batch) < page:
            break
        offset += page
    return rows


# ── Pattern definitions ───────────────────────────────────────────────────────

PATTERNS = [
    # (id, label, filter_type, filter_value, market_type, direction, stat_type, team, tier, hist_record, hist_n)
    ("nukethebooks_solo_props_under",   "nukethebooks player prop UNDER",            "exact_combo",    "juicereel_nukethebooks",                                            "player_prop", "UNDER", None,       None,  "A", "188-122", 310),
    ("nukethebooks_solo_props_over",    "nukethebooks player prop OVER",             "exact_combo",    "juicereel_nukethebooks",                                            "player_prop", "OVER",  None,       None,  "B", "235-187", 422),
    ("action_props_pts_reb_under",      "action pts+reb prop UNDER",                 "exact_combo",    "action",                                                            "player_prop", "UNDER", "pts_reb",  None,  "A", "144-52",  196),
    ("action_props_assists_under",      "action assists prop UNDER",                 "exact_combo",    "action",                                                            "player_prop", "UNDER", "assists",  None,  "B", "46-28",   74),
    ("action_props_pts_ast_under",      "action pts+ast prop UNDER",                 "exact_combo",    "action",                                                            "player_prop", "UNDER", "pts_ast",  None,  "B", "30-17",   47),
    ("action_solo_props_under",         "action player prop UNDER",                  "exact_combo",    "action",                                                            "player_prop", "UNDER", None,       None,  "A", "399-257", 656),
    ("sxebets_solo_props_under",        "juicereel_sxebets player prop UNDER",       "exact_combo",    "juicereel_sxebets",                                                 "player_prop", "UNDER", None,       None,  "B", "571-476", 1068),
    ("betql_props_rebounds_under",      "betql rebounds prop UNDER",                 "exact_combo",    "betql",                                                             "player_prop", "UNDER", "rebounds", None,  "B", "200-135", 335),
    ("betql_props_assists_under",       "betql assists prop UNDER",                  "exact_combo",    "betql",                                                             "player_prop", "UNDER", "assists",  None,  "B", "94-78",   172),
    ("betql_solo_props_under",          "betql player prop UNDER",                   "exact_combo",    "betql",                                                             "player_prop", "UNDER", None,       None,  "B", "2832-2336",5172),
    ("action_nukethebooks_moneyline",   "action + nukethebooks moneyline",           "source_pair",    ["action", "juicereel_nukethebooks"],                                "moneyline",   None,    None,       None,  "A", "33-12",   45),
    ("action_betql_nukethebooks_spread","action + betql + nukethebooks spread",      "source_contains",["action", "betql", "juicereel_nukethebooks"],                       "spread",      None,    None,       None,  "A", "29-11",   40),
    ("all_jr_total",                    "all 3 JuiceReel experts total",             "source_contains",["juicereel_nukethebooks","juicereel_sxebets","juicereel_unitvacuum"],"total",       None,    None,       None,  "A", "52-28",   80),
    ("nukethebooks_sxebets_total",      "nukethebooks + sxebets total",              "source_contains",["juicereel_nukethebooks","juicereel_sxebets"],                      "total",       None,    None,       None,  "A", "112-64",  176),
    ("action_betql_sxebets_total",      "action + betql + sxebets total",            "source_contains",["action","betql","juicereel_sxebets"],                              "total",       None,    None,       None,  "A", "60-34",   94),
    ("sxebets_unitvacuum_spread",       "sxebets + unitvacuum spread",               "source_pair",    ["juicereel_sxebets","juicereel_unitvacuum"],                        "spread",      None,    None,       None,  "A", "40-22",   62),
    ("nukethebooks_solo_spread",        "nukethebooks spread",                       "exact_combo",    "juicereel_nukethebooks",                                            "spread",      None,    None,       None,  "B", "186-148", 334),
    ("action_betql_unitvacuum_spread",  "action + betql + unitvacuum spread",        "source_contains",["action","betql","juicereel_unitvacuum"],                           "spread",      None,    None,       None,  "B", "35-22",   57),
    ("betql_nukethebooks_total",        "betql + nukethebooks total",                "source_pair",    ["betql","juicereel_nukethebooks"],                                  "total",       None,    None,       None,  "B", "45-34",   79),
    ("unitvacuum_solo_total",           "unitvacuum total",                          "exact_combo",    "juicereel_unitvacuum",                                              "total",       None,    None,       None,  "B", "80-60",   140),
    ("unitvacuum_solo_moneyline",       "unitvacuum moneyline",                      "exact_combo",    "juicereel_unitvacuum",                                              "moneyline",   None,    None,       None,  "B", "56-42",   98),
    ("betql_sxebets_props_under",       "betql + sxebets player prop UNDER",         "source_pair",    ["betql","juicereel_sxebets"],                                       "player_prop", "UNDER", None,       None,  "A", "31-18",   49),
    ("betql_sxebets_props_over",        "betql + sxebets player prop OVER",          "source_pair",    ["betql","juicereel_sxebets"],                                       "player_prop", "OVER",  None,       None,  "B", "40-27",   67),
    ("action_solo_spread",              "action spread",                             "exact_combo",    "action",                                                            "spread",      None,    None,       None,  "B", "258-205", 465),
    # BetQL team spreads
    ("betql_spread_okc",  "betql spread OKC", "exact_combo", "betql", "spread", None, None, "OKC", "A", "81-28",  109),
    ("betql_spread_bos",  "betql spread BOS", "exact_combo", "betql", "spread", None, None, "BOS", "A", "57-20",  77),
    ("betql_spread_min",  "betql spread MIN", "exact_combo", "betql", "spread", None, None, "MIN", "A", "48-17",  67),
    ("betql_spread_lac",  "betql spread LAC", "exact_combo", "betql", "spread", None, None, "LAC", "A", "59-23",  82),
    ("betql_spread_det",  "betql spread DET", "exact_combo", "betql", "spread", None, None, "DET", "A", "51-26",  77),
    ("betql_spread_phx",  "betql spread PHX", "exact_combo", "betql", "spread", None, None, "PHX", "A", "37-18",  57),
    ("betql_spread_hou",  "betql spread HOU", "exact_combo", "betql", "spread", None, None, "HOU", "A", "56-35",  91),
    ("betql_spread_gsw",  "betql spread GSW", "exact_combo", "betql", "spread", None, None, "GSW", "B", "58-35",  95),
    ("betql_spread_nyk",  "betql spread NYK", "exact_combo", "betql", "spread", None, None, "NYK", "B", "49-39",  91),
    ("betql_spread_chi",  "betql spread CHI", "exact_combo", "betql", "spread", None, None, "CHI", "B", "11-2",   13),
    # OddsTrader team moneylines
    ("oddstrader_ml_nyk", "oddstrader moneyline NYK", "exact_combo", "oddstrader", "moneyline", None, None, "NYK", "B", "22-5",  27),
    ("oddstrader_ml_sas", "oddstrader moneyline SAS", "exact_combo", "oddstrader", "moneyline", None, None, "SAS", "B", "21-5",  26),
    ("oddstrader_ml_hou", "oddstrader moneyline HOU", "exact_combo", "oddstrader", "moneyline", None, None, "HOU", "B", "15-4",  19),
    # BettingPros
    ("action_bettingpros_total_under",  "action + bettingpros experts total UNDER",  "exact_combo", "action|bettingpros_experts", "total",  "UNDER", None, None, "B", "76-49",  125),
    ("action_bettingpros_spread",       "action + bettingpros experts spread",       "exact_combo", "action|bettingpros_experts", "spread", None,    None, None, "B", "87-73",  160),
]

NEW_PATTERN_CANDIDATES = [
    ("NEW_action_oddstrader_spread",         "action + oddstrader spread",            "source_pair", ["action","oddstrader"],          "spread",      None,  None, None, "?", "34-13",  47),
    ("NEW_action_oddstrader_moneyline",      "action + oddstrader moneyline",         "source_pair", ["action","oddstrader"],          "moneyline",   None,  None, None, "?", "27-12",  39),
    ("NEW_bettingpros_experts_moneyline",    "bettingpros_experts moneyline (solo)",  "exact_combo", "bettingpros_experts",            "moneyline",   None,  None, None, "?", "88-47",  135),
    ("NEW_bettingpros_experts_props",        "bettingpros_experts player_prop (solo)","exact_combo", "bettingpros_experts",            "player_prop", None,  None, None, "?", "374-236",610),
    ("NEW_bettingpros_experts_total",        "bettingpros_experts total (solo)",      "exact_combo", "bettingpros_experts",            "total",       None,  None, None, "?", "53-37",  90),
    ("NEW_bettingpros_experts_spread",       "bettingpros_experts spread (solo)",     "exact_combo", "bettingpros_experts",            "spread",      None,  None, None, "?", "101-73", 174),
    ("NEW_action_betql_total",               "action + betql total",                  "source_pair", ["action","betql"],               "total",       None,  None, None, "?", "165-107",272),
    ("NEW_sportsline_moneyline",             "sportsline moneyline (solo)",           "exact_combo", "sportsline",                    "moneyline",   None,  None, None, "?", "25-17",  42),
]

ALL_PATTERNS = PATTERNS + NEW_PATTERN_CANDIDATES


# ── Row matching helpers ──────────────────────────────────────────────────────

def sources_in_combo(required: List[str], combo: Optional[str]) -> bool:
    if not combo:
        return False
    parts = set(combo.split("|"))
    return all(s in parts for s in required)


def row_matches_pattern(row: Dict, pat: Tuple, team_lookup: Dict[str, Tuple[str, str]]) -> bool:
    pid, label, filter_type, filter_value, market_type, direction, stat_type, team, tier, hist_record, hist_n = pat

    # market_type filter
    if row.get("market_type") != market_type:
        return False

    # direction filter
    if direction is not None:
        row_dir = (row.get("direction") or "").upper()
        if row_dir != direction.upper():
            return False

    # stat_type filter (atomic_stat)
    if stat_type is not None:
        if row.get("atomic_stat") != stat_type:
            return False

    # sources_combo filter
    combo = row.get("sources_combo") or ""
    if filter_type == "exact_combo":
        if combo != filter_value:
            return False
    elif filter_type in ("source_pair", "source_contains"):
        if not sources_in_combo(filter_value, combo):
            return False

    # team filter — look up away_team/home_team from signals table
    if team is not None:
        sig_id = row.get("signal_id")
        teams = team_lookup.get(sig_id)
        if not teams:
            return False
        away_team, home_team = teams
        if team not in (away_team, home_team):
            return False

    return True


# ── Odds distribution bucketing ───────────────────────────────────────────────

def odds_bucket(odds: Optional[float]) -> str:
    if odds is None:
        return "N/A"
    if odds <= -400:
        return "<=-400"
    elif odds <= -300:
        return "-400 to -301"
    elif odds <= -200:
        return "-300 to -201"
    elif odds <= -150:
        return "-200 to -151"
    elif odds <= -110:
        return "-150 to -111"
    elif odds < 0:
        return "-110 to -1"
    elif odds == 0:
        return "EVEN"
    else:
        return f"+{int(odds)}" if odds > 0 else str(odds)


def fmt_odds_dist(dist: Counter) -> str:
    if not dist:
        return "{}"
    buckets_order = ["<=-400", "-400 to -301", "-300 to -201", "-200 to -151",
                     "-150 to -111", "-110 to -1", "EVEN", "N/A"]
    parts = []
    for b in buckets_order:
        if b in dist:
            parts.append(f"{b}: {dist[b]}")
    # anything else (positive odds)
    for b in sorted(dist.keys()):
        if b not in buckets_order:
            parts.append(f"{b}: {dist[b]}")
    return "{" + ", ".join(parts) + "}"


# ── Summary collector ─────────────────────────────────────────────────────────

SUMMARY_ROWS: List[Dict] = []


# ── Main audit ────────────────────────────────────────────────────────────────

def main():
    print("Connecting to Supabase...")
    client = get_client()

    # 1. Fetch clean_graded_picks
    print("Fetching clean_graded_picks...")
    cgp = fetch_all(client, "clean_graded_picks",
                    "signal_id,day_key,event_key,market_type,selection,direction,"
                    "atomic_stat,player_key,stat_key,sources_combo,sources_count,"
                    "signal_type,score,result,graded_at_utc,units,direction_known,data_source")
    print(f"  -> {len(cgp):,} rows in clean_graded_picks")

    # 2. Fetch grades (signal_id, result, odds)
    print("Fetching grades table...")
    grades_raw = fetch_all(client, "grades", "signal_id,result,odds")
    grades_odds: Dict[str, Optional[float]] = {r["signal_id"]: r.get("odds") for r in grades_raw}
    print(f"  -> {len(grades_raw):,} rows in grades")

    # 3. Fetch juicereel_history_archive (signal_id, result, odds, away_team, home_team, direction)
    print("Fetching juicereel_history_archive...")
    jh_raw = fetch_all(client, "juicereel_history_archive",
                       "signal_id,result,odds,away_team,home_team,direction")
    jh_odds: Dict[str, Optional[float]] = {r["signal_id"]: r.get("odds") for r in jh_raw}
    jh_teams: Dict[str, Tuple[str, str]] = {
        r["signal_id"]: (r.get("away_team") or "", r.get("home_team") or "")
        for r in jh_raw
    }
    print(f"  -> {len(jh_raw):,} rows in juicereel_history_archive")

    # 4. Fetch signals (signal_id, away_team, home_team, direction)
    print("Fetching signals table (away_team, home_team)...")
    sigs_raw = fetch_all(client, "signals", "signal_id,away_team,home_team,direction")
    sig_teams: Dict[str, Tuple[str, str]] = {
        r["signal_id"]: (r.get("away_team") or "", r.get("home_team") or "")
        for r in sigs_raw
    }
    print(f"  -> {len(sigs_raw):,} rows in signals")

    # Merge team lookups (signals takes priority; fall back to jh_teams)
    team_lookup: Dict[str, Tuple[str, str]] = {}
    team_lookup.update(jh_teams)
    team_lookup.update(sig_teams)

    # Merge odds lookups (grades takes priority; fall back to jh_odds)
    odds_lookup: Dict[str, Optional[float]] = {}
    odds_lookup.update(jh_odds)
    odds_lookup.update(grades_odds)

    # Index cgp by signal_id for dedup checks
    print("Building indices...")
    # For each row, pre-compute direction as uppercase
    for row in cgp:
        if row.get("direction"):
            row["_dir_upper"] = row["direction"].upper()
        else:
            row["_dir_upper"] = None

    print(f"\nTotal rows in clean_graded_picks: {len(cgp):,}")
    print(f"Total unique signal_ids: {len({r['signal_id'] for r in cgp}):,}")
    print()

    # ── Audit each pattern ────────────────────────────────────────────────────

    for pat in ALL_PATTERNS:
        pid, label, filter_type, filter_value, market_type, direction, stat_type, team, tier, hist_record, hist_n = pat

        is_new = pid.startswith("NEW_")
        section = "NEW CANDIDATE" if is_new else "PATTERN"

        print(f"## {section}: {pid} [{tier}] — {label}")

        # Filter rows
        matched = [r for r in cgp if row_matches_pattern(r, pat, team_lookup)]

        raw_count = len(matched)
        signal_ids_seen = [r["signal_id"] for r in matched]
        dedup_signal_ids = list(dict.fromkeys(signal_ids_seen))  # preserve order, deduplicate
        dedup_count = len(dedup_signal_ids)

        # Build deduplicated result map (first occurrence wins)
        seen: Set[str] = set()
        deduped: List[Dict] = []
        for r in matched:
            sid = r["signal_id"]
            if sid not in seen:
                seen.add(sid)
                deduped.append(r)

        # Win/Loss on raw matched (before dedup)
        def wl_stats(rows: List[Dict]):
            wins = sum(1 for r in rows if (r.get("result") or "").upper() == "WIN")
            losses = sum(1 for r in rows if (r.get("result") or "").upper() == "LOSS")
            n = wins + losses
            pct = wins / n * 100 if n > 0 else 0.0
            return wins, losses, n, pct

        raw_w, raw_l, raw_n, raw_pct = wl_stats(matched)
        dedup_w, dedup_l, dedup_n, dedup_pct = wl_stats(deduped)

        # Display new record using deduplicated data
        new_record_str = f"{dedup_w}-{dedup_l}"
        new_pct_str = f"{dedup_pct:.1f}%"

        print(f"Hist record: {hist_record} (n={hist_n}) | New record from clean_graded_picks: {new_record_str} ({new_pct_str}) n={dedup_n}")
        print()
        print("Integrity Checks:")

        # Check 1: Duplicates
        dupe_flag = raw_count != dedup_count
        dupe_status = "FLAG: DUPE_SIGNAL_IDS" if dupe_flag else "PASS"
        print(f"- Duplicates: raw={raw_count} dedup={dedup_count} [{dupe_status}]")
        if dupe_flag:
            print(f"  Win rate on deduped: {dedup_w}-{dedup_l} ({dedup_pct:.1f}%) vs raw {raw_w}-{raw_l} ({raw_pct:.1f}%)")

        # Check 2: Direction check (for patterns with explicit direction)
        direction_flag = False
        dir_flag_str = "N/A (no direction filter)"
        if direction is not None:
            dir_unknown_rows = [r for r in deduped if not r.get("direction_known")]
            dir_unknown_count = len(dir_unknown_rows)
            dir_known_rows = [r for r in deduped if r.get("direction_known")]
            known_w, known_l, known_n, known_pct = wl_stats(dir_known_rows)
            if dir_unknown_count > 0:
                direction_flag = abs(known_pct - dedup_pct) >= 2.0
                flag_word = "FLAG" if direction_flag else "WARN"
                dir_flag_str = (f"{flag_word}: {dir_unknown_count}/{dedup_n} direction_unknown rows; "
                                f"win rate with known={known_w}-{known_l} ({known_pct:.1f}%) vs all={dedup_pct:.1f}%")
            else:
                dir_flag_str = f"PASS (0 direction_unknown rows)"
        print(f"- Direction unknown rows: [{dir_flag_str}]")

        # Check 3: Grade uniqueness (signal_id appearing multiple times in deduped — shouldn't happen after dedup)
        sid_counts = Counter(signal_ids_seen)
        multi_grade = {sid: cnt for sid, cnt in sid_counts.items() if cnt > 1}
        if multi_grade:
            grade_uniq_str = f"FLAG: {len(multi_grade)} signal_ids with multiple grades"
        else:
            grade_uniq_str = "PASS"
        print(f"- Grade uniqueness: [{grade_uniq_str}]")

        # Check 4: Monthly concentration
        month_counts: Counter = Counter()
        for r in deduped:
            dk = r.get("day_key") or ""
            if len(dk) >= 7:
                month_counts[dk[:7]] += 1

        month_flag = False
        month_flag_str = "PASS"
        if dedup_n > 0:
            max_month = month_counts.most_common(1)[0] if month_counts else ("?", 0)
            max_pct = max_month[1] / dedup_n * 100
            if max_pct > 40:
                month_flag = True
                month_flag_str = f"FLAG: {max_month[0]} = {max_pct:.1f}%"

        per_month_str = ", ".join(f"{m}: {c}" for m, c in sorted(month_counts.items()))
        print(f"- Monthly concentration: [{month_flag_str}]")
        print(f"  Per-month breakdown: {{{per_month_str}}}")

        # Heavy favorites check (odds lookup)
        sig_ids_set = {r["signal_id"] for r in deduped}
        odds_vals = [odds_lookup.get(sid) for sid in sig_ids_set]
        odds_not_null = [o for o in odds_vals if o is not None]
        n_no_odds = len(odds_vals) - len(odds_not_null)
        pct_no_odds = n_no_odds / len(odds_vals) * 100 if odds_vals else 0

        heavy_fav_count = sum(1 for o in odds_not_null if o is not None and o < -200)
        heavy_fav_pct = heavy_fav_count / len(odds_not_null) * 100 if odds_not_null else 0
        heavy_fav_flag = heavy_fav_pct > 50

        # Odds distribution
        dist: Counter = Counter()
        for o in odds_vals:
            dist[odds_bucket(o)] += 1

        heavy_fav_str = f"{heavy_fav_pct:.1f}% of picks at odds < -200"
        if len(odds_not_null) == 0:
            heavy_fav_str = f"N/A (no odds data: {pct_no_odds:.1f}%)"
        elif n_no_odds > 0:
            heavy_fav_str += f" (N/A: {n_no_odds} of {dedup_n} no odds data)"

        print(f"\nHeavy favorites: {heavy_fav_str}")
        print(f"Odds distribution: {fmt_odds_dist(dist)}")

        # Corrected win rate if any flags
        any_flag = dupe_flag or direction_flag or month_flag
        if any_flag:
            # Use deduped + direction_known only (most conservative)
            if direction is not None and direction_flag:
                dir_known_rows_dedup = [r for r in deduped if r.get("direction_known")]
                corr_w, corr_l, corr_n, corr_pct = wl_stats(dir_known_rows_dedup)
                print(f"\n[CORRECTED win rate (direction_known only)]: {corr_w}-{corr_l} ({corr_pct:.1f}%) n={corr_n} vs ORIGINAL {dedup_pct:.1f}%")
            elif dupe_flag:
                print(f"\n[CORRECTED win rate (deduped)]: {dedup_w}-{dedup_l} ({dedup_pct:.1f}%) n={dedup_n} vs raw {raw_pct:.1f}%")

        # Overall status
        status = "PASS"
        if heavy_fav_flag:
            status = "WARN"
        if dupe_flag or direction_flag or month_flag:
            status = "FLAG"
        if dupe_flag and direction_flag:
            status = "FLAG"

        print()

        # Collect summary row
        SUMMARY_ROWS.append({
            "id": pid,
            "tier": tier,
            "label": label[:50],
            "hist_record": hist_record,
            "new_W": dedup_w,
            "new_L": dedup_l,
            "new_N": dedup_n,
            "new_win_pct": f"{dedup_pct:.1f}%",
            "wilson": f"{wilson_lower(dedup_w, dedup_n):.3f}" if dedup_n >= 5 else "n/a",
            "dupe_flag": "Y" if dupe_flag else "-",
            "direction_flag": "Y" if direction_flag else "-",
            "month_flag": "Y" if month_flag else "-",
            "heavy_fav_pct": f"{heavy_fav_pct:.0f}%" if odds_not_null else "N/A",
            "status": status,
        })

        print("-" * 80)

    # ── Summary table ─────────────────────────────────────────────────────────
    print()
    print("=" * 120)
    print("## SUMMARY TABLE")
    print("=" * 120)

    # Column widths
    col_id   = max(len(r["id"]) for r in SUMMARY_ROWS) + 2
    col_tier = 6
    col_hist = 12
    col_new  = 10
    col_n    = 8
    col_pct  = 12
    col_wil  = 8
    col_flag = 6
    col_hfav = 12
    col_stat = 8

    header = (
        f"{'id':<{col_id}} {'tier':<{col_tier}} {'hist_record':<{col_hist}} "
        f"{'new_W-L':<{col_new}} {'new_N':<{col_n}} {'new_pct':<{col_pct}} "
        f"{'wilson':<{col_wil}} {'dupe':<{col_flag}} {'dir_flag':<{col_flag}} "
        f"{'month':<{col_flag}} {'heavy_fav%':<{col_hfav}} {'status':<{col_stat}}"
    )
    print(header)
    print("-" * len(header))

    for r in SUMMARY_ROWS:
        new_wl = f"{r['new_W']}-{r['new_L']}"
        row = (
            f"{r['id']:<{col_id}} {r['tier']:<{col_tier}} {r['hist_record']:<{col_hist}} "
            f"{new_wl:<{col_new}} {r['new_N']:<{col_n}} {r['new_win_pct']:<{col_pct}} "
            f"{r['wilson']:<{col_wil}} {r['dupe_flag']:<{col_flag}} {r['direction_flag']:<{col_flag}} "
            f"{r['month_flag']:<{col_flag}} {r['heavy_fav_pct']:<{col_hfav}} {r['status']:<{col_stat}}"
        )
        print(row)

    print()
    # Count statuses
    status_counts = Counter(r["status"] for r in SUMMARY_ROWS)
    print(f"STATUS SUMMARY: PASS={status_counts.get('PASS',0)}, WARN={status_counts.get('WARN',0)}, FLAG={status_counts.get('FLAG',0)}")
    print()

    # Patterns with regressions (new_win_pct < hist win rate by more than 5%)
    print("## REGRESSION ALERTS (new win% < hist win% by >5pp):")
    for r in SUMMARY_ROWS:
        try:
            hist_w, hist_l = r["hist_record"].split("-")
            hist_n = int(hist_w) + int(hist_l)
            hist_pct = int(hist_w) / hist_n * 100 if hist_n > 0 else 0
            new_pct_val = float(r["new_win_pct"].rstrip("%"))
            diff = hist_pct - new_pct_val
            if diff > 5:
                print(f"  {r['id']}: hist={hist_pct:.1f}% new={new_pct_val:.1f}% (diff={diff:.1f}pp) n={r['new_N']}")
        except Exception:
            pass

    print()
    print("## IMPROVEMENT ALERTS (new win% > hist win% by >5pp):")
    for r in SUMMARY_ROWS:
        try:
            hist_w, hist_l = r["hist_record"].split("-")
            hist_n = int(hist_w) + int(hist_l)
            hist_pct = int(hist_w) / hist_n * 100 if hist_n > 0 else 0
            new_pct_val = float(r["new_win_pct"].rstrip("%"))
            diff = new_pct_val - hist_pct
            if diff > 5:
                print(f"  {r['id']}: hist={hist_pct:.1f}% new={new_pct_val:.1f}% (diff=+{diff:.1f}pp) n={r['new_N']}")
        except Exception:
            pass

    print()
    print("Audit complete.")


if __name__ == "__main__":
    main()
