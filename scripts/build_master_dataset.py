#!/usr/bin/env python3
"""
Build a clean, canonical master dataset from NBA + MLB graded signals.

Outputs:
  data/master/master_signals.csv          – clean graded rows
  data/master/master_signals.jsonl        – same, JSONL
  data/master/master_source_performance.csv
  data/master/master_market_performance.csv
  data/master/master_pattern_performance.csv
  data/master/master_data_quality_issues.csv
  data/master/master_dataset_summary.md

Usage:
  python3 scripts/build_master_dataset.py --sport ALL
  python3 scripts/build_master_dataset.py --sport NBA --date 2026-01-26 --to 2026-04-23
  python3 scripts/build_master_dataset.py --sport ALL --include-mlb-props
"""

import argparse
import csv
import hashlib
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

# ── paths ─────────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent.parent

# NBA: graded_signals_latest has signal+grade pre-joined
NBA_GRADED_SIGNALS = BASE_DIR / "data" / "analysis" / "graded_signals_latest.jsonl"
# NBA grades (for roi_eligible, units, provider info not in graded_signals)
NBA_GRADES = BASE_DIR / "data" / "ledger" / "grades_latest.jsonl"

# MLB: separate signal + grade files (no pre-joined analysis file)
MLB_SIGNALS = BASE_DIR / "data" / "ledger" / "mlb" / "signals_latest.jsonl"
MLB_GRADES = BASE_DIR / "data" / "ledger" / "mlb" / "grades_latest.jsonl"

# Pattern registry
PATTERN_REGISTRY = BASE_DIR / "data" / "reports" / "pattern_registry_active.json"

# ── master row field order ────────────────────────────────────────────────────

MASTER_FIELDS = [
    # Identity
    "master_id", "sport", "date", "event_key", "matchup_key", "signal_id",
    "occurrence_id", "pick_id",
    # Bet
    "market_type", "selection", "side", "line", "odds", "bet_label",
    "player_name", "player_key", "team", "opponent", "stat_type",
    # Source
    "source_combo", "sources", "source_count", "primary_source",
    "signal_type", "consensus_type",
    # Tier / pattern
    "auto_tier", "published_tier", "tier", "pattern_id", "pattern_name",
    "pattern_reason", "confidence_bucket",
    # Grade
    "status", "result", "stat_value", "units", "roi_eligible", "provider",
    "provider_game_id", "provider_trace", "notes",
    # Quality
    "is_clean", "excluded_reason", "data_quality_flags",
]

ISSUE_FIELDS = [
    "sport", "date", "signal_id", "source_combo", "market_type", "selection",
    "line", "status", "issue_type", "severity", "reason", "recommended_fix",
]

# ── helpers ───────────────────────────────────────────────────────────────────

def _date_from_day_key(day_key: str) -> str:
    """Extract YYYY-MM-DD from day_key like 'NBA:2026:01:26'."""
    parts = day_key.split(":")
    if len(parts) >= 4:
        return f"{parts[1]}-{parts[2]}-{parts[3]}"
    return ""


def _sport_from_day_key(day_key: str) -> str:
    parts = day_key.split(":")
    return parts[0] if parts else ""


def _extract_player_name(selection: str) -> str:
    """Extract human-readable player name from selection like 'NBA:paolo_banchero::assists::OVER'."""
    if not selection:
        return ""
    parts = selection.split("::")
    if len(parts) >= 1:
        raw = parts[0].split(":")[-1]  # after sport prefix
        return raw.replace("_", " ").title()
    return ""


def _extract_direction(selection: str) -> str:
    if not selection:
        return ""
    parts = selection.split("::")
    return parts[-1] if parts else ""


def _extract_stat_type(selection: str, atomic_stat: str = None) -> str:
    if atomic_stat:
        return atomic_stat
    if not selection:
        return ""
    parts = selection.split("::")
    if len(parts) >= 3:
        return parts[1]
    return ""


def _compute_master_id(sport: str, date: str, signal_id: str) -> str:
    raw = f"{sport}|{date}|{signal_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _tier_from_source_count(source_count: int) -> str:
    if source_count >= 5:
        return "A"
    if source_count >= 4:
        return "B"  # note: actual tier depends on pattern registry
    if source_count >= 3:
        return "C"
    if source_count >= 2:
        return "D"
    return "E"


def _confidence_bucket(score: int) -> str:
    if score is None:
        return ""
    if score >= 80:
        return "very_high"
    if score >= 60:
        return "high"
    if score >= 40:
        return "medium"
    if score >= 20:
        return "low"
    return "very_low"


def _safe_str(v):
    if v is None:
        return ""
    if isinstance(v, list):
        return "|".join(str(x) for x in v)
    return str(v)


# ── loaders ───────────────────────────────────────────────────────────────────

def load_jsonl(path: Path) -> list[dict]:
    rows = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def load_pattern_registry(path: Path) -> dict:
    """Returns {pattern_id: row} from active registry."""
    if not path.exists():
        return {}
    with open(path) as f:
        data = json.load(f)
    return {r["id"]: r for r in data.get("rows", [])}


def load_nba_grade_index(path: Path) -> dict:
    """Build signal_id → grade dict from grades_latest.jsonl."""
    idx = {}
    if not path.exists():
        return idx
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            g = json.loads(line)
            sid = g.get("signal_id")
            if sid:
                idx[sid] = g
    return idx


# ── row builders ──────────────────────────────────────────────────────────────

def build_nba_row(sig: dict, grade: dict | None, patterns: dict) -> dict:
    """Build a master row from an NBA graded_signals_latest record."""
    day_key = sig.get("day_key", "")
    date = sig.get("derived_date") or _date_from_day_key(day_key)
    sport = "NBA"
    signal_id = sig.get("signal_id", "")
    selection = sig.get("selection", "")
    sources = sig.get("sources_present") or sig.get("sources") or []
    sources_combo = sig.get("sources_combo", "")
    source_count = len(sources) if isinstance(sources, list) else sources_combo.count("|") + 1
    market_type = sig.get("market_type", "")
    atomic_stat = sig.get("atomic_stat") or sig.get("stat_key", "")
    direction = _extract_direction(selection) or sig.get("direction", "")
    score = sig.get("score")

    # Grade fields — from graded_signals (pre-joined) or separate grade record
    result = sig.get("result", "")
    grade_status = sig.get("grade_status", "")
    units = sig.get("units")
    provider_meta = sig.get("provider_meta") or {}
    provider = ""
    provider_game_id = ""
    provider_trace = ""
    roi_eligible = None

    if grade:
        roi_eligible = grade.get("roi_eligible")
        units = units if units is not None else grade.get("units")
        provider = grade.get("provider", "") if "provider" in grade else ""
        provider_game_id = grade.get("provider_game_id", "")
        gi = grade.get("games_info") or {}
        if gi:
            provider = provider or gi.get("provider", "")
            provider_game_id = provider_game_id or gi.get("matched_game_id", "")
            provider_trace = gi.get("match_orientation", "")

    # Pattern matching
    pattern_id = ""
    pattern_name = ""
    pattern_tier = ""
    for pid, prow in patterns.items():
        if prow.get("market_type") == market_type:
            p_stat = prow.get("stat_type") or prow.get("atomic_stat", "")
            p_dir = prow.get("direction", "")
            if p_stat and p_stat == atomic_stat and (not p_dir or p_dir == direction):
                src = prow.get("source_id", "")
                if src and src in sources_combo:
                    pattern_id = pid
                    pattern_name = prow.get("label", "")
                    pattern_tier = prow.get("tier_eligible", "")
                    break

    auto_tier = _tier_from_source_count(source_count)
    tier = pattern_tier or auto_tier

    # Teams
    home_team = sig.get("home_team", "")
    away_team = sig.get("away_team", "")
    event_key = sig.get("canonical_event_key") or sig.get("event_key_safe", "")
    player_name = _extract_player_name(selection) if market_type == "player_prop" else ""
    player_key = sig.get("player_id", "")
    stat_type = _extract_stat_type(selection, atomic_stat)
    odds = sig.get("best_odds")
    notes = sig.get("notes", "")

    return {
        "master_id": _compute_master_id(sport, date, signal_id),
        "sport": sport,
        "date": date,
        "event_key": event_key,
        "matchup_key": sig.get("matchup_key", "") if "matchup_key" in sig else f"{away_team}@{home_team}",
        "signal_id": signal_id,
        "occurrence_id": "",
        "pick_id": "",
        "market_type": market_type,
        "selection": selection,
        "side": direction,
        "line": sig.get("line"),
        "odds": odds,
        "bet_label": f"{player_name} {stat_type} {direction} {sig.get('line','')}" if market_type == "player_prop" else f"{selection} {direction}",
        "player_name": player_name,
        "player_key": player_key,
        "team": home_team,
        "opponent": away_team,
        "stat_type": stat_type,
        "source_combo": sources_combo,
        "sources": "|".join(sources) if isinstance(sources, list) else str(sources),
        "source_count": source_count,
        "primary_source": sources[0] if sources else "",
        "signal_type": sig.get("signal_type", ""),
        "consensus_type": "",
        "auto_tier": auto_tier,
        "published_tier": pattern_tier,
        "tier": tier,
        "pattern_id": pattern_id,
        "pattern_name": pattern_name,
        "pattern_reason": "",
        "confidence_bucket": _confidence_bucket(score),
        "status": grade_status if grade_status else ("GRADED" if result else "UNGRADED"),
        "result": result or "",
        "stat_value": sig.get("score", ""),  # 'score' in graded_signals is the consensus score
        "units": units,
        "roi_eligible": roi_eligible if roi_eligible is not None else False,
        "provider": provider,
        "provider_game_id": provider_game_id,
        "provider_trace": provider_trace,
        "notes": notes,
        "is_clean": None,  # filled later
        "excluded_reason": "",
        "data_quality_flags": "",
    }


def build_mlb_row(sig: dict, grade: dict | None, patterns: dict, include_props: bool) -> dict:
    """Build a master row from MLB signal + grade records."""
    day_key = sig.get("day_key", "")
    date = _date_from_day_key(day_key)
    sport = "MLB"
    signal_id = sig.get("signal_id", "")
    selection = sig.get("selection", "")
    sources = sig.get("sources_present") or sig.get("sources") or []
    sources_combo = sig.get("sources_combo", "")
    source_count = len(sources) if isinstance(sources, list) else sources_combo.count("|") + 1
    market_type = sig.get("market_type", "")
    atomic_stat = sig.get("atomic_stat", "")
    direction = _extract_direction(selection) or sig.get("direction", "")
    score = sig.get("score")

    # Grade fields
    result = ""
    grade_status = ""
    units = None
    roi_eligible = None
    provider = ""
    provider_game_id = ""
    provider_trace = ""
    notes = ""

    if grade:
        result = grade.get("result", "") or ""
        status_raw = grade.get("status", "")
        grade_status = status_raw
        units = grade.get("units")
        roi_eligible = grade.get("roi_eligible")
        notes = grade.get("notes", "") or grade.get("grade_notes", "") or ""
        provider_game_id = grade.get("provider_game_id", "")
        gi = grade.get("games_info") or {}
        if gi:
            provider = gi.get("provider", "")
            provider_trace = gi.get("match_orientation", "")

    # Teams
    home_team = sig.get("home_team", "")
    away_team = sig.get("away_team", "")
    event_key = sig.get("event_key", "")
    player_name = _extract_player_name(selection) if market_type == "player_prop" else ""
    player_key = sig.get("player_id", "")
    stat_type = _extract_stat_type(selection, atomic_stat)
    odds = sig.get("best_odds")

    return {
        "master_id": _compute_master_id(sport, date, signal_id),
        "sport": sport,
        "date": date,
        "event_key": event_key,
        "matchup_key": sig.get("matchup_key", "") if "matchup_key" in sig else f"{away_team}@{home_team}",
        "signal_id": signal_id,
        "occurrence_id": sig.get("occurrence_id", ""),
        "pick_id": "",
        "market_type": market_type,
        "selection": selection,
        "side": direction,
        "line": sig.get("line"),
        "odds": odds,
        "bet_label": f"{player_name} {stat_type} {direction} {sig.get('line','')}" if market_type == "player_prop" else f"{selection} {direction}",
        "player_name": player_name,
        "player_key": player_key,
        "team": home_team,
        "opponent": away_team,
        "stat_type": stat_type,
        "source_combo": sources_combo,
        "sources": "|".join(sources) if isinstance(sources, list) else str(sources),
        "source_count": source_count,
        "primary_source": sources[0] if sources else "",
        "signal_type": sig.get("signal_type", ""),
        "consensus_type": "",
        "auto_tier": _tier_from_source_count(source_count),
        "published_tier": "",
        "tier": _tier_from_source_count(source_count),
        "pattern_id": "",
        "pattern_name": "",
        "pattern_reason": "",
        "confidence_bucket": _confidence_bucket(score),
        "status": grade_status or "UNGRADED",
        "result": result,
        "stat_value": "",
        "units": units,
        "roi_eligible": roi_eligible if roi_eligible is not None else False,
        "provider": provider,
        "provider_game_id": provider_game_id,
        "provider_trace": provider_trace,
        "notes": notes,
        "is_clean": None,  # filled later
        "excluded_reason": "",
        "data_quality_flags": "",
    }


# ── clean / reject logic ─────────────────────────────────────────────────────

def evaluate_clean(row: dict, include_mlb_props: bool) -> tuple[bool, list[dict]]:
    """Return (is_clean, list_of_issues). A row is clean if no blocking issues."""
    issues = []
    sport = row["sport"]
    market = row["market_type"]
    result = row["result"]
    status = row["status"]
    line = row["line"]

    def issue(issue_type, severity, reason, fix=""):
        issues.append({
            "sport": sport,
            "date": row["date"],
            "signal_id": row["signal_id"],
            "source_combo": row["source_combo"],
            "market_type": market,
            "selection": row["selection"],
            "line": line,
            "status": status,
            "issue_type": issue_type,
            "severity": severity,
            "reason": reason,
            "recommended_fix": fix,
        })

    # Hard failures
    if sport not in ("NBA", "MLB"):
        issue("wrong_sport", "critical", f"Unexpected sport: {sport}", "remove")
        return False, issues

    if not row["date"]:
        issue("missing_date", "critical", "No date", "investigate source")
        return False, issues

    if not row["event_key"]:
        issue("missing_event_key", "critical", "No event_key", "investigate source")
        return False, issues

    if not market:
        issue("missing_market_type", "critical", "No market_type", "investigate source")
        return False, issues

    if not row["selection"]:
        issue("missing_selection", "critical", "No selection", "investigate source")
        return False, issues

    if status == "ERROR":
        issue("error_status", "high", f"Grade status ERROR: {row['notes']}", "fix grading")
        return False, issues

    if status == "INELIGIBLE":
        issue("ineligible", "medium", f"Ineligible: {row['notes']}", "review eligibility")
        return False, issues

    if status in ("PENDING", "UNGRADED"):
        issue("not_graded", "medium", f"Status: {status}", "wait for grading")
        return False, issues

    if result not in ("WIN", "LOSS", "PUSH"):
        issue("bad_result", "high", f"Result '{result}' not WIN/LOSS/PUSH", "fix grading")
        return False, issues

    if not row["roi_eligible"]:
        issue("not_roi_eligible", "low", "roi_eligible is false", "review roi criteria")
        return False, issues

    # Line required for props and spreads and totals
    if market in ("player_prop", "spread", "game_total", "team_total") and line is None:
        issue("missing_line", "high", f"Missing line for {market}", "fix normalizer")
        return False, issues

    # Player prop specific
    if market == "player_prop":
        if not row["player_key"]:
            issue("missing_player_key", "medium", "Player prop with no player_key", "fix normalizer")
            return False, issues

    # MLB-specific: exclude player props unless explicitly included
    if sport == "MLB" and market == "player_prop" and not include_mlb_props:
        issue("mlb_prop_excluded", "info", "MLB player props excluded from clean set", "use --include-mlb-props")
        return False, issues

    # Warnings (non-blocking)
    if row["odds"] is None:
        flags = row.get("data_quality_flags", "")
        row["data_quality_flags"] = (flags + "|missing_odds").strip("|") if flags else "missing_odds"

    return True, issues


# ── performance aggregation ───────────────────────────────────────────────────

def compute_source_performance(clean_rows: list[dict]) -> list[dict]:
    groups = defaultdict(lambda: {"wins": 0, "losses": 0, "pushes": 0, "units": 0.0, "odds_sum": 0.0, "odds_n": 0})
    for r in clean_rows:
        # By source_combo
        key = (r["sport"], r["source_combo"], r["source_combo"], r["market_type"])
        g = groups[key]
        if r["result"] == "WIN":
            g["wins"] += 1
        elif r["result"] == "LOSS":
            g["losses"] += 1
        else:
            g["pushes"] += 1
        if r["units"] is not None:
            g["units"] += float(r["units"])
        if r["odds"] is not None:
            g["odds_sum"] += float(r["odds"])
            g["odds_n"] += 1
        # Also by individual source
        sources = r["sources"].split("|") if r["sources"] else []
        for src in sources:
            skey = (r["sport"], r["source_combo"], src, r["market_type"])
            sg = groups[skey]
            if r["result"] == "WIN":
                sg["wins"] += 1
            elif r["result"] == "LOSS":
                sg["losses"] += 1
            else:
                sg["pushes"] += 1
            if r["units"] is not None:
                sg["units"] += float(r["units"])
            if r["odds"] is not None:
                sg["odds_sum"] += float(r["odds"])
                sg["odds_n"] += 1

    out = []
    for (sport, combo, source, mkt), g in sorted(groups.items()):
        n = g["wins"] + g["losses"] + g["pushes"]
        wr = g["wins"] / n if n else 0
        avg_odds = g["odds_sum"] / g["odds_n"] if g["odds_n"] else None
        roi = g["units"] / n if n else 0
        cb = ""
        if n >= 100:
            cb = "high"
        elif n >= 50:
            cb = "medium"
        elif n >= 20:
            cb = "low"
        else:
            cb = "very_low"
        out.append({
            "sport": sport, "source_combo": combo, "source": source,
            "market_type": mkt, "n": n, "wins": g["wins"], "losses": g["losses"],
            "pushes": g["pushes"], "win_rate": round(wr, 4), "units": round(g["units"], 2),
            "roi": round(roi, 4), "avg_odds": round(avg_odds, 1) if avg_odds else "",
            "confidence_bucket": cb,
        })
    return out


def compute_market_performance(clean_rows: list[dict]) -> list[dict]:
    groups = defaultdict(lambda: {"wins": 0, "losses": 0, "pushes": 0, "units": 0.0})
    for r in clean_rows:
        key = (r["sport"], r["market_type"])
        g = groups[key]
        if r["result"] == "WIN":
            g["wins"] += 1
        elif r["result"] == "LOSS":
            g["losses"] += 1
        else:
            g["pushes"] += 1
        if r["units"] is not None:
            g["units"] += float(r["units"])

    out = []
    for (sport, mkt), g in sorted(groups.items()):
        n = g["wins"] + g["losses"] + g["pushes"]
        wr = g["wins"] / n if n else 0
        roi = g["units"] / n if n else 0
        out.append({
            "sport": sport, "market_type": mkt, "n": n, "wins": g["wins"],
            "losses": g["losses"], "pushes": g["pushes"],
            "win_rate": round(wr, 4), "units": round(g["units"], 2),
            "roi": round(roi, 4),
        })
    return out


def compute_pattern_performance(clean_rows: list[dict]) -> list[dict]:
    groups = defaultdict(lambda: {"wins": 0, "losses": 0, "pushes": 0, "units": 0.0})
    for r in clean_rows:
        pid = r["pattern_id"] or r["tier"]
        pname = r["pattern_name"] or r["tier"]
        key = (r["sport"], pid, pname, r["market_type"], r["source_combo"], r["tier"])
        g = groups[key]
        if r["result"] == "WIN":
            g["wins"] += 1
        elif r["result"] == "LOSS":
            g["losses"] += 1
        else:
            g["pushes"] += 1
        if r["units"] is not None:
            g["units"] += float(r["units"])

    out = []
    for (sport, pid, pname, mkt, combo, tier), g in sorted(groups.items()):
        n = g["wins"] + g["losses"] + g["pushes"]
        wr = g["wins"] / n if n else 0
        roi = g["units"] / n if n else 0
        cb = "high" if n >= 100 else "medium" if n >= 50 else "low" if n >= 20 else "very_low"
        out.append({
            "sport": sport, "pattern_id": pid, "pattern_name": pname,
            "market_type": mkt, "source_combo": combo, "tier": tier,
            "n": n, "wins": g["wins"], "losses": g["losses"], "pushes": g["pushes"],
            "win_rate": round(wr, 4), "units": round(g["units"], 2),
            "roi": round(roi, 4), "confidence_bucket": cb,
        })
    return out


# ── writers ───────────────────────────────────────────────────────────────────

def write_csv(rows: list[dict], path: Path, fields: list[str]):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: _safe_str(r.get(k, "")) for k in fields})
    print(f"  wrote {path.name}: {len(rows)} rows")


def write_jsonl(rows: list[dict], path: Path):
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r, default=str) + "\n")
    print(f"  wrote {path.name}: {len(rows)} rows")


def write_summary(
    out_dir: Path,
    inputs_used: dict,
    clean_rows: list[dict],
    rejected_rows: list[dict],
    all_issues: list[dict],
    source_perf: list[dict],
    market_perf: list[dict],
    pattern_perf: list[dict],
    include_mlb_props: bool,
    date_from: str | None,
    date_to: str | None,
):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    nba_clean = [r for r in clean_rows if r["sport"] == "NBA"]
    mlb_clean = [r for r in clean_rows if r["sport"] == "MLB"]
    nba_rejected = [r for r in rejected_rows if r["sport"] == "NBA"]
    mlb_rejected = [r for r in rejected_rows if r["sport"] == "MLB"]

    nba_dates = sorted(set(r["date"] for r in nba_clean)) if nba_clean else []
    mlb_dates = sorted(set(r["date"] for r in mlb_clean)) if mlb_clean else []

    # Source counts
    nba_source_counts = Counter(r["source_combo"] for r in nba_clean)
    mlb_source_counts = Counter(r["source_combo"] for r in mlb_clean)

    # Market counts
    nba_market_counts = Counter(r["market_type"] for r in nba_clean)
    mlb_market_counts = Counter(r["market_type"] for r in mlb_clean)

    # Issue breakdown
    issue_types = Counter(i["issue_type"] for i in all_issues)
    top_issues = issue_types.most_common(10)

    # Readiness
    nba_ready = len(nba_clean) >= 100
    mlb_ready = len(mlb_clean) >= 50
    dataset_ready = nba_ready  # MLB not required for readiness

    lines = [
        "# Master Dataset Summary",
        "",
        f"**Generated:** {now}",
        f"**Date range filter:** {date_from or 'all'} to {date_to or 'all'}",
        f"**MLB props included:** {include_mlb_props}",
        "",
        "## Input Files",
        "",
    ]
    for label, path in inputs_used.items():
        exists = "FOUND" if Path(path).exists() else "MISSING"
        lines.append(f"- {label}: `{path}` [{exists}]")

    lines += [
        "",
        "## Row Counts",
        "",
        f"| Sport | Clean | Rejected | Total |",
        f"|-------|-------|----------|-------|",
        f"| NBA   | {len(nba_clean):,} | {len(nba_rejected):,} | {len(nba_clean)+len(nba_rejected):,} |",
        f"| MLB   | {len(mlb_clean):,} | {len(mlb_rejected):,} | {len(mlb_clean)+len(mlb_rejected):,} |",
        f"| **Total** | **{len(clean_rows):,}** | **{len(rejected_rows):,}** | **{len(clean_rows)+len(rejected_rows):,}** |",
        "",
        "## Date Ranges",
        "",
        f"- NBA: {nba_dates[0] if nba_dates else 'n/a'} to {nba_dates[-1] if nba_dates else 'n/a'} ({len(nba_dates)} days)",
        f"- MLB: {mlb_dates[0] if mlb_dates else 'n/a'} to {mlb_dates[-1] if mlb_dates else 'n/a'} ({len(mlb_dates)} days)",
        "",
        "## Rows by Source Combo (NBA)",
        "",
    ]
    for combo, cnt in nba_source_counts.most_common(15):
        lines.append(f"- `{combo}`: {cnt:,}")

    lines += ["", "## Rows by Source Combo (MLB)", ""]
    for combo, cnt in mlb_source_counts.most_common(15):
        lines.append(f"- `{combo}`: {cnt:,}")

    lines += ["", "## Rows by Market", ""]
    for sport_label, mc in [("NBA", nba_market_counts), ("MLB", mlb_market_counts)]:
        for mkt, cnt in mc.most_common():
            lines.append(f"- {sport_label} {mkt}: {cnt:,}")

    lines += ["", "## Performance Summary", ""]
    for mp in market_perf:
        lines.append(f"- {mp['sport']} {mp['market_type']}: {mp['wins']}W-{mp['losses']}L-{mp['pushes']}P "
                      f"({mp['win_rate']:.1%}) units={mp['units']:+.1f} roi={mp['roi']:.1%}")

    lines += ["", "## Top Quality Issues", ""]
    for itype, cnt in top_issues:
        lines.append(f"- `{itype}`: {cnt:,}")

    lines += [
        "",
        "## Readiness",
        "",
        f"- NBA: {'READY' if nba_ready else 'NOT READY'} ({len(nba_clean):,} clean rows)",
        f"- MLB: {'READY' if mlb_ready else 'NOT READY'} ({len(mlb_clean):,} clean rows)",
        f"- **Master dataset READY_FOR_SOURCE_EVALUATION:** {'YES' if dataset_ready else 'NO'}",
        "",
        "## What Is Still Not Ready",
        "",
    ]
    if not nba_ready:
        lines.append("- NBA: insufficient clean graded rows")
    if not mlb_ready:
        lines.append("- MLB: insufficient clean graded rows or player props not yet confidently graded")
    if not include_mlb_props:
        lines.append("- MLB player props are excluded from clean set (use --include-mlb-props to include)")

    path = out_dir / "master_dataset_summary.md"
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"  wrote master_dataset_summary.md")


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Build master dataset from graded signals")
    parser.add_argument("--sport", default="ALL", choices=["NBA", "MLB", "ALL"])
    parser.add_argument("--date", help="Start date YYYY-MM-DD")
    parser.add_argument("--to", help="End date YYYY-MM-DD")
    parser.add_argument("--include-mlb-props", action="store_true", default=False)
    parser.add_argument("--out-dir", default=str(BASE_DIR / "data" / "master"))
    parser.add_argument("--write-parquet", action="store_true", default=False)
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    sports = ["NBA", "MLB"] if args.sport == "ALL" else [args.sport]
    date_from = args.date
    date_to = args.to

    print(f"Building master dataset: sports={sports}, date={date_from or 'all'} to {date_to or 'all'}")

    # Track inputs
    inputs_used = {}
    all_rows = []
    all_issues = []

    # Load pattern registry
    patterns = load_pattern_registry(PATTERN_REGISTRY)
    if patterns:
        inputs_used["pattern_registry"] = str(PATTERN_REGISTRY)
        print(f"  loaded {len(patterns)} active patterns")

    # ── NBA ────────────────────────────────────────────────────────────────
    if "NBA" in sports:
        if not NBA_GRADED_SIGNALS.exists():
            print(f"  WARNING: NBA graded signals not found: {NBA_GRADED_SIGNALS}")
        else:
            inputs_used["nba_graded_signals"] = str(NBA_GRADED_SIGNALS)
            print(f"  loading NBA graded signals from {NBA_GRADED_SIGNALS.name}...")
            nba_sigs = load_jsonl(NBA_GRADED_SIGNALS)
            print(f"  loaded {len(nba_sigs):,} NBA graded signal rows")

            # Load grade index for roi_eligible / units / provider
            nba_grade_idx = {}
            if NBA_GRADES.exists():
                inputs_used["nba_grades"] = str(NBA_GRADES)
                print(f"  loading NBA grade index...")
                nba_grade_idx = load_nba_grade_index(NBA_GRADES)
                print(f"  loaded {len(nba_grade_idx):,} NBA grade records")

            for sig in nba_sigs:
                day_key = sig.get("day_key", "")
                date = sig.get("derived_date") or _date_from_day_key(day_key)
                if date_from and date < date_from:
                    continue
                if date_to and date > date_to:
                    continue

                grade = nba_grade_idx.get(sig.get("signal_id"))
                row = build_nba_row(sig, grade, patterns)
                all_rows.append(row)

    # ── MLB ────────────────────────────────────────────────────────────────
    if "MLB" in sports:
        if not MLB_SIGNALS.exists():
            print(f"  WARNING: MLB signals not found: {MLB_SIGNALS}")
        elif not MLB_GRADES.exists():
            print(f"  WARNING: MLB grades not found: {MLB_GRADES}")
        else:
            inputs_used["mlb_signals"] = str(MLB_SIGNALS)
            inputs_used["mlb_grades"] = str(MLB_GRADES)
            print(f"  loading MLB signals from {MLB_SIGNALS.name}...")
            mlb_sigs = load_jsonl(MLB_SIGNALS)
            print(f"  loaded {len(mlb_sigs):,} MLB signal rows")

            print(f"  loading MLB grades...")
            mlb_grades_raw = load_jsonl(MLB_GRADES)
            mlb_grade_idx = {}
            for g in mlb_grades_raw:
                sid = g.get("signal_id")
                if sid:
                    mlb_grade_idx[sid] = g
            print(f"  loaded {len(mlb_grade_idx):,} MLB grade records")

            for sig in mlb_sigs:
                day_key = sig.get("day_key", "")
                date = _date_from_day_key(day_key)
                if date_from and date < date_from:
                    continue
                if date_to and date > date_to:
                    continue

                grade = mlb_grade_idx.get(sig.get("signal_id"))
                row = build_mlb_row(sig, grade, patterns, args.include_mlb_props)
                all_rows.append(row)

    print(f"\n  total rows before clean/reject: {len(all_rows):,}")

    # ── dedup ──────────────────────────────────────────────────────────────
    seen_keys = {}
    deduped_rows = []
    dup_count = 0
    for row in all_rows:
        dedup_key = (
            row["sport"], row["date"], row["event_key"], row["market_type"],
            row["selection"], str(row["line"]), row["source_combo"], row["signal_type"],
            row["signal_id"],
        )
        if dedup_key in seen_keys:
            dup_count += 1
            all_issues.append({
                "sport": row["sport"], "date": row["date"], "signal_id": row["signal_id"],
                "source_combo": row["source_combo"], "market_type": row["market_type"],
                "selection": row["selection"], "line": row["line"], "status": row["status"],
                "issue_type": "duplicate", "severity": "low",
                "reason": f"Duplicate of master_id={seen_keys[dedup_key]}",
                "recommended_fix": "deduped automatically",
            })
            continue
        seen_keys[dedup_key] = row["master_id"]
        deduped_rows.append(row)

    if dup_count:
        print(f"  removed {dup_count} duplicates")

    # ── evaluate clean/reject ──────────────────────────────────────────────
    clean_rows = []
    rejected_rows = []

    for row in deduped_rows:
        is_clean, issues = evaluate_clean(row, args.include_mlb_props)
        row["is_clean"] = is_clean
        if not is_clean and issues:
            row["excluded_reason"] = issues[0]["issue_type"]
        all_issues.extend(issues)
        if is_clean:
            clean_rows.append(row)
        else:
            rejected_rows.append(row)

    print(f"  clean: {len(clean_rows):,}  |  rejected: {len(rejected_rows):,}")

    # ── performance tables ─────────────────────────────────────────────────
    source_perf = compute_source_performance(clean_rows)
    market_perf = compute_market_performance(clean_rows)
    pattern_perf = compute_pattern_performance(clean_rows)

    # ── write outputs ──────────────────────────────────────────────────────
    print("\nWriting outputs:")
    write_csv(clean_rows, out_dir / "master_signals.csv", MASTER_FIELDS)
    write_jsonl(clean_rows, out_dir / "master_signals.jsonl")

    src_fields = ["sport", "source_combo", "source", "market_type", "n", "wins", "losses",
                  "pushes", "win_rate", "units", "roi", "avg_odds", "confidence_bucket"]
    write_csv(source_perf, out_dir / "master_source_performance.csv", src_fields)

    mkt_fields = ["sport", "market_type", "n", "wins", "losses", "pushes", "win_rate", "units", "roi"]
    write_csv(market_perf, out_dir / "master_market_performance.csv", mkt_fields)

    pat_fields = ["sport", "pattern_id", "pattern_name", "market_type", "source_combo", "tier",
                  "n", "wins", "losses", "pushes", "win_rate", "units", "roi", "confidence_bucket"]
    write_csv(pattern_perf, out_dir / "master_pattern_performance.csv", pat_fields)

    write_csv(all_issues, out_dir / "master_data_quality_issues.csv", ISSUE_FIELDS)

    write_summary(out_dir, inputs_used, clean_rows, rejected_rows, all_issues,
                  source_perf, market_perf, pattern_perf,
                  args.include_mlb_props, date_from, date_to)

    # ── parquet (optional) ─────────────────────────────────────────────────
    if args.write_parquet:
        try:
            import pandas as pd
            df = pd.DataFrame(clean_rows)
            pq_path = out_dir / "master_signals.parquet"
            df.to_parquet(pq_path, index=False)
            print(f"  wrote master_signals.parquet: {len(df)} rows")
        except ImportError:
            print("  skipped parquet: pandas/pyarrow not installed")

    # ── final report ───────────────────────────────────────────────────────
    nba_clean_n = sum(1 for r in clean_rows if r["sport"] == "NBA")
    mlb_clean_n = sum(1 for r in clean_rows if r["sport"] == "MLB")
    nba_rej = sum(1 for r in rejected_rows if r["sport"] == "NBA")
    mlb_rej = sum(1 for r in rejected_rows if r["sport"] == "MLB")

    issue_types = Counter(i["issue_type"] for i in all_issues)

    print(f"\n{'='*60}")
    print(f"MASTER DATASET BUILD COMPLETE")
    print(f"{'='*60}")
    print(f"  Files created in: {out_dir}")
    print(f"  Input files discovered: {len(inputs_used)}")
    for label, p in inputs_used.items():
        print(f"    - {label}: {Path(p).name}")
    print(f"\n  Clean rows:    NBA={nba_clean_n:,}  MLB={mlb_clean_n:,}  Total={len(clean_rows):,}")
    print(f"  Rejected rows: NBA={nba_rej:,}  MLB={mlb_rej:,}  Total={len(rejected_rows):,}")
    print(f"\n  Top rejection reasons:")
    for itype, cnt in issue_types.most_common(10):
        print(f"    {itype}: {cnt:,}")
    print(f"\n  Performance rows: source={len(source_perf)}, market={len(market_perf)}, pattern={len(pattern_perf)}")
    print(f"\n  NBA ready: {'YES' if nba_clean_n >= 100 else 'NO'} ({nba_clean_n:,} clean)")
    print(f"  MLB ready: {'YES' if mlb_clean_n >= 50 else 'NO'} ({mlb_clean_n:,} clean)")
    print(f"  Master dataset READY_FOR_SOURCE_EVALUATION: {'YES' if nba_clean_n >= 100 else 'NO'}")


if __name__ == "__main__":
    main()
