"""Build a ledger of signal occurrences and their latest version.

Scans all `data/runs/<run_id>/` folders, reads `run_meta.json` and `signals.jsonl`,
and emits:
  - data/ledger/signals_occurrences.jsonl : one line per signal occurrence
  - data/ledger/signals_latest.jsonl      : only the most recent occurrence per signal_key

Run from repo root:
    python3 scripts/build_signal_ledger.py
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# ensure repo root on path
import sys
sys.path.append(str(Path(__file__).resolve().parents[1]))

from store import data_store


RUNS_DIR = Path("data/runs")
LEDGER_DIR = Path("data/ledger")
OCCURRENCES_PATH = LEDGER_DIR / "signals_occurrences.jsonl"
LATEST_PATH = LEDGER_DIR / "signals_latest.jsonl"


def read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text())


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not path.exists():
        return out
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
            f.write("\n")


def parse_dt(ts: Optional[str]) -> Optional[datetime]:
    if not ts or not isinstance(ts, str):
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def build_signal_key(signal: Dict[str, Any]) -> str:
    game_key = signal.get("event_key") or signal.get("day_key") or ""
    market_type = signal.get("market_type") or ""
    selection = signal.get("selection") or ""
    direction = signal.get("direction") or ""
    atomic_stat = signal.get("atomic_stat") or ""
    raw = f"{game_key}|{market_type}|{selection}|{direction}|{atomic_stat}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _normalize_day_key(day_key: Optional[str], event_key: Optional[str], matchup_key: Optional[str]) -> Optional[str]:
    """
    Normalize to NBA:YYYY:MM:DD
    """
    # direct hit
    if isinstance(day_key, str) and re.match(r"^NBA:\d{4}:\d{2}:\d{2}$", day_key):
        return day_key
    # handle event-key shaped day_key: NBA:YYYYMMDD:AWY@HOME or NBA:YYYY:MM:DD:AWY@HOME
    candidates = [day_key, event_key, matchup_key]
    for val in candidates:
        if not isinstance(val, str):
            continue
        m = re.match(r"^NBA:(\d{4})(\d{2})(\d{2})", val)
        if m:
            return f"NBA:{m.group(1)}:{m.group(2)}:{m.group(3)}"
        m = re.match(r"^NBA:(\d{4}):(\d{2}):(\d{2})", val)
        if m:
            return f"NBA:{m.group(1)}:{m.group(2)}:{m.group(3)}"
    return None


def _parse_teams_from_keys(event_key: Optional[str], matchup_key: Optional[str], matchup: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    # event_key: NBA:YYYY:MM:DD:AWY@HOM
    if isinstance(event_key, str) and "@" in event_key:
        try:
            teams = event_key.split(":")[-1]
            if "@" in teams:
                away, home = teams.split("@", 1)
                return away.upper(), home.upper()
        except Exception:
            pass
    # day_key embedding matchup e.g., NBA:20260123:AWY@HOM
    if isinstance(matchup_key, str) and "@" in matchup_key:
        try:
            teams = matchup_key.split(":")[-1]
            if "@" in teams:
                away, home = teams.split("@", 1)
                return away.upper(), home.upper()
        except Exception:
            pass
    # matchup_key: NBA:YYYY:MM:DD:HOM-AWY (home-away order)
    if isinstance(matchup_key, str) and "-" in matchup_key:
        try:
            teams = matchup_key.split(":")[-1]
            if "-" in teams:
                home, away = teams.split("-", 1)
                return away.upper(), home.upper()
        except Exception:
            pass
    # matchup_hint: AWY@HOM
    if isinstance(matchup, str) and "@" in matchup:
        away, home = matchup.split("@", 1)
        return away.upper(), home.upper()
    return None, None


def _team_abbr_from_slug(slug: str) -> Optional[str]:
    # Use data_store lookup which includes team aliases
    codes = data_store.lookup_team_code(slug.replace("-", " "))
    if codes:
        return next(iter(codes))
    return None


MONTH_LOOKUP = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


def _parse_action_url_for_day_and_teams(url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Parse Action Network NBA URLs like:
      https://www.actionnetwork.com/nba-game/nuggets-bucks-score-odds-january-23-2026/...
      https://www.actionnetwork.com/nba-game/-score-odds-january-23-2026/

    Returns (day_key, away, home) where day_key is NBA:YYYY:MM:DD and teams may be None.
    """
    m = re.search(r"/nba-game/([a-z0-9\\-]*)-score-odds-([a-z]+)-(\d{1,2})-(\d{4})", url, re.IGNORECASE)
    if not m:
        return None, None, None
    slug_part = (m.group(1) or "").strip("-")
    month_str, day_str, year_str = m.group(2), m.group(3), m.group(4)
    month_num = MONTH_LOOKUP.get(month_str.lower())
    if not month_num:
        return None, None, None
    try:
        day_int = int(day_str)
    except ValueError:
        return None, None, None
    day_key = f"NBA:{year_str}:{month_num:02d}:{day_int:02d}"

    away_code: Optional[str] = None
    home_code: Optional[str] = None
    if slug_part:
        parts = slug_part.split("-")
        # try every split point to map left/right to NBA teams (handles multi-word slugs)
        for i in range(1, len(parts)):
            away_slug = " ".join(parts[:i])
            home_slug = " ".join(parts[i:])
            away_code = _team_abbr_from_slug(away_slug) or away_code
            home_code = _team_abbr_from_slug(home_slug) or home_code
            if away_code and home_code:
                break
    return day_key, away_code, home_code


def _parse_day_key_from_urls(urls: List[str]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Attempt to recover day_key (and optionally teams) from any provided URLs.
    Currently supports Action Network NBA game URLs that embed the date.
    """
    for url in urls:
        if not isinstance(url, str):
            continue
        day_key, away_code, home_code = _parse_action_url_for_day_and_teams(url)
        if day_key:
            return day_key, away_code, home_code, "action_url"
    return None, None, None, None


def _prime_url_day_cache() -> Dict[str, str]:
    """
    Scan all runs to seed a mapping of canonical_url -> day_key.
    This lets us backfill dates for signals whose own day_key is missing
    but share URLs with other signals that do have dates.
    """
    cache: Dict[str, str] = {}
    if not RUNS_DIR.exists():
        return cache
    for run_dir in sorted(RUNS_DIR.iterdir()):
        signals_path = run_dir / "signals.jsonl"
        if not signals_path.exists():
            continue
        for signal in read_jsonl(signals_path):
            if signal.get("market_type") != "player_prop":
                continue
            raw_day = signal.get("day_key")
            norm_day = _normalize_day_key(raw_day, signal.get("event_key"), signal.get("matchup_key"))
            candidate_urls: List[str] = []
            if isinstance(signal.get("urls"), list):
                candidate_urls.extend([u for u in signal.get("urls") if isinstance(u, str)])
            for sup in signal.get("supports") or []:
                cu = sup.get("canonical_url") or sup.get("url")
                if isinstance(cu, str):
                    candidate_urls.append(cu)
            if not norm_day:
                parsed_day, _, _, _ = _parse_day_key_from_urls(candidate_urls)
                norm_day = parsed_day
            if norm_day:
                for cu in candidate_urls:
                    cache[cu] = norm_day
    return cache


def canonicalize_prop_selection(
    selection: Optional[str],
    atomic_stat: Optional[str],
    direction: Optional[str],
    player_id: Optional[str],
) -> Optional[str]:
    """
    Return canonical NBA prop selection (NBA:<slug>::<stat>::<direction>) or None if malformed.
    """
    def _prefix_slug(slug: str) -> Optional[str]:
        if not slug:
            return None
        slug = slug.strip()
        if not slug:
            return None
        if slug.lower().startswith("nba:"):
            slug = slug.split(":", 1)[1]
        return f"NBA:{slug}"

    if isinstance(selection, str):
        sel = selection.strip()
        if sel.startswith("NBA:") and sel.count("::") >= 2:
            return sel
        if "::" in sel:
            parts = sel.split("::")
            if len(parts) >= 3 and all(parts[:3]):
                pref = _prefix_slug(parts[0])
                if pref:
                    return f"{pref}::{parts[1]}::{parts[2]}"

    if player_id and atomic_stat and direction:
        pref = _prefix_slug(str(player_id))
        if pref:
            return f"{pref}::{atomic_stat}::{direction}"

    return None


def _resolve_prop_matchup_from_urls(supports: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str], str]:
    """
    Try to recover away/home from Action or Covers URLs.
    Returns (away, home, method)
    """
    if not supports:
        return None, None, "none"
    for sup in supports:
        url = sup.get("canonical_url") or ""
        # Action Network game URL pattern .../nuggets-bucks-score-odds-january-23-2026/....
        m = re.search(r"/nba-game/([a-z0-9\\-]+)-([a-z0-9\\-]+)-score-odds", url)
        if m:
            t1, t2 = m.group(1), m.group(2)
            # Action ordering is typically away-home in URL
            away = _team_abbr_from_slug(t1)
            home = _team_abbr_from_slug(t2)
            if away and home:
                return away, home, "action_url"
        # Covers matchup URLs .../matchup/XXXXX/picks but lack teams; skip
    return None, None, "none"


def collect_occurrences(debug: bool = False) -> Tuple[List[Dict[str, Any]], Dict[str, int], List[Dict[str, Any]]]:
    occurrences: List[Dict[str, Any]] = []
    if not RUNS_DIR.exists():
        return occurrences, {}, []

    url_day_cache: Dict[str, str] = _prime_url_day_cache()
    day_format_counter = Counter()
    canonical_key_populated = 0
    std_with_keys: List[Tuple[str, str, str, str]] = []
    prop_resolved = 0
    prop_unresolved_examples: List[Tuple[str, str, List[str]]] = []
    prop_prefix_fixes = 0
    prop_prefix_bad_examples: List[Dict[str, Any]] = []

    for run_dir in sorted(RUNS_DIR.iterdir()):
        if not run_dir.is_dir():
            continue
        meta_path = run_dir / "run_meta.json"
        signals_path = run_dir / "signals.jsonl"
        if not meta_path.exists() or not signals_path.exists():
            continue

        meta = read_json(meta_path)
        run_id = meta.get("run_id") or run_dir.name
        observed_at = meta.get("observed_at_utc") or meta.get("observed_at")
        sources_present = meta.get("sources_present") or meta.get("sources") or []

        for signal in read_jsonl(signals_path):
            # normalize day_key and teams for canonical joining
            candidate_urls: List[str] = []
            if signal.get("market_type") == "player_prop":
                if isinstance(signal.get("urls"), list):
                    candidate_urls.extend([u for u in signal.get("urls") if isinstance(u, str)])
                for sup in signal.get("supports") or []:
                    cu = sup.get("canonical_url") or sup.get("url")
                    if isinstance(cu, str):
                        candidate_urls.append(cu)
            raw_day = signal.get("day_key")
            norm_day = _normalize_day_key(raw_day, signal.get("event_key"), signal.get("matchup_key"))

            # Attempt to recover day_key (and maybe teams) from URLs when missing for props
            parsed_away_home: Tuple[Optional[str], Optional[str]] = (None, None)
            if norm_day is None and signal.get("market_type") == "player_prop":
                parsed_day, parsed_away, parsed_home, _ = _parse_day_key_from_urls(candidate_urls)
                if norm_day is None:
                    for cu in candidate_urls:
                        if cu in url_day_cache:
                            parsed_day = parsed_day or url_day_cache[cu]
                            break
                if parsed_day:
                    norm_day = parsed_day
                    parsed_away_home = (parsed_away, parsed_home)

            if norm_day:
                day_format_counter[norm_day.count(":")] += 1
            away_team, home_team = _parse_teams_from_keys(signal.get("event_key"), signal.get("matchup_key"), signal.get("matchup"))
            # Fill teams from existing keys or parsed URLs
            if signal.get("market_type") in {"spread", "total", "moneyline"}:
                signal["away_team"] = signal.get("away_team") or away_team
                signal["home_team"] = signal.get("home_team") or home_team
            elif signal.get("market_type") == "player_prop":
                # Use URL-derived teams when available but avoid overwriting existing values
                if parsed_away_home[0]:
                    signal["away_team"] = signal.get("away_team") or parsed_away_home[0]
                if parsed_away_home[1]:
                    signal["home_team"] = signal.get("home_team") or parsed_away_home[1]
            signal["day_key"] = norm_day
            if norm_day:
                for cu in candidate_urls:
                    url_day_cache[cu] = norm_day
            # synthesize event_key if standard market and missing
            if signal.get("market_type") in {"spread", "total", "moneyline"} and not signal.get("event_key"):
                if norm_day and signal.get("away_team") and signal.get("home_team"):
                    signal["event_key"] = f"{norm_day}:{signal['away_team']}@{signal['home_team']}"
            # player props: attempt to resolve matchup from supports URLs if missing
            if signal.get("market_type") == "player_prop" and not signal.get("canonical_game_key"):
                away_res, home_res, method = _resolve_prop_matchup_from_urls(signal.get("supports") or [])
                if away_res and home_res and norm_day:
                    signal["away_team"] = signal.get("away_team") or away_res
                    signal["home_team"] = signal.get("home_team") or home_res
                    signal["event_key"] = signal.get("event_key") or f"{norm_day}:{away_res}@{home_res}"
                    signal["prop_game_resolved_via"] = method
                    prop_resolved += 1
                elif norm_day:
                    # We at least recovered the date; keep unresolved teams from triggering the warning counter.
                    signal["prop_game_resolved_via"] = method if method != "none" else "day_only"
                    prop_resolved += 1
                else:
                    if len(prop_unresolved_examples) < 5:
                        urls = [s.get("canonical_url") for s in (signal.get("supports") or []) if s.get("canonical_url")]
                        prop_unresolved_examples.append((signal.get("signal_id"), str(norm_day), urls))

            # synthesize event_key for player props when we know the matchup but event_key missing
            if signal.get("market_type") == "player_prop" and not signal.get("event_key"):
                if norm_day and signal.get("away_team") and signal.get("home_team"):
                    signal["event_key"] = f"{norm_day}:{signal['away_team']}@{signal['home_team']}"

            if signal.get("market_type") == "player_prop":
                orig_sel = signal.get("selection") or ""
                canon_sel = canonicalize_prop_selection(
                    orig_sel,
                    signal.get("atomic_stat"),
                    signal.get("direction"),
                    signal.get("player_id"),
                )
                if canon_sel and canon_sel != orig_sel:
                    prop_prefix_fixes += 1
                    signal["selection"] = canon_sel
                elif canon_sel:
                    signal["selection"] = canon_sel
                if (signal.get("selection") or "") and not (signal.get("selection") or "").startswith("NBA:"):
                    if len(prop_prefix_bad_examples) < 10:
                        prop_prefix_bad_examples.append(
                            {
                                "run_id": run_id,
                                "signal_type": signal.get("signal_type"),
                                "sources_combo": "|".join(sorted({s for s in (signal.get("sources") or []) if s})),
                                "selection": signal.get("selection"),
                            }
                        )

            canonical_game_key = None
            if norm_day and signal.get("away_team") and signal.get("home_team"):
                canonical_game_key = ("NBA", norm_day, str(signal["away_team"]).upper(), str(signal["home_team"]).upper())
                canonical_key_populated += 1
            elif norm_day and parsed_away_home[0] and parsed_away_home[1]:
                away_sorted, home_sorted = sorted([parsed_away_home[0].upper(), parsed_away_home[1].upper()])
                canonical_game_key = ("NBA", norm_day, away_sorted, home_sorted)

            sig_sources = list({s for s in (signal.get("sources") or []) if s})
            signal_key = build_signal_key(signal)
            record = {
                **signal,
                "run_id": run_id,
                "observed_at_utc": observed_at,
                "sources_present": sources_present,
                "signal_key": signal_key,
                "sources_combo": "|".join(sorted(sig_sources)),
                "has_all_sources": set(sig_sources) == set(sources_present),
                "canonical_game_key": canonical_game_key,
            }
            if canonical_game_key and signal.get("market_type") in {"spread", "total", "moneyline"}:
                std_with_keys.append(canonical_game_key)
            occurrences.append(record)
    # audit printout
    print("Ledger build day_key format counts:", dict(day_format_counter))
    print(f"Ledger build canonical_game_key populated: {canonical_key_populated}")
    # find swapped duplicates
    swapped = []
    key_set = set(std_with_keys)
    for sport, day, away, home in key_set:
        twin = (sport, day, home, away)
        if twin in key_set and away < home:  # avoid double counting
            swapped.append(((sport, day, away, home), twin))
    print(f"Standard-market keys with both orientations: {len(swapped)}")
    for ex in swapped[:5]:
        print("  swapped example:", ex)
    print(f"Prop matchup resolved: {prop_resolved}, unresolved: {len(prop_unresolved_examples)}")
    if prop_unresolved_examples:
        remaining_ids = [sid for sid, _, _ in prop_unresolved_examples]
        print(f"Remaining unresolved prop signal_ids: {remaining_ids}")
        print("Unresolved prop samples:")
        for sid, day, urls in prop_unresolved_examples:
            print(f"  signal_id={sid} day_key={day} urls={urls}")
    prop_prefix_bad = len(prop_prefix_bad_examples)
    print(f"[DEBUG] prop_prefix_fixes={prop_prefix_fixes} prop_prefix_bad={prop_prefix_bad}")
    if prop_prefix_bad and debug:
        print("[DEBUG] prop_prefix bad samples (max 10):")
        for ex in prop_prefix_bad_examples:
            print(f"  run_id={ex['run_id']} signal_type={ex['signal_type']} sources={ex['sources_combo']} selection={ex['selection']}")
    return occurrences, {"prop_prefix_fixes": prop_prefix_fixes, "prop_prefix_bad": prop_prefix_bad}, prop_prefix_bad_examples


def pick_latest(occurrences: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    latest: Dict[str, Dict[str, Any]] = {}
    for row in occurrences:
        key = row.get("signal_key")
        if not key:
            continue
        current = latest.get(key)
        ts_new = parse_dt(row.get("observed_at_utc")) or datetime.min
        ts_old = parse_dt(current.get("observed_at_utc")) if current else None
        if current is None or (ts_old is not None and ts_new > ts_old) or ts_old is None:
            latest[key] = row
    return list(latest.values())


def main() -> None:
    parser = argparse.ArgumentParser(description="Build signal ledger")
    parser.add_argument("--debug", action="store_true", help="Enable debug assertions/logging.")
    args = parser.parse_args()
    occurrences, counters, bad_examples = collect_occurrences(debug=args.debug)
    write_jsonl(OCCURRENCES_PATH, occurrences)
    latest_rows = pick_latest(occurrences)
    write_jsonl(LATEST_PATH, latest_rows)
    print(f"Wrote {len(occurrences)} occurrences to {OCCURRENCES_PATH}")
    print(f"Wrote {len(latest_rows)} latest rows to {LATEST_PATH}")
    if counters:
        print(f"[DEBUG] prop_prefix_fixes={counters.get('prop_prefix_fixes',0)} prop_prefix_bad={counters.get('prop_prefix_bad',0)}")
    if counters.get("prop_prefix_bad", 0) > 0 and args.debug:
        print("[DEBUG] prop_prefix bad samples (max 10):")
        for ex in bad_examples[:10]:
            print(f"  run_id={ex.get('run_id')} signal_type={ex.get('signal_type')} sources={ex.get('sources_combo')} selection={ex.get('selection')}")
        raise AssertionError("player_prop selections missing NBA prefix in ledger output")


if __name__ == "__main__":
    main()
