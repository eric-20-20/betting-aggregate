"""
Supabase write client for the betting aggregate pipeline.

Used by push_to_supabase.py after each daily pipeline run.
All functions are idempotent (upsert, not insert).
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("supabase.writer")

REPO_ROOT = Path(__file__).resolve().parent.parent
BATCH_SIZE = 500     # for upsert/insert operations
IN_BATCH_SIZE = 50  # for .in_() filter — SHA256 IDs are 64 chars; 500×64 = 32KB URL → Bad Request

_client = None


def get_client():
    """Return a cached Supabase client."""
    global _client
    if _client is not None:
        return _client

    try:
        from supabase import create_client
    except ImportError:
        raise RuntimeError("supabase package not installed. Run: pip install supabase")

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")

    if not url or not key:
        env_file = REPO_ROOT / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if "=" in line and not line.startswith("#"):
                    k, _, v = line.partition("=")
                    os.environ.setdefault(k.strip(), v.strip())
            url = os.environ.get("SUPABASE_URL")
            key = os.environ.get("SUPABASE_KEY")

    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_KEY not found in env or .env")

    _client = create_client(url, key)
    return _client


def _batched(lst: List, size: int):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def _dedup_ss_rows(rows: List[Dict]) -> List[Dict]:
    """Return rows deduplicated by (signal_id, source_id, expert_slug, line), keeping first."""
    seen: set = set()
    out: List[Dict] = []
    for row in rows:
        key = (
            row.get("signal_id"),
            row.get("source_id") or "",
            row.get("expert_slug") or "",
            row.get("line") if row.get("line") is not None else -1000000,
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _upsert(table: str, rows: List[Dict], conflict_col: str) -> int:
    if not rows:
        return 0
    client = get_client()
    for batch in _batched(rows, BATCH_SIZE):
        client.table(table).upsert(batch, on_conflict=conflict_col).execute()
    return len(rows)


# ──────────────────────────────────────────────
# Expert registry cache
# ──────────────────────────────────────────────

_expert_cache: Dict[str, Optional[int]] = {}


def _ensure_expert(source_id: str, slug: str, display_name: Optional[str] = None,
                   profile_url: Optional[str] = None) -> Optional[int]:
    if not source_id or not slug:
        return None
    key = f"{source_id}|{slug}"
    if key in _expert_cache:
        return _expert_cache[key]

    client = get_client()
    row = {"source_id": source_id, "slug": slug,
           "display_name": display_name or slug, "profile_url": profile_url}
    try:
        resp = client.table("experts").upsert(row, on_conflict="source_id,slug").execute()
        if resp.data:
            _expert_cache[key] = resp.data[0]["id"]
            return resp.data[0]["id"]
        resp2 = client.table("experts").select("id").eq("source_id", source_id).eq("slug", slug).execute()
        if resp2.data:
            _expert_cache[key] = resp2.data[0]["id"]
            return resp2.data[0]["id"]
    except Exception as e:
        logger.warning("Failed to ensure expert %s/%s: %s", source_id, slug, e)

    _expert_cache[key] = None
    return None


# ──────────────────────────────────────────────
# Public write functions
# ──────────────────────────────────────────────

def upsert_signals(signal_records: List[Dict]) -> int:
    """
    Upsert signals from signals_latest.jsonl records.
    Also populates signal_sources junction rows.
    Returns total rows written (signals + signal_sources).
    """
    signal_rows: List[Dict] = []
    ss_rows: List[Dict] = []

    for r in signal_records:
        sid = r.get("signal_id")
        if not sid:
            continue

        sources_present = r.get("sources_present") or r.get("sources", [])

        signal_rows.append({
            "signal_id":       sid,
            "day_key":         r.get("day_key"),
            "event_key":       r.get("event_key") or r.get("canonical_event_key"),
            "sport":           r.get("sport", "NBA"),
            "away_team":       r.get("away_team"),
            "home_team":       r.get("home_team"),
            "market_type":     r.get("market_type"),
            "selection":       r.get("selection"),
            "direction":       r.get("direction"),
            "line":            r.get("line"),
            "line_min":        r.get("line_min"),
            "line_max":        r.get("line_max"),
            "atomic_stat":     r.get("atomic_stat"),
            "player_key":      r.get("player_key") or r.get("player_id"),
            "sources_combo":   r.get("sources_combo"),
            "sources_count":   len(sources_present),
            "signal_type":     r.get("signal_type"),
            "score":           r.get("score"),
            "run_id":          r.get("run_id"),
            "observed_at_utc": r.get("observed_at_utc"),
        })

        for sup in r.get("supports") or []:
            source_id = sup.get("source_id")
            expert_name = sup.get("expert_name")
            expert_slug = None
            expert_id = None
            if expert_name:
                expert_slug = expert_name.lower().replace(" ", "_").replace(".", "")
            if source_id and expert_slug:
                expert_id = _ensure_expert(source_id, expert_slug, display_name=expert_name)

            ss_rows.append({
                "signal_id":    sid,
                "source_id":    source_id,
                "expert_id":    expert_id,
                "expert_slug":  expert_slug,
                "expert_name":  expert_name,
                "line":         sup.get("line"),
                "odds":         None,
                "raw_pick_text": sup.get("raw_pick_text"),
            })

    total = _upsert("signals", signal_rows, "signal_id")

    # Delete existing signal_sources for these signal_ids then re-insert
    if ss_rows:
        ss_rows = _dedup_ss_rows(ss_rows)
        signal_ids = list({r["signal_id"] for r in ss_rows})
        client = get_client()
        for batch in _batched(signal_ids, IN_BATCH_SIZE):
            client.table("signal_sources").delete().in_("signal_id", batch).execute()
        for batch in _batched(ss_rows, BATCH_SIZE):
            client.table("signal_sources").insert(batch).execute()
        total += len(ss_rows)

    return total


def upsert_grades(grade_records: List[Dict]) -> int:
    """
    Upsert grades from grades_latest.jsonl records.

    Also updates expert_result / expert_graded_line on signal_sources rows when
    a grade_record carries a 'source_grades' list (set by grade_signal_sources()
    in grade_signals_nba.py for player_prop signals).
    """
    rows = []
    source_grade_updates: List[Dict] = []  # (signal_id, source_id, expert_slug, expert_result, expert_graded_line)

    for r in grade_records:
        sid = r.get("signal_id")
        if not sid:
            continue
        rows.append({
            "signal_id":     sid,
            "day_key":       r.get("day_key"),
            "result":        r.get("result"),
            "status":        r.get("status"),
            "line":          r.get("line"),
            "odds":          r.get("odds"),
            "stat_value":    r.get("stat_value") or r.get("actual_stat_value"),
            "market_type":   r.get("market_type"),
            "selection":     r.get("selection"),
            "direction":     r.get("direction"),
            "player_key":    r.get("player_key"),
            "provider":      r.get("provider"),
            "graded_at_utc": r.get("graded_at_utc"),
            "notes":         r.get("notes"),
            "units":         r.get("units"),
        })

        # Collect per-source grade updates if present
        for sg in r.get("source_grades") or []:
            if sg.get("expert_result"):
                source_grade_updates.append({
                    "signal_id":          sid,
                    "source_id":          sg.get("source_id"),
                    "expert_slug":        sg.get("expert_slug"),
                    "expert_result":      sg.get("expert_result"),
                    "expert_graded_line": sg.get("expert_graded_line"),
                })

    total = _upsert("grades", rows, "signal_id")

    # Update expert_result / expert_graded_line on matching signal_sources rows
    if source_grade_updates:
        client = get_client()
        for upd in source_grade_updates:
            try:
                q = (
                    client.table("signal_sources")
                    .update({
                        "expert_result":      upd["expert_result"],
                        "expert_graded_line": upd["expert_graded_line"],
                    })
                    .eq("signal_id", upd["signal_id"])
                    .eq("source_id", upd["source_id"])
                )
                if upd.get("expert_slug"):
                    q = q.eq("expert_slug", upd["expert_slug"])
                else:
                    q = q.is_("expert_slug", "null")
                graded_line = upd.get("expert_graded_line")
                if graded_line is not None:
                    q = q.eq("line", graded_line)
                else:
                    q = q.is_("line", "null")
                q.execute()
                total += 1
            except Exception as e:
                logger.warning(
                    "Failed to update expert_result for signal_id=%s source=%s expert=%s: %s",
                    upd["signal_id"], upd["source_id"], upd.get("expert_slug"), e,
                )

    return total


def upsert_plays(date_str: str, play_records: List[Dict]) -> int:
    """Upsert plays from a plays_*.json plays array."""
    rows = []
    for play in play_records:
        sig = play.get("signal", {})
        signal_id = sig.get("signal_id")
        if not signal_id:
            continue

        matched = play.get("matched_pattern")
        pattern_id = matched.get("id") if isinstance(matched, dict) else matched

        rows.append({
            "date":            date_str,
            "signal_id":       signal_id,
            "tier":            play.get("tier"),
            "rank":            play.get("rank"),
            "wilson_score":    play.get("wilson_score"),
            "composite_score": play.get("composite_score"),
            "matched_pattern": pattern_id,
            "summary":         play.get("summary"),
            "factors":         json.dumps(play.get("factors")) if play.get("factors") else None,
            "expert_detail":   json.dumps(play.get("expert_detail")) if play.get("expert_detail") else None,
        })
    return _upsert("plays", rows, "date,signal_id")


def upsert_occurrences(occurrence_records: List[Dict]) -> int:
    """
    Upsert graded_occurrences rows from graded_occurrences_latest.jsonl records.
    Each row is one source-level occurrence with its grade result.
    Returns number of rows written.
    """
    rows: List[Dict] = []
    for r in occurrence_records:
        oid = r.get("occurrence_id")
        if not oid:
            continue
        sources_present = r.get("signal_sources_present") or []
        # Derive day_key from event_key if missing ("NBA:2026:01:23:OKC@ORL" -> "NBA:2026:01:23")
        day_key = r.get("day_key")
        if not day_key:
            ek = r.get("event_key") or ""
            parts = ek.split(":")
            if len(parts) >= 4:
                day_key = ":".join(parts[:4])
        if not day_key:
            continue  # can't store without day_key — skip
        rows.append({
            "occurrence_id":          oid,
            "signal_id":              r.get("signal_id"),
            "day_key":                day_key,
            "event_key":              r.get("event_key"),
            "sport":                  r.get("sport", "NBA"),
            "away_team":              r.get("away_team"),
            "home_team":              r.get("home_team"),
            "market_type":            r.get("market_type"),
            "direction":              r.get("direction"),
            "line":                   r.get("line"),
            "atomic_stat":            r.get("atomic_stat"),
            "player_key":             r.get("player_key"),
            "stat_key":               r.get("stat_key"),
            "occ_source_id":          r.get("occ_source_id"),
            "occ_sources_combo":      r.get("occ_sources_combo"),
            "signal_sources_combo":   r.get("signal_sources_combo") or r.get("sources_combo"),
            "signal_sources_present": sources_present,
            "sources_count":          len(sources_present),
            "expert_name":            r.get("expert_name"),
            "result":                 r.get("result"),
            "units":                  r.get("units"),
            "score":                  r.get("score"),
            "signal_type":            r.get("signal_type"),
            "run_id":                 r.get("run_id"),
            "observed_at_utc":        r.get("observed_at_utc"),
        })
    return _upsert("graded_occurrences", rows, "occurrence_id")


def push_daily_run(signals_file: str, grades_file: str, plays_file: str) -> None:
    """
    Convenience wrapper: push a single day's pipeline output to Supabase.
    Silently no-ops if any file doesn't exist.
    """
    # Signals
    if os.path.exists(signals_file):
        with open(signals_file) as f:
            records = [json.loads(l) for l in f if l.strip()]
        n = upsert_signals(records)
        logger.info("Pushed %d signal rows from %s", n, signals_file)
    else:
        logger.debug("Signals file not found: %s", signals_file)

    # Grades
    if os.path.exists(grades_file):
        with open(grades_file) as f:
            records = [json.loads(l) for l in f if l.strip()]
        n = upsert_grades(records)
        logger.info("Pushed %d grade rows from %s", n, grades_file)
    else:
        logger.debug("Grades file not found: %s", grades_file)

    # Plays
    if os.path.exists(plays_file):
        with open(plays_file) as f:
            data = json.load(f)
        date_str = data.get("meta", {}).get("date") or Path(plays_file).stem.replace("plays_", "")
        n = upsert_plays(date_str, data.get("plays", []))
        logger.info("Pushed %d play rows from %s", n, plays_file)
    else:
        logger.debug("Plays file not found: %s", plays_file)
