import argparse
import json
import os
import secrets
import subprocess
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

# Tracking output locations:
# - Snapshots remain in out/ (or --out-dir), e.g., consensus_nba_v1.json.
# - data/runs/<run_id>/ holds per-run JSONL logs and run_meta.json.
# - data/latest/ mirrors snapshot JSONs for quick reads.

from mappings import atomic_stats as mapping_atomic_stats, normalize_stat_key
from store import append_jsonl, copy_file, ensure_dir, sha256_json, write_json

DEFAULT_INPUT_FILES = [
    "normalized_action_nba.json",
    "normalized_covers_nba.json",
    "normalized_sportscapping_nba.json",
    "normalized_betql_spread_nba.json",
    "normalized_betql_total_nba.json",
]

HARD_DEBUG_META: Dict[str, Any] = {}

def ensure_out_dir(out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)


def load_records(out_dir: str) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for fname in DEFAULT_INPUT_FILES:
        path = os.path.join(out_dir, fname)
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    records.extend(data)
        except json.JSONDecodeError:
            continue
    return records


def load_raw_records(out_dir: str) -> List[Dict[str, Any]]:
    raw_paths = ["raw_action_nba.json", "raw_covers_nba.json"]
    raw: List[Dict[str, Any]] = []
    for fname in raw_paths:
        path = os.path.join(out_dir, fname)
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    raw.extend(data)
        except json.JSONDecodeError:
            continue
    return raw


def day_key(event_key: str | None) -> str | None:
    if not event_key or not isinstance(event_key, str):
        return None
    parts = event_key.split(":")
    if len(parts) >= 4:
        return ":".join(parts[:4])
    if len(parts) >= 3:
        return ":".join(parts[:3])
    return None


def expert_identity(prov: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    source_id = prov.get("source_id")
    if not source_id:
        return None
    if prov.get("expert_profile"):
        return (source_id, prov["expert_profile"])
    if prov.get("expert_slug"):
        return (source_id, prov["expert_slug"])
    if prov.get("expert_handle"):
        return (source_id, prov["expert_handle"])
    if prov.get("expert_name"):
        return (source_id, prov["expert_name"])
    if prov.get("raw_fingerprint"):
        return (source_id, prov["raw_fingerprint"])
    return (source_id, "unknown")


def compute_expert_id(prov: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    ident = expert_identity(prov)
    if ident:
        return ident
    src = prov.get("source_id")
    if src and prov.get("canonical_url"):
        return (src, prov["canonical_url"])
    if src:
        return (src, "unknown")
    return None


def sort_strength_key(item: Dict[str, Any]) -> Tuple[int, int, int]:
    return (
        -item.get("source_strength", 0),
        -item.get("expert_strength", 0),
        -item.get("count_total", 0),
    )


def clamp_score(val: float) -> int:
    return int(max(0, min(100, round(val))))


def compute_score(source_strength: int, expert_strength: int, count_total: int, penalty: float = 0) -> int:
    base = 20
    base += 25 * max(0, source_strength - 1)
    base += 15 * max(0, expert_strength - 1)
    base += 5 * max(0, count_total - expert_strength)
    base -= penalty
    return clamp_score(base)


def normalize_team_selection(selection: str | None) -> Optional[str]:
    if not selection or not isinstance(selection, str):
        return None
    return selection.lower().replace(" ", "_")


def normalize_player_id(player_id: Optional[str]) -> Optional[str]:
    if not player_id or not isinstance(player_id, str):
        return None
    pid = player_id.lower()
    if pid.startswith("nba:"):
        pid = pid.split(":", 1)[1]
    for suffix in ["_total_projection", "_projection", "_and"]:
        if pid.endswith(suffix):
            pid = pid[: -len(suffix)]
    tokens = [t for t in pid.replace("-", "_").split("_") if t]
    # strip trailing generational suffixes (jr/sr/ii/iii/iv/v)
    suffix_tokens = {"jr", "sr", "ii", "iii", "iv", "v"}
    while tokens and tokens[-1] in suffix_tokens:
        tokens.pop()
    if not tokens:
        return None
    if len(tokens) == 1:
        # single token: treat as last name only
        last = tokens[0]
        first_initial = last[0] if last else ""
    else:
        first = tokens[0]
        last = tokens[-1]
        first_initial = first[0] if first else ""
    if not first_initial or not tokens[-1]:
        return pid
    return f"{first_initial}_{tokens[-1]}"


def _format_line_cluster(lines: List[float], kind: str) -> str:
    if not lines:
        return ""
    uniq = sorted(set(round(float(l), 2) for l in lines))
    def fmt(val: float) -> str:
        return f"{val:+.1f}" if kind == "spread" else f"{val:.1f}"

    disp = "/".join(fmt(l) for l in uniq)
    if len(uniq) == 1:
        return disp
    median = uniq[len(uniq) // 2] if len(uniq) % 2 else (uniq[len(uniq) // 2 - 1] + uniq[len(uniq) // 2]) / 2
    return f"{disp} (median={fmt(median)} range=[{fmt(uniq[0])},{fmt(uniq[-1])}])"


def _format_spread_selection(team: str, lines: List[float]) -> str:
    return f"{team} {_format_line_cluster(lines, 'spread')}"


def _format_total_selection(direction: str, lines: List[float]) -> str:
    dir_up = direction.upper() if isinstance(direction, str) else direction
    return f"{dir_up} {_format_line_cluster(lines, 'total')}"


def dedupe_signals(signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for sig in signals:
        sig_type = sig.get("signal_type")
        if sig_type in {"avoid_conflict", "conflict"}:
            key = (
                sig_type,
                sig.get("day_key"),
                sig.get("market_type"),
                sig.get("selection"),
                tuple(sorted(sig.get("selections", []) or [])),
                tuple(sorted(sig.get("sources", []) or [])),
            )
        else:
            key = (
                sig_type,
                sig.get("day_key"),
                sig.get("market_type"),
                sig.get("selection"),
                tuple(sorted(sig.get("selections", []) or [])),
                tuple(sorted(sig.get("sources", []) or [])),
                sig.get("player_id"),
                sig.get("atomic_stat"),
                sig.get("direction"),
                sig.get("line_median"),
            )
        if key in seen:
            # merge richer metadata into existing entry
            for existing in deduped:
                existing_key = (
                    existing.get("signal_type"),
                    existing.get("day_key"),
                    existing.get("market_type"),
                    existing.get("selection"),
                    tuple(sorted(existing.get("selections", []) or [])),
                    tuple(sorted(existing.get("sources", []) or [])),
                ) if sig_type in {"avoid_conflict", "conflict"} else (
                    existing.get("signal_type"),
                    existing.get("day_key"),
                    existing.get("market_type"),
                    existing.get("selection"),
                    tuple(sorted(existing.get("selections", []) or [])),
                    tuple(sorted(existing.get("sources", []) or [])),
                    existing.get("player_id"),
                    existing.get("atomic_stat"),
                    existing.get("direction"),
                    existing.get("line_median"),
                )
                if existing_key == key:
                    for field in ["lines_by_selection", "experts_by_selection", "experts_total", "sample_urls"]:
                        if field in sig and sig.get(field) and not existing.get(field):
                            existing[field] = sig[field]
                    break
            continue
        seen.add(key)
        deduped.append(sig)
    return deduped


def soft_conflict_rows(soft_groups: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    conflicts: List[Dict[str, Any]] = []
    standard = [g for g in soft_groups if g.get("market_type") in {"spread", "moneyline", "total"}]
    bucket: Dict[Tuple[Any, Any], List[Dict[str, Any]]] = {}
    for g in standard:
        key = (g.get("matchup_key") or g.get("event_key"), g.get("market_type"))
        bucket.setdefault(key, []).append(g)
    for key, groups in bucket.items():
        sels = {}
        for g in groups:
            sel = g.get("selection")
            if sel is None:
                continue
            sels.setdefault(sel, []).append(g)
        if len(sels) < 2:
            continue
        sources_union = set()
        experts_by_selection: Dict[str, Set[str]] = {}
        lines_by_selection: Dict[str, List[float]] = {}
        for sel, glist in sels.items():
            experts_set: Set[str] = set()
            for g in glist:
                sources_union.update(g.get("sources") or [])
                for exp in g.get("experts_raw") or []:
                    experts_set.add(str(exp))
                if g.get("lines"):
                    lines_by_selection.setdefault(sel, []).extend(g.get("lines"))
            experts_by_selection[sel] = experts_set
        conflicts.append(
            {
                "signal_type": "avoid_conflict",
                "event_key": key[0],
                "market_type": key[1],
                "selection": f"{key[1]}_conflict",
                "selections": sorted(sels.keys()),
                "sources": sorted(sources_union),
                "experts_by_selection": {k: sorted(v) for k, v in experts_by_selection.items()},
                "lines_by_selection": {k: sorted(set(v)) for k, v in lines_by_selection.items()},
                "experts_total": sum(len(v) for v in experts_by_selection.values()),
                "count_total": sum(g.get("count_total", 0) for g in groups),
            }
        )
    return conflicts


def _best_odds(odds_list: List[Any]) -> Optional[Any]:
    best = None
    best_decimal = -1.0
    for o in odds_list:
        try:
            val = float(o)
        except (TypeError, ValueError):
            continue
        decimal = 1 + val / 100 if val > 0 else 1 - 100 / val if val < 0 else 1.0
        if decimal > best_decimal:
            best_decimal = decimal
            best = o
    return best


def _parse_team_from_selection(selection: Optional[str], event: Dict[str, Any]) -> Optional[str]:
    if not selection or not isinstance(selection, str):
        return None
    token = selection.split("_")[0].split("::")[0]
    token = token.upper()
    away_val = (event or {}).get("away_team")
    home_val = (event or {}).get("home_team")
    away = str(away_val).upper() if away_val else None
    home = str(home_val).upper() if home_val else None
    for t in (token, token.replace("TEAM", ""), token.replace("ML", ""), token.replace("SPREAD", "")):
        t_norm = t.strip().upper()
        if t_norm and t_norm in {away, home}:
            return t_norm
    return token if len(token) in (2, 3, 4) else None


def _parse_direction(side: Optional[str], selection: Optional[str]) -> Optional[str]:
    for val in (side, selection):
        if not val or not isinstance(val, str):
            continue
        low = val.lower()
        if "over" in low:
            return "OVER"
        if "under" in low:
            return "UNDER"
    return None


def _normalize_matchup_key(event: Dict[str, Any]) -> Optional[str]:
    ev_key = event.get("event_key")
    dk = day_key(ev_key)
    away = event.get("away_team")
    home = event.get("home_team")
    matchup_key = event.get("matchup_key")
    if not matchup_key and dk and away and home:
        t1, t2 = sorted([str(away).upper(), str(home).upper()])
        matchup_key = f"{dk}:{t1}-{t2}"
    return matchup_key or ev_key


def _bucket_line(line: Any) -> Optional[float]:
    try:
        return round(float(line) * 2) / 2
    except (TypeError, ValueError):
        return None


def _canonical_hard_key(event: Dict[str, Any], market: Dict[str, Any], provenance: Dict[str, Any]) -> Optional[Tuple[Tuple[Any, ...], Optional[str], Optional[float]]]:
    market_type = market.get("market_type")
    if market_type not in {"moneyline", "spread", "total"}:
        return None

    ev_key = event.get("event_key")
    dk = day_key(ev_key)
    away = event.get("away_team")
    home = event.get("home_team")
    matchup_key = event.get("matchup_key")
    if not matchup_key and dk and away and home:
        t1, t2 = sorted([str(away).upper(), str(home).upper()])
        matchup_key = f"{dk}:{t1}-{t2}"
    if not matchup_key:
        matchup_key = ev_key
    if not (dk and away and home and matchup_key):
        return None

    away_u = str(away).upper()
    home_u = str(home).upper()

    selection = market.get("selection")
    side = market.get("side")
    line = market.get("line")

    canonical_selection = None
    line_bucket = None
    core_key: Optional[Tuple[Any, ...]] = None
    bet_kind = (provenance or {}).get("source_surface")
    if bet_kind and isinstance(bet_kind, str) and bet_kind.startswith("betql_"):
        bet_kind = "model" if "model" in bet_kind else "probet" if "probet" in bet_kind else None

    if market_type == "moneyline":
        team = _parse_team_from_selection(selection, {"away_team": away_u, "home_team": home_u}) or _parse_team_from_selection(side, {"away_team": away_u, "home_team": home_u})
        if not team:
            return None
        canonical_selection = team.upper()
        core_key = (matchup_key, "moneyline", canonical_selection, bet_kind)
    elif market_type == "spread":
        line_bucket = _bucket_line(line)
        team = _parse_team_from_selection(selection, {"away_team": away_u, "home_team": home_u}) or _parse_team_from_selection(side, {"away_team": away_u, "home_team": home_u})
        if not team:
            return None
        canonical_selection = team.upper()
        core_key = (matchup_key, "spread", canonical_selection, bet_kind)
    elif market_type == "total":
        line_bucket = _bucket_line(line)
        direction = _parse_direction(side, selection)
        if not direction:
            return None
        canonical_selection = "GAME_TOTAL"
        core_key = (matchup_key, "total", direction, bet_kind)

    if not core_key:
        return None
    return core_key, canonical_selection, line_bucket


def group_hard(records: List[Dict[str, Any]], debug: bool = False) -> List[Dict[str, Any]]:
    groups: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    prop_groups: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}
    near_miss_props: List[Dict[str, Any]] = []
    for rec in records:
        if not rec.get("eligible_for_consensus"):
            continue
        event = rec.get("event") or {}
        market = rec.get("market") or {}
        prov = rec.get("provenance") or {}

        # Non-player markets
        parsed = _canonical_hard_key(event, market, prov)
        if parsed:
            core_key, canonical_selection, lb = parsed
            market_type = market.get("market_type")
            selection = market.get("selection")
            side = market.get("side")
            line = market.get("line")
            odds = market.get("odds")
            dk = day_key(event.get("event_key"))
            away = event.get("away_team")
            home = event.get("home_team")
            group_matchup = core_key[0] if core_key else (event.get("matchup_key") or f"{str(away).upper()}@{str(home).upper()}")
            group = groups.setdefault(
                core_key,
                {
                    "day_key": dk,
                    "event_key": event.get("event_key"),
                    "matchup": group_matchup,
                    "home_team": home,
                    "away_team": away,
                    "market_type": market_type,
                    "selection": selection,
                    "side": side,
                    "canonical_selection": canonical_selection,
                    "line_bucket": lb,
                    "lines": [],
                    "odds_samples": [],
                    "sources_set": set(),
                    "experts_set": set(),
                    "odds_list": [],
                    "sample_urls": [],
                    "count_total": 0,
                },
            )
            group["count_total"] += 1
            if line is not None:
                group["lines"].append(line)
            if odds is not None:
                group["odds_samples"].append(odds)
            source_id = prov.get("source_id")
            if source_id:
                group["sources_set"].add(source_id)
            expert = expert_identity(prov)
            if expert:
                group["experts_set"].add(expert)
            url = prov.get("canonical_url")
            if url and len(group["sample_urls"]) < 3 and url not in group["sample_urls"]:
                group["sample_urls"].append(url)
            continue

        # Player prop markets for hard consensus
        if (market.get("market_type") != "player_prop"):
            continue
        parsed_sel = parse_selection_from_record(rec)
        if not parsed_sel:
            continue
        player_id, stat_key, direction = parsed_sel
        dk = day_key((event or {}).get("event_key"))
        away = (event or {}).get("away_team")
        home = (event or {}).get("home_team")
        if not (dk and direction):
            continue
        matchup = f"{str(away).upper()}@{str(home).upper()}" if away and home else None
        stat_key = normalize_stat_key(stat_key)
        lines_val = market.get("line")
        odds_val = market.get("odds")
        source_id = prov.get("source_id")
        expert = expert_identity(prov)
        url = prov.get("canonical_url")
        for atomic in mapping_atomic_stats(stat_key):
            if atomic not in ALLOWED_ATOMIC:
                continue
            identity = (dk, player_id, atomic, direction)
            prop_groups.setdefault(identity, []).append(
                {
                    "line": lines_val,
                    "odds": odds_val,
                    "source_id": source_id,
                    "expert": expert,
                    "url": url,
                    "matchup": matchup,
                }
            )

    results: List[Dict[str, Any]] = []
    for core, group in groups.items():
        source_strength = len(group["sources_set"])
        expert_strength = len(group["experts_set"])
        lines = group.get("lines", [])
        line_median = None
        line_min = None
        line_max = None
        if lines:
            lines_sorted = sorted(lines)
            n = len(lines_sorted)
            line_median = lines_sorted[n // 2] if n % 2 else (lines_sorted[n // 2 - 1] + lines_sorted[n // 2]) / 2
            line_min = lines_sorted[0]
            line_max = lines_sorted[-1]
        odds_list = group.get("odds_samples", [])
        best_odds = _best_odds(odds_list)
        results.append(
            {
                "canonical_key": core,
                "day_key": group["day_key"],
                "event_key": group["event_key"],
                "matchup": group.get("matchup"),
                "home_team": group.get("home_team"),
                "away_team": group.get("away_team"),
                "market_type": group["market_type"],
                "selection": group["selection"],
                "side": group["side"],
                "canonical_selection": group.get("canonical_selection"),
                "line_bucket": group.get("line_bucket"),
                "lines": lines,
                "line_median": line_median,
                "line_min": line_min,
                "line_max": line_max,
                "odds": odds_list,
                "odds_list": odds_list,
                "best_odds": best_odds,
                "sources": sorted(group["sources_set"]),
                "source_strength": source_strength,
                "expert_strength": expert_strength,
                "experts_raw": sorted(f"{src}:{val}" for (src, val) in group["experts_set"] if src and val),
                "count_total": group["count_total"],
                "sample_urls": group["sample_urls"],
            }
        )

    # Player prop hard consensus clustering (line tolerance <= 1.0)
    for identity, entries in prop_groups.items():
        dk, player_id, atomic_stat, direction = identity
        # sort by line
        valid_entries = [e for e in entries if e.get("line") is not None]
        if not valid_entries:
            continue
        valid_entries.sort(key=lambda e: float(e["line"]))
        clusters: List[List[Dict[str, Any]]] = []
        current: List[Dict[str, Any]] = []
        cluster_min = None
        for ent in valid_entries:
            line_val = float(ent["line"])
            if current and cluster_min is not None and line_val - cluster_min > 1.0:
                clusters.append(current)
                current = []
                cluster_min = None
            if cluster_min is None:
                cluster_min = line_val
            current.append(ent)
        if current:
            clusters.append(current)

        made_cluster = False
        for cl in clusters:
            sources_set = {e.get("source_id") for e in cl if e.get("source_id")}
            if len(sources_set) < 2:
                continue
            made_cluster = True
            matchup = None
            for e in cl:
                if e.get("matchup"):
                    matchup = e.get("matchup")
                    break
            lines = sorted({float(e["line"]) for e in cl})
            line_min = lines[0]
            line_max = lines[-1]
            n = len(lines)
            line_median = lines[n // 2] if n % 2 else (lines[n // 2 - 1] + lines[n // 2]) / 2
            odds_list = [e.get("odds") for e in cl if e.get("odds") is not None]
            best_odds = _best_odds(odds_list)
            sample_urls: List[str] = []
            for e in cl:
                url = e.get("url")
                if url and url not in sample_urls and len(sample_urls) < 3:
                    sample_urls.append(url)
            experts_set = {e.get("expert") for e in cl if e.get("expert")}
            lines_by_source: Dict[str, List[float]] = {}
            for e in cl:
                src = e.get("source_id")
                if not src or e.get("line") is None:
                    continue
                lines_by_source.setdefault(src, []).append(float(e["line"]))
            results.append(
                {
                    "canonical_key": identity,
                    "day_key": dk,
                    "matchup": matchup,
                    "market_type": "player_prop",
                    "player_id": player_id,
                    "atomic_stat": atomic_stat,
                    "direction": direction,
                    "canonical_selection": f"{player_id}::{atomic_stat}::{direction}" if player_id and atomic_stat and direction else f"{player_id}::{atomic_stat}",
                    "line_bucket": None,
                    "lines": lines,
                    "line_median": line_median,
                    "line_min": line_min,
                    "line_max": line_max,
                    "odds": odds_list,
                    "odds_list": odds_list,
                    "best_odds": best_odds,
                    "lines_by_source": lines_by_source,
                    "sources": sorted(sources_set),
                    "source_strength": len(sources_set),
                    "expert_strength": len(experts_set),
                    "experts_raw": sorted(f"{src}:{val}" for (src, val) in experts_set if src and val),
                    "count_total": len(cl),
                    "sample_urls": sample_urls,
                }
            )
        if not made_cluster:
            sources_total = {e.get("source_id") for e in valid_entries if e.get("source_id")}
            if len(sources_total) >= 2:
                lines_all = sorted({float(e["line"]) for e in valid_entries})
                near_miss_props.append(
                    {
                        "identity": identity,
                        "sources": sorted(sources_total),
                        "lines": lines_all,
                        "spread": max(lines_all) - min(lines_all) if lines_all else None,
                    }
                )
    HARD_DEBUG_META["prop_groups"] = prop_groups
    HARD_DEBUG_META["near_miss_props"] = near_miss_props
    results.sort(key=sort_strength_key)
    return results


ALLOWED_ATOMIC = {"points", "rebounds", "assists", "threes_made"}


def parse_selection_from_record(rec: Dict[str, Any]) -> Optional[Tuple[str, str, str]]:
    market = rec.get("market") or {}
    selection = market.get("selection")
    side = market.get("side")
    player_id = market.get("player_key")
    stat_key = market.get("stat_key")
    direction = None

    if player_id and isinstance(player_id, str) and player_id.startswith("NBA:"):
        player_id = player_id.split(":", 1)[1]

    if selection and "::" in selection:
        parts = selection.split("::")
        if len(parts) >= 3:
            player_id = player_id or parts[0]
            stat_key = stat_key or parts[1]
            direction = parts[2]

    if not direction and side:
        side_lower = side.lower()
        if "over" in side_lower:
            direction = "OVER"
        elif "under" in side_lower:
            direction = "UNDER"
        else:
            direction = side

    if stat_key:
        stat_key = normalize_stat_key(stat_key)

    if player_id and stat_key and direction:
        canonical_player = normalize_player_id(player_id)
        return canonical_player or player_id, stat_key, direction
    return None


def build_atomic_signals(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    signals: List[Dict[str, Any]] = []
    for rec in records:
        if not rec.get("eligible_for_consensus"):
            continue
        market = rec.get("market") or {}
        if market.get("market_type") != "player_prop":
            continue
        event = rec.get("event") or {}
        prov = rec.get("provenance") or {}
        dk = day_key(event.get("event_key"))
        parsed = parse_selection_from_record(rec)
        if not (dk and parsed):
            continue
        player_id, stat_key, direction = parsed
        display_player_id = (market.get("player_key") or market.get("selection") or player_id)
        matchup = None
        if event.get("away_team") and event.get("home_team"):
            matchup = f"{str(event['away_team']).upper()}@{str(event['home_team']).upper()}"
        stat_key = normalize_stat_key(stat_key)
        for atomic in mapping_atomic_stats(stat_key):
            if atomic not in ALLOWED_ATOMIC:
                continue
            signals.append(
                {
                    "day_key": dk,
                    "player_id": player_id,
                    "atomic_stat": atomic,
                    "direction": direction,
                    "source_id": prov.get("source_id"),
                    "expert": compute_expert_id(prov),
                    "expert_name": prov.get("expert_name"),
                    "expert_handle": prov.get("expert_handle"),
                    "display_player_id": display_player_id,
                    "matchup": matchup,
                    "sample_url": prov.get("canonical_url"),
                    "stat_key": stat_key,
                    "line": market.get("line"),
                    "selection": market.get("selection"),
                }
            )
    return signals


def group_soft(records: List[Dict[str, Any]], hard_identities: Optional[Set[Tuple[Any, ...]]] = None, debug_soft_tokens: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    signals = build_atomic_signals(records)
    groups: Dict[Tuple[Any, Any, Any, Any], Dict[str, Any]] = {}
    standard_groups: Dict[Tuple[Any, ...], Dict[str, Any]] = {}

    def append_standard_signal(rec: Dict[str, Any]) -> None:
        event = rec.get("event") or {}
        market = rec.get("market") or {}
        prov = rec.get("provenance") or {}
        mk_type = market.get("market_type")
        if mk_type not in {"spread", "moneyline", "total"}:
            return
        dk = day_key(event.get("event_key"))
        away = event.get("away_team")
        home = event.get("home_team")
        if not (dk and away and home):
            return
        matchup = f"{str(away).upper()}@{str(home).upper()}"
        selection = market.get("selection")
        side = market.get("side")
        line = market.get("line")
        team = _parse_team_from_selection(selection, {"away_team": away, "home_team": home}) or _parse_team_from_selection(
            side, {"away_team": away, "home_team": home}
        )
        direction = _parse_direction(side, selection)
        if mk_type == "moneyline":
            if not team:
                return
            key = (dk, matchup, "moneyline", team.upper())
            base_sel = team.upper()
        elif mk_type == "spread":
            if line is None or not team:
                return
            key = (dk, matchup, "spread", team.upper())
            base_sel = team.upper()
        else:  # total
            if line is None or not direction:
                return
            key = (dk, matchup, "total", direction)
            base_sel = direction
        grp = standard_groups.setdefault(
            key,
            {
                "day_key": dk,
                "matchup": matchup,
                "market_type": mk_type,
                "selection": base_sel,
                "direction": direction,
                "lines": [],
                "sources_set": set(),
                "experts_set": set(),
                "sample_urls": [],
                "count_total": 0,
                "records": [],
            },
        )
        grp["count_total"] += 1
        if line is not None:
            grp["lines"].append(line)
        src = prov.get("source_id")
        if src:
            grp["sources_set"].add(src)
        expert = compute_expert_id(prov)
        if expert:
            grp["experts_set"].add(expert)
        url = prov.get("canonical_url")
        if url and len(grp["sample_urls"]) < 3 and url not in grp["sample_urls"]:
            grp["sample_urls"].append(url)
        grp["records"].append({"rec": rec, "expert_id": expert, "source_id": src})

    for rec in records:
        if rec.get("eligible_for_consensus"):
            append_standard_signal(rec)
    for sig in signals:
        key = (sig["day_key"], sig["player_id"], sig["atomic_stat"], sig["direction"])
        group = groups.setdefault(
            key,
            {
                "day_key": sig["day_key"],
                "player_id": sig["player_id"],
                "atomic_stat": sig["atomic_stat"],
                "direction": sig["direction"],
                "matchup": sig.get("matchup"),
                "sources_set": set(),
                "experts_set": set(),
                "expert_names_set": set(),
                "sample_urls": [],
                "count_total": 0,
                "supports": [],
            },
        )
        group["count_total"] += 1
        source_id = sig.get("source_id")
        if source_id:
            group["sources_set"].add(source_id)
        expert = sig.get("expert")
        if expert:
            group["experts_set"].add(expert)
        url = sig.get("sample_url")
        if url and len(group["sample_urls"]) < 3 and url not in group["sample_urls"]:
            group["sample_urls"].append(url)
        if sig.get("expert_name"):
            group["expert_names_set"].add(sig["expert_name"])
        group["supports"].append(
            {
                "stat_key": sig.get("stat_key"),
                "line": sig.get("line"),
                "source_id": source_id,
                "selection": sig.get("selection"),
                "display_player_id": sig.get("display_player_id"),
                "expert_name": sig.get("expert_name"),
                "expert_handle": sig.get("expert_handle"),
                "canonical_url": sig.get("sample_url"),
                "direction": sig.get("direction"),
            }
        )

    results: List[Dict[str, Any]] = []
    for group in groups.values():
        source_strength = len(group["sources_set"])
        expert_strength = len(group["experts_set"])
        lines = [float(l) for l in (sup.get("line") for sup in group["supports"]) if isinstance(l, (int, float))]
        line_median = None
        line_min = None
        line_max = None
        if lines:
            lines_sorted = sorted(lines)
            n = len(lines_sorted)
            line_median = lines_sorted[n // 2] if n % 2 == 1 else (lines_sorted[n // 2 - 1] + lines_sorted[n // 2]) / 2
            line_min = lines_sorted[0]
            line_max = lines_sorted[-1]
        if source_strength != 1:
            continue
        if not (expert_strength >= 2 or group["count_total"] >= 2):
            continue
        identity = (
            group["day_key"],
            group["player_id"],
            group["atomic_stat"],
            group["direction"],
            line_median,
        )
        if hard_identities and identity in hard_identities:
            continue
        results.append(
            {
                "day_key": group["day_key"],
                "player_id": group["player_id"],
                "atomic_stat": group["atomic_stat"],
                "direction": group["direction"],
                "matchup": group.get("matchup"),
                "sources": sorted(group["sources_set"]),
                "source_strength": source_strength,
                "expert_strength": expert_strength,
                "experts_raw": sorted(f"{src}:{val}" for (src, val) in group["experts_set"] if src and val),
                "expert_names": sorted(group["expert_names_set"]),
                "count_total": group["count_total"],
                "sample_urls": group["sample_urls"],
                "supports": group["supports"],
                "line_median": line_median,
                "line_min": line_min,
                "line_max": line_max,
            }
        )

    # Standard (non-prop) soft consensus within single source, clustered by line tolerance
    def cluster_lines(lines_list: List[float], tolerance: float) -> List[List[float]]:
        if not lines_list:
            return []
        sorted_lines = sorted(lines_list)
        clusters: List[List[float]] = []
        cur: List[float] = []
        start = None
        for ln in sorted_lines:
            if start is None or ln - start <= tolerance:
                if start is None:
                    start = ln
                cur.append(ln)
            else:
                clusters.append(cur)
                cur = [ln]
                start = ln
        if cur:
            clusters.append(cur)
        return clusters

    debug_info: List[str] = []

    def matches_debug(grp_key: Tuple[Any, ...]) -> bool:
        if not debug_soft_tokens:
            return False
        joined = " ".join(str(x) for x in grp_key if x).lower()
        return all(tok.lower() in joined for tok in debug_soft_tokens)

    conflict_ids_local: Set[Tuple[Any, Any]] = set()
    # detect conflicts within standard groups (different selections same day+market_type)
    sels_bucket: Dict[Tuple[Any, Any], Set[Any]] = {}
    for key, grp in standard_groups.items():
        dk = grp.get("day_key")
        mkt = grp.get("market_type")
        sels_bucket.setdefault((dk, mkt), set()).add(grp.get("selection"))
    for key, sels in sels_bucket.items():
        if len(sels) >= 2:
            conflict_ids_local.add(key)

    for key, grp in standard_groups.items():
        source_strength = len(grp["sources_set"])
        expert_strength = len(grp["experts_set"])
        if source_strength != 1:
            continue
        if not (expert_strength >= 2 or grp["count_total"] >= 2):
            continue
        tolerance = 1.0 if grp["market_type"] == "spread" else 1.0 if grp["market_type"] == "total" else 0.0
        line_clusters = cluster_lines([float(l) for l in grp.get("lines", []) if isinstance(l, (int, float))], tolerance)
        for cl in line_clusters or [[]]:
            if grp["market_type"] != "moneyline" and not cl:
                continue
            line_min = min(cl) if cl else None
            line_max = max(cl) if cl else None
            line_median = None
            if cl:
                n = len(cl)
                line_median = cl[n // 2] if n % 2 else (cl[n // 2 - 1] + cl[n // 2]) / 2
            identity = (
                grp["day_key"],
                grp["matchup"],
                grp["market_type"],
                grp["selection"],
                line_median,
            )
            if hard_identities and identity in hard_identities:
                continue
            if (grp.get("day_key"), grp.get("market_type")) in conflict_ids_local:
                if matches_debug(key):
                    print(f"[DEBUG soft-market] SUPPRESS due to conflict_id={(grp.get('day_key'), grp.get('market_type'))} selection={grp.get('selection')} lines={cl}")
                continue
            if matches_debug(key):
                debug_info.append(
                    f"[DEBUG soft-market] key={key} cluster={cl} expert_strength={expert_strength} count_total={grp['count_total']}"
                )
            results.append(
                {
                    "day_key": grp["day_key"],
                    "matchup": grp["matchup"],
                    "market_type": grp["market_type"],
                    "selection": grp["selection"],
                    "direction": grp.get("direction"),
                    "sources": sorted(grp["sources_set"]),
                    "source_strength": source_strength,
                    "expert_strength": expert_strength,
                    "experts_raw": sorted(f"{src}:{val}" for (src, val) in grp["experts_set"] if src and val),
                    "expert_names": [],
                    "count_total": grp["count_total"],
                    "sample_urls": grp["sample_urls"],
                    "supports": [],
                    "line_median": line_median,
                    "line_min": line_min,
                    "line_max": line_max,
                    "lines": sorted(set(cl)),
                }
            )

    if debug_info:
        for line in debug_info:
            print(line)
    results.sort(key=sort_strength_key)
    return results


def group_within_source(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    signals = build_atomic_signals(records)
    groups: Dict[Tuple[Any, Any, Any, Any, Any], Dict[str, Any]] = {}
    for sig in signals:
        source_id = sig.get("source_id")
        if not source_id:
            continue
        key = (sig["day_key"], sig["player_id"], sig["atomic_stat"], sig["direction"], source_id)
        group = groups.setdefault(
            key,
            {
                "day_key": sig["day_key"],
                "player_id": sig["player_id"],
                "atomic_stat": sig["atomic_stat"],
                "direction": sig["direction"],
                "source_id": source_id,
                "source": source_id,
                "experts_set": set(),
                "expert_names_set": set(),
                "sample_urls": [],
                "count_total": 0,
                "supports": [],
            },
        )
        group["count_total"] += 1
        expert = sig.get("expert")
        if expert:
            group["experts_set"].add(expert)
        url = sig.get("sample_url")
        if url and len(group["sample_urls"]) < 3 and url not in group["sample_urls"]:
            group["sample_urls"].append(url)
        if sig.get("expert_name"):
            group["expert_names_set"].add(sig["expert_name"])
        group["supports"].append(
            {
                "stat_key": sig.get("stat_key"),
                "line": sig.get("line"),
                "selection": sig.get("selection"),
                "display_player_id": sig.get("display_player_id"),
                "expert_name": sig.get("expert_name"),
                "expert_handle": sig.get("expert_handle"),
                "canonical_url": sig.get("sample_url"),
                "direction": sig.get("direction"),
            }
        )

    results: List[Dict[str, Any]] = []
    for group in groups.values():
        expert_strength = len(group["experts_set"])
        sources = [group["source_id"]] if group.get("source_id") else []
        results.append(
            {
                "day_key": group["day_key"],
                "player_id": group["player_id"],
                "atomic_stat": group["atomic_stat"],
                "direction": group["direction"],
                "source_id": group["source_id"],
                "source": group["source_id"],
                "sources": sources,
                "source_strength": len(sources) if sources else 0,
                "expert_strength": expert_strength,
                "experts_raw": sorted(f"{src}:{val}" for (src, val) in group["experts_set"] if src and val),
                "expert_names": sorted(group["expert_names_set"]),
                "count_total": group["count_total"],
                "sample_urls": group["sample_urls"],
                "supports": group["supports"],
            }
        )
    results.sort(key=lambda r: (-r.get("expert_strength", 0), -r.get("count_total", 0)))
    return results


def eligible_counts(records: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for rec in records:
        if not rec.get("eligible_for_consensus"):
            continue
        prov = rec.get("provenance") or {}
        src = prov.get("source_id", "unknown")
        counts[src] = counts.get(src, 0) + 1
    return counts


def print_section(title: str, rows: List[Dict[str, Any]], row_formatter) -> None:
    print(title)
    if not rows:
        print("No results.")
        return
    for idx, item in enumerate(rows, 1):
        print(f"{idx}. {row_formatter(item)}")


def format_hard_row(item: Dict[str, Any]) -> str:
    line_part = ""
    if item.get("line_median") is not None:
        line_part = f" line={item.get('line_median')} [{item.get('line_min')},{item.get('line_max')}]"
    if item.get("line_bucket") is not None:
        line_part += f" bucket={item.get('line_bucket')}"
    return (
        f"{item.get('day_key')} {item.get('canonical_selection') or item.get('selection')} {item.get('side') or item.get('direction')}"
        f"{line_part} src_strength={item.get('source_strength')} exp_strength={item.get('expert_strength')} "
        f"count={item.get('count_total')} best_odds={item.get('best_odds')} sources={','.join(item.get('sources', []))} "
        f"sample_urls={item.get('sample_urls', [])}"
    )


def format_soft_row(item: Dict[str, Any]) -> str:
    mkt = item.get("market_type")
    if mkt in {"spread", "moneyline", "total"}:
        if mkt == "spread":
            subject = _format_spread_selection(item.get("selection"), item.get("lines") or [])
        elif mkt == "total":
            subject = _format_total_selection(item.get("selection") or item.get("direction"), item.get("lines") or [])
        else:  # moneyline
            subject = f"{item.get('selection')} ML"
        return (
            f"{item.get('day_key')} {subject} "
            f"exp_strength={item.get('expert_strength')} count={item.get('count_total')} "
            f"sources={','.join(item.get('sources', []))}"
        )
    subject = f"{item.get('player_id')}::{item.get('atomic_stat')}::{item.get('direction')}"
    return (
        f"{item.get('day_key')} {subject} "
        f"exp_strength={item.get('expert_strength')} count={item.get('count_total')} "
        f"sources={','.join(item.get('sources', []))} sample_urls={item.get('sample_urls', [])}"
    )


def format_within_row(item: Dict[str, Any]) -> str:
    return (
        f"{item.get('day_key')} {item.get('player_id')}::{item.get('atomic_stat')}::{item.get('direction')} "
        f"source={item.get('source')} exp_strength={item.get('expert_strength')} "
        f"count={item.get('count_total')} sample_urls={item.get('sample_urls', [])}"
    )


def format_signal_row(item: Dict[str, Any]) -> str:
    subject = item.get("player_id") or item.get("selection")
    detail = f"{item.get('atomic_stat')}" if item.get("atomic_stat") else item.get("market_type")
    line_str = ""
    if item.get("line_median") is not None:
        line_str = f" line={item.get('line_median')} [{item.get('line_min')},{item.get('line_max')}]"
    return (
        f"{item.get('day_key')} {subject} {detail} {item.get('direction')} "
        f"score={item.get('score')} src={item.get('source_strength')} "
        f"exp={item.get('expert_strength')} "
        f"ae={item.get('atomic_evidence_count', 0)} ce={item.get('combo_evidence_count', 0)} "
        f"we={item.get('weighted_expert_strength', item.get('expert_strength'))} "
        f"count={item.get('count_total')} "
        f"sources={','.join(item.get('sources', []))}{line_str}"
    )


def format_avoid_row(item: Dict[str, Any]) -> str:
    return (
        f"{item.get('day_key')} {item.get('player_id')}::{item.get('atomic_stat')} CONFLICT "
        f"over(exp={item.get('over_expert_strength')}|count={item.get('over_count_total')}) "
        f"under(exp={item.get('under_expert_strength')}|count={item.get('under_count_total')}) "
        f"sources={','.join(item.get('sources', []))} "
        f"lines_over={item.get('over_lines', [])} lines_under={item.get('under_lines', [])}"
    )


def format_conflict_row(item: Dict[str, Any]) -> str:
    key_disp = item.get("matchup_key") or item.get("matchup") or item.get("event_key") or item.get("day_key") or "N/A"
    if item.get("market_type") == "player_prop":
        subject = f"{item.get('player_id')}::{item.get('atomic_stat')}"
        return (
            f"{key_disp} {subject} CONFLICT selections={item.get('selections', [])} "
            f"sources={','.join(item.get('sources', []))} count={item.get('count_total')}"
        )
    mkt = item.get("market_type")
    selections = item.get("selections", [])
    experts_by_selection = item.get("experts_by_selection") or {}
    lines_by_selection = item.get("lines_by_selection") or {}
    parts = []
    for sel in selections:
        lines = lines_by_selection.get(sel) or []
        if mkt == "spread":
            parts.append(_format_spread_selection(sel, lines))
        elif mkt == "total":
            parts.append(_format_total_selection(sel, lines))
        else:
            parts.append(f"{sel} ML")
        if experts_by_selection.get(sel):
            parts[-1] += f" (experts={len(experts_by_selection[sel])})"
    parts_joined = " vs ".join(parts) if parts else selections
    return (
        f"{key_disp} {mkt} CONFLICT: {parts_joined} "
        f"sources={','.join(item.get('sources', []))} count={item.get('count_total')}"
    )


def format_standard_hard_row(item: Dict[str, Any]) -> str:
    key_disp = item.get("matchup") or item.get("matchup_key") or item.get("event_key") or "N/A"
    sel = item.get("canonical_selection") or item.get("selection") or item.get("side")
    urls = item.get("sample_urls", [])
    return (
        f"{key_disp} {item.get('market_type')} HARD selection={sel} "
        f"sources={','.join(item.get('sources', []))} urls={urls}"
    )


def format_standard_conflict_row(item: Dict[str, Any]) -> str:
    key_disp = item.get("matchup_key") or item.get("matchup") or item.get("event_key") or "N/A"
    sels = item.get("selections", [])
    return (
        f"{key_disp} {item.get('market_type')} CONFLICT: {sels} "
        f"sources={','.join(item.get('sources', []))} count={item.get('count_total')}"
    )


def compute_line_stats(supports: List[Dict[str, Any]], atomic_stat: Optional[str], direction: Optional[str]) -> Tuple[Optional[float], Optional[float], Optional[float], List[float]]:
    lines: List[float] = []
    lines_atomic: List[float] = []
    lines_combo: List[float] = []
    for sup in supports:
        if atomic_stat and sup.get("atomic_stat") and sup.get("atomic_stat") != atomic_stat:
            continue
        if direction and sup.get("direction") and sup.get("direction") != direction:
            continue
        line_val = sup.get("line")
        if line_val is None:
            continue
        try:
            fval = float(line_val)
            lines.append(fval)
            if sup.get("evidence_type") == "atomic":
                lines_atomic.append(fval)
            elif sup.get("evidence_type") == "combo":
                lines_combo.append(fval)
        except (TypeError, ValueError):
            continue
    if not lines:
        return None, None, None, [], [], []
    lines_sorted = sorted(lines)
    n = len(lines_sorted)
    median = lines_sorted[n // 2] if n % 2 == 1 else (lines_sorted[n // 2 - 1] + lines_sorted[n // 2]) / 2
    return (
        median,
        lines_sorted[0],
        lines_sorted[-1],
        sorted(set(lines_sorted)),
        sorted(set(lines_atomic)),
        sorted(set(lines_combo)),
    )


def build_supports_from_group(group: Dict[str, Any]) -> List[Dict[str, Any]]:
    supports: List[Dict[str, Any]] = []
    sample_urls = group.get("sample_urls") or []
    atomic_stat = group.get("atomic_stat")
    existing = group.get("supports") or []
    for sup in existing:
        evidence_type = None
        sup_stat_key = sup.get("stat_key")
        sup_atomic = sup.get("atomic_stat") or atomic_stat
        if sup_atomic and sup_stat_key:
            evidence_type = "atomic" if normalize_stat_key(str(sup_stat_key)) == normalize_stat_key(str(sup_atomic)) else "combo"
        supports.append(
            {
                "source_id": sup.get("source_id") or group.get("source_id"),
                "expert_name": sup.get("expert_name"),
                "expert_handle": sup.get("expert_handle"),
                "selection": sup.get("selection") or group.get("selection"),
                "display_player_id": sup.get("display_player_id") or group.get("player_id"),
                "stat_key": sup.get("stat_key") or group.get("stat_key") or group.get("atomic_stat"),
                "atomic_stat": sup.get("atomic_stat") or group.get("atomic_stat"),
                "direction": sup.get("direction") or group.get("direction") or group.get("side"),
                "line": sup.get("line") if sup.get("line") is not None else group.get("line"),
                "canonical_url": sup.get("canonical_url") or (sample_urls[0] if sample_urls else None),
                "evidence_type": evidence_type,
            }
        )
    if supports:
        return supports
    sources = group.get("sources") or ([group.get("source_id")] if group.get("source_id") else [])
    direction = group.get("direction") or group.get("side")
    if sources:
        for idx, src in enumerate(sources):
            supports.append(
                {
                    "source_id": src,
                    "selection": group.get("selection"),
                    "display_player_id": group.get("player_id"),
                    "stat_key": group.get("stat_key") or group.get("atomic_stat"),
                    "atomic_stat": group.get("atomic_stat"),
                    "direction": direction,
                    "line": group.get("line"),
                    "canonical_url": sample_urls[idx] if idx < len(sample_urls) else (sample_urls[0] if sample_urls else None),
                }
            )
    elif sample_urls:
        supports.append(
            {
                "source_id": None,
                "selection": group.get("selection"),
                "display_player_id": group.get("player_id"),
                "stat_key": group.get("stat_key") or group.get("atomic_stat"),
                "atomic_stat": group.get("atomic_stat"),
                "direction": direction,
                "line": group.get("line"),
                "canonical_url": sample_urls[0],
            }
        )
    return supports


def derive_experts(group: Dict[str, Any]) -> List[str]:
    names: Set[str] = set()
    for nm in group.get("expert_names") or []:
        if nm:
            names.add(str(nm))
    for sup in group.get("supports") or []:
        nm = sup.get("expert_name")
        if nm:
            names.add(str(nm))
        handle = sup.get("expert_handle")
        if handle:
            names.add(str(handle))
    if names:
        return sorted(names)
    raw: Set[str] = set()
    for ident in group.get("experts_raw") or []:
        if ident:
            raw.add(str(ident))
    if raw:
        return sorted(raw)
    sources = group.get("sources") or ([group.get("source_id")] if group.get("source_id") else [])
    return sorted({str(src) for src in sources if src})


def build_unified_signals(
    hard_groups: List[Dict[str, Any]],
    soft_groups: List[Dict[str, Any]],
    within_groups: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    unified: List[Dict[str, Any]] = []
    candidates: List[Dict[str, Any]] = []

    def selection_fields(player_id: Optional[str], atomic_stat: Optional[str], direction: Optional[str], supports: List[Dict[str, Any]]) -> Tuple[str, Optional[str], Optional[str]]:
        sel_default = None
        if player_id and atomic_stat and direction:
            sel_default = f"{player_id}::{atomic_stat}::{direction}"
        selection_example = None
        atomic_supports = []
        for sup in supports:
            sup_atomic = sup.get("atomic_stat")
            sup_dir = sup.get("direction")
            if sup_atomic == atomic_stat and (not direction or sup_dir == direction) and sup.get("evidence_type") != "combo":
                atomic_supports.append(sup)
        if atomic_supports:
            selection_example = atomic_supports[0].get("selection")
            selection_source = "atomic"
        elif supports:
            selection_example = supports[0].get("selection")
            selection_source = "combo"
        else:
            selection_source = None
        selection_value = selection_example or sel_default or (supports[0].get("selection") if supports else None) or sel_default
        return selection_value, selection_source, selection_example

    def compute_evidence_counts(supports: List[Dict[str, Any]], atomic_stat: Optional[str]) -> Tuple[int, int, float]:
        atomic_ids: Set[str] = set()
        combo_ids: Set[str] = set()
        for sup in supports:
            if atomic_stat and sup.get("atomic_stat") != atomic_stat:
                continue
            evid = sup.get("evidence_type")
            expert_id = sup.get("expert_handle") or sup.get("expert_name") or f"{sup.get('source_id')}:{sup.get('selection')}"
            if evid == "atomic":
                atomic_ids.add(expert_id)
            elif evid == "combo":
                combo_ids.add(expert_id)
        atomic_ct = len(atomic_ids)
        combo_ct = len(combo_ids)
        weighted = atomic_ct * 1.0 + combo_ct * 0.65
        return atomic_ct, combo_ct, weighted

    def selection_fields(player_id: Optional[str], atomic_stat: Optional[str], direction: Optional[str], supports: List[Dict[str, Any]]) -> Tuple[str, Optional[str], Optional[str]]:
        sel_default = None
        if player_id and atomic_stat and direction:
            sel_default = f"{player_id}::{atomic_stat}::{direction}"
        atomic_supports = [
            sup for sup in supports if sup.get("atomic_stat") == atomic_stat and sup.get("evidence_type") != "combo"
        ]
        selection_example = None
        if atomic_supports:
            selection_example = atomic_supports[0].get("selection")
            selection_source = "atomic"
        elif supports:
            selection_example = supports[0].get("selection")
            selection_source = "combo"
        else:
            selection_source = None
        selection_value = selection_example or sel_default or (supports[0].get("selection") if supports else None) or sel_default
        return selection_value, selection_source, selection_example

    def compute_evidence_counts(supports: List[Dict[str, Any]], atomic_stat: Optional[str]) -> Tuple[int, int, float]:
        atomic_ids: Set[str] = set()
        combo_ids: Set[str] = set()
        for sup in supports:
            if atomic_stat and sup.get("atomic_stat") != atomic_stat:
                continue
            evid = sup.get("evidence_type")
            expert_id = sup.get("expert_handle") or sup.get("expert_name") or f"{sup.get('source_id')}:{sup.get('selection')}"
            if evid == "atomic":
                atomic_ids.add(expert_id)
            elif evid == "combo":
                combo_ids.add(expert_id)
        atomic_ct = len(atomic_ids)
        combo_ct = len(combo_ids)
        weighted = atomic_ct * 1.0 + combo_ct * 0.65
        return atomic_ct, combo_ct, weighted

    # Hard signals (non-atomic)
    for group in hard_groups:
        if group.get("source_strength", 0) < 2:
            continue
        if group.get("market_type") == "player_prop":
            player_id = group.get("player_id")
            atomic_stat = group.get("atomic_stat")
            direction = group.get("direction") or group.get("side")
            selection_val = (
                group.get("canonical_selection")
                or (f"{player_id}::{atomic_stat}::{direction}" if player_id and atomic_stat and direction else group.get("selection"))
            )
            selection_source = "hard_prop"
            selection_example = selection_val
            line_median = group.get("line_median")
            line_min = group.get("line_min")
            line_max = group.get("line_max")
            lines = group.get("lines") or []
            odds_list = group.get("odds_list") or group.get("odds") or []
            best_odds = group.get("best_odds")
        else:
            player_id, _, _ = parse_selection_from_record(
                {
                    "market": {"selection": group.get("selection"), "stat_key": group.get("stat_key"), "side": group.get("side")},
                    "provenance": {},
                    "event": {"event_key": group.get("event_key")},
                }
            ) or (None, None, None)
            selection_val, selection_source, selection_example = selection_fields(player_id, None, group.get("side"), [])
            line_median = group.get("line")
            line_min = group.get("line")
            line_max = group.get("line")
            lines = [group.get("line")] if group.get("line") is not None else []
            odds_list = group.get("odds_list") or group.get("odds") or []
            best_odds = group.get("best_odds")
        sources = group.get("sources") or []
        source_strength = group.get("source_strength", len(sources))
        expert_strength = group.get("expert_strength", 0)
        count_total = group.get("count_total", 0)
        score = compute_score(source_strength, expert_strength, count_total)
        supports = build_supports_from_group(group)
        experts = derive_experts(group)
        candidates.append(
            {
                "signal_type": "hard_cross_source",
                "day_key": group.get("day_key"),
                "event_key": group.get("event_key"),
                "market_type": group.get("market_type"),
                "player_id": player_id,
                "selection": selection_val or group.get("selection"),
                "selection_source": selection_source,
                "selection_example": selection_example or group.get("selection"),
                "atomic_stat": atomic_stat if group.get("market_type") == "player_prop" else None,
                "direction": direction if group.get("market_type") == "player_prop" else group.get("side"),
                "line": line_median,
                "line_median": line_median,
                "line_min": line_min,
                "line_max": line_max,
                "lines": lines,
                "score": score,
                "source_strength": source_strength,
                "expert_strength": expert_strength,
                "count_total": count_total,
                "sources": sources,
                "experts": experts,
                "supports": supports,
                "odds": odds_list,
                "odds_list": odds_list,
                "best_odds": best_odds,
            }
        )

    def build_atomic_candidate(group: Dict[str, Any], signal_type: str) -> Optional[Dict[str, Any]]:
        sources = group.get("sources") or ([group.get("source_id")] if group.get("source_id") else [])
        source_strength = group.get("source_strength", len(sources) if sources else 0)
        expert_strength = group.get("expert_strength", 0)
        count_total = group.get("count_total", 0)
        supports = build_supports_from_group(group)
        line_median, line_min, line_max, lines, lines_atomic, lines_combo = compute_line_stats(
            supports, group.get("atomic_stat"), group.get("direction")
        )
        penalty = 0
        if line_min is not None and line_max is not None:
            penalty = min(10, max(0.0, line_max - line_min))
        atomic_ct, combo_ct, weighted_expert_strength = compute_evidence_counts(supports, group.get("atomic_stat"))
        score = compute_score(source_strength, weighted_expert_strength, count_total, penalty=penalty)
        experts = derive_experts(group)
        selection_val, selection_source, selection_example = selection_fields(
            group.get("player_id"), group.get("atomic_stat"), group.get("direction"), supports
        )
        return {
            "signal_type": signal_type,
            "day_key": group.get("day_key"),
            "event_key": None,
            "market_type": "player_prop",
            "player_id": group.get("player_id"),
            "selection": selection_val,
            "selection_source": selection_source,
            "selection_example": selection_example,
            "atomic_stat": group.get("atomic_stat"),
            "direction": group.get("direction"),
            "line": line_median,
            "line_median": line_median,
            "line_min": line_min,
            "line_max": line_max,
            "lines": lines,
            "lines_atomic": lines_atomic,
            "lines_combo": lines_combo,
            "score": score,
            "source_strength": source_strength,
            "expert_strength": expert_strength,
            "atomic_evidence_count": atomic_ct,
            "combo_evidence_count": combo_ct,
            "weighted_expert_strength": weighted_expert_strength,
            "count_total": count_total,
            "sources": sources,
            "experts": experts,
            "supports": supports,
        }

    for group in soft_groups:
        if group.get("market_type") in {"spread", "moneyline", "total"}:
            if group.get("expert_strength", 0) < 2 and group.get("count_total", 0) < 2:
                continue
            sources = group.get("sources") or []
            source_strength = group.get("source_strength", len(sources))
            expert_strength = group.get("expert_strength", 0)
            count_total = group.get("count_total", 0)
            lines = group.get("lines") or []
            line_median = group.get("line_median")
            line_min = group.get("line_min")
            line_max = group.get("line_max")
            score = compute_score(source_strength, expert_strength, count_total)
            candidates.append(
                {
                    "signal_type": "soft_standard",
                    "day_key": group.get("day_key"),
                    "event_key": None,
                    "market_type": group.get("market_type"),
                    "matchup": group.get("matchup"),
                    "selection": group.get("selection"),
                    "direction": group.get("direction"),
                    "line": line_median,
                    "line_median": line_median,
                    "line_min": line_min,
                    "line_max": line_max,
                    "lines": lines,
                    "score": score,
                    "source_strength": source_strength,
                    "expert_strength": expert_strength,
                    "count_total": count_total,
                    "sources": sources,
                    "experts": derive_experts(group),
                    "supports": group.get("supports") or [],
                }
            )
        else:
            if group.get("expert_strength", 0) < 2 and group.get("count_total", 0) < 2:
                continue
            sig = build_atomic_candidate(group, "soft_atomic")
            if sig:
                candidates.append(sig)

    for group in within_groups:
        if group.get("expert_strength", 0) < 2:
            continue
        sig = build_atomic_candidate(group, "within_source")
        if sig:
            candidates.append(sig)

    # Bucket by stat for conflict detection
    buckets: Dict[Tuple[Any, Any, Any, Any], Dict[str, List[Dict[str, Any]]]] = {}
    final_signals: List[Dict[str, Any]] = []
    for sig in candidates:
        if sig.get("signal_type") not in {"soft_atomic", "within_source"}:
            final_signals.append(sig)
            continue
        if not sig.get("atomic_stat") or sig.get("direction") not in {"OVER", "UNDER"}:
            final_signals.append(sig)
            continue
        key = (sig.get("day_key"), sig.get("market_type"), sig.get("player_id"), sig.get("atomic_stat"))
        bucket = buckets.setdefault(key, {"OVER": [], "UNDER": []})
        bucket[sig.get("direction")].append(sig)

    for key, dirs in buckets.items():
        over_sigs = dirs.get("OVER") or []
        under_sigs = dirs.get("UNDER") or []
        if over_sigs and under_sigs:
            supports_over = [sup for sig in over_sigs for sup in sig.get("supports", [])]
            supports_under = [sup for sig in under_sigs for sup in sig.get("supports", [])]
            experts_over = set([exp for sig in over_sigs for exp in sig.get("experts", [])])
            experts_under = set([exp for sig in under_sigs for exp in sig.get("experts", [])])
            sources_over = set([src for sig in over_sigs for src in sig.get("sources", [])])
            sources_under = set([src for sig in under_sigs for src in sig.get("sources", [])])
            lines_over = sorted({sup.get("line") for sup in supports_over if sup.get("line") is not None})
            lines_under = sorted({sup.get("line") for sup in supports_under if sup.get("line") is not None})
            sample_urls = []
            for sup in supports_over + supports_under:
                url = sup.get("canonical_url")
                if url and url not in sample_urls and len(sample_urls) < 3:
                    sample_urls.append(url)
            unified.append(
                {
                    "signal_type": "avoid_conflict",
                    "day_key": key[0],
                    "event_key": None,
                    "market_type": "player_prop",
                    "player_id": key[2],
                    "selection": f"{key[2]}::{key[3]}::CONFLICT" if key[2] and key[3] else "CONFLICT",
                    "selection_source": "conflict",
                    "selection_example": None,
                    "atomic_stat": key[3],
                    "direction": "CONFLICT",
                    "line": None,
                    "lines": [],
                    "lines_over": lines_over,
                    "lines_under": lines_under,
                    "score": max([sig.get("score", 0) for sig in over_sigs + under_sigs]),
                    "source_strength": len(sources_over | sources_under),
                    "expert_strength": len(experts_over | experts_under),
                    "over_expert_strength": len(experts_over),
                    "under_expert_strength": len(experts_under),
                    "over_count_total": len(supports_over),
                    "under_count_total": len(supports_under),
                    "sources": sorted(sources_over | sources_under),
                    "experts": sorted(experts_over | experts_under),
                    "supports": supports_over + supports_under,
                    "conflict": {
                        "over": {"supports": supports_over, "experts": sorted(experts_over), "sources": sorted(sources_over)},
                        "under": {"supports": supports_under, "experts": sorted(experts_under), "sources": sorted(sources_under)},
                    },
                    "over_lines": lines_over,
                    "under_lines": lines_under,
                    "sample_urls": sample_urls,
                }
            )
        else:
            final_signals.extend(over_sigs or under_sigs)

    unified.extend(final_signals)
    unified.sort(key=lambda s: s.get("score", 0), reverse=True)
    return unified


def detect_conflicts(
    hard_groups: List[Dict[str, Any]],
    soft_groups: List[Dict[str, Any]],
    within_groups: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    conflicts: List[Dict[str, Any]] = []

    # moneyline / spread / total from hard groups
    bucket_mls: Dict[Tuple[str, str], Dict[str, Dict[str, Any]]] = {}
    bucket_spread: Dict[Tuple[str, str], Dict[str, Dict[str, Any]]] = {}
    bucket_total: Dict[Tuple[str, str], Dict[str, Dict[str, Any]]] = {}
    for g in hard_groups:
        if g.get("market_type") == "moneyline":
            key = (g.get("matchup") or g.get("event_key"), "moneyline")
            bucket_mls.setdefault(key, {})[g.get("selection")] = g
        elif g.get("market_type") == "spread":
            key = (g.get("matchup") or g.get("event_key"), "spread")
            bucket_spread.setdefault(key, {})[g.get("selection")] = g
        elif g.get("market_type") == "total":
            key = (g.get("matchup") or g.get("event_key"), "total")
            bucket_total.setdefault(key, {})[g.get("side")] = g

    for key, sel_map in bucket_mls.items():
        if len(sel_map) >= 2:
            groups = list(sel_map.values())
            sources = sorted(set(s for g in groups for s in g.get("sources", [])))
            conflicts.append(
                {
                    "signal_type": "avoid_conflict",
                    "event_key": key[0],
                    "matchup_key": key[0],
                    "market_type": "moneyline",
                    "selection": "moneyline_conflict",
                    "selections": list(sel_map.keys()),
                    "sources": sources,
                    "count_total": sum(g.get("count_total", 0) for g in groups),
                }
            )
    for key, sel_map in bucket_spread.items():
        groups = list(sel_map.values())
        sources = sorted(set(s for g in groups for s in g.get("sources", [])))
        if len(sel_map) >= 2 and len(sources) >= 2:
            conflicts.append(
                {
                    "signal_type": "avoid_conflict",
                    "event_key": key[0],
                    "matchup_key": key[0],
                    "market_type": "spread",
                    "selection": "spread_conflict",
                    "selections": list(sel_map.keys()),
                    "sources": sources,
                    "count_total": sum(g.get("count_total", 0) for g in groups),
                }
            )
            print(f"[DEBUG] spread group => AVOID (conflict) matchup_key={key[0]} selections={list(sel_map.keys())} sources={sources}")
        elif len(sel_map) == 1 and len(sources) >= 2:
            sel_only = next(iter(sel_map.keys()))
            print(f"[DEBUG] spread group => HARD matchup_key={key[0]} selection={sel_only} sources={sources} count={sum(g.get('count_total',0) for g in groups)}")
    for (evk, _), side_map in bucket_total.items():
        if side_map.get("over") and side_map.get("under"):
            groups = [side_map["over"], side_map["under"]]
            sources = sorted(set(s for g in groups for s in g.get("sources", [])))
            conflicts.append(
                {
                    "signal_type": "avoid_conflict",
                    "event_key": evk,
                    "matchup_key": evk,
                    "market_type": "total",
                    "selection": "total_conflict",
                    "selections": ["over", "under"],
                    "sources": sources,
                    "count_total": sum(g.get("count_total", 0) for g in groups),
                }
            )

    # player props handled in build_unified (with supports), so skip here to avoid duplicates
    return conflicts


def build_standard_directional_consensus(records: List[Dict[str, Any]], debug: bool = False) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Cross-source directional consensus for standard markets ignoring lines."""
    allowed_markets = {"spread", "total", "moneyline"}
    votes: Dict[Tuple[str, str], Dict[str, Set[str]]] = {}
    lines_by_key_sel: Dict[Tuple[str, str, str], List[float]] = {}
    meta: Dict[Tuple[str, str], Dict[str, Any]] = {}
    sample_urls: Dict[Tuple[str, str], Set[str]] = {}

    for rec in records:
        if not rec.get("eligible_for_consensus"):
            continue
        prov = rec.get("provenance") or {}
        src = prov.get("source_id")
        if not src:
            continue
        event = rec.get("event") or {}
        market = rec.get("market") or {}
        mkt = market.get("market_type")
        if mkt not in allowed_markets:
            continue

        matchup_key = _normalize_matchup_key(event)
        event_key = event.get("event_key")
        group_key = matchup_key or event_key
        if not group_key:
            continue

        selection_norm: Optional[str] = None
        if mkt == "total":
            selection_norm = _parse_direction(market.get("side"), market.get("selection"))
        else:
            selection_norm = _parse_team_from_selection(market.get("selection"), event) or _parse_team_from_selection(market.get("side"), event)
        if not selection_norm:
            continue
        selection_norm = selection_norm.upper()
        line_val = None
        try:
            if market.get("line") is not None:
                line_val = float(market.get("line"))
        except (TypeError, ValueError):
            line_val = None

        key = (group_key, mkt)
        votes.setdefault(key, {}).setdefault(src, set()).add(selection_norm)
        if line_val is not None:
            lines_by_key_sel.setdefault((group_key, mkt, selection_norm), []).append(line_val)
        if key not in meta:
            meta[key] = {
                "day_key": day_key(event_key),
                "event_key": event_key,
                "matchup": group_key,
                "away_team": event.get("away_team"),
                "home_team": event.get("home_team"),
            }
        sample_urls.setdefault(key, set())
        url = prov.get("canonical_url")
        if url and len(sample_urls[key]) < 3:
            sample_urls[key].add(url)

    hard_results: List[Dict[str, Any]] = []
    avoid_results: List[Dict[str, Any]] = []

    for key, src_map in votes.items():
        group_key, mkt = key
        sources_with_votes = [s for s, sels in src_map.items() if sels]
        if len(sources_with_votes) < 2:
            continue
        unique_sels: Set[str] = set()
        for sels in src_map.values():
            unique_sels.update(sels)

        # line tolerance check for agreeing selection
        line_min = line_max = line_range = None
        tol = 2.0 if mkt == "total" else 1.5 if mkt == "spread" else None
        if len(unique_sels) == 1:
            sel_only = next(iter(unique_sels))
            lines = lines_by_key_sel.get((group_key, mkt, sel_only), [])
            if lines:
                line_min = min(lines)
                line_max = max(lines)
                line_range = line_max - line_min

        if mkt == "spread" and debug:
            if len(unique_sels) > 1:
                print("spread groups where sources>=2 but selection count>1 => avoid")
            else:
                print("spread groups where sources>=2 and selection count==1 => hard")
        if mkt == "total" and debug:
            if len(unique_sels) > 1:
                print("total groups where sources>=2 but selection count>1 => avoid")
            else:
                print("total groups where sources>=2 and selection count==1 => hard")
        if mkt == "moneyline" and debug:
            if len(unique_sels) > 1:
                print("moneyline groups where sources>=2 but selection count>1 => avoid")
            else:
                print("moneyline groups where sources>=2 and selection count==1 => hard")

        count_total = sum(len(v) for v in src_map.values())
        meta_row = meta.get(key, {})
        if len(unique_sels) == 1 and (tol is None or line_range is None or line_range <= tol):
            sel_only = next(iter(unique_sels))
            hard_results.append(
                {
                    "canonical_key": ("STD_DIR", group_key, mkt, sel_only),
                    "day_key": meta_row.get("day_key"),
                    "event_key": meta_row.get("event_key") or group_key,
                    "matchup": meta_row.get("matchup") or group_key,
                    "home_team": meta_row.get("home_team"),
                    "away_team": meta_row.get("away_team"),
                    "market_type": mkt,
                    "selection": "GAME_TOTAL" if mkt == "total" else sel_only,
                    "side": sel_only if mkt == "total" else sel_only,
                    "canonical_selection": sel_only,
                    "line_bucket": None,
                    "lines": [],
                    "line_median": None,
                    "line_min": line_min,
                    "line_max": line_max,
                    "best_odds": None,
                    "sources": sorted(sources_with_votes),
                    "sources_set": set(sources_with_votes),
                    "experts_set": set(),
                    "count_total": count_total,
                    "source_strength": len(sources_with_votes),
                    "expert_strength": 0,
                    "sample_urls": list(sample_urls.get(key, []))[:3],
                }
            )
        else:
            avoid_results.append(
                {
                    "signal_type": "avoid_conflict",
                    "event_key": meta_row.get("event_key") or group_key,
                    "matchup_key": meta_row.get("matchup") or group_key,
                    "market_type": mkt,
                    "selection": f"{mkt}_conflict",
                    "selections": sorted(unique_sels),
                    "sources": sorted(sources_with_votes),
                    "count_total": count_total,
                }
            )

    return hard_results, avoid_results


def build_avoid_signals(signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    directional: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    for sig in signals:
        day = sig.get("day_key")
        player_id = sig.get("player_id")
        atomic_stat = sig.get("atomic_stat")
        direction = sig.get("direction")
        if not (day and player_id and atomic_stat and direction in {"OVER", "UNDER"}):
            continue
        key = (day, player_id, atomic_stat, direction)
        entry = directional.setdefault(
            key,
            {
                "day_key": day,
                "player_id": player_id,
                "atomic_stat": atomic_stat,
                "direction": direction,
                "sources": set(),
                "experts": set(),
                "supports": [],
                "count_total": 0,
                "event_key": sig.get("event_key"),
            },
        )
        entry["sources"].update(sig.get("sources") or [])
        entry["experts"].update(sig.get("experts") or [])
        entry["supports"].extend(sig.get("supports") or [])
        entry["count_total"] += sig.get("count_total", 0)

    for entry in directional.values():
        entry["source_strength"] = len(entry["sources"])
        entry["expert_strength"] = len(entry["experts"])
        entry["score"] = compute_score(entry["source_strength"], entry["expert_strength"], entry["count_total"])

    grouped: Dict[Tuple[str, str, str], Dict[str, Dict[str, Any]]] = {}
    for key, entry in directional.items():
        dk, pid, atomic_stat, direction = key
        grouped.setdefault((dk, pid, atomic_stat), {})[direction] = entry

    avoid_signals: List[Dict[str, Any]] = []
    for (dk, pid, atomic_stat), dir_map in grouped.items():
        over = dir_map.get("OVER")
        under = dir_map.get("UNDER")
        if not (over and under):
            continue
        over_ok = over["expert_strength"] >= 2 or over["score"] >= 60
        under_ok = under["expert_strength"] >= 2 or under["score"] >= 60
        if not (over_ok and under_ok):
            continue
        sources_union = set(over["sources"]) | set(under["sources"])
        experts_union = set(over["experts"]) | set(under["experts"])
        avoid_signals.append(
            {
                "signal_type": "avoid_conflict",
                "day_key": dk,
                "event_key": over.get("event_key") or under.get("event_key"),
                "market_type": "player_prop",
                "player_id": pid,
                "selection": f"{pid}::{atomic_stat}::CONFLICT",
                "atomic_stat": atomic_stat,
                "direction": "CONFLICT",
                "line": None,
                "score": max(over["score"], under["score"]),
                "source_strength": len(sources_union),
                "expert_strength": len(experts_union),
                "count_total": over["count_total"] + under["count_total"],
                "sources": sorted(sources_union),
                "experts": sorted(experts_union),
                "supports": over["supports"] + under["supports"],
                "over_supports": over["supports"],
                "under_supports": under["supports"],
                "over_score": over["score"],
                "under_score": under["score"],
                "sources_over": sorted(over["sources"]),
                "sources_under": sorted(under["sources"]),
                "experts_over": sorted(over["experts"]),
                "experts_under": sorted(under["experts"]),
            }
        )
    return avoid_signals


def compute_expert_id_value(prov: Dict[str, Any]) -> Optional[str]:
    ident = expert_identity(prov)
    if ident:
        return f"{ident[0]}:{ident[1]}"
    src = prov.get("source_id")
    if src and prov.get("canonical_url"):
        return f"{src}:{prov.get('canonical_url')}"
    if src and prov.get("raw_fingerprint"):
        return f"{src}:{prov.get('raw_fingerprint')}"
    return None


def compute_evidence_id(rec: Dict[str, Any]) -> Optional[str]:
    prov = rec.get("provenance") or {}
    event = rec.get("event") or {}
    market = rec.get("market") or {}
    player_id = normalize_player_id(market.get("player_key")) if market.get("player_key") else None
    components = {
        "source_id": prov.get("source_id"),
        "source_surface": prov.get("source_surface"),
        "day_key": day_key(event.get("event_key")) or event.get("day_key"),
        "event_key": event.get("event_key"),
        "market_type": market.get("market_type"),
        "selection": market.get("selection"),
        "side": market.get("side"),
        "stat_key": market.get("stat_key"),
        "atomic_stat": market.get("atomic_stat"),
        "player_id": player_id,
        "direction": _parse_direction(market.get("side"), market.get("selection")),
        "line": _bucket_line(market.get("line")),
        "odds": market.get("odds"),
        "expert_id": compute_expert_id_value(prov),
    }
    if not components.get("source_id"):
        return None
    return sha256_json(components)


def compute_signal_id(sig: Dict[str, Any]) -> Optional[str]:
    sig_type = sig.get("signal_type")
    if not sig_type:
        return None
    key = {
        "signal_type": sig_type,
        "day_key": sig.get("day_key"),
        "market_type": sig.get("market_type"),
        "selection": sig.get("selection"),
        "player_id": sig.get("player_id"),
        "atomic_stat": sig.get("atomic_stat"),
        "direction": sig.get("direction"),
    }
    if sig_type in {"avoid_conflict", "conflict"}:
        sels = sig.get("selections") or []
        key["selections"] = sorted(sels)
    return sha256_json(key)


def default_run_id() -> str:
    now = datetime.now(timezone.utc)
    suffix = secrets.token_hex(3)
    return now.strftime("%Y-%m-%dT%H-%M-%SZ") + f"-{suffix}"


def write_snapshot_latest(out_dir: str, data_dir: str) -> None:
    latest_dir = os.path.join(data_dir, "latest")
    ensure_dir(latest_dir)
    for fname in os.listdir(out_dir):
        src = os.path.join(out_dir, fname)
        dst = os.path.join(latest_dir, fname)
        if os.path.isfile(src):
            copy_file(src, dst)


def run_git_commit() -> Optional[str]:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=os.getcwd(), stderr=subprocess.DEVNULL).decode().strip()
    except Exception:
        return None


def append_and_fsync(path: str, records: Iterable[Dict[str, Any]]) -> None:
    append_jsonl(path, records)


def main(
    debug: bool = False,
    find_prop: Optional[str] = None,
    find_market: Optional[str] = None,
    debug_soft_tokens: Optional[List[str]] = None,
    track: bool = True,
    run_id: Optional[str] = None,
    data_dir: str = "data",
    out_dir: str = "out",
) -> None:
    records = load_records(out_dir)
    raw_records = load_raw_records(out_dir)
    eligible = [r for r in records if r.get("eligible_for_consensus")]

    for rec in records:
        prov = rec.get("provenance") or {}
        prov["expert_id"] = compute_expert_id_value(prov)
        rec["provenance"] = prov
        evid = compute_evidence_id(rec)
        if evid:
            rec["evidence_id"] = evid

    # Cross-source overlap audit (always print)
    by_source_event_keys: Dict[str, Set[str]] = {}
    by_source_day_keys: Dict[str, Set[str]] = {}
    by_source_matchups: Dict[str, Set[Tuple[str, Optional[str], Optional[str]]]] = {}
    market_counts: Dict[Tuple[str, str], int] = {}
    standard_samples: Dict[str, List[Dict[str, Any]]] = {}
    missing_matchup_by_source: Dict[str, int] = {}
    standard_group_keys: Dict[str, Set[str]] = {}
    for rec in eligible:
        prov = rec.get("provenance") or {}
        src = prov.get("source_id", "unknown")
        ev = rec.get("event") or {}
        mk = rec.get("market") or {}
        ev_key = ev.get("event_key")
        dk = day_key(ev_key)
        by_source_event_keys.setdefault(src, set())
        by_source_day_keys.setdefault(src, set())
        by_source_matchups.setdefault(src, set())
        standard_samples.setdefault(src, [])
        missing_matchup_by_source.setdefault(src, 0)
        standard_group_keys.setdefault(src, set())
        if ev_key:
            by_source_event_keys[src].add(ev_key)
        if dk:
            by_source_day_keys[src].add(dk)
        away = ev.get("away_team")
        home = ev.get("home_team")
        if dk and away and home:
            t1, t2 = sorted([str(away).upper(), str(home).upper()])
            by_source_matchups[src].add((dk, t1, t2))
        mkt = mk.get("market_type") or "unknown"
        market_counts[(src, mkt)] = market_counts.get((src, mkt), 0) + 1
        is_standard = mkt in {"spread", "total", "moneyline"}
        if is_standard:
            matchup_key_val = ev.get("matchup_key")
            if not matchup_key_val and dk and away and home:
                t1, t2 = sorted([str(away).upper(), str(home).upper()])
                matchup_key_val = f"{dk}:{t1}-{t2}"
            group_key = matchup_key_val or ev_key
            if group_key:
                standard_group_keys[src].add(group_key)
            if not matchup_key_val:
                missing_matchup_by_source[src] += 1
            if len(standard_samples[src]) < 10:
                standard_samples[src].append(
                    {
                        "source": src,
                        "market_type": mkt,
                        "event_key": ev_key,
                        "matchup_key": matchup_key_val,
                        "away_team": away,
                        "home_team": home,
                        "selection": mk.get("selection"),
                        "side": mk.get("side"),
                        "line": mk.get("line"),
                    }
                )

    action_events = by_source_event_keys.get("action", set())
    covers_events = by_source_event_keys.get("covers", set())
    action_days = by_source_day_keys.get("action", set())
    covers_days = by_source_day_keys.get("covers", set())
    action_matchups = by_source_matchups.get("action", set())
    covers_matchups = by_source_matchups.get("covers", set())
    action_std_keys = standard_group_keys.get("action", set())
    covers_std_keys = standard_group_keys.get("covers", set())

    print("CROSS-SOURCE OVERLAP AUDIT:")
    print(
        f"  eligible_counts: action={len([r for r in eligible if (r.get('provenance') or {}).get('source_id')=='action'])} "
        f"covers={len([r for r in eligible if (r.get('provenance') or {}).get('source_id')=='covers'])}"
    )
    print(
        f"  unique event_keys: action={len(action_events)} covers={len(covers_events)}"
    )
    print(f"  unique day_keys: action={len(action_days)} covers={len(covers_days)}")
    print(f"  event_key intersection size={len(action_events & covers_events)}")
    print(f"  day_key intersection size={len(action_days & covers_days)}")
    print(f"  matchup intersection size={len(action_matchups & covers_matchups)}")
    print(f"  standard group_key intersection size={len(action_std_keys & covers_std_keys)}")

    if debug:
        print(f"[DEBUG standard audit] missing_matchup_key_by_source={missing_matchup_by_source}")
        for src in ["action", "covers", "betql", "sportscapping"]:
            samples = standard_samples.get(src, [])
            if not samples:
                continue
            print(f"[DEBUG standard audit] source={src} samples (first {len(samples)}):")
            for row in samples:
                print(f"    {row}")

        watson_debug: List[Dict[str, Any]] = []
        for rec in eligible:
            mk = rec.get("market") or {}
            sel = mk.get("selection") or ""
            line = mk.get("line")
            if "watson" in str(sel).lower() and line == 18.5:
                parsed = parse_selection_from_record(rec)
                event = rec.get("event") or {}
                prov = rec.get("provenance") or {}
                watson_debug.append(
                    {
                        "source": prov.get("source_id"),
                        "raw_player_key": mk.get("player_key"),
                        "parsed_player_id": parsed[0] if parsed else None,
                        "normalized_player_id": normalize_player_id(mk.get("player_key") or mk.get("selection")),
                        "selection": sel,
                        "stat_key": mk.get("stat_key"),
                        "direction": mk.get("side") or (parsed[2] if parsed else None),
                        "day_key": day_key(event.get("event_key")),
                    }
                )
        if watson_debug:
            print("[DEBUG] Watson 18.5 records across sources:")
            for row in watson_debug:
                print(f"  {row}")

    # Targeted prop search debug
    if find_prop:
        tokens = [t.strip() for t in find_prop.split("|") if t.strip()]
        print(f"[DEBUG find-prop] searching tokens={tokens}")
        for rec in records:
            prov = rec.get("provenance") or {}
            mk = rec.get("market") or {}
            sel = mk.get("selection") or ""
            expert_name = prov.get("expert_name") or ""
            player_key = mk.get("player_key") or ""
            haystack = " ".join([str(sel), str(expert_name), str(player_key)]).lower()
            if not any(tok.lower() in haystack for tok in tokens):
                continue
            event = rec.get("event") or {}
            parsed = parse_selection_from_record(rec)
            direction = mk.get("side")
            stat_key = mk.get("stat_key")
            atomic_stat = None
            if stat_key:
                for atomic in mapping_atomic_stats(normalize_stat_key(stat_key)):
                    atomic_stat = atomic
                    break
            normalized_player = normalize_player_id(player_key or sel)
            dk = day_key(event.get("event_key"))
            matchup = None
            if event.get("away_team") and event.get("home_team"):
                matchup = f"{str(event['away_team']).upper()}@{str(event['home_team']).upper()}"
            identity_sig = (dk, matchup, normalized_player, atomic_stat, direction)
            print(
                f"[DEBUG find-prop] src={prov.get('source_id')} eligible={rec.get('eligible_for_consensus')} reason={rec.get('ineligibility_reason')}\n"
                f"  event={event.get('event_key')} day={dk} matchup={matchup}\n"
                f"  market_type={mk.get('market_type')} sel={sel} side={direction}\n"
                f"  player_key={player_key} parsed_player_id={(parsed[0] if parsed else None)} normalized_player_id={normalized_player}\n"
                f"  stat_key={stat_key} atomic_stat={atomic_stat}\n"
                f"  line={mk.get('line')} odds={mk.get('odds')} url={prov.get('canonical_url')}\n"
                f"  prop_identity={identity_sig}\n"
            )

    if find_market:
        tokens = [t.strip() for t in find_market.split("|") if t.strip()]
        print(f"[DEBUG find-market] searching tokens={tokens}")
        for rec in records:
            prov = rec.get("provenance") or {}
            mk = rec.get("market") or {}
            sel = mk.get("selection") or ""
            expert_name = prov.get("expert_name") or ""
            player_key = mk.get("player_key") or ""
            haystack = " ".join([str(sel), str(expert_name), str(player_key)]).lower()
            if not any(tok.lower() in haystack for tok in tokens):
                continue
            event = rec.get("event") or {}
            dk = day_key(event.get("event_key"))
            matchup = None
            if event.get("away_team") and event.get("home_team"):
                matchup = f"{str(event['away_team']).upper()}@{str(event['home_team']).upper()}"
            print(
                f"[DEBUG find-market] src={prov.get('source_id')} eligible={rec.get('eligible_for_consensus')} reason={rec.get('ineligibility_reason')}\n"
                f"  day={dk} matchup={matchup} market={mk.get('market_type')} sel={sel} side={mk.get('side')} line={mk.get('line')} odds={mk.get('odds')}\n"
                f"  expert_name={prov.get('expert_name')} handle={prov.get('expert_handle')} profile={prov.get('expert_profile')} slug={prov.get('expert_slug')}\n"
                f"  canonical_url={prov.get('canonical_url')} raw_fingerprint={prov.get('raw_fingerprint')}\n"
            )

    if len(action_days & covers_days) == 0:
        def sample_rows(source_id: str) -> List[Dict[str, Any]]:
            rows = [r for r in eligible if (r.get("provenance") or {}).get("source_id") == source_id]
            return rows[:5]

        for src in ("action", "covers"):
            samples = sample_rows(src)
            print(f"  samples for {src} (max 5):")
            for rec in samples:
                ev = rec.get("event") or {}
                mk = rec.get("market") or {}
                print(
                    f"    event={ev.get('event_key')} day={day_key(ev.get('event_key'))} "
                    f"away={ev.get('away_team')} home={ev.get('home_team')} "
                    f"market={mk.get('market_type')} sel={mk.get('selection')} side={mk.get('side')} line={mk.get('line')}"
                )

    print("  market_type counts by source:")
    for (src, mkt), cnt in sorted(market_counts.items()):
        print(f"    {src}:{mkt} -> {cnt}")

    if debug:
        print(
            f"[DEBUG] records_loaded={len(records)} eligible={len(eligible)} by_source={eligible_counts(records)}"
        )
        by_market: Dict[str, int] = {}
        for rec in eligible:
            m = (rec.get("market") or {}).get("market_type") or "unknown"
            by_market[m] = by_market.get(m, 0) + 1
        print(f"[DEBUG] eligible by market: {by_market}")
        samples_by_source: Dict[str, List[Dict[str, Any]]] = {}
        for rec in eligible:
            src = (rec.get("provenance") or {}).get("source_id") or "unknown"
            if len(samples_by_source.get(src, [])) < 3:
                samples_by_source.setdefault(src, []).append(rec)
        for src, samples in samples_by_source.items():
            print(f"[DEBUG] sample records for source={src}")
            for rec in samples:
                ev = rec.get("event") or {}
                mk = rec.get("market") or {}
                prov = rec.get("provenance") or {}
                print(
                    f"  event={ev.get('event_key')} market={mk.get('market_type')} sel={mk.get('selection')} "
                    f"side={mk.get('side')} line={mk.get('line')} odds={mk.get('odds')} url={prov.get('canonical_url')}"
                )

    hard_groups = group_hard(records)
    std_hard, std_avoid = build_standard_directional_consensus(records, debug=debug)
    if std_hard:
        hard_groups.extend(std_hard)
    hard_identities: Set[Tuple[Any, ...]] = set()
    for g in hard_groups:
        # only passing hard_cross_source (source_strength>=2) should block soft
        if g.get("source_strength", 0) < 2:
            continue
        if g.get("market_type") == "player_prop":
            identity = (
                g.get("day_key"),
                g.get("player_id"),
                g.get("atomic_stat"),
                g.get("direction") or g.get("side"),
                g.get("line_median"),
            )
        else:
            identity = (
                g.get("day_key"),
                g.get("matchup"),
                g.get("market_type"),
                g.get("canonical_selection") or g.get("selection") or g.get("direction"),
                g.get("line_median"),
            )
        hard_identities.add(identity)

    soft_groups = group_soft(records, hard_identities=hard_identities, debug_soft_tokens=debug_soft_tokens)
    soft_conflicts = soft_conflict_rows(soft_groups)
    within_groups = group_within_source(records)
    overlap_days = action_days & covers_days
    if overlap_days:
        key_samples: Dict[str, List[Tuple[Any, ...]]] = {"action": [], "covers": []}
        keys_by_source: Dict[str, Set[Tuple[Any, ...]]] = {"action": set(), "covers": set()}
        for g in hard_groups:
            if g.get("day_key") not in overlap_days:
                continue
            for src in g.get("sources", []):
                if src in key_samples and len(key_samples[src]) < 10:
                    key_samples[src].append(g.get("canonical_key"))
                if src in keys_by_source:
                    keys_by_source[src].add(g.get("canonical_key"))
        print("[DEBUG] canonical hard keys on overlapping day_keys (top 10 per source):")
        for src, arr in key_samples.items():
            print(f"  {src}: {arr}")
        intersection = keys_by_source.get("action", set()) & keys_by_source.get("covers", set())
        print(f"[DEBUG] canonical key intersection count={len(intersection)} samples={list(intersection)[:5]}")
    ensure_dir(out_dir)
    with open(os.path.join(out_dir, "consensus_nba_hard.json"), "w", encoding="utf-8") as f:
        json.dump(hard_groups, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "consensus_nba_soft.json"), "w", encoding="utf-8") as f:
        json.dump(soft_groups, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "consensus_nba_within.json"), "w", encoding="utf-8") as f:
        json.dump(within_groups, f, ensure_ascii=False, indent=2)

    unified_signals = build_unified_signals(hard_groups, soft_groups, within_groups)
    conflicts_extra = detect_conflicts(hard_groups, soft_groups, within_groups)
    if std_avoid:
        conflicts_extra.extend(std_avoid)
    if conflicts_extra:
        unified_signals.extend(conflicts_extra)
        unified_signals.sort(key=lambda s: s.get("score", 0) if s.get("score") is not None else 0, reverse=True)
    unified_before = len(unified_signals)
    signals_pre_dedupe = list(unified_signals)
    unified_signals = dedupe_signals(unified_signals)
    unified_after = len(unified_signals)
    # add signal_id to unified signals for tracking stability
    for sig in unified_signals:
        sid = compute_signal_id(sig)
        if sid:
            sig["signal_id"] = sid

    with open(os.path.join(out_dir, "consensus_nba_v1.json"), "w", encoding="utf-8") as f:
        json.dump(unified_signals, f, ensure_ascii=False, indent=2)

    hard_filtered = [g for g in hard_groups if g.get("source_strength", 0) >= 2]
    soft_filtered = [g for g in soft_groups if g.get("expert_strength", 0) >= 2 or g.get("count_total", 0) >= 2]
    conflict_rows_raw = detect_conflicts(hard_groups, soft_groups, within_groups)
    conflict_rows_raw.extend([s for s in unified_signals if s.get("signal_type") == "avoid_conflict"])
    conflict_rows_raw.extend(soft_conflicts)
    conflict_rows = dedupe_signals(conflict_rows_raw)

    # Build conflict identities for standard markets (event_key, market_type)
    conflict_ids: Set[Tuple[Any, Any]] = set()
    for row in conflict_rows:
        if row.get("signal_type") != "avoid_conflict":
            continue
        mkt = row.get("market_type")
        if mkt in {"spread", "moneyline", "total"}:
            conflict_ids.add((row.get("event_key"), mkt))

    # Also detect conflicts within soft standard signals (multiple selections same event_key+market_type)
    soft_std = [s for s in soft_groups if s.get("market_type") in {"spread", "moneyline", "total"}]
    bucket: Dict[Tuple[Any, Any], Set[Any]] = {}
    conflict_ids_local: Set[Tuple[Any, Any]] = set()
    for s in soft_std:
        key = (s.get("event_key"), s.get("market_type"))
        bucket.setdefault(key, set()).add(s.get("selection"))
    for key, sels in bucket.items():
        if len(sels) >= 2:
            conflict_ids.add(key)
            conflict_ids_local.add(key)

    filtered_soft_groups: List[Dict[str, Any]] = []
    for s in soft_groups:
        if s.get("market_type") in {"spread", "moneyline", "total"} and (s.get("event_key"), s.get("market_type")) in conflict_ids:
            if debug and (not debug_soft_tokens or all(tok.lower() in str(s).lower() for tok in debug_soft_tokens)):
                print(f"[SOFT SUPPRESSED] due to conflict_id={(s.get('event_key'), s.get('market_type'))} selection={s.get('selection')} lines={s.get('lines')}")
            continue
        filtered_soft_groups.append(s)
    soft_groups = filtered_soft_groups

    print_section("HARD CONSENSUS (>=2 sources):", hard_filtered, format_hard_row)
    std_hard_filtered = std_hard  # already requires >=2 sources
    print_section("HARD STANDARD CONSENSUS (>=2 sources):", std_hard_filtered, format_standard_hard_row)
    std_avoid_filtered = std_avoid
    print_section("AVOID STANDARD CONFLICTS:", std_avoid_filtered, format_standard_conflict_row)
    if not hard_filtered and not std_hard_filtered:
        if overlap_days:
            print("[DEBUG] HARD CONSENSUS empty; see canonical key overlap above.")
        # show top prop identities
        prop_groups_meta = HARD_DEBUG_META.get("prop_groups", {})
        top_props = sorted(
            [(ident, len(entries)) for ident, entries in prop_groups_meta.items()],
            key=lambda x: x[1],
            reverse=True,
        )[:5]
        if top_props:
            print("[DEBUG] top prop identities by count (all sources, any line):")
            for ident, cnt in top_props:
                print(f"  {ident} count={cnt}")
        near_miss_props = HARD_DEBUG_META.get("near_miss_props", [])
        if near_miss_props:
            print("[DEBUG] prop near-misses (same identity, lines > tolerance):")
            for nm in near_miss_props[:5]:
                print(f"  {nm['identity']} sources={nm['sources']} lines={nm['lines']} spread={nm['spread']}")
    print_section("SOFT CONSENSUS (>=2 experts, player props):", soft_filtered, format_soft_row)
    print_section("CONFLICTS / AVOIDS:", conflict_rows, format_conflict_row)

    # Tracking writes
    if track:
        rid = run_id or default_run_id()
        run_dir = os.path.join(data_dir, "runs", rid)
        ensure_dir(run_dir)

        raw_path = os.path.join(run_dir, "raw_records.jsonl")
        norm_path = os.path.join(run_dir, "normalized_records.jsonl")
        signals_pre_path = os.path.join(run_dir, "signals_pre_dedupe.jsonl")
        signals_path = os.path.join(run_dir, "signals.jsonl")
        meta_path = os.path.join(run_dir, "run_meta.json")

        append_and_fsync(raw_path, raw_records if raw_records else records)
        append_and_fsync(norm_path, records)
        append_and_fsync(signals_pre_path, signals_pre_dedupe)
        append_and_fsync(signals_path, dedupe_signals(unified_signals))

        observed_at = datetime.now(timezone.utc).isoformat()
        git_commit = run_git_commit()
        counts = {
            "raw_records": len(raw_records) if raw_records else len(records),
            "normalized_records": len(records),
            "eligible_records": len(eligible),
            "hard_signals": len(hard_groups),
            "soft_signals": len(soft_groups),
            "avoid_signals": len([s for s in conflict_rows if s.get("signal_type") == "avoid_conflict"]),
            "total_unified_signals": len(unified_signals),
        }
        market_counts = {}
        for rec in records:
            prov = rec.get("provenance") or {}
            mk = rec.get("market") or {}
            src = prov.get("source_id", "unknown")
            mkt = mk.get("market_type") or "unknown"
            market_counts[(src, mkt)] = market_counts.get((src, mkt), 0) + 1
        sources_present = sorted({(rec.get("provenance") or {}).get("source_id") or "unknown" for rec in records})
        meta = {
            "schema_version": 1,
            "run_id": rid,
            "observed_at_utc": observed_at,
            "git_commit": git_commit,
            "command_argv": sys.argv,
            "python_version": sys.version,
            "counts": counts,
            "sources_present": sources_present,
            "market_counts": {f"{src}:{mkt}": cnt for (src, mkt), cnt in market_counts.items()},
        }
        write_json(meta_path, meta)

        # Copy snapshots to latest
        write_snapshot_latest(out_dir, data_dir)

    if debug:
        print(
            f"[DEBUG] hard_groups total={len(hard_groups)} passing={len(hard_filtered)} "
            f"soft_groups total={len(soft_groups)} passing={len(soft_filtered)} "
            f"within_groups total={len(within_groups)}"
        )
        avoid_before = len([s for s in unified_signals if s.get("signal_type") == "avoid_conflict"]) + len(
            [s for s in conflict_rows_raw if s.get("signal_type") == "avoid_conflict"]
        )
        avoid_after = len([s for s in conflict_rows if s.get("signal_type") == "avoid_conflict"])
        print(f"[DEBUG] avoid_conflict count before_dedupe={avoid_before} after_dedupe={avoid_after}")
        if avoid_after < avoid_before:
            # detect remaining duplicates if any
            keys_seen = {}
            dup_keys = []
            for sig in conflict_rows:
                if sig.get("signal_type") != "avoid_conflict":
                    continue
                key = (
                    sig.get("signal_type"),
                    sig.get("day_key"),
                    sig.get("market_type"),
                    sig.get("selection"),
                    tuple(sorted(sig.get("selections", []) or [])),
                    tuple(sorted(sig.get("sources", []) or [])),
                )
                if key in keys_seen:
                    dup_keys.append(key)
                else:
                    keys_seen[key] = sig
            if dup_keys:
                print("[DEBUG] duplicate avoid_conflict keys still present:")
                for k in dup_keys:
                    print(f"  key={k}")
        unified_by_type: Dict[str, int] = {}
        for sig in unified_signals:
            sig_type = sig.get("signal_type") or "unknown"
            unified_by_type[sig_type] = unified_by_type.get(sig_type, 0) + 1
        print(f"[DEBUG] unified totals by type: {unified_by_type} conflicts_total={len(conflict_rows)}")
        if conflict_rows:
            print(f"[DEBUG] conflict sample: {json.dumps(conflict_rows[0], ensure_ascii=False)[:320]}")
        sample_support = None
        for sig in unified_signals:
            if sig.get("supports"):
                sample_support = sig["supports"][0]
                break
        if sample_support:
            print(
                "[DEBUG] sample support: "
                f"expert_name={sample_support.get('expert_name')} "
                f"handle={sample_support.get('expert_handle')} "
                f"url={sample_support.get('canonical_url')}"
            )
        for sig_type in ["hard_cross_source", "soft_atomic", "avoid_conflict"]:
            samples = [s for s in unified_signals if s.get("signal_type") == sig_type][:2]
            for sample in samples:
                print(f"[DEBUG] sample {sig_type}: {json.dumps(sample, ensure_ascii=False)[:500]}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute NBA consensus (hard, soft, within, unified).")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")
    parser.add_argument("--find-prop", type=str, help="Debug: regex or pipe-delimited tokens to search for props.")
    parser.add_argument("--find-market", type=str, help="Debug: regex or pipe-delimited tokens to search markets.")
    parser.add_argument("--debug-soft-market", type=str, help="Debug soft grouping for a given market (tokens).")
    parser.add_argument("--track", action=argparse.BooleanOptionalAction, default=True, help="Enable tracking writes (default on).")
    parser.add_argument("--run-id", type=str, help="Optional run id override.")
    parser.add_argument("--data-dir", type=str, default="data", help="Directory for tracking data (default: data).")
    parser.add_argument("--out-dir", type=str, default="out", help="Directory for snapshot outputs (default: out).")
    args = parser.parse_args()
    debug_soft_tokens = [t.strip() for t in args.debug_soft_market.split() if t.strip()] if args.debug_soft_market else None
    main(
        debug=args.debug,
        find_prop=args.find_prop,
        find_market=args.find_market,
        debug_soft_tokens=debug_soft_tokens,
        track=args.track,
        run_id=args.run_id,
        data_dir=args.data_dir,
        out_dir=args.out_dir,
    )
