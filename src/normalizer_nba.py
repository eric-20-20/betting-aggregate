from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from collections import Counter
from pathlib import Path
import urllib.request

from store import data_store, write_json
from action_ingest import parse_standard_market
from utils import normalize_text
from src.player_aliases_nba import normalize_player_key


def build_matchup_key(day_key: Optional[str], team_a: Optional[str], team_b: Optional[str]) -> Optional[str]:
    """Order-invariant matchup key using canonical team codes/names."""
    if not (day_key and team_a and team_b):
        return None
    t1, t2 = sorted([str(team_a).upper(), str(team_b).upper()])
    return f"{day_key}:{t1}-{t2}"


def _parse_dt(value: Any) -> Optional[datetime]:
    """Parse ISO8601 string or datetime into an aware UTC datetime."""
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalize_team(token: Optional[str]) -> Optional[str]:
    if not token:
        return None
    normalized = token.strip()
    # Quick aliases for common BetQL/Action naming
    alias_map = {
        "suns": "PHX",
        "suns.": "PHX",
        "sixers": "PHI",
        "76ers": "PHI",
        "blazers": "POR",
        "trail blazers": "POR",
        "trail-blazers": "POR",
        "pels": "NOP",
        "pelicans": "NOP",
        "knicks": "NYK",
        "nets": "BKN",
        "suns": "PHX",
        "warriors": "GSW",
        "spurs": "SAS",
        "timberwolves": "MIN",
        "t-wolves": "MIN",
    }
    if normalized.lower() in alias_map:
        return alias_map[normalized.lower()]
    matches = data_store.lookup_team_code(normalized)
    if len(matches) == 1:
        return next(iter(matches))
    if len(normalized) == 3 and normalized.upper() in data_store.teams:
        return normalized.upper()
    return None


def _parse_teams_from_canonical(url: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if not url:
        return None, None
    m = re.search(r"/nba-game/([^/]+)/", url)
    if not m:
        return None, None
    slug = m.group(1)
    slug = slug.split("-score")[0]
    tokens = [t for t in slug.split("-") if t]
    if len(tokens) < 2:
        return None, None
    for i in range(1, len(tokens)):
        away_raw = " ".join(tokens[:i])
        home_raw = " ".join(tokens[i:])
        away_code = _normalize_team(away_raw)
        home_code = _normalize_team(home_raw)
        if away_code and home_code:
            return away_code, home_code
    return None, None


def _build_day_and_event(dt: Optional[datetime], away: Optional[str], home: Optional[str], sport: str = "NBA") -> tuple[Optional[str], Optional[str]]:
    if not dt:
        return None, None
    # Convert to US Eastern time for game day assignment
    # Games at 9pm ET on Feb 12 should be day_key SPORT:2026:02:12, not Feb 13
    from zoneinfo import ZoneInfo
    eastern = ZoneInfo("America/New_York")
    dt_eastern = dt.astimezone(eastern)
    day_key = f"{sport}:{dt_eastern.year:04d}:{dt_eastern.month:02d}:{dt_eastern.day:02d}"
    if away and home:
        return day_key, f"{day_key}:{away}@{home}"
    return day_key, day_key


def build_event_key(raw: Dict[str, Any]) -> Optional[str]:
    """Backwards-compatible helper: returns the event_key string."""
    dt = _parse_dt(raw.get("event_start_time_utc")) or _parse_dt(raw.get("observed_at_utc"))
    away = raw.get("away_team")
    home = raw.get("home_team")
    sport = raw.get("sport") or "NBA"
    _, ev = _build_day_and_event(dt, away, home, sport=sport)
    return ev


def _clean_player_tokens(text: str) -> str:
    # Expand compressed initials like "G.Antetokounmpo" -> "G Antetokounmpo"
    text = re.sub(r"(?<![A-Za-z])([A-Z])\.([A-Za-z]+)", r"\1 \2", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_direction(text: str) -> Optional[str]:
    if not text:
        return None
    match = re.search(r"\b(over|under)\b", text, re.IGNORECASE)
    if match:
        return match.group(1).upper()

    prefix = re.search(r"\b([ou])\s*\d", text, re.IGNORECASE)
    if prefix:
        return "OVER" if prefix.group(1).lower() == "o" else "UNDER"
    return None


def _extract_line(text: str) -> Optional[float]:
    if not text:
        return None
    match = re.search(r"\b(?:over|under|o|u)\s*([0-9]+(?:\.[0-9]+)?)", text, re.IGNORECASE)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None


def _extract_odds(text: str) -> Optional[int]:
    if not text:
        return None
    match = re.search(r"([+-]\d{2,4})", text)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None


def _extract_stat_key(raw: Dict[str, Any]) -> Optional[str]:
    for field in ("raw_pick_text", "raw_block"):
        text = raw.get(field)
        if text:
            key = data_store.lookup_stat_key(text)
            if key:
                return key
    return None


def _extract_player_name(raw_pick_text: str) -> Optional[str]:
    if not raw_pick_text:
        return None

    cleaned = _clean_player_tokens(raw_pick_text)
    match = re.search(r"^\s*([A-Za-z\s\.'-]+?)\s+(?:over|under|o|u)\b", cleaned, re.IGNORECASE)
    if match:
        candidate = match.group(1)
    else:
        tokens = cleaned.split()
        stop = {"over", "under", "o", "u"}
        candidate_parts: List[str] = []
        for tok in tokens:
            if tok.lower() in stop:
                break
            if re.fullmatch(r"[+-]?\d+(?:\.\d+)?", tok):
                break
            candidate_parts.append(tok)
        candidate = " ".join(candidate_parts)

    candidate = _clean_player_tokens(candidate)
    return candidate or None


def _resolve_player_key(player_name: Optional[str]) -> Optional[str]:
    if not player_name:
        return None
    existing = data_store.lookup_player_key(player_name)
    if existing:
        return existing

    full_name = " ".join(word.capitalize() for word in player_name.split()) or player_name
    return data_store.add_player(full_name=full_name, alias_text=player_name, is_verified=False)


def _build_selection(player_key: Optional[str], stat_key: Optional[str], direction: Optional[str]) -> Optional[str]:
    if not (player_key and stat_key and direction):
        return None
    return f"{player_key}::{stat_key}::{direction}"


def _eligibility(
    market_type: str,
    event_key: Optional[str],
    away_team: Optional[str],
    home_team: Optional[str],
    player_key: Optional[str],
    stat_key: Optional[str],
    direction: Optional[str],
    line: Optional[float],
    selection: Optional[str],
) -> tuple[bool, Optional[str]]:
    if not event_key:
        return False, "missing_event_key"

    if market_type == "player_prop":
        if not player_key:
            return False, "unparsed_player"
        if not stat_key:
            return False, "unparsed_stat"
        if direction is None:
            return False, "unparsed_direction"
        if line is None:
            return False, "unparsed_line"
        return True, None

    if market_type not in {"spread", "moneyline", "total"}:
        return False, "unsupported_market"
    if not (away_team and home_team):
        return False, "unparsed_team"
    if market_type in {"spread", "moneyline"}:
        if not selection:
            return False, "unparsed_team"
        if market_type == "spread" and line is None:
            return False, "unparsed_line"
    if market_type == "total":
        if direction is None:
            return False, "unparsed_direction"
        if line is None:
            return False, "unparsed_line"
    return True, None


def normalize_raw_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    pick_text = raw.get("raw_pick_text") or ""
    block_text = raw.get("raw_block") or ""

    # Teams
    away_raw = raw.get("away_team")
    home_raw = raw.get("home_team")
    if not (away_raw and home_raw):
        away_raw, home_raw = _parse_teams_from_canonical(raw.get("canonical_url"))
    away_team = _normalize_team(away_raw) if away_raw else None
    home_team = _normalize_team(home_raw) if home_raw else None

    dt = _parse_dt(raw.get("event_start_time_utc")) or _parse_dt(raw.get("observed_at_utc"))
    sport = raw.get("sport") or "NBA"
    day_key, event_key = _build_day_and_event(dt, away_team, home_team, sport=sport)
    matchup_key = build_matchup_key(day_key, away_team, home_team)

    is_prop_pick = is_covers_player_prop(pick_text, block_text) if raw.get("source_id") == "covers" else False
    market_type = None
    direction = None
    line = None
    odds = None
    selection = None
    player_key = None
    stat_key = None

    # Covers-first strict routing (mutually exclusive)
    if raw.get("source_id") == "covers":
        sp_ok, sp_team, sp_line = is_covers_spread(pick_text)
        gt_ok, gt_line, gt_dir = is_covers_game_total(pick_text)
        if sp_ok:
            market_type = "spread"
            direction = sp_team
            selection = _normalize_team(sp_team)
            line = sp_line
        elif gt_ok:
            market_type = "total"
            direction = (gt_dir or "").upper() if gt_dir else None
            selection = direction
            line = gt_line
        elif is_prop_pick:
            market_type = "player_prop"

    if market_type is None and (raw.get("market_family") == "player_prop"):
        market_type = "player_prop"

    # Last-resort guard for Covers mislabels
    if market_type is None and raw.get("source_id") == "covers" and is_covers_player_prop(pick_text, block_text):
        market_type = "player_prop"

    manual_ineligible_reason = None

    if market_type == "player_prop":
        direction = _extract_direction(pick_text) or _extract_direction(block_text)
        line = _extract_line(pick_text) or _extract_line(block_text)
        odds = _extract_odds(pick_text) or _extract_odds(block_text)
        stat_key = _extract_stat_key(raw)
        player_name = _extract_player_name(pick_text)
        player_key = _resolve_player_key(player_name)
        player_key = normalize_player_key(player_key)
        selection = _build_selection(player_key, stat_key, direction)
    else:
        unrealistic_line = False
        odds = _extract_odds(pick_text) or _extract_odds(block_text)
        odds = int(odds) if isinstance(odds, str) and re.match(r"[+-]\d{2,4}", odds) else odds

        parsed = parse_standard_market(pick_text)
        market_type = market_type or parsed.get("market_type") or "unknown"
        odds = odds or parsed.get("odds")
        if market_type == "spread":
            team_code = _normalize_team(parsed.get("team_code"))
            selection = team_code
            direction = team_code
            line = parsed.get("line") if parsed.get("line") is not None else raw.get("line_hint")
            if line is not None and abs(line) > 40:
                unrealistic_line = True
        elif market_type == "moneyline":
            team_code = _normalize_team(parsed.get("team_code"))
            selection = team_code
            direction = team_code
            line = None
        elif market_type == "total":
            direction = (parsed.get("side") or "").upper() if parsed.get("side") else None
            selection = direction
            line = parsed.get("line") if parsed.get("line") is not None else raw.get("line_hint")
            if line is not None and not (150 <= line <= 300):
                unrealistic_line = True

        # Covers standard sanity checks
        if raw.get("source_id") == "covers":
            if market_type == "spread" and line is not None and abs(line) > 30:
                market_type = "unknown"
            if market_type == "total" and line is not None and not (170 <= line <= 280):
                market_type = "unknown"

    if market_type is None or market_type == "unknown":
        manual_ineligible_reason = "unparsed_market"
        market_type = market_type or "unknown"

    event = {
        "sport": raw.get("sport"),
        "event_key": event_key,
        "day_key": day_key,
        "matchup_key": matchup_key,
        "away_team": away_team,
        "home_team": home_team,
        "event_start_time_utc": raw.get("event_start_time_utc"),
    }

    # Extract unit_size from stake_hint (e.g., "0.7u" → 0.7)
    unit_size = None
    stake_hint = raw.get("stake_hint")
    if stake_hint:
        _m = re.match(r"(\d+(?:\.\d+)?)\s*u", str(stake_hint), re.IGNORECASE)
        if _m:
            unit_size = float(_m.group(1))

    market = {
        "market_type": market_type,
        "market_family": raw.get("market_family"),
        "selection": selection,
        "player_key": player_key,
        "stat_key": stat_key,
        "line": line,
        "odds": odds,
        "side": direction,
        "unit_size": unit_size,
    }

    provenance = {
        "source_id": raw.get("source_id"),
        "source_surface": raw.get("source_surface"),
        "canonical_url": raw.get("canonical_url"),
        "raw_pick_text": raw.get("raw_pick_text"),
        "raw_block": raw.get("raw_block"),
        "raw_fingerprint": raw.get("raw_fingerprint"),
        "expert_name": raw.get("expert_name"),
        "expert_handle": raw.get("expert_handle"),
        "expert_profile": raw.get("expert_profile"),
        "expert_slug": raw.get("expert_slug"),
        "matchup_hint": raw.get("matchup_hint"),
    }

    eligible, reason = _eligibility(
        market_type,
        event_key,
        away_team,
        home_team,
        player_key,
        stat_key,
        direction,
        line,
        selection,
    )
    if manual_ineligible_reason:
        eligible, reason = False, manual_ineligible_reason
    if not reason and 'unrealistic_line' in locals() and unrealistic_line:
        eligible, reason = False, "unrealistic_line"

    return {
        "event": event,
        "market": market,
        "provenance": provenance,
        "eligible_for_consensus": eligible,
        "ineligibility_reason": reason,
    }


def _parse_matchup_id(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    m = re.search(r"/(?:sport/basketball/nba/)?matchup/(\d+)/picks", url)
    return m.group(1) if m else None


def _covers_mid_from_url(url: Optional[str]) -> Optional[str]:
    if not url or not isinstance(url, str):
        return None
    for pat in (r"/sport/basketball/nba/matchup/([0-9]+)/picks", r"/nba/matchup/([0-9]+)/picks", r"/matchup/([0-9]+)/picks"):
        m = re.search(pat, url)
        if m:
            return m.group(1)
    return None


def _fetch_covers_matchup_teams(mid: str) -> tuple[Optional[str], Optional[str], str]:
    """Fetch and cache Covers matchup page to extract teams."""
    cache_dir = Path("data/cache/covers")
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"matchup_{mid}.html"
    html = None
    source = "none"

    def _load_html_from_cache() -> Optional[str]:
        try:
            return cache_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        except Exception:
            return None

    def _save_html(text: str) -> None:
        try:
            cache_path.write_text(text, encoding="utf-8")
        except Exception:
            pass

    html = _load_html_from_cache()
    if html:
        source = "cache"
    else:
        url = f"https://www.covers.com/sport/basketball/nba/matchup/{mid}/picks"
        headers = {"User-Agent": "Mozilla/5.0 (compatible; BettingAggregateBot/1.0)"}
        try:
            try:
                import requests  # type: ignore

                resp = requests.get(url, headers=headers, timeout=6)
                if resp.ok:
                    html = resp.text
                    _save_html(html)
                    source = "http"
            except Exception:
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=6) as resp:  # type: ignore
                    html_bytes = resp.read()
                    html = html_bytes.decode("utf-8", errors="ignore")
                    _save_html(html)
                    source = "http"
        except Exception:
            return None, None, "fetch_failed"

    if not html:
        return None, None, "empty_html"

    def _parse_jsonld(text: str) -> tuple[Optional[str], Optional[str], str]:
        for m in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', text, re.IGNORECASE | re.DOTALL):
            raw_json = m.group(1).strip()
            try:
                data = json.loads(raw_json)
            except json.JSONDecodeError:
                # Handle malformed JSON with extra trailing braces
                # Find proper end by matching braces
                depth = 0
                end_pos = 0
                for i, c in enumerate(raw_json):
                    if c == '{':
                        depth += 1
                    elif c == '}':
                        depth -= 1
                        if depth == 0:
                            end_pos = i + 1
                            break
                if end_pos > 0:
                    try:
                        data = json.loads(raw_json[:end_pos])
                    except Exception:
                        continue
                else:
                    continue
            except Exception:
                continue
            candidates = data if isinstance(data, list) else [data]
            for obj in candidates:
                if not isinstance(obj, dict):
                    continue
                home = obj.get("homeTeam") or obj.get("home_team")
                away = obj.get("awayTeam") or obj.get("away_team")
                if isinstance(home, dict):
                    home = home.get("name") or home.get("alternateName")
                if isinstance(away, dict):
                    away = away.get("name") or away.get("alternateName")
                home_code = _normalize_team(home) if isinstance(home, str) else None
                away_code = _normalize_team(away) if isinstance(away, str) else None
                if home_code and away_code:
                    method = f"{source}_jsonld"
                    return away_code, home_code, method
        return None, None, ""

    away_team, home_team, method = _parse_jsonld(html)
    if away_team and home_team:
        return away_team, home_team, method

    title_match = re.search(r"<title>([^<]+)</title>", html, re.IGNORECASE)
    if title_match:
        title = title_match.group(1)
        m_vs = re.search(r"([A-Za-z .'-]+)\s+vs\.?\s+([A-Za-z .'-]+)", title)
        if m_vs:
            away_raw, home_raw = m_vs.group(1), m_vs.group(2)
            away_code = _normalize_team(away_raw)
            home_code = _normalize_team(home_raw)
            if away_code and home_code:
                return away_code, home_code, f"{source}_title"

    return None, None, "unparsed_html"


def _is_strong_method(method: Optional[str]) -> bool:
    return bool(method) and (method == "inferred_consensus" or method.endswith("_jsonld") or method.endswith("_title"))


def _is_weak_method(method: Optional[str]) -> bool:
    return not _is_strong_method(method)


def resolve_covers_matchup_teams(mid: str, url: str, debug: bool = False) -> tuple[Optional[str], Optional[str], str]:
    cache_dir = Path("data/cache/covers")
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"matchup_{mid}.json"
    cache_data = None
    if cache_path.exists():
        try:
            cache_data = json.loads(cache_path.read_text(encoding="utf-8"))
            away_c = cache_data.get("away")
            home_c = cache_data.get("home")
            if away_c in data_store.teams and home_c in data_store.teams:
                return away_c, home_c, f"cache_{cache_data.get('method', 'unknown')}"
        except Exception:
            cache_data = None

    html = None
    method = "fetch_failed"
    a_code = h_code = None
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.covers.com/",
    }
    try:
        try:
            import requests  # type: ignore

            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                html = resp.text
            else:
                method = f"fetch_failed_{resp.status_code}"
        except Exception:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as r:  # type: ignore
                html_bytes = r.read()
                html = html_bytes.decode("utf-8", errors="ignore")
    except Exception:
        html = None

    if not html:
        return None, None, method

    def to_code(name: Optional[str]) -> Optional[str]:
        if not name:
            return None
        matches = data_store.lookup_team_code(name)
        if len(matches) == 1:
            return next(iter(matches))
        return None

    def parse_pair(text: Optional[str]) -> tuple[Optional[str], Optional[str], Optional[str]]:
        if not text:
            return None, None, None
        m = re.search(r"([A-Za-z .'-]+)\s+vs\.?\s+([A-Za-z .'-]+)", text)
        if not m:
            return None, None, None
        a_raw, h_raw = m.group(1), m.group(2)
        a_code, h_code = to_code(a_raw), to_code(h_raw)
        if a_code and h_code:
            return a_code, h_code, "title"
        return None, None, None

    # JSON-LD parse
    for m in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.IGNORECASE | re.DOTALL):
        raw_json = m.group(1).strip()
        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError:
            # Handle malformed JSON with extra trailing braces
            depth = 0
            end_pos = 0
            for i, c in enumerate(raw_json):
                if c == '{':
                    depth += 1
                elif c == '}':
                    depth -= 1
                    if depth == 0:
                        end_pos = i + 1
                        break
            if end_pos > 0:
                try:
                    data = json.loads(raw_json[:end_pos])
                except Exception:
                    continue
            else:
                continue
        except Exception:
            continue
        objs = data if isinstance(data, list) else [data]
        for obj in objs:
            if not isinstance(obj, dict):
                continue
            if obj.get("@type") == "SportsEvent":
                home = obj.get("homeTeam") or {}
                away = obj.get("awayTeam") or {}
                if isinstance(home, dict):
                    home = home.get("name")
                if isinstance(away, dict):
                    away = away.get("name")
                a_code, h_code = to_code(away), to_code(home)
                if a_code and h_code:
                    method = "jsonld"
                    break
            title_like = obj.get("name") or obj.get("headline")
            a_code = h_code = None
            if isinstance(title_like, str):
                a_code, h_code, _ = parse_pair(title_like)
                if a_code and h_code:
                    method = "jsonld_title"
                    break
        if html and method in {"jsonld", "jsonld_title"}:
            break

    if method not in {"jsonld", "jsonld_title"}:
        for tag in ["og:title", "twitter:title"]:
            pat = rf'<meta[^>]+(?:property|name)=["\']{tag}["\'][^>]+content=["\']([^"\']+)["\']'
            mt = re.search(pat, html, re.IGNORECASE)
            if mt:
                a_code, h_code, _ = parse_pair(mt.group(1))
                if a_code and h_code:
                    method = f"{tag.replace(':','_')}"
                    break

    if method.startswith("fetch_failed"):
        a_code = h_code = None

    if method not in {"jsonld", "jsonld_title"} and not method.startswith("og") and not method.startswith("twitter"):
        a_code = h_code = None
        a_code, h_code, parsed_method = parse_pair(re.search(r"<title>([^<]+)</title>", html, re.IGNORECASE).group(1) if re.search(r"<title>([^<]+)</title>", html, re.IGNORECASE) else None)
        if a_code and h_code:
            method = parsed_method or "title"

    if not (a_code and h_code):
        # header fallback
        hdr = re.search(r"([A-Za-z .'-]+)\s+vs\.?\s+([A-Za-z .'-]+)", html)
        if hdr:
            a_code, h_code = to_code(hdr.group(1)), to_code(hdr.group(2))
            if a_code and h_code:
                method = "header"

    if a_code and h_code:
        cache_payload = {
            "mid": mid,
            "away": a_code,
            "home": h_code,
            "method": method,
            "fetched_at_utc": datetime.utcnow().isoformat() + "Z",
            "url": url,
        }
        try:
            cache_path.write_text(json.dumps(cache_payload, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass
        return a_code, h_code, method

    return None, None, "unresolved"


def _infer_teams_from_text(text: str) -> list[str]:
    tokens = re.findall(r"[A-Za-z][A-Za-z\-']+", text or "")
    found: list[str] = []
    n = len(tokens)
    for i in range(n):
        spans = [tokens[i]]
        if i + 1 < n:
            spans.append(f"{tokens[i]} {tokens[i+1]}")
        for span in spans:
            codes = data_store.lookup_team_code(span)
            if len(codes) == 1:
                code = next(iter(codes))
                if code not in found:
                    found.append(code)
            if len(found) >= 2:
                return found[:2]
    return found[:2]


def is_covers_game_total(raw_pick_text: str) -> tuple[bool, Optional[float], Optional[str]]:
    text_raw = raw_pick_text or ""
    text = re.sub(r"\s+", " ", text_raw).strip().lower()

    # Must begin with over/under followed by a numeric total
    m = re.match(r"^(over|under)\s+([0-9]+(?:\.5)?)\s*(?:\(?[+-]\d{2,4}\)?\s*)?$", text)
    if not m:
        return False, None, None

    direction = m.group(1).upper()
    try:
        line = float(m.group(2))
    except ValueError:
        return False, None, None

    if not (150 <= line <= 300):
        return False, None, None

    # Exclude player-ish text or stat props
    stat_keywords = [
        "points",
        "rebounds",
        "assists",
        "steals",
        "blocks",
        "pra",
        "points+rebounds+assists",
        "pts+reb+ast",
        "3-pointers",
        "3 pointers",
    ]
    if any(k in text for k in stat_keywords):
        return False, None, None

    if re.search(r"\b[A-Z][a-z]+\s+[A-Z][a-z]+\b", text_raw):
        return False, None, None

    return True, line, direction


def is_covers_spread(raw_pick_text: str) -> tuple[bool, Optional[str], Optional[float]]:
    text = re.sub(r"\s+", " ", (raw_pick_text or "")).strip()
    m = re.match(r"(?i)^([A-Za-z]{2,4})\s*([+-]\d+(?:\.5)?)\s*(?:\(?[+-]\d{2,4}\)?\s*)?$", text)
    if not m:
        return False, None, None

    team = m.group(1).upper()
    try:
        line = float(m.group(2))
    except ValueError:
        return False, None, None

    if abs(line) > 40:
        return False, None, None
    return True, team, line


def is_covers_player_prop(raw_pick_text: str, raw_block: str) -> bool:
    text_raw = f"{raw_pick_text or ''} {raw_block or ''}"
    text_norm = normalize_text(text_raw)

    # Exclude totals/spreads that match strict detectors
    if is_covers_spread(raw_pick_text)[0] or is_covers_game_total(raw_pick_text)[0]:
        return False

    stat_keywords = [
        "points scored",
        "points",
        "rebounds",
        "assists",
        "steals",
        "blocks",
        "pra",
        "pts+reb+ast",
        "points+rebounds+assists",
        "3-pointers",
        "3 pointers",
        "3-pointers made",
        "total rebounds",
        "total assists",
        "total points",
    ]
    label_keywords = ["projection", "points scored", "total rebounds", "total assists", "3-pointers made"]

    has_stat = any(k in text_norm for k in stat_keywords)
    has_label = any(k in text_norm for k in label_keywords)

    return has_stat or has_label


def _safe_write_normalized(out_path: str, records: list, source_label: str = "") -> None:
    """Write normalized records, but protect existing data from empty overwrite."""
    if not records and os.path.exists(out_path):
        try:
            with open(out_path, "r") as f:
                existing = json.load(f)
            if existing:
                print(f"  [PROTECTED] {source_label or out_path}: scraper returned 0 records, keeping existing {len(existing)} records")
                return
        except (json.JSONDecodeError, OSError):
            pass
    write_json(out_path, records)


def normalize_file(raw_path: str, out_path: str, debug: bool = False) -> List[Dict[str, Any]]:
    if not os.path.exists(raw_path):
        _safe_write_normalized(out_path, [], os.path.basename(out_path))
        return []

    try:
        with open(raw_path, "r", encoding="utf-8") as f:
            raw_records = json.load(f)
    except json.JSONDecodeError:
        raw_records = []

    # Build Covers matchup_id -> (away, home, method) map from player props (if teams present)
    covers_matchups: Dict[str, tuple[Optional[str], Optional[str], str]] = {}
    covers_seen_order: Dict[str, list[str]] = {}
    covers_mids: set[str] = set()
    covers_inferred_pairs: Dict[str, Counter[tuple[str, str]]] = {}
    resolved_cache = resolved_fetch = resolved_failed = 0
    props_missing_event_key = 0

    def _record_inferred_pair(mid: str, codes: List[str]) -> None:
        if len(codes) < 2:
            return
        c1, c2 = codes[0], codes[1]
        if not (c1 and c2):
            return
        pair_key = tuple(sorted([c1, c2]))
        covers_inferred_pairs.setdefault(mid, Counter())
        covers_inferred_pairs[mid][pair_key] += 1
    if isinstance(raw_records, list):
        for item in raw_records:
            if not isinstance(item, dict):
                continue
            if item.get("source_id") != "covers":
                continue
            mid = _covers_mid_from_url(item.get("canonical_url"))
            if not mid:
                continue
            covers_mids.add(mid)
            is_prop_family = (item.get("market_family") or "").lower() == "player_prop" or is_covers_player_prop(item.get("raw_pick_text") or "", item.get("raw_block") or "")
            if is_prop_family:
                # For props, do not rely on inferred text; resolver will be used later.
                pass
            else:
                a = _normalize_team(item.get("away_team"))
                h = _normalize_team(item.get("home_team"))
                inferred_sources = [
                    item.get("raw_block"),
                    item.get("raw_pick_text"),
                    f"{item.get('raw_block') or ''} {item.get('raw_pick_text') or ''}",
                ]
                if not (a and h):
                    for src_text in inferred_sources:
                        inferred = _infer_teams_from_text(src_text or "")
                        if len(inferred) >= 2:
                            a = a or inferred[0]
                            h = h or inferred[1]
                            _record_inferred_pair(mid, inferred)
                            break
                else:
                    for src_text in inferred_sources:
                        inferred = _infer_teams_from_text(src_text or "")
                        if len(inferred) >= 2:
                            _record_inferred_pair(mid, inferred)
                            break
                if mid not in covers_matchups or (a and h and (covers_matchups[mid][0] is None or covers_matchups[mid][1] is None)):
                    covers_matchups[mid] = (a, h, "inferred_prop")
            # Track team codes appearing in standard picks to use as fallback
            parsed = parse_standard_market(item.get("raw_pick_text") or "") if (item.get("market_family") or "").lower() == "standard" else None
            team_code = None
            if parsed:
                team_code = _normalize_team(parsed.get("team_code"))
            if team_code:
                covers_seen_order.setdefault(mid, [])
                if team_code not in covers_seen_order[mid]:
                    covers_seen_order[mid].append(team_code)
            else:
                inferred = _infer_teams_from_text(item.get("raw_block") or item.get("raw_pick_text") or "")
                if inferred:
                    covers_seen_order.setdefault(mid, [])
                    for code in inferred:
                        if code not in covers_seen_order[mid]:
                            covers_seen_order[mid].append(code)
    # Fill missing matchup map using team codes seen across picks
    for mid, teams in covers_seen_order.items():
        if mid in covers_matchups and covers_matchups[mid][0] and covers_matchups[mid][1]:
            continue
        if len(teams) >= 2:
            covers_matchups[mid] = (teams[0], teams[1], "seen_order")
            _record_inferred_pair(mid, teams[:2])
    def maybe_promote_mid(mid: str, debug_flag: bool = False) -> bool:
        counter = covers_inferred_pairs.get(mid, Counter())
        if not counter:
            return False
        top = counter.most_common(2)
        top_pair, top_count = top[0]
        next_count = top[1][1] if len(top) > 1 else 0
        total = sum(counter.values())
        if top_count >= 2 and top_count >= next_count + 1:
            away_prom, home_prom = top_pair
            order_list = covers_seen_order.get(mid, [])
            if order_list and away_prom in order_list and home_prom in order_list:
                try:
                    idx_a = order_list.index(away_prom)
                    idx_h = order_list.index(home_prom)
                    if idx_h < idx_a:
                        away_prom, home_prom = home_prom, away_prom
                except ValueError:
                    pass
            covers_matchups[mid] = (away_prom, home_prom, "inferred_consensus")
            if debug_flag:
                print(f"[DEBUG covers] promoted inferred matchup: mid={mid} away={away_prom} home={home_prom} count={top_count} via=inferred_consensus")
            return True
        else:
            if debug_flag and total >= 2:
                print(f"[DEBUG covers] promotion_candidate: mid={mid} pairs={counter.most_common()} chosen=None")
        return False

    # Upgrade weak/incomplete mids via fetch
    for mid, (a, h, method) in list(covers_matchups.items()):
        if _is_strong_method(method):
            continue
        a2, h2, m2 = _fetch_covers_matchup_teams(mid)
        if a2 and h2 and _is_strong_method(m2):
            covers_matchups[mid] = (a2, h2, m2)
            if debug:
                print(f"[DEBUG covers] upgraded matchup teams: mid={mid} away={a2} home={h2} via={m2} (was {method})")
        else:
            maybe_promote_mid(mid, debug_flag=debug)
    # Promote consistent inferred pairs to inferred_consensus (for mids not covered above)
    for mid in covers_inferred_pairs:
        if mid not in covers_matchups or _is_weak_method(covers_matchups[mid][2]):
            maybe_promote_mid(mid, debug_flag=debug)
    # Last-resort fetch for mids seen but not mapped
    for mid in covers_mids:
        current = covers_matchups.get(mid)
        need_fetch = current is None or not (current[0] and current[1])
        if not need_fetch:
            continue
        away_f, home_f, via = _fetch_covers_matchup_teams(mid)
        if away_f and home_f:
            covers_matchups[mid] = (away_f, home_f, via)
            if debug:
                print(f"[DEBUG covers] fetched matchup teams: mid={mid} away={away_f} home={home_f} via={via}")
    if debug and covers_matchups:
        print(f"[DEBUG covers] matchup_ids from props: {len(covers_matchups)}")

    totals_ct = spreads_ct = props_ct = enriched_std_ct = skipped_props_ct = 0
    normalized: List[Dict[str, Any]] = []
    mid_team_cache: Dict[str, tuple[str, str, str]] = {}
    if isinstance(raw_records, list):
        for item in raw_records:
            if isinstance(item, dict):
                enriched_now = False
                is_prop = item.get("source_id") == "covers" and (
                    (item.get("market_family") or "").lower() == "player_prop"
                    or is_covers_player_prop(item.get("raw_pick_text") or "", item.get("raw_block") or "")
                )
                if item.get("source_id") == "covers":
                    mid = _covers_mid_from_url(item.get("canonical_url"))
                    if is_prop and debug:
                        print(f"[DEBUG covers] pre_prop: url={item.get('canonical_url')} mid={mid} away={item.get('away_team')} home={item.get('home_team')}")
                    if is_prop and mid is None:
                        if debug:
                            print(f"[DEBUG covers] prop_missing_mid url={item.get('canonical_url')}")
                    if mid:
                        if mid in mid_team_cache:
                            away_cached, home_cached, via_cached = mid_team_cache[mid]
                            if (item.get("away_team") is None):
                                item["away_team"] = away_cached
                            if (item.get("home_team") is None):
                                item["home_team"] = home_cached
                            item["_covers_matchup_method"] = via_cached
                            enriched_now = True
                        if (item.get("away_team") is None or item.get("home_team") is None):
                            away_res, home_res, via = resolve_covers_matchup_teams(mid, item.get("canonical_url") or "", debug=debug)
                            if away_res and home_res:
                                item["away_team"] = item.get("away_team") or away_res
                                item["home_team"] = item.get("home_team") or home_res
                                item["_covers_matchup_method"] = via
                                mid_team_cache[mid] = (item["away_team"], item["home_team"], via)
                                enriched_now = True
                                if via.startswith("cache"):
                                    resolved_cache += 1
                                elif via.startswith("fetch_failed") or via == "unresolved":
                                    resolved_failed += 1
                                else:
                                    resolved_fetch += 1
                            else:
                                resolved_failed += 1
                        if not enriched_now and mid in covers_matchups:
                            away_enr, home_enr, method = covers_matchups[mid]
                            if away_enr and home_enr:
                                if (item.get("away_team") is None or item.get("away_team") == ""):
                                    item["away_team"] = away_enr
                                    enriched_now = True
                                if (item.get("home_team") is None or item.get("home_team") == ""):
                                    item["home_team"] = home_enr
                                    enriched_now = True
                                item["_covers_matchup_method"] = method
                                mid_team_cache[mid] = (item["away_team"], item["home_team"], method)
                        if debug and (is_prop or enriched_now):
                            print(f"[DEBUG covers] post_prop: mid={mid} away={item.get('away_team')} home={item.get('home_team')} via={item.get('_covers_matchup_method')}")
                    if is_prop and not enriched_now:
                        # last-resort: infer from text for props if resolver failed
                        inferred = _infer_teams_from_text(f"{item.get('raw_block') or ''} {item.get('raw_pick_text') or ''}")
                        if len(inferred) < 2:
                            text_all = f"{item.get('raw_block') or ''} {item.get('raw_pick_text') or ''}".lower()
                            name_map = {
                                "embiid": "PHI",
                                "harden": "PHI",
                                "maxey": "PHI",
                                "kawhi": "LAC",
                                "leonard": "LAC",
                                "zubac": "LAC",
                                "george": "LAC",
                                "clippers": "LAC",
                                "sixers": "PHI",
                                "76ers": "PHI",
                            }
                            teams_guess = []
                            for key, code in name_map.items():
                                if key in text_all and code not in teams_guess:
                                    teams_guess.append(code)
                                if len(teams_guess) >= 2:
                                    break
                            inferred = teams_guess
                        if len(inferred) >= 2:
                            item["away_team"] = item.get("away_team") or inferred[0]
                            item["home_team"] = item.get("home_team") or inferred[1]
                            item["_covers_matchup_method"] = "inferred_text_prop"
                            if mid:
                                mid_team_cache[mid] = (item["away_team"], item["home_team"], "inferred_text_prop")
                            enriched_now = True
                            if debug:
                                print(f"[DEBUG covers] fallback infer_prop: mid={mid} away={item.get('away_team')} home={item.get('home_team')}")
                        if not enriched_now:
                            skipped_props_ct += 1
                            if debug:
                                print(f"[DEBUG covers] could_not_enrich_prop: mid={mid} reason=no_matchup_or_unresolved text={item.get('raw_pick_text')}")

                normalized_record = normalize_raw_record(item)
                normalized.append(normalized_record)

                if item.get("source_id") == "covers":
                    mt = normalized_record.get("market", {}).get("market_type")
                    if mt == "total":
                        totals_ct += 1
                    elif mt == "spread":
                        spreads_ct += 1
                    elif mt == "player_prop":
                        props_ct += 1
                    if mt in {"spread", "total", "moneyline"} and enriched_now:
                        enriched_std_ct += 1
                        if debug:
                            mid = _covers_mid_from_url(item.get("canonical_url"))
                            line = normalized_record.get("market", {}).get("line")
                            print(f"[DEBUG covers] enrich ({mt}): mid={mid} away={item.get('away_team')} home={item.get('home_team')} line={line}")
    if debug:
        print(f"[DEBUG covers classify] totals={totals_ct} spreads={spreads_ct} props={props_ct} enriched_standard={enriched_std_ct} skipped_props={skipped_props_ct}")
        missing_props = [
            r
            for r in normalized
            if (r.get("provenance") or {}).get("source_id") == "covers"
            and (r.get("market") or {}).get("market_type") == "player_prop"
            and ("@" not in ((r.get("event") or {}).get("event_key") or ""))
        ]
        props_missing_event_key = len(missing_props)
        print(
            f"[DEBUG covers] resolve summary: cache={resolved_cache} fetch={resolved_fetch} failed={resolved_failed} props_missing_event_key={props_missing_event_key}"
        )
        if props_missing_event_key > 0:
            samples = [
                (
                    (p.get("provenance") or {}).get("canonical_url"),
                    _covers_mid_from_url((p.get("provenance") or {}).get("canonical_url")),
                    (p.get("event") or {}).get("event_key"),
                )
                for p in missing_props[:5]
            ]
            print(f"[DEBUG covers] missing props samples: {samples}")

    _safe_write_normalized(out_path, normalized, os.path.basename(out_path))
    return normalized


__all__ = ["normalize_raw_record", "normalize_file", "build_event_key"]
