from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Dict, Optional, Set, Tuple

from betql_utils import slugify_player_name

STATIC_ALIAS_MAP: Dict[str, str] = {
    "l_jame": "lebron_james",
    "l_james": "lebron_james",
    "d_mitchell": "donovan_mitchell",
    "c_pencer": "cam_spencer",
    "c_spencer": "cam_spencer",
    "k_town": "karl_anthony_towns",
    "k_towns": "karl_anthony_towns",
    "j_ugg": "jalen_suggs",
    "j_sugg": "jalen_suggs",
    "j_on": "jalen_johnson",
    "j_johnson": "jalen_johnson",
    "d_green": "draymond_green",
    "k_durant": "kevin_durant",
    "s_curry": "stephen_curry",
    "j_brown": "jaylen_brown",
    "a_davis": "anthony_davis",
}

_ALIAS_CACHE: Dict[str, str] = {}
_ALIAS_SOURCE: str = ""


def _load_json(path: Path) -> Dict[str, str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return {str(k): str(v) for k, v in data.items()}
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}
    return {}


def _tokenize_slug(slug: str) -> Tuple[str, ...]:
    return tuple(t for t in slug.split("_") if t)


def _derive_dynamic_aliases(out_dir: Path) -> Dict[str, str]:
    """
    Build alias map by comparing abbreviated slugs from action/covers to full slugs from BetQL.
    Only add unambiguous matches.
    """
    betql_path = out_dir / "normalized_betql_prop_nba.json"
    action_path = out_dir / "normalized_action_nba.json"
    covers_path = out_dir / "normalized_covers_nba.json"

    def collect_slugs(path: Path) -> Set[str]:
        slugs: Set[str] = set()
        if not path.exists():
            return slugs
        try:
            import json

            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, list):
                return slugs
            for rec in data:
                mk = rec.get("market") if isinstance(rec, dict) else {}
                if not isinstance(mk, dict):
                    continue
                pk = mk.get("player_key")
                if isinstance(pk, str):
                    slug = pk.split(":", 1)[1] if pk.lower().startswith("nba:") else pk
                    slugs.add(slug)
        except Exception:
            return slugs
        return slugs

    full_slugs = collect_slugs(betql_path)
    abbrev_slugs = collect_slugs(action_path) | collect_slugs(covers_path)

    dynamic: Dict[str, str] = {}
    for short in abbrev_slugs:
        if short in full_slugs:
            continue
        tokens_short = _tokenize_slug(short)
        if not tokens_short:
            continue
        last_short = tokens_short[-1]
        candidates = []
        for full in full_slugs:
            tokens_full = _tokenize_slug(full)
            if not tokens_full:
                continue
            if tokens_full[-1] != last_short:
                continue
            # initials match check
            if len(tokens_short[0]) == 1 and tokens_full[0].startswith(tokens_short[0]):
                candidates.append(full)
            elif full.startswith(short):
                candidates.append(full)
        if len(candidates) == 1:
            dynamic[short] = candidates[0]
    return dynamic


def generate_dynamic_alias_map(out_dir: str = "out") -> Dict[str, str]:
    out_path = Path(out_dir) / "player_alias_map_nba.json"
    dynamic = _derive_dynamic_aliases(Path(out_dir))
    if dynamic:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(dynamic, f, ensure_ascii=False, indent=2, sort_keys=True)
    return dynamic


def load_alias_map(out_dir: str = "out") -> Dict[str, str]:
    global _ALIAS_CACHE, _ALIAS_SOURCE
    if _ALIAS_CACHE:
        return _ALIAS_CACHE
    out_path = Path(out_dir) / "player_alias_map_nba.json"
    merged = {}
    merged.update(STATIC_ALIAS_MAP)
    merged.update(_load_json(out_path))
    _ALIAS_CACHE = merged
    _ALIAS_SOURCE = str(out_path)
    return _ALIAS_CACHE


def normalize_player_slug(raw_slug: Optional[str], out_dir: str = "out") -> Optional[str]:
    if not raw_slug:
        return None
    slug = str(raw_slug)
    slug = slug.split(":", 1)[1] if slug.lower().startswith("nba:") else slug
    slug = slug.strip().lower()
    if slug in {"m_bridge", "m_bridges"}:
        return None
    aliases = load_alias_map(out_dir)
    if slug in aliases:
        # Always honor static aliases; for dynamically derived aliases, skip if the slug already
        # looks like an initial+lastname form (e.g., j_brunson) to keep canonical slugs stable.
        if slug in STATIC_ALIAS_MAP:
            return aliases[slug]
        parts = slug.split("_")
        if not (len(parts) >= 2 and len(parts[0]) == 1):
            return aliases[slug]
    # If the input already looks like a canonical slug (letters/digits/underscores), keep it.
    if re.fullmatch(r"[a-z][a-z0-9_]*", slug):
        return slug
    return slugify_player_name(slug) or slug


def normalize_player_key(player_key: Optional[str], out_dir: str = "out") -> Optional[str]:
    if not player_key or not isinstance(player_key, str):
        return player_key
    slug = normalize_player_slug(player_key, out_dir=out_dir)
    return f"NBA:{slug}" if slug else player_key


def alias_cache_info() -> Tuple[str, int]:
    return _ALIAS_SOURCE, len(_ALIAS_CACHE)
