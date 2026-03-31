import re
from typing import Optional, Tuple

from store import data_store
from utils import normalize_text


# Canonical atomic breakdowns for stat families.
ATOMIC_STAT_MAP = {
    "points": {"points"},
    "rebounds": {"rebounds"},
    "assists": {"assists"},
    "threes": {"threes"},
    "threes_made": {"threes"},
    "pts_reb": {"points", "rebounds"},
    "pts_ast": {"points", "assists"},
    "reb_ast": {"rebounds", "assists"},
    "pts_reb_ast": {"points", "rebounds", "assists"},
}


def normalize_stat_key(stat_key: str) -> str:
    """
    Normalize stat key/alias into canonical composite keys so downstream consensus can align.
    Examples: "Pts+Rebs" -> "pts_reb", "PRA" -> "pts_reb_ast".
    """
    raw = normalize_text(stat_key)
    raw = raw.replace("+", "_").replace(" ", "_")
    alias_map = {
        "points": "points",
        "point": "points",
        "pts": "points",
        "rebounds": "rebounds",
        "rebound": "rebounds",
        "reb": "rebounds",
        "rebs": "rebounds",
        "assists": "assists",
        "assist": "assists",
        "ast": "assists",
        "asts": "assists",
        "threes_made": "threes",
        "threes": "threes",
        "three_pointers": "threes",
        "3pm": "threes",
        "3pt": "threes",
        "3pts": "threes",
        "pts_reb": "pts_reb",
        "pts_rebs": "pts_reb",
        "points_rebounds": "pts_reb",
        "points_rebs": "pts_reb",
        "pr": "pts_reb",
        "pts_ast": "pts_ast",
        "points_assists": "pts_ast",
        "points_ast": "pts_ast",
        "pa": "pts_ast",
        "reb_ast": "reb_ast",
        "rebs_ast": "reb_ast",
        "rebounds_assists": "reb_ast",
        "rebounds_ast": "reb_ast",
        "ra": "reb_ast",
        "pts_reb_ast": "pts_reb_ast",
        "pts_reb_asts": "pts_reb_ast",
        "pts_rebs_ast": "pts_reb_ast",
        "pts_rebs_asts": "pts_reb_ast",
        "points_rebounds_assists": "pts_reb_ast",
        "points_rebs_assists": "pts_reb_ast",
        "points_rebs_ast": "pts_reb_ast",
        "pra": "pts_reb_ast",
    }
    return alias_map.get(raw, raw)


def atomic_stats(stat_key: str) -> set[str]:
    """
    Return the atomic stat components for a composite stat.
    Falls back to the normalized stat_key when unknown so we don't silently drop signals.
    """
    normalized = normalize_stat_key(stat_key)
    return ATOMIC_STAT_MAP.get(normalized, {normalized})


def map_teams(raw_text: str) -> Optional[Tuple[str, str]]:
    separators = re.split(r"@", raw_text)
    parts = [part.strip() for part in separators if part.strip()] if len(separators) > 1 else None
    if not parts or len(parts) != 2:
        parts = re.split(r"\b(at|vs|vs\.|v)\b", raw_text, flags=re.IGNORECASE)
        parts = [part.strip() for part in parts if part and part.lower() not in {"at", "vs", "vs.", "v"}]

    if len(parts) != 2:
        return None

    away_alias, home_alias = parts
    away_codes = data_store.lookup_team_code(away_alias)
    home_codes = data_store.lookup_team_code(home_alias)

    if away_codes & home_codes:
        return None

    if len(away_codes) != 1 or len(home_codes) != 1:
        return None

    away_code = next(iter(away_codes))
    home_code = next(iter(home_codes))
    if away_code == home_code:
        return None
    return away_code, home_code


def _extract_player_alias(raw_pick_text: str) -> str:
    normalized = normalize_text(raw_pick_text)
    tokens = normalized.split()

    stat_alias_tokens = set()
    for alias in data_store.prop_stat_alias_index.keys():
        stat_alias_tokens.update(alias.split())

    filtered_tokens = []
    skip_tokens = {"over", "under", "o", "u", "ou", "ov", "un"}
    for token in tokens:
        if token in skip_tokens:
            continue
        if token.isdigit():
            continue
        if token in stat_alias_tokens:
            continue
        filtered_tokens.append(token)

    candidate = " ".join(filtered_tokens).strip()
    return candidate or normalized


def map_player(raw_pick_text: str) -> str:
    candidate_alias = _extract_player_alias(raw_pick_text)
    existing_player_key = data_store.lookup_player_key(candidate_alias)
    if existing_player_key:
        return existing_player_key

    full_name = " ".join(word.capitalize() for word in candidate_alias.split()) or raw_pick_text.strip()
    return data_store.add_player(full_name=full_name, alias_text=candidate_alias, is_verified=False)


def map_prop_stat(raw_pick_text: str) -> Optional[str]:
    normalized = normalize_text(raw_pick_text)
    aliases = sorted(data_store.prop_stat_alias_index.keys(), key=len, reverse=True)
    composite_keys = {"pts_reb", "pts_ast", "reb_ast", "pts_reb_ast"}

    def match_alias(alias: str) -> Optional[str]:
        raw_stat_key = data_store.prop_stat_alias_index[alias]
        canonical = raw_stat_key if raw_stat_key == "threes_made" else normalize_stat_key(raw_stat_key)
        if f" {alias} " in f" {normalized} ":
            return canonical
        alias_tokens = alias.split()
        if len(alias_tokens) == 1 and alias_tokens[0] in normalized.split():
            return canonical
        return None

    composite_aliases = [a for a in aliases if normalize_stat_key(data_store.prop_stat_alias_index[a]) in composite_keys]
    for alias in composite_aliases:
        matched = match_alias(alias)
        if matched:
            return matched

    for alias in aliases:
        matched = match_alias(alias)
        if matched:
            return matched
    return None


if __name__ == "__main__":
    assert map_prop_stat("A.Thompson o23.5 Pts+Rebs -117") == "pts_reb", "Pts+Rebs should map to pts_reb"
    assert map_prop_stat("A.Thompson u17.5 Pts -112") == "points", "Pts should map to points"
    print(f"normalize_stat_key('Pts+Rebs') -> {normalize_stat_key('Pts+Rebs')}")
    print(f"atomic_stats('Pts+Rebs') -> {atomic_stats('Pts+Rebs')}")
    print(f"normalize_stat_key('PRA') -> {normalize_stat_key('PRA')}")
    print(f"atomic_stats('PRA') -> {atomic_stats('PRA')}")
    print("map_prop_stat smoke tests passed.")
