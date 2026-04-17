import hashlib
import json
import os
import shutil
import tempfile
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Set

# NOTE: This in-memory store is a temporary persistence layer.
# It will be replaced by a database-backed implementation later.

# Sport constants
NBA_SPORT = "NBA"
NCAAB_SPORT = "NCAAB"
MLB_SPORT = "MLB"
SPORT = NBA_SPORT  # Default for backward compatibility

from models import Player, PlayerAlias, PropStatAlias, Team, TeamAlias
from utils import normalize_text


class DataStore:
    def __init__(self, sport: str = NBA_SPORT) -> None:
        self.sport = sport
        self.reset()

    def reset(self) -> None:
        # Import sport-specific data lazily
        if self.sport == NCAAB_SPORT:
            from data_ncaab import (
                NCAAB_TEAM_SEED,
                build_ncaab_prop_stat_aliases,
                build_ncaab_team_aliases,
            )
            team_seed = NCAAB_TEAM_SEED
            team_aliases_func = build_ncaab_team_aliases
            prop_stat_aliases_func = build_ncaab_prop_stat_aliases
        elif self.sport == MLB_SPORT:
            from data_mlb import (
                MLB_TEAM_SEED,
                build_mlb_prop_stat_aliases,
                build_mlb_team_aliases,
            )
            team_seed = MLB_TEAM_SEED
            team_aliases_func = build_mlb_team_aliases
            prop_stat_aliases_func = build_mlb_prop_stat_aliases
        else:
            from data import TEAM_SEED, build_prop_stat_aliases, build_team_aliases
            team_seed = TEAM_SEED
            team_aliases_func = build_team_aliases
            prop_stat_aliases_func = build_prop_stat_aliases

        self.teams: Dict[str, Team] = {team.code: team for team in team_seed}
        self.team_aliases: List[TeamAlias] = team_aliases_func()
        self.team_alias_index: Dict[str, Set[str]] = defaultdict(set)
        for alias in self.team_aliases:
            self.team_alias_index[alias.normalized_alias].add(alias.team_code)

        self.players: Dict[str, Player] = {}
        self.player_aliases: List[PlayerAlias] = []
        self.player_alias_index: Dict[str, str] = {}

        self.prop_stat_aliases: List[PropStatAlias] = prop_stat_aliases_func()
        self.prop_stat_alias_index: Dict[str, str] = {
            alias.normalized_alias: alias.stat_key for alias in self.prop_stat_aliases
        }

    def lookup_team_code(self, alias_text: str) -> Set[str]:
        normalized = normalize_text(alias_text)
        return self.team_alias_index.get(normalized, set())

    def lookup_player_key(self, alias_text: str) -> str | None:
        normalized = normalize_text(alias_text)
        return self.player_alias_index.get(normalized)

    def add_player(self, full_name: str, alias_text: str, is_verified: bool = False) -> str:
        normalized_alias = normalize_text(alias_text)
        tokens = normalized_alias.split()
        base = "_".join(tokens) if tokens else "player"
        base_key = f"{self.sport}:{base}" if not base.startswith(f"{self.sport}:") else base

        player_key = base_key
        counter = 2
        while player_key in self.players:
            player_key = f"{base_key}_{counter}"
            counter += 1

        player = Player(player_key=player_key, full_name=full_name, is_verified=is_verified)
        self.players[player_key] = player

        alias = PlayerAlias(
            player_key=player_key,
            alias=alias_text,
            normalized_alias=normalized_alias,
        )
        self.player_aliases.append(alias)
        self.player_alias_index[normalized_alias] = player_key
        return player_key

    def lookup_stat_key(self, text: str) -> str | None:
        normalized = normalize_text(text)
        aliases = sorted(self.prop_stat_alias_index.keys(), key=len, reverse=True)
        for alias in aliases:
            stat_key = self.prop_stat_alias_index[alias]
            if f" {alias} " in f" {normalized} ":
                return stat_key
            if alias in normalized.split():
                return stat_key
        return None


# Default NBA store for backward compatibility
data_store = DataStore(NBA_SPORT)

# Sport-specific store cache
_sport_stores: Dict[str, DataStore] = {}


def get_data_store(sport: str = NBA_SPORT) -> DataStore:
    """Get or create a DataStore for the specified sport.

    Args:
        sport: Sport identifier (NBA_SPORT, NCAAB_SPORT, or MLB_SPORT)

    Returns:
        DataStore configured for the specified sport
    """
    global _sport_stores
    if sport not in _sport_stores:
        _sport_stores[sport] = DataStore(sport)
    return _sport_stores[sport]


def ensure_dir(path: str) -> None:
    """Create a directory if it does not exist."""
    if not path:
        return
    os.makedirs(path, exist_ok=True)


def write_json(path: str, obj) -> None:
    """Atomically write JSON with stable formatting."""
    directory = os.path.dirname(path) or "."
    ensure_dir(directory)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=directory, encoding="utf-8") as tmp:
        json.dump(obj, tmp, ensure_ascii=False, sort_keys=True, indent=2)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_path = tmp.name
    os.replace(tmp_path, path)


def append_jsonl(path: str, iterable: Iterable[object]) -> None:
    """Append objects as JSONL with fsync for durability."""
    directory = os.path.dirname(path) or "."
    ensure_dir(directory)
    with open(path, "a", encoding="utf-8") as f:
        for item in iterable:
            f.write(json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
            f.write("\n")
        f.flush()
        os.fsync(f.fileno())


def copy_file(src: str, dst: str) -> None:
    directory = os.path.dirname(dst) or "."
    ensure_dir(directory)
    shutil.copy2(src, dst)


def sha256_json(obj: object) -> str:
    payload = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
