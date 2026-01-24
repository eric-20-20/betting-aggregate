from dataclasses import dataclass


@dataclass
class Team:
    code: str
    city: str
    nickname: str


@dataclass
class TeamAlias:
    team_code: str
    alias: str
    normalized_alias: str


@dataclass
class Player:
    player_key: str
    full_name: str
    is_verified: bool


@dataclass
class PlayerAlias:
    player_key: str
    alias: str
    normalized_alias: str


@dataclass
class PropStatAlias:
    stat_key: str
    alias: str
    normalized_alias: str
