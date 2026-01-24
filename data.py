from typing import Dict, List

from models import PropStatAlias, Team, TeamAlias
from utils import normalize_text


TEAM_SEED: List[Team] = [
    Team(code="ATL", city="Atlanta", nickname="Hawks"),
    Team(code="BOS", city="Boston", nickname="Celtics"),
    Team(code="BKN", city="Brooklyn", nickname="Nets"),
    Team(code="CHA", city="Charlotte", nickname="Hornets"),
    Team(code="CHI", city="Chicago", nickname="Bulls"),
    Team(code="CLE", city="Cleveland", nickname="Cavaliers"),
    Team(code="DAL", city="Dallas", nickname="Mavericks"),
    Team(code="DEN", city="Denver", nickname="Nuggets"),
    Team(code="DET", city="Detroit", nickname="Pistons"),
    Team(code="GSW", city="Golden State", nickname="Warriors"),
    Team(code="HOU", city="Houston", nickname="Rockets"),
    Team(code="IND", city="Indiana", nickname="Pacers"),
    Team(code="LAC", city="Los Angeles", nickname="Clippers"),
    Team(code="LAL", city="Los Angeles", nickname="Lakers"),
    Team(code="MEM", city="Memphis", nickname="Grizzlies"),
    Team(code="MIA", city="Miami", nickname="Heat"),
    Team(code="MIL", city="Milwaukee", nickname="Bucks"),
    Team(code="MIN", city="Minnesota", nickname="Timberwolves"),
    Team(code="NOP", city="New Orleans", nickname="Pelicans"),
    Team(code="NYK", city="New York", nickname="Knicks"),
    Team(code="OKC", city="Oklahoma City", nickname="Thunder"),
    Team(code="ORL", city="Orlando", nickname="Magic"),
    Team(code="PHI", city="Philadelphia", nickname="76ers"),
    Team(code="PHX", city="Phoenix", nickname="Suns"),
    Team(code="POR", city="Portland", nickname="Trail Blazers"),
    Team(code="SAC", city="Sacramento", nickname="Kings"),
    Team(code="SAS", city="San Antonio", nickname="Spurs"),
    Team(code="TOR", city="Toronto", nickname="Raptors"),
    Team(code="UTA", city="Utah", nickname="Jazz"),
    Team(code="WAS", city="Washington", nickname="Wizards"),
]


def build_team_aliases() -> List[TeamAlias]:
    aliases: List[TeamAlias] = []
    shorthand: Dict[str, List[str]] = {
        "LAL": ["LA Lakers"],
        "LAC": ["LA Clippers"],
        "GSW": ["Golden State Warriors", "Dubs"],
        "NYK": ["NY Knicks", "NY Knick", "Knick"],
        "NOP": ["New Orleans Pelicans", "Pelicans"],
        "PHI": ["Sixers", "76ers", "Philadelphia Sixers"],
        "PHX": ["Phoenix Suns"],
        "SAS": ["San Antonio Spurs"],
        "UTA": ["Utah Jazz"],
    }

    for team in TEAM_SEED:
        alias_strings = {
            f"{team.city} {team.nickname}",
            team.nickname,
            team.code,
        }
        alias_strings.update(shorthand.get(team.code, []))

        for alias in alias_strings:
            normalized = normalize_text(alias)
            aliases.append(
                TeamAlias(
                    team_code=team.code,
                    alias=alias,
                    normalized_alias=normalized,
                )
            )

    return aliases


PROP_STAT_ALIASES_SEED: Dict[str, List[str]] = {
    "points": ["points", "point", "pts", "points scored"],
    "rebounds": ["rebounds", "rebound", "reb", "boards", "rebs"],
    "assists": ["assists", "assist", "ast", "asts"],
    "threes_made": [
        "3pt",
        "3pts",
        "three pointers",
        "threes",
        "3pm",
        "3 point",
        "3pt made",
    ],
    "pts_reb_ast": [
        "pra",
        "pts reb ast",
        "pts rebs asts",
        "points rebounds assists",
        "points+rebounds+assists",
        "pts+rebs+asts",
    ],
    "pts_reb": [
        "pr",
        "pts reb",
        "pts rebs",
        "points rebs",
        "points rebounds",
        "points+rebounds",
        "points+rebs",
        "pts+reb",
        "pts+rebs",
    ],
    "pts_ast": ["pa", "pts ast", "points assists", "points+assists", "pts+ast"],
    "reb_ast": [
        "ra",
        "reb ast",
        "rebounds assists",
        "rebounds+assists",
        "rebs+ast",
        "reb+ast",
        "rebs asts",
        "rebs+asts",
    ],
    "steals": ["steals", "steal", "stl", "stls"],
    "blocks": ["blocks", "block", "blk", "blks"],
}


def build_prop_stat_aliases() -> List[PropStatAlias]:
    aliases: List[PropStatAlias] = []
    for stat_key, alias_strings in PROP_STAT_ALIASES_SEED.items():
        for alias in alias_strings:
            aliases.append(
                PropStatAlias(
                    stat_key=stat_key,
                    alias=alias,
                    normalized_alias=normalize_text(alias),
                )
            )
    return aliases
