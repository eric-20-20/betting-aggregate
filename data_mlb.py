from typing import Dict, List

from models import PropStatAlias, Team, TeamAlias
from utils import normalize_text


MLB_TEAM_SEED: List[Team] = [
    Team(code="ARI", city="Arizona", nickname="Diamondbacks"),
    Team(code="ATL", city="Atlanta", nickname="Braves"),
    Team(code="BAL", city="Baltimore", nickname="Orioles"),
    Team(code="BOS", city="Boston", nickname="Red Sox"),
    Team(code="CHC", city="Chicago", nickname="Cubs"),
    Team(code="CHW", city="Chicago", nickname="White Sox"),
    Team(code="CIN", city="Cincinnati", nickname="Reds"),
    Team(code="CLE", city="Cleveland", nickname="Guardians"),
    Team(code="COL", city="Colorado", nickname="Rockies"),
    Team(code="DET", city="Detroit", nickname="Tigers"),
    Team(code="HOU", city="Houston", nickname="Astros"),
    Team(code="KCR", city="Kansas City", nickname="Royals"),
    Team(code="LAA", city="Los Angeles", nickname="Angels"),
    Team(code="LAD", city="Los Angeles", nickname="Dodgers"),
    Team(code="MIA", city="Miami", nickname="Marlins"),
    Team(code="MIL", city="Milwaukee", nickname="Brewers"),
    Team(code="MIN", city="Minnesota", nickname="Twins"),
    Team(code="NYM", city="New York", nickname="Mets"),
    Team(code="NYY", city="New York", nickname="Yankees"),
    Team(code="OAK", city="Oakland", nickname="Athletics"),
    Team(code="PHI", city="Philadelphia", nickname="Phillies"),
    Team(code="PIT", city="Pittsburgh", nickname="Pirates"),
    Team(code="SDP", city="San Diego", nickname="Padres"),
    Team(code="SEA", city="Seattle", nickname="Mariners"),
    Team(code="SFG", city="San Francisco", nickname="Giants"),
    Team(code="STL", city="St. Louis", nickname="Cardinals"),
    Team(code="TBR", city="Tampa Bay", nickname="Rays"),
    Team(code="TEX", city="Texas", nickname="Rangers"),
    Team(code="TOR", city="Toronto", nickname="Blue Jays"),
    Team(code="WSN", city="Washington", nickname="Nationals"),
]


def build_mlb_team_aliases() -> List[TeamAlias]:
    aliases: List[TeamAlias] = []
    shorthand: Dict[str, List[str]] = {
        "ARI": ["D-Backs", "Dbacks", "Diamondbacks", "Arizona Dbacks"],
        "ATL": ["Braves"],
        "BAL": ["Orioles", "O's"],
        "BOS": ["Red Sox", "Boston Red Sox"],
        "CHC": ["Cubs", "Chicago Cubs"],
        "CHW": ["White Sox", "Chi Sox", "Chicago White Sox"],
        "CIN": ["Reds", "Cincinnati Reds"],
        "CLE": ["Guardians", "Cleveland Guardians"],
        "COL": ["Rockies", "Colorado Rockies"],
        "DET": ["Tigers", "Detroit Tigers"],
        "HOU": ["Astros", "Houston Astros"],
        "KCR": ["Royals", "Kansas City Royals", "KC Royals", "KC"],
        "LAA": ["Angels", "Los Angeles Angels", "LA Angels"],
        "LAD": ["Dodgers", "Los Angeles Dodgers", "LA Dodgers"],
        "MIA": ["Marlins", "Miami Marlins"],
        "MIL": ["Brewers", "Milwaukee Brewers"],
        "MIN": ["Twins", "Minnesota Twins"],
        "NYM": ["Mets", "New York Mets"],
        "NYY": ["Yankees", "New York Yankees"],
        "OAK": ["Athletics", "A's", "Oakland Athletics"],
        "PHI": ["Phillies", "Philadelphia Phillies"],
        "PIT": ["Pirates", "Pittsburgh Pirates"],
        "SDP": ["Padres", "San Diego Padres", "SD Padres", "San Diego"],
        "SEA": ["Mariners", "Seattle Mariners"],
        "SFG": ["Giants", "San Francisco Giants", "SF Giants", "San Francisco"],
        "STL": ["Cardinals", "St Louis Cardinals", "St. Louis Cardinals"],
        "TBR": ["Rays", "Tampa Bay Rays", "Tampa"],
        "TEX": ["Rangers", "Texas Rangers"],
        "TOR": ["Blue Jays", "Toronto Blue Jays", "Jays"],
        "WSN": ["Nationals", "Washington Nationals", "Nats"],
    }

    for team in MLB_TEAM_SEED:
        alias_strings = {
            f"{team.city} {team.nickname}",
            team.nickname,
            team.code,
        }
        alias_strings.update(shorthand.get(team.code, []))
        for alias in alias_strings:
            aliases.append(
                TeamAlias(
                    team_code=team.code,
                    alias=alias,
                    normalized_alias=normalize_text(alias),
                )
            )
    return aliases


def build_mlb_prop_stat_aliases() -> List[PropStatAlias]:
    return []
