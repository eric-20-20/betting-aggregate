"""NCAAB team seed data and alias builders.

This module provides team data for college basketball (NCAAB).
Teams are organized by conference for easier maintenance.
"""

from typing import Dict, List

from models import PropStatAlias, Team, TeamAlias
from utils import normalize_text


# ============================================================================
# ACC (Atlantic Coast Conference)
# ============================================================================
ACC_TEAMS: List[Team] = [
    Team(code="BC", city="Boston College", nickname="Eagles"),
    Team(code="CAL", city="California", nickname="Golden Bears"),
    Team(code="CLEM", city="Clemson", nickname="Tigers"),
    Team(code="DUKE", city="Duke", nickname="Blue Devils"),
    Team(code="FSU", city="Florida State", nickname="Seminoles"),
    Team(code="GT", city="Georgia Tech", nickname="Yellow Jackets"),
    Team(code="LOU", city="Louisville", nickname="Cardinals"),
    Team(code="MIA", city="Miami", nickname="Hurricanes"),
    Team(code="UNC", city="North Carolina", nickname="Tar Heels"),
    Team(code="NCST", city="NC State", nickname="Wolfpack"),
    Team(code="ND", city="Notre Dame", nickname="Fighting Irish"),
    Team(code="PITT", city="Pittsburgh", nickname="Panthers"),
    Team(code="SMU", city="SMU", nickname="Mustangs"),
    Team(code="STAN", city="Stanford", nickname="Cardinal"),
    Team(code="SYR", city="Syracuse", nickname="Orange"),
    Team(code="UVA", city="Virginia", nickname="Cavaliers"),
    Team(code="VT", city="Virginia Tech", nickname="Hokies"),
    Team(code="WAKE", city="Wake Forest", nickname="Demon Deacons"),
]

# ============================================================================
# Big East
# ============================================================================
BIG_EAST_TEAMS: List[Team] = [
    Team(code="BUT", city="Butler", nickname="Bulldogs"),
    Team(code="CONN", city="Connecticut", nickname="Huskies"),
    Team(code="CREIGH", city="Creighton", nickname="Bluejays"),
    Team(code="DEP", city="DePaul", nickname="Blue Demons"),
    Team(code="GTOWN", city="Georgetown", nickname="Hoyas"),
    Team(code="MARQ", city="Marquette", nickname="Golden Eagles"),
    Team(code="PROV", city="Providence", nickname="Friars"),
    Team(code="HALL", city="Seton Hall", nickname="Pirates"),
    Team(code="STJO", city="St. John's", nickname="Red Storm"),
    Team(code="NOVA", city="Villanova", nickname="Wildcats"),
    Team(code="XAV", city="Xavier", nickname="Musketeers"),
]

# ============================================================================
# Big Ten
# ============================================================================
BIG_TEN_TEAMS: List[Team] = [
    Team(code="ILL", city="Illinois", nickname="Fighting Illini"),
    Team(code="IND", city="Indiana", nickname="Hoosiers"),
    Team(code="IOWA", city="Iowa", nickname="Hawkeyes"),
    Team(code="MD", city="Maryland", nickname="Terrapins"),
    Team(code="MICH", city="Michigan", nickname="Wolverines"),
    Team(code="MSU", city="Michigan State", nickname="Spartans"),
    Team(code="MINN", city="Minnesota", nickname="Golden Gophers"),
    Team(code="NEB", city="Nebraska", nickname="Cornhuskers"),
    Team(code="NW", city="Northwestern", nickname="Wildcats"),
    Team(code="OSU", city="Ohio State", nickname="Buckeyes"),
    Team(code="ORE", city="Oregon", nickname="Ducks"),
    Team(code="PSU", city="Penn State", nickname="Nittany Lions"),
    Team(code="PUR", city="Purdue", nickname="Boilermakers"),
    Team(code="RUT", city="Rutgers", nickname="Scarlet Knights"),
    Team(code="UCLA", city="UCLA", nickname="Bruins"),
    Team(code="USC", city="USC", nickname="Trojans"),
    Team(code="WASH", city="Washington", nickname="Huskies"),
    Team(code="WIS", city="Wisconsin", nickname="Badgers"),
]

# ============================================================================
# Big 12
# ============================================================================
BIG_12_TEAMS: List[Team] = [
    Team(code="ARIZ", city="Arizona", nickname="Wildcats"),
    Team(code="ASU", city="Arizona State", nickname="Sun Devils"),
    Team(code="BAY", city="Baylor", nickname="Bears"),
    Team(code="BYU", city="BYU", nickname="Cougars"),
    Team(code="CIN", city="Cincinnati", nickname="Bearcats"),
    Team(code="COL", city="Colorado", nickname="Buffaloes"),
    Team(code="HOU", city="Houston", nickname="Cougars"),
    Team(code="ISU", city="Iowa State", nickname="Cyclones"),
    Team(code="KAN", city="Kansas", nickname="Jayhawks"),
    Team(code="KSU", city="Kansas State", nickname="Wildcats"),
    Team(code="OSU", city="Oklahoma State", nickname="Cowboys"),
    Team(code="TCU", city="TCU", nickname="Horned Frogs"),
    Team(code="TTU", city="Texas Tech", nickname="Red Raiders"),
    Team(code="UCF", city="UCF", nickname="Knights"),
    Team(code="UTAH", city="Utah", nickname="Utes"),
    Team(code="WVU", city="West Virginia", nickname="Mountaineers"),
]

# ============================================================================
# SEC (Southeastern Conference)
# ============================================================================
SEC_TEAMS: List[Team] = [
    Team(code="ALA", city="Alabama", nickname="Crimson Tide"),
    Team(code="ARK", city="Arkansas", nickname="Razorbacks"),
    Team(code="AUB", city="Auburn", nickname="Tigers"),
    Team(code="FLA", city="Florida", nickname="Gators"),
    Team(code="UGA", city="Georgia", nickname="Bulldogs"),
    Team(code="UK", city="Kentucky", nickname="Wildcats"),
    Team(code="LSU", city="LSU", nickname="Tigers"),
    Team(code="MISS", city="Ole Miss", nickname="Rebels"),
    Team(code="MSST", city="Mississippi State", nickname="Bulldogs"),
    Team(code="MIZZ", city="Missouri", nickname="Tigers"),
    Team(code="OU", city="Oklahoma", nickname="Sooners"),
    Team(code="SCAR", city="South Carolina", nickname="Gamecocks"),
    Team(code="TENN", city="Tennessee", nickname="Volunteers"),
    Team(code="TEX", city="Texas", nickname="Longhorns"),
    Team(code="TAMU", city="Texas A&M", nickname="Aggies"),
    Team(code="VAN", city="Vanderbilt", nickname="Commodores"),
]

# ============================================================================
# Top Mid-Majors & Tournament Regulars
# ============================================================================
MID_MAJOR_TEAMS: List[Team] = [
    # West Coast Conference
    Team(code="GONZ", city="Gonzaga", nickname="Bulldogs"),
    Team(code="STMY", city="Saint Mary's", nickname="Gaels"),
    # American Athletic
    Team(code="MEM", city="Memphis", nickname="Tigers"),
    Team(code="TULN", city="Tulane", nickname="Green Wave"),
    # Mountain West
    Team(code="SDSU", city="San Diego State", nickname="Aztecs"),
    Team(code="BSU", city="Boise State", nickname="Broncos"),
    Team(code="NEV", city="Nevada", nickname="Wolf Pack"),
    Team(code="UNLV", city="UNLV", nickname="Rebels"),
    Team(code="CSU", city="Colorado State", nickname="Rams"),
    # Atlantic 10
    Team(code="DAY", city="Dayton", nickname="Flyers"),
    Team(code="VCU", city="VCU", nickname="Rams"),
    Team(code="RICH", city="Richmond", nickname="Spiders"),
    Team(code="STLO", city="Saint Louis", nickname="Billikens"),
    # Missouri Valley
    Team(code="DRKE", city="Drake", nickname="Bulldogs"),
    # Conference USA
    Team(code="FAU", city="Florida Atlantic", nickname="Owls"),
    # Sun Belt
    Team(code="JAX", city="Jacksonville State", nickname="Gamecocks"),
    # Ivy League
    Team(code="PRIN", city="Princeton", nickname="Tigers"),
    Team(code="YALE", city="Yale", nickname="Bulldogs"),
    Team(code="BRWN", city="Brown", nickname="Bears"),
    # CAA
    Team(code="CHAR", city="Charleston", nickname="Cougars"),
    # MAC (Mid-American Conference)
    Team(code="BGSU", city="Bowling Green", nickname="Falcons"),
    Team(code="MIO", city="Miami (Ohio)", nickname="RedHawks"),
    # Horizon
    Team(code="OAK", city="Oakland", nickname="Golden Grizzlies"),
    Team(code="MILW", city="Milwaukee", nickname="Panthers"),
    Team(code="DETM", city="Detroit Mercy", nickname="Titans"),
]


# Combine all teams
NCAAB_TEAM_SEED: List[Team] = (
    ACC_TEAMS
    + BIG_EAST_TEAMS
    + BIG_TEN_TEAMS
    + BIG_12_TEAMS
    + SEC_TEAMS
    + MID_MAJOR_TEAMS
)


def build_ncaab_team_aliases() -> List[TeamAlias]:
    """Build aliases for all NCAAB teams."""
    aliases: List[TeamAlias] = []

    # Additional shorthand/alternate names
    shorthand: Dict[str, List[str]] = {
        # ACC
        "UNC": ["North Carolina", "Carolina", "Tar Heels", "Heels"],
        "DUKE": ["Duke Blue Devils", "Dukies"],
        "NCST": ["N.C. State", "North Carolina State", "NC State"],
        "VT": ["VA Tech", "Hokies"],
        "BC": ["Boston Col"],
        "GT": ["Ga Tech", "Georgia Tech Yellow Jackets"],
        "FSU": ["Florida St", "Fla State", "Noles"],
        "UVA": ["Virginia Cavaliers", "Wahoos"],
        "ND": ["Notre Dame Fighting Irish"],
        "WAKE": ["Wake", "Deacs"],

        # Big East
        "CONN": ["UConn", "Connecticut Huskies"],
        "NOVA": ["Villanova Wildcats", "Nova"],
        "GTOWN": ["Georgetown Hoyas"],
        "CREIGH": ["Creighton", "Jays"],
        "MARQ": ["Marquette Golden Eagles"],
        "HALL": ["Seton Hall Pirates"],
        "STJO": ["St. Johns", "St John's", "Johnnies"],
        "XAV": ["Xavier Musketeers"],
        "BUT": ["Butler Bulldogs"],
        "PROV": ["Providence Friars"],
        "DEP": ["DePaul Blue Demons"],

        # Big Ten
        "MICH": ["Michigan Wolverines", "U of M", "UMich"],
        "MSU": ["Michigan St", "Mich State", "Spartans", "Sparty"],
        "OSU": ["Ohio St", "Buckeyes", "tOSU"],
        "PUR": ["Purdue Boilermakers", "Boilers"],
        "IND": ["Indiana Hoosiers", "IU"],
        "ILL": ["Illinois Fighting Illini", "Illini"],
        "IOWA": ["Iowa Hawkeyes", "Hawks"],
        "WIS": ["Wisconsin Badgers", "Wiscy"],
        "MINN": ["Minnesota Golden Gophers", "Gophers"],
        "NW": ["Northwestern Wildcats"],
        "NEB": ["Nebraska Cornhuskers", "Huskers"],
        "MD": ["Maryland Terrapins", "Terps"],
        "RUT": ["Rutgers Scarlet Knights"],
        "PSU": ["Penn St", "Penn State Nittany Lions"],
        "UCLA": ["UCLA Bruins"],
        "USC": ["USC Trojans", "Southern Cal"],
        "ORE": ["Oregon Ducks"],
        "WASH": ["Washington Huskies", "UW"],

        # Big 12
        "KAN": ["Kansas Jayhawks", "KU", "Rock Chalk"],
        "BAY": ["Baylor Bears"],
        "TTU": ["Texas Tech Red Raiders", "Tech"],
        "TCU": ["TCU Horned Frogs"],
        "ISU": ["Iowa St", "Cyclones"],
        "KSU": ["Kansas St", "K-State"],
        "WVU": ["West Virginia Mountaineers", "WVa"],
        "HOU": ["Houston Cougars", "Coogs"],
        "CIN": ["Cincinnati Bearcats", "Cincy"],
        "UCF": ["UCF Knights", "Central Florida"],
        "BYU": ["Brigham Young", "BYU Cougars"],
        "ARIZ": ["Arizona Wildcats", "Zona"],
        "ASU": ["Arizona St", "Sun Devils"],
        "COL": ["Colorado Buffaloes", "Buffs", "CU"],
        "UTAH": ["Utah Utes"],

        # SEC
        "UK": ["Kentucky", "Kentucky Wildcats", "Cats"],
        "ALA": ["Alabama Crimson Tide", "Bama", "Roll Tide"],
        "TENN": ["Tennessee Volunteers", "Vols"],
        "AUB": ["Auburn Tigers", "War Eagle"],
        "FLA": ["Florida Gators", "UF"],
        "UGA": ["Georgia Bulldogs", "Dawgs"],
        "LSU": ["LSU Tigers", "Louisiana State"],
        "ARK": ["Arkansas Razorbacks", "Hogs"],
        "MISS": ["Ole Miss Rebels", "Mississippi"],
        "MSST": ["Miss State", "Mississippi St", "Bulldogs"],
        "MIZZ": ["Mizzou", "Missouri Tigers"],
        "SCAR": ["South Carolina Gamecocks", "USC East"],
        "VAN": ["Vanderbilt Commodores", "Vandy"],
        "TEX": ["Texas Longhorns", "UT", "Horns"],
        "TAMU": ["Texas A&M Aggies", "Aggies"],
        "OU": ["Oklahoma Sooners", "Sooners"],

        # Top Mid-Majors
        "GONZ": ["Gonzaga Bulldogs", "Zags"],
        "STMY": ["Saint Marys", "St Mary's", "Gaels"],
        "SDSU": ["San Diego St", "Aztecs"],
        "MEM": ["Memphis Tigers"],
        "DAY": ["Dayton Flyers"],
        "VCU": ["VCU Rams", "Virginia Commonwealth"],
        "UNLV": ["UNLV Rebels", "Vegas"],
        "BSU": ["Boise St", "Boise State Broncos"],
        "NEV": ["Nevada Wolf Pack"],
        "CSU": ["Colorado St", "Colorado State Rams"],
        "FAU": ["Florida Atlantic Owls"],
        "DRKE": ["Drake Bulldogs"],
        "PRIN": ["Princeton Tigers"],
        "YALE": ["Yale Bulldogs"],
        "CHAR": ["Charleston Cougars", "College of Charleston"],
        "RICH": ["Richmond Spiders"],
        "STLO": ["Saint Louis Billikens", "St Louis", "SLU"],
        "OAK": ["Oakland Golden Grizzlies"],
        "BRWN": ["Brown Bears"],
        "BGSU": ["Bowling Green Falcons", "BG", "BG State"],
        "MIO": ["Miami Ohio", "Miami (Ohio)", "Miami OH", "Miami RedHawks", "RedHawks"],
        "MILW": ["Milwaukee Panthers", "UWM", "UW-Milwaukee", "Wisc-Milwaukee"],
        "DETM": ["Detroit Mercy Titans", "Detroit", "UDM", "U of Detroit"],
    }

    for team in NCAAB_TEAM_SEED:
        alias_strings = {
            f"{team.city} {team.nickname}",
            team.nickname,
            team.code,
            team.city,
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


# NCAAB uses the same prop stat aliases as NBA
NCAAB_PROP_STAT_ALIASES_SEED: Dict[str, List[str]] = {
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


def build_ncaab_prop_stat_aliases() -> List[PropStatAlias]:
    """Build prop stat aliases for NCAAB."""
    aliases: List[PropStatAlias] = []
    for stat_key, alias_strings in NCAAB_PROP_STAT_ALIASES_SEED.items():
        for alias in alias_strings:
            aliases.append(
                PropStatAlias(
                    stat_key=stat_key,
                    alias=alias,
                    normalized_alias=normalize_text(alias),
                )
            )
    return aliases
