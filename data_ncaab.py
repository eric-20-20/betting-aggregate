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

# ============================================================================
# Extended Mid-Majors & Low-Majors (added for SportsLine expert page coverage)
# ============================================================================
EXTENDED_TEAMS: List[Team] = [
    # American Athletic
    Team(code="TEMPLE", city="Temple", nickname="Owls"),
    Team(code="TULSA", city="Tulsa", nickname="Golden Hurricane"),
    Team(code="SFLA", city="South Florida", nickname="Bulls"),
    Team(code="WKNT", city="Wichita State", nickname="Shockers"),
    # Atlantic 10
    Team(code="MASS", city="Massachusetts", nickname="Minutemen"),
    Team(code="FORD", city="Fordham", nickname="Rams"),
    Team(code="GEO", city="George Mason", nickname="Patriots"),
    Team(code="URI", city="Rhode Island", nickname="Rams"),
    Team(code="DVSN", city="Davidson", nickname="Wildcats"),
    # Big South
    Team(code="RAD", city="Radford", nickname="Highlanders"),
    Team(code="CAMP", city="Campbell", nickname="Camels"),
    Team(code="LIPP", city="Lipscomb", nickname="Bisons"),
    # Big West
    Team(code="UCSD", city="UC San Diego", nickname="Tritons"),
    Team(code="UCDV", city="UC Davis", nickname="Aggies"),
    Team(code="UCRV", city="UC Riverside", nickname="Highlanders"),
    Team(code="LBST", city="Long Beach State", nickname="Beach"),
    Team(code="CSUF", city="Cal State Fullerton", nickname="Titans"),
    Team(code="CSUN", city="Cal State Northridge", nickname="Matadors"),
    Team(code="PACF", city="Pacific", nickname="Tigers"),
    # CAA
    Team(code="DREX", city="Drexel", nickname="Dragons"),
    Team(code="NEAST", city="Northeastern", nickname="Huskies"),
    Team(code="UNCW", city="Wilmington", nickname="Seahawks"),
    Team(code="TOWS", city="Towson", nickname="Tigers"),
    Team(code="WMAR", city="William & Mary", nickname="Tribe"),
    # Conference USA
    Team(code="MRSH", city="Marshall", nickname="Thundering Herd"),
    Team(code="NTTX", city="North Texas", nickname="Mean Green"),
    Team(code="UTEP", city="UTEP", nickname="Miners"),
    Team(code="LTECH", city="Louisiana Tech", nickname="Bulldogs"),
    Team(code="SOAL", city="South Alabama", nickname="Jaguars"),
    # Horizon
    Team(code="WRST", city="Wright State", nickname="Raiders"),
    Team(code="CLST", city="Cleveland State", nickname="Vikings"),
    Team(code="YOST", city="Youngstown State", nickname="Penguins"),
    Team(code="UIC", city="Illinois Chicago", nickname="Flames"),
    Team(code="NILLU", city="Northern Illinois", nickname="Huskies"),
    # Ivy League
    Team(code="PENN", city="Pennsylvania", nickname="Quakers"),
    Team(code="COLM", city="Columbia", nickname="Lions"),
    # MAAC
    Team(code="MARST", city="Marist", nickname="Red Foxes"),
    Team(code="FAIR", city="Fairfield", nickname="Stags"),
    Team(code="LIUN", city="LIU", nickname="Sharks"),
    Team(code="SACH", city="Sacred Heart", nickname="Pioneers"),
    # MAC
    Team(code="AKR", city="Akron", nickname="Zips"),
    Team(code="BUFF", city="Buffalo", nickname="Bulls"),
    Team(code="WMICH", city="Western Michigan", nickname="Broncos"),
    # Missouri Valley
    Team(code="BRAD", city="Bradley", nickname="Braves"),
    Team(code="ILSU", city="Southern Illinois", nickname="Salukis"),
    Team(code="MOSU", city="Missouri State", nickname="Bears"),
    Team(code="Niowa", city="Northern Iowa", nickname="Panthers"),
    Team(code="VALPZ", city="Valparaiso", nickname="Beacons"),
    Team(code="ILUI", city="Illinois State", nickname="Redbirds"),
    # Mountain West
    Team(code="WYO", city="Wyoming", nickname="Cowboys"),
    Team(code="NMSU", city="New Mexico State", nickname="Aggies"),
    Team(code="USU", city="Utah State", nickname="Aggies"),
    Team(code="UNM", city="New Mexico", nickname="Lobos"),
    Team(code="SJSU", city="San Jose State", nickname="Spartans"),
    Team(code="NCOL", city="Northern Colorado", nickname="Bears"),
    Team(code="NARIZT", city="Northern Arizona", nickname="Lumberjacks"),
    # NEC
    Team(code="MRMCK", city="Merrimack", nickname="Warriors"),
    Team(code="ROMO", city="Robert Morris", nickname="Colonials"),
    Team(code="STHRT", city="St. Thomas (MN)", nickname="Tommies"),
    # Patriot League
    Team(code="NAVY", city="Navy", nickname="Midshipmen"),
    Team(code="LEHIGH", city="Lehigh", nickname="Mountain Hawks"),
    Team(code="COLG", city="Colgate", nickname="Raiders"),
    # SEC/ACC adjacent
    Team(code="GASO", city="Georgia Southern", nickname="Eagles"),
    Team(code="GAST", city="Georgia State", nickname="Panthers"),
    Team(code="NALA", city="North Alabama", nickname="Lions"),
    # SoCon
    Team(code="CHAT", city="Chattanooga", nickname="Mocs"),
    Team(code="FURM", city="Furman", nickname="Paladins"),
    Team(code="SAMF", city="Samford", nickname="Bulldogs"),
    Team(code="VMI", city="VMI", nickname="Keydets"),
    Team(code="WOFF", city="Wofford", nickname="Terriers"),
    # Southern
    Team(code="SFAC", city="Stephen F. Austin", nickname="Lumberjacks"),
    Team(code="SELL", city="SE Louisiana", nickname="Lions"),
    Team(code="NTXST", city="Nicholls State", nickname="Colonels"),
    # Summit League
    Team(code="SDAK", city="South Dakota", nickname="Coyotes"),
    Team(code="SDAKST", city="South Dakota State", nickname="Jackrabbits"),
    Team(code="NDAKST", city="North Dakota State", nickname="Bison"),
    Team(code="OMAHA", city="Omaha", nickname="Mavericks"),
    Team(code="ORLU", city="Oral Roberts", nickname="Golden Eagles"),
    # Sun Belt
    Team(code="ODU", city="Old Dominion", nickname="Monarchs"),
    Team(code="ECKU", city="Eastern Kentucky", nickname="Colonels"),
    Team(code="NFLA", city="North Florida", nickname="Ospreys"),
    Team(code="GRAM", city="Grambling State", nickname="Tigers"),
    Team(code="SELA", city="Southeastern Louisiana", nickname="Lions"),
    # WAC
    Team(code="GCU", city="Grand Canyon", nickname="Antelopes"),
    Team(code="TARLT", city="Tarleton State", nickname="Texans"),
    Team(code="SOUTAH", city="Southern Utah", nickname="Thunderbirds"),
    Team(code="WMILL", city="Western Illinois", nickname="Leathernecks"),
    Team(code="SEATT", city="Seattle", nickname="Seattle Redhawks"),
    # West Coast Conference
    Team(code="SCL", city="Santa Clara", nickname="Broncos"),
    Team(code="LMU", city="LMU", nickname="Lions"),
    Team(code="PORT", city="Portland", nickname="Pilots"),
    Team(code="SIUE", city="SIUE", nickname="Cougars"),
    # SWAC
    Team(code="PVAM", city="Prairie View", nickname="Panthers"),
    Team(code="BETHCK", city="Bethune-Cookman", nickname="Wildcats"),
    Team(code="FAMU", city="Florida A&M", nickname="Rattlers"),
    Team(code="GRST", city="Grambling", nickname="Tigers"),
    Team(code="NFST", city="Norfolk State", nickname="Spartans"),
    # Big Sky
    Team(code="MTST", city="Montana State", nickname="Bobcats"),
    Team(code="EBOSG", city="Eastern Washington", nickname="Eagles"),
    # Misc
    Team(code="STJOS", city="Saint Joseph's", nickname="Hawks"),
    Team(code="UMBC", city="UMBC", nickname="Retrievers"),
    Team(code="MORST", city="Morehead State", nickname="Eagles"),
    Team(code="MURR", city="Murray State", nickname="Racers"),
    Team(code="LUKYK", city="Loyola Chicago", nickname="Ramblers"),
    Team(code="NCARO", city="North Carolina A&T", nickname="Aggies"),
    Team(code="OSUST", city="Oregon State", nickname="Beavers"),
    Team(code="WASHST", city="Washington State", nickname="Cougars"),
    # Additional
    Team(code="BELMT", city="Belmont", nickname="Bruins"),
    Team(code="WKU", city="Western Kentucky", nickname="Hilltoppers"),
    Team(code="SACST", city="Sacramento State", nickname="Hornets"),
    Team(code="MCNS", city="McNeese", nickname="Cowboys"),
    Team(code="BU", city="Boston University", nickname="Terriers"),
    Team(code="CHSO", city="Charleston Southern", nickname="Buccaneers"),
    Team(code="MONM", city="Monmouth", nickname="Hawks"),
    Team(code="TNST", city="Tennessee State", nickname="Tigers"),
    Team(code="DENV", city="Denver", nickname="Pioneers"),
    Team(code="UCI", city="UC Irvine", nickname="Anteaters"),
    Team(code="TTECH", city="Tennessee Tech", nickname="Golden Eagles"),
    Team(code="UTM", city="UT Martin", nickname="Skyhawks"),
    Team(code="QUNS", city="Queens", nickname="Royals"),
    Team(code="UNCG", city="UNC Greensboro", nickname="Spartans"),
    Team(code="KENT", city="Kent State", nickname="Golden Flashes"),
    Team(code="KENN", city="Kennesaw State", nickname="Owls"),
    Team(code="EMU", city="Eastern Michigan", nickname="Eagles"),
    Team(code="HOW", city="Howard", nickname="Bison"),
    # NCAA Tournament First Four / at-large teams added 2026-03-19
    Team(code="HPU", city="High Point", nickname="Panthers"),
    Team(code="CBU", city="California Baptist", nickname="Lancers"),
    Team(code="IDAHO", city="Idaho", nickname="Vandals"),
    Team(code="SIEN", city="Siena", nickname="Saints"),
    Team(code="TROY", city="Troy", nickname="Trojans"),
]


# Combine all teams
NCAAB_TEAM_SEED: List[Team] = (
    ACC_TEAMS
    + BIG_EAST_TEAMS
    + BIG_TEN_TEAMS
    + BIG_12_TEAMS
    + SEC_TEAMS
    + MID_MAJOR_TEAMS
    + EXTENDED_TEAMS
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
        "OSU": ["Ohio St", "Buckeyes", "tOSU", "Oklahoma State Cowboys", "Okla. St.", "Okla St", "Oklahoma St."],
        "PUR": ["Purdue Boilermakers", "Boilers"],
        "IND": ["Indiana Hoosiers", "IU"],
        "ILL": ["Illinois Fighting Illini", "Illini"],
        "IOWA": ["Iowa Hawkeyes", "Hawks"],
        "WIS": ["Wisconsin Badgers", "Wiscy"],
        "MINN": ["Minnesota Golden Gophers", "Gophers"],
        "NW": ["Northwestern Wildcats", "Northwestern"],
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
        "MSST": ["Miss State", "Mississippi St", "Bulldogs", "Miss. St.", "Miss St."],
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
        "MIO": ["Miami Ohio", "Miami (Ohio)", "Miami OH", "Miami RedHawks", "RedHawks", "MIO"],
        "MILW": ["Milwaukee Panthers", "UWM", "UW-Milwaukee", "Wisc-Milwaukee", "MIL"],
        "DETM": ["Detroit Mercy Titans", "Detroit", "UDM", "U of Detroit"],

        # Extended teams — SportsLine display name variants
        "TEMPLE": ["Temple Owls"],
        "TULSA": ["Tulsa Golden Hurricane"],
        "SFLA": ["South Florida Bulls", "South Florida", "S. Florida", "USF"],
        "WKNT": ["Wichita State Shockers", "Wichita St.", "Wichita St"],
        "MASS": ["Massachusetts Minutemen", "Massachusetts", "UMass", "UMASS"],
        "FORD": ["Fordham Rams"],
        "GEO": ["George Mason Patriots", "George Mason"],
        "URI": ["Rhode Island Rams", "Rhode Island"],
        "DVSN": ["Davidson Wildcats", "Davidson"],
        "RAD": ["Radford Highlanders", "Radford"],
        "CAMP": ["Campbell Camels", "Campbell"],
        "LIPP": ["Lipscomb Bisons", "Lipscomb"],
        "UCSD": ["UC San Diego Tritons", "UC San Diego"],
        "UCDV": ["UC Davis Aggies", "UC Davis"],
        "UCRV": ["UC Riverside Highlanders", "UC Riverside"],
        "PACF": ["Pacific Tigers", "Pacific"],
        "DREX": ["Drexel Dragons", "Drexel"],
        "NEAST": ["Northeastern Huskies", "Northeastern"],
        "UNCW": ["Wilmington Seahawks", "UNCW", "UNC Wilmington"],
        "TOWS": ["Towson Tigers", "Towson"],
        "WMAR": ["William & Mary Tribe", "William & Mary"],
        "MRSH": ["Marshall Thundering Herd", "Marshall"],
        "NTTX": ["North Texas Mean Green", "North Texas"],
        "LTECH": ["Louisiana Tech Bulldogs", "La. Tech", "Louisiana Tech"],
        "SOAL": ["South Alabama Jaguars", "South Alabama"],
        "WRST": ["Wright State Raiders", "Wright St.", "Wright St"],
        "CLST": ["Cleveland State Vikings", "Clev. St.", "Cleveland St"],
        "YOST": ["Youngstown State Penguins", "Youngstown St.", "Youngstown St"],
        "UIC": ["Illinois Chicago Flames", "UIC"],
        "NILLU": ["Northern Illinois Huskies", "N. Illinois", "Northern Illinois"],
        "PENN": ["Pennsylvania Quakers", "Penn"],
        "COLM": ["Columbia Lions", "Columbia"],
        "MARST": ["Marist Red Foxes", "Marist"],
        "FAIR": ["Fairfield Stags", "Fairfield"],
        "LIUN": ["LIU Sharks", "LIU"],
        "SACH": ["Sacred Heart Pioneers", "Sacred Heart"],
        "AKR": ["Akron Zips", "Akron"],
        "BUFF": ["Buffalo Bulls", "Buffalo"],
        "WMICH": ["Western Michigan Broncos", "W. Michigan", "Western Michigan"],
        "BRAD": ["Bradley Braves", "Bradley"],
        "ILSU": ["Southern Illinois Salukis", "S. Illinois", "Southern Illinois"],
        "MOSU": ["Missouri State Bears", "Missouri St"],
        "Niowa": ["Northern Iowa Panthers", "N. Iowa", "Northern Iowa"],
        "VALPZ": ["Valparaiso Beacons", "Valparaiso"],
        "WYO": ["Wyoming Cowboys", "Wyoming"],
        "NMSU": ["New Mexico State Aggies", "N. Mex. St.", "New Mexico State"],
        "USU": ["Utah State Aggies", "Utah St.", "Utah St"],
        "UNM": ["New Mexico Lobos", "New Mexico"],
        "NCOL": ["Northern Colorado Bears", "N. Colorado", "Northern Colorado"],
        "NARIZT": ["Northern Arizona Lumberjacks", "N. Arizona", "Northern Arizona"],
        "MRMCK": ["Merrimack Warriors", "Merrimack"],
        "ROMO": ["Robert Morris Colonials", "Robert Morris"],
        "STHRT": ["St. Thomas Tommies", "St. Thomas (MN)", "St Thomas MN"],
        "NAVY": ["Navy Midshipmen", "Navy"],
        "LEHIGH": ["Lehigh Mountain Hawks", "Lehigh"],
        "GASO": ["Georgia Southern Eagles", "Ga. Southern", "Georgia Southern"],
        "GAST": ["Georgia State Panthers", "Georgia St.", "Georgia State"],
        "NALA": ["North Alabama Lions", "N. Alabama", "Northern Alabama"],
        "CHAT": ["Chattanooga Mocs", "Chattanooga", "UTC"],
        "FURM": ["Furman Paladins", "Furman"],
        "SAMF": ["Samford Bulldogs", "Samford"],
        "VMI": ["VMI Keydets", "VMI"],
        "SFAC": ["Stephen F. Austin Lumberjacks", "SF Austin", "SFA", "Stephen F Austin"],
        "SELL": ["SE Louisiana Lions", "SE Louisiana"],
        "SDAK": ["South Dakota Coyotes", "South Dakota"],
        "SDAKST": ["South Dakota State Jackrabbits", "S. Dak. St.", "South Dakota St"],
        "NDAKST": ["North Dakota State Bison", "N. Dakota St.", "North Dakota St"],
        "OMAHA": ["Omaha Mavericks", "Omaha", "Nebraska-Omaha", "Neb.-Omaha"],
        "ORLU": ["Oral Roberts Golden Eagles", "Oral Roberts"],
        "ODU": ["Old Dominion Monarchs", "Old Dominion"],
        "ECKU": ["Eastern Kentucky Colonels", "E. Kentucky", "Eastern Kentucky"],
        "NFLA": ["North Florida Ospreys", "North Florida"],
        "GRAM": ["Grambling State Tigers", "Grambling St.", "Grambling"],
        "GCU": ["Grand Canyon Antelopes", "Grand Canyon"],
        "TARLT": ["Tarleton State Texans", "Tarleton St.", "Tarleton State"],
        "SOUTAH": ["Southern Utah Thunderbirds", "So. Utah", "Southern Utah"],
        "WMILL": ["Western Illinois Leathernecks", "W. Illinois", "Western Illinois"],
        "SCL": ["Santa Clara Broncos", "Santa Clara"],
        "LMU": ["LMU Lions", "LMU", "Loyola Marymount"],
        "PORT": ["Portland Pilots", "Portland", "POR"],
        "SIUE": ["SIUE Cougars", "SIUE"],
        "PVAM": ["Prairie View Panthers", "Prairie View"],
        "BETHCK": ["Bethune-Cookman Wildcats", "Bethune-Cook.", "Bethune Cookman"],
        "FAMU": ["Florida A&M Rattlers", "Florida A&M"],
        "NFST": ["Norfolk State Spartans", "Norfolk St.", "Norfolk State"],
        "MTST": ["Montana State Bobcats", "Montana St.", "Montana State"],
        "STJOS": ["Saint Joseph's Hawks", "Saint Joseph's", "St. Joseph's", "SJU"],
        "UMBC": ["UMBC Retrievers", "UMBC"],
        "MURR": ["Murray State Racers", "Murray St.", "Murray State"],
        "LUKYK": ["Loyola Chicago Ramblers", "Loyola Chi.", "Loyola Chicago"],
        "OSUST": ["Oregon State Beavers", "Oregon St.", "Oregon State"],
        "WASHST": ["Washington State Cougars", "Wash. St.", "Washington State"],
        # Codes used by SportsLine as-is
        "CHAR": ["C. Carolina", "Coastal Carolina", "CHA", "Chas"],
        "BELMT": ["Belmont Bruins", "Belmont"],
        "WKU": ["Western Kentucky Hilltoppers", "W. Kentucky", "Western Kentucky"],
        "SACST": ["Sacramento State Hornets", "Sacramento St.", "Sacramento St"],
        "MCNS": ["McNeese Cowboys", "McNeese", "McNeese State", "MCN"],
        "BU": ["Boston University Terriers", "Boston U.", "Boston University"],
        "MORST": ["Morehead State Eagles", "Morehead St.", "Morehead State"],
        "NCST": ["NC State", "N.C. State", "N. Carolina State"],
        "UNC": ["N. Carolina", "North Carolina"],
        "BELMT": ["Belmont Bruins", "Belmont"],
        "JAX": ["Jax. State", "Jacksonville State", "Jacksonville St"],
        "SCAR": ["S. Carolina", "South Carolina Gamecocks"],
        "CSU": ["Colo. St.", "Colorado St.", "Colorado State"],
        "ISU": ["Iowa St.", "Iowa State"],
        "STMY": ["Saint Mary's", "St. Mary's"],
        "NW": ["Northwestern Wildcats"],
        "WASH": ["WAS", "Washington Huskies"],
        "SEATT": ["Seattle U", "Seattle University", "Seattle Redhawks"],
        # Extended teams added for SportsLine NCAAB expert picks
        "MONM": ["Monmouth Hawks", "Monmouth"],
        "MSU": ["Mich. St.", "Michigan St.", "Michigan State Spartans"],
        "DETM": ["Detroit", "DET", "Detroit Mercy Titans", "U of Detroit", "UDM"],
        "EBOSG": ["E. Washington", "Eastern Washington Eagles"],
        "CSUF": ["CS Fullerton", "Cal State Fullerton Titans"],
        "SJSU": ["San Jose St.", "San Jose State Spartans"],
        "MIO": ["Miami Ohio", "Miami (Ohio)", "Miami OH", "Miami RedHawks", "RedHawks", "MIO"],
        "TNST": ["Tennessee St.", "Tennessee State Tigers", "Tenn. St."],
        "DENV": ["Denver Pioneers", "DEN"],
        "UCI": ["UC Irvine Anteaters", "UC Irvine"],
        "TTECH": ["Tenn. Tech", "Tennessee Tech Golden Eagles"],
        "UTM": ["UT Martin Skyhawks"],
        "QUNS": ["Queens Royals", "Queens NC"],
        "UNCG": ["UNC Greensboro Spartans"],
        "KENT": ["Kent St.", "Kent State Golden Flashes"],
        "KENN": ["Kennesaw St.", "Kennesaw State Owls"],
        "EMU": ["E. Michigan", "Eastern Michigan Eagles"],
        "HOW": ["Howard Bison", "Howard University"],
        # New tournament teams (2026-03-19)
        "HPU": ["High Point Panthers", "High Point"],
        "CBU": ["Cal Baptist", "California Baptist Lancers"],
        "IDAHO": ["Idaho Vandals", "Idaho"],
        "SIEN": ["Siena Saints", "Siena", "SIE"],
        "TROY": ["Troy Trojans", "Troy"],
        # Additional aliases for existing teams (JuiceReel spellings)
        "PENN": ["UPenn", "U Penn", "upenn"],
        "STLO": ["STL Billikens", "St. Louis Billikens", "Saint Louis U", "St Louis", "St. Louis", "SLU"],
        "STMY": ["St Mary", "St. Mary", "St Marys CA", "Saint Mary's CA", "SMC"],
        "NDAKST": ["North Dakota St", "N. Dak. St.", "NDSU"],
        "MSU": ["Michigan St", "Michigan St."],
        # ESPN abbreviations that differ from our codes
        "SFLA": ["USF", "South Florida Bulls"],
        "TAMU": ["TA&M", "Texas A&M Aggies", "TXAM"],
        "IDAHO": ["IDHO", "Idaho Vandals"],
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
