#!/usr/bin/env python3
"""
Convert Dimers NCAAB backfill data to normalized schema.
Reads out/backfill_dimers_ncaab_bets.jsonl -> out/normalized_dimers_ncaab_backfill.jsonl
"""

import json
import re
from pathlib import Path
from collections import Counter
from typing import Optional, Tuple

INPUT_FILE = Path("out/backfill_dimers_ncaab_bets.jsonl")
OUTPUT_FILE = Path("out/normalized_dimers_ncaab_backfill.jsonl")

# Full name -> NCAAB team code mapping
# Covers both common names Dimers uses and our internal codes
NCAAB_NAME_MAP = {
    # Exact pick_text names from Dimers
    "abilene christian": "SFAC",
    "alabama a&m": "AAMU",
    "alabama state": "ALST",
    "albany": "ALBY",
    "alcorn state": "ALCN",
    "american university": "AMER",
    "appalachian state": "APP",
    "arizona": "ARIZ",
    "arizona state": "ASU",
    "arkansas": "ARK",
    "arkansas state": "ARST",
    "arkansas-little rock": "UALR",
    "arkansas-pine bluff": "UAPB",
    "army": "ARMY",
    "auburn": "AUB",
    "austin peay": "PEAY",
    "ball state": "BALL",
    "baylor": "BAY",
    "bellarmine": "BELL",
    "belmont": "BELMT",
    "bethune-cookman": "BETHCK",
    "binghamton": "BING",
    "boise state": "BSU",
    "boston college": "BC",
    "boston university": "BU",
    "bowling green": "BGSU",
    "bradley": "BRAD",
    "brown": "BRWN",
    "bryant": "UMBC",   # Bryant University
    "bryant university": "BRY",
    "bucknell": "BUCK",
    "buffalo": "BUFF",
    "butler": "BUT",
    "byu": "BYU",
    "cal poly": "CP",
    "cal state bakersfield": "CSB",
    "cal state fullerton": "CSUF",
    "cal state northridge": "CSUN",
    "california": "CAL",
    "california baptist": "CBU",
    "campbell": "CAMP",
    "canisius": "CAN",
    "central arkansas": "CARK",
    "central michigan": "CMU",
    "charleston": "CHAR",
    "charlotte": "CHAR49",
    "chattanooga": "CHAT",
    "cincinnati": "CIN",
    "citadel": "CIT",
    "clemson": "CLEM",
    "cleveland state": "CLST",
    "colgate": "COLG",
    "colorado": "COL",
    "colorado state": "CSU",
    "columbia": "COLM",
    "connecticut": "CONN",
    "coppin state": "COPP",
    "cornell": "COR",
    "creighton": "CREIGH",
    "dartmouth": "DART",
    "davidson": "DVSN",
    "dayton": "DAY",
    "delaware": "DEL",
    "denver": "DENV",
    "depaul": "DEP",
    "detroit mercy": "DETM",
    "drake": "DRKE",
    "drexel": "DREX",
    "duke": "DUKE",
    "duquesne": "DUQ",
    "east carolina": "ECU",
    "east tennessee state": "ETSU",
    "eastern kentucky": "ECKU",
    "eastern michigan": "EMU",
    "eastern washington": "EBOSG",
    "elon": "ELON",
    "evansville": "EVAN",
    "fairfield": "FAIR",
    "fairleigh dickinson": "FDU",
    "florida": "FLA",
    "florida a&m": "FAMU",
    "florida atlantic": "FAU",
    "florida gulf coast": "FGCU",
    "florida international": "FIU",
    "florida state": "FSU",
    "fordham": "FORD",
    "fresno state": "FRES",
    "furman": "FURM",
    "gardner-webb": "GARDN",
    "george mason": "GEO",
    "george washington": "GW",
    "georgetown": "GTOWN",
    "georgia": "UGA",
    "georgia southern": "GASO",
    "georgia state": "GAST",
    "georgia tech": "GT",
    "gonzaga": "GONZ",
    "grambling state": "GRAM",
    "grand canyon": "GCU",
    "hampton": "HAMP",
    "harvard": "HARV",
    "hawaii": "HAW",
    "high point": "HP",
    "hofstra": "HOF",
    "houston": "HOU",
    "howard": "HOW",
    "idaho": "IDHO",
    "idaho state": "IDST",
    "illinois": "ILL",
    "illinois state": "ILUI",
    "illinois-chicago": "UIC",
    "indiana": "IND",
    "indiana state": "INST",
    "iona": "IONA",
    "iowa": "IOWA",
    "iowa state": "ISU",
    "jacksonville": "JAC",
    "jacksonville state": "JAX",
    "james madison": "JMU",
    "kansas": "KAN",
    "kansas city": "UMKC",
    "kansas state": "KSU",
    "kennesaw state": "KENN",
    "kent state": "KENT",
    "kentucky": "UK",
    "la salle": "LAS",
    "lafayette": "LAF",
    "lamar": "LAM",
    "lehigh": "LEHIGH",
    "liberty": "LIB",
    "lipscomb": "LIPP",
    "liu": "LIUN",
    "long beach state": "LBST",
    "longwood": "LONG",
    "louisiana tech": "LTECH",
    "louisiana-lafayette": "ULL",
    "louisiana-monroe": "ULM",
    "louisville": "LOU",
    "loyola (md)": "LOMD",
    "loyola chicago": "LUKYK",
    "loyola maryland": "LOMD",
    "lmu": "LMU",
    "maine": "ME",
    "manhattan": "MAN",
    "marist": "MARST",
    "marquette": "MARQ",
    "marshall": "MRSH",
    "maryland": "MD",
    "maryland-eastern shore": "UMES",
    "massachusetts": "MASS",
    "massachusetts-lowell": "MASSL",
    "memphis": "MEM",
    "mercer": "MER",
    "merrimack": "MRMCK",
    "miami (fl)": "MIA",
    "miami (ohio)": "MIO",
    "michigan": "MICH",
    "michigan state": "MSU",
    "middle tennessee": "MTU",
    "milwaukee": "MILW",
    "minnesota": "MINN",
    "mississippi state": "MSST",
    "missouri": "MIZZ",
    "missouri state": "MOSU",
    "montana": "MONT",
    "montana state": "MTST",
    "morehead state": "MORST",
    "morgan state": "MORG",
    "mount st. mary's": "MSM",
    "murray state": "MURR",
    "n.j.i.t.": "NJIT",
    "nebraska": "NEB",
    "nevada": "NEV",
    "new mexico": "UNM",
    "new mexico state": "NMSU",
    "niagara": "NIAG",
    "nicholls state": "NTXST",
    "north alabama": "NALA",
    "north carolina": "UNC",
    "north carolina a&t": "NCARO",
    "north carolina central": "NCCU",
    "north carolina state": "NCST",
    "north carolina-wilmington": "UNCW",
    "north dakota": "UND",
    "north dakota state": "NDAKST",
    "north florida": "NFLA",
    "north texas": "NTTX",
    "northeastern": "NEAST",
    "northern arizona": "NARIZT",
    "northern colorado": "NCOL",
    "northern illinois": "NILLU",
    "northern iowa": "Niowa",
    "northern kentucky": "NKU",
    "northwestern": "NW",
    "northwestern state": "NWST",
    "notre dame": "ND",
    "oakland": "OAK",
    "ohio": "OHIO",
    "ohio state": "OSU",
    "ohio university": "OHIO",
    "oklahoma": "OU",
    "oklahoma state": "OKST",
    "old dominion": "ODU",
    "ole miss": "MISS",
    "omaha": "OMAHA",
    "oral roberts": "ORLU",
    "oregon": "ORE",
    "oregon state": "OSUST",
    "pacific": "PACF",
    "penn state": "PSU",
    "pennsylvania": "PENN",
    "pepperdine": "PEPP",
    "pittsburgh": "PITT",
    "portland": "PORT",
    "portland state": "PRST",
    "prairie view a&m": "PVAM",
    "presbyterian": "PRE",
    "princeton": "PRIN",
    "providence": "PROV",
    "purdue": "PUR",
    "purdue fort wayne": "PFW",
    "queens": "QUNS",
    "quinnipiac": "QUIN",
    "radford": "RAD",
    "rhode island": "URI",
    "rice": "RICE",
    "richmond": "RICH",
    "rider": "RID",
    "robert morris": "ROMO",
    "rutgers": "RUT",
    "sacred heart": "SACH",
    "saint joseph's (pa)": "STJOS",
    "saint joseph's": "STJOS",
    "saint louis": "STLO",
    "saint mary's": "STMY",
    "sam houston state": "SHSU",
    "samford": "SAMF",
    "san diego": "USD",
    "san diego state": "SDSU",
    "san francisco": "SF",
    "santa clara": "SCL",
    "se louisiana": "SELA",
    "seattle": "SEATT",
    "seton hall": "HALL",
    "siena": "SIE",
    "siu-edwardsville": "SIUE",
    "south alabama": "SOAL",
    "south carolina": "SCAR",
    "south carolina state": "SCST",
    "south dakota": "SDAK",
    "south dakota state": "SDAKST",
    "south florida": "SFLA",
    "southeast missouri state": "SEMO",
    "southeastern louisiana": "SELA",
    "southern illinois": "ILSU",
    "southern indiana": "USI",
    "southern miss": "USM",
    "southern university": "SOU",
    "southern utah": "SOUTAH",
    "st. bonaventure": "SBON",
    "st. francis (pa)": "SFP",
    "st. john's": "STJO",
    "st. peter's": "SPC",
    "st. thomas (mn)": "STHRT",
    "stanford": "STAN",
    "stephen f. austin": "SFAC",
    "stetson": "STET",
    "stony brook": "STON",
    "syracuse": "SYR",
    "tarleton state": "TARLT",
    "temple": "TEMPLE",
    "tennessee": "TENN",
    "tennessee state": "TNST",
    "tennessee tech": "TTECH",
    "tennessee-martin": "UTM",
    "texas": "TEX",
    "texas a&m": "TAMU",
    "texas a&m commerce": "TAMC",
    "texas a&m-cc": "AMCC",
    "texas rio grande valley": "UTRGV",
    "texas southern": "TXSO",
    "texas state": "TXST",
    "texas tech": "TTU",
    "texas-arlington": "UTA",
    "toledo": "TOL",
    "towson": "TOWS",
    "troy": "TROY",
    "tulane": "TULN",
    "tulsa": "TULSA",
    "uab": "UAB",
    "uc irvine": "UCI",
    "uc riverside": "UCRV",
    "uc san diego": "UCSD",
    "uc santa barbara": "UCSB",
    "ucla": "UCLA",
    "unc asheville": "UNCA",
    "unlv": "UNLV",
    "usc": "USC",
    "usc upstate": "SCUS",
    "utah": "UTAH",
    "utah state": "USU",
    "utah valley": "UVU",
    "utep": "UTEP",
    "utsa": "UTSA",
    "vanderbilt": "VAN",
    "vcu": "VCU",
    "vermont": "UVM",
    "villanova": "NOVA",
    "virginia": "UVA",
    "virginia military": "VMI",
    "virginia tech": "VT",
    "wagner": "WAG",
    "wake forest": "WAKE",
    "washington": "WASH",
    "washington state": "WASHST",
    "weber state": "WEB",
    "west virginia": "WVU",
    "western carolina": "WCU",
    "western illinois": "WMILL",
    "western kentucky": "WKU",
    "western michigan": "WMICH",
    "wichita state": "WKNT",
    "william & mary": "WMAR",
    "winthrop": "WIN",
    "wisconsin": "WIS",
    "wofford": "WOFF",
    "wright state": "WRST",
    "wyoming": "WYO",
    "xavier": "XAV",
    "yale": "YALE",
    "youngstown state": "YOST",
    # Additional common variations
    "uconn": "CONN",
    "miami": "MIA",
    "ohio u": "OHIO",
    "njit": "NJIT",
    "tcu": "TCU",
    "lsu": "LSU",
    "unc": "UNC",
    "ucf": "UCF",
    "fiu": "FIU",
    "fau": "FAU",
    "vmi": "VMI",
    "uab": "UAB",
    "utsa": "UTSA",
    "uta": "UTA",
    "utrgv": "UTRGV",
    "umkc": "UMKC",
    "siue": "SIUE",
    "etsu": "ETSU",
    "uncw": "UNCW",
    "sfac": "SFAC",
    "pvam": "PVAM",
    "shsu": "SHSU",
    "smu": "SMU",
    "byu": "BYU",
    "vcu": "VCU",
    "wku": "WKU",
}


def normalize_name(name: str) -> Optional[str]:
    """Resolve a full team name to NCAAB code."""
    if not name:
        return None
    low = name.lower().strip()
    # Direct lookup
    if low in NCAAB_NAME_MAP:
        return NCAAB_NAME_MAP[low]
    # Try removing trailing 's' (e.g. "Bulldogs" -> "Bulldog") — unlikely but try
    # Try known short codes
    if len(name) <= 5 and name.upper() == name:
        return NCAAB_NAME_MAP.get(name.lower()) or name
    return None


def parse_matchup(matchup: str) -> Tuple[Optional[str], Optional[str]]:
    """Parse 'Away vs. Home' matchup string. Returns (away_code, home_code)."""
    if not matchup:
        return None, None
    m = re.match(r"(.+?)\s+vs\.?\s+(.+)", matchup, re.IGNORECASE)
    if m:
        away = normalize_name(m.group(1).strip())
        home = normalize_name(m.group(2).strip())
        return away, home
    return None, None


def convert_record(orig: dict) -> Optional[dict]:
    game_date = orig.get("game_date", "")
    matchup = orig.get("matchup", "")
    market_type = orig.get("market_type", "")
    pick_text = orig.get("pick_text", "")

    # Parse teams from matchup
    away_team, home_team = parse_matchup(matchup)

    # Build day_key
    parts = game_date.split("-")
    if len(parts) != 3:
        return None
    day_key = f"NCAAB:{parts[0]}:{parts[1]}:{parts[2]}"

    # Build selection/side/line
    selection = None
    side = None
    line = None
    team_code = None

    if market_type == "moneyline":
        m = re.match(r"(.+)\s+win", pick_text, re.IGNORECASE)
        if m:
            team_code = normalize_name(m.group(1).strip())
        if team_code:
            selection = team_code
            side = team_code

    elif market_type == "spread":
        m = re.match(r"(.+?)\s+([+-]?[\d]+\.?[\d]*)\s*$", pick_text, re.IGNORECASE)
        if m:
            team_code = normalize_name(m.group(1).strip())
            try:
                line = float(m.group(2))
            except ValueError:
                pass
        if team_code:
            selection = team_code
            side = team_code

    elif market_type == "total":
        m = re.match(r"(over|under)\s+([\d.]+)", pick_text, re.IGNORECASE)
        if m:
            side = m.group(1).upper()
            try:
                line = float(m.group(2))
            except ValueError:
                pass
            selection = side
        # Totals don't need team resolution — fill teams from matchup if available
        if not away_team or not home_team:
            # Try to get teams from URL slug
            url = orig.get("game_url", "")
            slug = url.split("/")[-1]
            parts_slug = slug.split("_")
            if len(parts_slug) >= 4:
                # format: YYYY_daynum_away_home
                away_slug = parts_slug[-2]
                home_slug = parts_slug[-1]
                # Use slugs as-is as fallback (uppercase)
                if not away_team:
                    away_team = away_slug.upper()
                if not home_team:
                    home_team = home_slug.upper()

    # For spread/moneyline, we need team resolution
    if market_type in ("spread", "moneyline") and not selection:
        return None

    # For totals, we need at least side+line
    if market_type == "total" and not selection:
        return None

    # For events, need both teams
    if not away_team or not home_team:
        return None

    # Sorted matchup_key
    t1, t2 = sorted([away_team, home_team])
    matchup_key = f"{day_key}:{t1}-{t2}"
    event_key = f"{day_key}:{away_team}@{home_team}"

    # Parse result from final_score
    result_block = None
    final_score = orig.get("final_score")
    if final_score:
        # "72-73" format — away_score-home_score
        sm = re.match(r"(\d+)-(\d+)", str(final_score))
        if sm:
            away_score = int(sm.group(1))
            home_score = int(sm.group(2))
            # Grade based on market type
            status = None
            if market_type == "moneyline":
                if team_code == away_team:
                    status = "WIN" if away_score > home_score else ("PUSH" if away_score == home_score else "LOSS")
                elif team_code == home_team:
                    status = "WIN" if home_score > away_score else ("PUSH" if home_score == away_score else "LOSS")
            elif market_type == "spread" and line is not None:
                if team_code == away_team:
                    covered = (away_score + line) - home_score
                elif team_code == home_team:
                    covered = (home_score + line) - away_score
                else:
                    covered = None
                if covered is not None:
                    status = "WIN" if covered > 0 else ("PUSH" if covered == 0 else "LOSS")
            elif market_type == "total" and line is not None:
                total = away_score + home_score
                if side == "OVER":
                    status = "WIN" if total > line else ("PUSH" if total == line else "LOSS")
                elif side == "UNDER":
                    status = "WIN" if total < line else ("PUSH" if total == line else "LOSS")

            if status:
                result_block = {
                    "status": status,
                    "graded_by": "dimers_ncaab_backfill",
                    "away_score": away_score,
                    "home_score": home_score,
                }

    # Parse odds
    odds = None
    best_odds = orig.get("best_odds")
    if best_odds:
        m_odds = re.search(r"([+-]?\d+)", str(best_odds))
        if m_odds:
            try:
                odds = int(m_odds.group(1))
            except ValueError:
                pass

    return {
        "provenance": {
            "source_id": "dimers",
            "source_surface": "dimers_ncaab_schedule_backfill",
            "sport": "NCAAB",
            "observed_at_utc": orig.get("observed_at_utc"),
            "canonical_url": orig.get("game_url"),
            "raw_fingerprint": orig.get("raw_fingerprint"),
            "raw_pick_text": pick_text,
            "expert_name": "Dimers AI Model",
            "probability_pct": orig.get("probability_pct"),
            "edge_pct": orig.get("edge_pct"),
            "best_odds": best_odds,
        },
        "event": {
            "sport": "NCAAB",
            "event_key": event_key,
            "day_key": day_key,
            "matchup_key": matchup_key,
            "away_team": away_team,
            "home_team": home_team,
            "event_start_time_utc": None,
        },
        "market": {
            "market_type": market_type,
            "market_family": "standard",
            "side": side,
            "selection": selection,
            "line": line,
            "odds": odds,
            "team_code": team_code,
        },
        "result": result_block,
        "eligible_for_consensus": True,
        "ineligibility_reason": None,
    }


def main():
    records = []
    with open(INPUT_FILE) as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    print(f"[ncaab_convert] Loaded {len(records)} raw records")

    converted = []
    skipped = 0
    status_counts = Counter()

    for orig in records:
        result = convert_record(orig)
        if result:
            converted.append(result)
            if result.get("result"):
                status_counts[result["result"]["status"]] += 1
            else:
                status_counts["UNGRADED"] += 1
        else:
            skipped += 1

    print(f"[ncaab_convert] Converted: {len(converted)}, Skipped: {skipped}")
    mkt = Counter(r["market"]["market_type"] for r in converted)
    print(f"[ncaab_convert] Market types: {dict(mkt)}")
    print(f"[ncaab_convert] Results: {dict(status_counts)}")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        for r in converted:
            f.write(json.dumps(r) + "\n")

    dates = sorted(set(r["event"]["day_key"] for r in converted))
    if dates:
        print(f"[ncaab_convert] Date range: {dates[0]} to {dates[-1]} ({len(dates)} days)")
    print(f"[ncaab_convert] Wrote {len(converted)} records to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
