#!/usr/bin/env python
from __future__ import annotations

import sys
import argparse
import pathlib
from pathlib import Path
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from store import ensure_dir, sha256_json, write_json

SPORT = "NBA"
DEFAULT_URL = "https://www.sportscapping.com/free-nba-picks.html"


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def slugify(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return slug or None


def parse_datetime(text: str) -> Optional[datetime]:
    """
    Parse strings like "Jan 23 '26, 07:00 PM ET" or "January 23, 2026 7:00 PM EST".
    Default timezone: America/New_York when not provided.
    """
    if not text:
        return None
    month_map = {
        "jan": "Jan",
        "feb": "Feb",
        "mar": "Mar",
        "apr": "Apr",
        "may": "May",
        "jun": "Jun",
        "jul": "Jul",
        "aug": "Aug",
        "sep": "Sep",
        "sept": "Sep",
        "oct": "Oct",
        "nov": "Nov",
        "dec": "Dec",
    }
    pattern = re.compile(
        r"(?P<mon>Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
        r"\s+"
        r"(?P<day>\d{1,2})"
        r"[,\s']+"
        r"(?P<year>\d{2,4})"
        r".{0,6}?"
        r"(?P<time>\d{1,2}:\d{2})"
        r"\s*"
        r"(?P<ampm>AM|PM)"
        r"(?:\s*(?P<tz>ET|EST|EDT))?",
        re.IGNORECASE,
    )
    m = pattern.search(text)
    if not m:
        return None
    mon = month_map[m.group("mon").lower()[:3]]
    day = int(m.group("day"))
    year = int(m.group("year"))
    if year < 100:
        year += 2000
    clock = m.group("time")
    ampm = m.group("ampm").upper()
    tzname = m.group("tz") or "ET"
    tz = ZoneInfo("America/New_York") if tzname in {"ET", "EST", "EDT"} else timezone.utc
    try:
        local_dt = datetime.strptime(f"{mon} {day:02d} {year} {clock} {ampm}", "%b %d %Y %I:%M %p")
    except ValueError:
        return None
    local_dt = local_dt.replace(tzinfo=tz)
    return local_dt.astimezone(timezone.utc)


def detect_market_family(pick_text: str) -> str:
    txt = pick_text.lower()
    if re.search(r"\bover\b|\bunder\b", txt) and re.search(r"\bpts?\b|points|rebounds|assists|pra|threes", txt):
        return "player_prop"
    if re.search(r"\bo/u\b", txt) or re.search(r"\bover\b|\bunder\b", txt):
        return "total"
    if re.search(r"[+-]\d+(?:\.\d+)?(?:½|\.5)?", txt):
        return "spread"
    if re.search(r"\bml\b|\bmoneyline\b", txt):
        return "moneyline"
    return "unknown"


def pick_parent(tag) -> Any:
    """Choose a reasonable block container for a 'Play on' text node."""
    for parent in tag.parents:
        if parent.name == "body":
            return tag.parent
        classes = " ".join(parent.get("class", [])).lower()
        if any(key in classes for key in ("pick", "free", "analysis", "article", "post", "entry", "card")):
            return parent
    return tag.parent


def extract_pick_text(raw: str) -> str:
    if not raw:
        return ""
    m = re.search(r"play on:\s*(.+)", raw, flags=re.IGNORECASE | re.DOTALL)
    if m:
        return m.group(1).strip()
    return raw.strip()


def parse_blocks(soup: BeautifulSoup, canonical_url: str, observed_iso: str) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    blocks = soup.select("div.clearfix.content-heading.user-backend-full-top")
    for block in blocks if blocks else soup.find_all(string=re.compile(r"play on:", re.IGNORECASE)):
        if hasattr(block, "select_one"):
            node = block
        else:
            node = pick_parent(block)
        block_text = node.get_text(" ", strip=True) if node else str(block)

        # expert name
        expert_name = None
        if node:
            h3 = node.find("h3")
            if h3 and h3.get_text(strip=True):
                expert_name = h3.get_text(strip=True)

        # matchup
        matchup_hint = None
        header_text = None
        if node:
            game_div = node.select_one("div.free-pick-game-info div.free-pick-game")
            if game_div:
                text = game_div.get_text(" ", strip=True)
                # remove sport token and bars
                text = re.sub(r"\bNBA\b", "", text, flags=re.IGNORECASE)
                text = text.replace("|", " ")
                matchup_hint = " ".join(text.split()).strip()
                header_text = game_div.get_text(" ", strip=True)

        # raw pick
        raw_pick_text = ""
        if node:
            green = node.select_one("div.alert.alert-success.free-pick-green")
            if green:
                bold = green.find("b")
                if bold and bold.get_text(strip=True):
                    raw_pick_text = bold.get_text(" ", strip=True)
                else:
                    raw_pick_text = green.get_text(" ", strip=True)
        if not raw_pick_text:
            raw_pick_text = extract_pick_text(block_text)

        lines = [s.strip() for s in node.stripped_strings if s.strip()] if hasattr(node, "stripped_strings") else []

        expert_profile = None
        if node:
            link = node.find("a", href=True)
            if link:
                expert_profile = link["href"]

        # Released timestamp
        source_updated = None
        rel_match = re.search(r"Released on\s+([^|]+)", block_text, re.IGNORECASE)
        if rel_match:
            source_updated = parse_datetime(rel_match.group(1))

        # fallback regex if selectors miss
        if not matchup_hint and lines:
            for line in lines:
                m = re.search(r"nba\s*\|\s*(.+?)\s+(?:vs|v)\s+(.+)", line, re.IGNORECASE)
                if m:
                    matchup_hint = f"{m.group(1).strip()} vs {m.group(2).strip()}"
                    header_text = line
                    break

        # Event start time
        event_start = None
        evt_match = re.search(
            r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[^\d]*\d{1,2}[^\d]{1,4}(?:'|\d)\d{1,2}[^\d]{1,4}\d{1,2}:\d{2}\s*[AP]M[^|]*",
            block_text,
            re.IGNORECASE,
        )
        if evt_match:
            event_start = parse_datetime(evt_match.group(0))

        record: Dict[str, Any] = {
            "source_id": "sportscapping",
            "source_surface": "free_nba_picks",
            "sport": SPORT,
            "market_family": detect_market_family(raw_pick_text),
            "observed_at_utc": observed_iso,
            "canonical_url": canonical_url,
            "raw_pick_text": raw_pick_text,
            "raw_block": block_text,
            "raw_fingerprint": None,  # filled after
            "source_updated_at_utc": source_updated.isoformat() if isinstance(source_updated, datetime) else None,
            "event_start_time_utc": event_start.isoformat() if isinstance(event_start, datetime) else None,
            "player_hint": None,
            "stat_hint": None,
            "line_hint": None,
            "odds_hint": None,
            "expert_name": expert_name,
            "expert_handle": None,
            "expert_profile": expert_profile,
            "expert_slug": slugify(expert_name),
            "tailing_handle": None,
            "matchup_hint": matchup_hint,
            "header_text": header_text,
        }

        record["raw_fingerprint"] = sha256_json({k: v for k, v in record.items() if k != "raw_fingerprint"})
        results.append(record)
    return results


def fetch_html(url: str) -> str:
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return resp.text


def main(url: str, out_path: str) -> None:
    observed_iso = now_utc_iso()
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    records = parse_blocks(soup, url, observed_iso)
    ensure_dir(pathlib.Path(out_path).parent.as_posix())
    write_json(out_path, records)
    print(f"wrote {len(records)} records -> {out_path}")


def cli() -> None:
    parser = argparse.ArgumentParser(description="Probe Sportscapping free NBA picks")
    parser.add_argument("--url", default=DEFAULT_URL, help="Page URL to fetch")
    parser.add_argument("--out", default="data/raw_sportscapping_nba.json", help="Output JSON path")
    args = parser.parse_args()
    main(args.url, args.out)


if __name__ == "__main__":
    cli()
