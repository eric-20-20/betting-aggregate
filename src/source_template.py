"""Source Template: Copy this file to create a new source implementation.

QUICK START:
1. Copy this file to src/{your_source}_source.py
2. Replace "example" with your source name (e.g., "draftkings")
3. Implement the abstract methods marked with TODO
4. Run `python -m pytest tests/test_{your_source}_source.py -v`
5. Add to ingestion pipeline

Target: <2 hours to onboard a new source.

Example sources to reference:
- src/action_ingest.py (expert picks from game pages)
- src/covers_ingest.py (expert picks from matchup pages)
- scripts/backfill_sportsline_expert_picks.py (authenticated API)
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Import the source contract
from source_contract import (
    BaseSource,
    RawPickSchema,
    NormalizedPickSchema,
    ValidationResult,
    build_event_key,
    build_day_key,
    build_player_key,
    slugify_player_name,
    NBA_SPORT,
    NCAAB_SPORT,
)

# Import replay harness for snapshot testing
from source_replay import ReplayHarness, capture_html_snapshot

# Import your team data store
try:
    from store import DataStore
except ImportError:
    DataStore = None


# =============================================================================
# CONFIGURATION (Customize these)
# =============================================================================

# TODO: Set your source identifier (lowercase, no spaces)
SOURCE_ID = "example"

# TODO: Set your source surface (describes what part of the source this scrapes)
SOURCE_SURFACE = "example_expert_picks"

# TODO: Set default sport (can be overridden)
DEFAULT_SPORT = NBA_SPORT

# TODO: Configure URLs for each sport
LISTING_URLS = {
    NBA_SPORT: "https://example.com/nba/picks",
    NCAAB_SPORT: "https://example.com/college-basketball/picks",
}

# TODO: Configure patterns for parsing
GAME_URL_PATTERN = re.compile(r"/game/([^/]+)/(\d+)")
EXPERT_NAME_PATTERN = re.compile(r'"expert_name":\s*"([^"]+)"')


# =============================================================================
# SOURCE IMPLEMENTATION
# =============================================================================

class ExampleSource(BaseSource):
    """Example source implementation.

    TODO: Rename this class to {YourSource}Source
    """

    def __init__(
        self,
        sport: str = DEFAULT_SPORT,
        debug: bool = False,
        replay_mode: str = "passthrough",  # "capture", "replay", "passthrough"
        store: Optional[Any] = None,
    ):
        """Initialize the source.

        Args:
            sport: Sport to scrape (NBA, NCAAB)
            debug: Enable debug logging
            replay_mode: Snapshot mode for testing
            store: DataStore instance (will create if None)
        """
        self._sport = sport
        self.debug = debug
        self.replay = ReplayHarness(SOURCE_ID, sport=sport, mode=replay_mode)
        self.store = store or (DataStore(sport=sport) if DataStore else None)

        # State for parsing
        self._raw_picks: List[RawPickSchema] = []
        self._errors: List[str] = []

    # -------------------------------------------------------------------------
    # Required Properties (implement these)
    # -------------------------------------------------------------------------

    @property
    def source_id(self) -> str:
        """Unique identifier for this source."""
        return SOURCE_ID

    @property
    def source_surface(self) -> str:
        """Surface identifier."""
        return SOURCE_SURFACE

    @property
    def sport(self) -> str:
        """Sport this instance is scraping."""
        return self._sport

    # -------------------------------------------------------------------------
    # Required Methods (implement these)
    # -------------------------------------------------------------------------

    def fetch_raw_picks(self) -> List[RawPickSchema]:
        """Fetch and parse raw picks from the source.

        TODO: Implement your scraping logic here.

        This method should:
        1. Fetch the listing page(s)
        2. Parse out individual pick "cards" or entries
        3. For each pick, create a RawPickSchema

        Returns:
            List of RawPickSchema instances
        """
        self._raw_picks = []
        self._errors = []

        listing_url = LISTING_URLS.get(self.sport)
        if not listing_url:
            raise ValueError(f"No listing URL configured for sport: {self.sport}")

        # Try replay first if in replay mode
        if self.replay.mode == "replay":
            html = self.replay.replay(listing_url)
            if html:
                return self._parse_listing_html(listing_url, html)

        # Fetch live content
        html = self._fetch_url(listing_url)
        if not html:
            return []

        # Capture snapshot if in capture mode
        if self.replay.mode == "capture":
            self.replay.capture("html", listing_url, html, tags=["listing"])

        return self._parse_listing_html(listing_url, html)

    def normalize_pick(self, raw: RawPickSchema) -> Optional[NormalizedPickSchema]:
        """Normalize a single raw pick.

        TODO: Implement your normalization logic here.

        This method should:
        1. Parse team codes from raw_pick_text or matchup_hint
        2. Determine market_type (spread, total, moneyline, player_prop)
        3. Extract line, odds, selection
        4. Validate the pick is complete enough for consensus

        Returns:
            NormalizedPickSchema or None if normalization fails
        """
        try:
            # Parse teams from matchup hint
            # TODO: Adjust parsing for your source's format
            away_team, home_team = self._parse_teams(raw.matchup_hint or raw.raw_pick_text)
            if not away_team or not home_team:
                return self._ineligible(raw, "could not parse teams")

            # Parse date from event_start_time_utc or observed_at_utc
            event_time = raw.event_start_time_utc or raw.observed_at_utc
            dt = datetime.fromisoformat(event_time.replace("Z", "+00:00"))

            # Build canonical keys
            event_key = build_event_key(
                self.sport,
                dt.year, dt.month, dt.day,
                away_team, home_team
            )
            day_key = build_day_key(self.sport, dt.year, dt.month, dt.day)

            # Parse market details
            # TODO: Implement market parsing for your source
            market_type, selection, line, odds, side = self._parse_market(raw)
            if not market_type:
                return self._ineligible(raw, "could not determine market type")

            # Build normalized pick
            return NormalizedPickSchema(
                provenance={
                    "source_id": self.source_id,
                    "source_surface": self.source_surface,
                    "sport": self.sport,
                    "observed_at_utc": raw.observed_at_utc,
                    "canonical_url": raw.canonical_url,
                    "raw_fingerprint": raw.raw_fingerprint,
                    "raw_pick_text": raw.raw_pick_text,
                    "expert_name": raw.expert_name,
                    "expert_handle": raw.expert_handle,
                },
                event={
                    "sport": self.sport,
                    "event_key": event_key,
                    "day_key": day_key,
                    "away_team": away_team,
                    "home_team": home_team,
                    "event_start_time_utc": raw.event_start_time_utc,
                },
                market={
                    "market_type": market_type,
                    "market_family": "player_prop" if market_type == "player_prop" else "standard",
                    "selection": selection,
                    "line": line,
                    "odds": odds,
                    "side": side,
                },
                eligible_for_consensus=True,
                ineligibility_reason=None,
            )

        except Exception as e:
            self._errors.append(f"normalize_pick error: {e}")
            return None

    # -------------------------------------------------------------------------
    # Helper Methods (customize as needed)
    # -------------------------------------------------------------------------

    def _fetch_url(self, url: str) -> Optional[str]:
        """Fetch URL content.

        TODO: Implement your fetch logic. Options:
        - requests.get() for simple sites
        - Playwright for JS-rendered sites
        - Session with cookies for authenticated sites
        """
        try:
            import requests
            resp = requests.get(url, timeout=30, headers={
                "User-Agent": "Mozilla/5.0 (compatible; betting-aggregate/1.0)",
            })
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            self._errors.append(f"fetch error: {url}: {e}")
            return None

    def _parse_listing_html(self, url: str, html: str) -> List[RawPickSchema]:
        """Parse listing page HTML into raw picks.

        TODO: Implement your HTML parsing logic here.
        """
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            self._errors.append("beautifulsoup4 not installed")
            return []

        soup = BeautifulSoup(html, "html.parser")
        picks = []

        # TODO: Find pick cards/entries in the HTML
        # Example: cards = soup.select(".pick-card")
        cards = soup.select(".pick-card")

        for card in cards:
            try:
                # TODO: Extract fields from each card
                raw_pick_text = card.get_text(strip=True)[:500]
                raw_block = str(card)[:2000]

                # Extract metadata
                expert_name = self._extract_expert_name(card)
                matchup_hint = self._extract_matchup(card)

                # Create RawPickSchema
                pick = RawPickSchema(
                    source_id=self.source_id,
                    source_surface=self.source_surface,
                    sport=self.sport,
                    market_family="standard",  # or "player_prop"
                    observed_at_utc=datetime.now(timezone.utc).isoformat(),
                    canonical_url=url,
                    raw_pick_text=raw_pick_text,
                    raw_block=raw_block,
                    raw_fingerprint=RawPickSchema.compute_fingerprint(
                        self.source_id, self.source_surface, url, raw_pick_text
                    ),
                    expert_name=expert_name,
                    matchup_hint=matchup_hint,
                )
                picks.append(pick)

            except Exception as e:
                self._errors.append(f"parse card error: {e}")
                continue

        self._raw_picks = picks
        return picks

    def _extract_expert_name(self, card) -> Optional[str]:
        """Extract expert name from a card element.

        TODO: Customize for your source's HTML structure.
        """
        # Example: name_elem = card.select_one(".expert-name")
        # return name_elem.text.strip() if name_elem else None
        return None

    def _extract_matchup(self, card) -> Optional[str]:
        """Extract matchup string from a card element.

        TODO: Customize for your source's HTML structure.
        """
        # Example: matchup_elem = card.select_one(".matchup")
        # return matchup_elem.text.strip() if matchup_elem else None
        return None

    def _parse_teams(self, matchup_str: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        """Parse away and home teams from matchup string.

        TODO: Implement team parsing for your source's format.

        Common formats:
        - "LAL @ BOS" -> ("LAL", "BOS")
        - "Lakers at Celtics" -> need to map to codes
        - "LAL vs BOS" -> ("LAL", "BOS")
        """
        if not matchup_str:
            return None, None

        # Try common patterns
        # Pattern 1: "AWAY @ HOME"
        match = re.search(r"([A-Z]{2,5})\s*[@vV][sS]?\s*([A-Z]{2,5})", matchup_str)
        if match:
            return match.group(1), match.group(2)

        # Pattern 2: Use store for name -> code lookup
        if self.store:
            # TODO: Use self.store.team_by_name() or similar
            pass

        return None, None

    def _parse_market(
        self, raw: RawPickSchema
    ) -> Tuple[Optional[str], Optional[str], Optional[float], Optional[int], Optional[str]]:
        """Parse market type and details from raw pick.

        TODO: Implement market parsing for your source's format.

        Returns:
            (market_type, selection, line, odds, side)
        """
        text = raw.raw_pick_text.upper()

        # Detect market type
        if "SPREAD" in text or re.search(r"[+-]\d+\.?\d*", text):
            market_type = "spread"
        elif "OVER" in text or "UNDER" in text or "TOTAL" in text:
            market_type = "total"
        elif "MONEYLINE" in text or "ML" in text or "WIN" in text:
            market_type = "moneyline"
        elif raw.player_hint or raw.stat_hint:
            market_type = "player_prop"
        else:
            return None, None, None, None, None

        # Extract line
        line = raw.line_hint
        if line is None:
            line_match = re.search(r"[+-]?(\d+\.?\d*)", raw.raw_pick_text)
            if line_match:
                line = float(line_match.group(1))

        # Extract odds
        odds = raw.odds_hint

        # Determine selection/side
        selection = None
        side = None

        if market_type == "total":
            side = "OVER" if "OVER" in text else "UNDER"
        elif market_type == "spread":
            # TODO: Determine which team is the selection
            pass
        elif market_type == "moneyline":
            # TODO: Determine which team is the selection
            pass

        return market_type, selection, line, odds, side

    def _ineligible(
        self, raw: RawPickSchema, reason: str
    ) -> NormalizedPickSchema:
        """Create an ineligible normalized pick."""
        return NormalizedPickSchema(
            provenance={
                "source_id": self.source_id,
                "source_surface": self.source_surface,
                "sport": self.sport,
                "observed_at_utc": raw.observed_at_utc,
                "canonical_url": raw.canonical_url,
                "raw_fingerprint": raw.raw_fingerprint,
            },
            event={},
            market={},
            eligible_for_consensus=False,
            ineligibility_reason=reason,
        )

    # -------------------------------------------------------------------------
    # Debug & Utility Methods
    # -------------------------------------------------------------------------

    def log(self, msg: str) -> None:
        """Log a debug message."""
        if self.debug:
            print(f"[{self.source_id}] {msg}")

    def get_errors(self) -> List[str]:
        """Get accumulated errors."""
        return self._errors

    def get_stats(self) -> Dict[str, Any]:
        """Get ingestion statistics."""
        return {
            "source_id": self.source_id,
            "source_surface": self.source_surface,
            "sport": self.sport,
            "raw_count": len(self._raw_picks),
            "error_count": len(self._errors),
        }


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

def main():
    """CLI entry point for testing the source."""
    import argparse

    parser = argparse.ArgumentParser(description=f"Ingest from {SOURCE_ID}")
    parser.add_argument("--sport", choices=["NBA", "NCAAB"], default=DEFAULT_SPORT)
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    parser.add_argument("--capture", action="store_true", help="Capture snapshots")
    parser.add_argument("--replay", action="store_true", help="Use captured snapshots")
    parser.add_argument("--output", "-o", help="Output file path")
    parser.add_argument("--validate", action="store_true", help="Validate all picks")
    args = parser.parse_args()

    # Determine replay mode
    if args.capture:
        replay_mode = "capture"
    elif args.replay:
        replay_mode = "replay"
    else:
        replay_mode = "passthrough"

    # Initialize source
    source = ExampleSource(
        sport=args.sport,
        debug=args.debug,
        replay_mode=replay_mode,
    )

    # Run ingestion
    print(f"Ingesting from {SOURCE_ID} ({args.sport})...")
    normalized, raw, stats = source.ingest(validate=args.validate)

    # Print stats
    print(f"\nStats:")
    print(f"  Raw picks:        {stats['raw_count']}")
    print(f"  Normalized:       {stats['normalized_count']}")
    print(f"  Eligible:         {stats['eligible_count']}")
    print(f"  Ineligible:       {stats['ineligible_count']}")
    if stats['errors']:
        print(f"  Errors:           {len(stats['errors'])}")
        for err in stats['errors'][:5]:
            print(f"    - {err}")

    # Output results
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(normalized, indent=2))
        print(f"\nWrote {len(normalized)} picks to {output_path}")

    # Show sample picks
    if normalized and args.debug:
        print("\nSample normalized pick:")
        print(json.dumps(normalized[0], indent=2))


if __name__ == "__main__":
    main()
