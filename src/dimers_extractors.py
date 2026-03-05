"""
Dimers.com DOM extraction functions for Playwright.

Extracts best bets and player props from Dimers pages.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from playwright.sync_api import Page, Locator


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass
class RawDimersRecord:
    """Raw record extracted from Dimers pages."""
    source_id: str
    source_surface: str
    sport: str
    market_family: str
    observed_at_utc: str
    canonical_url: str
    raw_pick_text: str
    raw_block: str
    raw_fingerprint: str

    # Dimers-specific metrics
    probability_pct: Optional[float] = None
    edge_pct: Optional[float] = None
    best_odds: Optional[str] = None
    badge: Optional[str] = None

    # Matchup info
    matchup_hint: Optional[str] = None
    event_date_hint: Optional[str] = None

    # For props
    player_name: Optional[str] = None
    stat_type: Optional[str] = None
    direction: Optional[str] = None
    line: Optional[float] = None

    # For game bets
    team_pick: Optional[str] = None
    market_type: Optional[str] = None


def _safe_text(locator: Locator, timeout_ms: int = 500) -> Optional[str]:
    """Safely extract text from a locator."""
    try:
        if locator.count() == 0:
            return None
        return locator.first.text_content(timeout=timeout_ms)
    except Exception:
        return None


def _classify_market(pick_text: str) -> str:
    """Classify market type from pick text."""
    if not pick_text:
        return "unknown"
    lower = pick_text.lower()

    if "win" in lower or " ml" in lower:
        return "moneyline"
    if re.search(r"[+-]\d+\.?\d*", pick_text) and "over" not in lower and "under" not in lower:
        return "spread"
    if "over" in lower or "under" in lower:
        return "total"
    return "unknown"


def click_load_more_until_done(page: Page, max_clicks: int = 20, debug: bool = False) -> int:
    """Click 'Load More' button until all content loaded."""
    clicks = 0
    selectors = [
        "button:has-text('Load More')",
        "button:has-text('Show More')",
        "button:has-text('View More')",
    ]
    selector = ", ".join(selectors)

    while clicks < max_clicks:
        btn = page.locator(selector)
        if btn.count() == 0:
            break
        try:
            first_btn = btn.first
            if not first_btn.is_visible(timeout=1000):
                break
            if first_btn.is_disabled():
                break
            first_btn.click()
            page.wait_for_timeout(1500)
            clicks += 1
            if debug:
                print(f"[dimers] Clicked load more, total clicks: {clicks}")
        except Exception as e:
            if debug:
                print(f"[dimers] Load more click failed: {e}")
            break
    return clicks


def scroll_to_load_all(page: Page, max_scrolls: int = 25, debug: bool = False) -> int:
    """Scroll page to trigger lazy loading."""
    scrolls = 0
    last_height = 0
    stable_count = 0

    for _ in range(max_scrolls):
        try:
            current_height = page.evaluate("document.body.scrollHeight")
            if current_height == last_height:
                stable_count += 1
                if stable_count >= 3:
                    break
            else:
                stable_count = 0
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(2000)
            scrolls += 1
            last_height = current_height
            if debug:
                print(f"[dimers] Scrolled, height: {current_height}")
        except Exception as e:
            if debug:
                print(f"[dimers] Scroll failed: {e}")
            break
    return scrolls


def extract_best_bets(page: Page, canonical_url: str, debug: bool = False) -> List[RawDimersRecord]:
    """
    Extract game bets from Dimers best-bets page using JS evaluation.
    """
    observed = _now_iso()
    records: List[RawDimersRecord] = []

    # NBA team names for filtering
    NBA_TEAMS = [
        'Hawks', 'Celtics', 'Nets', 'Hornets', 'Bulls', 'Cavaliers', 'Mavericks',
        'Nuggets', 'Pistons', 'Warriors', 'Rockets', 'Pacers', 'Clippers', 'Lakers',
        'Grizzlies', 'Heat', 'Bucks', 'Timberwolves', 'Pelicans', 'Knicks', 'Thunder',
        'Magic', 'Sixers', '76ers', 'Suns', 'Blazers', 'Kings', 'Spurs', 'Raptors',
        'Jazz', 'Wizards'
    ]

    # Use JavaScript to extract bet data from Dimers' Angular bet-row components.
    # Each bet card is an <app-value-bet-row class="value-bet"> element.
    bets_data = page.evaluate("""
        (nbaTeams) => {
            const bets = [];
            const cards = document.querySelectorAll('app-value-bet-row.value-bet');

            for (const el of cards) {
                const text = el.textContent || '';

                // Filter for NBA teams only
                const isNBA = nbaTeams.some(team => text.includes(team));
                if (!isNBA) continue;

                // Extract probability (may be locked behind paywall)
                let probability = null;
                const probMatch = text.match(/Probability[^\\d]*(\\d+\\.?\\d*)\\s*%/i);
                if (probMatch) probability = parseFloat(probMatch[1]);

                let edge = null;
                const edgeMatch = text.match(/Edge[^\\d]*(\\d+\\.?\\d*)\\s*%/i);
                if (edgeMatch) edge = parseFloat(edgeMatch[1]);

                // Extract matchup — look for "Team vs. Team"
                let matchup = null;
                let awayTeam = null;
                let homeTeam = null;
                const matchupMatch = text.match(/([A-Z][a-z]+(?:\\s[A-Z][a-z]+)?)\\s+(?:vs\\.?|@)\\s+([A-Z][a-z]+(?:\\s[A-Z][a-z]+)?)/);
                if (matchupMatch) {
                    awayTeam = matchupMatch[1].trim();
                    homeTeam = matchupMatch[2].trim();
                    matchup = awayTeam + ' vs. ' + homeTeam;
                }

                // Extract odds
                let odds = null;
                const oddsMatch = text.match(/([+-]\\d{3,4})/);
                if (oddsMatch) odds = oddsMatch[1];

                // Extract date
                let dateHint = null;
                const dateMatch = text.match(/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\\s+\\d{1,2}/i);
                if (dateMatch) dateHint = dateMatch[0];

                // Extract team pick and market type
                let teamPick = null;
                let marketType = 'unknown';
                let pickText = null;

                // Look for "TeamName win" pattern (moneyline)
                for (const team of nbaTeams) {
                    const winRegex = new RegExp(team + '\\\\s+win', 'i');
                    if (winRegex.test(text)) {
                        teamPick = team;
                        marketType = 'moneyline';
                        pickText = team + ' win';
                        break;
                    }
                }

                // Look for spread pattern "TeamName +/-X.X"
                if (!teamPick) {
                    for (const team of nbaTeams) {
                        const spreadRegex = new RegExp(team + '\\\\s+([+-]\\\\d+\\\\.?\\\\d*)', 'i');
                        const spreadMatch = text.match(spreadRegex);
                        if (spreadMatch) {
                            teamPick = team;
                            marketType = 'spread';
                            pickText = team + ' ' + spreadMatch[1];
                            break;
                        }
                    }
                }

                // Look for total (over/under)
                if (!teamPick && (text.toLowerCase().includes('over') || text.toLowerCase().includes('under'))) {
                    const totalMatch = text.match(/(Over|Under)\\s+([\\d.]+)/i);
                    if (totalMatch) {
                        marketType = 'total';
                        pickText = totalMatch[0];
                    }
                }

                // Badge extraction
                let badge = null;
                const badges = ['Sweet Spot', 'High Edge', 'Value Play', 'Best Value', 'Best Bet'];
                for (const b of badges) {
                    if (text.includes(b)) { badge = b; break; }
                }

                // Include if we have matchup data and a pick
                if (matchup && (teamPick || pickText)) {
                    bets.push({
                        text: text.slice(0, 500),
                        html: el.outerHTML.slice(0, 2000),
                        probability,
                        edge,
                        odds,
                        matchup,
                        dateHint,
                        pickText: pickText || matchup,
                        teamPick,
                        marketType,
                        badge
                    });
                }
            }
            return bets;
        }
    """, NBA_TEAMS)

    if debug:
        print(f"[dimers] JS extraction found {len(bets_data)} bet cards")

    for i, bet in enumerate(bets_data):
        try:
            probability_pct = bet.get("probability")
            edge_pct = bet.get("edge")
            best_odds = bet.get("odds")
            matchup_hint = bet.get("matchup")
            event_date_hint = bet.get("dateHint")
            pick_text = bet.get("pickText", "")
            team_pick = bet.get("teamPick")
            market_type = bet.get("marketType", "unknown")
            badge = bet.get("badge")
            card_html = bet.get("html", "")

            fingerprint = sha256_digest(f"dimers|best_bets|{matchup_hint or ''}|{pick_text}")

            record = RawDimersRecord(
                source_id="dimers",
                source_surface="dimers_best_bets",
                sport="NBA",
                market_family="standard",
                observed_at_utc=observed,
                canonical_url=canonical_url,
                raw_pick_text=pick_text,
                raw_block=card_html,
                raw_fingerprint=fingerprint,
                probability_pct=probability_pct,
                edge_pct=edge_pct,
                best_odds=best_odds,
                badge=badge,
                matchup_hint=matchup_hint,
                event_date_hint=event_date_hint,
                team_pick=team_pick,
                market_type=market_type,
            )
            records.append(record)

            if debug and i < 5:
                print(f"[dimers] Bet {i}: pick='{pick_text[:50]}' prob={probability_pct} edge={edge_pct} odds={best_odds}")

        except Exception as e:
            if debug:
                print(f"[dimers] Error processing bet {i}: {e}")
            continue

    if debug:
        print(f"[dimers] Extracted {len(records)} best bets")

    return records


def extract_best_props(page: Page, canonical_url: str, debug: bool = False) -> List[RawDimersRecord]:
    """
    Extract player props from Dimers best-props page using JS evaluation.
    """
    observed = _now_iso()
    records: List[RawDimersRecord] = []

    # Use JavaScript to extract prop data
    props_data = page.evaluate("""
        () => {
            const props = [];
            const processedFingerprints = new Set();

            // Find all potential prop card containers
            const allElements = document.querySelectorAll('*');

            for (const el of allElements) {
                const text = el.textContent || '';

                // Props must have Over/Under + Probability + Edge
                const hasOverUnder = /\\b(over|under)\\b/i.test(text);
                const hasProb = text.includes('Probability');
                const hasEdge = text.includes('Edge');

                if (!hasOverUnder || !hasProb || !hasEdge) continue;

                // Check element size - prop cards are typically 100-400px height
                const rect = el.getBoundingClientRect();
                if (rect.height < 80 || rect.height > 450 || rect.width < 200) continue;

                // Skip promotional content
                if (text.includes('DFS') || text.includes('Claim Now')) continue;
                if (text.includes('Fantasy') || text.includes('Lineup')) continue;
                if (text.includes('Deposit') || text.includes('Bonus')) continue;

                // Extract probability
                let probability = null;
                const probMatch = text.match(/Probability[^\\d]*(\\d+\\.?\\d*)\\s*%/i);
                if (probMatch) probability = parseFloat(probMatch[1]);

                // Extract edge
                let edge = null;
                const edgeMatch = text.match(/Edge[^\\d]*(\\d+\\.?\\d*)\\s*%/i);
                if (edgeMatch) edge = parseFloat(edgeMatch[1]);

                if (probability === null && edge === null) continue;

                // Extract player name more robustly
                // Look for pattern: "PlayerName Over/Under X.X StatType"
                // The player name typically appears before "Over" or "Under"
                let playerName = null;
                let direction = null;
                let line = null;
                let statType = null;

                // Try to find the full prop line pattern
                // Pattern: "FirstName LastName [Jr./Sr./III] Over/Under X.X [Total] StatType"
                const fullPropMatch = text.match(
                    /([A-Z][a-z]+(?:'[A-Z][a-z]+)?\\s+[A-Z][a-z]+(?:-[A-Z][a-z]+)?(?:\\s+(?:Jr\\.?|Sr\\.?|III?|IV|V))?)\\s+(Over|Under)\\s+([\\d.]+)\\s+(?:Total\\s+)?([A-Za-z]+(?:[\\s+-][A-Za-z]+)*?)(?:\\s|Probability|Edge|$)/i
                );

                if (fullPropMatch) {
                    playerName = fullPropMatch[1].trim();
                    direction = fullPropMatch[2];
                    line = parseFloat(fullPropMatch[3]);
                    statType = fullPropMatch[4].trim();

                    // Clean up stat type - remove trailing words that aren't stats
                    statType = statType.replace(/\\s*(Probability|Edge|Proj|Projection).*$/i, '').trim();
                }

                // If we didn't get player name, try alternative patterns
                if (!playerName) {
                    // Look for name before Over/Under
                    const nameMatch = text.match(/([A-Z][a-z]+(?:'[A-Z][a-z]+)?\\s+[A-Z][a-z]+(?:-[A-Z][a-z]+)?(?:\\s+(?:Jr\\.?|Sr\\.?|III?|IV|V))?)\\s+(?:Over|Under)/i);
                    if (nameMatch) {
                        playerName = nameMatch[1].trim();
                    }
                }

                // If we didn't get direction/line/stat, try to extract them separately
                if (!direction || !line) {
                    const propDetailsMatch = text.match(/(Over|Under)\\s+([\\d.]+)\\s+(?:Total\\s+)?([A-Za-z]+(?:[\\s+-][A-Za-z]+)*?)(?:\\s|Probability|Edge|$)/i);
                    if (propDetailsMatch) {
                        direction = direction || propDetailsMatch[1];
                        line = line || parseFloat(propDetailsMatch[2]);
                        statType = statType || propDetailsMatch[3].trim().replace(/\\s*(Probability|Edge|Proj|Projection).*$/i, '').trim();
                    }
                }

                // Skip if we don't have essential prop data
                if (!playerName || !direction || line === null) continue;

                // Create fingerprint to dedupe - use player + line + stat + direction
                const fingerprint = `${playerName}-${line}-${statType}-${direction}`;
                if (processedFingerprints.has(fingerprint)) continue;
                processedFingerprints.add(fingerprint);

                // Extract odds
                let odds = null;
                const oddsMatch = text.match(/([+-]\\d{3,4})/);
                if (oddsMatch) odds = oddsMatch[1];

                // Extract matchup
                let matchup = null;
                const matchupMatch = text.match(/([A-Z][a-z]+(?:\\s[A-Z][a-z]+)?)\\s+(?:vs\\.?|@)\\s+([A-Z][a-z]+(?:\\s[A-Z][a-z]+)?)/);
                if (matchupMatch) matchup = matchupMatch[1] + ' vs. ' + matchupMatch[2];

                // Extract date
                let dateHint = null;
                const dateMatch = text.match(/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\\s+\\d{1,2}/i);
                if (dateMatch) dateHint = dateMatch[0];

                // Badge extraction
                let badge = null;
                const badges = ['Board Crasher', 'Dime Dropper', 'High Edge', 'Sweet Spot', 'Hot Streak', 'Sharpshooter'];
                for (const b of badges) {
                    if (text.includes(b)) { badge = b; break; }
                }

                // Build clean pick text
                const pickText = `${playerName} ${direction} ${line} ${statType}`;

                props.push({
                    text: text.slice(0, 500),
                    html: el.outerHTML.slice(0, 2000),
                    playerName,
                    direction,
                    line,
                    statType,
                    probability,
                    edge,
                    odds,
                    matchup,
                    dateHint,
                    pickText,
                    badge
                });
            }
            return props;
        }
    """)

    if debug:
        print(f"[dimers] JS extraction found {len(props_data)} prop cards")

    for i, prop in enumerate(props_data):
        try:
            player_name = prop.get("playerName")
            direction = prop.get("direction")
            line = prop.get("line")
            stat_type = prop.get("statType")
            probability_pct = prop.get("probability")
            edge_pct = prop.get("edge")
            best_odds = prop.get("odds")
            matchup_hint = prop.get("matchup")
            event_date_hint = prop.get("dateHint")
            pick_text = prop.get("pickText", "")
            badge = prop.get("badge")
            card_html = prop.get("html", "")

            fingerprint = sha256_digest(
                f"dimers|props|{player_name or ''}|{stat_type or ''}|{direction or ''}|{line or ''}"
            )

            record = RawDimersRecord(
                source_id="dimers",
                source_surface="dimers_best_props",
                sport="NBA",
                market_family="player_prop",
                observed_at_utc=observed,
                canonical_url=canonical_url,
                raw_pick_text=pick_text,
                raw_block=card_html,
                raw_fingerprint=fingerprint,
                probability_pct=probability_pct,
                edge_pct=edge_pct,
                best_odds=best_odds,
                badge=badge,
                matchup_hint=matchup_hint,
                event_date_hint=event_date_hint,
                player_name=player_name,
                stat_type=stat_type,
                direction=direction,
                line=line,
            )
            records.append(record)

            if debug and i < 5:
                print(
                    f"[dimers] Prop {i}: {player_name} {direction} {line} {stat_type} "
                    f"prob={probability_pct} edge={edge_pct} odds={best_odds}"
                )

        except Exception as e:
            if debug:
                print(f"[dimers] Error processing prop {i}: {e}")
            continue

    if debug:
        print(f"[dimers] Extracted {len(records)} best props")

    return records
