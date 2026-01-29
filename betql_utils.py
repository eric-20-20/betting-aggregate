from __future__ import annotations

import re
import unicodedata
from typing import Optional


def slugify_player_name(name: Optional[str]) -> Optional[str]:
    """Stable slugifier for BetQL player names.

    - Unicode NFKD normalize and strip diacritics
    - Lowercase
    - Replace non-alphanumerics with underscores
    - Collapse multiple underscores and trim edges
    """
    if not name:
        return None
    normalized = unicodedata.normalize("NFKD", str(name))
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = normalized.lower()
    normalized = normalized.replace(".", " ")
    normalized = re.sub(r"[′’']", "", normalized)
    tokens = [t for t in re.split(r"[^a-z0-9]+", normalized) if t]
    combined: list[str] = []
    initial_cluster: list[str] = []
    while tokens and len(tokens[0]) == 1:
        initial_cluster.append(tokens.pop(0))
    if initial_cluster:
        combined.append("".join(initial_cluster))
    combined.extend(tokens)
    slug = "_".join(combined)
    slug = re.sub(r"_+", "_", slug).strip("_")
    return slug or None


def extract_player_name_from_text(text: str) -> Optional[str]:
    """
    Grab the player name span that appears before the Over/Under token.
    Handles matchup prefixes like 'LAL@CLE ' and keeps initials/apostrophes.
    """
    if not text:
        return None
    cleaned = text.strip()
    cleaned = re.sub(r"^[A-Z]{2,3}\s*[@vVsS]\s*[A-Z]{2,3}\s+", "", cleaned)
    cleaned = re.sub(r"^[A-Z]{2,3}@?[A-Z]{2,3}\s+", "", cleaned)
    cleaned = re.sub(r"^\d+\s*stars?\s*", "", cleaned, flags=re.IGNORECASE)
    m = re.search(r"\b(over|under)\b", cleaned, flags=re.IGNORECASE)
    segment = cleaned[: m.start()] if m else cleaned
    segment = re.sub(r"\s+", " ", segment).strip()
    return segment or None
