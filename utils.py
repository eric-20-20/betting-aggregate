import re


def normalize_text(text: str) -> str:
    """
    Lowercase, strip punctuation, and collapse whitespace.
    This normalization is used consistently for alias matching.
    """
    lowered = text.lower()
    stripped = re.sub(r"[^a-z0-9\s]", " ", lowered)
    collapsed = re.sub(r"\s+", " ", stripped).strip()
    return collapsed
