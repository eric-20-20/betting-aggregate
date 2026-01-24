import re
import requests

URL = "https://www.covers.com/sport/basketball/nba/matchup/362668/picks"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.covers.com/",
    "Connection": "keep-alive",
})

def snippet_around(html: str, needle: str, window: int = 400) -> str:
    i = html.lower().find(needle.lower())
    if i == -1:
        return ""
    start = max(0, i - window)
    end = min(len(html), i + len(needle) + window)
    return html[start:end].replace("\n", "\\n")

def main():
    r = SESSION.get(URL, timeout=20)
    r.raise_for_status()
    html = r.text

    print("=== BASIC ===")
    print("url:", URL)
    print("html_len:", len(html))

    # 1) Embedded state checks
    print("\n=== EMBEDDED STATE CHECKS ===")
    has_next = "__NEXT_DATA__" in html
    has_ldjson = 'application/ld+json' in html
    has_initial_state = bool(re.search(r'__INITIAL_STATE__|INITIAL_STATE|APOLLO_STATE', html))
    print("has __NEXT_DATA__:", has_next)
    print("has application/ld+json:", has_ldjson)
    print("has INITIAL_STATE/APOLLO_STATE:", has_initial_state)

    if has_next:
        print("\n--- snippet: __NEXT_DATA__ ---")
        print(snippet_around(html, "__NEXT_DATA__", window=600)[:2000])

    # 2) Look for pick-like strings in *raw HTML* (not soup text)
    print("\n=== RAW HTML PICK-SIGNAL CHECKS ===")

    needles = [
        "Pick made",
        "Betting Analyst",
        "MoneyLine",
        "Spread",
        "Total",
        "Over",
        "Under",
        "(+",
        "(-",
        "+120",
        "-110",
    ]

    for n in needles:
        found = n.lower() in html.lower()
        print(f"contains '{n}':", found)

    # Stronger regex signals:
    patterns = {
        "spread_like": r"\b[A-Z]{2,3}\s*[+-]\d+(?:\.\d+)?\b",
        "total_like": r"\b(?:Over|Under|O|U)\s*\d+(?:\.\d+)?\b",
        "moneyline_like": r"\(\s*[+-]\d{3,}\s*\)|\b[+-]\d{3,}\b",
        "prop_like": r"\b(?:Points|Rebounds|Assists|PRA|Pts\+Reb\+Ast)\b",
    }

    for name, pat in patterns.items():
        hits = list(re.finditer(pat, html, flags=re.IGNORECASE))
        print(f"{name} hits:", len(hits))
        if hits:
            m = hits[0]
            start = max(0, m.start() - 250)
            end = min(len(html), m.end() + 250)
            print(f"--- first {name} snippet ---")
            print(html[start:end].replace("\n", "\\n"))

    # 3) If picks are loaded via JS, you’ll often see API URLs in HTML
    print("\n=== API/JSON ENDPOINT HINTS ===")
    api_hints = re.findall(r"https?://[^\"']+(?:api|graphql|json|picks|odds)[^\"']*", html, flags=re.IGNORECASE)
    print("api_hint_urls_found:", len(api_hints))
    for u in api_hints[:15]:
        print(" -", u)

    print("\n=== DONE ===")

if __name__ == "__main__":
    main()