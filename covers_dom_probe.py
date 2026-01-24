import requests
from bs4 import BeautifulSoup
import re

URL = "https://www.covers.com/sport/basketball/nba/matchup/362668/picks"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.covers.com/",
    "Connection": "keep-alive",
})

def short(s: str, n: int = 240) -> str:
    s = re.sub(r"\s+", " ", s).strip()
    return s[:n] + ("…" if len(s) > n else "")

def css_path(tag) -> str:
    if not tag or not getattr(tag, "name", None):
        return ""
    parts = []
    cur = tag
    for _ in range(6):
        if not cur or not getattr(cur, "name", None):
            break
        ident = cur.name
        if cur.get("id"):
            ident += f"#{cur['id']}"
        cls = cur.get("class") or []
        if cls:
            ident += "." + ".".join(cls[:3])
        parts.append(ident)
        cur = cur.parent
    return " <- ".join(parts)

def main():
    r = SESSION.get(URL, timeout=25)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # Find exact Pick made badges
    nodes = soup.find_all(string=re.compile(r"Pick made:", re.IGNORECASE))
    print("pick_made_nodes:", len(nodes))
    if not nodes:
        return

    # For the first 2 nodes, walk ancestors and score them
    for idx, node in enumerate(nodes[:2]):
        print("\n==============================")
        print(f"NODE {idx}: {short(str(node), 200)}")
        parent = node.parent
        print("parent:", css_path(parent))

        # Walk up 15 ancestors; print summary + outer HTML length
        cur = parent
        for level in range(1, 16):
            if not cur:
                break
            txt = cur.get_text(" ", strip=True)
            txt_lower = txt.lower()

            # Signals we care about
            has_analyst = ("betting analyst" in txt_lower) or ("publishing editor" in txt_lower)
            has_pick_word = ("moneyline" in txt_lower) or ("spread" in txt_lower) or ("total" in txt_lower) or ("points scored" in txt_lower) or ("rebounds" in txt_lower) or ("assists" in txt_lower) or bool(re.search(r"\b[ou]\d+(?:\.\d+)?\b", txt_lower))
            has_banned = ("computer pick" in txt_lower) or ("ev model rating" in txt_lower) or ("consensus" in txt_lower)

            print(f"\n-- ancestor level {level} --")
            print("path:", css_path(cur))
            print("text_len:", len(txt))
            print("has_analyst:", has_analyst, "has_pick_signal:", has_pick_word, "has_banned_tokens:", has_banned)
            print("text_preview:", short(txt, 350))

            # If this looks like a real “pick card”, dump a chunk of HTML
            if has_analyst and has_pick_word:
                html = str(cur)
                print("\n>>> CANDIDATE CONTAINER FOUND (dumping outer HTML snippet) <<<")
                print("outer_html_len:", len(html))
                print(html[:2500].replace("\n", "\\n"))
                print("<<< END SNIPPET >>>")
                break

            cur = cur.parent

if __name__ == "__main__":
    main()