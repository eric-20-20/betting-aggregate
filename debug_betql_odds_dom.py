from playwright.sync_api import sync_playwright
from pathlib import Path
import json, re, time

STORAGE = "data/betql_storage_state.json"
URL = "https://betql.co/nba/odds"

def main():
    if not Path(STORAGE).exists():
        raise SystemExit(f"missing storage state: {STORAGE}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        ctx = browser.new_context(storage_state=STORAGE)
        page = ctx.new_page()
        page.goto(URL, wait_until="domcontentloaded")
        page.wait_for_timeout(2500)

        # 1) Do rating buttons exist?
        btn_count = page.locator("button.games-table-column__rating-button").count()
        print("rating_button_count =", btn_count)

        # 2) Do star svgs exist?
        svg_count = page.locator("button.games-table-column__rating-button svg").count()
        print("rating_button_svg_count =", svg_count)

        # 3) Grab first 5 buttons and count svgs + show snippet
        n = min(btn_count, 5)
        for i in range(n):
            btn = page.locator("button.games-table-column__rating-button").nth(i)
            svgs = btn.locator("svg").count()
            text = (btn.inner_text() or "").strip().replace("\n", " ")
            html = btn.inner_html()
            print(f"\nBTN[{i}] svgs={svgs} text={text[:60]}")
            print(html[:300])
        
        row_like = page.locator("[class*='games-table-row'], [class*='games-table__row'], [role='row']")
        print("row_like_count =", row_like.count())
        if row_like.count() > 0:
            r0 = row_like.first
            print("row0 has rating buttons =", r0.locator("button.games-table-column__rating-button").count())
            print("row0 text sample =", (r0.inner_text() or "")[:200].replace("\n"," "))

        # 4) Save a screenshot for sanity
        page.screenshot(path="betql_debug_odds.png", full_page=True)
        print("\nsaved betql_debug_odds.png")
        browser.close()

if __name__ == "__main__":
    main()