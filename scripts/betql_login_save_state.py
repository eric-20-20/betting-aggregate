# scripts/betql_login_save_state.py
from playwright.sync_api import sync_playwright
import os, sys
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
BETQL_URL = "https://betql.co/nba/odds"
STATE_PATH = "data/betql_storage_state.json"

def main() -> None:
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        page.goto(BETQL_URL, wait_until="domcontentloaded")

        print("\n1) Log into BetQL in the opened browser window.")
        print("2) After you see the logged-in page, come back here and press Enter.\n")
        input("Press Enter to save session state...")

        context.storage_state(path=STATE_PATH)
        print(f"\n✅ Saved storage state to: {STATE_PATH}\n")

        browser.close()

if __name__ == "__main__":
    main()