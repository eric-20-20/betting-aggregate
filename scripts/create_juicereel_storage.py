#!/usr/bin/env python3
"""
Create JuiceReel storage state for authenticated scraping.

This script opens a browser window for you to log in to JuiceReel.
After logging in, close the browser to save the session.

Usage:
    python3 scripts/create_juicereel_storage.py

The storage state will be saved to: data/juicereel_storage_state.json
"""

import sys
from pathlib import Path

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("Error: playwright is not installed")
    print("Install with: pip install playwright && playwright install chromium")
    sys.exit(1)

STORAGE_PATH = "data/juicereel_storage_state.json"
LOGIN_URL = "https://app.juicereel.com/login"


def main():
    print("=" * 60)
    print("JuiceReel Storage State Creator")
    print("=" * 60)
    print()
    print("A browser window will open to the JuiceReel login page.")
    print("Please log in with your JuiceReel account.")
    print()
    print("After logging in successfully:")
    print("  - Verify you can see the expert picks pages")
    print("  - Then CLOSE the browser window to save the session")
    print()
    print("=" * 60)
    print()

    Path("data").mkdir(exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1400, "height": 900},
        )
        page = context.new_page()

        print(f"Opening: {LOGIN_URL}")
        page.goto(LOGIN_URL, wait_until="domcontentloaded")

        print()
        print("Waiting for you to log in and close the browser...")
        print("(The session will be saved when you close the browser window)")
        print()

        try:
            page.wait_for_event("close", timeout=600000)  # 10-minute timeout
        except Exception:
            pass

        context.storage_state(path=STORAGE_PATH)

        try:
            context.close()
            browser.close()
        except Exception:
            pass

    print()
    print("=" * 60)
    print(f"Storage state saved to: {STORAGE_PATH}")
    print()
    print("You can now run:")
    print("  python3 scripts/backfill_juicereel.py --debug")
    print("  python3 juicereel_ingest.py --debug")
    print("=" * 60)


if __name__ == "__main__":
    main()
