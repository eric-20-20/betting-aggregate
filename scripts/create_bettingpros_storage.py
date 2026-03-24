#!/usr/bin/env python3
"""
Create BettingPros storage state for authenticated scraping.

This script opens a browser window for you to log in to BettingPros.
After logging in, close the browser to save the session.

Usage:
    python3 scripts/create_bettingpros_storage.py

The storage state will be saved to: data/bettingpros_storage_state.json

Credentials: BETTINGPROS_EMAIL / BETTINGPROS_PASSWORD
Falls back to BETQL_EMAIL / BETQL_PASSWORD if not set.
"""

import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("Error: playwright is not installed")
    print("Install with: pip install playwright && playwright install chromium")
    sys.exit(1)

STORAGE_PATH = ROOT / "data" / "bettingpros_storage_state.json"
LOGIN_URL = "https://www.bettingpros.com/login/"


def load_env() -> None:
    env_path = ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                v = v.strip().strip("'\"")
                os.environ.setdefault(k.strip(), v)


def get_credentials() -> tuple[str, str]:
    email = os.environ.get("BETTINGPROS_EMAIL") or os.environ.get("BETQL_EMAIL", "")
    password = os.environ.get("BETTINGPROS_PASSWORD") or os.environ.get("BETQL_PASSWORD", "")
    email, password = email.strip(), password.strip()
    return email, password


def main():
    load_env()
    email, password = get_credentials()

    print("=" * 60)
    print("BettingPros Storage State Creator")
    print("=" * 60)
    print()

    Path(ROOT / "data").mkdir(exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1400, "height": 900},
        )
        page = context.new_page()

        print(f"Opening: {LOGIN_URL}")
        page.goto(LOGIN_URL, wait_until="domcontentloaded")

        # Try to auto-fill credentials if available
        if email and password:
            print(f"  Auto-filling credentials for {email}...")
            try:
                page.wait_for_selector('input[type="email"], input[name="email"], input[placeholder*="email" i]', timeout=5000)
                email_sel = page.query_selector('input[type="email"], input[name="email"], input[placeholder*="email" i]')
                pw_sel = page.query_selector('input[type="password"]')
                if email_sel:
                    email_sel.fill(email)
                if pw_sel:
                    pw_sel.fill(password)
                print("  Credentials filled. Submit the form or adjust if needed.")
            except Exception:
                print("  Could not auto-fill credentials — please log in manually.")
        else:
            print("  No credentials found in .env — please log in manually.")

        print()
        print("After logging in successfully:")
        print("  - Verify you can see expert picks pages")
        print("  - Then CLOSE the browser window to save the session")
        print()
        print("=" * 60)
        print()

        try:
            page.wait_for_event("close", timeout=600000)  # 10-minute timeout
        except Exception:
            pass

        context.storage_state(path=str(STORAGE_PATH))

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
    print("  python3 bettingpros_ingest.py --debug")
    print("  python3 scripts/backfill_bettingpros.py --debug")
    print("=" * 60)


if __name__ == "__main__":
    main()
