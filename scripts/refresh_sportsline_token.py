#!/usr/bin/env python3
"""
Refresh SportsLine session by logging in automatically via Playwright.

SportsLine uses reCAPTCHA on login, which blocks headless browsers.
This script runs with a visible (non-headless) browser to bypass it,
but is otherwise fully automated — no user interaction required.

Credentials are read from SPORTSLINE_EMAIL / SPORTSLINE_PASSWORD in .env.
Falls back to BETQL_EMAIL / BETQL_PASSWORD if SportsLine-specific ones aren't set
(same credentials are used for both).

Usage:
    python3 scripts/refresh_sportsline_token.py
    python3 scripts/refresh_sportsline_token.py --check    # print expiry only
    python3 scripts/refresh_sportsline_token.py --force    # refresh even if fresh

Exit codes:
    0 = success (or not needed)
    1 = login failed
    2 = credentials not configured
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STATE_PATH = ROOT / "data" / "sportsline_storage_state.json"
LOGIN_URL = "https://www.sportsline.com/login"
POST_LOGIN_URL = "https://www.sportsline.com/nba/picks/"

# Cookie that signals a valid paid session (long-lived analytics/sub cookie)
AUTH_COOKIE = "_swb"


def load_env() -> None:
    env_path = ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())


def get_credentials() -> tuple[str, str]:
    email = os.environ.get("SPORTSLINE_EMAIL") or os.environ.get("BETQL_EMAIL", "")
    password = os.environ.get("SPORTSLINE_PASSWORD") or os.environ.get("BETQL_PASSWORD", "")
    email = email.strip()
    password = password.strip()
    if not email or not password:
        print("  ERROR: SPORTSLINE_EMAIL/PASSWORD (or BETQL_EMAIL/PASSWORD) must be set in .env",
              file=sys.stderr)
        sys.exit(2)
    return email, password


def get_session_age_days(state: dict) -> float:
    """Return how many days old the session is based on mtime, or inf if unknown."""
    if not STATE_PATH.exists():
        return float("inf")
    age = time.time() - STATE_PATH.stat().st_mtime
    return age / 86400


def is_logged_in(state: dict) -> bool:
    """Heuristic: check if userInfoCache shows a successful auth."""
    for origin in state.get("origins", []):
        for item in origin.get("localStorage", []):
            if item.get("name") == "userInfoCache":
                try:
                    cache = json.loads(item["value"])
                    return bool(cache.get("success"))
                except Exception:
                    pass
    return False


def do_login(email: str, password: str) -> bool:
    """Log in with visible browser (bypasses reCAPTCHA) and save storage state."""
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    print("  Launching browser (non-headless to bypass reCAPTCHA)...")
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled"],
        )
        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        )
        page = ctx.new_page()

        try:
            page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_selector('input[name="email"]', timeout=10000)

            page.fill('input[name="email"]', email)
            page.fill('input[name="password"]', password)
            # Use form.submit() to bypass reCAPTCHA button interaction
            page.evaluate("document.querySelector('form').submit()")

            # Wait to navigate away from the login page
            deadline = time.time() + 25
            logged_in = False
            while time.time() < deadline:
                if "login" not in page.url:
                    logged_in = True
                    break
                page.wait_for_timeout(500)

            if not logged_in:
                print(f"  ERROR: Still on login page after 25s. URL: {page.url}", file=sys.stderr)
                browser.close()
                return False

            # Navigate to picks page to ensure subscription cookies are set
            page.goto(POST_LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(2000)

            STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
            ctx.storage_state(path=str(STATE_PATH))
            browser.close()
            return True

        except PWTimeout as e:
            print(f"  ERROR: Timeout during SportsLine login: {e}", file=sys.stderr)
            browser.close()
            return False
        except Exception as e:
            print(f"  ERROR: Login failed: {e}", file=sys.stderr)
            browser.close()
            return False


def main() -> int:
    load_env()

    parser = argparse.ArgumentParser(description="Refresh SportsLine session")
    parser.add_argument("--check", action="store_true", help="Only print status, no refresh")
    parser.add_argument("--force", action="store_true", help="Refresh even if not near expiry")
    args = parser.parse_args()

    if STATE_PATH.exists():
        try:
            state = json.loads(STATE_PATH.read_text())
        except json.JSONDecodeError:
            state = {}
    else:
        state = {}
        print("  SportsLine storage state: MISSING")
        if args.check:
            return 1

    age_days = get_session_age_days(state)
    status = "fresh" if age_days <= 5 else "stale"
    print(f"  SportsLine session: {status} (last saved {age_days:.1f}d ago)")

    if args.check:
        return 0

    # Refresh if session is older than 5 days or --force
    if not args.force and age_days <= 5:
        print("  Session still fresh — no refresh needed.")
        return 0

    email, password = get_credentials()
    print(f"  Logging in as {email}...")

    success = do_login(email, password)
    if not success:
        return 1

    print("  Login successful — session saved.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
