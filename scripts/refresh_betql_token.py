#!/usr/bin/env python3
"""
Refresh BetQL session by logging in headlessly via Playwright.

BetQL uses a HS256 JWT (node_auth_token) with no refresh endpoint — the only
way to get a new token is to log in again. This script does that automatically
using credentials from the environment (.env).

Usage:
    python3 scripts/refresh_betql_token.py
    python3 scripts/refresh_betql_token.py --check   # just print expiry, no refresh
    python3 scripts/refresh_betql_token.py --force   # refresh even if not near expiry

Exit codes:
    0 = success (or not needed)
    1 = login failed
    2 = credentials not configured
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STATE_PATH = ROOT / "data" / "betql_storage_state.json"
LOGIN_URL = "https://betql.co/login"
POST_LOGIN_URL = "https://betql.co/nba/odds"


def get_credentials() -> tuple[str, str]:
    email = os.environ.get("BETQL_EMAIL", "").strip()
    password = os.environ.get("BETQL_PASSWORD", "").strip()
    if not email or not password:
        print("  ERROR: BETQL_EMAIL and BETQL_PASSWORD must be set in .env", file=sys.stderr)
        sys.exit(2)
    return email, password


def get_token_expiry(state: dict) -> float | None:
    for cookie in state.get("cookies", []):
        if cookie.get("name") == "node_auth_token":
            expires = cookie.get("expires", -1)
            if expires and expires > 0:
                return float(expires)
            # Decode JWT exp claim
            token = cookie.get("value", "")
            parts = token.split(".")
            if len(parts) == 3:
                try:
                    payload = parts[1] + "=" * (4 - len(parts[1]) % 4)
                    claims = json.loads(base64.b64decode(payload))
                    exp = claims.get("exp")
                    if exp:
                        return float(exp)
                except Exception:
                    pass
    return None


def do_login(email: str, password: str) -> bool:
    """Log in headlessly and save storage state. Returns True on success."""
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    print("  Launching headless browser...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        )
        page = ctx.new_page()

        try:
            page.goto(LOGIN_URL, wait_until="networkidle", timeout=30000)

            # Click Log In button to open the modal/form
            page.click('button:has-text("Log In")', timeout=10000)
            page.wait_for_selector('input[type="email"]', timeout=10000)

            page.fill('input[type="email"]', email)
            page.fill('input[type="password"]', password)

            # Submit — look for a submit button inside the form
            page.click('button[type="submit"], form button:has-text("Log")', timeout=5000)

            # Wait for the node_auth_token cookie to appear (signals successful login)
            deadline = time.time() + 20
            logged_in = False
            while time.time() < deadline:
                cookies = ctx.cookies()
                if any(c["name"] == "node_auth_token" for c in cookies):
                    logged_in = True
                    break
                page.wait_for_timeout(500)

            if not logged_in:
                print("  ERROR: node_auth_token cookie never appeared — login may have failed.")
                print("  Page URL:", page.url)
                browser.close()
                return False

            # Navigate to the main app page to ensure all session cookies are set
            page.goto(POST_LOGIN_URL, wait_until="networkidle", timeout=30000)

            STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
            ctx.storage_state(path=str(STATE_PATH))
            browser.close()
            return True

        except PWTimeout as e:
            print(f"  ERROR: Timeout during login: {e}", file=sys.stderr)
            browser.close()
            return False
        except Exception as e:
            print(f"  ERROR: Login failed: {e}", file=sys.stderr)
            browser.close()
            return False


def main() -> int:
    # Load .env if present
    env_path = ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())

    parser = argparse.ArgumentParser(description="Refresh BetQL session token")
    parser.add_argument("--check", action="store_true", help="Only print expiry, do not refresh")
    parser.add_argument("--force", action="store_true", help="Refresh even if not near expiry")
    args = parser.parse_args()

    # Read current state
    if STATE_PATH.exists():
        try:
            state = json.loads(STATE_PATH.read_text())
        except json.JSONDecodeError:
            state = {}
    else:
        state = {}

    expires_at = get_token_expiry(state)
    now = time.time()

    if expires_at:
        days_left = (expires_at - now) / 86400
        expires_str = datetime.fromtimestamp(expires_at).strftime("%Y-%m-%d %H:%M")
        print(f"  BetQL token expires: {expires_str} ({days_left:.1f}d left)")
    else:
        days_left = 0
        print("  BetQL token expiry: not found")

    if args.check:
        return 0

    # Refresh if token expires within 3 days or --force
    if not args.force and days_left > 3:
        print("  Token still fresh — no refresh needed.")
        return 0

    email, password = get_credentials()
    print(f"  Logging in as {email}...")

    success = do_login(email, password)
    if not success:
        return 1

    # Read back and confirm
    try:
        new_state = json.loads(STATE_PATH.read_text())
        new_expiry = get_token_expiry(new_state)
        if new_expiry:
            new_days = (new_expiry - now) / 86400
            new_str = datetime.fromtimestamp(new_expiry).strftime("%Y-%m-%d %H:%M")
            print(f"  Login successful. New token expires: {new_str} ({new_days:.1f}d left)")
        else:
            print("  Login successful (expiry unknown).")
    except Exception:
        print("  Login successful.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
