#!/usr/bin/env python3
"""
Refresh JuiceReel session by logging in headlessly via Playwright.

JuiceReel's id_token cookie lasts ~1 day and is set server-side (httpOnly).
This script logs in automatically using credentials from .env.

Credentials: JUICEREEL_EMAIL / JUICEREEL_PASSWORD
Falls back to BETQL_EMAIL / BETQL_PASSWORD if not set.

Usage:
    python3 scripts/refresh_juicereel_token.py
    python3 scripts/refresh_juicereel_token.py --check    # print expiry only
    python3 scripts/refresh_juicereel_token.py --force    # refresh even if fresh

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
STATE_PATH = ROOT / "data" / "juicereel_storage_state.json"
LOGIN_URL = "https://app.juicereel.com/login"
POST_LOGIN_URL = "https://app.juicereel.com"
AUTH_COOKIE = "id_token"


def load_env() -> None:
    env_path = ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())


def get_credentials() -> tuple[str, str]:
    email = os.environ.get("JUICEREEL_EMAIL") or os.environ.get("BETQL_EMAIL", "")
    password = os.environ.get("JUICEREEL_PASSWORD") or os.environ.get("BETQL_PASSWORD", "")
    email, password = email.strip(), password.strip()
    if not email or not password:
        print("  ERROR: JUICEREEL_EMAIL/PASSWORD (or BETQL_EMAIL/PASSWORD) must be set in .env",
              file=sys.stderr)
        sys.exit(2)
    return email, password


def get_token_expiry(state: dict) -> float | None:
    for cookie in state.get("cookies", []):
        if cookie.get("name") == AUTH_COOKIE:
            exp = cookie.get("expires", -1)
            if exp and exp > 0:
                return float(exp)
    return None


def do_login(email: str, password: str) -> bool:
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
            page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_selector('input[placeholder="Username or Email"]', timeout=10000)

            page.fill('input[placeholder="Username or Email"]', email)
            page.fill('input[type="password"]', password)
            page.click('button:has-text("Login")')

            # Wait to navigate away from login page
            deadline = time.time() + 20
            logged_in = False
            while time.time() < deadline:
                if "login" not in page.url:
                    logged_in = True
                    break
                page.wait_for_timeout(500)

            if not logged_in:
                print(f"  ERROR: Still on login page after 20s. URL: {page.url}", file=sys.stderr)
                browser.close()
                return False

            # Confirm id_token cookie was set
            cookies = ctx.cookies()
            if not any(c["name"] == AUTH_COOKIE for c in cookies):
                print("  ERROR: id_token cookie not found after login.", file=sys.stderr)
                browser.close()
                return False

            STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
            ctx.storage_state(path=str(STATE_PATH))
            browser.close()
            return True

        except PWTimeout as e:
            print(f"  ERROR: Timeout during JuiceReel login: {e}", file=sys.stderr)
            browser.close()
            return False
        except Exception as e:
            print(f"  ERROR: Login failed: {e}", file=sys.stderr)
            browser.close()
            return False


def main() -> int:
    load_env()

    parser = argparse.ArgumentParser(description="Refresh JuiceReel session token")
    parser.add_argument("--check", action="store_true", help="Only print expiry, no refresh")
    parser.add_argument("--force", action="store_true", help="Refresh even if not near expiry")
    args = parser.parse_args()

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
        print(f"  JuiceReel token expires: {expires_str} ({days_left:.1f}d left)")
    else:
        days_left = 0
        print("  JuiceReel token: not found or expired")

    if args.check:
        return 0

    # Refresh if token expires within 12 hours or --force (it only lasts ~1 day)
    if not args.force and days_left > 0.5:
        print("  Token still fresh — no refresh needed.")
        return 0

    email, password = get_credentials()
    print(f"  Logging in as {email}...")

    success = do_login(email, password)
    if not success:
        return 1

    try:
        new_state = json.loads(STATE_PATH.read_text())
        new_expiry = get_token_expiry(new_state)
        if new_expiry:
            new_str = datetime.fromtimestamp(new_expiry).strftime("%Y-%m-%d %H:%M")
            new_days = (new_expiry - now) / 86400
            print(f"  Login successful. New token expires: {new_str} ({new_days:.1f}d left)")
        else:
            print("  Login successful.")
    except Exception:
        print("  Login successful.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
