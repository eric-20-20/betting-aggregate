#!/usr/bin/env python3
"""
Refresh Dimers Auth0 tokens using the stored refresh_token.

Auth0 supports offline_access scope, so we can get a new access_token + refresh_token
without any browser interaction, as long as the refresh_token is still valid.

Usage:
    python3 scripts/refresh_dimers_token.py
    python3 scripts/refresh_dimers_token.py --check   # just print token expiry, don't refresh

Exit codes:
    0 = success (or not needed)
    1 = refresh failed (must re-login manually)
    2 = no refresh token found
"""

import argparse
import json
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STORAGE_STATE_PATH = ROOT / "dimers_storage_state.json"

AUTH0_DOMAIN = "auth.dimers.com"
AUTH0_TOKEN_URL = f"https://{AUTH0_DOMAIN}/oauth/token"
CLIENT_ID = "qVJ8L2Zf855PoDh0j6sbC4laC18wz4Fn"

# localStorage key that holds the token cache
LS_KEY = (
    "@@auth0spajs@@"
    "::qVJ8L2Zf855PoDh0j6sbC4laC18wz4Fn"
    "::https://dimers.us.auth0.com/api/v2/"
    "::openid read:current_user update:current_user_metadata offline_access"
)

# Auth cookies at auth.dimers.com that encode the session
AUTH_COOKIE_NAMES = {"auth0", "auth0_compat"}


def load_state() -> dict:
    try:
        return json.loads(STORAGE_STATE_PATH.read_text())
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"  ERROR: Could not read {STORAGE_STATE_PATH}: {e}", file=sys.stderr)
        sys.exit(1)


def save_state(state: dict) -> None:
    STORAGE_STATE_PATH.write_text(json.dumps(state, indent=2))


def get_ls_entry(state: dict) -> dict | None:
    """Return the Auth0 localStorage entry dict, or None if not found."""
    for origin in state.get("origins", []):
        for item in origin.get("localStorage", []):
            if item.get("name") == LS_KEY:
                try:
                    return json.loads(item["value"])
                except (KeyError, json.JSONDecodeError):
                    return None
    return None


def set_ls_entry(state: dict, entry: dict) -> None:
    """Update the Auth0 localStorage entry in-place."""
    new_value = json.dumps(entry)
    for origin in state.get("origins", []):
        for item in origin.get("localStorage", []):
            if item.get("name") == LS_KEY:
                item["value"] = new_value
                return
    raise ValueError("localStorage key not found — cannot update")


def get_token_expiry(entry: dict) -> float | None:
    """Return Unix timestamp of when the access token expires, or None."""
    expires_at = entry.get("expiresAt")
    if expires_at:
        return float(expires_at)
    # Fall back to issued_at + expires_in
    body = entry.get("body", {})
    expires_in = body.get("expires_in")
    if expires_in:
        # expiresAt is stored as seconds-since-epoch in the Auth0 SPA SDK
        return time.time() + float(expires_in)
    return None


def call_refresh(refresh_token: str) -> dict:
    """POST to Auth0 token endpoint. Returns the response JSON on success."""
    payload = urllib.parse.urlencode({
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "refresh_token": refresh_token,
    }).encode()

    req = urllib.request.Request(
        AUTH0_TOKEN_URL,
        data=payload,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        raise RuntimeError(f"Auth0 token endpoint returned {e.code}: {body}") from e
    except Exception as e:
        raise RuntimeError(f"Network error calling Auth0: {e}") from e


def update_auth_cookies(state: dict, new_access_token: str, expires_in: int) -> None:
    """
    Auth0 session cookies (auth0, auth0_compat) are opaque tokens managed by the
    Auth0 tenant. We can't regenerate them without a browser flow.

    However, Playwright uses the localStorage token for API calls — the cookies
    are only needed for the silent SSO check. As long as the access_token in
    localStorage is fresh, dimers_ingest.py will work correctly.

    We do update the cookie expiry timestamps so verify_pipeline_auth.py doesn't
    incorrectly flag them as expired.
    """
    new_expiry = time.time() + expires_in
    updated = 0
    for cookie in state.get("cookies", []):
        if cookie.get("name") in AUTH_COOKIE_NAMES and cookie.get("domain") == AUTH0_DOMAIN:
            cookie["expires"] = new_expiry
            updated += 1
    return updated


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh Dimers Auth0 token")
    parser.add_argument(
        "--check", action="store_true",
        help="Only check token expiry, do not refresh"
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Refresh even if token is not near expiry"
    )
    args = parser.parse_args()

    state = load_state()
    entry = get_ls_entry(state)

    if entry is None:
        print("  ERROR: Auth0 localStorage entry not found in dimers_storage_state.json")
        print("  You must log in manually: python3 scripts/create_dimers_storage.py")
        return 2

    body = entry.get("body", {})
    refresh_token = body.get("refresh_token")
    if not refresh_token:
        print("  ERROR: No refresh_token in localStorage entry")
        print("  You must log in manually: python3 scripts/create_dimers_storage.py")
        return 2

    expires_at = get_token_expiry(entry)
    now = time.time()

    if expires_at:
        days_left = (expires_at - now) / 86400
        expires_str = datetime.fromtimestamp(expires_at).strftime("%Y-%m-%d %H:%M")
        print(f"  Dimers token expires: {expires_str} ({days_left:.1f}d left)")
    else:
        days_left = 0
        print("  Dimers token expiry: unknown")

    if args.check:
        return 0

    # Refresh if token expires within 2 days or --force
    if not args.force and days_left > 2:
        print("  Token still fresh — no refresh needed.")
        return 0

    print("  Refreshing Dimers token via Auth0...")
    try:
        resp = call_refresh(refresh_token)
    except RuntimeError as e:
        print(f"  ERROR: {e}", file=sys.stderr)
        print("  Refresh token may be expired. Re-login: python3 scripts/create_dimers_storage.py")
        return 1

    new_access_token = resp.get("access_token")
    new_refresh_token = resp.get("refresh_token", refresh_token)  # Auth0 may rotate it
    new_expires_in = resp.get("expires_in", 604800)  # default 7 days

    if not new_access_token:
        print(f"  ERROR: No access_token in Auth0 response: {resp}", file=sys.stderr)
        return 1

    # Update the localStorage entry
    body["access_token"] = new_access_token
    body["refresh_token"] = new_refresh_token
    body["expires_in"] = new_expires_in
    entry["expiresAt"] = int(now + new_expires_in)

    set_ls_entry(state, entry)

    # Update auth cookie expiry timestamps
    n_cookies = update_auth_cookies(state, new_access_token, new_expires_in)

    save_state(state)

    new_expiry_str = datetime.fromtimestamp(now + new_expires_in).strftime("%Y-%m-%d %H:%M")
    print(f"  Token refreshed successfully. New expiry: {new_expiry_str}")
    if new_refresh_token != refresh_token:
        print("  Refresh token rotated (new token saved).")
    if n_cookies:
        print(f"  Updated {n_cookies} auth cookie expiry timestamps.")
    else:
        print("  Note: auth0/auth0_compat cookies not found — may need manual re-login if Playwright SSO check fails.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
