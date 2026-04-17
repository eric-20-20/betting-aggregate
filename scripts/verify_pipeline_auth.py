#!/usr/bin/env python3
"""
Verify authentication status for all pipeline scrapers.

Checks storage state files for expired cookies/tokens and prints a summary.
Exit code 0 = all valid, 1 = at least one expired.

Usage:
    python3 scripts/verify_pipeline_auth.py
"""

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _juicereel_enabled() -> bool:
    return os.getenv("ENABLE_JUICEREEL", "false") in {"1", "true", "TRUE", "yes", "YES", "on", "ON"}

# Service definitions: (name, storage_state_path, auth_cookie_names, re-login command)
SERVICES = [
    {
        "name": "BetQL",
        "path": ROOT / "data" / "betql_storage_state.json",
        "auth_cookies": ["node_auth_token"],
        "relogin": "python3 scripts/refresh_betql_token.py --force",
    },
    {
        "name": "Dimers",
        "path": ROOT / "dimers_storage_state.json",
        "auth_cookies": ["auth0", "auth0_compat"],
        "relogin": "python3 scripts/refresh_dimers_token.py  # or: python3 scripts/create_dimers_storage.py",
    },
    {
        "name": "SportsLine",
        "path": ROOT / "data" / "sportsline_storage_state.json",
        "auth_cookies": ["_swb"],  # long-lived CBS subscription cookie
        "relogin": "python3 scripts/refresh_sportsline_token.py --force",
    },
    {
        "name": "VegasInsider",
        "path": ROOT / "data" / "vegasinsider_storage_state.json",
        "auth_cookies": ["rl_session"],
        "relogin": "python3 scripts/create_vegasinsider_storage.py",
    },
    {
        "name": "JuiceReel",
        "path": ROOT / "data" / "juicereel_storage_state.json",
        "auth_cookies": ["id_token"],  # ~1 day expiry — auto-refreshed by run_daily.sh
        "relogin": "python3 scripts/refresh_juicereel_token.py --force",
    },
]


def check_service(svc: dict) -> dict:
    """Check a single service's auth status. Returns a result dict."""
    result = {
        "name": svc["name"],
        "status": "MISSING",
        "expires": None,
        "days_left": None,
        "relogin": svc["relogin"],
    }

    path = svc["path"]
    if not path.exists():
        return result

    try:
        with open(path) as f:
            state = json.load(f)
    except (json.JSONDecodeError, OSError):
        result["status"] = "ERROR"
        return result

    now = time.time()
    earliest_expiry = None
    found_auth = False

    for cookie in state.get("cookies", []):
        name = cookie.get("name", "")
        if name in svc["auth_cookies"]:
            found_auth = True
            expires = cookie.get("expires", -1)
            if expires <= 0:
                continue  # Session cookie, no expiry
            if earliest_expiry is None or expires < earliest_expiry:
                earliest_expiry = expires

    if not found_auth:
        # No auth cookies found — check if file is very old
        mtime = os.path.getmtime(path)
        age_days = (now - mtime) / 86400
        if age_days > 14:
            result["status"] = "STALE"
            result["days_left"] = -age_days
        else:
            result["status"] = "NO_AUTH_COOKIE"
        return result

    if earliest_expiry is None:
        result["status"] = "VALID"
        return result

    days_left = (earliest_expiry - now) / 86400
    result["expires"] = datetime.fromtimestamp(earliest_expiry)
    result["days_left"] = days_left

    if days_left < 0:
        result["status"] = "EXPIRED"
    elif days_left < 1:
        result["status"] = "EXPIRING"
    else:
        result["status"] = "VALID"

    return result


STATUS_SYMBOLS = {
    "VALID": "\u2713",
    "EXPIRING": "\u26a0",
    "EXPIRED": "\u2717",
    "MISSING": "\u2717",
    "STALE": "\u26a0",
    "NO_AUTH_COOKIE": "?",
    "ERROR": "\u2717",
}


def main():
    services = list(SERVICES)
    if not _juicereel_enabled():
        services = [svc for svc in services if svc["name"] != "JuiceReel"]

    results = [check_service(svc) for svc in services]

    print()
    print("  Pipeline Auth Status")
    print("  " + "=" * 60)
    if not _juicereel_enabled():
        print("  • JuiceReel auth check skipped (disabled via ENABLE_JUICEREEL)")

    any_expired = False

    for r in results:
        sym = STATUS_SYMBOLS.get(r["status"], "?")
        name = r["name"].ljust(14)

        if r["status"] == "VALID":
            days = f"{r['days_left']:.0f}d left" if r["days_left"] else ""
            expires = r["expires"].strftime("%Y-%m-%d %H:%M") if r["expires"] else "no expiry"
            print(f"  {sym}  {name} VALID      (expires {expires}, {days})")

        elif r["status"] == "EXPIRING":
            hours = r["days_left"] * 24
            print(f"  {sym}  {name} EXPIRING   ({hours:.1f}h left — refresh soon!)")
            print(f"      -> {r['relogin']}")

        elif r["status"] == "EXPIRED":
            ago = abs(r["days_left"])
            print(f"  {sym}  {name} EXPIRED    ({ago:.0f}d ago on {r['expires'].strftime('%Y-%m-%d')})")
            print(f"      -> {r['relogin']}")
            any_expired = True

        elif r["status"] == "MISSING":
            print(f"  {sym}  {name} MISSING    (no storage state file)")
            print(f"      -> {r['relogin']}")
            any_expired = True

        elif r["status"] == "STALE":
            ago = abs(r["days_left"])
            print(f"  {sym}  {name} STALE      (file {ago:.0f}d old, may need refresh)")
            print(f"      -> {r['relogin']}")

        elif r["status"] == "NO_AUTH_COOKIE":
            print(f"  ?  {name} NO AUTH    (no auth cookies found — may use free tier)")

        else:
            print(f"  {sym}  {name} ERROR      (could not parse storage state)")
            any_expired = True

    print("  " + "=" * 60)

    expired_names = [r["name"] for r in results if r["status"] in ("EXPIRED", "MISSING")]
    expiring_names = [r["name"] for r in results if r["status"] == "EXPIRING"]

    if expired_names:
        print(f"  EXPIRED: {', '.join(expired_names)}")
    if expiring_names:
        print(f"  EXPIRING SOON: {', '.join(expiring_names)}")
    if not expired_names and not expiring_names:
        print("  All auth tokens valid.")

    print()
    return 1 if any_expired else 0


if __name__ == "__main__":
    sys.exit(main())
