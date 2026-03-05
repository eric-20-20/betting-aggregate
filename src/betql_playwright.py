from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Tuple

from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext, Playwright

_log = logging.getLogger(__name__)


def require_storage_state(path: str = "data/betql_storage_state.json") -> str:
    if not os.path.exists(path):
        raise ValueError(f"BetQL storage state missing at {path}. Please login and save state first.")
    # Check if auth token is expired
    try:
        with open(path) as f:
            state = json.load(f)
        for cookie in state.get("cookies", []):
            if cookie.get("name") == "node_auth_token":
                expires = cookie.get("expires", -1)
                if expires > 0 and expires < time.time():
                    from datetime import datetime
                    exp_dt = datetime.fromtimestamp(expires)
                    raise ValueError(
                        f"BetQL auth token expired on {exp_dt:%Y-%m-%d %H:%M}. "
                        f"Re-login with: python3 scripts/betql_login_save_state.py"
                    )
                days_left = (expires - time.time()) / 86400 if expires > 0 else -1
                if 0 < days_left < 1:
                    _log.warning("BetQL auth token expires in %.1f hours — consider re-logging in soon", days_left * 24)
                return path
        _log.warning("No node_auth_token cookie found in storage state — auth may fail")
    except (json.JSONDecodeError, KeyError):
        _log.warning("Could not parse storage state at %s", path)
    return path


class BetQLSession:
    def __init__(self, storage_state_path: str, headless: bool = True) -> None:
        self.storage_state_path = require_storage_state(storage_state_path)
        self.headless = headless
        self._p: Playwright | None = None
        self.browser: Browser | None = None
        self.context: BrowserContext | None = None

    def __enter__(self) -> "BetQLSession":
        self._p = sync_playwright().start()
        self.browser = self._p.chromium.launch(headless=self.headless)
        self.context = self.browser.new_context(
            storage_state=self.storage_state_path, viewport={"width": 1400, "height": 900}
        )
        return self

    def open(self, url: str, wait_for: str | None = None, timeout_ms: int = 60000) -> Page:
        if not self.context:
            raise RuntimeError("BetQLSession is not started; use context manager or call __enter__().")
        page = self.context.new_page()
        used_wait_until = "domcontentloaded"
        try:
            page.goto(url, wait_until=used_wait_until, timeout=timeout_ms)
        except Exception:
            try:
                used_wait_until = "load"
                page.goto(url, wait_until=used_wait_until, timeout=timeout_ms)
            except Exception:
                logger = getattr(__import__("logging"), "getLogger")(__name__)
                logger.warning("Retry navigation failed for %s", url)
                raise
        try:
            page.wait_for_selector("body", timeout=timeout_ms)
        except Exception:
            pass
        if wait_for:
            try:
                page.wait_for_selector(wait_for, timeout=timeout_ms)
            except Exception:
                logger = getattr(__import__("logging"), "getLogger")(__name__)
                logger.warning("wait_for selector %s timed out for %s", wait_for, url)
        logger = getattr(__import__("logging"), "getLogger")(__name__)
        try:
            logger.debug("[betql-session] goto url=%s wait_until=%s final_url=%s", url, used_wait_until, page.url)
        except Exception:
            pass
        return page

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        try:
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if self._p:
                self._p.stop()
        finally:
            self.context = None
            self.browser = None
            self._p = None


def _dismiss_modals(page: Page) -> None:
    """Try to dismiss any popups or modals that might be blocking content."""
    # Common modal close button selectors
    close_selectors = [
        "button[aria-label='Close']",
        "button.close",
        ".modal-close",
        "[data-dismiss='modal']",
        "button:has-text('Close')",
        "button:has-text('No thanks')",
        "button:has-text('Maybe later')",
    ]
    for selector in close_selectors:
        try:
            btn = page.locator(selector).first
            if btn.is_visible(timeout=500):
                btn.click(timeout=1000)
                page.wait_for_timeout(300)
        except Exception:
            pass


def wait_for_ready(page: Page, surface: str, timeout: int = 45000) -> None:
    try:
        # First try to dismiss any modals
        _dismiss_modals(page)

        # Wait for network to settle a bit
        try:
            page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass

        if surface == "model":
            # Try multiple selectors - the rating button or the team cells
            try:
                page.wait_for_selector("button.games-table-column__rating-button", state="attached", timeout=timeout)
            except Exception:
                # Fallback to just team cells if rating buttons not found
                page.wait_for_selector("div.games-table-column__team-cell", state="attached", timeout=timeout)
            page.wait_for_selector("div.games-table-column__team-cell, img[src*='/NBA/']", state="attached", timeout=timeout)
        elif surface == "sharps":
            page.wait_for_selector("div.games-table-column__team-cell, img[src*='/NBA/']", state="attached", timeout=timeout)
            page.wait_for_selector(".games-table-column__current-line-cell", state="attached", timeout=timeout)
        elif surface == "props":
            page.wait_for_selector("div.carousel-track div.carousel-pane > button", state="attached", timeout=timeout)
            page.wait_for_selector("div.team-player-props-head .team-name", state="attached", timeout=timeout)
            page.wait_for_selector("div.player-props", state="attached", timeout=timeout)
        elif surface == "game":
            page.wait_for_selector("body", timeout=timeout)
            try:
                page.wait_for_selector("text=Props", timeout=timeout)
            except Exception:
                pass
        else:
            page.wait_for_selector("body", timeout=timeout)
    except Exception as exc:
        title = page.title()
        snippet = page.content()[:5000]
        try:
            page.screenshot(path="out/betql_ready_fail.png", full_page=True)
        except Exception:
            pass
        raise RuntimeError(f"wait_for_ready failed for surface={surface} title={title} err={exc}\n{snippet}") from exc
