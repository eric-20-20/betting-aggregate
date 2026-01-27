from __future__ import annotations

import os
from pathlib import Path
from typing import Tuple

from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext, Playwright


def require_storage_state(path: str = "data/betql_storage_state.json") -> str:
    if not os.path.exists(path):
        raise ValueError(f"BetQL storage state missing at {path}. Please login and save state first.")
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

    def open(self, url: str) -> Page:
        if not self.context:
            raise RuntimeError("BetQLSession is not started; use context manager or call __enter__().")
        page = self.context.new_page()
        page.goto(url, wait_until="networkidle")
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


def wait_for_ready(page: Page, surface: str, timeout: int = 45000) -> None:
    try:
        if surface == "model":
            page.wait_for_selector("button.games-table-column__rating-button", state="attached", timeout=timeout)
            page.wait_for_selector("div.games-table-column__team-cell, img[src*='/NBA/']", state="attached", timeout=timeout)
        elif surface == "sharps":
            page.wait_for_selector("div.game-row, div.game-table-row", state="attached", timeout=timeout)
        elif surface == "props":
            page.wait_for_selector("div.player-prop-card, div.best-bets-card", state="attached", timeout=timeout)
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
