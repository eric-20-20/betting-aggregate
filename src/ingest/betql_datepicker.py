from __future__ import annotations

import logging
import time
from datetime import date, datetime
from typing import Dict, Optional, Tuple, List, Callable, Any

from playwright.sync_api import Page, TimeoutError, Locator

MENU_SELECTOR = ".rotoql-date-picker__menu.dropdown-menu"
BUTTON_SELECTOR = "button.rotoql-date-picker__button"
MONTH_SELECTOR = ".rotoql-date-picker__header-selected-month"
LEFT_SELECTOR = ".rotoql-date-picker__header-left-button"
RIGHT_SELECTOR = ".rotoql-date-picker__header-right-button"
DAY_SELECTOR = ".rotoql-date-picker__calendar-cell span"
DATEPICKER_ROOT = "div.rotoql-date-picker.games-view__date-picker"
BLOCKED_SUBSTRING = "/nba/game-predictions/"
BLOCKED_SNIPPETS = ["salesmanago", "google", "segment", "analytics", "/war/"]


def _parse_month_label(text: str) -> Tuple[int, int]:
    text = (text or "").strip()
    for fmt in ("%B %Y", "%b %Y"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.year, dt.month
        except ValueError:
            continue
    raise ValueError(f"Unrecognized month label: {text}")


def _month_index(year: int, month: int) -> int:
    return year * 12 + month


def _wait_for_menu(root: Locator, timeout: int = 5000) -> None:
    root.locator(MENU_SELECTOR).wait_for(state="visible", timeout=timeout)


def _ensure_button(page: Page) -> Locator:
    root = page.locator(DATEPICKER_ROOT)
    btn = root.locator(BUTTON_SELECTOR).first
    btn.wait_for(state="visible", timeout=5000)
    return btn


def _navigate_to_month(root: Locator, target_year: int, target_month: int) -> str:
    target_idx = _month_index(target_year, target_month)
    last_label = ""
    for _ in range(48):  # four years safety net
        label = root.locator(MONTH_SELECTOR).inner_text().strip()
        last_label = label
        cur_year, cur_month = _parse_month_label(label)
        cur_idx = _month_index(cur_year, cur_month)
        if cur_idx == target_idx:
            return label

        direction = LEFT_SELECTOR if cur_idx > target_idx else RIGHT_SELECTOR
        root.locator(direction).click()
        root.page.wait_for_timeout(150)
    raise RuntimeError(f"Unable to reach {target_month}/{target_year}; last header {last_label}")


def _click_day(root: Locator, day: int, logger: Optional[logging.Logger] = None) -> None:
    """
    Click the calendar cell for the given day, with fallbacks to force/mouse clicks when pointer interception occurs.
    Prefers enabled in-month cells.
    """
    day_str = str(day)
    spans = root.locator(DAY_SELECTOR, has_text=day_str)
    total = spans.count()
    if total == 0:
        raise RuntimeError(f"Day {day} not found in calendar")

    chosen_cell: Locator | None = None
    chosen_span: Locator | None = None
    for i in range(total):
        span = spans.nth(i)
        cell = span.locator("xpath=ancestor::*[contains(@class,'rotoql-date-picker__calendar-cell')][1]")
        if cell.count() == 0:
            continue
        cell = cell.first
        try:
            cls = (cell.get_attribute("class") or "").lower()
            aria_disabled = (cell.get_attribute("aria-disabled") or "").lower() == "true"
            if any(tok in cls for tok in ["disabled", "outside", "muted"]) or aria_disabled:
                continue
        except Exception:
            pass
        chosen_cell = cell
        chosen_span = span
        break

    if chosen_cell is None:
        chosen_span = spans.first
        chosen_cell = chosen_span.locator("xpath=ancestor::*[contains(@class,'rotoql-date-picker__calendar-cell')][1]").first

    page = root.page
    strategy = None
    try:
        chosen_cell.click(timeout=1500)
        strategy = "cell_click"
        if logger:
            logger.debug("[datepicker] day=%s using cell click (%s matches)", day_str, total)
        return
    except Exception:
        strategy = "cell_click_failed"
    try:
        chosen_cell.click(timeout=1500, force=True)
        strategy = "cell_click_force"
        if logger:
            logger.debug("[datepicker] day=%s using force click (%s matches)", day_str, total)
        return
    except Exception:
        strategy = "cell_click_force_failed"

    # Mouse fallback
    if chosen_span:
        bbox = None
        try:
            bbox = chosen_span.bounding_box()
        except Exception:
            bbox = None
        if bbox and page:
            try:
                page.mouse.click(bbox["x"] + bbox["width"] / 2, bbox["y"] + bbox["height"] / 2)
                strategy = "mouse_click"
                if logger:
                    logger.debug("[datepicker] day=%s using mouse click (%s matches)", day_str, total)
                return
            except Exception:
                strategy = "mouse_click_failed"

    raise RuntimeError(f"Failed to click day {day} (strategy={strategy} matches={total})")


def _response_matches(
    resp,
    expected_url_substrings: Optional[list[str]] = None,
    response_predicate: Optional[Callable[[Any], bool]] = None,
) -> bool:
    try:
        if resp.status not in {200, 304}:
            return False
        url = resp.url or ""
        if BLOCKED_SUBSTRING in url:
            return False
        if any(bad in url for bad in BLOCKED_SNIPPETS):
            return False
        if response_predicate:
            return response_predicate(resp)
        if expected_url_substrings:
            return any(sub in url for sub in expected_url_substrings)
        return False
    except Exception:
        return False


def _wait_for_loaded_signal(
    page: Page,
    btn: Locator,
    prev_button_text: Optional[str],
    response_timeout_ms: int,
    fallback_timeout_ms: int,
    logger: Optional[logging.Logger],
    expected_url_substrings: Optional[list[str]] = None,
    response_predicate: Optional[Callable[[Any], bool]] = None,
) -> Dict[str, Optional[str]]:
    triggered_url = None
    try:
        with page.expect_response(
            lambda r: _response_matches(r, expected_url_substrings, response_predicate),
            timeout=response_timeout_ms,
        ) as resp_info:
            pass
        resp = resp_info.value
        triggered_url = resp.url
        if logger:
            logger.debug("[datepicker] loaded-signal via response url=%s", triggered_url)
    except TimeoutError:
        if logger:
            logger.debug(
                "[datepicker] no response match in %sms; will rely on DOM text change",
                response_timeout_ms,
            )

    final_button_text = None
    button_changed = False
    if prev_button_text:
        deadline = time.time() + (fallback_timeout_ms / 1000)
        while time.time() < deadline:
            try:
                final_button_text = btn.inner_text().strip()
            except Exception:
                final_button_text = None
            if final_button_text and final_button_text != prev_button_text:
                button_changed = True
                break
            page.wait_for_timeout(250)
    else:
        try:
            final_button_text = btn.inner_text().strip()
        except Exception:
            final_button_text = None

    return {
        "triggered_url": triggered_url,
        "final_button_text": final_button_text,
        "button_changed": button_changed,
    }


def switch_date(
    page: Page,
    target: date,
    logger: Optional[logging.Logger] = None,
    response_timeout_ms: int = 15000,
    fallback_timeout_ms: int = 8000,
    expected_url_substrings: Optional[list[str]] = None,
    response_predicate: Optional[Callable[[Any], bool]] = None,
) -> Dict[str, Optional[str]]:
    """
    Drive the BetQL date picker to `target`, waiting for the first game fetch (-vs- URL) to finish.
    Returns debug info with the response URL (if any) and final button text.
    """
    root = page.locator(DATEPICKER_ROOT)
    btn = _ensure_button(page)
    prev_button_text = None
    try:
        prev_button_text = btn.inner_text().strip()
    except Exception:
        prev_button_text = None

    btn.click()
    _wait_for_menu(root)

    month_label = _navigate_to_month(root, target.year, target.month)
    _click_day(root, target.day, logger=logger)

    # short settle before load state waits
    page.wait_for_timeout(150)
    try:
        page.wait_for_load_state("domcontentloaded", timeout=4000)
    except Exception:
        pass

    info = _wait_for_loaded_signal(
        page,
        btn=btn,
        prev_button_text=prev_button_text,
        response_timeout_ms=response_timeout_ms,
        fallback_timeout_ms=fallback_timeout_ms,
        logger=logger,
        expected_url_substrings=expected_url_substrings,
        response_predicate=response_predicate,
    )

    if not info.get("triggered_url"):
        try:
            page.wait_for_load_state("networkidle", timeout=4000)
        except Exception:
            pass

    try:
        root.locator(MENU_SELECTOR).wait_for(state="hidden", timeout=3000)
    except TimeoutError:
        pass

    if logger:
        logger.debug(
            "[datepicker] switched to %s (header=%s) prev_btn=%s final_btn=%s response=%s",
            target.isoformat(),
            month_label,
            prev_button_text,
            info.get("final_button_text"),
            info.get("triggered_url"),
        )

    info.update(
        {
            "target_date": target.isoformat(),
            "prev_button_text": prev_button_text,
            "header_after_nav": month_label,
        }
    )
    return info
