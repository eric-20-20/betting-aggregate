from __future__ import annotations

import time
import logging
from typing import Any, Callable, Dict, List, Tuple

from playwright.sync_api import Page


def wait_for_extractor_ready(
    page: Page,
    extractor_fn: Callable[..., Tuple[List[Dict[str, Any]], Dict[str, Any]]],
    extractor_args: Tuple[Any, ...],
    ready_predicate: Callable[[List[Dict[str, Any]], Dict[str, Any]], bool],
    timeout_ms: int = 15000,
    poll_ms: int = 500,
    logger: logging.Logger | None = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    """
    Poll the extractor until its dbg signals that content is present.
    Returns (records, dbg, meta).
    """
    start = time.time()
    attempts = 0
    last_records: List[Dict[str, Any]] = []
    last_dbg: Dict[str, Any] = {}
    scroll_every = max(1, int(1500 / max(1, poll_ms)))  # roughly every 1.5s

    while True:
        attempts += 1
        # force debug=True so dbg counters populate
        records, dbg = extractor_fn(*extractor_args, debug=True)
        last_records, last_dbg = records, dbg
        if ready_predicate(records, dbg):
            elapsed_ms = int((time.time() - start) * 1000)
            return records, dbg, {
                "ready": True,
                "attempts": attempts,
                "elapsed_ms": elapsed_ms,
                "timeout": False,
            }

        elapsed_ms = int((time.time() - start) * 1000)
        if elapsed_ms >= timeout_ms:
            return last_records, last_dbg, {
                "ready": False,
                "attempts": attempts,
                "elapsed_ms": elapsed_ms,
                "timeout": True,
            }

        page.wait_for_timeout(poll_ms)
        if attempts % scroll_every == 0:
            try:
                page.mouse.wheel(0, 1200)
                if logger:
                    logger.debug("[ready_poll] scrolled during wait (attempt=%s)", attempts)
            except Exception:
                pass
