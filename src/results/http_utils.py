"""HTTP utilities with retry, headers, and diagnostics."""

from __future__ import annotations

import time
from typing import Dict, Optional, Tuple

import requests

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}


def http_get_with_retry(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 25,
    retries: int = 2,
    backoff: float = 0.5,
    allow_redirects: bool = True,
) -> Tuple[Optional[requests.Response], Optional[Exception]]:
    hdrs = DEFAULT_HEADERS.copy()
    if headers:
        hdrs.update(headers)
    last_exc: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, headers=hdrs, timeout=timeout, allow_redirects=allow_redirects)
            return resp, None
        except Exception as exc:  # pragma: no cover (network)
            last_exc = exc
            time.sleep(backoff)
            continue
    return None, last_exc
