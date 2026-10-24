"""HTTP client with the behaviours you need to survive a real API.

* timeouts everywhere (a hung socket stalls a whole DAG)
* retry only what is retryable: 429, 5xx, network errors, and 403-with-rate-limit-exhausted
* exponential backoff with FULL JITTER (avoids synchronized retry storms)
* honour Retry-After / X-RateLimit-Reset over our own guess
* proactive throttling: stop *before* the quota hits zero
* give up (RateLimitExceeded) instead of sleeping for an hour inside a batch task
* Link-header pagination, ETag conditional requests (304s are free against GitHub quota)
"""
from __future__ import annotations

import email.utils
import logging
import random
import time
from collections import Counter
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Mapping

import httpx

log = logging.getLogger("ghpipe.client")

RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}


class ApiError(Exception):
    """Non-retryable HTTP error (401/403/404/422...). Retrying will not help."""

    def __init__(self, status: int, url: str, body: str):
        super().__init__(f"{status} for {url}: {body[:200]}")
        self.status = status


class RetriesExhausted(Exception):
    pass


class RateLimitExceeded(Exception):
    """Quota is exhausted and the reset is further away than we are willing to wait."""

    def __init__(self, wait_seconds: float):
        super().__init__(f"rate limited; reset in {wait_seconds:.0f}s")
        self.wait_seconds = wait_seconds


@dataclass
class ApiResponse:
    status: int
    json: Any
    headers: Mapping[str, str]
    url: str
    next_url: str | None

    @property
    def not_modified(self) -> bool:
        return self.status == 304

    @property
    def etag(self) -> str | None:
        return self.headers.get("etag")


class ApiClient:
    def __init__(
        self,
        base_url: str,
        token: str | None = None,
        *,
        max_retries: int = 5,
        backoff_base: float = 0.5,
        backoff_cap: float = 30.0,
        max_wait: float = 120.0,
        min_remaining: int = 1,
        timeout: float = 10.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.time,
        rng: random.Random | None = None,
        transport: httpx.BaseTransport | None = None,
    ):
        headers = {"Accept": "application/vnd.github+json", "User-Agent": "ghpipe/0.1"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        self._http = httpx.Client(base_url=base_url, headers=headers, timeout=timeout, transport=transport)
        self.max_retries, self.backoff_base, self.backoff_cap = max_retries, backoff_base, backoff_cap
        self.max_wait, self.min_remaining = max_wait, min_remaining
        self._sleep, self._clock, self._rng = sleep, clock, rng or random.Random()
        self.stats: Counter[str] = Counter()

    def close(self) -> None:
        self._http.close()

    # -- retry policy -------------------------------------------------------------------------
    def _backoff(self, attempt: int) -> float:
        return self._rng.uniform(0, min(self.backoff_cap, self.backoff_base * 2**attempt))

    @staticmethod
    def _rate_limited(r: httpx.Response) -> bool:
        # GitHub signals an exhausted primary quota with 403 + remaining=0, not 429.
        return r.status_code == 403 and r.headers.get("x-ratelimit-remaining") == "0"

    def _server_wait(self, r: httpx.Response) -> float | None:
        """How long the server itself told us to wait, if it did."""
        ra = r.headers.get("retry-after")
        if ra:
            if ra.isdigit():
                return float(ra)
            parsed = email.utils.parsedate_to_datetime(ra)  # HTTP-date form
            return max(parsed.timestamp() - self._clock(), 0.0)
        if r.headers.get("x-ratelimit-remaining") == "0" and r.headers.get("x-ratelimit-reset"):
            return max(float(r.headers["x-ratelimit-reset"]) - self._clock(), 0.0) + 1.0
        return None

    def _throttle_if_low(self, r: httpx.Response) -> None:
        remaining, reset = r.headers.get("x-ratelimit-remaining"), r.headers.get("x-ratelimit-reset")
        if remaining is None or reset is None or int(remaining) > self.min_remaining:
            return
        wait = max(float(reset) - self._clock(), 0.0) + 1.0
        if wait > self.max_wait:
            raise RateLimitExceeded(wait)
        log.info("quota low (remaining=%s); sleeping %.1fs until reset", remaining, wait)
        self.stats["throttle_sleeps"] += 1
        self._sleep(wait)

    # -- core request -------------------------------------------------------------------------
    def get(self, url: str, params: dict | None = None, etag: str | None = None) -> ApiResponse:
        headers = {"If-None-Match": etag} if etag else {}
        for attempt in range(self.max_retries + 1):
            reason = ""
            wait: float | None = None
            self.stats["requests"] += 1
            try:
                r = self._http.get(url, params=params, headers=headers)
            except httpx.TransportError as e:  # timeouts, resets, DNS...
                reason, wait = type(e).__name__, self._backoff(attempt)
            else:
                if r.status_code == 304 or r.is_success:
                    self._throttle_if_low(r)
                    nxt = r.links.get("next", {}).get("url")
                    body = None if r.status_code == 304 else r.json()
                    return ApiResponse(r.status_code, body, r.headers, str(r.url), nxt)
                if r.status_code not in RETRYABLE_STATUS and not self._rate_limited(r):
                    raise ApiError(r.status_code, str(r.url), r.text)
                reason = str(r.status_code)
                wait = self._server_wait(r)
                if wait is None:
                    wait = self._backoff(attempt)
                elif wait > self.max_wait:
                    raise RateLimitExceeded(wait)
            if attempt == self.max_retries:
                raise RetriesExhausted(f"{url}: gave up after {attempt + 1} attempts (last: {reason})")
            self.stats["retries"] += 1
            log.warning("retryable failure (%s) on %s; attempt %d, sleeping %.2fs", reason, url, attempt + 1, wait)
            self._sleep(wait)
        raise AssertionError("unreachable")

    def paginate(self, path: str, params: dict | None = None, etag: str | None = None) -> Iterator[ApiResponse]:
        """Follow rel="next" links. The ETag (if given) applies to the first page only."""
        resp = self.get(path, params=params, etag=etag)
        yield resp
        while not resp.not_modified and resp.next_url:
            resp = self.get(resp.next_url)  # next_url already carries every query param
            yield resp
