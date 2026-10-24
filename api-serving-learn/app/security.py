"""STAGE 2 — authentication, rate limiting, and conditional GETs for the API *we* serve.

In part 1 you were the client of a rate-limited API. Now you are the server, so you see why the headers
you parsed there (Retry-After, X-RateLimit-*) exist: they let well-behaved clients slow down on their own.
"""
from __future__ import annotations

import hashlib
import json
import math
import threading
import time
from typing import Callable

from fastapi import Header, HTTPException, Request
from fastapi.responses import Response

from .settings import Settings, hash_key


class TokenBucket:
    """LEARN: token bucket = allow bursts up to `capacity`, sustained rate of `refill_per_sec`.
    Each client has its own bucket, so one noisy consumer can't starve the others.

    This is per-process memory. With N API replicas each would allow `capacity`, so production uses a shared
    store (Redis) or does it at the gateway (Kong, Envoy, Cloudflare, API Gateway). The algorithm is the same."""

    def __init__(self, capacity: int, refill_per_sec: float, clock: Callable[[], float] = time.monotonic):
        self.capacity, self.refill, self.clock = capacity, refill_per_sec, clock
        self._state: dict[str, tuple[float, float]] = {}  # client -> (tokens, last_seen)
        self._lock = threading.Lock()

    def take(self, client: str) -> tuple[bool, int, int]:
        """returns (allowed, remaining_whole_tokens, retry_after_seconds)"""
        with self._lock:
            now = self.clock()
            tokens, last = self._state.get(client, (float(self.capacity), now))
            tokens = min(self.capacity, tokens + (now - last) * self.refill)
            if tokens >= 1:
                self._state[client] = (tokens - 1, now)
                return True, int(tokens - 1), 0
            self._state[client] = (tokens, now)
            return False, 0, math.ceil((1 - tokens) / self.refill)


def make_guard(settings: Settings, limiter: TokenBucket):
    def guard(request: Request, x_api_key: str | None = Header(default=None)) -> str:
        client = settings.api_key_hashes.get(hash_key(x_api_key)) if x_api_key else None
        if client is None:
            # 401 = "I don't know who you are". (403 = "I know who you are and you may not do this".)
            raise HTTPException(401, "invalid or missing X-API-Key", headers={"WWW-Authenticate": "ApiKey"})
        allowed, remaining, retry_after = limiter.take(client)
        hdrs = {"X-RateLimit-Limit": str(limiter.capacity), "X-RateLimit-Remaining": str(remaining)}
        if not allowed:
            raise HTTPException(429, "rate limit exceeded", headers={**hdrs, "Retry-After": str(retry_after)})
        request.state.rate_headers = hdrs  # picked up by conditional_json below
        return client

    return guard


def conditional_json(request: Request, payload: dict) -> Response:
    """LEARN: ETag + If-None-Match. The client sends the ETag it last saw; if nothing changed we answer 304
    with no body. This is the server half of what part 1's client did to poll GitHub for free."""
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    etag = '"' + hashlib.sha256(body).hexdigest()[:32] + '"'
    headers = {"ETag": etag, "Cache-Control": "private, max-age=0, must-revalidate",
               **getattr(request.state, "rate_headers", {})}
    if request.headers.get("if-none-match") == etag:
        return Response(status_code=304, headers=headers)
    return Response(body, media_type="application/json", headers=headers)
