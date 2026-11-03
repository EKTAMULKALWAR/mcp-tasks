"""Task 4 — Resilient model router for an LLM Gateway.

Flow per request:
  1. authenticate tenant (Bearer key, hashed)            -> 401 gateway error
  2. estimate cost, atomically reserve in SQLite window  -> 429 gateway error + Retry-After
  3. try providers in order; each attempt is bounded by asyncio.timeout(3.0s).
     Fail over on: 429, timeout, connection error, 5xx.  Do NOT fail over on other 4xx
     (the request itself is bad — a second provider would just bill for the same error).
  4. settle reservation to real usage (or release it if nothing succeeded)
  5. any failure -> standardized payload; upstream bodies/stack traces only go to the log

Standard error payload:
  {"error": {"type": "...", "code": "...", "message": "...", "request_id": "..."}}
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import httpx
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from ratelimit import SlidingWindowTokenLimiter, tenant_hash

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("llm-router")


# ------------------------------------------------------------------ config

@dataclass
class Provider:
    name: str
    url: str
    model: str
    api_key: str = ""
    timeout_s: float = 3.0


@dataclass
class Settings:
    db_path: str = os.environ.get("RATE_DB_PATH", "gateway_ratelimit.db")
    tokens_per_minute: int = int(os.environ.get("TENANT_TPM", 50_000))
    default_max_tokens: int = 1024
    providers: list[Provider] = field(default_factory=lambda: [
        Provider("primary", os.environ.get("PRIMARY_URL", "http://127.0.0.1:9201/v1/chat/completions"),
                 os.environ.get("PRIMARY_MODEL", "gpt-primary"), os.environ.get("PRIMARY_KEY", "")),
        Provider("secondary", os.environ.get("SECONDARY_URL", "http://127.0.0.1:9202/v1/chat/completions"),
                 os.environ.get("SECONDARY_MODEL", "claude-backup"), os.environ.get("SECONDARY_KEY", "")),
    ])


# ------------------------------------------------------------------ errors

class GatewayError(Exception):
    def __init__(self, status: int, type_: str, code: str, message: str, headers: dict | None = None):
        super().__init__(message)
        self.status, self.type, self.code, self.message, self.headers = status, type_, code, message, headers or {}


def error_response(err: GatewayError, request_id: str) -> JSONResponse:
    return JSONResponse(
        {"error": {"type": err.type, "code": err.code, "message": err.message, "request_id": request_id}},
        status_code=err.status, headers={**err.headers, "X-Request-Id": request_id})


class RetryableUpstream(Exception):
    """Internal signal: this provider failed in a way that justifies trying the next."""


# ------------------------------------------------------------------ helpers

def estimate_prompt_tokens(body: dict) -> int:
    # ~4 chars/token heuristic + per-message overhead; swap in tiktoken for exact counts.
    chars = 0
    for m in body.get("messages", []):
        c = m.get("content", "")
        chars += len(c) if isinstance(c, str) else len(json.dumps(c))
    return chars // 4 + 4 * len(body.get("messages", [])) + 3


def validate_body(body: object) -> dict:
    if not isinstance(body, dict) or not isinstance(body.get("messages"), list) or not body["messages"]:
        raise GatewayError(400, "invalid_request_error", "invalid_body", "Body must include a non-empty 'messages' list")
    mt = body.get("max_tokens")
    if mt is not None and (not isinstance(mt, int) or isinstance(mt, bool) or mt <= 0):
        raise GatewayError(400, "invalid_request_error", "invalid_max_tokens", "'max_tokens' must be a positive integer")
    return body


# ------------------------------------------------------------------ router

class ModelRouter:
    def __init__(self, settings: Settings, client: httpx.AsyncClient, limiter: SlidingWindowTokenLimiter):
        self.s, self.client, self.limiter = settings, client, limiter

    async def _attempt(self, p: Provider, body: dict, request_id: str) -> dict:
        payload = {**body, "model": p.model}
        headers = {"X-Request-Id": request_id}
        if p.api_key:
            headers["Authorization"] = f"Bearer {p.api_key}"
        try:
            # asyncio.timeout bounds the *whole* attempt (connect + send + read). On expiry
            # the in-flight httpx call is cancelled and its connection is discarded, so a
            # slow primary can't complete later and race the fallback's response.
            async with asyncio.timeout(p.timeout_s):
                resp = await self.client.post(p.url, json=payload, headers=headers)
        except TimeoutError:
            log.warning("[%s] %s timed out after %.1fs", request_id, p.name, p.timeout_s)
            raise RetryableUpstream("timeout")
        except httpx.HTTPError as exc:
            log.warning("[%s] %s transport error: %r", request_id, p.name, exc)
            raise RetryableUpstream("transport")

        if resp.status_code == 429 or resp.status_code >= 500:
            log.warning("[%s] %s returned %d: %.200s", request_id, p.name, resp.status_code, resp.text)
            raise RetryableUpstream(str(resp.status_code))
        if resp.status_code >= 400:
            log.info("[%s] %s rejected request %d: %.200s", request_id, p.name, resp.status_code, resp.text)
            raise GatewayError(400, "invalid_request_error", "upstream_rejected",
                               "The model provider rejected the request")
        try:
            data = resp.json()
            data["choices"]  # minimal shape check
        except Exception:
            log.warning("[%s] %s returned malformed body", request_id, p.name)
            raise RetryableUpstream("malformed")
        return data

    async def complete(self, api_key: str, body: dict, request_id: str) -> tuple[dict, dict]:
        tenant = tenant_hash(api_key)
        cost = estimate_prompt_tokens(body) + body.get("max_tokens", self.s.default_max_tokens)
        decision = await self.limiter.reserve(tenant, cost)
        rl_headers = {"X-RateLimit-Limit": str(decision.limit),
                      "X-RateLimit-Remaining": str(decision.remaining)}
        if not decision.allowed:
            retry = max(1, int(decision.retry_after + 0.999))
            raise GatewayError(429, "rate_limit_error", "tenant_token_limit",
                               f"Token rate limit of {decision.limit}/min exceeded",
                               {**rl_headers, "Retry-After": str(retry)})

        rid, settled = decision.reservation_id, False
        try:
            for i, p in enumerate(self.s.providers):
                try:
                    data = await self._attempt(p, body, request_id)
                except RetryableUpstream as why:
                    if i + 1 < len(self.s.providers):
                        log.info("[%s] failing over %s -> %s (%s)", request_id, p.name,
                                 self.s.providers[i + 1].name, why)
                    continue
                usage = (data.get("usage") or {}).get("total_tokens")
                await self.limiter.settle(rid, usage if isinstance(usage, int) else cost)
                settled = True
                return data, {**rl_headers, "X-Gateway-Provider": p.name}
            raise GatewayError(503, "upstream_unavailable", "all_providers_failed",
                               "All model providers are currently unavailable. Please retry shortly.",
                               {**rl_headers, "Retry-After": "2"})
        finally:
            if not settled:
                # nothing was consumed; shield so a client disconnect can't leak the reservation
                await asyncio.shield(self.limiter.release(rid))


# ------------------------------------------------------------------ app

def create_app(settings: Settings | None = None, client: httpx.AsyncClient | None = None) -> FastAPI:
    settings = settings or Settings()
    @asynccontextmanager
    async def lifespan(_: FastAPI):
        yield
        await app.state.client.aclose()
        app.state.limiter.close()

    app = FastAPI(title="LLM Gateway Router", lifespan=lifespan)
    app.state.limiter = SlidingWindowTokenLimiter(settings.db_path, settings.tokens_per_minute)
    app.state.client = client or httpx.AsyncClient(limits=httpx.Limits(max_connections=200))
    app.state.router = ModelRouter(settings, app.state.client, app.state.limiter)

    @app.middleware("http")
    async def request_id_mw(request: Request, call_next):
        request.state.request_id = request.headers.get("x-request-id") or f"req_{uuid.uuid4().hex[:16]}"
        try:
            return await call_next(request)
        except Exception:
            # last line of defence: log the trace, return nothing internal
            log.exception("[%s] unhandled error", request.state.request_id)
            return error_response(GatewayError(500, "internal_error", "internal_error",
                                               "An internal error occurred"), request.state.request_id)

    @app.exception_handler(GatewayError)
    async def _ge(request: Request, exc: GatewayError):
        return error_response(exc, request.state.request_id)

    @app.exception_handler(StarletteHTTPException)
    async def _he(request: Request, exc: StarletteHTTPException):
        return error_response(GatewayError(exc.status_code, "invalid_request_error", "http_error",
                                           "Not found" if exc.status_code == 404 else "Request error"),
                              request.state.request_id)

    @app.exception_handler(RequestValidationError)
    async def _ve(request: Request, exc: RequestValidationError):
        return error_response(GatewayError(400, "invalid_request_error", "invalid_body", "Malformed request"),
                              request.state.request_id)

    @app.post("/v1/chat/completions")
    async def completions(request: Request):
        rid = request.state.request_id
        auth = request.headers.get("authorization", "")
        scheme, _, key = auth.partition(" ")
        if scheme.lower() != "bearer" or not key.strip():
            raise GatewayError(401, "authentication_error", "missing_api_key", "Provide 'Authorization: Bearer <api key>'")
        try:
            body = validate_body(json.loads(await request.body()))
        except json.JSONDecodeError:
            raise GatewayError(400, "invalid_request_error", "invalid_json", "Body is not valid JSON")
        if body.get("stream"):
            raise GatewayError(400, "invalid_request_error", "stream_unsupported", "Streaming not supported on this route")
        data, headers = await app.state.router.complete(key.strip(), body, rid)
        return JSONResponse(data, headers={**headers, "X-Request-Id": rid})

    return app


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(create_app(), host="127.0.0.1", port=8100)
