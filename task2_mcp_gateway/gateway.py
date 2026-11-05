"""Task 2 — MCP Security Gateway (HTTP JSON-RPC reverse proxy with tool-level RBAC).

Run:
  python task2_mcp_gateway/mock_downstream.py          # :9000
  GATEWAY_JWT_SECRET=<32+ byte secret> python task2_mcp_gateway/gateway.py   # :8080

Auth: `Authorization: Bearer <HS256 JWT>` carrying a `role` claim ("admin" | "viewer").
Signature, expiry and role are all checked; any doubt -> rejected (fail closed).

Policy, per JSON-RPC request object (single or batch):
  tools/list                 -> forwarded transparently
  tools/call, admin_* tool   -> forwarded only if role == admin, else -32001 (downstream never called)
  tools/call, other tool     -> forwarded
  anything else (initialize, ping, notifications/...) -> forwarded

Wire-format handling: -32700 parse error, -32600 invalid request, -32602 for a
tools/call with no string params.name. Batches are split: denied entries are answered
by the gateway, allowed ones are forwarded as a (smaller) batch, results re-merged.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import unicodedata
from contextlib import asynccontextmanager
from typing import Any

import httpx
import jwt
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from starlette.background import BackgroundTask

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("mcp-gateway")

DOWNSTREAM_URL = os.environ.get("DOWNSTREAM_URL", "http://127.0.0.1:9000/mcp")
DEV_JWT_SECRET = "dev-only-secret-change-me-0123456789abcdef"  # public in the repo: never use outside development
JWT_SECRET = os.environ.get("GATEWAY_JWT_SECRET") or DEV_JWT_SECRET  # empty counts as unset
USING_DEV_SECRET = JWT_SECRET == DEV_JWT_SECRET
MAX_BODY_BYTES = int(os.environ.get("MAX_BODY_BYTES", 1_048_576))  # 1 MiB
ALLOWED_ROLES = {"admin", "viewer"}
ADMIN_PREFIX = "admin_"

PARSE_ERROR, INVALID_REQUEST, INVALID_PARAMS = -32700, -32600, -32602
UNAUTHORIZED_TOOL, UNAUTHENTICATED, UPSTREAM_ERROR = -32001, -32000, -32003

# Headers that must not be copied hop-to-hop, plus the client's credential (never leaked downstream).
HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "transfer-encoding",
    "te",
    "trailer",
    "upgrade",
    "proxy-authorization",
    "proxy-authenticate",
    "host",
    "content-length",
    "authorization",
}


def rpc_error(id_: Any, code: int, message: str, data: Any = None) -> dict:
    err: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        err["data"] = data
    return {"jsonrpc": "2.0", "id": id_, "error": err}


# ------------------------------------------------------------------ auth


class AuthError(Exception):
    pass


def role_from_header(header: str | None) -> str:
    if not header:
        raise AuthError("missing Authorization header")
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise AuthError("expected 'Bearer <token>'")
    try:
        claims = jwt.decode(token.strip(), JWT_SECRET, algorithms=["HS256"], options={"require": ["exp", "role"]})
    except jwt.PyJWTError as exc:
        raise AuthError(f"invalid token: {exc.__class__.__name__}")
    role = claims.get("role")
    if role not in ALLOWED_ROLES:
        raise AuthError("unknown role")
    return role


# ------------------------------------------------------------------ policy


def is_valid_request_obj(msg: Any) -> bool:
    if not (isinstance(msg, dict) and msg.get("jsonrpc") == "2.0" and isinstance(msg.get("method"), str)):
        return False
    if "id" not in msg:
        return True  # notification
    id_ = msg["id"]
    return id_ is None or (isinstance(id_, (str, int)) and not isinstance(id_, bool))


def evaluate(msg: Any, role: str) -> dict | None:
    """Return a gateway-generated JSON-RPC response to short-circuit, or None to forward."""
    if not is_valid_request_obj(msg):
        return rpc_error(msg.get("id") if isinstance(msg, dict) else None, INVALID_REQUEST, "Invalid Request")
    if msg["method"] != "tools/call":
        return None  # tools/list and lifecycle methods pass through
    params = msg.get("params")
    name = params.get("name") if isinstance(params, dict) else None
    if not isinstance(name, str) or not name:
        return rpc_error(msg.get("id"), INVALID_PARAMS, "tools/call requires string params.name")
    # NFKC + strip + casefold so "Admin_reset_key", " admin_x", fullwidth "ａdmin_x" / "ＡＤＭＩＮ＿x" can't slip
    # past the prefix check if the downstream normalises names. `name` itself stays exactly as the client sent it.
    canonical = unicodedata.normalize("NFKC", name).strip().casefold()
    if canonical.startswith(ADMIN_PREFIX) and role != "admin":
        log.warning("denied tool=%r role=%s", name, role)
        return rpc_error(msg.get("id"), UNAUTHORIZED_TOOL, "Unauthorized Tool Call", {"tool": name})
    return None


# ------------------------------------------------------------------ proxy

client: httpx.AsyncClient | None = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    global client
    if USING_DEV_SECRET:
        log.warning(
            "GATEWAY_JWT_SECRET is not set: signing/verifying tokens with the public development secret. "
            "Anyone who has read the source can mint an admin token. Set GATEWAY_JWT_SECRET (32+ bytes)."
        )
    client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=5.0))
    yield
    await client.aclose()


app = FastAPI(title="MCP Security Gateway", lifespan=lifespan)


def forward_headers(req: Request) -> dict[str, str]:
    return {k: v for k, v in req.headers.items() if k.lower() not in HOP_BY_HOP}


async def forward(req: Request, payload: Any) -> httpx.Response:
    assert client is not None, "gateway used before startup"
    upstream = client.build_request(
        "POST", DOWNSTREAM_URL, headers=forward_headers(req), content=json.dumps(payload).encode()
    )
    return await client.send(upstream, stream=True)


def response_headers(resp: httpx.Response) -> dict[str, str]:
    return {k: v for k, v in resp.headers.items() if k.lower() not in HOP_BY_HOP | {"content-encoding"}}


async def read_body_capped(req: Request, limit: int) -> bytes | None:
    """Read the request body, giving up (None) as soon as it exceeds `limit` bytes,
    so an oversized upload is never fully buffered."""
    declared = req.headers.get("content-length", "")
    if declared.isdigit() and int(declared) > limit:
        return None
    chunks: list[bytes] = []
    size = 0
    async for chunk in req.stream():
        size += len(chunk)
        if size > limit:
            return None
        chunks.append(chunk)
    return b"".join(chunks)


@app.post("/mcp")
async def mcp_endpoint(req: Request) -> Response:
    # 1. authenticate (before even parsing, so unauthenticated callers learn nothing)
    try:
        role = role_from_header(req.headers.get("authorization"))
    except AuthError as exc:
        log.info("auth rejected: %s", exc)
        return JSONResponse(
            rpc_error(None, UNAUTHENTICATED, "Unauthenticated"),
            status_code=401,
            headers={"WWW-Authenticate": 'Bearer error="invalid_token"'},
        )

    # 2. parse wire format
    raw = await read_body_capped(req, MAX_BODY_BYTES)
    if raw is None:
        return JSONResponse(
            rpc_error(None, INVALID_REQUEST, "Request body too large", {"max_bytes": MAX_BODY_BYTES}), status_code=413
        )
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError, RecursionError):  # RecursionError: absurdly deep nesting
        return JSONResponse(rpc_error(None, PARSE_ERROR, "Parse error"))

    is_batch = isinstance(payload, list)
    if is_batch and not payload:
        return JSONResponse(rpc_error(None, INVALID_REQUEST, "Invalid Request: empty batch"))
    messages = payload if is_batch else [payload]

    # 3. per-message policy
    local: list[dict] = []
    to_forward: list[dict] = []
    for m in messages:
        verdict = evaluate(m, role)
        if verdict is None:
            to_forward.append(m)
        elif not (isinstance(m, dict) and "id" not in m and m.get("jsonrpc") == "2.0"):
            local.append(verdict)  # notifications get no response, even when denied

    # 4a. single message (or nothing to forward): simple paths, stream passthrough preserved
    if not to_forward:
        if not local:
            return Response(status_code=202)
        return JSONResponse(local if is_batch else local[0])

    try:
        upstream = await forward(req, to_forward if is_batch else to_forward[0])
    except httpx.HTTPError as exc:
        log.error("downstream unreachable: %s", exc.__class__.__name__)
        err = [rpc_error(m.get("id"), UPSTREAM_ERROR, "Downstream unavailable") for m in to_forward if "id" in m]
        body = (local + err) if is_batch else (err[0] if err else None)
        return JSONResponse(body, status_code=502) if body else Response(status_code=502)

    if not is_batch or not local:
        # Transparent: stream bytes (JSON or SSE) back untouched.
        return StreamingResponse(
            upstream.aiter_raw(),
            status_code=upstream.status_code,
            headers=response_headers(upstream),
            background=BackgroundTask(upstream.aclose),
        )

    # 4b. mixed batch: merge gateway-generated errors with downstream results
    downstream_body = await upstream.aread()
    await upstream.aclose()
    try:
        downstream_results = json.loads(downstream_body) if downstream_body else []
    except (json.JSONDecodeError, RecursionError):
        downstream_results = [
            rpc_error(m.get("id"), UPSTREAM_ERROR, "Bad downstream response") for m in to_forward if "id" in m
        ]
    if isinstance(downstream_results, dict):
        downstream_results = [downstream_results]
    return JSONResponse(
        local + downstream_results,
        headers={k: v for k, v in response_headers(upstream).items() if k.lower().startswith("mcp-")},
    )


@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("PORT", 8080)))
