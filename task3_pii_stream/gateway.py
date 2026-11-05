"""Task 3 — LLM Gateway streaming endpoint with real-time PII redaction.

POST /v1/chat/completions  (OpenAI-compatible body, stream=true)
  -> forwards to PROVIDER_URL (any OpenAI-compatible streaming API; mock included)
  -> parses SSE line-by-line as bytes arrive
  -> runs each choice's delta.content through its own StreamRedactor
  -> re-emits OpenAI-shaped SSE chunks immediately

Latency/memory design:
  * No accumulation: each upstream line is processed and yielded before the next is read;
    the only retained text is the redactor's hold-back tail (<= 256 chars per choice).
  * TTFT: role/metadata chunks are passed through at once; content is delayed by at most
    the trailing partial word (or a partial email / digit run).
  * Backpressure is natural: the async generator only pulls from upstream when the
    client consumes, and closing the client connection closes the upstream stream.

Run:  python mock_provider.py   (port 9100)
      PROVIDER_URL=http://127.0.0.1:9100/v1/chat/completions python gateway.py  (port 8090)
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from redactor import StreamRedactor, redact

logging.basicConfig(stream=sys.stderr, level=logging.INFO)
log = logging.getLogger("pii-gateway")

PROVIDER_URL = os.environ.get("PROVIDER_URL", "http://127.0.0.1:9100/v1/chat/completions")
PROVIDER_KEY = os.environ.get("PROVIDER_API_KEY", "")

client = httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=5.0))


@asynccontextmanager
async def lifespan(_: FastAPI):
    yield
    await client.aclose()


app = FastAPI(title="LLM Gateway — PII streaming guardrail", lifespan=lifespan)


# ---- standard gateway error shape (same as the Task 4 router):
#      {"error": {"type", "code", "message", "request_id"}}  -- never provider statuses, bodies or stack traces

_SAFE_REQUEST_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


def new_request_id(supplied: str | None = None) -> str:
    """Honour a well-formed client X-Request-Id, otherwise generate one."""
    return supplied if supplied and _SAFE_REQUEST_ID.match(supplied) else f"req_{uuid.uuid4().hex[:16]}"


def error_body(type_: str, code: str, message: str, request_id: str) -> dict:
    return {"error": {"type": type_, "code": code, "message": message, "request_id": request_id}}


def error_response(status: int, type_: str, code: str, message: str, request_id: str) -> JSONResponse:
    return JSONResponse(
        error_body(type_, code, message, request_id), status_code=status, headers={"X-Request-Id": request_id}
    )


@app.middleware("http")
async def request_id_mw(request: Request, call_next):
    request.state.request_id = new_request_id(request.headers.get("x-request-id"))
    try:
        return await call_next(request)
    except Exception:
        log.exception("[%s] unhandled error", request.state.request_id)  # trace to the log only
        return error_response(
            500, "internal_error", "internal_error", "An internal error occurred", request.state.request_id
        )


@app.exception_handler(StarletteHTTPException)
async def http_error(request: Request, exc: StarletteHTTPException):
    message = "Not found" if exc.status_code == 404 else "Request error"
    return error_response(exc.status_code, "invalid_request_error", "http_error", message, request.state.request_id)


def sse(obj: dict | str) -> bytes:
    data = obj if isinstance(obj, str) else json.dumps(obj, separators=(",", ":"))
    return f"data: {data}\n\n".encode()


async def redacted_stream(upstream: httpx.Response, request_id: str | None = None) -> AsyncIterator[bytes]:
    redactors: dict[int, StreamRedactor] = {}
    last_meta: dict = {}
    try:
        async for line in upstream.aiter_lines():
            if not line.startswith("data:"):
                continue  # comments / keep-alives / event: lines
            data = line[5:].strip()
            if data == "[DONE]":
                break
            try:
                evt = json.loads(data)
            except json.JSONDecodeError:
                log.warning("dropping malformed upstream SSE line")
                continue
            if not isinstance(evt, dict):
                log.warning("dropping non-object upstream SSE event")
                continue
            last_meta = {k: evt.get(k) for k in ("id", "object", "created", "model")}
            kept = []
            for choice in evt.get("choices", []):
                delta = choice.get("delta")
                if not isinstance(delta, dict):
                    delta = choice["delta"] = {}
                idx = choice.get("index", 0)
                content = delta.get("content")
                out = None
                if isinstance(content, str):
                    out = redactors.setdefault(idx, StreamRedactor()).feed(content)
                if choice.get("finish_reason") and idx in redactors:
                    # choice is ending: release its held tail *in this event*, so no
                    # content is ever sent after finish_reason (OpenAI stream contract)
                    out = (out or "") + redactors.pop(idx).flush()
                if out is not None:
                    if out:
                        delta["content"] = out
                    else:
                        delta.pop("content", None)
                # drop choices that now carry nothing (text is being held back)
                if delta or choice.get("finish_reason") or choice.get("logprobs"):
                    kept.append(choice)
            if not kept and evt.get("choices"):
                continue  # nothing to tell the client yet; don't send empty chunks
            evt["choices"] = kept
            yield sse(evt)
        # end of stream: flush any held tails
        for idx, r in redactors.items():
            tail = r.flush()
            if tail:
                yield sse({**last_meta, "choices": [{"index": idx, "delta": {"content": tail}, "finish_reason": None}]})
        yield sse("[DONE]")
    except httpx.HTTPError as exc:
        log.error("upstream stream broke: %s", exc.__class__.__name__)
        yield sse(
            error_body(
                "upstream_error",
                "upstream_stream_interrupted",
                "Upstream stream interrupted",
                request_id or new_request_id(),
            )
        )
    finally:
        await upstream.aclose()


@app.post("/v1/chat/completions")
async def completions(req: Request):
    rid = req.state.request_id
    try:
        body = await req.json()
    except (ValueError, RecursionError):  # RecursionError: absurdly deep nesting
        return error_response(400, "invalid_request_error", "invalid_json", "Body is not valid JSON", rid)
    if not isinstance(body, dict):
        return error_response(400, "invalid_request_error", "invalid_body", "Body must be a JSON object", rid)
    headers = {"Authorization": f"Bearer {PROVIDER_KEY}"} if PROVIDER_KEY else {}
    try:
        upstream = await client.send(
            client.build_request("POST", PROVIDER_URL, json=body, headers=headers), stream=True
        )
    except httpx.HTTPError as exc:
        log.warning("[%s] provider unreachable: %r", rid, exc)
        return error_response(502, "upstream_unavailable", "provider_unavailable", "Provider unavailable", rid)
    if upstream.status_code != 200:
        log.warning("[%s] provider answered %d", rid, upstream.status_code)  # logged, never returned
        await upstream.aclose()
        return error_response(502, "upstream_error", "provider_error", "The model provider returned an error", rid)

    if not body.get("stream"):
        try:
            data = json.loads(await upstream.aread())
        except (ValueError, RecursionError):
            log.warning("[%s] provider returned a non-JSON body", rid)
            return error_response(502, "upstream_error", "provider_error", "The model provider returned an error", rid)
        finally:
            await upstream.aclose()
        for c in data.get("choices", []):
            msg = c.get("message") or {}
            if isinstance(msg.get("content"), str):
                msg["content"] = redact(msg["content"])
        return JSONResponse(data, headers={"X-Request-Id": rid})

    return StreamingResponse(
        redacted_stream(upstream, rid),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "X-Request-Id": rid},
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("PORT", 8090)))
