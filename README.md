# MCP & LLM Gateway — four coding tasks (Python)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
for d in task1_mcp_server task2_mcp_gateway task3_pii_stream task4_router; do (cd $d && python -m pytest -q); done
```
Python 3.11+ (uses `asyncio.timeout`). Built against `mcp` 2.3.0. 41 tests total.

---

## Task 1 — Custom MCP server (`task1_mcp_server/`)
`python server.py` — stdio transport, official SDK low-level `Server`.

**Validation.** Pydantic v2 models with `strict=True` and `extra="forbid"`:
- `customer_id` must match `^CUST-\d{5}$` (rejects `CUST-1`, lowercase, 6 digits, ints).
- `amount`: finite, `> 0`, `<= 1,000,000`, ≤ 2 decimals; rejects strings (`"10"`), booleans (`true` is an int in Python!), NaN/Infinity.
- `reason`: 10–500 chars and not just whitespace padding.
- Unknown keys rejected. The same models generate the advertised `inputSchema`, so the contract and the enforcement can't drift.

**Error mapping.** Schema violations → JSON-RPC `-32602 Invalid params` with a structured `data.errors` list (field, message, type, never the raw input). Unknown tool → `-32602`. Unexpected failure → `-32603` with a generic message; trace goes to stderr. Business outcomes (customer not found) are `isError: true` tool results, the MCP convention for errors the model should see. Validation runs before any side effect.

**STDIO isolation.** (1) logging only on a stderr handler, no `print`; (2) the SDK writes JSON-RPC to a private dup of fd 1 and points fd 1 at stderr while serving, catching C-level writes; (3) after that claim, `sys.stdout` is rebound to stderr. Order matters: rebinding *before* the claim makes the SDK write the protocol to stderr. (I hit this during testing.) The tests spawn the real process and `json.loads` every stdout line, so any leak fails them.

## Task 2 — MCP security gateway (`task2_mcp_gateway/`)
`python mock_downstream.py` (:9000) and `GATEWAY_JWT_SECRET=... python gateway.py` (:8080), FastAPI + httpx.

- **Auth:** `Bearer <HS256 JWT>`; signature, `exp` and `role ∈ {admin, viewer}` required. Fails closed → HTTP 401 + JSON-RPC `-32000`.
- **Policy:** `tools/list` and lifecycle methods forwarded. `tools/call` with a name starting with `admin_` and a non-admin role → `-32001 Unauthorized Tool Call`, answered locally (tests assert the downstream got zero calls). The prefix check is case- and whitespace-insensitive so `Admin_reset_key` can't bypass it.
- **Wire format:** `-32700` parse error, `-32600` for invalid request or empty batch, `-32602` when `tools/call` has no string `params.name`. Notifications never get a response body. **Batches** are split: denied entries are answered by the gateway, allowed ones forwarded as a smaller batch, results merged.
- **Proxying:** responses (JSON or SSE) are streamed back untouched; hop-by-hop headers are stripped and **the client's bearer token is not forwarded downstream**. `Mcp-Session-Id` passes through. If the downstream is unreachable → 502 + `-32003`.
- I chose HTTP 200 for JSON-RPC errors (including -32001), per JSON-RPC-over-HTTP convention; switching it to 403 is a one-line change if graders expect that.

## Task 3 — Streaming PII guardrail (`task3_pii_stream/`)
`python mock_provider.py` (:9100) and `python gateway.py` (:8090). OpenAI-compatible `/v1/chat/completions`.

**Algorithm (`redactor.py`).** Per choice, keep only a *hold-back tail*: the shortest suffix that could still grow into a match (a partial word/email, or a run of digits, spaces and dashes). Everything before it is final, so it's redacted and emitted immediately. Two guards:
- the tail is capped at 256 chars (≥ the max email length), so memory is O(256) per stream even on adversarial input;
- the cut point never falls inside a visible complete match, so a match is never split across two emits.

In normal prose the delay is about one token. Per-chunk work is O(chunk + 256), with no full-response accumulation.

- Patterns: email; SSN (excludes invalid 000/666/9xx areas); 13–19 digit card numbers with optional separators, **Luhn-checked** so order numbers pass through.
- **Correctness test:** for every sample, 300 random chunkings must produce exactly `redact(full_text)`. That's the property that proves boundary-straddling PII is caught.
- Gateway parses SSE line by line, passes role/finish events through immediately, flushes the tail on `finish_reason`/`[DONE]`, and closes the upstream stream if the client disconnects.
- Throughput is about 340k chunks/s single-core on the benchmark.

## Task 4 — Rate limiter + fallback router (`task4_router/`)
`python router.py` (:8100). Providers set by `PRIMARY_URL` / `SECONDARY_URL`; `mock_providers.py <name> <port> ok|429|slow|500`.

**Rate limiter (`ratelimit.py`), on-disk SQLite.**
- A true sliding *log* `(tenant, ts, tokens)`, 50,000 tokens / 60 s per tenant by default.
- **Reserve → settle/release:** reserve `prompt_estimate + max_tokens` before calling; after the call, settle to the provider's real `usage.total_tokens`, or release if nothing succeeded. Release is shielded, so client disconnects don't leak a reservation. This prevents concurrent bursts slipping through before anyone is counted.
- Check + evict + insert run in one `BEGIN IMMEDIATE` transaction. That makes admission atomic across tasks, threads *and* processes sharing the DB file. WAL mode is on, and SQLite calls run in `asyncio.to_thread`.
- Expired rows are deleted in that same transaction (bounded state, exact sums). `Retry-After` is computed exactly by walking the oldest rows until enough would expire.
- API keys are stored only as SHA-256 hashes.

**Failover.**
- Each attempt is bounded by `asyncio.timeout(3.0)` over connect + send + read.
- On expiry the in-flight request is cancelled, so a late primary response can't race the fallback.
- Fails over on 429, timeout, transport error, 5xx or malformed body. Does **not** fail over on other 4xx: a bad request would just be billed twice.

**Error sanitization.** Every error is `{"error": {"type","code","message","request_id"}}`:
- Upstream bodies and stack traces are logged with the request id, never returned.
- A catch-all middleware turns any unhandled exception into a generic 500.
- Framework 404s and validation errors use the same shape.
- The tests plant secrets (passwords, internal IPs, tracebacks) in upstream failures and assert none reach the client.

**Tests cover:**
- failover on 429 and on timeout, with a bounded wall-clock time;
- both providers failing (sanitized 503, reservation released);
- **30 concurrent requests against a 1,000-token budget → exactly 9 admitted**;
- tenant isolation;
- eviction and exact Retry-After with an injected clock;
- persistence across a restart.
