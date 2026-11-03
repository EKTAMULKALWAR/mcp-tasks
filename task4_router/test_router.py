import asyncio
import json
import time

import httpx
import pytest

from ratelimit import SlidingWindowTokenLimiter, tenant_hash
from router import Provider, Settings, create_app

OK = {"choices": [{"index": 0, "message": {"role": "assistant", "content": "hi"}}],
      "usage": {"total_tokens": 15}}


def make(tmp_path, primary, secondary=lambda r: httpx.Response(200, json=OK), tpm=50_000, timeout=0.3, total=6.0):
    async def handler(request: httpx.Request):
        fn = primary if request.url.host == "primary" else secondary
        out = fn(request)
        return await out if asyncio.iscoroutine(out) else out

    s = Settings(db_path=str(tmp_path / "rl.db"), tokens_per_minute=tpm, total_timeout_s=total, providers=[
        Provider("primary", "http://primary/v1/chat/completions", "m1", timeout_s=timeout),
        Provider("secondary", "http://secondary/v1/chat/completions", "m2", timeout_s=timeout)])
    app = create_app(s, httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    return app, httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw")


BODY = {"messages": [{"role": "user", "content": "hello"}], "max_tokens": 100}
H = {"Authorization": "Bearer tenant-a-key"}


async def test_primary_ok(tmp_path):
    app, c = make(tmp_path, lambda r: httpx.Response(200, json=OK))
    r = await c.post("/v1/chat/completions", json=BODY, headers=H)
    assert r.status_code == 200 and r.headers["x-gateway-provider"] == "primary"
    assert await app.state.limiter.usage(tenant_hash("tenant-a-key")) == 15


async def test_failover_on_429(tmp_path):
    _, c = make(tmp_path, lambda r: httpx.Response(429, json={"trace": "secret"}))
    r = await c.post("/v1/chat/completions", json=BODY, headers=H)
    assert r.status_code == 200 and r.headers["x-gateway-provider"] == "secondary"


async def test_failover_on_timeout_is_bounded(tmp_path):
    async def slow(r):
        await asyncio.sleep(5)
        return httpx.Response(200, json=OK)

    _, c = make(tmp_path, slow, timeout=0.3)
    t = time.perf_counter()
    r = await c.post("/v1/chat/completions", json=BODY, headers=H)
    assert r.headers["x-gateway-provider"] == "secondary"
    assert time.perf_counter() - t < 1.0


async def test_both_fail_sanitized_and_reservation_released(tmp_path):
    app, c = make(tmp_path, lambda r: httpx.Response(500, text="Traceback: db password=hunter2"),
                  lambda r: (_ for _ in ()).throw(httpx.ConnectError("10.0.0.7:443 refused")))
    r = await c.post("/v1/chat/completions", json=BODY, headers=H)
    assert r.status_code == 503
    err = r.json()["error"]
    assert set(err) == {"type", "code", "message", "request_id"} and err["code"] == "all_providers_failed"
    assert "hunter2" not in r.text and "10.0.0.7" not in r.text and "Traceback" not in r.text
    assert await app.state.limiter.usage(tenant_hash("tenant-a-key")) == 0


async def test_4xx_not_failed_over(tmp_path):
    calls = []
    _, c = make(tmp_path, lambda r: httpx.Response(400, json={"internal": "x"}),
                lambda r: calls.append(1) or httpx.Response(200, json=OK))
    r = await c.post("/v1/chat/completions", json=BODY, headers=H)
    assert r.status_code == 400 and r.json()["error"]["code"] == "upstream_rejected" and not calls


async def test_unexpected_exception_sanitized(tmp_path):
    def boom(r):
        raise RuntimeError("SECRET internal detail")
    _, c = make(tmp_path, boom, boom)
    r = await c.post("/v1/chat/completions", json=BODY, headers=H)
    assert r.status_code == 500 and "SECRET" not in r.text and r.json()["error"]["code"] == "internal_error"


async def test_bad_requests(tmp_path):
    _, c = make(tmp_path, lambda r: httpx.Response(200, json=OK))
    assert (await c.post("/v1/chat/completions", json=BODY)).json()["error"]["code"] == "missing_api_key"
    assert (await c.post("/v1/chat/completions", content=b"{", headers=H)).json()["error"]["code"] == "invalid_json"
    assert (await c.post("/v1/chat/completions", json={"messages": []}, headers=H)).status_code == 400
    assert (await c.post("/v1/chat/completions", json={**BODY, "max_tokens": -1}, headers=H)).status_code == 400
    assert (await c.post("/nope", headers=H)).json()["error"]["type"] == "invalid_request_error"


async def test_concurrent_admission_never_exceeds_limit(tmp_path):
    async def slow_ok(r):
        await asyncio.sleep(0.05)
        return httpx.Response(200, json={**OK, "usage": {"total_tokens": 108}})
    _, c = make(tmp_path, slow_ok, tpm=1000)
    rs = await asyncio.gather(*[c.post("/v1/chat/completions", json=BODY, headers=H) for _ in range(30)])
    codes = [r.status_code for r in rs]
    assert codes.count(200) == 9 and codes.count(429) == 21
    r429 = next(r for r in rs if r.status_code == 429)
    assert int(r429.headers["retry-after"]) >= 1 and r429.json()["error"]["code"] == "tenant_token_limit"


async def test_tenants_isolated(tmp_path):
    _, c = make(tmp_path, lambda r: httpx.Response(200, json={**OK, "usage": {"total_tokens": 900}}), tpm=1000)
    assert (await c.post("/v1/chat/completions", json=BODY, headers=H)).status_code == 200
    assert (await c.post("/v1/chat/completions", json=BODY, headers=H)).status_code == 429
    other = {"Authorization": "Bearer tenant-b-key"}
    assert (await c.post("/v1/chat/completions", json=BODY, headers=other)).status_code == 200


def test_sliding_window_eviction_and_retry_after(tmp_path):
    now = [1000.0]
    lim = SlidingWindowTokenLimiter(str(tmp_path / "w.db"), 100, 60, clock=lambda: now[0])
    assert lim._reserve_sync("t", 60).allowed
    now[0] = 1030
    assert lim._reserve_sync("t", 40).allowed
    d = lim._reserve_sync("t", 10)
    assert not d.allowed and d.retry_after == pytest.approx(30)
    now[0] = 1060.001
    assert lim._reserve_sync("t", 50).allowed
    rows = lim._conn.execute("SELECT COUNT(*) FROM token_usage").fetchone()[0]
    assert rows == 2
    assert not lim._reserve_sync("t", 101).allowed


def test_state_persists_on_disk(tmp_path):
    p = str(tmp_path / "p.db")
    a = SlidingWindowTokenLimiter(p, 100)
    a._reserve_sync("t", 90)
    a.close()
    b = SlidingWindowTokenLimiter(p, 100)
    assert not b._reserve_sync("t", 20).allowed


# ---- regressions for bugs found in review

async def test_non_object_messages_rejected_not_500(tmp_path):
    _, c = make(tmp_path, lambda r: httpx.Response(200, json=OK))
    r = await c.post("/v1/chat/completions", json={"messages": ["hi"]}, headers=H)
    assert r.status_code == 400 and r.json()["error"]["code"] == "invalid_body"


async def test_request_larger_than_window_is_400_not_retryable_429(tmp_path):
    app, c = make(tmp_path, lambda r: httpx.Response(200, json=OK), tpm=1000)
    r = await c.post("/v1/chat/completions", json={**BODY, "max_tokens": 5000}, headers=H)
    assert r.status_code == 400 and r.json()["error"]["code"] == "request_too_large"
    assert "retry-after" not in r.headers
    assert await app.state.limiter.usage(tenant_hash("tenant-a-key")) == 0


async def test_rate_limit_store_failure_is_sanitized_503(tmp_path):
    import sqlite3
    app, c = make(tmp_path, lambda r: httpx.Response(200, json=OK))

    def locked(*a):
        raise sqlite3.OperationalError("database is locked: /var/secret/rl.db")
    app.state.limiter._reserve_sync = locked
    r = await c.post("/v1/chat/completions", json=BODY, headers=H)
    assert r.status_code == 503 and r.json()["error"]["code"] == "rate_limiter_unavailable"
    assert "secret" not in r.text and r.headers["retry-after"] == "1"


# ---- overall deadline

async def test_total_deadline_bounds_latency(tmp_path):
    async def slow(r):
        await asyncio.sleep(5)
        return httpx.Response(200, json=OK)

    # per-attempt budget 0.3s each would allow 0.6s; the total cap of 0.4s must win
    _, c = make(tmp_path, slow, slow, timeout=0.3, total=0.4)
    t = time.perf_counter()
    r = await c.post("/v1/chat/completions", json=BODY, headers=H)
    elapsed = time.perf_counter() - t
    assert r.status_code == 503 and r.json()["error"]["code"] == "all_providers_failed"
    assert elapsed < 0.55, elapsed


async def test_exhausted_deadline_skips_secondary(tmp_path):
    seen = []

    async def slow(r):
        await asyncio.sleep(5)

    _, c = make(tmp_path, slow, lambda r: seen.append(1) or httpx.Response(200, json=OK),
                timeout=0.3, total=0.3)
    r = await c.post("/v1/chat/completions", json=BODY, headers=H)
    assert r.status_code == 503 and not seen


# ---- background sweep of idle tenants

def test_sweep_evicts_idle_tenants_in_batches(tmp_path):
    now = [1000.0]
    lim = SlidingWindowTokenLimiter(str(tmp_path / "s.db"), 1000, 60, clock=lambda: now[0])
    for t in ("a", "b", "c"):
        lim._reserve_sync(t, 10)
        lim._reserve_sync(t, 10)
    now[0] = 1050
    lim._reserve_sync("fresh", 10)
    now[0] = 1061  # the six old rows expired; "fresh" has not
    assert lim._sweep_sync(batch=4) == 4          # bounded: only one batch per transaction
    assert lim._sweep_sync(batch=4) == 2
    left = lim._conn.execute("SELECT tenant FROM token_usage").fetchall()
    assert left == [("fresh",)]


async def test_async_sweep_loops_over_batches(tmp_path):
    now = [0.0]
    lim = SlidingWindowTokenLimiter(str(tmp_path / "s.db"), 1000, 60, clock=lambda: now[0])
    for i in range(5):
        lim._reserve_sync(f"t{i}", 1)
    now[0] = 100
    assert await lim.sweep(batch=2) == 5


async def test_sweeper_task_runs_in_lifespan_and_is_cancelled(tmp_path):
    now = [0.0]
    app, _ = make(tmp_path, lambda r: httpx.Response(200, json=OK))
    lim = app.state.limiter
    lim.clock = lambda: now[0]
    lim._reserve_sync("idle-tenant", 5)
    app.state.router.s.sweep_interval_s = 0.01
    async with app.router.lifespan_context(app):
        now[0] = 100
        for _ in range(100):
            await asyncio.sleep(0.01)
            if lim._conn.execute("SELECT COUNT(*) FROM token_usage").fetchone()[0] == 0:
                break
        assert lim._conn.execute("SELECT COUNT(*) FROM token_usage").fetchone()[0] == 0
        assert not app.state.sweeper.done()
    assert app.state.sweeper.cancelled()
