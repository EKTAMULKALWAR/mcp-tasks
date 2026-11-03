import asyncio
import json
import time

import httpx
import pytest

from ratelimit import SlidingWindowTokenLimiter, tenant_hash
from router import Provider, Settings, create_app

OK = {"choices": [{"index": 0, "message": {"role": "assistant", "content": "hi"}}],
      "usage": {"total_tokens": 15}}


def make(tmp_path, primary, secondary=lambda r: httpx.Response(200, json=OK), tpm=50_000, timeout=0.3):
    async def handler(request: httpx.Request):
        fn = primary if request.url.host == "primary" else secondary
        out = fn(request)
        return await out if asyncio.iscoroutine(out) else out

    s = Settings(db_path=str(tmp_path / "rl.db"), tokens_per_minute=tpm, providers=[
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
