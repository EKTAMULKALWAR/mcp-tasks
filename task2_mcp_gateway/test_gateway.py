import time

import httpx
import jwt
import pytest

import gateway
import mock_downstream

pytestmark = pytest.mark.asyncio


def token(role, exp_delta=300, secret=gateway.JWT_SECRET):
    return jwt.encode({"sub": "u1", "role": role, "exp": int(time.time()) + exp_delta}, secret, algorithm="HS256")


@pytest.fixture
async def gw():
    mock_downstream.app.state.calls = []
    gateway.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=mock_downstream.app))
    gateway.DOWNSTREAM_URL = "http://downstream/mcp"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://gw") as c:
        yield c
    await gateway.client.aclose()


def call(name, id_=1):
    return {"jsonrpc": "2.0", "id": id_, "method": "tools/call", "params": {"name": name, "arguments": {}}}


def auth(role):
    return {"Authorization": f"Bearer {token(role)}"}


async def test_tools_list_forwarded_transparently(gw):
    payload = {"jsonrpc": "2.0", "id": 7, "method": "tools/list", "params": {"cursor": "abc"}}
    r = await gw.post("/mcp", json=payload, headers=auth("viewer"))
    assert r.json()["result"]["tools"][1]["name"] == "admin_reset_key"
    # the downstream received exactly the payload the client sent (one call, nothing added or dropped)
    assert mock_downstream.app.state.calls == [payload]
    assert r.headers["mcp-session-id"] == "sess-123"
    # client credential is not leaked downstream
    assert "authorization" not in mock_downstream.app.state.last_headers


@pytest.mark.parametrize("name", ["admin_reset_key", "Admin_reset_key", "ADMIN_x", " admin_x"])
async def test_viewer_blocked_from_admin_tools_without_downstream_call(gw, name):
    r = await gw.post("/mcp", json=call(name, 9), headers=auth("viewer"))
    assert r.json() == {
        "jsonrpc": "2.0",
        "id": 9,
        "error": {"code": -32001, "message": "Unauthorized Tool Call", "data": {"tool": name}},
    }
    assert mock_downstream.app.state.calls == []


async def test_admin_allowed(gw):
    r = await gw.post("/mcp", json=call("admin_reset_key"), headers=auth("admin"))
    assert r.json()["result"]["content"][0]["text"] == "ran admin_reset_key"


async def test_viewer_regular_tool_allowed(gw):
    r = await gw.post("/mcp", json=call("get_weather"), headers=auth("viewer"))
    assert r.json()["result"]["content"][0]["text"] == "ran get_weather"


@pytest.mark.parametrize(
    "hdr",
    [
        None,
        "Basic abc",
        "Bearer ",
        "Bearer garbage",
        f"Bearer {token('viewer', exp_delta=-10)}",
        f"Bearer {token('admin', secret='wrong-secret-wrong-secret-wrong-secret')}",
        f"Bearer {token('superuser')}",
    ],
)
async def test_bad_auth_rejected(gw, hdr):
    headers = {"Authorization": hdr} if hdr else {}
    r = await gw.post("/mcp", json=call("get_weather"), headers=headers)
    assert r.status_code == 401 and r.json()["error"]["code"] == -32000
    assert mock_downstream.app.state.calls == []


async def test_wire_format_errors(gw):
    h = auth("admin")
    assert (await gw.post("/mcp", content=b"{not json", headers=h)).json()["error"]["code"] == -32700
    assert (await gw.post("/mcp", json=[], headers=h)).json()["error"]["code"] == -32600
    assert (await gw.post("/mcp", json={"id": 1, "method": "x"}, headers=h)).json()["error"]["code"] == -32600
    bad = {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": 5}}
    assert (await gw.post("/mcp", json=bad, headers=h)).json()["error"]["code"] == -32602


async def test_mixed_batch_split_and_merged(gw):
    batch = [call("get_weather", 1), call("admin_reset_key", 2), {"jsonrpc": "2.0", "id": 3, "method": "tools/list"}]
    r = await gw.post("/mcp", json=batch, headers=auth("viewer"))
    by_id = {m["id"]: m for m in r.json()}
    assert by_id[2]["error"]["code"] == -32001
    assert "result" in by_id[1] and "result" in by_id[3]
    assert [m["id"] for m in mock_downstream.app.state.calls] == [1, 3]


async def test_downstream_down_returns_jsonrpc_error(gw):
    gateway.client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: (_ for _ in ()).throw(httpx.ConnectError("boom")))
    )
    r = await gw.post("/mcp", json=call("get_weather", 4), headers=auth("viewer"))
    assert r.status_code == 502 and r.json()["error"]["code"] == -32003


# ---- JWT algorithm confusion

PAYLOAD = {"sub": "u1", "role": "admin", "exp": int(time.time()) + 300}


@pytest.mark.parametrize(
    "tok",
    [
        jwt.encode(PAYLOAD, None, algorithm="none"),  # type: ignore[arg-type]  # unsigned token
        jwt.encode(PAYLOAD, gateway.JWT_SECRET, algorithm="HS512"),  # right key, wrong algorithm
        jwt.encode(PAYLOAD, gateway.JWT_SECRET, algorithm="HS384"),
        jwt.encode(PAYLOAD, gateway.JWT_SECRET, algorithm="HS256") + "x",  # tampered signature
    ],
    ids=["alg-none", "hs512", "hs384", "bad-signature"],
)
async def test_algorithm_confusion_rejected(gw, tok):
    r = await gw.post("/mcp", json=call("admin_reset_key"), headers={"Authorization": f"Bearer {tok}"})
    assert r.status_code == 401 and r.json()["error"]["code"] == -32000
    assert mock_downstream.app.state.calls == []


# ---- notifications never get a response body


async def test_denied_notification_gets_no_response_body(gw):
    note = {"jsonrpc": "2.0", "method": "tools/call", "params": {"name": "admin_reset_key"}}
    r = await gw.post("/mcp", json=note, headers=auth("viewer"))
    assert r.status_code == 202 and r.content == b""
    assert mock_downstream.app.state.calls == []  # denied, so not forwarded either


async def test_denied_notification_in_batch_is_omitted_from_results(gw):
    batch = [
        {"jsonrpc": "2.0", "method": "tools/call", "params": {"name": "admin_reset_key"}},
        call("admin_reset_key", 5),
    ]
    r = await gw.post("/mcp", json=batch, headers=auth("viewer"))
    assert [m["id"] for m in r.json()] == [5]  # only the request with an id is answered


async def test_allowed_notification_has_no_body(gw):
    r = await gw.post("/mcp", json={"jsonrpc": "2.0", "method": "notifications/initialized"}, headers=auth("viewer"))
    assert r.status_code == 202 and r.content == b""


# ---- body size limit


async def test_oversized_body_rejected_without_forwarding(gw, monkeypatch):
    monkeypatch.setattr(gateway, "MAX_BODY_BYTES", 200)
    big = call("get_weather")
    big["params"]["arguments"] = {"pad": "x" * 500}
    r = await gw.post("/mcp", json=big, headers=auth("admin"))
    assert r.status_code == 413
    assert r.json()["error"]["code"] == -32600 and r.json()["error"]["data"] == {"max_bytes": 200}
    assert mock_downstream.app.state.calls == []


async def test_oversized_chunked_body_without_content_length(gw, monkeypatch):
    monkeypatch.setattr(gateway, "MAX_BODY_BYTES", 100)

    async def body():  # no Content-Length -> must be capped while streaming
        for _ in range(10):
            yield b"x" * 50

    r = await gw.post("/mcp", content=body(), headers=auth("admin"))
    assert r.status_code == 413 and mock_downstream.app.state.calls == []


async def test_body_at_limit_is_accepted(gw, monkeypatch):
    payload = {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
    import json as _json

    monkeypatch.setattr(gateway, "MAX_BODY_BYTES", len(_json.dumps(payload).encode()) + 5)
    r = await gw.post("/mcp", json=payload, headers=auth("viewer"))
    assert r.status_code == 200 and "tools" in r.json()["result"]


# ---- development-secret warning


async def _startup_warnings(caplog):
    with caplog.at_level("WARNING", logger="mcp-gateway"):
        async with gateway.app.router.lifespan_context(gateway.app):
            pass
    return [r for r in caplog.records if "GATEWAY_JWT_SECRET is not set" in r.getMessage()]


async def test_warns_at_startup_when_using_the_dev_secret(caplog, monkeypatch):
    monkeypatch.setattr(gateway, "USING_DEV_SECRET", True)
    warnings = await _startup_warnings(caplog)
    assert len(warnings) == 1 and warnings[0].levelname == "WARNING"


async def test_no_warning_when_secret_is_configured(caplog, monkeypatch):
    monkeypatch.setattr(gateway, "USING_DEV_SECRET", False)
    assert await _startup_warnings(caplog) == []


@pytest.mark.parametrize(("env", "is_dev"), [(None, True), ("", True), ("x" * 40, False)])
async def test_secret_selection_from_environment(env, is_dev, monkeypatch):
    import importlib

    if env is None:
        monkeypatch.delenv("GATEWAY_JWT_SECRET", raising=False)
    else:
        monkeypatch.setenv("GATEWAY_JWT_SECRET", env)
    try:
        reloaded = importlib.reload(gateway)
        assert reloaded.USING_DEV_SECRET is is_dev
        assert (reloaded.JWT_SECRET == reloaded.DEV_JWT_SECRET) is is_dev  # empty env must not become an empty secret
    finally:
        monkeypatch.delenv("GATEWAY_JWT_SECRET", raising=False)
        importlib.reload(gateway)
