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
    r = await gw.post("/mcp", json={"jsonrpc": "2.0", "id": 7, "method": "tools/list"}, headers=auth("viewer"))
    assert r.json()["result"]["tools"][1]["name"] == "admin_reset_key"
    assert r.headers["mcp-session-id"] == "sess-123"
    # client credential is not leaked downstream
    assert "authorization" not in mock_downstream.app.state.last_headers


@pytest.mark.parametrize("name", ["admin_reset_key", "Admin_reset_key", "ADMIN_x", " admin_x"])
async def test_viewer_blocked_from_admin_tools_without_downstream_call(gw, name):
    r = await gw.post("/mcp", json=call(name, 9), headers=auth("viewer"))
    assert r.json() == {"jsonrpc": "2.0", "id": 9,
                        "error": {"code": -32001, "message": "Unauthorized Tool Call", "data": {"tool": name}}}
    assert mock_downstream.app.state.calls == []


async def test_admin_allowed(gw):
    r = await gw.post("/mcp", json=call("admin_reset_key"), headers=auth("admin"))
    assert r.json()["result"]["content"][0]["text"] == "ran admin_reset_key"


async def test_viewer_regular_tool_allowed(gw):
    r = await gw.post("/mcp", json=call("get_weather"), headers=auth("viewer"))
    assert r.json()["result"]["content"][0]["text"] == "ran get_weather"


@pytest.mark.parametrize("hdr", [None, "Basic abc", "Bearer ", "Bearer garbage",
                                 f"Bearer {token('viewer', exp_delta=-10)}",
                                 f"Bearer {token('admin', secret='wrong-secret-wrong-secret-wrong-secret')}",
                                 f"Bearer {token('superuser')}"])
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
    batch = [call("get_weather", 1), call("admin_reset_key", 2),
             {"jsonrpc": "2.0", "id": 3, "method": "tools/list"}]
    r = await gw.post("/mcp", json=batch, headers=auth("viewer"))
    by_id = {m["id"]: m for m in r.json()}
    assert by_id[2]["error"]["code"] == -32001
    assert "result" in by_id[1] and "result" in by_id[3]
    assert [m["id"] for m in mock_downstream.app.state.calls] == [1, 3]


async def test_downstream_down_returns_jsonrpc_error(gw):
    gateway.client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: (_ for _ in ()).throw(
        httpx.ConnectError("boom"))))
    r = await gw.post("/mcp", json=call("get_weather", 4), headers=auth("viewer"))
    assert r.status_code == 502 and r.json()["error"]["code"] == -32003
