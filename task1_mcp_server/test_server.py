"""Wire-level tests: spawn the server, speak raw JSON-RPC over pipes, assert stdout purity."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

SERVER = Path(__file__).with_name("server.py")


def run_session(calls):
    msgs = [
        {
            "jsonrpc": "2.0",
            "id": 0,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "t", "version": "0"},
            },
        },
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
    ]
    for i, (name, args) in enumerate(calls, start=2):
        msgs.append({"jsonrpc": "2.0", "id": i, "method": "tools/call", "params": {"name": name, "arguments": args}})
    p = subprocess.Popen(
        [sys.executable, str(SERVER)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    for m in msgs:
        p.stdin.write(json.dumps(m) + "\n")
    p.stdin.flush()
    expected = sum(1 for m in msgs if "id" in m)
    parsed = []
    while len([m for m in parsed if "id" in m]) < expected:
        line = p.stdout.readline()
        assert line, "server closed stdout early"
        parsed.append(json.loads(line))  # fails loudly if any non-JSON leaked to stdout
    rest, stderr = p.communicate(timeout=10)  # closes stdin -> server sees EOF and exits
    assert rest.strip() == "", f"unexpected trailing stdout: {rest!r}"
    assert all(m.get("jsonrpc") == "2.0" for m in parsed)
    return {m["id"]: m for m in parsed if "id" in m}, stderr


def test_stdout_is_pure_jsonrpc_and_logs_go_to_stderr():
    by_id, stderr = run_session([("get_customer_record", {"customer_id": "CUST-00001"})])
    assert "starting customer-mcp" in stderr
    assert {t["name"] for t in by_id[1]["result"]["tools"]} == {"get_customer_record", "trigger_refund"}
    assert "Ada Lovelace" in by_id[2]["result"]["content"][0]["text"]


BAD = [
    ("get_customer_record", {"customer_id": "CUST-1"}),
    ("get_customer_record", {"customer_id": "cust-00001"}),
    ("get_customer_record", {"customer_id": "CUST-000011"}),
    ("get_customer_record", {"customer_id": 12345}),
    ("get_customer_record", {"customer_id": "CUST-\u0661\u0662\u0663\u0664\u0665"}),  # Arabic-Indic digits
    ("get_customer_record", {"customer_id": "CUST-\uff11\uff12\uff13\uff14\uff15"}),  # fullwidth digits
    ("trigger_refund", {"customer_id": "CUST-\u0661\u0662\u0663\u0664\u0665", "amount": 5, "reason": "damaged item"}),
    ("get_customer_record", {}),
    ("get_customer_record", {"customer_id": "CUST-00001", "extra": 1}),
    ("trigger_refund", {"customer_id": "CUST-00001", "amount": -5, "reason": "damaged item"}),
    ("trigger_refund", {"customer_id": "CUST-00001", "amount": 0, "reason": "damaged item"}),
    ("trigger_refund", {"customer_id": "CUST-00001", "amount": "10", "reason": "damaged item"}),
    ("trigger_refund", {"customer_id": "CUST-00001", "amount": True, "reason": "damaged item"}),
    ("trigger_refund", {"customer_id": "CUST-00001", "amount": 10.123, "reason": "damaged item"}),
    ("trigger_refund", {"customer_id": "CUST-00001", "amount": 1000000.01, "reason": "damaged item"}),
    ("trigger_refund", {"customer_id": "CUST-00001", "amount": 10, "reason": "short"}),
    ("trigger_refund", {"customer_id": "CUST-00001", "amount": 10, "reason": "   a      "}),
    ("no_such_tool", {}),
]


def test_invalid_inputs_return_jsonrpc_invalid_params():
    by_id, _ = run_session(BAD)
    for i, case in enumerate(BAD, start=2):
        assert "error" in by_id[i], case
        assert by_id[i]["error"]["code"] == -32602, (case, by_id[i])


def test_valid_refund_and_not_found():
    by_id, _ = run_session(
        [
            ("trigger_refund", {"customer_id": "CUST-00042", "amount": 19.99, "reason": "Item arrived damaged"}),
            ("trigger_refund", {"customer_id": "CUST-99999", "amount": 5, "reason": "Item arrived damaged"}),
        ]
    )
    assert "Refund RF-" in by_id[2]["result"]["content"][0]["text"]
    assert by_id[3]["result"]["isError"] is True


# ---- reference-client test: official SDK ClientSession over a real stdio subprocess


async def _with_sdk_client(fn):
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    params = StdioServerParameters(command=sys.executable, args=[str(SERVER)])
    async with stdio_client(params, errlog=open(os.devnull, "w")) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            return await fn(session)


async def test_official_sdk_client_end_to_end():
    from mcp.shared.exceptions import MCPError

    async def scenario(session):
        tools = await session.list_tools()
        names = {t.name for t in tools.tools}
        schemas = {t.name: t.input_schema for t in tools.tools}

        ok = await session.call_tool("get_customer_record", {"customer_id": "CUST-00001"})
        refund = await session.call_tool(
            "trigger_refund", {"customer_id": "CUST-00042", "amount": 19.99, "reason": "Item arrived damaged"}
        )
        missing = await session.call_tool("get_customer_record", {"customer_id": "CUST-99999"})
        with pytest.raises(MCPError) as bad_id:
            await session.call_tool("get_customer_record", {"customer_id": "nope"})
        with pytest.raises(MCPError) as bad_amount:
            await session.call_tool(
                "trigger_refund", {"customer_id": "CUST-00001", "amount": -1, "reason": "long enough reason"}
            )
        # session still healthy after protocol errors
        again = await session.call_tool("get_customer_record", {"customer_id": "CUST-00042"})
        return names, schemas, ok, refund, missing, bad_id.value, bad_amount.value, again

    names, schemas, ok, refund, missing, bad_id, bad_amount, again = await _with_sdk_client(scenario)
    assert names == {"get_customer_record", "trigger_refund"}
    assert schemas["get_customer_record"]["properties"]["customer_id"]["pattern"] == r"^CUST-[0-9]{5}$"
    assert schemas["trigger_refund"]["properties"]["reason"]["minLength"] == 10
    assert not ok.is_error and "Ada Lovelace" in ok.content[0].text
    assert "Refund RF-" in refund.content[0].text
    assert missing.is_error is True  # business error: tool result, not protocol error
    assert bad_id.code == -32602 and bad_amount.code == -32602
    assert [e["field"] for e in bad_amount.data["errors"]] == ["amount"]
    assert not again.is_error and "Alan Turing" in again.content[0].text


# ---- -32603: an unexpected failure inside a handler (in-process: the real Server, via the SDK's in-memory client)


class _ExplodingCustomers(dict):
    def get(self, *a, **k):
        raise RuntimeError("SECRET db password=hunter2 at 10.0.0.7")

    __contains__ = get  # the refund path uses `in`


async def test_unexpected_handler_failure_is_internal_error_minus_32603(monkeypatch, caplog):
    from mcp import Client
    from mcp.shared.exceptions import MCPError

    import server as srv

    monkeypatch.setattr(srv, "CUSTOMERS", _ExplodingCustomers())
    with caplog.at_level("ERROR", logger="customer-mcp"):
        async with Client(srv.server) as client:
            with pytest.raises(MCPError) as lookup:
                await client.call_tool("get_customer_record", {"customer_id": "CUST-00001"})
            with pytest.raises(MCPError) as refund:
                await client.call_tool(
                    "trigger_refund", {"customer_id": "CUST-00001", "amount": 5, "reason": "damaged item"}
                )
            # the session survives, and validation errors still map to -32602
            with pytest.raises(MCPError) as bad:
                await client.call_tool("get_customer_record", {"customer_id": "nope"})
    for err in (lookup.value, refund.value):
        assert err.code == -32603 and err.message == "Internal error"
        assert "hunter2" not in str(err) and "10.0.0.7" not in str(err) and not err.data
    assert bad.value.code == -32602
    # the detail went to the log (stderr), with the stack trace
    assert "hunter2" in caplog.text and "Traceback" in caplog.text
