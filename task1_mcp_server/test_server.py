"""Wire-level tests: spawn the server, speak raw JSON-RPC over pipes, assert stdout purity."""
import json
import subprocess
import sys
from pathlib import Path

SERVER = Path(__file__).with_name("server.py")


def run_session(calls):
    msgs = [
        {"jsonrpc": "2.0", "id": 0, "method": "initialize",
         "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                    "clientInfo": {"name": "t", "version": "0"}}},
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
    ]
    for i, (name, args) in enumerate(calls, start=2):
        msgs.append({"jsonrpc": "2.0", "id": i, "method": "tools/call",
                     "params": {"name": name, "arguments": args}})
    p = subprocess.Popen([sys.executable, str(SERVER)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, text=True)
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
    ("get_customer_record", {}),
    ("get_customer_record", {"customer_id": "CUST-00001", "extra": 1}),
    ("trigger_refund", {"customer_id": "CUST-00001", "amount": -5, "reason": "damaged item"}),
    ("trigger_refund", {"customer_id": "CUST-00001", "amount": 0, "reason": "damaged item"}),
    ("trigger_refund", {"customer_id": "CUST-00001", "amount": "10", "reason": "damaged item"}),
    ("trigger_refund", {"customer_id": "CUST-00001", "amount": True, "reason": "damaged item"}),
    ("trigger_refund", {"customer_id": "CUST-00001", "amount": 10.123, "reason": "damaged item"}),
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
    by_id, _ = run_session([
        ("trigger_refund", {"customer_id": "CUST-00042", "amount": 19.99, "reason": "Item arrived damaged"}),
        ("trigger_refund", {"customer_id": "CUST-99999", "amount": 5, "reason": "Item arrived damaged"}),
    ])
    assert "Refund RF-" in by_id[2]["result"]["content"][0]["text"]
    assert by_id[3]["result"]["isError"] is True
