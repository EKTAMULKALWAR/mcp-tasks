"""Scenario checks for `make demo`. Servers are already running (see demo.sh); ports come from env."""

import json
import os
import socket
import subprocess
import sys
import time

import httpx
import jwt

E = os.environ
ROOT = E["DEMO_ROOT"]
results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  ({detail})" if detail else ""), flush=True)


def wait_ports(ports, timeout=30):
    deadline = time.time() + timeout
    for port in ports:
        while True:
            try:
                socket.create_connection(("127.0.0.1", int(port)), timeout=0.5).close()
                break
            except OSError:
                if time.time() > deadline:
                    sys.exit(f"FAIL  server on port {port} did not start")
                time.sleep(0.1)


# ------------------------------------------------------------------ Task 1
def task1() -> None:
    print("\n== Task 1: MCP stdio server")
    msgs = [
        {
            "jsonrpc": "2.0",
            "id": 0,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "demo", "version": "0"},
            },
        },
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "get_customer_record", "arguments": {"customer_id": "CUST-00001"}},
        },
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "trigger_refund",
                "arguments": {"customer_id": "CUST-00001", "amount": 25.5, "reason": "Item arrived damaged"},
            },
        },
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "get_customer_record", "arguments": {"customer_id": "CUST-1"}},
        },
        {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "trigger_refund",
                "arguments": {"customer_id": "CUST-00001", "amount": -5, "reason": "short"},
            },
        },
    ]
    p = subprocess.Popen(
        [E["DEMO_PY"], os.path.join(ROOT, "task1_mcp_server", "server.py")],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    for m in msgs:
        p.stdin.write(json.dumps(m) + "\n")
    p.stdin.flush()
    lines, pure = [], True
    expected = sum(1 for m in msgs if "id" in m)  # wait for every reply before closing stdin (EOF = shutdown)
    while len([m for m in lines if "id" in m]) < expected:
        line = p.stdout.readline()
        if not line:
            break
        try:
            lines.append(json.loads(line))
        except json.JSONDecodeError:
            pure = False
    out, err = p.communicate(timeout=20)
    pure = pure and out.strip() == ""
    by_id = {m["id"]: m for m in lines if "id" in m}
    check("stdout carries only JSON-RPC (logs on stderr)", pure and "starting customer-mcp" in err)
    check(
        "valid get_customer_record",
        "Ada Lovelace" in by_id.get(1, {}).get("result", {}).get("content", [{}])[0].get("text", ""),
    )
    check("valid trigger_refund", "Refund RF-" in str(by_id.get(2)))
    check("invalid customer_id -> -32602", by_id.get(3, {}).get("error", {}).get("code") == -32602)
    check(
        "invalid amount+reason -> -32602",
        by_id.get(4, {}).get("error", {}).get("code") == -32602,
        f"fields: {[e['field'] for e in by_id.get(4, {}).get('error', {}).get('data', {}).get('errors', [])]}",
    )


# ------------------------------------------------------------------ Task 2
def tok(role):
    return jwt.encode({"sub": "demo", "role": role, "exp": int(time.time()) + 600}, E["DEMO_SECRET"], "HS256")


def task2() -> None:
    print("\n== Task 2: MCP security gateway")
    url = f"http://127.0.0.1:{E['DEMO_GW2']}/mcp"
    call = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "admin_reset_key"}}
    with httpx.Client() as c:
        r = c.post(url, json=call, headers={"Authorization": f"Bearer {tok('viewer')}"})
        check("viewer calling admin_reset_key blocked with -32001", r.json().get("error", {}).get("code") == -32001)
        r = c.post(url, json=call, headers={"Authorization": f"Bearer {tok('admin')}"})
        check("admin calling admin_reset_key allowed", "ran admin_reset_key" in r.text)
        r = c.post(
            url,
            json={"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
            headers={"Authorization": f"Bearer {tok('viewer')}"},
        )
        check("tools/list forwarded for viewer", len(r.json()["result"]["tools"]) == 2)
        r = c.post(url, json=call)
        check("no token -> 401", r.status_code == 401)
        d = c.get(f"http://127.0.0.1:{E['DEMO_DS']}/docs")  # downstream is up, gateway just never reached it
        check("mock downstream is up (so the denial above was a gateway decision)", d.status_code == 200)


# ------------------------------------------------------------------ Task 3
def task3() -> None:
    print("\n== Task 3: streaming PII redaction")
    url = f"http://127.0.0.1:{E['DEMO_GW3']}/v1/chat/completions"
    text, first, t0 = "", None, time.perf_counter()
    with httpx.stream("POST", url, json={"stream": True, "messages": []}, timeout=20) as r:
        for line in r.iter_lines():
            if line.startswith("data: {"):
                for ch in json.loads(line[6:])["choices"]:
                    piece = ch["delta"].get("content") or ""
                    if piece and first is None:
                        first = time.perf_counter() - t0
                    text += piece
    leaked = [s for s in ("jane.doe", "123-45-6789", "4111") if s in text]
    check(
        "email, SSN and card redacted across chunk boundaries",
        not leaked and text.count("[REDACTED]") == 3,
        f"{text.count('[REDACTED]')} redactions",
    )
    check("non-PII digits (order number) preserved", "1234567890123" in text)
    check("first content token arrived promptly", first is not None and first < 1.0, f"TTFT {first * 1000:.0f} ms")


# ------------------------------------------------------------------ Task 4
def router_call(port, key="demo-key", max_tokens=100):
    t = time.perf_counter()
    r = httpx.post(
        f"http://127.0.0.1:{port}/v1/chat/completions",
        timeout=20,
        headers={"Authorization": f"Bearer {key}"},
        json={"messages": [{"role": "user", "content": "hi"}], "max_tokens": max_tokens},
    )
    return r, time.perf_counter() - t


def task4() -> None:
    print("\n== Task 4: rate limiter + fallback router")
    r, _ = router_call(E["DEMO_R_429"])
    check(
        "primary 429 -> failover to secondary", r.status_code == 200 and r.headers["x-gateway-provider"] == "secondary"
    )

    r1, _ = router_call(E["DEMO_R_LIMIT"], max_tokens=100)  # settles to 15 real tokens
    r2, _ = router_call(E["DEMO_R_LIMIT"], max_tokens=480)  # 15 + ~487 > 500 budget
    check(
        "tenant over its token budget -> 429 with Retry-After",
        r1.status_code == 200
        and r2.status_code == 429
        and int(r2.headers["retry-after"]) >= 1
        and r2.json()["error"]["code"] == "tenant_token_limit",
    )

    r, dt = router_call(E["DEMO_R_SLOW"])
    check(
        "primary timeout (3s) -> failover to secondary",
        r.status_code == 200 and r.headers["x-gateway-provider"] == "secondary" and 2.9 <= dt < 4.5,
        f"{dt:.2f}s",
    )

    r, _ = router_call(E["DEMO_R_DOWN"])
    err = r.json().get("error", {})
    check(
        "both providers down -> sanitized 503",
        r.status_code == 503
        and set(err) == {"type", "code", "message", "request_id"}
        and "Traceback" not in r.text
        and "secret" not in r.text,
        err.get("code", ""),
    )


if __name__ == "__main__":
    wait_ports(
        [E[k] for k in ("DEMO_GW2", "DEMO_DS", "DEMO_GW3", "DEMO_R_429", "DEMO_R_LIMIT", "DEMO_R_SLOW", "DEMO_R_DOWN")]
    )
    time.sleep(0.5)  # mock providers bind a moment after the routers
    for fn in (task1, task2, task3, task4):
        try:
            fn()
        except Exception as exc:  # a crashed scenario is a FAIL, not a traceback
            check(f"{fn.__name__} scenario crashed", False, repr(exc))
    failed = [n for n, ok, _ in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} scenarios passed")
    sys.exit(1 if failed else 0)
