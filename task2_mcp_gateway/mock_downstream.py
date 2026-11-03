"""Mock downstream MCP server (JSON-RPC over HTTP). Counts calls so tests can prove short-circuiting."""

import os

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

app = FastAPI()
app.state.calls = []

TOOLS = [
    {"name": "get_weather", "description": "Weather lookup", "inputSchema": {"type": "object"}},
    {"name": "admin_reset_key", "description": "Rotate an API key", "inputSchema": {"type": "object"}},
]


def handle(msg: dict):
    app.state.calls.append(msg)
    if "id" not in msg:
        return None
    if msg["method"] == "tools/list":
        return {"jsonrpc": "2.0", "id": msg["id"], "result": {"tools": TOOLS}}
    if msg["method"] == "tools/call":
        name = msg["params"]["name"]
        return {
            "jsonrpc": "2.0",
            "id": msg["id"],
            "result": {"content": [{"type": "text", "text": f"ran {name}"}], "isError": False},
        }
    return {"jsonrpc": "2.0", "id": msg["id"], "error": {"code": -32601, "message": "Method not found"}}


@app.post("/mcp")
async def mcp(req: Request):
    payload = await req.json()
    app.state.last_headers = dict(req.headers)
    if isinstance(payload, list):
        out = [r for r in (handle(m) for m in payload) if r]
        return JSONResponse(out) if out else Response(status_code=202)
    r = handle(payload)
    return JSONResponse(r, headers={"Mcp-Session-Id": "sess-123"}) if r else Response(status_code=202)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("PORT", 9000)))
