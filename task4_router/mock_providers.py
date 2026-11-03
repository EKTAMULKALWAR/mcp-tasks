"""Mock primary/secondary providers. Behaviour is set per process via MODE env:
ok | 429 | slow | 500.   python mock_providers.py primary 9201 slow"""
import asyncio
import sys

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

name, port, mode = sys.argv[1], int(sys.argv[2]), sys.argv[3] if len(sys.argv) > 3 else "ok"
app = FastAPI()


@app.post("/v1/chat/completions")
async def completions(req: Request):
    body = await req.json()
    if mode == "429":
        return JSONResponse({"error": "rate limited", "trace": "internal stack here"}, status_code=429)
    if mode == "500":
        return JSONResponse({"error": "Traceback (most recent call last): secret internals"}, status_code=500)
    if mode == "slow":
        await asyncio.sleep(10)
    return {"id": f"cmpl-{name}", "object": "chat.completion", "model": body["model"],
            "choices": [{"index": 0, "message": {"role": "assistant", "content": f"hello from {name}"},
                         "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=port)
