"""Mock OpenAI-compatible streaming provider. Splits text into small random chunks so PII
straddles chunk boundaries."""
import asyncio
import json
import random
import time

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

app = FastAPI()
DEFAULT_TEXT = ("Sure! Contact jane.doe+billing@example.co.uk or call support. "
                "Her SSN is 123-45-6789 and card 4111 1111 1111 1111 expires soon. "
                "Order 1234567890123 shipped.")


def chunks(text: str, seed: int = 0):
    rnd = random.Random(seed)
    i = 0
    while i < len(text):
        n = rnd.randint(1, 6)
        yield text[i:i + n]
        i += n


@app.post("/v1/chat/completions")
async def completions(req: Request):
    body = await req.json()
    text = body.get("mock_text", DEFAULT_TEXT)
    delay = body.get("mock_delay", 0.0)

    async def gen():
        base = {"id": "chatcmpl-mock", "object": "chat.completion.chunk", "created": int(time.time()),
                "model": body.get("model", "mock")}
        yield f"data: {json.dumps({**base, 'choices': [{'index': 0, 'delta': {'role': 'assistant'}}]})}\n\n"
        for c in chunks(text, body.get("mock_seed", 0)):
            if delay:
                await asyncio.sleep(delay)
            yield f"data: {json.dumps({**base, 'choices': [{'index': 0, 'delta': {'content': c}, 'finish_reason': None}]})}\n\n"
        yield f"data: {json.dumps({**base, 'choices': [{'index': 0, 'delta': {}, 'finish_reason': 'stop'}]})}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=9100)
