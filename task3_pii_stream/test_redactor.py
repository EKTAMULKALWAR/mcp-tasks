import json
import random

import httpx
import pytest

import gateway
import mock_provider
from redactor import MAX_HOLD, REDACTED, StreamRedactor, redact

SAMPLES = [
    "Email jane.doe+billing@example.co.uk now.",
    "SSN 123-45-6789, card 4111 1111 1111 1111, amex 3782-822463-10005? no: 378282246310005.",
    "Order 1234567890123 is not a card (fails Luhn). Phone 555-1234.",
    "a@b.co a@b.com. x@y.museum,end",
    "plain prose with no pii at all, just words and numbers like 42 and 2026.",
    "edge 000-12-3456 isn't a valid SSN; 123-45-6789 is.",
]


def stream_through(text, seed):
    rnd = random.Random(seed)
    r, out, i = StreamRedactor(), [], 0
    while i < len(text):
        n = rnd.randint(1, 8)
        out.append(r.feed(text[i:i + n]))
        i += n
    out.append(r.flush())
    return "".join(out)


@pytest.mark.parametrize("text", SAMPLES)
def test_streaming_equals_whole_text_redaction_for_any_chunking(text):
    expected = redact(text)
    for seed in range(300):
        assert stream_through(text, seed) == expected


def test_patterns():
    out = redact(SAMPLES[1] + " " + SAMPLES[0] + " " + SAMPLES[2])
    assert "123-45-6789" not in out and "4111" not in out and "378282246310005" not in out
    assert "jane.doe" not in out and out.count(REDACTED) >= 4
    assert "1234567890123" in out  # Luhn-invalid digit run kept


def test_low_latency_prose_released_immediately():
    r = StreamRedactor()
    assert r.feed("Hello there, ") == "Hello there, "
    assert r.feed("wor") == ""          # partial word held
    assert r.feed("ld! ") == "world! "  # released at the boundary


def test_memory_bounded_on_adversarial_input():
    r = StreamRedactor()
    for _ in range(10_000):
        r.feed("9" * 100)
        assert len(r._tail) <= MAX_HOLD


@pytest.mark.asyncio
async def test_end_to_end_sse():
    gateway.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=mock_provider.app))
    gateway.PROVIDER_URL = "http://provider/v1/chat/completions"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://gw") as c:
        async with c.stream("POST", "/v1/chat/completions", json={"stream": True, "messages": []}) as resp:
            text, done = "", False
            async for line in resp.aiter_lines():
                if not line.startswith("data:"):
                    continue
                d = line[5:].strip()
                if d == "[DONE]":
                    done = True
                    break
                for ch in json.loads(d)["choices"]:
                    text += ch["delta"].get("content") or ""
    assert done
    assert text == redact(mock_provider.DEFAULT_TEXT)
    assert "@" not in text and "123-45-6789" not in text and "4111" not in text


@pytest.mark.asyncio
async def test_no_content_after_finish_and_no_empty_chunks():
    gateway.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=mock_provider.app))
    gateway.PROVIDER_URL = "http://provider/v1/chat/completions"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://gw") as c:
        r = await c.post("/v1/chat/completions", json={"stream": True, "messages": []})
    events = [json.loads(l[5:]) for l in r.text.splitlines()
              if l.startswith("data:") and l[5:].strip() != "[DONE]"]
    finished = False
    for e in events:
        for ch in e["choices"]:
            assert not finished, "content arrived after finish_reason"
            assert ch["delta"] or ch.get("finish_reason"), "empty chunk sent"
            if ch.get("finish_reason"):
                finished = True
    assert events[-1]["choices"][0]["finish_reason"] == "stop"
    assert events[-1]["choices"][0]["delta"].get("content", "").endswith("shipped.")
