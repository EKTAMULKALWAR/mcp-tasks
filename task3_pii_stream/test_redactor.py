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
        out.append(r.feed(text[i : i + n]))
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
    assert r.feed("wor") == ""  # partial word held
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
    events = [json.loads(ln[5:]) for ln in r.text.splitlines() if ln.startswith("data:") and ln[5:].strip() != "[DONE]"]
    finished = False
    for e in events:
        for ch in e["choices"]:
            assert not finished, "content arrived after finish_reason"
            assert ch["delta"] or ch.get("finish_reason"), "empty chunk sent"
            if ch.get("finish_reason"):
                finished = True
    assert events[-1]["choices"][0]["finish_reason"] == "stop"
    assert events[-1]["choices"][0]["delta"].get("content", "").endswith("shipped.")


# ---- helpers: a fake upstream that speaks SSE lines and records whether it was closed


def _evt(index, content=None, finish=None):
    delta = {} if content is None else {"content": content}
    return "data: " + json.dumps(
        {
            "id": "x",
            "object": "chat.completion.chunk",
            "created": 0,
            "model": "m",
            "choices": [{"index": index, "delta": delta, "finish_reason": finish}],
        }
    )


class FakeUpstream:
    def __init__(self, lines):
        self._lines, self.closed, self.lines_served = lines, False, 0

    async def aiter_lines(self):
        for line in self._lines:
            self.lines_served += 1
            yield line

    async def aclose(self):
        self.closed = True


async def test_upstream_closed_when_client_disconnects_mid_stream():
    endless = (_evt(0, "word ") for _ in range(1_000_000))
    up = FakeUpstream(endless)
    gen = gateway.redacted_stream(up)
    for _ in range(3):
        await gen.__anext__()
    assert not up.closed
    await gen.aclose()  # what Starlette does when the client goes away
    assert up.closed
    assert up.lines_served < 10  # we stopped pulling from upstream immediately


async def test_upstream_closed_on_normal_completion_and_on_error():
    up = FakeUpstream([_evt(0, "hi"), _evt(0, None, "stop"), "data: [DONE]"])
    _ = [c async for c in gateway.redacted_stream(up)]
    assert up.closed

    class Broken(FakeUpstream):
        async def aiter_lines(self):
            yield _evt(0, "hi")
            raise httpx.ReadError("secret upstream detail 10.0.0.9")

    up = Broken([])
    out = b"".join([c async for c in gateway.redacted_stream(up)])
    assert up.closed and b"Upstream stream interrupted" in out and b"10.0.0.9" not in out


async def test_multiple_choices_are_redacted_independently():
    texts = {0: "Mail bob.smith@example.com today, ok?", 1: "Card 4111 1111 1111 1111 and SSN 123-45-6789 end."}
    chunks = {i: [t[k : k + 3] for k in range(0, len(t), 3)] for i, t in texts.items()}
    lines = [_evt(0), _evt(1)]
    for k in range(max(map(len, chunks.values()))):  # interleave the two choices chunk by chunk
        for i in (0, 1):
            if k < len(chunks[i]):
                lines.append(_evt(i, chunks[i][k]))
    lines += [_evt(0, None, "stop"), _evt(1, None, "stop"), "data: [DONE]"]

    got, finished = {0: "", 1: ""}, set()
    async for raw in gateway.redacted_stream(FakeUpstream(lines)):
        line = raw.decode().strip()
        if line == "data: [DONE]":
            continue
        for ch in json.loads(line[5:])["choices"]:
            assert ch["index"] not in finished, "content after finish_reason"
            got[ch["index"]] += ch["delta"].get("content", "")
            if ch.get("finish_reason"):
                finished.add(ch["index"])
    assert finished == {0, 1}
    assert got == {0: redact(texts[0]), 1: redact(texts[1])}
    assert "bob" not in got[0] and "4111" not in got[1] and "123-45" not in got[1]


async def test_invalid_json_body_is_400_and_non_object_events_are_skipped():
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://gw") as c:
        assert (await c.post("/v1/chat/completions", content=b"{nope")).status_code == 400
        assert (await c.post("/v1/chat/completions", json=[1, 2])).status_code == 400
    out = b"".join(
        [
            c
            async for c in gateway.redacted_stream(
                FakeUpstream(["data: 5", 'data: "str"', _evt(0, "ok "), _evt(0, None, "stop"), "data: [DONE]"])
            )
        ]
    )
    assert b'"ok "' in out
