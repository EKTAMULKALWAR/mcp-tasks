"""Incremental PII redactor for token streams.

Core idea — emit everything that can no longer become part of a PII match, hold back
only the shortest suffix that still could.

On every chunk we look at  buffer = held_tail + chunk  and compute a cut point h:
  * h starts at the beginning of the trailing "candidate run": the suffix that is
    still a plausible prefix of an email (word chars, ., +, -, optional @domain) or
    of a digit sequence (digits, spaces, dashes). In normal prose that's just the last
    partial word, so latency overhead is ~one token.
  * h is clamped so the held tail never exceeds MAX_HOLD chars (bounded memory even
    for adversarial input like a 10 MB run of digits). MAX_HOLD >= the longest match
    we redact (RFC email max 254), so a clamp can never split a real match...
  * ...and if h lands inside a complete match we already see, h moves to that match's
    start, so a match is always redacted whole, never across two emits.
Then buffer[:h] is redacted and emitted, buffer[h:] becomes the new tail.

Work per chunk is O(len(chunk) + MAX_HOLD); memory is O(MAX_HOLD + chunk).
"""
from __future__ import annotations

import re

REDACTED = "[REDACTED]"
MAX_HOLD = 256

EMAIL = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}"
SSN = r"(?<!\d)(?!000|666|9\d\d)\d{3}-(?!00)\d{2}-(?!0000)\d{4}(?!\d)"
CARD = r"(?<!\d)\d(?:[ -]?\d){12,18}(?!\d)"  # 13–19 digits, optional single space/dash separators

PII_RE = re.compile(f"(?P<email>{EMAIL})|(?P<ssn>{SSN})|(?P<card>{CARD})")
# Suffix that could still grow into a match. Searched only within the last MAX_HOLD chars.
TAIL_RE = re.compile(r"(?:[A-Za-z0-9._%+-]+(?:@[A-Za-z0-9.-]*)?|\d[\d -]*)\Z")


def luhn_ok(digits: str) -> bool:
    total, alt = 0, False
    for ch in reversed(digits):
        d = ord(ch) - 48
        if alt:
            d = d * 2 - 9 if d > 4 else d * 2
        total += d
        alt = not alt
    return total % 10 == 0


def _replacement(m: re.Match) -> str:
    if m.lastgroup == "card":
        digits = re.sub(r"\D", "", m.group())
        if not luhn_ok(digits):
            return m.group()  # order numbers, timestamps etc. pass through
    return REDACTED


def redact(text: str) -> str:
    return PII_RE.sub(_replacement, text)


class StreamRedactor:
    __slots__ = ("_tail",)

    def __init__(self) -> None:
        self._tail = ""

    def feed(self, chunk: str) -> str:
        if not chunk:
            return ""
        buf = self._tail + chunk
        n = len(buf)
        window_start = max(0, n - MAX_HOLD)

        m = TAIL_RE.search(buf, window_start)
        # search starts at window_start, so a run longer than MAX_HOLD is clamped there
        h = m.start() if m else n

        # never cut through a complete match visible in the region around the cut
        scan_from = max(0, h - MAX_HOLD)
        for pm in PII_RE.finditer(buf, scan_from):
            if pm.start() < h < pm.end():
                h = pm.start()
                break
            if pm.start() >= h:
                break

        self._tail = buf[h:]
        return redact(buf[:h]) if h else ""

    def flush(self) -> str:
        out, self._tail = redact(self._tail), ""
        return out
