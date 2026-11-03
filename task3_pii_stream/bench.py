"""Micro-benchmark: cost of the redactor versus a plain passthrough.

Measures (all in-process, no network, so the numbers isolate the redaction work):
  * added CPU latency per chunk (mean / p50 / p99) on token-sized chunks
  * time-to-first-token (TTFT) overhead: how many chunks the first visible text is delayed
    (the redactor holds back a trailing partial word), converted to ms at an assumed
    upstream pace (TOKEN_INTERVAL_MS, default 20 ms/token -- a *simulated* pace)
  * throughput in chunks/s and MB/s
  * memory: the largest hold-back tail ever retained

Run:  python bench.py   (or `make bench`)
"""

from __future__ import annotations

import random
import statistics
import time

from redactor import MAX_HOLD, StreamRedactor

TOKEN_INTERVAL_MS = 20.0
ROUNDS = 20_000

PROSE = (
    "Thanks for reaching out. I have reviewed your account and the refund for order 1234567890123 "
    "was processed yesterday. You can contact support at help.desk+billing@example.co.uk or call "
    "us during business hours. For verification we hold the card ending 4111 1111 1111 1111 and the "
    "SSN 123-45-6789 on file; neither will be shown again. "
)


def tokens(text: str, seed: int) -> list[str]:
    """Word-ish tokens with leading spaces, 1-3 words each (how LLM APIs typically chunk)."""
    rnd, words, out, i = random.Random(seed), text.split(" "), [], 0
    while i < len(words):
        n = rnd.randint(1, 3)
        out.append((" " if i else "") + " ".join(words[i : i + n]))
        i += n
    return out


def pct(sorted_vals: list[float], q: float) -> float:
    return sorted_vals[min(len(sorted_vals) - 1, int(q * len(sorted_vals)))]


def time_per_chunk(make, chunks: list[str], rounds: int) -> list[float]:
    samples: list[float] = []
    for _ in range(rounds):
        r = make()
        for c in chunks:
            t = time.perf_counter_ns()
            r(c)
            samples.append((time.perf_counter_ns() - t) / 1000)  # microseconds
    return samples


def main() -> None:
    chunks = tokens(PROSE, seed=1)
    nbytes = sum(len(c) for c in chunks)

    def make_plain():
        return lambda c: c

    def make_redact():
        return StreamRedactor().feed

    rounds = max(1, ROUNDS // len(chunks))
    plain = time_per_chunk(make_plain, chunks, rounds)
    red = time_per_chunk(make_redact, chunks, rounds)
    plain.sort()
    red.sort()

    # TTFT: index of the first chunk that yields non-empty output
    r, first_out = StreamRedactor(), None
    for i, c in enumerate(chunks):
        if r.feed(c):
            first_out = i
            break
    delay_chunks = first_out or 0  # passthrough shows text at chunk 0

    # throughput
    t = time.perf_counter()
    for _ in range(rounds):
        feed = StreamRedactor().feed
        for c in chunks:
            feed(c)
    dt = time.perf_counter() - t
    n_chunks = rounds * len(chunks)

    # worst-case memory: adversarial 1 MB of digits, tail must stay <= MAX_HOLD
    r, worst = StreamRedactor(), 0
    for _ in range(10_000):
        r.feed("9" * 100)
        worst = max(worst, len(r._tail))

    rows = [
        ("metric", "passthrough", "redactor", "added"),
        (
            "mean / chunk (us)",
            f"{statistics.fmean(plain):.2f}",
            f"{statistics.fmean(red):.2f}",
            f"{statistics.fmean(red) - statistics.fmean(plain):+.2f}",
        ),
        (
            "p50 / chunk (us)",
            f"{pct(plain, 0.5):.2f}",
            f"{pct(red, 0.5):.2f}",
            f"{pct(red, 0.5) - pct(plain, 0.5):+.2f}",
        ),
        (
            "p99 / chunk (us)",
            f"{pct(plain, 0.99):.2f}",
            f"{pct(red, 0.99):.2f}",
            f"{pct(red, 0.99) - pct(plain, 0.99):+.2f}",
        ),
        ("TTFT delay (chunks)", "0", str(delay_chunks), f"+{delay_chunks}"),
        (
            f"TTFT delay (ms @ {TOKEN_INTERVAL_MS:.0f} ms/token)",
            "0",
            f"{delay_chunks * TOKEN_INTERVAL_MS:.0f}",
            f"+{delay_chunks * TOKEN_INTERVAL_MS:.0f}",
        ),
        ("throughput (chunks/s)", "-", f"{n_chunks / dt:,.0f}", ""),
        ("throughput (MB/s)", "-", f"{rounds * nbytes / dt / 1e6:.1f}", ""),
        ("max retained tail (chars)", "0", f"{worst} (cap {MAX_HOLD})", ""),
    ]
    widths = [max(len(row[i]) for row in rows) for i in range(4)]
    for k, row in enumerate(rows):
        print("  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)))
        if k == 0:
            print("  ".join("-" * w for w in widths))
    print(
        f"\n{len(chunks)} chunks/stream, {rounds} streams, {nbytes} bytes/stream. "
        "In-process timings: network and JSON/SSE framing are not included."
    )


if __name__ == "__main__":
    main()
