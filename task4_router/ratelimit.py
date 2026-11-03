"""Token-aware sliding-window rate limiter backed by an on-disk SQLite database.

Model: every request *reserves* an estimated token cost (prompt estimate + max_tokens)
before calling a provider, then *settles* to the real usage reported by the provider,
or *releases* the reservation if no provider produced a completion. This prevents a
burst of concurrent requests from all passing the check before any of them is counted.

Window: a true sliding log — rows (tenant, ts, tokens). A request is admitted iff
sum(tokens where ts > now - window) + cost <= limit. Expired rows are evicted inside
the same transaction, so state never grows unbounded and the sum is always exact.

Atomicity: check + evict + insert run inside one `BEGIN IMMEDIATE` transaction, which
takes SQLite's write lock up front. That makes admission race-free across threads,
asyncio tasks *and* multiple gateway processes sharing the same DB file. WAL mode keeps
readers unblocked. All blocking sqlite work runs in a worker thread (asyncio.to_thread)
so the event loop is never stalled.

Tenant keys are stored as SHA-256 hashes, never in plaintext.
"""
from __future__ import annotations

import asyncio
import hashlib
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Callable

SCHEMA = """
CREATE TABLE IF NOT EXISTS token_usage (
    id          TEXT PRIMARY KEY,
    tenant      TEXT NOT NULL,
    ts          REAL NOT NULL,
    tokens      INTEGER NOT NULL CHECK (tokens >= 0),
    settled     INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS ix_usage_tenant_ts ON token_usage (tenant, ts);
"""


@dataclass(frozen=True)
class Decision:
    allowed: bool
    reservation_id: str | None
    limit: int
    remaining: int
    retry_after: float  # seconds until enough capacity frees up (0 when allowed)


def tenant_hash(api_key: str) -> str:
    return hashlib.sha256(api_key.encode()).hexdigest()


class SlidingWindowTokenLimiter:
    def __init__(self, db_path: str, limit_tokens: int = 50_000, window_s: float = 60.0,
                 clock: Callable[[], float] = time.time) -> None:
        self.db_path, self.limit, self.window, self.clock = db_path, limit_tokens, window_s, clock
        self._conn = sqlite3.connect(db_path, isolation_level=None, check_same_thread=False, timeout=5.0)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(SCHEMA)
        self._lock = threading.Lock()  # one sqlite connection -> serialise thread access

    def close(self) -> None:
        self._conn.close()

    # ---------------------------------------------------------- sync core

    def _txn(self, fn):
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                out = fn(cur)
                cur.execute("COMMIT")
                return out
            except BaseException:
                cur.execute("ROLLBACK")
                raise

    def _reserve_sync(self, tenant: str, cost: int) -> Decision:
        if cost > self.limit:  # can never fit, don't make the client wait forever
            return Decision(False, None, self.limit, 0, self.window)

        def run(cur: sqlite3.Cursor) -> Decision:
            now = self.clock()
            cutoff = now - self.window
            cur.execute("DELETE FROM token_usage WHERE tenant = ? AND ts <= ?", (tenant, cutoff))  # eviction
            used = cur.execute("SELECT COALESCE(SUM(tokens), 0) FROM token_usage WHERE tenant = ?",
                               (tenant,)).fetchone()[0]
            if used + cost <= self.limit:
                rid = uuid.uuid4().hex
                cur.execute("INSERT INTO token_usage (id, tenant, ts, tokens) VALUES (?, ?, ?, ?)",
                            (rid, tenant, now, cost))
                return Decision(True, rid, self.limit, self.limit - used - cost, 0.0)
            # exact retry-after: walk oldest rows until enough tokens would have expired
            need, freed = used + cost - self.limit, 0
            retry = self.window
            for ts, tokens in cur.execute(
                    "SELECT ts, tokens FROM token_usage WHERE tenant = ? ORDER BY ts", (tenant,)):
                freed += tokens
                if freed >= need:
                    retry = max(0.0, ts + self.window - now)
                    break
            return Decision(False, None, self.limit, max(0, self.limit - used), retry)

        return self._txn(run)

    def _settle_sync(self, rid: str, actual_tokens: int) -> None:
        # Keeps the original timestamp: the request is still charged at the moment it started.
        self._txn(lambda c: c.execute("UPDATE token_usage SET tokens = ?, settled = 1 WHERE id = ?",
                                      (max(0, int(actual_tokens)), rid)))

    def _release_sync(self, rid: str) -> None:
        self._txn(lambda c: c.execute("DELETE FROM token_usage WHERE id = ?", (rid,)))

    def _usage_sync(self, tenant: str) -> int:
        cutoff = self.clock() - self.window
        with self._lock:
            return self._conn.execute(
                "SELECT COALESCE(SUM(tokens),0) FROM token_usage WHERE tenant=? AND ts > ?",
                (tenant, cutoff)).fetchone()[0]

    # ---------------------------------------------------------- async API

    async def reserve(self, tenant: str, cost: int) -> Decision:
        return await asyncio.to_thread(self._reserve_sync, tenant, cost)

    async def settle(self, rid: str, actual_tokens: int) -> None:
        await asyncio.to_thread(self._settle_sync, rid, actual_tokens)

    async def release(self, rid: str) -> None:
        await asyncio.to_thread(self._release_sync, rid)

    async def usage(self, tenant: str) -> int:
        return await asyncio.to_thread(self._usage_sync, tenant)
