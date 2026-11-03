"""Tenant key registry: maps an API key to a tenant name without ever storing the key.

File format (JSON object), tenant name -> SHA-256 hex of the API key, or a list of hashes
(several active keys per tenant, e.g. during key rotation):

    {"acme": "<sha256 hex>", "globex": ["<sha256 hex>", "<sha256 hex>"]}

Build entries with `python hash_key.py <key>`.

Lookup hashes the presented key and compares it against *every* stored hash with
hmac.compare_digest and without early exit, so response time does not reveal which
(or how many) stored hashes matched. Hashing first also means keys of any length
compare as equal-length digests.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from pathlib import Path

_SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")


def key_digest(api_key: str) -> str:
    return hashlib.sha256(api_key.encode()).hexdigest()


class TenantRegistry:
    def __init__(self, entries: dict[str, list[str]]) -> None:
        # flat (digest, tenant) list: iterated in full on every lookup
        self._entries = [(digest, tenant) for tenant, digests in entries.items() for digest in digests]

    @classmethod
    def from_file(cls, path: str) -> TenantRegistry:
        """Parse and validate the key file. Raises ValueError on any problem (fail closed at startup)."""
        try:
            raw = json.loads(Path(path).read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"cannot read tenant key file {path!r}: {exc.__class__.__name__}") from None
        if not isinstance(raw, dict) or not raw:
            raise ValueError("tenant key file must be a non-empty JSON object of tenant -> sha256 hex")
        entries: dict[str, list[str]] = {}
        for tenant, value in raw.items():
            digests = [value] if isinstance(value, str) else value
            if not tenant or not isinstance(digests, list) or not digests:
                raise ValueError(f"invalid entry for tenant {tenant!r}")
            cleaned = []
            for d in digests:
                if not isinstance(d, str) or not _SHA256_HEX.match(d.lower()):
                    raise ValueError(f"tenant {tenant!r}: value is not a SHA-256 hex digest (do not store raw keys)")
                cleaned.append(d.lower())
            entries[tenant] = cleaned
        return cls(entries)

    def resolve(self, api_key: str) -> str | None:
        """Return the tenant name for this key, or None."""
        presented = key_digest(api_key)
        found: str | None = None
        for digest, tenant in self._entries:  # no early exit: constant work per lookup
            if hmac.compare_digest(presented, digest):
                found = tenant
        return found
