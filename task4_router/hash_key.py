"""Print the SHA-256 hex of an API key, for building the TENANT_KEYS_FILE registry.

python hash_key.py my-secret-key        # key as an argument
echo -n my-secret-key | python hash_key.py   # or on stdin, to keep it out of shell history
"""

import sys

from tenants import key_digest

if __name__ == "__main__":
    if len(sys.argv) > 2:
        sys.exit(__doc__)
    key = sys.argv[1] if len(sys.argv) == 2 else sys.stdin.read().rstrip("\n")
    if not key:
        sys.exit(__doc__)
    print(key_digest(key))
