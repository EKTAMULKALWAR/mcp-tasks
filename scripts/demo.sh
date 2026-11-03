#!/usr/bin/env bash
# One-command end-to-end demo: starts every service on free ports, runs the
# scenarios in scripts/demo_checks.py, prints PASS/FAIL per scenario, and always
# kills the servers (trap) -- even if a check fails or you press Ctrl-C.
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PY="${PY:-$ROOT/.venv/bin/python}"
[ -x "$PY" ] || PY="$(command -v python3)"
TMP="$(mktemp -d)"
PIDS=()

cleanup() {
  for pid in ${PIDS[@]+"${PIDS[@]}"}; do kill "$pid" 2>/dev/null; done
  sleep 1  # grace period; then force-kill stragglers (e.g. the "slow" mock holding an in-flight request)
  for pid in ${PIDS[@]+"${PIDS[@]}"}; do kill -9 "$pid" 2>/dev/null; done
  wait 2>/dev/null
  rm -rf "$TMP"
}
trap cleanup EXIT INT TERM

free_port() { "$PY" -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'; }

# start <name> <dir> <ENV=VAL ...> -- <script> [args...]   (logs in $TMP/<name>.log)
start() {
  local name="$1" dir="$2"; shift 2
  local envs=()
  while [ "$1" != "--" ]; do envs+=("$1"); shift; done
  shift
  ( cd "$ROOT/$dir" && exec env ${envs[@]+"${envs[@]}"} "$PY" "$@" ) >"$TMP/$name.log" 2>&1 &
  PIDS+=($!)
}

SECRET="demo-secret-demo-secret-demo-secret-0123456789"
P_DS=$(free_port); P_GW2=$(free_port); P_PROV=$(free_port); P_GW3=$(free_port)
P_OK=$(free_port); P_429=$(free_port); P_SLOW=$(free_port); P_500=$(free_port)
P_R_429=$(free_port); P_R_LIMIT=$(free_port); P_R_SLOW=$(free_port); P_R_DOWN=$(free_port)

# Task 2: mock downstream MCP server + security gateway
start downstream task2_mcp_gateway PORT=$P_DS -- mock_downstream.py
start gateway2 task2_mcp_gateway PORT=$P_GW2 GATEWAY_JWT_SECRET=$SECRET \
      DOWNSTREAM_URL=http://127.0.0.1:$P_DS/mcp -- gateway.py

# Task 3: mock streaming provider + PII gateway
start provider task3_pii_stream PORT=$P_PROV -- mock_provider.py
start gateway3 task3_pii_stream PORT=$P_GW3 PROVIDER_URL=http://127.0.0.1:$P_PROV/v1/chat/completions -- gateway.py

# Task 4: mock providers in four behaviours, and one router per scenario
start m_ok task4_router PORT=$P_OK -- mock_providers.py secondary $P_OK ok
start m_429 task4_router -- mock_providers.py primary $P_429 429
start m_slow task4_router -- mock_providers.py primary $P_SLOW slow
start m_500 task4_router -- mock_providers.py primary $P_500 500
# Tenant key registry: only the SHA-256 of the demo key is written to disk (never the key itself).
DEMO_KEY="demo-key"
printf '{"demo": "%s"}\n' "$("$PY" "$ROOT/task4_router/hash_key.py" "$DEMO_KEY")" >"$TMP/tenant_keys.json"

router() {  # name port primary_port secondary_port [extra env]
  start "$1" task4_router PORT=$2 RATE_DB_PATH="$TMP/$1.db" TENANT_KEYS_FILE="$TMP/tenant_keys.json" \
        PRIMARY_URL=http://127.0.0.1:$3/v1/chat/completions \
        SECONDARY_URL=http://127.0.0.1:$4/v1/chat/completions "${@:5}" -- router.py
}
router r_429   $P_R_429   $P_429  $P_OK
router r_limit $P_R_LIMIT $P_OK   $P_OK TENANT_TPM=500
router r_slow  $P_R_SLOW  $P_SLOW $P_OK
router r_down  $P_R_DOWN  $P_500  $P_500

export DEMO_SECRET=$SECRET DEMO_ROOT=$ROOT DEMO_PY=$PY
export DEMO_GW2=$P_GW2 DEMO_DS=$P_DS DEMO_GW3=$P_GW3
export DEMO_R_429=$P_R_429 DEMO_R_LIMIT=$P_R_LIMIT DEMO_R_SLOW=$P_R_SLOW DEMO_R_DOWN=$P_R_DOWN
# Run the checker in the background and `wait`, so bash runs the trap immediately on a signal
# (it would otherwise defer the trap until the foreground command finished).
"$PY" "$ROOT/scripts/demo_checks.py" &
PIDS+=($!)
wait $!
status=$?
if [ $status -ne 0 ]; then
  echo; echo "--- server logs (tail) ---"
  for f in "$TMP"/*.log; do echo "## $(basename "$f")"; tail -n 5 "$f"; done
fi
exit $status
