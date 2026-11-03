PY ?= $(shell test -x .venv/bin/python && echo .venv/bin/python || echo python3)
TASKS = task1_mcp_server task2_mcp_gateway task3_pii_stream task4_router

.PHONY: test lint format typecheck demo bench

test:
	@set -e; for d in $(TASKS); do echo "== $$d"; (cd $$d && ../$(PY) -m pytest -q); done

lint:
	$(PY) -m ruff check .
	$(PY) -m ruff format --check .

format:
	$(PY) -m ruff check --fix .
	$(PY) -m ruff format .

typecheck:
	@set -e; for d in $(TASKS); do echo "== mypy $$d"; (cd $$d && ../$(PY) -m mypy --ignore-missing-imports .); done

demo:
	@PY=$(abspath $(PY)) ./scripts/demo.sh

bench:
	cd task3_pii_stream && ../$(PY) bench.py
