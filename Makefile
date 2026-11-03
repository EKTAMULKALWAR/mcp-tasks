# Interpreter: PY=... to override; defaults to the project venv, else python3 on PATH.
PY ?= $(shell test -x .venv/bin/python && echo .venv/bin/python || echo python3)
PYBIN := $(if $(findstring /,$(PY)),$(abspath $(PY)),$(shell command -v $(PY)))
TASKS = task1_mcp_server task2_mcp_gateway task3_pii_stream task4_router

.PHONY: test lint format typecheck demo bench

test:
	@set -e; for d in $(TASKS); do echo "== $$d"; (cd $$d && $(PYBIN) -m pytest -q); done

lint:
	$(PYBIN) -m ruff check .
	$(PYBIN) -m ruff format --check .

format:
	$(PYBIN) -m ruff check --fix .
	$(PYBIN) -m ruff format .

typecheck:
	@set -e; for d in $(TASKS); do echo "== mypy $$d"; (cd $$d && $(PYBIN) -m mypy --ignore-missing-imports .); done

demo:
	@PY=$(PYBIN) ./scripts/demo.sh

bench:
	cd task3_pii_stream && $(PYBIN) bench.py
