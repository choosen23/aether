.PHONY: test lint smoke

test:
	uv run --group dev python -m pytest tests/unit tests/contract
	uv run --group dev python -m pytest tests/load -q
	npm --prefix web test

lint:
	uv run --group dev ruff check .
	uv run --group dev mypy packages services
	npm --prefix web run lint
	npm --prefix web run typecheck

smoke:
	uv run --group dev python -m pytest tests/integration
	npm --prefix web run test:e2e
