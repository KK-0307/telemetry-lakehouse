.PHONY: setup lint format test verify bench demo clean

setup:
	uv sync

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff format .
	uv run ruff check --fix .

test:
	uv run pytest

verify:
	uv run telemetry verify

bench:
	uv run telemetry benchmark --events 1000000 --scales 100000 --repeats 3 --label local

demo:
	scripts/demo.sh

clean:
	rm -rf lakehouse lakehouse-demo .pytest_cache .ruff_cache .hypothesis
