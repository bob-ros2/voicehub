lint:
	ruff check .

test:
	pytest -q tests

.PHONY: lint test
