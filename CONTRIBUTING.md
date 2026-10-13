# Contributing

Use English throughout the project. Install with `uv sync --extra dev --frozen`.
Run `ruff check .`, `ruff format --check .`, `mypy`, and `pytest -q` before review.
Recovery-affecting changes must also pass the PostgreSQL suite using a dedicated database.

Include the concrete behavior, validation evidence, and compatibility effects in changes.
Do not substitute successful-path tests for real process-crash coverage. Keep probes out
of the installed package and avoid changing adjacent SDK checkouts.

Dependency upgrades require the recovery matrix. Never label untested capabilities as
supported. Releases require an updated readiness review, clean wheel installation,
version/changelog updates, and explicit publication authorization.
