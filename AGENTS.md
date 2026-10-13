# Repository conventions

Use English for all source code, documentation, issues, and pull requests.
Keep SDK experiments and test probes out of the installed package.
Do not claim production readiness without meeting docs/production-readiness.md.
Use stable names and compatible durable-call order across pending histories.
Run ruff check, ruff format --check, mypy, and pytest for implementation changes.
Changes to dependency versions require recovery tests on SQLite and PostgreSQL.
Never publish a package or create a remote repository without user authorization.
