# Repository conventions

Keep SDK experiments and test probes out of the installed package.
Do not claim production readiness without meeting docs/production-readiness.md.
Use stable names and compatible durable-call order across pending histories.
Run ruff check, ruff format --check, mypy, and pytest for implementation changes.
Changes to dependency versions require recovery tests on SQLite and PostgreSQL.

Prefer named helpers over nested expressions. Comments should explain a constraint or
reason. Document observed behavior and concrete limitations; avoid promotional claims,
repeated disclaimers, and generic roadmap prose.
