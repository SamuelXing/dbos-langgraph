# Changelog

## 0.1.0a2 (unreleased)

- Exercise three consecutive worker crashes under seeded completion delays for
  cycles and Send fan-out.

- Check recovered node inputs before applying their state updates.
- Raise ReplayMismatchError when routing changes select a different stored input.
- Require JSON-compatible node inputs and updates.
- Add a crash/recovery test for changed Send order.

The stored node-result format changed. Recover histories created by 0.1.0a1 with
0.1.0a1; there is no migration to the new format.

## 0.1.0a1 — unreleased

- Extract the native LangGraph/DBOS adapter into an independent package.
- Isolate node histories with child workflows and retain native Pregel semantics.
- Validate registration names, builder ownership, node signatures, and invocation limits.
- Add typed package metadata, reproducible dependencies, crash fixtures, and CI configuration.

No package or remote repository has been published.
