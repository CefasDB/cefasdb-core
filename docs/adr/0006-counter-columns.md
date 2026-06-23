# ADR 0006 - Counter Columns

## Status

Accepted.

## Context

CefasDB already exposes `AtomicUpdate` for per-key numeric increments.
Before this ADR, a counter value was only a regular `N` attribute, so
any `PutItem` or SQL `INSERT` could overwrite or remove it by accident.

ScyllaDB-style counters make counter intent part of the schema. The
storage representation can still be numeric, but the write surface must
make non-counter mutations explicit failures.

## Decisions

### 1. Schema marker

`AttributeDefinition.Type = "COUNTER"` marks a top-level attribute as a
counter column.

Counter columns:

- are persisted with the existing `N` attribute codec;
- cannot be the partition or sort key;
- cannot declare vector dimensions;
- cannot be declared as a nested document path.

### 2. Mutation contract

Regular replace-style writes cannot set, overwrite, or remove a counter
column. That includes `PutItem`, SQL `INSERT`, and batch put operations.

`UpdateItem` and SQL `UPDATE` cannot target a counter column. They may
still update non-counter columns on a row that already contains a counter;
the executor's internal full-row rewrite is allowed only after the
assignment list has been validated.

`AtomicUpdate` is the only public mutation path for counter columns. For
counter columns it accepts `INCR_RETURN` and `ADD_RETURN`; `SET` and
`APPLY` remain available for non-counter attributes only.

### 3. Distributed merge

This ADR does not introduce replica-local counter acceptance or CRDT
merge. A counter column remains linearized through the shard leader. A
future distributed counter policy can replace the plain `N` codec for
that mode without changing the schema marker.

## Consequences

- Existing tables are unchanged.
- New counter columns require no on-disk migration.
- Clients get an explicit error instead of silent counter reset when
  they use a regular write path.
