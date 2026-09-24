# ADR 0003: Version event contracts

## Context
Need stable cross-service and cross-language evolution path.

## Decision
Use explicit `schema_version` and checked-in AsyncAPI/contracts fixtures.

## Consequences
Contract changes are visible and testable across Python and TypeScript.

## Alternatives
Implicit schema evolution without versioning.
