# Integration Test Fixtures

`db_conn` is the default fixture for PostgreSQL-backed integration tests.
It acquires one connection from `pg_pool`, opens a transaction, yields the
connection to the test, and rolls the transaction back during teardown.

That rollback only protects writes made on the same connection. Tests that
open extra connections or start their own pools can commit state outside the
transaction boundary, so this directory also provides an autouse truncate
fallback. Before and after any test that uses `pg_pool` or `db_conn`, the
fixture truncates every public table with `RESTART IDENTITY CASCADE`.

The truncate fallback is the escape hatch for cross-connection tests such as
runner pool lifecycle checks. Keep default tests on `db_conn` when possible,
and only rely on extra connections when the behavior under test genuinely
crosses the transaction boundary.
